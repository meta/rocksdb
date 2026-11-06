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
#include "env/mock_env.h"
#include "port/stack_trace.h"
#include "util/atomic.h"
#include "util/defer.h"

namespace ROCKSDB_NAMESPACE {

class DBWalIteratorTest : public DBTestBase {
 public:
  DBWalIteratorTest() : DBTestBase("db_log_iter_test", /*env_do_fsync=*/true) {}

  std::unique_ptr<WalIterator> OpenWalIter(const SequenceNumber seq) {
    std::unique_ptr<WalIterator> iter;
    Status status = dbfull()->GetUpdatesSince(seq, &iter);
    EXPECT_OK(status);
    EXPECT_TRUE(iter->Valid());
    return iter;
  }
};

namespace {
SequenceNumber ReadRecords(std::unique_ptr<WalIterator>& iter, int& count,
                           bool expect_ok = true) {
  count = 0;
  SequenceNumber lastSequence = 0;
  BatchResult res;
  while (iter->Valid()) {
    res = iter->GetBatch();
    EXPECT_TRUE(res.sequence > lastSequence);
    ++count;
    lastSequence = res.sequence;
    EXPECT_OK(iter->status());
    iter->Next();
  }
  if (expect_ok) {
    EXPECT_OK(iter->status());
  } else {
    EXPECT_NOK(iter->status());
  }
  return res.sequence;
}

void ExpectRecords(const int expected_no_records,
                   std::unique_ptr<WalIterator>& iter) {
  int num_records;
  ReadRecords(iter, num_records);
  ASSERT_EQ(num_records, expected_no_records);
}
}  // anonymous namespace

// A WAL iterator tailing a recycled blog WAL must resume past the stale tail
// left by the previous incarnation: bytes past the writer's frontier are old
// data, not corruption, and later appends must become visible via Next().
TEST_F(DBWalIteratorTest, RecycledWalResumesAfterCachedStaleTail) {
  Options options = OptionsForLogIterTest();
  options.use_blog_format_for_wals = true;
  options.recycle_log_file_num = 1;
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
  DestroyAndReopen(options);
  Random rnd(301);
  ASSERT_OK(Put("big", rnd.RandomString(256 * 1024)));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("seed", "s"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("first", "v"));
  ASSERT_OK(db_->FlushWAL(true));
  const SequenceNumber first_sequence = db_->GetLatestSequenceNumber();
  std::unique_ptr<WalIterator> iter = OpenWalIter(first_sequence);
  ExpectRecords(1, iter);
  for (int round = 0; round < 4; ++round) {
    ASSERT_OK(Put(Key(round), rnd.RandomString(40000)));
    ASSERT_OK(db_->FlushWAL(true));
    iter->Next();
    ASSERT_TRUE(iter->Valid()) << iter->status().ToString();
    ASSERT_EQ(iter->GetBatch().sequence, first_sequence + round + 1);
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }
}

// A live reader on a non-recycled blog WAL hits short tails as the file
// grows, but those bytes are never invalidated, so the tail must be
// continued sequentially without reopening the input.
TEST_F(DBWalIteratorTest, LiveNonRecycledTailDoesNotRefresh) {
  Options options = OptionsForLogIterTest();
  options.use_blog_format_for_wals = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("key1", DummyString(128)));
  auto iter = OpenWalIter(0);
  ASSERT_TRUE(iter->Valid());
  // Short tail on a live file: nothing to deliver yet, no error.
  iter->Next();
  ASSERT_TRUE(!iter->Valid());
  ASSERT_OK(iter->status());
  // The writer finishes the tail; the next poll delivers it in place.
  ASSERT_OK(Put("key2", DummyString(128)));
  ASSERT_OK(db_->FlushWAL(true));
  iter->Next();
  ASSERT_TRUE(iter->Valid()) << iter->status().ToString();
  ASSERT_EQ(iter->GetBatch().sequence, 2U);
  ASSERT_OK(iter->status());
}

TEST_F(DBWalIteratorTest, Basic) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);
    ASSERT_OK(Put(0, "key1", DummyString(1024)));
    ASSERT_OK(Put(1, "key2", DummyString(1024)));
    ASSERT_OK(Put(1, "key2", DummyString(1024)));
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 3U);
    {
      auto iter = OpenWalIter(0);
      ExpectRecords(3, iter);
    }
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    env_->SleepForMicroseconds(2 * 1000 * 1000);
    {
      ASSERT_OK(Put(0, "key4", DummyString(1024)));
      ASSERT_OK(Put(1, "key5", DummyString(1024)));
      ASSERT_OK(Put(0, "key6", DummyString(1024)));
    }
    {
      auto iter = OpenWalIter(0);
      ExpectRecords(6, iter);
    }
  } while (ChangeCompactOptions());
}

#ifndef NDEBUG  // sync point is not included with DNDEBUG build
TEST_F(DBWalIteratorTest, Race) {
  static const int LOG_ITERATOR_RACE_TEST_COUNT = 2;
  static const char* sync_points[LOG_ITERATOR_RACE_TEST_COUNT][4] = {
      {"WalManager::GetSortedWalFiles:1", "WalManager::PurgeObsoleteFiles:1",
       "WalManager::PurgeObsoleteFiles:2", "WalManager::GetSortedWalFiles:2"},
      {"WalManager::GetSortedWalsOfType:1", "WalManager::PurgeObsoleteFiles:1",
       "WalManager::PurgeObsoleteFiles:2",
       "WalManager::GetSortedWalsOfType:2"}};
  for (int test = 0; test < LOG_ITERATOR_RACE_TEST_COUNT; ++test) {
    // Setup sync point dependency to reproduce the race condition of
    // a log file moved to archived dir, in the middle of GetSortedWalFiles
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
        {sync_points[test][0], sync_points[test][1]},
        {sync_points[test][2], sync_points[test][3]},
    });

    do {
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearTrace();
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
      Options options = OptionsForLogIterTest();
      DestroyAndReopen(options);
      ASSERT_OK(Put("key1", DummyString(1024)));
      ASSERT_OK(dbfull()->Flush(FlushOptions()));
      ASSERT_OK(Put("key2", DummyString(1024)));
      ASSERT_OK(dbfull()->Flush(FlushOptions()));
      ASSERT_OK(Put("key3", DummyString(1024)));
      ASSERT_OK(dbfull()->Flush(FlushOptions()));
      ASSERT_OK(Put("key4", DummyString(1024)));
      ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 4U);
      ASSERT_OK(dbfull()->FlushWAL(false));

      {
        auto iter = OpenWalIter(0);
        ExpectRecords(4, iter);
      }

      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
      // trigger async flush, and log move. Well, log move will
      // wait until the GetSortedWalFiles:1 to reproduce the race
      // condition
      FlushOptions flush_options;
      flush_options.wait = false;
      ASSERT_OK(dbfull()->Flush(flush_options));

      // "key5" would be written in a new memtable and log
      ASSERT_OK(Put("key5", DummyString(1024)));
      ASSERT_OK(dbfull()->FlushWAL(false));
      {
        // this iter would miss "key4" if not fixed
        auto iter = OpenWalIter(0);
        ExpectRecords(5, iter);
      }
    } while (ChangeCompactOptions());
  }
}

TEST_F(DBWalIteratorTest, CheckWhenArchive) {
  RelaxedAtomic<bool> callback_hit{};
  do {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearTrace();
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    ColumnFamilyHandle* cf;
    auto s = dbfull()->CreateColumnFamily(ColumnFamilyOptions(), "CF", &cf);
    ASSERT_TRUE(s.ok());

    ASSERT_OK(dbfull()->Put(WriteOptions(), cf, "key1", DummyString(1024)));

    ASSERT_OK(dbfull()->Put(WriteOptions(), "key2", DummyString(1024)));

    ASSERT_OK(dbfull()->Flush(FlushOptions()));

    ASSERT_OK(dbfull()->Put(WriteOptions(), "key3", DummyString(1024)));

    ASSERT_OK(dbfull()->Flush(FlushOptions()));

    ASSERT_OK(dbfull()->Put(WriteOptions(), "key4", DummyString(1024)));
    ASSERT_OK(dbfull()->Flush(FlushOptions()));

    callback_hit.StoreRelaxed(false);
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "WalManager::PurgeObsoleteFiles:1", [&](void*) {
          auto iter = OpenWalIter(0);
          ExpectRecords(4, iter);
          callback_hit.StoreRelaxed(true);
        });

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_OK(dbfull()->Flush(FlushOptions(), cf));
    // Try lots of things to ensure callback is triggered
    ASSERT_OK(dbfull()->TEST_SwitchWAL());
    ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
    ASSERT_OK(dbfull()->TEST_WaitForPurge());
    delete cf;
    ASSERT_TRUE(callback_hit.LoadRelaxed());
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    Close();
  } while (ChangeCompactOptions());
}
#endif

TEST_F(DBWalIteratorTest, StallAtLastRecord) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    ASSERT_OK(Put("key1", DummyString(1024)));
    auto iter = OpenWalIter(0);
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(!iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_OK(Put("key2", DummyString(1024)));
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
  } while (ChangeCompactOptions());
}

TEST_F(DBWalIteratorTest, CheckAfterRestart) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    ASSERT_OK(Put("key1", DummyString(1024)));
    ASSERT_OK(Put("key2", DummyString(1023)));
    ASSERT_OK(dbfull()->Flush(FlushOptions()));
    Reopen(options);
    auto iter = OpenWalIter(0);
    ExpectRecords(2, iter);
  } while (ChangeCompactOptions());
}

TEST_F(DBWalIteratorTest, CorruptedLog) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);

    for (int i = 0; i < 1024; i++) {
      ASSERT_OK(Put("key" + std::to_string(i), DummyString(10)));
    }

    ASSERT_OK(Flush());
    ASSERT_OK(db_->FlushWAL(false));

    // Corrupt this log to create a gap
    ASSERT_OK(db_->DisableFileDeletions());

    VectorLogPtr wal_files;
    ASSERT_OK(db_->GetSortedWalFiles(wal_files));
    ASSERT_FALSE(wal_files.empty());

    const auto logfile_path = dbname_ + "/" + wal_files.front()->PathName();
    ASSERT_OK(test::TruncateFile(env_, logfile_path,
                                 wal_files.front()->SizeFileBytes() / 2));

    ASSERT_OK(db_->EnableFileDeletions());

    // Insert a new entry to a new log file
    ASSERT_OK(Put("key1025", DummyString(10)));
    ASSERT_OK(db_->FlushWAL(false));

    // Try to read from the beginning. Should stop before the gap and read less
    // than 1025 entries
    auto iter = OpenWalIter(0);
    int count = 0;
    SequenceNumber last_sequence_read = ReadRecords(iter, count, false);
    ASSERT_LT(last_sequence_read, 1025U);

    // Try to read past the gap, should be able to seek to key1025
    auto iter2 = OpenWalIter(last_sequence_read + 1);
    ExpectRecords(1, iter2);
  } while (ChangeCompactOptions());
}

TEST_F(DBWalIteratorTest, BatchOperations) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);
    WriteBatch batch;
    ASSERT_OK(batch.Put(handles_[1], "key1", DummyString(1024)));
    ASSERT_OK(batch.Put(handles_[0], "key2", DummyString(1024)));
    ASSERT_OK(batch.Put(handles_[1], "key3", DummyString(1024)));
    ASSERT_OK(batch.Delete(handles_[0], "key2"));
    ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));
    ASSERT_OK(Flush(1));
    ASSERT_OK(Flush(0));
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    ASSERT_OK(Put(1, "key4", DummyString(1024)));
    auto iter = OpenWalIter(3);
    ExpectRecords(2, iter);
  } while (ChangeCompactOptions());
}

TEST_F(DBWalIteratorTest, Blobs) {
  Options options = OptionsForLogIterTest();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  {
    WriteBatch batch;
    ASSERT_OK(batch.Put(handles_[1], "key1", DummyString(1024)));
    ASSERT_OK(batch.Put(handles_[0], "key2", DummyString(1024)));
    ASSERT_OK(batch.PutLogData(Slice("blob1")));
    ASSERT_OK(batch.Put(handles_[1], "key3", DummyString(1024)));
    ASSERT_OK(batch.PutLogData(Slice("blob2")));
    ASSERT_OK(batch.Delete(handles_[0], "key2"));
    ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
  }

  auto res = OpenWalIter(0)->GetBatch();
  struct Handler : public WriteBatch::Handler {
    std::string seen;
    Status PutCF(uint32_t cf, const Slice& key, const Slice& value) override {
      seen += "Put(" + std::to_string(cf) + ", " + key.ToString() + ", " +
              std::to_string(value.size()) + ")";
      return Status::OK();
    }
    Status MergeCF(uint32_t cf, const Slice& key, const Slice& value) override {
      seen += "Merge(" + std::to_string(cf) + ", " + key.ToString() + ", " +
              std::to_string(value.size()) + ")";
      return Status::OK();
    }
    void LogData(const Slice& blob) override {
      seen += "LogData(" + blob.ToString() + ")";
    }
    Status DeleteCF(uint32_t cf, const Slice& key) override {
      seen += "Delete(" + std::to_string(cf) + ", " + key.ToString() + ")";
      return Status::OK();
    }
  } handler;
  ASSERT_OK(res.writeBatchPtr->Iterate(&handler));
  ASSERT_EQ(
      "Put(1, key1, 1024)"
      "Put(0, key2, 1024)"
      "LogData(blob1)"
      "Put(1, key3, 1024)"
      "LogData(blob2)"
      "Delete(0, key2)",
      handler.seen);
}

// The tests below pin the semantics documented on WalIterator and
// DB::GetUpdatesSince, including the sharp edges, so that changing any of
// them is a deliberate and visible decision rather than an accident.

// A caught-up iterator is usable, not finished: Next() is how a consumer
// tails a DB.
TEST_F(DBWalIteratorTest, CaughtUpThenResumes) {
  Options options = OptionsForLogIterTest();
  DestroyAndReopen(options);
  ASSERT_OK(Put("key1", DummyString(128)));

  auto iter = OpenWalIter(0);
  ASSERT_TRUE(iter->Valid());
  iter->Next();

  // Caught up: !Valid(), but status() is OK and the iterator is not spent.
  ASSERT_TRUE(!iter->Valid());
  ASSERT_OK(iter->status());

  // A later write to the same WAL is picked up by calling Next() again,
  // without rebuilding the iterator.
  ASSERT_OK(Put("key2", DummyString(128)));
  iter->Next();
  ASSERT_TRUE(iter->Valid()) << iter->status().ToString();
  ASSERT_OK(iter->status());
  ASSERT_EQ(2U, iter->GetBatch().sequence);
}

TEST_F(DBWalIteratorTest, CaughtUpAfterShortReadResumesBothFormats) {
  for (bool blog : {false, true}) {
    SCOPED_TRACE(blog ? "blog WAL" : "legacy WAL");
    Options options = OptionsForLogIterTest();
    options.use_blog_format_for_wals = blog;
    options.wal_compression = kNoCompression;
    DestroyAndReopen(options);

    for (int i = 0; i < 40; ++i) {
      ASSERT_OK(Put(Key(i), DummyString(1024)));
    }
    std::unique_ptr<WalIterator> iter = OpenWalIter(0);
    ExpectRecords(40, iter);
    ASSERT_OK(iter->status());

    for (SequenceNumber sequence = 41; sequence <= 43; ++sequence) {
      ASSERT_OK(Put("tail", DummyString(128)));
      iter->Next();
      ASSERT_TRUE(iter->Valid()) << iter->status().ToString();
      ASSERT_OK(iter->status());
      ASSERT_EQ(sequence, iter->GetBatch().sequence);
      iter->Next();
      ASSERT_FALSE(iter->Valid());
      ASSERT_OK(iter->status());
    }
    Close();
  }
}

// A WAL with no records yet (blog header only, or a legacy compression type
// record only) is listed with a placeholder start sequence. Neither listing it
// in that state nor searching past it may hide the earlier WAL that holds the
// requested sequence.
TEST_F(DBWalIteratorTest, NoRecordWalDoesNotHideEarlierWal) {
  for (bool blog : {false, true}) {
    if (!blog && !StreamingCompressionTypeSupported(kZSTD)) {
      continue;
    }
    SCOPED_TRACE(blog ? "blog WAL" : "legacy compressed WAL");
    Options options = OptionsForLogIterTest();
    options.use_blog_format_for_wals = blog;
    options.wal_compression = blog ? kNoCompression : kZSTD;
    DestroyAndReopen(options);
    for (int i = 0; i < 10; ++i) {
      ASSERT_OK(Put(Key(i), DummyString(16)));
    }
    // The new live WAL holds no records until the next write.
    Reopen(options);
    VectorWalPtr wals;
    ASSERT_OK(db_->GetSortedWalFiles(wals));
    {
      std::unique_ptr<WalIterator> iter = OpenWalIter(5);
      ASSERT_EQ(iter->GetBatch().sequence, 5U);
    }
    ASSERT_OK(Put("after", DummyString(16)));
    {
      std::unique_ptr<WalIterator> iter = OpenWalIter(5);
      ASSERT_EQ(iter->GetBatch().sequence, 5U);
    }
    Close();
  }
}

// Writes that bypass the WAL still consume sequence numbers, so they leave
// holes that this API surfaces as the end of the run.
TEST_F(DBWalIteratorTest, GapFromDisableWAL) {
  Options options = OptionsForLogIterTest();
  DestroyAndReopen(options);

  WriteOptions no_wal;
  no_wal.disableWAL = true;
  ASSERT_OK(Put("key1", DummyString(128)));          // seq 1, in the WAL
  ASSERT_OK(Put("key2", DummyString(128), no_wal));  // seq 2, not in the WAL
  ASSERT_OK(Put("key3", DummyString(128)));          // seq 3, in the WAL

  auto iter = OpenWalIter(0);
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(1U, iter->GetBatch().sequence);

  // The run stops at the hole rather than silently jumping to seq 3.
  iter->Next();
  ASSERT_TRUE(!iter->Valid());
  ASSERT_TRUE(iter->status().IsNotFound()) << iter->status().ToString();

  // And the iterator is spent: further Next() calls do nothing and the
  // status never changes.
  iter->Next();
  iter->Next();
  ASSERT_TRUE(!iter->Valid());
  ASSERT_TRUE(iter->status().IsNotFound());
}

// GetUpdatesSince is permissive about its starting point: if the requested
// sequence number is not in the WAL it silently starts later and still
// reports OK. This is the path by which a consumer recovering from a spent
// iterator can lose data without noticing.
TEST_F(DBWalIteratorTest, SilentlySkipsUnavailableStart) {
  Options options = OptionsForLogIterTest();
  DestroyAndReopen(options);

  WriteOptions no_wal;
  no_wal.disableWAL = true;
  ASSERT_OK(Put("key1", DummyString(128), no_wal));  // seq 1, not in the WAL
  ASSERT_OK(Put("key2", DummyString(128)));          // seq 2, in the WAL

  std::unique_ptr<WalIterator> iter;
  ASSERT_OK(dbfull()->GetUpdatesSince(1, &iter));
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  // Asked for seq 1, silently given seq 2, with no error anywhere.
  ASSERT_EQ(2U, iter->GetBatch().sequence);
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
