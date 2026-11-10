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
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/statistics.h"
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

  void StartTail(std::unique_ptr<WalIterator>* iter, SequenceNumber* next_seq,
                 std::string* seen);
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

// Renders a batch as "Put(key)"/"Delete(key)" so that a test can assert on
// what was actually delivered rather than only on how much was.
std::string BatchContents(const WriteBatch& batch) {
  struct Handler : public WriteBatch::Handler {
    std::string seen;
    Status PutCF(uint32_t /*cf*/, const Slice& key,
                 const Slice& /*value*/) override {
      seen += "Put(" + key.ToString() + ")";
      return Status::OK();
    }
    Status DeleteCF(uint32_t /*cf*/, const Slice& key) override {
      seen += "Delete(" + key.ToString() + ")";
      return Status::OK();
    }
  } handler;
  EXPECT_OK(batch.Iterate(&handler));
  return handler.seen;
}

// Reads everything the iterator can currently deliver, appending the contents
// of each batch to *contents. Unlike ReadRecords() above, this checks that the
// run is contiguous rather than merely increasing: every batch must start at
// *next_expected_seq, which is advanced past the batch on the way out. A
// skipped or repeated sequence number therefore fails here, which is what the
// tests below need when the iterator crosses from one WAL into another.
void DrainContiguous(std::unique_ptr<WalIterator>& iter,
                     SequenceNumber* next_expected_seq, std::string* contents) {
  while (iter->Valid()) {
    ASSERT_OK(iter->status());
    BatchResult res = iter->GetBatch();
    ASSERT_EQ(*next_expected_seq, res.sequence);
    *next_expected_seq = res.sequence + res.writeBatchPtr->Count();
    *contents += BatchContents(*res.writeBatchPtr);
    iter->Next();
  }
}
}  // anonymous namespace

void DBWalIteratorTest::StartTail(std::unique_ptr<WalIterator>* iter,
                                  SequenceNumber* next_seq, std::string* seen) {
  ASSERT_OK(Put("key1", DummyString(128)));
  *iter = OpenWalIter(0);
  ASSERT_TRUE((*iter)->Valid());
  *next_seq = 1;
  seen->clear();
  DrainContiguous(*iter, next_seq, seen);
  ASSERT_EQ("Put(key1)", *seen);
  ASSERT_EQ(2U, *next_seq);
  ASSERT_OK((*iter)->status());
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
    ASSERT_FALSE(iter->Valid());
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
  for (bool tail_rotations : {false, true}) {
    SCOPED_TRACE(tail_rotations);
    Options options = OptionsForLogIterTest();
    options.wal_iterator_tail_rotations = tail_rotations;
    DestroyAndReopen(options);
    ASSERT_OK(Put("key1", DummyString(128)));

    std::unique_ptr<WalIterator> iter = OpenWalIter(0);
    ASSERT_TRUE(iter->Valid());
    iter->Next();

    // Caught up: !Valid(), but status() is OK and the iterator is not spent.
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());

    // A later write to the same WAL is picked up by calling Next() again,
    // without rebuilding the iterator.
    ASSERT_OK(Put("key2", DummyString(128)));
    iter->Next();
    ASSERT_TRUE(iter->Valid()) << iter->status().ToString();
    ASSERT_OK(iter->status());
    ASSERT_EQ(2U, iter->GetBatch().sequence);
  }
}

// Writes that bypass the WAL still consume sequence numbers, so they leave
// holes that this API surfaces as the end of the run.
TEST_F(DBWalIteratorTest, GapFromDisableWAL) {
  for (bool tail_rotations : {false, true}) {
    SCOPED_TRACE(tail_rotations);
    Options options = OptionsForLogIterTest();
    options.wal_iterator_tail_rotations = tail_rotations;
    DestroyAndReopen(options);

    WriteOptions no_wal;
    no_wal.disableWAL = true;
    ASSERT_OK(Put("key1", DummyString(128)));          // seq 1, in the WAL
    ASSERT_OK(Put("key2", DummyString(128), no_wal));  // seq 2, not in the WAL
    ASSERT_OK(Put("key3", DummyString(128)));          // seq 3, in the WAL

    std::unique_ptr<WalIterator> iter = OpenWalIter(0);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(1U, iter->GetBatch().sequence);

    // The run stops at the hole rather than silently jumping to seq 3.
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_EQ(Status::kNotFound, iter->status().code());

    // And the iterator is spent: further Next() calls do nothing and the
    // status never changes.
    iter->Next();
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_EQ(Status::kNotFound, iter->status().code());
  }
}

// Regression for ingestion gaps while the tail-rotation option is enabled.
TEST_F(DBWalIteratorTest, GapFromIngestionReseek) {
  Options options = OptionsForLogIterTest();
  options.wal_iterator_tail_rotations = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("persisted", "old"));  // seq 1
  ASSERT_OK(Flush());
  ASSERT_OK(Put("other", "before"));  // seq 2, disjoint from ingested range
  std::unique_ptr<WalIterator> iter = OpenWalIter(2);
  ASSERT_EQ(2U, iter->GetBatch().sequence);
  std::unique_ptr<WalFile> wal_before;
  ASSERT_OK(db_->GetCurrentWalFile(&wal_before));

  const std::string sst_path = dbname_ + "/ingest.sst";
  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.Open(sst_path));
  ASSERT_OK(writer.Put("persisted", "new"));
  ASSERT_OK(writer.Finish());
  IngestExternalFileOptions ingest_options;
  ingest_options.allow_blocking_flush = false;
  ASSERT_OK(db_->IngestExternalFile({sst_path}, ingest_options));
  ASSERT_EQ(3U, db_->GetLatestSequenceNumber());
  ASSERT_OK(Put("other", "after"));  // seq 4, in the same WAL as seq 2
  std::unique_ptr<WalFile> wal_after;
  ASSERT_OK(db_->GetCurrentWalFile(&wal_after));
  ASSERT_EQ(wal_before->LogNumber(), wal_after->LogNumber());

  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_EQ(Status::kNotFound, iter->status().code());
  ASSERT_OK(db_->GetUpdatesSince(3, &iter));
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ(4U, iter->GetBatch().sequence);
  ASSERT_OK(Put("other", "continued"));
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ(5U, iter->GetBatch().sequence);
}

// GetUpdatesSince is permissive about its starting point: if the requested
// sequence number is not in the WAL it silently starts later and still
// reports OK. This is the path by which a consumer recovering from a spent
// iterator can lose data without noticing.
TEST_F(DBWalIteratorTest, SilentlySkipsUnavailableStart) {
  for (bool tail_rotations : {false, true}) {
    SCOPED_TRACE(tail_rotations);
    Options options = OptionsForLogIterTest();
    options.wal_iterator_tail_rotations = tail_rotations;
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
}

// The tests below cover DBOptions::wal_iterator_tail_rotations, which lets a
// caught-up iterator continue into a WAL it was not built with, after checking
// that the WAL picks up exactly where the iterator left off.

TEST_F(DBWalIteratorTest, TailRotationSingleRotationContinues) {
  Options options = OptionsForLogIterTest();
  options.statistics = CreateDBStatistics();
  options.wal_iterator_tail_rotations = true;
  DestroyAndReopen(options);

  std::unique_ptr<WalIterator> iter;
  SequenceNumber next_seq;
  std::string seen;
  ASSERT_NO_FATAL_FAILURE(StartTail(&iter, &next_seq, &seen));

  // Rotate the WAL and write two batches to the new one.
  ASSERT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("key2", DummyString(128)));  // seq 2
  ASSERT_OK(Put("key3", DummyString(128)));  // seq 3
  ASSERT_OK(db_->FlushWAL(false));

  // The iterator crosses into the new WAL on its own, and what it delivers
  // continues the run exactly: the expected keys, in order, with no skipped
  // and no repeated sequence number (checked by DrainContiguous).
  iter->Next();
  ASSERT_TRUE(iter->Valid()) << iter->status().ToString();
  seen.clear();
  DrainContiguous(iter, &next_seq, &seen);
  ASSERT_EQ("Put(key2)Put(key3)", seen);
  ASSERT_EQ(4U, next_seq);
  ASSERT_OK(iter->status());
  ASSERT_EQ(1U,
            options.statistics->getTickerCount(WAL_ITERATOR_TAIL_ROTATIONS));
}

TEST_F(DBWalIteratorTest, TailRotationMultipleRotationsContinuesOnFastPath) {
  Options options = OptionsForLogIterTest();
  options.statistics = CreateDBStatistics();
  options.wal_iterator_tail_rotations = true;
  DestroyAndReopen(options);
  // Create a second column family so that flushing one CF rotates the WAL
  // without making old WALs obsolete (the other CF still references them).
  CreateAndReopenWithCF({"secondary"}, options);

  // Write to both CFs so both reference the initial WAL
  ASSERT_OK(Put(0, "key1", DummyString(128)));     // seq 1
  ASSERT_OK(Put(1, "anchor1", DummyString(128)));  // seq 2

  std::unique_ptr<WalIterator> iter = OpenWalIter(0);
  ASSERT_TRUE(iter->Valid());
  SequenceNumber next_seq = 1;
  std::string seen;
  DrainContiguous(iter, &next_seq, &seen);
  ASSERT_EQ("Put(key1)Put(anchor1)", seen);
  ASSERT_EQ(3U, next_seq);
  ASSERT_OK(iter->status());

  // Flush default CF -> rotates WAL. Old WAL stays alive because CF1 refs it.
  ASSERT_OK(Flush(0));
  ASSERT_OK(Put(0, "key2", DummyString(128)));     // seq 3, in W2
  ASSERT_OK(Put(1, "anchor2", DummyString(128)));  // seq 4, in W2

  // Flush default CF again -> rotates WAL again. W2 stays alive (CF1 refs it).
  ASSERT_OK(Flush(0));
  ASSERT_OK(Put(0, "key3", DummyString(128)));  // seq 5, in W3
  ASSERT_OK(db_->FlushWAL(false));

  // The iterator is caught up at the end of W1 and two rotations have
  // happened. It walks W2 first, then W3 on the next end-of-file, delivering
  // every batch in between exactly once.
  iter->Next();
  ASSERT_TRUE(iter->Valid()) << iter->status().ToString();
  seen.clear();
  DrainContiguous(iter, &next_seq, &seen);
  ASSERT_EQ("Put(key2)Put(anchor2)Put(key3)", seen);
  ASSERT_EQ(6U, next_seq);
  ASSERT_OK(iter->status());
  ASSERT_EQ(2U,
            options.statistics->getTickerCount(WAL_ITERATOR_TAIL_ROTATIONS));
}

// LastSequence() advancing means writes were accepted, not that they are in a
// WAL. A write with disableWAL therefore leaves a hole, and a hole at the tail
// is exactly what the continuity check declines.
TEST_F(DBWalIteratorTest, TailRotationDisableWalHoleAtTailDeclines) {
  Options options = OptionsForLogIterTest();
  options.wal_iterator_tail_rotations = true;
  DestroyAndReopen(options);

  WriteOptions no_wal;
  no_wal.disableWAL = true;

  std::unique_ptr<WalIterator> iter;
  SequenceNumber next_seq;
  std::string seen;
  ASSERT_NO_FATAL_FAILURE(StartTail(&iter, &next_seq, &seen));

  // Rotate, then leave a hole: seq 2 reaches no WAL, so the new WAL starts at
  // seq 3 and cannot continue a run that stopped after seq 1.
  ASSERT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("key2", DummyString(128), no_wal));  // seq 2, not in the WAL
  ASSERT_OK(Put("key3", DummyString(128)));          // seq 3, in the new WAL
  ASSERT_OK(db_->FlushWAL(false));

  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_EQ(Status::kNotFound, iter->status().code());
}

TEST_F(DBWalIteratorTest, TailRotationOptInOffPreservesBehavior) {
  // Default options: wal_iterator_tail_rotations is false
  Options options = OptionsForLogIterTest();
  options.statistics = CreateDBStatistics();
  ASSERT_FALSE(options.wal_iterator_tail_rotations);
  DestroyAndReopen(options);

  ASSERT_OK(Put("key1", DummyString(128)));
  std::unique_ptr<WalIterator> iter = OpenWalIter(0);
  ASSERT_TRUE(iter->Valid());
  iter->Next();
  // Caught up, not end of file.
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  // Rotate and write to the new WAL
  ASSERT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("key2", DummyString(128)));
  ASSERT_OK(db_->FlushWAL(false));

  // Without the opt-in, a rotation always ends the run.
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_EQ(Status::kTryAgain, iter->status().code());
  ASSERT_EQ(0U,
            options.statistics->getTickerCount(WAL_ITERATOR_TAIL_ROTATIONS));
}

// The successor WAL exists but nothing has reached it on disk yet. With
// manual_wal_flush this is exact rather than a race: no record is written to
// the file until FlushWAL().
TEST_F(DBWalIteratorTest, TailRotationSuccessorNotYetOnDiskDeclines) {
  Options options = OptionsForLogIterTest();
  options.wal_iterator_tail_rotations = true;
  options.manual_wal_flush = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("key1", DummyString(128)));  // seq 1
  ASSERT_OK(db_->FlushWAL(false));

  std::unique_ptr<WalIterator> iter = OpenWalIter(0);
  ASSERT_TRUE(iter->Valid());
  SequenceNumber next_seq = 1;
  std::string seen;
  DrainContiguous(iter, &next_seq, &seen);
  ASSERT_EQ("Put(key1)", seen);
  ASSERT_EQ(2U, next_seq);
  ASSERT_OK(iter->status());

  // Rotate, then write without flushing: the successor WAL exists and is
  // empty, while LastSequence() has moved past what the iterator delivered.
  ASSERT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("key2", DummyString(128)));  // seq 2, still buffered
  ASSERT_EQ(2U, dbfull()->GetLatestSequenceNumber());

  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_EQ(Status::kTryAgain, iter->status().code());

  // Once the record is on disk, a rebuilt iterator picks up the run where the
  // spent one left off.
  ASSERT_OK(db_->FlushWAL(false));
  std::unique_ptr<WalIterator> iter2 = OpenWalIter(next_seq);
  ASSERT_TRUE(iter2->Valid());
  std::string seen2;
  DrainContiguous(iter2, &next_seq, &seen2);
  ASSERT_EQ("Put(key2)", seen2);
  ASSERT_EQ(3U, next_seq);
  ASSERT_OK(iter2->status());
}

TEST_F(DBWalIteratorTest, TailRotationWalCompression) {
  if (!StreamingCompressionTypeSupported(kZSTD)) {
    ROCKSDB_GTEST_BYPASS("ZSTD streaming compression not supported");
    return;
  }
  Options options = OptionsForLogIterTest();
  options.wal_iterator_tail_rotations = true;
  options.wal_compression = kZSTD;
  DestroyAndReopen(options);

  std::unique_ptr<WalIterator> iter;
  SequenceNumber next_seq;
  std::string seen;
  ASSERT_NO_FATAL_FAILURE(StartTail(&iter, &next_seq, &seen));

  // A compressed WAL with a record in it reads back normally, so the fast path
  // works as it does without compression.
  ASSERT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("key2", DummyString(128)));  // seq 2, in W2
  ASSERT_OK(db_->FlushWAL(false));
  iter->Next();
  ASSERT_TRUE(iter->Valid()) << iter->status().ToString();
  seen.clear();
  DrainContiguous(iter, &next_seq, &seen);
  ASSERT_EQ("Put(key2)", seen);
  ASSERT_EQ(3U, next_seq);
  ASSERT_OK(iter->status());

  // A new compressed WAL contains compression metadata but no write batch.
  // The reader must not mistake that metadata for a contiguous update.
  ASSERT_OK(dbfull()->Flush(FlushOptions()));
  WriteOptions no_wal;
  no_wal.disableWAL = true;
  ASSERT_OK(Put("key3", DummyString(128), no_wal));  // seq 3, not in any WAL
  ASSERT_OK(db_->FlushWAL(false));

  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_EQ(Status::kTryAgain, iter->status().code());
}

// A recycled WAL starts life holding the bytes of a previous WAL. Those stale
// records carry the old WAL's log number and must not be delivered as updates
// from the new incarnation.
TEST_F(DBWalIteratorTest, TailRotationRecycledSuccessorIgnoresStaleRecords) {
  Options options = OptionsForLogIterTest();
  options.wal_iterator_tail_rotations = true;
  options.recycle_log_file_num = 1;
  // Keep obsolete WALs on the recycle list, with a compatible recovery mode.
  options.WAL_ttl_seconds = 0;
  options.WAL_size_limit_MB = 0;
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
  // Lets the successor WAL be written to without anything reaching the file,
  // so that it still holds nothing but the recycled file's stale bytes when
  // the iterator consults it. WriteOptions::disableWAL, used elsewhere for
  // this, is rejected when recycling is on.
  options.manual_wal_flush = true;
  DestroyAndReopen(options);

  // Fill the recycle list: this flush makes the WAL holding "stale" obsolete,
  // and it is kept for reuse with its records still in it.
  ASSERT_OK(Put("stale", DummyString(128)));  // seq 1
  ASSERT_OK(dbfull()->Flush(FlushOptions()));

  ASSERT_OK(Put("key1", DummyString(128)));  // seq 2, in the second WAL
  ASSERT_OK(db_->FlushWAL(false));

  std::unique_ptr<WalIterator> iter = OpenWalIter(2);
  ASSERT_TRUE(iter->Valid());
  SequenceNumber next_seq = 2;
  std::string seen;
  DrainContiguous(iter, &next_seq, &seen);
  ASSERT_EQ("Put(key1)", seen);
  ASSERT_EQ(3U, next_seq);
  ASSERT_OK(iter->status());

  // Rotate. The new WAL is created by renaming the recycled file, so it starts
  // out holding the "stale" record.
  ASSERT_OK(dbfull()->Flush(FlushOptions()));

  // Pin that setup: a WAL nothing has been written to would be empty on disk
  // had it not been recycled.
  std::unique_ptr<WalFile> current_wal;
  ASSERT_OK(db_->GetCurrentWalFile(&current_wal));
  uint64_t recycled_bytes = 0;
  ASSERT_OK(
      env_->GetFileSize(dbname_ + current_wal->PathName(), &recycled_bytes));
  ASSERT_GT(recycled_bytes, 0U);

  // Advance LastSequence() without anything reaching the successor WAL.
  ASSERT_OK(Put("key2", DummyString(128)));  // seq 3, still buffered
  ASSERT_EQ(3U, dbfull()->GetLatestSequenceNumber());

  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_EQ(Status::kTryAgain, iter->status().code());
  // And the stale record is not delivered as data either: once the buffered
  // record is on disk, a rebuilt iterator continues the run with it.
  ASSERT_OK(db_->FlushWAL(false));
  std::unique_ptr<WalIterator> iter2 = OpenWalIter(next_seq);
  ASSERT_TRUE(iter2->Valid());
  std::string seen2;
  DrainContiguous(iter2, &next_seq, &seen2);
  ASSERT_EQ("Put(key2)", seen2);
  ASSERT_EQ(4U, next_seq);
  ASSERT_OK(iter2->status());
}

#ifndef NDEBUG  // These tests use debug-only purge and sync-point hooks.
TEST_F(DBWalIteratorTest, TailRotationPurgedSuccessorDeclines) {
  Options options = OptionsForLogIterTest();
  options.wal_iterator_tail_rotations = true;
  // Obsolete WALs are deleted outright rather than archived, so that the
  // successor really is gone rather than merely moved.
  options.WAL_ttl_seconds = 0;
  options.WAL_size_limit_MB = 0;
  DestroyAndReopen(options);

  std::unique_ptr<WalIterator> iter;
  SequenceNumber next_seq;
  std::string seen;
  ASSERT_NO_FATAL_FAILURE(StartTail(&iter, &next_seq, &seen));

  // Hold off deletion so that the WAL numbers can be observed before the
  // purge, which is what makes the assertions below exact.
  ASSERT_OK(db_->DisableFileDeletions());

  // Rotate twice, writing to each new WAL, so that the WAL immediately after
  // the iterator's is itself obsolete by the end.
  ASSERT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("key2", DummyString(128)));  // seq 2, in W2
  ASSERT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Put("key3", DummyString(128)));  // seq 3, in W3
  ASSERT_OK(db_->FlushWAL(false));

  VectorLogPtr wals;
  ASSERT_OK(db_->GetSortedWalFiles(wals));
  ASSERT_EQ(3U, wals.size());
  const uint64_t successor_wal = wals[1]->LogNumber();
  const uint64_t newest_wal = wals[2]->LogNumber();
  ASSERT_EQ(2U, wals[1]->StartSequence());

  ASSERT_OK(db_->EnableFileDeletions());
  dbfull()->TEST_DeleteObsoleteFiles();

  // Only the newest WAL is left. In particular the successor holding seq 2 is
  // gone, so the only WAL the fast path can find starts at seq 3 and the
  // continuity check must decline it.
  ASSERT_OK(db_->GetSortedWalFiles(wals));
  ASSERT_EQ(1U, wals.size());
  ASSERT_EQ(newest_wal, wals[0]->LogNumber());
  ASSERT_NE(successor_wal, wals[0]->LogNumber());
  ASSERT_EQ(3U, wals[0]->StartSequence());

  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_EQ(Status::kNotFound, iter->status().code());
}

// Exercise the interval between discovering a successor and opening its reader.
TEST_F(DBWalIteratorTest, TailRotationSuccessorChangesBeforeOpen) {
  class WalOpenErrorFS : public FileSystemWrapper {
   public:
    explicit WalOpenErrorFS(const std::shared_ptr<FileSystem>& target)
        : FileSystemWrapper(target) {}
    const char* Name() const override { return "WalOpenErrorFS"; }
    IOStatus NewSequentialFile(const std::string& fname,
                               const FileOptions& file_opts,
                               std::unique_ptr<FSSequentialFile>* result,
                               IODebugContext* dbg) override {
      if (fail_open.load() && fname == live_wal) {
        return IOStatus::IOError("successor open failed");
      }
      IOStatus status =
          target()->NewSequentialFile(fname, file_opts, result, dbg);
      if (status.ok() && fail_read.load() && fname == live_wal) {
        class ReadErrorFile : public FSSequentialFileWrapper {
         public:
          explicit ReadErrorFile(std::unique_ptr<FSSequentialFile> file)
              : FSSequentialFileWrapper(file.get()), file_(std::move(file)) {}
          IOStatus Read(size_t, const IOOptions&, Slice* read_result, char*,
                        IODebugContext*) override {
            *read_result = Slice();
            return IOStatus::IOError("successor read failed");
          }

         private:
          std::unique_ptr<FSSequentialFile> file_;
        };
        result->reset(new ReadErrorFile(std::move(*result)));
      }
      if (generic_missing.load() && fname == live_wal &&
          (status.IsNotFound() || status.IsPathNotFound())) {
        return IOStatus::IOError("missing file reported as generic I/O error");
      }
      return status;
    }
    std::string live_wal;
    std::atomic<bool> fail_open{false};
    std::atomic<bool> fail_read{false};
    std::atomic<bool> generic_missing{false};
  };

  std::shared_ptr<WalOpenErrorFS> fs =
      std::make_shared<WalOpenErrorFS>(env_->GetFileSystem());
  std::unique_ptr<Env> fault_env = NewCompositeEnv(fs);
  Defer close_db([&] { Close(); });
  enum class Action {
    kPurge,
    kArchive,
    kArchiveGenericError,
    kIOError,
    kReadIOError
  };
  for (bool tail_rotations : {false, true}) {
    SCOPED_TRACE(tail_rotations);
    for (Action action :
         {Action::kPurge, Action::kArchive, Action::kArchiveGenericError,
          Action::kIOError, Action::kReadIOError}) {
      if (!tail_rotations && action == Action::kReadIOError) {
        continue;  // Preserve the existing snapshot reader's error policy.
      }
      SCOPED_TRACE(lossless_cast<int>(action));
      const bool archive_wal =
          action == Action::kArchive || action == Action::kArchiveGenericError;
      Options options = OptionsForLogIterTest();
      options.env = fault_env.get();
      options.wal_iterator_tail_rotations = tail_rotations;
      options.WAL_ttl_seconds = archive_wal ? 60 : 0;
      options.WAL_size_limit_MB = 0;
      DestroyAndReopen(options);

      std::unique_ptr<WalIterator> iter;
      SequenceNumber next_seq;
      std::string seen;
      ASSERT_NO_FATAL_FAILURE(StartTail(&iter, &next_seq, &seen));

      // Keep the successor around after it becomes obsolete until discovery
      // has selected it for the next read.
      ASSERT_OK(db_->DisableFileDeletions());
      ASSERT_OK(Flush());
      ASSERT_OK(Put("key2", "value"));
      if (!tail_rotations) {
        // Include W2 in the original snapshot to exercise the default open
        // path.
        iter = OpenWalIter(1);
        ASSERT_EQ(1U, iter->GetBatch().sequence);
      }
      std::unique_ptr<WalFile> successor;
      ASSERT_OK(db_->GetCurrentWalFile(&successor));
      fs->live_wal = LogFileName(dbname_, successor->LogNumber());
      const std::string archive =
          ArchivedLogFileName(dbname_, successor->LogNumber());
      ASSERT_OK(Flush());
      ASSERT_OK(Put("key3", "value"));
      ASSERT_OK(db_->FlushWAL(false));
      ASSERT_EQ(Status::kNotFound, env_->FileExists(archive).code());

      int open_count = 0;
      SyncPoint::GetInstance()->SetCallBack(
          "WalIteratorImpl::OpenLogFile:BeforeOpen", [&](void*) {
            if (++open_count != 1) {
              return;
            }
            fs->generic_missing.store(action == Action::kArchiveGenericError);
            if (action == Action::kReadIOError) {
              fs->fail_read.store(true);
            } else if (action == Action::kIOError) {
              fs->fail_open.store(true);
            } else {
              ASSERT_OK(db_->EnableFileDeletions());
              dbfull()->TEST_DeleteObsoleteFiles();
              ASSERT_EQ(Status::kNotFound,
                        env_->FileExists(fs->live_wal).code());
              if (archive_wal) {
                ASSERT_OK(env_->FileExists(archive));
              }
            }
          });
      SyncPoint::GetInstance()->EnableProcessing();
      Defer cleanup_sync_point([&] {
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearAllCallBacks();
        fs->fail_open.store(false);
        fs->fail_read.store(false);
        fs->generic_missing.store(false);
      });

      iter->Next();
      ASSERT_GE(open_count, 1);
      if (archive_wal) {
        seen.clear();
        DrainContiguous(iter, &next_seq, &seen);
        ASSERT_EQ(tail_rotations ? "Put(key2)Put(key3)" : "Put(key2)", seen);
        ASSERT_EQ(tail_rotations ? 4U : 3U, next_seq);
        ASSERT_EQ(tail_rotations ? Status::kOk : Status::kTryAgain,
                  iter->status().code());
      } else {
        ASSERT_FALSE(iter->Valid());
        const Status status = iter->status();
        if (action == Action::kPurge && !tail_rotations) {
          ASSERT_TRUE(status.IsNotFound() || status.IsPathNotFound())
              << status.ToString();
        } else {
          ASSERT_EQ(
              action == Action::kPurge ? Status::kTryAgain : Status::kIOError,
              status.code());
        }
      }
    }
  }
}

#endif  // NDEBUG

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
