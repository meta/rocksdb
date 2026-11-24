//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/column_family.h"
#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {

class DBTestInPlaceUpdate : public DBTestBase {
 public:
  DBTestInPlaceUpdate()
      : DBTestBase("db_inplace_update_test", /*env_do_fsync=*/true) {}

 protected:
  Options InPlaceUpdateOptions() {
    Options options = CurrentOptions();
    options.inplace_update_support = true;
    options.allow_concurrent_memtable_write = false;
    options.statistics = CreateDBStatistics();
    return options;
  }

  static Options WithoutInPlaceUpdates(Options options) {
    options.inplace_update_support = false;
    return options;
  }

  // Opens the DB with `options` (with in-place updates) for the default column
  // family, and with the same options without in-place updates for column
  // family 1 ("regular").
  void ReopenWithRegularCF(const Options& options) {
    ReopenWithColumnFamilies(
        {kDefaultColumnFamilyName, "regular"},
        std::vector<Options>{options, WithoutInPlaceUpdates(options)});
  }

  void DestroyAndReopenWithRegularCF(const Options& options) {
    DestroyAndReopen(options);
    CreateColumnFamilies({"regular"}, WithoutInPlaceUpdates(options));
    ReopenWithRegularCF(options);
  }

  uint64_t NumRangeDeletionsInMemtable(int cf) {
    return static_cast_with_check<ColumnFamilyHandleImpl>(handles_[cf])
        ->cfd()
        ->mem()
        ->NumRangeDeletion();
  }
};

TEST_F(DBTestInPlaceUpdate, InPlaceUpdate) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.inplace_update_support = true;
    options.env = env_;
    options.write_buffer_size = 100000;
    options.allow_concurrent_memtable_write = false;
    Reopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // Update key with values of smaller size
    int numValues = 10;
    for (int i = numValues; i > 0; i--) {
      std::string value = DummyString(i, 'a');
      ASSERT_OK(Put(1, "key", value));
      ASSERT_EQ(value, Get(1, "key"));
    }

    // Only 1 instance for that key.
    validateNumberOfEntries(1, 1);
  } while (ChangeCompactOptions());
}

TEST_F(DBTestInPlaceUpdate, InPlaceUpdateLargeNewValue) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.inplace_update_support = true;
    options.env = env_;
    options.write_buffer_size = 100000;
    options.allow_concurrent_memtable_write = false;
    Reopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // Update key with values of larger size
    int numValues = 10;
    for (int i = 0; i < numValues; i++) {
      std::string value = DummyString(i, 'a');
      ASSERT_OK(Put(1, "key", value));
      ASSERT_EQ(value, Get(1, "key"));
    }

    // All 10 updates exist in the internal iterator
    validateNumberOfEntries(numValues, 1);
  } while (ChangeCompactOptions());
}

TEST_F(DBTestInPlaceUpdate, InPlaceUpdateEntitySmallerNewValue) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.inplace_update_support = true;
    options.env = env_;
    options.allow_concurrent_memtable_write = false;

    Reopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // Update key with values of smaller size
    constexpr int num_values = 10;
    for (int i = num_values; i > 0; --i) {
      constexpr char key[] = "key";
      const std::string value = DummyString(i, 'a');
      WideColumns wide_columns{{"attr", value}};

      ASSERT_OK(db_->PutEntity(WriteOptions(), handles_[1], key, wide_columns));
      // TODO: use Get to check entity once it's supported
    }

    // Only 1 instance for that key.
    validateNumberOfEntries(1, 1);
  } while (ChangeCompactOptions());
}

TEST_F(DBTestInPlaceUpdate, InPlaceUpdateEntityLargerNewValue) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.inplace_update_support = true;
    options.env = env_;
    options.allow_concurrent_memtable_write = false;

    Reopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // Update key with values of larger size
    constexpr int num_values = 10;
    for (int i = 0; i < num_values; ++i) {
      constexpr char key[] = "key";
      const std::string value = DummyString(i, 'a');
      WideColumns wide_columns{{"attr", value}};

      ASSERT_OK(db_->PutEntity(WriteOptions(), handles_[1], key, wide_columns));
      // TODO: use Get to check entity once it's supported
    }

    // All 10 updates exist in the internal iterator
    validateNumberOfEntries(num_values, 1);
  } while (ChangeCompactOptions());
}

TEST_F(DBTestInPlaceUpdate, InPlaceUpdateCallbackSmallerSize) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.inplace_update_support = true;

    options.env = env_;
    options.write_buffer_size = 100000;
    options.inplace_callback =
        ROCKSDB_NAMESPACE::DBTestInPlaceUpdate::updateInPlaceSmallerSize;
    options.allow_concurrent_memtable_write = false;
    Reopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // Update key with values of smaller size
    int numValues = 10;
    ASSERT_OK(Put(1, "key", DummyString(numValues, 'a')));
    ASSERT_EQ(DummyString(numValues, 'c'), Get(1, "key"));

    for (int i = numValues; i > 0; i--) {
      ASSERT_OK(Put(1, "key", DummyString(i, 'a')));
      ASSERT_EQ(DummyString(i - 1, 'b'), Get(1, "key"));
    }

    // Only 1 instance for that key.
    validateNumberOfEntries(1, 1);
  } while (ChangeCompactOptions());
}

TEST_F(DBTestInPlaceUpdate, InPlaceUpdateCallbackSmallerVarintSize) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.inplace_update_support = true;

    options.env = env_;
    options.write_buffer_size = 100000;
    options.inplace_callback =
        ROCKSDB_NAMESPACE::DBTestInPlaceUpdate::updateInPlaceSmallerVarintSize;
    options.allow_concurrent_memtable_write = false;
    Reopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // Update key with values of smaller varint size
    int numValues = 265;
    ASSERT_OK(Put(1, "key", DummyString(numValues, 'a')));
    ASSERT_EQ(DummyString(numValues, 'c'), Get(1, "key"));

    for (int i = numValues; i > 0; i--) {
      ASSERT_OK(Put(1, "key", DummyString(i, 'a')));
      ASSERT_EQ(DummyString(1, 'b'), Get(1, "key"));
    }

    // Only 1 instance for that key.
    validateNumberOfEntries(1, 1);
  } while (ChangeCompactOptions());
}

TEST_F(DBTestInPlaceUpdate, InPlaceUpdateCallbackLargeNewValue) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.inplace_update_support = true;

    options.env = env_;
    options.write_buffer_size = 100000;
    options.inplace_callback =
        ROCKSDB_NAMESPACE::DBTestInPlaceUpdate::updateInPlaceLargerSize;
    options.allow_concurrent_memtable_write = false;
    Reopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // Update key with values of larger size
    int numValues = 10;
    for (int i = 0; i < numValues; i++) {
      ASSERT_OK(Put(1, "key", DummyString(i, 'a')));
      ASSERT_EQ(DummyString(i, 'c'), Get(1, "key"));
    }

    // No inplace updates. All updates are puts with new seq number
    // All 10 updates exist in the internal iterator
    validateNumberOfEntries(numValues, 1);
  } while (ChangeCompactOptions());
}

TEST_F(DBTestInPlaceUpdate, InPlaceUpdateCallbackNoAction) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.inplace_update_support = true;

    options.env = env_;
    options.write_buffer_size = 100000;
    options.inplace_callback =
        ROCKSDB_NAMESPACE::DBTestInPlaceUpdate::updateInPlaceNoAction;
    options.allow_concurrent_memtable_write = false;
    Reopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // Callback function requests no actions from db
    ASSERT_OK(Put(1, "key", DummyString(1, 'a')));
    ASSERT_EQ(Get(1, "key"), "NOT_FOUND");
  } while (ChangeCompactOptions());
}

TEST_F(DBTestInPlaceUpdate, InPlaceUpdateAndSnapshot) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.inplace_update_support = true;
    options.env = env_;
    options.write_buffer_size = 100000;
    options.allow_concurrent_memtable_write = false;
    Reopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // Update key with values of smaller size, and
    // run GetSnapshot and ReleaseSnapshot
    int numValues = 2;
    for (int i = numValues; i > 0; i--) {
      const Snapshot* s = db_->GetSnapshot();
      ASSERT_EQ(nullptr, s);
      std::string value = DummyString(i, 'a');
      ASSERT_OK(Put(1, "key", value));
      ASSERT_EQ(value, Get(1, "key"));
      // release s (nullptr)
      db_->ReleaseSnapshot(s);
    }

    // Only 1 instance for that key.
    validateNumberOfEntries(1, 1);
  } while (ChangeCompactOptions());
}

// An in-place update keeps the sequence number of the entry it overwrites, so a
// range tombstone with a sequence number between the two writes would hide the
// new value. DeleteRange() is rejected for such a column family.
TEST_F(DBTestInPlaceUpdate, DeleteRangeRejected) {
  Options options = InPlaceUpdateOptions();
  DestroyAndReopenWithRegularCF(options);

  ASSERT_OK(Put(0, "k", "v1"));
  Status s = db_->DeleteRange(WriteOptions(), handles_[0], "a", "z");
  // "v2" is no larger than "v1", so it overwrites "v1" in place and keeps the
  // sequence number of "v1", which is older than the range tombstone's.
  ASSERT_OK(Put(0, "k", "v2"));
  ASSERT_EQ(1, TestGetTickerCount(options, NUMBER_KEYS_UPDATED));
  // Read before checking `s`, so that this test fails on the hidden value if
  // the DeleteRange() is accepted.
  ASSERT_EQ("v2", Get(0, "k"));
  ASSERT_EQ(s.code(), Status::Code::kNotSupported);
  ASSERT_EQ(0, NumRangeDeletionsInMemtable(0));
  // The DeleteRange() was rejected before the WAL write, so the DB has no
  // background error and keeps accepting writes, as the Put() above shows.
  ASSERT_OK(dbfull()->TEST_GetBGError());

  // A column family without in-place updates in the same DB still accepts it.
  ASSERT_OK(Put(1, "k", "v1"));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), handles_[1], "a", "z"));
  ASSERT_EQ("NOT_FOUND", Get(1, "k"));
  ASSERT_OK(Put(1, "k", "v2"));
  ASSERT_EQ("v2", Get(1, "k"));
}

// A WriteBatch with a range deletion for a column family with in-place updates
// is rejected as a whole, before any of it reaches the WAL or a memtable.
TEST_F(DBTestInPlaceUpdate, WriteBatchWithDeleteRangeRejected) {
  Options options = InPlaceUpdateOptions();
  DestroyAndReopenWithRegularCF(options);
  ASSERT_OK(Put(0, "k", "v1"));
  const SequenceNumber seqno = db_->GetLatestSequenceNumber();

  WriteBatch batch;
  ASSERT_OK(batch.Put(handles_[1], "x", "vx"));
  ASSERT_OK(batch.DeleteRange(handles_[0], "a", "z"));
  ASSERT_OK(batch.Put(handles_[0], "y", "vy"));
  ASSERT_EQ(db_->Write(WriteOptions(), &batch).code(),
            Status::Code::kNotSupported);
  ASSERT_EQ(seqno, db_->GetLatestSequenceNumber());
  ASSERT_OK(dbfull()->TEST_GetBGError());
  // Reopening replays the WAL, which would bring back any part of the batch
  // that was written to it.
  ReopenWithRegularCF(options);
  ASSERT_EQ("v1", Get(0, "k"));
  ASSERT_EQ("NOT_FOUND", Get(0, "y"));
  ASSERT_EQ("NOT_FOUND", Get(1, "x"));
  ASSERT_OK(Put(0, "k", "v2"));
  ASSERT_EQ("v2", Get(0, "k"));

  // A batch whose range deletions are all for the column family without
  // in-place updates is accepted.
  batch.Clear();
  ASSERT_OK(batch.Put(handles_[0], "y", "vy"));
  ASSERT_OK(batch.Put(handles_[1], "x", "vx"));
  ASSERT_OK(batch.DeleteRange(handles_[1], "a", "z"));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  ASSERT_EQ("vy", Get(0, "y"));
  ASSERT_EQ("NOT_FOUND", Get(1, "x"));
}

// Writes look up the column families of their range deletions only while the
// DB has a column family with in-place updates, also when that column family
// is created or dropped after DB::Open().
TEST_F(DBTestInPlaceUpdate, DeleteRangeCheckFollowsColumnFamilies) {
  DestroyAndReopen(WithoutInPlaceUpdates(InPlaceUpdateOptions()));
  int num_lookups = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::CheckRangeDeletionsSupported:FindColumnFamilies",
      [&](void* /*arg*/) { ++num_lookups; });
  SyncPoint::GetInstance()->EnableProcessing();
  ColumnFamilyHandle* default_cf = db_->DefaultColumnFamily();

  ASSERT_OK(db_->DeleteRange(WriteOptions(), default_cf, "a", "z"));
  ASSERT_EQ(0, num_lookups);

  CreateColumnFamilies({"inplace1", "inplace2"}, InPlaceUpdateOptions());
  ASSERT_EQ(db_->DeleteRange(WriteOptions(), handles_[0], "a", "z").code(),
            Status::Code::kNotSupported);
  ASSERT_OK(db_->DeleteRange(WriteOptions(), default_cf, "a", "z"));
  ASSERT_EQ(2, num_lookups);

  // "inplace2" still has in-place updates.
  ASSERT_OK(db_->DropColumnFamily(handles_[0]));
  ASSERT_EQ(db_->DeleteRange(WriteOptions(), handles_[1], "a", "z").code(),
            Status::Code::kNotSupported);
  ASSERT_EQ(3, num_lookups);

  ASSERT_OK(db_->DropColumnFamily(handles_[1]));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), default_cf, "a", "z"));
  ASSERT_EQ(3, num_lookups);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

// Read-path range tombstone conversion inserts range tombstones into the
// memtable, so it is disabled for column families with in-place updates.
TEST_F(DBTestInPlaceUpdate, NoReadPathRangeTombstoneConversion) {
  Options options = InPlaceUpdateOptions();
  options.min_tombstones_for_range_conversion = 2;
  DestroyAndReopenWithRegularCF(options);

  for (int cf : {0, 1}) {
    ASSERT_OK(Put(cf, "a", "va"));
    for (const char* k : {"b1", "b2", "b3"}) {
      ASSERT_OK(Delete(cf, k));
    }
    ASSERT_OK(Put(cf, "c", "vc"));
    std::unique_ptr<Iterator> iter(
        db_->NewIterator(ReadOptions(), handles_[cf]));
    std::vector<std::string> keys;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      keys.push_back(iter->key().ToString());
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ((std::vector<std::string>{"a", "c"}), keys);
  }
  // Only the scan of the column family without in-place updates converted its
  // point tombstones.
  ASSERT_EQ(0, NumRangeDeletionsInMemtable(0));
  ASSERT_EQ(1, NumRangeDeletionsInMemtable(1));
  ASSERT_EQ(1,
            TestGetTickerCount(options, READ_PATH_RANGE_TOMBSTONES_INSERTED));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
