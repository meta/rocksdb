//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/udt_util.h"

#include <gtest/gtest.h>

#include "db/dbformat.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {
namespace {
static constexpr char kTestKeyWithoutTs[] = "key";
static constexpr char kValuePlaceHolder[] = "value";
static const uint64_t kWriteUnixTime = 100;
}  // namespace

class HandleTimestampSizeDifferenceTest : public testing::Test {
 public:
  HandleTimestampSizeDifferenceTest() = default;

  // Test handler used to collect the column family id and user keys contained
  // in a WriteBatch for test verification. And verifies the value part stays
  // the same if it's available.
  class KeyCollector : public WriteBatch::Handler {
   public:
    explicit KeyCollector() = default;

    ~KeyCollector() override = default;

    Status PutCF(uint32_t cf, const Slice& key, const Slice& value) override {
      if (value.compare(kValuePlaceHolder) != 0) {
        return Status::InvalidArgument();
      }
      return AddKey(cf, key);
    }

    Status TimedPutCF(uint32_t cf, const Slice& key, const Slice& value,
                      uint64_t write_unix_time) override {
      if (value.compare(kValuePlaceHolder) != 0) {
        return Status::InvalidArgument();
      }
      if (write_unix_time != kWriteUnixTime) {
        return Status::InvalidArgument();
      }
      return AddKey(cf, key);
    }

    Status PutEntityCF(uint32_t cf, const Slice& key,
                       const Slice& entity) override {
      Slice entity_copy = entity;
      WideColumns columns;
      Status s =
          WideColumnSerialization::DeserializeSimple(entity_copy, columns);
      if (!s.ok()) {
        return s;
      }
      if (columns.size() != 1) {
        return Status::InvalidArgument();
      }
      if (columns[0].value().compare(kValuePlaceHolder) != 0) {
        return Status::InvalidArgument();
      }
      return AddKey(cf, key);
    }

    Status DeleteCF(uint32_t cf, const Slice& key) override {
      return AddKey(cf, key);
    }

    Status SingleDeleteCF(uint32_t cf, const Slice& key) override {
      return AddKey(cf, key);
    }

    Status DeleteRangeCF(uint32_t cf, const Slice& begin_key,
                         const Slice& end_key) override {
      Status status = AddKey(cf, begin_key);
      if (!status.ok()) {
        return status;
      }
      return AddKey(cf, end_key);
    }

    Status MergeCF(uint32_t cf, const Slice& key, const Slice& value) override {
      if (value.compare(kValuePlaceHolder) != 0) {
        return Status::InvalidArgument();
      }
      return AddKey(cf, key);
    }

    Status PutBlobIndexCF(uint32_t cf, const Slice& key,
                          const Slice& value) override {
      if (value.compare(kValuePlaceHolder) != 0) {
        return Status::InvalidArgument();
      }
      return AddKey(cf, key);
    }

    Status MarkBeginPrepare(bool) override { return Status::OK(); }

    Status MarkEndPrepare(const Slice&) override { return Status::OK(); }

    Status MarkRollback(const Slice&) override { return Status::OK(); }

    Status MarkCommit(const Slice&) override { return Status::OK(); }

    Status MarkCommitWithTimestamp(const Slice&, const Slice&) override {
      return Status::OK();
    }

    Status MarkNoop(bool) override { return Status::OK(); }

    const std::vector<std::pair<uint32_t, const Slice>>& GetKeys() const {
      return keys_;
    }

   private:
    Status AddKey(uint32_t cf, const Slice& key) {
      keys_.emplace_back(cf, key);
      return Status::OK();
    }
    std::vector<std::pair<uint32_t, const Slice>> keys_;
  };

  void CreateKey(std::string* key_buf, size_t ts_sz) {
    if (ts_sz > 0) {
      AppendKeyWithMinTimestamp(key_buf, kTestKeyWithoutTs, ts_sz);
    } else {
      key_buf->assign(kTestKeyWithoutTs);
    }
  }

  void CreateWriteBatch(const UnorderedMap<uint32_t, size_t>& ts_sz_for_batch,
                        WriteBatch* batch) {
    for (const auto& [cf_id, ts_sz] : ts_sz_for_batch) {
      std::string key;
      CreateKey(&key, ts_sz);
      ASSERT_OK(WriteBatchInternal::Put(batch, cf_id, key, kValuePlaceHolder));
      ASSERT_OK(WriteBatchInternal::Delete(batch, cf_id, key));
      ASSERT_OK(WriteBatchInternal::SingleDelete(batch, cf_id, key));
      ASSERT_OK(WriteBatchInternal::DeleteRange(batch, cf_id, key, key));
      ASSERT_OK(
          WriteBatchInternal::Merge(batch, cf_id, key, kValuePlaceHolder));
      ASSERT_OK(WriteBatchInternal::PutBlobIndex(batch, cf_id, key,
                                                 kValuePlaceHolder));
      ASSERT_OK(WriteBatchInternal::TimedPut(
          batch, cf_id, key, kValuePlaceHolder, kWriteUnixTime));
      WideColumns columns{{kDefaultWideColumnName, kValuePlaceHolder}};
      ASSERT_OK(WriteBatchInternal::PutEntity(batch, cf_id, key, columns));
    }
  }

  void CheckSequenceEqual(const WriteBatch& orig_batch,
                          const WriteBatch& new_batch) {
    ASSERT_EQ(WriteBatchInternal::Sequence(&orig_batch),
              WriteBatchInternal::Sequence(&new_batch));
  }
  void CheckCountEqual(const WriteBatch& orig_batch,
                       const WriteBatch& new_batch) {
    ASSERT_EQ(WriteBatchInternal::Count(&orig_batch),
              WriteBatchInternal::Count(&new_batch));
  }

  void VerifyKeys(
      const std::vector<std::pair<uint32_t, const Slice>>& keys_with_ts,
      const std::vector<std::pair<uint32_t, const Slice>>& keys_without_ts,
      size_t ts_sz, std::optional<uint32_t> dropped_cf) {
    ASSERT_EQ(keys_with_ts.size(), keys_without_ts.size());
    const std::string kTsMin(ts_sz, static_cast<unsigned char>(0));
    for (size_t i = 0; i < keys_with_ts.size(); i++) {
      // TimestampRecoveryHandler ignores dropped column family and copy it over
      // as is. Check the keys stay the same.
      if (dropped_cf.has_value() &&
          keys_with_ts[i].first == dropped_cf.value()) {
        ASSERT_EQ(keys_with_ts[i].first, keys_without_ts[i].first);
        ASSERT_EQ(keys_with_ts[i].second, keys_without_ts[i].second);
        continue;
      }
      const Slice& key_with_ts = keys_with_ts[i].second;
      const Slice& key_without_ts = keys_without_ts[i].second;
      ASSERT_TRUE(key_with_ts.starts_with(key_without_ts));
      ASSERT_EQ(key_with_ts.size() - key_without_ts.size(), ts_sz);
      ASSERT_TRUE(key_with_ts.ends_with(kTsMin));
    }
  }

  void CheckContentsWithTimestampStripping(const WriteBatch& orig_batch,
                                           const WriteBatch& new_batch,
                                           size_t ts_sz,
                                           std::optional<uint32_t> dropped_cf) {
    CheckSequenceEqual(orig_batch, new_batch);
    CheckCountEqual(orig_batch, new_batch);
    KeyCollector collector_for_orig_batch;
    ASSERT_OK(orig_batch.Iterate(&collector_for_orig_batch));
    KeyCollector collector_for_new_batch;
    ASSERT_OK(new_batch.Iterate(&collector_for_new_batch));
    VerifyKeys(collector_for_orig_batch.GetKeys(),
               collector_for_new_batch.GetKeys(), ts_sz, dropped_cf);
  }

  void CheckContentsWithTimestampPadding(const WriteBatch& orig_batch,
                                         const WriteBatch& new_batch,
                                         size_t ts_sz) {
    CheckSequenceEqual(orig_batch, new_batch);
    CheckCountEqual(orig_batch, new_batch);
    KeyCollector collector_for_orig_batch;
    ASSERT_OK(orig_batch.Iterate(&collector_for_orig_batch));
    KeyCollector collector_for_new_batch;
    ASSERT_OK(new_batch.Iterate(&collector_for_new_batch));
    VerifyKeys(collector_for_new_batch.GetKeys(),
               collector_for_orig_batch.GetKeys(), ts_sz,
               std::nullopt /* dropped_cf */);
  }
};

TEST_F(HandleTimestampSizeDifferenceTest, AllColumnFamiliesConsistent) {
  UnorderedMap<uint32_t, size_t> running_ts_sz = {{1, sizeof(uint64_t)},
                                                  {2, 0}};
  UnorderedMap<uint32_t, size_t> record_ts_sz = {{1, sizeof(uint64_t)}};
  WriteBatch batch;
  CreateWriteBatch(running_ts_sz, &batch);

  // All `check_mode` pass with OK status and `batch` not checked or updated.
  ASSERT_OK(HandleWriteBatchTimestampSizeDifference(
      &batch, running_ts_sz, record_ts_sz,
      TimestampSizeConsistencyMode::kVerifyConsistency,
      /* seq_per_batch */ false, /* batch_per_txn */ true));
  std::unique_ptr<WriteBatch> new_batch(nullptr);
  ASSERT_OK(HandleWriteBatchTimestampSizeDifference(
      &batch, running_ts_sz, record_ts_sz,
      TimestampSizeConsistencyMode::kReconcileInconsistency,
      /* seq_per_batch */ false, /* batch_per_txn */ true, &new_batch));
  ASSERT_TRUE(new_batch.get() == nullptr);
}

TEST_F(HandleTimestampSizeDifferenceTest,
       AllInconsistentColumnFamiliesDropped) {
  UnorderedMap<uint32_t, size_t> running_ts_sz = {{2, 0}};
  UnorderedMap<uint32_t, size_t> record_ts_sz = {{1, sizeof(uint64_t)},
                                                 {3, sizeof(char)}};
  WriteBatch batch;
  CreateWriteBatch(record_ts_sz, &batch);

  // All `check_mode` pass with OK status and `batch` not checked or updated.
  ASSERT_OK(HandleWriteBatchTimestampSizeDifference(
      &batch, running_ts_sz, record_ts_sz,
      TimestampSizeConsistencyMode::kVerifyConsistency,
      /* seq_per_batch */ false, /* batch_per_txn */ true));
  std::unique_ptr<WriteBatch> new_batch(nullptr);
  ASSERT_OK(HandleWriteBatchTimestampSizeDifference(
      &batch, running_ts_sz, record_ts_sz,
      TimestampSizeConsistencyMode::kReconcileInconsistency,
      /* seq_per_batch */ false, /* batch_per_txn */ true, &new_batch));
  ASSERT_TRUE(new_batch.get() == nullptr);
}

TEST_F(HandleTimestampSizeDifferenceTest, InvolvedColumnFamiliesConsistent) {
  UnorderedMap<uint32_t, size_t> running_ts_sz = {{1, sizeof(uint64_t)},
                                                  {2, sizeof(char)}};
  UnorderedMap<uint32_t, size_t> record_ts_sz = {{1, sizeof(uint64_t)}};
  WriteBatch batch;
  CreateWriteBatch(record_ts_sz, &batch);

  // All `check_mode` pass with OK status and `batch` not updated.
  ASSERT_OK(HandleWriteBatchTimestampSizeDifference(
      &batch, running_ts_sz, record_ts_sz,
      TimestampSizeConsistencyMode::kVerifyConsistency,
      /* seq_per_batch */ false, /* batch_per_txn */ true));
  std::unique_ptr<WriteBatch> new_batch(nullptr);
  ASSERT_OK(HandleWriteBatchTimestampSizeDifference(
      &batch, running_ts_sz, record_ts_sz,
      TimestampSizeConsistencyMode::kReconcileInconsistency,
      /* seq_per_batch */ false, /* batch_per_txn */ true, &new_batch));
  ASSERT_TRUE(new_batch.get() == nullptr);
}

TEST_F(HandleTimestampSizeDifferenceTest,
       InconsistentColumnFamilyNeedsTimestampStripping) {
  UnorderedMap<uint32_t, size_t> running_ts_sz = {{1, 0}, {2, sizeof(char)}};
  UnorderedMap<uint32_t, size_t> record_ts_sz = {{1, sizeof(uint64_t)}};
  WriteBatch batch;
  CreateWriteBatch(record_ts_sz, &batch);

  // kVerifyConsistency doesn't tolerate inconsistency for running column
  // families.
  ASSERT_TRUE(HandleWriteBatchTimestampSizeDifference(
                  &batch, running_ts_sz, record_ts_sz,
                  TimestampSizeConsistencyMode::kVerifyConsistency,
                  /* seq_per_batch */ false, /* batch_per_txn */ true)
                  .IsInvalidArgument());

  std::unique_ptr<WriteBatch> new_batch(nullptr);
  ASSERT_OK(HandleWriteBatchTimestampSizeDifference(
      &batch, running_ts_sz, record_ts_sz,
      TimestampSizeConsistencyMode::kReconcileInconsistency,
      /* seq_per_batch */ false, /* batch_per_txn */ true, &new_batch));
  ASSERT_TRUE(new_batch.get() != nullptr);
  CheckContentsWithTimestampStripping(batch, *new_batch, sizeof(uint64_t),
                                      std::nullopt /* dropped_cf */);
}

TEST_F(HandleTimestampSizeDifferenceTest,
       InconsistentColumnFamilyNeedsTimestampPadding) {
  UnorderedMap<uint32_t, size_t> running_ts_sz = {{1, sizeof(uint64_t)}};
  // Make `record_ts_sz` not contain zero timestamp size entries to follow the
  // behavior of actual WAL log timestamp size record.
  UnorderedMap<uint32_t, size_t> record_ts_sz;
  WriteBatch batch;
  CreateWriteBatch({{1, 0}}, &batch);

  // kVerifyConsistency doesn't tolerate inconsistency for running column
  // families.
  ASSERT_TRUE(HandleWriteBatchTimestampSizeDifference(
                  &batch, running_ts_sz, record_ts_sz,
                  TimestampSizeConsistencyMode::kVerifyConsistency,
                  /* seq_per_batch */ false, /* batch_per_txn */ true)
                  .IsInvalidArgument());

  std::unique_ptr<WriteBatch> new_batch(nullptr);
  ASSERT_OK(HandleWriteBatchTimestampSizeDifference(
      &batch, running_ts_sz, record_ts_sz,
      TimestampSizeConsistencyMode::kReconcileInconsistency,
      /* seq_per_batch */ false, /* batch_per_txn */ true, &new_batch));
  ASSERT_TRUE(new_batch.get() != nullptr);
  CheckContentsWithTimestampPadding(batch, *new_batch, sizeof(uint64_t));
}

TEST_F(HandleTimestampSizeDifferenceTest,
       InconsistencyReconcileCopyOverDroppedColumnFamily) {
  UnorderedMap<uint32_t, size_t> running_ts_sz = {{1, 0}};
  UnorderedMap<uint32_t, size_t> record_ts_sz = {{1, sizeof(uint64_t)},
                                                 {2, sizeof(char)}};
  WriteBatch batch;
  CreateWriteBatch(record_ts_sz, &batch);
  std::unique_ptr<WriteBatch> new_batch(nullptr);

  // kReconcileInconsistency tolerate inconsistency for dropped column family
  // and all related entries copied over to the new WriteBatch.
  ASSERT_OK(HandleWriteBatchTimestampSizeDifference(
      &batch, running_ts_sz, record_ts_sz,
      TimestampSizeConsistencyMode::kReconcileInconsistency,
      /* seq_per_batch */ false, /* batch_per_txn */ true, &new_batch));

  ASSERT_TRUE(new_batch.get() != nullptr);
  CheckContentsWithTimestampStripping(batch, *new_batch, sizeof(uint64_t),
                                      std::optional<uint32_t>(2));
}

TEST_F(HandleTimestampSizeDifferenceTest, UnrecoverableInconsistency) {
  UnorderedMap<uint32_t, size_t> running_ts_sz = {{1, sizeof(char)}};
  UnorderedMap<uint32_t, size_t> record_ts_sz = {{1, sizeof(uint64_t)}};
  WriteBatch batch;
  CreateWriteBatch(record_ts_sz, &batch);

  ASSERT_TRUE(HandleWriteBatchTimestampSizeDifference(
                  &batch, running_ts_sz, record_ts_sz,
                  TimestampSizeConsistencyMode::kVerifyConsistency,
                  /* seq_per_batch */ false, /* batch_per_txn */ true)
                  .IsInvalidArgument());

  ASSERT_TRUE(HandleWriteBatchTimestampSizeDifference(
                  &batch, running_ts_sz, record_ts_sz,
                  TimestampSizeConsistencyMode::kReconcileInconsistency,
                  /* seq_per_batch */ false, /* batch_per_txn */ true)
                  .IsInvalidArgument());
}

TEST(ValidateUserDefinedTimestampsOptionsTest, EnableUserDefinedTimestamps) {
  bool mark_sst_files = false;
  const Comparator* new_comparator = test::BytewiseComparatorWithU64TsWrapper();
  const Comparator* old_comparator = BytewiseComparator();
  ASSERT_OK(ValidateUserDefinedTimestampsOptions(
      new_comparator, std::string(old_comparator->Name()),
      false /*new_persist_udt*/, true /*old_persist_udt*/, &mark_sst_files));
  ASSERT_TRUE(mark_sst_files);

  ASSERT_OK(ValidateUserDefinedTimestampsOptions(
      new_comparator, std::string(old_comparator->Name()),
      false /*new_persist_udt*/, false /*old_persist_udt*/, &mark_sst_files));
  ASSERT_TRUE(mark_sst_files);
}

TEST(ValidateUserDefinedTimestampsOptionsTest,
     EnableUserDefinedTimestampsNewPersistUDTFlagIncorrect) {
  bool mark_sst_files = false;
  const Comparator* new_comparator = test::BytewiseComparatorWithU64TsWrapper();
  const Comparator* old_comparator = BytewiseComparator();
  ASSERT_TRUE(ValidateUserDefinedTimestampsOptions(
                  new_comparator, std::string(old_comparator->Name()),
                  true /*new_persist_udt*/, true /*old_persist_udt*/,
                  &mark_sst_files)
                  .IsInvalidArgument());
  ASSERT_TRUE(ValidateUserDefinedTimestampsOptions(
                  new_comparator, std::string(old_comparator->Name()),
                  true /*new_persist_udt*/, false /*old_persist_udt*/,
                  &mark_sst_files)
                  .IsInvalidArgument());
}

TEST(ValidateUserDefinedTimestampsOptionsTest, DisableUserDefinedTimestamps) {
  bool mark_sst_files = false;
  const Comparator* new_comparator = ReverseBytewiseComparator();
  const Comparator* old_comparator =
      test::ReverseBytewiseComparatorWithU64TsWrapper();
  ASSERT_OK(ValidateUserDefinedTimestampsOptions(
      new_comparator, std::string(old_comparator->Name()),
      false /*new_persist_udt*/, false /*old_persist_udt*/, &mark_sst_files));
  ASSERT_FALSE(mark_sst_files);

  ASSERT_OK(ValidateUserDefinedTimestampsOptions(
      new_comparator, std::string(old_comparator->Name()),
      true /*new_persist_udt*/, false /*old_persist_udt*/, &mark_sst_files));
  ASSERT_FALSE(mark_sst_files);
}

TEST(ValidateUserDefinedTimestampsOptionsTest,
     DisableUserDefinedTimestampsOldPersistUDTFlagIncorrect) {
  bool mark_sst_files = false;
  const Comparator* new_comparator = BytewiseComparator();
  const Comparator* old_comparator = test::BytewiseComparatorWithU64TsWrapper();
  ASSERT_TRUE(ValidateUserDefinedTimestampsOptions(
                  new_comparator, std::string(old_comparator->Name()),
                  false /*new_persist_udt*/, true /*old_persist_udt*/,
                  &mark_sst_files)
                  .IsInvalidArgument());
  ASSERT_TRUE(ValidateUserDefinedTimestampsOptions(
                  new_comparator, std::string(old_comparator->Name()),
                  true /*new_persist_udt*/, true /*old_persist_udt*/,
                  &mark_sst_files)
                  .IsInvalidArgument());
}

TEST(ValidateUserDefinedTimestampsOptionsTest, UserComparatorUnchanged) {
  bool mark_sst_files = false;
  const Comparator* ucmp_without_ts = BytewiseComparator();
  const Comparator* ucmp_with_ts = test::BytewiseComparatorWithU64TsWrapper();
  ASSERT_OK(ValidateUserDefinedTimestampsOptions(
      ucmp_without_ts, std::string(ucmp_without_ts->Name()),
      false /*new_persist_udt*/, false /*old_persist_udt*/, &mark_sst_files));
  ASSERT_FALSE(mark_sst_files);
  ASSERT_OK(ValidateUserDefinedTimestampsOptions(
      ucmp_without_ts, std::string(ucmp_without_ts->Name()),
      true /*new_persist_udt*/, true /*old_persist_udt*/, &mark_sst_files));
  ASSERT_FALSE(mark_sst_files);
  ASSERT_OK(ValidateUserDefinedTimestampsOptions(
      ucmp_without_ts, std::string(ucmp_without_ts->Name()),
      true /*new_persist_udt*/, false /*old_persist_udt*/, &mark_sst_files));
  ASSERT_FALSE(mark_sst_files);
  ASSERT_OK(ValidateUserDefinedTimestampsOptions(
      ucmp_without_ts, std::string(ucmp_without_ts->Name()),
      false /*new_persist_udt*/, true /*old_persist_udt*/, &mark_sst_files));
  ASSERT_FALSE(mark_sst_files);

  ASSERT_OK(ValidateUserDefinedTimestampsOptions(
      ucmp_with_ts, std::string(ucmp_with_ts->Name()), true /*new_persist_udt*/,
      true /*old_persist_udt*/, &mark_sst_files));
  ASSERT_FALSE(mark_sst_files);
  ASSERT_OK(ValidateUserDefinedTimestampsOptions(
      ucmp_with_ts, std::string(ucmp_with_ts->Name()),
      false /*new_persist_udt*/, false /*old_persist_udt*/, &mark_sst_files));
  ASSERT_FALSE(mark_sst_files);
  ASSERT_TRUE(ValidateUserDefinedTimestampsOptions(
                  ucmp_with_ts, std::string(ucmp_with_ts->Name()),
                  true /*new_persist_udt*/, false /*old_persist_udt*/,
                  &mark_sst_files)
                  .IsInvalidArgument());
  ASSERT_TRUE(ValidateUserDefinedTimestampsOptions(
                  ucmp_with_ts, std::string(ucmp_with_ts->Name()),
                  false /*new_persist_udt*/, true /*old_persist_udt*/,
                  &mark_sst_files)
                  .IsInvalidArgument());
}

TEST(ValidateUserDefinedTimestampsOptionsTest, InvalidUserComparatorChange) {
  bool mark_sst_files = false;
  const Comparator* new_comparator = BytewiseComparator();
  const Comparator* old_comparator = ReverseBytewiseComparator();
  ASSERT_TRUE(ValidateUserDefinedTimestampsOptions(
                  new_comparator, std::string(old_comparator->Name()),
                  false /*new_persist_udt*/, true /*old_persist_udt*/,
                  &mark_sst_files)
                  .IsInvalidArgument());
}

TEST(GetFullHistoryTsLowFromU64CutoffTsTest, Success) {
  std::string cutoff_ts;
  uint64_t cutoff_ts_int = 3;
  PutFixed64(&cutoff_ts, 3);
  Slice cutoff_ts_slice = cutoff_ts;
  std::string actual_full_history_ts_low;
  GetFullHistoryTsLowFromU64CutoffTs(&cutoff_ts_slice,
                                     &actual_full_history_ts_low);

  std::string expected_ts_low;
  PutFixed64(&expected_ts_low, cutoff_ts_int + 1);
  ASSERT_EQ(expected_ts_low, actual_full_history_ts_low);
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
