//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_test_util.h"
#include "db/wide/wide_column_test_util.h"
#include "rocksdb/attribute_groups.h"

namespace ROCKSDB_NAMESPACE {

class CoalescingIteratorTest : public DBTestBase {
 public:
  CoalescingIteratorTest()
      : DBTestBase("coalescing_iterator_test", /*env_do_fsync=*/true) {}

  // Verify Iteration of CoalescingIterator
  // by SeekToFirst() + Next() and SeekToLast() + Prev()
  void VerifyCoalescingIterator(const std::vector<ColumnFamilyHandle*>& cfhs,
                                const std::vector<Slice>& expected_keys,
                                const std::vector<Slice>& expected_values,
                                const std::optional<std::vector<WideColumns>>&
                                    expected_wide_columns = std::nullopt,
                                const Slice* lower_bound = nullptr,
                                const Slice* upper_bound = nullptr,
                                bool allow_unprepared_value = false) {
    const size_t num_keys = expected_keys.size();

    ReadOptions read_options;
    read_options.iterate_lower_bound = lower_bound;
    read_options.iterate_upper_bound = upper_bound;
    read_options.allow_unprepared_value = allow_unprepared_value;

    std::unique_ptr<Iterator> iter =
        db_->NewCoalescingIterator(read_options, cfhs);

    auto check_iter_entry = [&](size_t idx) {
      ASSERT_EQ(iter->key(), expected_keys[idx]);

      if (allow_unprepared_value) {
        ASSERT_TRUE(iter->value().empty());
        ASSERT_TRUE(iter->PrepareValue());
        ASSERT_TRUE(iter->Valid());
      }

      ASSERT_EQ(iter->value(), expected_values[idx]);
      if (expected_wide_columns.has_value()) {
        ASSERT_EQ(iter->columns(), expected_wide_columns.value()[idx]);
      }
    };

    {
      size_t i = 0;
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        check_iter_entry(i);
        ++i;
      }

      ASSERT_EQ(num_keys, i);
      ASSERT_OK(iter->status());
    }

    {
      size_t i = 0;
      for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
        check_iter_entry(num_keys - 1 - i);
        ++i;
      }

      ASSERT_EQ(num_keys, i);
      ASSERT_OK(iter->status());
    }
  }

  void VerifyExpectedKeys(ColumnFamilyHandle* cfh,
                          const std::vector<Slice>& expected_keys) {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions(), cfh));

    size_t i = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_EQ(iter->key(), expected_keys[i]);
      ++i;
    }

    ASSERT_EQ(i, expected_keys.size());
    ASSERT_OK(iter->status());
  }
};

TEST_F(CoalescingIteratorTest, InvalidArguments) {
  Options options = GetDefaultOptions();
  {
    CreateAndReopenWithCF({"cf_1", "cf_2", "cf_3"}, options);

    // Invalid - No CF is provided
    std::unique_ptr<Iterator> iter_with_no_cf =
        db_->NewCoalescingIterator(ReadOptions(), {});
    ASSERT_NOK(iter_with_no_cf->status());
    ASSERT_TRUE(iter_with_no_cf->status().IsInvalidArgument());
  }
}

TEST_F(CoalescingIteratorTest, SimpleValues) {
  Options options = GetDefaultOptions();
  {
    // Case 1: Unique key per CF
    CreateAndReopenWithCF({"cf_1", "cf_2", "cf_3"}, options);

    ASSERT_OK(Put(0, "key_1", "key_1_cf_0_val"));
    ASSERT_OK(Put(1, "key_2", "key_2_cf_1_val"));
    ASSERT_OK(Put(2, "key_3", "key_3_cf_2_val"));
    ASSERT_OK(Put(3, "key_4", "key_4_cf_3_val"));

    std::vector<Slice> expected_keys = {"key_1", "key_2", "key_3", "key_4"};
    std::vector<Slice> expected_values = {"key_1_cf_0_val", "key_2_cf_1_val",
                                          "key_3_cf_2_val", "key_4_cf_3_val"};

    // Test for iteration over CF default->1->2->3
    std::vector<ColumnFamilyHandle*> cfhs_order_0_1_2_3 = {
        handles_[0], handles_[1], handles_[2], handles_[3]};
    VerifyCoalescingIterator(cfhs_order_0_1_2_3, expected_keys,
                             expected_values);

    // Test for iteration over CF 3->1->default_cf->2
    std::vector<ColumnFamilyHandle*> cfhs_order_3_1_0_2 = {
        handles_[3], handles_[1], handles_[0], handles_[2]};
    // Iteration order and the return values should be the same since keys are
    // unique per CF
    VerifyCoalescingIterator(cfhs_order_3_1_0_2, expected_keys,
                             expected_values);

    // Verify Seek()
    {
      std::unique_ptr<Iterator> iter =
          db_->NewCoalescingIterator(ReadOptions(), cfhs_order_0_1_2_3);
      iter->Seek("");
      ASSERT_EQ(IterStatus(iter.get()), "key_1->key_1_cf_0_val");
      iter->Seek("key_1");
      ASSERT_EQ(IterStatus(iter.get()), "key_1->key_1_cf_0_val");
      iter->Seek("key_2");
      ASSERT_EQ(IterStatus(iter.get()), "key_2->key_2_cf_1_val");
      iter->Next();
      ASSERT_EQ(IterStatus(iter.get()), "key_3->key_3_cf_2_val");
      iter->Seek("key_x");
      ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
    }
    // Verify SeekForPrev()
    {
      std::unique_ptr<Iterator> iter =
          db_->NewCoalescingIterator(ReadOptions(), cfhs_order_0_1_2_3);
      iter->SeekForPrev("");
      ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
      iter->SeekForPrev("key_1");
      ASSERT_EQ(IterStatus(iter.get()), "key_1->key_1_cf_0_val");
      iter->Next();
      ASSERT_EQ(IterStatus(iter.get()), "key_2->key_2_cf_1_val");
      iter->SeekForPrev("key_x");
      ASSERT_EQ(IterStatus(iter.get()), "key_4->key_4_cf_3_val");
      iter->Prev();
      ASSERT_EQ(IterStatus(iter.get()), "key_3->key_3_cf_2_val");
      iter->Next();
      ASSERT_EQ(IterStatus(iter.get()), "key_4->key_4_cf_3_val");
      iter->Next();
      ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
    }
  }
  {
    // Case 2: Same key in multiple CFs
    options = CurrentOptions(options);
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"cf_1", "cf_2", "cf_3"}, options);

    ASSERT_OK(Put(0, "key_1", "key_1_cf_0_val"));
    ASSERT_OK(Put(3, "key_1", "key_1_cf_3_val"));
    ASSERT_OK(Put(1, "key_2", "key_2_cf_1_val"));
    ASSERT_OK(Put(2, "key_2", "key_2_cf_2_val"));
    ASSERT_OK(Put(0, "key_3", "key_3_cf_0_val"));
    ASSERT_OK(Put(1, "key_3", "key_3_cf_1_val"));
    ASSERT_OK(Put(3, "key_3", "key_3_cf_3_val"));

    std::vector<Slice> expected_keys = {"key_1", "key_2", "key_3"};

    // Test for iteration over CFs default->1->2->3
    std::vector<ColumnFamilyHandle*> cfhs_order_0_1_2_3 = {
        handles_[0], handles_[1], handles_[2], handles_[3]};
    std::vector<Slice> expected_values = {"key_1_cf_3_val", "key_2_cf_2_val",
                                          "key_3_cf_3_val"};
    VerifyCoalescingIterator(cfhs_order_0_1_2_3, expected_keys,
                             expected_values);

    // Test for iteration over CFs 3->2->default_cf->1
    std::vector<ColumnFamilyHandle*> cfhs_order_3_2_0_1 = {
        handles_[3], handles_[2], handles_[0], handles_[1]};
    expected_values = {"key_1_cf_0_val", "key_2_cf_1_val", "key_3_cf_1_val"};
    VerifyCoalescingIterator(cfhs_order_3_2_0_1, expected_keys,
                             expected_values);

    // Verify Seek()
    {
      std::unique_ptr<Iterator> iter =
          db_->NewCoalescingIterator(ReadOptions(), cfhs_order_3_2_0_1);
      iter->Seek("");
      ASSERT_EQ(IterStatus(iter.get()), "key_1->key_1_cf_0_val");
      iter->Seek("key_1");
      ASSERT_EQ(IterStatus(iter.get()), "key_1->key_1_cf_0_val");
      iter->Seek("key_2");
      ASSERT_EQ(IterStatus(iter.get()), "key_2->key_2_cf_1_val");
      iter->Next();
      ASSERT_EQ(IterStatus(iter.get()), "key_3->key_3_cf_1_val");
      iter->Seek("key_x");
      ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
    }
    // Verify SeekForPrev()
    {
      std::unique_ptr<Iterator> iter =
          db_->NewCoalescingIterator(ReadOptions(), cfhs_order_3_2_0_1);
      iter->SeekForPrev("");
      ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
      iter->SeekForPrev("key_1");
      ASSERT_EQ(IterStatus(iter.get()), "key_1->key_1_cf_0_val");
      iter->Next();
      ASSERT_EQ(IterStatus(iter.get()), "key_2->key_2_cf_1_val");
      iter->SeekForPrev("key_x");
      ASSERT_EQ(IterStatus(iter.get()), "key_3->key_3_cf_1_val");
      iter->Next();
      ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
    }
  }
}

TEST_F(CoalescingIteratorTest, LowerAndUpperBounds) {
  Options options = GetDefaultOptions();
  {
    // Case 1: Unique key per CF
    CreateAndReopenWithCF({"cf_1", "cf_2", "cf_3"}, options);

    ASSERT_OK(Put(0, "key_1", "key_1_cf_0_val"));
    ASSERT_OK(Put(1, "key_2", "key_2_cf_1_val"));
    ASSERT_OK(Put(2, "key_3", "key_3_cf_2_val"));
    ASSERT_OK(Put(3, "key_4", "key_4_cf_3_val"));

    std::vector<ColumnFamilyHandle*> cfhs_order_0_1_2_3 = {
        handles_[0], handles_[1], handles_[2], handles_[3]};

    // with lower_bound
    {
      // lower_bound is inclusive
      Slice lb = Slice("key_2");
      std::vector<Slice> expected_keys = {"key_2", "key_3", "key_4"};
      std::vector<Slice> expected_values = {"key_2_cf_1_val", "key_3_cf_2_val",
                                            "key_4_cf_3_val"};
      VerifyCoalescingIterator(cfhs_order_0_1_2_3, expected_keys,
                               expected_values, std::nullopt, &lb);
    }
    // with upper_bound
    {
      // upper_bound is exclusive
      Slice ub = Slice("key_3");
      std::vector<Slice> expected_keys = {"key_1", "key_2"};
      std::vector<Slice> expected_values = {"key_1_cf_0_val", "key_2_cf_1_val"};
      VerifyCoalescingIterator(cfhs_order_0_1_2_3, expected_keys,
                               expected_values, std::nullopt, nullptr, &ub);
    }
    // with lower and upper bound
    {
      Slice lb = Slice("key_2");
      Slice ub = Slice("key_4");
      std::vector<Slice> expected_keys = {"key_2", "key_3"};
      std::vector<Slice> expected_values = {"key_2_cf_1_val", "key_3_cf_2_val"};
      VerifyCoalescingIterator(cfhs_order_0_1_2_3, expected_keys,
                               expected_values, std::nullopt, &lb, &ub);
    }

    {
      Slice lb = Slice("key_2");
      Slice ub = Slice("key_4");
      ReadOptions read_options;
      read_options.iterate_lower_bound = &lb;
      read_options.iterate_upper_bound = &ub;
      // Verify Seek() with bounds
      {
        std::unique_ptr<Iterator> iter =
            db_->NewCoalescingIterator(read_options, cfhs_order_0_1_2_3);
        iter->Seek("");
        ASSERT_EQ(IterStatus(iter.get()), "key_2->key_2_cf_1_val");
        iter->Next();
        ASSERT_EQ(IterStatus(iter.get()), "key_3->key_3_cf_2_val");
        iter->Seek("key_x");
        ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
      }
      // Verify SeekForPrev() with bounds
      {
        std::unique_ptr<Iterator> iter =
            db_->NewCoalescingIterator(read_options, cfhs_order_0_1_2_3);
        iter->SeekForPrev("");
        ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
        iter->SeekForPrev("key_1");
        ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
        iter->SeekForPrev("key_2");
        ASSERT_EQ(IterStatus(iter.get()), "key_2->key_2_cf_1_val");
        iter->Next();
        ASSERT_EQ(IterStatus(iter.get()), "key_3->key_3_cf_2_val");
        iter->SeekForPrev("key_x");
        ASSERT_EQ(IterStatus(iter.get()), "key_3->key_3_cf_2_val");
        iter->Prev();
        ASSERT_EQ(IterStatus(iter.get()), "key_2->key_2_cf_1_val");
        iter->Next();
        ASSERT_EQ(IterStatus(iter.get()), "key_3->key_3_cf_2_val");
        iter->Next();
        ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
      }
    }
  }
  {
    // Case 2: Same key in multiple CFs
    options = CurrentOptions(options);
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"cf_1", "cf_2", "cf_3"}, options);

    ASSERT_OK(Put(0, "key_1", "key_1_cf_0_val"));
    ASSERT_OK(Put(3, "key_1", "key_1_cf_3_val"));
    ASSERT_OK(Put(1, "key_2", "key_2_cf_1_val"));
    ASSERT_OK(Put(2, "key_2", "key_2_cf_2_val"));
    ASSERT_OK(Put(0, "key_3", "key_3_cf_0_val"));
    ASSERT_OK(Put(1, "key_3", "key_3_cf_1_val"));
    ASSERT_OK(Put(3, "key_3", "key_3_cf_3_val"));

    // Test for iteration over CFs default->1->2->3
    std::vector<ColumnFamilyHandle*> cfhs_order_0_1_2_3 = {
        handles_[0], handles_[1], handles_[2], handles_[3]};
    // with lower_bound
    {
      // lower_bound is inclusive
      Slice lb = Slice("key_2");
      std::vector<Slice> expected_keys = {"key_2", "key_3"};
      std::vector<Slice> expected_values = {"key_2_cf_2_val", "key_3_cf_3_val"};
      VerifyCoalescingIterator(cfhs_order_0_1_2_3, expected_keys,
                               expected_values, std::nullopt, &lb);
    }
    // with upper_bound
    {
      // upper_bound is exclusive
      Slice ub = Slice("key_3");
      std::vector<Slice> expected_keys = {"key_1", "key_2"};
      std::vector<Slice> expected_values = {"key_1_cf_3_val", "key_2_cf_2_val"};
      VerifyCoalescingIterator(cfhs_order_0_1_2_3, expected_keys,
                               expected_values, std::nullopt, nullptr, &ub);
    }
    // with lower and upper bound
    {
      Slice lb = Slice("key_2");
      Slice ub = Slice("key_3");
      std::vector<Slice> expected_keys = {"key_2"};
      std::vector<Slice> expected_values = {"key_2_cf_2_val"};
      VerifyCoalescingIterator(cfhs_order_0_1_2_3, expected_keys,
                               expected_values, std::nullopt, &lb, &ub);
    }

    // Test for iteration over CFs 3->2->default_cf->1
    std::vector<ColumnFamilyHandle*> cfhs_order_3_2_0_1 = {
        handles_[3], handles_[2], handles_[0], handles_[1]};
    {
      // lower_bound is inclusive
      Slice lb = Slice("key_2");
      std::vector<Slice> expected_keys = {"key_2", "key_3"};
      std::vector<Slice> expected_values = {"key_2_cf_1_val", "key_3_cf_1_val"};
      VerifyCoalescingIterator(cfhs_order_3_2_0_1, expected_keys,
                               expected_values, std::nullopt, &lb);
    }
    // with upper_bound
    {
      // upper_bound is exclusive
      Slice ub = Slice("key_3");
      std::vector<Slice> expected_keys = {"key_1", "key_2"};
      std::vector<Slice> expected_values = {"key_1_cf_0_val", "key_2_cf_1_val"};
      VerifyCoalescingIterator(cfhs_order_3_2_0_1, expected_keys,
                               expected_values, std::nullopt, nullptr, &ub);
    }
    // with lower and upper bound
    {
      Slice lb = Slice("key_2");
      Slice ub = Slice("key_3");
      std::vector<Slice> expected_keys = {"key_2"};
      std::vector<Slice> expected_values = {"key_2_cf_1_val"};
      VerifyCoalescingIterator(cfhs_order_3_2_0_1, expected_keys,
                               expected_values, std::nullopt, &lb, &ub);
    }
    {
      Slice lb = Slice("key_2");
      Slice ub = Slice("key_3");
      ReadOptions read_options;
      read_options.iterate_lower_bound = &lb;
      read_options.iterate_upper_bound = &ub;
      // Verify Seek() with bounds
      {
        std::unique_ptr<Iterator> iter =
            db_->NewCoalescingIterator(read_options, cfhs_order_3_2_0_1);
        iter->Seek("");
        ASSERT_EQ(IterStatus(iter.get()), "key_2->key_2_cf_1_val");
        iter->Next();
        ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
        iter->Seek("key_x");
        ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
      }
      // Verify SeekForPrev() with bounds
      {
        std::unique_ptr<Iterator> iter =
            db_->NewCoalescingIterator(read_options, cfhs_order_3_2_0_1);
        iter->SeekForPrev("");
        ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
        iter->SeekForPrev("key_1");
        ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
        iter->SeekForPrev("key_2");
        ASSERT_EQ(IterStatus(iter.get()), "key_2->key_2_cf_1_val");
        iter->SeekForPrev("key_x");
        ASSERT_EQ(IterStatus(iter.get()), "key_2->key_2_cf_1_val");
        iter->Next();
        ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
      }
    }
  }
}

TEST_F(CoalescingIteratorTest, ConsistentViewExplicitSnapshot) {
  Options options = GetDefaultOptions();
  options.atomic_flush = true;
  CreateAndReopenWithCF({"cf_1", "cf_2", "cf_3"}, options);

  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                  "cf" + std::to_string(i) + "_val"));
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BGWorkFlush:done",
        "DBImpl::MultiCFSnapshot::BeforeCheckingSnapshot"}});

  bool flushed = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiCFSnapshot::AfterRefSV", [&](void* /*arg*/) {
        if (!flushed) {
          for (int i = 0; i < 4; ++i) {
            ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                          "cf" + std::to_string(i) + "_val_new"));
          }
          ASSERT_OK(Flush());
          flushed = true;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  std::vector<ColumnFamilyHandle*> cfhs_order_0_1_2_3 = {
      handles_[0], handles_[1], handles_[2], handles_[3]};
  ReadOptions read_options;
  const Snapshot* snapshot = db_->GetSnapshot();
  read_options.snapshot = snapshot;
  // Verify Seek()
  {
    std::unique_ptr<Iterator> iter =
        db_->NewCoalescingIterator(read_options, cfhs_order_0_1_2_3);
    iter->Seek("");
    ASSERT_EQ(IterStatus(iter.get()), "cf0_key->cf0_val");
    iter->Next();
    ASSERT_EQ(IterStatus(iter.get()), "cf1_key->cf1_val");
  }
  // Verify SeekForPrev()
  {
    std::unique_ptr<Iterator> iter =
        db_->NewCoalescingIterator(read_options, cfhs_order_0_1_2_3);
    iter->SeekForPrev("");
    ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
    iter->SeekForPrev("cf2_key");
    ASSERT_EQ(IterStatus(iter.get()), "cf2_key->cf2_val");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter.get()), "cf1_key->cf1_val");
  }
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(CoalescingIteratorTest, ConsistentViewImplicitSnapshot) {
  Options options = GetDefaultOptions();
  CreateAndReopenWithCF({"cf_1", "cf_2", "cf_3"}, options);

  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                  "cf" + std::to_string(i) + "_val"));
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BGWorkFlush:done",
        "DBImpl::MultiCFSnapshot::BeforeCheckingSnapshot"}});

  bool flushed = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiCFSnapshot::AfterRefSV", [&](void* /*arg*/) {
        if (!flushed) {
          for (int i = 0; i < 4; ++i) {
            ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                          "cf" + std::to_string(i) + "_val_new"));
          }
          ASSERT_OK(Flush(1));
          flushed = true;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  std::vector<ColumnFamilyHandle*> cfhs_order_0_1_2_3 = {
      handles_[0], handles_[1], handles_[2], handles_[3]};
  // Verify Seek()
  {
    std::unique_ptr<Iterator> iter =
        db_->NewCoalescingIterator(ReadOptions(), cfhs_order_0_1_2_3);
    iter->Seek("cf2_key");
    ASSERT_EQ(IterStatus(iter.get()), "cf2_key->cf2_val_new");
    iter->Next();
    ASSERT_EQ(IterStatus(iter.get()), "cf3_key->cf3_val_new");
  }
  // Verify SeekForPrev()
  {
    std::unique_ptr<Iterator> iter =
        db_->NewCoalescingIterator(ReadOptions(), cfhs_order_0_1_2_3);
    iter->SeekForPrev("");
    ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
    iter->SeekForPrev("cf1_key");
    ASSERT_EQ(IterStatus(iter.get()), "cf1_key->cf1_val_new");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter.get()), "cf0_key->cf0_val_new");
  }
}

TEST_F(CoalescingIteratorTest, EmptyCfs) {
  Options options = GetDefaultOptions();
  {
    // Case 1: No keys in any of the CFs
    CreateAndReopenWithCF({"cf_1", "cf_2", "cf_3"}, options);
    std::unique_ptr<Iterator> iter =
        db_->NewCoalescingIterator(ReadOptions(), handles_);
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
    iter->Seek("foo");
    ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
    iter->SeekForPrev("foo");
    ASSERT_EQ(IterStatus(iter.get()), "(invalid)");

    ASSERT_OK(iter->status());
  }
  {
    // Case 2: A single key exists in only one of the CF. Rest CFs are empty.
    ASSERT_OK(Put(1, "key_1", "key_1_cf_1_val"));
    std::unique_ptr<Iterator> iter =
        db_->NewCoalescingIterator(ReadOptions(), handles_);
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter.get()), "key_1->key_1_cf_1_val");
    iter->Next();
    ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter.get()), "key_1->key_1_cf_1_val");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
  }
  {
    // Case 3: same key exists in all of the CFs except one (cf_2)
    ASSERT_OK(Put(0, "key_1", "key_1_cf_0_val"));
    ASSERT_OK(Put(3, "key_1", "key_1_cf_3_val"));
    // handles_ are in the order of 0->1->2->3
    std::unique_ptr<Iterator> iter =
        db_->NewCoalescingIterator(ReadOptions(), handles_);
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter.get()), "key_1->key_1_cf_3_val");
    iter->Next();
    ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter.get()), "key_1->key_1_cf_3_val");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
  }
}

TEST_F(CoalescingIteratorTest, SeekClearsErrorStatus) {
  // Under prefix_seek_opt_in_only, SeekToFirst() and SeekToLast() of a
  // prefix_same_as_start iterator fail with NotSupported. A later seek must
  // clear that error, as for any Iterator.
  Options options = GetDefaultOptions();
  options.prefix_extractor.reset(NewFixedPrefixTransform(4));
  options.prefix_seek_opt_in_only = true;
  CreateAndReopenWithCF({"cf_1"}, options);
  ASSERT_OK(Put(0, "kkkk1", "cf_0_val"));
  ASSERT_OK(Put(1, "kkkk2", "cf_1_val"));

  ReadOptions read_options;
  read_options.prefix_same_as_start = true;
  std::unique_ptr<Iterator> iter =
      db_->NewCoalescingIterator(read_options, handles_);
  iter->SeekToFirst();
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().IsNotSupported());
  iter->Seek("kkkk");
  ASSERT_EQ(IterStatus(iter.get()), "kkkk1->cf_0_val");
  iter->SeekToLast();
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().IsNotSupported());
  iter->SeekForPrev("kkkk9");
  ASSERT_EQ(IterStatus(iter.get()), "kkkk2->cf_1_val");
}

TEST_F(CoalescingIteratorTest, WideColumns) {
  // Set up the DB and Column Families
  Options options = GetDefaultOptions();
  CreateAndReopenWithCF({"cf_1", "cf_2", "cf_3"}, options);

  constexpr char key_1[] = "key_1";
  WideColumns key_1_columns_in_cf_2{
      {kDefaultWideColumnName, "cf_2_col_val_0_key_1"},
      {"cf_2_col_name_1", "cf_2_col_val_1_key_1"},
      {"cf_2_col_name_2", "cf_2_col_val_2_key_1"},
      {"cf_overlap_col_name", "cf_2_overlap_value_key_1"}};
  WideColumns key_1_columns_in_cf_3{
      {"cf_3_col_name_1", "cf_3_col_val_1_key_1"},
      {"cf_3_col_name_2", "cf_3_col_val_2_key_1"},
      {"cf_3_col_name_3", "cf_3_col_val_3_key_1"},
      {"cf_overlap_col_name", "cf_3_overlap_value_key_1"}};
  WideColumns key_1_expected_columns_cfh_order_2_3{
      {kDefaultWideColumnName, "cf_2_col_val_0_key_1"},
      {"cf_2_col_name_1", "cf_2_col_val_1_key_1"},
      {"cf_2_col_name_2", "cf_2_col_val_2_key_1"},
      {"cf_3_col_name_1", "cf_3_col_val_1_key_1"},
      {"cf_3_col_name_2", "cf_3_col_val_2_key_1"},
      {"cf_3_col_name_3", "cf_3_col_val_3_key_1"},
      {"cf_overlap_col_name", "cf_3_overlap_value_key_1"}};
  WideColumns key_1_expected_columns_cfh_order_3_2{
      {kDefaultWideColumnName, "cf_2_col_val_0_key_1"},
      {"cf_2_col_name_1", "cf_2_col_val_1_key_1"},
      {"cf_2_col_name_2", "cf_2_col_val_2_key_1"},
      {"cf_3_col_name_1", "cf_3_col_val_1_key_1"},
      {"cf_3_col_name_2", "cf_3_col_val_2_key_1"},
      {"cf_3_col_name_3", "cf_3_col_val_3_key_1"},
      {"cf_overlap_col_name", "cf_2_overlap_value_key_1"}};

  constexpr char key_2[] = "key_2";
  WideColumns key_2_columns_in_cf_1{
      {"cf_overlap_col_name", "cf_1_overlap_value_key_2"}};
  WideColumns key_2_columns_in_cf_2{
      {"cf_2_col_name_1", "cf_2_col_val_1_key_2"},
      {"cf_2_col_name_2", "cf_2_col_val_2_key_2"},
      {"cf_overlap_col_name", "cf_2_overlap_value_key_2"}};
  WideColumns key_2_expected_columns_cfh_order_1_2{
      {"cf_2_col_name_1", "cf_2_col_val_1_key_2"},
      {"cf_2_col_name_2", "cf_2_col_val_2_key_2"},
      {"cf_overlap_col_name", "cf_2_overlap_value_key_2"}};
  WideColumns key_2_expected_columns_cfh_order_2_1{
      {"cf_2_col_name_1", "cf_2_col_val_1_key_2"},
      {"cf_2_col_name_2", "cf_2_col_val_2_key_2"},
      {"cf_overlap_col_name", "cf_1_overlap_value_key_2"}};

  constexpr char key_3[] = "key_3";
  WideColumns key_3_columns_in_cf_1{
      {"cf_1_col_name_1", "cf_1_col_val_1_key_3"}};
  WideColumns key_3_columns_in_cf_3{
      {"cf_3_col_name_1", "cf_3_col_val_1_key_3"}};
  WideColumns key_3_expected_columns{
      {"cf_1_col_name_1", "cf_1_col_val_1_key_3"},
      {"cf_3_col_name_1", "cf_3_col_val_1_key_3"},
  };

  constexpr char key_4[] = "key_4";
  WideColumns key_4_columns_in_cf_0{
      {"cf_0_col_name_1", "cf_0_col_val_1_key_4"}};
  WideColumns key_4_columns_in_cf_2{
      {"cf_2_col_name_1", "cf_2_col_val_1_key_4"}};
  WideColumns key_4_expected_columns{
      {"cf_0_col_name_1", "cf_0_col_val_1_key_4"},
      {"cf_2_col_name_1", "cf_2_col_val_1_key_4"},
  };

  // Use AttributeGroup PutEntity API to insert them together
  AttributeGroups key_1_attribute_groups{
      AttributeGroup(handles_[2], key_1_columns_in_cf_2),
      AttributeGroup(handles_[3], key_1_columns_in_cf_3)};
  AttributeGroups key_2_attribute_groups{
      AttributeGroup(handles_[1], key_2_columns_in_cf_1),
      AttributeGroup(handles_[2], key_2_columns_in_cf_2)};
  AttributeGroups key_3_attribute_groups{
      AttributeGroup(handles_[1], key_3_columns_in_cf_1),
      AttributeGroup(handles_[3], key_3_columns_in_cf_3)};
  AttributeGroups key_4_attribute_groups{
      AttributeGroup(handles_[0], key_4_columns_in_cf_0),
      AttributeGroup(handles_[2], key_4_columns_in_cf_2)};

  ASSERT_OK(db_->PutEntity(WriteOptions(), key_1, key_1_attribute_groups));
  ASSERT_OK(db_->PutEntity(WriteOptions(), key_2, key_2_attribute_groups));
  ASSERT_OK(db_->PutEntity(WriteOptions(), key_3, key_3_attribute_groups));
  ASSERT_OK(db_->PutEntity(WriteOptions(), key_4, key_4_attribute_groups));

  // Keys should be returned in order regardless of cfh order
  std::vector<Slice> expected_keys = {key_1, key_2, key_3, key_4};

  // Since value for kDefaultWideColumnName only exists for key_1, rest will
  // return empty value after coalesced
  std::vector<Slice> expected_values = {"cf_2_col_val_0_key_1", "", "", ""};

  // Test for iteration over CF default->1->2->3
  {
    std::vector<ColumnFamilyHandle*> cfhs_order_0_1_2_3 = {
        handles_[0], handles_[1], handles_[2], handles_[3]};

    // Coalesced columns
    std::vector<WideColumns> expected_wide_columns_0_1_2_3 = {
        key_1_expected_columns_cfh_order_2_3,
        key_2_expected_columns_cfh_order_1_2, key_3_expected_columns,
        key_4_expected_columns};

    VerifyCoalescingIterator(cfhs_order_0_1_2_3, expected_keys, expected_values,
                             expected_wide_columns_0_1_2_3);
  }

  // Test for iteration over CF 3->2->default->1
  {
    std::vector<ColumnFamilyHandle*> cfhs_order_3_2_0_1 = {
        handles_[3], handles_[2], handles_[0], handles_[1]};

    // Coalesced columns
    std::vector<WideColumns> expected_wide_columns_3_2_0_1 = {
        key_1_expected_columns_cfh_order_3_2,
        key_2_expected_columns_cfh_order_2_1, key_3_expected_columns,
        key_4_expected_columns};

    VerifyCoalescingIterator(cfhs_order_3_2_0_1, expected_keys, expected_values,
                             expected_wide_columns_3_2_0_1);
  }
}

TEST_F(CoalescingIteratorTest, DifferentComparatorsInMultiCFs) {
  // This test creates two column families with two different comparators.
  // Attempting to create the CoalescingIterator should fail.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  DestroyAndReopen(options);
  options.comparator = BytewiseComparator();
  CreateColumnFamilies({"cf_forward"}, options);
  options.comparator = ReverseBytewiseComparator();
  CreateColumnFamilies({"cf_reverse"}, options);

  ASSERT_OK(Put(0, "key_1", "value_1"));
  ASSERT_OK(Put(0, "key_2", "value_2"));
  ASSERT_OK(Put(0, "key_3", "value_3"));
  ASSERT_OK(Put(1, "key_1", "value_1"));
  ASSERT_OK(Put(1, "key_2", "value_2"));
  ASSERT_OK(Put(1, "key_3", "value_3"));

  VerifyExpectedKeys(handles_[0], {"key_1", "key_2", "key_3"});
  VerifyExpectedKeys(handles_[1], {"key_3", "key_2", "key_1"});

  std::unique_ptr<Iterator> iter =
      db_->NewCoalescingIterator(ReadOptions(), handles_);
  ASSERT_NOK(iter->status());
  ASSERT_TRUE(iter->status().IsInvalidArgument());
}

TEST_F(CoalescingIteratorTest, CustomComparatorsInMultiCFs) {
  // This test creates two column families with the same custom test
  // comparators (but instantiated independently). Attempting to create the
  // CoalescingIterator should not fail.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  DestroyAndReopen(options);
  static auto comparator_1 =
      std::make_unique<test::SimpleSuffixReverseComparator>(
          test::SimpleSuffixReverseComparator());
  static auto comparator_2 =
      std::make_unique<test::SimpleSuffixReverseComparator>(
          test::SimpleSuffixReverseComparator());
  ASSERT_NE(comparator_1, comparator_2);

  options.comparator = comparator_1.get();
  CreateColumnFamilies({"cf_1"}, options);
  options.comparator = comparator_2.get();
  CreateColumnFamilies({"cf_2"}, options);

  ASSERT_OK(Put(0, "key_001_001", "value_0_3"));
  ASSERT_OK(Put(0, "key_001_002", "value_0_2"));
  ASSERT_OK(Put(0, "key_001_003", "value_0_1"));
  ASSERT_OK(Put(0, "key_002_001", "value_0_6"));
  ASSERT_OK(Put(0, "key_002_002", "value_0_5"));
  ASSERT_OK(Put(0, "key_002_003", "value_0_4"));
  ASSERT_OK(Put(1, "key_001_001", "value_1_3"));
  ASSERT_OK(Put(1, "key_001_002", "value_1_2"));
  ASSERT_OK(Put(1, "key_001_003", "value_1_1"));
  ASSERT_OK(Put(1, "key_003_004", "value_1_6"));
  ASSERT_OK(Put(1, "key_003_005", "value_1_5"));
  ASSERT_OK(Put(1, "key_003_006", "value_1_4"));

  VerifyExpectedKeys(
      handles_[0], {"key_001_003", "key_001_002", "key_001_001", "key_002_003",
                    "key_002_002", "key_002_001"});
  VerifyExpectedKeys(
      handles_[1], {"key_001_003", "key_001_002", "key_001_001", "key_003_006",
                    "key_003_005", "key_003_004"});

  std::vector<Slice> expected_keys = {
      "key_001_003", "key_001_002", "key_001_001", "key_002_003", "key_002_002",
      "key_002_001", "key_003_006", "key_003_005", "key_003_004"};
  std::vector<Slice> expected_values = {"value_1_1", "value_1_2", "value_1_3",
                                        "value_0_4", "value_0_5", "value_0_6",
                                        "value_1_4", "value_1_5", "value_1_6"};
  std::unique_ptr<Iterator> iter =
      db_->NewCoalescingIterator(ReadOptions(), handles_);

  size_t i = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_EQ(expected_keys[i], iter->key());
    ASSERT_EQ(expected_values[i], iter->value());
    ++i;
  }

  ASSERT_EQ(expected_keys.size(), i);
  ASSERT_OK(iter->status());
}

TEST_F(CoalescingIteratorTest, AllowUnpreparedValue) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;

  CreateAndReopenWithCF({"cf_1", "cf_2", "cf_3"}, options);

  ASSERT_OK(Put(0, "key_1", "key_1_cf_0_val"));
  ASSERT_OK(Put(3, "key_1", "key_1_cf_3_val"));
  ASSERT_OK(Put(1, "key_2", "key_2_cf_1_val"));
  ASSERT_OK(Put(2, "key_2", "key_2_cf_2_val"));
  ASSERT_OK(Put(0, "key_3", "key_3_cf_0_val"));
  ASSERT_OK(Put(1, "key_3", "key_3_cf_1_val"));
  ASSERT_OK(Put(3, "key_3", "key_3_cf_3_val"));

  ASSERT_OK(Flush());

  std::vector<ColumnFamilyHandle*> cfhs_order_3_2_0_1{handles_[3], handles_[2],
                                                      handles_[0], handles_[1]};
  std::vector<Slice> expected_keys{"key_1", "key_2", "key_3"};
  std::vector<Slice> expected_values{"key_1_cf_0_val", "key_2_cf_1_val",
                                     "key_3_cf_1_val"};

  VerifyCoalescingIterator(cfhs_order_3_2_0_1, expected_keys, expected_values,
                           /* expected_wide_columns */ std::nullopt,
                           /* lower_bound */ nullptr, /* upper_bound */ nullptr,
                           /* allow_unprepared_value */ true);

  ReadOptions read_options;
  read_options.allow_unprepared_value = true;

  {
    std::unique_ptr<Iterator> iter =
        db_->NewCoalescingIterator(read_options, cfhs_order_3_2_0_1);
    iter->Seek("");
    ASSERT_EQ(IterStatus(iter.get()), "key_1->");
    ASSERT_TRUE(iter->PrepareValue());
    ASSERT_EQ(IterStatus(iter.get()), "key_1->key_1_cf_0_val");

    iter->Seek("key_1");
    ASSERT_EQ(IterStatus(iter.get()), "key_1->");
    ASSERT_TRUE(iter->PrepareValue());
    ASSERT_EQ(IterStatus(iter.get()), "key_1->key_1_cf_0_val");

    iter->Seek("key_2");
    ASSERT_EQ(IterStatus(iter.get()), "key_2->");
    ASSERT_TRUE(iter->PrepareValue());
    ASSERT_EQ(IterStatus(iter.get()), "key_2->key_2_cf_1_val");

    iter->Next();
    ASSERT_EQ(IterStatus(iter.get()), "key_3->");
    ASSERT_TRUE(iter->PrepareValue());
    ASSERT_EQ(IterStatus(iter.get()), "key_3->key_3_cf_1_val");

    iter->Seek("key_x");
    ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
  }

  {
    std::unique_ptr<Iterator> iter =
        db_->NewCoalescingIterator(read_options, cfhs_order_3_2_0_1);
    iter->SeekForPrev("");
    ASSERT_EQ(IterStatus(iter.get()), "(invalid)");

    iter->SeekForPrev("key_1");
    ASSERT_EQ(IterStatus(iter.get()), "key_1->");
    ASSERT_TRUE(iter->PrepareValue());
    ASSERT_EQ(IterStatus(iter.get()), "key_1->key_1_cf_0_val");

    iter->Next();
    ASSERT_EQ(IterStatus(iter.get()), "key_2->");
    ASSERT_TRUE(iter->PrepareValue());
    ASSERT_EQ(IterStatus(iter.get()), "key_2->key_2_cf_1_val");

    iter->SeekForPrev("key_x");
    ASSERT_EQ(IterStatus(iter.get()), "key_3->");
    ASSERT_TRUE(iter->PrepareValue());
    ASSERT_EQ(IterStatus(iter.get()), "key_3->key_3_cf_1_val");

    iter->Next();
    ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
  }
}

TEST_F(CoalescingIteratorTest, AllowUnpreparedValueCorruption) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;

  CreateAndReopenWithCF({"cf_1", "cf_2", "cf_3"}, options);

  ASSERT_OK(Put(0, "key_1", "key_1_cf_0_val"));
  ASSERT_OK(Put(3, "key_1", "key_1_cf_3_val"));
  ASSERT_OK(Put(1, "key_2", "key_2_cf_1_val"));
  ASSERT_OK(Put(2, "key_2", "key_2_cf_2_val"));
  ASSERT_OK(Put(0, "key_3", "key_3_cf_0_val"));
  ASSERT_OK(Put(1, "key_3", "key_3_cf_1_val"));
  ASSERT_OK(Put(3, "key_3", "key_3_cf_3_val"));

  ASSERT_OK(Flush());

  ReadOptions read_options;
  read_options.allow_unprepared_value = true;

  std::vector<ColumnFamilyHandle*> cfhs_order_3_2_0_1{handles_[3], handles_[2],
                                                      handles_[0], handles_[1]};

  std::unique_ptr<Iterator> iter =
      db_->NewCoalescingIterator(read_options, cfhs_order_3_2_0_1);
  iter->SeekToFirst();

  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ(iter->key(), "key_1");
  ASSERT_TRUE(iter->value().empty());

  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileReader::GetBlob:TamperWithResult", [](void* arg) {
        Slice* const blob_index = static_cast<Slice*>(arg);
        assert(blob_index);
        assert(!blob_index->empty());
        blob_index->remove_prefix(1);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_FALSE(iter->PrepareValue());
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().IsCorruption());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(CoalescingIteratorTest,
       SingleColumnFamilyAllowUnpreparedValueMatchesDirectIterator) {
  Options options = GetDefaultOptions();
  CreateAndReopenWithCF({"cf_1"}, options);

  ASSERT_OK(Put(0, "key_1", "value_1"));
  ASSERT_OK(Put(0, "key_2", "value_2"));

  ReadOptions read_options;
  read_options.allow_unprepared_value = true;

  std::unique_ptr<Iterator> coalescing =
      db_->NewCoalescingIterator(read_options, {handles_[0]});
  std::unique_ptr<Iterator> direct(db_->NewIterator(read_options, handles_[0]));

  auto verify_same_entry = [&](const Slice& target) {
    coalescing->Seek(target);
    direct->Seek(target);

    ASSERT_EQ(direct->Valid(), coalescing->Valid());
    ASSERT_EQ(direct->status(), coalescing->status());
    ASSERT_TRUE(coalescing->Valid());
    ASSERT_EQ(direct->key(), coalescing->key());
    ASSERT_EQ(direct->value(), coalescing->value());
    ASSERT_EQ(direct->columns(), coalescing->columns());
  };

  verify_same_entry("key_1");
  verify_same_entry("key_2");
}

TEST_F(CoalescingIteratorTest, AllowUnpreparedValueWithSnapshotAutoRefresh) {
  constexpr int kNumKeys = 32;

  auto make_value = [](int cf, const char* phase, int key) {
    return "cf" + std::to_string(cf) + "_" + phase + "_" + std::to_string(key) +
           std::string(32, static_cast<char>('a' + cf));
  };

  Options options = GetDefaultOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 16;

  CreateAndReopenWithCF({"cf_1"}, options);

  for (int key = 0; key < kNumKeys; ++key) {
    ASSERT_OK(Put(0, Key(key), make_value(0, "base", key)));
    ASSERT_OK(Put(1, Key(key), make_value(1, "base", key)));
  }
  ASSERT_OK(Flush({0, 1}));
  MoveFilesToLevel(1, 0);
  MoveFilesToLevel(1, 1);

  for (int key = 0; key < kNumKeys; ++key) {
    ASSERT_OK(Put(0, Key(key), make_value(0, "visible", key)));
    ASSERT_OK(Put(1, Key(key), make_value(1, "visible", key)));
  }

  ReadOptions read_options;
  read_options.allow_unprepared_value = true;
  read_options.auto_refresh_iterator_with_snapshot = true;
  const Snapshot* snapshot = db_->GetSnapshot();
  read_options.snapshot = snapshot;

  std::vector<ColumnFamilyHandle*> cfhs_order_1_0 = {handles_[1], handles_[0]};
  std::unique_ptr<Iterator> iter =
      db_->NewCoalescingIterator(read_options, cfhs_order_1_0);

  int seen = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_EQ(Key(seen), iter->key());
    ASSERT_TRUE(iter->value().empty());
    ASSERT_TRUE(iter->PrepareValue());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(make_value(0, "visible", seen), iter->value().ToString());

    if (seen == kNumKeys / 2) {
      ASSERT_OK(Flush({0, 1}));
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), handles_[0], nullptr,
                                  nullptr));
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                                  nullptr));
    }
    ++seen;
  }

  ASSERT_EQ(kNumKeys, seen);
  ASSERT_OK(iter->status());

  db_->ReleaseSnapshot(snapshot);
}

class AttributeGroupIteratorTest : public DBTestBase {
 public:
  AttributeGroupIteratorTest()
      : DBTestBase("attribute_group_iterator_test", /*env_do_fsync=*/true) {}

  void VerifyAttributeGroupIterator(
      const std::vector<ColumnFamilyHandle*>& cfhs,
      const std::vector<Slice>& expected_keys,
      const std::vector<IteratorAttributeGroups>& expected_attribute_groups,
      const Slice* lower_bound = nullptr, const Slice* upper_bound = nullptr,
      bool allow_unprepared_value = false) {
    const size_t num_keys = expected_keys.size();

    ReadOptions read_options;
    read_options.iterate_lower_bound = lower_bound;
    read_options.iterate_upper_bound = upper_bound;
    read_options.allow_unprepared_value = allow_unprepared_value;

    std::unique_ptr<AttributeGroupIterator> iter =
        db_->NewAttributeGroupIterator(read_options, cfhs);

    auto check_iter_entry = [&](size_t idx) {
      ASSERT_EQ(iter->key(), expected_keys[idx]);

      if (allow_unprepared_value) {
        ASSERT_TRUE(iter->attribute_groups().empty());
        ASSERT_TRUE(iter->PrepareValue());
        ASSERT_TRUE(iter->Valid());
      }

      ASSERT_EQ(iter->attribute_groups(), expected_attribute_groups[idx]);
    };

    {
      size_t i = 0;
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        check_iter_entry(i);
        ++i;
      }

      ASSERT_EQ(i, num_keys);
      ASSERT_OK(iter->status());
    }

    {
      size_t i = 0;
      for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
        check_iter_entry(num_keys - 1 - i);
        ++i;
      }

      ASSERT_EQ(i, num_keys);
      ASSERT_OK(iter->status());
    }
  }
};

TEST_F(AttributeGroupIteratorTest, IterateAttributeGroups) {
  // Set up the DB and Column Families
  Options options = GetDefaultOptions();
  CreateAndReopenWithCF({"cf_1", "cf_2", "cf_3"}, options);

  constexpr char key_1[] = "key_1";
  WideColumns key_1_columns_in_cf_2{
      {kDefaultWideColumnName, "cf_2_col_val_0_key_1"},
      {"cf_2_col_name_1", "cf_2_col_val_1_key_1"},
      {"cf_2_col_name_2", "cf_2_col_val_2_key_1"}};
  WideColumns key_1_columns_in_cf_3{
      {"cf_3_col_name_1", "cf_3_col_val_1_key_1"},
      {"cf_3_col_name_2", "cf_3_col_val_2_key_1"},
      {"cf_3_col_name_3", "cf_3_col_val_3_key_1"}};

  constexpr char key_2[] = "key_2";
  WideColumns key_2_columns_in_cf_1{
      {"cf_1_col_name_1", "cf_1_col_val_1_key_2"}};
  WideColumns key_2_columns_in_cf_2{
      {"cf_2_col_name_1", "cf_2_col_val_1_key_2"},
      {"cf_2_col_name_2", "cf_2_col_val_2_key_2"}};

  constexpr char key_3[] = "key_3";
  WideColumns key_3_columns_in_cf_1{
      {"cf_1_col_name_1", "cf_1_col_val_1_key_3"}};
  WideColumns key_3_columns_in_cf_3{
      {"cf_3_col_name_1", "cf_3_col_val_1_key_3"}};

  constexpr char key_4[] = "key_4";
  WideColumns key_4_columns_in_cf_0{
      {"cf_0_col_name_1", "cf_0_col_val_1_key_4"}};
  WideColumns key_4_columns_in_cf_2{
      {"cf_2_col_name_1", "cf_2_col_val_1_key_4"}};

  AttributeGroups key_1_attribute_groups{
      AttributeGroup(handles_[2], key_1_columns_in_cf_2),
      AttributeGroup(handles_[3], key_1_columns_in_cf_3)};
  AttributeGroups key_2_attribute_groups{
      AttributeGroup(handles_[1], key_2_columns_in_cf_1),
      AttributeGroup(handles_[2], key_2_columns_in_cf_2)};
  AttributeGroups key_3_attribute_groups{
      AttributeGroup(handles_[1], key_3_columns_in_cf_1),
      AttributeGroup(handles_[3], key_3_columns_in_cf_3)};
  AttributeGroups key_4_attribute_groups{
      AttributeGroup(handles_[0], key_4_columns_in_cf_0),
      AttributeGroup(handles_[2], key_4_columns_in_cf_2)};

  ASSERT_OK(db_->PutEntity(WriteOptions(), key_1, key_1_attribute_groups));
  ASSERT_OK(db_->PutEntity(WriteOptions(), key_2, key_2_attribute_groups));
  ASSERT_OK(db_->PutEntity(WriteOptions(), key_3, key_3_attribute_groups));
  ASSERT_OK(db_->PutEntity(WriteOptions(), key_4, key_4_attribute_groups));

  IteratorAttributeGroups key_1_expected_attribute_groups{
      IteratorAttributeGroup(key_1_attribute_groups[0]),
      IteratorAttributeGroup(key_1_attribute_groups[1])};
  IteratorAttributeGroups key_2_expected_attribute_groups{
      IteratorAttributeGroup(key_2_attribute_groups[0]),
      IteratorAttributeGroup(key_2_attribute_groups[1])};
  IteratorAttributeGroups key_3_expected_attribute_groups{
      IteratorAttributeGroup(key_3_attribute_groups[0]),
      IteratorAttributeGroup(key_3_attribute_groups[1])};
  IteratorAttributeGroups key_4_expected_attribute_groups{
      IteratorAttributeGroup(key_4_attribute_groups[0]),
      IteratorAttributeGroup(key_4_attribute_groups[1])};

  // Test for iteration over CF default->1->2->3
  std::vector<ColumnFamilyHandle*> cfhs_order_0_1_2_3 = {
      handles_[0], handles_[1], handles_[2], handles_[3]};
  {
    std::vector<Slice> expected_keys = {key_1, key_2, key_3, key_4};
    std::vector<IteratorAttributeGroups> expected_attribute_groups{
        key_1_expected_attribute_groups, key_2_expected_attribute_groups,
        key_3_expected_attribute_groups, key_4_expected_attribute_groups};
    VerifyAttributeGroupIterator(cfhs_order_0_1_2_3, expected_keys,
                                 expected_attribute_groups);
  }

  Slice lb = Slice("key_2");
  Slice ub = Slice("key_4");

  // Test for lower bound only
  {
    std::vector<Slice> expected_keys = {key_2, key_3, key_4};
    std::vector<IteratorAttributeGroups> expected_attribute_groups{
        key_2_expected_attribute_groups, key_3_expected_attribute_groups,
        key_4_expected_attribute_groups};
    VerifyAttributeGroupIterator(cfhs_order_0_1_2_3, expected_keys,
                                 expected_attribute_groups, &lb);
  }

  // Test for upper bound only
  {
    std::vector<Slice> expected_keys = {key_1, key_2, key_3};
    std::vector<IteratorAttributeGroups> expected_attribute_groups{
        key_1_expected_attribute_groups, key_2_expected_attribute_groups,
        key_3_expected_attribute_groups};
    VerifyAttributeGroupIterator(cfhs_order_0_1_2_3, expected_keys,
                                 expected_attribute_groups, nullptr, &ub);
  }

  // Test for lower and upper bound
  {
    std::vector<Slice> expected_keys = {key_2, key_3};
    std::vector<IteratorAttributeGroups> expected_attribute_groups{
        key_2_expected_attribute_groups, key_3_expected_attribute_groups};
    VerifyAttributeGroupIterator(cfhs_order_0_1_2_3, expected_keys,
                                 expected_attribute_groups, &lb, &ub);
  }
}

TEST_F(AttributeGroupIteratorTest, AllowUnpreparedValue) {
  Options options = GetDefaultOptions();
  CreateAndReopenWithCF({"cf_1", "cf_2", "cf_3"}, options);

  constexpr char key_1[] = "key_1";
  WideColumns key_1_columns_in_cf_2{
      {kDefaultWideColumnName, "cf_2_col_val_0_key_1"},
      {"cf_2_col_name_1", "cf_2_col_val_1_key_1"},
      {"cf_2_col_name_2", "cf_2_col_val_2_key_1"}};
  WideColumns key_1_columns_in_cf_3{
      {"cf_3_col_name_1", "cf_3_col_val_1_key_1"},
      {"cf_3_col_name_2", "cf_3_col_val_2_key_1"},
      {"cf_3_col_name_3", "cf_3_col_val_3_key_1"}};

  constexpr char key_2[] = "key_2";
  WideColumns key_2_columns_in_cf_1{
      {"cf_1_col_name_1", "cf_1_col_val_1_key_2"}};
  WideColumns key_2_columns_in_cf_2{
      {"cf_2_col_name_1", "cf_2_col_val_1_key_2"},
      {"cf_2_col_name_2", "cf_2_col_val_2_key_2"}};

  constexpr char key_3[] = "key_3";
  WideColumns key_3_columns_in_cf_1{
      {"cf_1_col_name_1", "cf_1_col_val_1_key_3"}};
  WideColumns key_3_columns_in_cf_3{
      {"cf_3_col_name_1", "cf_3_col_val_1_key_3"}};

  constexpr char key_4[] = "key_4";
  WideColumns key_4_columns_in_cf_0{
      {"cf_0_col_name_1", "cf_0_col_val_1_key_4"}};
  WideColumns key_4_columns_in_cf_2{
      {"cf_2_col_name_1", "cf_2_col_val_1_key_4"}};

  AttributeGroups key_1_attribute_groups{
      AttributeGroup(handles_[2], key_1_columns_in_cf_2),
      AttributeGroup(handles_[3], key_1_columns_in_cf_3)};
  AttributeGroups key_2_attribute_groups{
      AttributeGroup(handles_[1], key_2_columns_in_cf_1),
      AttributeGroup(handles_[2], key_2_columns_in_cf_2)};
  AttributeGroups key_3_attribute_groups{
      AttributeGroup(handles_[1], key_3_columns_in_cf_1),
      AttributeGroup(handles_[3], key_3_columns_in_cf_3)};
  AttributeGroups key_4_attribute_groups{
      AttributeGroup(handles_[0], key_4_columns_in_cf_0),
      AttributeGroup(handles_[2], key_4_columns_in_cf_2)};

  ASSERT_OK(db_->PutEntity(WriteOptions(), key_1, key_1_attribute_groups));
  ASSERT_OK(db_->PutEntity(WriteOptions(), key_2, key_2_attribute_groups));
  ASSERT_OK(db_->PutEntity(WriteOptions(), key_3, key_3_attribute_groups));
  ASSERT_OK(db_->PutEntity(WriteOptions(), key_4, key_4_attribute_groups));

  IteratorAttributeGroups key_1_expected_attribute_groups{
      IteratorAttributeGroup(key_1_attribute_groups[0]),
      IteratorAttributeGroup(key_1_attribute_groups[1])};
  IteratorAttributeGroups key_2_expected_attribute_groups{
      IteratorAttributeGroup(key_2_attribute_groups[0]),
      IteratorAttributeGroup(key_2_attribute_groups[1])};
  IteratorAttributeGroups key_3_expected_attribute_groups{
      IteratorAttributeGroup(key_3_attribute_groups[0]),
      IteratorAttributeGroup(key_3_attribute_groups[1])};
  IteratorAttributeGroups key_4_expected_attribute_groups{
      IteratorAttributeGroup(key_4_attribute_groups[0]),
      IteratorAttributeGroup(key_4_attribute_groups[1])};

  std::vector<ColumnFamilyHandle*> cfhs_order_0_1_2_3{handles_[0], handles_[1],
                                                      handles_[2], handles_[3]};
  std::vector<Slice> expected_keys{key_1, key_2, key_3, key_4};
  std::vector<IteratorAttributeGroups> expected_attribute_groups{
      key_1_expected_attribute_groups, key_2_expected_attribute_groups,
      key_3_expected_attribute_groups, key_4_expected_attribute_groups};
  VerifyAttributeGroupIterator(
      cfhs_order_0_1_2_3, expected_keys, expected_attribute_groups,
      /* lower_bound */ nullptr, /* upper_bound */ nullptr,
      /* allow_unprepared_value */ true);
}

TEST_F(AttributeGroupIteratorTest, BlobBackedWideColumns) {
  Options options =
      wide_column_test_util::GetDirectWriteOptions(GetDefaultOptions());
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"cf_1", "cf_2"}, options);

  constexpr char key_1[] = "key_1";
  const std::string key_1_default_blob =
      wide_column_test_util::GenerateLargeValue(96, 'a');
  const std::string key_1_cf_2_blob =
      wide_column_test_util::GenerateLargeValue(128, 'b');
  WideColumns key_1_default_columns{
      {kDefaultWideColumnName, key_1_default_blob}, {"meta", "default_meta_1"}};
  WideColumns key_1_cf_2_columns{{"blob", key_1_cf_2_blob},
                                 {"tag", "cf_2_tag_1"}};

  constexpr char key_2[] = "key_2";
  const std::string key_2_cf_1_blob =
      wide_column_test_util::GenerateLargeValue(112, 'c');
  const std::string key_2_cf_2_blob =
      wide_column_test_util::GenerateLargeValue(144, 'd');
  WideColumns key_2_cf_1_columns{{kDefaultWideColumnName, key_2_cf_1_blob},
                                 {"meta", "cf_1_meta_2"}};
  WideColumns key_2_cf_2_columns{{"blob", key_2_cf_2_blob},
                                 {"tag", "cf_2_tag_2"}};

  AttributeGroups key_1_attribute_groups{
      AttributeGroup(handles_[0], key_1_default_columns),
      AttributeGroup(handles_[2], key_1_cf_2_columns)};
  AttributeGroups key_2_attribute_groups{
      AttributeGroup(handles_[1], key_2_cf_1_columns),
      AttributeGroup(handles_[2], key_2_cf_2_columns)};

  ASSERT_OK(db_->PutEntity(WriteOptions(), key_1, key_1_attribute_groups));
  ASSERT_OK(db_->PutEntity(WriteOptions(), key_2, key_2_attribute_groups));
  auto count_blob_files = [&]() -> size_t {
    std::vector<std::string> files;
    Status s = env_->GetChildren(dbname_, &files);
    EXPECT_OK(s);
    if (!s.ok()) {
      return 0;
    }

    size_t blob_files = 0;
    for (const auto& file : files) {
      if (file.size() > 5 && file.substr(file.size() - 5) == ".blob") {
        ++blob_files;
      }
    }
    return blob_files;
  };
  ASSERT_GT(count_blob_files(), 0U);

  IteratorAttributeGroups key_1_expected_attribute_groups{
      IteratorAttributeGroup(key_1_attribute_groups[0]),
      IteratorAttributeGroup(key_1_attribute_groups[1])};
  IteratorAttributeGroups key_2_expected_attribute_groups{
      IteratorAttributeGroup(key_2_attribute_groups[0]),
      IteratorAttributeGroup(key_2_attribute_groups[1])};

  const std::vector<ColumnFamilyHandle*> cfhs_order_0_1_2 = {
      handles_[0], handles_[1], handles_[2]};
  const std::vector<Slice> expected_keys = {key_1, key_2};
  const std::vector<IteratorAttributeGroups> expected_attribute_groups{
      key_1_expected_attribute_groups, key_2_expected_attribute_groups};

  auto verify = [&](bool allow_unprepared_value) {
    VerifyAttributeGroupIterator(
        cfhs_order_0_1_2, expected_keys, expected_attribute_groups,
        /* lower_bound */ nullptr, /* upper_bound */ nullptr,
        allow_unprepared_value);
  };

  verify(/* allow_unprepared_value */ false);
  verify(/* allow_unprepared_value */ true);

  ASSERT_OK(Flush({0, 1, 2}));
  ASSERT_GT(count_blob_files(), 0U);

  verify(/* allow_unprepared_value */ false);
  verify(/* allow_unprepared_value */ true);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
