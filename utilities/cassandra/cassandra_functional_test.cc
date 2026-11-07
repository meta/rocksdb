// Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <iostream>

#include "db/db_impl/db_impl.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/utilities/object_registry.h"
#include "test_util/testharness.h"
#include "util/cast_util.h"
#include "util/random.h"
#include "utilities/cassandra/cassandra_compaction_filter.h"
#include "utilities/cassandra/merge_operator.h"
#include "utilities/cassandra/test_utils.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE::cassandra {

// Path to the database on file system
const std::string kDbName = test::PerThreadDBPath("cassandra_functional_test");

class CassandraStore {
 public:
  explicit CassandraStore(UnownedPtr<DB> db)
      : db_(db), write_option_(), get_option_() {
    assert(db);
  }

  bool Append(const std::string& key, const RowValue& val) {
    std::string result;
    val.Serialize(&result);
    Slice valSlice(result.data(), result.size());
    auto s = db_->Merge(write_option_, key, valSlice);

    if (s.ok()) {
      return true;
    } else {
      std::cerr << "ERROR " << s.ToString() << std::endl;
      return false;
    }
  }

  bool Put(const std::string& key, const RowValue& val) {
    std::string result;
    val.Serialize(&result);
    Slice valSlice(result.data(), result.size());
    auto s = db_->Put(write_option_, key, valSlice);
    if (s.ok()) {
      return true;
    } else {
      std::cerr << "ERROR " << s.ToString() << std::endl;
      return false;
    }
  }

  Status Flush() {
    Status s = dbfull()->TEST_FlushMemTable();
    if (s.ok()) {
      s = dbfull()->TEST_WaitForCompact();
    }
    return s;
  }

  Status Compact() {
    return dbfull()->TEST_CompactRange(0, nullptr, nullptr,
                                       db_->DefaultColumnFamily());
  }

  std::tuple<bool, RowValue> Get(const std::string& key) {
    std::string result;
    auto s = db_->Get(get_option_, key, &result);

    if (s.ok()) {
      return std::make_tuple(
          true, RowValue::Deserialize(result.data(), result.size()));
    }

    if (!s.IsNotFound()) {
      std::cerr << "ERROR " << s.ToString() << std::endl;
    }

    return std::make_tuple(false, RowValue(0, 0));
  }

 private:
  UnownedPtr<DB> db_;
  WriteOptions write_option_;
  ReadOptions get_option_;

  DBImpl* dbfull() { return static_cast_with_check<DBImpl>(db_.get()); }
};

class TestCompactionFilterFactory : public CompactionFilterFactory {
 public:
  explicit TestCompactionFilterFactory(bool purge_ttl_on_expiration,
                                       int32_t gc_grace_period_in_seconds)
      : purge_ttl_on_expiration_(purge_ttl_on_expiration),
        gc_grace_period_in_seconds_(gc_grace_period_in_seconds) {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::unique_ptr<CompactionFilter>(new CassandraCompactionFilter(
        purge_ttl_on_expiration_, gc_grace_period_in_seconds_));
  }

  const char* Name() const override { return "TestCompactionFilterFactory"; }

 private:
  bool purge_ttl_on_expiration_;
  int32_t gc_grace_period_in_seconds_;
};

// The class for unit-testing
class CassandraFunctionalTest : public testing::Test {
 public:
  CassandraFunctionalTest() {
    EXPECT_OK(
        DestroyDB(kDbName, Options()));  // Start each test with a fresh DB
  }

  std::unique_ptr<DB> OpenDb() {
    Options options;
    options.create_if_missing = true;
    options.merge_operator = std::make_shared<CassandraValueMergeOperator>(
        gc_grace_period_in_seconds_);
    auto* cf_factory = new TestCompactionFilterFactory(
        purge_ttl_on_expiration_, gc_grace_period_in_seconds_);
    options.compaction_filter_factory.reset(cf_factory);
    std::unique_ptr<DB> db;
    EXPECT_OK(DB::Open(options, kDbName, &db));
    return db;
  }

  bool purge_ttl_on_expiration_ = false;
  int32_t gc_grace_period_in_seconds_ = 100;
};

// THE TEST CASES BEGIN HERE

TEST_F(CassandraFunctionalTest, SimpleMergeTest) {
  auto db = OpenDb();
  CassandraStore store(db.get());
  int64_t now = time(nullptr);

  store.Append(
      "k1",
      CreateTestRowValue({
          CreateTestColumnSpec(kTombstone, 0, ToMicroSeconds(now + 5)),
          CreateTestColumnSpec(kColumn, 1, ToMicroSeconds(now + 8)),
          CreateTestColumnSpec(kExpiringColumn, 2, ToMicroSeconds(now + 5)),
      }));
  store.Append(
      "k1",
      CreateTestRowValue({
          CreateTestColumnSpec(kColumn, 0, ToMicroSeconds(now + 2)),
          CreateTestColumnSpec(kExpiringColumn, 1, ToMicroSeconds(now + 5)),
          CreateTestColumnSpec(kTombstone, 2, ToMicroSeconds(now + 7)),
          CreateTestColumnSpec(kExpiringColumn, 7, ToMicroSeconds(now + 17)),
      }));
  store.Append(
      "k1",
      CreateTestRowValue({
          CreateTestColumnSpec(kExpiringColumn, 0, ToMicroSeconds(now + 6)),
          CreateTestColumnSpec(kTombstone, 1, ToMicroSeconds(now + 5)),
          CreateTestColumnSpec(kColumn, 2, ToMicroSeconds(now + 4)),
          CreateTestColumnSpec(kTombstone, 11, ToMicroSeconds(now + 11)),
      }));

  auto ret = store.Get("k1");

  ASSERT_TRUE(std::get<0>(ret));
  RowValue& merged = std::get<1>(ret);
  EXPECT_EQ(merged.get_columns().size(), 5);
  VerifyRowValueColumns(merged.get_columns(), 0, kExpiringColumn, 0,
                        ToMicroSeconds(now + 6));
  VerifyRowValueColumns(merged.get_columns(), 1, kColumn, 1,
                        ToMicroSeconds(now + 8));
  VerifyRowValueColumns(merged.get_columns(), 2, kTombstone, 2,
                        ToMicroSeconds(now + 7));
  VerifyRowValueColumns(merged.get_columns(), 3, kExpiringColumn, 7,
                        ToMicroSeconds(now + 17));
  VerifyRowValueColumns(merged.get_columns(), 4, kTombstone, 11,
                        ToMicroSeconds(now + 11));
}

constexpr int64_t kTestTimeoutSecs = 600;

TEST_F(CassandraFunctionalTest,
       CompactionShouldConvertExpiredColumnsToTombstone) {
  auto db = OpenDb();
  CassandraStore store(db.get());
  int64_t now = time(nullptr);

  store.Append(
      "k1",
      CreateTestRowValue(
          {CreateTestColumnSpec(kExpiringColumn, 0,
                                ToMicroSeconds(now - kTtl - 20)),  // expired
           CreateTestColumnSpec(
               kExpiringColumn, 1,
               ToMicroSeconds(now - kTtl + kTestTimeoutSecs)),  // not expired
           CreateTestColumnSpec(kTombstone, 3, ToMicroSeconds(now))}));

  ASSERT_OK(store.Flush());

  store.Append(
      "k1",
      CreateTestRowValue(
          {CreateTestColumnSpec(kExpiringColumn, 0,
                                ToMicroSeconds(now - kTtl - 10)),  // expired
           CreateTestColumnSpec(kColumn, 2, ToMicroSeconds(now))}));

  ASSERT_OK(store.Flush());
  ASSERT_OK(store.Compact());

  auto ret = store.Get("k1");
  ASSERT_TRUE(std::get<0>(ret));
  RowValue& merged = std::get<1>(ret);
  EXPECT_EQ(merged.get_columns().size(), 4);
  VerifyRowValueColumns(merged.get_columns(), 0, kTombstone, 0,
                        ToMicroSeconds(now - 10));
  VerifyRowValueColumns(merged.get_columns(), 1, kExpiringColumn, 1,
                        ToMicroSeconds(now - kTtl + kTestTimeoutSecs));
  VerifyRowValueColumns(merged.get_columns(), 2, kColumn, 2,
                        ToMicroSeconds(now));
  VerifyRowValueColumns(merged.get_columns(), 3, kTombstone, 3,
                        ToMicroSeconds(now));
}

TEST_F(CassandraFunctionalTest,
       CompactionShouldPurgeExpiredColumnsIfPurgeTtlIsOn) {
  purge_ttl_on_expiration_ = true;
  auto db = OpenDb();
  CassandraStore store(db.get());
  int64_t now = time(nullptr);

  store.Append(
      "k1",
      CreateTestRowValue(
          {CreateTestColumnSpec(kExpiringColumn, 0,
                                ToMicroSeconds(now - kTtl - 20)),  // expired
           CreateTestColumnSpec(kExpiringColumn, 1,
                                ToMicroSeconds(now)),  // not expired
           CreateTestColumnSpec(kTombstone, 3, ToMicroSeconds(now))}));

  ASSERT_OK(store.Flush());

  store.Append(
      "k1",
      CreateTestRowValue(
          {CreateTestColumnSpec(kExpiringColumn, 0,
                                ToMicroSeconds(now - kTtl - 10)),  // expired
           CreateTestColumnSpec(kColumn, 2, ToMicroSeconds(now))}));

  ASSERT_OK(store.Flush());
  ASSERT_OK(store.Compact());

  auto ret = store.Get("k1");
  ASSERT_TRUE(std::get<0>(ret));
  RowValue& merged = std::get<1>(ret);
  EXPECT_EQ(merged.get_columns().size(), 3);
  VerifyRowValueColumns(merged.get_columns(), 0, kExpiringColumn, 1,
                        ToMicroSeconds(now));
  VerifyRowValueColumns(merged.get_columns(), 1, kColumn, 2,
                        ToMicroSeconds(now));
  VerifyRowValueColumns(merged.get_columns(), 2, kTombstone, 3,
                        ToMicroSeconds(now));
}

TEST_F(CassandraFunctionalTest,
       CompactionShouldRemoveRowWhenAllColumnsExpiredIfPurgeTtlIsOn) {
  purge_ttl_on_expiration_ = true;
  auto db = OpenDb();
  CassandraStore store(db.get());
  int64_t now = time(nullptr);

  store.Append("k1", CreateTestRowValue({
                         CreateTestColumnSpec(kExpiringColumn, 0,
                                              ToMicroSeconds(now - kTtl - 20)),
                         CreateTestColumnSpec(kExpiringColumn, 1,
                                              ToMicroSeconds(now - kTtl - 20)),
                     }));

  ASSERT_OK(store.Flush());

  store.Append("k1", CreateTestRowValue({
                         CreateTestColumnSpec(kExpiringColumn, 0,
                                              ToMicroSeconds(now - kTtl - 10)),
                     }));

  ASSERT_OK(store.Flush());
  ASSERT_OK(store.Compact());
  ASSERT_FALSE(std::get<0>(store.Get("k1")));
}

TEST_F(CassandraFunctionalTest,
       CompactionShouldRemoveTombstoneExceedingGCGracePeriod) {
  purge_ttl_on_expiration_ = true;
  auto db = OpenDb();
  CassandraStore store(db.get());
  int64_t now = time(nullptr);

  store.Append("k1",
               CreateTestRowValue(
                   {CreateTestColumnSpec(
                        kTombstone, 0,
                        ToMicroSeconds(now - gc_grace_period_in_seconds_ - 1)),
                    CreateTestColumnSpec(kColumn, 1, ToMicroSeconds(now))}));

  store.Append("k2", CreateTestRowValue({CreateTestColumnSpec(
                         kColumn, 0, ToMicroSeconds(now))}));

  ASSERT_OK(store.Flush());

  store.Append("k1", CreateTestRowValue({
                         CreateTestColumnSpec(kColumn, 1, ToMicroSeconds(now)),
                     }));

  ASSERT_OK(store.Flush());
  ASSERT_OK(store.Compact());

  auto ret = store.Get("k1");
  ASSERT_TRUE(std::get<0>(ret));
  RowValue& gced = std::get<1>(ret);
  EXPECT_EQ(gced.get_columns().size(), 1);
  VerifyRowValueColumns(gced.get_columns(), 0, kColumn, 1, ToMicroSeconds(now));
}

TEST_F(CassandraFunctionalTest, CompactionShouldRemoveTombstoneFromPut) {
  purge_ttl_on_expiration_ = true;
  auto db = OpenDb();
  CassandraStore store(db.get());
  int64_t now = time(nullptr);

  store.Put("k1",
            CreateTestRowValue({
                CreateTestColumnSpec(
                    kTombstone, 0,
                    ToMicroSeconds(now - gc_grace_period_in_seconds_ - 1)),
            }));

  ASSERT_OK(store.Flush());
  ASSERT_OK(store.Compact());
  ASSERT_FALSE(std::get<0>(store.Get("k1")));
}

TEST_F(CassandraFunctionalTest, LoadMergeOperator) {
  ConfigOptions config_options;
  std::shared_ptr<MergeOperator> mo;
  config_options.ignore_unsupported_options = false;

  ASSERT_NOK(MergeOperator::CreateFromString(
      config_options, CassandraValueMergeOperator::kClassName(), &mo));

  config_options.registry->AddLibrary("cassandra", RegisterCassandraObjects,
                                      "cassandra");

  ASSERT_OK(MergeOperator::CreateFromString(
      config_options, CassandraValueMergeOperator::kClassName(), &mo));
  ASSERT_NE(mo, nullptr);
  ASSERT_STREQ(mo->Name(), CassandraValueMergeOperator::kClassName());
  mo.reset();
  ASSERT_OK(MergeOperator::CreateFromString(
      config_options,
      std::string("operands_limit=20;gc_grace_period_in_seconds=42;id=") +
          CassandraValueMergeOperator::kClassName(),
      &mo));
  ASSERT_NE(mo, nullptr);
  ASSERT_STREQ(mo->Name(), CassandraValueMergeOperator::kClassName());
  const auto* opts = mo->GetOptions<CassandraOptions>();
  ASSERT_NE(opts, nullptr);
  ASSERT_EQ(opts->gc_grace_period_in_seconds, 42);
  ASSERT_EQ(opts->operands_limit, 20);
}

TEST_F(CassandraFunctionalTest, LoadCompactionFilter) {
  ConfigOptions config_options;
  const CompactionFilter* filter = nullptr;
  config_options.ignore_unsupported_options = false;

  ASSERT_NOK(CompactionFilter::CreateFromString(
      config_options, CassandraCompactionFilter::kClassName(), &filter));
  config_options.registry->AddLibrary("cassandra", RegisterCassandraObjects,
                                      "cassandra");

  ASSERT_OK(CompactionFilter::CreateFromString(
      config_options, CassandraCompactionFilter::kClassName(), &filter));
  ASSERT_NE(filter, nullptr);
  ASSERT_STREQ(filter->Name(), CassandraCompactionFilter::kClassName());
  delete filter;
  filter = nullptr;
  ASSERT_OK(CompactionFilter::CreateFromString(
      config_options,
      std::string(
          "purge_ttl_on_expiration=true;gc_grace_period_in_seconds=42;id=") +
          CassandraCompactionFilter::kClassName(),
      &filter));
  ASSERT_NE(filter, nullptr);
  ASSERT_STREQ(filter->Name(), CassandraCompactionFilter::kClassName());
  const auto* opts = filter->GetOptions<CassandraOptions>();
  ASSERT_NE(opts, nullptr);
  ASSERT_EQ(opts->gc_grace_period_in_seconds, 42);
  ASSERT_TRUE(opts->purge_ttl_on_expiration);
  delete filter;
}

TEST_F(CassandraFunctionalTest, LoadCompactionFilterFactory) {
  ConfigOptions config_options;
  std::shared_ptr<CompactionFilterFactory> factory;

  config_options.ignore_unsupported_options = false;
  ASSERT_NOK(CompactionFilterFactory::CreateFromString(
      config_options, CassandraCompactionFilterFactory::kClassName(),
      &factory));
  config_options.registry->AddLibrary("cassandra", RegisterCassandraObjects,
                                      "cassandra");

  ASSERT_OK(CompactionFilterFactory::CreateFromString(
      config_options, CassandraCompactionFilterFactory::kClassName(),
      &factory));
  ASSERT_NE(factory, nullptr);
  ASSERT_STREQ(factory->Name(), CassandraCompactionFilterFactory::kClassName());
  factory.reset();
  ASSERT_OK(CompactionFilterFactory::CreateFromString(
      config_options,
      std::string(
          "purge_ttl_on_expiration=true;gc_grace_period_in_seconds=42;id=") +
          CassandraCompactionFilterFactory::kClassName(),
      &factory));
  ASSERT_NE(factory, nullptr);
  ASSERT_STREQ(factory->Name(), CassandraCompactionFilterFactory::kClassName());
  const auto* opts = factory->GetOptions<CassandraOptions>();
  ASSERT_NE(opts, nullptr);
  ASSERT_EQ(opts->gc_grace_period_in_seconds, 42);
  ASSERT_TRUE(opts->purge_ttl_on_expiration);
}

}  // namespace ROCKSDB_NAMESPACE::cassandra

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
