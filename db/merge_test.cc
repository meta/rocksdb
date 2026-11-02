//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include <cassert>
#include <iostream>
#include <memory>

#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "db/merge_helper.h"
#include "db/write_batch_internal.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/utilities/db_ttl.h"
#include "rocksdb/wide_columns.h"
#include "test_util/testharness.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

bool use_compression;

class MergeTest : public testing::Test {};

size_t num_merge_operator_calls;
void resetNumMergeOperatorCalls() { num_merge_operator_calls = 0; }

size_t num_partial_merge_calls;
void resetNumPartialMergeCalls() { num_partial_merge_calls = 0; }

class CountMergeOperator : public AssociativeMergeOperator {
 public:
  CountMergeOperator() {
    mergeOperator_ = MergeOperators::CreateUInt64AddOperator();
  }

  bool Merge(const Slice& key, const Slice* existing_value, const Slice& value,
             std::string* new_value, Logger* logger) const override {
    assert(new_value->empty());
    ++num_merge_operator_calls;
    if (existing_value == nullptr) {
      new_value->assign(value.data(), value.size());
      return true;
    }

    return mergeOperator_->PartialMerge(key, *existing_value, value, new_value,
                                        logger);
  }

  bool PartialMergeMulti(const Slice& key,
                         const std::deque<Slice>& operand_list,
                         std::string* new_value,
                         Logger* logger) const override {
    assert(new_value->empty());
    ++num_partial_merge_calls;
    return mergeOperator_->PartialMergeMulti(key, operand_list, new_value,
                                             logger);
  }

  const char* Name() const override { return "UInt64AddOperator"; }

 private:
  std::shared_ptr<MergeOperator> mergeOperator_;
};

class EnvMergeTest : public EnvWrapper {
 public:
  EnvMergeTest() : EnvWrapper(Env::Default()) {}
  static const char* kClassName() { return "MergeEnv"; }
  const char* Name() const override { return kClassName(); }
  //  ~EnvMergeTest() override {}

  uint64_t NowNanos() override {
    ++now_nanos_count_;
    return target()->NowNanos();
  }

  static uint64_t now_nanos_count_;

  static std::unique_ptr<EnvMergeTest> singleton_;

  static EnvMergeTest* GetInstance() {
    if (nullptr == singleton_) {
      singleton_.reset(new EnvMergeTest);
    }
    return singleton_.get();
  }
};

uint64_t EnvMergeTest::now_nanos_count_{0};
std::unique_ptr<EnvMergeTest> EnvMergeTest::singleton_;

std::unique_ptr<DB> OpenDb(const std::string& dbname, const bool ttl = false,
                           const size_t max_successive_merges = 0) {
  std::unique_ptr<DB> db;
  Options options;
  options.create_if_missing = true;
  options.merge_operator = std::make_shared<CountMergeOperator>();
  options.max_successive_merges = max_successive_merges;
  options.env = EnvMergeTest::GetInstance();
  EXPECT_OK(DestroyDB(dbname, Options()));
  Status s;
  if (ttl) {
    DBWithTTL* db_with_ttl;
    s = DBWithTTL::Open(options, dbname, &db_with_ttl);
    db.reset(db_with_ttl);
  } else {
    s = DB::Open(options, dbname, &db);
  }
  EXPECT_OK(s);
  assert(s.ok());
  // Allowed to call NowNanos during DB creation (in GenerateRawUniqueId() for
  // session ID)
  EnvMergeTest::now_nanos_count_ = 0;
  return db;
}

// Imagine we are maintaining a set of uint64 counters.
// Each counter has a distinct name. And we would like
// to support four high level operations:
// set, add, get and remove
// This is a quick implementation without a Merge operation.
class Counters {
 protected:
  UnownedPtr<DB> db_;

  WriteOptions put_option_;
  ReadOptions get_option_;
  WriteOptions delete_option_;

  uint64_t default_;

 public:
  explicit Counters(UnownedPtr<DB> db, uint64_t defaultCount = 0)
      : db_(db),
        put_option_(),
        get_option_(),
        delete_option_(),
        default_(defaultCount) {
    assert(db_);
  }

  virtual ~Counters() = default;

  // public interface of Counters.
  // All four functions return false
  // if the underlying level db operation failed.

  // mapped to a levedb Put
  bool set(const std::string& key, uint64_t value) {
    // just treat the internal rep of int64 as the string
    char buf[sizeof(value)];
    EncodeFixed64(buf, value);
    Slice slice(buf, sizeof(value));
    auto s = db_->Put(put_option_, key, slice);

    if (s.ok()) {
      return true;
    } else {
      std::cerr << s.ToString() << std::endl;
      return false;
    }
  }

  // mapped to a rocksdb Delete
  bool remove(const std::string& key) {
    auto s = db_->Delete(delete_option_, key);

    if (s.ok()) {
      return true;
    } else {
      std::cerr << s.ToString() << std::endl;
      return false;
    }
  }

  // mapped to a rocksdb Get
  bool get(const std::string& key, uint64_t* value) {
    std::string str;
    auto s = db_->Get(get_option_, key, &str);

    if (s.IsNotFound()) {
      // return default value if not found;
      *value = default_;
      return true;
    } else if (s.ok()) {
      // deserialization
      if (str.size() != sizeof(uint64_t)) {
        std::cerr << "value corruption\n";
        return false;
      }
      *value = DecodeFixed64(str.data());
      return true;
    } else {
      std::cerr << s.ToString() << std::endl;
      return false;
    }
  }

  // 'add' is implemented as get -> modify -> set
  // An alternative is a single merge operation, see MergeBasedCounters
  virtual bool add(const std::string& key, uint64_t value) {
    uint64_t base = default_;
    return get(key, &base) && set(key, base + value);
  }

  // convenience functions for testing
  void assert_set(const std::string& key, uint64_t value) {
    assert(set(key, value));
  }

  void assert_remove(const std::string& key) { assert(remove(key)); }

  uint64_t assert_get(const std::string& key) {
    uint64_t value = default_;
    int result = get(key, &value);
    assert(result);
    if (result == 0) {
      exit(1);  // Disable unused variable warning.
    }
    return value;
  }

  void assert_add(const std::string& key, uint64_t value) {
    int result = add(key, value);
    assert(result);
    if (result == 0) {
      exit(1);  // Disable unused variable warning.
    }
  }
};

// Implement 'add' directly with the new Merge operation
class MergeBasedCounters : public Counters {
 private:
  WriteOptions merge_option_;  // for merge

 public:
  explicit MergeBasedCounters(UnownedPtr<DB> db, uint64_t defaultCount = 0)
      : Counters(db, defaultCount), merge_option_() {}

  // mapped to a rocksdb Merge operation
  bool add(const std::string& key, uint64_t value) override {
    char encoded[sizeof(uint64_t)];
    EncodeFixed64(encoded, value);
    Slice slice(encoded, sizeof(uint64_t));
    auto s = db_->Merge(merge_option_, key, slice);

    if (s.ok()) {
      return true;
    } else {
      std::cerr << s.ToString() << std::endl;
      return false;
    }
  }
};

void dumpDb(const std::unique_ptr<DB>& db) {
  auto it = std::unique_ptr<Iterator>(db->NewIterator(ReadOptions()));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    // uint64_t value = DecodeFixed64(it->value().data());
    // std::cout << it->key().ToString() << ": " << value << std::endl;
  }
  assert(it->status().ok());  // Check for any errors found during the scan
}

void testCounters(Counters& counters, const std::unique_ptr<DB>& db,
                  bool test_compaction) {
  FlushOptions o;
  o.wait = true;

  counters.assert_set("a", 1);

  if (test_compaction) {
    ASSERT_OK(db->Flush(o));
  }

  ASSERT_EQ(counters.assert_get("a"), 1);

  counters.assert_remove("b");

  // defaut value is 0 if non-existent
  ASSERT_EQ(counters.assert_get("b"), 0);

  counters.assert_add("a", 2);

  if (test_compaction) {
    ASSERT_OK(db->Flush(o));
  }

  // 1+2 = 3
  ASSERT_EQ(counters.assert_get("a"), 3);

  dumpDb(db);

  // 1+...+49 = ?
  uint64_t sum = 0;
  for (int i = 1; i < 50; i++) {
    counters.assert_add("b", i);
    sum += i;
  }
  ASSERT_EQ(counters.assert_get("b"), sum);

  dumpDb(db);

  if (test_compaction) {
    ASSERT_OK(db->Flush(o));

    ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));

    dumpDb(db);

    ASSERT_EQ(counters.assert_get("a"), 3);
    ASSERT_EQ(counters.assert_get("b"), sum);
  }
}

void testCountersWithFlushAndCompaction(Counters& counters,
                                        const std::unique_ptr<DB>& db) {
  ASSERT_OK(db->Put({}, "1", "1"));
  ASSERT_OK(db->Flush(FlushOptions()));

  std::atomic<int> cnt{0};
  const auto get_thread_id = [&cnt]() {
    thread_local int thread_id{cnt++};
    return thread_id;
  };
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:BeforeWriterWaiting", [&](void* /*arg*/) {
        int thread_id = get_thread_id();
        if (1 == thread_id) {
          TEST_SYNC_POINT(
              "testCountersWithFlushAndCompaction::bg_compact_thread:0");
        } else if (2 == thread_id) {
          TEST_SYNC_POINT(
              "testCountersWithFlushAndCompaction::bg_flush_thread:0");
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void* /*arg*/) {
        int thread_id = get_thread_id();
        if (0 == thread_id) {
          TEST_SYNC_POINT(
              "testCountersWithFlushAndCompaction::set_options_thread:0");
          TEST_SYNC_POINT(
              "testCountersWithFlushAndCompaction::set_options_thread:1");
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WakeUpAndDone", [&](void* arg) {
        auto* mutex = static_cast<InstrumentedMutex*>(arg);
        mutex->AssertHeld();
        int thread_id = get_thread_id();
        ASSERT_EQ(2, thread_id);
        mutex->Unlock();
        TEST_SYNC_POINT(
            "testCountersWithFlushAndCompaction::bg_flush_thread:1");
        TEST_SYNC_POINT(
            "testCountersWithFlushAndCompaction::bg_flush_thread:2");
        mutex->Lock();
      });
  SyncPoint::GetInstance()->LoadDependency({
      {"testCountersWithFlushAndCompaction::set_options_thread:0",
       "testCountersWithCompactionAndFlush:BeforeCompact"},
      {"testCountersWithFlushAndCompaction::bg_compact_thread:0",
       "testCountersWithFlushAndCompaction:BeforeIncCounters"},
      {"testCountersWithFlushAndCompaction::bg_flush_thread:0",
       "testCountersWithFlushAndCompaction::set_options_thread:1"},
      {"testCountersWithFlushAndCompaction::bg_flush_thread:1",
       "testCountersWithFlushAndCompaction:BeforeVerification"},
      {"testCountersWithFlushAndCompaction:AfterGet",
       "testCountersWithFlushAndCompaction::bg_flush_thread:2"},
  });
  // This test relies on old behavior of SetOptions writing to the
  // manifest. Here we restore that old behavior for reproducer purposes.
  // (Brief attempts to use an alternative to SetOptions failed.)
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SetOptions:dummy_edit", [&](void* arg) {
        auto* dummy_edit = static_cast<VersionEdit*>(arg);
        dummy_edit->Clear();
      });
  SyncPoint::GetInstance()->EnableProcessing();

  port::Thread set_options_thread([&]() {
    // SetOptions() returns early for a request that changes no option, so set
    // a harmless option to a new value to reach LogAndApply().
    ASSERT_OK(static_cast_with_check<DBImpl>(db.get())->SetOptions(
        {{"report_bg_io_stats", "true"}}));
  });
  TEST_SYNC_POINT("testCountersWithCompactionAndFlush:BeforeCompact");
  port::Thread compact_thread([&]() {
    ASSERT_OK(static_cast_with_check<DBImpl>(db.get())->CompactRange(
        CompactRangeOptions(), db->DefaultColumnFamily(), nullptr, nullptr));
  });

  TEST_SYNC_POINT("testCountersWithFlushAndCompaction:BeforeIncCounters");
  counters.add("test-key", 1);

  FlushOptions flush_opts;
  flush_opts.wait = false;
  ASSERT_OK(db->Flush(flush_opts));

  TEST_SYNC_POINT("testCountersWithFlushAndCompaction:BeforeVerification");
  std::string expected;
  PutFixed64(&expected, 1);
  std::string actual;
  Status s = db->Get(ReadOptions(), "test-key", &actual);
  TEST_SYNC_POINT("testCountersWithFlushAndCompaction:AfterGet");
  set_options_thread.join();
  compact_thread.join();
  ASSERT_OK(s);
  ASSERT_EQ(expected, actual);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

void testSuccessiveMerge(Counters& counters, size_t max_num_merges,
                         size_t num_merges) {
  counters.assert_remove("z");
  uint64_t sum = 0;

  for (size_t i = 1; i <= num_merges; ++i) {
    resetNumMergeOperatorCalls();
    counters.assert_add("z", i);
    sum += i;

    if (i % (max_num_merges + 1) == 0) {
      ASSERT_EQ(num_merge_operator_calls, max_num_merges + 1);
    } else {
      ASSERT_EQ(num_merge_operator_calls, 0);
    }

    resetNumMergeOperatorCalls();
    ASSERT_EQ(counters.assert_get("z"), sum);
    ASSERT_EQ(num_merge_operator_calls, i % (max_num_merges + 1));
  }
}

void testPartialMerge(Counters* counters, const std::unique_ptr<DB>& db,
                      size_t max_merge, size_t min_merge, size_t count) {
  FlushOptions o;
  o.wait = true;

  // Test case 1: partial merge should be called when the number of merge
  //              operands exceeds the threshold.
  uint64_t tmp_sum = 0;
  resetNumPartialMergeCalls();
  for (size_t i = 1; i <= count; i++) {
    counters->assert_add("b", i);
    tmp_sum += i;
  }
  ASSERT_OK(db->Flush(o));
  ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(tmp_sum, counters->assert_get("b"));
  if (count > max_merge) {
    // in this case, FullMerge should be called instead.
    ASSERT_EQ(num_partial_merge_calls, 0U);
  } else {
    // if count >= min_merge, then partial merge should be called once.
    ASSERT_EQ((count >= min_merge), (num_partial_merge_calls == 1));
  }

  // Test case 2: partial merge should not be called when a put is found.
  resetNumPartialMergeCalls();
  tmp_sum = 0;
  ASSERT_OK(db->Put(ROCKSDB_NAMESPACE::WriteOptions(), "c", "10"));
  for (size_t i = 1; i <= count; i++) {
    counters->assert_add("c", i);
    tmp_sum += i;
  }
  ASSERT_OK(db->Flush(o));
  ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(tmp_sum, counters->assert_get("c"));
  ASSERT_EQ(num_partial_merge_calls, 0U);
  // NowNanos was previously called in MergeHelper::FilterMerge(), which
  // harmed performance.
  ASSERT_EQ(EnvMergeTest::now_nanos_count_, 0U);
}

void testSingleBatchSuccessiveMerge(const std::unique_ptr<DB>& db,
                                    size_t max_num_merges, size_t num_merges) {
  ASSERT_GT(num_merges, max_num_merges);

  Slice key("BatchSuccessiveMerge");
  uint64_t merge_value = 1;
  char buf[sizeof(merge_value)];
  EncodeFixed64(buf, merge_value);
  Slice merge_value_slice(buf, sizeof(merge_value));

  // Create the batch
  WriteBatch batch;
  for (size_t i = 0; i < num_merges; ++i) {
    ASSERT_OK(batch.Merge(key, merge_value_slice));
  }

  // Apply to memtable and count the number of merges
  resetNumMergeOperatorCalls();
  ASSERT_OK(db->Write(WriteOptions(), &batch));
  ASSERT_EQ(
      num_merge_operator_calls,
      static_cast<size_t>(num_merges - (num_merges % (max_num_merges + 1))));

  // Get the value
  resetNumMergeOperatorCalls();
  std::string get_value_str;
  ASSERT_OK(db->Get(ReadOptions(), key, &get_value_str));
  assert(get_value_str.size() == sizeof(uint64_t));
  uint64_t get_value = DecodeFixed64(get_value_str.data());
  ASSERT_EQ(get_value, num_merges * merge_value);
  ASSERT_EQ(num_merge_operator_calls,
            static_cast<size_t>((num_merges % (max_num_merges + 1))));
}

void runTest(const std::string& dbname, const bool use_ttl = false) {
  {
    auto db = OpenDb(dbname, use_ttl);

    {
      Counters counters(db.get(), 0);
      testCounters(counters, db, true);
    }

    {
      MergeBasedCounters counters(db.get(), 0);
      testCounters(counters, db, use_compression);
    }
  }

  ASSERT_OK(DestroyDB(dbname, Options()));

  {
    size_t max_merge = 5;
    auto db = OpenDb(dbname, use_ttl, max_merge);
    MergeBasedCounters counters(db.get(), 0);
    testCounters(counters, db, use_compression);
    testSuccessiveMerge(counters, max_merge, max_merge * 2);
    testSingleBatchSuccessiveMerge(db, 5, 7);
    ASSERT_OK(db->Close());
    ASSERT_OK(DestroyDB(dbname, Options()));
  }

  {
    size_t max_merge = 100;
    // Min merge is hard-coded to 2.
    uint32_t min_merge = 2;
    for (uint32_t count = min_merge - 1; count <= min_merge + 1; count++) {
      auto db = OpenDb(dbname, use_ttl, max_merge);
      MergeBasedCounters counters(db.get(), 0);
      testPartialMerge(&counters, db, max_merge, min_merge, count);
      ASSERT_OK(db->Close());
      ASSERT_OK(DestroyDB(dbname, Options()));
    }
    {
      auto db = OpenDb(dbname, use_ttl, max_merge);
      MergeBasedCounters counters(db.get(), 0);
      testPartialMerge(&counters, db, max_merge, min_merge, min_merge * 10);
      ASSERT_OK(db->Close());
      ASSERT_OK(DestroyDB(dbname, Options()));
    }
  }

  {
    {
      auto db = OpenDb(dbname);
      MergeBasedCounters counters(db.get(), 0);
      counters.add("test-key", 1);
      counters.add("test-key", 1);
      counters.add("test-key", 1);
      ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    }

    std::unique_ptr<DB> reopen_db;
    ASSERT_OK(DB::Open(Options(), dbname, &reopen_db));
    std::string value;
    ASSERT_NOK(reopen_db->Get(ReadOptions(), "test-key", &value));
    reopen_db.reset();
    ASSERT_OK(DestroyDB(dbname, Options()));
  }

  /* Temporary remove this test
  {
    std::cout << "Test merge-operator not set after reopen (recovery case)\n";
    {
      auto db = OpenDb(dbname);
      MergeBasedCounters counters(db.get(), 0);
      counters.add("test-key", 1);
      counters.add("test-key", 1);
      counters.add("test-key", 1);
    }

    std::unique_ptr<DB> reopen_db;
    ASSERT_TRUE(DB::Open(Options(), dbname, &reopen_db).IsInvalidArgument());
  }
  */
}

TEST_F(MergeTest, MergeDbTest) {
  runTest(test::PerThreadDBPath("merge_testdb"));
}

TEST_F(MergeTest, MergeDbTtlTest) {
  runTest(test::PerThreadDBPath("merge_testdbttl"),
          true);  // Run test on TTL database
}

TEST_F(MergeTest, MergeWithCompactionAndFlush) {
  const std::string dbname =
      test::PerThreadDBPath("merge_with_compaction_and_flush");
  {
    auto db = OpenDb(dbname);
    {
      MergeBasedCounters counters(db.get(), 0);
      testCountersWithFlushAndCompaction(counters, db);
    }
  }
  ASSERT_OK(DestroyDB(dbname, Options()));
}

TEST_F(MergeTest, FullMergeV3FallbackNewValue) {
  // Test that the default FullMergeV3 implementation correctly handles the case
  // when FullMergeV2 results in a new value.

  const Slice key("foo");
  const MergeOperator::MergeOperationInputV3::OperandList operands{
      "first", "second", "third"};
  constexpr Logger* logger = nullptr;

  auto append_operator =
      MergeOperators::CreateStringAppendOperator(std::string());

  // No existing value
  {
    MergeOperator::MergeOperationInputV3::ExistingValue existing_value;
    const MergeOperator::MergeOperationInputV3 merge_in(
        key, std::move(existing_value), operands, logger);

    MergeOperator::MergeOperationOutputV3 merge_out;

    ASSERT_TRUE(append_operator->FullMergeV3(merge_in, &merge_out));

    const auto& result = std::get<std::string>(merge_out.new_value);
    ASSERT_EQ(result, operands[0].ToString() + operands[1].ToString() +
                          operands[2].ToString());
  }

  // Plain existing value
  {
    const Slice plain("plain");
    MergeOperator::MergeOperationInputV3::ExistingValue existing_value(plain);
    const MergeOperator::MergeOperationInputV3 merge_in(
        key, std::move(existing_value), operands, logger);

    MergeOperator::MergeOperationOutputV3 merge_out;

    ASSERT_TRUE(append_operator->FullMergeV3(merge_in, &merge_out));

    const auto& result = std::get<std::string>(merge_out.new_value);
    ASSERT_EQ(result, plain.ToString() + operands[0].ToString() +
                          operands[1].ToString() + operands[2].ToString());
  }

  // Wide-column existing value with default column
  {
    const WideColumns entity{
        {kDefaultWideColumnName, "default"}, {"one", "1"}, {"two", "2"}};
    MergeOperator::MergeOperationInputV3::ExistingValue existing_value(entity);
    const MergeOperator::MergeOperationInputV3 merge_in(
        key, std::move(existing_value), operands, logger);

    MergeOperator::MergeOperationOutputV3 merge_out;

    ASSERT_TRUE(append_operator->FullMergeV3(merge_in, &merge_out));

    const auto& result =
        std::get<MergeOperator::MergeOperationOutputV3::NewColumns>(
            merge_out.new_value);
    ASSERT_EQ(result.size(), entity.size());
    ASSERT_EQ(result[0].first, entity[0].name());
    ASSERT_EQ(result[0].second,
              entity[0].value().ToString() + operands[0].ToString() +
                  operands[1].ToString() + operands[2].ToString());
    ASSERT_EQ(result[1].first, entity[1].name());
    ASSERT_EQ(result[1].second, entity[1].value());
    ASSERT_EQ(result[2].first, entity[2].name());
    ASSERT_EQ(result[2].second, entity[2].value());
  }

  // Wide-column existing value without default column
  {
    const WideColumns entity{{"one", "1"}, {"two", "2"}};
    MergeOperator::MergeOperationInputV3::ExistingValue existing_value(entity);
    const MergeOperator::MergeOperationInputV3 merge_in(
        key, std::move(existing_value), operands, logger);

    MergeOperator::MergeOperationOutputV3 merge_out;

    ASSERT_TRUE(append_operator->FullMergeV3(merge_in, &merge_out));

    const auto& result =
        std::get<MergeOperator::MergeOperationOutputV3::NewColumns>(
            merge_out.new_value);
    ASSERT_EQ(result.size(), entity.size() + 1);
    ASSERT_EQ(result[0].first, kDefaultWideColumnName);
    ASSERT_EQ(result[0].second, operands[0].ToString() +
                                    operands[1].ToString() +
                                    operands[2].ToString());
    ASSERT_EQ(result[1].first, entity[0].name());
    ASSERT_EQ(result[1].second, entity[0].value());
    ASSERT_EQ(result[2].first, entity[1].name());
    ASSERT_EQ(result[2].second, entity[1].value());
  }
}

TEST_F(MergeTest, FullMergeV3FallbackExistingOperand) {
  // Test that the default FullMergeV3 implementation correctly handles the case
  // when FullMergeV2 results in an existing operand.

  const Slice key("foo");
  const MergeOperator::MergeOperationInputV3::OperandList operands{
      "first", "second", "third"};
  constexpr Logger* logger = nullptr;

  auto put_operator = MergeOperators::CreatePutOperator();

  // No existing value
  {
    MergeOperator::MergeOperationInputV3::ExistingValue existing_value;
    const MergeOperator::MergeOperationInputV3 merge_in(
        key, std::move(existing_value), operands, logger);

    MergeOperator::MergeOperationOutputV3 merge_out;

    ASSERT_TRUE(put_operator->FullMergeV3(merge_in, &merge_out));

    const auto& result = std::get<Slice>(merge_out.new_value);
    ASSERT_EQ(result.data(), operands.back().data());
    ASSERT_EQ(result.size(), operands.back().size());
  }

  // Plain existing value
  {
    const Slice plain("plain");
    MergeOperator::MergeOperationInputV3::ExistingValue existing_value(plain);
    const MergeOperator::MergeOperationInputV3 merge_in(
        key, std::move(existing_value), operands, logger);

    MergeOperator::MergeOperationOutputV3 merge_out;

    ASSERT_TRUE(put_operator->FullMergeV3(merge_in, &merge_out));

    const auto& result = std::get<Slice>(merge_out.new_value);
    ASSERT_EQ(result.data(), operands.back().data());
    ASSERT_EQ(result.size(), operands.back().size());
  }

  // Wide-column existing value with default column
  {
    const WideColumns entity{
        {kDefaultWideColumnName, "default"}, {"one", "1"}, {"two", "2"}};
    MergeOperator::MergeOperationInputV3::ExistingValue existing_value(entity);
    const MergeOperator::MergeOperationInputV3 merge_in(
        key, std::move(existing_value), operands, logger);

    MergeOperator::MergeOperationOutputV3 merge_out;

    ASSERT_TRUE(put_operator->FullMergeV3(merge_in, &merge_out));

    const auto& result =
        std::get<MergeOperator::MergeOperationOutputV3::NewColumns>(
            merge_out.new_value);
    ASSERT_EQ(result.size(), entity.size());
    ASSERT_EQ(result[0].first, entity[0].name());
    ASSERT_EQ(result[0].second, operands.back());
    ASSERT_EQ(result[1].first, entity[1].name());
    ASSERT_EQ(result[1].second, entity[1].value());
    ASSERT_EQ(result[2].first, entity[2].name());
    ASSERT_EQ(result[2].second, entity[2].value());
  }

  // Wide-column existing value without default column
  {
    const WideColumns entity{{"one", "1"}, {"two", "2"}};
    MergeOperator::MergeOperationInputV3::ExistingValue existing_value(entity);
    const MergeOperator::MergeOperationInputV3 merge_in(
        key, std::move(existing_value), operands, logger);

    MergeOperator::MergeOperationOutputV3 merge_out;

    ASSERT_TRUE(put_operator->FullMergeV3(merge_in, &merge_out));

    const auto& result =
        std::get<MergeOperator::MergeOperationOutputV3::NewColumns>(
            merge_out.new_value);
    ASSERT_EQ(result.size(), entity.size() + 1);
    ASSERT_EQ(result[0].first, kDefaultWideColumnName);
    ASSERT_EQ(result[0].second, operands.back());
    ASSERT_EQ(result[1].first, entity[0].name());
    ASSERT_EQ(result[1].second, entity[0].value());
    ASSERT_EQ(result[2].first, entity[1].name());
    ASSERT_EQ(result[2].second, entity[1].value());
  }
}

TEST_F(MergeTest, FullMergeV3FallbackFailure) {
  // Test that the default FullMergeV3 implementation correctly handles the case
  // when FullMergeV2 fails.

  const Slice key("foo");
  const MergeOperator::MergeOperationInputV3::OperandList operands{
      "first", "second", "third"};
  constexpr Logger* logger = nullptr;

  class FailMergeOperator : public MergeOperator {
   public:
    bool FullMergeV2(const MergeOperationInput& /* merge_in */,
                     MergeOperationOutput* merge_out) const override {
      assert(merge_out);
      merge_out->op_failure_scope = OpFailureScope::kMustMerge;

      return false;
    }

    const char* Name() const override { return "FailMergeOperator"; }
  };

  FailMergeOperator fail_operator;

  // No existing value
  {
    MergeOperator::MergeOperationInputV3::ExistingValue existing_value;
    const MergeOperator::MergeOperationInputV3 merge_in(
        key, std::move(existing_value), operands, logger);

    MergeOperator::MergeOperationOutputV3 merge_out;

    ASSERT_FALSE(fail_operator.FullMergeV3(merge_in, &merge_out));
    ASSERT_EQ(merge_out.op_failure_scope,
              MergeOperator::OpFailureScope::kMustMerge);
  }

  // Plain existing value
  {
    const Slice plain("plain");
    MergeOperator::MergeOperationInputV3::ExistingValue existing_value(plain);
    const MergeOperator::MergeOperationInputV3 merge_in(
        key, std::move(existing_value), operands, logger);

    MergeOperator::MergeOperationOutputV3 merge_out;

    ASSERT_FALSE(fail_operator.FullMergeV3(merge_in, &merge_out));
    ASSERT_EQ(merge_out.op_failure_scope,
              MergeOperator::OpFailureScope::kMustMerge);
  }

  // Wide-column existing value with default column
  {
    const WideColumns entity{
        {kDefaultWideColumnName, "default"}, {"one", "1"}, {"two", "2"}};
    MergeOperator::MergeOperationInputV3::ExistingValue existing_value(entity);
    const MergeOperator::MergeOperationInputV3 merge_in(
        key, std::move(existing_value), operands, logger);

    MergeOperator::MergeOperationOutputV3 merge_out;

    ASSERT_FALSE(fail_operator.FullMergeV3(merge_in, &merge_out));
    ASSERT_EQ(merge_out.op_failure_scope,
              MergeOperator::OpFailureScope::kMustMerge);
  }

  // Wide-column existing value without default column
  {
    const WideColumns entity{{"one", "1"}, {"two", "2"}};
    MergeOperator::MergeOperationInputV3::ExistingValue existing_value(entity);
    const MergeOperator::MergeOperationInputV3 merge_in(
        key, std::move(existing_value), operands, logger);

    MergeOperator::MergeOperationOutputV3 merge_out;

    ASSERT_FALSE(fail_operator.FullMergeV3(merge_in, &merge_out));
    ASSERT_EQ(merge_out.op_failure_scope,
              MergeOperator::OpFailureScope::kMustMerge);
  }
}

// Uses anonymous mmap (lazy-zeroed) so the large operands don't consume
// physical memory -- only the merge result does (~4GB peak).
TEST_F(MergeTest, LargeMergeResultRejected) {
  if (!test::HasBigMem()) {
    ROCKSDB_GTEST_BYPASS("insufficient memory for reliable continuous testing");
    return;
  }

  // A merge operator that produces a result > 4GB by concatenating operands
  class ConcatOperator : public MergeOperator {
   public:
    bool FullMergeV2(const MergeOperationInput& merge_in,
                     MergeOperationOutput* merge_out) const override {
      merge_out->new_value.clear();
      if (merge_in.existing_value) {
        merge_out->new_value.append(merge_in.existing_value->data(),
                                    merge_in.existing_value->size());
      }
      for (const auto& op : merge_in.operand_list) {
        merge_out->new_value.append(op.data(), op.size());
      }
      return true;
    }
    const char* Name() const override { return "ConcatOperator"; }
  };

  // Use TimedFullMerge directly - two operands that sum to > 4GB
  constexpr size_t kHalfSize =
      (size_t{std::numeric_limits<uint32_t>::max()} + 1) / 2;
  MemMapping mm1 = MemMapping::AllocateLazyZeroed(kHalfSize);
  ASSERT_NE(nullptr, mm1.Get());
  MemMapping mm2 = MemMapping::AllocateLazyZeroed(kHalfSize);
  ASSERT_NE(nullptr, mm2.Get());
  std::vector<Slice> operands{mm1.AsSlice(), mm2.AsSlice()};
  std::string result;
  Slice result_operand;
  ValueType result_type;

  ConcatOperator concat_op;
  Status s = MergeHelper::TimedFullMerge(
      &concat_op, "key", MergeHelper::kNoBaseValue, operands, nullptr, nullptr,
      SystemClock::Default().get(), false, nullptr, &result, &result_operand,
      &result_type);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(s.ToString().find("4GB") != std::string::npos);

  // Control: just under 4GB should be accepted
  result.clear();
  result.shrink_to_fit();
  operands = {mm1.AsSlice(), Slice(mm2.AsSlice().data(), kHalfSize - 1)};
  s = MergeHelper::TimedFullMerge(&concat_op, "key", MergeHelper::kNoBaseValue,
                                  operands, nullptr, nullptr,
                                  SystemClock::Default().get(), false, nullptr,
                                  &result, &result_operand, &result_type);
  ASSERT_OK(s);
  ASSERT_EQ(result.size(), size_t{std::numeric_limits<uint32_t>::max()});
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::use_compression = false;
  if (argc > 1) {
    ROCKSDB_NAMESPACE::use_compression = true;
  }

  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
