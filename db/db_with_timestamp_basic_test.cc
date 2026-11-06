//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_with_timestamp_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/utilities/debug.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_builder.h"
#include "test_util/sync_point.h"
#include "test_util/testutil.h"
#include "utilities/fault_injection_env.h"
#include "utilities/merge_operators/string_append/stringappend2.h"

namespace ROCKSDB_NAMESPACE {
namespace {
std::string EncodeAsUint64(uint64_t v) {
  std::string dst;
  PutFixed64(&dst, v);
  return dst;
}
}  // namespace
class DBBasicTestWithTimestamp : public DBBasicTestWithTimestampBase {
 public:
  DBBasicTestWithTimestamp()
      : DBBasicTestWithTimestampBase("db_basic_test_with_timestamp") {}
};

TEST_F(DBBasicTestWithTimestamp, GetWithMetadataTimestampAndNewerVersion) {
  Options options = CurrentOptions();
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  DestroyAndReopen(options);

  const std::string old_timestamp = EncodeAsUint64(1);
  const std::string new_timestamp = EncodeAsUint64(2);
  ASSERT_OK(db_->Put(WriteOptions(), "key", old_timestamp, "old"));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->Put(WriteOptions(), "key", new_timestamp, "new"));
  ASSERT_OK(Flush());

  Slice read_timestamp(new_timestamp);
  ReadOptions read_options;
  read_options.snapshot = snapshot;
  read_options.timestamp = &read_timestamp;

  OutputMetadata output_metadata;
  output_metadata.WantTimestamp().WantNewerVersionPresent();
  std::string value;
  ASSERT_OK(
      db_->GetWithMetadata(read_options, "key", &value, &output_metadata));
  ASSERT_EQ("old", value);
  ASSERT_EQ(old_timestamp, *output_metadata.timestamp);
  ASSERT_TRUE(*output_metadata.newer_version_present);

  MultiGetOutputMetadata multiget_output_metadata;
  multiget_output_metadata.WantTimestamps().WantNewerVersionPresent();
  std::vector<Slice> keys{"key"};
  std::vector<std::string> values;
  const std::vector<Status> statuses = db_->MultiGetWithMetadata(
      read_options, keys, &values, &multiget_output_metadata);
  ASSERT_OK(statuses[0]);
  ASSERT_EQ("old", values[0]);
  ASSERT_EQ(old_timestamp, (*multiget_output_metadata.timestamps)[0]);
  ASSERT_TRUE((*multiget_output_metadata.newer_version_present)[0]);

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBBasicTestWithTimestamp, SanityChecks) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.avoid_flush_during_shutdown = true;
  options.merge_operator = MergeOperators::CreateStringAppendTESTOperator();
  DestroyAndReopen(options);

  Options options1 = CurrentOptions();
  options1.env = env_;
  options1.comparator = test::BytewiseComparatorWithU64TsWrapper();
  options1.merge_operator = MergeOperators::CreateStringAppendTESTOperator();
  assert(options1.comparator &&
         options1.comparator->timestamp_size() == sizeof(uint64_t));
  ColumnFamilyHandle* handle = nullptr;
  Status s = db_->CreateColumnFamily(options1, "data", &handle);
  ASSERT_OK(s);

  std::string dummy_ts(sizeof(uint64_t), '\0');
  // Perform timestamp operations on default cf.
  ASSERT_TRUE(
      db_->Put(WriteOptions(), "key", dummy_ts, "value").IsInvalidArgument());
  ASSERT_TRUE(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), "key",
                         dummy_ts, "value")
                  .IsInvalidArgument());
  ASSERT_TRUE(db_->Delete(WriteOptions(), "key", dummy_ts).IsInvalidArgument());
  ASSERT_TRUE(
      db_->SingleDelete(WriteOptions(), "key", dummy_ts).IsInvalidArgument());
  ASSERT_TRUE(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               "begin_key", "end_key", dummy_ts)
                  .IsInvalidArgument());

  // Perform non-timestamp operations on "data" cf.
  ASSERT_TRUE(
      db_->Put(WriteOptions(), handle, "key", "value").IsInvalidArgument());
  ASSERT_TRUE(db_->Delete(WriteOptions(), handle, "key").IsInvalidArgument());
  ASSERT_TRUE(
      db_->SingleDelete(WriteOptions(), handle, "key").IsInvalidArgument());

  ASSERT_TRUE(
      db_->Merge(WriteOptions(), handle, "key", "value").IsInvalidArgument());
  ASSERT_TRUE(db_->DeleteRange(WriteOptions(), handle, "begin_key", "end_key")
                  .IsInvalidArgument());

  {
    WriteBatch wb;
    ASSERT_OK(wb.Put(handle, "key", "value"));
    ASSERT_TRUE(db_->Write(WriteOptions(), &wb).IsInvalidArgument());
  }
  {
    WriteBatch wb;
    ASSERT_OK(wb.Delete(handle, "key"));
    ASSERT_TRUE(db_->Write(WriteOptions(), &wb).IsInvalidArgument());
  }
  {
    WriteBatch wb;
    ASSERT_OK(wb.SingleDelete(handle, "key"));
    ASSERT_TRUE(db_->Write(WriteOptions(), &wb).IsInvalidArgument());
  }
  {
    WriteBatch wb;
    ASSERT_OK(wb.DeleteRange(handle, "begin_key", "end_key"));
    ASSERT_TRUE(db_->Write(WriteOptions(), &wb).IsInvalidArgument());
  }

  // Perform timestamp operations with timestamps of incorrect size.
  const std::string wrong_ts(sizeof(uint32_t), '\0');
  ASSERT_TRUE(db_->Put(WriteOptions(), handle, "key", wrong_ts, "value")
                  .IsInvalidArgument());
  ASSERT_TRUE(db_->Merge(WriteOptions(), handle, "key", wrong_ts, "value")
                  .IsInvalidArgument());
  ASSERT_TRUE(
      db_->Delete(WriteOptions(), handle, "key", wrong_ts).IsInvalidArgument());
  ASSERT_TRUE(db_->SingleDelete(WriteOptions(), handle, "key", wrong_ts)
                  .IsInvalidArgument());
  ASSERT_TRUE(
      db_->DeleteRange(WriteOptions(), handle, "begin_key", "end_key", wrong_ts)
          .IsInvalidArgument());

  delete handle;
}

TEST_F(DBBasicTestWithTimestamp, MixedCfs) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.avoid_flush_during_shutdown = true;
  DestroyAndReopen(options);

  Options options1 = CurrentOptions();
  options1.env = env_;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options1.comparator = &test_cmp;
  ColumnFamilyHandle* handle = nullptr;
  Status s = db_->CreateColumnFamily(options1, "data", &handle);
  ASSERT_OK(s);

  WriteBatch wb;
  ASSERT_OK(wb.Put("a", "value"));
  ASSERT_OK(wb.Put(handle, "a", "value"));
  {
    std::string ts = Timestamp(1, 0);
    const auto ts_sz_func = [kTimestampSize, handle](uint32_t cf_id) {
      assert(handle);
      if (cf_id == 0) {
        return static_cast<size_t>(0);
      } else if (cf_id == handle->GetID()) {
        return kTimestampSize;
      } else {
        assert(false);
        return std::numeric_limits<size_t>::max();
      }
    };
    ASSERT_OK(wb.UpdateTimestamps(ts, ts_sz_func));
    ASSERT_OK(db_->Write(WriteOptions(), &wb));
  }

  const auto verify_db = [this](ColumnFamilyHandle* h, const std::string& key,
                                const std::string& ts,
                                const std::string& expected_value) {
    ASSERT_EQ(expected_value, Get(key));
    Slice read_ts_slice(ts);
    ReadOptions read_opts;
    read_opts.timestamp = &read_ts_slice;
    std::string value;
    ASSERT_OK(db_->Get(read_opts, h, key, &value));
    ASSERT_EQ(expected_value, value);
  };

  verify_db(handle, "a", Timestamp(1, 0), "value");

  delete handle;
  Close();

  std::vector<ColumnFamilyDescriptor> cf_descs;
  cf_descs.emplace_back(kDefaultColumnFamilyName, options);
  cf_descs.emplace_back("data", options1);
  options.create_if_missing = false;
  s = DB::Open(options, dbname_, cf_descs, &handles_, &db_);
  ASSERT_OK(s);

  verify_db(handles_[1], "a", Timestamp(1, 0), "value");

  Close();
}

TEST_F(DBBasicTestWithTimestamp, MultiGetMultipleCfs) {
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.avoid_flush_during_shutdown = true;
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  Options options1 = CurrentOptions();
  options1.env = env_;
  options1.comparator = &test_cmp;
  ColumnFamilyHandle* handle = nullptr;
  Status s = db_->CreateColumnFamily(options1, "data", &handle);
  ASSERT_OK(s);

  std::string ts = Timestamp(1, 0);
  WriteBatch wb(0, 0, 0, kTimestampSize);
  ASSERT_OK(wb.Put("a", "value"));
  ASSERT_OK(wb.Put(handle, "a", "value"));
  const auto ts_sz_func = [kTimestampSize](uint32_t /*cf_id*/) {
    return kTimestampSize;
  };
  ASSERT_OK(wb.UpdateTimestamps(ts, ts_sz_func));
  ASSERT_OK(db_->Write(WriteOptions(), &wb));

  int num_keys = 2;
  std::vector<Slice> keys;
  std::vector<std::string> expected_values;
  for (int i = 0; i < num_keys; i++) {
    keys.push_back("a");
    expected_values.push_back("value");
  }
  std::vector<ColumnFamilyHandle*> handles;
  handles.push_back(db_->DefaultColumnFamily());
  handles.push_back(handle);

  {
    Slice read_ts_slice(ts);
    ReadOptions read_opts;
    read_opts.timestamp = &read_ts_slice;

    std::vector<PinnableSlice> values;
    values.resize(num_keys);
    std::vector<Status> statuses;
    statuses.resize(num_keys);
    std::vector<std::string> timestamps;
    timestamps.resize(num_keys);

    db_->MultiGet(read_opts, num_keys, handles.data(), keys.data(),
                  values.data(), timestamps.data(), statuses.data());

    for (int i = 0; i < num_keys; i++) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(expected_values[i], values[i].ToString());
      ASSERT_EQ(ts, timestamps[i]);
    }
  }

  delete handle;
  Close();
}

TEST_F(DBBasicTestWithTimestamp, CompactRangeWithSpecifiedRange) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  WriteOptions write_opts;
  std::string ts = Timestamp(1, 0);

  ASSERT_OK(db_->Put(write_opts, "foo1", ts, "bar"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->Put(write_opts, "foo2", ts, "bar"));
  ASSERT_OK(Flush());

  std::string start_str = "foo";
  std::string end_str = "foo2";
  Slice start(start_str);
  Slice end(end_str);
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, &end));

  Close();
}

TEST_F(DBBasicTestWithTimestamp, GcPreserveLatestVersionBelowFullHistoryLow) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  std::string ts_str = Timestamp(1, 0);
  WriteOptions wopts;
  ASSERT_OK(db_->Put(wopts, "k1", ts_str, "v1"));
  ASSERT_OK(db_->Put(wopts, "k2", ts_str, "v2"));
  ASSERT_OK(db_->Put(wopts, "k3", ts_str, "v3"));

  ts_str = Timestamp(2, 0);
  ASSERT_OK(db_->Delete(wopts, "k3", ts_str));

  ts_str = Timestamp(4, 0);
  ASSERT_OK(db_->Put(wopts, "k1", ts_str, "v5"));

  ts_str = Timestamp(5, 0);
  ASSERT_OK(
      db_->DeleteRange(wopts, db_->DefaultColumnFamily(), "k0", "k9", ts_str));

  ts_str = Timestamp(3, 0);
  Slice ts = ts_str;
  CompactRangeOptions cro;
  cro.full_history_ts_low = &ts;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  ASSERT_OK(Flush());

  ReadOptions ropts;
  ropts.timestamp = &ts;
  std::string value;
  Status s = db_->Get(ropts, "k1", &value);
  ASSERT_OK(s);
  ASSERT_EQ("v1", value);

  std::string key_ts;
  ASSERT_TRUE(db_->Get(ropts, "k3", &value, &key_ts).IsNotFound());
  ASSERT_EQ(Timestamp(2, 0), key_ts);

  ts_str = Timestamp(5, 0);
  ts = ts_str;
  ropts.timestamp = &ts;
  ASSERT_TRUE(db_->Get(ropts, "k2", &value, &key_ts).IsNotFound());
  ASSERT_EQ(Timestamp(5, 0), key_ts);
  ASSERT_TRUE(db_->Get(ropts, "k2", &value).IsNotFound());

  Close();
}

TEST_F(DBBasicTestWithTimestamp, UpdateFullHistoryTsLow) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  const std::string kKey = "test kKey";

  // Test set ts_low first and flush()
  int current_ts_low = 5;
  std::string ts_low_str = Timestamp(current_ts_low, 0);
  Slice ts_low = ts_low_str;
  CompactRangeOptions comp_opts;
  comp_opts.full_history_ts_low = &ts_low;
  comp_opts.bottommost_level_compaction = BottommostLevelCompaction::kForce;

  ASSERT_OK(db_->CompactRange(comp_opts, nullptr, nullptr));

  auto* cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(db_->DefaultColumnFamily())
          ->cfd();
  auto result_ts_low = cfd->GetFullHistoryTsLow();

  ASSERT_TRUE(test_cmp.CompareTimestamp(ts_low, result_ts_low) == 0);

  for (int i = 0; i < 10; i++) {
    WriteOptions write_opts;
    std::string ts = Timestamp(i, 0);
    ASSERT_OK(db_->Put(write_opts, kKey, ts, Key(i)));
  }
  ASSERT_OK(Flush());

  for (int i = 0; i < 10; i++) {
    ReadOptions read_opts;
    std::string ts_str = Timestamp(i, 0);
    Slice ts = ts_str;
    read_opts.timestamp = &ts;
    std::string value;
    Status status = db_->Get(read_opts, kKey, &value);
    if (i < current_ts_low) {
      ASSERT_TRUE(status.IsInvalidArgument());
    } else {
      ASSERT_OK(status);
      ASSERT_TRUE(value.compare(Key(i)) == 0);
    }
  }

  // Test set ts_low and then trigger compaction
  for (int i = 10; i < 20; i++) {
    WriteOptions write_opts;
    std::string ts = Timestamp(i, 0);
    ASSERT_OK(db_->Put(write_opts, kKey, ts, Key(i)));
  }

  ASSERT_OK(Flush());

  current_ts_low = 15;
  ts_low_str = Timestamp(current_ts_low, 0);
  ts_low = ts_low_str;
  comp_opts.full_history_ts_low = &ts_low;
  ASSERT_OK(db_->CompactRange(comp_opts, nullptr, nullptr));
  result_ts_low = cfd->GetFullHistoryTsLow();
  ASSERT_TRUE(test_cmp.CompareTimestamp(ts_low, result_ts_low) == 0);

  for (int i = current_ts_low; i < 20; i++) {
    ReadOptions read_opts;
    std::string ts_str = Timestamp(i, 0);
    Slice ts = ts_str;
    read_opts.timestamp = &ts;
    std::string value;
    Status status = db_->Get(read_opts, kKey, &value);
    ASSERT_OK(status);
    ASSERT_TRUE(value.compare(Key(i)) == 0);
  }

  // Test invalid compaction with range
  Slice start(kKey);
  Slice end(kKey);
  Status s = db_->CompactRange(comp_opts, &start, &end);
  ASSERT_TRUE(s.IsInvalidArgument());
  s = db_->CompactRange(comp_opts, &start, nullptr);
  ASSERT_TRUE(s.IsInvalidArgument());
  s = db_->CompactRange(comp_opts, nullptr, &end);
  ASSERT_TRUE(s.IsInvalidArgument());

  // Test invalid compaction with the decreasing ts_low
  ts_low_str = Timestamp(current_ts_low - 1, 0);
  ts_low = ts_low_str;
  comp_opts.full_history_ts_low = &ts_low;
  s = db_->CompactRange(comp_opts, nullptr, nullptr);
  ASSERT_TRUE(s.IsInvalidArgument());

  Close();
}

TEST_F(DBBasicTestWithTimestamp, UpdateFullHistoryTsLowWithPublicAPI) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  std::string ts_low_str = Timestamp(9, 0);
  ASSERT_OK(
      db_->IncreaseFullHistoryTsLow(db_->DefaultColumnFamily(), ts_low_str));
  std::string result_ts_low;
  ASSERT_OK(db_->GetFullHistoryTsLow(nullptr, &result_ts_low));
  ASSERT_TRUE(test_cmp.CompareTimestamp(ts_low_str, result_ts_low) == 0);
  // test increase full_history_low backward
  std::string ts_low_str_back = Timestamp(8, 0);
  auto s = db_->IncreaseFullHistoryTsLow(db_->DefaultColumnFamily(),
                                         ts_low_str_back);
  ASSERT_EQ(s, Status::InvalidArgument());
  // test IncreaseFullHistoryTsLow with a timestamp whose length is longger
  // than the cf's timestamp size
  std::string ts_low_str_long(Timestamp(0, 0).size() + 1, 'a');
  s = db_->IncreaseFullHistoryTsLow(db_->DefaultColumnFamily(),
                                    ts_low_str_long);
  ASSERT_EQ(s, Status::InvalidArgument());
  // test IncreaseFullHistoryTsLow with a timestamp which is null
  std::string ts_low_str_null;
  s = db_->IncreaseFullHistoryTsLow(db_->DefaultColumnFamily(),
                                    ts_low_str_null);
  ASSERT_EQ(s, Status::InvalidArgument());
  // test IncreaseFullHistoryTsLow for a column family that does not enable
  // timestamp
  options.comparator = BytewiseComparator();
  DestroyAndReopen(options);
  ts_low_str = Timestamp(10, 0);
  s = db_->IncreaseFullHistoryTsLow(db_->DefaultColumnFamily(), ts_low_str);
  ASSERT_EQ(s, Status::InvalidArgument());
  // test GetFullHistoryTsLow for a column family that does not enable
  // timestamp
  std::string current_ts_low;
  s = db_->GetFullHistoryTsLow(db_->DefaultColumnFamily(), &current_ts_low);
  ASSERT_EQ(s, Status::InvalidArgument());
  Close();
}

TEST_F(DBBasicTestWithTimestamp, GetApproximateSizes) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100000000;  // Large write buffer
  options.compression = kNoCompression;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  auto default_cf = db_->DefaultColumnFamily();

  WriteOptions write_opts;
  std::string ts = Timestamp(1, 0);

  const int N = 128;
  Random rnd(301);
  for (int i = 0; i < N; i++) {
    ASSERT_OK(db_->Put(write_opts, Key(i), ts, rnd.RandomString(1024)));
  }

  uint64_t size;
  std::string start = Key(50);
  std::string end = Key(60);
  Range r(start, end);
  SizeApproximationOptions size_approx_options;
  size_approx_options.include_memtables = true;
  size_approx_options.include_files = true;
  ASSERT_OK(
      db_->GetApproximateSizes(size_approx_options, default_cf, &r, 1, &size));
  ASSERT_GT(size, 6000);
  ASSERT_LT(size, 204800);

  // test multiple ranges
  std::vector<Range> ranges;
  std::string start_tmp = Key(10);
  std::string end_tmp = Key(20);
  ranges.emplace_back(start_tmp, end_tmp);
  ranges.emplace_back(start, end);
  uint64_t range_sizes[2];
  ASSERT_OK(db_->GetApproximateSizes(size_approx_options, default_cf,
                                     ranges.data(), 2, range_sizes));

  ASSERT_EQ(range_sizes[1], size);

  // Zero if not including mem table
  ASSERT_OK(db_->GetApproximateSizes(&r, 1, &size));
  ASSERT_EQ(size, 0);

  start = Key(500);
  end = Key(600);
  r = Range(start, end);
  ASSERT_OK(
      db_->GetApproximateSizes(size_approx_options, default_cf, &r, 1, &size));
  ASSERT_EQ(size, 0);

  // Test range boundaries
  ASSERT_OK(db_->Put(write_opts, Key(1000), ts, rnd.RandomString(1024)));
  // Should include start key
  start = Key(1000);
  end = Key(1100);
  r = Range(start, end);
  ASSERT_OK(
      db_->GetApproximateSizes(size_approx_options, default_cf, &r, 1, &size));
  ASSERT_GT(size, 0);

  uint64_t total_mem_count;
  uint64_t total_mem_size;
  db_->GetApproximateMemTableStats(default_cf, r, &total_mem_count,
                                   &total_mem_size);
  ASSERT_GT(total_mem_count, 0);
  ASSERT_GT(total_mem_size, 0);

  // Should exclude end key
  start = Key(900);
  end = Key(1000);
  r = Range(start, end);
  ASSERT_OK(
      db_->GetApproximateSizes(size_approx_options, default_cf, &r, 1, &size));
  ASSERT_EQ(size, 0);

  Close();
}

TEST_F(DBBasicTestWithTimestamp, SimpleIterate) {
  const int kNumKeysPerFile = 128;
  const uint64_t kMaxKey = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  const std::vector<uint64_t> start_keys = {1, 0};
  const std::vector<std::string> write_timestamps = {Timestamp(1, 0),
                                                     Timestamp(3, 0)};
  const std::vector<std::string> read_timestamps = {Timestamp(2, 0),
                                                    Timestamp(4, 0)};
  for (size_t i = 0; i < write_timestamps.size(); ++i) {
    WriteOptions write_opts;
    for (uint64_t key = start_keys[i]; key <= kMaxKey; ++key) {
      Status s = db_->Put(write_opts, Key1(key), write_timestamps[i],
                          "value" + std::to_string(i));
      ASSERT_OK(s);
    }
  }
  for (size_t i = 0; i < read_timestamps.size(); ++i) {
    ReadOptions read_opts;
    Slice read_ts = read_timestamps[i];
    read_opts.timestamp = &read_ts;
    std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));
    int count = 0;
    uint64_t key = 0;
    // Forward iterate.
    for (it->Seek(Key1(0)), key = start_keys[i]; it->Valid();
         it->Next(), ++count, ++key) {
      CheckIterUserEntry(it.get(), Key1(key), kTypeValue,
                         "value" + std::to_string(i), write_timestamps[i]);
    }
    size_t expected_count = kMaxKey - start_keys[i] + 1;
    ASSERT_EQ(expected_count, count);

    // Backward iterate.
    count = 0;
    for (it->SeekForPrev(Key1(kMaxKey)), key = kMaxKey; it->Valid();
         it->Prev(), ++count, --key) {
      CheckIterUserEntry(it.get(), Key1(key), kTypeValue,
                         "value" + std::to_string(i), write_timestamps[i]);
    }
    ASSERT_OK(it->status());
    ASSERT_EQ(static_cast<size_t>(kMaxKey) - start_keys[i] + 1, count);

    // SeekToFirst()/SeekToLast() with lower/upper bounds.
    // Then iter with lower and upper bounds.
    uint64_t l = 0;
    uint64_t r = kMaxKey + 1;
    while (l < r) {
      std::string lb_str = Key1(l);
      Slice lb = lb_str;
      std::string ub_str = Key1(r);
      Slice ub = ub_str;
      read_opts.iterate_lower_bound = &lb;
      read_opts.iterate_upper_bound = &ub;
      it.reset(db_->NewIterator(read_opts));
      for (it->SeekToFirst(), key = std::max(l, start_keys[i]), count = 0;
           it->Valid(); it->Next(), ++key, ++count) {
        CheckIterUserEntry(it.get(), Key1(key), kTypeValue,
                           "value" + std::to_string(i), write_timestamps[i]);
      }
      ASSERT_OK(it->status());
      ASSERT_EQ(r - std::max(l, start_keys[i]), count);

      for (it->SeekToLast(), key = std::min(r, kMaxKey + 1), count = 0;
           it->Valid(); it->Prev(), --key, ++count) {
        CheckIterUserEntry(it.get(), Key1(key - 1), kTypeValue,
                           "value" + std::to_string(i), write_timestamps[i]);
      }
      ASSERT_OK(it->status());
      l += (kMaxKey / 100);
      r -= (kMaxKey / 100);
    }
  }
  Close();
}

TEST_F(DBBasicTestWithTimestamp, TrimHistoryTest) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  auto check_value_by_ts = [](DB* db, Slice key, std::string readTs,
                              Status status, std::string checkValue,
                              std::string expected_ts) {
    ReadOptions ropts;
    Slice ts = readTs;
    ropts.timestamp = &ts;
    std::string value;
    std::string key_ts;
    Status s = db->Get(ropts, key, &value, &key_ts);
    ASSERT_TRUE(s == status);
    if (s.ok()) {
      ASSERT_EQ(checkValue, value);
    }
    if (s.ok() || s.IsNotFound()) {
      ASSERT_EQ(expected_ts, key_ts);
    }
  };
  // Construct data of different versions with different ts
  ASSERT_OK(db_->Put(WriteOptions(), "k1", Timestamp(2, 0), "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "k1", Timestamp(4, 0), "v2"));
  ASSERT_OK(db_->Delete(WriteOptions(), "k1", Timestamp(5, 0)));
  ASSERT_OK(db_->Put(WriteOptions(), "k1", Timestamp(6, 0), "v3"));
  check_value_by_ts(db_.get(), "k1", Timestamp(7, 0), Status::OK(), "v3",
                    Timestamp(6, 0));
  ASSERT_OK(Flush());
  Close();

  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(kDefaultColumnFamilyName, cf_options);
  DBOptions db_options(options);

  // Trim data whose version > Timestamp(5, 0), read(k1, ts(7)) <- NOT_FOUND.
  ASSERT_OK(DB::OpenAndTrimHistory(db_options, dbname_, column_families,
                                   &handles_, &db_, Timestamp(5, 0)));
  check_value_by_ts(db_.get(), "k1", Timestamp(7, 0), Status::NotFound(), "",
                    Timestamp(5, 0));
  Close();

  // Trim data whose timestamp > Timestamp(4, 0), read(k1, ts(7)) <- v2
  ASSERT_OK(DB::OpenAndTrimHistory(db_options, dbname_, column_families,
                                   &handles_, &db_, Timestamp(4, 0)));
  check_value_by_ts(db_.get(), "k1", Timestamp(7, 0), Status::OK(), "v2",
                    Timestamp(4, 0));
  Close();

  Reopen(options);
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "k1",
                             "k3", Timestamp(7, 0)));
  check_value_by_ts(db_.get(), "k1", Timestamp(8, 0), Status::NotFound(), "",
                    Timestamp(7, 0));
  Close();
  // Trim data whose timestamp > Timestamp(6, 0), read(k1, ts(8)) <- v2
  ASSERT_OK(DB::OpenAndTrimHistory(db_options, dbname_, column_families,
                                   &handles_, &db_, Timestamp(6, 0)));
  check_value_by_ts(db_.get(), "k1", Timestamp(8, 0), Status::OK(), "v2",
                    Timestamp(4, 0));
  Close();
}

TEST_F(DBBasicTestWithTimestamp, OpenAndTrimHistoryInvalidOptionTest) {
  Destroy(last_options_);

  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;

  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(kDefaultColumnFamilyName, cf_options);
  DBOptions db_options(options);

  // OpenAndTrimHistory should not work with avoid_flush_during_recovery
  db_options.avoid_flush_during_recovery = true;
  ASSERT_TRUE(DB::OpenAndTrimHistory(db_options, dbname_, column_families,
                                     &handles_, &db_, Timestamp(0, 0))
                  .IsInvalidArgument());
}

TEST_F(DBBasicTestWithTimestamp, GetTimestampTableProperties) {
  Options options = CurrentOptions();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  // Create 2 tables
  for (int table = 0; table < 2; ++table) {
    for (int i = 0; i < 10; i++) {
      std::string ts = Timestamp(i, 0);
      ASSERT_OK(db_->Put(WriteOptions(), "key", ts, Key(i)));
    }
    ASSERT_OK(Flush());
  }

  TablePropertiesCollection props;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&props));
  ASSERT_EQ(2U, props.size());
  for (const auto& item : props) {
    auto& user_collected = item.second->user_collected_properties;
    ASSERT_TRUE(user_collected.find("rocksdb.timestamp_min") !=
                user_collected.end());
    ASSERT_TRUE(user_collected.find("rocksdb.timestamp_max") !=
                user_collected.end());
    ASSERT_EQ(user_collected.at("rocksdb.timestamp_min"), Timestamp(0, 0));
    ASSERT_EQ(user_collected.at("rocksdb.timestamp_max"), Timestamp(9, 0));
  }
  Close();
}

class DBBasicTestWithTimestampTableOptions
    : public DBBasicTestWithTimestampBase,
      public testing::WithParamInterface<BlockBasedTableOptions::IndexType> {
 public:
  explicit DBBasicTestWithTimestampTableOptions()
      : DBBasicTestWithTimestampBase(
            "db_basic_test_with_timestamp_table_options") {}
};

INSTANTIATE_TEST_CASE_P(
    Timestamp, DBBasicTestWithTimestampTableOptions,
    testing::Values(
        BlockBasedTableOptions::IndexType::kBinarySearch,
        BlockBasedTableOptions::IndexType::kHashSearch,
        BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch,
        BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey));

TEST_P(DBBasicTestWithTimestampTableOptions, GetAndMultiGet) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  options.compression = kNoCompression;
  BlockBasedTableOptions bbto;
  bbto.index_type = GetParam();
  bbto.block_size = 100;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator cmp(kTimestampSize);
  options.comparator = &cmp;
  DestroyAndReopen(options);
  constexpr uint64_t kNumKeys = 1024;
  for (uint64_t k = 0; k < kNumKeys; ++k) {
    WriteOptions write_opts;
    ASSERT_OK(db_->Put(write_opts, Key1(k), Timestamp(1, 0),
                       "value" + std::to_string(k)));
  }
  ASSERT_OK(Flush());
  {
    ReadOptions read_opts;
    read_opts.total_order_seek = true;
    std::string ts_str = Timestamp(2, 0);
    Slice ts = ts_str;
    read_opts.timestamp = &ts;
    std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));
    // verify Get()
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      std::string value_from_get;
      std::string key_str(it->key().data(), it->key().size());
      std::string timestamp;
      ASSERT_OK(db_->Get(read_opts, key_str, &value_from_get, &timestamp));
      ASSERT_EQ(it->value(), value_from_get);
      ASSERT_EQ(Timestamp(1, 0), timestamp);
    }
    ASSERT_OK(it->status());

    // verify MultiGet()
    constexpr uint64_t step = 2;
    static_assert(0 == (kNumKeys % step),
                  "kNumKeys must be a multiple of step");
    for (uint64_t k = 0; k < kNumKeys; k += 2) {
      std::vector<std::string> key_strs;
      std::vector<Slice> keys;
      for (size_t i = 0; i < step; ++i) {
        key_strs.push_back(Key1(k + i));
      }
      for (size_t i = 0; i < step; ++i) {
        keys.emplace_back(key_strs[i]);
      }
      std::vector<std::string> values;
      std::vector<std::string> timestamps;
      std::vector<Status> statuses =
          db_->MultiGet(read_opts, keys, &values, &timestamps);
      ASSERT_EQ(step, statuses.size());
      ASSERT_EQ(step, values.size());
      ASSERT_EQ(step, timestamps.size());
      for (uint64_t i = 0; i < step; ++i) {
        ASSERT_OK(statuses[i]);
        ASSERT_EQ("value" + std::to_string(k + i), values[i]);
        ASSERT_EQ(Timestamp(1, 0), timestamps[i]);
      }
    }
  }
  Close();
}

TEST_P(DBBasicTestWithTimestampTableOptions, SeekWithPrefixLessThanKey) {
  Options options = CurrentOptions();
  options.prefix_seek_opt_in_only = false;  // Use legacy prefix seek
  options.env = env_;
  options.create_if_missing = true;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  options.memtable_whole_key_filtering = true;
  options.memtable_prefix_bloom_size_ratio = 0.1;
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.cache_index_and_filter_blocks = true;
  bbto.whole_key_filtering = true;
  bbto.index_type = GetParam();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  WriteOptions write_opts;
  std::string ts = Timestamp(1, 0);

  ASSERT_OK(db_->Put(write_opts, "foo1", ts, "bar"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->Put(write_opts, "foo2", ts, "bar"));
  ASSERT_OK(Flush());

  // Move sst file to next level
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_OK(db_->Put(write_opts, "foo3", ts, "bar"));
  ASSERT_OK(Flush());

  ReadOptions read_opts;
  Slice read_ts = ts;
  read_opts.timestamp = &read_ts;
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    iter->Seek("foo");
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());

    iter->Seek("bbb");
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  Close();
}

TEST_P(DBBasicTestWithTimestampTableOptions, SeekWithCappedPrefix) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  // All of the keys or this test must be longer than 3 characters
  constexpr int kMinKeyLen = 3;
  options.prefix_extractor.reset(NewCappedPrefixTransform(kMinKeyLen));
  options.memtable_whole_key_filtering = true;
  options.memtable_prefix_bloom_size_ratio = 0.1;
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.cache_index_and_filter_blocks = true;
  bbto.whole_key_filtering = true;
  bbto.index_type = GetParam();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  WriteOptions write_opts;
  std::string ts = Timestamp(1, 0);

  ASSERT_OK(db_->Put(write_opts, "foo1", ts, "bar"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->Put(write_opts, "foo2", ts, "bar"));
  ASSERT_OK(Flush());

  // Move sst file to next level
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_OK(db_->Put(write_opts, "foo3", ts, "bar"));
  ASSERT_OK(Flush());

  ReadOptions read_opts;
  ts = Timestamp(2, 0);
  Slice read_ts = ts;
  read_opts.timestamp = &read_ts;
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    // Make sure the prefix extractor doesn't include timestamp, otherwise it
    // may return invalid result.
    iter->Seek("foo");
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  Close();
}

TEST_P(DBBasicTestWithTimestampTableOptions, SeekWithBound) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.cache_index_and_filter_blocks = true;
  bbto.whole_key_filtering = true;
  bbto.index_type = GetParam();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  WriteOptions write_opts;
  std::string ts = Timestamp(1, 0);

  ASSERT_OK(db_->Put(write_opts, "foo1", ts, "bar1"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->Put(write_opts, "foo2", ts, "bar2"));
  ASSERT_OK(Flush());

  // Move sst file to next level
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  for (int i = 3; i < 9; ++i) {
    ASSERT_OK(db_->Put(write_opts, "foo" + std::to_string(i), ts,
                       "bar" + std::to_string(i)));
  }
  ASSERT_OK(Flush());

  ReadOptions read_opts;
  ts = Timestamp(2, 0);
  Slice read_ts = ts;
  read_opts.timestamp = &read_ts;
  std::string up_bound = "foo5";  // exclusive
  Slice up_bound_slice = up_bound;
  std::string lo_bound = "foo2";  // inclusive
  Slice lo_bound_slice = lo_bound;
  read_opts.iterate_upper_bound = &up_bound_slice;
  read_opts.iterate_lower_bound = &lo_bound_slice;
  read_opts.auto_prefix_mode = true;
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    // Make sure the prefix extractor doesn't include timestamp, otherwise it
    // may return invalid result.
    iter->Seek("foo");
    CheckIterUserEntry(iter.get(), lo_bound, kTypeValue, "bar2",
                       Timestamp(1, 0));
    iter->SeekToFirst();
    CheckIterUserEntry(iter.get(), lo_bound, kTypeValue, "bar2",
                       Timestamp(1, 0));
    iter->SeekForPrev("g");
    CheckIterUserEntry(iter.get(), "foo4", kTypeValue, "bar4", Timestamp(1, 0));
    iter->SeekToLast();
    CheckIterUserEntry(iter.get(), "foo4", kTypeValue, "bar4", Timestamp(1, 0));
  }

  Close();
}

TEST_F(DBBasicTestWithTimestamp, ChangeIterationDirection) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.env = env_;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));
  options.prefix_seek_opt_in_only = false;  // Use legacy prefix seek
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  DestroyAndReopen(options);
  const std::vector<std::string> timestamps = {Timestamp(1, 1), Timestamp(0, 2),
                                               Timestamp(4, 3)};
  const std::vector<std::tuple<std::string, std::string>> kvs = {
      std::make_tuple("aa", "value1"), std::make_tuple("ab", "value2")};
  for (const auto& ts : timestamps) {
    WriteBatch wb(0, 0, 0, kTimestampSize);
    for (const auto& kv : kvs) {
      const std::string& key = std::get<0>(kv);
      const std::string& value = std::get<1>(kv);
      ASSERT_OK(wb.Put(key, value));
    }

    ASSERT_OK(wb.UpdateTimestamps(
        ts, [kTimestampSize](uint32_t) { return kTimestampSize; }));
    ASSERT_OK(db_->Write(WriteOptions(), &wb));
  }
  std::string read_ts_str = Timestamp(5, 3);
  Slice read_ts = read_ts_str;
  ReadOptions read_opts;
  read_opts.timestamp = &read_ts;
  std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));

  it->SeekToFirst();
  ASSERT_TRUE(it->Valid());
  it->Prev();
  ASSERT_FALSE(it->Valid());

  it->SeekToLast();
  ASSERT_TRUE(it->Valid());
  uint64_t prev_reseek_count =
      options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION);
  ASSERT_EQ(0, prev_reseek_count);
  it->Next();
  ASSERT_FALSE(it->Valid());
  ASSERT_EQ(1 + prev_reseek_count,
            options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION));

  it->Seek(std::get<0>(kvs[0]));
  CheckIterUserEntry(it.get(), std::get<0>(kvs[0]), kTypeValue,
                     std::get<1>(kvs[0]), Timestamp(4, 3));
  it->Next();
  CheckIterUserEntry(it.get(), std::get<0>(kvs[1]), kTypeValue,
                     std::get<1>(kvs[1]), Timestamp(4, 3));
  it->Prev();
  CheckIterUserEntry(it.get(), std::get<0>(kvs[0]), kTypeValue,
                     std::get<1>(kvs[0]), Timestamp(4, 3));

  prev_reseek_count =
      options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION);
  ASSERT_EQ(1, prev_reseek_count);
  it->Next();
  CheckIterUserEntry(it.get(), std::get<0>(kvs[1]), kTypeValue,
                     std::get<1>(kvs[1]), Timestamp(4, 3));
  ASSERT_EQ(1 + prev_reseek_count,
            options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION));

  it->SeekForPrev(std::get<0>(kvs[1]));
  CheckIterUserEntry(it.get(), std::get<0>(kvs[1]), kTypeValue,
                     std::get<1>(kvs[1]), Timestamp(4, 3));
  it->Prev();
  CheckIterUserEntry(it.get(), std::get<0>(kvs[0]), kTypeValue,
                     std::get<1>(kvs[0]), Timestamp(4, 3));

  prev_reseek_count =
      options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION);
  it->Next();
  CheckIterUserEntry(it.get(), std::get<0>(kvs[1]), kTypeValue,
                     std::get<1>(kvs[1]), Timestamp(4, 3));
  ASSERT_EQ(1 + prev_reseek_count,
            options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION));

  it.reset();
  Close();
}

TEST_F(DBBasicTestWithTimestamp, SimpleForwardIterateLowerTsBound) {
  constexpr int kNumKeysPerFile = 128;
  constexpr uint64_t kMaxKey = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  const std::vector<std::string> write_timestamps = {Timestamp(1, 0),
                                                     Timestamp(3, 0)};
  const std::vector<std::string> read_timestamps = {Timestamp(2, 0),
                                                    Timestamp(4, 0)};
  const std::vector<std::string> read_timestamps_lb = {Timestamp(1, 0),
                                                       Timestamp(1, 0)};
  for (size_t i = 0; i < write_timestamps.size(); ++i) {
    WriteOptions write_opts;
    for (uint64_t key = 0; key <= kMaxKey; ++key) {
      Status s = db_->Put(write_opts, Key1(key), write_timestamps[i],
                          "value" + std::to_string(i));
      ASSERT_OK(s);
    }
  }
  for (size_t i = 0; i < read_timestamps.size(); ++i) {
    ReadOptions read_opts;
    Slice read_ts = read_timestamps[i];
    Slice read_ts_lb = read_timestamps_lb[i];
    read_opts.timestamp = &read_ts;
    read_opts.iter_start_ts = &read_ts_lb;
    std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));
    int count = 0;
    uint64_t key = 0;
    for (it->Seek(Key1(0)), key = 0; it->Valid(); it->Next(), ++count, ++key) {
      CheckIterEntry(it.get(), Key1(key), kTypeValue,
                     "value" + std::to_string(i), write_timestamps[i]);
      if (i > 0) {
        it->Next();
        CheckIterEntry(it.get(), Key1(key), kTypeValue,
                       "value" + std::to_string(i - 1),
                       write_timestamps[i - 1]);
      }
    }
    ASSERT_OK(it->status());
    size_t expected_count = kMaxKey + 1;
    ASSERT_EQ(expected_count, count);
  }
  // Delete all keys@ts=5 and check iteration result with start ts set
  {
    std::string write_timestamp = Timestamp(5, 0);
    WriteOptions write_opts;
    for (uint64_t key = 0; key < kMaxKey + 1; ++key) {
      Status s = db_->Delete(write_opts, Key1(key), write_timestamp);
      ASSERT_OK(s);
    }

    std::string read_timestamp = Timestamp(6, 0);
    ReadOptions read_opts;
    Slice read_ts = read_timestamp;
    read_opts.timestamp = &read_ts;
    std::string read_timestamp_lb = Timestamp(2, 0);
    Slice read_ts_lb = read_timestamp_lb;
    read_opts.iter_start_ts = &read_ts_lb;
    std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));
    int count = 0;
    uint64_t key = 0;
    for (it->Seek(Key1(0)), key = 0; it->Valid(); it->Next(), ++count, ++key) {
      CheckIterEntry(it.get(), Key1(key), kTypeDeletionWithTimestamp, Slice(),
                     write_timestamp);
      // Skip key@ts=3 and land on tombstone key@ts=5
      it->Next();
    }
    ASSERT_EQ(kMaxKey + 1, count);
  }
  Close();
}

TEST_F(DBBasicTestWithTimestamp, BackwardIterateLowerTsBound) {
  constexpr int kNumKeysPerFile = 128;
  constexpr uint64_t kMaxKey = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  const std::vector<std::string> write_timestamps = {Timestamp(1, 0),
                                                     Timestamp(3, 0)};
  const std::vector<std::string> read_timestamps = {Timestamp(2, 0),
                                                    Timestamp(4, 0)};
  const std::vector<std::string> read_timestamps_lb = {Timestamp(1, 0),
                                                       Timestamp(1, 0)};
  for (size_t i = 0; i < write_timestamps.size(); ++i) {
    WriteOptions write_opts;
    for (uint64_t key = 0; key <= kMaxKey; ++key) {
      Status s = db_->Put(write_opts, Key1(key), write_timestamps[i],
                          "value" + std::to_string(i));
      ASSERT_OK(s);
    }
  }
  for (size_t i = 0; i < read_timestamps.size(); ++i) {
    ReadOptions read_opts;
    Slice read_ts = read_timestamps[i];
    Slice read_ts_lb = read_timestamps_lb[i];
    read_opts.timestamp = &read_ts;
    read_opts.iter_start_ts = &read_ts_lb;
    std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));
    int count = 0;
    uint64_t key = 0;
    for (it->SeekForPrev(Key1(kMaxKey)), key = kMaxKey; it->Valid();
         it->Prev(), ++count, --key) {
      CheckIterEntry(it.get(), Key1(key), kTypeValue, "value0",
                     write_timestamps[0]);
      if (i > 0) {
        it->Prev();
        CheckIterEntry(it.get(), Key1(key), kTypeValue, "value1",
                       write_timestamps[1]);
      }
    }
    ASSERT_OK(it->status());
    size_t expected_count = kMaxKey + 1;
    ASSERT_EQ(expected_count, count);
  }
  // Delete all keys@ts=5 and check iteration result with start ts set
  {
    std::string write_timestamp = Timestamp(5, 0);
    WriteOptions write_opts;
    for (uint64_t key = 0; key < kMaxKey + 1; ++key) {
      Status s = db_->Delete(write_opts, Key1(key), write_timestamp);
      ASSERT_OK(s);
    }

    std::string read_timestamp = Timestamp(6, 0);
    ReadOptions read_opts;
    Slice read_ts = read_timestamp;
    read_opts.timestamp = &read_ts;
    std::string read_timestamp_lb = Timestamp(2, 0);
    Slice read_ts_lb = read_timestamp_lb;
    read_opts.iter_start_ts = &read_ts_lb;
    std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));
    int count = 0;
    uint64_t key = kMaxKey;
    for (it->SeekForPrev(Key1(key)), key = kMaxKey; it->Valid();
         it->Prev(), ++count, --key) {
      CheckIterEntry(it.get(), Key1(key), kTypeValue, "value1",
                     Timestamp(3, 0));
      it->Prev();
      CheckIterEntry(it.get(), Key1(key), kTypeDeletionWithTimestamp, Slice(),
                     write_timestamp);
    }
    ASSERT_OK(it->status());
    ASSERT_EQ(kMaxKey + 1, count);
  }
  Close();
}

TEST_F(DBBasicTestWithTimestamp, SimpleBackwardIterateLowerTsBound) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  std::string ts_ub_buf = Timestamp(5, 0);
  Slice ts_ub = ts_ub_buf;
  std::string ts_lb_buf = Timestamp(1, 0);
  Slice ts_lb = ts_lb_buf;

  {
    ReadOptions read_opts;
    read_opts.timestamp = &ts_ub;
    read_opts.iter_start_ts = &ts_lb;
    std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));
    it->SeekToLast();
    ASSERT_FALSE(it->Valid());
    ASSERT_OK(it->status());

    it->SeekForPrev("foo");
    ASSERT_FALSE(it->Valid());
    ASSERT_OK(it->status());
  }

  // Test iterate_upper_bound
  ASSERT_OK(db_->Put(WriteOptions(), "a", Timestamp(0, 0), "v0"));
  ASSERT_OK(db_->SingleDelete(WriteOptions(), "a", Timestamp(1, 0)));

  for (int i = 0; i < 5; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "b", Timestamp(i, 0),
                       "v" + std::to_string(i)));
  }

  {
    ReadOptions read_opts;
    read_opts.timestamp = &ts_ub;
    read_opts.iter_start_ts = &ts_lb;
    std::string key_ub_str = "b";  // exclusive
    Slice key_ub = key_ub_str;
    read_opts.iterate_upper_bound = &key_ub;
    std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));
    it->SeekToLast();
    CheckIterEntry(it.get(), "a", kTypeSingleDeletion, Slice(),
                   Timestamp(1, 0));

    key_ub_str = "a";  // exclusive
    key_ub = key_ub_str;
    read_opts.iterate_upper_bound = &key_ub;
    it.reset(db_->NewIterator(read_opts));
    it->SeekToLast();
    ASSERT_FALSE(it->Valid());
    ASSERT_OK(it->status());
  }

  Close();
}

TEST_F(DBBasicTestWithTimestamp, BackwardIterateLowerTsBound_Reseek) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.max_sequential_skip_in_iterations = 2;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  for (int i = 0; i < 10; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "a", Timestamp(i, 0),
                       "v" + std::to_string(i)));
  }

  for (int i = 0; i < 10; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "b", Timestamp(i, 0),
                       "v" + std::to_string(i)));
  }

  {
    std::string ts_ub_buf = Timestamp(6, 0);
    Slice ts_ub = ts_ub_buf;
    std::string ts_lb_buf = Timestamp(4, 0);
    Slice ts_lb = ts_lb_buf;

    ReadOptions read_opts;
    read_opts.timestamp = &ts_ub;
    read_opts.iter_start_ts = &ts_lb;
    std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));
    it->SeekToLast();
    for (int i = 0; i < 3 && it->Valid(); it->Prev(), ++i) {
      CheckIterEntry(it.get(), "b", kTypeValue, "v" + std::to_string(4 + i),
                     Timestamp(4 + i, 0));
    }
    for (int i = 0; i < 3 && it->Valid(); it->Prev(), ++i) {
      CheckIterEntry(it.get(), "a", kTypeValue, "v" + std::to_string(4 + i),
                     Timestamp(4 + i, 0));
    }
    ASSERT_OK(it->status());
  }

  Close();
}

TEST_F(DBBasicTestWithTimestamp, ReseekToTargetTimestamp) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  constexpr size_t kNumKeys = 16;
  options.max_sequential_skip_in_iterations = kNumKeys / 2;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  // Insert kNumKeys
  WriteOptions write_opts;
  Status s;
  for (size_t i = 0; i != kNumKeys; ++i) {
    std::string ts = Timestamp(static_cast<uint64_t>(i + 1), 0);
    s = db_->Put(write_opts, "foo", ts, "value" + std::to_string(i));
    ASSERT_OK(s);
  }
  {
    ReadOptions read_opts;
    std::string ts_str = Timestamp(1, 0);
    Slice ts = ts_str;
    read_opts.timestamp = &ts;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    iter->SeekToFirst();
    CheckIterUserEntry(iter.get(), "foo", kTypeValue, "value0", ts_str);
    ASSERT_EQ(
        1, options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION));

    ts_str = Timestamp(kNumKeys, 0);
    ts = ts_str;
    read_opts.timestamp = &ts;
    iter.reset(db_->NewIterator(read_opts));
    iter->SeekToLast();
    CheckIterUserEntry(iter.get(), "foo", kTypeValue,
                       "value" + std::to_string(kNumKeys - 1), ts_str);
    ASSERT_EQ(
        2, options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION));
  }
  Close();
}

TEST_F(DBBasicTestWithTimestamp, ReseekToNextUserKey) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  constexpr size_t kNumKeys = 16;
  options.max_sequential_skip_in_iterations = kNumKeys / 2;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  // Write kNumKeys + 1 keys
  WriteOptions write_opts;
  Status s;
  for (size_t i = 0; i != kNumKeys; ++i) {
    std::string ts = Timestamp(static_cast<uint64_t>(i + 1), 0);
    s = db_->Put(write_opts, "a", ts, "value" + std::to_string(i));
    ASSERT_OK(s);
  }
  {
    std::string ts_str = Timestamp(static_cast<uint64_t>(kNumKeys + 1), 0);
    WriteBatch batch(0, 0, 0, kTimestampSize);
    {
      ASSERT_OK(batch.Put("a", "new_value"));
    }
    {
      ASSERT_OK(batch.Put("b", "new_value"));
    }
    s = batch.UpdateTimestamps(
        ts_str, [kTimestampSize](uint32_t) { return kTimestampSize; });
    ASSERT_OK(s);
    s = db_->Write(write_opts, &batch);
    ASSERT_OK(s);
  }
  {
    ReadOptions read_opts;
    std::string ts_str = Timestamp(static_cast<uint64_t>(kNumKeys + 1), 0);
    Slice ts = ts_str;
    read_opts.timestamp = &ts;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    iter->Seek("a");
    iter->Next();
    CheckIterUserEntry(iter.get(), "b", kTypeValue, "new_value", ts_str);
    ASSERT_EQ(
        1, options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION));
  }
  Close();
}

TEST_F(DBBasicTestWithTimestamp, ReseekToUserKeyBeforeSavedKey) {
  Options options = GetDefaultOptions();
  options.env = env_;
  options.create_if_missing = true;
  constexpr size_t kNumKeys = 16;
  options.max_sequential_skip_in_iterations = kNumKeys / 2;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  for (size_t i = 0; i < kNumKeys; ++i) {
    std::string ts = Timestamp(static_cast<uint64_t>(i + 1), 0);
    WriteOptions write_opts;
    Status s = db_->Put(write_opts, "b", ts, "value" + std::to_string(i));
    ASSERT_OK(s);
  }
  {
    std::string ts = Timestamp(1, 0);
    WriteOptions write_opts;
    ASSERT_OK(db_->Put(write_opts, "a", ts, "value"));
  }
  {
    ReadOptions read_opts;
    std::string ts_str = Timestamp(1, 0);
    Slice ts = ts_str;
    read_opts.timestamp = &ts;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    iter->SeekToLast();
    iter->Prev();
    CheckIterUserEntry(iter.get(), "a", kTypeValue, "value", ts_str);
    ASSERT_EQ(
        1, options.statistics->getTickerCount(NUMBER_OF_RESEEKS_IN_ITERATION));
  }
  Close();
}

class ReverseIterationWithUnpreparedBlobTest
    : public DBBasicTestWithTimestampBase,
      public testing::WithParamInterface<std::tuple<bool, uint64_t>> {
 public:
  ReverseIterationWithUnpreparedBlobTest()
      : DBBasicTestWithTimestampBase(
            "db_basic_test_with_timestamp_reverse_with_unprepare") {}
};
INSTANTIATE_TEST_CASE_P(ReverseIterationWithUnpreparedBlobTest,
                        ReverseIterationWithUnpreparedBlobTest,
                        ::testing::Combine(::testing::Values(true, false),
                                           ::testing::Values(0, 2)));
TEST_P(ReverseIterationWithUnpreparedBlobTest, Basic) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.env = env_;
  options.enable_blob_files = true;
  options.max_sequential_skip_in_iterations = std::get<1>(GetParam());

  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;

  DestroyAndReopen(options);

  constexpr uint64_t kMaxKey = 1024;

  const std::vector<std::string> write_timestamps = {Timestamp(1, 0),
                                                     Timestamp(3, 0)};
  for (uint64_t key = 0; key <= kMaxKey; ++key) {
    for (size_t i = 0; i < write_timestamps.size(); ++i) {
      ASSERT_OK(db_->Put(WriteOptions(), Key1(key), write_timestamps[i],
                         Key1(key) + "value" + std::to_string(i)));
    }
  }

  ASSERT_OK(Flush());

  {
    const std::string read_timestamp_str = Timestamp(4, 0);
    const Slice read_timestamp(read_timestamp_str);

    ReadOptions read_opts;
    read_opts.timestamp = &read_timestamp;
    read_opts.allow_unprepared_value = std::get<0>(GetParam());

    std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));

    it->SeekForPrev(Key1(kMaxKey));
    uint64_t key = kMaxKey;
    int count = 0;
    while (it->Valid()) {
      ASSERT_OK(it->status());

      ASSERT_TRUE(it->PrepareValue());
      ASSERT_TRUE(it->Valid());
      ASSERT_OK(it->status());
      ASSERT_EQ(it->key(), Key1(key));
      ASSERT_EQ(it->timestamp(), Timestamp(3, 0));
      ASSERT_EQ(it->value(), Key1(key) + "value" + std::to_string(1));
      key--;
      count++;
      it->Prev();
    }
    ASSERT_OK(it->status());
    ASSERT_EQ(kMaxKey + 1, count);
  }

  Close();
}

TEST_F(DBBasicTestWithTimestamp, MultiGetWithFastLocalBloom) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.cache_index_and_filter_blocks = true;
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  // Write any value
  WriteOptions write_opts;
  std::string ts = Timestamp(1, 0);

  ASSERT_OK(db_->Put(write_opts, "foo", ts, "bar"));

  ASSERT_OK(Flush());

  // Read with MultiGet
  ReadOptions read_opts;
  Slice read_ts = ts;
  read_opts.timestamp = &read_ts;
  size_t batch_size = 1;
  std::vector<Slice> keys(batch_size);
  std::vector<PinnableSlice> values(batch_size);
  std::vector<Status> statuses(batch_size);
  std::vector<std::string> timestamps(batch_size);
  keys[0] = "foo";
  ColumnFamilyHandle* cfh = db_->DefaultColumnFamily();
  db_->MultiGet(read_opts, cfh, batch_size, keys.data(), values.data(),
                timestamps.data(), statuses.data(), true);

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(Timestamp(1, 0), timestamps[0]);
  for (auto& elem : values) {
    elem.Reset();
  }

  ASSERT_OK(db_->SingleDelete(WriteOptions(), "foo", Timestamp(2, 0)));
  ts = Timestamp(3, 0);
  read_ts = ts;
  read_opts.timestamp = &read_ts;
  db_->MultiGet(read_opts, cfh, batch_size, keys.data(), values.data(),
                timestamps.data(), statuses.data(), true);
  ASSERT_TRUE(statuses[0].IsNotFound());
  ASSERT_EQ(Timestamp(2, 0), timestamps[0]);

  Close();
}

TEST_P(DBBasicTestWithTimestampTableOptions, MultiGetWithPrefix) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.prefix_extractor.reset(NewCappedPrefixTransform(5));
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.cache_index_and_filter_blocks = true;
  bbto.whole_key_filtering = false;
  bbto.index_type = GetParam();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  // Write any value
  WriteOptions write_opts;
  std::string ts = Timestamp(1, 0);

  ASSERT_OK(db_->Put(write_opts, "foo", ts, "bar"));

  ASSERT_OK(Flush());

  // Read with MultiGet
  ReadOptions read_opts;
  Slice read_ts = ts;
  read_opts.timestamp = &read_ts;
  size_t batch_size = 1;
  std::vector<Slice> keys(batch_size);
  std::vector<PinnableSlice> values(batch_size);
  std::vector<Status> statuses(batch_size);
  std::vector<std::string> timestamps(batch_size);
  keys[0] = "foo";
  ColumnFamilyHandle* cfh = db_->DefaultColumnFamily();
  db_->MultiGet(read_opts, cfh, batch_size, keys.data(), values.data(),
                timestamps.data(), statuses.data(), true);

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(Timestamp(1, 0), timestamps[0]);
  for (auto& elem : values) {
    elem.Reset();
  }

  ASSERT_OK(db_->SingleDelete(WriteOptions(), "foo", Timestamp(2, 0)));
  // TODO re-enable after fixing a bug of kHashSearch
  if (GetParam() != BlockBasedTableOptions::IndexType::kHashSearch) {
    ASSERT_OK(Flush());
  }

  ts = Timestamp(3, 0);
  read_ts = ts;
  db_->MultiGet(read_opts, cfh, batch_size, keys.data(), values.data(),
                timestamps.data(), statuses.data(), true);
  ASSERT_TRUE(statuses[0].IsNotFound());
  ASSERT_EQ(Timestamp(2, 0), timestamps[0]);

  Close();
}

TEST_P(DBBasicTestWithTimestampTableOptions, MultiGetWithMemBloomFilter) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.prefix_extractor.reset(NewCappedPrefixTransform(5));
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.cache_index_and_filter_blocks = true;
  bbto.whole_key_filtering = false;
  bbto.index_type = GetParam();
  options.memtable_prefix_bloom_size_ratio = 0.1;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  // Write any value
  WriteOptions write_opts;
  std::string ts = Timestamp(1, 0);

  ASSERT_OK(db_->Put(write_opts, "foo", ts, "bar"));

  // Read with MultiGet
  ts = Timestamp(2, 0);
  Slice read_ts = ts;
  ReadOptions read_opts;
  read_opts.timestamp = &read_ts;
  size_t batch_size = 1;
  std::vector<Slice> keys(batch_size);
  std::vector<PinnableSlice> values(batch_size);
  std::vector<Status> statuses(batch_size);
  keys[0] = "foo";
  ColumnFamilyHandle* cfh = db_->DefaultColumnFamily();
  db_->MultiGet(read_opts, cfh, batch_size, keys.data(), values.data(),
                statuses.data());

  ASSERT_OK(statuses[0]);
  Close();
}

TEST_F(DBBasicTestWithTimestamp, MultiGetRangeFiltering) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.cache_index_and_filter_blocks = true;
  bbto.whole_key_filtering = false;
  options.memtable_prefix_bloom_size_ratio = 0.1;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  // Write any value
  WriteOptions write_opts;
  std::string ts = Timestamp(1, 0);

  // random data
  for (int i = 0; i < 3; i++) {
    auto key = std::to_string(i * 10);
    auto value = std::to_string(i * 10);
    Slice key_slice = key;
    Slice value_slice = value;
    ASSERT_OK(db_->Put(write_opts, key_slice, ts, value_slice));
    ASSERT_OK(Flush());
  }

  // Make num_levels to 2 to do key range filtering of sst files
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_OK(db_->Put(write_opts, "foo", ts, "bar"));

  ASSERT_OK(Flush());

  // Read with MultiGet
  ts = Timestamp(2, 0);
  Slice read_ts = ts;
  ReadOptions read_opts;
  read_opts.timestamp = &read_ts;
  size_t batch_size = 1;
  std::vector<Slice> keys(batch_size);
  std::vector<PinnableSlice> values(batch_size);
  std::vector<Status> statuses(batch_size);
  keys[0] = "foo";
  ColumnFamilyHandle* cfh = db_->DefaultColumnFamily();
  db_->MultiGet(read_opts, cfh, batch_size, keys.data(), values.data(),
                statuses.data());

  ASSERT_OK(statuses[0]);
  Close();
}

TEST_F(DBBasicTestWithTimestamp, GetWithRowCache) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  LRUCacheOptions cache_options;
  cache_options.capacity = 8192;
  options.row_cache = cache_options.MakeSharedRowCache();

  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  WriteOptions write_opts;
  std::string ts_early = Timestamp(1, 0);
  std::string ts_later = Timestamp(10, 0);
  Slice ts_later_slice = ts_later;

  const Snapshot* snap_with_nothing = db_->GetSnapshot();
  ASSERT_OK(db_->Put(write_opts, "foo", ts_early, "bar"));
  ASSERT_OK(db_->Put(write_opts, "foo2", ts_early, "bar2"));
  ASSERT_OK(db_->Put(write_opts, "foo3", ts_early, "bar3"));

  const Snapshot* snap_with_foo = db_->GetSnapshot();
  ASSERT_OK(Flush());

  ReadOptions read_opts;
  read_opts.timestamp = &ts_later_slice;

  std::string read_value;
  std::string read_ts;
  Status s;

  int expected_hit_count = 0;
  int expected_miss_count = 0;
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), expected_hit_count);
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS), expected_miss_count);

  {
    read_opts.timestamp = nullptr;
    s = db_->Get(read_opts, "foo", &read_value);
    ASSERT_NOK(s);
    ASSERT_TRUE(s.IsInvalidArgument());
  }

  // Mix use of Get
  {
    read_opts.timestamp = &ts_later_slice;

    // Use Get without ts first, expect cache entry to store the correct ts
    s = db_->Get(read_opts, "foo2", &read_value);
    ASSERT_OK(s);
    ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), expected_hit_count);
    ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS),
              ++expected_miss_count);
    ASSERT_EQ(read_value, "bar2");

    s = db_->Get(read_opts, "foo2", &read_value, &read_ts);
    ASSERT_OK(s);
    ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), ++expected_hit_count);
    ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS), expected_miss_count);
    ASSERT_EQ(read_ts, ts_early);
    ASSERT_EQ(read_value, "bar2");

    // Use Get with ts first, expect the Get without ts can get correct record
    s = db_->Get(read_opts, "foo3", &read_value, &read_ts);
    ASSERT_OK(s);
    ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), expected_hit_count);
    ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS),
              ++expected_miss_count);
    ASSERT_EQ(read_ts, ts_early);
    ASSERT_EQ(read_value, "bar3");

    s = db_->Get(read_opts, "foo3", &read_value);
    ASSERT_OK(s);
    ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), ++expected_hit_count);
    ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS), expected_miss_count);
    ASSERT_EQ(read_value, "bar3");
  }

  {
    // Test with consecutive calls of Get with ts.
    s = db_->Get(read_opts, "foo", &read_value, &read_ts);
    ASSERT_OK(s);
    ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), expected_hit_count);
    ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS),
              ++expected_miss_count);
    ASSERT_EQ(read_ts, ts_early);
    ASSERT_EQ(read_value, "bar");

    // Test repeated get on cache entry
    for (int i = 0; i < 3; i++) {
      s = db_->Get(read_opts, "foo", &read_value, &read_ts);
      ASSERT_OK(s);
      ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT),
                ++expected_hit_count);
      ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS),
                expected_miss_count);
      ASSERT_EQ(read_ts, ts_early);
      ASSERT_EQ(read_value, "bar");
    }
  }

  {
    std::string ts_nothing = Timestamp(0, 0);
    Slice ts_nothing_slice = ts_nothing;
    read_opts.timestamp = &ts_nothing_slice;
    s = db_->Get(read_opts, "foo", &read_value, &read_ts);
    ASSERT_TRUE(s.IsNotFound());
    ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), expected_hit_count);
    ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS),
              ++expected_miss_count);
  }

  {
    read_opts.snapshot = snap_with_foo;
    read_opts.timestamp = &ts_later_slice;
    s = db_->Get(read_opts, "foo", &read_value, &read_ts);
    ASSERT_OK(s);
    ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), expected_hit_count);
    ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS),
              ++expected_miss_count);
    ASSERT_EQ(read_ts, ts_early);
    ASSERT_EQ(read_value, "bar");

    s = db_->Get(read_opts, "foo", &read_value, &read_ts);
    ASSERT_OK(s);
    ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), ++expected_hit_count);
    ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS), expected_miss_count);
    ASSERT_EQ(read_ts, ts_early);
    ASSERT_EQ(read_value, "bar");
  }

  {
    read_opts.snapshot = snap_with_nothing;
    s = db_->Get(read_opts, "foo", &read_value, &read_ts);
    ASSERT_TRUE(s.IsNotFound());
    ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), expected_hit_count);
    ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS),
              ++expected_miss_count);

    s = db_->Get(read_opts, "foo", &read_value, &read_ts);
    ASSERT_TRUE(s.IsNotFound());
    ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), expected_hit_count);
    ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS),
              ++expected_miss_count);
  }

  db_->ReleaseSnapshot(snap_with_nothing);
  db_->ReleaseSnapshot(snap_with_foo);
  Close();
}

TEST_F(DBBasicTestWithTimestamp, GetWithRowCacheMultiSST) {
  BlockBasedTableOptions table_options;
  table_options.block_size = 1;
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  LRUCacheOptions cache_options;
  cache_options.capacity = 8192;
  options.row_cache = cache_options.MakeSharedRowCache();

  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.merge_operator = MergeOperators::CreateStringAppendTESTOperator();
  options.disable_auto_compactions = true;

  DestroyAndReopen(options);

  std::string ts_early = Timestamp(1, 0);
  std::string ts_later = Timestamp(10, 0);
  Slice ts_later_slice = ts_later;

  ASSERT_OK(db_->Put(WriteOptions(), "foo", ts_early, "v1"));
  ASSERT_OK(Flush());

  ColumnFamilyHandle* default_cf = db_->DefaultColumnFamily();
  ASSERT_OK(
      db_->Merge(WriteOptions(), default_cf, "foo", Timestamp(2, 0), "v2"));
  ASSERT_OK(
      db_->Merge(WriteOptions(), default_cf, "foo", Timestamp(3, 0), "v3"));
  ASSERT_OK(Flush());

  ReadOptions read_opts;
  read_opts.timestamp = &ts_later_slice;

  std::string read_value;
  std::string read_ts;
  Status s;

  {
    // Since there are two SST files, will trigger the table lookup twice.
    s = db_->Get(read_opts, "foo", &read_value, &read_ts);
    ASSERT_OK(s);
    ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), 0);
    ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS), 2);
    ASSERT_EQ(read_ts, Timestamp(3, 0));
    ASSERT_EQ(read_value, "v1,v2,v3");

    s = db_->Get(read_opts, "foo", &read_value, &read_ts);
    ASSERT_OK(s);
    ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), 2);
    ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS), 2);
    ASSERT_EQ(read_ts, Timestamp(3, 0));
    ASSERT_EQ(read_value, "v1,v2,v3");
  }
}

TEST_P(DBBasicTestWithTimestampTableOptions, MultiGetPrefixFilter) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.prefix_extractor.reset(NewCappedPrefixTransform(3));
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.cache_index_and_filter_blocks = true;
  bbto.whole_key_filtering = false;
  bbto.index_type = GetParam();
  options.memtable_prefix_bloom_size_ratio = 0.1;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  WriteOptions write_opts;
  std::string ts = Timestamp(1, 0);

  ASSERT_OK(db_->Put(write_opts, "foo", ts, "bar"));

  ASSERT_OK(Flush());
  // Read with MultiGet
  ts = Timestamp(2, 0);
  Slice read_ts = ts;
  ReadOptions read_opts;
  read_opts.timestamp = &read_ts;
  size_t batch_size = 1;
  std::vector<Slice> keys(batch_size);
  std::vector<std::string> values(batch_size);
  std::vector<std::string> timestamps(batch_size);
  keys[0] = "foo";
  ColumnFamilyHandle* cfh = db_->DefaultColumnFamily();
  std::vector<ColumnFamilyHandle*> cfhs(keys.size(), cfh);
  std::vector<Status> statuses =
      db_->MultiGet(read_opts, cfhs, keys, &values, &timestamps);

  ASSERT_OK(statuses[0]);
  Close();
}

TEST_F(DBBasicTestWithTimestamp, MaxKeysSkippedDuringNext) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  constexpr size_t max_skippable_internal_keys = 2;
  const size_t kNumKeys = max_skippable_internal_keys + 2;
  WriteOptions write_opts;
  Status s;
  {
    std::string ts = Timestamp(1, 0);
    ASSERT_OK(db_->Put(write_opts, "a", ts, "value"));
  }
  for (size_t i = 0; i < kNumKeys; ++i) {
    std::string ts = Timestamp(static_cast<uint64_t>(i + 1), 0);
    s = db_->Put(write_opts, "b", ts, "value" + std::to_string(i));
    ASSERT_OK(s);
  }
  {
    ReadOptions read_opts;
    read_opts.max_skippable_internal_keys = max_skippable_internal_keys;
    std::string ts_str = Timestamp(1, 0);
    Slice ts = ts_str;
    read_opts.timestamp = &ts;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    iter->SeekToFirst();
    iter->Next();
    ASSERT_TRUE(iter->status().IsIncomplete());
  }
  Close();
}

TEST_F(DBBasicTestWithTimestamp, MaxKeysSkippedDuringPrev) {
  Options options = GetDefaultOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  constexpr size_t max_skippable_internal_keys = 2;
  const size_t kNumKeys = max_skippable_internal_keys + 2;
  WriteOptions write_opts;
  Status s;
  {
    std::string ts = Timestamp(1, 0);
    ASSERT_OK(db_->Put(write_opts, "b", ts, "value"));
  }
  for (size_t i = 0; i < kNumKeys; ++i) {
    std::string ts = Timestamp(static_cast<uint64_t>(i + 1), 0);
    s = db_->Put(write_opts, "a", ts, "value" + std::to_string(i));
    ASSERT_OK(s);
  }
  {
    ReadOptions read_opts;
    read_opts.max_skippable_internal_keys = max_skippable_internal_keys;
    std::string ts_str = Timestamp(1, 0);
    Slice ts = ts_str;
    read_opts.timestamp = &ts;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    iter->SeekToLast();
    iter->Prev();
    ASSERT_TRUE(iter->status().IsIncomplete());
  }
  Close();
}

// Create two L0, and compact them to a new L1. In this test, L1 is L_bottom.
// Two L0s:
//       f1                                  f2
// <a, 1, kTypeValue>    <a, 3, kTypeDeletionWithTimestamp>...<b, 2, kTypeValue>
// Since f2.smallest < f1.largest < f2.largest
// f1 and f2 will be the inputs of a real compaction instead of trivial move.
TEST_F(DBBasicTestWithTimestamp, CompactDeletionWithTimestampMarkerToBottom) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.num_levels = 2;
  options.level0_file_num_compaction_trigger = 2;
  DestroyAndReopen(options);
  WriteOptions write_opts;
  std::string ts = Timestamp(1, 0);
  ASSERT_OK(db_->Put(write_opts, "a", ts, "value0"));
  ASSERT_OK(Flush());

  ts = Timestamp(2, 0);
  ASSERT_OK(db_->Put(write_opts, "b", ts, "value0"));
  ts = Timestamp(3, 0);
  ASSERT_OK(db_->Delete(write_opts, "a", ts));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ReadOptions read_opts;
  ts = Timestamp(1, 0);
  Slice read_ts = ts;
  read_opts.timestamp = &read_ts;
  std::string value;
  Status s = db_->Get(read_opts, "a", &value);
  ASSERT_OK(s);
  ASSERT_EQ("value0", value);

  ts = Timestamp(3, 0);
  read_ts = ts;
  read_opts.timestamp = &read_ts;
  std::string key_ts;
  s = db_->Get(read_opts, "a", &value, &key_ts);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(Timestamp(3, 0), key_ts);

  // Time-travel to the past before deletion
  ts = Timestamp(2, 0);
  read_ts = ts;
  read_opts.timestamp = &read_ts;
  s = db_->Get(read_opts, "a", &value);
  ASSERT_OK(s);
  ASSERT_EQ("value0", value);
  Close();
}

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
class DBBasicTestWithTimestampFilterPrefixSettings
    : public DBBasicTestWithTimestampBase,
      public testing::WithParamInterface<
          std::tuple<std::shared_ptr<const FilterPolicy>, bool, bool,
                     std::shared_ptr<const SliceTransform>, bool, double,
                     BlockBasedTableOptions::IndexType>> {
 public:
  DBBasicTestWithTimestampFilterPrefixSettings()
      : DBBasicTestWithTimestampBase(
            "db_basic_test_with_timestamp_filter_prefix") {}
};

TEST_P(DBBasicTestWithTimestampFilterPrefixSettings, GetAndMultiGet) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  BlockBasedTableOptions bbto;
  bbto.filter_policy = std::get<0>(GetParam());
  bbto.whole_key_filtering = std::get<1>(GetParam());
  bbto.cache_index_and_filter_blocks = std::get<2>(GetParam());
  bbto.index_type = std::get<6>(GetParam());
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  options.prefix_extractor = std::get<3>(GetParam());
  options.memtable_whole_key_filtering = std::get<4>(GetParam());
  options.memtable_prefix_bloom_size_ratio = std::get<5>(GetParam());

  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  const int kMaxKey = 1000;

  // Write any value
  WriteOptions write_opts;
  std::string ts = Timestamp(1, 0);

  int idx = 0;
  for (; idx < kMaxKey / 4; idx++) {
    ASSERT_OK(db_->Put(write_opts, Key1(idx), ts, "bar"));
    ASSERT_OK(db_->Put(write_opts, KeyWithPrefix("foo", idx), ts, "bar"));
  }

  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  for (; idx < kMaxKey / 2; idx++) {
    ASSERT_OK(db_->Put(write_opts, Key1(idx), ts, "bar"));
    ASSERT_OK(db_->Put(write_opts, KeyWithPrefix("foo", idx), ts, "bar"));
  }

  ASSERT_OK(Flush());

  for (; idx < kMaxKey; idx++) {
    ASSERT_OK(db_->Put(write_opts, Key1(idx), ts, "bar"));
    ASSERT_OK(db_->Put(write_opts, KeyWithPrefix("foo", idx), ts, "bar"));
  }

  // Read with MultiGet
  ReadOptions read_opts;
  Slice read_ts = ts;
  read_opts.timestamp = &read_ts;

  for (idx = 0; idx < kMaxKey; idx++) {
    size_t batch_size = 4;
    std::vector<std::string> keys_str(batch_size);
    std::vector<PinnableSlice> values(batch_size);
    std::vector<Status> statuses(batch_size);
    ColumnFamilyHandle* cfh = db_->DefaultColumnFamily();

    keys_str[0] = Key1(idx);
    keys_str[1] = KeyWithPrefix("foo", idx);
    keys_str[2] = Key1(kMaxKey + idx);
    keys_str[3] = KeyWithPrefix("foo", kMaxKey + idx);

    auto keys = ConvertStrToSlice(keys_str);

    db_->MultiGet(read_opts, cfh, batch_size, keys.data(), values.data(),
                  statuses.data());

    for (int i = 0; i < 2; i++) {
      ASSERT_OK(statuses[i]);
    }
    for (int i = 2; i < 4; i++) {
      ASSERT_TRUE(statuses[i].IsNotFound());
    }

    for (int i = 0; i < 2; i++) {
      std::string value;
      ASSERT_OK(db_->Get(read_opts, keys[i], &value));
      std::unique_ptr<Iterator> it1(db_->NewIterator(read_opts));
      ASSERT_NE(nullptr, it1);
      ASSERT_OK(it1->status());
      it1->Seek(keys[i]);
      ASSERT_TRUE(it1->Valid());
    }

    for (int i = 2; i < 4; i++) {
      std::string value;
      Status s = db_->Get(read_opts, keys[i], &value);
      ASSERT_TRUE(s.IsNotFound());
    }
  }
  Close();
}

INSTANTIATE_TEST_CASE_P(
    Timestamp, DBBasicTestWithTimestampFilterPrefixSettings,
    ::testing::Combine(
        ::testing::Values(
            std::shared_ptr<const FilterPolicy>(nullptr),
            std::shared_ptr<const FilterPolicy>(NewBloomFilterPolicy(10, true)),
            std::shared_ptr<const FilterPolicy>(NewBloomFilterPolicy(10,
                                                                     false))),
        ::testing::Bool(), ::testing::Bool(),
        ::testing::Values(
            std::shared_ptr<const SliceTransform>(NewFixedPrefixTransform(1)),
            std::shared_ptr<const SliceTransform>(NewFixedPrefixTransform(4)),
            std::shared_ptr<const SliceTransform>(NewFixedPrefixTransform(7)),
            std::shared_ptr<const SliceTransform>(NewFixedPrefixTransform(8))),
        ::testing::Bool(), ::testing::Values(0, 0.1),
        ::testing::Values(
            BlockBasedTableOptions::IndexType::kBinarySearch,
            BlockBasedTableOptions::IndexType::kHashSearch,
            BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch,
            BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey)));
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

class DataVisibilityTest : public DBBasicTestWithTimestampBase {
 public:
  DataVisibilityTest() : DBBasicTestWithTimestampBase("data_visibility_test") {
    // Initialize test data
    for (int i = 0; i < kTestDataSize; i++) {
      test_data_[i].key = "key" + std::to_string(i);
      test_data_[i].value = "value" + std::to_string(i);
      test_data_[i].timestamp = Timestamp(i, 0);
      test_data_[i].ts = i;
      test_data_[i].seq_num = kMaxSequenceNumber;
    }
  }

 protected:
  struct TestData {
    std::string key;
    std::string value;
    int ts;
    std::string timestamp;
    SequenceNumber seq_num;
  };

  constexpr static int kTestDataSize = 3;
  TestData test_data_[kTestDataSize];

  void PutTestData(int index, ColumnFamilyHandle* cfh = nullptr) {
    ASSERT_LE(index, kTestDataSize);
    WriteOptions write_opts;

    if (cfh == nullptr) {
      ASSERT_OK(db_->Put(write_opts, test_data_[index].key,
                         test_data_[index].timestamp, test_data_[index].value));
      const Snapshot* snap = db_->GetSnapshot();
      test_data_[index].seq_num = snap->GetSequenceNumber();
      if (index > 0) {
        ASSERT_GT(test_data_[index].seq_num, test_data_[index - 1].seq_num);
      }
      db_->ReleaseSnapshot(snap);
    } else {
      ASSERT_OK(db_->Put(write_opts, cfh, test_data_[index].key,
                         test_data_[index].timestamp, test_data_[index].value));
    }
  }

  void AssertVisibility(int ts, SequenceNumber seq,
                        std::vector<Status> statuses) {
    ASSERT_EQ(kTestDataSize, statuses.size());
    for (int i = 0; i < kTestDataSize; i++) {
      if (test_data_[i].seq_num <= seq && test_data_[i].ts <= ts) {
        ASSERT_OK(statuses[i]);
      } else {
        ASSERT_TRUE(statuses[i].IsNotFound());
      }
    }
  }

  std::vector<Slice> GetKeys() {
    std::vector<Slice> ret(kTestDataSize);
    for (int i = 0; i < kTestDataSize; i++) {
      ret[i] = test_data_[i].key;
    }
    return ret;
  }

  void VerifyDefaultCF(int ts, const Snapshot* snap = nullptr) {
    ReadOptions read_opts;
    std::string read_ts = Timestamp(ts, 0);
    Slice read_ts_slice = read_ts;
    read_opts.timestamp = &read_ts_slice;
    read_opts.snapshot = snap;

    ColumnFamilyHandle* cfh = db_->DefaultColumnFamily();
    std::vector<ColumnFamilyHandle*> cfs(kTestDataSize, cfh);
    SequenceNumber seq =
        snap ? snap->GetSequenceNumber() : kMaxSequenceNumber - 1;

    // There're several MultiGet interfaces with not exactly the same
    // implementations, query data with all of them.
    auto keys = GetKeys();
    std::vector<std::string> values;
    auto s1 = db_->MultiGet(read_opts, cfs, keys, &values);
    AssertVisibility(ts, seq, s1);

    auto s2 = db_->MultiGet(read_opts, keys, &values);
    AssertVisibility(ts, seq, s2);

    std::vector<std::string> timestamps;
    auto s3 = db_->MultiGet(read_opts, cfs, keys, &values, &timestamps);
    AssertVisibility(ts, seq, s3);

    auto s4 = db_->MultiGet(read_opts, keys, &values, &timestamps);
    AssertVisibility(ts, seq, s4);

    std::vector<PinnableSlice> values_ps5(kTestDataSize);
    std::vector<Status> s5(kTestDataSize);
    db_->MultiGet(read_opts, cfh, kTestDataSize, keys.data(), values_ps5.data(),
                  s5.data());
    AssertVisibility(ts, seq, s5);

    std::vector<PinnableSlice> values_ps6(kTestDataSize);
    std::vector<Status> s6(kTestDataSize);
    std::vector<std::string> timestamps_array(kTestDataSize);
    db_->MultiGet(read_opts, cfh, kTestDataSize, keys.data(), values_ps6.data(),
                  timestamps_array.data(), s6.data());
    AssertVisibility(ts, seq, s6);

    std::vector<PinnableSlice> values_ps7(kTestDataSize);
    std::vector<Status> s7(kTestDataSize);
    db_->MultiGet(read_opts, kTestDataSize, cfs.data(), keys.data(),
                  values_ps7.data(), s7.data());
    AssertVisibility(ts, seq, s7);

    std::vector<PinnableSlice> values_ps8(kTestDataSize);
    std::vector<Status> s8(kTestDataSize);
    db_->MultiGet(read_opts, kTestDataSize, cfs.data(), keys.data(),
                  values_ps8.data(), timestamps_array.data(), s8.data());
    AssertVisibility(ts, seq, s8);
  }

  void VerifyDefaultCF(const Snapshot* snap = nullptr) {
    for (int i = 0; i <= kTestDataSize; i++) {
      VerifyDefaultCF(i, snap);
    }
  }
};

// Application specifies timestamp but not snapshot.
//           reader              writer
//                               ts'=90
//           ts=100
//           seq=10
//                               seq'=11
//                               write finishes
//         GetImpl(ts,seq)
// It is OK to return <k, t1, s1> if ts>=t1 AND seq>=s1. If ts>=t1 but seq<s1,
// the key should not be returned.
TEST_F(DataVisibilityTest, PointLookupWithoutSnapshot1) {
  Options options = CurrentOptions();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::GetImpl:3",
       "DataVisibilityTest::PointLookupWithoutSnapshot1:BeforePut"},
      {"DataVisibilityTest::PointLookupWithoutSnapshot1:AfterPut",
       "DBImpl::GetImpl:4"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  port::Thread writer_thread([this]() {
    std::string write_ts = Timestamp(1, 0);
    WriteOptions write_opts;
    TEST_SYNC_POINT(
        "DataVisibilityTest::PointLookupWithoutSnapshot1:BeforePut");
    Status s = db_->Put(write_opts, "foo", write_ts, "value");
    ASSERT_OK(s);
    TEST_SYNC_POINT("DataVisibilityTest::PointLookupWithoutSnapshot1:AfterPut");
  });
  ReadOptions read_opts;
  std::string read_ts_str = Timestamp(3, 0);
  Slice read_ts = read_ts_str;
  read_opts.timestamp = &read_ts;
  std::string value;
  Status s = db_->Get(read_opts, "foo", &value);

  writer_thread.join();
  ASSERT_TRUE(s.IsNotFound());
  Close();
}

// Application specifies timestamp but not snapshot.
//           reader              writer
//                               ts'=90
//           ts=100
//           seq=10
//                               seq'=11
//                               write finishes
//                               Flush
//         GetImpl(ts,seq)
// It is OK to return <k, t1, s1> if ts>=t1 AND seq>=s1. If ts>=t1 but seq<s1,
// the key should not be returned.
TEST_F(DataVisibilityTest, PointLookupWithoutSnapshot2) {
  Options options = CurrentOptions();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::GetImpl:3",
       "DataVisibilityTest::PointLookupWithoutSnapshot2:BeforePut"},
      {"DataVisibilityTest::PointLookupWithoutSnapshot2:AfterPut",
       "DBImpl::GetImpl:4"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  port::Thread writer_thread([this]() {
    std::string write_ts = Timestamp(1, 0);
    WriteOptions write_opts;
    TEST_SYNC_POINT(
        "DataVisibilityTest::PointLookupWithoutSnapshot2:BeforePut");
    Status s = db_->Put(write_opts, "foo", write_ts, "value");
    ASSERT_OK(s);
    ASSERT_OK(Flush());

    write_ts = Timestamp(2, 0);
    s = db_->Put(write_opts, "bar", write_ts, "value");
    ASSERT_OK(s);
    TEST_SYNC_POINT("DataVisibilityTest::PointLookupWithoutSnapshot2:AfterPut");
  });
  ReadOptions read_opts;
  std::string read_ts_str = Timestamp(3, 0);
  Slice read_ts = read_ts_str;
  read_opts.timestamp = &read_ts;
  std::string value;
  Status s = db_->Get(read_opts, "foo", &value);
  writer_thread.join();
  ASSERT_TRUE(s.IsNotFound());
  Close();
}

// Application specifies both timestamp and snapshot.
//       reader               writer
//       seq=10
//                            ts'=90
//       ts=100
//                            seq'=11
//                            write finishes
//       GetImpl(ts,seq)
// Since application specifies both timestamp and snapshot, application expects
// to see data that visible in BOTH timestamp and sequence number. Therefore,
// <k, t1, s1> can be returned only if t1<=ts AND s1<=seq.
TEST_F(DataVisibilityTest, PointLookupWithSnapshot1) {
  Options options = CurrentOptions();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({
      {"DataVisibilityTest::PointLookupWithSnapshot1:AfterTakingSnap",
       "DataVisibilityTest::PointLookupWithSnapshot1:BeforePut"},
      {"DataVisibilityTest::PointLookupWithSnapshot1:AfterPut",
       "DBImpl::GetImpl:1"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  port::Thread writer_thread([this]() {
    std::string write_ts = Timestamp(1, 0);
    WriteOptions write_opts;
    TEST_SYNC_POINT("DataVisibilityTest::PointLookupWithSnapshot1:BeforePut");
    Status s = db_->Put(write_opts, "foo", write_ts, "value");
    TEST_SYNC_POINT("DataVisibilityTest::PointLookupWithSnapshot1:AfterPut");
    ASSERT_OK(s);
  });
  ReadOptions read_opts;
  const Snapshot* snap = db_->GetSnapshot();
  TEST_SYNC_POINT(
      "DataVisibilityTest::PointLookupWithSnapshot1:AfterTakingSnap");
  read_opts.snapshot = snap;
  std::string read_ts_str = Timestamp(3, 0);
  Slice read_ts = read_ts_str;
  read_opts.timestamp = &read_ts;
  std::string value;
  Status s = db_->Get(read_opts, "foo", &value);
  writer_thread.join();

  ASSERT_TRUE(s.IsNotFound());

  db_->ReleaseSnapshot(snap);
  Close();
}

// Application specifies both timestamp and snapshot.
//       reader               writer
//       seq=10
//                            ts'=90
//       ts=100
//                            seq'=11
//                            write finishes
//                            Flush
//       GetImpl(ts,seq)
// Since application specifies both timestamp and snapshot, application expects
// to see data that visible in BOTH timestamp and sequence number. Therefore,
// <k, t1, s1> can be returned only if t1<=ts AND s1<=seq.
TEST_F(DataVisibilityTest, PointLookupWithSnapshot2) {
  Options options = CurrentOptions();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({
      {"DataVisibilityTest::PointLookupWithSnapshot2:AfterTakingSnap",
       "DataVisibilityTest::PointLookupWithSnapshot2:BeforePut"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  port::Thread writer_thread([this]() {
    std::string write_ts = Timestamp(1, 0);
    WriteOptions write_opts;
    TEST_SYNC_POINT("DataVisibilityTest::PointLookupWithSnapshot2:BeforePut");
    Status s = db_->Put(write_opts, "foo", write_ts, "value1");
    ASSERT_OK(s);
    ASSERT_OK(Flush());

    write_ts = Timestamp(2, 0);
    s = db_->Put(write_opts, "bar", write_ts, "value2");
    ASSERT_OK(s);
  });
  const Snapshot* snap = db_->GetSnapshot();
  TEST_SYNC_POINT(
      "DataVisibilityTest::PointLookupWithSnapshot2:AfterTakingSnap");
  writer_thread.join();
  std::string read_ts_str = Timestamp(3, 0);
  Slice read_ts = read_ts_str;
  ReadOptions read_opts;
  read_opts.snapshot = snap;
  read_opts.timestamp = &read_ts;
  std::string value;
  Status s = db_->Get(read_opts, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());
  db_->ReleaseSnapshot(snap);
  Close();
}

// Application specifies timestamp but not snapshot.
//      reader                writer
//                            ts'=90
//      ts=100
//      seq=10
//                            seq'=11
//                            write finishes
//      scan(ts,seq)
// <k, t1, s1> can be seen in scan as long as ts>=t1 AND seq>=s1. If ts>=t1 but
// seq<s1, then the key should not be returned.
TEST_F(DataVisibilityTest, RangeScanWithoutSnapshot) {
  Options options = CurrentOptions();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::NewIterator:3",
       "DataVisibilityTest::RangeScanWithoutSnapshot:BeforePut"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  port::Thread writer_thread([this]() {
    WriteOptions write_opts;
    TEST_SYNC_POINT("DataVisibilityTest::RangeScanWithoutSnapshot:BeforePut");
    for (int i = 0; i < 3; ++i) {
      std::string write_ts = Timestamp(i + 1, 0);
      Status s = db_->Put(write_opts, "key" + std::to_string(i), write_ts,
                          "value" + std::to_string(i));
      ASSERT_OK(s);
    }
  });
  std::string read_ts_str = Timestamp(10, 0);
  Slice read_ts = read_ts_str;
  ReadOptions read_opts;
  read_opts.total_order_seek = true;
  read_opts.timestamp = &read_ts;
  Iterator* it = db_->NewIterator(read_opts);
  ASSERT_NE(nullptr, it);
  writer_thread.join();
  it->SeekToFirst();
  ASSERT_FALSE(it->Valid());
  delete it;
  Close();
}

// Application specifies both timestamp and snapshot.
//       reader         writer
//       seq=10
//                      ts'=90
//       ts=100         seq'=11
//                      write finishes
//       scan(ts,seq)
// <k, t1, s1> can be seen by the scan only if t1<=ts AND s1<=seq. If t1<=ts
// but s1>seq, then the key should not be returned.
TEST_F(DataVisibilityTest, RangeScanWithSnapshot) {
  Options options = CurrentOptions();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({
      {"DataVisibilityTest::RangeScanWithSnapshot:AfterTakingSnapshot",
       "DataVisibilityTest::RangeScanWithSnapshot:BeforePut"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  port::Thread writer_thread([this]() {
    WriteOptions write_opts;
    TEST_SYNC_POINT("DataVisibilityTest::RangeScanWithSnapshot:BeforePut");
    for (int i = 0; i < 3; ++i) {
      std::string write_ts = Timestamp(i + 1, 0);
      Status s = db_->Put(write_opts, "key" + std::to_string(i), write_ts,
                          "value" + std::to_string(i));
      ASSERT_OK(s);
    }
  });
  const Snapshot* snap = db_->GetSnapshot();
  TEST_SYNC_POINT(
      "DataVisibilityTest::RangeScanWithSnapshot:AfterTakingSnapshot");

  writer_thread.join();

  std::string read_ts_str = Timestamp(10, 0);
  Slice read_ts = read_ts_str;
  ReadOptions read_opts;
  read_opts.snapshot = snap;
  read_opts.total_order_seek = true;
  read_opts.timestamp = &read_ts;
  Iterator* it = db_->NewIterator(read_opts);
  ASSERT_NE(nullptr, it);
  it->Seek("key0");
  ASSERT_FALSE(it->Valid());

  delete it;
  db_->ReleaseSnapshot(snap);
  Close();
}

// Application specifies both timestamp and snapshot.
// Query each combination and make sure for MultiGet key <k, t1, s1>, only
// return keys that ts>=t1 AND seq>=s1.
TEST_F(DataVisibilityTest, MultiGetWithTimestamp) {
  Options options = CurrentOptions();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  const Snapshot* snap0 = db_->GetSnapshot();
  PutTestData(0);
  VerifyDefaultCF();
  VerifyDefaultCF(snap0);

  const Snapshot* snap1 = db_->GetSnapshot();
  PutTestData(1);
  VerifyDefaultCF();
  VerifyDefaultCF(snap0);
  VerifyDefaultCF(snap1);

  ASSERT_OK(Flush());

  const Snapshot* snap2 = db_->GetSnapshot();
  PutTestData(2);
  VerifyDefaultCF();
  VerifyDefaultCF(snap0);
  VerifyDefaultCF(snap1);
  VerifyDefaultCF(snap2);

  db_->ReleaseSnapshot(snap0);
  db_->ReleaseSnapshot(snap1);
  db_->ReleaseSnapshot(snap2);

  Close();
}

// Application specifies timestamp but not snapshot.
//           reader              writer
//                               ts'=0, 1
//           ts=3
//           seq=10
//                               seq'=11, 12
//                               write finishes
//         MultiGet(ts,seq)
// For MultiGet <k, t1, s1>, only return keys that ts>=t1 AND seq>=s1.
TEST_F(DataVisibilityTest, MultiGetWithoutSnapshot) {
  Options options = CurrentOptions();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::MultiCFSnapshot:AfterGetSeqNum1",
       "DataVisibilityTest::MultiGetWithoutSnapshot:BeforePut"},
      {"DataVisibilityTest::MultiGetWithoutSnapshot:AfterPut",
       "DBImpl::MultiCFSnapshot:AfterGetSeqNum2"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  port::Thread writer_thread([this]() {
    TEST_SYNC_POINT("DataVisibilityTest::MultiGetWithoutSnapshot:BeforePut");
    PutTestData(0);
    PutTestData(1);
    TEST_SYNC_POINT("DataVisibilityTest::MultiGetWithoutSnapshot:AfterPut");
  });

  ReadOptions read_opts;
  std::string read_ts = Timestamp(kTestDataSize, 0);
  Slice read_ts_slice = read_ts;
  read_opts.timestamp = &read_ts_slice;
  auto keys = GetKeys();
  std::vector<std::string> values;
  auto ss = db_->MultiGet(read_opts, keys, &values);

  writer_thread.join();
  for (const auto& s : ss) {
    ASSERT_TRUE(s.IsNotFound());
  }
  VerifyDefaultCF();
  Close();
}

TEST_F(DataVisibilityTest, MultiGetCrossCF) {
  Options options = CurrentOptions();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  CreateAndReopenWithCF({"second"}, options);
  ColumnFamilyHandle* second_cf = handles_[1];

  const Snapshot* snap0 = db_->GetSnapshot();
  PutTestData(0);
  PutTestData(0, second_cf);
  VerifyDefaultCF();
  VerifyDefaultCF(snap0);

  const Snapshot* snap1 = db_->GetSnapshot();
  PutTestData(1);
  PutTestData(1, second_cf);
  VerifyDefaultCF();
  VerifyDefaultCF(snap0);
  VerifyDefaultCF(snap1);

  ASSERT_OK(Flush());

  const Snapshot* snap2 = db_->GetSnapshot();
  PutTestData(2);
  PutTestData(2, second_cf);
  VerifyDefaultCF();
  VerifyDefaultCF(snap0);
  VerifyDefaultCF(snap1);
  VerifyDefaultCF(snap2);

  ReadOptions read_opts;
  std::string read_ts = Timestamp(kTestDataSize, 0);
  Slice read_ts_slice = read_ts;
  read_opts.timestamp = &read_ts_slice;
  read_opts.snapshot = snap1;
  auto keys = GetKeys();
  auto keys2 = GetKeys();
  keys.insert(keys.end(), keys2.begin(), keys2.end());
  std::vector<ColumnFamilyHandle*> cfs(kTestDataSize,
                                       db_->DefaultColumnFamily());
  std::vector<ColumnFamilyHandle*> cfs2(kTestDataSize, second_cf);
  cfs.insert(cfs.end(), cfs2.begin(), cfs2.end());

  std::vector<std::string> values;
  auto ss = db_->MultiGet(read_opts, cfs, keys, &values);
  for (int i = 0; i < 2 * kTestDataSize; i++) {
    if (i % 3 == 0) {
      // only the first key for each column family should be returned
      ASSERT_OK(ss[i]);
    } else {
      ASSERT_TRUE(ss[i].IsNotFound());
    }
  }

  db_->ReleaseSnapshot(snap0);
  db_->ReleaseSnapshot(snap1);
  db_->ReleaseSnapshot(snap2);
  Close();
}

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
class DBBasicTestWithTimestampCompressionSettings
    : public DBBasicTestWithTimestampBase,
      public testing::WithParamInterface<
          std::tuple<std::shared_ptr<const FilterPolicy>, CompressionType,
                     uint32_t, uint32_t>> {
 public:
  DBBasicTestWithTimestampCompressionSettings()
      : DBBasicTestWithTimestampBase(
            "db_basic_test_with_timestamp_compression") {}
};

TEST_P(DBBasicTestWithTimestampCompressionSettings, PutAndGet) {
  const int kNumKeysPerFile = 1024;
  const size_t kNumTimestamps = 4;
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.env = env_;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  size_t ts_sz = Timestamp(0, 0).size();
  TestComparator test_cmp(ts_sz);
  options.comparator = &test_cmp;
  BlockBasedTableOptions bbto;
  bbto.filter_policy = std::get<0>(GetParam());
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  const CompressionType comp_type = std::get<1>(GetParam());
#if LZ4_VERSION_NUMBER < 10400  // r124+
  if (comp_type == kLZ4Compression || comp_type == kLZ4HCCompression) {
    return;
  }
#endif  // LZ4_VERSION_NUMBER >= 10400
  if (!ZSTD_Supported() && comp_type == kZSTD) {
    return;
  }
  if (!Zlib_Supported() && comp_type == kZlibCompression) {
    return;
  }

  options.compression = comp_type;
  options.compression_opts.max_dict_bytes = std::get<2>(GetParam());
  if (comp_type == kZSTD) {
    options.compression_opts.zstd_max_train_bytes = std::get<2>(GetParam());
  }
  options.compression_opts.parallel_threads = std::get<3>(GetParam());
  options.target_file_size_base = 1 << 26;  // 64MB
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  size_t num_cfs = handles_.size();
  ASSERT_EQ(2, num_cfs);
  std::vector<std::string> write_ts_list;
  std::vector<std::string> read_ts_list;

  for (size_t i = 0; i != kNumTimestamps; ++i) {
    write_ts_list.push_back(Timestamp(i * 2, 0));
    read_ts_list.push_back(Timestamp(1 + i * 2, 0));
    const Slice write_ts = write_ts_list.back();
    WriteOptions wopts;
    for (int cf = 0; cf != static_cast<int>(num_cfs); ++cf) {
      for (size_t j = 0; j != (kNumKeysPerFile - 1) / kNumTimestamps; ++j) {
        ASSERT_OK(
            db_->Put(wopts, handles_[cf], Key1(j), write_ts,
                     "value_" + std::to_string(j) + "_" + std::to_string(i)));
      }
    }
  }
  const auto& verify_db_func = [&]() {
    for (size_t i = 0; i != kNumTimestamps; ++i) {
      ReadOptions ropts;
      const Slice read_ts = read_ts_list[i];
      ropts.timestamp = &read_ts;
      for (int cf = 0; cf != static_cast<int>(num_cfs); ++cf) {
        ColumnFamilyHandle* cfh = handles_[cf];
        for (size_t j = 0; j != (kNumKeysPerFile - 1) / kNumTimestamps; ++j) {
          std::string value;
          ASSERT_OK(db_->Get(ropts, cfh, Key1(j), &value));
          ASSERT_EQ("value_" + std::to_string(j) + "_" + std::to_string(i),
                    value);
        }
      }
    }
  };
  verify_db_func();
  Close();
}

TEST_P(DBBasicTestWithTimestampCompressionSettings, PutDeleteGet) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  const int kNumKeysPerFile = 1024;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  BlockBasedTableOptions bbto;
  bbto.filter_policy = std::get<0>(GetParam());
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  const CompressionType comp_type = std::get<1>(GetParam());
#if LZ4_VERSION_NUMBER < 10400  // r124+
  if (comp_type == kLZ4Compression || comp_type == kLZ4HCCompression) {
    return;
  }
#endif  // LZ4_VERSION_NUMBER >= 10400
  if (!ZSTD_Supported() && comp_type == kZSTD) {
    return;
  }
  if (!Zlib_Supported() && comp_type == kZlibCompression) {
    return;
  }

  options.compression = comp_type;
  options.compression_opts.max_dict_bytes = std::get<2>(GetParam());
  if (comp_type == kZSTD) {
    options.compression_opts.zstd_max_train_bytes = std::get<2>(GetParam());
  }
  options.compression_opts.parallel_threads = std::get<3>(GetParam());
  options.target_file_size_base = 1 << 26;  // 64MB

  DestroyAndReopen(options);

  const size_t kNumL0Files =
      static_cast<size_t>(Options().level0_file_num_compaction_trigger);
  {
    // Half of the keys will go through Deletion and remaining half with
    // SingleDeletion. Generate enough L0 files with ts=1 to trigger compaction
    // to L1
    std::string ts = Timestamp(1, 0);
    WriteOptions wopts;
    for (size_t i = 0; i < kNumL0Files; ++i) {
      for (int j = 0; j < kNumKeysPerFile; ++j) {
        ASSERT_OK(db_->Put(wopts, Key1(j), ts, "value" + std::to_string(i)));
      }
      ASSERT_OK(db_->Flush(FlushOptions()));
    }
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    // Generate another L0 at ts=3
    ts = Timestamp(3, 0);
    for (int i = 0; i < kNumKeysPerFile; ++i) {
      std::string key_str = Key1(i);
      Slice key(key_str);
      if ((i % 3) == 0) {
        if (i < kNumKeysPerFile / 2) {
          ASSERT_OK(db_->Delete(wopts, key, ts));
        } else {
          ASSERT_OK(db_->SingleDelete(wopts, key, ts));
        }
      } else {
        ASSERT_OK(db_->Put(wopts, key, ts, "new_value"));
      }
    }
    ASSERT_OK(db_->Flush(FlushOptions()));
    // Populate memtable at ts=5
    ts = Timestamp(5, 0);
    for (int i = 0; i != kNumKeysPerFile; ++i) {
      std::string key_str = Key1(i);
      Slice key(key_str);
      if ((i % 3) == 1) {
        if (i < kNumKeysPerFile / 2) {
          ASSERT_OK(db_->Delete(wopts, key, ts));
        } else {
          ASSERT_OK(db_->SingleDelete(wopts, key, ts));
        }
      } else if ((i % 3) == 2) {
        ASSERT_OK(db_->Put(wopts, key, ts, "new_value_2"));
      }
    }
  }
  {
    std::string ts_str = Timestamp(6, 0);
    Slice ts = ts_str;
    ReadOptions ropts;
    ropts.timestamp = &ts;
    for (uint64_t i = 0; i != static_cast<uint64_t>(kNumKeysPerFile); ++i) {
      std::string value;
      std::string key_ts;
      Status s = db_->Get(ropts, Key1(i), &value, &key_ts);
      if ((i % 3) == 2) {
        ASSERT_OK(s);
        ASSERT_EQ("new_value_2", value);
        ASSERT_EQ(Timestamp(5, 0), key_ts);
      } else if ((i % 3) == 1) {
        ASSERT_TRUE(s.IsNotFound());
        ASSERT_EQ(Timestamp(5, 0), key_ts);
      } else {
        ASSERT_TRUE(s.IsNotFound());
        ASSERT_EQ(Timestamp(3, 0), key_ts);
      }
    }
  }
}

// A class which remembers the name of each flushed file.
class FlushedFileCollector : public EventListener {
 public:
  FlushedFileCollector() = default;
  ~FlushedFileCollector() override = default;

  void OnFlushCompleted(DB* /*db*/, const FlushJobInfo& info) override {
    InstrumentedMutexLock lock(&mutex_);
    flushed_files_.push_back(info.file_path);
  }

  std::vector<std::string> GetFlushedFiles() {
    std::vector<std::string> result;
    {
      InstrumentedMutexLock lock(&mutex_);
      result = flushed_files_;
    }
    return result;
  }

  void ClearFlushedFiles() {
    InstrumentedMutexLock lock(&mutex_);
    flushed_files_.clear();
  }

 private:
  std::vector<std::string> flushed_files_;
  InstrumentedMutex mutex_;
};

TEST_P(DBBasicTestWithTimestampCompressionSettings, PutAndGetWithCompaction) {
  const int kNumKeysPerFile = 1024;
  const size_t kNumTimestamps = 2;
  const size_t kNumKeysPerTimestamp = (kNumKeysPerFile - 1) / kNumTimestamps;
  const size_t kSplitPosBase = kNumKeysPerTimestamp / 2;
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.env = env_;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));

  FlushedFileCollector* collector = new FlushedFileCollector();
  options.listeners.emplace_back(collector);

  size_t ts_sz = Timestamp(0, 0).size();
  TestComparator test_cmp(ts_sz);
  options.comparator = &test_cmp;
  BlockBasedTableOptions bbto;
  bbto.filter_policy = std::get<0>(GetParam());
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  const CompressionType comp_type = std::get<1>(GetParam());
#if LZ4_VERSION_NUMBER < 10400  // r124+
  if (comp_type == kLZ4Compression || comp_type == kLZ4HCCompression) {
    return;
  }
#endif  // LZ4_VERSION_NUMBER >= 10400
  if (!ZSTD_Supported() && comp_type == kZSTD) {
    return;
  }
  if (!Zlib_Supported() && comp_type == kZlibCompression) {
    return;
  }

  options.compression = comp_type;
  options.compression_opts.max_dict_bytes = std::get<2>(GetParam());
  if (comp_type == kZSTD) {
    options.compression_opts.zstd_max_train_bytes = std::get<2>(GetParam());
  }
  options.compression_opts.parallel_threads = std::get<3>(GetParam());
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  size_t num_cfs = handles_.size();
  ASSERT_EQ(2, num_cfs);
  std::vector<std::string> write_ts_list;
  std::vector<std::string> read_ts_list;

  const auto& verify_records_func = [&](size_t i, size_t begin, size_t end,
                                        ColumnFamilyHandle* cfh) {
    std::string value;
    std::string timestamp;

    ReadOptions ropts;
    const Slice read_ts = read_ts_list[i];
    ropts.timestamp = &read_ts;
    std::string expected_timestamp =
        std::string(write_ts_list[i].data(), write_ts_list[i].size());

    for (size_t j = begin; j <= end; ++j) {
      ASSERT_OK(db_->Get(ropts, cfh, Key1(j), &value, &timestamp));
      ASSERT_EQ("value_" + std::to_string(j) + "_" + std::to_string(i), value);
      ASSERT_EQ(expected_timestamp, timestamp);
    }
  };

  for (size_t i = 0; i != kNumTimestamps; ++i) {
    write_ts_list.push_back(Timestamp(i * 2, 0));
    read_ts_list.push_back(Timestamp(1 + i * 2, 0));
    const Slice write_ts = write_ts_list.back();
    WriteOptions wopts;
    for (int cf = 0; cf != static_cast<int>(num_cfs); ++cf) {
      size_t memtable_get_start = 0;
      for (size_t j = 0; j != kNumKeysPerTimestamp; ++j) {
        ASSERT_OK(
            db_->Put(wopts, handles_[cf], Key1(j), write_ts,
                     "value_" + std::to_string(j) + "_" + std::to_string(i)));
        if (j == kSplitPosBase + i || j == kNumKeysPerTimestamp - 1) {
          verify_records_func(i, memtable_get_start, j, handles_[cf]);
          memtable_get_start = j + 1;

          // flush all keys with the same timestamp to two sst files, split at
          // incremental positions such that lowerlevel[1].smallest.userkey ==
          // higherlevel[0].largest.userkey
          ASSERT_OK(Flush(cf));
          ASSERT_OK(dbfull()->TEST_WaitForCompact());  // wait for flush (which
                                                       // is also a compaction)

          // compact files (2 at each level) to a lower level such that all
          // keys with the same timestamp is at one level, with newer versions
          // at higher levels.
          CompactionOptions compact_opt;
          compact_opt.compression = kNoCompression;
          ASSERT_OK(db_->CompactFiles(compact_opt, handles_[cf],
                                      collector->GetFlushedFiles(),
                                      static_cast<int>(kNumTimestamps - i)));
          collector->ClearFlushedFiles();
        }
      }
    }
  }
  const auto& verify_db_func = [&]() {
    for (size_t i = 0; i != kNumTimestamps; ++i) {
      ReadOptions ropts;
      const Slice read_ts = read_ts_list[i];
      ropts.timestamp = &read_ts;
      std::string expected_timestamp(write_ts_list[i].data(),
                                     write_ts_list[i].size());
      for (int cf = 0; cf != static_cast<int>(num_cfs); ++cf) {
        ColumnFamilyHandle* cfh = handles_[cf];
        verify_records_func(i, 0, kNumKeysPerTimestamp - 1, cfh);
      }
    }
  };
  verify_db_func();
  Close();
}

TEST_F(DBBasicTestWithTimestamp, BatchWriteAndMultiGet) {
  const int kNumKeysPerFile = 8192;
  const size_t kNumTimestamps = 2;
  const size_t kNumKeysPerTimestamp = (kNumKeysPerFile - 1) / kNumTimestamps;
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.env = env_;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  options.memtable_prefix_bloom_size_ratio = 0.1;
  options.memtable_whole_key_filtering = true;

  size_t ts_sz = Timestamp(0, 0).size();
  TestComparator test_cmp(ts_sz);
  options.comparator = &test_cmp;
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(
      10 /*bits_per_key*/, false /*use_block_based_builder*/));
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  size_t num_cfs = handles_.size();
  ASSERT_EQ(2, num_cfs);
  std::vector<std::string> write_ts_list;
  std::vector<std::string> read_ts_list;

  const auto& verify_records_func = [&](size_t i, ColumnFamilyHandle* cfh) {
    std::vector<Slice> keys;
    std::vector<std::string> key_vals;
    std::vector<std::string> values;
    std::vector<std::string> timestamps;

    for (size_t j = 0; j != kNumKeysPerTimestamp; ++j) {
      key_vals.push_back(Key1(j));
    }
    for (size_t j = 0; j != kNumKeysPerTimestamp; ++j) {
      keys.emplace_back(key_vals[j]);
    }

    ReadOptions ropts;
    const Slice read_ts = read_ts_list[i];
    ropts.timestamp = &read_ts;
    std::string expected_timestamp(write_ts_list[i].data(),
                                   write_ts_list[i].size());

    std::vector<ColumnFamilyHandle*> cfhs(keys.size(), cfh);
    std::vector<Status> statuses =
        db_->MultiGet(ropts, cfhs, keys, &values, &timestamps);
    for (size_t j = 0; j != kNumKeysPerTimestamp; ++j) {
      ASSERT_OK(statuses[j]);
      ASSERT_EQ("value_" + std::to_string(j) + "_" + std::to_string(i),
                values[j]);
      ASSERT_EQ(expected_timestamp, timestamps[j]);
    }
  };

  const std::string dummy_ts(ts_sz, '\0');
  for (size_t i = 0; i != kNumTimestamps; ++i) {
    write_ts_list.push_back(Timestamp(i * 2, 0));
    read_ts_list.push_back(Timestamp(1 + i * 2, 0));
    const Slice& write_ts = write_ts_list.back();
    for (int cf = 0; cf != static_cast<int>(num_cfs); ++cf) {
      WriteOptions wopts;
      WriteBatch batch(0, 0, 0, ts_sz);
      for (size_t j = 0; j != kNumKeysPerTimestamp; ++j) {
        const std::string key = Key1(j);
        const std::string value =
            "value_" + std::to_string(j) + "_" + std::to_string(i);
        ASSERT_OK(batch.Put(handles_[cf], key, value));
      }
      ASSERT_OK(batch.UpdateTimestamps(write_ts,
                                       [ts_sz](uint32_t) { return ts_sz; }));
      ASSERT_OK(db_->Write(wopts, &batch));

      verify_records_func(i, handles_[cf]);

      ASSERT_OK(Flush(cf));
    }
  }

  const auto& verify_db_func = [&]() {
    for (size_t i = 0; i != kNumTimestamps; ++i) {
      ReadOptions ropts;
      const Slice read_ts = read_ts_list[i];
      ropts.timestamp = &read_ts;
      for (int cf = 0; cf != static_cast<int>(num_cfs); ++cf) {
        ColumnFamilyHandle* cfh = handles_[cf];
        verify_records_func(i, cfh);
      }
    }
  };
  verify_db_func();
  Close();
}

TEST_F(DBBasicTestWithTimestamp, MultiGetNoReturnTs) {
  Options options = CurrentOptions();
  options.env = env_;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  WriteOptions write_opts;
  std::string ts = Timestamp(1, 0);
  ASSERT_OK(db_->Put(write_opts, "foo", ts, "value"));
  ASSERT_OK(db_->Put(write_opts, "bar", ts, "value"));
  ASSERT_OK(db_->Put(write_opts, "fooxxxxxxxxxxxxxxxx", ts, "value"));
  ASSERT_OK(db_->Put(write_opts, "barxxxxxxxxxxxxxxxx", ts, "value"));
  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();
  ts = Timestamp(2, 0);
  Slice read_ts = ts;
  ReadOptions read_opts;
  read_opts.timestamp = &read_ts;
  {
    ColumnFamilyHandle* column_families[] = {cfh, cfh};
    Slice keys[] = {"foo", "bar"};
    PinnableSlice values[] = {PinnableSlice(), PinnableSlice()};
    Status statuses[] = {Status::OK(), Status::OK()};
    dbfull()->MultiGet(read_opts, /*num_keys=*/2, &column_families[0], &keys[0],
                       &values[0], &statuses[0], /*sorted_input=*/false);
    for (const auto& s : statuses) {
      ASSERT_OK(s);
    }
  }
  {
    ColumnFamilyHandle* column_families[] = {cfh, cfh, cfh, cfh};
    // Make user keys longer than configured timestamp size (16 bytes) to
    // verify RocksDB does not use the trailing bytes 'x' as timestamp.
    Slice keys[] = {"fooxxxxxxxxxxxxxxxx", "barxxxxxxxxxxxxxxxx", "foo", "bar"};
    PinnableSlice values[] = {PinnableSlice(), PinnableSlice(), PinnableSlice(),
                              PinnableSlice()};
    Status statuses[] = {Status::OK(), Status::OK(), Status::OK(),
                         Status::OK()};
    dbfull()->MultiGet(read_opts, /*num_keys=*/4, &column_families[0], &keys[0],
                       &values[0], &statuses[0], /*sorted_input=*/false);
    for (const auto& s : statuses) {
      ASSERT_OK(s);
    }
  }
  Close();
}

INSTANTIATE_TEST_CASE_P(
    Timestamp, DBBasicTestWithTimestampCompressionSettings,
    ::testing::Combine(
        ::testing::Values(std::shared_ptr<const FilterPolicy>(nullptr),
                          std::shared_ptr<const FilterPolicy>(
                              NewBloomFilterPolicy(10, false))),
        ::testing::Values(kNoCompression, kZlibCompression, kLZ4Compression,
                          kLZ4HCCompression, kZSTD),
        ::testing::Values(0, 1 << 14), ::testing::Values(1, 4)));

class DBBasicTestWithTimestampPrefixSeek
    : public DBBasicTestWithTimestampBase,
      public testing::WithParamInterface<
          std::tuple<std::shared_ptr<const SliceTransform>,
                     std::shared_ptr<const FilterPolicy>, bool,
                     BlockBasedTableOptions::IndexType>> {
 public:
  DBBasicTestWithTimestampPrefixSeek()
      : DBBasicTestWithTimestampBase(
            "/db_basic_test_with_timestamp_prefix_seek") {}
};

TEST_P(DBBasicTestWithTimestampPrefixSeek, IterateWithPrefix) {
  const size_t kNumKeysPerFile = 128;
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.prefix_extractor = std::get<0>(GetParam());
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  BlockBasedTableOptions bbto;
  bbto.filter_policy = std::get<1>(GetParam());
  bbto.index_type = std::get<3>(GetParam());
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  const uint64_t kMaxKey = 0xffffffffffffffff;
  const uint64_t kMinKey = 0xfffffffffffff000;
  const std::vector<std::string> write_ts_list = {Timestamp(3, 0xffffffff),
                                                  Timestamp(6, 0xffffffff)};
  WriteOptions write_opts;
  {
    for (size_t i = 0; i != write_ts_list.size(); ++i) {
      for (uint64_t key = kMaxKey; key >= kMinKey; --key) {
        Status s = db_->Put(write_opts, Key1(key), write_ts_list[i],
                            "value" + std::to_string(i));
        ASSERT_OK(s);
      }
    }
  }
  const std::vector<std::string> read_ts_list = {Timestamp(5, 0xffffffff),
                                                 Timestamp(9, 0xffffffff)};
  {
    ReadOptions read_opts;
    read_opts.total_order_seek = false;
    read_opts.prefix_same_as_start = std::get<2>(GetParam());
    fprintf(stdout, "%s %s %d\n", options.prefix_extractor->Name(),
            bbto.filter_policy ? bbto.filter_policy->Name() : "null",
            static_cast<int>(read_opts.prefix_same_as_start));
    for (size_t i = 0; i != read_ts_list.size(); ++i) {
      Slice read_ts = read_ts_list[i];
      read_opts.timestamp = &read_ts;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));

      // Seek to kMaxKey
      iter->Seek(Key1(kMaxKey));
      CheckIterUserEntry(iter.get(), Key1(kMaxKey), kTypeValue,
                         "value" + std::to_string(i), write_ts_list[i]);
      iter->Next();
      ASSERT_FALSE(iter->Valid());
      ASSERT_OK(iter->status());

      // Seek to kMinKey
      iter->Seek(Key1(kMinKey));
      CheckIterUserEntry(iter.get(), Key1(kMinKey), kTypeValue,
                         "value" + std::to_string(i), write_ts_list[i]);
      iter->Prev();
      ASSERT_FALSE(iter->Valid());
      ASSERT_OK(iter->status());
    }
    const std::vector<uint64_t> targets = {kMinKey, kMinKey + 0x10,
                                           kMinKey + 0x100, kMaxKey};
    const SliceTransform* const pe = options.prefix_extractor.get();
    ASSERT_NE(nullptr, pe);
    const size_t kPrefixShift =
        8 * (Key1(0).size() - pe->Transform(Key1(0)).size());
    const uint64_t kPrefixMask =
        ~((static_cast<uint64_t>(1) << kPrefixShift) - 1);
    const uint64_t kNumKeysWithinPrefix =
        (static_cast<uint64_t>(1) << kPrefixShift);
    for (size_t i = 0; i != read_ts_list.size(); ++i) {
      Slice read_ts = read_ts_list[i];
      read_opts.timestamp = &read_ts;
      std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));
      // Forward and backward iterate.
      for (size_t j = 0; j != targets.size(); ++j) {
        std::string start_key = Key1(targets[j]);
        uint64_t expected_ub =
            (targets[j] & kPrefixMask) - 1 + kNumKeysWithinPrefix;
        uint64_t expected_key = targets[j];
        size_t count = 0;
        it->Seek(Key1(targets[j]));
        while (it->Valid()) {
          std::string saved_prev_key;
          saved_prev_key.assign(it->key().data(), it->key().size());

          // Out of prefix
          if (!read_opts.prefix_same_as_start &&
              pe->Transform(saved_prev_key) != pe->Transform(start_key)) {
            break;
          }
          CheckIterUserEntry(it.get(), Key1(expected_key), kTypeValue,
                             "value" + std::to_string(i), write_ts_list[i]);
          ++count;
          ++expected_key;
          it->Next();
        }
        ASSERT_OK(it->status());
        ASSERT_EQ(expected_ub - targets[j] + 1, count);

        count = 0;
        expected_key = targets[j];
        it->SeekForPrev(start_key);
        uint64_t expected_lb = (targets[j] & kPrefixMask);
        while (it->Valid()) {
          // Out of prefix
          if (!read_opts.prefix_same_as_start &&
              pe->Transform(it->key()) != pe->Transform(start_key)) {
            break;
          }
          CheckIterUserEntry(it.get(), Key1(expected_key), kTypeValue,
                             "value" + std::to_string(i), write_ts_list[i]);
          ++count;
          --expected_key;
          it->Prev();
        }
        ASSERT_OK(it->status());
        ASSERT_EQ(targets[j] - std::max(expected_lb, kMinKey) + 1, count);
      }
    }
  }
  Close();
}

// TODO(yanqin): consider handling non-fixed-length prefix extractors, e.g.
// NoopTransform.
INSTANTIATE_TEST_CASE_P(
    Timestamp, DBBasicTestWithTimestampPrefixSeek,
    ::testing::Combine(
        ::testing::Values(
            std::shared_ptr<const SliceTransform>(NewFixedPrefixTransform(1)),
            std::shared_ptr<const SliceTransform>(NewFixedPrefixTransform(4)),
            std::shared_ptr<const SliceTransform>(NewFixedPrefixTransform(7)),
            std::shared_ptr<const SliceTransform>(NewFixedPrefixTransform(8))),
        ::testing::Values(std::shared_ptr<const FilterPolicy>(nullptr),
                          std::shared_ptr<const FilterPolicy>(
                              NewBloomFilterPolicy(10 /*bits_per_key*/, false)),
                          std::shared_ptr<const FilterPolicy>(
                              NewBloomFilterPolicy(20 /*bits_per_key*/,
                                                   false))),
        ::testing::Bool(),
        ::testing::Values(
            BlockBasedTableOptions::IndexType::kBinarySearch,
            BlockBasedTableOptions::IndexType::kHashSearch,
            BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch,
            BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey)));

class DBBasicTestWithTsIterTombstones
    : public DBBasicTestWithTimestampBase,
      public testing::WithParamInterface<
          std::tuple<std::shared_ptr<const SliceTransform>,
                     std::shared_ptr<const FilterPolicy>, int,
                     BlockBasedTableOptions::IndexType>> {
 public:
  DBBasicTestWithTsIterTombstones()
      : DBBasicTestWithTimestampBase("/db_basic_ts_iter_tombstones") {}
};

TEST_P(DBBasicTestWithTsIterTombstones, IterWithDelete) {
  constexpr size_t kNumKeysPerFile = 128;
  Options options = CurrentOptions();
  options.env = env_;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.prefix_extractor = std::get<0>(GetParam());
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  BlockBasedTableOptions bbto;
  bbto.filter_policy = std::get<1>(GetParam());
  bbto.index_type = std::get<3>(GetParam());
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  options.num_levels = std::get<2>(GetParam());
  DestroyAndReopen(options);
  std::vector<std::string> write_ts_strs = {Timestamp(2, 0), Timestamp(4, 0)};
  constexpr uint64_t kMaxKey = 0xffffffffffffffff;
  constexpr uint64_t kMinKey = 0xfffffffffffff000;
  // Insert kMinKey...kMaxKey
  uint64_t key = kMinKey;
  WriteOptions write_opts;
  Slice ts = write_ts_strs[0];
  do {
    Status s = db_->Put(write_opts, Key1(key), write_ts_strs[0],
                        "value" + std::to_string(key));
    ASSERT_OK(s);
    if (kMaxKey == key) {
      break;
    }
    ++key;
  } while (true);

  for (key = kMaxKey; key >= kMinKey; --key) {
    Status s;
    if (0 != (key % 2)) {
      s = db_->Put(write_opts, Key1(key), write_ts_strs[1],
                   "value1" + std::to_string(key));
    } else {
      s = db_->Delete(write_opts, Key1(key), write_ts_strs[1]);
    }
    ASSERT_OK(s);
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  {
    std::string read_ts = Timestamp(4, 0);
    ts = read_ts;
    ReadOptions read_opts;
    read_opts.total_order_seek = true;
    read_opts.timestamp = &ts;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    size_t count = 0;
    key = kMinKey + 1;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next(), ++count, key += 2) {
      ASSERT_EQ(Key1(key), iter->key());
      ASSERT_EQ("value1" + std::to_string(key), iter->value());
    }
    ASSERT_EQ((kMaxKey - kMinKey + 1) / 2, count);

    for (iter->SeekToLast(), count = 0, key = kMaxKey; iter->Valid();
         key -= 2, ++count, iter->Prev()) {
      ASSERT_EQ(Key1(key), iter->key());
      ASSERT_EQ("value1" + std::to_string(key), iter->value());
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ((kMaxKey - kMinKey + 1) / 2, count);
  }
  Close();
}

INSTANTIATE_TEST_CASE_P(
    Timestamp, DBBasicTestWithTsIterTombstones,
    ::testing::Combine(
        ::testing::Values(
            std::shared_ptr<const SliceTransform>(NewFixedPrefixTransform(7)),
            std::shared_ptr<const SliceTransform>(NewFixedPrefixTransform(8))),
        ::testing::Values(std::shared_ptr<const FilterPolicy>(nullptr),
                          std::shared_ptr<const FilterPolicy>(
                              NewBloomFilterPolicy(10, false)),
                          std::shared_ptr<const FilterPolicy>(
                              NewBloomFilterPolicy(20, false))),
        ::testing::Values(2, 6),
        ::testing::Values(
            BlockBasedTableOptions::IndexType::kBinarySearch,
            BlockBasedTableOptions::IndexType::kHashSearch,
            BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch,
            BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey)));
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

class UpdateFullHistoryTsLowTest : public DBBasicTestWithTimestampBase {
 public:
  UpdateFullHistoryTsLowTest()
      : DBBasicTestWithTimestampBase("/update_full_history_ts_low_test") {}
};

TEST_F(UpdateFullHistoryTsLowTest, ConcurrentUpdate) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  std::string lower_ts_low = Timestamp(10, 0);
  std::string higher_ts_low = Timestamp(25, 0);
  const size_t kTimestampSize = lower_ts_low.size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;

  DestroyAndReopen(options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  // This workaround swaps `lower_ts_low` originally used for update by the
  // caller to `higher_ts_low` after its writer is queued to make sure
  // the caller will always get a TryAgain error.
  // It mimics cases where two threads update full_history_ts_low concurrently
  // with one thread writing a higher ts_low and one thread writing a lower
  // ts_low.
  VersionEdit* version_edit;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::IncreaseFullHistoryTsLowImpl:BeforeEdit",
      [&](void* arg) { version_edit = static_cast<VersionEdit*>(arg); });
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:BeforeWriterWaiting",
      [&](void* /*arg*/) { version_edit->SetFullHistoryTsLow(higher_ts_low); });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_TRUE(
      db_->IncreaseFullHistoryTsLow(db_->DefaultColumnFamily(), lower_ts_low)
          .IsTryAgain());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  Close();
}

// Tests the effect of flag `persist_user_defined_timestamps` on the file
// boundaries contained in the Manifest, a.k.a FileMetaData.smallest,
// FileMetaData.largest.
class HandleFileBoundariesTest
    : public DBBasicTestWithTimestampBase,
      public testing::WithParamInterface<test::UserDefinedTimestampTestMode> {
 public:
  HandleFileBoundariesTest()
      : DBBasicTestWithTimestampBase("/handle_file_boundaries") {}
};

TEST_P(HandleFileBoundariesTest, ConfigurePersistUdtWithPut) {
  Options options = CurrentOptions();
  options.env = env_;
  // Write a timestamp that is not the min timestamp to help test the behavior
  // of flag `persist_user_defined_timestamps`.
  std::string write_ts;
  std::string min_ts;
  PutFixed64(&write_ts, 1);
  PutFixed64(&min_ts, 0);
  std::string smallest_ukey_without_ts = "bar";
  std::string largest_ukey_without_ts = "foo";
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  bool persist_udt = test::ShouldPersistUDT(GetParam());
  options.persist_user_defined_timestamps = persist_udt;
  if (!persist_udt) {
    options.allow_concurrent_memtable_write = false;
  }
  DestroyAndReopen(options);

  ASSERT_OK(
      db_->Put(WriteOptions(), smallest_ukey_without_ts, write_ts, "val1"));
  ASSERT_OK(
      db_->Put(WriteOptions(), largest_ukey_without_ts, write_ts, "val2"));

  // Create a L0 SST file and its record is added to the Manifest.
  ASSERT_OK(Flush());
  Close();

  options.create_if_missing = false;
  // Reopen the DB and process manifest file.
  Reopen(options);

  std::vector<std::vector<FileMetaData>> level_to_files;
  dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                  &level_to_files);
  ASSERT_GT(level_to_files.size(), 1);
  // L0 only has one SST file.
  ASSERT_EQ(level_to_files[0].size(), 1);
  auto file_meta = level_to_files[0][0];
  if (persist_udt) {
    ASSERT_EQ(smallest_ukey_without_ts + write_ts,
              file_meta.smallest.user_key());
    ASSERT_EQ(largest_ukey_without_ts + write_ts, file_meta.largest.user_key());
  } else {
    // If `persist_user_defined_timestamps` is false, the file boundaries should
    // have the min timestamp. Behind the scenes, when file boundaries in
    // FileMetaData is persisted to Manifest, the original user-defined
    // timestamps in user key are stripped. When manifest is read and processed
    // during DB open, a min timestamp is padded to the file boundaries. This
    // test's writes contain non min timestamp to verify this logic end-to-end.
    ASSERT_EQ(smallest_ukey_without_ts + min_ts, file_meta.smallest.user_key());
    ASSERT_EQ(largest_ukey_without_ts + min_ts, file_meta.largest.user_key());
  }
  Close();
}

TEST_P(HandleFileBoundariesTest, ConfigurePersistUdtWithRangeDelete) {
  Options options = CurrentOptions();
  options.env = env_;
  // Write a timestamp that is not the min/max timestamp to help test the
  // behavior of flag `persist_user_defined_timestamps`.
  std::string write_ts;
  std::string min_ts;
  std::string max_ts;
  PutFixed64(&write_ts, 1);
  PutFixed64(&min_ts, 0);
  PutFixed64(&max_ts, std::numeric_limits<uint64_t>::max());
  std::string smallest_ukey_without_ts = "bar";
  std::string largest_ukey_without_ts = "foo";
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  bool persist_udt = test::ShouldPersistUDT(GetParam());
  options.persist_user_defined_timestamps = persist_udt;
  if (!persist_udt) {
    options.allow_concurrent_memtable_write = false;
  }
  DestroyAndReopen(options);

  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                             smallest_ukey_without_ts, largest_ukey_without_ts,
                             write_ts));

  // Create a L0 SST file and its record is added to the Manifest.
  ASSERT_OK(Flush());
  Close();

  options.create_if_missing = false;
  // Reopen the DB and process manifest file.
  Reopen(options);

  std::vector<std::vector<FileMetaData>> level_to_files;
  dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                  &level_to_files);
  ASSERT_GT(level_to_files.size(), 1);
  // L0 only has one SST file.
  ASSERT_EQ(level_to_files[0].size(), 1);
  auto file_meta = level_to_files[0][0];
  if (persist_udt) {
    ASSERT_EQ(smallest_ukey_without_ts + write_ts,
              file_meta.smallest.user_key());
  } else {
    ASSERT_EQ(smallest_ukey_without_ts + min_ts, file_meta.smallest.user_key());
  }
  // When right file boundary comes from range deletion, it uses max timestamp
  // and a range deletion sentinel that uses the max sequence number to mark the
  // end key exclusive. This is regardless of whether timestamp is persisted.
  ASSERT_EQ(largest_ukey_without_ts + max_ts, file_meta.largest.user_key());
  auto largest_footer = ExtractInternalKeyFooter(file_meta.largest.Encode());
  ASSERT_EQ(largest_footer, kRangeTombstoneSentinel);
  Close();
}

INSTANTIATE_TEST_CASE_P(
    ConfigurePersistUdt, HandleFileBoundariesTest,
    ::testing::Values(
        test::UserDefinedTimestampTestMode::kStripUserDefinedTimestamp,
        test::UserDefinedTimestampTestMode::kNormal));

// Test params:
// 1) whether to flush before close
class EnableDisableUDTTest : public DBBasicTestWithTimestampBase,
                             public testing::WithParamInterface<bool> {
 public:
  EnableDisableUDTTest()
      : DBBasicTestWithTimestampBase("/enable_disable_udt") {}
};

INSTANTIATE_TEST_CASE_P(EnableDisableUDTTest, EnableDisableUDTTest,
                        ::testing::Values(true, false));

TEST_P(EnableDisableUDTTest, Basic) {
  Options options = CurrentOptions();
  // Un-flushed data before close will involve a WAL replay on DB reopen.
  bool flush_before_close = GetParam();
  options.env = env_;
  options.comparator = BytewiseComparator();
  options.persist_user_defined_timestamps = true;
  DestroyAndReopen(options);

  ReadOptions ropts;
  std::string read_ts;
  std::string value;
  std::string key_ts;

  // Create one SST file, its user keys have no user-defined timestamps.
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "val0"));
  ASSERT_OK(db_->Put(WriteOptions(), "bar", "val0"));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), "bar", "baz"));
  ASSERT_OK(db_->Get(ReadOptions(), "foo", &value));
  ASSERT_EQ("val0", value);
  ASSERT_TRUE(db_->Get(ReadOptions(), "bar", &value).IsNotFound());
  if (flush_before_close) {
    ASSERT_OK(Flush(0));
  }
  Close();

  // Reopen the existing column family and enable user-defined timestamps
  // feature for it.
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  options.persist_user_defined_timestamps = false;
  options.allow_concurrent_memtable_write = false;
  Reopen(options);
  // Read data from previous session before and after compaction.
  read_ts = EncodeAsUint64(1);
  Slice read_ts_slice = read_ts;
  ropts.timestamp = &read_ts_slice;
  for (int i = 0; i < 2; i++) {
    ASSERT_TRUE(db_->Get(ReadOptions(), "foo", &value).IsInvalidArgument());
    // Entries in pre-existing SST files are treated as if they have minimum
    // user-defined timestamps.
    ASSERT_OK(db_->Get(ropts, "foo", &value, &key_ts));
    ASSERT_EQ("val0", value);
    ASSERT_EQ(EncodeAsUint64(0), key_ts);
    ASSERT_TRUE(db_->Get(ropts, "bar", &value, &key_ts).IsNotFound());
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  }

  // Do timestamped read / write.
  ASSERT_OK(db_->Put(WriteOptions(), "foo", EncodeAsUint64(1), "val1"));
  ASSERT_OK(db_->Put(WriteOptions(), "bar", EncodeAsUint64(1), "val1"));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), "bar", "baz", EncodeAsUint64(2)));
  ASSERT_OK(db_->Get(ropts, "foo", &value, &key_ts));
  ASSERT_EQ("val1", value);
  ASSERT_EQ(EncodeAsUint64(1), key_ts);
  ASSERT_OK(db_->Get(ropts, "bar", &value, &key_ts));
  ASSERT_EQ("val1", value);
  ASSERT_EQ(EncodeAsUint64(1), key_ts);
  read_ts = EncodeAsUint64(2);
  ASSERT_TRUE(db_->Get(ropts, "bar", &value, &key_ts).IsNotFound());
  // The user keys in this SST file don't have user-defined timestamps either,
  // because `persist_user_defined_timestamps` flag is set to false.
  if (flush_before_close) {
    ASSERT_OK(Flush(0));
  }
  Close();

  // Reopen the existing column family while disabling user-defined timestamps.
  options.comparator = BytewiseComparator();
  Reopen(options);

  // Read data from previous session before and after compaction.
  for (int i = 0; i < 2; i++) {
    ASSERT_TRUE(db_->Get(ropts, "foo", &value).IsInvalidArgument());
    ASSERT_OK(db_->Get(ReadOptions(), "foo", &value));
    ASSERT_EQ("val1", value);
    ASSERT_TRUE(db_->Get(ReadOptions(), "bar", &value).IsNotFound());
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  }

  // Continue to write / read the column family without user-defined timestamps.
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "val2"));
  ASSERT_OK(db_->Put(WriteOptions(), "bar", "val2"));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), "bar", "baz"));
  ASSERT_OK(db_->Get(ReadOptions(), "foo", &value));
  ASSERT_EQ("val2", value);
  ASSERT_TRUE(db_->Get(ReadOptions(), "bar", &value).IsNotFound());
  if (flush_before_close) {
    ASSERT_OK(Flush(0));
  }
  Close();
}

// Tests that as long as the
// `ReadOptions.timestamp >= SuperVersion.full_history_ts_low` sanity check
// passes. The read will be consistent even if the column family's
// full_history_ts_low is concurrently increased and collapsed some history
// above `ReadOptions.timestamp`.
TEST_F(DBBasicTestWithTimestamp,
       FullHistoryTsLowSanityCheckPassReadIsConsistent) {
  Options options = CurrentOptions();
  options.env = env_;
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  // Use UDT in memtable only feature for this test, so we can control that
  // newly set `full_history_ts_low` collapse history when Flush happens.
  options.persist_user_defined_timestamps = false;
  options.allow_concurrent_memtable_write = false;
  DestroyAndReopen(options);
  std::string min_ts;
  PutFixed64(&min_ts, 0);

  // Write two versions of the key (1, v1), (3, v3), and always read with
  // timestamp 2.
  std::string write_ts;
  PutFixed64(&write_ts, 1);
  ASSERT_OK(db_->Put(WriteOptions(), "foo", write_ts, "val1"));

  std::string read_ts;
  PutFixed64(&read_ts, 2);
  Slice read_ts_slice = read_ts;
  ReadOptions read_opts;
  read_opts.timestamp = &read_ts_slice;

  // First read, no full_history_ts_low set, sanity check pass.
  std::string value;
  std::string timestamp;
  ASSERT_OK(db_->Get(read_opts, "foo", &value, &timestamp));
  ASSERT_EQ("val1", value);
  ASSERT_EQ(write_ts, timestamp);

  std::string full_history_ts_low;
  std::string marked_ts_low;
  PutFixed64(&full_history_ts_low, 2);
  marked_ts_low = full_history_ts_low;
  ASSERT_OK(db_->IncreaseFullHistoryTsLow(db_->DefaultColumnFamily(),
                                          full_history_ts_low));
  ASSERT_OK(Flush(0));

  // Write the (3, v3) entry after flush, otherwise with UDT in memtable only
  // the previous Flush(0) with full_history_ts_low = 2 will be postponed
  // waiting for (3, v3) to expire too.
  write_ts.clear();
  PutFixed64(&write_ts, 3);
  ASSERT_OK(db_->Put(WriteOptions(), "foo", write_ts, "val3"));

  // Second read:
  // ReadOptions.timestamp(2) >= SuperVersion.full_history_ts_low(2),
  // and ReadOptions.timestamp(2) >= ColumnFamilyData.full_history_ts_low(2).
  // history below 2 is collapsed. Reading at 2 or above 2 is ok.
  // Sanity check pass. Read return consistent value, but timestamp is already
  // collapsed.
  ASSERT_OK(db_->Get(read_opts, "foo", &value, &timestamp));
  ASSERT_EQ("val1", value);
  ASSERT_EQ(min_ts, timestamp);

  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::GetImpl:AfterAcquireSv", [&](void* /*arg*/) {
        // Concurrently increasing full_history_ts_low and flush to create a
        // new SuperVersion
        std::string current_ts_low;
        ASSERT_OK(db_->GetFullHistoryTsLow(db_->DefaultColumnFamily(),
                                           &current_ts_low));
        if (current_ts_low.empty() || current_ts_low != marked_ts_low) {
          return;
        }
        full_history_ts_low.clear();
        PutFixed64(&full_history_ts_low, 4);
        ASSERT_OK(db_->IncreaseFullHistoryTsLow(db_->DefaultColumnFamily(),
                                                full_history_ts_low));
        ASSERT_OK(Flush(0));
      });

  SyncPoint::GetInstance()->EnableProcessing();

  // Third read:
  // ReadOptions.timestamp(2) >= SuperVersion.full_history_ts_low(2),
  // but ReadOptions.timestamp(2) < ColumnFamilyData.full_history_ts_low(4).
  // History below 4 is collapsed in the newly installed SuperVersion. But the
  // SuperVersion attached to this read still has the history below 4 available.
  // Sanity check pass. Read return consistent value, timestamp is collapsed.
  ASSERT_OK(db_->Get(read_opts, "foo", &value, &timestamp));
  ASSERT_EQ("val1", value);
  ASSERT_EQ(min_ts, timestamp);

  // Fourth read:
  // ReadOptions.timestamp(2) < SuperVersion.full_history_ts_low(4).
  // Sanity check fails. Had it succeeded, the read would return "v3",
  // which is inconsistent.
  ASSERT_TRUE(
      db_->Get(read_opts, "foo", &value, &timestamp).IsInvalidArgument());
  Close();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

// Tests that in cases when
// `ReadOptions.timestamp >= SuperVersion.full_history_ts_low` sanity check
// fails. The referenced SuperVersion is dereferenced and cleaned up properly
// for all read APIs that involves this sanity check.
TEST_F(DBBasicTestWithTimestamp, FullHistoryTsLowSanityCheckFail) {
  Options options = CurrentOptions();
  options.env = env_;
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  // Use UDT in memtable only feature for this test, so we can control that
  // newly set `full_history_ts_low` collapse history when Flush happens.
  options.persist_user_defined_timestamps = false;
  options.allow_concurrent_memtable_write = false;
  DestroyAndReopen(options);

  ColumnFamilyHandle* handle2 = nullptr;
  Status s = db_->CreateColumnFamily(options, "data", &handle2);
  ASSERT_OK(s);

  std::string write_ts;
  PutFixed64(&write_ts, 1);
  ASSERT_OK(db_->Put(WriteOptions(), "foo", write_ts, "val1"));
  ASSERT_OK(db_->Put(WriteOptions(), handle2, "foo", write_ts, "val1"));

  std::string full_history_ts_low;
  PutFixed64(&full_history_ts_low, 3);
  ASSERT_OK(db_->IncreaseFullHistoryTsLow(db_->DefaultColumnFamily(),
                                          full_history_ts_low));
  ASSERT_OK(db_->IncreaseFullHistoryTsLow(handle2, full_history_ts_low));
  ASSERT_OK(Flush(0));
  ASSERT_OK(db_->Flush(FlushOptions(), handle2));

  std::string read_ts;
  PutFixed64(&read_ts, 2);
  Slice read_ts_slice = read_ts;
  ReadOptions read_opts;
  read_opts.timestamp = &read_ts_slice;

  // Get()
  std::string value;
  ASSERT_TRUE(db_->Get(read_opts, "foo", &value).IsInvalidArgument());

  // MultiGet()
  std::vector<ColumnFamilyHandle*> cfhs = {db_->DefaultColumnFamily(), handle2};
  {
    std::vector<std::string> key_vals = {"foo", "foo"};
    std::vector<Slice> keys;
    std::vector<std::string> values;
    for (size_t j = 0; j < 2; ++j) {
      keys.emplace_back(key_vals[j]);
    }

    std::vector<Status> statuses =
        db_->MultiGet(read_opts, cfhs, keys, &values);
    for (const auto& status : statuses) {
      ASSERT_TRUE(status.IsInvalidArgument());
    }
  }

  // MultiGet with only one column family
  {
    std::vector<ColumnFamilyHandle*> one_cfh = {db_->DefaultColumnFamily()};
    std::vector<std::string> key_vals = {"foo"};
    std::vector<Slice> keys;
    std::vector<std::string> values;
    for (size_t j = 0; j < 1; ++j) {
      keys.emplace_back(key_vals[j]);
    }

    std::vector<Status> statuses =
        db_->MultiGet(read_opts, one_cfh, keys, &values);
    for (const auto& status : statuses) {
      ASSERT_TRUE(status.IsInvalidArgument());
    }
  }

  // Overloaded version of MultiGet
  ColumnFamilyHandle* column_families[] = {db_->DefaultColumnFamily(), handle2};
  {
    Slice keys[] = {"foo", "foo"};
    PinnableSlice values[] = {PinnableSlice(), PinnableSlice()};
    Status statuses[] = {Status::OK(), Status::OK()};
    db_->MultiGet(read_opts, /*num_keys=*/2, &column_families[0], &keys[0],
                  &values[0], &statuses[0], /*sorted_input=*/false);
    for (const auto& status : statuses) {
      ASSERT_TRUE(status.IsInvalidArgument());
    }
  }

  // Overloaded versions of MultiGet with one column family
  {
    ColumnFamilyHandle* one_column_family[] = {db_->DefaultColumnFamily()};
    Slice keys[] = {"foo"};
    PinnableSlice values[] = {PinnableSlice()};
    Status statuses[] = {Status::OK()};
    db_->MultiGet(read_opts, /*num_keys=*/1, &one_column_family[0], &keys[0],
                  &values[0], &statuses[0], /*sorted_input=*/false);
    for (const auto& status : statuses) {
      ASSERT_TRUE(status.IsInvalidArgument());
    }
  }

  // Single column family MultiGetEntity()
  {
    Slice keys[] = {"foo", "foo"};
    PinnableWideColumns results[2];
    Status statuses[2];
    db_->MultiGetEntity(read_opts, db_->DefaultColumnFamily(), /*num_keys=*/2,
                        &keys[0], &results[0], &statuses[0]);
    for (const auto& status : statuses) {
      ASSERT_TRUE(status.IsInvalidArgument());
    }
  }

  // NewIterator()
  std::unique_ptr<Iterator> iter(
      db_->NewIterator(read_opts, db_->DefaultColumnFamily()));
  ASSERT_TRUE(iter->status().IsInvalidArgument());
  std::unique_ptr<Iterator> iter2(db_->NewIterator(read_opts, handle2));
  ASSERT_TRUE(iter2->status().IsInvalidArgument());

  // NewIterators()
  std::vector<Iterator*> iterators;
  ASSERT_TRUE(
      db_->NewIterators(read_opts, cfhs, &iterators).IsInvalidArgument());
  delete handle2;
  Close();
}

// Verifies that reading with a snapshot and a FRESH timestamp succeeds even
// after full_history_ts_low advances past the snapshot's original timestamp.
// This models the db_stress scenario where a long-running snapshot's saved
// timestamp becomes stale due to concurrent flushes advancing
// full_history_ts_low (persist_user_defined_timestamps=false).
TEST_F(DBBasicTestWithTimestamp,
       SnapshotReadWithFreshTimestampAfterTsLowAdvances) {
  Options options = CurrentOptions();
  options.env = env_;
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  options.persist_user_defined_timestamps = false;
  options.allow_concurrent_memtable_write = false;
  DestroyAndReopen(options);

  // Write a key at timestamp 100.
  std::string write_ts;
  PutFixed64(&write_ts, 100);
  ASSERT_OK(db_->Put(WriteOptions(), "key1", write_ts, "value1"));

  // Take a snapshot (pins the data at this sequence number).
  const Snapshot* snap = db_->GetSnapshot();
  ASSERT_NE(snap, nullptr);

  // Simulate what happens in the stress test: the original read timestamp at
  // snapshot time would have been ~100. Now advance full_history_ts_low well
  // past 100 by writing more data and flushing.
  std::string write_ts2;
  PutFixed64(&write_ts2, 500);
  ASSERT_OK(db_->Put(WriteOptions(), "key1", write_ts2, "value2"));

  // Advance full_history_ts_low to 400 (past the original timestamp 100).
  std::string ts_low;
  PutFixed64(&ts_low, 400);
  ASSERT_OK(db_->IncreaseFullHistoryTsLow(db_->DefaultColumnFamily(), ts_low));
  ASSERT_OK(Flush());

  // Omitting ReadOptions.timestamp is not a valid way to bypass the stale
  // timestamp check because this column family enables user-defined timestamps.
  {
    ReadOptions read_opts;
    read_opts.snapshot = snap;
    std::string value;
    Status s = db_->Get(read_opts, "key1", &value);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  }

  // With persist_user_defined_timestamps=false and the flush,
  // full_history_ts_low auto-advances past 500 (the highest write). Reading
  // with the snapshot but the OLD (stale) timestamp of 100, which is below the
  // auto-advanced threshold, should fail with InvalidArgument because
  // read_ts < full_history_ts_low.
  {
    ReadOptions read_opts;
    read_opts.snapshot = snap;
    std::string stale_ts;
    PutFixed64(&stale_ts, 100);
    Slice stale_ts_slice = stale_ts;
    read_opts.timestamp = &stale_ts_slice;
    std::string value;
    Status s = db_->Get(read_opts, "key1", &value);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  }

  // Reading with the snapshot but a FRESH timestamp (>= full_history_ts_low)
  // should succeed. The snapshot pins data visibility by sequence number, so
  // the fresh timestamp only needs to pass the full_history_ts_low check.
  // When persist_user_defined_timestamps=false and we flush,
  // full_history_ts_low automatically advances to just above the newest write
  // timestamp in the flushed memtable (i.e. just above 500). So we use 1000 to
  // have comfortable margin above 500.
  {
    ReadOptions read_opts;
    read_opts.snapshot = snap;
    std::string fresh_ts;
    PutFixed64(&fresh_ts, 1000);
    Slice fresh_ts_slice = fresh_ts;
    read_opts.timestamp = &fresh_ts_slice;
    std::string value;
    Status s = db_->Get(read_opts, "key1", &value);
    ASSERT_OK(s);
    ASSERT_EQ(value, "value1");
  }

  db_->ReleaseSnapshot(snap);
  Close();
}

TEST_F(DBBasicTestWithTimestamp,
       GCPreserveRangeTombstoneWhenNoOrSmallFullHistoryLow) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  std::string ts_str = Timestamp(1, 0);
  WriteOptions wopts;
  ASSERT_OK(db_->Put(wopts, "k1", ts_str, "v1"));
  ASSERT_OK(db_->Put(wopts, "k2", ts_str, "v2"));
  ASSERT_OK(db_->Put(wopts, "k3", ts_str, "v3"));
  ts_str = Timestamp(2, 0);
  ASSERT_OK(
      db_->DeleteRange(wopts, db_->DefaultColumnFamily(), "k1", "k3", ts_str));

  ts_str = Timestamp(3, 0);
  Slice ts = ts_str;
  ReadOptions ropts;
  ropts.timestamp = &ts;
  CompactRangeOptions cro;
  cro.full_history_ts_low = nullptr;
  std::string value;
  std::string key_ts;
  Status s;
  auto verify = [&] {
    s = db_->Get(ropts, "k1", &value);
    ASSERT_TRUE(s.IsNotFound());

    s = db_->Get(ropts, "k2", &value, &key_ts);
    ASSERT_TRUE(s.IsNotFound());
    ASSERT_EQ(key_ts, Timestamp(2, 0));

    ASSERT_OK(db_->Get(ropts, "k3", &value, &key_ts));
    ASSERT_EQ(value, "v3");
    ASSERT_EQ(Timestamp(1, 0), key_ts);

    size_t batch_size = 3;
    std::vector<std::string> key_strs = {"k1", "k2", "k3"};
    std::vector<Slice> keys{key_strs.begin(), key_strs.end()};
    std::vector<PinnableSlice> values(batch_size);
    std::vector<Status> statuses(batch_size);
    db_->MultiGet(ropts, db_->DefaultColumnFamily(), batch_size, keys.data(),
                  values.data(), statuses.data(), true /* sorted_input */);
    ASSERT_TRUE(statuses[0].IsNotFound());
    ASSERT_TRUE(statuses[1].IsNotFound());
    ASSERT_OK(statuses[2]);
    ;
    ASSERT_EQ(values[2], "v3");
  };
  verify();
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  verify();
  std::string lb = Timestamp(0, 0);
  Slice lb_slice = lb;
  cro.full_history_ts_low = &lb_slice;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  verify();
  Close();
}

TEST_F(DBBasicTestWithTimestamp,
       GCRangeTombstonesAndCoveredKeysRespectingTslow) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.cache_index_and_filter_blocks = true;
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.num_levels = 2;
  DestroyAndReopen(options);

  WriteOptions wopts;
  ASSERT_OK(db_->Put(wopts, "k1", Timestamp(1, 0), "v1"));
  ASSERT_OK(db_->Delete(wopts, "k2", Timestamp(2, 0)));
  ASSERT_OK(db_->DeleteRange(wopts, db_->DefaultColumnFamily(), "k1", "k3",
                             Timestamp(3, 0)));
  ASSERT_OK(db_->Put(wopts, "k3", Timestamp(4, 0), "v3"));

  ReadOptions ropts;
  std::string read_ts = Timestamp(5, 0);
  Slice read_ts_slice = read_ts;
  ropts.timestamp = &read_ts_slice;
  size_t batch_size = 3;
  std::vector<std::string> key_strs = {"k1", "k2", "k3"};
  std::vector<Slice> keys = {key_strs.begin(), key_strs.end()};
  std::vector<PinnableSlice> values(batch_size);
  std::vector<Status> statuses(batch_size);
  std::vector<std::string> timestamps(batch_size);
  db_->MultiGet(ropts, db_->DefaultColumnFamily(), batch_size, keys.data(),
                values.data(), timestamps.data(), statuses.data(),
                true /* sorted_input */);
  ASSERT_TRUE(statuses[0].IsNotFound());
  ASSERT_EQ(timestamps[0], Timestamp(3, 0));
  ASSERT_TRUE(statuses[1].IsNotFound());
  // DeleteRange has a higher timestamp than Delete for "k2"
  ASSERT_EQ(timestamps[1], Timestamp(3, 0));
  ASSERT_OK(statuses[2]);
  ASSERT_EQ(values[2], "v3");
  ASSERT_EQ(timestamps[2], Timestamp(4, 0));

  CompactRangeOptions cro;
  // Range tombstone has timestamp >= full_history_ts_low, covered keys
  // are not dropped.
  std::string compaction_ts_str = Timestamp(2, 0);
  Slice compaction_ts = compaction_ts_str;
  cro.full_history_ts_low = &compaction_ts;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ropts.timestamp = &compaction_ts;
  std::string value;
  std::string ts;
  ASSERT_OK(db_->Get(ropts, "k1", &value, &ts));
  ASSERT_EQ(value, "v1");
  // timestamp is below full_history_ts_low, zeroed out as the key goes into
  // bottommost level
  ASSERT_EQ(ts, Timestamp(0, 0));
  ASSERT_TRUE(db_->Get(ropts, "k2", &value, &ts).IsNotFound());
  ASSERT_EQ(ts, Timestamp(2, 0));

  compaction_ts_str = Timestamp(4, 0);
  compaction_ts = compaction_ts_str;
  cro.full_history_ts_low = &compaction_ts;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ropts.timestamp = &read_ts_slice;
  // k1, k2 and the range tombstone should be dropped
  // k3 should still exist
  db_->MultiGet(ropts, db_->DefaultColumnFamily(), batch_size, keys.data(),
                values.data(), timestamps.data(), statuses.data(),
                true /* sorted_input */);
  ASSERT_TRUE(statuses[0].IsNotFound());
  ASSERT_TRUE(timestamps[0].empty());
  ASSERT_TRUE(statuses[1].IsNotFound());
  ASSERT_TRUE(timestamps[1].empty());
  ASSERT_OK(statuses[2]);
  ASSERT_EQ(values[2], "v3");
  ASSERT_EQ(timestamps[2], Timestamp(4, 0));

  Close();
}

class DeleteRangeWithTimestampTableOptions
    : public DBBasicTestWithTimestampBase,
      public testing::WithParamInterface<
          std::tuple<BlockBasedTableOptions::IndexType,
                     test::UserDefinedTimestampTestMode>> {
 public:
  explicit DeleteRangeWithTimestampTableOptions()
      : DBBasicTestWithTimestampBase(
            "delete_range_with_timestamp_table_options") {}
};

INSTANTIATE_TEST_CASE_P(
    Timestamp, DeleteRangeWithTimestampTableOptions,
    testing::Combine(
        testing::Values(
            BlockBasedTableOptions::IndexType::kBinarySearch,
            BlockBasedTableOptions::IndexType::kHashSearch,
            BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch,
            BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey),
        testing::Values(
            test::UserDefinedTimestampTestMode::kNormal,
            test::UserDefinedTimestampTestMode::kStripUserDefinedTimestamp)));

TEST_P(DeleteRangeWithTimestampTableOptions, BasicReadAndIterate) {
  const int kNum = 200;
  const int kRangeBegin = 50;
  const int kRangeEnd = 150;
  const int kNumPerFile = 25;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  options.compression = kNoCompression;
  BlockBasedTableOptions bbto;
  bbto.index_type = std::get<0>(GetParam());
  bbto.block_size = 100;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  options.env = env_;
  options.create_if_missing = true;
  bool persist_udt = test::ShouldPersistUDT(std::get<1>(GetParam()));
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  options.persist_user_defined_timestamps = persist_udt;
  // UDT in memtables only not compatible with concurrent memtable writes.
  options.allow_concurrent_memtable_write = persist_udt;
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(kNumPerFile));
  DestroyAndReopen(options);

  // Write half of the keys before the tombstone and half after the tombstone.
  // Only covered keys (i.e., within the range and older than the tombstone)
  // should be deleted.
  std::string full_history_ts_low;
  int cutoff_ts = 0;
  for (int i = 0; i < kNum; ++i) {
    std::string write_ts;
    PutFixed64(&write_ts, i);
    if (i == kNum / 2) {
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 Key1(kRangeBegin), Key1(kRangeEnd), write_ts));
    }
    ASSERT_OK(
        db_->Put(WriteOptions(), Key1(i), write_ts, "val" + std::to_string(i)));
    if (i == kNum - kNumPerFile) {
      if (!persist_udt) {
        // When UDTs are not persisted, mark the timestamps in the Memtables as
        // all expired so the followed flush can go through.
        cutoff_ts = i + 1;
        PutFixed64(&full_history_ts_low, cutoff_ts);
        ASSERT_OK(db_->IncreaseFullHistoryTsLow(db_->DefaultColumnFamily(),
                                                full_history_ts_low));
      }
      ASSERT_OK(Flush());
    }
  }

  ReadOptions read_opts;
  read_opts.total_order_seek = true;
  std::string read_ts;
  PutFixed64(&read_ts, kNum);
  Slice read_ts_slice = read_ts;
  read_opts.timestamp = &read_ts_slice;
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    ASSERT_OK(iter->status());

    int expected = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_EQ(Key1(expected), iter->key());
      if (expected == kRangeBegin - 1) {
        expected = kNum / 2;
      } else {
        ++expected;
      }
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(kNum, expected);

    expected = kNum / 2;
    for (iter->Seek(Key1(kNum / 2)); iter->Valid(); iter->Next()) {
      ASSERT_EQ(Key1(expected), iter->key());
      ++expected;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(kNum, expected);

    expected = kRangeBegin - 1;
    for (iter->SeekForPrev(Key1(kNum / 2 - 1)); iter->Valid(); iter->Prev()) {
      ASSERT_EQ(Key1(expected), iter->key());
      --expected;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(-1, expected);

    // Cannot read below the cutoff timestamp when timestamps are not persisted.
    if (persist_udt) {
      read_ts.clear();
      PutFixed64(&read_ts, 0);
      read_ts_slice = read_ts;
      read_opts.timestamp = &read_ts_slice;
      iter.reset(db_->NewIterator(read_opts));
      iter->SeekToFirst();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), Key1(0));
      iter->Next();
      ASSERT_FALSE(iter->Valid());
      ASSERT_OK(iter->status());
    }
  }

  read_ts.clear();
  PutFixed64(&read_ts, kNum);
  read_ts_slice = read_ts;
  read_opts.timestamp = &read_ts_slice;
  std::string value;
  std::string timestamp;
  Status s;
  std::string expected_ts;
  int int_expected_ts;
  for (int i = 0; i < kNum; ++i) {
    s = db_->Get(read_opts, Key1(i), &value, &timestamp);
    if (i >= kRangeBegin && i < kNum / 2) {
      ASSERT_TRUE(s.IsNotFound());
      int_expected_ts = (persist_udt || kNum / 2 >= cutoff_ts) ? kNum / 2 : 0;
    } else {
      ASSERT_OK(s);
      ASSERT_EQ(value, "val" + std::to_string(i));
      int_expected_ts = (persist_udt || i >= cutoff_ts) ? i : 0;
    }
    expected_ts.clear();
    PutFixed64(&expected_ts, int_expected_ts);
    ASSERT_EQ(timestamp, expected_ts);
  }

  size_t batch_size = kNum;
  std::vector<std::string> key_strs(batch_size);
  std::vector<Slice> keys(batch_size);
  std::vector<PinnableSlice> values(batch_size);
  std::vector<Status> statuses(batch_size);
  std::vector<std::string> timestamps(batch_size);
  for (int i = 0; i < kNum; ++i) {
    key_strs[i] = Key1(i);
    keys[i] = key_strs[i];
  }
  db_->MultiGet(read_opts, db_->DefaultColumnFamily(), batch_size, keys.data(),
                values.data(), timestamps.data(), statuses.data(),
                true /* sorted_input */);
  for (int i = 0; i < kNum; ++i) {
    if (i >= kRangeBegin && i < kNum / 2) {
      ASSERT_TRUE(statuses[i].IsNotFound());
      int_expected_ts = (persist_udt || kNum / 2 >= cutoff_ts) ? kNum / 2 : 0;
    } else {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(values[i], "val" + std::to_string(i));
      int_expected_ts = (persist_udt || i >= cutoff_ts) ? i : 0;
    }
    expected_ts.clear();
    PutFixed64(&expected_ts, int_expected_ts);
    ASSERT_EQ(timestamps[i], expected_ts);
  }

  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  if (!persist_udt) {
    // Mark everything expired so manual compaction can go through
    full_history_ts_low.clear();
    PutFixed64(&full_history_ts_low, kNum);
    ASSERT_OK(db_->IncreaseFullHistoryTsLow(db_->DefaultColumnFamily(),
                                            full_history_ts_low));
  }
  Slice compaction_ts = full_history_ts_low;
  cro.full_history_ts_low = &compaction_ts;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  for (int i = kRangeBegin; i < kNum / 2; ++i) {
    s = db_->Get(read_opts, Key1(i), &value, &timestamp);
    ASSERT_TRUE(s.IsNotFound());
    if (persist_udt) {
      expected_ts.clear();
      PutFixed64(&expected_ts, kNum / 2);
      ASSERT_EQ(timestamp, expected_ts);
    } else {
      // When timestamps are not persisted, data in SST files all logically have
      // min timestamp. A compaction to the last level will drop the range
      // tombstone.
      ASSERT_TRUE(timestamp.empty());
    }
  }
  Close();
}

TEST_F(DBBasicTestWithTimestamp, DeleteRangeGetIteratorWithSnapshot) {
  // 4 keys 0, 1, 2, 3 at timestamps 0, 1, 2, 3 respectively.
  // A range tombstone [1, 3) at timestamp 1 and has a sequence number between
  // key 1 and 2.
  Options options = CurrentOptions();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  WriteOptions write_opts;
  std::string put_ts = Timestamp(0, 0);
  const int kNum = 4;
  const int kNumPerFile = 1;
  const int kRangeBegin = 1;
  const int kRangeEnd = 3;
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(kNumPerFile));
  const Snapshot* before_tombstone = nullptr;
  const Snapshot* after_tombstone = nullptr;
  for (int i = 0; i < kNum; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), Key1(i), Timestamp(i, 0),
                       "val" + std::to_string(i)));
    if (i == kRangeBegin) {
      before_tombstone = db_->GetSnapshot();
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 Key1(kRangeBegin), Key1(kRangeEnd),
                                 Timestamp(kRangeBegin, 0)));
    }
    if (i == kNum / 2) {
      ASSERT_OK(Flush());
    }
  }
  assert(before_tombstone);
  after_tombstone = db_->GetSnapshot();
  // snapshot and ts before tombstone
  std::string read_ts_str = Timestamp(kRangeBegin - 1, 0);  // (0, 0)
  Slice read_ts = read_ts_str;
  ReadOptions read_opts;
  read_opts.timestamp = &read_ts;
  read_opts.snapshot = before_tombstone;
  std::vector<Status> expected_status = {
      Status::OK(), Status::NotFound(), Status::NotFound(), Status::NotFound()};
  std::vector<std::string> expected_values(kNum);
  expected_values[0] = "val" + std::to_string(0);
  std::vector<std::string> expected_timestamps(kNum);
  expected_timestamps[0] = Timestamp(0, 0);

  size_t batch_size = kNum;
  std::vector<std::string> key_strs(batch_size);
  std::vector<Slice> keys(batch_size);
  std::vector<PinnableSlice> values(batch_size);
  std::vector<Status> statuses(batch_size);
  std::vector<std::string> timestamps(batch_size);
  for (int i = 0; i < kNum; ++i) {
    key_strs[i] = Key1(i);
    keys[i] = key_strs[i];
  }

  auto verify = [&] {
    db_->MultiGet(read_opts, db_->DefaultColumnFamily(), batch_size,
                  keys.data(), values.data(), timestamps.data(),
                  statuses.data(), true /* sorted_input */);
    std::string value;
    std::string timestamp;
    Status s;
    for (int i = 0; i < kNum; ++i) {
      s = db_->Get(read_opts, Key1(i), &value, &timestamp);
      ASSERT_EQ(s, expected_status[i]);
      ASSERT_EQ(statuses[i], expected_status[i]);
      if (s.ok()) {
        ASSERT_EQ(value, expected_values[i]);
        ASSERT_EQ(values[i], expected_values[i]);
      }
      if (!timestamp.empty()) {
        ASSERT_EQ(timestamp, expected_timestamps[i]);
        ASSERT_EQ(timestamps[i], expected_timestamps[i]);
      } else {
        ASSERT_TRUE(timestamps[i].empty());
      }
    }
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    std::unique_ptr<Iterator> iter_for_seek(db_->NewIterator(read_opts));
    iter->SeekToFirst();
    for (int i = 0; i < kNum; ++i) {
      if (expected_status[i].ok()) {
        auto verify_iter = [&](Iterator* iter_ptr) {
          ASSERT_TRUE(iter_ptr->Valid());
          ASSERT_EQ(iter_ptr->key(), keys[i]);
          ASSERT_EQ(iter_ptr->value(), expected_values[i]);
          ASSERT_EQ(iter_ptr->timestamp(), expected_timestamps[i]);
        };
        verify_iter(iter.get());
        iter->Next();

        iter_for_seek->Seek(keys[i]);
        verify_iter(iter_for_seek.get());

        iter_for_seek->SeekForPrev(keys[i]);
        verify_iter(iter_for_seek.get());
      }
    }
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  };

  verify();

  // snapshot before tombstone and ts after tombstone
  read_ts_str = Timestamp(kNum, 0);  // (4, 0)
  read_ts = read_ts_str;
  read_opts.timestamp = &read_ts;
  read_opts.snapshot = before_tombstone;
  expected_status[1] = Status::OK();
  expected_timestamps[1] = Timestamp(1, 0);
  expected_values[1] = "val" + std::to_string(1);
  verify();

  // snapshot after tombstone and ts before tombstone
  read_ts_str = Timestamp(kRangeBegin - 1, 0);  // (0, 0)
  read_ts = read_ts_str;
  read_opts.timestamp = &read_ts;
  read_opts.snapshot = after_tombstone;
  expected_status[1] = Status::NotFound();
  expected_timestamps[1].clear();
  expected_values[1].clear();
  verify();

  // snapshot and ts after tombstone
  read_ts_str = Timestamp(kNum, 0);  // (4, 0)
  read_ts = read_ts_str;
  read_opts.timestamp = &read_ts;
  read_opts.snapshot = after_tombstone;
  for (int i = 0; i < kNum; ++i) {
    if (i == kRangeBegin) {
      expected_status[i] = Status::NotFound();
      expected_values[i].clear();
    } else {
      expected_status[i] = Status::OK();
      expected_values[i] = "val" + std::to_string(i);
    }
    expected_timestamps[i] = Timestamp(i, 0);
  }
  verify();

  db_->ReleaseSnapshot(before_tombstone);
  db_->ReleaseSnapshot(after_tombstone);
  Close();
}

TEST_F(DBBasicTestWithTimestamp, MergeBasic) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.merge_operator = std::make_shared<StringAppendTESTOperator>('.');
  DestroyAndReopen(options);

  const std::array<std::string, 3> write_ts_strs = {
      Timestamp(100, 0), Timestamp(200, 0), Timestamp(300, 0)};
  constexpr size_t kNumOfUniqKeys = 100;
  ColumnFamilyHandle* default_cf = db_->DefaultColumnFamily();

  for (size_t i = 0; i < write_ts_strs.size(); ++i) {
    for (size_t j = 0; j < kNumOfUniqKeys; ++j) {
      Status s;
      if (i == 0) {
        const std::string val = "v" + std::to_string(j) + "_0";
        s = db_->Put(WriteOptions(), Key1(j), write_ts_strs[i], val);
      } else {
        const std::string merge_op = std::to_string(i);
        s = db_->Merge(WriteOptions(), default_cf, Key1(j), write_ts_strs[i],
                       merge_op);
      }
      ASSERT_OK(s);
    }
  }

  std::array<std::string, 3> read_ts_strs = {
      Timestamp(150, 0), Timestamp(250, 0), Timestamp(350, 0)};

  const auto verify_db_with_get = [&]() {
    for (size_t i = 0; i < kNumOfUniqKeys; ++i) {
      const std::string base_val = "v" + std::to_string(i) + "_0";
      const std::array<std::string, 3> expected_values = {
          base_val, base_val + ".1", base_val + ".1.2"};
      const std::array<std::string, 3>& expected_ts = write_ts_strs;
      ReadOptions read_opts;
      for (size_t j = 0; j < read_ts_strs.size(); ++j) {
        Slice read_ts = read_ts_strs[j];
        read_opts.timestamp = &read_ts;
        std::string value;
        std::string ts;
        const Status s = db_->Get(read_opts, Key1(i), &value, &ts);
        ASSERT_OK(s);
        ASSERT_EQ(expected_values[j], value);
        ASSERT_EQ(expected_ts[j], ts);

        // Do Seek/SeekForPrev
        std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));
        it->Seek(Key1(i));
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(expected_values[j], it->value());
        ASSERT_EQ(expected_ts[j], it->timestamp());

        it->SeekForPrev(Key1(i));
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(expected_values[j], it->value());
        ASSERT_EQ(expected_ts[j], it->timestamp());
      }
    }
  };

  const auto verify_db_with_iterator = [&]() {
    std::string value_suffix;
    for (size_t i = 0; i < read_ts_strs.size(); ++i) {
      ReadOptions read_opts;
      Slice read_ts = read_ts_strs[i];
      read_opts.timestamp = &read_ts;
      std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));
      size_t key_int_val = 0;
      for (it->SeekToFirst(); it->Valid(); it->Next(), ++key_int_val) {
        const std::string key = Key1(key_int_val);
        const std::string value =
            "v" + std::to_string(key_int_val) + "_0" + value_suffix;
        ASSERT_EQ(key, it->key());
        ASSERT_EQ(value, it->value());
        ASSERT_EQ(write_ts_strs[i], it->timestamp());
      }
      EXPECT_OK(it->status());
      ASSERT_EQ(kNumOfUniqKeys, key_int_val);

      key_int_val = kNumOfUniqKeys - 1;
      for (it->SeekToLast(); it->Valid(); it->Prev(), --key_int_val) {
        const std::string key = Key1(key_int_val);
        const std::string value =
            "v" + std::to_string(key_int_val) + "_0" + value_suffix;
        ASSERT_EQ(key, it->key());
        ASSERT_EQ(value, it->value());
        ASSERT_EQ(write_ts_strs[i], it->timestamp());
      }
      ASSERT_OK(it->status());
      ASSERT_EQ(std::numeric_limits<size_t>::max(), key_int_val);

      value_suffix = value_suffix + "." + std::to_string(i + 1);
    }
  };

  verify_db_with_get();
  verify_db_with_iterator();

  ASSERT_OK(db_->Flush(FlushOptions()));

  verify_db_with_get();
  verify_db_with_iterator();

  Close();
}

TEST_F(DBBasicTestWithTimestamp, MergeAfterDeletion) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.merge_operator = std::make_shared<StringAppendTESTOperator>('.');
  DestroyAndReopen(options);

  ColumnFamilyHandle* const column_family = db_->DefaultColumnFamily();

  const size_t num_keys_per_file = 10;
  const size_t num_merges_per_key = 2;
  for (size_t i = 0; i < num_keys_per_file; ++i) {
    std::string ts = Timestamp(i + 10000, 0);
    Status s = db_->Delete(WriteOptions(), Key1(i), ts);
    ASSERT_OK(s);
    for (size_t j = 1; j <= num_merges_per_key; ++j) {
      ts = Timestamp(i + 10000 + j, 0);
      s = db_->Merge(WriteOptions(), column_family, Key1(i), ts,
                     std::to_string(j));
      ASSERT_OK(s);
    }
  }

  const auto verify_db = [&]() {
    ReadOptions read_opts;
    std::string read_ts_str = Timestamp(20000, 0);
    Slice ts = read_ts_str;
    read_opts.timestamp = &ts;
    std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));
    size_t count = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next(), ++count) {
      std::string key = Key1(count);
      ASSERT_EQ(key, it->key());
      std::string value;
      for (size_t j = 1; j <= num_merges_per_key; ++j) {
        value.append(std::to_string(j));
        if (j < num_merges_per_key) {
          value.push_back('.');
        }
      }
      ASSERT_EQ(value, it->value());
      std::string ts1 = Timestamp(count + 10000 + num_merges_per_key, 0);
      ASSERT_EQ(ts1, it->timestamp());
    }
    ASSERT_OK(it->status());
    ASSERT_EQ(num_keys_per_file, count);
    for (it->SeekToLast(); it->Valid(); it->Prev(), --count) {
      std::string key = Key1(count - 1);
      ASSERT_EQ(key, it->key());
      std::string value;
      for (size_t j = 1; j <= num_merges_per_key; ++j) {
        value.append(std::to_string(j));
        if (j < num_merges_per_key) {
          value.push_back('.');
        }
      }
      ASSERT_EQ(value, it->value());
      std::string ts1 = Timestamp(count - 1 + 10000 + num_merges_per_key, 0);
      ASSERT_EQ(ts1, it->timestamp());
    }
    ASSERT_OK(it->status());
    ASSERT_EQ(0, count);
  };

  verify_db();

  Close();
}

TEST_F(DBBasicTestWithTimestamp, RangeTombstoneApproximateSize) {
  // Test code path for calculating range tombstone compensated size
  // during flush and compaction.
  Options options = CurrentOptions();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  // So that the compaction below is non-bottommost and will calcualte
  // compensated range tombstone size.
  ASSERT_OK(db_->Put(WriteOptions(), Key(1), Timestamp(1, 0), "val"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(5);
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(1), Timestamp(1, 0)));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(1),
                             Key(2), Timestamp(2, 0)));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->RunManualCompaction(
      static_cast_with_check<ColumnFamilyHandleImpl>(db_->DefaultColumnFamily())
          ->cfd(),
      0 /* input_level */, 1 /* output_level */, CompactRangeOptions(),
      nullptr /* begin */, nullptr /* end */, true /* exclusive */,
      true /* disallow_trivial_move */,
      std::numeric_limits<uint64_t>::max() /* max_file_num_to_ignore */,
      "" /*trim_ts*/));
}

TEST_F(DBBasicTestWithTimestamp, IterSeekToLastWithIterateUpperbound) {
  // Test for a bug fix where DBIter::SeekToLast() could fail when
  // iterate_upper_bound and iter_start_ts are both set.
  Options options = CurrentOptions();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  DestroyAndReopen(options);

  ASSERT_OK(db_->Put(WriteOptions(), Key(1), Timestamp(2, 0), "val"));
  ReadOptions ro;
  std::string k = Key(1);
  Slice k_slice = k;
  ro.iterate_upper_bound = &k_slice;
  std::string ts = Timestamp(3, 0);
  Slice read_ts = ts;
  ro.timestamp = &read_ts;
  std::string start_ts = Timestamp(0, 0);
  Slice start_ts_slice = start_ts;
  ro.iter_start_ts = &start_ts_slice;
  std::unique_ptr<Iterator> iter{db_->NewIterator(ro)};
  iter->SeekToLast();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
}

TEST_F(DBBasicTestWithTimestamp, TimestampFilterTableReadOnGet) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  BlockBasedTableOptions bbto;
  bbto.block_size = 100;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  // Put
  // Create two SST files
  // file1: key => [1, 3], timestamp => [10, 20]
  // file2, key => [2, 4], timestamp => [30, 40]
  {
    WriteOptions write_opts;
    std::string write_ts = Timestamp(10, 0);
    ASSERT_OK(db_->Put(write_opts, Key1(1), write_ts, "value1"));
    write_ts = Timestamp(20, 0);
    ASSERT_OK(db_->Put(write_opts, Key1(3), write_ts, "value3"));
    ASSERT_OK(Flush());

    write_ts = Timestamp(30, 0);
    ASSERT_OK(db_->Put(write_opts, Key1(2), write_ts, "value2"));
    write_ts = Timestamp(40, 0);
    ASSERT_OK(db_->Put(write_opts, Key1(4), write_ts, "value4"));
    ASSERT_OK(Flush());
  }

  // Get with timestamp
  {
    auto prev_checked_events = options.statistics->getTickerCount(
        Tickers::TIMESTAMP_FILTER_TABLE_CHECKED);
    auto prev_filtered_events = options.statistics->getTickerCount(
        Tickers::TIMESTAMP_FILTER_TABLE_FILTERED);

    // key=3 (ts=20) does not exist at timestamp=1
    std::string read_ts_str = Timestamp(1, 0);
    Slice read_ts_slice = Slice(read_ts_str);
    ReadOptions read_opts;
    read_opts.timestamp = &read_ts_slice;
    std::string value_from_get;
    std::string timestamp_from_get;
    auto status =
        db_->Get(read_opts, Key1(3), &value_from_get, &timestamp_from_get);
    ASSERT_TRUE(status.IsNotFound());
    ASSERT_EQ(value_from_get, std::string(""));
    ASSERT_EQ(timestamp_from_get, std::string(""));

    // key=3 is in the key ranges for both files, so both files will be queried.
    // The table read was skipped because the timestamp is out of the table
    // range, i.e.., 1 < [10,20], [30,40].
    // The tickers increase by 2 due to 2 files.
    ASSERT_EQ(prev_checked_events + 2,
              options.statistics->getTickerCount(
                  Tickers::TIMESTAMP_FILTER_TABLE_CHECKED));
    ASSERT_EQ(prev_filtered_events + 2,
              options.statistics->getTickerCount(
                  Tickers::TIMESTAMP_FILTER_TABLE_FILTERED));

    // key=3 (ts=20) exists at timestamp = 25
    read_ts_str = Timestamp(25, 0);
    read_ts_slice = Slice(read_ts_str);
    read_opts.timestamp = &read_ts_slice;
    ASSERT_OK(
        db_->Get(read_opts, Key1(3), &value_from_get, &timestamp_from_get));
    ASSERT_EQ("value3", value_from_get);
    ASSERT_EQ(Timestamp(20, 0), timestamp_from_get);

    // file1 was not skipped, because the timestamp is in range, [10,20] < 25.
    // file2 was skipped, because the timestamp is not in range, 25 < [30,40].
    // So the checked ticker increase by 2 due to 2 files;
    // filtered ticker increase by 1 because file2 was skipped
    ASSERT_EQ(prev_checked_events + 4,
              options.statistics->getTickerCount(
                  Tickers::TIMESTAMP_FILTER_TABLE_CHECKED));
    ASSERT_EQ(prev_filtered_events + 3,
              options.statistics->getTickerCount(
                  Tickers::TIMESTAMP_FILTER_TABLE_FILTERED));
  }

  Close();
}

class GetNewestUserDefinedTimestampTest : public DBBasicTestWithTimestampBase {
 public:
  explicit GetNewestUserDefinedTimestampTest()
      : DBBasicTestWithTimestampBase("get_newest_udt_test") {}
};

TEST_F(GetNewestUserDefinedTimestampTest, Basic) {
  std::string newest_timestamp;
  // UDT disabled, get InvalidArgument.
  ASSERT_TRUE(db_->GetNewestUserDefinedTimestamp(nullptr, &newest_timestamp)
                  .IsInvalidArgument());
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.max_write_buffer_number = 5;
  options.min_write_buffer_number_to_merge = 4;
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();

  DestroyAndReopen(options);
  // UDT persisted, get NotSupported.
  ASSERT_TRUE(db_->GetNewestUserDefinedTimestamp(nullptr, &newest_timestamp)
                  .IsNotSupported());

  options.persist_user_defined_timestamps = false;
  options.allow_concurrent_memtable_write = false;

  DestroyAndReopen(options);
  ASSERT_TRUE(
      db_->GetNewestUserDefinedTimestamp(nullptr, nullptr).IsInvalidArgument());

  ColumnFamilyHandleImpl* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(
      db_->DefaultColumnFamily());
  ColumnFamilyData* cfd = cfh->cfd();
  // The column family hasn't seen any user defined timestamp
  ASSERT_OK(db_->GetNewestUserDefinedTimestamp(nullptr, &newest_timestamp));
  ASSERT_TRUE(newest_timestamp.empty());

  ASSERT_OK(db_->Put(WriteOptions(), Key(1), EncodeAsUint64(1), "val1"));
  // Testing get newest timestamp from mutable memtable.
  ASSERT_OK(db_->GetNewestUserDefinedTimestamp(nullptr, &newest_timestamp));
  ASSERT_EQ(EncodeAsUint64(1), newest_timestamp);

  ASSERT_OK(db_->Put(WriteOptions(), Key(1), EncodeAsUint64(2), "val2"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable(cfd));
  // Testing get the newest timestamp from immutable memtable because the
  // mutable one is empty.
  ASSERT_OK(db_->GetNewestUserDefinedTimestamp(nullptr, &newest_timestamp));
  ASSERT_EQ(EncodeAsUint64(2), newest_timestamp);

  ASSERT_OK(db_->Put(WriteOptions(), Key(1), EncodeAsUint64(3), "val3"));
  ASSERT_OK(db_->Put(WriteOptions(), Key(1), EncodeAsUint64(4), "val4"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable(cfd));
  // Testing get the newest timestamp from the more recent immutable memtable
  // when there are multiple immutable memtables.
  ASSERT_OK(db_->GetNewestUserDefinedTimestamp(nullptr, &newest_timestamp));
  ASSERT_EQ(EncodeAsUint64(4), newest_timestamp);

  ASSERT_OK(db_->Put(WriteOptions(), Key(1), EncodeAsUint64(5), "val5"));
  // Testing get newest timestamp from mutable memtable when it has data, in the
  // presence of immutable memtables.
  ASSERT_OK(db_->GetNewestUserDefinedTimestamp(nullptr, &newest_timestamp));
  ASSERT_EQ(EncodeAsUint64(5), newest_timestamp);

  ASSERT_OK(Flush());
  // After flushing and all the user defined timestamp are flushed. User defined
  // timestamp info for SST files is available from MANIFEST.
  ASSERT_OK(db_->GetNewestUserDefinedTimestamp(nullptr, &newest_timestamp));
  ASSERT_EQ(EncodeAsUint64(5), newest_timestamp);

  Reopen(options);
  // Similar after flush, when there is no memtables, but some SST files,
  // if MANIFEST records the upperbound of flushed timestamps because timestamps
  // are not persisted in SST files, this info can be found.
  ASSERT_OK(db_->GetNewestUserDefinedTimestamp(nullptr, &newest_timestamp));
  ASSERT_EQ(EncodeAsUint64(5), newest_timestamp);

  Close();
}

TEST_F(GetNewestUserDefinedTimestampTest, ConcurrentWrites) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  options.persist_user_defined_timestamps = false;
  options.allow_concurrent_memtable_write = false;

  DestroyAndReopen(options);

  std::vector<std::thread> threads;
  threads.reserve(10);
  std::atomic<uint64_t> current_ts{0};
  for (int i = 0; i < 10; i++) {
    threads.emplace_back([this, i, &current_ts]() {
      if (i % 2 == 0) {
        std::string newest_timestamp;
        ASSERT_OK(
            db_->GetNewestUserDefinedTimestamp(nullptr, &newest_timestamp));
      } else {
        uint64_t write_ts = current_ts.fetch_add(1);
        ASSERT_OK(db_->Put(WriteOptions(), Key(1), EncodeAsUint64(write_ts),
                           "val" + std::to_string(i)));
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }
  Close();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
