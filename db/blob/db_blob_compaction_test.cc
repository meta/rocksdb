//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

class DBBlobCompactionTest : public DBTestBase {
 public:
  explicit DBBlobCompactionTest()
      : DBTestBase("db_blob_compaction_test", /*env_do_fsync=*/false) {}

  const std::vector<InternalStats::CompactionStats>& GetCompactionStats() {
    VersionSet* const versions = dbfull()->GetVersionSet();
    assert(versions);
    assert(versions->GetColumnFamilySet());

    ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
    assert(cfd);

    const InternalStats* const internal_stats = cfd->internal_stats();
    assert(internal_stats);

    return internal_stats->TEST_GetCompactionStats();
  }
};

namespace {

class FilterByKeyLength : public CompactionFilter {
 public:
  explicit FilterByKeyLength(size_t len) : length_threshold_(len) {}
  const char* Name() const override {
    return "rocksdb.compaction.filter.by.key.length";
  }
  CompactionFilter::Decision FilterBlobByKey(
      int /*level*/, const Slice& key, std::string* /*new_value*/,
      std::string* /*skip_until*/) const override {
    if (key.size() < length_threshold_) {
      return CompactionFilter::Decision::kRemove;
    }
    return CompactionFilter::Decision::kKeep;
  }

 private:
  size_t length_threshold_;
};

class FilterByValueLength : public CompactionFilter {
 public:
  explicit FilterByValueLength(size_t len) : length_threshold_(len) {}
  const char* Name() const override {
    return "rocksdb.compaction.filter.by.value.length";
  }
  CompactionFilter::Decision FilterV2(
      int /*level*/, const Slice& /*key*/, ValueType /*value_type*/,
      const Slice& existing_value, std::string* /*new_value*/,
      std::string* /*skip_until*/) const override {
    if (existing_value.size() < length_threshold_) {
      return CompactionFilter::Decision::kRemove;
    }
    return CompactionFilter::Decision::kKeep;
  }

 private:
  size_t length_threshold_;
};

class BadBlobCompactionFilter : public CompactionFilter {
 public:
  explicit BadBlobCompactionFilter(std::string prefix,
                                   CompactionFilter::Decision filter_by_key,
                                   CompactionFilter::Decision filter_v2)
      : prefix_(std::move(prefix)),
        filter_blob_by_key_(filter_by_key),
        filter_v2_(filter_v2) {}
  const char* Name() const override { return "rocksdb.compaction.filter.bad"; }
  CompactionFilter::Decision FilterBlobByKey(
      int /*level*/, const Slice& key, std::string* /*new_value*/,
      std::string* /*skip_until*/) const override {
    if (key.size() >= prefix_.size() &&
        0 == strncmp(prefix_.data(), key.data(), prefix_.size())) {
      return CompactionFilter::Decision::kUndetermined;
    }
    return filter_blob_by_key_;
  }
  CompactionFilter::Decision FilterV2(
      int /*level*/, const Slice& /*key*/, ValueType /*value_type*/,
      const Slice& /*existing_value*/, std::string* /*new_value*/,
      std::string* /*skip_until*/) const override {
    return filter_v2_;
  }

 private:
  const std::string prefix_;
  const CompactionFilter::Decision filter_blob_by_key_;
  const CompactionFilter::Decision filter_v2_;
};

class ValueBlindWriteFilter : public CompactionFilter {
 public:
  explicit ValueBlindWriteFilter(std::string new_val)
      : new_value_(std::move(new_val)) {}
  const char* Name() const override {
    return "rocksdb.compaction.filter.blind.write";
  }
  CompactionFilter::Decision FilterBlobByKey(
      int level, const Slice& key, std::string* new_value,
      std::string* skip_until) const override;

 private:
  const std::string new_value_;
};

CompactionFilter::Decision ValueBlindWriteFilter::FilterBlobByKey(
    int /*level*/, const Slice& /*key*/, std::string* new_value,
    std::string* /*skip_until*/) const {
  assert(new_value);
  new_value->assign(new_value_);
  return CompactionFilter::Decision::kChangeValue;
}

class ValueMutationFilter : public CompactionFilter {
 public:
  explicit ValueMutationFilter(std::string padding)
      : padding_(std::move(padding)) {}
  const char* Name() const override {
    return "rocksdb.compaction.filter.value.mutation";
  }
  CompactionFilter::Decision FilterV2(int level, const Slice& key,
                                      ValueType value_type,
                                      const Slice& existing_value,
                                      std::string* new_value,
                                      std::string* skip_until) const override;

 private:
  const std::string padding_;
};

CompactionFilter::Decision ValueMutationFilter::FilterV2(
    int /*level*/, const Slice& /*key*/, ValueType value_type,
    const Slice& existing_value, std::string* new_value,
    std::string* /*skip_until*/) const {
  assert(CompactionFilter::ValueType::kBlobIndex != value_type);
  if (CompactionFilter::ValueType::kValue != value_type) {
    return CompactionFilter::Decision::kKeep;
  }
  assert(new_value);
  new_value->assign(existing_value.data(), existing_value.size());
  new_value->append(padding_);
  return CompactionFilter::Decision::kChangeValue;
}

class AlwaysKeepFilter : public CompactionFilter {
 public:
  explicit AlwaysKeepFilter() = default;
  const char* Name() const override {
    return "rocksdb.compaction.filter.always.keep";
  }
  CompactionFilter::Decision FilterV2(
      int /*level*/, const Slice& /*key*/, ValueType /*value_type*/,
      const Slice& /*existing_value*/, std::string* /*new_value*/,
      std::string* /*skip_until*/) const override {
    return CompactionFilter::Decision::kKeep;
  }
};

class SkipUntilFilter : public CompactionFilter {
 public:
  explicit SkipUntilFilter(std::string skip_until)
      : skip_until_(std::move(skip_until)) {}

  const char* Name() const override {
    return "rocksdb.compaction.filter.skip.until";
  }

  CompactionFilter::Decision FilterV2(int /* level */, const Slice& /* key */,
                                      ValueType /* value_type */,
                                      const Slice& /* existing_value */,
                                      std::string* /* new_value */,
                                      std::string* skip_until) const override {
    assert(skip_until);
    *skip_until = skip_until_;

    return CompactionFilter::Decision::kRemoveAndSkipUntil;
  }

 private:
  std::string skip_until_;
};

}  // anonymous namespace

class DBBlobBadCompactionFilterTest
    : public DBBlobCompactionTest,
      public testing::WithParamInterface<
          std::tuple<std::string, CompactionFilter::Decision,
                     CompactionFilter::Decision>> {
 public:
  explicit DBBlobBadCompactionFilterTest()
      : compaction_filter_guard_(new BadBlobCompactionFilter(
            std::get<0>(GetParam()), std::get<1>(GetParam()),
            std::get<2>(GetParam()))) {}

 protected:
  std::unique_ptr<CompactionFilter> compaction_filter_guard_;
};

INSTANTIATE_TEST_CASE_P(
    BadCompactionFilter, DBBlobBadCompactionFilterTest,
    testing::Combine(
        testing::Values("a"),
        testing::Values(CompactionFilter::Decision::kChangeBlobIndex,
                        CompactionFilter::Decision::kIOError),
        testing::Values(CompactionFilter::Decision::kUndetermined,
                        CompactionFilter::Decision::kChangeBlobIndex,
                        CompactionFilter::Decision::kIOError)));

TEST_F(DBBlobCompactionTest, FilterByKeyLength) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.create_if_missing = true;
  constexpr size_t kKeyLength = 2;
  std::unique_ptr<CompactionFilter> compaction_filter_guard(
      new FilterByKeyLength(kKeyLength));
  options.compaction_filter = compaction_filter_guard.get();

  constexpr char short_key[] = "a";
  constexpr char long_key[] = "abc";
  constexpr char blob_value[] = "value";

  DestroyAndReopen(options);
  ASSERT_OK(Put(short_key, blob_value));
  ASSERT_OK(Put(long_key, blob_value));
  ASSERT_OK(Flush());
  CompactRangeOptions cro;
  ASSERT_OK(db_->CompactRange(cro, /*begin=*/nullptr, /*end=*/nullptr));
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), short_key, &value).IsNotFound());
  value.clear();
  ASSERT_OK(db_->Get(ReadOptions(), long_key, &value));
  ASSERT_EQ("value", value);

  const auto& compaction_stats = GetCompactionStats();
  ASSERT_GE(compaction_stats.size(), 2);

  // Filter decides between kKeep and kRemove solely based on key;
  // this involves neither reading nor writing blobs
  ASSERT_EQ(compaction_stats[1].bytes_read_blob, 0);
  ASSERT_EQ(compaction_stats[1].bytes_written_blob, 0);

  Close();
}

TEST_F(DBBlobCompactionTest, FilterByValueLength) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 5;
  options.create_if_missing = true;
  constexpr size_t kValueLength = 5;
  std::unique_ptr<CompactionFilter> compaction_filter_guard(
      new FilterByValueLength(kValueLength));
  options.compaction_filter = compaction_filter_guard.get();

  const std::vector<std::string> short_value_keys = {"a", "e", "j"};
  constexpr char short_value[] = "val";
  const std::vector<std::string> long_value_keys = {"b", "f", "k"};
  constexpr char long_value[] = "valuevalue";

  DestroyAndReopen(options);
  for (size_t i = 0; i < short_value_keys.size(); ++i) {
    ASSERT_OK(Put(short_value_keys[i], short_value));
  }
  for (size_t i = 0; i < short_value_keys.size(); ++i) {
    ASSERT_OK(Put(long_value_keys[i], long_value));
  }
  ASSERT_OK(Flush());
  CompactRangeOptions cro;
  ASSERT_OK(db_->CompactRange(cro, /*begin=*/nullptr, /*end=*/nullptr));
  std::string value;
  for (size_t i = 0; i < short_value_keys.size(); ++i) {
    ASSERT_TRUE(
        db_->Get(ReadOptions(), short_value_keys[i], &value).IsNotFound());
    value.clear();
  }
  for (size_t i = 0; i < long_value_keys.size(); ++i) {
    ASSERT_OK(db_->Get(ReadOptions(), long_value_keys[i], &value));
    ASSERT_EQ(long_value, value);
  }

  const auto& compaction_stats = GetCompactionStats();
  ASSERT_GE(compaction_stats.size(), 2);

  // Filter decides between kKeep and kRemove based on value;
  // this involves reading but not writing blobs
  ASSERT_GT(compaction_stats[1].bytes_read_blob, 0);
  ASSERT_EQ(compaction_stats[1].bytes_written_blob, 0);

  Close();
}

TEST_F(DBBlobCompactionTest, BlobCompactWithStartingLevel) {
  Options options = GetDefaultOptions();

  options.enable_blob_files = true;
  options.min_blob_size = 1000;
  options.blob_file_starting_level = 5;
  options.create_if_missing = true;

  // Open DB with fixed-prefix sst-partitioner so that compaction will cut
  // new table file when encountering a new key whose 1-byte prefix changes.
  constexpr size_t key_len = 1;
  options.sst_partitioner_factory =
      NewSstPartitionerFixedPrefixFactory(key_len);

  ASSERT_OK(TryReopen(options));

  constexpr size_t blob_size = 3000;

  constexpr char first_key[] = "a";
  const std::string first_blob(blob_size, 'a');
  ASSERT_OK(Put(first_key, first_blob));

  constexpr char second_key[] = "b";
  const std::string second_blob(2 * blob_size, 'b');
  ASSERT_OK(Put(second_key, second_blob));

  constexpr char third_key[] = "d";
  const std::string third_blob(blob_size, 'd');
  ASSERT_OK(Put(third_key, third_blob));

  ASSERT_OK(Flush());

  constexpr char fourth_key[] = "c";
  const std::string fourth_blob(blob_size, 'c');
  ASSERT_OK(Put(fourth_key, fourth_blob));

  ASSERT_OK(Flush());

  ASSERT_EQ(0, GetBlobFileNumbers().size());
  ASSERT_EQ(2, NumTableFilesAtLevel(/*level=*/0));
  ASSERT_EQ(0, NumTableFilesAtLevel(/*level=*/1));

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                              /*end=*/nullptr));

  // No blob file should be created since blob_file_starting_level is 5.
  ASSERT_EQ(0, GetBlobFileNumbers().size());
  ASSERT_EQ(0, NumTableFilesAtLevel(/*level=*/0));
  ASSERT_EQ(4, NumTableFilesAtLevel(/*level=*/1));

  {
    options.blob_file_starting_level = 1;
    DestroyAndReopen(options);

    ASSERT_OK(Put(first_key, first_blob));
    ASSERT_OK(Put(second_key, second_blob));
    ASSERT_OK(Put(third_key, third_blob));
    ASSERT_OK(Flush());
    ASSERT_OK(Put(fourth_key, fourth_blob));
    ASSERT_OK(Flush());

    ASSERT_EQ(0, GetBlobFileNumbers().size());
    ASSERT_EQ(2, NumTableFilesAtLevel(/*level=*/0));
    ASSERT_EQ(0, NumTableFilesAtLevel(/*level=*/1));

    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                                /*end=*/nullptr));
    // The compaction's output level equals to blob_file_starting_level.
    ASSERT_EQ(1, GetBlobFileNumbers().size());
    ASSERT_EQ(0, NumTableFilesAtLevel(/*level=*/0));
    ASSERT_EQ(4, NumTableFilesAtLevel(/*level=*/1));
  }

  Close();
}

TEST_F(DBBlobCompactionTest, BlindWriteFilter) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.create_if_missing = true;
  constexpr char new_blob_value[] = "new_blob_value";
  std::unique_ptr<CompactionFilter> compaction_filter_guard(
      new ValueBlindWriteFilter(new_blob_value));
  options.compaction_filter = compaction_filter_guard.get();
  DestroyAndReopen(options);
  const std::vector<std::string> keys = {"a", "b", "c"};
  const std::vector<std::string> values = {"a_value", "b_value", "c_value"};
  assert(keys.size() == values.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_OK(Put(keys[i], values[i]));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                              /*end=*/nullptr));
  for (const auto& key : keys) {
    ASSERT_EQ(new_blob_value, Get(key));
  }

  const auto& compaction_stats = GetCompactionStats();
  ASSERT_GE(compaction_stats.size(), 2);

  // Filter unconditionally changes value in FilterBlobByKey;
  // this involves writing but not reading blobs
  ASSERT_EQ(compaction_stats[1].bytes_read_blob, 0);
  ASSERT_GT(compaction_stats[1].bytes_written_blob, 0);

  Close();
}

TEST_F(DBBlobCompactionTest, SkipUntilFilter) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;

  std::unique_ptr<CompactionFilter> compaction_filter_guard(
      new SkipUntilFilter("z"));
  options.compaction_filter = compaction_filter_guard.get();

  Reopen(options);

  const std::vector<std::string> keys{"a", "b", "c"};
  const std::vector<std::string> values{"a_value", "b_value", "c_value"};
  assert(keys.size() == values.size());

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_OK(Put(keys[i], values[i]));
  }

  ASSERT_OK(Flush());

  int process_in_flow_called = 0;

  SyncPoint::GetInstance()->SetCallBack(
      "BlobCountingIterator::UpdateAndCountBlobIfNeeded:ProcessInFlow",
      [&process_in_flow_called](void* /* arg */) { ++process_in_flow_called; });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /* begin */ nullptr,
                              /* end */ nullptr));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  for (const auto& key : keys) {
    ASSERT_EQ(Get(key), "NOT_FOUND");
  }

  // Make sure SkipUntil was performed using iteration rather than Seek
  ASSERT_EQ(process_in_flow_called, keys.size());

  Close();
}

TEST_P(DBBlobBadCompactionFilterTest, BadDecisionFromCompactionFilter) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.create_if_missing = true;
  options.compaction_filter = compaction_filter_guard_.get();
  DestroyAndReopen(options);
  ASSERT_OK(Put("b", "value"));
  ASSERT_OK(Flush());
  ASSERT_TRUE(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                                /*end=*/nullptr)
                  .IsNotSupported());
  Close();

  DestroyAndReopen(options);
  std::string key(std::get<0>(GetParam()));
  ASSERT_OK(Put(key, "value"));
  ASSERT_OK(Flush());
  ASSERT_TRUE(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                                /*end=*/nullptr)
                  .IsNotSupported());
  Close();
}

TEST_F(DBBlobCompactionTest, CompactionFilterInlinedTTLIndex) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  std::unique_ptr<CompactionFilter> compaction_filter_guard(
      new ValueMutationFilter(""));
  options.compaction_filter = compaction_filter_guard.get();
  DestroyAndReopen(options);
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";
  // Fake an inlined TTL blob index.
  std::string blob_index;
  constexpr uint64_t expiration = 1234567890;
  BlobIndex::EncodeInlinedTTL(&blob_index, expiration, blob);
  WriteBatch batch;
  ASSERT_OK(WriteBatchInternal::PutBlobIndex(&batch, 0, key, blob_index));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  ASSERT_OK(Flush());
  ASSERT_TRUE(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                                /*end=*/nullptr)
                  .IsCorruption());
  Close();
}

TEST_F(DBBlobCompactionTest, CompactionFilter) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  constexpr char padding[] = "_delta";
  std::unique_ptr<CompactionFilter> compaction_filter_guard(
      new ValueMutationFilter(padding));
  options.compaction_filter = compaction_filter_guard.get();
  DestroyAndReopen(options);
  const std::vector<std::pair<std::string, std::string>> kvs = {
      {"a", "a_value"}, {"b", "b_value"}, {"c", "c_value"}};
  for (const auto& kv : kvs) {
    ASSERT_OK(Put(kv.first, kv.second));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                              /*end=*/nullptr));
  for (const auto& kv : kvs) {
    ASSERT_EQ(kv.second + std::string(padding), Get(kv.first));
  }

  const auto& compaction_stats = GetCompactionStats();
  ASSERT_GE(compaction_stats.size(), 2);

  // Filter changes the value using the previous value in FilterV2;
  // this involves reading and writing blobs
  ASSERT_GT(compaction_stats[1].bytes_read_blob, 0);
  ASSERT_GT(compaction_stats[1].bytes_written_blob, 0);

  Close();
}

TEST_F(DBBlobCompactionTest, CorruptedBlobIndex) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  std::unique_ptr<CompactionFilter> compaction_filter_guard(
      new ValueMutationFilter(""));
  options.compaction_filter = compaction_filter_guard.get();
  DestroyAndReopen(options);

  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  ASSERT_OK(Put(key, blob));
  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionIterator::InvokeFilterIfNeeded::TamperWithBlobIndex",
      [](void* arg) {
        Slice* const blob_index = static_cast<Slice*>(arg);
        assert(blob_index);
        assert(!blob_index->empty());
        blob_index->remove_prefix(1);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_TRUE(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                                /*end=*/nullptr)
                  .IsCorruption());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  Close();
}

TEST_F(DBBlobCompactionTest, CompactionFilterReadBlobAndKeep) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  std::unique_ptr<CompactionFilter> compaction_filter_guard(
      new AlwaysKeepFilter());
  options.compaction_filter = compaction_filter_guard.get();
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "foo_value"));
  ASSERT_OK(Flush());
  std::vector<uint64_t> blob_files = GetBlobFileNumbers();
  ASSERT_EQ(1, blob_files.size());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                              /*end=*/nullptr));
  ASSERT_EQ(blob_files, GetBlobFileNumbers());

  const auto& compaction_stats = GetCompactionStats();
  ASSERT_GE(compaction_stats.size(), 2);

  // Filter decides to keep the existing value in FilterV2;
  // this involves reading but not writing blobs
  ASSERT_GT(compaction_stats[1].bytes_read_blob, 0);
  ASSERT_EQ(compaction_stats[1].bytes_written_blob, 0);

  Close();
}

TEST_F(DBBlobCompactionTest, TrackGarbage) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;

  Reopen(options);

  // First table+blob file pair: 4 blobs with different keys
  constexpr char first_key[] = "first_key";
  constexpr char first_value[] = "first_value";
  constexpr char second_key[] = "second_key";
  constexpr char second_value[] = "second_value";
  constexpr char third_key[] = "third_key";
  constexpr char third_value[] = "third_value";
  constexpr char fourth_key[] = "fourth_key";
  constexpr char fourth_value[] = "fourth_value";

  ASSERT_OK(Put(first_key, first_value));
  ASSERT_OK(Put(second_key, second_value));
  ASSERT_OK(Put(third_key, third_value));
  ASSERT_OK(Put(fourth_key, fourth_value));
  ASSERT_OK(Flush());

  // Second table+blob file pair: overwrite 2 existing keys
  constexpr char new_first_value[] = "new_first_value";
  constexpr char new_second_value[] = "new_second_value";

  ASSERT_OK(Put(first_key, new_first_value));
  ASSERT_OK(Put(second_key, new_second_value));
  ASSERT_OK(Flush());

  // Compact them together. The first blob file should have 2 garbage blobs
  // corresponding to the 2 overwritten keys.
  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), begin, end));

  VersionSet* const versions = dbfull()->GetVersionSet();
  assert(versions);
  assert(versions->GetColumnFamilySet());

  ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
  assert(cfd);

  Version* const current = cfd->current();
  assert(current);

  const VersionStorageInfo* const storage_info = current->storage_info();
  assert(storage_info);

  const auto& blob_files = storage_info->GetBlobFiles();
  ASSERT_EQ(blob_files.size(), 2);

  {
    const auto& meta = blob_files.front();
    assert(meta);

    constexpr uint64_t first_expected_bytes =
        sizeof(first_value) - 1 +
        BlobLogRecord::CalculateAdjustmentForRecordHeader(sizeof(first_key) -
                                                          1);
    constexpr uint64_t second_expected_bytes =
        sizeof(second_value) - 1 +
        BlobLogRecord::CalculateAdjustmentForRecordHeader(sizeof(second_key) -
                                                          1);
    constexpr uint64_t third_expected_bytes =
        sizeof(third_value) - 1 +
        BlobLogRecord::CalculateAdjustmentForRecordHeader(sizeof(third_key) -
                                                          1);
    constexpr uint64_t fourth_expected_bytes =
        sizeof(fourth_value) - 1 +
        BlobLogRecord::CalculateAdjustmentForRecordHeader(sizeof(fourth_key) -
                                                          1);

    ASSERT_EQ(meta->GetTotalBlobCount(), 4);
    ASSERT_EQ(meta->GetTotalBlobBytes(),
              first_expected_bytes + second_expected_bytes +
                  third_expected_bytes + fourth_expected_bytes);
    ASSERT_EQ(meta->GetGarbageBlobCount(), 2);
    ASSERT_EQ(meta->GetGarbageBlobBytes(),
              first_expected_bytes + second_expected_bytes);
  }

  {
    const auto& meta = blob_files.back();
    assert(meta);

    constexpr uint64_t new_first_expected_bytes =
        sizeof(new_first_value) - 1 +
        BlobLogRecord::CalculateAdjustmentForRecordHeader(sizeof(first_key) -
                                                          1);
    constexpr uint64_t new_second_expected_bytes =
        sizeof(new_second_value) - 1 +
        BlobLogRecord::CalculateAdjustmentForRecordHeader(sizeof(second_key) -
                                                          1);

    ASSERT_EQ(meta->GetTotalBlobCount(), 2);
    ASSERT_EQ(meta->GetTotalBlobBytes(),
              new_first_expected_bytes + new_second_expected_bytes);
    ASSERT_EQ(meta->GetGarbageBlobCount(), 0);
    ASSERT_EQ(meta->GetGarbageBlobBytes(), 0);
  }
}

TEST_F(DBBlobCompactionTest, MergeBlobWithBase) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  options.disable_auto_compactions = true;

  Reopen(options);
  ASSERT_OK(Put("Key1", "v1_1"));
  ASSERT_OK(Put("Key2", "v2_1"));
  ASSERT_OK(Flush());

  ASSERT_OK(Merge("Key1", "v1_2"));
  ASSERT_OK(Merge("Key2", "v2_2"));
  ASSERT_OK(Flush());

  ASSERT_OK(Merge("Key1", "v1_3"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                              /*end=*/nullptr));
  ASSERT_EQ(Get("Key1"), "v1_1,v1_2,v1_3");
  ASSERT_EQ(Get("Key2"), "v2_1,v2_2");
  Close();
}

TEST_F(DBBlobCompactionTest, CompactionReadaheadGarbageCollection) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  options.blob_compaction_readahead_size = 1 << 10;
  options.disable_auto_compactions = true;

  Reopen(options);

  ASSERT_OK(Put("key", "lime"));
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("key", "pie"));
  ASSERT_OK(Put("foo", "baz"));
  ASSERT_OK(Flush());

  size_t num_non_prefetch_reads = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileReader::GetBlob:ReadFromFile",
      [&num_non_prefetch_reads](void* /* arg */) { ++num_non_prefetch_reads; });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), begin, end));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_EQ(Get("key"), "pie");
  ASSERT_EQ(Get("foo"), "baz");
  ASSERT_EQ(num_non_prefetch_reads, 0);

  Close();
}

TEST_F(DBBlobCompactionTest, CompactionReadaheadFilter) {
  Options options = GetDefaultOptions();

  std::unique_ptr<CompactionFilter> compaction_filter_guard(
      new ValueMutationFilter("pie"));

  options.compaction_filter = compaction_filter_guard.get();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.blob_compaction_readahead_size = 1 << 10;
  options.disable_auto_compactions = true;

  Reopen(options);

  ASSERT_OK(Put("key", "lime"));
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Flush());

  size_t num_non_prefetch_reads = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileReader::GetBlob:ReadFromFile",
      [&num_non_prefetch_reads](void* /* arg */) { ++num_non_prefetch_reads; });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), begin, end));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_EQ(Get("key"), "limepie");
  ASSERT_EQ(Get("foo"), "barpie");
  ASSERT_EQ(num_non_prefetch_reads, 0);

  Close();
}

TEST_F(DBBlobCompactionTest, CompactionReadaheadMerge) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.blob_compaction_readahead_size = 1 << 10;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  options.disable_auto_compactions = true;

  Reopen(options);

  ASSERT_OK(Put("key", "lime"));
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Flush());

  ASSERT_OK(Merge("key", "pie"));
  ASSERT_OK(Merge("foo", "baz"));
  ASSERT_OK(Flush());

  size_t num_non_prefetch_reads = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileReader::GetBlob:ReadFromFile",
      [&num_non_prefetch_reads](void* /* arg */) { ++num_non_prefetch_reads; });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), begin, end));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_EQ(Get("key"), "lime,pie");
  ASSERT_EQ(Get("foo"), "bar,baz");
  ASSERT_EQ(num_non_prefetch_reads, 0);

  Close();
}

TEST_F(DBBlobCompactionTest, CompactionDoNotFillCache) {
  Options options = GetDefaultOptions();

  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  options.disable_auto_compactions = true;
  options.statistics = CreateDBStatistics();

  LRUCacheOptions cache_options;
  cache_options.capacity = 1 << 20;
  cache_options.metadata_charge_policy = kDontChargeCacheMetadata;

  options.blob_cache = NewLRUCache(cache_options);

  Reopen(options);

  ASSERT_OK(Put("key", "lime"));
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("key", "pie"));
  ASSERT_OK(Put("foo", "baz"));
  ASSERT_OK(Flush());

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), begin, end));

  ASSERT_EQ(options.statistics->getTickerCount(BLOB_DB_CACHE_ADD), 0);

  Close();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
