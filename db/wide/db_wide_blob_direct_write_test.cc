//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <array>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "db/blob/blob_file_partition_manager.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/column_family.h"
#include "db/db_test_util.h"
#include "db/wide/wide_column_serialization.h"
#include "db/wide/wide_column_test_util.h"
#include "db/write_batch_internal.h"
#include "port/stack_trace.h"
#include "util/coding.h"
#include "utilities/trie_index/trie_index_factory.h"

namespace ROCKSDB_NAMESPACE {

using wide_column_test_util::GenerateLargeValue;
using wide_column_test_util::GetOptionsForBlobTest;

using WideColumnStringPairs = std::vector<std::pair<std::string, std::string>>;

// `WideColumns` only stores non-owning `Slice`s, so tests that synthesize
// dynamic column data need separate backing storage that outlives the DB call
// or comparison.
static WideColumns ToWideColumns(const WideColumnStringPairs& columns) {
  WideColumns wide_columns;
  wide_columns.reserve(columns.size());
  for (const auto& column : columns) {
    wide_columns.emplace_back(column.first, column.second);
  }
  return wide_columns;
}

class DBWideBlobDirectWriteTest : public DBTestBase {
 protected:
  DBWideBlobDirectWriteTest()
      : DBTestBase("db_wide_blob_direct_write_test",
                   /*env_do_fsync=*/false) {}

  Options GetBlobTestOptions() {
    return wide_column_test_util::GetOptionsForBlobTest(GetDefaultOptions());
  }

  Options GetDirectWriteOptions() {
    return wide_column_test_util::GetDirectWriteOptions(GetDefaultOptions());
  }

  size_t CountBlobFiles() { return DBTestBase::GetBlobFileNumbers().size(); }

  static std::string MakeIteratorStressKey(int index) {
    std::string key;
    auto append_big_endian_u64 = [](uint64_t value, std::string* dst) {
      for (int shift = 56; shift >= 0; shift -= 8) {
        dst->push_back(static_cast<char>((value >> shift) & 0xff));
      }
    };
    append_big_endian_u64(0x3900000000000000ULL | static_cast<uint64_t>(index),
                          &key);
    append_big_endian_u64(0x1200000000000000ULL | static_cast<uint64_t>(index),
                          &key);
    append_big_endian_u64(0x2900000000000000ULL | static_cast<uint64_t>(index),
                          &key);
    return key;
  }

  static WideColumnStringPairs MakeIteratorStressColumnData(char fill,
                                                            int index) {
    return WideColumnStringPairs{
        {"", std::string(128, fill) + "_" + std::to_string(index)},
        {"meta", std::string(96, static_cast<char>(fill + 1)) + "_" +
                     std::to_string(index)}};
  }

  static WideColumnStringPairs MakeSecondaryIteratorStressColumnData(
      char fill, int index) {
    return WideColumnStringPairs{
        {"aux", std::string(112, fill) + "_" + std::to_string(index)},
        {"zeta", std::string(80, static_cast<char>(fill + 1)) + "_" +
                     std::to_string(index)}};
  }

  static WideColumnStringPairs MakeCoalescedIteratorStressColumnData(
      char primary_fill, char secondary_fill, int index) {
    const auto primary = MakeIteratorStressColumnData(primary_fill, index);
    const auto secondary =
        MakeSecondaryIteratorStressColumnData(secondary_fill, index);
    return WideColumnStringPairs{
        {primary[0].first, primary[0].second},
        {secondary[0].first, secondary[0].second},
        {primary[1].first, primary[1].second},
        {secondary[1].first, secondary[1].second},
    };
  }

  void PopulateIteratorStressEntities(int num_keys) {
    for (int index = 0; index < num_keys; ++index) {
      const auto columns_data = MakeIteratorStressColumnData('A', index);
      ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                               MakeIteratorStressKey(index),
                               ToWideColumns(columns_data)));
    }
    ASSERT_OK(Flush());
    MoveFilesToLevel(1);

    for (int index = 0; index < num_keys; ++index) {
      const auto columns_data = MakeIteratorStressColumnData('C', index);
      ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                               MakeIteratorStressKey(index),
                               ToWideColumns(columns_data)));
    }
  }

  void PopulateMultiCfIteratorStressEntities(
      int num_keys, ColumnFamilyHandle* secondary_cfh) {
    for (int index = 0; index < num_keys; ++index) {
      ASSERT_OK(db_->PutEntity(
          WriteOptions(), db_->DefaultColumnFamily(),
          MakeIteratorStressKey(index),
          ToWideColumns(MakeIteratorStressColumnData('A', index))));
      ASSERT_OK(db_->PutEntity(
          WriteOptions(), secondary_cfh, MakeIteratorStressKey(index),
          ToWideColumns(MakeSecondaryIteratorStressColumnData('K', index))));
    }
    ASSERT_OK(Flush({0, 1}));
    MoveFilesToLevel(1, 0);
    MoveFilesToLevel(1, 1);

    for (int index = 0; index < num_keys; ++index) {
      ASSERT_OK(db_->PutEntity(
          WriteOptions(), db_->DefaultColumnFamily(),
          MakeIteratorStressKey(index),
          ToWideColumns(MakeIteratorStressColumnData('C', index))));
      ASSERT_OK(db_->PutEntity(
          WriteOptions(), secondary_cfh, MakeIteratorStressKey(index),
          ToWideColumns(MakeSecondaryIteratorStressColumnData('M', index))));
    }
  }

  struct SingleCfCoalescingIteratorScenario {
    bool use_trie_index = false;
    bool refresh_before_seek = false;
    bool refresh_mid_iteration = false;
    bool reuse_iterator_before_refresh = false;
    bool create_control_before_refresh = true;
  };

  // DBImpl::NewCoalescingIterator() intentionally returns the plain child
  // iterator when only one CF is requested. Keep this direct-iterator fast
  // path covered since it is what single-CF callers actually exercise.
  void VerifySingleCfCoalescingIteratorMatchesDirectIterator(
      const SingleCfCoalescingIteratorScenario& scenario) {
    constexpr int kNumKeys = 32;

    Options options = GetDirectWriteOptions();
    options.min_blob_size = 64;
    options.disable_auto_compactions = true;
    options.prefix_extractor.reset(NewFixedPrefixTransform(7));

    Reopen(options);
    PopulateIteratorStressEntities(kNumKeys);

    ReadOptions coalescing_ro;
    coalescing_ro.allow_unprepared_value = true;
    coalescing_ro.auto_refresh_iterator_with_snapshot = true;
    coalescing_ro.pin_data = true;
    const Snapshot* snapshot = db_->GetSnapshot();
    coalescing_ro.snapshot = snapshot;

    trie_index::TrieIndexFactory trie_index_factory;
    if (scenario.use_trie_index) {
      coalescing_ro.read_index = ReadOptions::ReadIndex::kPreferCustom;
    }

    ReadOptions control_ro = coalescing_ro;
    control_ro.allow_unprepared_value = false;
    control_ro.total_order_seek = true;

    std::unique_ptr<Iterator> coalescing =
        db_->NewCoalescingIterator(coalescing_ro, {db_->DefaultColumnFamily()});
    std::unique_ptr<Iterator> control;
    if (scenario.create_control_before_refresh) {
      control.reset(db_->NewIterator(control_ro, db_->DefaultColumnFamily()));
    }

    if (scenario.reuse_iterator_before_refresh) {
      const int first_index = kNumKeys / 8;
      const auto expected_columns_data =
          MakeIteratorStressColumnData('C', first_index);
      const WideColumns expected_columns = ToWideColumns(expected_columns_data);
      coalescing->Seek(MakeIteratorStressKey(first_index));
      ASSERT_TRUE(coalescing->Valid());
      ASSERT_TRUE(coalescing->PrepareValue());
      ASSERT_EQ(expected_columns, coalescing->columns());
      coalescing->Next();
      ASSERT_TRUE(coalescing->Valid());
      ASSERT_TRUE(coalescing->PrepareValue());
    }

    if (scenario.refresh_before_seek) {
      ASSERT_OK(Flush());
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    }

    if (!control) {
      control.reset(db_->NewIterator(control_ro, db_->DefaultColumnFamily()));
    }

    const int start_index = kNumKeys / 4;
    const std::string start_key = MakeIteratorStressKey(start_index);

    coalescing->Seek(start_key);
    control->Seek(start_key);

    for (int index = start_index; index < kNumKeys; ++index) {
      ASSERT_TRUE(coalescing->Valid());
      ASSERT_TRUE(control->Valid());
      ASSERT_EQ(MakeIteratorStressKey(index), coalescing->key().ToString());
      ASSERT_EQ(control->key(), coalescing->key());

      ASSERT_TRUE(coalescing->PrepareValue());
      ASSERT_EQ(control->value(), coalescing->value());
      ASSERT_EQ(control->columns(), coalescing->columns());

      if (scenario.refresh_mid_iteration && index == kNumKeys / 2) {
        ASSERT_OK(Flush());
        ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
      }

      coalescing->Next();
      control->Next();
    }

    ASSERT_FALSE(coalescing->Valid());
    ASSERT_FALSE(control->Valid());
    ASSERT_OK(coalescing->status());
    ASSERT_OK(control->status());

    db_->ReleaseSnapshot(snapshot);
  }
};

namespace {

std::string EncodeFixedTTL(uint64_t ttl) {
  std::string encoded;
  PutFixed64(&encoded, ttl);
  return encoded;
}

char ValueFillForIndex(size_t index) {
  return static_cast<char>('A' + (index % 26));
}

WideColumnStringPairs BuildTTLWideEntityData(const std::string& value,
                                             const std::string& ttl) {
  return WideColumnStringPairs{{"", value}, {"ttl", ttl}};
}

std::string BuildTTLKey(uint32_t partition, size_t cycle) {
  char key_buf[48];
  snprintf(key_buf, sizeof(key_buf), "lazy_ttl_p%03u_c%zu", partition, cycle);
  return key_buf;
}

class TTLOnlyLazyDropFilter : public CompactionFilter {
 public:
  TTLOnlyLazyDropFilter(std::atomic<uint64_t>* ttl_cutoff,
                        std::atomic<int>* filter_call_count,
                        std::atomic<int>* ttl_columns_seen,
                        std::atomic<int>* ttl_bad_size_count,
                        std::atomic<int>* missing_ttl_count,
                        std::atomic<int>* blob_columns_seen)
      : ttl_cutoff_(ttl_cutoff),
        filter_call_count_(filter_call_count),
        ttl_columns_seen_(ttl_columns_seen),
        ttl_bad_size_count_(ttl_bad_size_count),
        missing_ttl_count_(missing_ttl_count),
        blob_columns_seen_(blob_columns_seen) {}

  Decision FilterV4(
      int /*level*/, const Slice& /*key*/, ValueType value_type,
      const Slice* /*existing_value*/, const WideColumns* existing_columns,
      std::string* /*new_value*/,
      std::vector<std::pair<std::string, std::string>>* /*new_columns*/,
      std::string* /*skip_until*/,
      WideColumnBlobResolver* blob_resolver = nullptr) const override {
    if (value_type != ValueType::kWideColumnEntity || !existing_columns) {
      return Decision::kKeep;
    }

    ++(*filter_call_count_);

    for (size_t i = 0; i < existing_columns->size(); ++i) {
      if (blob_resolver != nullptr && blob_resolver->IsBlobColumn(i)) {
        ++(*blob_columns_seen_);
        continue;
      }

      const auto& column = (*existing_columns)[i];
      if (column.name() != "ttl") {
        continue;
      }

      ++(*ttl_columns_seen_);
      if (column.value().size() != sizeof(uint64_t)) {
        ++(*ttl_bad_size_count_);
        return Decision::kKeep;
      }

      if (DecodeFixed64(column.value().data()) <
          ttl_cutoff_->load(std::memory_order_relaxed)) {
        return Decision::kRemove;
      }
      return Decision::kKeep;
    }

    ++(*missing_ttl_count_);
    return Decision::kKeep;
  }

  bool SupportsFilterV4() const override { return true; }
  const char* Name() const override { return "TTLOnlyLazyDropFilter"; }

 private:
  std::atomic<uint64_t>* ttl_cutoff_;
  std::atomic<int>* filter_call_count_;
  std::atomic<int>* ttl_columns_seen_;
  std::atomic<int>* ttl_bad_size_count_;
  std::atomic<int>* missing_ttl_count_;
  std::atomic<int>* blob_columns_seen_;
};

class FlushOnlyTTLOnlyLazyDropFilterFactory : public CompactionFilterFactory {
 public:
  FlushOnlyTTLOnlyLazyDropFilterFactory(std::atomic<uint64_t>* ttl_cutoff,
                                        std::atomic<int>* filter_call_count,
                                        std::atomic<int>* ttl_columns_seen,
                                        std::atomic<int>* ttl_bad_size_count,
                                        std::atomic<int>* missing_ttl_count,
                                        std::atomic<int>* blob_columns_seen)
      : ttl_cutoff_(ttl_cutoff),
        filter_call_count_(filter_call_count),
        ttl_columns_seen_(ttl_columns_seen),
        ttl_bad_size_count_(ttl_bad_size_count),
        missing_ttl_count_(missing_ttl_count),
        blob_columns_seen_(blob_columns_seen) {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::make_unique<TTLOnlyLazyDropFilter>(
        ttl_cutoff_, filter_call_count_, ttl_columns_seen_, ttl_bad_size_count_,
        missing_ttl_count_, blob_columns_seen_);
  }

  bool ShouldFilterTableFileCreation(
      TableFileCreationReason reason) const override {
    return reason == TableFileCreationReason::kFlush;
  }

  const char* Name() const override {
    return "FlushOnlyTTLOnlyLazyDropFilterFactory";
  }

 private:
  std::atomic<uint64_t>* ttl_cutoff_;
  std::atomic<int>* filter_call_count_;
  std::atomic<int>* ttl_columns_seen_;
  std::atomic<int>* ttl_bad_size_count_;
  std::atomic<int>* missing_ttl_count_;
  std::atomic<int>* blob_columns_seen_;
};

class ResolvingWideValueFilter : public CompactionFilter {
 public:
  ResolvingWideValueFilter(std::atomic<int>* filter_call_count,
                           std::atomic<int>* resolve_attempt_count,
                           std::atomic<int>* resolve_failure_count,
                           std::string* resolved_default_value)
      : filter_call_count_(filter_call_count),
        resolve_attempt_count_(resolve_attempt_count),
        resolve_failure_count_(resolve_failure_count),
        resolved_default_value_(resolved_default_value) {}

  Decision FilterV4(
      int /*level*/, const Slice& /*key*/, ValueType value_type,
      const Slice* /*existing_value*/, const WideColumns* existing_columns,
      std::string* /*new_value*/,
      std::vector<std::pair<std::string, std::string>>* /*new_columns*/,
      std::string* /*skip_until*/,
      WideColumnBlobResolver* blob_resolver = nullptr) const override {
    if (value_type != ValueType::kWideColumnEntity || !existing_columns) {
      return Decision::kKeep;
    }

    ++(*filter_call_count_);

    if (blob_resolver == nullptr) {
      ++(*resolve_failure_count_);
      return Decision::kKeep;
    }

    for (size_t i = 0; i < existing_columns->size(); ++i) {
      const auto& column = (*existing_columns)[i];
      if (column.name() != kDefaultWideColumnName ||
          !blob_resolver->IsBlobColumn(i)) {
        continue;
      }

      ++(*resolve_attempt_count_);
      Slice resolved_value;
      const Status s = blob_resolver->ResolveColumn(i, &resolved_value);
      if (!s.ok()) {
        ++(*resolve_failure_count_);
        return Decision::kKeep;
      }

      *resolved_default_value_ = resolved_value.ToString();
      return Decision::kKeep;
    }

    ++(*resolve_failure_count_);
    return Decision::kKeep;
  }

  bool SupportsFilterV4() const override { return true; }
  const char* Name() const override { return "ResolvingWideValueFilter"; }

 private:
  std::atomic<int>* filter_call_count_;
  std::atomic<int>* resolve_attempt_count_;
  std::atomic<int>* resolve_failure_count_;
  std::string* resolved_default_value_;
};

class FlushOnlyResolvingWideValueFilterFactory
    : public CompactionFilterFactory {
 public:
  FlushOnlyResolvingWideValueFilterFactory(
      std::atomic<int>* filter_call_count,
      std::atomic<int>* resolve_attempt_count,
      std::atomic<int>* resolve_failure_count,
      std::string* resolved_default_value)
      : filter_call_count_(filter_call_count),
        resolve_attempt_count_(resolve_attempt_count),
        resolve_failure_count_(resolve_failure_count),
        resolved_default_value_(resolved_default_value) {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::make_unique<ResolvingWideValueFilter>(
        filter_call_count_, resolve_attempt_count_, resolve_failure_count_,
        resolved_default_value_);
  }

  bool ShouldFilterTableFileCreation(
      TableFileCreationReason reason) const override {
    return reason == TableFileCreationReason::kFlush;
  }

  const char* Name() const override {
    return "FlushOnlyResolvingWideValueFilterFactory";
  }

 private:
  std::atomic<int>* filter_call_count_;
  std::atomic<int>* resolve_attempt_count_;
  std::atomic<int>* resolve_failure_count_;
  std::string* resolved_default_value_;
};

class BlobResolvingErrorIgnoringFilter : public CompactionFilter {
 public:
  BlobResolvingErrorIgnoringFilter(std::atomic<int>* filter_call_count,
                                   std::atomic<int>* resolve_error_count,
                                   std::string* resolve_error_status)
      : filter_call_count_(filter_call_count),
        resolve_error_count_(resolve_error_count),
        resolve_error_status_(resolve_error_status) {}

  Decision FilterV4(
      int /*level*/, const Slice& /*key*/, ValueType value_type,
      const Slice* /*existing_value*/, const WideColumns* existing_columns,
      std::string* /*new_value*/,
      std::vector<std::pair<std::string, std::string>>* /*new_columns*/,
      std::string* /*skip_until*/,
      WideColumnBlobResolver* blob_resolver = nullptr) const override {
    if (value_type != ValueType::kWideColumnEntity || !existing_columns ||
        blob_resolver == nullptr) {
      return Decision::kKeep;
    }

    ++(*filter_call_count_);

    for (size_t i = 0; i < existing_columns->size(); ++i) {
      if (!blob_resolver->IsBlobColumn(i)) {
        continue;
      }

      const auto& column = (*existing_columns)[i];
      if (column.name() != "blob_attr") {
        continue;
      }

      Slice resolved_value;
      const Status s = blob_resolver->ResolveColumn(i, &resolved_value);
      if (!s.ok()) {
        ++(*resolve_error_count_);
        *resolve_error_status_ = s.ToString();
      }
      break;
    }

    // Even if the filter returns kKeep after observing the read failure,
    // flush must fail and surface the resolver error.
    return Decision::kKeep;
  }

  bool SupportsFilterV4() const override { return true; }
  const char* Name() const override {
    return "BlobResolvingErrorIgnoringFilter";
  }

 private:
  std::atomic<int>* filter_call_count_;
  std::atomic<int>* resolve_error_count_;
  std::string* resolve_error_status_;
};

class FlushOnlyBlobResolvingErrorIgnoringFilterFactory
    : public CompactionFilterFactory {
 public:
  FlushOnlyBlobResolvingErrorIgnoringFilterFactory(
      std::atomic<int>* filter_call_count,
      std::atomic<int>* resolve_error_count, std::string* resolve_error_status)
      : filter_call_count_(filter_call_count),
        resolve_error_count_(resolve_error_count),
        resolve_error_status_(resolve_error_status) {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::make_unique<BlobResolvingErrorIgnoringFilter>(
        filter_call_count_, resolve_error_count_, resolve_error_status_);
  }

  bool ShouldFilterTableFileCreation(
      TableFileCreationReason reason) const override {
    return reason == TableFileCreationReason::kFlush;
  }

  const char* Name() const override {
    return "FlushOnlyBlobResolvingErrorIgnoringFilterFactory";
  }

 private:
  std::atomic<int>* filter_call_count_;
  std::atomic<int>* resolve_error_count_;
  std::string* resolve_error_status_;
};

class NameBasedWideColumnPartitionStrategy : public BlobFilePartitionStrategy {
 public:
  using BlobFilePartitionStrategy::SelectPartition;

  const char* Name() const override {
    return "NameBasedWideColumnPartitionStrategy";
  }

  uint32_t SelectPartition(uint32_t /*num_partitions*/,
                           uint32_t /*column_family_id*/, const Slice& /*key*/,
                           const Slice& /*value*/) override {
    value_calls_.fetch_add(1, std::memory_order_relaxed);
    return 0;
  }

  uint32_t SelectPartition(uint32_t /*num_partitions*/,
                           uint32_t /*column_family_id*/, const Slice& /*key*/,
                           const WideColumns& columns) override {
    wide_columns_calls_.fetch_add(1, std::memory_order_relaxed);
    for (const auto& column : columns) {
      if (column.name() == "ttl") {
        return column.value() == "00000001" ? 1 : 3;
      }
    }
    return 0;
  }

  uint32_t value_calls() const {
    return value_calls_.load(std::memory_order_relaxed);
  }

  uint32_t wide_columns_calls() const {
    return wide_columns_calls_.load(std::memory_order_relaxed);
  }

 private:
  mutable std::atomic<uint32_t> value_calls_{0};
  mutable std::atomic<uint32_t> wide_columns_calls_{0};
};

}  // namespace

TEST_F(DBWideBlobDirectWriteTest, PutEntityRejectsEmptyAttributeGroups) {
  const AttributeGroups attribute_groups;
  ASSERT_TRUE(
      db_->PutEntity(WriteOptions(), "empty_attribute_groups", attribute_groups)
          .IsInvalidArgument());
  ASSERT_EQ("NOT_FOUND", Get("empty_attribute_groups"));
}

TEST_F(DBWideBlobDirectWriteTest,
       PutEntitySerializedRejectsExistingBlobReferencesWhenDirectWriteEnabled) {
  Options options = GetDirectWriteOptions();
  options.min_blob_size = 64;

  Reopen(options);

  const auto columns_data = MakeIteratorStressColumnData('Q', 0);
  const WideColumns columns = ToWideColumns(columns_data);

  std::string blob_index_encoding;
  BlobIndex::EncodeBlob(&blob_index_encoding, /*file_number=*/123,
                        /*offset=*/0, columns[1].value().size(),
                        kNoCompression);
  BlobIndex blob_index;
  ASSERT_OK(blob_index.DecodeFrom(blob_index_encoding));

  std::string entity;
  ASSERT_OK(
      WideColumnSerialization::SerializeV2(columns, {{1, blob_index}}, entity));

  WriteBatch batch;
  ASSERT_OK(WriteBatchInternal::PutEntitySerialized(
      &batch, /*column_family_id=*/0, "serialized_blob_entity", entity));

  const Status s = db_->Write(WriteOptions(), &batch);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_EQ("NOT_FOUND", Get("serialized_blob_entity"));
}

TEST_F(DBWideBlobDirectWriteTest, DirectWriteWideEntityBeforeAndAfterFlush) {
  Options options = GetDirectWriteOptions();
  options.min_blob_size = 64;

  Reopen(options);

  const std::string key1 = "entity_key_a";
  const std::string key2 = "entity_key_b";
  const std::string value1(128, 'a');
  const std::string value2(160, 'b');
  const std::string ttl1 = "00000001";
  const std::string ttl2 = "00000002";

  WideColumns columns1{
      {kDefaultWideColumnName, value1}, {"ttl", ttl1}, {"type", "cold"}};
  WideColumns columns2{
      {kDefaultWideColumnName, value2}, {"ttl", ttl2}, {"type", "hot"}};

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key1,
                           columns1));

  WriteBatch batch;
  ASSERT_OK(batch.PutEntity(db_->DefaultColumnFamily(), key2, columns2));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  const std::array<Slice, 2> keys{Slice(key1), Slice(key2)};

  auto verify = [&]() {
    {
      PinnableSlice result;
      ASSERT_OK(
          db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key1, &result));
      ASSERT_EQ(result, value1);
    }

    {
      PinnableSlice result;
      ASSERT_OK(
          db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key2, &result));
      ASSERT_EQ(result, value2);
    }

    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key1,
                               &result));
      ASSERT_EQ(result.columns(), columns1);
    }

    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key2,
                               &result));
      ASSERT_EQ(result.columns(), columns2);
    }

    {
      std::array<PinnableWideColumns, 2> results;
      std::array<Status, 2> statuses;
      db_->MultiGetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                          keys.size(), keys.data(), results.data(),
                          statuses.data());
      ASSERT_OK(statuses[0]);
      ASSERT_EQ(results[0].columns(), columns1);
      ASSERT_OK(statuses[1]);
      ASSERT_EQ(results[1].columns(), columns2);
    }

    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
      iter->SeekToFirst();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), key1);
      ASSERT_EQ(iter->value(), value1);
      ASSERT_EQ(iter->columns(), columns1);

      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), key2);
      ASSERT_EQ(iter->value(), value2);
      ASSERT_EQ(iter->columns(), columns2);

      iter->Next();
      ASSERT_FALSE(iter->Valid());
      ASSERT_OK(iter->status());
    }
  };

  verify();

  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  verify();

  auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(
      db_->DefaultColumnFamily());
  auto* mgr = cfh->cfd()->blob_partition_manager();
  ASSERT_NE(mgr, nullptr);

  std::vector<BlobFileAddition> additions;
  std::vector<BlobFileGarbage> garbages;
  ASSERT_OK(mgr->PrepareFlushAdditions(
      WriteOptions(), {cfh->cfd()->imm()->GetEarliestMemTableID()}, &additions,
      &garbages));
  ASSERT_FALSE(additions.empty());
  ASSERT_TRUE(garbages.empty());

  uint64_t total_blob_count = 0;
  for (const auto& addition : additions) {
    total_blob_count += addition.GetTotalBlobCount();
  }
  ASSERT_EQ(total_blob_count, 2U)
      << "Both DB::PutEntity and WriteBatch::PutEntity should direct-write "
         "their large default columns before flush";

  ASSERT_OK(Flush());
  ASSERT_GT(CountBlobFiles(), 0U);
  verify();

  Close();
  Reopen(options);
  verify();
}

TEST_F(DBWideBlobDirectWriteTest,
       DirectWriteAutoFlushPreservesWideEntityBlobGenerationOrder) {
  Options options = GetDirectWriteOptions();
  options.allow_concurrent_memtable_write = false;
  options.disable_auto_compactions = true;
  options.arena_block_size = 4096;
  options.write_buffer_size = 500000;
  options.max_write_buffer_number = 4;
  options.write_buffer_manager =
      std::make_shared<WriteBufferManager>(100, nullptr, false);
  options.min_blob_size = 64;
  options.blob_direct_write_partitions = 4;
  options.blob_direct_write_partition_strategy =
      std::make_shared<NameBasedWideColumnPartitionStrategy>();

  Reopen(options);

  WriteOptions write_options;
  write_options.disableWAL = true;

  const std::string first_value(128, 'a');
  const std::string first_meta(96, 'b');
  const WideColumns first_columns{{kDefaultWideColumnName, first_value},
                                  {"meta", first_meta},
                                  {"ttl", "00000001"}};
  const std::string second_value(128, 'c');
  const std::string second_meta(96, 'd');
  const WideColumns second_columns{{kDefaultWideColumnName, second_value},
                                   {"meta", second_meta},
                                   {"ttl", "00000002"}};

  ASSERT_OK(db_->PutEntity(write_options, db_->DefaultColumnFamily(),
                           "first_entity", first_columns));
  ASSERT_OK(db_->PutEntity(write_options, db_->DefaultColumnFamily(),
                           "second_entity", second_columns));

  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(Flush());

  PinnableWideColumns first_result;
  ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                           "first_entity", &first_result));
  ASSERT_EQ(first_result.columns(), first_columns);

  PinnableWideColumns second_result;
  ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                           "second_entity", &second_result));
  ASSERT_EQ(second_result.columns(), second_columns);

  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);
  ASSERT_EQ(cf_meta.blob_files.size(), 2U);

  std::vector<uint64_t> blob_file_numbers;
  blob_file_numbers.reserve(cf_meta.blob_files.size());
  for (const auto& blob_meta : cf_meta.blob_files) {
    blob_file_numbers.push_back(blob_meta.blob_file_number);
  }
  std::sort(blob_file_numbers.begin(), blob_file_numbers.end());

  std::vector<LiveFileMetaData> live_files;
  db_->GetLiveFilesMetaData(&live_files);
  ASSERT_EQ(live_files.size(), 2U);
  std::sort(live_files.begin(), live_files.end(),
            [](const LiveFileMetaData& lhs, const LiveFileMetaData& rhs) {
              return lhs.smallest_seqno < rhs.smallest_seqno;
            });

  ASSERT_EQ(live_files[0].oldest_blob_file_number, blob_file_numbers[0]);
  ASSERT_EQ(live_files[1].oldest_blob_file_number, blob_file_numbers[1]);
}

TEST_F(DBWideBlobDirectWriteTest,
       DirectWriteCustomPartitionStrategyUsesWideColumnOverload) {
  Options options = GetDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  options.blob_file_size = 1 << 20;
  options.min_blob_size = 64;
  auto strategy = std::make_shared<NameBasedWideColumnPartitionStrategy>();
  options.blob_direct_write_partition_strategy = strategy;

  Reopen(options);

  const std::string key1 = "entity_strategy_key_a";
  const std::string key2 = "entity_strategy_key_b";
  const std::string value1(128, 'a');
  const std::string meta1(128, 'b');
  const std::string ttl1 = "00000001";
  const std::string value2(144, 'c');
  const std::string meta2(144, 'd');
  const std::string ttl2 = "00000002";
  const WideColumns columns1{
      {kDefaultWideColumnName, value1}, {"meta", meta1}, {"ttl", ttl1}};
  const WideColumns columns2{
      {kDefaultWideColumnName, value2}, {"meta", meta2}, {"ttl", ttl2}};

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key1,
                           columns1));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key2,
                           columns2));

  ASSERT_OK(Flush());
  ASSERT_EQ(CountBlobFiles(), 2U);
  ASSERT_EQ(strategy->wide_columns_calls(), 2U);
  ASSERT_EQ(strategy->value_calls(), 0U);

  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);
  ASSERT_EQ(cf_meta.blob_file_count, 2U);
  ASSERT_EQ(cf_meta.blob_files.size(), 2U);

  PinnableWideColumns result;
  ASSERT_OK(
      db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key1, &result));
  ASSERT_EQ(result.columns(), columns1);

  result.Reset();
  ASSERT_OK(
      db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key2, &result));
  ASSERT_EQ(result.columns(), columns2);
}

TEST_F(DBWideBlobDirectWriteTest,
       SingleCfCoalescingIteratorMatchesDirectIteratorAcrossAutoRefresh) {
  SingleCfCoalescingIteratorScenario scenario;
  scenario.refresh_mid_iteration = true;
  VerifySingleCfCoalescingIteratorMatchesDirectIterator(scenario);
}

TEST_F(DBWideBlobDirectWriteTest,
       SingleCfCoalescingIteratorMatchesDirectIteratorAfterSeekRefresh) {
  SingleCfCoalescingIteratorScenario scenario;
  scenario.use_trie_index = true;
  scenario.refresh_before_seek = true;
  VerifySingleCfCoalescingIteratorMatchesDirectIterator(scenario);
}

TEST_F(
    DBWideBlobDirectWriteTest,
    SingleCfCoalescingIteratorMatchesDirectIteratorAfterReuseAndSeekRefresh) {
  SingleCfCoalescingIteratorScenario scenario;
  scenario.use_trie_index = true;
  scenario.refresh_before_seek = true;
  scenario.reuse_iterator_before_refresh = true;
  scenario.create_control_before_refresh = false;
  VerifySingleCfCoalescingIteratorMatchesDirectIterator(scenario);
}

TEST_F(DBWideBlobDirectWriteTest,
       DirectWriteWideEntityLazyTTLFlushTracksExpiredBlobGarbage) {
  std::atomic<int> filter_call_count{0};
  std::atomic<int> ttl_columns_seen{0};
  std::atomic<int> ttl_bad_size_count{0};
  std::atomic<int> missing_ttl_count{0};
  std::atomic<int> blob_columns_seen{0};
  std::atomic<uint64_t> ttl_cutoff{1000};

  auto filter_factory = std::make_shared<FlushOnlyTTLOnlyLazyDropFilterFactory>(
      &ttl_cutoff, &filter_call_count, &ttl_columns_seen, &ttl_bad_size_count,
      &missing_ttl_count, &blob_columns_seen);

  Options options = GetDirectWriteOptions();
  options.allow_concurrent_memtable_write = false;
  options.blob_direct_write_partitions = 1;
  options.min_blob_size = 64;
  options.blob_file_size = 1 << 20;
  options.disable_auto_compactions = true;
  options.enable_blob_garbage_collection = false;
  options.compaction_filter_factory = filter_factory;

  Reopen(options);

  const std::string expired_key = "expired_entity";
  const std::string live_key = "live_entity";
  const std::string expired_value = GenerateLargeValue(4096, 'E');
  const std::string live_value = GenerateLargeValue(4096, 'L');
  const std::string expired_ttl = EncodeFixedTTL(10);
  const std::string live_ttl = EncodeFixedTTL(1000);
  const auto expired_columns_data =
      BuildTTLWideEntityData(expired_value, expired_ttl);
  const WideColumns expired_columns = ToWideColumns(expired_columns_data);
  const auto live_columns_data = BuildTTLWideEntityData(live_value, live_ttl);
  const WideColumns live_columns = ToWideColumns(live_columns_data);

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           expired_key, expired_columns));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), live_key,
                           live_columns));

  ASSERT_OK(Flush());

  ASSERT_GE(filter_call_count.load(), 2);
  ASSERT_EQ(ttl_columns_seen.load(), filter_call_count.load());
  ASSERT_EQ(ttl_bad_size_count.load(), 0);
  ASSERT_EQ(missing_ttl_count.load(), 0);
  ASSERT_EQ(blob_columns_seen.load(), filter_call_count.load());

  PinnableWideColumns result;
  ASSERT_TRUE(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                             expired_key, &result)
                  .IsNotFound());
  ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), live_key,
                           &result));
  ASSERT_EQ(result.columns(), live_columns);

  const uint64_t expired_record_bytes =
      expired_value.size() +
      BlobLogRecord::CalculateAdjustmentForRecordHeader(expired_key.size());
  const uint64_t live_record_bytes =
      live_value.size() +
      BlobLogRecord::CalculateAdjustmentForRecordHeader(live_key.size());

  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);
  ASSERT_EQ(cf_meta.blob_file_count, 1U);
  ASSERT_EQ(cf_meta.blob_files.size(), 1U);

  const BlobMetaData& blob_meta = cf_meta.blob_files[0];
  ASSERT_EQ(blob_meta.total_blob_count, 2U);
  ASSERT_EQ(blob_meta.total_blob_bytes,
            expired_record_bytes + live_record_bytes);
  ASSERT_EQ(blob_meta.garbage_blob_count, 1U);
  ASSERT_EQ(blob_meta.garbage_blob_bytes, expired_record_bytes);
}

TEST_F(DBWideBlobDirectWriteTest,
       DirectWriteWideEntityLazyResolverMissingBlobFailsFlush) {
  // Goal: verify flush-time FilterV4 lazy resolution errors fail the flush for
  // direct-write wide entities. The filter resolves one blob-backed column,
  // records the resulting read error, and still returns kKeep; flush must
  // propagate the resolver status and latch bg_error instead of silently
  // preserving the entry.
  std::atomic<int> filter_call_count{0};
  std::atomic<int> resolve_error_count{0};
  std::string resolve_error_status;

  Options options = GetDirectWriteOptions();
  options.min_blob_size = 64;
  options.compaction_filter_factory =
      std::make_shared<FlushOnlyBlobResolvingErrorIgnoringFilterFactory>(
          &filter_call_count, &resolve_error_count, &resolve_error_status);

  Reopen(options);

  constexpr char key[] = "flush_missing_blob_key";
  const std::string blob_attr_value(128, 'b');
  const WideColumns columns{{kDefaultWideColumnName, "inline-default"},
                            {"blob_attr", blob_attr_value},
                            {"ttl", "00000001"}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());

  auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(
      db_->DefaultColumnFamily());
  auto* mgr = cfh->cfd()->blob_partition_manager();
  ASSERT_NE(mgr, nullptr);

  std::vector<BlobFileAddition> additions;
  std::vector<BlobFileGarbage> garbages;
  ASSERT_OK(mgr->PrepareFlushAdditions(
      WriteOptions(), {cfh->cfd()->imm()->GetEarliestMemTableID()}, &additions,
      &garbages));
  ASSERT_EQ(additions.size(), 1U);
  ASSERT_TRUE(garbages.empty());

  ASSERT_OK(env_->DeleteFile(
      BlobFileName(dbname_, additions.front().GetBlobFileNumber())));

  const Status status = Flush();
  ASSERT_FALSE(status.ok())
      << "Flush should fail when FilterV4 lazy blob resolution hits a "
         "missing blob file";
  ASSERT_TRUE(status.IsCorruption() || status.IsIOError() ||
              status.IsNotFound())
      << status.ToString();
  ASSERT_GE(filter_call_count.load(), 1);
  ASSERT_EQ(resolve_error_count.load(), 1);
  ASSERT_FALSE(resolve_error_status.empty());

  const Status bg_error = dbfull()->TEST_GetBGError();
  ASSERT_FALSE(bg_error.ok());
  ASSERT_TRUE(bg_error.IsCorruption() || bg_error.IsIOError() ||
              bg_error.IsNotFound())
      << bg_error.ToString();
  ASSERT_GE(static_cast<int>(bg_error.severity()),
            static_cast<int>(Status::Severity::kHardError));
}

TEST_F(DBWideBlobDirectWriteTest,
       DirectWriteWideEntityFlushOverwriteElisionTracksBlobGarbage) {
  Options options = GetDirectWriteOptions();
  options.allow_concurrent_memtable_write = false;
  options.blob_direct_write_partitions = 1;
  options.min_blob_size = 64;
  options.blob_file_size = 1 << 20;
  options.disable_auto_compactions = true;
  options.enable_blob_garbage_collection = false;

  Reopen(options);

  const std::string key = "overwritten_entity";
  const std::string old_value = GenerateLargeValue(4096, 'O');
  const std::string new_value = GenerateLargeValue(4096, 'N');
  const auto old_columns_data =
      WideColumnStringPairs{{"", old_value}, {"meta", "old_inline_meta"}};
  const WideColumns old_columns = ToWideColumns(old_columns_data);
  const auto new_columns_data =
      WideColumnStringPairs{{"", new_value}, {"meta", "new_inline_meta"}};
  const WideColumns new_columns = ToWideColumns(new_columns_data);

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key,
                           old_columns));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key,
                           new_columns));

  ASSERT_OK(Flush());

  PinnableWideColumns result;
  ASSERT_OK(
      db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key, &result));
  ASSERT_EQ(result.columns(), new_columns);

  const uint64_t old_record_bytes =
      old_value.size() +
      BlobLogRecord::CalculateAdjustmentForRecordHeader(key.size());
  const uint64_t new_record_bytes =
      new_value.size() +
      BlobLogRecord::CalculateAdjustmentForRecordHeader(key.size());

  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);
  ASSERT_EQ(cf_meta.blob_file_count, 1U);
  ASSERT_EQ(cf_meta.blob_files.size(), 1U);

  const BlobMetaData& blob_meta = cf_meta.blob_files[0];
  ASSERT_EQ(blob_meta.total_blob_count, 2U);
  ASSERT_EQ(blob_meta.total_blob_bytes, old_record_bytes + new_record_bytes);
  ASSERT_EQ(blob_meta.garbage_blob_count, 1U);
  ASSERT_EQ(blob_meta.garbage_blob_bytes, old_record_bytes);
}

TEST_F(DBWideBlobDirectWriteTest,
       DirectWriteWideEntityLazyTTLFlushAllExpiredDoesNotLeakBlobGeneration) {
  std::atomic<int> filter_call_count{0};
  std::atomic<int> ttl_columns_seen{0};
  std::atomic<int> ttl_bad_size_count{0};
  std::atomic<int> missing_ttl_count{0};
  std::atomic<int> blob_columns_seen{0};
  std::atomic<uint64_t> ttl_cutoff{1000};

  auto filter_factory = std::make_shared<FlushOnlyTTLOnlyLazyDropFilterFactory>(
      &ttl_cutoff, &filter_call_count, &ttl_columns_seen, &ttl_bad_size_count,
      &missing_ttl_count, &blob_columns_seen);

  Options options = GetDirectWriteOptions();
  options.allow_concurrent_memtable_write = false;
  options.blob_direct_write_partitions = 1;
  options.min_blob_size = 64;
  options.blob_file_size = 1 << 20;
  options.disable_auto_compactions = true;
  options.enable_blob_garbage_collection = false;
  options.compaction_filter_factory = filter_factory;

  Reopen(options);

  const std::string expired_key = "expired_only_entity";
  const std::string expired_value = GenerateLargeValue(4096, 'X');
  const std::string expired_ttl = EncodeFixedTTL(10);
  const auto expired_columns_data =
      BuildTTLWideEntityData(expired_value, expired_ttl);
  const WideColumns expired_columns = ToWideColumns(expired_columns_data);

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           expired_key, expired_columns));

  ASSERT_OK(Flush());

  ASSERT_GE(filter_call_count.load(), 1);
  ASSERT_EQ(ttl_columns_seen.load(), filter_call_count.load());
  ASSERT_EQ(ttl_bad_size_count.load(), 0);
  ASSERT_EQ(missing_ttl_count.load(), 0);
  ASSERT_EQ(blob_columns_seen.load(), filter_call_count.load());

  PinnableWideColumns result;
  ASSERT_TRUE(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                             expired_key, &result)
                  .IsNotFound());

  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);
  ASSERT_EQ(cf_meta.blob_file_count, 0U);
  ASSERT_TRUE(cf_meta.blob_files.empty());

  const std::string live_key = "live_after_empty_flush";
  const std::string live_value = GenerateLargeValue(4096, 'Y');
  const std::string live_ttl = EncodeFixedTTL(5000);
  const auto live_columns_data = BuildTTLWideEntityData(live_value, live_ttl);
  const WideColumns live_columns = ToWideColumns(live_columns_data);

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), live_key,
                           live_columns));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), live_key,
                           &result));
  ASSERT_EQ(result.columns(), live_columns);

  const uint64_t live_record_bytes =
      live_value.size() +
      BlobLogRecord::CalculateAdjustmentForRecordHeader(live_key.size());

  db_->GetColumnFamilyMetaData(&cf_meta);
  ASSERT_EQ(cf_meta.blob_file_count, 1U);
  ASSERT_EQ(cf_meta.blob_files.size(), 1U);
  ASSERT_EQ(cf_meta.blob_files[0].total_blob_count, 1U);
  ASSERT_EQ(cf_meta.blob_files[0].total_blob_bytes, live_record_bytes);
  ASSERT_EQ(cf_meta.blob_files[0].garbage_blob_count, 0U);
  ASSERT_EQ(cf_meta.blob_files[0].garbage_blob_bytes, 0U);
}

TEST_F(DBWideBlobDirectWriteTest,
       DirectWriteWideEntityBlobResolverWorksOnFlush) {
  std::atomic<int> filter_call_count{0};
  std::atomic<int> resolve_attempt_count{0};
  std::atomic<int> resolve_failure_count{0};
  std::string resolved_default_value;

  auto filter_factory =
      std::make_shared<FlushOnlyResolvingWideValueFilterFactory>(
          &filter_call_count, &resolve_attempt_count, &resolve_failure_count,
          &resolved_default_value);

  Options options = GetDirectWriteOptions();
  options.allow_concurrent_memtable_write = false;
  options.blob_direct_write_partitions = 1;
  options.min_blob_size = 64;
  options.blob_file_size = 1 << 20;
  options.disable_auto_compactions = true;
  options.enable_blob_garbage_collection = false;
  options.compaction_filter_factory = filter_factory;

  Reopen(options);

  const std::string key = "resolver_entity";
  const std::string large_value = GenerateLargeValue(4096, 'Z');
  const std::string ttl_value = EncodeFixedTTL(5000);
  const auto columns_data = BuildTTLWideEntityData(large_value, ttl_value);
  const WideColumns columns = ToWideColumns(columns_data);

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  ASSERT_GE(filter_call_count.load(), 1);
  ASSERT_GE(resolve_attempt_count.load(), 1);
  ASSERT_EQ(resolve_failure_count.load(), 0);
  ASSERT_EQ(resolved_default_value, large_value);

  PinnableWideColumns result;
  ASSERT_OK(
      db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key, &result));
  ASSERT_EQ(result.columns(), columns);
}

TEST_F(DBWideBlobDirectWriteTest,
       MultiCfCoalescingIteratorResolvesBlobBackedColumnsAfterSeekRefresh) {
  constexpr int kNumKeys = 32;

  Options options = GetDirectWriteOptions();
  options.min_blob_size = 64;
  options.disable_auto_compactions = true;
  options.prefix_extractor.reset(NewFixedPrefixTransform(7));

  Reopen(options);
  CreateColumnFamilies({"cf_1"}, options);
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "cf_1"}, options);
  PopulateMultiCfIteratorStressEntities(kNumKeys, handles_[1]);

  ReadOptions read_options;
  read_options.allow_unprepared_value = true;
  read_options.auto_refresh_iterator_with_snapshot = true;
  read_options.pin_data = true;
  const Snapshot* snapshot = db_->GetSnapshot();
  read_options.snapshot = snapshot;

  trie_index::TrieIndexFactory trie_index_factory;
  read_options.read_index = ReadOptions::ReadIndex::kPreferCustom;

  std::vector<ColumnFamilyHandle*> cfhs{handles_[1], handles_[0]};
  std::unique_ptr<Iterator> coalescing =
      db_->NewCoalescingIterator(read_options, cfhs);

  const int first_index = kNumKeys / 8;
  const auto expected_first_data =
      MakeCoalescedIteratorStressColumnData('C', 'M', first_index);
  const WideColumns expected_first = ToWideColumns(expected_first_data);
  coalescing->Seek(MakeIteratorStressKey(first_index));
  ASSERT_TRUE(coalescing->Valid());
  ASSERT_TRUE(coalescing->value().empty());
  ASSERT_TRUE(coalescing->PrepareValue());
  ASSERT_EQ(expected_first.front().value().ToString(),
            coalescing->value().ToString());
  ASSERT_EQ(expected_first, coalescing->columns());

  coalescing->Next();
  ASSERT_TRUE(coalescing->Valid());
  ASSERT_TRUE(coalescing->PrepareValue());

  ASSERT_OK(Flush({0, 1}));
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), handles_[0], nullptr, nullptr));
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr, nullptr));

  const int start_index = kNumKeys / 4;
  const std::string start_key = MakeIteratorStressKey(start_index);
  coalescing->Seek(start_key);

  for (int index = start_index; index < kNumKeys; ++index) {
    const auto expected_columns_data =
        MakeCoalescedIteratorStressColumnData('C', 'M', index);
    const WideColumns expected_columns = ToWideColumns(expected_columns_data);

    ASSERT_TRUE(coalescing->Valid());
    ASSERT_EQ(MakeIteratorStressKey(index), coalescing->key().ToString());
    ASSERT_TRUE(coalescing->value().empty());
    ASSERT_TRUE(coalescing->PrepareValue());
    ASSERT_EQ(expected_columns.front().value().ToString(),
              coalescing->value().ToString());
    ASSERT_EQ(expected_columns, coalescing->columns());

    coalescing->Next();
  }

  ASSERT_FALSE(coalescing->Valid());
  ASSERT_OK(coalescing->status());

  coalescing.reset();
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBWideBlobDirectWriteTest,
       SnapshotMultiGetEntityMatchesPointGetAcrossFlushAndTrieIndex) {
  struct SnapshotReadCase {
    const char* name;
    bool use_trie_index;
    bool duplicate_first_key;
    bool flush_before_snapshot;
  };

  const std::array<SnapshotReadCase, 8> cases{{
      {"mem_plain_unique", false, false, false},
      {"mem_plain_duplicate", false, true, false},
      {"mem_trie_unique", true, false, false},
      {"mem_trie_duplicate", true, true, false},
      {"sst_plain_unique", false, false, true},
      {"sst_plain_duplicate", false, true, true},
      {"sst_trie_unique", true, false, true},
      {"sst_trie_duplicate", true, true, true},
  }};

  for (const auto& test_case : cases) {
    SCOPED_TRACE(test_case.name);

    Options options = GetDirectWriteOptions();
    options.min_blob_size = 64;
    options.blob_direct_write_partitions = 4;

    Reopen(options);

    const std::string key1 = "snapshot_key_a";
    const std::string key2 = "snapshot_key_b";
    const std::string old_value1(128, 'a');
    const std::string old_meta1(96, 'b');
    const std::string new_value1(144, 'c');
    const std::string new_meta1(104, 'd');
    const std::string old_value2(160, 'e');
    const std::string old_meta2(112, 'f');
    const std::string new_value2(176, 'g');
    const std::string new_meta2(120, 'h');
    const WideColumns old_columns1{
        {kDefaultWideColumnName, old_value1},
        {"meta", old_meta1},
    };
    const WideColumns new_columns1{
        {kDefaultWideColumnName, new_value1},
        {"meta", new_meta1},
    };
    const WideColumns old_columns2{
        {kDefaultWideColumnName, old_value2},
        {"meta", old_meta2},
    };
    const WideColumns new_columns2{
        {kDefaultWideColumnName, new_value2},
        {"meta", new_meta2},
    };

    ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key1,
                             old_columns1));
    ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key2,
                             old_columns2));
    if (test_case.flush_before_snapshot) {
      ASSERT_OK(Flush());
    }

    const Snapshot* snapshot = db_->GetSnapshot();

    ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key1,
                             new_columns1));
    ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key2,
                             new_columns2));

    ReadOptions read_options;
    read_options.snapshot = snapshot;

    trie_index::TrieIndexFactory trie_index_factory;
    if (test_case.use_trie_index) {
      read_options.read_index = ReadOptions::ReadIndex::kPreferCustom;
    }

    const auto verify_snapshot_reads = [&]() {
      std::array<Slice, 3> keys{
          {Slice(key1),
           test_case.duplicate_first_key ? Slice(key1) : Slice(key2),
           Slice(key2)}};
      const std::array<WideColumns, 3> expected_columns{
          {old_columns1,
           test_case.duplicate_first_key ? old_columns1 : old_columns2,
           old_columns2}};

      std::array<PinnableWideColumns, 3> multiget_results;
      std::array<Status, 3> multiget_statuses;

      db_->MultiGetEntity(read_options, db_->DefaultColumnFamily(), keys.size(),
                          keys.data(), multiget_results.data(),
                          multiget_statuses.data());

      for (size_t i = 0; i < keys.size(); ++i) {
        ASSERT_OK(multiget_statuses[i]);
        ASSERT_EQ(expected_columns[i], multiget_results[i].columns());

        PinnableWideColumns get_result;
        ASSERT_OK(db_->GetEntity(read_options, db_->DefaultColumnFamily(),
                                 keys[i], &get_result));
        ASSERT_EQ(expected_columns[i], get_result.columns());
        ASSERT_EQ(get_result.columns(), multiget_results[i].columns());
      }
    };

    verify_snapshot_reads();
    ASSERT_OK(Flush());
    verify_snapshot_reads();

    {
      PinnableWideColumns latest_result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key1,
                               &latest_result));
      ASSERT_EQ(new_columns1, latest_result.columns());
    }

    {
      PinnableWideColumns latest_result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key2,
                               &latest_result));
      ASSERT_EQ(new_columns2, latest_result.columns());
    }

    db_->ReleaseSnapshot(snapshot);
  }
}

TEST_F(DBWideBlobDirectWriteTest,
       DirectWriteIteratorValueScanEagerlyResolvesBlobColumns) {
  struct IteratorScanCase {
    const char* name;
    std::string default_value;
    std::string blob_attr_value;
  };

  const std::array<IteratorScanCase, 2> cases{{
      {"inline_default", "inline-default", std::string(128, 'b')},
      {"blob_default", std::string(128, 'd'), std::string(160, 'e')},
  }};

  for (const auto& test_case : cases) {
    SCOPED_TRACE(test_case.name);

    Options options = GetDirectWriteOptions();
    options.min_blob_size = 64;
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

    DestroyAndReopen(options);

    const std::string key = std::string("wide_entity_key_") + test_case.name;
    const std::string ttl = "00000001";
    const WideColumns columns{{kDefaultWideColumnName, test_case.default_value},
                              {"blob_attr", test_case.blob_attr_value},
                              {"ttl", ttl}};

    ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key,
                             columns));
    ASSERT_OK(Flush());
    ASSERT_GT(CountBlobFiles(), 0U);

    Close();
    Reopen(options);
    ASSERT_OK(options.statistics->Reset());

    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), key);
    ASSERT_EQ(iter->value(), test_case.default_value);

    const uint64_t blob_bytes_before_columns =
        options.statistics->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_READ);
    ASSERT_GT(blob_bytes_before_columns, 0U)
        << "Iterator positioning should resolve all blob-backed columns before "
           "the entry becomes valid";

    ASSERT_EQ(iter->columns(), columns);
    ASSERT_OK(iter->status());

    const uint64_t blob_bytes_after_columns =
        options.statistics->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_READ);
    ASSERT_EQ(blob_bytes_after_columns, blob_bytes_before_columns)
        << "columns() should be a pure accessor for an already-valid iterator";
  }
}

TEST_F(DBWideBlobDirectWriteTest,
       DirectWriteIteratorBlobResolutionErrorInvalidatesEntry) {
  // Goal: verify iterator positioning clears Valid() when resolving a
  // blob-backed wide column fails. We keep the default column inline so the
  // old lazy columns() behavior would have exposed a seemingly valid entry
  // until columns() tried to read the missing blob data.
  Options options = GetDirectWriteOptions();
  options.min_blob_size = 64;

  DestroyAndReopen(options);

  constexpr char key[] = "wide_entity_key_missing_blob";
  const std::string blob_attr_value(128, 'b');
  const WideColumns columns{{kDefaultWideColumnName, "inline-default"},
                            {"blob_attr", blob_attr_value},
                            {"ttl", "00000001"}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  const auto blob_files = GetBlobFileNumbers();
  ASSERT_EQ(blob_files.size(), 1U);

  Close();
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, blob_files.front())));
  Reopen(options);

  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  iter->SeekToFirst();

  const Status status = iter->status();
  ASSERT_FALSE(iter->Valid());
  ASSERT_FALSE(status.ok()) << status.ToString();
  ASSERT_TRUE(status.IsCorruption() || status.IsIOError() ||
              status.IsNotFound())
      << status.ToString();
}

TEST_F(DBWideBlobDirectWriteTest,
       DirectWriteWideEntityLazyTTLCompactionDropsExpiredBlobFiles) {
  constexpr size_t kKeysPerPartition = 4;
  constexpr size_t kExpiredCycles = 2;
  constexpr size_t kLargeValueSize = 104 * 1024;
  constexpr uint64_t kExpiredTTL = 10;
  constexpr uint64_t kLiveTTLBase = 1000;

  for (const uint32_t partitions : {32U, 128U}) {
    SCOPED_TRACE("partitions=" + std::to_string(partitions));

    const size_t kTotalKeys = partitions * kKeysPerPartition;
    const size_t kExpiredKeys = partitions * kExpiredCycles;
    const size_t kLiveKeys = kTotalKeys - kExpiredKeys;

    std::atomic<int> filter_call_count{0};
    std::atomic<int> ttl_columns_seen{0};
    std::atomic<int> ttl_bad_size_count{0};
    std::atomic<int> missing_ttl_count{0};
    std::atomic<int> blob_columns_seen{0};
    std::atomic<uint64_t> ttl_cutoff{kLiveTTLBase};
    TTLOnlyLazyDropFilter filter(&ttl_cutoff, &filter_call_count,
                                 &ttl_columns_seen, &ttl_bad_size_count,
                                 &missing_ttl_count, &blob_columns_seen);

    Options options = GetBlobTestOptions();
    options.statistics = CreateDBStatistics();
    options.allow_concurrent_memtable_write = false;
    options.enable_blob_direct_write = true;
    options.blob_direct_write_partitions = partitions;
    options.min_blob_size = 512;
    options.blob_file_size = 256 * 1024;
    options.compaction_style = kCompactionStyleUniversal;
    options.compaction_filter = &filter;
    options.enable_blob_garbage_collection = false;

    DestroyAndReopen(options);

    auto for_each_key = [&](size_t begin_cycle, size_t end_cycle, auto&& fn) {
      for (size_t cycle = begin_cycle; cycle < end_cycle; ++cycle) {
        for (uint32_t partition = 0; partition < partitions; ++partition) {
          const size_t key_index = cycle * partitions + partition;
          fn(cycle, partition, key_index);
        }
      }
    };

    auto expected_ttl_for = [&](size_t cycle, size_t key_index) -> uint64_t {
      if (cycle < kExpiredCycles) {
        return kExpiredTTL;
      }
      return kLiveTTLBase + key_index;
    };

    // Insert more than one key per direct-write partition so each partition
    // reuses its active blob file. With blob_file_size sized for roughly two
    // values per file, each partition gets one expired-only file followed by
    // one live-only file.
    for_each_key(
        0, kKeysPerPartition,
        [&](size_t cycle, uint32_t partition, size_t key_index) {
          const std::string key = BuildTTLKey(partition, cycle);
          const std::string value =
              GenerateLargeValue(kLargeValueSize, ValueFillForIndex(key_index));
          const std::string ttl =
              EncodeFixedTTL(expected_ttl_for(cycle, key_index));
          const auto columns_data = BuildTTLWideEntityData(value, ttl);
          const WideColumns columns = ToWideColumns(columns_data);

          ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                                   key, columns));
        });
    ASSERT_OK(Flush());

    const auto blob_files_before = GetBlobFileNumbers();
    ASSERT_GT(blob_files_before.size(), partitions)
        << "Expected each partition to span multiple blob files";
    ASSERT_LT(blob_files_before.size(), kTotalKeys)
        << "Expected multiple keys to share direct-write blob files";

    ASSERT_OK(options.statistics->Reset());
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

    ASSERT_GE(filter_call_count.load(), static_cast<int>(kTotalKeys));
    ASSERT_EQ(ttl_columns_seen.load(), filter_call_count.load());
    ASSERT_EQ(ttl_bad_size_count.load(), 0)
        << "Compaction filter should only see 8-byte inline TTL values";
    ASSERT_EQ(missing_ttl_count.load(), 0)
        << "Every entity in this workload must carry a TTL column";
    ASSERT_EQ(blob_columns_seen.load(), filter_call_count.load())
        << "Expected exactly one blob-backed default column per entity";

    const uint64_t compact_read_bytes =
        options.statistics->getTickerCount(COMPACT_READ_BYTES);
    ASSERT_GT(compact_read_bytes, 0)
        << "Compaction should have processed the SST data";

    const uint64_t blob_bytes_read =
        options.statistics->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_READ);
    ASSERT_EQ(blob_bytes_read, 0)
        << "TTL-only compaction filter should not read blob payloads";

    const auto blob_files_after = GetBlobFileNumbers();
    ASSERT_LT(blob_files_after.size(), blob_files_before.size())
        << "Compaction should drop blob files owned only by expired entities";
    ASSERT_GE(blob_files_after.size(), partitions)
        << "Live blobs should keep at least one file per partition";

    {
      size_t live_count = 0;
      std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        ++live_count;
      }
      ASSERT_OK(iter->status());
      ASSERT_EQ(live_count, kLiveKeys);
    }

    for_each_key(0, kExpiredCycles,
                 [&](size_t cycle, uint32_t partition, size_t /*key_index*/) {
                   const std::string key = BuildTTLKey(partition, cycle);
                   PinnableSlice value;
                   ASSERT_TRUE(db_->Get(ReadOptions(),
                                        db_->DefaultColumnFamily(), key, &value)
                                   .IsNotFound())
                       << "Expired key " << key << " should be dropped";
                 });

    for_each_key(
        kExpiredCycles, kKeysPerPartition,
        [&](size_t cycle, uint32_t partition, size_t key_index) {
          const std::string key = BuildTTLKey(partition, cycle);
          const std::string expected_value =
              GenerateLargeValue(kLargeValueSize, ValueFillForIndex(key_index));
          const std::string expected_ttl =
              EncodeFixedTTL(expected_ttl_for(cycle, key_index));
          const auto expected_data =
              BuildTTLWideEntityData(expected_value, expected_ttl);
          const WideColumns expected = ToWideColumns(expected_data);

          PinnableWideColumns result;
          ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                                   key, &result));
          ASSERT_EQ(result.columns(), expected);
        });

    ttl_cutoff.store(std::numeric_limits<uint64_t>::max(),
                     std::memory_order_relaxed);
    ASSERT_OK(options.statistics->Reset());
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

    ASSERT_EQ(options.statistics->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_READ),
              0)
        << "Advancing the TTL cutoff should still avoid blob payload reads";
    ASSERT_TRUE(GetBlobFileNumbers().empty())
        << "All blob files should be dropped after every entity expires";

    Close();
  }
}

TEST_F(DBWideBlobDirectWriteTest,
       DirectWriteGetEntityReturnsZeroCopyBlobBuffersAcrossMove) {
  // Protects the zero-copy blob resolution optimization for direct-write wide
  // entities read pre-flush: each resolved blob-backed column Slice must point
  // directly at the buffer fetched during resolution (no copy), and must stay
  // valid and identical after the PinnableWideColumns result is moved (the
  // backing store uses address-stable forward_list nodes). A sync point
  // captures the fetched buffer pointers so we can compare them against
  // columns() before and after moving, for both GetEntity and MultiGetEntity.
  Options options = GetDirectWriteOptions();
  options.min_blob_size = 64;

  Reopen(options);

  const std::string key = "zero_copy_entity";
  const std::string inline_value = "inline";  // < min_blob_size: stays inline
  const std::string large1(128, 'a');         // >= min_blob_size: blob column
  const std::string large2(160, 'b');         // >= min_blob_size: blob column
  const WideColumns columns{{kDefaultWideColumnName, inline_value},
                            {"big1", large1},
                            {"big2", large2}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  // Intentionally do not flush: the entity (and its direct-write blob
  // references) stays in the memtable so reads resolve through
  // DBImpl::ResolveDirectWriteWideColumns().

  std::vector<const char*> fetched_ptrs;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::ResolveDirectWriteWideColumns:BlobFetched", [&](void* arg) {
        fetched_ptrs.push_back(static_cast<PinnableSlice*>(arg)->data());
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // GetEntity: two blob columns are fetched, each resolved column Slice points
  // straight at its fetched buffer, and the pointers survive move-construction
  // and move-assignment.
  {
    fetched_ptrs.clear();
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    ASSERT_EQ(result.columns(), columns);
    ASSERT_EQ(fetched_ptrs.size(), 2U);

    // columns() is sorted by name -> [default(""), big1, big2]; blob fetches
    // happen in that order, so fetched_ptrs[0]=big1, fetched_ptrs[1]=big2.
    const char* big1_ptr = result.columns()[1].value().data();
    const char* big2_ptr = result.columns()[2].value().data();
    ASSERT_EQ(big1_ptr, fetched_ptrs[0]);
    ASSERT_EQ(big2_ptr, fetched_ptrs[1]);

    PinnableWideColumns moved(std::move(result));
    ASSERT_EQ(moved.columns(), columns);
    ASSERT_EQ(moved.columns()[1].value().data(), big1_ptr);
    ASSERT_EQ(moved.columns()[2].value().data(), big2_ptr);

    PinnableWideColumns move_assigned;
    move_assigned = std::move(moved);
    ASSERT_EQ(move_assigned.columns(), columns);
    ASSERT_EQ(move_assigned.columns()[1].value().data(), big1_ptr);
    ASSERT_EQ(move_assigned.columns()[2].value().data(), big2_ptr);
  }

  // MultiGetEntity: same zero-copy guarantee, including across a move.
  {
    fetched_ptrs.clear();
    const std::array<Slice, 1> keys{Slice(key)};
    std::array<PinnableWideColumns, 1> results;
    std::array<Status, 1> statuses;
    db_->MultiGetEntity(ReadOptions(), db_->DefaultColumnFamily(), keys.size(),
                        keys.data(), results.data(), statuses.data());
    ASSERT_OK(statuses[0]);
    ASSERT_EQ(results[0].columns(), columns);
    ASSERT_EQ(fetched_ptrs.size(), 2U);

    const char* big1_ptr = results[0].columns()[1].value().data();
    const char* big2_ptr = results[0].columns()[2].value().data();
    ASSERT_EQ(big1_ptr, fetched_ptrs[0]);
    ASSERT_EQ(big2_ptr, fetched_ptrs[1]);

    PinnableWideColumns moved(std::move(results[0]));
    ASSERT_EQ(moved.columns(), columns);
    ASSERT_EQ(moved.columns()[1].value().data(), big1_ptr);
    ASSERT_EQ(moved.columns()[2].value().data(), big2_ptr);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBWideBlobDirectWriteTest,
       MultiGetEntitySoftLimitBoundsDirectWriteBlobResolution) {
  // value_size_soft_limit bounds direct-write blob resolution, which happens in
  // MultiGetImpl's post-processing loop (after the batch-loop check counted
  // only the small encoded blob-index bytes). With two over-limit entities, the
  // first one is read -- making progress even though it exceeds the limit --
  // and once the returned size is over the limit the second one is aborted
  // before its blob is fetched.
  Options options = GetDirectWriteOptions();

  Reopen(options);

  // Sorted order visits "a_large" before "b_large".
  const std::string first_key = "a_large";
  const std::string second_key = "b_large";
  const std::string first_blob(10000, 'a');   // >= min_blob_size: blob column
  const std::string second_blob(10000, 'b');  // >= min_blob_size: blob column
  const WideColumns first_columns{{kDefaultWideColumnName, "1"},
                                  {"c", first_blob}};
  const WideColumns second_columns{{kDefaultWideColumnName, "2"},
                                   {"c", second_blob}};

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           first_key, first_columns));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           second_key, second_columns));
  // Intentionally do not flush: the entities (and their direct-write blob
  // references) stay in the memtable so reads resolve through
  // DBImpl::ResolveDirectWriteWideColumns() in the post-processing loop.

  int blob_fetches = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::ResolveDirectWriteWideColumns:BlobFetched",
      [&](void* /*arg*/) { ++blob_fetches; });
  SyncPoint::GetInstance()->EnableProcessing();

  // Below either entity's resolved size (~10000 bytes), so reading the first
  // already pushes the returned total over the limit.
  ReadOptions read_options;
  read_options.value_size_soft_limit = 2000;

  const std::array<Slice, 2> keys{Slice(first_key), Slice(second_key)};
  std::array<PinnableWideColumns, 2> results;
  std::array<Status, 2> statuses;
  db_->MultiGetEntity(read_options, db_->DefaultColumnFamily(), keys.size(),
                      keys.data(), results.data(), statuses.data());

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(results[0].columns(), first_columns);
  ASSERT_TRUE(statuses[1].IsAborted());
  // Only the first key's blob was fetched; the second key's fetch was skipped
  // because the returned size was already over the limit.
  ASSERT_EQ(blob_fetches, 1);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBWideBlobDirectWriteTest,
       MultiGetEntitySoftLimitAlwaysReadsOversizedKey) {
  // "Always make progress": a single entity whose blob value alone exceeds the
  // soft limit is still read (not aborted), so a caller cannot loop forever
  // retrying a key that by itself is over the limit.
  Options options = GetDirectWriteOptions();

  Reopen(options);

  const std::string key = "oversized";
  const std::string large_blob(10000, 'b');  // >= min_blob_size: blob column
  const WideColumns columns{{kDefaultWideColumnName, "v"}, {"c", large_blob}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  // Intentionally do not flush: resolution happens in the post-processing loop.

  int blob_fetches = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::ResolveDirectWriteWideColumns:BlobFetched",
      [&](void* /*arg*/) { ++blob_fetches; });
  SyncPoint::GetInstance()->EnableProcessing();

  // Far below the entity's resolved size (~10000 bytes).
  ReadOptions read_options;
  read_options.value_size_soft_limit = 2000;

  const std::array<Slice, 1> keys{Slice(key)};
  std::array<PinnableWideColumns, 1> results;
  std::array<Status, 1> statuses;
  db_->MultiGetEntity(read_options, db_->DefaultColumnFamily(), keys.size(),
                      keys.data(), results.data(), statuses.data());

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(results[0].columns(), columns);
  ASSERT_EQ(blob_fetches, 1);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBWideBlobDirectWriteTest,
       MultiGetSoftLimitBoundsDirectWritePlainValue) {
  // Same behavior for plain values stored as direct-write blob references (the
  // is_blob_index path): the first over-limit value is read (progress), and the
  // second is aborted once the returned size is over the limit.
  Options options = GetDirectWriteOptions();

  Reopen(options);

  // Sorted order visits "a_large" before "b_large".
  const std::string first_key = "a_large";
  const std::string second_key = "b_large";
  const std::string first_value(10000, 'a');   // >= min_blob_size: direct write
  const std::string second_value(10000, 'b');  // >= min_blob_size: direct write

  ASSERT_OK(db_->Put(WriteOptions(), first_key, first_value));
  ASSERT_OK(db_->Put(WriteOptions(), second_key, second_value));
  // Intentionally do not flush: the values stay in the memtable as direct-write
  // blob references resolved in the post-processing loop.

  ReadOptions read_options;
  read_options.value_size_soft_limit = 2000;

  const std::array<Slice, 2> keys{Slice(first_key), Slice(second_key)};
  std::array<PinnableSlice, 2> values;
  std::array<Status, 2> statuses;
  db_->MultiGet(read_options, db_->DefaultColumnFamily(), keys.size(),
                keys.data(), values.data(), statuses.data());

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(values[0], first_value);
  ASSERT_TRUE(statuses[1].IsAborted());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
