//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cstring>
#include <iomanip>
#include <sstream>
#include <string>

#include "cache/cache_entry_roles.h"
#include "cache/cache_reservation_manager.h"
#include "db/db_test_util.h"
#include "options/options_helper.h"
#include "port/stack_trace.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/convenience.h"
#include "rocksdb/experimental.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/filter_policy_internal.h"
#include "table/format.h"
#include "test_util/testutil.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {
std::shared_ptr<const FilterPolicy> Create(double bits_per_key,
                                           const std::string& name) {
  return BloomLikeFilterPolicy::Create(name, bits_per_key);
}
const std::string kLegacyBloom = test::LegacyBloomFilterPolicy::kClassName();
const std::string kFastLocalBloom =
    test::FastLocalBloomFilterPolicy::kClassName();
const std::string kStandard128Ribbon =
    test::Standard128RibbonFilterPolicy::kClassName();
const std::string kAutoBloom = BloomFilterPolicy::kClassName();
const std::string kAutoRibbon = RibbonFilterPolicy::kClassName();

enum class FilterPartitioning {
  kUnpartitionedFilter,
  kCoupledPartitionedFilter,
  kDecoupledPartitionedFilter,
};

template <typename T>
T Pop(T& var) {
  auto rv = var;
  var = 0;
  return rv;
}
PerfContextByLevel& GetLevelPerfContext(uint32_t level) {
  return (*(get_perf_context()->level_to_perf_context))[level];
}
}  // anonymous namespace

// DB tests related to bloom filter.

class DBBloomFilterTest : public DBTestBase {
 public:
  DBBloomFilterTest()
      : DBTestBase("db_bloom_filter_test", /*env_do_fsync=*/true) {}

  bool PartitionFilters() {
    return filter_partitioning_ != FilterPartitioning::kUnpartitionedFilter;
  }

  void SetInTableOptions(BlockBasedTableOptions* table_options) {
    table_options->partition_filters = PartitionFilters();
    if (PartitionFilters()) {
      table_options->index_type =
          BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
    }
  }

 protected:
  FilterPartitioning filter_partitioning_ =
      FilterPartitioning::kUnpartitionedFilter;
};

class DBBloomFilterTestWithPartitioningParam
    : public DBBloomFilterTest,
      public testing::WithParamInterface<FilterPartitioning> {
 public:
  ~DBBloomFilterTestWithPartitioningParam() override = default;

  void SetUp() override { filter_partitioning_ = GetParam(); }
};

class DBBloomFilterTestWithFormatParams
    : public DBBloomFilterTest,
      public testing::WithParamInterface<
          std::tuple<std::string, FilterPartitioning, uint32_t>> {
 protected:
  std::string bfp_impl_;
  double bits_per_key_;
  uint32_t format_version_;

 public:
  ~DBBloomFilterTestWithFormatParams() override = default;

  void SetUp() override {
    bfp_impl_ = std::get<0>(GetParam());
    bits_per_key_ = 10;  // default;
    filter_partitioning_ = std::get<1>(GetParam());
    format_version_ = std::get<2>(GetParam());
  }

  void SetInTableOptions(BlockBasedTableOptions* table_options) {
    DBBloomFilterTest::SetInTableOptions(table_options);
    table_options->filter_policy = Create(bits_per_key_, bfp_impl_);
    table_options->format_version = format_version_;
  }
};

class DBBloomFilterTestDefFormatVersion
    : public DBBloomFilterTestWithFormatParams {};

class SliceTransformLimitedDomainGeneric : public SliceTransform {
  const char* Name() const override {
    return "SliceTransformLimitedDomainGeneric";
  }

  Slice Transform(const Slice& src) const override {
    return Slice(src.data(), 5);
  }

  bool InDomain(const Slice& src) const override {
    // prefix will be x????
    return src.size() >= 5;
  }
};

// KeyMayExist can lead to a few false positives, but not false negatives.
// To make test deterministic, use a much larger number of bits per key-20 than
// bits in the key, so that false positives are eliminated
TEST_P(DBBloomFilterTestDefFormatVersion, KeyMayExist) {
  do {
    ReadOptions ropts;
    std::string value;
    anon::OptionsOverride options_override;
    options_override.filter_policy = Create(20, bfp_impl_);
    options_override.partition_filters = PartitionFilters();
    options_override.metadata_block_size = 32;
    options_override.full_block_cache = true;
    Options options = CurrentOptions(options_override);
    if (PartitionFilters()) {
      auto* table_options =
          options.table_factory->GetOptions<BlockBasedTableOptions>();
      if (table_options != nullptr &&
          table_options->index_type !=
              BlockBasedTableOptions::kTwoLevelIndexSearch) {
        // In the current implementation partitioned filters depend on
        // partitioned indexes
        continue;
      }
    }
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    CreateAndReopenWithCF({"pikachu"}, options);

    ASSERT_FALSE(db_->KeyMayExist(ropts, handles_[1], "a", &value));

    ASSERT_OK(Put(1, "a", "b"));
    bool value_found = false;
    ASSERT_TRUE(
        db_->KeyMayExist(ropts, handles_[1], "a", &value, &value_found));
    ASSERT_TRUE(value_found);
    ASSERT_EQ("b", value);

    ASSERT_OK(Flush(1));
    value.clear();

    uint64_t numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    uint64_t cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    ASSERT_TRUE(
        db_->KeyMayExist(ropts, handles_[1], "a", &value, &value_found));
    ASSERT_FALSE(value_found);
    // assert that no new files were opened and no new blocks were
    // read into block cache.
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));

    ASSERT_OK(Delete(1, "a"));

    numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    ASSERT_FALSE(db_->KeyMayExist(ropts, handles_[1], "a", &value));
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));

    ASSERT_OK(Flush(1));
    ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1],
                                          true /* disallow trivial move */));

    numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    ASSERT_FALSE(db_->KeyMayExist(ropts, handles_[1], "a", &value));
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));

    ASSERT_OK(Delete(1, "c"));

    numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    ASSERT_FALSE(db_->KeyMayExist(ropts, handles_[1], "c", &value));
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));

    // KeyMayExist function only checks data in block caches, which is not used
    // by plain table format.
  } while (
      ChangeOptions(kSkipPlainTable | kSkipHashIndex | kSkipFIFOCompaction));
}

TEST_P(DBBloomFilterTestWithPartitioningParam,
       GetFilterByPrefixBloomCustomPrefixExtractor) {
  Options options = last_options_;
  options.prefix_extractor =
      std::make_shared<SliceTransformLimitedDomainGeneric>();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  get_perf_context()->EnablePerLevelPerfContext();
  BlockBasedTableOptions bbto;
  SetInTableOptions(&bbto);
  bbto.filter_policy.reset(NewBloomFilterPolicy(10));
  bbto.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  WriteOptions wo;
  ReadOptions ro;
  FlushOptions fo;
  fo.wait = true;
  std::string value;

  ASSERT_OK(dbfull()->Put(wo, "barbarbar", "foo"));
  ASSERT_OK(dbfull()->Put(wo, "barbarbar2", "foo2"));
  ASSERT_OK(dbfull()->Put(wo, "foofoofoo", "bar"));

  ASSERT_OK(dbfull()->Flush(fo));

  ASSERT_EQ("foo", Get("barbarbar"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(Pop(GetLevelPerfContext(0).bloom_filter_useful), 0);

  ASSERT_EQ("foo2", Get("barbarbar2"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(Pop(GetLevelPerfContext(0).bloom_filter_useful), 0);

  ASSERT_EQ("NOT_FOUND", Get("barbarbar3"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(Pop(GetLevelPerfContext(0).bloom_filter_useful), 0);

  ASSERT_EQ("NOT_FOUND", Get("barfoofoo"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 1);
  EXPECT_EQ(Pop(GetLevelPerfContext(0).bloom_filter_useful), 1);

  ASSERT_EQ("NOT_FOUND", Get("foobarbar"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 1);
  EXPECT_EQ(Pop(GetLevelPerfContext(0).bloom_filter_useful), 1);

  ro.total_order_seek = true;
  // NOTE: total_order_seek no longer affects Get()
  ASSERT_EQ("NOT_FOUND", Get("foobarbar"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 1);
  EXPECT_EQ(Pop(GetLevelPerfContext(0).bloom_filter_useful), 1);

  // No bloom on extractor changed
  ASSERT_OK(db_->SetOptions({{"prefix_extractor", "capped:10"}}));
  ASSERT_EQ("NOT_FOUND", Get("foobarbar"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(Pop(GetLevelPerfContext(0).bloom_filter_useful), 0);

  // No bloom on extractor changed, after re-open
  options.prefix_extractor.reset(NewCappedPrefixTransform(10));
  Reopen(options);
  ASSERT_EQ("NOT_FOUND", Get("foobarbar"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(Pop(GetLevelPerfContext(0).bloom_filter_useful), 0);

  get_perf_context()->Reset();
}

TEST_P(DBBloomFilterTestWithPartitioningParam, GetFilterByPrefixBloom) {
  Options options = last_options_;
  options.prefix_extractor.reset(NewFixedPrefixTransform(8));
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  get_perf_context()->EnablePerLevelPerfContext();
  BlockBasedTableOptions bbto;
  SetInTableOptions(&bbto);
  bbto.filter_policy.reset(NewBloomFilterPolicy(10));
  bbto.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  WriteOptions wo;
  ReadOptions ro;
  FlushOptions fo;
  fo.wait = true;
  std::string value;

  ASSERT_OK(dbfull()->Put(wo, "barbarbar", "foo"));
  ASSERT_OK(dbfull()->Put(wo, "barbarbar2", "foo2"));
  ASSERT_OK(dbfull()->Put(wo, "foofoofoo", "bar"));

  ASSERT_OK(dbfull()->Flush(fo));

  ASSERT_EQ("foo", Get("barbarbar"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  ASSERT_EQ("foo2", Get("barbarbar2"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  ASSERT_EQ("NOT_FOUND", Get("barbarbar3"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(Pop(GetLevelPerfContext(0).bloom_filter_useful), 0);

  ASSERT_EQ("NOT_FOUND", Get("barfoofoo"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 1);
  EXPECT_EQ(Pop(GetLevelPerfContext(0).bloom_filter_useful), 1);

  ASSERT_EQ("NOT_FOUND", Get("foobarbar"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 1);
  EXPECT_EQ(Pop(GetLevelPerfContext(0).bloom_filter_useful), 1);

  ro.total_order_seek = true;
  // NOTE: total_order_seek no longer affects Get()
  ASSERT_EQ("NOT_FOUND", Get("foobarbar"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 1);
  EXPECT_EQ(Pop(GetLevelPerfContext(0).bloom_filter_useful), 1);

  // No bloom on extractor changed
  ASSERT_OK(db_->SetOptions({{"prefix_extractor", "capped:10"}}));
  ASSERT_EQ("NOT_FOUND", Get("foobarbar"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(Pop(GetLevelPerfContext(0).bloom_filter_useful), 0);

  get_perf_context()->Reset();
}

TEST_P(DBBloomFilterTestWithPartitioningParam, FilterNumEntriesCoalesce) {
  for (bool prefix : {true, false}) {
    SCOPED_TRACE("prefix=" + std::to_string(prefix));
    for (bool whole : {true, false}) {
      SCOPED_TRACE("whole=" + std::to_string(whole));
      Options options = last_options_;
      options.prefix_extractor.reset();
      if (prefix) {
        options.prefix_extractor.reset(NewFixedPrefixTransform(3));
      }
      BlockBasedTableOptions bbto;
      SetInTableOptions(&bbto);
      bbto.filter_policy.reset(NewBloomFilterPolicy(10));
      bbto.whole_key_filtering = whole;
      options.table_factory.reset(NewBlockBasedTableFactory(bbto));
      DestroyAndReopen(options);

      // Need a snapshot to allow keeping multiple entries for the same key
      std::vector<const Snapshot*> snapshots;
      for (int i = 1; i <= 3; ++i) {
        std::string val = "val" + std::to_string(i);
        ASSERT_OK(Put("foo1", val));
        ASSERT_OK(Put("foo2", val));
        ASSERT_OK(Put("bar1", val));
        ASSERT_OK(Put("bar2", val));
        ASSERT_OK(Put("bar3", val));
        snapshots.push_back(db_->GetSnapshot());
      }
      ASSERT_OK(Flush());

      TablePropertiesCollection tpc;
      ASSERT_OK(db_->GetPropertiesOfAllTables(&tpc));
      // sanity checks
      ASSERT_EQ(tpc.size(), 1U);
      auto& tp = *tpc.begin()->second;
      EXPECT_EQ(tp.num_entries, 3U * 5U);

      // test checks
      unsigned ex_filter_entries = 0;
      if (whole) {
        ex_filter_entries += 5;  // unique keys
      }
      if (prefix) {
        ex_filter_entries += 2;  // unique prefixes
      }
      EXPECT_EQ(tp.num_filter_entries, ex_filter_entries);

      for (auto* sn : snapshots) {
        db_->ReleaseSnapshot(sn);
      }
    }
  }
}

TEST_P(DBBloomFilterTestWithPartitioningParam, WholeKeyFilterProp) {
  Options options = last_options_;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  get_perf_context()->EnablePerLevelPerfContext();

  BlockBasedTableOptions bbto;
  SetInTableOptions(&bbto);
  bbto.filter_policy.reset(NewBloomFilterPolicy(10));
  bbto.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  WriteOptions wo;
  ReadOptions ro;
  FlushOptions fo;
  fo.wait = true;
  std::string value;

  ASSERT_OK(dbfull()->Put(wo, "foobar", "foo"));
  // Needs insert some keys to make sure files are not filtered out by key
  // ranges.
  ASSERT_OK(dbfull()->Put(wo, "aaa", ""));
  ASSERT_OK(dbfull()->Put(wo, "zzz", ""));
  ASSERT_OK(dbfull()->Flush(fo));

  Reopen(options);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 0);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 0);
  ASSERT_EQ("NOT_FOUND", Get("bar"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 1);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 0);
  ASSERT_EQ("foo", Get("foobar"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 0);

  // Reopen with whole key filtering enabled and prefix extractor
  // NULL. Bloom filter should be off for both of whole key and
  // prefix bloom.
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  options.prefix_extractor.reset();
  Reopen(options);

  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 0);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 0);
  ASSERT_EQ("NOT_FOUND", Get("bar"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 0);
  ASSERT_EQ("foo", Get("foobar"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 0);
  // Write DB with only full key filtering.
  ASSERT_OK(dbfull()->Put(wo, "foobar", "foo"));
  // Needs insert some keys to make sure files are not filtered out by key
  // ranges.
  ASSERT_OK(dbfull()->Put(wo, "aaa", ""));
  ASSERT_OK(dbfull()->Put(wo, "zzz", ""));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Reopen with both of whole key off and prefix extractor enabled.
  // Still no bloom filter should be used.
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  bbto.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);

  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 0);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 0);
  ASSERT_EQ("NOT_FOUND", Get("bar"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 0);
  ASSERT_EQ("foo", Get("foobar"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 0);

  // Try to create a DB with mixed files:
  ASSERT_OK(dbfull()->Put(wo, "foobar", "foo"));
  // Needs insert some keys to make sure files are not filtered out by key
  // ranges.
  ASSERT_OK(dbfull()->Put(wo, "aaa", ""));
  ASSERT_OK(dbfull()->Put(wo, "zzz", ""));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  options.prefix_extractor.reset();
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);

  // Try to create a DB with mixed files.
  ASSERT_OK(dbfull()->Put(wo, "barfoo", "bar"));
  // In this case needs insert some keys to make sure files are
  // not filtered out by key ranges.
  ASSERT_OK(dbfull()->Put(wo, "aaa", ""));
  ASSERT_OK(dbfull()->Put(wo, "zzz", ""));
  ASSERT_OK(Flush());

  // Now we have two files:
  // File 1: An older file with prefix bloom (disabled)
  // File 2: A newer file with whole bloom filter.
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 0);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("NOT_FOUND", Get("bar"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("foo", Get("foobar"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("bar", Get("barfoo"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 0);

  // Reopen with the same setting: only whole key is used
  Reopen(options);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 0);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("NOT_FOUND", Get("bar"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("foo", Get("foobar"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("bar", Get("barfoo"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 0);

  // Restart with both filters are allowed
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 0);
  // File 1 will has it filtered out.
  // File 2 will not, as prefix `foo` exists in the file.
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("NOT_FOUND", Get("bar"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 1);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("foo", Get("foobar"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("bar", Get("barfoo"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 0);

  // Restart with only prefix bloom is allowed.
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  bbto.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 0);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 0);
  ASSERT_EQ("NOT_FOUND", Get("bar"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 1);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 0);
  ASSERT_EQ("foo", Get("foobar"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 0);
  ASSERT_EQ("bar", Get("barfoo"));
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 0);
  uint64_t bloom_filter_useful_all_levels = 0;
  for (auto& kv : (*(get_perf_context()->level_to_perf_context))) {
    if (kv.second.bloom_filter_useful > 0) {
      bloom_filter_useful_all_levels += kv.second.bloom_filter_useful;
    }
  }
  ASSERT_EQ(12, bloom_filter_useful_all_levels);
  get_perf_context()->Reset();
}

INSTANTIATE_TEST_CASE_P(
    DBBloomFilterTestWithPartitioningParam,
    DBBloomFilterTestWithPartitioningParam,
    ::testing::Values(FilterPartitioning::kUnpartitionedFilter,
                      FilterPartitioning::kCoupledPartitionedFilter,
                      FilterPartitioning::kDecoupledPartitionedFilter));

TEST_P(DBBloomFilterTestWithFormatParams, BloomFilter) {
  do {
    Options options = CurrentOptions();
    env_->count_random_reads_ = true;
    options.env = env_;
    // ChangeCompactOptions() only changes compaction style, which does not
    // trigger reset of table_factory
    BlockBasedTableOptions table_options;
    // When partitioned filters are coupled to index blocks, they tend to get
    // extra fractional bits per key when rounding up to the next cache line
    // size. Here we correct for that to get similar effective bits per key.
    bits_per_key_ = table_options.decouple_partitioned_filters ? 10.5 : 10;
    SetInTableOptions(&table_options);
    table_options.no_block_cache = true;
    table_options.optimize_filters_for_memory = false;
    if (format_version_ >= 4) {
      // value delta encoding challenged more with index interval > 1
      table_options.index_block_restart_interval = 8;
    }
    // This test is rather sensitive to the actual filter partition block size,
    // and keeping that consistent between coupled and uncoupled requires a
    // different metadata block size for this example (where it controls index
    // block size).
    table_options.metadata_block_size =
        table_options.decouple_partitioned_filters ? 320 : 32;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    CreateAndReopenWithCF({"pikachu"}, options);

    // Populate multiple layers
    const int N = 10000;
    for (int i = 0; i < N; i++) {
      ASSERT_OK(Put(1, Key(i), Key(i)));
    }
    Compact(1, "a", "z");
    for (int i = 0; i < N; i += 100) {
      ASSERT_OK(Put(1, Key(i), Key(i)));
    }
    ASSERT_OK(Flush(1));

    // Prevent auto compactions triggered by seeks
    env_->delay_sstable_sync_.store(true, std::memory_order_release);

    // Lookup present keys.  Should rarely read from small sstable.
    env_->random_read_counter_.Reset();
    for (int i = 0; i < N; i++) {
      ASSERT_EQ(Key(i), Get(1, Key(i)));
    }
    int reads = env_->random_read_counter_.Read();
    fprintf(stderr, "%d present => %d reads\n", N, reads);
    ASSERT_GE(reads, N);
    if (PartitionFilters()) {
      // Without block cache, we read an extra partition filter per each
      // level*read and a partition index per each read
      ASSERT_LE(reads, 4 * N + 2 * N / 100);
    } else {
      ASSERT_LE(reads, N + 2 * N / 100);
    }

    // Lookup present keys.  Should rarely read from either sstable.
    env_->random_read_counter_.Reset();
    for (int i = 0; i < N; i++) {
      ASSERT_EQ("NOT_FOUND", Get(1, Key(i) + ".missing"));
    }
    reads = env_->random_read_counter_.Read();
    fprintf(stderr, "%d missing => %d reads\n", N, reads);
    if (PartitionFilters()) {
      // With partitioned filter we read one extra filter per level per each
      // missed read.
      ASSERT_LE(reads, 2 * N + 3 * N / 100);
    } else {
      ASSERT_LE(reads, 3 * N / 100);
    }

    // Sanity check some table properties
    std::map<std::string, std::string> props;
    ASSERT_TRUE(db_->GetMapProperty(
        handles_[1], DB::Properties::kAggregatedTableProperties, &props));
    uint64_t nkeys = N + N / 100;
    uint64_t filter_size = ParseUint64(props["filter_size"]);
    EXPECT_LE(filter_size,
              (PartitionFilters() ? 12 : 11) * nkeys / /*bits / byte*/ 8);
    if (bfp_impl_ == kAutoRibbon) {
      // Sometimes using Ribbon filter which is more space-efficient
      EXPECT_GE(filter_size, 7 * nkeys / /*bits / byte*/ 8);
    } else {
      // Always Bloom
      EXPECT_GE(filter_size, 10 * nkeys / /*bits / byte*/ 8);
    }

    uint64_t num_filter_entries = ParseUint64(props["num_filter_entries"]);
    EXPECT_EQ(num_filter_entries, nkeys);

    env_->delay_sstable_sync_.store(false, std::memory_order_release);
    Close();
  } while (ChangeCompactOptions());
}

namespace {

class AlwaysTrueBitsBuilder : public FilterBitsBuilder {
 public:
  void AddKey(const Slice&) override { ++count_; }
  void AddKeyAndAlt(const Slice&, const Slice&) override { count_ += 2; }
  size_t EstimateEntriesAdded() override { return count_; }
  Slice Finish(std::unique_ptr<const char[]>* /* buf */) override {
    count_ = 0;
    // Interpreted as "always true" filter (0 probes over 1 byte of
    // payload, 5 bytes metadata)
    return Slice("\0\0\0\0\0\0", kAlwaysTrueFilterBytes);
  }
  using FilterBitsBuilder::Finish;
  size_t ApproximateNumEntries(size_t) override { return SIZE_MAX; }
  size_t CalculateSpace(size_t /* num_entries */) override {
    return kAlwaysTrueFilterBytes;
  }
  double EstimatedFpRate(size_t /* num_entries */,
                         size_t /* bytes */) override {
    return 1.0;
  }

 private:
  static constexpr size_t kAlwaysTrueFilterBytes = 6;
  size_t count_ = 0;
};

class AlwaysTrueFilterPolicy : public ReadOnlyBuiltinFilterPolicy {
 public:
  explicit AlwaysTrueFilterPolicy(bool skip) : skip_(skip) {}

  FilterBitsBuilder* GetBuilderWithContext(
      const FilterBuildingContext&) const override {
    if (skip_) {
      return nullptr;
    } else {
      return new AlwaysTrueBitsBuilder();
    }
  }

 private:
  bool skip_;
};

}  // anonymous namespace

TEST_P(DBBloomFilterTestWithFormatParams, SkipFilterOnEssentiallyZeroBpk) {
  constexpr int maxKey = 10;
  auto PutFn = [&]() {
    int i;
    // Put
    for (i = 0; i < maxKey; i++) {
      ASSERT_OK(Put(Key(i), Key(i)));
    }
    ASSERT_OK(Flush());
  };
  auto GetFn = [&]() {
    int i;
    // Get OK
    for (i = 0; i < maxKey; i++) {
      ASSERT_EQ(Key(i), Get(Key(i)));
    }
    // Get NotFound
    for (; i < maxKey * 2; i++) {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  };
  auto PutAndGetFn = [&]() {
    PutFn();
    GetFn();
  };
  std::map<std::string, std::string> props;
  const auto& kAggTableProps = DB::Properties::kAggregatedTableProperties;

  Options options = CurrentOptions();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  SetInTableOptions(&table_options);

  // Test 1: bits per key < 0.5 means skip filters -> no filter
  // constructed or read.
  table_options.filter_policy = Create(0.4, bfp_impl_);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);
  PutAndGetFn();

  // Verify no filter access nor contruction
  EXPECT_EQ(TestGetTickerCount(options, BLOOM_FILTER_FULL_POSITIVE), 0);
  EXPECT_EQ(TestGetTickerCount(options, BLOOM_FILTER_FULL_TRUE_POSITIVE), 0);

  props.clear();
  ASSERT_TRUE(db_->GetMapProperty(kAggTableProps, &props));
  EXPECT_EQ(props["filter_size"], "0");

  // Test 2: use custom API to skip filters -> no filter constructed
  // or read.
  table_options.filter_policy.reset(
      new AlwaysTrueFilterPolicy(/* skip */ true));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);
  PutAndGetFn();

  // Verify no filter access nor construction
  EXPECT_EQ(TestGetTickerCount(options, BLOOM_FILTER_FULL_POSITIVE), 0);
  EXPECT_EQ(TestGetTickerCount(options, BLOOM_FILTER_FULL_TRUE_POSITIVE), 0);

  props.clear();
  ASSERT_TRUE(db_->GetMapProperty(kAggTableProps, &props));
  EXPECT_EQ(props["filter_size"], "0");

  // Control test: using an actual filter with 100% FP rate -> the filter
  // is constructed and checked on read.
  table_options.filter_policy.reset(
      new AlwaysTrueFilterPolicy(/* skip */ false));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);
  PutAndGetFn();

  // Verify filter is accessed (and constructed)
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_FULL_POSITIVE), maxKey * 2);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_FULL_TRUE_POSITIVE), maxKey);
  props.clear();
  ASSERT_TRUE(db_->GetMapProperty(kAggTableProps, &props));
  EXPECT_NE(props["filter_size"], "0");

  // Test 3 (options test): Able to read existing filters with longstanding
  // generated options file entry `filter_policy=rocksdb.BuiltinBloomFilter`
  ASSERT_OK(FilterPolicy::CreateFromString(ConfigOptions(),
                                           "rocksdb.BuiltinBloomFilter",
                                           &table_options.filter_policy));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  GetFn();

  // Verify filter is accessed
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_FULL_POSITIVE), maxKey * 2);
  EXPECT_EQ(PopTicker(options, BLOOM_FILTER_FULL_TRUE_POSITIVE), maxKey);

  // But new filters are not generated (configuration details unknown)
  DestroyAndReopen(options);
  PutAndGetFn();

  // Verify no filter access nor construction
  EXPECT_EQ(TestGetTickerCount(options, BLOOM_FILTER_FULL_POSITIVE), 0);
  EXPECT_EQ(TestGetTickerCount(options, BLOOM_FILTER_FULL_TRUE_POSITIVE), 0);

  props.clear();
  ASSERT_TRUE(db_->GetMapProperty(kAggTableProps, &props));
  EXPECT_EQ(props["filter_size"], "0");
}

TEST_P(DBBloomFilterTestWithFormatParams, FilterBitsBuilderDedup) {
  BlockBasedTableOptions table_options;
  SetInTableOptions(&table_options);
  FilterBuildingContext context{table_options};
  std::unique_ptr<FilterBitsBuilder> builder{
      table_options.filter_policy->GetBuilderWithContext(context)};

  ASSERT_EQ(builder->EstimateEntriesAdded(), 0U);
  // Check for sufficient de-duplication between regular keys and alt keys
  // (prefixes), keeping in mind that the key might equal its prefix.

  builder->AddKey("abc");
  ASSERT_EQ(builder->EstimateEntriesAdded(), 1U);
  builder->AddKeyAndAlt("abc1", "abc");
  ASSERT_EQ(builder->EstimateEntriesAdded(), 2U);
  builder->AddKeyAndAlt("bcd", "bcd");
  ASSERT_EQ(builder->EstimateEntriesAdded(), 3U);
  builder->AddKeyAndAlt("cde-1", "cde");
  ASSERT_EQ(builder->EstimateEntriesAdded(), 5U);
  builder->AddKeyAndAlt("cde", "cde");
  ASSERT_EQ(builder->EstimateEntriesAdded(), 5U);
  builder->AddKeyAndAlt("cde1", "cde");
  ASSERT_EQ(builder->EstimateEntriesAdded(), 6U);
  builder->AddKeyAndAlt("def-1", "def");
  ASSERT_EQ(builder->EstimateEntriesAdded(), 8U);
  builder->AddKeyAndAlt("def", "def");
  ASSERT_EQ(builder->EstimateEntriesAdded(), 8U);
  builder->AddKey("def$$");  // Like not in extractor domain
  ASSERT_EQ(builder->EstimateEntriesAdded(), 9U);
  builder->AddKey("def$$");
  ASSERT_EQ(builder->EstimateEntriesAdded(), 9U);
  builder->AddKeyAndAlt("efg42", "efg");
  ASSERT_EQ(builder->EstimateEntriesAdded(), 11U);
  builder->AddKeyAndAlt("efg", "efg");  // Like extra "alt" on a partition
  ASSERT_EQ(builder->EstimateEntriesAdded(), 11U);
}

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
INSTANTIATE_TEST_CASE_P(
    FormatDef, DBBloomFilterTestDefFormatVersion,
    ::testing::Values(
        std::make_tuple(kAutoBloom,
                        FilterPartitioning::kCoupledPartitionedFilter,
                        test::kDefaultFormatVersion),
        std::make_tuple(kAutoBloom,
                        FilterPartitioning::kDecoupledPartitionedFilter,
                        test::kDefaultFormatVersion),
        std::make_tuple(kAutoBloom, FilterPartitioning::kUnpartitionedFilter,
                        test::kDefaultFormatVersion),
        std::make_tuple(kAutoRibbon, FilterPartitioning::kUnpartitionedFilter,
                        test::kDefaultFormatVersion)));

INSTANTIATE_TEST_CASE_P(
    FormatDef, DBBloomFilterTestWithFormatParams,
    ::testing::Values(
        std::make_tuple(kAutoBloom,
                        FilterPartitioning::kCoupledPartitionedFilter,
                        test::kDefaultFormatVersion),
        std::make_tuple(kAutoBloom,
                        FilterPartitioning::kDecoupledPartitionedFilter,
                        test::kDefaultFormatVersion),
        std::make_tuple(kAutoBloom, FilterPartitioning::kUnpartitionedFilter,
                        test::kDefaultFormatVersion),
        std::make_tuple(kAutoRibbon, FilterPartitioning::kUnpartitionedFilter,
                        test::kDefaultFormatVersion)));

INSTANTIATE_TEST_CASE_P(
    FormatLatest, DBBloomFilterTestWithFormatParams,
    ::testing::Values(
        std::make_tuple(kAutoBloom,
                        FilterPartitioning::kCoupledPartitionedFilter,
                        BlockBasedTableOptions::kLatestSupportedFormatVersion),
        std::make_tuple(kAutoBloom,
                        FilterPartitioning::kDecoupledPartitionedFilter,
                        BlockBasedTableOptions::kLatestSupportedFormatVersion),
        std::make_tuple(kAutoBloom, FilterPartitioning::kUnpartitionedFilter,
                        BlockBasedTableOptions::kLatestSupportedFormatVersion),
        std::make_tuple(
            kAutoRibbon, FilterPartitioning::kUnpartitionedFilter,
            BlockBasedTableOptions::kLatestSupportedFormatVersion)));
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

TEST_F(DBBloomFilterTest, PartitionedPrefixFilterMultiGetKeysOutOfDomain) {
  for (bool decouple : {false, true}) {
    SCOPED_TRACE("decouple_partitioned_filters=" + std::to_string(decouple));
    Options options = CurrentOptions();
    options.prefix_extractor.reset(NewFixedPrefixTransform(4));
    BlockBasedTableOptions bbto;
    bbto.filter_policy.reset(NewBloomFilterPolicy(10));
    bbto.whole_key_filtering = false;
    bbto.partition_filters = true;
    bbto.decouple_partitioned_filters = decouple;
    bbto.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
    bbto.metadata_block_size = 1;
    bbto.block_size = 1;
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    DestroyAndReopen(options);

    std::vector<std::string> keys;
    std::vector<std::string> expected;
    for (char c = 'a'; c <= 'j'; ++c) {
      keys.emplace_back(1, c);
      expected.push_back("v_" + keys.back());
      ASSERT_OK(Put(keys.back(), expected.back()));
    }
    ASSERT_OK(Put("kkkk1", "v_kkkk1"));
    ASSERT_OK(Flush());
    keys.emplace_back("aaaa");
    expected.emplace_back("NOT_FOUND");
    keys.emplace_back("kkkk1");
    expected.emplace_back("v_kkkk1");

    for (size_t i = 0; i < keys.size(); ++i) {
      ASSERT_EQ(expected[i], Get(keys[i]));
    }
    ASSERT_EQ(expected, MultiGet(keys));
  }
}

TEST_F(DBBloomFilterTest, BloomFilterRate) {
  while (ChangeFilterOptions()) {
    Options options = CurrentOptions();
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    get_perf_context()->EnablePerLevelPerfContext();
    CreateAndReopenWithCF({"pikachu"}, options);

    const int maxKey = 10000;
    for (int i = 0; i < maxKey; i++) {
      ASSERT_OK(Put(1, Key(i), Key(i)));
    }
    // Add a large key to make the file contain wide range
    ASSERT_OK(Put(1, Key(maxKey + 55555), Key(maxKey + 55555)));
    ASSERT_OK(Flush(1));

    // Check if they can be found
    for (int i = 0; i < maxKey; i++) {
      ASSERT_EQ(Key(i), Get(1, Key(i)));
    }
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);

    // Check if filter is useful
    for (int i = 0; i < maxKey; i++) {
      ASSERT_EQ("NOT_FOUND", Get(1, Key(i + 33333)));
    }
    ASSERT_GE(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), maxKey * 0.98);
    ASSERT_GE(GetLevelPerfContext(0).bloom_filter_useful, maxKey * 0.98);
    get_perf_context()->Reset();
  }
}

namespace {
struct CompatibilityConfig {
  std::shared_ptr<const FilterPolicy> policy;
  bool partitioned;
  uint32_t format_version;

  void SetInTableOptions(BlockBasedTableOptions* table_options) {
    table_options->filter_policy = policy;
    table_options->partition_filters = partitioned;
    if (partitioned) {
      table_options->index_type =
          BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
    } else {
      table_options->index_type =
          BlockBasedTableOptions::IndexType::kBinarySearch;
    }
    table_options->format_version = format_version;
  }
};
// High bits per key -> almost no FPs
std::shared_ptr<const FilterPolicy> kCompatibilityBloomPolicy{
    NewBloomFilterPolicy(20)};
// bloom_before_level=-1 -> always use Ribbon
std::shared_ptr<const FilterPolicy> kCompatibilityRibbonPolicy{
    NewRibbonFilterPolicy(20, -1)};

std::vector<CompatibilityConfig> kCompatibilityConfigs = {
    {kCompatibilityBloomPolicy, false, BlockBasedTableOptions().format_version},
    {kCompatibilityBloomPolicy, true, BlockBasedTableOptions().format_version},
    {kCompatibilityBloomPolicy, false, /* legacy Bloom */ 4U},
    {kCompatibilityRibbonPolicy, false,
     BlockBasedTableOptions().format_version},
    {kCompatibilityRibbonPolicy, true, BlockBasedTableOptions().format_version},
};
}  // anonymous namespace

TEST_F(DBBloomFilterTest, BloomFilterCompatibility) {
  Options options = CurrentOptions();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.level0_file_num_compaction_trigger =
      static_cast<int>(kCompatibilityConfigs.size()) + 1;
  options.max_open_files = -1;

  Close();

  // Create one file for each kind of filter. Each file covers a distinct key
  // range.
  for (size_t i = 0; i < kCompatibilityConfigs.size(); ++i) {
    BlockBasedTableOptions table_options;
    kCompatibilityConfigs[i].SetInTableOptions(&table_options);
    ASSERT_TRUE(table_options.filter_policy != nullptr);
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    Reopen(options);

    std::string prefix = std::to_string(i) + "_";
    ASSERT_OK(Put(prefix + "A", "val"));
    ASSERT_OK(Put(prefix + "Z", "val"));
    ASSERT_OK(Flush());
  }

  // Test filter is used between each pair of {reader,writer} configurations,
  // because any built-in FilterPolicy should be able to read filters from any
  // other built-in FilterPolicy
  for (size_t i = 0; i < kCompatibilityConfigs.size(); ++i) {
    BlockBasedTableOptions table_options;
    kCompatibilityConfigs[i].SetInTableOptions(&table_options);
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    Reopen(options);
    for (size_t j = 0; j < kCompatibilityConfigs.size(); ++j) {
      std::string prefix = std::to_string(j) + "_";
      ASSERT_EQ("val", Get(prefix + "A"));  // Filter positive
      ASSERT_EQ("val", Get(prefix + "Z"));  // Filter positive
      // Filter negative, with high probability
      ASSERT_EQ("NOT_FOUND", Get(prefix + "Q"));
      EXPECT_EQ(PopTicker(options, BLOOM_FILTER_FULL_POSITIVE), 2);
      EXPECT_EQ(PopTicker(options, BLOOM_FILTER_USEFUL), 1);
    }
  }
}

// To align with the type of hash entry being reserved in implementation.
using FilterConstructionReserveMemoryHash = uint64_t;

class ChargeFilterConstructionTestWithParam
    : public DBBloomFilterTest,
      public testing::WithParamInterface<
          std::tuple<CacheEntryRoleOptions::Decision, std::string,
                     FilterPartitioning, bool>> {
 public:
  ChargeFilterConstructionTestWithParam()
      : num_key_(0),
        charge_filter_construction_(std::get<0>(GetParam())),
        policy_(std::get<1>(GetParam())),
        detect_filter_construct_corruption_(std::get<3>(GetParam())) {
    filter_partitioning_ = std::get<2>(GetParam());
    if (charge_filter_construction_ ==
            CacheEntryRoleOptions::Decision::kDisabled ||
        policy_ == kLegacyBloom) {
      // For these cases, we only interested in whether filter construction
      // cache charging happens instead of its accuracy. Therefore we don't
      // need many keys.
      num_key_ = 5;
    } else if (PartitionFilters()) {
      // For PartitionFilter case, since we set
      // table_options.metadata_block_size big enough such that each partition
      // trigger at least 1 dummy entry reservation each for hash entries and
      // final filter, we need a large number of keys to ensure we have at least
      // two partitions.
      num_key_ = 18 *
                 CacheReservationManagerImpl<
                     CacheEntryRole::kFilterConstruction>::GetDummyEntrySize() /
                 sizeof(FilterConstructionReserveMemoryHash);
    } else if (policy_ == kFastLocalBloom) {
      // For Bloom Filter + FullFilter case, since we design the num_key_ to
      // make hash entry cache charging be a multiple of dummy entries, the
      // correct behavior of charging final filter on top of it will trigger at
      // least another dummy entry insertion. Therefore we can assert that
      // behavior and we don't need a large number of keys to verify we
      // indeed charge the final filter for in cache, even though final
      // filter is a lot smaller than hash entries.
      num_key_ = 1 *
                 CacheReservationManagerImpl<
                     CacheEntryRole::kFilterConstruction>::GetDummyEntrySize() /
                 sizeof(FilterConstructionReserveMemoryHash);
    } else {
      // For Ribbon Filter + FullFilter case, we need a large enough number of
      // keys so that charging final filter after releasing the hash entries
      // reservation will trigger at least another dummy entry (or equivalently
      // to saying, causing another peak in cache charging) as banding
      // reservation might not be a multiple of dummy entry.
      num_key_ = 12 *
                 CacheReservationManagerImpl<
                     CacheEntryRole::kFilterConstruction>::GetDummyEntrySize() /
                 sizeof(FilterConstructionReserveMemoryHash);
    }
  }

  BlockBasedTableOptions GetBlockBasedTableOptions() {
    BlockBasedTableOptions table_options;
    SetInTableOptions(&table_options);

    // We set cache capacity big enough to prevent cache full for convenience in
    // calculation.
    constexpr std::size_t kCacheCapacity = 100 * 1024 * 1024;

    table_options.cache_usage_options.options_overrides.insert(
        {CacheEntryRole::kFilterConstruction,
         {/*.charged = */ charge_filter_construction_}});
    table_options.filter_policy = Create(10, policy_);
    if (table_options.partition_filters) {
      // We set table_options.metadata_block_size big enough so that each
      // partition trigger at least 1 dummy entry insertion each for hash
      // entries and final filter.
      table_options.metadata_block_size = 409000;
    }
    table_options.detect_filter_construct_corruption =
        detect_filter_construct_corruption_;

    LRUCacheOptions lo;
    lo.capacity = kCacheCapacity;
    lo.num_shard_bits = 0;  // 2^0 shard
    lo.strict_capacity_limit = true;
    cache_ = std::make_shared<
        TargetCacheChargeTrackingCache<CacheEntryRole::kFilterConstruction>>(
        (NewLRUCache(lo)));
    table_options.block_cache = cache_;

    return table_options;
  }

  std::size_t GetNumKey() { return num_key_; }

  CacheEntryRoleOptions::Decision ChargeFilterConstructMemory() {
    return charge_filter_construction_;
  }

  std::string GetFilterPolicy() { return policy_; }

  std::shared_ptr<
      TargetCacheChargeTrackingCache<CacheEntryRole::kFilterConstruction>>
  GetCache() {
    return cache_;
  }

 private:
  std::size_t num_key_;
  CacheEntryRoleOptions::Decision charge_filter_construction_;
  std::string policy_;
  std::shared_ptr<
      TargetCacheChargeTrackingCache<CacheEntryRole::kFilterConstruction>>
      cache_;
  bool detect_filter_construct_corruption_;
};

INSTANTIATE_TEST_CASE_P(
    ChargeFilterConstructionTestWithParam,
    ChargeFilterConstructionTestWithParam,
    ::testing::Values(
        std::make_tuple(CacheEntryRoleOptions::Decision::kDisabled,
                        kFastLocalBloom,
                        FilterPartitioning::kUnpartitionedFilter, false),

        std::make_tuple(CacheEntryRoleOptions::Decision::kEnabled,
                        kFastLocalBloom,
                        FilterPartitioning::kUnpartitionedFilter, false),
        std::make_tuple(CacheEntryRoleOptions::Decision::kEnabled,
                        kFastLocalBloom,
                        FilterPartitioning::kUnpartitionedFilter, true),
        std::make_tuple(CacheEntryRoleOptions::Decision::kEnabled,
                        kFastLocalBloom,
                        FilterPartitioning::kCoupledPartitionedFilter, false),
        std::make_tuple(CacheEntryRoleOptions::Decision::kEnabled,
                        kFastLocalBloom,
                        FilterPartitioning::kCoupledPartitionedFilter, true),
        std::make_tuple(CacheEntryRoleOptions::Decision::kEnabled,
                        kFastLocalBloom,
                        FilterPartitioning::kDecoupledPartitionedFilter, true),

        std::make_tuple(CacheEntryRoleOptions::Decision::kEnabled,
                        kStandard128Ribbon,
                        FilterPartitioning::kUnpartitionedFilter, false),
        std::make_tuple(CacheEntryRoleOptions::Decision::kEnabled,
                        kStandard128Ribbon,
                        FilterPartitioning::kUnpartitionedFilter, true),
        std::make_tuple(CacheEntryRoleOptions::Decision::kEnabled,
                        kStandard128Ribbon,
                        FilterPartitioning::kCoupledPartitionedFilter, false),
        std::make_tuple(CacheEntryRoleOptions::Decision::kEnabled,
                        kStandard128Ribbon,
                        FilterPartitioning::kCoupledPartitionedFilter, true),
        std::make_tuple(CacheEntryRoleOptions::Decision::kEnabled,
                        kStandard128Ribbon,
                        FilterPartitioning::kDecoupledPartitionedFilter, true),

        std::make_tuple(CacheEntryRoleOptions::Decision::kEnabled, kLegacyBloom,
                        FilterPartitioning::kUnpartitionedFilter, false)));

// TODO: Speed up this test, and reduce disk space usage (~700MB)
// The current test inserts many keys (on the scale of dummy entry size)
// in order to make small memory user (e.g, final filter, partitioned hash
// entries/filter/banding) , which is proportional to the number of
// keys, big enough so that its cache charging triggers dummy entry insertion
// and becomes observable in the test.
//
// However, inserting that many keys slows down this test and leaves future
// developers an opportunity to speed it up.
//
// Possible approaches & challenges:
// 1. Use sync point during cache charging of filter construction
//
// Benefit: It does not rely on triggering dummy entry insertion
// but the sync point to verify small memory user is charged correctly.
//
// Challenge: this approach is intrusive.
//
// 2. Make dummy entry size configurable and set it small in the test
//
// Benefit: It increases the precision of cache charging and therefore
// small memory usage can still trigger insertion of dummy entry.
//
// Challenge: change CacheReservationManager related APIs and a hack
// might be needed to control the size of dummmy entry of
// CacheReservationManager used in filter construction for testing
// since CacheReservationManager is not exposed at the high level.
//
TEST_P(ChargeFilterConstructionTestWithParam, Basic) {
  Options options = CurrentOptions();
  // We set write_buffer_size big enough so that in the case where there is
  // filter construction cache charging, flush won't be triggered before we
  // manually trigger it for clean testing
  options.write_buffer_size = 640 << 20;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  std::shared_ptr<
      TargetCacheChargeTrackingCache<CacheEntryRole::kFilterConstruction>>
      cache = GetCache();
  options.create_if_missing = true;
  // Disable auto compaction to prevent its unexpected side effect
  // to the number of keys per partition designed by us in the test
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  int num_key = static_cast<int>(GetNumKey());
  for (int i = 0; i < num_key; i++) {
    ASSERT_OK(Put(Key(i), Key(i)));
  }

  ASSERT_EQ(cache->GetChargedCacheIncrementSum(), 0)
      << "Flush was triggered too early in the test case with filter "
         "construction cache charging - please make sure no flush triggered "
         "during the key insertions above";

  ASSERT_OK(Flush());

  bool charge_filter_construction = (ChargeFilterConstructMemory() ==
                                     CacheEntryRoleOptions::Decision::kEnabled);
  std::string policy = GetFilterPolicy();
  bool detect_filter_construct_corruption =
      table_options.detect_filter_construct_corruption;

  std::deque<std::size_t> filter_construction_cache_res_peaks =
      cache->GetChargedCachePeaks();
  std::size_t filter_construction_cache_res_increments_sum =
      cache->GetChargedCacheIncrementSum();

  if (!charge_filter_construction) {
    EXPECT_EQ(filter_construction_cache_res_peaks.size(), 0);
    return;
  }

  if (policy == kLegacyBloom) {
    EXPECT_EQ(filter_construction_cache_res_peaks.size(), 0)
        << "There shouldn't be filter construction cache charging as this "
           "feature does not support kLegacyBloom";
    return;
  }

  const std::size_t kDummyEntrySize = CacheReservationManagerImpl<
      CacheEntryRole::kFilterConstruction>::GetDummyEntrySize();

  const std::size_t predicted_hash_entries_cache_res =
      num_key * sizeof(FilterConstructionReserveMemoryHash);
  ASSERT_EQ(predicted_hash_entries_cache_res % kDummyEntrySize, 0)
      << "It's by this test's design that predicted_hash_entries_cache_res is "
         "a multipe of dummy entry";

  const std::size_t predicted_hash_entries_cache_res_dummy_entry_num =
      predicted_hash_entries_cache_res / kDummyEntrySize;
  const std::size_t predicted_final_filter_cache_res =
      static_cast<std::size_t>(
          std::ceil(1.0 * predicted_hash_entries_cache_res_dummy_entry_num / 6 *
                    (policy == kStandard128Ribbon ? 0.7 : 1))) *
      kDummyEntrySize;
  const std::size_t predicted_banding_cache_res =
      static_cast<std::size_t>(
          std::ceil(predicted_hash_entries_cache_res_dummy_entry_num * 2.5)) *
      kDummyEntrySize;

  if (policy == kFastLocalBloom) {
    /* kFastLocalBloom + FullFilter
     *        p0
     *       /  \
     *    b /    \
     *     /      \
     *    /        \
     *  0/          \
     *  hash entries = b - 0, final filter = p0 - b
     *  p0 = hash entries + final filter
     *
     *  The test is designed in a way such that the reservation for b is a
     *  multiple of dummy entries so that reservation for (p0 - b)
     *  will trigger at least another dummy entry insertion.
     *
     * kFastLocalBloom + FullFilter +
     * detect_filter_construct_corruption
     *  The peak p0 stays the same as
     *  (kFastLocalBloom + FullFilter) but just lasts
     *  longer since we release hash entries reservation later.
     *
     * kFastLocalBloom + PartitionedFilter
     *                   p1
     *                  /  \
     *        p0     b'/    \
     *       /  \     /      \
     *    b /    \   /        \
     *     /      \ /          \
     *    /        a            \
     *  0/                       \
     *  partitioned hash entries1 = b - 0, partitioned hash entries1 = b' - a
     *  parittioned final filter1 = p0 - b, parittioned final filter2 = p1 - b'
     *
     *  (increment p0 - 0) + (increment p1 - a)
     *  = partitioned hash entries1 + partitioned hash entries2
     *  + parittioned final filter1 + parittioned final filter2
     *  = hash entries + final filter
     *
     * kFastLocalBloom + PartitionedFilter +
     * detect_filter_construct_corruption
     *  The peak p0, p1 stay the same as
     *  (kFastLocalBloom + PartitionedFilter) but just
     *  last longer since we release hash entries reservation later.
     *
     */
    if (!PartitionFilters()) {
      EXPECT_EQ(filter_construction_cache_res_peaks.size(), 1)
          << "Filter construction cache charging should have only 1 peak in "
             "case: kFastLocalBloom + FullFilter";
      std::size_t filter_construction_cache_res_peak =
          filter_construction_cache_res_peaks[0];
      EXPECT_GT(filter_construction_cache_res_peak,
                predicted_hash_entries_cache_res)
          << "The testing number of hash entries is designed to make hash "
             "entries cache charging be multiples of dummy entries"
             " so the correct behavior of charging final filter on top of it"
             " should've triggered at least another dummy entry insertion";

      std::size_t predicted_filter_construction_cache_res_peak =
          predicted_hash_entries_cache_res + predicted_final_filter_cache_res;
      EXPECT_GE(filter_construction_cache_res_peak,
                predicted_filter_construction_cache_res_peak * 0.9);
      EXPECT_LE(filter_construction_cache_res_peak,
                predicted_filter_construction_cache_res_peak * 1.1);
      return;
    } else {
      EXPECT_GE(filter_construction_cache_res_peaks.size(), 2)
          << "Filter construction cache charging should have multiple peaks "
             "in case: kFastLocalBloom + "
             "PartitionedFilter";
      std::size_t predicted_filter_construction_cache_res_increments_sum =
          predicted_hash_entries_cache_res + predicted_final_filter_cache_res;
      EXPECT_GE(filter_construction_cache_res_increments_sum,
                predicted_filter_construction_cache_res_increments_sum * 0.9);
      EXPECT_LE(filter_construction_cache_res_increments_sum,
                predicted_filter_construction_cache_res_increments_sum * 1.1);
      return;
    }
  }

  if (policy == kStandard128Ribbon) {
    /* kStandard128Ribbon + FullFilter
     *        p0
     *       /  \  p1
     *      /    \/\
     *   b /     b' \
     *    /          \
     *  0/            \
     *  hash entries = b - 0, banding = p0 - b, final filter = p1 - b'
     *  p0 = hash entries + banding
     *
     *  The test is designed in a way such that the reservation for (p1 - b')
     *  will trigger at least another dummy entry insertion
     *  (or equivalently to saying, creating another peak).
     *
     * kStandard128Ribbon + FullFilter +
     * detect_filter_construct_corruption
     *
     *         new p0
     *          /  \
     *         /    \
     *     pre p0    \
     *       /        \
     *      /          \
     *   b /            \
     *    /              \
     *  0/                \
     *  hash entries = b - 0, banding = pre p0 - b,
     *  final filter = new p0 - pre p0
     *  new p0 =  hash entries + banding + final filter
     *
     *  The previous p0 will no longer be a peak since under
     *  detect_filter_construct_corruption == true, we do not release hash
     *  entries reserveration (like p0 - b' previously) until after final filter
     *  creation and post-verification
     *
     * kStandard128Ribbon + PartitionedFilter
     *                     p3
     *        p0           /\  p4
     *       /  \ p1      /  \ /\
     *      /    \/\  b''/    a' \
     *   b /     b' \   /         \
     *    /          \ /           \
     *  0/            a             \
     *  partitioned hash entries1 = b - 0, partitioned hash entries2 = b'' - a
     *  partitioned banding1 = p0 - b, partitioned banding2 = p3 - b''
     *  parittioned final filter1 = p1 - b',parittioned final filter2 = p4 - a'
     *
     *  (increment p0 - 0) + (increment p1 - b')
     *  + (increment p3 - a) + (increment p4 - a')
     *  = partitioned hash entries1 + partitioned hash entries2
     *  + parittioned banding1 + parittioned banding2
     *  + parittioned final filter1 + parittioned final filter2
     *  = hash entries + banding + final filter
     *
     * kStandard128Ribbon + PartitionedFilter +
     * detect_filter_construct_corruption
     *
     *                          new p3
     *                          /    \
     *                        pre p3  \
     *        new p0          /        \
     *         /  \          /          \
     *      pre p0 \        /            \
     *       /      \    b'/              \
     *      /        \    /                \
     *   b /          \  /                  \
     *    /            \a                    \
     *  0/                                    \
     *  partitioned hash entries1 = b - 0, partitioned hash entries2 = b' - a
     *  partitioned banding1 = pre p0 - b, partitioned banding2 = pre p3 - b'
     *  parittioned final filter1 = new p0 - pre p0,
     *  parittioned final filter2 = new p3 - pre p3
     *
     *  The previous p0 and p3 will no longer be a peak since under
     *  detect_filter_construct_corruption == true, we do not release hash
     *  entries reserveration (like p0 - b', p3 - a' previously) until after
     *  parittioned final filter creation and post-verification
     *
     *  However, increments sum stay the same as shown below:
     *    (increment new p0 - 0) + (increment new p3 - a)
     *  = partitioned hash entries1 + partitioned hash entries2
     *  + parittioned banding1 + parittioned banding2
     *  + parittioned final filter1 + parittioned final filter2
     *  = hash entries + banding + final filter
     *
     */
    if (!PartitionFilters()) {
      ASSERT_GE(
          std::floor(
              1.0 * predicted_final_filter_cache_res /
              CacheReservationManagerImpl<
                  CacheEntryRole::kFilterConstruction>::GetDummyEntrySize()),
          1)
          << "Final filter cache charging too small for this test - please "
             "increase the number of keys";
      if (!detect_filter_construct_corruption) {
        EXPECT_EQ(filter_construction_cache_res_peaks.size(), 2)
            << "Filter construction cache charging should have 2 peaks in "
               "case: kStandard128Ribbon + "
               "FullFilter. "
               "The second peak is resulted from charging the final filter "
               "after "
               "decreasing the hash entry reservation since the testing final "
               "filter reservation is designed to be at least 1 dummy entry "
               "size";

        std::size_t filter_construction_cache_res_peak =
            filter_construction_cache_res_peaks[0];
        std::size_t predicted_filter_construction_cache_res_peak =
            predicted_hash_entries_cache_res + predicted_banding_cache_res;
        EXPECT_GE(filter_construction_cache_res_peak,
                  predicted_filter_construction_cache_res_peak * 0.9);
        EXPECT_LE(filter_construction_cache_res_peak,
                  predicted_filter_construction_cache_res_peak * 1.1);
      } else {
        EXPECT_EQ(filter_construction_cache_res_peaks.size(), 1)
            << "Filter construction cache charging should have 1 peaks in "
               "case: kStandard128Ribbon + FullFilter "
               "+ detect_filter_construct_corruption. "
               "The previous second peak now disappears since we don't "
               "decrease the hash entry reservation"
               "until after final filter reservation and post-verification";

        std::size_t filter_construction_cache_res_peak =
            filter_construction_cache_res_peaks[0];
        std::size_t predicted_filter_construction_cache_res_peak =
            predicted_hash_entries_cache_res + predicted_banding_cache_res +
            predicted_final_filter_cache_res;
        EXPECT_GE(filter_construction_cache_res_peak,
                  predicted_filter_construction_cache_res_peak * 0.9);
        EXPECT_LE(filter_construction_cache_res_peak,
                  predicted_filter_construction_cache_res_peak * 1.1);
      }
      return;
    } else {
      if (!detect_filter_construct_corruption) {
        EXPECT_GE(filter_construction_cache_res_peaks.size(), 3)
            << "Filter construction cache charging should have more than 3 "
               "peaks "
               "in case: kStandard128Ribbon + "
               "PartitionedFilter";
      } else {
        EXPECT_GE(filter_construction_cache_res_peaks.size(), 2)
            << "Filter construction cache charging should have more than 2 "
               "peaks "
               "in case: kStandard128Ribbon + "
               "PartitionedFilter + detect_filter_construct_corruption";
      }
      std::size_t predicted_filter_construction_cache_res_increments_sum =
          predicted_hash_entries_cache_res + predicted_banding_cache_res +
          predicted_final_filter_cache_res;
      EXPECT_GE(filter_construction_cache_res_increments_sum,
                predicted_filter_construction_cache_res_increments_sum * 0.9);
      EXPECT_LE(filter_construction_cache_res_increments_sum,
                predicted_filter_construction_cache_res_increments_sum * 1.1);
      return;
    }
  }
}

class DBFilterConstructionCorruptionTestWithParam
    : public DBBloomFilterTest,
      public testing::WithParamInterface<
          std::tuple<bool /* detect_filter_construct_corruption */, std::string,
                     FilterPartitioning>> {
 public:
  void SetInTableOptions(BlockBasedTableOptions* table_options) {
    table_options->detect_filter_construct_corruption = std::get<0>(GetParam());
    table_options->filter_policy = Create(10, std::get<1>(GetParam()));
    filter_partitioning_ = std::get<2>(GetParam());
    DBBloomFilterTest::SetInTableOptions(table_options);
    if (PartitionFilters()) {
      // We set table_options.metadata_block_size small enough so we can
      // trigger filter partitioning with GetNumKey() amount of keys
      table_options->metadata_block_size = 10;
    }
  }

  // Return an appropriate amount of keys for testing
  // to generate a long filter (i.e, size >= 8 + kMetadataLen)
  std::size_t GetNumKey() { return 5000; }
};

INSTANTIATE_TEST_CASE_P(
    DBFilterConstructionCorruptionTestWithParam,
    DBFilterConstructionCorruptionTestWithParam,
    ::testing::Values(
        std::make_tuple(false, kFastLocalBloom,
                        FilterPartitioning::kUnpartitionedFilter),
        std::make_tuple(true, kFastLocalBloom,
                        FilterPartitioning::kUnpartitionedFilter),
        std::make_tuple(true, kFastLocalBloom,
                        FilterPartitioning::kCoupledPartitionedFilter),
        std::make_tuple(true, kFastLocalBloom,
                        FilterPartitioning::kDecoupledPartitionedFilter),
        std::make_tuple(true, kStandard128Ribbon,
                        FilterPartitioning::kUnpartitionedFilter),
        std::make_tuple(true, kStandard128Ribbon,
                        FilterPartitioning::kCoupledPartitionedFilter),
        std::make_tuple(true, kStandard128Ribbon,
                        FilterPartitioning::kDecoupledPartitionedFilter)));

TEST_P(DBFilterConstructionCorruptionTestWithParam, DetectCorruption) {
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  SetInTableOptions(&table_options);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.create_if_missing = true;
  options.disable_auto_compactions = true;

  DestroyAndReopen(options);
  int num_key = static_cast<int>(GetNumKey());
  Status s;

  // Case 1: No corruption in filter construction
  for (int i = 0; i < num_key; i++) {
    ASSERT_OK(Put(Key(i), Key(i)));
  }
  s = Flush();
  EXPECT_TRUE(s.ok());

  // Case 2: Corruption of hash entries in filter construction
  for (int i = 0; i < num_key; i++) {
    ASSERT_OK(Put(Key(i), Key(i)));
  }

  SyncPoint::GetInstance()->SetCallBack(
      "XXPH3FilterBitsBuilder::Finish::TamperHashEntries", [&](void* arg) {
        std::deque<uint64_t>* hash_entries_to_corrupt =
            (std::deque<uint64_t>*)arg;
        assert(!hash_entries_to_corrupt->empty());
        *(hash_entries_to_corrupt->begin()) =
            *(hash_entries_to_corrupt->begin()) ^ uint64_t { 1 };
      });
  SyncPoint::GetInstance()->EnableProcessing();

  s = Flush();

  if (table_options.detect_filter_construct_corruption) {
    EXPECT_TRUE(s.IsCorruption());
    EXPECT_TRUE(
        s.ToString().find("Filter's hash entries checksum mismatched") !=
        std::string::npos);
  } else {
    EXPECT_TRUE(s.ok());
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearCallBack(
      "XXPH3FilterBitsBuilder::Finish::"
      "TamperHashEntries");

  // Case 3: Corruption of filter content in filter construction
  DestroyAndReopen(options);

  for (int i = 0; i < num_key; i++) {
    ASSERT_OK(Put(Key(i), Key(i)));
  }

  SyncPoint::GetInstance()->SetCallBack(
      "XXPH3FilterBitsBuilder::Finish::TamperFilter", [&](void* arg) {
        std::pair<std::unique_ptr<char[]>*, std::size_t>* TEST_arg_pair =
            (std::pair<std::unique_ptr<char[]>*, std::size_t>*)arg;
        std::size_t filter_size = TEST_arg_pair->second;
        // 5 is the kMetadataLen and
        assert(filter_size >= 8 + 5);
        std::unique_ptr<char[]>* filter_content_to_corrupt =
            TEST_arg_pair->first;
        std::memset(filter_content_to_corrupt->get(), '\0', 8);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  s = Flush();

  if (table_options.detect_filter_construct_corruption) {
    EXPECT_TRUE(s.IsCorruption());
    EXPECT_TRUE(s.ToString().find("Corrupted filter content") !=
                std::string::npos);
  } else {
    EXPECT_TRUE(s.ok());
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearCallBack(
      "XXPH3FilterBitsBuilder::Finish::"
      "TamperFilter");
}

// RocksDB lite does not support dynamic options
TEST_P(DBFilterConstructionCorruptionTestWithParam,
       DynamicallyTurnOnAndOffDetectConstructCorruption) {
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  SetInTableOptions(&table_options);
  // We intend to turn on
  // table_options.detect_filter_construct_corruption dynamically
  // therefore we override this test parmater's value
  table_options.detect_filter_construct_corruption = false;

  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.create_if_missing = true;

  int num_key = static_cast<int>(GetNumKey());
  Status s;

  DestroyAndReopen(options);

  // Case 1: !table_options.detect_filter_construct_corruption
  for (int i = 0; i < num_key; i++) {
    ASSERT_OK(Put(Key(i), Key(i)));
  }

  SyncPoint::GetInstance()->SetCallBack(
      "XXPH3FilterBitsBuilder::Finish::TamperHashEntries", [&](void* arg) {
        std::deque<uint64_t>* hash_entries_to_corrupt =
            (std::deque<uint64_t>*)arg;
        assert(!hash_entries_to_corrupt->empty());
        *(hash_entries_to_corrupt->begin()) =
            *(hash_entries_to_corrupt->begin()) ^ uint64_t { 1 };
      });
  SyncPoint::GetInstance()->EnableProcessing();

  s = Flush();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearCallBack(
      "XXPH3FilterBitsBuilder::Finish::"
      "TamperHashEntries");

  ASSERT_FALSE(table_options.detect_filter_construct_corruption);
  EXPECT_TRUE(s.ok());

  // Case 2: dynamically turn on
  // table_options.detect_filter_construct_corruption
  ASSERT_OK(db_->SetOptions({{"block_based_table_factory",
                              "{detect_filter_construct_corruption=true;}"}}));

  for (int i = 0; i < num_key; i++) {
    ASSERT_OK(Put(Key(i), Key(i)));
  }

  SyncPoint::GetInstance()->SetCallBack(
      "XXPH3FilterBitsBuilder::Finish::TamperHashEntries", [&](void* arg) {
        std::deque<uint64_t>* hash_entries_to_corrupt =
            (std::deque<uint64_t>*)arg;
        assert(!hash_entries_to_corrupt->empty());
        *(hash_entries_to_corrupt->begin()) =
            *(hash_entries_to_corrupt->begin()) ^ uint64_t { 1 };
      });
  SyncPoint::GetInstance()->EnableProcessing();

  s = Flush();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearCallBack(
      "XXPH3FilterBitsBuilder::Finish::"
      "TamperHashEntries");

  auto updated_table_options =
      db_->GetOptions().table_factory->GetOptions<BlockBasedTableOptions>();
  EXPECT_TRUE(updated_table_options->detect_filter_construct_corruption);
  EXPECT_TRUE(s.IsCorruption());
  EXPECT_TRUE(s.ToString().find("Filter's hash entries checksum mismatched") !=
              std::string::npos);

  // Case 3: dynamically turn off
  // table_options.detect_filter_construct_corruption
  ASSERT_OK(db_->SetOptions({{"block_based_table_factory",
                              "{detect_filter_construct_corruption=false;}"}}));
  updated_table_options =
      db_->GetOptions().table_factory->GetOptions<BlockBasedTableOptions>();
  EXPECT_FALSE(updated_table_options->detect_filter_construct_corruption);
}

namespace {
// NOTE: This class is referenced by HISTORY.md as a model for a wrapper
// FilterPolicy selecting among configurations based on context.
class LevelAndStyleCustomFilterPolicy : public FilterPolicy {
 public:
  explicit LevelAndStyleCustomFilterPolicy(int bpk_fifo, int bpk_l0_other,
                                           int bpk_otherwise)
      : policy_fifo_(NewBloomFilterPolicy(bpk_fifo)),
        policy_l0_other_(NewBloomFilterPolicy(bpk_l0_other)),
        policy_otherwise_(NewBloomFilterPolicy(bpk_otherwise)) {}

  const char* Name() const override {
    return "LevelAndStyleCustomFilterPolicy";
  }

  // OK to use built-in policy name because we are deferring to a
  // built-in builder. We aren't changing the serialized format.
  const char* CompatibilityName() const override {
    return policy_fifo_->CompatibilityName();
  }

  FilterBitsBuilder* GetBuilderWithContext(
      const FilterBuildingContext& context) const override {
    if (context.compaction_style == kCompactionStyleFIFO) {
      return policy_fifo_->GetBuilderWithContext(context);
    } else if (context.level_at_creation == 0) {
      return policy_l0_other_->GetBuilderWithContext(context);
    } else {
      return policy_otherwise_->GetBuilderWithContext(context);
    }
  }

  FilterBitsReader* GetFilterBitsReader(const Slice& contents) const override {
    // OK to defer to any of them; they all can parse built-in filters
    // from any settings.
    return policy_fifo_->GetFilterBitsReader(contents);
  }

 private:
  const std::unique_ptr<const FilterPolicy> policy_fifo_;
  const std::unique_ptr<const FilterPolicy> policy_l0_other_;
  const std::unique_ptr<const FilterPolicy> policy_otherwise_;
};

static std::map<TableFileCreationReason, std::string>
    table_file_creation_reason_to_string{
        {TableFileCreationReason::kCompaction, "kCompaction"},
        {TableFileCreationReason::kFlush, "kFlush"},
        {TableFileCreationReason::kMisc, "kMisc"},
        {TableFileCreationReason::kRecovery, "kRecovery"},
        {TableFileCreationReason::kSstFileWriter, "kSstFileWriter"},
    };

class TestingContextCustomFilterPolicy
    : public LevelAndStyleCustomFilterPolicy {
 public:
  explicit TestingContextCustomFilterPolicy(int bpk_fifo, int bpk_l0_other,
                                            int bpk_otherwise)
      : LevelAndStyleCustomFilterPolicy(bpk_fifo, bpk_l0_other, bpk_otherwise) {
  }

  FilterBitsBuilder* GetBuilderWithContext(
      const FilterBuildingContext& context) const override {
    test_report_ += "cf=";
    test_report_ += context.column_family_name;
    // Token rather than path, so expectations don't depend on test directory
    test_report_ += ",db=";
    if (context.db_name.empty()) {
      test_report_ += "none";
    } else if (context.db_name == Slice(expected_db_name_)) {
      test_report_ += "expected";
    } else {
      test_report_ += "UNEXPECTED:" + context.db_name.ToString();
    }
    test_report_ += ",s=";
    test_report_ +=
        OptionsHelper::compaction_style_to_string[context.compaction_style];
    test_report_ += ",n=";
    test_report_ += std::to_string(context.num_levels);
    test_report_ += ",l=";
    test_report_ += std::to_string(context.level_at_creation);
    test_report_ += ",b=";
    test_report_ += std::to_string(int{context.is_bottommost});
    test_report_ += ",r=";
    test_report_ += table_file_creation_reason_to_string[context.reason];
    test_report_ += "\n";

    return LevelAndStyleCustomFilterPolicy::GetBuilderWithContext(context);
  }

  std::string DumpTestReport() {
    std::string rv;
    std::swap(rv, test_report_);
    return rv;
  }

  void SetExpectedDbName(std::string db_name) {
    expected_db_name_ = std::move(db_name);
  }

 private:
  mutable std::string test_report_;
  std::string expected_db_name_;
};
}  // anonymous namespace

TEST_F(DBBloomFilterTest, ContextCustomFilterPolicy) {
  auto policy = std::make_shared<TestingContextCustomFilterPolicy>(15, 8, 5);
  Options options;
  for (bool fifo : {true, false}) {
    options = CurrentOptions();
    options.max_open_files = fifo ? -1 : options.max_open_files;
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    options.compaction_style =
        fifo ? kCompactionStyleFIFO : kCompactionStyleLevel;

    BlockBasedTableOptions table_options;
    table_options.filter_policy = policy;
    table_options.format_version = 5;
    table_options.optimize_filters_for_memory = false;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    ASSERT_OK(TryReopen(options));
    CreateAndReopenWithCF({fifo ? "abe" : "bob"}, options);
    policy->SetExpectedDbName(dbname_);

    const char* expected_flush_report =
        fifo
            ? "cf=abe,db=expected,s=kCompactionStyleFIFO,n=7,l=0,b=0,r=kFlush\n"
            : "cf=bob,db=expected,s=kCompactionStyleLevel,n=7,l=0,b=0,r="
              "kFlush\n";

    const int maxKey = 10000;
    for (int i = 0; i < maxKey / 2; i++) {
      ASSERT_OK(Put(1, Key(i), Key(i)));
    }
    // Add a large key to make the file contain wide range
    ASSERT_OK(Put(1, Key(maxKey + 55555), Key(maxKey + 55555)));
    ASSERT_OK(Flush(1));
    EXPECT_EQ(policy->DumpTestReport(), expected_flush_report);

    for (int i = maxKey / 2; i < maxKey; i++) {
      ASSERT_OK(Put(1, Key(i), Key(i)));
    }
    ASSERT_OK(Flush(1));
    EXPECT_EQ(policy->DumpTestReport(), expected_flush_report);

    // Check that they can be found
    for (int i = 0; i < maxKey; i++) {
      ASSERT_EQ(Key(i), Get(1, Key(i)));
    }
    // Since we have two tables / two filters, we might have Bloom checks on
    // our queries, but no more than one "useful" per query on a found key.
    EXPECT_LE(PopTicker(options, BLOOM_FILTER_USEFUL), maxKey);

    // Check that we have two filters, each about
    // fifo: 0.12% FP rate (15 bits per key)
    // level: 2.3% FP rate (8 bits per key)
    for (int i = 0; i < maxKey; i++) {
      ASSERT_EQ("NOT_FOUND", Get(1, Key(i + 33333)));
    }
    {
      auto useful_count = PopTicker(options, BLOOM_FILTER_USEFUL);
      EXPECT_GE(useful_count, maxKey * 2 * (fifo ? 0.9980 : 0.975));
      EXPECT_LE(useful_count, maxKey * 2 * (fifo ? 0.9995 : 0.98));
    }

    if (!fifo) {  // FIFO doesn't fully support CompactRange
      // Full compaction
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                                  nullptr));
      EXPECT_EQ(policy->DumpTestReport(),
                "cf=bob,db=expected,s=kCompactionStyleLevel,n=7,l=1,b=1,r="
                "kCompaction\n");

      // Check that we now have one filter, about 9.2% FP rate (5 bits per key)
      for (int i = 0; i < maxKey; i++) {
        ASSERT_EQ("NOT_FOUND", Get(1, Key(i + 33333)));
      }
      {
        auto useful_count = PopTicker(options, BLOOM_FILTER_USEFUL);
        EXPECT_GE(useful_count, maxKey * 0.90);
        EXPECT_LE(useful_count, maxKey * 0.91);
      }
    } else {
      // Also try external SST file
      {
        std::string file_path = dbname_ + "/external.sst";
        SstFileWriter sst_file_writer(EnvOptions(), options, handles_[1]);
        ASSERT_OK(sst_file_writer.Open(file_path));
        ASSERT_OK(sst_file_writer.Put("key", "value"));
        ASSERT_OK(sst_file_writer.Finish());
      }
      // Note: kCompactionStyleLevel is default, ignored if num_levels == -1
      EXPECT_EQ(policy->DumpTestReport(),
                "cf=abe,db=none,s=kCompactionStyleLevel,n=-1,l=-1,b=0,r="
                "kSstFileWriter\n");
    }

    // Destroy
    ASSERT_OK(dbfull()->DropColumnFamily(handles_[1]));
    ASSERT_OK(dbfull()->DestroyColumnFamilyHandle(handles_[1]));
    handles_[1] = nullptr;
  }
}

TEST_F(DBBloomFilterTest, MutatingRibbonFilterPolicy) {
  // Test that RibbonFilterPolicy has a mutable bloom_before_level fields that
  // can be updated through SetOptions

  Options options = CurrentOptions();
  options.statistics = CreateDBStatistics();
  auto& stats = *options.statistics;
  BlockBasedTableOptions table_options;
  // First config forces Bloom filter, to establish a baseline before
  // SetOptions().
  table_options.filter_policy.reset(NewRibbonFilterPolicy(10, INT_MAX));
  double expected_bpk = 10.0;
  // Other configs to try, with approx expected bits per key
  std::vector<std::pair<std::string, double>> configs = {{"-1", 7.0},
                                                         {"0", 10.0}};

  table_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  ASSERT_OK(TryReopen(options));

  char v[] = "a";

  for (;; ++(v[0])) {
    const int maxKey = 8000;
    for (int i = 0; i < maxKey; i++) {
      ASSERT_OK(Put(Key(i), v));
    }
    ASSERT_OK(Flush());

    for (int i = 0; i < maxKey; i++) {
      ASSERT_EQ(Get(Key(i)), v);
    }

    uint64_t filter_bytes =
        stats.getAndResetTickerCount(BLOCK_CACHE_FILTER_BYTES_INSERT);

    EXPECT_NEAR(filter_bytes * 8.0 / maxKey, expected_bpk, 0.3);

    if (configs.empty()) {
      break;
    }
    std::string factory_field =
        (v[0] & 1) ? "table_factory" : "block_based_table_factory";

    // Some irrelevant SetOptions to be sure they don't interfere
    ASSERT_OK(db_->SetOptions({{"level0_file_num_compaction_trigger", "10"}}));
    ASSERT_OK(
        db_->SetOptions({{"block_based_table_factory", "{block_size=1234}"}}));
    ASSERT_OK(db_->SetOptions({{factory_field + ".block_size", "12345"}}));

    // Test the mutable field we're interested in
    ASSERT_OK(
        db_->SetOptions({{factory_field + ".filter_policy.bloom_before_level",
                          configs.back().first}}));
    // FilterPolicy pointer should not have changed
    ASSERT_EQ(db_->GetOptions()
                  .table_factory->GetOptions<BlockBasedTableOptions>()
                  ->filter_policy.get(),
              table_options.filter_policy.get());

    // Ensure original object is mutated
    std::string val;
    ASSERT_OK(
        table_options.filter_policy->GetOption({}, "bloom_before_level", &val));
    ASSERT_EQ(configs.back().first, val);

    expected_bpk = configs.back().second;
    configs.pop_back();
  }
}

TEST_F(DBBloomFilterTest, MutableFilterPolicy) {
  // Test that BlockBasedTableOptions::filter_policy is mutable (replaceable)
  // with SetOptions.

  Options options = CurrentOptions();
  options.statistics = CreateDBStatistics();
  auto& stats = *options.statistics;
  BlockBasedTableOptions table_options;
  // First config, to make sure there's no issues with this shared ptr
  // etc. when the DB switches filter policies.
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  double expected_bpk = 10.0;
  // Other configs to try
  std::vector<std::pair<std::string, double>> configs = {
      {"ribbonfilter:10:-1", 7.0},
      {"bloomfilter:5", 5.0},
      {"nullptr", 0.0},
      // As serialized in OPTIONS file
      {"{id=ribbonfilter:12:-1;bloom_before_level=-1;}", 8.4}};

  table_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.level0_file_num_compaction_trigger =
      static_cast<int>(configs.size()) + 2;

  ASSERT_OK(TryReopen(options));

  char v[] = "a";

  for (;; ++(v[0])) {
    const int maxKey = 8000;
    for (int i = 0; i < maxKey; i++) {
      ASSERT_OK(Put(Key(i), v));
    }
    ASSERT_OK(Flush());

    for (int i = 0; i < maxKey; i++) {
      ASSERT_EQ(Get(Key(i)), v);
    }

    uint64_t filter_bytes =
        stats.getAndResetTickerCount(BLOCK_CACHE_FILTER_BYTES_INSERT);

    EXPECT_NEAR(filter_bytes * 8.0 / maxKey, expected_bpk, 0.3);

    if (configs.empty()) {
      break;
    }

    ASSERT_OK(
        db_->SetOptions({{"block_based_table_factory",
                          "{filter_policy=" + configs.back().first + "}"}}));
    expected_bpk = configs.back().second;
    configs.pop_back();
  }
}

class SliceTransformLimitedDomain : public SliceTransform {
  const char* Name() const override { return "SliceTransformLimitedDomain"; }

  Slice Transform(const Slice& src) const override {
    return Slice(src.data(), 5);
  }

  bool InDomain(const Slice& src) const override {
    // prefix will be x????
    return src.size() >= 5 && src[0] == 'x';
  }
};

TEST_F(DBBloomFilterTest, PrefixExtractorWithFilter1) {
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(ROCKSDB_NAMESPACE::NewBloomFilterPolicy(10));
  bbto.whole_key_filtering = false;

  Options options = CurrentOptions();
  options.prefix_extractor = std::make_shared<SliceTransformLimitedDomain>();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  DestroyAndReopen(options);

  ASSERT_OK(Put("x1111_AAAA", "val1"));
  ASSERT_OK(Put("x1112_AAAA", "val2"));
  ASSERT_OK(Put("x1113_AAAA", "val3"));
  ASSERT_OK(Put("x1114_AAAA", "val4"));
  // Not in domain, wont be added to filter
  ASSERT_OK(Put("zzzzz_AAAA", "val5"));

  ASSERT_OK(Flush());

  ASSERT_EQ(Get("x1111_AAAA"), "val1");
  ASSERT_EQ(Get("x1112_AAAA"), "val2");
  ASSERT_EQ(Get("x1113_AAAA"), "val3");
  ASSERT_EQ(Get("x1114_AAAA"), "val4");
  // Was not added to filter but rocksdb will try to read it from the filter
  ASSERT_EQ(Get("zzzzz_AAAA"), "val5");
}

TEST_F(DBBloomFilterTest, PrefixExtractorWithFilter2) {
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(ROCKSDB_NAMESPACE::NewBloomFilterPolicy(10));

  Options options = CurrentOptions();
  options.prefix_extractor = std::make_shared<SliceTransformLimitedDomain>();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  DestroyAndReopen(options);

  ASSERT_OK(Put("x1113_AAAA", "val3"));
  ASSERT_OK(Put("x1114_AAAA", "val4"));
  // Not in domain, wont be added to filter
  ASSERT_OK(Put("zzzzz_AAAA", "val1"));
  ASSERT_OK(Put("zzzzz_AAAB", "val2"));
  ASSERT_OK(Put("zzzzz_AAAC", "val3"));
  ASSERT_OK(Put("zzzzz_AAAD", "val4"));

  ASSERT_OK(Flush());

  std::vector<std::string> iter_res;
  auto iter = db_->NewIterator(ReadOptions());
  // Seek to a key that was not in Domain
  for (iter->Seek("zzzzz_AAAA"); iter->Valid(); iter->Next()) {
    iter_res.emplace_back(iter->value().ToString());
  }
  ASSERT_OK(iter->status());

  std::vector<std::string> expected_res = {"val1", "val2", "val3", "val4"};
  ASSERT_EQ(iter_res, expected_res);
  delete iter;
}

TEST_F(DBBloomFilterTest, MemtableWholeKeyBloomFilter) {
  // regression test for #2743. the range delete tombstones in memtable should
  // be added even when Get() skips searching due to its prefix bloom filter
  const int kMemtableSize = 1 << 20;              // 1MB
  const int kMemtablePrefixFilterSize = 1 << 13;  // 8KB
  const int kPrefixLen = 4;
  Options options = CurrentOptions();
  options.memtable_prefix_bloom_size_ratio =
      static_cast<double>(kMemtablePrefixFilterSize) / kMemtableSize;
  options.prefix_extractor.reset(
      ROCKSDB_NAMESPACE::NewFixedPrefixTransform(kPrefixLen));
  options.write_buffer_size = kMemtableSize;
  options.memtable_whole_key_filtering = false;
  Reopen(options);
  std::string key1("AAAABBBB");
  std::string key2("AAAACCCC");  // not in DB
  std::string key3("AAAADDDD");
  std::string key4("AAAAEEEE");
  std::string value1("Value1");
  std::string value3("Value3");
  std::string value4("Value4");

  ASSERT_OK(Put(key1, value1, WriteOptions()));

  // check memtable bloom stats
  ASSERT_EQ("NOT_FOUND", Get(key2));
  ASSERT_EQ(0, get_perf_context()->bloom_memtable_miss_count);
  // same prefix, bloom filter false positive
  ASSERT_EQ(1, get_perf_context()->bloom_memtable_hit_count);

  // enable whole key bloom filter
  options.memtable_whole_key_filtering = true;
  Reopen(options);
  // check memtable bloom stats
  ASSERT_OK(Put(key3, value3, WriteOptions()));
  ASSERT_EQ("NOT_FOUND", Get(key2));
  // whole key bloom filter kicks in and determines it's a miss
  ASSERT_EQ(1, get_perf_context()->bloom_memtable_miss_count);
  ASSERT_EQ(1, get_perf_context()->bloom_memtable_hit_count);

  // verify whole key filtering does not depend on prefix_extractor
  options.prefix_extractor.reset();
  Reopen(options);
  // check memtable bloom stats
  ASSERT_OK(Put(key4, value4, WriteOptions()));
  ASSERT_EQ("NOT_FOUND", Get(key2));
  // whole key bloom filter kicks in and determines it's a miss
  ASSERT_EQ(2, get_perf_context()->bloom_memtable_miss_count);
  ASSERT_EQ(1, get_perf_context()->bloom_memtable_hit_count);
}

TEST_F(DBBloomFilterTest, MemtableWholeKeyBloomFilterMultiGet) {
  Options options = CurrentOptions();
  options.memtable_prefix_bloom_size_ratio = 0.015;
  options.memtable_whole_key_filtering = true;
  Reopen(options);
  std::string key1("AA");
  std::string key2("BB");
  std::string key3("CC");
  std::string key4("DD");
  std::string key_not("EE");
  std::string value1("Value1");
  std::string value2("Value2");
  std::string value3("Value3");
  std::string value4("Value4");

  ASSERT_OK(Put(key1, value1, WriteOptions()));
  ASSERT_OK(Put(key2, value2, WriteOptions()));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(key3, value3, WriteOptions()));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(Put(key4, value4, WriteOptions()));

  // Delete key2 and key3
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "BA", "CZ"));

  // Read without snapshot
  auto results = MultiGet({key_not, key1, key2, key3, key4});
  ASSERT_EQ(results[0], "NOT_FOUND");
  ASSERT_EQ(results[1], value1);
  ASSERT_EQ(results[2], "NOT_FOUND");
  ASSERT_EQ(results[3], "NOT_FOUND");
  ASSERT_EQ(results[4], value4);

  // Also check Get
  ASSERT_EQ(Get(key1), value1);
  ASSERT_EQ(Get(key2), "NOT_FOUND");
  ASSERT_EQ(Get(key3), "NOT_FOUND");
  ASSERT_EQ(Get(key4), value4);

  // Read with snapshot
  results = MultiGet({key_not, key1, key2, key3, key4}, snapshot);
  ASSERT_EQ(results[0], "NOT_FOUND");
  ASSERT_EQ(results[1], value1);
  ASSERT_EQ(results[2], value2);
  ASSERT_EQ(results[3], value3);
  ASSERT_EQ(results[4], "NOT_FOUND");

  // Also check Get
  ASSERT_EQ(Get(key1, snapshot), value1);
  ASSERT_EQ(Get(key2, snapshot), value2);
  ASSERT_EQ(Get(key3, snapshot), value3);
  ASSERT_EQ(Get(key4, snapshot), "NOT_FOUND");

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBBloomFilterTest, TestMemtableBloomAndWBM) {
  Options options = CurrentOptions();
  options.arena_block_size = 4096;
  options.write_buffer_size = 4000000;
  std::shared_ptr<Cache> cache = NewLRUCache(LRUCacheOptions(
      options.write_buffer_size * 3 /* capacity */, 1 /* num_shard_bits */,
      false /* strict_capacity_limit */, 0.0 /* high_pri_pool_ratio */,
      nullptr /* memory_allocator */, kDefaultToAdaptiveMutex,
      kDontChargeCacheMetadata));

  options.write_buffer_manager.reset(
      new WriteBufferManager(options.write_buffer_size, cache));
  Reopen(options);
  ASSERT_OK(Put("foo", "bar"));

  const auto kDummyEntrySize =
      CacheReservationManagerImpl<CacheEntryRole::kMisc>::GetDummyEntrySize();

  // Just the start of a memtable, no Bloom
  ASSERT_GE(cache->GetUsage(), options.arena_block_size);
  ASSERT_LE(cache->GetUsage(), kDummyEntrySize);

  // Now enable memtable bloom filter
  const double kRatio = 0.25;
  options.memtable_prefix_bloom_size_ratio = kRatio;
  options.memtable_whole_key_filtering = true;
  Reopen(options);
  ASSERT_OK(Put("foo2", "bar2"));

  // Expecting a memtable Bloom of ratio times write_buffer_size, memory tracked
  auto bloom_size = static_cast<size_t>(kRatio * options.write_buffer_size);
  ASSERT_GE(cache->GetUsage(), bloom_size);
  ASSERT_LE(cache->GetUsage(), bloom_size + 2U * kDummyEntrySize);

  // Now get another memtable and test
  // Pin this memtable with an iterator
  std::unique_ptr<Iterator> iter{db_->NewIterator({})};
  iter->Seek("foo2");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo2", "bar2"));

  // Expecting twice as much
  ASSERT_GE(cache->GetUsage(), 2U * bloom_size);
  ASSERT_LE(cache->GetUsage(), 2U * bloom_size + 2U * kDummyEntrySize);
}

namespace {
std::pair<uint64_t, uint64_t> GetBloomStat(const Options& options, bool sst) {
  if (sst) {
    return {options.statistics->getAndResetTickerCount(
                NON_LAST_LEVEL_SEEK_FILTER_MATCH),
            options.statistics->getAndResetTickerCount(
                NON_LAST_LEVEL_SEEK_FILTERED)};
  } else {
    auto hit = std::exchange(get_perf_context()->bloom_memtable_hit_count, 0);
    auto miss = std::exchange(get_perf_context()->bloom_memtable_miss_count, 0);
    return {hit, miss};
  }
}

std::pair<uint64_t, uint64_t> HitAndMiss(uint64_t hits, uint64_t misses) {
  return {hits, misses};
}
}  // namespace

TEST_F(DBBloomFilterTest, MemtablePrefixBloom) {
  Options options = CurrentOptions();
  options.prefix_extractor.reset(NewFixedPrefixTransform(4));
  options.memtable_prefix_bloom_size_ratio = 0.25;
  Reopen(options);
  ASSERT_FALSE(options.prefix_extractor->InDomain("key"));
  ASSERT_OK(Put("key", "v"));
  ASSERT_OK(Put("goat1", "g1"));
  ASSERT_OK(Put("goat2", "g2"));

  // Reset from other tests
  GetBloomStat(options, false);

  // Out of domain (Get)
  ASSERT_EQ("v", Get("key"));
  ASSERT_EQ(HitAndMiss(0, 0), GetBloomStat(options, false));

  // In domain (Get)
  ASSERT_EQ("g1", Get("goat1"));
  ASSERT_EQ(HitAndMiss(1, 0), GetBloomStat(options, false));
  ASSERT_EQ("NOT_FOUND", Get("goat9"));
  ASSERT_EQ(HitAndMiss(1, 0), GetBloomStat(options, false));
  ASSERT_EQ("NOT_FOUND", Get("goan1"));
  ASSERT_EQ(HitAndMiss(0, 1), GetBloomStat(options, false));

  ReadOptions ropts;
  if (options.prefix_seek_opt_in_only) {
    ropts.prefix_same_as_start = true;
  }
  std::unique_ptr<Iterator> iter(db_->NewIterator(ropts));
  // Out of domain (scan)
  iter->Seek("ke");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("key", iter->key());
  iter->SeekForPrev("kez");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("key", iter->key());
  ASSERT_EQ(HitAndMiss(0, 0), GetBloomStat(options, false));

  // In domain (scan)
  iter->Seek("goan");
  ASSERT_OK(iter->status());
  ASSERT_FALSE(iter->Valid());
  ASSERT_EQ(HitAndMiss(0, 1), GetBloomStat(options, false));
  iter->Seek("goat");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("goat1", iter->key());
  ASSERT_EQ(HitAndMiss(1, 0), GetBloomStat(options, false));

  // Changing prefix extractor should affect prefix query semantics
  // and bypass the existing memtable Bloom filter
  ASSERT_OK(db_->SetOptions({{"prefix_extractor", "fixed:5"}}));
  iter.reset(db_->NewIterator(ropts));
  // Now out of domain (scan)
  iter->Seek("goan");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("goat1", iter->key());
  ASSERT_EQ(HitAndMiss(0, 0), GetBloomStat(options, false));
  // In domain (scan)
  iter->Seek("goat2");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("goat2", iter->key());
  ASSERT_EQ(HitAndMiss(0, 0), GetBloomStat(options, false));
  // In domain (scan)
  if (ropts.prefix_same_as_start) {
    iter->Seek("goat0");
    ASSERT_OK(iter->status());
    ASSERT_FALSE(iter->Valid());
    ASSERT_EQ(HitAndMiss(0, 0), GetBloomStat(options, false));
  } else {
    // NOTE: legacy prefix Seek may return keys outside of prefix
  }

  // Start a fresh new memtable, using new prefix extractor
  ASSERT_OK(SingleDelete("key"));
  ASSERT_OK(SingleDelete("goat1"));
  ASSERT_OK(SingleDelete("goat2"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("key", "_v"));
  ASSERT_OK(Put("goat1", "_g1"));
  ASSERT_OK(Put("goat2", "_g2"));

  iter.reset(db_->NewIterator(ropts));

  // Still out of domain (Get)
  ASSERT_EQ("_v", Get("key"));
  ASSERT_EQ(HitAndMiss(0, 0), GetBloomStat(options, false));

  // Still in domain (Get)
  ASSERT_EQ("_g1", Get("goat1"));
  ASSERT_EQ(HitAndMiss(1, 0), GetBloomStat(options, false));
  ASSERT_EQ("NOT_FOUND", Get("goat11"));
  ASSERT_EQ(HitAndMiss(1, 0), GetBloomStat(options, false));
  ASSERT_EQ("NOT_FOUND", Get("goat9"));
  ASSERT_EQ(HitAndMiss(0, 1), GetBloomStat(options, false));
  ASSERT_EQ("NOT_FOUND", Get("goan1"));
  ASSERT_EQ(HitAndMiss(0, 1), GetBloomStat(options, false));

  // Now out of domain (scan)
  iter->Seek("goan");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("goat1", iter->key());
  ASSERT_EQ(HitAndMiss(0, 0), GetBloomStat(options, false));
  // In domain (scan)
  iter->Seek("goat2");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("goat2", iter->key());
  ASSERT_EQ(HitAndMiss(1, 0), GetBloomStat(options, false));
  // In domain (scan)
  iter->Seek("goat0");
  ASSERT_OK(iter->status());
  ASSERT_FALSE(iter->Valid());
  ASSERT_EQ(HitAndMiss(0, 1), GetBloomStat(options, false));
}

class DBBloomFilterTestVaryPrefixAndFormatVer
    : public DBTestBase,
      public testing::WithParamInterface<std::tuple<bool, uint32_t>> {
 protected:
  bool use_prefix_;
  uint32_t format_version_;

 public:
  DBBloomFilterTestVaryPrefixAndFormatVer()
      : DBTestBase("db_bloom_filter_tests", /*env_do_fsync=*/true) {}

  ~DBBloomFilterTestVaryPrefixAndFormatVer() override = default;

  void SetUp() override {
    use_prefix_ = std::get<0>(GetParam());
    format_version_ = std::get<1>(GetParam());
  }

  static std::string UKey(uint32_t i) { return Key(static_cast<int>(i)); }
};

TEST_P(DBBloomFilterTestVaryPrefixAndFormatVer, PartitionedMultiGet) {
  Options options = CurrentOptions();
  if (use_prefix_) {
    // Entire key from UKey()
    options.prefix_extractor.reset(NewCappedPrefixTransform(9));
  }
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(20));
  bbto.partition_filters = true;
  bbto.index_type = BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  bbto.whole_key_filtering = !use_prefix_;
  if (use_prefix_) {  // (not related to prefix, just alternating between)
    // Make sure code appropriately deals with metadata block size setting
    // that is "too small" (smaller than minimum size for filter builder)
    bbto.metadata_block_size = 63;
  } else {
    // Make sure the test will work even on platforms with large minimum
    // filter size, due to large cache line size.
    // (Largest cache line size + 10+% overhead.)
    bbto.metadata_block_size = 290;
  }
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);
  ReadOptions ropts;

  constexpr uint32_t N = 12000;
  // Add N/2 evens
  for (uint32_t i = 0; i < N; i += 2) {
    ASSERT_OK(Put(UKey(i), UKey(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_EQ(TotalTableFiles(), 1);

  constexpr uint32_t Q = 29;
  // MultiGet In
  std::array<std::string, Q> keys;
  std::array<Slice, Q> key_slices;
  std::array<ColumnFamilyHandle*, Q> column_families;
  // MultiGet Out
  std::array<Status, Q> statuses;
  std::array<PinnableSlice, Q> values;

  PopTicker(options, BLOCK_CACHE_FILTER_HIT);
  PopTicker(options, BLOCK_CACHE_FILTER_MISS);
  PopTicker(options, BLOOM_FILTER_PREFIX_USEFUL);
  PopTicker(options, BLOOM_FILTER_USEFUL);
  PopTicker(options, BLOOM_FILTER_PREFIX_CHECKED);
  PopTicker(options, BLOOM_FILTER_FULL_POSITIVE);
  PopTicker(options, BLOOM_FILTER_FULL_TRUE_POSITIVE);
  PopTicker(options, BLOOM_FILTER_PREFIX_TRUE_POSITIVE);

  // Check that initial clump of keys only loads one partition filter from
  // block cache.
  // And that spread out keys load many partition filters.
  // In both cases, mix present vs. not present keys.
  for (uint32_t stride : {uint32_t{1}, (N / Q) | 1}) {
    for (uint32_t i = 0; i < Q; ++i) {
      keys[i] = UKey(i * stride);
      key_slices[i] = Slice(keys[i]);
      column_families[i] = db_->DefaultColumnFamily();
      statuses[i] = Status();
      values[i] = PinnableSlice();
    }

    get_perf_context()->Reset();
    const PerfLevel previous_perf_level = GetPerfLevel();
    SetPerfLevel(PerfLevel::kEnableTime);
    db_->MultiGet(ropts, Q, column_families.data(), key_slices.data(),
                  values.data(),
                  /*timestamps=*/nullptr, statuses.data(), true);
    SetPerfLevel(previous_perf_level);

    // Confirm correct status results
    uint32_t number_not_found = 0;
    for (uint32_t i = 0; i < Q; ++i) {
      if ((i * stride % 2) == 0) {
        ASSERT_OK(statuses[i]);
      } else {
        ASSERT_TRUE(statuses[i].IsNotFound());
        ++number_not_found;
      }
    }

    // Confirm correct Bloom stats (no FPs)
    uint64_t filter_useful =
        PopTicker(options, use_prefix_ ? BLOOM_FILTER_PREFIX_USEFUL
                                       : BLOOM_FILTER_USEFUL);
    uint64_t filter_checked =
        PopTicker(options, use_prefix_ ? BLOOM_FILTER_PREFIX_CHECKED
                                       : BLOOM_FILTER_FULL_POSITIVE) +
        (use_prefix_ ? 0 : filter_useful);
    EXPECT_EQ(filter_useful, number_not_found);
    EXPECT_EQ(filter_checked, Q);
    EXPECT_EQ(PopTicker(options, use_prefix_ ? BLOOM_FILTER_PREFIX_TRUE_POSITIVE
                                             : BLOOM_FILTER_FULL_TRUE_POSITIVE),
              Q - number_not_found);

    // Confirm no duplicate loading same filter partition
    uint64_t filter_accesses = PopTicker(options, BLOCK_CACHE_FILTER_HIT) +
                               PopTicker(options, BLOCK_CACHE_FILTER_MISS);
    // Partition reads must contribute to the filter-block timing metric.
    EXPECT_GT(get_perf_context()->read_filter_block_nanos, 0);
    if (stride == 1) {
      EXPECT_EQ(filter_accesses, 1);
    } else {
      // for large stride
      EXPECT_GE(filter_accesses, Q / 2 + 1);
    }
  }

  // Check that a clump of keys (present and not) works when spanning
  // two partitions
  int found_spanning = 0;
  for (uint32_t start = 0; start < N / 2;) {
    for (uint32_t i = 0; i < Q; ++i) {
      keys[i] = UKey(start + i);
      key_slices[i] = Slice(keys[i]);
      column_families[i] = db_->DefaultColumnFamily();
      statuses[i] = Status();
      values[i] = PinnableSlice();
    }

    db_->MultiGet(ropts, Q, column_families.data(), key_slices.data(),
                  values.data(),
                  /*timestamps=*/nullptr, statuses.data(), true);

    // Confirm correct status results
    uint32_t number_not_found = 0;
    for (uint32_t i = 0; i < Q; ++i) {
      if (((start + i) % 2) == 0) {
        ASSERT_OK(statuses[i]);
      } else {
        ASSERT_TRUE(statuses[i].IsNotFound());
        ++number_not_found;
      }
    }

    // Confirm correct Bloom stats (might see some FPs)
    uint64_t filter_useful =
        PopTicker(options, use_prefix_ ? BLOOM_FILTER_PREFIX_USEFUL
                                       : BLOOM_FILTER_USEFUL);
    uint64_t filter_checked =
        PopTicker(options, use_prefix_ ? BLOOM_FILTER_PREFIX_CHECKED
                                       : BLOOM_FILTER_FULL_POSITIVE) +
        (use_prefix_ ? 0 : filter_useful);
    EXPECT_GE(filter_useful, number_not_found - 2);  // possible FP
    EXPECT_EQ(filter_checked, Q);
    EXPECT_EQ(PopTicker(options, use_prefix_ ? BLOOM_FILTER_PREFIX_TRUE_POSITIVE
                                             : BLOOM_FILTER_FULL_TRUE_POSITIVE),
              Q - number_not_found);

    // Confirm no duplicate loading of same filter partition
    uint64_t filter_accesses = PopTicker(options, BLOCK_CACHE_FILTER_HIT) +
                               PopTicker(options, BLOCK_CACHE_FILTER_MISS);
    if (filter_accesses == 2) {
      // Spanned across partitions.
      ++found_spanning;
      if (found_spanning >= 2) {
        break;
      } else {
        // Ensure that at least once we have at least one present and
        // one non-present key on both sides of partition boundary.
        start += 2;
      }
    } else {
      EXPECT_EQ(filter_accesses, 1);
      // See explanation at "start += 2"
      start += Q - 4;
    }
  }
  EXPECT_TRUE(found_spanning >= 2);
}

INSTANTIATE_TEST_CASE_P(DBBloomFilterTestVaryPrefixAndFormatVer,
                        DBBloomFilterTestVaryPrefixAndFormatVer,
                        ::testing::Values(
                            // (use_prefix, format_version)
                            std::make_tuple(false, 2),
                            std::make_tuple(false, 3),
                            std::make_tuple(false, 4),
                            std::make_tuple(false, 5), std::make_tuple(true, 2),
                            std::make_tuple(true, 3), std::make_tuple(true, 4),
                            std::make_tuple(true, 5)));

namespace {
static const std::string kPlainTable = "test_PlainTableBloom";
}  // anonymous namespace

class BloomStatsTestWithParam
    : public DBBloomFilterTest,
      public testing::WithParamInterface<
          std::tuple<std::string, FilterPartitioning>> {
 public:
  BloomStatsTestWithParam() {
    bfp_impl_ = std::get<0>(GetParam());
    filter_partitioning_ = std::get<1>(GetParam());

    options_.create_if_missing = true;
    options_.prefix_extractor.reset(
        ROCKSDB_NAMESPACE::NewFixedPrefixTransform(4));
    options_.memtable_prefix_bloom_size_ratio =
        8.0 * 1024.0 / static_cast<double>(options_.write_buffer_size);
    if (bfp_impl_ == kPlainTable) {
      assert(!PartitionFilters());  // not supported in plain table
      PlainTableOptions table_options;
      options_.table_factory.reset(NewPlainTableFactory(table_options));
    } else {
      BlockBasedTableOptions table_options;
      if (PartitionFilters()) {
        table_options.partition_filters = PartitionFilters();
        table_options.index_type =
            BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
      }
      table_options.filter_policy = Create(10, bfp_impl_);
      options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
    }
    options_.env = env_;

    get_perf_context()->Reset();
    DestroyAndReopen(options_);
  }

  ~BloomStatsTestWithParam() override {
    get_perf_context()->Reset();
    Destroy(options_);
  }

  // Required if inheriting from testing::WithParamInterface<>
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  std::string bfp_impl_;
  Options options_;
};

// 1 Insert 2 K-V pairs into DB
// 2 Call Get() for both keys - expext memtable bloom hit stat to be 2
// 3 Call Get() for nonexisting key - expect memtable bloom miss stat to be 1
// 4 Call Flush() to create SST
// 5 Call Get() for both keys - expext SST bloom hit stat to be 2
// 6 Call Get() for nonexisting key - expect SST bloom miss stat to be 1
// Test both: block and plain SST
TEST_P(BloomStatsTestWithParam, BloomStatsTest) {
  std::string key1("AAAA");
  std::string key2("RXDB");  // not in DB
  std::string key3("ZBRA");
  std::string value1("Value1");
  std::string value3("Value3");

  ASSERT_OK(Put(key1, value1, WriteOptions()));
  ASSERT_OK(Put(key3, value3, WriteOptions()));

  // check memtable bloom stats
  ASSERT_EQ(value1, Get(key1));
  ASSERT_EQ(1, get_perf_context()->bloom_memtable_hit_count);
  ASSERT_EQ(value3, Get(key3));
  ASSERT_EQ(2, get_perf_context()->bloom_memtable_hit_count);
  ASSERT_EQ(0, get_perf_context()->bloom_memtable_miss_count);

  ASSERT_EQ("NOT_FOUND", Get(key2));
  ASSERT_EQ(1, get_perf_context()->bloom_memtable_miss_count);
  ASSERT_EQ(2, get_perf_context()->bloom_memtable_hit_count);

  // sanity checks
  ASSERT_EQ(0, get_perf_context()->bloom_sst_hit_count);
  ASSERT_EQ(0, get_perf_context()->bloom_sst_miss_count);

  ASSERT_OK(Flush());

  // sanity checks
  ASSERT_EQ(0, get_perf_context()->bloom_sst_hit_count);
  ASSERT_EQ(0, get_perf_context()->bloom_sst_miss_count);

  // check SST bloom stats
  ASSERT_EQ(value1, Get(key1));
  ASSERT_EQ(1, get_perf_context()->bloom_sst_hit_count);
  ASSERT_EQ(value3, Get(key3));
  ASSERT_EQ(2, get_perf_context()->bloom_sst_hit_count);

  ASSERT_EQ("NOT_FOUND", Get(key2));
  ASSERT_EQ(1, get_perf_context()->bloom_sst_miss_count);
}

// Same scenario as in BloomStatsTest but using an iterator
TEST_P(BloomStatsTestWithParam, BloomStatsTestWithIter) {
  std::string key1("AAAA");
  std::string key2("RXDB");  // not in DB
  std::string key3("ZBRA");
  std::string value1("Value1");
  std::string value3("Value3");

  ASSERT_OK(Put(key1, value1, WriteOptions()));
  ASSERT_OK(Put(key3, value3, WriteOptions()));

  ReadOptions ropts;
  if (options_.prefix_seek_opt_in_only) {
    ropts.prefix_same_as_start = true;
  }
  std::unique_ptr<Iterator> iter(dbfull()->NewIterator(ropts));

  // check memtable bloom stats
  iter->Seek(key1);
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(value1, iter->value().ToString());
  ASSERT_EQ(1, get_perf_context()->bloom_memtable_hit_count);
  ASSERT_EQ(0, get_perf_context()->bloom_memtable_miss_count);

  iter->Seek(key3);
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(value3, iter->value().ToString());
  ASSERT_EQ(2, get_perf_context()->bloom_memtable_hit_count);
  ASSERT_EQ(0, get_perf_context()->bloom_memtable_miss_count);

  iter->Seek(key2);
  ASSERT_OK(iter->status());
  ASSERT_FALSE(iter->Valid());
  ASSERT_EQ(1, get_perf_context()->bloom_memtable_miss_count);
  ASSERT_EQ(2, get_perf_context()->bloom_memtable_hit_count);

  ASSERT_OK(Flush());

  iter.reset(dbfull()->NewIterator(ropts));

  // Check SST bloom stats
  iter->Seek(key1);
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(value1, iter->value().ToString());
  ASSERT_EQ(1, get_perf_context()->bloom_sst_hit_count);

  iter->Seek(key3);
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(value3, iter->value().ToString());
  uint64_t expected_hits = 2;
  ASSERT_EQ(expected_hits, get_perf_context()->bloom_sst_hit_count);

  iter->Seek(key2);
  ASSERT_OK(iter->status());
  ASSERT_FALSE(iter->Valid());
  ASSERT_EQ(1, get_perf_context()->bloom_sst_miss_count);
  ASSERT_EQ(expected_hits, get_perf_context()->bloom_sst_hit_count);
}

INSTANTIATE_TEST_CASE_P(
    BloomStatsTestWithParam, BloomStatsTestWithParam,
    ::testing::Values(
        std::make_tuple(kLegacyBloom, FilterPartitioning::kUnpartitionedFilter),
        std::make_tuple(kLegacyBloom,
                        FilterPartitioning::kCoupledPartitionedFilter),
        std::make_tuple(kLegacyBloom,
                        FilterPartitioning::kDecoupledPartitionedFilter),
        std::make_tuple(kFastLocalBloom,
                        FilterPartitioning::kUnpartitionedFilter),
        std::make_tuple(kFastLocalBloom,
                        FilterPartitioning::kCoupledPartitionedFilter),
        std::make_tuple(kFastLocalBloom,
                        FilterPartitioning::kDecoupledPartitionedFilter),
        std::make_tuple(kPlainTable,
                        FilterPartitioning::kUnpartitionedFilter)));

namespace {
void PrefixScanInit(DBBloomFilterTest* dbtest) {
  char buf[100];
  std::string keystr;
  const int small_range_sstfiles = 5;
  const int big_range_sstfiles = 5;

  // Generate 11 sst files with the following prefix ranges.
  // GROUP 0: [0,10]                              (level 1)
  // GROUP 1: [1,2], [2,3], [3,4], [4,5], [5, 6]  (level 0)
  // GROUP 2: [0,6], [0,7], [0,8], [0,9], [0,10]  (level 0)
  //
  // A seek with the previous API would do 11 random I/Os (to all the
  // files).  With the new API and a prefix filter enabled, we should
  // only do 2 random I/O, to the 2 files containing the key.

  // GROUP 0
  snprintf(buf, sizeof(buf), "%02d______:start", 0);
  keystr = std::string(buf);
  ASSERT_OK(dbtest->Put(keystr, keystr));
  snprintf(buf, sizeof(buf), "%02d______:end", 10);
  keystr = std::string(buf);
  ASSERT_OK(dbtest->Put(keystr, keystr));
  ASSERT_OK(dbtest->Flush());
  ASSERT_OK(dbtest->dbfull()->CompactRange(CompactRangeOptions(), nullptr,
                                           nullptr));  // move to level 1

  // GROUP 1
  for (int i = 1; i <= small_range_sstfiles; i++) {
    snprintf(buf, sizeof(buf), "%02d______:start", i);
    keystr = std::string(buf);
    ASSERT_OK(dbtest->Put(keystr, keystr));
    snprintf(buf, sizeof(buf), "%02d______:end", i + 1);
    keystr = std::string(buf);
    ASSERT_OK(dbtest->Put(keystr, keystr));
    ASSERT_OK(dbtest->Flush());
  }

  // GROUP 2
  for (int i = 1; i <= big_range_sstfiles; i++) {
    snprintf(buf, sizeof(buf), "%02d______:start", 0);
    keystr = std::string(buf);
    ASSERT_OK(dbtest->Put(keystr, keystr));
    snprintf(buf, sizeof(buf), "%02d______:end", small_range_sstfiles + i + 1);
    keystr = std::string(buf);
    ASSERT_OK(dbtest->Put(keystr, keystr));
    ASSERT_OK(dbtest->Flush());
  }
}
}  // anonymous namespace

TEST_F(DBBloomFilterTest, PrefixScan) {
  while (ChangeFilterOptions()) {
    int count;
    Slice prefix;
    Slice key;
    char buf[100];
    Iterator* iter;
    snprintf(buf, sizeof(buf), "03______:");
    prefix = Slice(buf, 8);
    key = Slice(buf, 9);
    ASSERT_EQ(key.difference_offset(prefix), 8);
    ASSERT_EQ(prefix.difference_offset(key), 8);
    // db configs
    env_->count_random_reads_ = true;
    Options options = CurrentOptions();
    options.env = env_;
    options.prefix_extractor.reset(NewFixedPrefixTransform(8));
    options.disable_auto_compactions = true;
    options.max_background_compactions = 2;
    options.create_if_missing = true;
    options.memtable_factory.reset(NewHashSkipListRepFactory(16));
    assert(!options.unordered_write);
    // It is incompatible with allow_concurrent_memtable_write=false
    options.allow_concurrent_memtable_write = false;

    BlockBasedTableOptions table_options;
    table_options.no_block_cache = true;
    table_options.filter_policy.reset(NewBloomFilterPolicy(10));
    table_options.whole_key_filtering = false;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    // 11 RAND I/Os
    DestroyAndReopen(options);
    PrefixScanInit(this);
    count = 0;
    env_->random_read_counter_.Reset();
    ReadOptions ropts;
    if (options.prefix_seek_opt_in_only) {
      ropts.prefix_same_as_start = true;
    }
    iter = db_->NewIterator(ropts);
    for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
      if (!iter->key().starts_with(prefix)) {
        ASSERT_FALSE(ropts.prefix_same_as_start);
        break;
      }
      count++;
    }
    ASSERT_OK(iter->status());
    delete iter;
    ASSERT_EQ(count, 2);
    ASSERT_EQ(env_->random_read_counter_.Read(), 2);
    Close();
  }  // end of while
}

TEST_F(DBBloomFilterTest, OptimizeFiltersForHits) {
  const int kNumKeysPerFlush = 1000;

  Options options = CurrentOptions();
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFlush));
  options.target_file_size_base = 64 * 1024;
  options.level0_file_num_compaction_trigger = 2;
  options.level0_slowdown_writes_trigger = 2;
  options.level0_stop_writes_trigger = 4;
  options.max_bytes_for_level_base = 256 * 1024;
  options.max_write_buffer_number = 2;
  options.max_background_compactions = 8;
  options.max_background_flushes = 8;
  options.compression = kNoCompression;
  options.compaction_style = kCompactionStyleLevel;
  options.level_compaction_dynamic_level_bytes = true;
  BlockBasedTableOptions bbto;
  bbto.cache_index_and_filter_blocks = true;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10));
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  options.optimize_filters_for_hits = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  get_perf_context()->Reset();
  get_perf_context()->EnablePerLevelPerfContext();
  CreateAndReopenWithCF({"mypikachu"}, options);

  int numkeys = 200000;

  // Generate randomly shuffled keys, so the updates are almost
  // random.
  std::vector<int> keys;
  keys.reserve(numkeys);
  for (int i = 0; i < numkeys; i += 2) {
    keys.push_back(i);
  }
  RandomShuffle(std::begin(keys), std::end(keys), /*seed*/ 42);
  int num_inserted = 0;
  for (int key : keys) {
    ASSERT_OK(Put(1, Key(key), "val"));
    num_inserted++;
    // The write after each `kNumKeysPerFlush` keys triggers a flush. Always
    // wait for that flush and any follow-on compactions for deterministic LSM
    // shape.
    if (num_inserted > kNumKeysPerFlush &&
        num_inserted % kNumKeysPerFlush == 1) {
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[1]));
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
    }
  }
  ASSERT_OK(Put(1, Key(0), "val"));
  ASSERT_OK(Put(1, Key(numkeys), "val"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  if (NumTableFilesAtLevel(0, 1) == 0) {
    // No Level 0 file. Create one.
    ASSERT_OK(Put(1, Key(0), "val"));
    ASSERT_OK(Put(1, Key(numkeys), "val"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }

  for (int i = 1; i < numkeys; i += 2) {
    ASSERT_EQ(Get(1, Key(i)), "NOT_FOUND");
  }

  ASSERT_EQ(0, TestGetTickerCount(options, GET_HIT_L0));
  ASSERT_EQ(0, TestGetTickerCount(options, GET_HIT_L1));
  ASSERT_EQ(0, TestGetTickerCount(options, GET_HIT_L2_AND_UP));

  // Now we have three sorted run, L0, L5 and L6 with most files in L6 have
  // no bloom filter. Most keys be checked bloom filters twice.
  ASSERT_GT(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 65000 * 2);
  ASSERT_LT(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 120000 * 2);
  uint64_t bloom_filter_useful_all_levels = 0;
  for (auto& kv : (*(get_perf_context()->level_to_perf_context))) {
    if (kv.second.bloom_filter_useful > 0) {
      bloom_filter_useful_all_levels += kv.second.bloom_filter_useful;
    }
  }
  ASSERT_GT(bloom_filter_useful_all_levels, 65000 * 2);
  ASSERT_LT(bloom_filter_useful_all_levels, 120000 * 2);

  for (int i = 0; i < numkeys; i += 2) {
    ASSERT_EQ(Get(1, Key(i)), "val");
  }

  // Part 2 (read path): rewrite last level with blooms, then verify they get
  // cached only if !optimize_filters_for_hits
  options.disable_auto_compactions = true;
  options.num_levels = 9;
  options.optimize_filters_for_hits = false;
  options.statistics = CreateDBStatistics();
  bbto.block_cache.reset();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  ReopenWithColumnFamilies({"default", "mypikachu"}, options);
  MoveFilesToLevel(7 /* level */, 1 /* column family index */);

  std::string value = Get(1, Key(0));
  uint64_t prev_cache_filter_hits =
      TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT);
  value = Get(1, Key(0));
  ASSERT_EQ(prev_cache_filter_hits + 1,
            TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));

  // Now that we know the filter blocks exist in the last level files, see if
  // filter caching is skipped for this optimization
  options.optimize_filters_for_hits = true;
  options.statistics = CreateDBStatistics();
  bbto.block_cache.reset();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  ReopenWithColumnFamilies({"default", "mypikachu"}, options);

  value = Get(1, Key(0));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
  ASSERT_EQ(2 /* index and data block */,
            TestGetTickerCount(options, BLOCK_CACHE_ADD));

  // Check filter block ignored for files preloaded during DB::Open()
  options.max_open_files = -1;
  options.statistics = CreateDBStatistics();
  bbto.block_cache.reset();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  ReopenWithColumnFamilies({"default", "mypikachu"}, options);

  uint64_t prev_cache_filter_misses =
      TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS);
  prev_cache_filter_hits = TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT);
  Get(1, Key(0));
  ASSERT_EQ(prev_cache_filter_misses,
            TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(prev_cache_filter_hits,
            TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));

  // Check filter block ignored for file trivially-moved to bottom level
  bbto.block_cache.reset();
  options.max_open_files = 100;  // setting > -1 makes it not preload all files
  options.statistics = CreateDBStatistics();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  ReopenWithColumnFamilies({"default", "mypikachu"}, options);

  ASSERT_OK(Put(1, Key(numkeys + 1), "val"));
  ASSERT_OK(Flush(1));

  int32_t trivial_move = 0;
  int32_t non_trivial_move = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* /*arg*/) { trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial",
      [&](void* /*arg*/) { non_trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kSkip;
  compact_options.change_level = true;
  compact_options.target_level = 7;
  ASSERT_OK(db_->CompactRange(compact_options, handles_[1], nullptr, nullptr));

  ASSERT_GE(trivial_move, 1);
  ASSERT_EQ(non_trivial_move, 0);

  prev_cache_filter_hits = TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT);
  prev_cache_filter_misses =
      TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS);
  value = Get(1, Key(numkeys + 1));
  ASSERT_EQ(prev_cache_filter_hits,
            TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
  ASSERT_EQ(prev_cache_filter_misses,
            TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));

  // Check filter block not cached for iterator
  bbto.block_cache.reset();
  options.statistics = CreateDBStatistics();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  ReopenWithColumnFamilies({"default", "mypikachu"}, options);

  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions(), handles_[1]));
  iter->SeekToFirst();
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
  ASSERT_EQ(2 /* index and data block */,
            TestGetTickerCount(options, BLOCK_CACHE_ADD));
  get_perf_context()->Reset();
}

int CountIter(std::unique_ptr<Iterator>& iter, const Slice& key) {
  int count = 0;
  for (iter->Seek(key); iter->Valid(); iter->Next()) {
    count++;
    // Access key & value as if we were using them
    (void)iter->key();
    (void)iter->value();
  }
  EXPECT_OK(iter->status());
  return count;
}

// use iterate_upper_bound to hint compatiability of existing bloom filters.
// The BF is considered compatible if 1) upper bound and seek key transform
// into the same string, or 2) the transformed seek key is of the same length
// as the upper bound and two keys are adjacent according to the comparator.
TEST_F(DBBloomFilterTest, DynamicBloomFilterUpperBound) {
  for (const auto& bfp_impl : BloomLikeFilterPolicy::GetAllFixedImpls()) {
    Options options;
    options.create_if_missing = true;
    options.env = CurrentOptions().env;
    options.prefix_extractor.reset(NewCappedPrefixTransform(4));
    options.disable_auto_compactions = true;
    options.statistics = CreateDBStatistics();
    // Enable prefix bloom for SST files
    BlockBasedTableOptions table_options;
    table_options.cache_index_and_filter_blocks = true;
    table_options.filter_policy = Create(10, bfp_impl);
    table_options.index_shortening = BlockBasedTableOptions::
        IndexShorteningMode::kShortenSeparatorsAndSuccessor;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    DestroyAndReopen(options);

    ASSERT_OK(Put("abcdxxx0", "val1"));
    ASSERT_OK(Put("abcdxxx1", "val2"));
    ASSERT_OK(Put("abcdxxx2", "val3"));
    ASSERT_OK(Put("abcdxxx3", "val4"));
    ASSERT_OK(dbfull()->Flush(FlushOptions()));
    {
      // prefix_extractor has not changed, BF will always be read
      Slice upper_bound("abce");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "abcd0000"), 4);
      ASSERT_EQ(TestGetTickerCount(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH),
                1);
      ASSERT_EQ(TestGetTickerCount(options, NON_LAST_LEVEL_SEEK_FILTERED), 0);
      ASSERT_EQ(TestGetTickerCount(
                    options, NON_LAST_LEVEL_SEEK_DATA_USEFUL_FILTER_MATCH),
                1);
    }
    {
      Slice upper_bound("abcdzzzz");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "abcd0000"), 4);
      ASSERT_EQ(TestGetTickerCount(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH),
                2);
      ASSERT_EQ(TestGetTickerCount(options, NON_LAST_LEVEL_SEEK_FILTERED), 0);
    }
    ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "fixed:5"}}));
    ASSERT_EQ(dbfull()->GetOptions().prefix_extractor->AsString(),
              "rocksdb.FixedPrefix.5");
    {
      // BF changed, [abcdxx00, abce) is a valid bound, will trigger BF read
      Slice upper_bound("abce");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "abcdxx00"), 4);
      // should check bloom filter since upper bound meets requirement
      ASSERT_EQ(TestGetTickerCount(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH),
                3);
      ASSERT_EQ(TestGetTickerCount(options, NON_LAST_LEVEL_SEEK_FILTERED), 0);
    }
    {
      // [abcdxx01, abcey) is not valid bound since upper bound is too long for
      // the BF in SST (capped:4)
      Slice upper_bound("abcey");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "abcdxx01"), 4);
      // should skip bloom filter since upper bound is too long
      ASSERT_EQ(TestGetTickerCount(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH),
                3);
      ASSERT_EQ(TestGetTickerCount(options, NON_LAST_LEVEL_SEEK_FILTERED), 0);
    }
    {
      // [abcdxx02, abcdy) is a valid bound since the prefix is the same
      Slice upper_bound("abcdy");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "abcdxx02"), 4);
      // should check bloom filter since upper bound matches transformed seek
      // key
      ASSERT_EQ(TestGetTickerCount(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH),
                4);
      ASSERT_EQ(TestGetTickerCount(options, NON_LAST_LEVEL_SEEK_FILTERED), 0);
    }
    {
      // [aaaaaaaa, abce) is not a valid bound since 1) they don't share the
      // same prefix, 2) the prefixes are not consecutive
      Slice upper_bound("abce");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "aaaaaaaa"), 0);
      // should skip bloom filter since mismatch is found
      ASSERT_EQ(TestGetTickerCount(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH),
                4);
      ASSERT_EQ(TestGetTickerCount(options, NON_LAST_LEVEL_SEEK_FILTERED), 0);
    }
    ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "fixed:3"}}));
    {
      // [abc, abd) is not a valid bound since the upper bound is too short
      // for BF (capped:4)
      Slice upper_bound("abd");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "abc"), 4);
      ASSERT_EQ(TestGetTickerCount(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH),
                4);
      ASSERT_EQ(TestGetTickerCount(options, NON_LAST_LEVEL_SEEK_FILTERED), 0);
    }
    // Same with re-open
    options.prefix_extractor.reset(NewFixedPrefixTransform(3));
    Reopen(options);
    {
      Slice upper_bound("abd");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "abc"), 4);
      ASSERT_EQ(TestGetTickerCount(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH),
                4);
      ASSERT_EQ(TestGetTickerCount(options, NON_LAST_LEVEL_SEEK_FILTERED), 0);
    }
    // Set back to capped:4 and verify BF is always read
    options.prefix_extractor.reset(NewCappedPrefixTransform(4));
    Reopen(options);
    {
      Slice upper_bound("abd");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "abc"), 0);
      ASSERT_EQ(TestGetTickerCount(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH),
                4);
      ASSERT_EQ(TestGetTickerCount(options, NON_LAST_LEVEL_SEEK_FILTERED), 1);
    }
    // Same if there's a problem initally loading prefix transform
    SyncPoint::GetInstance()->SetCallBack(
        "BlockBasedTable::Open::ForceNullTablePrefixExtractor",
        [&](void* arg) { *static_cast<bool*>(arg) = true; });
    SyncPoint::GetInstance()->EnableProcessing();
    Reopen(options);
    {
      Slice upper_bound("abd");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "abc"), 0);
      ASSERT_EQ(TestGetTickerCount(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH),
                4);
      ASSERT_EQ(TestGetTickerCount(options, NON_LAST_LEVEL_SEEK_FILTERED), 2);
    }
    SyncPoint::GetInstance()->DisableProcessing();
  }
}

// Create multiple SST files each with a different prefix_extractor config,
// verify iterators can read all SST files using the latest config.
TEST_F(DBBloomFilterTest, DynamicBloomFilterMultipleSST) {
  for (const auto& bfp_impl : BloomLikeFilterPolicy::GetAllFixedImpls()) {
    Options options;
    options.env = CurrentOptions().env;
    options.create_if_missing = true;
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    options.disable_auto_compactions = true;
    options.statistics = CreateDBStatistics();
    // Enable prefix bloom for SST files
    BlockBasedTableOptions table_options;
    table_options.filter_policy = Create(10, bfp_impl);
    table_options.cache_index_and_filter_blocks = true;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    DestroyAndReopen(options);

    Slice upper_bound("foz90000");
    ReadOptions read_options;
    read_options.prefix_same_as_start = true;

    // first SST with fixed:1 BF
    ASSERT_OK(Put("foo2", "bar2"));
    ASSERT_OK(Put("foo", "bar"));
    ASSERT_OK(Put("foq1", "bar1"));
    ASSERT_OK(Put("fpa", "0"));
    ASSERT_OK(dbfull()->Flush(FlushOptions()));
    std::unique_ptr<Iterator> iter_old(db_->NewIterator(read_options));
    ASSERT_EQ(CountIter(iter_old, "foo"), 4);
    EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTERED), 0);
    EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH), 1);

    ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "capped:3"}}));
    ASSERT_EQ(dbfull()->GetOptions().prefix_extractor->AsString(),
              "rocksdb.CappedPrefix.3");
    read_options.iterate_upper_bound = &upper_bound;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_EQ(CountIter(iter, "foo"), 2);
    EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTERED), 0);
    EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH), 1);
    ASSERT_EQ(CountIter(iter, "gpk"), 0);
    EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTERED), 0);
    EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH), 0);

    // second SST with capped:3 BF
    ASSERT_OK(Put("foo3", "bar3"));
    ASSERT_OK(Put("foo4", "bar4"));
    ASSERT_OK(Put("foq5", "bar5"));
    ASSERT_OK(Put("fpb", "1"));
    ASSERT_OK(dbfull()->Flush(FlushOptions()));
    {
      // BF is cappped:3 now
      std::unique_ptr<Iterator> iter_tmp(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter_tmp, "foo"), 4);
      EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTERED), 0);
      EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH), 2);
      ASSERT_EQ(CountIter(iter_tmp, "gpk"), 0);
      // both counters are incremented because BF is "not changed" for 1 of the
      // 2 SST files, so filter is checked once and found no match.
      EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTERED), 1);
      EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH), 0);
    }

    ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "fixed:2"}}));
    ASSERT_EQ(dbfull()->GetOptions().prefix_extractor->AsString(),
              "rocksdb.FixedPrefix.2");
    // third SST with fixed:2 BF
    ASSERT_OK(Put("foo6", "bar6"));
    ASSERT_OK(Put("foo7", "bar7"));
    ASSERT_OK(Put("foq8", "bar8"));
    ASSERT_OK(Put("fpc", "2"));
    ASSERT_OK(dbfull()->Flush(FlushOptions()));
    {
      // BF is fixed:2 now
      std::unique_ptr<Iterator> iter_tmp(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter_tmp, "foo"), 9);
      // the first and last BF are checked
      EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTERED), 0);
      EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH), 2);
      ASSERT_EQ(CountIter(iter_tmp, "gpk"), 0);
      // only last BF is checked and not found
      EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTERED), 1);
      EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH), 0);
    }

    // iter_old can only see the first SST
    ASSERT_EQ(CountIter(iter_old, "foo"), 4);
    EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTERED), 0);
    EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH), 1);
    // same with iter, but different prefix extractor
    ASSERT_EQ(CountIter(iter, "foo"), 2);
    EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTERED), 0);
    EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH), 1);

    {
      // keys in all three SSTs are visible to iterator
      // The range of [foo, foz90000] is compatible with (fixed:1) and (fixed:2)
      std::unique_ptr<Iterator> iter_all(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter_all, "foo"), 9);
      EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTERED), 0);
      EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH), 2);
      ASSERT_EQ(CountIter(iter_all, "gpk"), 0);
      // FIXME? isn't seek key out of SST range?
      EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTERED), 1);
      EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH), 0);
    }
    ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "capped:3"}}));
    ASSERT_EQ(dbfull()->GetOptions().prefix_extractor->AsString(),
              "rocksdb.CappedPrefix.3");
    {
      std::unique_ptr<Iterator> iter_all(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter_all, "foo"), 6);
      // all three SST are checked because the current options has the same as
      // the remaining SST (capped:3)
      EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTERED), 0);
      EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH), 3);
      ASSERT_EQ(CountIter(iter_all, "gpk"), 0);
      // FIXME? isn't seek key out of SST range?
      EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTERED), 1);
      EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH), 0);
    }
    // TODO(Zhongyi): Maybe also need to add Get calls to test point look up?
  }
}

// Create a new column family in a running DB, change prefix_extractor
// dynamically, verify the iterator created on the new column family behaves
// as expected
TEST_F(DBBloomFilterTest, DynamicBloomFilterNewColumnFamily) {
  int iteration = 0;
  for (const auto& bfp_impl : BloomLikeFilterPolicy::GetAllFixedImpls()) {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    options.disable_auto_compactions = true;
    options.statistics = CreateDBStatistics();
    // Enable prefix bloom for SST files
    BlockBasedTableOptions table_options;
    table_options.cache_index_and_filter_blocks = true;
    table_options.filter_policy = Create(10, bfp_impl);
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    CreateAndReopenWithCF({"pikachu" + std::to_string(iteration)}, options);
    ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    // create a new CF and set prefix_extractor dynamically
    options.prefix_extractor.reset(NewCappedPrefixTransform(3));
    CreateColumnFamilies({"ramen_dojo_" + std::to_string(iteration)}, options);
    ASSERT_EQ(dbfull()->GetOptions(handles_[2]).prefix_extractor->AsString(),
              "rocksdb.CappedPrefix.3");
    ASSERT_OK(Put(2, "foo3", "bar3"));
    ASSERT_OK(Put(2, "foo4", "bar4"));
    ASSERT_OK(Put(2, "foo5", "bar5"));
    ASSERT_OK(Put(2, "foq6", "bar6"));
    ASSERT_OK(Put(2, "fpq7", "bar7"));
    ASSERT_OK(dbfull()->Flush(FlushOptions()));
    {
      std::unique_ptr<Iterator> iter(
          db_->NewIterator(read_options, handles_[2]));
      ASSERT_EQ(CountIter(iter, "foo"), 3);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED), 0);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
    }
    ASSERT_OK(
        dbfull()->SetOptions(handles_[2], {{"prefix_extractor", "fixed:2"}}));
    ASSERT_EQ(dbfull()->GetOptions(handles_[2]).prefix_extractor->AsString(),
              "rocksdb.FixedPrefix.2");
    {
      std::unique_ptr<Iterator> iter(
          db_->NewIterator(read_options, handles_[2]));
      ASSERT_EQ(CountIter(iter, "foo"), 4);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED), 0);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
    }
    ASSERT_OK(dbfull()->DropColumnFamily(handles_[2]));
    ASSERT_OK(dbfull()->DestroyColumnFamilyHandle(handles_[2]));
    handles_[2] = nullptr;
    ASSERT_OK(dbfull()->DropColumnFamily(handles_[1]));
    ASSERT_OK(dbfull()->DestroyColumnFamilyHandle(handles_[1]));
    handles_[1] = nullptr;
    iteration++;
  }
}

// Verify it's possible to change prefix_extractor at runtime and iterators
// behaves as expected
TEST_F(DBBloomFilterTest, DynamicBloomFilterOptions) {
  for (const auto& bfp_impl : BloomLikeFilterPolicy::GetAllFixedImpls()) {
    Options options;
    options.env = CurrentOptions().env;
    options.create_if_missing = true;
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    options.disable_auto_compactions = true;
    options.statistics = CreateDBStatistics();
    // Enable prefix bloom for SST files
    BlockBasedTableOptions table_options;
    table_options.cache_index_and_filter_blocks = true;
    table_options.filter_policy = Create(10, bfp_impl);
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    DestroyAndReopen(options);

    ASSERT_OK(Put("foo2", "bar2"));
    ASSERT_OK(Put("foo", "bar"));
    ASSERT_OK(Put("foo1", "bar1"));
    ASSERT_OK(Put("fpa", "0"));
    ASSERT_OK(dbfull()->Flush(FlushOptions()));
    ASSERT_OK(Put("foo3", "bar3"));
    ASSERT_OK(Put("foo4", "bar4"));
    ASSERT_OK(Put("foo5", "bar5"));
    ASSERT_OK(Put("fpb", "1"));
    ASSERT_OK(dbfull()->Flush(FlushOptions()));
    ASSERT_OK(Put("foo6", "bar6"));
    ASSERT_OK(Put("foo7", "bar7"));
    ASSERT_OK(Put("foo8", "bar8"));
    ASSERT_OK(Put("fpc", "2"));
    ASSERT_OK(dbfull()->Flush(FlushOptions()));

    ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "foo"), 12);
      EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTERED), 0);
      EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH), 3);
    }
    std::unique_ptr<Iterator> iter_old(db_->NewIterator(read_options));
    ASSERT_EQ(CountIter(iter_old, "foo"), 12);
    EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTERED), 0);
    EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH), 3);

    ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "capped:3"}}));
    ASSERT_EQ(dbfull()->GetOptions().prefix_extractor->AsString(),
              "rocksdb.CappedPrefix.3");
    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      // "fp*" should be skipped
      ASSERT_EQ(CountIter(iter, "foo"), 9);
      EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTERED), 0);
      EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH), 0);
    }

    // iterator created before should not be affected and see all keys
    ASSERT_EQ(CountIter(iter_old, "foo"), 12);
    EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTERED), 0);
    EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH), 3);
    ASSERT_EQ(CountIter(iter_old, "abc"), 0);
    // FIXME? isn't seek key out of SST range?
    EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTERED), 3);
    EXPECT_EQ(PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH), 0);
  }
}

TEST_F(DBBloomFilterTest, SeekForPrevWithPartitionedFilters) {
  for (bool wkf : {true, false}) {
    SCOPED_TRACE("whole_key_filtering=" + std::to_string(wkf));
    Options options = CurrentOptions();
    constexpr size_t kNumKeys = 10000;
    static_assert(kNumKeys <= 10000, "kNumKeys have to be <= 10000");
    options.memtable_factory.reset(
        test::NewSpecialSkipListFactory(kNumKeys + 10));
    options.create_if_missing = true;
    constexpr size_t kPrefixLength = 4;
    options.prefix_extractor.reset(NewFixedPrefixTransform(kPrefixLength));
    options.compression = kNoCompression;
    BlockBasedTableOptions bbto;
    bbto.filter_policy.reset(NewBloomFilterPolicy(50));
    bbto.index_shortening =
        BlockBasedTableOptions::IndexShorteningMode::kNoShortening;
    bbto.block_size = 128;
    bbto.metadata_block_size = 128;
    bbto.partition_filters = true;
    bbto.index_type = BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
    bbto.whole_key_filtering = wkf;
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    DestroyAndReopen(options);

    const std::string value(64, '\0');

    WriteOptions write_opts;
    write_opts.disableWAL = true;
    for (size_t i = 0; i < kNumKeys; ++i) {
      std::ostringstream oss;
      oss << std::setfill('0') << std::setw(4) << std::fixed << i;
      ASSERT_OK(db_->Put(write_opts, oss.str(), value));
    }
    ASSERT_OK(Flush());

    ReadOptions read_opts;
    // Use legacy, implicit prefix seek
    read_opts.total_order_seek = false;
    read_opts.auto_prefix_mode = false;
    std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));
    for (size_t i = 0; i < kNumKeys; ++i) {
      // Seek with a key after each one added but with same prefix. One will
      // surely cross a partition boundary.
      std::ostringstream oss;
      oss << std::setfill('0') << std::setw(4) << std::fixed << i << "a";
      it->SeekForPrev(oss.str());
      ASSERT_OK(it->status());
      ASSERT_TRUE(it->Valid());
    }
  }
}

namespace {
class BackwardBytewiseComparator : public Comparator {
 public:
  const char* Name() const override { return "BackwardBytewiseComparator"; }

  int Compare(const Slice& a, const Slice& b) const override {
    int min_size_neg = -static_cast<int>(std::min(a.size(), b.size()));
    const char* a_end = a.data() + a.size();
    const char* b_end = b.data() + b.size();
    for (int i = -1; i >= min_size_neg; --i) {
      if (a_end[i] != b_end[i]) {
        if (static_cast<unsigned char>(a_end[i]) <
            static_cast<unsigned char>(b_end[i])) {
          return -1;
        } else {
          return 1;
        }
      }
    }
    return static_cast<int>(a.size()) - static_cast<int>(b.size());
  }

  void FindShortestSeparator(std::string* /*start*/,
                             const Slice& /*limit*/) const override {}

  void FindShortSuccessor(std::string* /*key*/) const override {}
};

const BackwardBytewiseComparator kBackwardBytewiseComparator{};

class FixedSuffix4Transform : public SliceTransform {
  const char* Name() const override { return "FixedSuffixTransform"; }

  Slice Transform(const Slice& src) const override {
    return Slice(src.data() + src.size() - 4, 4);
  }

  bool InDomain(const Slice& src) const override { return src.size() >= 4; }
};
}  // anonymous namespace

// This uses a prefix_extractor + comparator combination that violates
// one of the old obsolete, unnecessary axioms of prefix extraction:
// * key.starts_with(prefix(key))
// This axiom is not really needed, and we validate that here.
TEST_F(DBBloomFilterTest, WeirdPrefixExtractorWithFilter1) {
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(ROCKSDB_NAMESPACE::NewBloomFilterPolicy(10));
  bbto.whole_key_filtering = false;

  Options options = CurrentOptions();
  options.comparator = &kBackwardBytewiseComparator;
  options.prefix_extractor = std::make_shared<FixedSuffix4Transform>();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  options.memtable_prefix_bloom_size_ratio = 0.1;
  options.statistics = CreateDBStatistics();

  DestroyAndReopen(options);

  ASSERT_OK(Put("321aaaa", "val1"));
  ASSERT_OK(Put("112aaaa", "val2"));
  ASSERT_OK(Put("009aaaa", "val3"));
  ASSERT_OK(Put("baa", "val4"));  // out of domain
  ASSERT_OK(Put("321abaa", "val5"));
  ASSERT_OK(Put("zzz", "val6"));  // out of domain

  for (auto flushed : {false, true}) {
    SCOPED_TRACE("flushed=" + std::to_string(flushed));
    if (flushed) {
      ASSERT_OK(Flush());
    }
    ReadOptions read_options;
    if (flushed) {  // TODO: support auto_prefix_mode in memtable?
      read_options.auto_prefix_mode = true;
    }
    EXPECT_EQ(GetBloomStat(options, flushed), HitAndMiss(0, 0));
    {
      Slice ub("999aaaa");
      read_options.iterate_upper_bound = &ub;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      EXPECT_EQ(CountIter(iter, "aaaa"), 3);
      EXPECT_EQ(GetBloomStat(options, flushed), HitAndMiss(1, 0));
    }
    {
      Slice ub("999abaa");
      read_options.iterate_upper_bound = &ub;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      EXPECT_EQ(CountIter(iter, "abaa"), 1);
      EXPECT_EQ(GetBloomStat(options, flushed), HitAndMiss(1, 0));
    }
    {
      Slice ub("999acaa");
      read_options.iterate_upper_bound = &ub;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      EXPECT_EQ(CountIter(iter, "acaa"), 0);
      EXPECT_EQ(GetBloomStat(options, flushed), HitAndMiss(0, 1));
    }
    {
      Slice ub("zzzz");
      read_options.iterate_upper_bound = &ub;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      EXPECT_EQ(CountIter(iter, "baa"), 3);
      if (flushed) {  // TODO: fix memtable case
        EXPECT_EQ(GetBloomStat(options, flushed), HitAndMiss(0, 0));
      }
    }
  }
}

// This uses a prefix_extractor + comparator combination that violates
// one of the old obsolete, unnecessary axioms of prefix extraction:
// * Compare(prefix(key), key) <= 0
// This axiom is not really needed, and we validate that here.
TEST_F(DBBloomFilterTest, WeirdPrefixExtractorWithFilter2) {
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(ROCKSDB_NAMESPACE::NewBloomFilterPolicy(10));
  bbto.whole_key_filtering = false;

  Options options = CurrentOptions();
  options.comparator = ReverseBytewiseComparator();
  options.prefix_extractor.reset(NewFixedPrefixTransform(4));
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  options.memtable_prefix_bloom_size_ratio = 0.1;
  options.statistics = CreateDBStatistics();

  DestroyAndReopen(options);

  ASSERT_OK(Put("aaaa123", "val1"));
  ASSERT_OK(Put("aaaa211", "val2"));
  ASSERT_OK(Put("aaaa900", "val3"));
  ASSERT_OK(Put("aab", "val4"));  // out of domain
  ASSERT_OK(Put("aaba123", "val5"));
  ASSERT_OK(Put("qqqq123", "val7"));
  ASSERT_OK(Put("qqqq", "val8"));
  ASSERT_OK(Put("zzz", "val8"));  // out of domain

  for (auto flushed : {false, true}) {
    SCOPED_TRACE("flushed=" + std::to_string(flushed));
    if (flushed) {
      ASSERT_OK(Flush());
    }
    ReadOptions read_options;
    if (flushed) {  // TODO: support auto_prefix_mode in memtable?
      read_options.auto_prefix_mode = true;
    } else {
      // Reset from other tests
      GetBloomStat(options, flushed);
    }
    EXPECT_EQ(GetBloomStat(options, flushed), HitAndMiss(0, 0));
    {
      Slice ub("aaaa000");
      read_options.iterate_upper_bound = &ub;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      EXPECT_EQ(CountIter(iter, "aaaa999"), 3);
      EXPECT_EQ(GetBloomStat(options, flushed), HitAndMiss(1, 0));
    }
    {
      // Note: prefix does work as upper bound
      Slice ub("aaaa");
      read_options.iterate_upper_bound = &ub;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      EXPECT_EQ(CountIter(iter, "aaaa999"), 3);
      EXPECT_EQ(GetBloomStat(options, flushed), HitAndMiss(1, 0));
    }
    {
      // Note: prefix does not work here as seek key
      Slice ub("aaaa500");
      read_options.iterate_upper_bound = &ub;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      EXPECT_EQ(CountIter(iter, "aaaa"), 0);
      EXPECT_EQ(GetBloomStat(options, flushed), HitAndMiss(1, 0));
    }
    {
      Slice ub("aaba000");
      read_options.iterate_upper_bound = &ub;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      EXPECT_EQ(CountIter(iter, "aaba999"), 1);
      EXPECT_EQ(GetBloomStat(options, flushed), HitAndMiss(1, 0));
    }
    {
      Slice ub("aaca000");
      read_options.iterate_upper_bound = &ub;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      EXPECT_EQ(CountIter(iter, "aaca999"), 0);
      EXPECT_EQ(GetBloomStat(options, flushed), HitAndMiss(0, 1));
    }
    {
      Slice ub("aaaz");
      read_options.iterate_upper_bound = &ub;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      EXPECT_EQ(CountIter(iter, "zzz"), 5);
      EXPECT_EQ(GetBloomStat(options, flushed), HitAndMiss(0, 0));
    }
    {
      // Note: prefix does work here as seek key, but only finds key equal
      // to prefix (others with same prefix are less)
      read_options.auto_prefix_mode = false;
      read_options.iterate_upper_bound = nullptr;
      read_options.prefix_same_as_start = true;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      EXPECT_EQ(CountIter(iter, "qqqq"), 1);
      EXPECT_EQ(GetBloomStat(options, flushed), HitAndMiss(1, 0));
    }
  }
}

namespace {
// A weird comparator that in combination with NonIdempotentFixed4Transform
// breaks an old axiom of prefix filtering.
class WeirdComparator : public Comparator {
 public:
  const char* Name() const override { return "WeirdComparator"; }

  int Compare(const Slice& a, const Slice& b) const override {
    bool a_in = a.size() >= 5;
    bool b_in = b.size() >= 5;
    if (a_in != b_in) {
      // Order keys after prefixes
      return a_in - b_in;
    }
    if (a_in) {
      return BytewiseComparator()->Compare(a, b);
    } else {
      // Different ordering on the prefixes
      return ReverseBytewiseComparator()->Compare(a, b);
    }
  }

  void FindShortestSeparator(std::string* /*start*/,
                             const Slice& /*limit*/) const override {}

  void FindShortSuccessor(std::string* /*key*/) const override {}
};
const WeirdComparator kWeirdComparator{};

// Non-idempotentent because prefix is always 4 bytes, but this is
// out-of-domain for keys to be assigned prefixes (>= 5 bytes)
class NonIdempotentFixed4Transform : public SliceTransform {
  const char* Name() const override { return "NonIdempotentFixed4Transform"; }

  Slice Transform(const Slice& src) const override {
    return Slice(src.data(), 4);
  }

  bool InDomain(const Slice& src) const override { return src.size() >= 5; }
};
}  // anonymous namespace

// This uses a prefix_extractor + comparator combination that violates
// two of the old obsolete, unnecessary axioms of prefix extraction:
// * prefix(prefix(key)) == prefix(key)
// * If Compare(k1, k2) <= 0, then Compare(prefix(k1), prefix(k2)) <= 0
// This axiom is not really needed, and we validate that here.
TEST_F(DBBloomFilterTest, WeirdPrefixExtractorWithFilter3) {
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(ROCKSDB_NAMESPACE::NewBloomFilterPolicy(10));
  bbto.whole_key_filtering = false;

  Options options = CurrentOptions();
  options.prefix_extractor = std::make_shared<NonIdempotentFixed4Transform>();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  options.memtable_prefix_bloom_size_ratio = 0.1;
  options.statistics = CreateDBStatistics();

  for (auto weird_comparator : {false, true}) {
    if (weird_comparator) {
      options.comparator = &kWeirdComparator;
    }
    DestroyAndReopen(options);

    ASSERT_OK(Put("aaaa123", "val1"));
    ASSERT_OK(Put("aaaa211", "val2"));
    ASSERT_OK(Put("aaaa900", "val3"));
    ASSERT_OK(Put("aab", "val4"));  // out of domain
    ASSERT_OK(Put("aaba123", "val5"));
    ASSERT_OK(Put("qqqq123", "val7"));
    ASSERT_OK(Put("qqqq", "val8"));  // out of domain
    ASSERT_OK(Put("zzzz", "val8"));  // out of domain

    for (auto flushed : {false, true}) {
      SCOPED_TRACE("flushed=" + std::to_string(flushed));
      if (flushed) {
        ASSERT_OK(Flush());
      }
      ReadOptions read_options;
      if (flushed) {  // TODO: support auto_prefix_mode in memtable?
        read_options.auto_prefix_mode = true;
      } else {
        // Reset from other tests
        GetBloomStat(options, flushed);
      }
      EXPECT_EQ(GetBloomStat(options, flushed), HitAndMiss(0, 0));
      {
        Slice ub("aaaa999");
        read_options.iterate_upper_bound = &ub;
        std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
        EXPECT_EQ(CountIter(iter, "aaaa000"), 3);
        EXPECT_EQ(GetBloomStat(options, flushed), HitAndMiss(1, 0));
      }
      {
        // Note: prefix as seek key is not bloom-optimized
        // Note: the count works with weird_comparator because "aaaa" is
        // ordered as the last of the prefixes
        Slice ub("aaaa999");
        read_options.iterate_upper_bound = &ub;
        std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
        EXPECT_EQ(CountIter(iter, "aaaa"), 3);
        EXPECT_EQ(GetBloomStat(options, flushed), HitAndMiss(0, 0));
      }
      {
        Slice ub("aaba9");
        read_options.iterate_upper_bound = &ub;
        std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
        EXPECT_EQ(CountIter(iter, "aaba0"), 1);
        EXPECT_EQ(GetBloomStat(options, flushed), HitAndMiss(1, 0));
      }
      {
        Slice ub("aaca9");
        read_options.iterate_upper_bound = &ub;
        std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
        EXPECT_EQ(CountIter(iter, "aaca0"), 0);
        EXPECT_EQ(GetBloomStat(options, flushed), HitAndMiss(0, 1));
      }
      {
        Slice ub("qqqq9");
        read_options.iterate_upper_bound = &ub;
        std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
        EXPECT_EQ(CountIter(iter, "qqqq0"), 1);
        EXPECT_EQ(GetBloomStat(options, flushed), HitAndMiss(1, 0));
      }
      {
        // Note: prefix as seek key is not bloom-optimized
        Slice ub("qqqq9");
        read_options.iterate_upper_bound = &ub;
        std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
        EXPECT_EQ(CountIter(iter, "qqqq"), weird_comparator ? 7 : 2);
        EXPECT_EQ(GetBloomStat(options, flushed), HitAndMiss(0, 0));
      }
      {
        // Note: prefix as seek key is not bloom-optimized
        Slice ub("zzzz9");
        read_options.iterate_upper_bound = &ub;
        std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
        EXPECT_EQ(CountIter(iter, "zzzz"), weird_comparator ? 8 : 1);
        EXPECT_EQ(GetBloomStat(options, flushed), HitAndMiss(0, 0));
      }
      {
        Slice ub("zzzz9");
        read_options.iterate_upper_bound = &ub;
        std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
        EXPECT_EQ(CountIter(iter, "aab"), weird_comparator ? 6 : 5);
        EXPECT_EQ(GetBloomStat(options, flushed), HitAndMiss(0, 0));
      }
    }
  }
}

using experimental::KeySegmentsExtractor;
using experimental::MakeSharedBytewiseMinMaxSQFC;
using experimental::MakeSharedCappedKeySegmentsExtractor;
using experimental::MakeSharedReverseBytewiseMinMaxSQFC;
using experimental::SelectKeySegment;
using experimental::SstQueryFilterConfigs;
using experimental::SstQueryFilterConfigsManager;
using KeyCategorySet = KeySegmentsExtractor::KeyCategorySet;

static std::vector<std::string> RangeQueryKeys(
    SstQueryFilterConfigsManager::Factory& factory, DB& db, const Slice& lb,
    const Slice& ub) {
  ReadOptions ro;
  ro.iterate_lower_bound = &lb;
  ro.iterate_upper_bound = &ub;
  auto filter = factory.GetTableFilterForRangeQuery(lb, ub);
  ro.table_filter = &filter;
  auto it = db.NewIterator(ro);
  std::vector<std::string> ret;
  for (it->Seek(lb); it->Valid(); it->Next()) {
    ret.push_back(it->key().ToString());
  }
  EXPECT_OK(it->status());
  delete it;
  return ret;
};

TEST_F(DBBloomFilterTest, SstQueryFilter) {
  struct MySegmentExtractor : public KeySegmentsExtractor {
    char min_first_char;
    char max_first_char;
    char delim_char;
    MySegmentExtractor(char _min_first_char, char _max_first_char,
                       char _delim_char)
        : min_first_char(_min_first_char),
          max_first_char(_max_first_char),
          delim_char(_delim_char) {}

    const char* Name() const override { return "MySegmentExtractor"; }

    std::string GetId() const override {
      return std::string("MySegmentExtractor+") + min_first_char +
             max_first_char + delim_char;
    }

    void Extract(const Slice& key_or_bound, KeyKind /*kind*/,
                 Result* result) const override {
      size_t len = key_or_bound.size();
      if (len == 0) {
        result->category = KeySegmentsExtractor::kReservedLowCategory;
      } else if (static_cast<unsigned char>(key_or_bound[0]) <
                 static_cast<unsigned char>(min_first_char)) {
        result->category = KeySegmentsExtractor::kReservedLowCategory;
      } else if (static_cast<unsigned char>(key_or_bound[0]) >
                 static_cast<unsigned char>(max_first_char)) {
        result->category = KeySegmentsExtractor::kReservedHighCategory;
      }
      for (uint32_t i = 0; i < len; ++i) {
        if (key_or_bound[i] == delim_char || i + 1 == key_or_bound.size()) {
          result->segment_ends.push_back(i + 1);
        }
      }
    }
  };

  // Use '_' as delimiter, but different spans for default category
  auto extractor_to_c = std::make_shared<MySegmentExtractor>('a', 'c', '_');
  auto extractor_to_z = std::make_shared<MySegmentExtractor>('a', 'z', '_');
  auto extractor_alt = std::make_shared<MySegmentExtractor>('0', '9', '_');

  // Filter on 2nd field, only for default category
  auto filter1_def = MakeSharedBytewiseMinMaxSQFC(
      experimental::SelectKeySegment(1),
      KeyCategorySet{KeySegmentsExtractor::kDefaultCategory});

  // Also filter on 3rd field regardless of category
  auto filter2_all =
      MakeSharedBytewiseMinMaxSQFC(experimental::SelectKeySegment(2));

  SstQueryFilterConfigs configs1 = {{filter1_def, filter2_all}, extractor_to_c};
  SstQueryFilterConfigs configs2 = {{filter1_def, filter2_all}, extractor_to_z};
  SstQueryFilterConfigs configs3 = {{filter2_all}, extractor_alt};

  SstQueryFilterConfigsManager::Data data = {
      {42, {{"foo", configs1}}}, {43, {{"foo", configs2}, {"bar", configs3}}}};

  std::shared_ptr<SstQueryFilterConfigsManager> configs_manager;
  ASSERT_OK(SstQueryFilterConfigsManager::MakeShared(data, &configs_manager));

  // Test manager behaviors
  auto MakeFactory = [configs_manager](
                         const std::string& configs_name,
                         SstQueryFilterConfigsManager::FilteringVersion ver)
      -> std::shared_ptr<SstQueryFilterConfigsManager::Factory> {
    std::shared_ptr<SstQueryFilterConfigsManager::Factory> factory;
    Status s = configs_manager->MakeSharedFactory(configs_name, ver, &factory);
    assert(s.ok());
    return factory;
  };
  std::shared_ptr<SstQueryFilterConfigsManager::Factory> factory;

  // Version 0 is always OK, returning empty/not found configs
  ASSERT_TRUE(MakeFactory("blah", 0)->GetConfigs().IsEmptyNotFound());
  ASSERT_TRUE(MakeFactory("foo", 0)->GetConfigs().IsEmptyNotFound());
  ASSERT_TRUE(MakeFactory("bar", 0)->GetConfigs().IsEmptyNotFound());

  // We can't be sure about the proper configuration for versions outside the
  // known range (and reserved version 0).
  ASSERT_TRUE(configs_manager->MakeSharedFactory("foo", 1, &factory)
                  .IsInvalidArgument());
  ASSERT_TRUE(configs_manager->MakeSharedFactory("foo", 41, &factory)
                  .IsInvalidArgument());
  ASSERT_TRUE(configs_manager->MakeSharedFactory("foo", 44, &factory)
                  .IsInvalidArgument());
  ASSERT_TRUE(configs_manager->MakeSharedFactory("foo", 500, &factory)
                  .IsInvalidArgument());

  ASSERT_TRUE(MakeFactory("blah", 42)->GetConfigs().IsEmptyNotFound());
  ASSERT_TRUE(MakeFactory("blah", 43)->GetConfigs().IsEmptyNotFound());
  ASSERT_FALSE(MakeFactory("foo", 42)->GetConfigs().IsEmptyNotFound());
  ASSERT_FALSE(MakeFactory("foo", 43)->GetConfigs().IsEmptyNotFound());
  ASSERT_TRUE(MakeFactory("bar", 42)->GetConfigs().IsEmptyNotFound());
  ASSERT_FALSE(MakeFactory("bar", 43)->GetConfigs().IsEmptyNotFound());

  ASSERT_OK(configs_manager->MakeSharedFactory("foo", 42, &factory));
  ASSERT_EQ(factory->GetConfigsName(), "foo");
  ASSERT_EQ(factory->GetConfigs().IsEmptyNotFound(), false);

  Options options = CurrentOptions();
  options.statistics = CreateDBStatistics();
  options.table_properties_collector_factories.push_back(factory);

  DestroyAndReopen(options);

  // For lower level file
  ASSERT_OK(Put("   ", "val0"));
  ASSERT_OK(Put("   _345_678", "val0"));
  ASSERT_OK(Put("aaa", "val0"));
  ASSERT_OK(Put("abc_123", "val1"));
  ASSERT_OK(Put("abc_13", "val2"));
  ASSERT_OK(Put("abc_156_987", "val3"));
  ASSERT_OK(Put("bcd_1722", "val4"));
  ASSERT_OK(Put("xyz_145", "val5"));
  ASSERT_OK(Put("xyz_167", "val6"));
  ASSERT_OK(Put("xyz_178", "val7"));
  ASSERT_OK(Put("zzz", "val0"));
  ASSERT_OK(Put("~~~", "val0"));
  ASSERT_OK(Put("~~~_456_789", "val0"));

  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  ASSERT_EQ(factory->GetFilteringVersion(), 42U);
  ASSERT_NOK(factory->SetFilteringVersion(41));
  ASSERT_NOK(factory->SetFilteringVersion(44));
  ASSERT_EQ(factory->GetFilteringVersion(), 42U);
  ASSERT_OK(factory->SetFilteringVersion(43));
  ASSERT_EQ(factory->GetFilteringVersion(), 43U);

  // For higher level file
  ASSERT_OK(Put("   ", "val0"));
  ASSERT_OK(Put("   _345_680", "val0"));
  ASSERT_OK(Put("aaa", "val9"));
  ASSERT_OK(Put("abc_234", "val1"));
  ASSERT_OK(Put("abc_245_567", "val2"));
  ASSERT_OK(Put("abc_25", "val3"));
  ASSERT_OK(Put("xyz_180", "val4"));
  ASSERT_OK(Put("xyz_191", "val4"));
  ASSERT_OK(Put("xyz_260", "val4"));
  ASSERT_OK(Put("zzz", "val9"));
  ASSERT_OK(Put("~~~", "val0"));
  ASSERT_OK(Put("~~~_456_790", "val0"));

  ASSERT_OK(Flush());

  using Keys = std::vector<std::string>;
  auto RangeQuery =
      [factory, db = db_.get()](
          std::string lb, std::string ub,
          std::shared_ptr<SstQueryFilterConfigsManager::Factory> alt_factory =
              nullptr) {
        return RangeQueryKeys(alt_factory ? *alt_factory : *factory, *db, lb,
                              ub);
      };

  // Control 1: range is not filtered but min/max filter is checked
  // because of common prefix leading up to 2nd segment
  // TODO/future: statistics for when filter is checked vs. not applicable
  EXPECT_EQ(RangeQuery("abc_150", "abc_249"),
            Keys({"abc_156_987", "abc_234", "abc_245_567"}));
  EXPECT_EQ(TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA), 2);

  // Test 1: range is filtered to just lowest level, fully containing the
  // segments in that category
  EXPECT_EQ(RangeQuery("abc_100", "abc_179"),
            Keys({"abc_123", "abc_13", "abc_156_987"}));
  EXPECT_EQ(TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA), 1);

  // Test 2: range is filtered to just lowest level, partial overlap
  EXPECT_EQ(RangeQuery("abc_1500_x_y", "abc_16QQ"), Keys({"abc_156_987"}));
  EXPECT_EQ(TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA), 1);

  // Test 3: range is filtered to just highest level, fully containing the
  // segments in that category but would be overlapping the range for the other
  // file if the filter included all categories
  EXPECT_EQ(RangeQuery("abc_200", "abc_300"),
            Keys({"abc_234", "abc_245_567", "abc_25"}));
  EXPECT_EQ(TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA), 1);

  // Test 4: range is filtered to just highest level, partial overlap (etc.)
  EXPECT_EQ(RangeQuery("abc_200", "abc_249"), Keys({"abc_234", "abc_245_567"}));
  EXPECT_EQ(TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA), 1);

  // Test 5: range is filtered from both levels, because of category scope
  EXPECT_EQ(RangeQuery("abc_300", "abc_400"), Keys({}));
  EXPECT_EQ(TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA), 0);

  // Control 2: range is not filtered because association between 1st and
  // 2nd segment is not represented
  EXPECT_EQ(RangeQuery("abc_170", "abc_190"), Keys({}));
  EXPECT_EQ(TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA), 2);

  // Control 3: range is not filtered because there's no (bloom) filter on
  // 1st segment (like prefix filtering)
  EXPECT_EQ(RangeQuery("baa_170", "baa_190"), Keys({}));
  EXPECT_EQ(TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA), 2);

  // Control 4: range is not filtered because difference in segments leading
  // up to 2nd segment
  EXPECT_EQ(RangeQuery("abc_500", "abd_501"), Keys({}));
  EXPECT_EQ(TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA), 2);

  // TODO: exclusive upper bound tests

  // ======= Testing 3rd segment (cross-category filter) =======
  // Control 5: not filtered because of segment range overlap
  EXPECT_EQ(RangeQuery(" z__700", " z__750"), Keys({}));
  EXPECT_EQ(TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA), 2);

  // Test 6: filtered on both levels
  EXPECT_EQ(RangeQuery(" z__100", " z__300"), Keys({}));
  EXPECT_EQ(TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA), 0);

  // Control 6: finding something, with 2nd segment filter helping
  EXPECT_EQ(RangeQuery("abc_156_9", "abc_156_99"), Keys({"abc_156_987"}));
  EXPECT_EQ(TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA), 1);

  EXPECT_EQ(RangeQuery("abc_245_56", "abc_245_57"), Keys({"abc_245_567"}));
  EXPECT_EQ(TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA), 1);

  // Test 6: filtered on both levels, for different segments
  EXPECT_EQ(RangeQuery("abc_245_900", "abc_245_999"), Keys({}));
  EXPECT_EQ(TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA), 0);

  // ======= Testing extractor read portability =======
  EXPECT_EQ(RangeQuery("abc_300", "abc_400"), Keys({}));
  EXPECT_EQ(TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA), 0);

  // Only modifies how filters are written
  ASSERT_OK(factory->SetFilteringVersion(0));
  ASSERT_EQ(factory->GetFilteringVersion(), 0U);
  ASSERT_EQ(factory->GetConfigs().IsEmptyNotFound(), true);

  EXPECT_EQ(RangeQuery("abc_300", "abc_400"), Keys({}));
  EXPECT_EQ(TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA), 0);

  // Even a different config name with different extractor can read
  EXPECT_EQ(RangeQuery("abc_300", "abc_400", MakeFactory("bar", 43)), Keys({}));
  EXPECT_EQ(TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA), 0);

  // Or a "not found" config name
  EXPECT_EQ(RangeQuery("abc_300", "abc_400", MakeFactory("blah", 43)),
            Keys({}));
  EXPECT_EQ(TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA), 0);
}

static std::vector<int> ExtractedSizes(const KeySegmentsExtractor& ex,
                                       const Slice& k) {
  KeySegmentsExtractor::Result r;
  ex.Extract(k, KeySegmentsExtractor::kFullUserKey, &r);
  std::vector<int> ret;
  uint32_t last = 0;
  for (const auto i : r.segment_ends) {
    ret.push_back(static_cast<int>(i - last));
    last = i;
  }
  return ret;
}

TEST_F(DBBloomFilterTest, FixedWidthSegments) {
  // Unit tests for
  auto extractor_none = MakeSharedCappedKeySegmentsExtractor({});
  auto extractor0b = MakeSharedCappedKeySegmentsExtractor({0});
  auto extractor1b = MakeSharedCappedKeySegmentsExtractor({1});
  auto extractor4b = MakeSharedCappedKeySegmentsExtractor({4});
  auto extractor4b0b = MakeSharedCappedKeySegmentsExtractor({4, 0});
  auto extractor4b0b4b = MakeSharedCappedKeySegmentsExtractor({4, 0, 4});
  auto extractor1b3b0b4b = MakeSharedCappedKeySegmentsExtractor({1, 3, 0, 4});

  ASSERT_EQ(extractor_none->GetId(), "CappedKeySegmentsExtractor");
  ASSERT_EQ(extractor0b->GetId(), "CappedKeySegmentsExtractor0b");
  ASSERT_EQ(extractor1b->GetId(), "CappedKeySegmentsExtractor1b");
  ASSERT_EQ(extractor4b->GetId(), "CappedKeySegmentsExtractor4b");
  ASSERT_EQ(extractor4b0b->GetId(), "CappedKeySegmentsExtractor4b0b");
  ASSERT_EQ(extractor4b0b4b->GetId(), "CappedKeySegmentsExtractor4b0b4b");
  ASSERT_EQ(extractor1b3b0b4b->GetId(), "CappedKeySegmentsExtractor1b3b0b4b");

  using V = std::vector<int>;
  ASSERT_EQ(V({}), ExtractedSizes(*extractor_none, {}));
  ASSERT_EQ(V({0}), ExtractedSizes(*extractor0b, {}));
  ASSERT_EQ(V({0}), ExtractedSizes(*extractor1b, {}));
  ASSERT_EQ(V({0}), ExtractedSizes(*extractor4b, {}));
  ASSERT_EQ(V({0, 0}), ExtractedSizes(*extractor4b0b, {}));
  ASSERT_EQ(V({0, 0, 0}), ExtractedSizes(*extractor4b0b4b, {}));
  ASSERT_EQ(V({0, 0, 0, 0}), ExtractedSizes(*extractor1b3b0b4b, {}));

  ASSERT_EQ(V({3}), ExtractedSizes(*extractor4b, "bla"));
  ASSERT_EQ(V({3, 0}), ExtractedSizes(*extractor4b0b, "bla"));
  ASSERT_EQ(V({1, 2, 0, 0}), ExtractedSizes(*extractor1b3b0b4b, "bla"));

  ASSERT_EQ(V({}), ExtractedSizes(*extractor_none, "blah"));
  ASSERT_EQ(V({0}), ExtractedSizes(*extractor0b, "blah"));
  ASSERT_EQ(V({1}), ExtractedSizes(*extractor1b, "blah"));
  ASSERT_EQ(V({4}), ExtractedSizes(*extractor4b, "blah"));
  ASSERT_EQ(V({4, 0}), ExtractedSizes(*extractor4b0b, "blah"));
  ASSERT_EQ(V({4, 0, 0}), ExtractedSizes(*extractor4b0b4b, "blah"));
  ASSERT_EQ(V({1, 3, 0, 0}), ExtractedSizes(*extractor1b3b0b4b, "blah"));

  ASSERT_EQ(V({4, 0}), ExtractedSizes(*extractor4b0b, "blah1"));
  ASSERT_EQ(V({4, 0, 1}), ExtractedSizes(*extractor4b0b4b, "blah1"));
  ASSERT_EQ(V({4, 0, 2}), ExtractedSizes(*extractor4b0b4b, "blah12"));
  ASSERT_EQ(V({4, 0, 3}), ExtractedSizes(*extractor4b0b4b, "blah123"));
  ASSERT_EQ(V({1, 3, 0, 3}), ExtractedSizes(*extractor1b3b0b4b, "blah123"));

  ASSERT_EQ(V({4}), ExtractedSizes(*extractor4b, "blah1234"));
  ASSERT_EQ(V({4, 0}), ExtractedSizes(*extractor4b0b, "blah1234"));
  ASSERT_EQ(V({4, 0, 4}), ExtractedSizes(*extractor4b0b4b, "blah1234"));
  ASSERT_EQ(V({1, 3, 0, 4}), ExtractedSizes(*extractor1b3b0b4b, "blah1234"));

  ASSERT_EQ(V({4, 0, 4}), ExtractedSizes(*extractor4b0b4b, "blah12345"));
  ASSERT_EQ(V({1, 3, 0, 4}), ExtractedSizes(*extractor1b3b0b4b, "blah12345"));

  // Filter config for second and fourth segment
  auto filter1 =
      MakeSharedReverseBytewiseMinMaxSQFC(experimental::SelectKeySegment(1));
  auto filter3 =
      MakeSharedReverseBytewiseMinMaxSQFC(experimental::SelectKeySegment(3));
  SstQueryFilterConfigs configs1 = {{filter1, filter3}, extractor1b3b0b4b};
  SstQueryFilterConfigsManager::Data data = {{42, {{"foo", configs1}}}};
  std::shared_ptr<SstQueryFilterConfigsManager> configs_manager;
  ASSERT_OK(SstQueryFilterConfigsManager::MakeShared(data, &configs_manager));
  std::shared_ptr<SstQueryFilterConfigsManager::Factory> f;
  ASSERT_OK(configs_manager->MakeSharedFactory("foo", 42, &f));

  ASSERT_EQ(f->GetConfigsName(), "foo");
  ASSERT_EQ(f->GetConfigs().IsEmptyNotFound(), false);

  Options options = CurrentOptions();
  options.statistics = CreateDBStatistics();
  options.table_properties_collector_factories.push_back(f);
  // Next most common comparator after bytewise
  options.comparator = ReverseBytewiseComparator();

  DestroyAndReopen(options);

  ASSERT_OK(Put("abcd1234", "val0"));
  ASSERT_OK(Put("abcd1245", "val1"));
  ASSERT_OK(Put("abcd99", "val2"));  // short key
  ASSERT_OK(Put("aqua1200", "val3"));
  ASSERT_OK(Put("aqua1230", "val4"));
  ASSERT_OK(Put("zen", "val5"));  // very short key
  ASSERT_OK(Put("azur1220", "val6"));
  ASSERT_OK(Put("blah", "val7"));
  ASSERT_OK(Put("blah2", "val8"));

  ASSERT_OK(Flush());

  using Keys = std::vector<std::string>;

  // Range is not filtered but segment 1 min/max filter is checked
  EXPECT_EQ(RangeQueryKeys(*f, *db_, "aczz0000", "acdc0000"), Keys({}));
  EXPECT_EQ(1, TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA));

  // Found (can't use segment 3 filter)
  EXPECT_EQ(RangeQueryKeys(*f, *db_, "aqzz0000", "aqdc0000"),
            Keys({"aqua1230", "aqua1200"}));
  EXPECT_EQ(1, TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA));

  // Filtered because of segment 1 min-max not intersecting [aaa, abb]
  EXPECT_EQ(RangeQueryKeys(*f, *db_, "zabb9999", "zaaa0000"), Keys({}));
  EXPECT_EQ(0, TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA));

  // Found
  EXPECT_EQ(RangeQueryKeys(*f, *db_, "aqua1200ZZZ", "aqua1000ZZZ"),
            Keys({"aqua1200"}));
  EXPECT_EQ(1, TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA));

  // Found despite short key
  EXPECT_EQ(RangeQueryKeys(*f, *db_, "aqua121", "aqua1"), Keys({"aqua1200"}));
  EXPECT_EQ(1, TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA));

  // Filtered because of segment 3 min-max not intersecting [1000, 1100]
  // Note that the empty string is tracked outside of the min-max range.
  EXPECT_EQ(RangeQueryKeys(*f, *db_, "aqua1100ZZZ", "aqua1000ZZZ"), Keys({}));
  EXPECT_EQ(0, TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA));

  // Also filtered despite short key
  EXPECT_EQ(RangeQueryKeys(*f, *db_, "aqua11", "aqua1"), Keys({}));
  EXPECT_EQ(0, TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA));

  // Found
  EXPECT_EQ(RangeQueryKeys(*f, *db_, "blah21", "blag"),
            Keys({"blah2", "blah"}));
  EXPECT_EQ(1, TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA));

  // Found
  EXPECT_EQ(RangeQueryKeys(*f, *db_, "blah0", "blag"), Keys({"blah"}));
  EXPECT_EQ(1, TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA));

  // Filtered because of segment 3 min-max not intersecting [0, 1]
  // Note that the empty string is tracked outside of the min-max range.
  EXPECT_EQ(RangeQueryKeys(*f, *db_, "blah1", "blah0"), Keys({}));
  EXPECT_EQ(0, TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA));
}

TEST_F(DBBloomFilterTest, SstQueryFilterKeySegmentRange) {
  // Up to seven 1-byte segments, so that each key segment range selects a
  // distinct substring of the key
  struct OneByteSegmentsExtractor : public KeySegmentsExtractor {
    const char* Name() const override { return "OneByteSegmentsExtractor"; }
    void Extract(const Slice& key_or_bound, KeyKind /*kind*/,
                 Result* result) const override {
      for (uint32_t i = 1; i <= key_or_bound.size() && i <= 7; ++i) {
        result->segment_ends.push_back(i);
      }
    }
  };
  auto extractor = std::make_shared<OneByteSegmentsExtractor>();

  // All the key segment ranges supported by the filter serialization, which
  // stores the selected range in the filter and decodes it at query time
  const std::vector<std::pair<uint8_t, uint8_t>> ranges = {
      {0, 1}, {0, 2}, {1, 2}, {0, 3}, {1, 3}, {2, 3}, {0, 4}, {1, 4},
      {2, 4}, {3, 4}, {0, 5}, {1, 5}, {2, 5}, {3, 5}, {4, 5}, {0, 6}};

  using Keys = std::vector<std::string>;
  for (const auto& [from, to] : ranges) {
    SCOPED_TRACE("SelectKeySegmentRange(" + std::to_string(from) + ", " +
                 std::to_string(to) + ")");
    auto filter = MakeSharedBytewiseMinMaxSQFC(
        experimental::SelectKeySegmentRange(from, to));
    SstQueryFilterConfigs configs = {{filter}, extractor};
    SstQueryFilterConfigsManager::Data data = {{42, {{"foo", configs}}}};
    std::shared_ptr<SstQueryFilterConfigsManager> configs_manager;
    ASSERT_OK(SstQueryFilterConfigsManager::MakeShared(data, &configs_manager));
    std::shared_ptr<SstQueryFilterConfigsManager::Factory> f;
    ASSERT_OK(configs_manager->MakeSharedFactory("foo", 42, &f));

    Options options = CurrentOptions();
    options.statistics = CreateDBStatistics();
    options.table_properties_collector_factories.push_back(f);
    DestroyAndReopen(options);

    ASSERT_OK(Put("0000000", "val0"));
    ASSERT_OK(Put("1111111", "val1"));
    ASSERT_OK(Flush());

    // Found: the selected segments of the bounds are within the min/max of
    // the same segments in the file
    EXPECT_EQ(RangeQueryKeys(*f, *db_, "1111111", "1111112"),
              Keys({"1111111"}));
    EXPECT_EQ(1, TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA));

    // Filtered: the selected segments of the bounds are above the max
    EXPECT_EQ(RangeQueryKeys(*f, *db_, "2222222", "2222223"), Keys({}));
    EXPECT_EQ(0, TestGetAndResetTickerCount(options, NON_LAST_LEVEL_SEEK_DATA));
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
