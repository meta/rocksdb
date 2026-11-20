//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/block_based/partitioned_filter_block.h"

#include <map>
#include <set>

#include "block_cache.h"
#include "index_builder.h"
#include "rocksdb/filter_policy.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/filter_policy_internal.h"
#include "table/format.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/coding.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

std::map<uint64_t, std::string> blooms;

class MockedBlockBasedTable : public BlockBasedTable {
 public:
  MockedBlockBasedTable(Rep* rep, PartitionedIndexBuilder* pib)
      : BlockBasedTable(rep, /*block_cache_tracer=*/nullptr) {
    // Initialize what Open normally does as much as necessary for the test
    rep->index_key_includes_seq = pib->separator_is_key_plus_seq();
    rep->index_value_is_full = !pib->get_use_value_delta_encoding();
    // Open decodes the footer from the file; there is no file here, and an
    // un-decoded Footer reports kInvalidFormatVersion. Match the format_version
    // the test's builders encode the index and filter-partition-index blocks
    // with, so reader-side codec selection (e.g. index_value_delta_escape())
    // stays consistent with how they were written, at any format_version.
    rep->footer.TEST_SetFormatVersion(rep->table_options.format_version);
  }
};

class MyPartitionedFilterBlockReader : public PartitionedFilterBlockReader {
 public:
  MyPartitionedFilterBlockReader(BlockBasedTable* t,
                                 CachableEntry<Block>&& filter_block)
      : PartitionedFilterBlockReader(
            t, std::move(filter_block.As<Block_kFilterPartitionIndex>())) {
    for (const auto& pair : blooms) {
      const uint64_t offset = pair.first;
      const std::string& bloom = pair.second;

      assert(t);
      assert(t->get_rep());
      CachableEntry<ParsedFullFilterBlock> block(
          new ParsedFullFilterBlock(
              t->get_rep()->table_options.filter_policy.get(),
              BlockContents(Slice(bloom))),
          nullptr /* cache */, nullptr /* cache_handle */,
          true /* own_value */);
      filter_map_[offset] = std::move(block);
    }
  }
};

class PartitionedFilterBlockTest
    : public testing::Test,
      virtual public ::testing::WithParamInterface<
          std::tuple<uint32_t, test::UserDefinedTimestampTestMode, bool>> {
 public:
  Options options_;
  ImmutableOptions ioptions_;
  EnvOptions env_options_;
  BlockBasedTableOptions table_options_;
  InternalKeyComparator icomp_;
  std::unique_ptr<BlockBasedTable> table_;
  std::shared_ptr<Cache> cache_;
  int bits_per_key_;
  size_t ts_sz_;
  bool user_defined_timestamps_persisted_;
  bool decouple_partitioned_filters;

  PartitionedFilterBlockTest() : bits_per_key_(10) {
    auto udt_test_mode = std::get<1>(GetParam());
    if (test::IsUDTEnabled(udt_test_mode)) {
      options_.comparator = test::BytewiseComparatorWithU64TsWrapper();
    }
    ts_sz_ = options_.comparator->timestamp_size();
    user_defined_timestamps_persisted_ = test::ShouldPersistUDT(udt_test_mode);
    icomp_ = InternalKeyComparator(options_.comparator);
    env_options_ = EnvOptions(options_);
    ioptions_ = ImmutableOptions(options_);
    table_options_.filter_policy.reset(
        NewBloomFilterPolicy(bits_per_key_, false));
    table_options_.format_version = std::get<0>(GetParam());
    table_options_.index_block_restart_interval = 3;
    table_options_.decouple_partitioned_filters = decouple_partitioned_filters =
        std::get<2>(GetParam());
  }

  ~PartitionedFilterBlockTest() override = default;

  static constexpr int kKeyNum = 4;
  static constexpr int kMissingKeyNum = 2;
  const std::string keys_without_ts[kKeyNum] = {"afoo", "bar", "box", "hello"};
  const std::string missing_keys_without_ts[kMissingKeyNum] = {"missing",
                                                               "other"};

  std::vector<std::string> PrepareKeys(const std::string* orig_keys,
                                       int number_of_keys) {
    std::vector<std::string> user_keys;
    if (ts_sz_ == 0) {
      user_keys.assign(orig_keys, orig_keys + number_of_keys);
    } else {
      for (int i = 0; i < number_of_keys; i++) {
        std::string key_with_ts;
        AppendKeyWithMinTimestamp(&key_with_ts, orig_keys[i], ts_sz_);
        user_keys.push_back(std::move(key_with_ts));
      }
    }
    return user_keys;
  }

  uint64_t MaxIndexSize() {
    uint64_t max_key_size = 0;
    for (int i = 0; i < kKeyNum; i++) {
      // If UDT is enabled, the size of each key would be increased by a
      // timestamp size.
      max_key_size = std::max(
          max_key_size, static_cast<uint64_t>(keys_without_ts[i].size()) +
                            ts_sz_ * sizeof(static_cast<unsigned char>(0)));
    }
    uint64_t max_index_size = kKeyNum * (max_key_size + 8 /*handle*/);
    return max_index_size;
  }

  uint64_t MaxFilterSize() {
    // General, rough over-approximation
    return kKeyNum * bits_per_key_ + (CACHE_LINE_SIZE * 8 + /*metadata*/ 5);
  }

  uint64_t last_offset = 10;
  BlockHandle Write(const Slice& slice) {
    BlockHandle bh(last_offset + 1, slice.size());
    blooms[bh.offset()] = slice.ToString();
    last_offset += bh.size();
    return bh;
  }

  PartitionedIndexBuilder* NewIndexBuilder() {
    const bool kValueDeltaEncoded = true;
    return PartitionedIndexBuilder::CreateIndexBuilder(
        &icomp_, !kValueDeltaEncoded, table_options_, ts_sz_,
        user_defined_timestamps_persisted_);
  }

  PartitionedFilterBlockBuilder* NewBuilder(
      PartitionedIndexBuilder* const p_index_builder,
      const SliceTransform* prefix_extractor = nullptr) {
    assert(table_options_.block_size_deviation <= 100);
    auto partition_size =
        static_cast<uint32_t>(((table_options_.metadata_block_size *
                                (100 - table_options_.block_size_deviation)) +
                               99) /
                              100);
    partition_size = std::max(partition_size, static_cast<uint32_t>(1));
    const bool kValueDeltaEncoded = true;
    return new PartitionedFilterBlockBuilder(
        prefix_extractor, table_options_.whole_key_filtering,
        BloomFilterPolicy::GetBuilderFromContext(
            FilterBuildingContext(table_options_)),
        table_options_.index_block_restart_interval, !kValueDeltaEncoded,
        table_options_.format_version, p_index_builder, partition_size, ts_sz_,
        user_defined_timestamps_persisted_, decouple_partitioned_filters);
  }

  PartitionedFilterBlockReader* NewReader(
      PartitionedFilterBlockBuilder* builder, PartitionedIndexBuilder* pib,
      bool expect_empty = false) {
    BlockHandle bh;
    Status status;
    Slice slice;
    std::unique_ptr<const char[]> filter_data;
    do {
      status = builder->Finish(bh, &slice, &filter_data);
      bh = Write(slice);
      if (expect_empty) {
        // Ensure most efficient "empty" filter is used
        EXPECT_OK(status);
        EXPECT_EQ(0, slice.size());
      }
    } while (status.IsIncomplete());

    constexpr bool skip_filters = false;
    constexpr uint64_t file_size = 12345;
    constexpr int level = 0;
    constexpr bool immortal_table = false;
    table_.reset(new MockedBlockBasedTable(
        new BlockBasedTable::Rep(ioptions_, env_options_, table_options_,
                                 icomp_, skip_filters, file_size, level,
                                 immortal_table,
                                 user_defined_timestamps_persisted_),
        pib));
    BlockContents contents(slice);
    CachableEntry<Block> block(
        new Block(std::move(contents), 0 /* read_amp_bytes_per_bit */, nullptr),
        nullptr /* cache */, nullptr /* cache_handle */, true /* own_value */);
    auto reader =
        new MyPartitionedFilterBlockReader(table_.get(), std::move(block));
    return reader;
  }

  void VerifyReader(PartitionedFilterBlockBuilder* builder,
                    PartitionedIndexBuilder* pib, bool empty = false) {
    std::unique_ptr<PartitionedFilterBlockReader> reader(
        NewReader(builder, pib, empty));
    // Querying added keys
    std::vector<std::string> keys = PrepareKeys(keys_without_ts, kKeyNum);
    for (const auto& key : keys) {
      auto ikey = InternalKey(key, 0, ValueType::kTypeValue);
      const Slice ikey_slice = Slice(*ikey.rep());
      ASSERT_TRUE(reader->KeyMayMatch(
          StripTimestampFromUserKey(key, ts_sz_), &ikey_slice,
          /*get_context=*/nullptr,
          /*lookup_context=*/nullptr, test::kReadOptionsNoIo));
    }
    {
      // querying a key twice
      auto ikey = InternalKey(keys[0], 0, ValueType::kTypeValue);
      const Slice ikey_slice = Slice(*ikey.rep());
      ASSERT_TRUE(reader->KeyMayMatch(
          StripTimestampFromUserKey(keys[0], ts_sz_), &ikey_slice,
          /*get_context=*/nullptr,
          /*lookup_context=*/nullptr, test::kReadOptionsNoIo));
    }
    // querying missing keys
    std::vector<std::string> missing_keys =
        PrepareKeys(missing_keys_without_ts, kMissingKeyNum);
    for (const auto& key : missing_keys) {
      auto ikey = InternalKey(key, 0, ValueType::kTypeValue);
      const Slice ikey_slice = Slice(*ikey.rep());
      if (empty) {
        ASSERT_TRUE(reader->KeyMayMatch(
            StripTimestampFromUserKey(key, ts_sz_), &ikey_slice,
            /*get_context=*/nullptr,
            /*lookup_context=*/nullptr, test::kReadOptionsNoIo));
      } else {
        // assuming a good hash function
        ASSERT_FALSE(reader->KeyMayMatch(
            StripTimestampFromUserKey(key, ts_sz_), &ikey_slice,
            /*get_context=*/nullptr,
            /*lookup_context=*/nullptr, test::kReadOptionsNoIo));
      }
    }
  }

  // Queries `reader` with a MultiGet range over `query_keys` (user keys
  // without timestamp, in sorted order) via the whole-key `KeysMayMatch`
  // path, and returns the subset of keys the filter reports as possibly
  // present (i.e. the keys not filtered out).
  std::set<std::string> MultiGetMayMatch(
      PartitionedFilterBlockReader* reader,
      const std::vector<std::string>& query_keys) {
    ReadOptions read_options = test::kReadOptionsNoIo;
    std::string min_ts(ts_sz_, '\0');
    Slice min_ts_slice(min_ts);
    if (ts_sz_ > 0) {
      read_options.timestamp = &min_ts_slice;
    }

    autovector<Slice, MultiGetContext::MAX_BATCH_SIZE> ukeys;
    autovector<PinnableSlice, MultiGetContext::MAX_BATCH_SIZE> values;
    autovector<Status, MultiGetContext::MAX_BATCH_SIZE> statuses;
    autovector<KeyContext, MultiGetContext::MAX_BATCH_SIZE> key_context;
    autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE> sorted_keys;
    values.resize(query_keys.size());
    statuses.resize(query_keys.size());
    for (const auto& key : query_keys) {
      ukeys.emplace_back(key);
    }
    for (size_t i = 0; i < query_keys.size(); ++i) {
      key_context.emplace_back(/*column_family=*/nullptr, ukeys[i], &values[i],
                               /*columns=*/nullptr, /*timestamp=*/nullptr,
                               &statuses[i]);
    }
    for (auto& key_ctx : key_context) {
      sorted_keys.emplace_back(&key_ctx);
    }
    MultiGetContext ctx(&sorted_keys, /*begin=*/0, sorted_keys.size(),
                        /*snapshot=*/0, read_options, /*fs=*/nullptr,
                        /*stats=*/nullptr);
    MultiGetContext::Range range = ctx.GetMultiGetRange();
    reader->KeysMayMatch(&range, /*lookup_context=*/nullptr, read_options);
    for (const auto& status : statuses) {
      status.PermitUncheckedOk();
    }

    std::set<std::string> may_match_keys;
    for (auto iter = range.begin(); iter != range.end(); ++iter) {
      may_match_keys.insert(iter->ukey_without_ts.ToString());
    }
    return may_match_keys;
  }

  int TestBlockPerKey() {
    std::unique_ptr<PartitionedIndexBuilder> pib(NewIndexBuilder());
    std::unique_ptr<PartitionedFilterBlockBuilder> builder(
        NewBuilder(pib.get()));
    int i = 0;
    std::vector<std::string> keys = PrepareKeys(keys_without_ts, kKeyNum);
    builder->Add(StripTimestampFromUserKey(keys[i], ts_sz_));
    CutABlock(pib.get(), keys[i], keys[i + 1]);
    i++;
    builder->Add(StripTimestampFromUserKey(keys[i], ts_sz_));
    CutABlock(pib.get(), keys[i], keys[i + 1]);
    i++;
    builder->Add(StripTimestampFromUserKey(keys[i], ts_sz_));
    builder->Add(StripTimestampFromUserKey(keys[i], ts_sz_));
    CutABlock(pib.get(), keys[i], keys[i + 1]);
    i++;
    builder->Add(StripTimestampFromUserKey(keys[i], ts_sz_));
    CutABlock(pib.get(), keys[i]);

    VerifyReader(builder.get(), pib.get());
    return CountNumOfIndexPartitions(pib.get());
  }

  void TestBlockPerTwoKeys(const SliceTransform* prefix_extractor = nullptr) {
    std::unique_ptr<PartitionedIndexBuilder> pib(NewIndexBuilder());
    std::unique_ptr<PartitionedFilterBlockBuilder> builder(
        NewBuilder(pib.get(), prefix_extractor));
    std::vector<std::string> keys = PrepareKeys(keys_without_ts, kKeyNum);
    int i = 0;
    builder->Add(StripTimestampFromUserKey(keys[i], ts_sz_));
    i++;
    builder->Add(StripTimestampFromUserKey(keys[i], ts_sz_));
    CutABlock(pib.get(), keys[i], keys[i + 1]);
    i++;
    builder->Add(StripTimestampFromUserKey(keys[i], ts_sz_));
    builder->Add(StripTimestampFromUserKey(keys[i], ts_sz_));
    i++;
    builder->Add(StripTimestampFromUserKey(keys[i], ts_sz_));
    CutABlock(pib.get(), keys[i]);

    VerifyReader(builder.get(), pib.get(), prefix_extractor);
  }

  void TestBlockPerAllKeys() {
    std::unique_ptr<PartitionedIndexBuilder> pib(NewIndexBuilder());
    std::unique_ptr<PartitionedFilterBlockBuilder> builder(
        NewBuilder(pib.get()));
    std::vector<std::string> keys = PrepareKeys(keys_without_ts, kKeyNum);
    int i = 0;
    builder->Add(StripTimestampFromUserKey(keys[i], ts_sz_));
    i++;
    builder->Add(StripTimestampFromUserKey(keys[i], ts_sz_));
    i++;
    builder->Add(StripTimestampFromUserKey(keys[i], ts_sz_));
    builder->Add(StripTimestampFromUserKey(keys[i], ts_sz_));
    i++;
    builder->Add(StripTimestampFromUserKey(keys[i], ts_sz_));
    CutABlock(pib.get(), keys[i]);

    VerifyReader(builder.get(), pib.get());
  }

  void CutABlock(PartitionedIndexBuilder* builder,
                 const std::string& user_key) {
    // Assuming a block is cut, add an entry to the index
    std::string key =
        std::string(*InternalKey(user_key, 0, ValueType::kTypeValue).rep());
    BlockHandle dont_care_block_handle(1, 1);
    std::string scratch;
    builder->AddIndexEntry(key, nullptr, dont_care_block_handle, &scratch,
                           false);
  }

  void CutABlock(PartitionedIndexBuilder* builder, const std::string& user_key,
                 const std::string& next_user_key) {
    // Assuming a block is cut, add an entry to the index
    std::string key = *InternalKey(user_key, 0, ValueType::kTypeValue).rep();
    std::string next_key =
        *InternalKey(next_user_key, 0, ValueType::kTypeValue).rep();
    BlockHandle dont_care_block_handle(1, 1);
    Slice slice = Slice(next_key.data(), next_key.size());
    std::string scratch;
    builder->AddIndexEntry(key, &slice, dont_care_block_handle, &scratch,
                           false);
  }

  int CountNumOfIndexPartitions(PartitionedIndexBuilder* builder) {
    IndexBuilder::IndexBlocks dont_care_ib;
    BlockHandle dont_care_bh(10, 10);
    Status s;
    int cnt = 0;
    do {
      s = builder->Finish(&dont_care_ib, dont_care_bh);
      cnt++;
    } while (s.IsIncomplete());
    return cnt - 1;  // 1 is 2nd level index
  }
};

// Format versions potentially intersting to partitioning
INSTANTIATE_TEST_CASE_P(
    FormatVersions, PartitionedFilterBlockTest,
    testing::Combine(
        testing::ValuesIn(std::set<uint32_t>{
            2, 3, 4, 5, test::kDefaultFormatVersion,
            BlockBasedTableOptions::kLatestSupportedFormatVersion}),
        testing::ValuesIn(test::GetUDTTestModes()), testing::Bool()));

TEST_P(PartitionedFilterBlockTest, EmptyBuilder) {
  std::unique_ptr<PartitionedIndexBuilder> pib(NewIndexBuilder());
  std::unique_ptr<PartitionedFilterBlockBuilder> builder(NewBuilder(pib.get()));
  const bool empty = true;
  VerifyReader(builder.get(), pib.get(), empty);
}

TEST_P(PartitionedFilterBlockTest, OneBlock) {
  uint64_t max_index_size = MaxIndexSize();
  for (uint64_t i = 1; i < max_index_size + 1; i++) {
    table_options_.metadata_block_size = i;
    TestBlockPerAllKeys();
  }
}

TEST_P(PartitionedFilterBlockTest, TwoBlocksPerKey) {
  uint64_t max_index_size = MaxIndexSize();
  for (uint64_t i = 1; i < max_index_size + 1; i++) {
    table_options_.metadata_block_size = i;
    TestBlockPerTwoKeys();
  }
}

// This reproduces the bug that a prefix is the same among multiple consecutive
// blocks but the bug would add it only to the first block.
TEST_P(PartitionedFilterBlockTest, SamePrefixInMultipleBlocks) {
  // some small number to cause partition cuts
  table_options_.metadata_block_size = 1;
  std::unique_ptr<const SliceTransform> prefix_extractor(
      ROCKSDB_NAMESPACE::NewFixedPrefixTransform(1));
  std::unique_ptr<PartitionedIndexBuilder> pib(NewIndexBuilder());
  std::unique_ptr<PartitionedFilterBlockBuilder> builder(
      NewBuilder(pib.get(), prefix_extractor.get()));
  const std::string pkeys_without_ts[3] = {"p-key10", "p-key20", "p-key30"};
  std::vector<std::string> pkeys =
      PrepareKeys(pkeys_without_ts, 3 /* number_of_keys */);
  builder->Add(StripTimestampFromUserKey(pkeys[0], ts_sz_));
  CutABlock(pib.get(), pkeys[0], pkeys[1]);
  builder->Add(StripTimestampFromUserKey(pkeys[1], ts_sz_));
  CutABlock(pib.get(), pkeys[1], pkeys[2]);
  builder->Add(StripTimestampFromUserKey(pkeys[2], ts_sz_));
  CutABlock(pib.get(), pkeys[2]);
  std::unique_ptr<PartitionedFilterBlockReader> reader(
      NewReader(builder.get(), pib.get()));
  for (const auto& key : pkeys) {
    auto ikey = InternalKey(key, 0, ValueType::kTypeValue);
    const Slice ikey_slice = Slice(*ikey.rep());
    ASSERT_TRUE(
        reader->PrefixMayMatch(prefix_extractor->Transform(key), &ikey_slice,
                               /*get_context=*/nullptr,
                               /*lookup_context=*/nullptr, ReadOptions()));
  }
  // Non-existent keys but with the same prefix
  const std::string pnonkeys_without_ts[4] = {"p-key9", "p-key11", "p-key21",
                                              "p-key31"};
  std::vector<std::string> pnonkeys =
      PrepareKeys(pnonkeys_without_ts, 4 /* number_of_keys */);
  for (const auto& key : pnonkeys) {
    auto ikey = InternalKey(key, 0, ValueType::kTypeValue);
    const Slice ikey_slice = Slice(*ikey.rep());
    ASSERT_TRUE(
        reader->PrefixMayMatch(prefix_extractor->Transform(key), &ikey_slice,
                               /*get_context=*/nullptr,
                               /*lookup_context=*/nullptr, ReadOptions()));
  }
}

// This reproduces the bug in format_version=3 that the seeking the prefix will
// lead us to the partition before the one that has filter for the prefix.
TEST_P(PartitionedFilterBlockTest, PrefixInWrongPartitionBug) {
  // some small number to cause partition cuts
  table_options_.metadata_block_size = 1;
  std::unique_ptr<const SliceTransform> prefix_extractor(
      ROCKSDB_NAMESPACE::NewFixedPrefixTransform(2));
  std::unique_ptr<PartitionedIndexBuilder> pib(NewIndexBuilder());
  std::unique_ptr<PartitionedFilterBlockBuilder> builder(
      NewBuilder(pib.get(), prefix_extractor.get()));
  // In the bug, searching for prefix "p3" on an index with format version 3,
  // will give the key "p3" and the partition of the keys that are <= p3, i.e.,
  // p2-keys, where the filter for prefix "p3" does not exist.
  const std::string pkeys_without_ts[] = {"p1-key1", "p2-key2", "p3-key3",
                                          "p4-key3", "p5-key3"};
  std::vector<std::string> pkeys =
      PrepareKeys(pkeys_without_ts, 5 /* number_of_keys */);
  builder->Add(StripTimestampFromUserKey(pkeys[0], ts_sz_));
  CutABlock(pib.get(), pkeys[0], pkeys[1]);
  builder->Add(StripTimestampFromUserKey(pkeys[1], ts_sz_));
  CutABlock(pib.get(), pkeys[1], pkeys[2]);
  builder->Add(StripTimestampFromUserKey(pkeys[2], ts_sz_));
  CutABlock(pib.get(), pkeys[2], pkeys[3]);
  builder->Add(StripTimestampFromUserKey(pkeys[3], ts_sz_));
  CutABlock(pib.get(), pkeys[3], pkeys[4]);
  builder->Add(StripTimestampFromUserKey(pkeys[4], ts_sz_));
  CutABlock(pib.get(), pkeys[4]);
  std::unique_ptr<PartitionedFilterBlockReader> reader(
      NewReader(builder.get(), pib.get()));
  for (const auto& key : pkeys) {
    auto prefix = prefix_extractor->Transform(key);
    auto ikey = InternalKey(key, 0, ValueType::kTypeValue);
    const Slice ikey_slice = Slice(*ikey.rep());
    ASSERT_TRUE(reader->PrefixMayMatch(prefix, &ikey_slice,
                                       /*get_context=*/nullptr,
                                       /*lookup_context=*/nullptr,
                                       ReadOptions()));
  }
}

// Exercises the MultiGet whole-key path (`KeysMayMatch` -> `MayMatch` over a
// `MultiGetRange`), which reuses a single top-level filter-partition index
// iterator across every key in the range. The range spans multiple filter
// partitions, so consulting a wrong partition for any key would surface as a
// false-negative Get (an added key incorrectly filtered out).
TEST_P(PartitionedFilterBlockTest, MultiGetSpanningPartitions) {
  // A small metadata block size cuts a filter partition after (almost) every
  // key, forcing the range to span several partitions.
  table_options_.metadata_block_size = 1;
  std::unique_ptr<PartitionedIndexBuilder> pib(NewIndexBuilder());
  std::unique_ptr<PartitionedFilterBlockBuilder> builder(NewBuilder(pib.get()));

  std::vector<std::string> keys = PrepareKeys(keys_without_ts, kKeyNum);
  int i = 0;
  builder->Add(StripTimestampFromUserKey(keys[i], ts_sz_));
  CutABlock(pib.get(), keys[i], keys[i + 1]);
  i++;
  builder->Add(StripTimestampFromUserKey(keys[i], ts_sz_));
  CutABlock(pib.get(), keys[i], keys[i + 1]);
  i++;
  builder->Add(StripTimestampFromUserKey(keys[i], ts_sz_));
  builder->Add(StripTimestampFromUserKey(keys[i], ts_sz_));
  CutABlock(pib.get(), keys[i], keys[i + 1]);
  i++;
  builder->Add(StripTimestampFromUserKey(keys[i], ts_sz_));
  CutABlock(pib.get(), keys[i]);

  std::unique_ptr<PartitionedFilterBlockReader> reader(
      NewReader(builder.get(), pib.get()));

  // Every added key must be reported as possibly present; a false negative
  // here would mean a wrong filter partition was consulted for that key.
  const std::vector<std::string> added_keys(keys_without_ts,
                                            keys_without_ts + kKeyNum);
  const std::set<std::string> expected(added_keys.begin(), added_keys.end());
  ASSERT_EQ(MultiGetMayMatch(reader.get(), added_keys), expected);

  // Keys that were never added should be filtered out (assuming a good hash
  // function).
  const std::vector<std::string> missing_keys(
      missing_keys_without_ts, missing_keys_without_ts + kMissingKeyNum);
  ASSERT_TRUE(MultiGetMayMatch(reader.get(), missing_keys).empty());

  // Confirm the range really did span at least 3 filter partitions.
  ASSERT_GE(CountNumOfIndexPartitions(pib.get()), 3);
}

TEST_P(PartitionedFilterBlockTest, OneBlockPerKey) {
  uint64_t max_index_size = MaxIndexSize();
  for (uint64_t i = 1; i < max_index_size + 1; i++) {
    table_options_.metadata_block_size = i;
    TestBlockPerKey();
  }
}

TEST_P(PartitionedFilterBlockTest, PartitionCount) {
  table_options_.metadata_block_size =
      std::max(MaxIndexSize(), MaxFilterSize());
  int partitions = TestBlockPerKey();
  ASSERT_EQ(partitions, 1);
  // A low number ensures cutting a block after each key
  table_options_.metadata_block_size = 1;
  partitions = TestBlockPerKey();
  ASSERT_EQ(partitions, kKeyNum - 1 /* last two keys make one flush */);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
