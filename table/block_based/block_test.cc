//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "table/block_based/block.h"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_builder.h"
#include "table/block_based/data_block_footer.h"
#include "table/format.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/coding.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

std::string GenerateInternalKey(int primary_key, int secondary_key,
                                int padding_size, Random* rnd,
                                size_t ts_sz = 0) {
  char buf[50];
  char* p = &buf[0];
  snprintf(buf, sizeof(buf), "%6d%4d", primary_key, secondary_key);
  std::string k(p);
  if (padding_size) {
    k += rnd->RandomString(padding_size);
  }
  AppendInternalKeyFooter(&k, 0 /* seqno */, kTypeValue);
  std::string key_with_ts;
  if (ts_sz > 0) {
    PadInternalKeyWithMinTimestamp(&key_with_ts, k, ts_sz);
    return key_with_ts;
  }

  return k;
}

// Generate random key value pairs.
// The generated key will be sorted. You can tune the parameters to generated
// different kinds of test key/value pairs for different scenario.
void GenerateRandomKVs(std::vector<std::string>* keys,
                       std::vector<std::string>* values, const int from,
                       const int len, const int step = 1,
                       const int padding_size = 0,
                       const int keys_share_prefix = 1, size_t ts_sz = 0) {
  Random rnd(302);

  // generate different prefix
  for (int i = from; i < from + len; i += step) {
    // generating keys that shares the prefix
    for (int j = 0; j < keys_share_prefix; ++j) {
      // `DataBlockIter` assumes it reads only internal keys.
      keys->emplace_back(GenerateInternalKey(i, j, padding_size, &rnd, ts_sz));

      // 100 bytes values
      values->emplace_back(rnd.RandomString(100));
    }
  }
}

// Test Param 0): key use delta encoding.
// Test Param 1): user-defined timestamp test mode.
// Test Param 2): data block index type.
// Test Param 3): restart interval.
// Test Param 4): use separated KV storage.
class BlockTest
    : public testing::Test,
      public testing::WithParamInterface<std::tuple<
          bool, test::UserDefinedTimestampTestMode,
          BlockBasedTableOptions::DataBlockIndexType, uint32_t, bool>> {
 public:
  bool keyUseDeltaEncoding() const { return std::get<0>(GetParam()); }
  bool isUDTEnabled() const {
    return test::IsUDTEnabled(std::get<1>(GetParam()));
  }
  bool shouldPersistUDT() const {
    return test::ShouldPersistUDT(std::get<1>(GetParam()));
  }

  BlockBasedTableOptions::DataBlockIndexType dataBlockIndexType() const {
    return std::get<2>(GetParam());
  }

  uint32_t getRestartInterval() const { return std::get<3>(GetParam()); }

  bool useSeparatedKVStorage() const { return std::get<4>(GetParam()); }
};

// block test
TEST_P(BlockTest, SimpleTest) {
  Random rnd(301);
  Options options = Options();
  if (isUDTEnabled()) {
    options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  }
  size_t ts_sz = options.comparator->timestamp_size();

  std::vector<std::string> keys;
  std::vector<std::string> values;
  BlockBasedTableOptions::DataBlockIndexType index_type =
      isUDTEnabled() ? BlockBasedTableOptions::kDataBlockBinarySearch
                     : dataBlockIndexType();
  BlockBuilder builder(
      static_cast<int>(getRestartInterval()), keyUseDeltaEncoding(),
      false /* use_value_delta_encoding */, index_type,
      0.75 /* data_block_hash_table_util_ratio */, ts_sz, shouldPersistUDT(),
      false /* is_user_key */, useSeparatedKVStorage(),
      nullptr /* statistics */, -1.0 /* uniform_cv_threshold */,
      false /* use_common_prefix */);
  int num_records = 20;

  GenerateRandomKVs(&keys, &values, 0, num_records, 1 /* step */,
                    0 /* padding_size */, 1 /* keys_share_prefix */, ts_sz);
  // add a bunch of records to a block
  for (int i = 0; i < num_records; i++) {
    builder.Add(keys[i], values[i]);
  }

  // read serialized contents of the block
  Slice rawblock = builder.Finish();

  // create block reader
  BlockContents contents;
  contents.data = rawblock;
  Block reader(std::move(contents), 0 /* read_amp_bytes_per_bit */,
               nullptr /* statistics */, getRestartInterval());

  // read contents of block sequentially
  int count = 0;
  InternalIterator* iter = reader.NewDataIterator(
      options.comparator, kDisableGlobalSequenceNumber, nullptr /* iter */,
      nullptr /* stats */, false /* block_contents_pinned */,
      shouldPersistUDT());
  for (iter->SeekToFirst(); iter->Valid(); count++, iter->Next()) {
    // read kv from block
    Slice k = iter->key();
    Slice v = iter->value();

    // compare with lookaside array
    ASSERT_EQ(k.ToString().compare(keys[count]), 0);
    ASSERT_EQ(v.ToString().compare(values[count]), 0);
  }
  delete iter;

  // read block contents randomly
  iter = reader.NewDataIterator(
      options.comparator, kDisableGlobalSequenceNumber, nullptr /* iter */,
      nullptr /* stats */, false /* block_contents_pinned */,
      shouldPersistUDT());
  for (int i = 0; i < num_records; i++) {
    // find a random key in the lookaside array
    int index = rnd.Uniform(num_records);
    Slice k(keys[index]);

    // search in block for this key
    iter->Seek(k);
    ASSERT_TRUE(iter->Valid());
    Slice v = iter->value();
    ASSERT_EQ(v.ToString().compare(values[index]), 0);
  }
  delete iter;
}

TEST(BlockCommonPrefixTest, PrefixDoesNotIncludeInternalKeyFooter) {
  const SequenceNumber sequence = 0x123456789abc;
  for (const std::string& user_key : {std::string(), std::string("user1")}) {
    for (ValueType type :
         {kTypeValue, kTypeDeletion, kTypeMerge, kTypeSingleDeletion}) {
      std::string key = user_key;
      AppendInternalKeyFooter(&key, sequence, type);
      for (size_t extension_size : {size_t{1}, size_t{8}}) {
        std::string neighbor = user_key;
        neighbor.append(key.data() + user_key.size(), extension_size);
        AppendInternalKeyFooter(&neighbor, sequence + 1, kTypeValue);
        for (int restart_interval : {1, 16}) {
          for (bool separated : {false, true}) {
            SCOPED_TRACE(testing::Message()
                         << "user_key_size=" << user_key.size() << " type="
                         << type << " extension_size=" << extension_size
                         << " restart_interval=" << restart_interval
                         << " separated=" << separated);
            BlockBuilder builder(
                restart_interval, true /* use_delta_encoding */,
                false /* use_value_delta_encoding */,
                BlockBasedTableOptions::kDataBlockBinarySearch, 0.75,
                0 /* ts_sz */, true /* persist_user_defined_timestamps */,
                false /* is_user_key */, separated, nullptr /* statistics */,
                -1.0 /* uniform_cv_threshold */, true /* use_common_prefix */);
            builder.Add(neighbor, "neighbor");
            builder.Add(key, "value");
            Slice data = builder.Finish();
            DataBlockFooter footer;
            ASSERT_OK(footer.DecodeFrom(&data));
            const uint32_t prefix_size =
                DecodeFixed32(data.data() + data.size() -
                              footer.num_restarts * sizeof(uint32_t));
            ASSERT_EQ(user_key.size(), prefix_size);
          }
        }
      }
    }
  }
}

namespace {

// Immutable pre-fix format-v8 block bytes. These are derived from the legacy
// encoding, not generated with the current BlockBuilder. The two-byte values
// are ASCII in data-block tests and valid varint BlockHandles in index tests.
const char kLegacyValueTagOverlapRestartBoundary[] =
    "\x75\x73\x65\x72\x31\x01"
    "\x00\x09\x02\x7a\x01\x09\x00\x00\x00\x00\x00\x00\x76\x30"
    "\x06\x09\x02\x79\x01\x08\x00\x00\x00\x00\x00\x00\x76\x31"
    "\x00\x08\x02\x01\x03\x00\x00\x00\x00\x00\x00\x76\x32"
    "\x06\x07\x02\x02\x00\x00\x00\x00\x00\x00\x76\x33"
    "\x06\x00\x00\x00\x22\x00\x00\x00\x02\x00\x00\x00";
static_assert(sizeof(kLegacyValueTagOverlapRestartBoundary) - 1 == 71);

const char kLegacyDeleteTagOverlap[] =
    "\x75\x73\x65\x72\x31\x00"
    "\x00\x08\x02\x01\x03\x00\x00\x00\x00\x00\x00\x76\x30"
    "\x06\x07\x02\x02\x00\x00\x00\x00\x00\x00\x76\x31"
    "\x06\x00\x00\x00\x01\x00\x00\x00";
static_assert(sizeof(kLegacyDeleteTagOverlap) - 1 == 39);

const char kLegacyEmptyUserFullFooterOverlap[] =
    "\x01\x00\x00\x00\x00\x00\x00\x00"
    "\x00\x08\x02\x01\x01\x00\x00\x00\x00\x00\x00\x6e\x30"
    "\x08\x00\x02\x76\x30"
    "\x08\x00\x00\x00\x01\x00\x00\x00";
static_assert(sizeof(kLegacyEmptyUserFullFooterOverlap) - 1 == 34);

// The 40-byte stored prefix contains a 32-byte user key and its whole footer.
// This makes the guaranteed user-key prefix exactly 32 bytes, not 33.
const char kLegacyLongUserFullFooterOverlap[] =
    "\x70\x70\x70\x70\x70\x70\x70\x70\x70\x70\x70\x70\x70\x70\x70\x70"
    "\x70\x70\x70\x70\x70\x70\x70\x70\x70\x70\x70\x70\x70\x70\x70\x70"
    "\x01\x00\x00\x00\x00\x00\x00\x00"
    "\x00\x08\x02\x01\x01\x00\x00\x00\x00\x00\x00\x6e\x30"
    "\x28\x00\x02\x76\x30"
    "\x28\x00\x00\x00\x01\x00\x00\x00";
static_assert(sizeof(kLegacyLongUserFullFooterOverlap) - 1 == 66);

struct LegacyCommonPrefixBlock {
  const char* name;
  const char* bytes;
  size_t size;
  uint32_t restart_interval;
  std::vector<std::pair<std::string, std::string>> entries;
  std::vector<std::string> seek_targets;
};

std::vector<LegacyCommonPrefixBlock> GetLegacyCommonPrefixBlocks() {
  const std::string empty_footer_user("\x01\x00\x00\x00\x00\x00\x00\x00", 8);
  const std::string long_user(32, 'p');
  const std::string long_footer_user = long_user + empty_footer_user;
  return {
      {"value_tag_overlap_restart_boundary",
       kLegacyValueTagOverlapRestartBoundary,
       sizeof(kLegacyValueTagOverlapRestartBoundary) - 1,
       2,
       {{InternalKey(std::string("user1\x01z", 7), 9, kTypeValue)
             .Encode()
             .ToString(),
         "v0"},
        {InternalKey(std::string("user1\x01y", 7), 8, kTypeValue)
             .Encode()
             .ToString(),
         "v1"},
        {InternalKey(std::string("user1\x01", 6), 3, kTypeValue)
             .Encode()
             .ToString(),
         "v2"},
        {InternalKey("user1", 2, kTypeValue).Encode().ToString(), "v3"}},
       {InternalKey("user1", kMaxSequenceNumber, kValueTypeForSeek)
            .Encode()
            .ToString(),
        InternalKey(std::string("user1\x01", 6), 2, kTypeValue)
            .Encode()
            .ToString(),
        InternalKey(std::string("user1\x02", 6), 5, kTypeValue)
            .Encode()
            .ToString(),
        InternalKey(std::string("user1\x01z", 7), 9, kTypeValue)
            .Encode()
            .ToString()}},
      {"delete_tag_overlap",
       kLegacyDeleteTagOverlap,
       sizeof(kLegacyDeleteTagOverlap) - 1,
       2,
       {{InternalKey(std::string("user1\x00", 6), 3, kTypeValue)
             .Encode()
             .ToString(),
         "v0"},
        {InternalKey("user1", 2, kTypeDeletion).Encode().ToString(), "v1"}},
       {InternalKey("user1", kMaxSequenceNumber, kValueTypeForSeek)
            .Encode()
            .ToString(),
        InternalKey(std::string("user1\x00", 6), 2, kTypeValue)
            .Encode()
            .ToString()}},
      {"empty_user_full_footer_overlap",
       kLegacyEmptyUserFullFooterOverlap,
       sizeof(kLegacyEmptyUserFullFooterOverlap) - 1,
       16,
       {{InternalKey(empty_footer_user, 1, kTypeValue).Encode().ToString(),
         "n0"},
        {InternalKey("", 0, kTypeValue).Encode().ToString(), "v0"}},
       {InternalKey(empty_footer_user, 0, kTypeValue).Encode().ToString(),
        InternalKey("", 0, kTypeValue).Encode().ToString()}},
      {"long_user_full_footer_overlap",
       kLegacyLongUserFullFooterOverlap,
       sizeof(kLegacyLongUserFullFooterOverlap) - 1,
       16,
       {{InternalKey(long_footer_user, 1, kTypeValue).Encode().ToString(),
         "n0"},
        {InternalKey(long_user, 0, kTypeValue).Encode().ToString(), "v0"}},
       {InternalKey(long_user, kMaxSequenceNumber, kValueTypeForSeek)
            .Encode()
            .ToString(),
        InternalKey(long_user, 0, kTypeValue).Encode().ToString(),
        InternalKey(long_footer_user, 0, kTypeValue).Encode().ToString(),
        // Mismatches before the guaranteed boundary can use the shortcut.
        InternalKey(std::string(31, 'p') + 'q', 0, kTypeValue)
            .Encode()
            .ToString(),
        InternalKey(std::string(31, 'p') + 'o', 0, kTypeValue)
            .Encode()
            .ToString(),
        // A mismatch at that boundary can still precede the shorter key.
        InternalKey(long_user + '\x00', 0, kTypeValue).Encode().ToString(),
        InternalKey(long_user + '\x02', 0, kTypeValue).Encode().ToString()}},
  };
}

}  // namespace

TEST(BlockCommonPrefixTest, LegacyDataBlocksPreserveSeekAndIteration) {
  const std::vector<LegacyCommonPrefixBlock> fixtures =
      GetLegacyCommonPrefixBlocks();
  const InternalKeyComparator comparator(ReverseBytewiseComparator());
  for (const LegacyCommonPrefixBlock& fixture : fixtures) {
    SCOPED_TRACE(fixture.name);
    BlockContents contents;
    contents.data = Slice(fixture.bytes, fixture.size);
    Block block(std::move(contents), 0 /* read_amp_bytes_per_bit */,
                nullptr /* statistics */, fixture.restart_interval);
    std::unique_ptr<DataBlockIter> iter(block.NewDataIterator(
        ReverseBytewiseComparator(), kDisableGlobalSequenceNumber, nullptr,
        nullptr, true /* block_contents_pinned */,
        true /* user_defined_timestamps_persisted */));

    ASSERT_OK(iter->status());
    for (size_t i = 1; i < fixture.entries.size(); ++i) {
      ASSERT_LE(comparator.Compare(fixture.entries[i - 1].first,
                                   fixture.entries[i].first),
                0);
    }

    for (const std::string& target : fixture.seek_targets) {
      size_t seek_index = fixture.entries.size();
      size_t seek_for_prev_index = fixture.entries.size();
      for (size_t i = 0; i < fixture.entries.size(); ++i) {
        const int cmp = comparator.Compare(fixture.entries[i].first, target);
        if (seek_index == fixture.entries.size() && cmp >= 0) {
          seek_index = i;
        }
        if (cmp <= 0) {
          seek_for_prev_index = i;
        }
      }

      iter->Seek(target);
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->Valid(), seek_index < fixture.entries.size());
      if (seek_index < fixture.entries.size()) {
        EXPECT_EQ(fixture.entries[seek_index].first, iter->key().ToString());
        EXPECT_EQ(fixture.entries[seek_index].second, iter->value().ToString());
        if (seek_index > 0) {
          iter->Prev();
          ASSERT_OK(iter->status());
          ASSERT_TRUE(iter->Valid());
          EXPECT_EQ(fixture.entries[seek_index - 1].first,
                    iter->key().ToString());
          iter->Next();
          ASSERT_OK(iter->status());
          ASSERT_TRUE(iter->Valid());
          EXPECT_EQ(fixture.entries[seek_index].first, iter->key().ToString());
        }
      }

      iter->SeekForPrev(target);
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->Valid(), seek_for_prev_index < fixture.entries.size());
      if (seek_for_prev_index < fixture.entries.size()) {
        EXPECT_EQ(fixture.entries[seek_for_prev_index].first,
                  iter->key().ToString());
        EXPECT_EQ(fixture.entries[seek_for_prev_index].second,
                  iter->value().ToString());
        if (seek_for_prev_index + 1 < fixture.entries.size()) {
          iter->Next();
          ASSERT_OK(iter->status());
          ASSERT_TRUE(iter->Valid());
          EXPECT_EQ(fixture.entries[seek_for_prev_index + 1].first,
                    iter->key().ToString());
          iter->Prev();
          ASSERT_OK(iter->status());
          ASSERT_TRUE(iter->Valid());
          EXPECT_EQ(fixture.entries[seek_for_prev_index].first,
                    iter->key().ToString());
        }
      }
    }

    iter->SeekToFirst();
    for (const auto& entry : fixture.entries) {
      ASSERT_OK(iter->status());
      ASSERT_TRUE(iter->Valid());
      EXPECT_EQ(entry.first, iter->key().ToString());
      EXPECT_EQ(entry.second, iter->value().ToString());
      iter->Next();
    }
    ASSERT_OK(iter->status());
    EXPECT_FALSE(iter->Valid());

    iter->SeekToLast();
    for (auto entry = fixture.entries.rbegin(); entry != fixture.entries.rend();
         ++entry) {
      ASSERT_OK(iter->status());
      ASSERT_TRUE(iter->Valid());
      EXPECT_EQ(entry->first, iter->key().ToString());
      EXPECT_EQ(entry->second, iter->value().ToString());
      iter->Prev();
    }
    ASSERT_OK(iter->status());
    EXPECT_FALSE(iter->Valid());
  }
}

TEST(BlockCommonPrefixTest, LegacyInternalKeyIndexBlockPreservesSeek) {
  const InternalKeyComparator comparator(ReverseBytewiseComparator());
  const std::vector<LegacyCommonPrefixBlock> fixtures =
      GetLegacyCommonPrefixBlocks();
  for (const LegacyCommonPrefixBlock& fixture : fixtures) {
    SCOPED_TRACE(fixture.name);
    BlockContents contents;
    contents.data = Slice(fixture.bytes, fixture.size);
    Block block(std::move(contents), 0 /* read_amp_bytes_per_bit */,
                nullptr /* statistics */, fixture.restart_interval);
    std::unique_ptr<IndexBlockIter> iter(block.NewIndexIterator(
        ReverseBytewiseComparator(), kDisableGlobalSequenceNumber, nullptr,
        nullptr, true /* total_order_seek */, false /* have_first_key */,
        true /* key_includes_seq */, true /* value_is_full */,
        true /* block_contents_pinned */,
        true /* user_defined_timestamps_persisted */,
        nullptr /* prefix_index */, BlockBasedTableOptions::kBinary,
        false /* value_delta_escape */));

    ASSERT_OK(iter->status());
    for (const std::string& target : fixture.seek_targets) {
      size_t seek_index = fixture.entries.size();
      for (size_t i = 0; i < fixture.entries.size(); ++i) {
        if (comparator.Compare(fixture.entries[i].first, target) >= 0) {
          seek_index = i;
          break;
        }
      }

      iter->Seek(target);
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->Valid(), seek_index < fixture.entries.size());
      if (seek_index < fixture.entries.size()) {
        EXPECT_EQ(fixture.entries[seek_index].first, iter->key().ToString());
        const IndexValue value = iter->value();
        EXPECT_EQ(static_cast<uint64_t>(fixture.entries[seek_index].second[0]),
                  value.handle.offset());
        EXPECT_EQ(static_cast<uint64_t>(fixture.entries[seek_index].second[1]),
                  value.handle.size());
      }
    }

    iter->SeekToFirst();
    for (const auto& entry : fixture.entries) {
      ASSERT_OK(iter->status());
      ASSERT_TRUE(iter->Valid());
      EXPECT_EQ(entry.first, iter->key().ToString());
      const IndexValue value = iter->value();
      EXPECT_EQ(static_cast<uint64_t>(entry.second[0]), value.handle.offset());
      EXPECT_EQ(static_cast<uint64_t>(entry.second[1]), value.handle.size());
      iter->Next();
    }
    ASSERT_OK(iter->status());
    EXPECT_FALSE(iter->Valid());
  }
}

// return the block contents
BlockContents GetBlockContents(
    std::unique_ptr<BlockBuilder>* builder,
    const std::vector<std::string>& keys,
    const std::vector<std::string>& values, bool key_use_delta_encoding,
    size_t ts_sz, bool should_persist_udt, const int /*prefix_group_size*/ = 1,
    BlockBasedTableOptions::DataBlockIndexType dblock_index_type =
        BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch,
    bool use_separated_kv_storage = false, uint32_t restart_interval = 1) {
  builder->reset(new BlockBuilder(
      static_cast<int>(restart_interval), key_use_delta_encoding,
      false /* use_value_delta_encoding */, dblock_index_type,
      0.75 /* data_block_hash_table_util_ratio */, ts_sz, should_persist_udt,
      false /* is_user_key */, use_separated_kv_storage, /*statistics=*/nullptr,
      /*uniform_cv_threshold=*/-1.0, /*use_common_prefix=*/false));

  // Add only half of the keys
  for (size_t i = 0; i < keys.size(); ++i) {
    (*builder)->Add(keys[i], values[i]);
  }
  Slice rawblock = (*builder)->Finish();

  BlockContents contents;
  contents.data = rawblock;

  return contents;
}

void CheckBlockContents(BlockContents contents, const int max_key,
                        const std::vector<std::string>& keys,
                        const std::vector<std::string>& values,
                        bool is_udt_enabled, bool should_persist_udt,
                        uint32_t restart_interval) {
  const size_t prefix_size = 6;
  // create block reader
  BlockContents contents_ref(contents.data);
  Block reader1(std::move(contents), 0 /* read_amp_bytes_per_bit */,
                nullptr /* statistics */, restart_interval);
  Block reader2(std::move(contents_ref), 0 /* read_amp_bytes_per_bit */,
                nullptr /* statistics */, restart_interval);

  std::unique_ptr<const SliceTransform> prefix_extractor(
      NewFixedPrefixTransform(prefix_size));

  std::unique_ptr<InternalIterator> regular_iter(reader2.NewDataIterator(
      is_udt_enabled ? test::BytewiseComparatorWithU64TsWrapper()
                     : BytewiseComparator(),
      kDisableGlobalSequenceNumber, nullptr /* iter */, nullptr /* stats */,
      false /* block_contents_pinned */, should_persist_udt));

  // Seek existent keys
  for (size_t i = 0; i < keys.size(); i++) {
    regular_iter->Seek(keys[i]);
    ASSERT_OK(regular_iter->status());
    ASSERT_TRUE(regular_iter->Valid());

    Slice v = regular_iter->value();
    ASSERT_EQ(v.ToString().compare(values[i]), 0);
  }

  // Seek non-existent keys.
  // For hash index, if no key with a given prefix is not found, iterator will
  // simply be set as invalid; whereas the binary search based iterator will
  // return the one that is closest.
  for (int i = 1; i < max_key - 1; i += 2) {
    // `DataBlockIter` assumes its APIs receive only internal keys.
    auto key = GenerateInternalKey(i, 0, 0, nullptr,
                                   is_udt_enabled ? 8 : 0 /* ts_sz */);
    regular_iter->Seek(key);
    ASSERT_TRUE(regular_iter->Valid());
  }
}

// In this test case, no two key share same prefix.
TEST_P(BlockTest, SimpleIndexHash) {
  const int kMaxKey = 100000;
  size_t ts_sz = isUDTEnabled() ? 8 : 0;
  std::vector<std::string> keys;
  std::vector<std::string> values;
  GenerateRandomKVs(&keys, &values, 0 /* first key id */,
                    kMaxKey /* last key id */, 2 /* step */,
                    8 /* padding size (8 bytes randomly generated suffix) */,
                    1 /* keys_share_prefix */, ts_sz);

  std::unique_ptr<BlockBuilder> builder;

  auto contents = GetBlockContents(
      &builder, keys, values, keyUseDeltaEncoding(), ts_sz, shouldPersistUDT(),
      1 /* prefix_group_size */,
      isUDTEnabled()
          ? BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch
          : dataBlockIndexType(),
      useSeparatedKVStorage(), getRestartInterval());

  CheckBlockContents(std::move(contents), kMaxKey, keys, values, isUDTEnabled(),
                     shouldPersistUDT(), getRestartInterval());
}

TEST_P(BlockTest, IndexHashWithSharedPrefix) {
  const int kMaxKey = 100000;
  // for each prefix, there will be 5 keys starts with it.
  const int kPrefixGroup = 5;
  size_t ts_sz = isUDTEnabled() ? 8 : 0;
  std::vector<std::string> keys;
  std::vector<std::string> values;
  // Generate keys with same prefix.
  GenerateRandomKVs(&keys, &values, 0,  // first key id
                    kMaxKey,            // last key id
                    2 /* step */,
                    10 /* padding size (8 bytes randomly generated suffix) */,
                    kPrefixGroup /* keys_share_prefix */, ts_sz);

  std::unique_ptr<BlockBuilder> builder;

  auto contents = GetBlockContents(
      &builder, keys, values, keyUseDeltaEncoding(), ts_sz, shouldPersistUDT(),
      kPrefixGroup,
      isUDTEnabled()
          ? BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch
          : dataBlockIndexType(),
      useSeparatedKVStorage(), getRestartInterval());

  CheckBlockContents(std::move(contents), kMaxKey, keys, values, isUDTEnabled(),
                     shouldPersistUDT(), getRestartInterval());
}

// Param 0: key use delta encoding
// Param 1: user-defined timestamp test mode
// Param 2: data block index type. User-defined timestamp feature is not
// compatible with `kDataBlockBinaryAndHash` data block index type because the
// user comparator doesn't provide a `CanKeysWithDifferentByteContentsBeEqual`
// override. This combination is disabled.
// Param 3: restart interval
// Param 4: use separated KV storage
INSTANTIATE_TEST_CASE_P(
    P, BlockTest,
    ::testing::Combine(
        ::testing::Bool(), ::testing::ValuesIn(test::GetUDTTestModes()),
        ::testing::Values(
            BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch,
            BlockBasedTableOptions::DataBlockIndexType::
                kDataBlockBinaryAndHash),
        ::testing::Values(1, 8, 16), ::testing::Bool()));

// A slow and accurate version of BlockReadAmpBitmap that simply store
// all the marked ranges in a set.
class BlockReadAmpBitmapSlowAndAccurate {
 public:
  void Mark(size_t start_offset, size_t end_offset) {
    assert(end_offset >= start_offset);
    marked_ranges_.emplace(end_offset, start_offset);
  }

  void ResetCheckSequence() { iter_valid_ = false; }

  // Return true if any byte in this range was Marked
  // This does linear search from the previous position. When calling
  // multiple times, `offset` needs to be incremental to get correct results.
  // Call ResetCheckSequence() to reset it.
  bool IsPinMarked(size_t offset) {
    if (iter_valid_) {
      // Has existing iterator, try linear search from
      // the iterator.
      for (int i = 0; i < 64; i++) {
        if (offset < iter_->second) {
          return false;
        }
        if (offset <= iter_->first) {
          return true;
        }

        iter_++;
        if (iter_ == marked_ranges_.end()) {
          iter_valid_ = false;
          return false;
        }
      }
    }
    // Initial call or have linear searched too many times.
    // Do binary search.
    iter_ = marked_ranges_.lower_bound(
        std::make_pair(offset, static_cast<size_t>(0)));
    if (iter_ == marked_ranges_.end()) {
      iter_valid_ = false;
      return false;
    }
    iter_valid_ = true;
    return offset <= iter_->first && offset >= iter_->second;
  }

 private:
  std::set<std::pair<size_t, size_t>> marked_ranges_;
  std::set<std::pair<size_t, size_t>>::iterator iter_;
  bool iter_valid_ = false;
};

TEST_F(BlockTest, BlockReadAmpBitmap) {
  uint32_t pin_offset = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "BlockReadAmpBitmap:rnd", [&pin_offset](void* arg) {
        pin_offset = *(static_cast<uint32_t*>(arg));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  std::vector<size_t> block_sizes = {
      1,                // 1 byte
      32,               // 32 bytes
      61,               // 61 bytes
      64,               // 64 bytes
      512,              // 0.5 KB
      1024,             // 1 KB
      1024 * 4,         // 4 KB
      1024 * 10,        // 10 KB
      1024 * 50,        // 50 KB
      1024 * 1024 * 4,  // 5 MB
      777,
      124653,
  };
  const size_t kBytesPerBit = 64;

  Random rnd(301);
  for (size_t block_size : block_sizes) {
    std::shared_ptr<Statistics> stats = ROCKSDB_NAMESPACE::CreateDBStatistics();
    BlockReadAmpBitmap read_amp_bitmap(block_size, kBytesPerBit, stats.get());
    BlockReadAmpBitmapSlowAndAccurate read_amp_slow_and_accurate;

    size_t needed_bits = (block_size / kBytesPerBit);
    if (block_size % kBytesPerBit != 0) {
      needed_bits++;
    }

    ASSERT_EQ(stats->getTickerCount(READ_AMP_TOTAL_READ_BYTES), block_size);

    // Generate some random entries
    std::vector<size_t> random_entry_offsets;
    for (int i = 0; i < 1000; i++) {
      random_entry_offsets.push_back(rnd.Next() % block_size);
    }
    std::sort(random_entry_offsets.begin(), random_entry_offsets.end());
    auto it =
        std::unique(random_entry_offsets.begin(), random_entry_offsets.end());
    random_entry_offsets.resize(
        std::distance(random_entry_offsets.begin(), it));

    std::vector<std::pair<size_t, size_t>> random_entries;
    for (size_t i = 0; i < random_entry_offsets.size(); i++) {
      size_t entry_start = random_entry_offsets[i];
      size_t entry_end;
      if (i + 1 < random_entry_offsets.size()) {
        entry_end = random_entry_offsets[i + 1] - 1;
      } else {
        entry_end = block_size - 1;
      }
      random_entries.emplace_back(entry_start, entry_end);
    }

    for (size_t i = 0; i < random_entries.size(); i++) {
      read_amp_slow_and_accurate.ResetCheckSequence();
      auto& current_entry = random_entries[rnd.Next() % random_entries.size()];

      read_amp_bitmap.Mark(static_cast<uint32_t>(current_entry.first),
                           static_cast<uint32_t>(current_entry.second));
      read_amp_slow_and_accurate.Mark(current_entry.first,
                                      current_entry.second);

      size_t total_bits = 0;
      for (size_t bit_idx = 0; bit_idx < needed_bits; bit_idx++) {
        total_bits += read_amp_slow_and_accurate.IsPinMarked(
            bit_idx * kBytesPerBit + pin_offset);
      }
      size_t expected_estimate_useful = total_bits * kBytesPerBit;
      size_t got_estimate_useful =
          stats->getTickerCount(READ_AMP_ESTIMATE_USEFUL_BYTES);
      ASSERT_EQ(expected_estimate_useful, got_estimate_useful);
    }
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(BlockTest, BlockWithReadAmpBitmap) {
  Random rnd(301);
  Options options = Options();

  std::vector<std::string> keys;
  std::vector<std::string> values;
  BlockBuilder builder(BlockBuilder::ForMetaBlock{}, 16);
  int num_records = 10000;

  GenerateRandomKVs(&keys, &values, 0, num_records, 1 /* step */);
  // add a bunch of records to a block
  for (int i = 0; i < num_records; i++) {
    builder.Add(keys[i], values[i]);
  }

  Slice rawblock = builder.Finish();
  const size_t kBytesPerBit = 8;

  // Read the block sequentially using Next()
  {
    std::shared_ptr<Statistics> stats = ROCKSDB_NAMESPACE::CreateDBStatistics();

    // create block reader
    BlockContents contents;
    contents.data = rawblock;
    Block reader(std::move(contents), kBytesPerBit, stats.get());

    // read contents of block sequentially
    size_t read_bytes = 0;
    DataBlockIter* iter = reader.NewDataIterator(
        options.comparator, kDisableGlobalSequenceNumber, nullptr, stats.get());
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      iter->value();
      read_bytes += iter->TEST_CurrentEntrySize();

      double semi_acc_read_amp =
          static_cast<double>(read_bytes) / rawblock.size();
      double read_amp = static_cast<double>(stats->getTickerCount(
                            READ_AMP_ESTIMATE_USEFUL_BYTES)) /
                        stats->getTickerCount(READ_AMP_TOTAL_READ_BYTES);

      // Error in read amplification will be less than 1% if we are reading
      // sequentially
      double error_pct = fabs(semi_acc_read_amp - read_amp) * 100;
      EXPECT_LT(error_pct, 1);
    }

    delete iter;
  }

  // Read the block sequentially using Seek()
  {
    std::shared_ptr<Statistics> stats = ROCKSDB_NAMESPACE::CreateDBStatistics();

    // create block reader
    BlockContents contents;
    contents.data = rawblock;
    Block reader(std::move(contents), kBytesPerBit, stats.get());

    size_t read_bytes = 0;
    DataBlockIter* iter = reader.NewDataIterator(
        options.comparator, kDisableGlobalSequenceNumber, nullptr, stats.get());
    for (int i = 0; i < num_records; i++) {
      Slice k(keys[i]);

      // search in block for this key
      iter->Seek(k);
      iter->value();
      read_bytes += iter->TEST_CurrentEntrySize();

      double semi_acc_read_amp =
          static_cast<double>(read_bytes) / rawblock.size();
      double read_amp = static_cast<double>(stats->getTickerCount(
                            READ_AMP_ESTIMATE_USEFUL_BYTES)) /
                        stats->getTickerCount(READ_AMP_TOTAL_READ_BYTES);

      // Error in read amplification will be less than 1% if we are reading
      // sequentially
      double error_pct = fabs(semi_acc_read_amp - read_amp) * 100;
      EXPECT_LT(error_pct, 1);
    }
    delete iter;
  }

  // Read the block randomly
  {
    std::shared_ptr<Statistics> stats = ROCKSDB_NAMESPACE::CreateDBStatistics();

    // create block reader
    BlockContents contents;
    contents.data = rawblock;
    Block reader(std::move(contents), kBytesPerBit, stats.get());

    size_t read_bytes = 0;
    DataBlockIter* iter = reader.NewDataIterator(
        options.comparator, kDisableGlobalSequenceNumber, nullptr, stats.get());
    std::unordered_set<int> read_keys;
    for (int i = 0; i < num_records; i++) {
      int index = rnd.Uniform(num_records);
      Slice k(keys[index]);

      iter->Seek(k);
      iter->value();
      if (read_keys.find(index) == read_keys.end()) {
        read_keys.insert(index);
        read_bytes += iter->TEST_CurrentEntrySize();
      }

      double semi_acc_read_amp =
          static_cast<double>(read_bytes) / rawblock.size();
      double read_amp = static_cast<double>(stats->getTickerCount(
                            READ_AMP_ESTIMATE_USEFUL_BYTES)) /
                        stats->getTickerCount(READ_AMP_TOTAL_READ_BYTES);

      double error_pct = fabs(semi_acc_read_amp - read_amp) * 100;
      // Error in read amplification will be less than 2% if we are reading
      // randomly
      EXPECT_LT(error_pct, 2);
    }
    delete iter;
  }
}

TEST_F(BlockTest, ReadAmpBitmapPow2) {
  std::shared_ptr<Statistics> stats = ROCKSDB_NAMESPACE::CreateDBStatistics();
  ASSERT_EQ(BlockReadAmpBitmap(100, 1, stats.get()).GetBytesPerBit(), 1u);
  ASSERT_EQ(BlockReadAmpBitmap(100, 2, stats.get()).GetBytesPerBit(), 2u);
  ASSERT_EQ(BlockReadAmpBitmap(100, 4, stats.get()).GetBytesPerBit(), 4u);
  ASSERT_EQ(BlockReadAmpBitmap(100, 8, stats.get()).GetBytesPerBit(), 8u);
  ASSERT_EQ(BlockReadAmpBitmap(100, 16, stats.get()).GetBytesPerBit(), 16u);
  ASSERT_EQ(BlockReadAmpBitmap(100, 32, stats.get()).GetBytesPerBit(), 32u);

  ASSERT_EQ(BlockReadAmpBitmap(100, 3, stats.get()).GetBytesPerBit(), 2u);
  ASSERT_EQ(BlockReadAmpBitmap(100, 7, stats.get()).GetBytesPerBit(), 4u);
  ASSERT_EQ(BlockReadAmpBitmap(100, 11, stats.get()).GetBytesPerBit(), 8u);
  ASSERT_EQ(BlockReadAmpBitmap(100, 17, stats.get()).GetBytesPerBit(), 16u);
  ASSERT_EQ(BlockReadAmpBitmap(100, 33, stats.get()).GetBytesPerBit(), 32u);
  ASSERT_EQ(BlockReadAmpBitmap(100, 35, stats.get()).GetBytesPerBit(), 32u);
}

void AddIndexBlockEntry(BlockBuilder& builder, const Slice& key,
                        const BlockHandle& bh, const BlockHandle* prev,
                        bool include_first_key,
                        const Slice& first_internal_key = Slice()) {
  IndexValue entry(bh, first_internal_key);
  std::string encoded_entry;
  entry.EncodeTo(&encoded_entry, include_first_key, nullptr);
  std::string delta_encoded_entry;
  if (prev) {
    entry.EncodeTo(&delta_encoded_entry, include_first_key, prev);
  }
  const Slice delta_slice(delta_encoded_entry);
  builder.Add(key, encoded_entry, delta_slice);
}

// Directly exercises the format_version >= 8 index value-delta escape codec
// (IndexValue::EncodeTo/DecodeFrom), independent of block/table machinery.
// See table/format.h FormatVersionUsesValueDeltaEscape.
TEST(IndexValueDeltaEscapeTest, EncodeDecodeRoundTrip) {
  constexpr uint64_t kTrailer = BlockBasedTable::kBlockTrailerSize;
  // Previous block handle. Size is large enough that prev.size() + delta stays
  // positive for all deltas below.
  const BlockHandle prev(1000, 4 * 1024 * 1024);
  const uint64_t contiguous_offset = prev.offset() + prev.size() + kTrailer;

  // Size deltas relative to prev.size(), including 0 (whose legacy encoding is
  // a bare 0x00 -- the byte fv8 reuses as the escape), negatives, and large
  // ones.
  const int64_t deltas[] = {0,    1,     -1,     5,       -5,        255,
                            -255, 12345, -12345, 1 << 20, -(1 << 20)};
  for (int64_t d : deltas) {
    SCOPED_TRACE("delta=" + std::to_string(d));
    const uint64_t new_size =
        static_cast<uint64_t>(static_cast<int64_t>(prev.size()) + d);
    const BlockHandle contig(contiguous_offset, new_size);
    const IndexValue iv(contig, Slice());

    // format_version <= 7 (escape disabled): bytes must be byte-identical to
    // the legacy PutVarsignedint64(size delta), and must round-trip.
    std::string fv7;
    iv.EncodeTo(&fv7, /*have_first_key=*/false, &prev,
                /*use_value_delta_escape=*/false);
    std::string expected_fv7;
    PutVarsignedint64(&expected_fv7, d);
    EXPECT_EQ(expected_fv7, fv7);
    {
      Slice in(fv7);
      IndexValue dec;
      ASSERT_OK(dec.DecodeFrom(&in, false, &prev, false));
      EXPECT_EQ(contig.offset(), dec.handle.offset());
      EXPECT_EQ(contig.size(), dec.handle.size());
      EXPECT_EQ(0u, in.size());
    }

    // format_version >= 8 (escape enabled): a genuine (contiguous) delta is
    // remapped so it never collides with the single-byte 0x00 escape, and it
    // round-trips.
    std::string fv8;
    iv.EncodeTo(&fv8, false, &prev, /*use_value_delta_escape=*/true);
    EXPECT_FALSE(fv8.size() == 1 && fv8[0] == '\0')
        << "a real delta must not encode to the reserved escape byte";
    {
      Slice in(fv8);
      IndexValue dec;
      ASSERT_OK(dec.DecodeFrom(&in, false, &prev, true));
      EXPECT_EQ(contig.offset(), dec.handle.offset());
      EXPECT_EQ(contig.size(), dec.handle.size());
      EXPECT_EQ(0u, in.size());
    }
  }

  // format_version >= 8 escape: a NON-contiguous handle is encoded as the 0x00
  // escape followed by a full BlockHandle, and decodes back to the exact
  // (offset, size) -- ignoring prev's implied contiguous offset.
  {
    const BlockHandle noncontig(contiguous_offset + 777 /* gap */, 4096);
    const IndexValue iv(noncontig, Slice());
    std::string fv8;
    iv.EncodeTo(&fv8, false, &prev, /*use_value_delta_escape=*/true);
    ASSERT_GE(fv8.size(), 1u);
    EXPECT_EQ('\0', fv8[0]);  // escape sentinel
    Slice in(fv8);
    IndexValue dec;
    ASSERT_OK(dec.DecodeFrom(&in, false, &prev, true));
    EXPECT_EQ(noncontig.offset(), dec.handle.offset());
    EXPECT_EQ(noncontig.size(), dec.handle.size());
    EXPECT_EQ(0u, in.size());
  }

  // A genuine legacy (fv7) 0x00 (delta 0) still decodes as delta 0 under fv7.
  {
    std::string bytes;
    PutVarsignedint64(&bytes, 0);
    ASSERT_EQ(1u, bytes.size());
    ASSERT_EQ('\0', bytes[0]);
    Slice in(bytes);
    IndexValue dec;
    ASSERT_OK(dec.DecodeFrom(&in, false, &prev,
                             /*use_value_delta_escape=*/false));
    EXPECT_EQ(contiguous_offset, dec.handle.offset());
    EXPECT_EQ(prev.size(), dec.handle.size());  // delta 0 -> unchanged size
  }
}

enum class KeyDistribution { kUniform, kNonUniform };

class IndexBlockTest
    : public testing::Test,
      public testing::WithParamInterface<
          std::tuple<bool, bool, bool, bool, test::UserDefinedTimestampTestMode,
                     BlockBasedTableOptions::BlockSearchType, int, int, int,
                     std::pair<int, KeyDistribution>>> {
 public:
  IndexBlockTest() = default;

  bool keyIncludesSeq() const { return std::get<0>(GetParam()); }
  bool useValueDeltaEncoding() const { return std::get<1>(GetParam()); }
  bool includeFirstKey() const { return std::get<2>(GetParam()); }
  bool useSeparatedKVStorage() const { return std::get<3>(GetParam()); }
  bool isUDTEnabled() const {
    return test::IsUDTEnabled(std::get<4>(GetParam()));
  }
  bool shouldPersistUDT() const {
    return test::ShouldPersistUDT(std::get<4>(GetParam()));
  }
  BlockBasedTableOptions::BlockSearchType indexSearchType() const {
    return isUDTEnabled() ? BlockBasedTableOptions::kBinary
                          : std::get<5>(GetParam());
  }
  int numRecords() const {
    return std::min(1 << keyLength(), std::get<6>(GetParam()));
  }
  int indexBlockRestartInterval() const { return std::get<7>(GetParam()); }
  int keyLength() const { return std::get<8>(GetParam()); }
  // prefix_length and key_distribution are bundled into a std::pair to stay
  // within gtest 1.8.1's 10-parameter Combine limit.
  int prefixLength() const { return std::get<9>(GetParam()).first; }
  KeyDistribution keyDistribution() const {
    return std::get<9>(GetParam()).second;
  }
};

// Similar to GenerateRandomKVs but for index block contents. Keys always
// contain a 0-sequence number, callers may extract the user key if needed.
void GenerateRandomIndexEntries(
    std::vector<std::string>* separators,
    std::vector<BlockHandle>* block_handles,
    std::vector<std::string>* first_keys, const int len, size_t ts_sz = 0,
    int key_length = 12, int prefix_length = 0,
    KeyDistribution distribution = KeyDistribution::kUniform) {
  Random rnd(42);
  std::string prefix(prefix_length, 'x');

  // For each of `len` blocks, we need to generate a first and last key.
  // Generate n*2 random keys, sort them, group into consecutive pairs.
  std::set<std::string> keys;

  // Two clusters with shared prefixes of effective_key_length - 2. This
  // stresses interpolation search's uniform distribution assumption.
  int cluster_prefix_len = std::max(0, key_length - 5);
  std::string cluster1_prefix = prefix + rnd.RandomString(cluster_prefix_len);
  std::string cluster2_prefix = prefix + rnd.RandomString(cluster_prefix_len);

  while ((int)keys.size() < len * 2) {
    std::string new_key;
    if (distribution == KeyDistribution::kNonUniform) {
      int remaining = key_length - cluster_prefix_len;
      const std::string& cp =
          (keys.size() % 2 == 0) ? cluster1_prefix : cluster2_prefix;
      new_key = cp + rnd.RandomString(std::max(1, remaining));
    } else {
      // Generate evenly-spaced keys to ensure numeric uniformity.
      // Encode the key index as big-endian with jitter to avoid
      // perfectly equal gaps while maintaining uniformity.
      uint64_t base =
          static_cast<uint64_t>(keys.size()) * 1000 + rnd.Uniform(100);
      std::string key_bytes(key_length, '\0');
      // Write big-endian uint64 into the last 8 bytes (or fewer if shorter)
      for (int j = key_length - 1; j >= 0 && base > 0; j--) {
        key_bytes[j] = static_cast<char>(base & 0xFF);
        base >>= 8;
      }
      new_key = prefix + key_bytes;
    }

    AppendInternalKeyFooter(&new_key, 0 /* seqno */, kTypeValue);
    if (ts_sz > 0) {
      std::string key;
      PadInternalKeyWithMinTimestamp(&key, new_key, ts_sz);
      keys.insert(std::move(key));
    } else {
      keys.insert(std::move(new_key));
    }
  }

  uint64_t offset = 0;
  for (auto it = keys.begin(); it != keys.end();) {
    first_keys->emplace_back(*it++);
    separators->emplace_back(*it++);
    uint64_t size = rnd.Uniform(1024 * 16);
    BlockHandle handle(offset, size);
    offset += size + BlockBasedTable::kBlockTrailerSize;
    block_handles->emplace_back(handle);
  }
}

TEST_P(IndexBlockTest, IndexValueEncodingTest) {
  Random rnd(301);
  Options options = Options();
  if (isUDTEnabled()) {
    options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  }
  size_t ts_sz = options.comparator->timestamp_size();

  std::vector<std::string> separators;
  std::vector<BlockHandle> block_handles;
  std::vector<std::string> first_keys;
  const bool kUseDeltaEncoding = true;
  BlockBuilder builder(
      indexBlockRestartInterval(), kUseDeltaEncoding, useValueDeltaEncoding(),
      BlockBasedTableOptions::kDataBlockBinarySearch,
      0.75 /* data_block_hash_table_util_ratio */, ts_sz, shouldPersistUDT(),
      !keyIncludesSeq(), useSeparatedKVStorage(), nullptr /* statistics */,
      0.2 /* uniform_cv_threshold */, false /* use_common_prefix */);

  int num_records = numRecords();

  GenerateRandomIndexEntries(&separators, &block_handles, &first_keys,
                             num_records, ts_sz, keyLength(), prefixLength(),
                             keyDistribution());
  BlockHandle last_encoded_handle;
  for (int i = 0; i < num_records; i++) {
    std::string first_key_to_persist_buf;
    Slice first_internal_key = first_keys[i];
    if (ts_sz > 0 && !shouldPersistUDT()) {
      StripTimestampFromInternalKey(&first_key_to_persist_buf, first_keys[i],
                                    ts_sz);
      first_internal_key = first_key_to_persist_buf;
    }
    const BlockHandle* prev =
        (useValueDeltaEncoding() && i > 0) ? &last_encoded_handle : nullptr;
    Slice add_key =
        keyIncludesSeq() ? Slice(separators[i]) : ExtractUserKey(separators[i]);
    AddIndexBlockEntry(builder, add_key, block_handles[i], prev,
                       includeFirstKey(), first_internal_key);
    last_encoded_handle = block_handles[i];
  }

  // read serialized contents of the block
  Slice rawblock = builder.Finish();

  // create block reader
  BlockContents contents;
  contents.data = rawblock;
  Block reader(std::move(contents), 0 /* read_amp_bytes_per_bit */,
               nullptr /* statistics */,
               static_cast<uint32_t>(indexBlockRestartInterval()));

  const bool kTotalOrderSeek = true;
  IndexBlockIter* kNullIter = nullptr;
  Statistics* kNullStats = nullptr;
  // read contents of block sequentially
  InternalIteratorBase<IndexValue>* iter = reader.NewIndexIterator(
      options.comparator, kDisableGlobalSequenceNumber, kNullIter, kNullStats,
      kTotalOrderSeek, includeFirstKey(), keyIncludesSeq(),
      !useValueDeltaEncoding(), false /* block_contents_pinned */,
      shouldPersistUDT(), nullptr /* prefix_index */, indexSearchType(),
      false /* value_delta_escape */);
  iter->SeekToFirst();
  for (int index = 0; index < num_records; ++index) {
    ASSERT_TRUE(iter->Valid());

    Slice k = iter->key();
    IndexValue v = iter->value();

    if (keyIncludesSeq()) {
      EXPECT_EQ(separators[index], k.ToString());
    } else {
      const Slice user_key = ExtractUserKey(separators[index]);
      EXPECT_EQ(user_key, k);
    }
    EXPECT_EQ(block_handles[index].offset(), v.handle.offset());
    EXPECT_EQ(block_handles[index].size(), v.handle.size());
    EXPECT_EQ(includeFirstKey() ? first_keys[index] : "",
              v.first_internal_key.ToString());

    iter->Next();
  }
  delete iter;

  // ScanForUniformity requires at least 3 restart points to determine
  // uniformity. With fewer restarts, is_uniform is always false.
  // When UDT is enabled, min-timestamps alter the key byte distribution,
  // so skip the uniformity check.
  if (!isUDTEnabled()) {
    bool expect_uniform = reader.NumRestarts() >= 3 &&
                          keyDistribution() == KeyDistribution::kUniform;
    EXPECT_EQ(reader.IsUniform(), expect_uniform);
    EXPECT_EQ(builder.IsUniform(), expect_uniform);
  }

  // read block contents randomly
  iter = reader.NewIndexIterator(
      options.comparator, kDisableGlobalSequenceNumber, kNullIter, kNullStats,
      kTotalOrderSeek, includeFirstKey(), keyIncludesSeq(),
      !useValueDeltaEncoding(), false /* block_contents_pinned */,
      shouldPersistUDT(), nullptr /* prefix_index */, indexSearchType(),
      false /* value_delta_escape */);
  for (int i = 0; i < num_records * 2; i++) {
    // find a random key in the lookaside array
    int index = rnd.Uniform(num_records);
    Slice k(separators[index]);

    // search in block for this key
    iter->Seek(k);
    ASSERT_TRUE(iter->Valid());
    IndexValue v = iter->value();
    if (keyIncludesSeq()) {
      EXPECT_EQ(separators[index], iter->key().ToString());
    } else {
      const Slice user_key = ExtractUserKey(separators[index]);
      EXPECT_EQ(user_key, iter->key());
    }
    EXPECT_EQ(block_handles[index].offset(), v.handle.offset());
    EXPECT_EQ(block_handles[index].size(), v.handle.size());
    EXPECT_EQ(includeFirstKey() ? first_keys[index] : "",
              v.first_internal_key.ToString());
  }
  delete iter;
}

// Param 0: key includes sequence number (whether to use user key or internal
// key as key entry in index block).
// Param 1: use value delta encoding
// Param 2: include first key
// Param 3: use separated KV storage
// Param 4: user-defined timestamp test mode
// Param 5: index search type (binary search or interpolation search)
// Param 6: number of records
// Param 7: index block restart interval
// Param 8: key length
// Param 9: (prefix_length, key_distribution) pair
INSTANTIATE_TEST_CASE_P(
    P, IndexBlockTest,
    ::testing::Combine(
        ::testing::Bool(), ::testing::Bool(), ::testing::Bool(),
        ::testing::Bool(), ::testing::ValuesIn(test::GetUDTTestModes()),
        ::testing::Values(
            BlockBasedTableOptions::BlockSearchType::kBinary,
            BlockBasedTableOptions::BlockSearchType::kInterpolation,
            BlockBasedTableOptions::BlockSearchType::kAuto),
        ::testing::Values(1, 100),    // num_records
        ::testing::Values(1, 16),     // index_block_restart_interval
        ::testing::Values(1, 8, 12),  // key_length
        ::testing::Values(std::make_pair(0, KeyDistribution::kUniform),
                          std::make_pair(0, KeyDistribution::kNonUniform),
                          std::make_pair(50, KeyDistribution::kUniform),
                          std::make_pair(50, KeyDistribution::kNonUniform))));

TEST(IndexBlockTest, InterpolationSearchPrefixBoundary) {
  const bool kIncludeFirstKey = false;
  const bool kUseValueDeltaEncoding = true;
  const uint64_t kBlockSize = 50;

  // 20 user keys sharing prefix "ABCDEFGHIJ" with evenly spaced suffixes.
  const std::string kPrefix = "ABCDEFGHIJ";
  const int kNumKeys = 20;
  std::vector<std::string> keys;
  keys.reserve(kNumKeys);
  for (int i = 0; i < kNumKeys; i++) {
    std::string suffix = std::to_string(i);
    char formatted_suffix[4];
    snprintf(formatted_suffix, sizeof(formatted_suffix), "%03d", i);
    keys.push_back(kPrefix + formatted_suffix);
  }

  std::vector<BlockHandle> handles;
  handles.reserve(kNumKeys);
  for (int i = 0; i < kNumKeys; i++) {
    handles.emplace_back(i * (kBlockSize + BlockBasedTable::kBlockTrailerSize),
                         kBlockSize);
  }

  BlockBuilder builder(
      1 /* restart_interval */, true /* use_delta_encoding */,
      kUseValueDeltaEncoding, BlockBasedTableOptions::kDataBlockBinarySearch,
      0.75 /* data_block_hash_table_util_ratio */, 0 /* ts_sz */,
      false /* persist_udt */, true /* is_user_key */,
      false /* use_separated_kv_storage */, nullptr /* statistics */,
      -1.0 /* uniform_cv_threshold */, false /* use_common_prefix */);

  for (int i = 0; i < kNumKeys; i++) {
    BlockHandle* prev = i > 0 ? &handles[i - 1] : nullptr;
    AddIndexBlockEntry(builder, keys[i], handles[i], prev, kIncludeFirstKey);
  }

  Slice rawblock = builder.Finish();
  BlockContents contents;
  contents.data = rawblock;
  Block reader(std::move(contents));

  // Seek targets must be internal keys since SeekImpl calls ExtractUserKey().
  auto make_target = [](const std::string& user_key) {
    std::string target = user_key;
    AppendInternalKeyFooter(&target, kMaxSequenceNumber, kValueTypeForSeek);
    return target;
  };

  std::unique_ptr<InternalIteratorBase<IndexValue>> iter(
      reader.NewIndexIterator(
          BytewiseComparator(), kDisableGlobalSequenceNumber,
          nullptr /* iter */, nullptr /* stats */, true /* total_order_seek */,
          kIncludeFirstKey, false /* key_includes_seq */,
          !kUseValueDeltaEncoding /* value_is_full */,
          false /* block_contents_pinned */,
          true /* user_defined_timestamps_persisted */,
          nullptr /* prefix_index */,
          BlockBasedTableOptions::BlockSearchType::kInterpolation,
          false /* value_delta_escape */));

  // Case 1: target prefix < shared prefix
  iter->Seek(make_target("AAAAAA"));
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(iter->key(), keys[0]);

  iter->Seek(make_target(""));
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(iter->key(), keys[0]);

  // Case 2: target prefix > shared prefix
  iter->Seek(make_target("ABCDEFGHZZ"));
  ASSERT_FALSE(iter->Valid());

  // Case 3: target is the prefix
  iter->Seek(make_target("ABCDEFGHIJ"));
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(iter->key(), keys[0]);

  // Case 4: target a subset of the prefix
  iter->Seek(make_target("ABCDEFG"));
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(iter->key(), keys[0]);
}

// Like the above test, but extend the shared prefix into internal bytes
TEST(IndexBlockTest, InterpolationSearchPrefixBoundary2) {
  const bool kIncludeFirstKey = false;
  const bool kUseValueDeltaEncoding = true;
  const uint64_t kBlockSize = 50;

  // 20 internal keys with the same user key but decreasing sequence numbers
  // (which is ascending InternalKeyComparator order).
  const std::string kUserKey = "ABCDEFGHIJ";
  const int kNumKeys = 20;
  std::vector<std::string> keys;
  keys.reserve(kNumKeys);
  for (int i = 0; i < kNumKeys; i++) {
    std::string ikey = kUserKey;
    SequenceNumber seq = static_cast<SequenceNumber>(kNumKeys - i);
    AppendInternalKeyFooter(&ikey, seq, kTypeValue);
    keys.push_back(ikey);
  }

  std::vector<BlockHandle> handles;
  handles.reserve(kNumKeys);
  for (int i = 0; i < kNumKeys; i++) {
    handles.emplace_back(i * (kBlockSize + BlockBasedTable::kBlockTrailerSize),
                         kBlockSize);
  }

  BlockBuilder builder(
      1 /* restart_interval */, true /* use_delta_encoding */,
      kUseValueDeltaEncoding, BlockBasedTableOptions::kDataBlockBinarySearch,
      0.75 /* data_block_hash_table_util_ratio */, 0 /* ts_sz */,
      false /* persist_udt */, false /* is_user_key */,
      false /* use_separated_kv_storage */, nullptr /* statistics */,
      -1.0 /* uniform_cv_threshold */, false /* use_common_prefix */);

  for (int i = 0; i < kNumKeys; i++) {
    BlockHandle* prev = i > 0 ? &handles[i - 1] : nullptr;
    AddIndexBlockEntry(builder, keys[i], handles[i], prev, kIncludeFirstKey);
  }

  Slice rawblock = builder.Finish();
  BlockContents contents;
  contents.data = rawblock;
  Block reader(std::move(contents));

  auto make_target = [&](const std::string& user_key,
                         SequenceNumber seq = kMaxSequenceNumber) {
    std::string target = user_key;
    AppendInternalKeyFooter(&target, seq, kTypeValue);
    return target;
  };

  std::unique_ptr<InternalIteratorBase<IndexValue>> iter(
      reader.NewIndexIterator(
          BytewiseComparator(), kDisableGlobalSequenceNumber,
          nullptr /* iter */, nullptr /* stats */, true /* total_order_seek */,
          kIncludeFirstKey, true /* key_includes_seq */,
          !kUseValueDeltaEncoding /* value_is_full */,
          false /* block_contents_pinned */,
          true /* user_defined_timestamps_persisted */,
          nullptr /* prefix_index */,
          BlockBasedTableOptions::BlockSearchType::kInterpolation,
          false /* value_delta_escape */));

  // Seek to each existing sequence number
  for (int i = 0; i < kNumKeys; i++) {
    SequenceNumber seq = static_cast<SequenceNumber>(kNumKeys - i);
    iter->Seek(make_target(kUserKey, seq));
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(iter->key(), keys[i]);
  }

  // Case 1: target prefix < shared prefix
  iter->Seek(make_target("AAAAAA"));
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(iter->key(), keys[0]);

  iter->Seek(make_target(""));
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(iter->key(), keys[0]);

  // Case 2: target prefix > shared prefix
  iter->Seek(make_target("ABCDEFGHZZ"));
  ASSERT_FALSE(iter->Valid());

  // Case 3: target has the same user key with kMaxSequenceNumber
  iter->Seek(make_target("ABCDEFGHIJ"));
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(iter->key(), keys[0]);

  // Case 4: target a subset of the prefix
  iter->Seek(make_target("ABCDEFG"));
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(iter->key(), keys[0]);

  // Case 5: target key is a prefix that also extends into the internal bytes
  // footer
  iter->Seek(make_target("ABCDEFGHIJ" + std::string(1, kTypeValue)));
  ASSERT_FALSE(iter->Valid());
}

class BlockPerKVChecksumTest : public DBTestBase {
 public:
  BlockPerKVChecksumTest()
      : DBTestBase("block_per_kv_checksum", /*env_do_fsync=*/false) {}

  template <typename TBlockIter>
  void TestIterateForward(std::unique_ptr<TBlockIter>& biter,
                          size_t& verification_count) {
    while (biter->Valid()) {
      verification_count = 0;
      biter->Next();
      if (biter->Valid()) {
        ASSERT_GE(verification_count, 1);
      }
    }
  }

  template <typename TBlockIter>
  void TestIterateBackward(std::unique_ptr<TBlockIter>& biter,
                           size_t& verification_count) {
    while (biter->Valid()) {
      verification_count = 0;
      biter->Prev();
      if (biter->Valid()) {
        ASSERT_GE(verification_count, 1);
      }
    }
  }

  template <typename TBlockIter>
  void TestSeekToFirst(std::unique_ptr<TBlockIter>& biter,
                       size_t& verification_count) {
    verification_count = 0;
    biter->SeekToFirst();
    ASSERT_GE(verification_count, 1);
    TestIterateForward(biter, verification_count);
  }

  template <typename TBlockIter>
  void TestSeekToLast(std::unique_ptr<TBlockIter>& biter,
                      size_t& verification_count) {
    verification_count = 0;
    biter->SeekToLast();
    ASSERT_GE(verification_count, 1);
    TestIterateBackward(biter, verification_count);
  }

  template <typename TBlockIter>
  void TestSeekForPrev(std::unique_ptr<TBlockIter>& biter,
                       size_t& verification_count, const std::string& k) {
    verification_count = 0;
    biter->SeekForPrev(k);
    ASSERT_GE(verification_count, 1);
    TestIterateBackward(biter, verification_count);
  }

  template <typename TBlockIter>
  void TestSeek(std::unique_ptr<TBlockIter>& biter, size_t& verification_count,
                const std::string& k) {
    verification_count = 0;
    biter->Seek(k);
    ASSERT_GE(verification_count, 1);
    TestIterateForward(biter, verification_count);
  }

  bool VerifyChecksum(uint32_t checksum_len, const char* checksum_ptr,
                      const Slice& key, const Slice& val) {
    if (!checksum_len) {
      return checksum_ptr == nullptr;
    }
    return ProtectionInfo64().ProtectKV(key, val).Verify(
        static_cast<uint8_t>(checksum_len), checksum_ptr);
  }
};

namespace {
const BlockBasedTableOptions* kTableOptions() {
  static BlockBasedTableOptions opts{};
  return &opts;
}
Decompressor* kDecompressor() {
  static auto mgr = GetBuiltinV2CompressionManager();
  static auto decomp = mgr->GetDecompressor();
  return decomp.get();
}
}  // namespace

TEST_F(BlockPerKVChecksumTest, EmptyBlock) {
  // Tests that empty block code path is not broken by per kv checksum.
  BlockBuilder builder(
      16 /* block_restart_interval */, true /* use_delta_encoding */,
      false /* use_value_delta_encoding */,
      BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch,
      0.75 /* data_block_hash_table_util_ratio */, 0 /* ts_sz */,
      true /* persist_user_defined_timestamps */, false /* is_user_key */,
      false /* use_separated_kv_storage */, nullptr /* statistics */,
      -1.0 /* uniform_cv_threshold */, false /* use_common_prefix */);
  Slice raw_block = builder.Finish();
  BlockContents contents;
  contents.data = raw_block;

  std::unique_ptr<Block_kData> data_block;
  Options options = Options();
  uint8_t protection_bytes_per_key = 8;
  BlockCreateContext create_context{kTableOptions(),
                                    nullptr,
                                    nullptr /* statistics */,
                                    kDecompressor(),
                                    protection_bytes_per_key,
                                    options.comparator,
                                    /*index_value_is_full=*/false,
                                    /*index_has_first_key=*/false,
                                    /*index_value_delta_escape=*/false};
  create_context.Create(&data_block, std::move(contents));
  std::unique_ptr<DataBlockIter> biter{data_block->NewDataIterator(
      options.comparator, kDisableGlobalSequenceNumber)};
  biter->SeekToFirst();
  ASSERT_FALSE(biter->Valid());
  ASSERT_OK(biter->status());
  Random rnd(33);
  biter->SeekForGet(GenerateInternalKey(1, 1, 10, &rnd));
  ASSERT_FALSE(biter->Valid());
  ASSERT_OK(biter->status());
  biter->SeekToLast();
  ASSERT_FALSE(biter->Valid());
  ASSERT_OK(biter->status());
  biter->Seek(GenerateInternalKey(1, 1, 10, &rnd));
  ASSERT_FALSE(biter->Valid());
  ASSERT_OK(biter->status());
  biter->SeekForPrev(GenerateInternalKey(1, 1, 10, &rnd));
  ASSERT_FALSE(biter->Valid());
  ASSERT_OK(biter->status());
}

TEST_F(BlockPerKVChecksumTest, UnsupportedOptionValue) {
  Options options = Options();
  options.block_protection_bytes_per_key = 128;
  Destroy(options);
  ASSERT_TRUE(TryReopen(options).IsNotSupported());
}

TEST_F(BlockPerKVChecksumTest, InitializeProtectionInfo) {
  // Make sure that the checksum construction code path does not break
  // when the block is itself already corrupted.
  Options options = Options();
  uint8_t protection_bytes_per_key = 8;
  BlockCreateContext create_context{kTableOptions(),
                                    nullptr /* ioptions */,
                                    nullptr /* statistics */,
                                    kDecompressor(),
                                    protection_bytes_per_key,
                                    options.comparator,
                                    /*index_value_is_full=*/false,
                                    /*index_has_first_key=*/false,
                                    /*index_value_delta_escape=*/false};

  {
    std::string invalid_content = "1";
    Slice raw_block = invalid_content;
    BlockContents contents;
    contents.data = raw_block;
    std::unique_ptr<Block_kData> data_block;
    create_context.Create(&data_block, std::move(contents));
    std::unique_ptr<DataBlockIter> iter{data_block->NewDataIterator(
        options.comparator, kDisableGlobalSequenceNumber)};
    ASSERT_TRUE(iter->status().IsCorruption());
  }
  {
    std::string invalid_content = "1";
    Slice raw_block = invalid_content;
    BlockContents contents;
    contents.data = raw_block;
    std::unique_ptr<Block_kIndex> index_block;
    create_context.Create(&index_block, std::move(contents));
    std::unique_ptr<IndexBlockIter> iter{index_block->NewIndexIterator(
        options.comparator, kDisableGlobalSequenceNumber, nullptr, nullptr,
        true, false, true, true, /*block_contents_pinned=*/false,
        /*user_defined_timestamps_persisted=*/true, /*prefix_index=*/nullptr,
        BlockBasedTableOptions::kBinary, /*value_delta_escape=*/false)};
    ASSERT_TRUE(iter->status().IsCorruption());
  }
  {
    std::string invalid_content = "1";
    Slice raw_block = invalid_content;
    BlockContents contents;
    contents.data = raw_block;
    std::unique_ptr<Block_kMetaIndex> meta_block;
    create_context.Create(&meta_block, std::move(contents));
    std::unique_ptr<MetaBlockIter> iter{meta_block->NewMetaIterator(true)};
    ASSERT_TRUE(iter->status().IsCorruption());
  }
}

TEST_F(BlockPerKVChecksumTest, CorruptHashIndexNumBucketsNoOverRead) {
  // Regression test for a heap-buffer-overflow(read) reachable from a corrupted
  // SST. A data block whose footer claims the hash index format but encodes a
  // NUM_BUCKETS larger than the block would, in release builds (where the
  // debug-only asserts in DataBlockHashIndex::Initialize are compiled out),
  // underflow map_offset. That inflated restart_offset_ was then trusted by
  // Block::GetCorruptionStatus() to size a Slice, driving an out-of-bounds read
  // in DataBlockFooter::DecodeFrom(). The block must instead be reported as
  // corrupt without reading past its heap allocation (verified under ASAN).
  Options options = Options();
  BlockCreateContext create_context{kTableOptions(),
                                    nullptr /* ioptions */,
                                    nullptr /* statistics */,
                                    kDecompressor(),
                                    0 /* protection_bytes_per_key */,
                                    options.comparator,
                                    /*index_value_is_full=*/false,
                                    /*index_has_first_key=*/false,
                                    /*index_value_delta_escape=*/false};

  // Body bytes followed by a 2-byte NUM_BUCKETS field, then the encoded footer
  // (values_section_offset + packed). Layout mirrors what DecodeFrom expects.
  std::string block_body(500, 'x');
  // NUM_BUCKETS far larger than the block: forces the map_offset underflow.
  PutFixed16(&block_body, 60000);
  DataBlockFooter footer;
  footer.num_restarts = 1;
  footer.index_type = BlockBasedTableOptions::kDataBlockBinaryAndHash;
  footer.separated_kv = true;
  footer.is_uniform = false;
  // Large offset so the (pre-fix) inflated restart_offset_ path marks size == 0
  // and routes through GetCorruptionStatus().
  footer.values_section_offset = 0xFFFFFFFFu;
  footer.EncodeTo(&block_body);

  // Use a tightly-sized heap allocation so any over-read hits an ASAN redzone.
  std::unique_ptr<char[]> buf(new char[block_body.size()]);
  memcpy(buf.get(), block_body.data(), block_body.size());
  BlockContents contents(std::move(buf), block_body.size());

  std::unique_ptr<Block_kData> data_block;
  create_context.Create(&data_block, std::move(contents));
  ASSERT_NE(data_block, nullptr);
  if (data_block == nullptr) {
    return;
  }
  std::unique_ptr<DataBlockIter> iter{data_block->NewDataIterator(
      options.comparator, kDisableGlobalSequenceNumber)};
  ASSERT_NE(iter, nullptr);
  if (iter == nullptr) {
    return;
  }
  ASSERT_TRUE(iter->status().IsCorruption());
}

// A data block footer with the reserved "extended metadata" bit (bit 30) set
// must be rejected as corruption. No writer sets it yet; format_version 8
// reserves it so a future extension can be added in the one shared decoder.
TEST(DataBlockFooterTest, ExtendedMetadataBitRejected) {
  DataBlockFooter footer;
  footer.num_restarts = 3;
  footer.index_type = BlockBasedTableOptions::kDataBlockBinarySearch;
  std::string encoded;
  footer.EncodeTo(&encoded);
  ASSERT_EQ(sizeof(uint32_t), encoded.size());

  // Sanity: the unmodified footer decodes fine.
  {
    Slice in(encoded);
    DataBlockFooter decoded;
    ASSERT_OK(decoded.DecodeFrom(&in));
    EXPECT_EQ(3u, decoded.num_restarts);
  }

  // Set bit 30 in the packed footer word and confirm it is rejected.
  uint32_t packed = DecodeFixed32(encoded.data());
  packed |= (uint32_t{1} << 30);
  std::string corrupt;
  PutFixed32(&corrupt, packed);
  Slice in(corrupt);
  DataBlockFooter decoded;
  Status s = decoded.DecodeFrom(&in);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

TEST_F(BlockPerKVChecksumTest, ApproximateMemory) {
  // Tests that ApproximateMemoryUsage() includes memory used by block kv
  // checksum.
  const int kNumRecords = 20;
  std::vector<std::string> keys;
  std::vector<std::string> values;
  GenerateRandomKVs(&keys, &values, 0, kNumRecords, 1 /* step */,
                    24 /* padding_size */);
  std::unique_ptr<BlockBuilder> builder;
  auto generate_block_content = [&]() {
    builder = std::make_unique<BlockBuilder>(BlockBuilder::ForMetaBlock{},
                                             16 /* restart_interval */);
    for (int i = 0; i < kNumRecords; ++i) {
      builder->Add(keys[i], values[i]);
    }
    Slice raw_block = builder->Finish();
    BlockContents contents;
    contents.data = raw_block;
    return contents;
  };

  Options options = Options();
  uint8_t protection_bytes_per_key = 8;
  BlockCreateContext with_checksum_create_context{
      kTableOptions(),
      nullptr /* ioptions */,
      nullptr /* statistics */,
      kDecompressor(),
      protection_bytes_per_key,
      options.comparator,
      true /* index_value_is_full */,
      /*index_has_first_key=*/false,
      /*index_value_delta_escape=*/false};
  BlockCreateContext create_context{kTableOptions(),
                                    nullptr /* ioptions */,
                                    nullptr /* statistics */,
                                    kDecompressor(),
                                    0,
                                    options.comparator,
                                    true /* index_value_is_full */,
                                    /*index_has_first_key=*/false,
                                    /*index_value_delta_escape=*/false};

  {
    std::unique_ptr<Block_kData> data_block;
    create_context.Create(&data_block, generate_block_content());
    size_t block_memory = data_block->ApproximateMemoryUsage();
    std::unique_ptr<Block_kData> with_checksum_data_block;
    with_checksum_create_context.Create(&with_checksum_data_block,
                                        generate_block_content());
    ASSERT_GT(with_checksum_data_block->ApproximateMemoryUsage() - block_memory,
              100);
  }

  {
    std::unique_ptr<Block_kData> meta_block;
    create_context.Create(&meta_block, generate_block_content());
    size_t block_memory = meta_block->ApproximateMemoryUsage();
    std::unique_ptr<Block_kData> with_checksum_meta_block;
    with_checksum_create_context.Create(&with_checksum_meta_block,
                                        generate_block_content());
    // Rough comparison to avoid flaky test due to memory allocation alignment.
    ASSERT_GT(with_checksum_meta_block->ApproximateMemoryUsage() - block_memory,
              100);
  }

  {
    // Index block has different contents.
    std::vector<std::string> separators;
    std::vector<BlockHandle> block_handles;
    std::vector<std::string> first_keys;
    GenerateRandomIndexEntries(&separators, &block_handles, &first_keys,
                               kNumRecords);
    auto generate_index_content = [&]() {
      builder = std::make_unique<BlockBuilder>(BlockBuilder::ForMetaBlock{},
                                               16 /* restart_interval */);
      BlockHandle last_encoded_handle;
      for (int i = 0; i < kNumRecords; ++i) {
        IndexValue entry(block_handles[i], first_keys[i]);
        std::string encoded_entry;
        std::string delta_encoded_entry;
        entry.EncodeTo(&encoded_entry, false, nullptr);
        last_encoded_handle = entry.handle;
        const Slice delta_encoded_entry_slice(delta_encoded_entry);
        builder->Add(separators[i], encoded_entry, delta_encoded_entry_slice);
      }
      Slice raw_block = builder->Finish();
      BlockContents contents;
      contents.data = raw_block;
      return contents;
    };

    std::unique_ptr<Block_kIndex> index_block;
    create_context.Create(&index_block, generate_index_content());
    size_t block_memory = index_block->ApproximateMemoryUsage();
    std::unique_ptr<Block_kIndex> with_checksum_index_block;
    with_checksum_create_context.Create(&with_checksum_index_block,
                                        generate_index_content());
    ASSERT_GT(
        with_checksum_index_block->ApproximateMemoryUsage() - block_memory,
        100);
  }
}

std::string GetDataBlockIndexTypeStr(
    BlockBasedTableOptions::DataBlockIndexType t) {
  return t == BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch
             ? "BinarySearch"
             : "BinaryAndHash";
}

class DataBlockKVChecksumTest
    : public BlockPerKVChecksumTest,
      public testing::WithParamInterface<std::tuple<
          BlockBasedTableOptions::DataBlockIndexType,
          uint8_t /* block_protection_bytes_per_key */,
          uint32_t /* restart_interval*/, bool /* use_delta_encoding */>> {
 public:
  DataBlockKVChecksumTest() = default;

  BlockBasedTableOptions::DataBlockIndexType GetDataBlockIndexType() const {
    return std::get<0>(GetParam());
  }
  uint8_t GetChecksumLen() const { return std::get<1>(GetParam()); }
  uint32_t GetRestartInterval() const { return std::get<2>(GetParam()); }
  bool GetUseDeltaEncoding() const { return std::get<3>(GetParam()); }

  std::unique_ptr<Block_kData> GenerateDataBlock(
      std::vector<std::string>& keys, std::vector<std::string>& values,
      int num_record) {
    BlockCreateContext create_context{kTableOptions(),
                                      nullptr /* statistics */,
                                      nullptr /* ioptions */,
                                      kDecompressor(),
                                      GetChecksumLen(),
                                      Options().comparator,
                                      /*index_value_is_full=*/false,
                                      /*index_has_first_key=*/false,
                                      /*index_value_delta_escape=*/false};
    builder_ = std::make_unique<BlockBuilder>(
        static_cast<int>(GetRestartInterval()),
        GetUseDeltaEncoding() /* use_delta_encoding */,
        false /* use_value_delta_encoding */, GetDataBlockIndexType(),
        0.75 /* data_block_hash_table_util_ratio */, 0 /* ts_sz */,
        true /* persist_user_defined_timestamps */, false /* is_user_key */,
        false /* use_separated_kv_storage */, nullptr /* statistics */,
        -1.0 /* uniform_cv_threshold */, false /* use_common_prefix */);
    for (int i = 0; i < num_record; i++) {
      builder_->Add(keys[i], values[i]);
    }
    Slice raw_block = builder_->Finish();
    BlockContents contents;
    contents.data = raw_block;
    std::unique_ptr<Block_kData> data_block;
    create_context.Create(&data_block, std::move(contents));
    return data_block;
  }

  std::unique_ptr<BlockBuilder> builder_;
};

INSTANTIATE_TEST_CASE_P(
    P, DataBlockKVChecksumTest,
    ::testing::Combine(
        ::testing::Values(
            BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch,
            BlockBasedTableOptions::DataBlockIndexType::
                kDataBlockBinaryAndHash),
        ::testing::Values(0, 1, 2, 4, 8) /* protection_bytes_per_key */,
        ::testing::Values(1, 2, 3, 8, 16) /* restart_interval */,
        ::testing::Values(false, true)) /* delta_encoding */,
    [](const testing::TestParamInfo<
        std::tuple<BlockBasedTableOptions::DataBlockIndexType, uint8_t,
                   uint32_t, bool>>& args) {
      std::ostringstream oss;
      oss << GetDataBlockIndexTypeStr(std::get<0>(args.param))
          << "ProtectionPerKey" << std::to_string(std::get<1>(args.param))
          << "RestartInterval" << std::to_string(std::get<2>(args.param))
          << "DeltaEncode" << std::to_string(std::get<3>(args.param));
      return oss.str();
    });

TEST_P(DataBlockKVChecksumTest, ChecksumConstructionAndVerification) {
  uint8_t protection_bytes_per_key = GetChecksumLen();
  std::vector<int> num_restart_intervals = {1, 16};
  for (const auto num_restart_interval : num_restart_intervals) {
    const int kNumRecords =
        num_restart_interval * static_cast<int>(GetRestartInterval());
    std::vector<std::string> keys;
    std::vector<std::string> values;
    GenerateRandomKVs(&keys, &values, 0, kNumRecords + 1, 1 /* step */,
                      24 /* padding_size */);
    SyncPoint::GetInstance()->DisableProcessing();
    std::unique_ptr<Block_kData> data_block =
        GenerateDataBlock(keys, values, kNumRecords);

    const char* checksum_ptr = data_block->TEST_GetKVChecksum();
    // Check checksum of correct length is generated
    for (int i = 0; i < kNumRecords; i++) {
      ASSERT_TRUE(VerifyChecksum(protection_bytes_per_key,
                                 checksum_ptr + i * protection_bytes_per_key,
                                 keys[i], values[i]));
    }
    std::vector<SequenceNumber> seqnos{kDisableGlobalSequenceNumber, 0};

    // Could just use a boolean flag. Use a counter here just to keep open the
    // possibility of checking the exact number of verifications in the future.
    size_t verification_count = 0;
    // The SyncPoint is placed before checking checksum_len == 0 in
    // Block::VerifyChecksum(). So verification count is incremented even with
    // protection_bytes_per_key = 0. No actual checksum computation is done in
    // that case (see Block::VerifyChecksum()).
    SyncPoint::GetInstance()->SetCallBack(
        "Block::VerifyChecksum::checksum_len",
        [&verification_count, protection_bytes_per_key](void* checksum_len) {
          ASSERT_EQ((*static_cast<uint8_t*>(checksum_len)),
                    protection_bytes_per_key);
          ++verification_count;
        });
    SyncPoint::GetInstance()->EnableProcessing();

    for (const auto seqno : seqnos) {
      std::unique_ptr<DataBlockIter> biter{
          data_block->NewDataIterator(Options().comparator, seqno)};

      // SeekForGet() some key that does not exist
      biter->SeekForGet(keys[kNumRecords]);
      TestIterateForward(biter, verification_count);

      verification_count = 0;
      biter->SeekForGet(keys[kNumRecords / 2]);
      ASSERT_GE(verification_count, 1);
      TestIterateForward(biter, verification_count);

      TestSeekToFirst(biter, verification_count);
      TestSeekToLast(biter, verification_count);
      TestSeekForPrev(biter, verification_count, keys[kNumRecords / 2]);
      TestSeek(biter, verification_count, keys[kNumRecords / 2]);
    }
  }
}

class IndexBlockKVChecksumTest
    : public BlockPerKVChecksumTest,
      public testing::WithParamInterface<
          std::tuple<BlockBasedTableOptions::DataBlockIndexType, uint8_t,
                     uint32_t, bool, bool>> {
 public:
  IndexBlockKVChecksumTest() = default;

  BlockBasedTableOptions::DataBlockIndexType GetDataBlockIndexType() const {
    return std::get<0>(GetParam());
  }
  uint8_t GetChecksumLen() const { return std::get<1>(GetParam()); }
  uint32_t GetRestartInterval() const { return std::get<2>(GetParam()); }
  bool UseValueDeltaEncoding() const { return std::get<3>(GetParam()); }
  bool IncludeFirstKey() const { return std::get<4>(GetParam()); }

  std::unique_ptr<Block_kIndex> GenerateIndexBlock(
      std::vector<std::string>& separators,
      std::vector<BlockHandle>& block_handles,
      std::vector<std::string>& first_keys, int num_record) {
    Options options = Options();
    uint8_t protection_bytes_per_key = GetChecksumLen();
    BlockCreateContext create_context{
        kTableOptions(),
        nullptr /* ioptions */,
        nullptr /* statistics */,
        kDecompressor(),
        protection_bytes_per_key,
        options.comparator,
        !UseValueDeltaEncoding() /* value_is_full */,
        IncludeFirstKey(),
        /*index_value_delta_escape=*/false};
    builder_ = std::make_unique<BlockBuilder>(
        static_cast<int>(GetRestartInterval()), true /* use_delta_encoding */,
        UseValueDeltaEncoding() /* use_value_delta_encoding */,
        GetDataBlockIndexType(), 0.75 /* data_block_hash_table_util_ratio */,
        0 /* ts_sz */, true /* persist_user_defined_timestamps */,
        false /* is_user_key */, false /* use_separated_kv_storage */,
        nullptr /* statistics */, -1.0 /* uniform_cv_threshold */,
        false /* use_common_prefix */);
    BlockHandle last_encoded_handle;
    for (int i = 0; i < num_record; i++) {
      IndexValue entry(block_handles[i], first_keys[i]);
      std::string encoded_entry;
      std::string delta_encoded_entry;
      entry.EncodeTo(&encoded_entry, IncludeFirstKey(), nullptr);
      if (UseValueDeltaEncoding() && i > 0) {
        entry.EncodeTo(&delta_encoded_entry, IncludeFirstKey(),
                       &last_encoded_handle);
      }

      last_encoded_handle = entry.handle;
      const Slice delta_encoded_entry_slice(delta_encoded_entry);
      builder_->Add(separators[i], encoded_entry, delta_encoded_entry_slice);
    }
    // read serialized contents of the block
    Slice raw_block = builder_->Finish();
    // create block reader
    BlockContents contents;
    contents.data = raw_block;
    std::unique_ptr<Block_kIndex> index_block;

    create_context.Create(&index_block, std::move(contents));
    return index_block;
  }

  std::unique_ptr<BlockBuilder> builder_;
};

INSTANTIATE_TEST_CASE_P(
    P, IndexBlockKVChecksumTest,
    ::testing::Combine(
        ::testing::Values(
            BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch,
            BlockBasedTableOptions::DataBlockIndexType::
                kDataBlockBinaryAndHash),
        ::testing::Values(0, 1, 2, 4, 8), ::testing::Values(1, 3, 8, 16),
        ::testing::Values(true, false), ::testing::Values(true, false)),
    [](const testing::TestParamInfo<
        std::tuple<BlockBasedTableOptions::DataBlockIndexType, uint8_t,
                   uint32_t, bool, bool>>& args) {
      std::ostringstream oss;
      oss << GetDataBlockIndexTypeStr(std::get<0>(args.param)) << "ProtBytes"
          << std::to_string(std::get<1>(args.param)) << "RestartInterval"
          << std::to_string(std::get<2>(args.param)) << "ValueDeltaEncode"
          << std::to_string(std::get<3>(args.param)) << "IncludeFirstKey"
          << std::to_string(std::get<4>(args.param));
      return oss.str();
    });

TEST_P(IndexBlockKVChecksumTest, ChecksumConstructionAndVerification) {
  Options options = Options();
  uint8_t protection_bytes_per_key = GetChecksumLen();
  std::vector<int> num_restart_intervals = {1, 16};
  std::vector<SequenceNumber> seqnos{kDisableGlobalSequenceNumber, 10001};

  for (const auto num_restart_interval : num_restart_intervals) {
    const int kNumRecords =
        num_restart_interval * static_cast<int>(GetRestartInterval());
    for (const auto seqno : seqnos) {
      std::vector<std::string> separators;
      std::vector<BlockHandle> block_handles;
      std::vector<std::string> first_keys;
      GenerateRandomIndexEntries(&separators, &block_handles, &first_keys,
                                 kNumRecords, 0 /* ts_sz */);
      SyncPoint::GetInstance()->DisableProcessing();
      std::unique_ptr<Block_kIndex> index_block = GenerateIndexBlock(
          separators, block_handles, first_keys, kNumRecords);
      IndexBlockIter* kNullIter = nullptr;
      Statistics* kNullStats = nullptr;
      // read contents of block sequentially
      std::unique_ptr<IndexBlockIter> biter{index_block->NewIndexIterator(
          options.comparator, seqno, kNullIter, kNullStats,
          true /* total_order_seek */, IncludeFirstKey() /* have_first_key */,
          true /* key_includes_seq */,
          !UseValueDeltaEncoding() /* value_is_full */,
          true /* block_contents_pinned*/,
          true /* user_defined_timestamps_persisted */,
          nullptr /* prefix_index */, BlockBasedTableOptions::kBinary,
          false /* value_delta_escape */)};
      biter->SeekToFirst();
      const char* checksum_ptr = index_block->TEST_GetKVChecksum();
      // Check checksum of correct length is generated
      for (int i = 0; i < kNumRecords; i++) {
        // Obtaining the actual content written as value to index block is not
        // trivial: delta-encoded value is only persisted when not at block
        // restart point and that keys share some byte (see more in
        // BlockBuilder::AddWithLastKeyImpl()). So here we just do verification
        // using value from iterator unlike tests for DataBlockIter or
        // MetaBlockIter.
        ASSERT_TRUE(VerifyChecksum(protection_bytes_per_key, checksum_ptr,
                                   biter->key(), biter->raw_value()));
      }

      size_t verification_count = 0;
      // The SyncPoint is placed before checking checksum_len == 0 in
      // Block::VerifyChecksum(). To make the testing code below simpler and not
      // having to differentiate 0 vs non-0 checksum_len, we do an explicit
      // assert checking on checksum_len here.
      SyncPoint::GetInstance()->SetCallBack(
          "Block::VerifyChecksum::checksum_len",
          [&verification_count, protection_bytes_per_key](void* checksum_len) {
            ASSERT_EQ((*static_cast<uint8_t*>(checksum_len)),
                      protection_bytes_per_key);
            ++verification_count;
          });
      SyncPoint::GetInstance()->EnableProcessing();

      TestSeekToFirst(biter, verification_count);
      TestSeekToLast(biter, verification_count);
      TestSeek(biter, verification_count, first_keys[kNumRecords / 2]);
    }
  }
}

class MetaIndexBlockKVChecksumTest
    : public BlockPerKVChecksumTest,
      public testing::WithParamInterface<
          uint8_t /* block_protection_bytes_per_key */> {
 public:
  MetaIndexBlockKVChecksumTest() = default;
  uint8_t GetChecksumLen() const { return GetParam(); }
  uint32_t GetRestartInterval() const { return 1; }

  std::unique_ptr<Block_kMetaIndex> GenerateMetaIndexBlock(
      std::vector<std::string>& keys, std::vector<std::string>& values,
      int num_record) {
    Options options = Options();
    uint8_t protection_bytes_per_key = GetChecksumLen();
    BlockCreateContext create_context{kTableOptions(),
                                      nullptr /* ioptions */,
                                      nullptr /* statistics */,
                                      kDecompressor(),
                                      protection_bytes_per_key,
                                      options.comparator,
                                      /*index_value_is_full=*/false,
                                      /*index_has_first_key=*/false,
                                      /*index_value_delta_escape=*/false};
    builder_ = std::make_unique<BlockBuilder>(
        BlockBuilder::ForMetaBlock{}, static_cast<int>(GetRestartInterval()));
    // add a bunch of records to a block
    for (int i = 0; i < num_record; i++) {
      builder_->Add(keys[i], values[i]);
    }
    Slice raw_block = builder_->Finish();
    BlockContents contents;
    contents.data = raw_block;
    std::unique_ptr<Block_kMetaIndex> meta_block;
    create_context.Create(&meta_block, std::move(contents));
    return meta_block;
  }

  std::unique_ptr<BlockBuilder> builder_;
};

INSTANTIATE_TEST_CASE_P(P, MetaIndexBlockKVChecksumTest,
                        ::testing::Values(0, 1, 2, 4, 8),
                        [](const testing::TestParamInfo<uint8_t>& args) {
                          std::ostringstream oss;
                          oss << "ProtBytes" << std::to_string(args.param);
                          return oss.str();
                        });

TEST_P(MetaIndexBlockKVChecksumTest, ChecksumConstructionAndVerification) {
  Options options = Options();
  uint8_t protection_bytes_per_key = GetChecksumLen();
  BlockCreateContext create_context{kTableOptions(),
                                    nullptr /* ioptions */,
                                    nullptr /* statistics */,
                                    kDecompressor(),
                                    protection_bytes_per_key,
                                    options.comparator,
                                    /*index_value_is_full=*/false,
                                    /*index_has_first_key=*/false,
                                    /*index_value_delta_escape=*/false};
  std::vector<int> num_restart_intervals = {1, 16};
  for (const auto num_restart_interval : num_restart_intervals) {
    const int kNumRecords = num_restart_interval * GetRestartInterval();
    std::vector<std::string> keys;
    std::vector<std::string> values;
    GenerateRandomKVs(&keys, &values, 0, kNumRecords + 1, 1 /* step */,
                      24 /* padding_size */);
    SyncPoint::GetInstance()->DisableProcessing();
    std::unique_ptr<Block_kMetaIndex> meta_block =
        GenerateMetaIndexBlock(keys, values, kNumRecords);
    const char* checksum_ptr = meta_block->TEST_GetKVChecksum();
    // Check checksum of correct length is generated
    for (int i = 0; i < kNumRecords; i++) {
      ASSERT_TRUE(VerifyChecksum(protection_bytes_per_key,
                                 checksum_ptr + i * protection_bytes_per_key,
                                 keys[i], values[i]));
    }

    size_t verification_count = 0;
    // The SyncPoint is placed before checking checksum_len == 0 in
    // Block::VerifyChecksum(). To make the testing code below simpler and not
    // having to differentiate 0 vs non-0 checksum_len, we do an explicit assert
    // checking on checksum_len here.
    SyncPoint::GetInstance()->SetCallBack(
        "Block::VerifyChecksum::checksum_len",
        [&verification_count, protection_bytes_per_key](void* checksum_len) {
          ASSERT_EQ((*static_cast<uint8_t*>(checksum_len)),
                    protection_bytes_per_key);
          ++verification_count;
        });
    SyncPoint::GetInstance()->EnableProcessing();
    // Check that block iterator does checksum verification
    std::unique_ptr<MetaBlockIter> biter{
        meta_block->NewMetaIterator(true /* block_contents_pinned */)};
    TestSeekToFirst(biter, verification_count);
    TestSeekToLast(biter, verification_count);
    TestSeek(biter, verification_count, keys[kNumRecords / 2]);
    TestSeekForPrev(biter, verification_count, keys[kNumRecords / 2]);
  }
}

class DataBlockKVChecksumCorruptionTest : public DataBlockKVChecksumTest {
 public:
  DataBlockKVChecksumCorruptionTest() = default;

  std::unique_ptr<DataBlockIter> GenerateDataBlockIter(
      std::vector<std::string>& keys, std::vector<std::string>& values,
      int num_record) {
    // During Block construction, we may create block iter to initialize per kv
    // checksum. Disable syncpoint that may be created for block iter methods.
    SyncPoint::GetInstance()->DisableProcessing();
    block_ = GenerateDataBlock(keys, values, num_record);
    std::unique_ptr<DataBlockIter> biter{block_->NewDataIterator(
        Options().comparator, kDisableGlobalSequenceNumber)};
    SyncPoint::GetInstance()->EnableProcessing();
    return biter;
  }

 protected:
  std::unique_ptr<Block_kData> block_;
};

TEST_P(DataBlockKVChecksumCorruptionTest, CorruptEntry) {
  std::vector<int> num_restart_intervals = {1, 3};
  for (const auto num_restart_interval : num_restart_intervals) {
    const int kNumRecords =
        num_restart_interval * static_cast<int>(GetRestartInterval());
    std::vector<std::string> keys;
    std::vector<std::string> values;
    GenerateRandomKVs(&keys, &values, 0, kNumRecords + 1, 1 /* step */,
                      24 /* padding_size */);
    SyncPoint::GetInstance()->SetCallBack(
        "BlockIter::UpdateKey::value", [](void* arg) {
          char* value = static_cast<char*>(arg);
          // values generated by GenerateRandomKVs are of length 100
          ++value[10];
        });

    // Purely for reducing the number of lines of code.
    using IterPtr = std::unique_ptr<DataBlockIter>;
    using IterAPI = void(IterPtr & iter, std::string&);

    std::string seek_key = keys[kNumRecords / 2];
    auto test_seek = [&](IterAPI iter_api) {
      IterPtr biter = GenerateDataBlockIter(keys, values, kNumRecords);
      ASSERT_OK(biter->status());
      iter_api(biter, seek_key);
      ASSERT_FALSE(biter->Valid());
      ASSERT_TRUE(biter->status().IsCorruption());
    };

    test_seek([](IterPtr& iter, std::string&) { iter->SeekToFirst(); });
    test_seek([](IterPtr& iter, std::string&) { iter->SeekToLast(); });
    test_seek([](IterPtr& iter, std::string& k) { iter->Seek(k); });
    test_seek([](IterPtr& iter, std::string& k) { iter->SeekForPrev(k); });
    test_seek([](IterPtr& iter, std::string& k) { iter->SeekForGet(k); });

    using IterStepAPI = void (DataBlockIter::*)();
    auto test_step = [&](IterStepAPI iter_api, std::string& k) {
      IterPtr biter = GenerateDataBlockIter(keys, values, kNumRecords);
      SyncPoint::GetInstance()->DisableProcessing();
      biter->Seek(k);
      ASSERT_TRUE(biter->Valid());
      ASSERT_OK(biter->status());
      SyncPoint::GetInstance()->EnableProcessing();
      std::invoke(iter_api, biter);
      ASSERT_FALSE(biter->Valid());
      ASSERT_TRUE(biter->status().IsCorruption());
    };

    if (kNumRecords > 1) {
      test_step(&DataBlockIter::Prev, seek_key);
      test_step(&DataBlockIter::Next, seek_key);
    }
  }
}

INSTANTIATE_TEST_CASE_P(
    P, DataBlockKVChecksumCorruptionTest,
    ::testing::Combine(
        ::testing::Values(
            BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch,
            BlockBasedTableOptions::DataBlockIndexType::
                kDataBlockBinaryAndHash),
        ::testing::Values(4, 8) /* block_protection_bytes_per_key */,
        ::testing::Values(1, 3, 8, 16) /* restart_interval */,
        ::testing::Values(false, true)),
    [](const testing::TestParamInfo<
        std::tuple<BlockBasedTableOptions::DataBlockIndexType, uint8_t,
                   uint32_t, bool>>& args) {
      std::ostringstream oss;
      oss << GetDataBlockIndexTypeStr(std::get<0>(args.param)) << "ProtBytes"
          << std::to_string(std::get<1>(args.param)) << "RestartInterval"
          << std::to_string(std::get<2>(args.param)) << "DeltaEncode"
          << std::to_string(std::get<3>(args.param));
      return oss.str();
    });

class IndexBlockKVChecksumCorruptionTest : public IndexBlockKVChecksumTest {
 public:
  IndexBlockKVChecksumCorruptionTest() = default;

  std::unique_ptr<IndexBlockIter> GenerateIndexBlockIter(
      std::vector<std::string>& separators,
      std::vector<BlockHandle>& block_handles,
      std::vector<std::string>& first_keys, int num_record,
      SequenceNumber seqno) {
    SyncPoint::GetInstance()->DisableProcessing();
    block_ =
        GenerateIndexBlock(separators, block_handles, first_keys, num_record);
    std::unique_ptr<IndexBlockIter> biter{block_->NewIndexIterator(
        Options().comparator, seqno, nullptr, nullptr,
        true /* total_order_seek */, IncludeFirstKey() /* have_first_key */,
        true /* key_includes_seq */,
        !UseValueDeltaEncoding() /* value_is_full */,
        true /* block_contents_pinned */,
        true /* user_defined_timestamps_persisted */,
        nullptr /* prefix_index */, BlockBasedTableOptions::kBinary,
        false /* value_delta_escape */)};
    SyncPoint::GetInstance()->EnableProcessing();
    return biter;
  }

 protected:
  std::unique_ptr<Block_kIndex> block_;
};

INSTANTIATE_TEST_CASE_P(
    P, IndexBlockKVChecksumCorruptionTest,
    ::testing::Combine(
        ::testing::Values(
            BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch,
            BlockBasedTableOptions::DataBlockIndexType::
                kDataBlockBinaryAndHash),
        ::testing::Values(4, 8) /* block_protection_bytes_per_key */,
        ::testing::Values(1, 3, 8, 16) /* restart_interval */,
        ::testing::Values(true, false), ::testing::Values(true, false)),
    [](const testing::TestParamInfo<
        std::tuple<BlockBasedTableOptions::DataBlockIndexType, uint8_t,
                   uint32_t, bool, bool>>& args) {
      std::ostringstream oss;
      oss << GetDataBlockIndexTypeStr(std::get<0>(args.param)) << "ProtBytes"
          << std::to_string(std::get<1>(args.param)) << "RestartInterval"
          << std::to_string(std::get<2>(args.param)) << "ValueDeltaEncode"
          << std::to_string(std::get<3>(args.param)) << "IncludeFirstKey"
          << std::to_string(std::get<4>(args.param));
      return oss.str();
    });

TEST_P(IndexBlockKVChecksumCorruptionTest, CorruptEntry) {
  std::vector<int> num_restart_intervals = {1, 3};
  std::vector<SequenceNumber> seqnos{kDisableGlobalSequenceNumber, 10001};

  for (const auto num_restart_interval : num_restart_intervals) {
    const int kNumRecords =
        num_restart_interval * static_cast<int>(GetRestartInterval());
    for (const auto seqno : seqnos) {
      std::vector<std::string> separators;
      std::vector<BlockHandle> block_handles;
      std::vector<std::string> first_keys;
      GenerateRandomIndexEntries(&separators, &block_handles, &first_keys,
                                 kNumRecords, 0 /* ts_sz */);
      SyncPoint::GetInstance()->SetCallBack(
          "BlockIter::UpdateKey::value", [](void* arg) {
            char* value = static_cast<char*>(arg);
            // value can be delta-encoded with different lengths, so we corrupt
            // first bytes here to be safe
            ++value[0];
          });

      using IterPtr = std::unique_ptr<IndexBlockIter>;
      using IterAPI = void(IterPtr & iter, std::string&);
      std::string seek_key = first_keys[kNumRecords / 2];
      auto test_seek = [&](IterAPI iter_api) {
        std::unique_ptr<IndexBlockIter> biter = GenerateIndexBlockIter(
            separators, block_handles, first_keys, kNumRecords, seqno);
        ASSERT_OK(biter->status());
        iter_api(biter, seek_key);
        ASSERT_FALSE(biter->Valid());
        ASSERT_TRUE(biter->status().IsCorruption());
      };
      test_seek([](IterPtr& iter, std::string&) { iter->SeekToFirst(); });
      test_seek([](IterPtr& iter, std::string&) { iter->SeekToLast(); });
      test_seek([](IterPtr& iter, std::string& k) { iter->Seek(k); });

      using IterStepAPI = void (IndexBlockIter::*)();
      auto test_step = [&](IterStepAPI iter_api, std::string& k) {
        std::unique_ptr<IndexBlockIter> biter = GenerateIndexBlockIter(
            separators, block_handles, first_keys, kNumRecords, seqno);
        SyncPoint::GetInstance()->DisableProcessing();
        biter->Seek(k);
        ASSERT_TRUE(biter->Valid());
        ASSERT_OK(biter->status());
        SyncPoint::GetInstance()->EnableProcessing();
        std::invoke(iter_api, biter);
        ASSERT_FALSE(biter->Valid());
        ASSERT_TRUE(biter->status().IsCorruption());
      };
      if (kNumRecords > 1) {
        test_step(&IndexBlockIter::Prev, seek_key);
        test_step(&IndexBlockIter::Next, seek_key);
      }
    }
  }
}

class MetaIndexBlockKVChecksumCorruptionTest
    : public MetaIndexBlockKVChecksumTest {
 public:
  MetaIndexBlockKVChecksumCorruptionTest() = default;

  std::unique_ptr<MetaBlockIter> GenerateMetaIndexBlockIter(
      std::vector<std::string>& keys, std::vector<std::string>& values,
      int num_record) {
    SyncPoint::GetInstance()->DisableProcessing();
    block_ = GenerateMetaIndexBlock(keys, values, num_record);
    std::unique_ptr<MetaBlockIter> biter{
        block_->NewMetaIterator(true /* block_contents_pinned */)};
    SyncPoint::GetInstance()->EnableProcessing();
    return biter;
  }

 protected:
  std::unique_ptr<Block_kMetaIndex> block_;
};

INSTANTIATE_TEST_CASE_P(
    P, MetaIndexBlockKVChecksumCorruptionTest,
    ::testing::Values(4, 8) /* block_protection_bytes_per_key */,
    [](const testing::TestParamInfo<uint8_t>& args) {
      std::ostringstream oss;
      oss << "ProtBytes" << std::to_string(args.param);
      return oss.str();
    });

TEST_P(MetaIndexBlockKVChecksumCorruptionTest, CorruptEntry) {
  Options options = Options();
  std::vector<int> num_restart_intervals = {1, 3};
  for (const auto num_restart_interval : num_restart_intervals) {
    const int kNumRecords =
        num_restart_interval * static_cast<int>(GetRestartInterval());
    std::vector<std::string> keys;
    std::vector<std::string> values;
    GenerateRandomKVs(&keys, &values, 0, kNumRecords + 1, 1 /* step */,
                      24 /* padding_size */);
    SyncPoint::GetInstance()->SetCallBack(
        "BlockIter::UpdateKey::value", [](void* arg) {
          char* value = static_cast<char*>(arg);
          // values generated by GenerateRandomKVs are of length 100
          ++value[10];
        });

    using IterPtr = std::unique_ptr<MetaBlockIter>;
    using IterAPI = void(IterPtr & iter, std::string&);
    using IterStepAPI = void (MetaBlockIter::*)();
    std::string seek_key = keys[kNumRecords / 2];
    auto test_seek = [&](IterAPI iter_api) {
      IterPtr biter = GenerateMetaIndexBlockIter(keys, values, kNumRecords);
      ASSERT_OK(biter->status());
      iter_api(biter, seek_key);
      ASSERT_FALSE(biter->Valid());
      ASSERT_TRUE(biter->status().IsCorruption());
    };

    test_seek([](IterPtr& iter, std::string&) { iter->SeekToFirst(); });
    test_seek([](IterPtr& iter, std::string&) { iter->SeekToLast(); });
    test_seek([](IterPtr& iter, std::string& k) { iter->Seek(k); });
    test_seek([](IterPtr& iter, std::string& k) { iter->SeekForPrev(k); });

    auto test_step = [&](IterStepAPI iter_api, const std::string& k) {
      IterPtr biter = GenerateMetaIndexBlockIter(keys, values, kNumRecords);
      SyncPoint::GetInstance()->DisableProcessing();
      biter->Seek(k);
      ASSERT_TRUE(biter->Valid());
      ASSERT_OK(biter->status());
      SyncPoint::GetInstance()->EnableProcessing();
      std::invoke(iter_api, biter);
      ASSERT_FALSE(biter->Valid());
      ASSERT_TRUE(biter->status().IsCorruption());
    };

    if (kNumRecords > 1) {
      test_step(&MetaBlockIter::Prev, seek_key);
      test_step(&MetaBlockIter::Next, seek_key);
    }
  }
}

class BlockEntryCorruptionTest : public testing::TestWithParam<bool> {
 public:
  bool useSeparatedKVStorage() const { return GetParam(); }

  std::string BuildBlock() {
    BlockBuilder builder(
        1 /* restart_interval */, true /* use_delta_encoding */,
        false /* use_value_delta_encoding */,
        BlockBasedTableOptions::kDataBlockBinarySearch,
        0 /* data_block_hash_table_util_ratio */, 0 /* ts_sz */,
        false /* persist_udt */, true /* is_user_key */,
        useSeparatedKVStorage(), nullptr /* statistics */,
        -1.0 /* uniform_cv_threshold */, false /* use_common_prefix */);
    builder.Add("key001", "val01");
    builder.Add("key002", "val02");
    builder.Add("key003", "val03");
    builder.Add("key004", "val04");
    Slice raw = builder.Finish();
    return std::string(raw.data(), raw.size());
  }

  // Get the restart offset for a given restart index from the raw block data.
  uint32_t GetRestartOffset(const std::string& block_data, int restart_idx) {
    size_t restarts_start = GetRestartsStart(block_data);
    return DecodeFixed32(block_data.data() + restarts_start +
                         restart_idx * sizeof(uint32_t));
  }

  void SetRestartOffset(std::string* block_data, int restart_idx,
                        uint32_t restart_offset) {
    size_t restarts_start = GetRestartsStart(*block_data);
    EncodeFixed32(
        &(*block_data)[restarts_start + restart_idx * sizeof(uint32_t)],
        restart_offset);
  }

  size_t GetRestartsStart(const std::string& block_data) {
    size_t footer_size = useSeparatedKVStorage() ? 8 : 4;
    uint32_t packed = DecodeFixed32(block_data.data() + block_data.size() - 4);
    uint32_t num_restarts = packed & DataBlockFooter::kMaxNumRestarts;
    return block_data.size() - footer_size - num_restarts * sizeof(uint32_t);
  }

  uint32_t GetKeyEnd(const std::string& block_data) {
    size_t footer_size = useSeparatedKVStorage() ? 8 : 4;
    uint32_t packed = DecodeFixed32(block_data.data() + block_data.size() - 4);
    uint32_t num_restarts = packed & DataBlockFooter::kMaxNumRestarts;
    uint32_t restarts_start = static_cast<uint32_t>(
        block_data.size() - footer_size - num_restarts * sizeof(uint32_t));
    if (useSeparatedKVStorage()) {
      // values_section_offset is stored as the 4 bytes before the packed word.
      return DecodeFixed32(block_data.data() + block_data.size() - 8);
    }
    return restarts_start;
  }

  uint32_t GetValueEnd(const std::string& block_data) {
    size_t footer_size = useSeparatedKVStorage() ? 8 : 4;
    uint32_t packed = DecodeFixed32(block_data.data() + block_data.size() - 4);
    uint32_t num_restarts = packed & DataBlockFooter::kMaxNumRestarts;
    return static_cast<uint32_t>(block_data.size() - footer_size -
                                 num_restarts * sizeof(uint32_t));
  }
};

INSTANTIATE_TEST_CASE_P(P, BlockEntryCorruptionTest, ::testing::Bool(),
                        [](const testing::TestParamInfo<bool>& args) {
                          return args.param ? "SeparatedKV" : "InlineKV";
                        });

TEST_P(BlockEntryCorruptionTest, CorruptedKeyLengthPastKeyEnd) {
  std::string block_data = BuildBlock();
  uint32_t key_end = GetKeyEnd(block_data);

  // Corrupt the key length of the first entry so that
  // the key data would extend past the keys-end boundary.
  uint32_t first_entry_offset = GetRestartOffset(block_data, 0);
  block_data[first_entry_offset + 1] = static_cast<char>(key_end);

  BlockContents contents;
  contents.data = Slice(block_data);
  Block block(std::move(contents), 0, nullptr, 1 /* restart_interval */);
  std::unique_ptr<MetaBlockIter> iter(block.NewMetaIterator());

  // SeekToFirst should hit the corrupted first entry immediately.
  iter->SeekToFirst();
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().IsCorruption());
}

TEST_P(BlockEntryCorruptionTest, CorruptedValueLengthPastValueEnd) {
  std::string block_data = BuildBlock();
  uint32_t value_end = GetValueEnd(block_data);

  // Corrupt the first entry so that its value would extend past the
  // values-end boundary (start of the restart array).
  uint32_t first_entry_offset = GetRestartOffset(block_data, 0);
  block_data[first_entry_offset + 2] = static_cast<char>(value_end);

  BlockContents contents;
  contents.data = Slice(block_data);
  Block block(std::move(contents), 0, nullptr, 1 /* restart_interval */);
  std::unique_ptr<MetaBlockIter> iter(block.NewMetaIterator());

  // SeekToFirst should hit the corrupted first entry immediately.
  iter->SeekToFirst();
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().IsCorruption());
}

TEST_P(BlockEntryCorruptionTest, DataIteratorRejectsEntryPastSectionEnd) {
  for (int length_byte_offset : {1, 2}) {
    SCOPED_TRACE("length_byte_offset=" + std::to_string(length_byte_offset));
    std::string block_data = BuildBlock();
    uint32_t first_entry_offset = GetRestartOffset(block_data, 0);
    uint32_t invalid_length = length_byte_offset == 1 ? GetKeyEnd(block_data)
                                                      : GetValueEnd(block_data);
    ASSERT_LT(invalid_length, 128);
    block_data[first_entry_offset + length_byte_offset] =
        static_cast<char>(invalid_length);

    BlockContents contents;
    contents.data = Slice(block_data);
    Block block(std::move(contents), 0, nullptr, 1 /* restart_interval */);
    std::unique_ptr<DataBlockIter> iter(block.NewDataIterator(
        BytewiseComparator(), kDisableGlobalSequenceNumber, nullptr, nullptr,
        true /* block_contents_pinned */,
        true /* user_defined_timestamps_persisted */));

    iter->SeekToFirst();
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsCorruption());
  }
}

TEST_P(BlockEntryCorruptionTest, SeekRejectsRestartKeyPastKeysEnd) {
  std::string block_data = BuildBlock();
  uint32_t key_end = GetKeyEnd(block_data);
  uint32_t second_entry_offset = GetRestartOffset(block_data, 1);
  ASSERT_LT(key_end, 128);
  block_data[second_entry_offset + 1] = static_cast<char>(key_end);

  BlockContents contents;
  contents.data = Slice(block_data);
  Block block(std::move(contents), 0, nullptr, 1 /* restart_interval */);
  std::unique_ptr<MetaBlockIter> iter(block.NewMetaIterator());

  iter->Seek("key999");
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().IsCorruption());
}

TEST_P(BlockEntryCorruptionTest, RejectsValueOffsetPastValuesEnd) {
  if (!useSeparatedKVStorage()) {
    return;
  }

  std::string block_data = BuildBlock();
  uint32_t first_entry_offset = GetRestartOffset(block_data, 0);
  uint32_t values_size = GetValueEnd(block_data) - GetKeyEnd(block_data);
  ASSERT_LT(values_size, 128);
  block_data[first_entry_offset + 3] = static_cast<char>(values_size);

  BlockContents contents;
  contents.data = Slice(block_data);
  Block block(std::move(contents), 0, nullptr, 1 /* restart_interval */);
  std::unique_ptr<DataBlockIter> iter(
      block.NewDataIterator(BytewiseComparator(), kDisableGlobalSequenceNumber,
                            nullptr, nullptr, true /* block_contents_pinned */,
                            true /* user_defined_timestamps_persisted */));

  iter->SeekToFirst();
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().IsCorruption());
}

TEST_P(BlockEntryCorruptionTest, InvalidRestartOffsets) {
  for (bool past_keys_end : {false, true}) {
    SCOPED_TRACE("past_keys_end=" + std::to_string(past_keys_end));
    std::string block_data = BuildBlock();
    uint32_t invalid_offset =
        past_keys_end ? GetKeyEnd(block_data) : GetRestartOffset(block_data, 0);
    SetRestartOffset(&block_data, 1, invalid_offset);

    BlockContents contents;
    contents.data = Slice(block_data);
    Block block(std::move(contents), 0, nullptr, 1 /* restart_interval */);
    ASSERT_EQ(block.size(), 0);
    std::unique_ptr<MetaBlockIter> iter(block.NewMetaIterator());
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsCorruption());
  }
}

TEST(BlockEntryCorruptionStandaloneTest, TruncatedEntryHeader) {
  for (size_t entry_size : {1U, 2U}) {
    SCOPED_TRACE("entry_size=" + std::to_string(entry_size));
    std::string block_data(entry_size, '\0');
    PutFixed32(&block_data, 0);
    DataBlockFooter(BlockBasedTableOptions::kDataBlockBinarySearch,
                    1 /* num_restarts */)
        .EncodeTo(&block_data);

    BlockContents contents;
    contents.data = Slice(block_data);
    Block block(std::move(contents), 0, nullptr, 1 /* restart_interval */);
    std::unique_ptr<MetaBlockIter> iter(block.NewMetaIterator());

    iter->SeekToFirst();
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsCorruption());
  }
}

TEST_F(BlockTest, SeparatedKVInvalidValuesSectionOffset) {
  BlockBuilder builder(
      16 /* restart_interval */, true /* use_delta_encoding */,
      false /* use_value_delta_encoding */,
      BlockBasedTableOptions::kDataBlockBinaryAndHash, 0.75 /* hash_ratio */,
      0 /* ts_sz */, true /* persist_user_defined_timestamps */,
      false /* is_user_key */, true /* separate_key_value */,
      nullptr /* statistics */, -1.0 /* uniform_cv_threshold */,
      false /* use_common_prefix */);

  for (int i = 0; i < 5; i++) {
    std::string key = "key" + std::to_string(i);
    AppendInternalKeyFooter(&key, 100 - i, kTypeValue);
    builder.Add(key, "value" + std::to_string(i));
  }

  Slice raw_block = builder.Finish();
  std::string block_data = raw_block.ToString();

  Slice footer_input(block_data);
  DataBlockFooter footer;
  ASSERT_OK(footer.DecodeFrom(&footer_input));
  ASSERT_TRUE(footer.separated_kv);
  footer.values_section_offset = static_cast<uint32_t>(block_data.size());

  std::string encoded_footer;
  footer.EncodeTo(&encoded_footer);
  ASSERT_GE(block_data.size(), encoded_footer.size());
  block_data.replace(block_data.size() - encoded_footer.size(),
                     encoded_footer.size(), encoded_footer);

  BlockContents contents;
  contents.data = Slice(block_data);

  Block block(std::move(contents), 0 /* read_amp_bytes_per_bit */,
              nullptr /* statistics */, 0 /* restart_interval */);

  ASSERT_EQ(block.size(), 0);

  std::unique_ptr<DataBlockIter> iter(
      block.NewDataIterator(BytewiseComparator(), kDisableGlobalSequenceNumber,
                            nullptr, nullptr, false, true));
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().IsCorruption());
  ASSERT_EQ(iter->status().ToString(), "Corruption: bad block contents");
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
