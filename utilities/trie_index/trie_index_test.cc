//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "file/writable_file_writer.h"
#include "options/cf_options.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "rocksdb/comparator.h"
#include "rocksdb/convenience.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "table/block_based/block.h"
#include "table/block_based/block_builder.h"
#include "table/block_based/user_defined_index_wrapper.h"
#include "table/format.h"
#include "table/table_builder.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"
#include "utilities/merge_operators.h"
#include "utilities/trie_index/trie.h"
#include "utilities/trie_index/trie_index_factory.h"

namespace ROCKSDB_NAMESPACE {
namespace trie_index {

// ============================================================================
// Helpers: pack sequence numbers with kTypeValue (1) into tags.
// Tests use plain sequence numbers for readability; these convert them to
// the tag format that IndexEntryContext and SeekContext expect.
IndexFactoryIterator::SeekContext SeekCtx(SequenceNumber seq) {
  return {PackSequenceAndType(seq, kValueTypeForSeek)};
}

IndexFactoryBuilder::IndexEntryContext EntryCtx(SequenceNumber last_seq,
                                                SequenceNumber first_seq) {
  return {last_seq ? ((static_cast<uint64_t>(last_seq) << 8) | 1) : 0,
          first_seq ? ((static_cast<uint64_t>(first_seq) << 8) | 1) : 0};
}

// Seed for randomized tests; log it with SCOPED_TRACE to reproduce a failure.
uint32_t TimeSeed() {
  return static_cast<uint32_t>(
      std::chrono::system_clock::now().time_since_epoch().count());
}

// A copy of data with one to three random bytes changed.
std::string FlipRandomBytes(std::string data, Random* rnd) {
  const int flips = 1 + rnd->Uniform(3);
  for (int f = 0; f < flips; f++) {
    data[rnd->Uniform(static_cast<int>(data.size()))] ^=
        static_cast<char>(1 + rnd->Uniform(255));
  }
  return data;
}

// Reading a malformed index may fail only with Corruption.
bool OkOrCorruption(const Status& s) {
  if (!s.ok()) {
    EXPECT_EQ(s.code(), Status::Code::kCorruption) << s.ToString();
  }
  return s.ok();
}

// The same for opening an index, which also reports NotSupported when the
// version field changed.
bool OpenedOrRejected(const Status& s) {
  return s.IsNotSupported() ? false : OkOrCorruption(s);
}

// ============================================================================
// TrieIndexFactory integration tests
// ============================================================================

class TrieIndexFactoryTest : public testing::Test {
 protected:
  void SetUp() override { factory_ = std::make_shared<TrieIndexFactory>(); }

  // ---- Table-driven test helpers ----

  // Descriptor for a single data block in a seqno test.
  struct TestBlock {
    std::string last_key;
    std::string next_key;  // empty = last block (nullptr)
    uint64_t offset;
    uint64_t size;
    SequenceNumber last_seq;
    SequenceNumber first_seq;
  };

  // Owns the builder (which holds the serialized data), reader, and iterator.
  // All must stay alive for the iterator to be usable.
  struct TrieTestContext {
    std::unique_ptr<IndexFactoryBuilder> builder;
    Slice index_contents;
    std::unique_ptr<IndexFactoryReader> reader;
    std::unique_ptr<IndexFactoryIterator> iter;
  };

  // Build a trie from TestBlocks and return a context with a ready iterator.
  TrieTestContext BuildTrieAndGetIterator(
      const std::vector<TestBlock>& blocks) {
    TrieTestContext ctx;
    IndexFactoryOptions option;
    option.comparator = BytewiseComparator();

    EXPECT_OK(factory_->NewBuilder(option, ctx.builder));
    for (const auto& b : blocks) {
      IndexFactoryBuilder::BlockHandle h{b.offset, b.size};
      std::string scratch;
      if (!b.next_key.empty()) {
        Slice next(b.next_key);
        ctx.builder->AddIndexEntry(Slice(b.last_key), &next, h, &scratch,
                                   EntryCtx(b.last_seq, b.first_seq));
      } else {
        ctx.builder->AddIndexEntry(Slice(b.last_key), nullptr, h, &scratch,
                                   EntryCtx(b.last_seq, 0));
      }
    }
    EXPECT_OK(ctx.builder->Finish(&ctx.index_contents));
    EXPECT_OK(factory_->NewReader(option, ctx.index_contents, ctx.reader));
    ReadOptions ro;
    ctx.iter = ctx.reader->NewIterator(ro);
    return ctx;
  }

  // Seek and assert the resulting block offset.
  static void AssertSeekOffset(IndexFactoryIterator* iter, const Slice& target,
                               SequenceNumber seq, uint64_t expected_offset) {
    IterateResult result;
    ASSERT_OK(iter->SeekAndGetResult(target, &result, SeekCtx(seq)));
    ASSERT_EQ(iter->value().offset, expected_offset)
        << "Seek(\"" << target.ToString() << "\"|" << seq
        << ") expected offset " << expected_offset << " but got "
        << iter->value().offset;
  }

  // Full forward scan: Seek to first_key|kMaxSequenceNumber, then Next through
  // all blocks, asserting each offset matches expected_offsets. Also asserts
  // kUnknown past the end.
  static void AssertFullForwardScan(
      IndexFactoryIterator* iter, const Slice& first_key,
      const std::vector<uint64_t>& expected_offsets) {
    ASSERT_FALSE(expected_offsets.empty());
    IterateResult result;
    ASSERT_OK(iter->SeekAndGetResult(first_key, &result,
                                     SeekCtx(kMaxSequenceNumber)));
    ASSERT_EQ(iter->value().offset, expected_offsets[0]);
    for (size_t i = 1; i < expected_offsets.size(); i++) {
      ASSERT_OK(iter->NextAndGetResult(&result));
      ASSERT_EQ(iter->value().offset, expected_offsets[i])
          << "Next failed at block " << i;
    }
    // Past end.
    ASSERT_OK(iter->NextAndGetResult(&result));
    ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);
  }

  // Blocks of 100 bytes laid out back to back.
  static std::vector<IndexFactoryBuilder::BlockHandle> UniformHandles(
      size_t n) {
    std::vector<IndexFactoryBuilder::BlockHandle> handles;
    handles.reserve(n);
    for (size_t i = 0; i < n; ++i) {
      handles.push_back({i * 100, 100});
    }
    return handles;
  }

  void AssertSerializedSizeBound(
      const std::vector<std::string>& keys,
      const std::vector<IndexFactoryBuilder::BlockHandle>& handles,
      bool parallel, uint64_t max_size_ratio) {
    std::unique_ptr<IndexFactoryBuilder> builder;
    ASSERT_OK(factory_->NewBuilder(IndexFactoryOptions(), builder));
    if (parallel && !builder->SupportsParallelAddEntry()) {
      return;
    }
    for (size_t i = 0; i < keys.size(); ++i) {
      const Slice key(keys[i]);
      const Slice next = i + 1 < keys.size() ? Slice(keys[i + 1]) : Slice();
      const Slice* next_key = i + 1 < keys.size() ? &next : nullptr;
      const IndexFactoryBuilder::BlockHandle& handle = handles[i];
      const IndexFactoryBuilder::IndexEntryContext context =
          EntryCtx(keys.size() - i, keys.size() - i - 1);
      std::string scratch;
      if (parallel) {
        auto prepared = builder->CreatePreparedAddEntry();
        builder->PrepareAddEntry(key, next_key, context, prepared.get());
        builder->FinishAddEntry(handle, prepared.get(), &scratch,
                                /*skip_delta_encoding=*/false);
      } else {
        builder->AddIndexEntry(key, next_key, handle, &scratch, context);
      }
    }
    const uint64_t estimate = builder->EstimatedSize();
    Slice contents;
    ASSERT_OK(builder->Finish(&contents));
    EXPECT_GE(estimate, contents.size());
    EXPECT_EQ(builder->EstimatedSize(), contents.size());
    if (max_size_ratio > 0) {
      EXPECT_LE(estimate, max_size_ratio * contents.size());
    }
  }

  std::shared_ptr<TrieIndexFactory> factory_;
};

TEST_F(TrieIndexFactoryTest, EstimatedSizeBoundsSerializedTrie) {
  struct KeySet {
    const char* name;
    std::vector<std::string> keys;
    uint64_t max_size_ratio = 0;
  };
  std::vector<KeySet> cases = {
      {"empty", {}},
      {"single", {"a"}},
      {"long", {std::string(1024, 'a')}},
      {"prefixes", {"a", "ab", "abc", "abcd", "b"}},
      {"overflow", std::vector<std::string>(64, "same")},
      {"dense", {}},
      {"shared_prefix", {}, 4},
      {"sparse", {}},
      {"dense_shared_prefix", {}, 4},
      {"random", {}},
  };
  for (int i = 1; i < 256; ++i) {
    cases[5].keys.emplace_back(1, static_cast<char>(i));
    cases[6].keys.push_back(std::string(64, 'p') + std::to_string(i));
  }
  for (int i = 1; i <= 64; ++i) {
    cases[7].keys.emplace_back(32 + i, static_cast<char>(i));
  }
  Random random(301);
  for (int i = 0; i < 4096; ++i) {
    cases[8].keys.push_back(std::string(1, static_cast<char>(i % 256)) +
                            std::string(512, 'p') + std::to_string(i));
    cases[9].keys.push_back(test::RandomKey(&random, 1 + i % 128));
  }

  for (auto& key_set : cases) {
    SCOPED_TRACE(key_set.name);
    std::sort(key_set.keys.begin(), key_set.keys.end());
    for (bool parallel : {false, true}) {
      SCOPED_TRACE(parallel);
      AssertSerializedSizeBound(key_set.keys,
                                UniformHandles(key_set.keys.size()), parallel,
                                key_set.max_size_ratio);
    }
  }

  // Blocks of varying size and spacing, one of them huge, as with block
  // alignment padding and a very large value.
  std::vector<IndexFactoryBuilder::BlockHandle> irregular;
  uint64_t offset = 0;
  for (size_t i = 0; i < cases[9].keys.size(); ++i) {
    const uint64_t size =
        i == 1000 ? uint64_t{1} << 30 : 1 + random.Uniform(8000);
    irregular.push_back({offset, size});
    offset += size + random.Uniform(4096);
  }
  for (bool parallel : {false, true}) {
    SCOPED_TRACE(parallel);
    AssertSerializedSizeBound(cases[9].keys, irregular, parallel, 0);
  }
}

// No real SST has handles like these, but Finish() accepts them, so the
// estimate must still bound its output.
TEST_F(TrieIndexFactoryTest, EstimatedSizeBoundsExtremeHandles) {
  constexpr uint64_t kMax = std::numeric_limits<uint64_t>::max();
  std::vector<std::string> keys;
  for (int i = 0; i < 63; ++i) {
    keys.push_back("k" + std::to_string(100 + i));
  }
  // Blocks alternating between a size of 1 and `large`, the last one large,
  // with equal gaps, shifted so that the last block ends at `end` modulo
  // 2^64.
  auto evenly_spaced = [&](uint64_t large, uint64_t gap, uint64_t end) {
    std::vector<IndexFactoryBuilder::BlockHandle> handles;
    uint64_t offset = 0;
    for (size_t i = 0; i < keys.size(); ++i) {
      const uint64_t size = (keys.size() - 1 - i) % 2 == 0 ? large : 1;
      handles.push_back({offset, size});
      offset += size + gap;
    }
    const uint64_t shift = end - (handles.back().offset + handles.back().size);
    for (IndexFactoryBuilder::BlockHandle& handle : handles) {
      handle.offset += shift;
    }
    return handles;
  };
  // Overlapping blocks with 56-bit offset and size residuals, whose largest
  // step plus largest size is 2^58.
  constexpr uint64_t kStep = uint64_t{1} << 55;
  constexpr uint64_t kLarge = (uint64_t{1} << 58) - kStep;
  std::vector<IndexFactoryBuilder::BlockHandle> wide_residuals;
  for (size_t i = 0; i < keys.size(); ++i) {
    wide_residuals.push_back(
        {i == 0 ? 0 : kStep + i - 1, i % 2 == 0 ? kLarge : kLarge - kStep});
  }
  const struct {
    const char* name;
    std::vector<IndexFactoryBuilder::BlockHandle> handles;
  } cases[] = {
      {"wide_residuals", wide_residuals},
      {"last_block_ends_past_max", evenly_spaced(uint64_t{1} << 50, 5, 1)},
      {"sentinel_past_max", evenly_spaced(uint64_t{1} << 50, 100, kMax - 50)},
  };
  for (const auto& c : cases) {
    SCOPED_TRACE(c.name);
    for (bool parallel : {false, true}) {
      SCOPED_TRACE(parallel);
      AssertSerializedSizeBound(keys, c.handles, parallel, 0);
    }
  }
}

TEST_F(TrieIndexFactoryTest, BlockHandlesPast32Bits) {
  constexpr uint64_t kLimit = UINT32_MAX;
  for (bool parallel : {false, true}) {
    for (bool large_offset : {false, true}) {
      for (size_t large_entry : {0U, 1U, 2U}) {
        SCOPED_TRACE(testing::Message()
                     << parallel << " " << large_offset << " " << large_entry);
        std::unique_ptr<IndexFactoryBuilder> builder;
        ASSERT_OK(factory_->NewBuilder(IndexFactoryOptions(), builder));
        if (parallel && !builder->SupportsParallelAddEntry()) {
          continue;
        }
        // Both blocks share one separator, so they form a run.
        const Slice key("same");
        std::vector<IndexFactoryBuilder::BlockHandle> handles;
        for (size_t i = 0; i < 2; ++i) {
          IndexFactoryBuilder::BlockHandle handle{kLimit, kLimit};
          // Offsets stay in block order.
          if (large_offset && i >= large_entry) {
            ++handle.offset;
          } else if (!large_offset && i == large_entry) {
            ++handle.size;
          }
          handles.push_back(handle);
          std::string scratch;
          const Slice* next = i == 0 ? &key : nullptr;
          const auto context = EntryCtx(200 - i * 100, 100);
          if (parallel) {
            auto prepared = builder->CreatePreparedAddEntry();
            builder->PrepareAddEntry(key, next, context, prepared.get());
            builder->FinishAddEntry(handle, prepared.get(), &scratch, false);
          } else {
            builder->AddIndexEntry(key, next, handle, &scratch, context);
          }
        }
        Slice contents;
        ASSERT_OK(builder->Finish(&contents));
        std::unique_ptr<IndexFactoryReader> reader;
        ASSERT_OK(factory_->NewReader(IndexFactoryOptions(), contents, reader));
        auto iter = reader->NewIterator(ReadOptions());
        IterateResult result;
        ASSERT_OK(iter->SeekToFirstAndGetResult(&result));
        EXPECT_EQ(iter->value().offset, handles[0].offset);
        EXPECT_EQ(iter->value().size, handles[0].size);
        ASSERT_OK(iter->NextAndGetResult(&result));
        EXPECT_EQ(iter->value().offset, handles[1].offset);
        EXPECT_EQ(iter->value().size, handles[1].size);
      }
    }
  }
}

TEST_F(TrieIndexFactoryTest, BasicBuildAndRead) {
  // Build a trie index using the factory interface.
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));
  ASSERT_NE(builder, nullptr);

  // Simulate SST construction with 5 data blocks.
  std::vector<std::string> last_keys = {"apple", "banana", "cherry", "date",
                                        "elderberry"};
  std::vector<std::string> first_keys = {"banana", "cherry", "date",
                                         "elderberry", ""};

  for (size_t i = 0; i < last_keys.size(); i++) {
    IndexFactoryBuilder::BlockHandle handle{i * 1000, 500};

    std::string scratch;
    Slice next_slice(first_keys[i]);
    const Slice* next = (i < last_keys.size() - 1) ? &next_slice : nullptr;
    builder->AddIndexEntry(Slice(last_keys[i]), next, handle, &scratch,
                           EntryCtx(100, 100));
  }

  // Finish building.
  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));
  ASSERT_GT(index_contents.size(), 0u);

  // Read the index.
  std::unique_ptr<IndexFactoryReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));
  ASSERT_NE(reader, nullptr);
  ASSERT_GT(reader->ApproximateMemoryUsage(), 0u);

  // Create an iterator and seek.
  ReadOptions ro;
  auto iter = reader->NewIterator(ro);
  ASSERT_NE(iter, nullptr);

  // Seek to "banana" -- should find the separator for the second block.
  IterateResult result;
  ASSERT_OK(iter->SeekAndGetResult(Slice("banana"), &result,
                                   SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
}

TEST_F(TrieIndexFactoryTest, EmptySeparator) {
  // The first block holds only the empty key.
  auto ctx = BuildTrieAndGetIterator({{"", "b", 0, 95, 10, 9},
                                      {"b", "c", 100, 95, 9, 8},
                                      {"c", "", 200, 95, 8, 0}});
  ASSERT_NO_FATAL_FAILURE(
      AssertFullForwardScan(ctx.iter.get(), Slice(""), {0, 100, 200}));
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), "", kMaxSequenceNumber, 0));
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), "a", kMaxSequenceNumber, 100));
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), "b", kMaxSequenceNumber, 100));
  IterateResult result;
  ASSERT_OK(ctx.iter->SeekToLastAndGetResult(&result));
  ASSERT_EQ(ctx.iter->value().offset, 200u);
  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(result.key, "");
  ASSERT_EQ(ctx.iter->value().offset, 0u);
}

TEST_F(TrieIndexFactoryTest, FactoryName) {
  ASSERT_STREQ(factory_->Name(), "trie_index");
}

// An OPTIONS file names the factory, so it must be reachable by that name and
// survive a round trip through the table options string.
TEST_F(TrieIndexFactoryTest, CreateFromName) {
  ConfigOptions config_options;
  std::shared_ptr<IndexFactory> factory;
  ASSERT_OK(
      IndexFactory::CreateFromString(config_options, "trie_index", &factory));
  ASSERT_NE(factory, nullptr);
  ASSERT_STREQ(factory->Name(), TrieIndexFactory::kClassName());

  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory = factory;
  std::unique_ptr<TableFactory> table_factory(
      NewBlockBasedTableFactory(table_options));
  std::string serialized;
  ASSERT_OK(table_factory->GetOptionString(config_options, &serialized));
  BlockBasedTableOptions parsed;
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, BlockBasedTableOptions(), serialized, &parsed));
  ASSERT_NE(parsed.user_defined_index_factory, nullptr);
  ASSERT_STREQ(parsed.user_defined_index_factory->Name(),
               TrieIndexFactory::kClassName());
}

TEST_F(TrieIndexFactoryTest, EmptyIndex) {
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));

  // Finish without adding any entries.
  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));
  // Empty index should produce empty or minimal contents.
}

TEST_F(TrieIndexFactoryTest, DoubleFinish) {
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));

  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));

  // Second Finish should fail.
  Slice index_contents2;
  Status s = builder->Finish(&index_contents2);
  ASSERT_EQ(s.code(), Status::Code::kInvalidArgument) << s.ToString();
}

TEST_F(TrieIndexFactoryTest, IteratorBoundsChecking) {
  // Test the bounds checking in the UDI iterator.
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<IndexFactoryBuilder> udi_builder;
  ASSERT_OK(factory_->NewBuilder(option, udi_builder));

  // Build index with 3 blocks.
  std::vector<std::string> separators;
  for (int i = 0; i < 3; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "key_%02d", i);
    std::string sep = buf;
    separators.push_back(sep);

    IndexFactoryBuilder::BlockHandle handle{static_cast<uint64_t>(i) * 1000,
                                            500};
    std::string scratch;
    if (i < 2) {
      char next_buf[16];
      snprintf(next_buf, sizeof(next_buf), "key_%02d", i + 1);
      Slice next(next_buf);
      udi_builder->AddIndexEntry(Slice(sep), &next, handle, &scratch,
                                 EntryCtx(100, 100));
    } else {
      udi_builder->AddIndexEntry(Slice(sep), nullptr, handle, &scratch,
                                 EntryCtx(100, 100));
    }
  }

  Slice index_contents;
  ASSERT_OK(udi_builder->Finish(&index_contents));

  std::unique_ptr<IndexFactoryReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));

  ReadOptions ro;
  auto iter = reader->NewIterator(ro);

  // Prepare with an upper bound.
  ScanOptions scan_opts(Slice("key_00"), Slice("key_01z"));
  iter->Prepare(&scan_opts, 1);

  // Seek to first key -- should be in bound.
  IterateResult result;
  ASSERT_OK(iter->SeekAndGetResult(Slice("key_00"), &result,
                                   SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);

  // Next -- should be in bound (key_01 < key_01z).
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);

  // Next -- the previous separator is for block 1 (< "key_01z"), so
  // CheckBounds conservatively returns kInbound. The data-level iterator
  // handles per-key filtering. This is correct per the UDI API contract:
  // since we don't store first-in-block keys, we cannot determine that
  // block 2 is fully out of bounds.
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
}

TEST_F(TrieIndexFactoryTest, IteratorNoBounds) {
  // Without Prepare(), bounds checking should always return kInbound.
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<IndexFactoryBuilder> udi_builder;
  ASSERT_OK(factory_->NewBuilder(option, udi_builder));

  IndexFactoryBuilder::BlockHandle handle{0, 500};
  std::string scratch;
  udi_builder->AddIndexEntry(Slice("key"), nullptr, handle, &scratch,
                             EntryCtx(100, 100));

  Slice index_contents;
  ASSERT_OK(udi_builder->Finish(&index_contents));

  std::unique_ptr<IndexFactoryReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));

  ReadOptions ro;
  auto iter = reader->NewIterator(ro);

  // No Prepare() call -- bounds should be kInbound.
  IterateResult result;
  ASSERT_OK(iter->SeekAndGetResult(Slice("key"), &result,
                                   SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
}

TEST_F(TrieIndexFactoryTest, UpperBoundDoesNotDropValidBlocks) {
  // Validates that CheckBounds uses the previous separator (not the current
  // one) as the reference key. The trie stores separator keys (upper
  // bounds on block contents), NOT first-in-block keys. CheckBounds must
  // use the previous separator (or seek target) as the reference key, not
  // the current separator, to avoid prematurely rejecting blocks that
  // contain keys within the limit.
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<IndexFactoryBuilder> udi_builder;
  ASSERT_OK(factory_->NewBuilder(option, udi_builder));

  // Block 0: last="az", next_first="c" -> separator ~= "b"
  {
    IndexFactoryBuilder::BlockHandle handle{0, 1000};
    std::string scratch;
    Slice next("c");
    udi_builder->AddIndexEntry(Slice("az"), &next, handle, &scratch,
                               EntryCtx(100, 100));
  }
  // Block 1: last="cz", next_first="e" -> separator ~= "d"
  {
    IndexFactoryBuilder::BlockHandle handle{1000, 1000};
    std::string scratch;
    Slice next("e");
    udi_builder->AddIndexEntry(Slice("cz"), &next, handle, &scratch,
                               EntryCtx(100, 100));
  }
  // Block 2: last="ez", no next -> separator ~= "f"
  {
    IndexFactoryBuilder::BlockHandle handle{2000, 1000};
    std::string scratch;
    udi_builder->AddIndexEntry(Slice("ez"), nullptr, handle, &scratch,
                               EntryCtx(100, 100));
  }

  Slice index_contents;
  ASSERT_OK(udi_builder->Finish(&index_contents));

  std::unique_ptr<IndexFactoryReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));

  ReadOptions ro;
  auto iter = reader->NewIterator(ro);

  // Upper bound "d": blocks 0 and 1 may contain keys < "d".
  ScanOptions scan_opts(Slice("a"), Slice("d"));
  iter->Prepare(&scan_opts, 1);

  IterateResult result;
  // Seek("a") -> lands on block 0 (separator "b"). target "a" < "d" ->
  // kInbound.
  ASSERT_OK(
      iter->SeekAndGetResult(Slice("a"), &result, SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(iter->value().offset, 0u);

  // Next() -> lands on block 1 (separator "d"). Previous separator "b" < "d"
  // -> kInbound (conservative). Block 1 contains keys like "c".."cz", which
  // are < "d", so returning kInbound is correct.
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(iter->value().offset, 1000u);

  // Next() -> lands on block 2 (separator "f"). Previous separator "d" >= "d"
  // -> kOutOfBound. All keys in block 2 are >= "d", so this is correct.
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kOutOfBound);
}

TEST_F(TrieIndexFactoryTest, PrevAfterPrepareDoesNotUseForwardUpperBound) {
  auto ctx = BuildTrieAndGetIterator({
      {"az", "b", 0, 500, 0, 0},
      {"bz", "d", 1000, 500, 0, 0},
      {"dz", "f", 2000, 500, 0, 0},
  });

  ScanOptions scans[] = {
      ScanOptions(Slice("a"), Slice("c")),
  };
  ctx.iter->Prepare(scans, 1);

  IterateResult result;
  ASSERT_OK(ctx.iter->SeekAndGetResult(Slice("a"), &result,
                                       SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 0u);

  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 1000u);

  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 0u);
}

TEST_F(TrieIndexFactoryTest, MultiScanBoundsAdvanceCorrectly) {
  // Validates that current_scan_idx_ advances correctly when
  // the seek target is past the current scan's limit. Otherwise all
  // bounds checks evaluate against scan 0's limit.
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<IndexFactoryBuilder> udi_builder;
  ASSERT_OK(factory_->NewBuilder(option, udi_builder));

  // 5 blocks with well-separated keys.
  struct BlockDef {
    const char* last_key;
    const char* next_first;
    uint64_t offset;
  };
  BlockDef blocks[] = {
      {"az", "c", 0},    {"cz", "e", 1000},     {"ez", "g", 2000},
      {"gz", "i", 3000}, {"iz", nullptr, 4000},
  };
  for (const auto& b : blocks) {
    IndexFactoryBuilder::BlockHandle handle{b.offset, 500};
    std::string scratch;
    if (b.next_first) {
      Slice next(b.next_first);
      udi_builder->AddIndexEntry(Slice(b.last_key), &next, handle, &scratch,
                                 {0, 0});
    } else {
      udi_builder->AddIndexEntry(Slice(b.last_key), nullptr, handle, &scratch,
                                 {0, 0});
    }
  }

  Slice index_contents;
  ASSERT_OK(udi_builder->Finish(&index_contents));

  std::unique_ptr<IndexFactoryReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));

  ReadOptions ro;
  auto iter = reader->NewIterator(ro);

  // Two non-overlapping scan ranges.
  ScanOptions scans[] = {
      ScanOptions(Slice("a"), Slice("c")),  // scan 0: blocks in [a, c)
      ScanOptions(Slice("e"), Slice("g")),  // scan 1: blocks in [e, g)
  };
  iter->Prepare(scans, 2);

  IterateResult result;

  // --- Scan 0 ---
  // Seek("a") -> block 0 (separator "b"). target "a" < limit "c" -> kInbound.
  ASSERT_OK(
      iter->SeekAndGetResult(Slice("a"), &result, SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(iter->value().offset, 0u);

  // Next() -> block 1 (separator "d"). prev "b" < "c" -> kInbound
  // (conservative).
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);

  // Next() -> block 2 (separator "f"). prev "d" >= "c" -> kOutOfBound.
  // Scan 0 is done.
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kOutOfBound);

  // --- Scan 1 ---
  // Seek("e") should advance current_scan_idx_ to 1 (target "e" >= scan 0
  // limit "c"), then check against scan 1's limit "g".
  // Lands on block 2 (separator "f"). target "e" < "g" -> kInbound.
  ASSERT_OK(
      iter->SeekAndGetResult(Slice("e"), &result, SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(iter->value().offset, 2000u);

  // Next() -> block 3 (separator "h"). prev "f" < "g" -> kInbound
  // (conservative).
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);

  // Next() -> block 4 (separator "j"). prev "h" >= "g" -> kOutOfBound.
  // Scan 1 is done.
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kOutOfBound);
}

TEST_F(TrieIndexFactoryTest, RejectsNonBytewiseComparator) {
  // The trie index requires bytewise ordering because it traverses keys
  // byte-by-byte. Non-bytewise comparators should be rejected.
  IndexFactoryOptions option;

  // ReverseBytewiseComparator should be rejected.
  option.comparator = ReverseBytewiseComparator();
  std::unique_ptr<IndexFactoryBuilder> builder;
  Status s = factory_->NewBuilder(option, builder);
  ASSERT_EQ(s.code(), Status::Code::kNotSupported) << s.ToString();
  ASSERT_EQ(builder, nullptr);

  // NewReader should also reject non-bytewise comparators.
  Slice dummy_data;
  std::unique_ptr<IndexFactoryReader> reader;
  s = factory_->NewReader(option, dummy_data, reader);
  ASSERT_EQ(s.code(), Status::Code::kNotSupported) << s.ToString();
  ASSERT_EQ(reader, nullptr);

  // BytewiseComparator should be accepted.
  option.comparator = BytewiseComparator();
  ASSERT_OK(factory_->NewBuilder(option, builder));
  ASSERT_NE(builder, nullptr);
}

TEST_F(TrieIndexFactoryTest, ApproximateMemoryUsageReportsOnlyAuxData) {
  // The reader reports its auxiliary heap allocations. The serialized block
  // is accounted separately by the table reader or block cache.
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<IndexFactoryBuilder> udi_builder;
  ASSERT_OK(factory_->NewBuilder(option, udi_builder));

  // Build a non-trivial index from 100 blocks.
  for (int i = 0; i < 100; i++) {
    char last_buf[32];
    char next_buf[32];
    snprintf(last_buf, sizeof(last_buf), "key_%04d", i);
    snprintf(next_buf, sizeof(next_buf), "key_%04d", i + 1);

    IndexFactoryBuilder::BlockHandle handle{static_cast<uint64_t>(i) * 1000,
                                            500};
    std::string scratch;
    if (i < 99) {
      Slice next(next_buf);
      udi_builder->AddIndexEntry(Slice(last_buf), &next, handle, &scratch,
                                 {0, 0});
    } else {
      udi_builder->AddIndexEntry(Slice(last_buf), nullptr, handle, &scratch,
                                 {0, 0});
    }
  }

  Slice index_contents;
  ASSERT_OK(udi_builder->Finish(&index_contents));
  size_t serialized_size = index_contents.size();

  std::unique_ptr<IndexFactoryReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));

  size_t mem_usage = reader->ApproximateMemoryUsage();
  ASSERT_GT(mem_usage, 0U);
  ASSERT_LT(mem_usage, serialized_size);
}

TEST_F(TrieIndexFactoryTest, EmptyTrieIterator) {
  // Seek on an iterator built from an empty index.
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));

  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));

  std::unique_ptr<IndexFactoryReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));

  ReadOptions ro;
  auto iter = reader->NewIterator(ro);
  ASSERT_NE(iter, nullptr);

  IterateResult result;
  ASSERT_OK(iter->SeekAndGetResult(Slice("anything"), &result,
                                   SeekCtx(kMaxSequenceNumber)));
  // kUnknown: no separator is >= target, so the target is past all blocks
  // in this SST. We return kUnknown (not kOutOfBound) because exhausting
  // this SST says nothing about the upper bound -- the next SST on the level
  // may still have in-bound keys.
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);
}

TEST_F(TrieIndexFactoryTest, PrepareWithZeroScans) {
  // Prepare with 0 scan ranges, then seek -- should behave as no bounds.
  auto ctx = BuildTrieAndGetIterator({
      {"az", "c", 0, 500, 0, 0},
      {"cz", "e", 1000, 500, 0, 0},
      {"ez", "", 2000, 500, 0, 0},
  });

  // Prepare with 0 scans.
  ctx.iter->Prepare(nullptr, 0);

  IterateResult result;
  ASSERT_OK(ctx.iter->SeekAndGetResult(Slice("a"), &result,
                                       SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
}

TEST_F(TrieIndexFactoryTest, RePrepareResetsScanState) {
  // Call Prepare twice -- second Prepare should reset scan state.
  auto ctx = BuildTrieAndGetIterator({
      {"az", "c", 0, 500, 0, 0},
      {"cz", "e", 1000, 500, 0, 0},
      {"ez", "", 2000, 500, 0, 0},
  });

  // First Prepare with limit "b".
  ScanOptions scan1(Slice("a"), Slice("b"));
  ctx.iter->Prepare(&scan1, 1);

  IterateResult result;
  ASSERT_OK(ctx.iter->SeekAndGetResult(Slice("a"), &result,
                                       SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);

  // Re-prepare with a broader limit "f".
  ScanOptions scan2(Slice("a"), Slice("f"));
  ctx.iter->Prepare(&scan2, 1);

  // Should be able to seek to "d" and get inbound with the new limit.
  ASSERT_OK(ctx.iter->SeekAndGetResult(Slice("d"), &result,
                                       SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
}

TEST_F(TrieIndexFactoryTest, ScanWithNoLimit) {
  // Prepare with a scan range that has no upper limit.
  auto ctx = BuildTrieAndGetIterator({
      {"az", "c", 0, 500, 0, 0},
      {"cz", "e", 1000, 500, 0, 0},
      {"ez", "", 2000, 500, 0, 0},
  });

  // ScanOptions with only a start, no limit.
  ScanOptions scan(Slice("a"));
  ctx.iter->Prepare(&scan, 1);

  // All seeks should be inbound with no limit.
  IterateResult result;
  ASSERT_OK(ctx.iter->SeekAndGetResult(Slice("a"), &result,
                                       SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);

  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);

  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);

  // No more blocks -- kUnknown because exhausting this SST doesn't imply
  // the upper bound was reached.
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);
}

TEST_F(TrieIndexFactoryTest, NewReaderWithCorruptedData) {
  // Attempt to create a reader from corrupted data.
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();

  std::string bad_data(10, '\xff');
  Slice bad_slice(bad_data);
  std::unique_ptr<IndexFactoryReader> reader;
  Status s = factory_->NewReader(option, bad_slice, reader);
  ASSERT_EQ(s.code(), Status::Code::kCorruption) << s.ToString();
}

// An index block that passes its checksum can still be malformed, e.g. when
// written by a buggy writer. The reader must reject it or read it without
// undefined behavior; run under ASAN/UBSAN to catch out-of-bounds reads.
TEST_F(TrieIndexFactoryTest, MalformedIndexIsRejectedOrReadable) {
  const uint32_t seed = TimeSeed();
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);

  // Long shared prefixes give compressed trie paths, and a user key spanning
  // three blocks gives a run.
  std::vector<std::string> keys = {""};
  for (int i = 0; i < 30; i++) {
    char suffix[8];
    snprintf(suffix, sizeof(suffix), "%04d", i * 3);
    keys.push_back("tenant/0001/objects/" + std::string(suffix) +
                   std::string(rnd.Uniform(12), 'x'));
  }
  std::vector<TestBlock> blocks;
  for (size_t i = 0; i < keys.size(); i++) {
    const uint64_t offset = blocks.size() * 100;
    if (i == keys.size() / 2) {
      blocks.push_back({keys[i], keys[i], offset, 95, 30, 29});
      blocks.push_back({keys[i], keys[i], offset + 100, 95, 20, 19});
      blocks.push_back({keys[i], keys[i + 1], offset + 200, 95, 10, 9});
    } else {
      blocks.push_back(
          {keys[i], i + 1 < keys.size() ? keys[i + 1] : "", offset, 95, 10, 9});
    }
  }
  TrieTestContext ctx = BuildTrieAndGetIterator(blocks);
  const std::string data = ctx.index_contents.ToString();

  auto exercise = [&](const std::string& mutated) {
    Slice contents(mutated);
    std::unique_ptr<IndexFactoryReader> reader;
    if (!OpenedOrRejected(
            factory_->NewReader(IndexFactoryOptions(), contents, reader))) {
      return;
    }
    std::unique_ptr<IndexFactoryIterator> iter =
        reader->NewIterator(ReadOptions());
    const size_t limit = 2 * blocks.size() + 10;
    IterateResult result;
    auto inbound = [&](const Status& s) {
      return OkOrCorruption(s) &&
             result.bound_check_result == IterBoundCheck::kInbound;
    };
    bool valid = inbound(iter->SeekToFirstAndGetResult(&result));
    for (size_t i = 0; valid && i < limit; i++) {
      iter->value();
      valid = inbound(iter->NextAndGetResult(&result));
    }
    valid = inbound(iter->SeekToLastAndGetResult(&result));
    for (size_t i = 0; valid && i < limit; i++) {
      valid = inbound(iter->PrevAndGetResult(&result));
    }
    for (const std::string& key : keys) {
      for (SequenceNumber seq : {kMaxSequenceNumber, SequenceNumber{15}}) {
        if (inbound(iter->SeekAndGetResult(key, &result, SeekCtx(seq)))) {
          iter->value();
          inbound(iter->NextAndGetResult(&result));
        }
      }
    }
  };

  // Counts and offsets are 32- or 64-bit fields: plant values that overflow
  // size arithmetic, and pairs of 32-bit counts whose sum wraps to zero.
  for (size_t pos = 0; pos + 4 <= data.size(); pos += 4) {
    for (uint32_t v : {0x7fffffffu, 0x80000001u, 0xffffffffu}) {
      std::string mutated = data;
      EncodeFixed32(&mutated[pos], v);
      exercise(mutated);
    }
    if (pos + 8 <= data.size()) {
      std::string mutated = data;
      EncodeFixed32(&mutated[pos], 0x80000001u);
      EncodeFixed32(&mutated[pos + 4], 0x80000001u);
      exercise(mutated);
      for (uint64_t v : {uint64_t{1} << 32, uint64_t{0x8000000000000002},
                         uint64_t{0xffffffffffffffff}}) {
        mutated = data;
        EncodeFixed64(&mutated[pos], v);
        exercise(mutated);
      }
    }
  }
  for (size_t len = 0; len < data.size(); len++) {
    exercise(data.substr(0, len));
  }
  for (int i = 0; i < 2000; i++) {
    exercise(FlipRandomBytes(data, &rnd));
  }
}

TEST_F(TrieIndexFactoryTest, OnKeyAddedNoOp) {
  // Verify that OnKeyAdded() is a no-op for the trie builder regardless of
  // the ValueType. The trie only uses separator keys from AddIndexEntry().
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));

  // Call OnKeyAdded with all ValueType variants -- all should be no-ops.
  builder->OnKeyAdded(Slice("key1"), IndexFactoryBuilder::ValueType::kValue,
                      Slice("value1"));
  builder->OnKeyAdded(Slice("key2"), IndexFactoryBuilder::ValueType::kDelete,
                      Slice(""));
  builder->OnKeyAdded(Slice("key3"), IndexFactoryBuilder::ValueType::kMerge,
                      Slice("merge_operand"));
  builder->OnKeyAdded(Slice("key4"), IndexFactoryBuilder::ValueType::kOther,
                      Slice("blob_ref"));
  builder->OnKeyAdded(Slice(""), IndexFactoryBuilder::ValueType::kValue,
                      Slice(""));

  // Building should still succeed (OnKeyAdded should not affect state).
  IndexFactoryBuilder::BlockHandle handle{0, 500};
  std::string scratch;
  builder->AddIndexEntry(Slice("key5"), nullptr, handle, &scratch,
                         EntryCtx(100, 100));

  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));
  ASSERT_GT(index_contents.size(), 0u);
}

TEST_F(TrieIndexFactoryTest, NullComparator) {
  // NewBuilder and NewReader with nullptr comparator should default to
  // BytewiseComparator. This tests that the null-comparator guard in both
  // NewBuilder and NewReader prevents null-pointer dereferences.
  IndexFactoryOptions option;
  option.comparator = nullptr;

  // Build a non-trivial index with null comparator. The builder internally
  // defaults to BytewiseComparator.
  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));
  ASSERT_NE(builder, nullptr);

  // Add some entries -- AddIndexEntry uses the defaulted comparator internally.
  std::string scratch;
  {
    IndexFactoryBuilder::BlockHandle h{0, 100};
    Slice next("b");
    builder->AddIndexEntry(Slice("a"), &next, h, &scratch, EntryCtx(100, 100));
  }
  {
    IndexFactoryBuilder::BlockHandle h{100, 100};
    builder->AddIndexEntry(Slice("b"), nullptr, h, &scratch,
                           EntryCtx(100, 100));
  }

  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));

  // NewReader with nullptr comparator must default to BytewiseComparator.
  // Storing a null comparator would cause a crash on Seek when CheckBounds
  // dereferences it.
  std::unique_ptr<IndexFactoryReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));
  ASSERT_NE(reader, nullptr);

  // Verify the reader works -- Seek uses the comparator for bounds checking.
  ReadOptions ro;
  auto iter = reader->NewIterator(ro);
  IterateResult result;
  ASSERT_OK(
      iter->SeekAndGetResult(Slice("a"), &result, SeekCtx(kMaxSequenceNumber)));
  EXPECT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  EXPECT_EQ(iter->value().offset, 0u);

  ASSERT_OK(
      iter->SeekAndGetResult(Slice("b"), &result, SeekCtx(kMaxSequenceNumber)));
  EXPECT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  EXPECT_EQ(iter->value().offset, 100u);
}

TEST_F(TrieIndexFactoryTest, SeekSucceedsButTargetPastLimit) {
  // Set up bounds with a limit, then seek to a target that is >= the limit.
  // CheckBounds should return kOutOfBound even though the trie Seek succeeds.
  auto ctx = BuildTrieAndGetIterator({
      {"az", "c", 0, 500, 0, 0},
      {"cz", "e", 1000, 500, 0, 0},
      {"ez", "", 2000, 500, 0, 0},
  });

  // Prepare with limit "c" (exclusive upper bound).
  ScanOptions scan(Slice("a"), Slice("c"));
  ctx.iter->Prepare(&scan, 1);

  // Seek to "c" -- target == limit, so CheckBounds returns kOutOfBound.
  IterateResult result;
  ASSERT_OK(ctx.iter->SeekAndGetResult(Slice("c"), &result,
                                       SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kOutOfBound);

  // Seek to "d" -- target > limit, also kOutOfBound.
  ASSERT_OK(ctx.iter->SeekAndGetResult(Slice("d"), &result,
                                       SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kOutOfBound);
}

// ============================================================================
// Same-user-key boundary tests
//
// These tests verify the same-user-key boundary handling: when the same user
// key spans a data block boundary with different sequence numbers, the block
// before the boundary keeps the user key as its separator and gets its last
// key's tag, so Seek can choose between the blocks by sequence number.
// ============================================================================

TEST_F(TrieIndexFactoryTest, SameUserKeyBoundaryGetsTag) {
  // When the same user key spans two adjacent data blocks (e.g.,
  // "foo"|seq=100 ends Block 0, "foo"|seq=50 starts Block 1), the
  // builder must detect this and give Block 0 its last key's tag.
  // Without this, FindShortestSeparator("foo", "foo") = "foo" and the
  // trie cannot distinguish the two blocks, causing incorrect Seek.
  auto ctx = BuildTrieAndGetIterator({
      // Block 0: last_key="foo" seq=100, next_first="foo" seq=50.
      // Same user key at boundary -- the entry gets a real tag.
      {"foo", "foo", 0, 1000, 100, 50},
      // Block 1: last_key="foo" seq=50, next_first="goo" seq=1.
      {"foo", "goo", 1000, 1000, 50, 1},
      // Block 2: last_key="goo" seq=1, no next.
      {"goo", "", 2000, 1000, 1, 0},
  });

  IterateResult result;

  // Seek for "foo" with seq=100 (highest seqno -- should find Block 0).
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("foo"), 100, 0));

  // Seek for "foo" with seq=75 (between 100 and 50). In RocksDB's internal
  // key ordering, higher seqno = "smaller" key. So:
  //   "foo"|seq=100 < "foo"|seq=75  (the separator for Block 0 is LESS than
  //                                   the target)
  // Therefore the trie correctly skips Block 0 and returns Block 1.
  // This matches the internal binary search index behavior exactly:
  // the index directs us to the NEXT block, and the data block iterator
  // within that block will find the first key >= target.
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("foo"), 75, 1000));

  // Seek for "foo" with seq=50 (exact seqno of Block 1's first key --
  // should find Block 1).
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("foo"), 50, 1000));

  // Seek for "foo" with seq=1 (lower than any foo seqno -- should find
  // Block 1, because in descending seqno order seq=1 comes after seq=50).
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("foo"), 1, 1000));

  // Seek for "goo" with kMaxSequenceNumber -- should find Block 2.
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("goo"), kMaxSequenceNumber, 2000));

  // Next from Block 0 should advance to Block 1, then Block 2.
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("foo"), 100, 0));
  ASSERT_NO_FATAL_FAILURE(
      AssertFullForwardScan(ctx.iter.get(), Slice("foo"), {0, 1000, 2000}));
}

TEST_F(TrieIndexFactoryTest, DistinctUserKeysSeekWithMaxSeqno) {
  // No two blocks share a user key. Verify that seeking with
  // kMaxSequenceNumber correctly finds each block.
  auto ctx = BuildTrieAndGetIterator({
      {"apple", "cherry", 0, 1000, 100, 50},
      {"cherry", "elderberry", 1000, 1000, 50, 1},
      {"elderberry", "", 2000, 1000, 1, 0},
  });

  // Seeking with kMaxSequenceNumber should work identically to no-seqno case.
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("apple"), kMaxSequenceNumber, 0));
  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(ctx.iter.get(), Slice("cherry"),
                                           kMaxSequenceNumber, 1000));
  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(ctx.iter.get(), Slice("elderberry"),
                                           kMaxSequenceNumber, 2000));
}

TEST_F(TrieIndexFactoryTest, MultipleSameUserKeyBoundaries) {
  // Multiple same-user-key boundaries in one SST: the same user key appears
  // across 3 consecutive blocks with decreasing seqnos.
  // Block 0: "key"|seq=300
  // Block 1: "key"|seq=200
  // Block 2: "key"|seq=100
  // Block 3: "other"|seq=50
  auto ctx = BuildTrieAndGetIterator({
      {"key", "key", 0, 1000, 300, 200},
      {"key", "key", 1000, 1000, 200, 100},
      {"key", "other", 2000, 1000, 100, 50},
      {"other", "", 3000, 1000, 50, 0},
  });

  // Seek "key"|seq=300 -> Block 0
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("key"), 300, 0));

  // Seek "key"|seq=250 -> Block 1. Blocks 0 and 1 form the "key" run with
  // tags 300 and 200 (Block 2 gets separator "l"), and Block 1 is the first
  // block of the run whose tag is at most 250.
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("key"), 250, 1000));

  // Seek "key"|seq=200 -> Block 1
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("key"), 200, 1000));

  // Seek "key"|seq=150 -> Block 2. Both blocks of the run have larger tags,
  // so Seek moves to the next separator, "l" (Block 2).
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("key"), 150, 2000));

  // Seek "key"|seq=100 -> Block 2
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("key"), 100, 2000));

  // Seek "key"|seq=1 -> Block 2 (below all key seqnos)
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("key"), 1, 2000));

  // Seek "other"|kMaxSequenceNumber -> Block 3
  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(ctx.iter.get(), Slice("other"),
                                           kMaxSequenceNumber, 3000));

  // Full forward scan: Block 0 -> 1 -> 2 -> 3
  ASSERT_NO_FATAL_FAILURE(AssertFullForwardScan(ctx.iter.get(), Slice("key"),
                                                {0, 1000, 2000, 3000}));
}

TEST_F(TrieIndexFactoryTest, SameUserKeyWithZeroSeqnos) {
  // During bottommost compaction, RocksDB zeroes sequence numbers for keys
  // that no longer need version discrimination. When the same user key spans
  // multiple data blocks and all versions have seqno=0, the blocks of its run
  // have no tags. This is valid: Seek lands on the first block of the run
  // for any target seqno, and Next() walks the run's blocks in order.
  auto ctx = BuildTrieAndGetIterator({
      {"key", "key", 0, 1000, 0, 0},
      {"key", "key", 1000, 1000, 0, 0},
      {"key", "zzz", 2000, 1000, 0, 0},
      {"zzz", "", 3000, 1000, 0, 0},
  });

  // Seek "key"|kMaxSequenceNumber -> Block 0, the first block of the run.
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("key"), kMaxSequenceNumber, 0));

  // Seek "key"|seq=0 -> Block 0, which has no tag.
  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(ctx.iter.get(), Slice("key"), 0, 0));

  // Full forward scan: Block 0 -> 1 -> 2 -> 3
  ASSERT_NO_FATAL_FAILURE(AssertFullForwardScan(ctx.iter.get(), Slice("key"),
                                                {0, 1000, 2000, 3000}));

  // SeekToFirst -> Block 0, then full scan.
  IterateResult result;
  ASSERT_OK(ctx.iter->SeekToFirstAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 0u);
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(ctx.iter->value().offset, 1000u);
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(ctx.iter->value().offset, 2000u);
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(ctx.iter->value().offset, 3000u);
}

TEST_F(TrieIndexFactoryTest, SameUserKeyLastBlockZeroSeqno) {
  // Same user key spans blocks including the last block in the SST, with
  // all seqnos zeroed (bottommost compaction). The last block's AddIndexEntry
  // call has first_key_in_next_block=nullptr and first_key_tag=0.
  auto ctx = BuildTrieAndGetIterator({
      {"key", "key", 0, 1000, 0, 0},
      {"key", "", 1000, 1000, 0, 0},  // last block
  });

  // Seek -> Block 0, the first block of the run.
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("key"), kMaxSequenceNumber, 0));

  // Next -> Block 1, the second block of the run.
  IterateResult result;
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 1000u);

  // Next -> exhausted.
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_NE(result.bound_check_result, IterBoundCheck::kInbound);
}

TEST_F(TrieIndexFactoryTest, TagsRoundTripSerialization) {
  // Verify that block tags survive serialization/deserialization. Build a
  // trie with a same-user-key boundary (a run of two tagged blocks),
  // serialize it, deserialize it, and verify that seeks pick by seqno.
  auto ctx = BuildTrieAndGetIterator({
      {"x", "x", 0, 500, 10, 5},
      {"x", "", 500, 500, 5, 0},
  });

  // Seek "x"|seq=10 -> Block 0
  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(ctx.iter.get(), Slice("x"), 10, 0));

  // Seek "x"|seq=5 -> Block 1
  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(ctx.iter.get(), Slice("x"), 5, 500));
}

TEST_F(TrieIndexFactoryTest, TagsWithBoundsChecking) {
  // Verify that bounds checking works correctly on blocks with tags. The
  // bounds comparison uses user keys only, never tags.
  auto ctx = BuildTrieAndGetIterator({
      {"key", "key", 0, 1000, 100, 50},
      {"key", "zzz", 1000, 1000, 50, 1},
      {"zzz", "", 2000, 1000, 1, 0},
  });

  // Set upper bound to "zzz".
  ScanOptions scan(Slice("key"), Slice("zzz"));
  ctx.iter->Prepare(&scan, 1);

  IterateResult result;

  // Seek "key"|seq=100 -> Block 0, within bounds.
  ASSERT_OK(ctx.iter->SeekAndGetResult(Slice("key"), &result, SeekCtx(100)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 0u);

  // Next -> Block 1, previous separator "key" < "zzz" -> kInbound.
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 1000u);
}

// ============================================================================
// Run tests
//
// These tests exercise runs: consecutive blocks that share a separator
// because one user key spans them. Key insight: FindShortestSeparator
// shortens separator keys when the two user keys differ. For example,
// FindShortestSeparator("aaa", "mmm") = "b". Only same-user-key boundaries
// (or keys that cannot be shortened) produce identical separators.
//
// Coverage:
//   - Long runs (10+ blocks with the same user key)
//   - Mixed runs (multiple different user keys each spanning several blocks)
//   - Adjacent runs (two different user keys, each with their own run)
//   - Seek with kMaxSequenceNumber on same-user-key blocks
//   - Seek with seq=0 on same-user-key blocks
//   - result.key is the separator (no tag appended)
//   - Seeking keys that are not separators
//   - Seeking past all keys / Next() past the last block of a run
//   - kUnknown on exhaustion after a run
//   - Next() transitions: run->next separator, run->next run
//   - Bounds checking while walking a run
//   - Re-seeking after a seek that moved past a run
// ============================================================================

TEST_F(TrieIndexFactoryTest, LargeRun) {
  // 12 blocks all with user key "key", seqnos descending from 1200 to 100.
  // The last "key" block transitions to "zzz", so
  // FindShortestSeparator("key", "zzz") = "l". The separators are:
  //   "key" (run of blocks 0-10, tags 1200..200) -> "l" (block 11) ->
  //   "zzz" (block 12)
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));

  const int kNumKeyBlocks = 12;
  for (int i = 0; i < kNumKeyBlocks; i++) {
    IndexFactoryBuilder::BlockHandle handle{static_cast<uint64_t>(i) * 1000,
                                            1000};
    std::string scratch;
    SequenceNumber seq = static_cast<SequenceNumber>((kNumKeyBlocks - i) * 100);

    if (i < kNumKeyBlocks - 1) {
      // Same-user-key boundary: "key" -> "key".
      Slice next("key");
      SequenceNumber next_seq =
          static_cast<SequenceNumber>((kNumKeyBlocks - i - 1) * 100);
      builder->AddIndexEntry(Slice("key"), &next, handle, &scratch,
                             EntryCtx(seq, next_seq));
    } else {
      // Last "key" block transitions to "zzz".
      // FindShortestSeparator("key", "zzz") = "l".
      Slice next("zzz");
      builder->AddIndexEntry(Slice("key"), &next, handle, &scratch,
                             EntryCtx(seq, 1));
    }
  }
  // Final "zzz" block. Last block uses "zzz" as separator (no shortening).
  {
    IndexFactoryBuilder::BlockHandle handle{
        static_cast<uint64_t>(kNumKeyBlocks) * 1000, 1000};
    std::string scratch;
    builder->AddIndexEntry(Slice("zzz"), nullptr, handle, &scratch,
                           EntryCtx(1, 0));
  }

  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));

  std::unique_ptr<IndexFactoryReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));

  ReadOptions ro;
  auto iter = reader->NewIterator(ro);
  IterateResult result;

  // Seek "key"|kMax -> Block 0.
  ASSERT_OK(iter->SeekAndGetResult(Slice("key"), &result,
                                   SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(iter->value().offset, 0u);

  // Seek "key"|1100 -> Block 1, the first block of the run whose tag is at
  // most 1100.
  ASSERT_OK(iter->SeekAndGetResult(Slice("key"), &result, SeekCtx(1100)));
  ASSERT_EQ(iter->value().offset, 1000u);

  // Seek "key"|550 -> Block 7 (tag 500), the first block of the run whose
  // tag is at most 550.
  ASSERT_OK(iter->SeekAndGetResult(Slice("key"), &result, SeekCtx(550)));
  ASSERT_EQ(iter->value().offset, 7000u);

  // Seek "key"|50 -> every block of the run has a larger tag (the smallest
  // is 200), so Seek moves to the next separator, "l" (block 11).
  ASSERT_OK(iter->SeekAndGetResult(Slice("key"), &result, SeekCtx(50)));
  ASSERT_EQ(iter->value().offset, 11000u);

  // Seek "key"|0 -> same: advance past run -> "l" (block 11).
  ASSERT_OK(iter->SeekAndGetResult(Slice("key"), &result, SeekCtx(0)));
  ASSERT_EQ(iter->value().offset, 11000u);

  // Full forward scan: blocks 0..10 ("key" run) -> 11 ("l") -> 12 ("zzz").
  ASSERT_OK(iter->SeekAndGetResult(Slice("key"), &result,
                                   SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(iter->value().offset, 0u);
  for (int i = 1; i <= kNumKeyBlocks; i++) {
    ASSERT_OK(iter->NextAndGetResult(&result));
    ASSERT_EQ(iter->value().offset, static_cast<uint64_t>(i) * 1000)
        << "Next failed at block " << i;
  }
  // Past end -- kUnknown (exhaustion doesn't imply upper bound reached).
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);
}

TEST_F(TrieIndexFactoryTest, MixedSameKeyRuns) {
  // Two different user keys that each span several blocks, plus distinct
  // blocks.
  //
  // Blocks (separator after FindShortestSeparator):
  //   Block 0: "aaa"|300 -> next "aaa"|200 (same-key -> separator "aaa")
  //   Block 1: "aaa"|200 -> next "aaa"|100 (same-key -> separator "aaa")
  //   Block 2: "aaa"|100 -> next "mmm"|60  (diff-key -> separator "b")
  //   Block 3: "mmm"|60  -> next "mmm"|30  (same-key -> separator "mmm")
  //   Block 4: "mmm"|30  -> next "zzz"|10  (diff-key -> separator "n")
  //   Block 5: "zzz"|10  -> null           (last -> separator "zzz")
  //
  // The last block of each user key gets a shortened separator, so the
  // "aaa" run is blocks 0-1 (tags 300, 200), and "mmm" has only block 3
  // (tag 60).
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));

  // User key "aaa": 3 blocks (the first two end at same-key boundaries).
  {
    IndexFactoryBuilder::BlockHandle h{0, 1000};
    std::string scratch;
    Slice next("aaa");
    builder->AddIndexEntry(Slice("aaa"), &next, h, &scratch,
                           EntryCtx(300, 200));
  }
  {
    IndexFactoryBuilder::BlockHandle h{1000, 1000};
    std::string scratch;
    Slice next("aaa");
    builder->AddIndexEntry(Slice("aaa"), &next, h, &scratch,
                           EntryCtx(200, 100));
  }
  // Last "aaa" block transitions to "mmm" -> separator "b".
  {
    IndexFactoryBuilder::BlockHandle h{2000, 1000};
    std::string scratch;
    Slice next("mmm");
    builder->AddIndexEntry(Slice("aaa"), &next, h, &scratch, EntryCtx(100, 60));
  }
  // User key "mmm": 2 blocks (one same-key boundary).
  {
    IndexFactoryBuilder::BlockHandle h{3000, 1000};
    std::string scratch;
    Slice next("mmm");
    builder->AddIndexEntry(Slice("mmm"), &next, h, &scratch, EntryCtx(60, 30));
  }
  // Last "mmm" block transitions to "zzz" -> separator "n".
  {
    IndexFactoryBuilder::BlockHandle h{4000, 1000};
    std::string scratch;
    Slice next("zzz");
    builder->AddIndexEntry(Slice("mmm"), &next, h, &scratch, EntryCtx(30, 10));
  }
  // "zzz": 1 block (last -> separator "zzz", no shortening).
  {
    IndexFactoryBuilder::BlockHandle h{5000, 1000};
    std::string scratch;
    builder->AddIndexEntry(Slice("zzz"), nullptr, h, &scratch, EntryCtx(10, 0));
  }

  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));

  std::unique_ptr<IndexFactoryReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));

  ReadOptions ro;
  auto iter = reader->NewIterator(ro);
  IterateResult result;

  // Separators: "aaa" (run of 2) -> "b" -> "mmm" -> "n" -> "zzz"
  //
  // Seek "aaa"|kMax -> Block 0.
  ASSERT_OK(iter->SeekAndGetResult(Slice("aaa"), &result,
                                   SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(iter->value().offset, 0u);

  // Seek "aaa"|250 -> Block 1, the first block of the run whose tag is at
  // most 250.
  ASSERT_OK(iter->SeekAndGetResult(Slice("aaa"), &result, SeekCtx(250)));
  ASSERT_EQ(iter->value().offset, 1000u);

  // Seek "aaa"|50 -> both blocks of the run have larger tags, so Seek moves
  // to the next separator, "b" (block 2).
  ASSERT_OK(iter->SeekAndGetResult(Slice("aaa"), &result, SeekCtx(50)));
  ASSERT_EQ(iter->value().offset, 2000u);

  // Seek "mmm"|kMax -> Block 3 (separator "mmm").
  ASSERT_OK(iter->SeekAndGetResult(Slice("mmm"), &result,
                                   SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(iter->value().offset, 3000u);

  // Seek "mmm"|45 -> Block 3's tag 60 is larger and "mmm" has no other
  // block, so Seek moves to the next separator, "n" (block 4).
  ASSERT_OK(iter->SeekAndGetResult(Slice("mmm"), &result, SeekCtx(45)));
  ASSERT_EQ(iter->value().offset, 4000u);

  // Full forward scan: 0 -> 1 -> 2 -> 3 -> 4 -> 5.
  ASSERT_OK(iter->SeekAndGetResult(Slice("aaa"), &result,
                                   SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(iter->value().offset, 0u);

  uint64_t expected_offsets[] = {0, 1000, 2000, 3000, 4000, 5000};
  for (int i = 1; i < 6; i++) {
    ASSERT_OK(iter->NextAndGetResult(&result));
    ASSERT_EQ(iter->value().offset, expected_offsets[i])
        << "Next failed at i=" << i;
  }
  // Past end -- kUnknown (exhaustion doesn't imply upper bound reached).
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);
}

TEST_F(TrieIndexFactoryTest, AdjacentSameKeyRuns) {
  // Two user keys that each span two blocks, one after the other. The last
  // "aaa" block has a different-key boundary, so it gets the shortened
  // separator "b", a single-block separator between the "aaa" and "bbb"
  // blocks.
  //
  // Entries (separator after FindShortestSeparator):
  //   Entry 0: "aaa" 200 next="aaa" -> sep="aaa" (same-key)
  //   Entry 1: "aaa" 100 next="bbb" -> sep="b"   (diff-key)
  //   Entry 2: "bbb"  80 next="bbb" -> sep="bbb"  (same-key)
  //   Entry 3: "bbb"  40 next=null  -> sep="bbb"  (last block, no shortening)
  //
  // Separators: "aaa" (1 block, tag 200) -> "b" (1 block) -> "bbb" (run of 2,
  // tags 80, 40)
  // Note: the last block joins the "bbb" run since its separator also is "bbb".
  auto ctx = BuildTrieAndGetIterator({
      {"aaa", "aaa", 0, 1000, 200, 100},
      {"aaa", "bbb", 1000, 1000, 100, 80},
      {"bbb", "bbb", 2000, 1000, 80, 40},
      {"bbb", "", 3000, 1000, 40, 0},
  });

  IterateResult result;

  // Full scan: "aaa" (block 0) -> "b" (block 1) -> "bbb" (block 2) -> "bbb"
  // (block 3, the second block of the run).
  ASSERT_OK(ctx.iter->SeekAndGetResult(Slice("aaa"), &result,
                                       SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(ctx.iter->value().offset, 0u);
  ASSERT_EQ(result.key.ToString(), "aaa");

  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(ctx.iter->value().offset, 1000u);
  ASSERT_EQ(result.key.ToString(), "b");

  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(ctx.iter->value().offset, 2000u);
  ASSERT_EQ(result.key.ToString(), "bbb");

  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(ctx.iter->value().offset, 3000u);
  ASSERT_EQ(result.key.ToString(), "bbb");

  // Past end -- kUnknown (exhaustion doesn't imply upper bound reached).
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);
}

TEST_F(TrieIndexFactoryTest, TaggedResultKeyIsUserKey) {
  // Verify that result.key from SeekAndGetResult is the separator, with no
  // tag appended. At a same-key boundary, the separator IS the original user
  // key. For different-key boundaries, the separator is shortened.
  //
  // Entries:
  //   "foo" 100 next="foo" -> sep="foo" (same-key)
  //   "foo"  50 next="zoo" -> sep="g"   (FindShortestSeparator("foo","zoo"))
  //   "zoo"   1 next=null  -> sep="zoo" (last block, no successor shortening)
  auto ctx = BuildTrieAndGetIterator({
      {"foo", "foo", 0, 500, 100, 50},
      {"foo", "zoo", 500, 500, 50, 1},
      {"zoo", "", 1000, 500, 1, 0},
  });

  IterateResult result;

  // Seek to "foo"|100 -> block 0. result.key = "foo" (3 bytes, no tag).
  ASSERT_OK(ctx.iter->SeekAndGetResult(Slice("foo"), &result, SeekCtx(100)));
  ASSERT_EQ(result.key.ToString(), "foo");
  ASSERT_EQ(result.key.size(), 3u);

  // Next -> block 1 (FindShortestSeparator("foo", "zoo") = "g").
  // result.key = "g" (1 byte).
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.key.size(), 1u);
  // The separator is "g" (or similar shortened form).
  ASSERT_GE(result.key.ToString(), "g");
  ASSERT_LT(result.key.ToString(), "zoo");
}

TEST_F(TrieIndexFactoryTest, SeekNonExistentKeyWithTags) {
  // Seeking keys that are not separators.
  //
  // Separators: "mmm" (1 block, tag 100) -> "n" -> "zzz"
  auto ctx = BuildTrieAndGetIterator({
      {"mmm", "mmm", 0, 1000, 100, 50},
      {"mmm", "zzz", 1000, 1000, 50, 1},
      {"zzz", "", 2000, 1000, 1, 0},
  });
  auto* iter = ctx.iter.get();

  // Seek "aaa" (before all keys) -> lands on "mmm", Block 0.
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(iter, Slice("aaa"), kMaxSequenceNumber, 0u));
  // Seek "nnn" (between "n" and "zzz") -> lands on "zzz", Block 2.
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(iter, Slice("nnn"), kMaxSequenceNumber, 2000u));

  // Seek "|" (past all keys, "|" > "zzz") -> kUnknown.
  IterateResult result;
  ASSERT_OK(
      iter->SeekAndGetResult(Slice("|"), &result, SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);
}

TEST_F(TrieIndexFactoryTest, TagsPastEndAndNextPastEnd) {
  // Verify seeking past all keys and Next() past the last block, which ends
  // a run.
  //
  // Separators: "key" (run of 2, tags 10, 5)
  auto ctx = BuildTrieAndGetIterator({
      {"key", "key", 0, 500, 10, 5},
      {"key", "", 500, 500, 5, 0},
  });
  auto* iter = ctx.iter.get();
  IterateResult result;

  // Seek past all keys -> kUnknown (exhaustion doesn't imply upper bound).
  ASSERT_OK(iter->SeekAndGetResult(Slice("zzz"), &result,
                                   SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);

  // Seek "key"|5 -> block 1, the first block of the run whose tag is at most
  // 5.
  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(iter, Slice("key"), 5, 500u));

  // Next from last block in run -> past end.
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);

  // Full forward scan: block 0 -> block 1 -> past end.
  ASSERT_NO_FATAL_FAILURE(AssertFullForwardScan(iter, Slice("key"), {0, 500}));
}

TEST_F(TrieIndexFactoryTest, OutOfBoundWithRun) {
  // Verify that bounds checking works correctly while walking a run and the
  // blocks after it.
  //
  // Entries:
  //   "key" 300 next="key" -> sep="key" (same-key)
  //   "key" 200 next="key" -> sep="key" (same-key)
  //   "key" 100 next="zzz" -> sep="l"
  //   "zzz"   1 next=null  -> sep="zzz" (last block, no shortening)
  //
  // Separators: "key" (run of 2, tags 300, 200) -> "l" -> "zzz"
  auto ctx = BuildTrieAndGetIterator({
      {"key", "key", 0, 1000, 300, 200},
      {"key", "key", 1000, 1000, 200, 100},
      {"key", "zzz", 2000, 1000, 100, 1},
      {"zzz", "", 3000, 1000, 1, 0},
  });

  // Set upper bound "zzz".
  ScanOptions scan(Slice("key"), Slice("zzz"));
  ctx.iter->Prepare(&scan, 1);

  IterateResult result;

  // Seek "key"|300 -> Block 0, target "key" < "zzz" -> kInbound.
  ASSERT_OK(ctx.iter->SeekAndGetResult(Slice("key"), &result, SeekCtx(300)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 0u);

  // Next -> Block 1 (second block of the "key" run), prev "key" < "zzz" ->
  // kInbound.
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 1000u);

  // Next -> "l" (block 2), prev "key" < "zzz" -> kInbound.
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 2000u);

  // Next -> "zzz" (block 3). prev key is "l", "l" < "zzz" -> kInbound.
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 3000u);

  // Now test with a tighter bound: limit = "l".
  // "l" is the separator for block 2. Since limit is exclusive and the
  // prev_key comparison uses "key" (the separator for block 0/1):
  ScanOptions scan2(Slice("key"), Slice("l"));
  ctx.iter->Prepare(&scan2, 1);

  // Seek "key"|300 -> Block 0, target "key" < "l" -> kInbound.
  ASSERT_OK(ctx.iter->SeekAndGetResult(Slice("key"), &result, SeekCtx(300)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);

  // Next -> Block 1 (second block of the run), prev "key" < "l" -> kInbound.
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);

  // Next -> "l" (block 2). prev "key" < "l" -> kInbound (conservative).
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);

  // Next -> "zzz" (block 3). prev "l" >= "l" -> kOutOfBound.
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kOutOfBound);
}

TEST_F(TrieIndexFactoryTest, SeekWithMaxSeqOnSameKeyBlocks) {
  // kMaxSequenceNumber should always land on the first block of a run.
  //
  // Separators: "key" (run of 4, tags 400, 300, 200, 100)
  auto ctx = BuildTrieAndGetIterator({
      {"key", "key", 0, 1000, 400, 300},
      {"key", "key", 1000, 1000, 300, 200},
      {"key", "key", 2000, 1000, 200, 100},
      {"key", "", 3000, 1000, 100, 0},
  });
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("key"), kMaxSequenceNumber, 0u));
}

TEST_F(TrieIndexFactoryTest, SeekWithZeroSeqOnSameKeyBlocks) {
  // seq=0 is below every tag of the run, so Seek moves past the whole run.
  //
  // Separators: "key" (run of 2, tags 300, 200) -> "l" -> "zzz"
  // Seek "key"|0 lands on "l" (block 2).
  auto ctx = BuildTrieAndGetIterator({
      {"key", "key", 0, 1000, 300, 200},
      {"key", "key", 1000, 1000, 200, 100},
      {"key", "zzz", 2000, 1000, 100, 1},
      {"zzz", "", 3000, 1000, 1, 0},
  });
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("key"), 0, 2000u));
}

TEST_F(TrieIndexFactoryTest, ZeroSeqMustNotSkipSmallerUserKey) {
  // Validates correctness of DBIter's forward-scan reseek path.
  //
  // Block 0 ends with user key "m" and block 1 starts with the same user key,
  // so block 0 gets separator "m" and a nonzero tag.
  // However, a seek target for a *smaller* user key "l"|0 must still land on
  // block 0, because block 0 can contain keys in ("l", "m"].
  //
  // Verifies that tags are NOT compared when the target user key is strictly
  // less than the separator. The tag comparison is only meaningful when user
  // keys are equal. If the target user key "l" < separator "m", the seek must
  // stay on block 0 regardless of seqno, because block 0 can contain keys up
  // to "m".
  auto ctx = BuildTrieAndGetIterator({
      {"m", "m", 0, 1000, 300, 200},
      {"y", "zzz", 1000, 1000, 100, 1},
      {"zzz", "", 2000, 1000, 1, 0},
  });

  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(ctx.iter.get(), Slice("l"), 0, 0u));
}

TEST_F(TrieIndexFactoryTest, NextFromRunToRun) {
  // Test Next() walking from one run, through a single-block separator, into
  // another run.
  //
  // Both user keys need several same-key boundaries to form runs. The last
  // "aaa" block gets the shortened separator "b", which sits between them.
  //
  // Entries:
  //   "aaa" 200 next="aaa" -> sep="aaa" (same-key)
  //   "aaa" 100 next="aaa" -> sep="aaa" (same-key)
  //   "aaa"  50 next="bbb" -> sep="b"   (diff-key)
  //   "bbb"  90 next="bbb" -> sep="bbb" (same-key)
  //   "bbb"  60 next="bbb" -> sep="bbb" (same-key)
  //   "bbb"  30 next=null  -> sep="bbb" (last block, no successor shortening)
  //
  // Separators: "aaa" (run of 2, tags 200, 100) -> "b" ->
  //   "bbb" (run of 3, tags 90, 60, 30)
  // Note: the last block's separator is "bbb" (not "c"), matching the standard
  // index builder's behavior with kShortenSeparators (the default). This means
  // the last block joins the "bbb" run, making it a 3-block run.
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));

  {
    IndexFactoryBuilder::BlockHandle h{0, 1000};
    std::string scratch;
    Slice next("aaa");
    builder->AddIndexEntry(Slice("aaa"), &next, h, &scratch,
                           EntryCtx(200, 100));
  }
  {
    IndexFactoryBuilder::BlockHandle h{1000, 1000};
    std::string scratch;
    Slice next("aaa");
    builder->AddIndexEntry(Slice("aaa"), &next, h, &scratch, EntryCtx(100, 50));
  }
  {
    IndexFactoryBuilder::BlockHandle h{2000, 1000};
    std::string scratch;
    Slice next("bbb");
    builder->AddIndexEntry(Slice("aaa"), &next, h, &scratch, EntryCtx(50, 90));
  }
  {
    IndexFactoryBuilder::BlockHandle h{3000, 1000};
    std::string scratch;
    Slice next("bbb");
    builder->AddIndexEntry(Slice("bbb"), &next, h, &scratch, EntryCtx(90, 60));
  }
  {
    IndexFactoryBuilder::BlockHandle h{4000, 1000};
    std::string scratch;
    Slice next("bbb");
    builder->AddIndexEntry(Slice("bbb"), &next, h, &scratch, EntryCtx(60, 30));
  }
  {
    IndexFactoryBuilder::BlockHandle h{5000, 1000};
    std::string scratch;
    builder->AddIndexEntry(Slice("bbb"), nullptr, h, &scratch, EntryCtx(30, 0));
  }

  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));

  std::unique_ptr<IndexFactoryReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));

  ReadOptions ro;
  auto iter = reader->NewIterator(ro);
  IterateResult result;

  // Full forward scan.
  ASSERT_OK(iter->SeekAndGetResult(Slice("aaa"), &result,
                                   SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(iter->value().offset, 0u);
  ASSERT_EQ(result.key.ToString(), "aaa");

  ASSERT_OK(iter->NextAndGetResult(&result));  // "aaa" (block 1)
  ASSERT_EQ(iter->value().offset, 1000u);
  ASSERT_EQ(result.key.ToString(), "aaa");

  ASSERT_OK(iter->NextAndGetResult(&result));  // -> "b" (block 2)
  ASSERT_EQ(iter->value().offset, 2000u);
  ASSERT_EQ(result.key.ToString(), "b");

  ASSERT_OK(iter->NextAndGetResult(&result));  // -> "bbb" (block 3)
  ASSERT_EQ(iter->value().offset, 3000u);
  ASSERT_EQ(result.key.ToString(), "bbb");

  ASSERT_OK(iter->NextAndGetResult(&result));  // "bbb" (block 4)
  ASSERT_EQ(iter->value().offset, 4000u);
  ASSERT_EQ(result.key.ToString(), "bbb");

  ASSERT_OK(iter->NextAndGetResult(&result));  // "bbb" (block 5)
  ASSERT_EQ(iter->value().offset, 5000u);
  ASSERT_EQ(result.key.ToString(), "bbb");

  // Past end -- kUnknown (exhaustion doesn't imply upper bound reached).
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);
}

TEST_F(TrieIndexFactoryTest, SingleBlockWithSeqnoActive) {
  // Both blocks have the same user key "x". Without successor shortening
  // for the last block, both separators are "x", forming a 2-block run:
  //   "x" (run of 2, tags 10, 5)
  auto ctx = BuildTrieAndGetIterator({
      {"x", "x", 0, 500, 10, 5},
      {"x", "", 500, 500, 5, 0},
  });
  auto* iter = ctx.iter.get();

  // Seek "x"|10 -> Block 0, whose tag is 10.
  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(iter, Slice("x"), 10, 0u));
  // Seek "x"|7 -> Block 1, the first block of the run whose tag is at most 7.
  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(iter, Slice("x"), 7, 500u));
  // Seek "x"|3 -> both tags are larger and there is no later separator, so
  // the iterator becomes invalid. This matches the standard index
  // behavior: seeking for internal key "x"|3 is past the standard index's
  // last separator "x"|5.
  {
    IterateResult result;
    ASSERT_OK(iter->SeekAndGetResult(Slice("x"), &result, SeekCtx(3)));
    ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);
  }
}

TEST_F(TrieIndexFactoryTest, TagDistinguishesSameSeqDifferentType) {
  // the trie must use the full tag (seq << 8 |
  // type) for block selection, not just the sequence number. Without this,
  // when two blocks share the same user key and same seqno but differ in
  // value type, the trie could return the wrong block.
  //
  // Scenario: block 0 has tag (50 << 8) | 0 = 12800
  // (kTypeDeletion) Target has tag (50 << 8) | 1 = 12801
  // (kTypeValue) In internal key order, higher tag = smaller key, so the
  // target (12801) sorts before block 0's last key (12800). The trie should
  // stay on Block 0.
  //
  // With seqno-only comparison: 50 < 50 is false -> stay. Same result.
  // But the reverse case matters:
  // Block 0 tag (50 << 8) | 1 = 12801 (kTypeValue)
  // Target tag (50 << 8) | 0 = 12800 (kTypeDeletion)
  // target (12800) < block 0's tag (12801) -> advance to Block 1.
  // With seqno-only: 50 < 50 is false -> stay (WRONG -- should advance).

  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();
  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));

  std::string scratch;
  // Block 0: last key = ("x", seqno=50, kTypeValue=1), packed=12801
  Slice next("x");
  builder->AddIndexEntry(Slice("x"), &next, {0, 500}, &scratch,
                         {(50ULL << 8) | 1, (50ULL << 8) | 0});
  // Block 1: last key = ("x", seqno=50, kTypeDeletion=0), packed=12800
  builder->AddIndexEntry(Slice("x"), nullptr, {500, 500}, &scratch,
                         {(50ULL << 8) | 0, 0});

  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));
  std::unique_ptr<IndexFactoryReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));
  ReadOptions ro;
  auto iter = reader->NewIterator(ro);

  IterateResult result;

  // Target: ("x", seqno=50, kTypeValue=1), packed=12801.
  // Block 0 tag=12801. 12801 >= 12801 -> stay on Block 0. Correct.
  ASSERT_OK(iter->SeekAndGetResult(Slice("x"), &result, {(50ULL << 8) | 1}));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(iter->value().offset, 0u);

  // Target: ("x", seqno=50, kTypeDeletion=0), packed=12800.
  // Block 0 tag=12801. 12800 < 12801 -> advance to Block 1. Correct.
  // With seqno-only comparison (without tag): 50 < 50 -> false ->
  // stay on Block 0 (WRONG).
  ASSERT_OK(iter->SeekAndGetResult(Slice("x"), &result, {(50ULL << 8) | 0}));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(iter->value().offset, 500u);
}

TEST_F(TrieIndexFactoryTest, ReSeekAfterRun) {
  // Verify that re-seeking after a seek that moved past a run lands on the
  // right block each time.
  //
  // Separators: "key" (run of 2, tags 300, 200) -> "l" -> "zzz"
  auto ctx = BuildTrieAndGetIterator({
      {"key", "key", 0, 1000, 300, 200},
      {"key", "key", 1000, 1000, 200, 100},
      {"key", "zzz", 2000, 1000, 100, 1},
      {"zzz", "", 3000, 1000, 1, 0},
  });
  auto* iter = ctx.iter.get();

  // Both tags of the run are larger than 150 -> Seek moves to "l" (block 2).
  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(iter, Slice("key"), 150, 2000u));
  // Re-seek to "key"|300 -> should reset to Block 0.
  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(iter, Slice("key"), 300, 0u));
  // Re-seek to "zzz" -> block 3.
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(iter, Slice("zzz"), kMaxSequenceNumber, 3000u));

  // Next should go past end.
  IterateResult result;
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);
}

TEST_F(TrieIndexFactoryTest, AllFfLastKeyWithSameKeyBoundary) {
  // Validates that an all-0xFF last key with same-user-key boundary preceding
  // it. The last block uses "\xff\xff" as its separator (no shortening), which
  // matches the previous entry's separator. AddIndexEntry detects the collision
  // and correctly treats it as a same-user-key continuation.
  std::string ff("\xff\xff", 2);
  auto ctx = BuildTrieAndGetIterator({
      {ff, ff, 0, 500, 200, 100},
      {ff, "", 500, 500, 100, 0},
  });
  auto* iter = ctx.iter.get();

  // Seek "\xff\xff"|200 -> Block 0.
  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(iter, Slice(ff), 200, 0u));
  // Seek "\xff\xff"|100 -> Block 1, the first block of the run whose tag is
  // at most 100.
  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(iter, Slice(ff), 100, 500u));

  // Seek "\xff\xff"|50 -> past entire run -> exhausted -> kUnknown.
  IterateResult result;
  ASSERT_OK(iter->SeekAndGetResult(Slice(ff), &result, SeekCtx(50)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);

  // Forward scan.
  ASSERT_NO_FATAL_FAILURE(AssertFullForwardScan(iter, Slice(ff), {0, 500}));
}

TEST_F(TrieIndexFactoryTest, AllFfLastKeyWithoutPrecedingSameKey) {
  // Complementary test: all-0xFF last key but NO same-user-key boundary.
  // Each separator ("b" and "\xff\xff") has a single block.
  std::string ff("\xff\xff", 2);
  auto ctx = BuildTrieAndGetIterator({
      {"aaa", ff, 0, 500, 100, 50},
      {ff, "", 500, 500, 50, 0},
  });
  ASSERT_NO_FATAL_FAILURE(
      AssertFullForwardScan(ctx.iter.get(), Slice("aaa"), {0, 500}));
}

TEST_F(TrieIndexFactoryTest, TagsRandomized) {
  // Randomized test: generate random block layouts with a mix of same-user-key
  // and distinct-key boundaries, build a trie, then verify:
  //   1. Full forward scan visits all blocks in offset order
  //   2. Seeking each block's key with its exact seqno returns the right offset
  //   3. Seeking with kMaxSequenceNumber returns the first block for that key
  //
  const uint32_t seed = TimeSeed();
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);

  // Run multiple iterations with different random layouts.
  for (int trial = 0; trial < 20; trial++) {
    SCOPED_TRACE("trial=" + std::to_string(trial));

    // Generate 5-30 blocks with random keys.
    const int num_blocks = 5 + static_cast<int>(rnd.Uniform(26));
    std::vector<TestBlock> blocks;
    blocks.reserve(num_blocks);

    // Track what we build for verification: (offset, user_key, seqno) tuples.
    struct BlockInfo {
      uint64_t offset;
      std::string user_key;
      SequenceNumber seqno;
    };
    std::vector<BlockInfo> block_infos;

    // Use formatted keys "key_NNNNN" with a monotonically increasing counter.
    // This guarantees:
    //   1. Strictly increasing key order (no infinite retry loops).
    //   2. FindShortestSeparator always shortens (gap at the numeric suffix
    //      is always >= 2 when we skip counter values), avoiding separator
    //      collisions that would conflate blocks into same-user-key runs.
    //      Separator collision edge cases are tested separately (AllFf*).
    uint32_t key_counter = 100 + rnd.Uniform(1000);

    auto make_key = [](uint32_t n) -> std::string {
      char buf[16];
      snprintf(buf, sizeof(buf), "key_%05u", n);
      return buf;
    };

    std::string current_key = make_key(key_counter);
    SequenceNumber seq = 10000;

    for (int i = 0; i < num_blocks; i++) {
      uint64_t offset = static_cast<uint64_t>(i) * 1000;
      SequenceNumber block_seq = seq;

      bool is_last = (i == num_blocks - 1);
      bool same_key_boundary =
          !is_last && (rnd.Uniform(3) == 0);  // ~33% chance

      if (is_last) {
        // Last block: no next key.
        blocks.push_back({current_key, "", offset, 500, block_seq, 0});
      } else if (same_key_boundary) {
        // Same-user-key boundary: next block has same key, lower seqno.
        SequenceNumber next_seq = seq - (1 + rnd.Uniform(100));
        blocks.push_back(
            {current_key, current_key, offset, 500, block_seq, next_seq});
        seq = next_seq;
      } else {
        // Different-key boundary. Skip 2+ counter values to ensure
        // FindShortestSeparator produces a distinct separator.
        key_counter += 2 + rnd.Uniform(10);
        std::string next_key = make_key(key_counter);
        SequenceNumber next_seq =
            static_cast<SequenceNumber>(1 + rnd.Uniform(10000));
        blocks.push_back(
            {current_key, next_key, offset, 500, block_seq, next_seq});
        current_key = next_key;
        seq = next_seq;
      }

      block_infos.push_back({offset, blocks.back().last_key, block_seq});
    }

    auto ctx = BuildTrieAndGetIterator(blocks);
    auto* iter = ctx.iter.get();

    // Verification 1: full forward scan visits all blocks in order.
    {
      std::vector<uint64_t> expected_offsets;
      expected_offsets.reserve(num_blocks);
      for (const auto& bi : block_infos) {
        expected_offsets.push_back(bi.offset);
      }
      // Find the smallest key to start the scan.
      ASSERT_NO_FATAL_FAILURE(AssertFullForwardScan(
          iter, Slice(block_infos[0].user_key), expected_offsets));
    }

    // Verification 2: seek with kMaxSequenceNumber for each distinct user key
    // should land on the first block with that key.
    {
      std::string prev_key;
      for (size_t i = 0; i < block_infos.size(); i++) {
        if (block_infos[i].user_key != prev_key) {
          ASSERT_NO_FATAL_FAILURE(
              AssertSeekOffset(iter, Slice(block_infos[i].user_key),
                               kMaxSequenceNumber, block_infos[i].offset));
          prev_key = block_infos[i].user_key;
        }
      }
    }
  }
}

// ============================================================================
// End-to-end SST integration tests
// ============================================================================

class TrieIndexSSTTest : public testing::Test {
 protected:
  void SetUp() override {
    trie_factory_ = std::make_shared<TrieIndexFactory>();
    sst_path_ = test::PerThreadDBPath("trie_index_sst_test.sst");
  }

  void TearDown() override {
    // Clean up SST file.
    Env::Default()->DeleteFile(sst_path_).PermitUncheckedError();
  }

  // Generate sorted key-value pairs: "key_NNNN" -> "value_NNNN".
  std::vector<std::pair<std::string, std::string>> GenerateKVs(int count) {
    std::vector<std::pair<std::string, std::string>> kvs;
    kvs.reserve(count);
    for (int i = 0; i < count; i++) {
      char key_buf[32];
      char val_buf[32];
      snprintf(key_buf, sizeof(key_buf), "key_%04d", i);
      snprintf(val_buf, sizeof(val_buf), "value_%04d", i);
      kvs.emplace_back(key_buf, val_buf);
    }
    return kvs;
  }

  // Write an SST file with the given key-value pairs using the trie UDI.
  Status WriteSST(const std::vector<std::pair<std::string, std::string>>& kvs) {
    Options options;
    BlockBasedTableOptions table_options;
    table_options.user_defined_index_factory = trie_factory_;
    table_options.index_mode =
        BlockBasedTableOptions::IndexMode::kStandardDefault;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    SstFileWriter writer(EnvOptions(), options);
    Status s = writer.Open(sst_path_);
    if (!s.ok()) {
      return s;
    }

    for (const auto& kv : kvs) {
      s = writer.Put(kv.first, kv.second);
      if (!s.ok()) {
        return s;
      }
    }
    return writer.Finish();
  }

  std::shared_ptr<TrieIndexFactory> trie_factory_;
  std::string sst_path_;
};

TEST_F(TrieIndexSSTTest, WriteAndReadWithTrieUDI) {
  // Write SST with trie UDI.
  auto kvs = GenerateKVs(100);
  ASSERT_OK(WriteSST(kvs));

  // Read without UDI (native index) -- should work since native index is
  // always present alongside the UDI.
  {
    Options options;
    BlockBasedTableOptions table_options;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    SstFileReader reader(options);
    ASSERT_OK(reader.Open(sst_path_));

    ReadOptions ro;
    std::unique_ptr<Iterator> iter(reader.NewIterator(ro));
    iter->SeekToFirst();
    int count = 0;
    for (; iter->Valid(); iter->Next()) {
      ASSERT_EQ(iter->key().ToString(), kvs[count].first)
          << "Key mismatch at " << count;
      ASSERT_EQ(iter->value().ToString(), kvs[count].second)
          << "Value mismatch at " << count;
      count++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(count, 100);
  }

  // Read WITH trie UDI -- use read_index in ReadOptions.
  {
    Options options;
    BlockBasedTableOptions table_options;
    table_options.user_defined_index_factory = trie_factory_;
    table_options.index_mode =
        BlockBasedTableOptions::IndexMode::kStandardDefault;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    SstFileReader reader(options);
    ASSERT_OK(reader.Open(sst_path_));

    ReadOptions ro;
    ro.read_index = ReadOptions::ReadIndex::kPreferCustom;
    std::unique_ptr<Iterator> iter(reader.NewIterator(ro));

    // Full forward scan via Seek.
    iter->Seek("key_0000");
    int count = 0;
    for (; iter->Valid(); iter->Next()) {
      ASSERT_LT(count, 100) << "Too many keys";
      ASSERT_EQ(iter->value().ToString(), kvs[count].second)
          << "Value mismatch at " << count;
      count++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(count, 100);
  }
}

TEST_F(TrieIndexSSTTest, SeekWithTrieUDI) {
  auto kvs = GenerateKVs(100);
  ASSERT_OK(WriteSST(kvs));

  Options options;
  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory = trie_factory_;
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  SstFileReader reader(options);
  ASSERT_OK(reader.Open(sst_path_));

  ReadOptions ro;
  ro.read_index = ReadOptions::ReadIndex::kPreferCustom;
  std::unique_ptr<Iterator> iter(reader.NewIterator(ro));

  // Seek to the middle.
  iter->Seek("key_0050");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  int count = 0;
  for (; iter->Valid(); iter->Next()) {
    count++;
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(count, 50);

  // Seek past all keys.
  iter->Seek("key_9999");
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  // Seek before all keys.
  iter->Seek("a");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ(iter->key().ToString(), kvs[0].first);
}

TEST_F(TrieIndexSSTTest, SeekWithUpperBound) {
  auto kvs = GenerateKVs(100);
  ASSERT_OK(WriteSST(kvs));

  Options options;
  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory = trie_factory_;
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  SstFileReader reader(options);
  ASSERT_OK(reader.Open(sst_path_));

  ReadOptions ro;
  ro.read_index = ReadOptions::ReadIndex::kPreferCustom;
  Slice upper_bound("key_0075");
  ro.iterate_upper_bound = &upper_bound;
  std::unique_ptr<Iterator> iter(reader.NewIterator(ro));

  iter->Seek("key_0050");
  ASSERT_TRUE(iter->Valid());
  int count = 0;
  for (; iter->Valid(); iter->Next()) {
    ASSERT_LT(iter->key().compare(upper_bound), 0)
        << "Key " << iter->key().ToString() << " >= upper bound";
    count++;
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(count, 25);
}

TEST_F(TrieIndexSSTTest, SmallSST) {
  // Edge case: SST with very few keys (just one data block).
  auto kvs = GenerateKVs(3);
  ASSERT_OK(WriteSST(kvs));

  Options options;
  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory = trie_factory_;
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  SstFileReader reader(options);
  ASSERT_OK(reader.Open(sst_path_));

  ReadOptions ro;
  ro.read_index = ReadOptions::ReadIndex::kPreferCustom;
  std::unique_ptr<Iterator> iter(reader.NewIterator(ro));

  iter->Seek("key_0000");
  int count = 0;
  for (; iter->Valid(); iter->Next()) {
    ASSERT_EQ(iter->value().ToString(), kvs[count].second);
    count++;
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(count, 3);
}

// Validates that mixed key types (Put, Delete, Merge, SingleDelete) work
// correctly with the compression dictionary buffered-mode code path. The
// UDI wrapper forwards OnKeyAdded() to the internal ShortenedIndexBuilder
// for all value types, ensuring current_block_first_internal_key_ is always
// populated before the buffered-block replay in MaybeEnterUnbuffered().
TEST_F(TrieIndexSSTTest, MixedKeyTypesWithCompressionDict) {
  const auto& dict_compressions = GetSupportedDictCompressions();
  if (dict_compressions.empty()) {
    ROCKSDB_GTEST_SKIP("No dictionary-capable compression available");
    return;
  }
  const auto kCompression = dict_compressions[0];

  BlockBasedTableOptions table_options;
  // Use kBinarySearchWithFirstKey so that include_first_key_ is true in the
  // ShortenedIndexBuilder, which is required to trigger the assertion in
  // GetFirstInternalKey().
  table_options.index_type =
      BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey;
  table_options.user_defined_index_factory = trie_factory_;
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;

  Options options;
  options.compression = kCompression;
  // Enable compression dictionary to trigger buffered mode in the table
  // builder. In buffered mode, OnKeyAdded() is deferred to the replay in
  // MaybeEnterUnbuffered(), which is the code path that hit the crash.
  options.compression_opts.max_dict_bytes = 4096;
  options.compression_opts.max_dict_buffer_bytes = 4096;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  // Build an SST directly using the TableBuilder interface so we have
  // fine-grained control over internal key types (Delete, Merge, etc.).
  test::StringSink* sink = new test::StringSink();
  std::unique_ptr<FSWritableFile> holder(sink);
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(holder), "test_file_name", FileOptions()));

  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  InternalKeyComparator ikc(options.comparator);
  InternalTblPropCollFactories internal_tbl_prop_coll_factories;

  const ReadOptions read_options;
  const WriteOptions write_options;
  std::unique_ptr<TableBuilder> builder(options.table_factory->NewTableBuilder(
      TableBuilderOptions(
          ioptions, moptions, read_options, write_options, ikc,
          &internal_tbl_prop_coll_factories, kCompression,
          options.compression_opts,
          TablePropertiesCollectorFactory::Context::kUnknownColumnFamily,
          "test_cf", -1 /* level */, kUnknownNewestKeyTime),
      file_writer.get()));

  // Add enough keys to fill multiple data blocks. Mix in Delete and Merge
  // entries to exercise the compression dictionary buffered-mode code path
  // with all operation types.
  constexpr int kNumKeys = 1000;
  for (int i = 0; i < kNumKeys; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "key_%06d", i);
    std::string user_key(buf);

    ValueType type;
    if (i % 10 == 5) {
      type = kTypeDeletion;
    } else if (i % 10 == 7) {
      type = kTypeMerge;
    } else {
      type = kTypeValue;
    }

    // Use decreasing sequence numbers so the ordering is valid:
    // different user keys are sorted ascending, within the same user key
    // they would be sorted by descending seqno, but all keys here are unique.
    SequenceNumber seq = static_cast<SequenceNumber>(kNumKeys - i);
    InternalKey ik(user_key, seq, type);
    // Deletions use empty value; everything else gets a payload.
    std::string value = (type == kTypeDeletion) ? "" : "value_" + user_key;
    builder->Add(ik.Encode(), value);
  }

  // Before the original fix, Finish() would crash with:
  //   Assertion `!current_block_first_internal_key_.empty()' failed.
  // during the MaybeEnterUnbuffered() replay of buffered data blocks.
  // The UDI wrapper now supports all operation types, so Finish() succeeds.
  Status s = builder->Finish();
  ASSERT_OK(s);
}

// ============================================================================
// Mixed key type tests -- verifies UDI works with Delete, Merge, etc.
// ============================================================================

TEST_F(TrieIndexSSTTest, MixedKeyTypesWithTrieUDI) {
  // Write an SST containing Puts, Deletes, and Merges using the trie UDI.
  // This validates that the UDI builder wrapper correctly handles all
  // operation types, and the resulting SST is readable via both the native
  // index and the trie UDI.
  Options options;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory = trie_factory_;
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.Open(sst_path_));

  // Write sorted entries with mixed types. SstFileWriter requires keys
  // to be added in strictly increasing order.
  ASSERT_OK(writer.Put("key_0001", "value_0001"));
  ASSERT_OK(writer.Merge("key_0002", "merge_operand_0002"));
  ASSERT_OK(writer.Put("key_0003", "value_0003"));
  ASSERT_OK(writer.Delete("key_0004"));
  ASSERT_OK(writer.Put("key_0005", "value_0005"));
  ASSERT_OK(writer.Merge("key_0006", "merge_operand_0006"));
  ASSERT_OK(writer.Delete("key_0007"));
  ASSERT_OK(writer.Put("key_0008", "value_0008"));

  ASSERT_OK(writer.Finish());

  // Verify the SST is structurally correct using the native index. The native
  // binary search index is always present alongside the UDI. DBIter hides
  // delete tombstones, so we expect 6 visible entries (4 Puts + 2 Merges).
  {
    Options read_options;
    read_options.merge_operator = MergeOperators::CreateStringAppendOperator();
    BlockBasedTableOptions read_table_options;
    read_options.table_factory.reset(
        NewBlockBasedTableFactory(read_table_options));

    SstFileReader reader(read_options);
    ASSERT_OK(reader.Open(sst_path_));

    ReadOptions ro;
    std::unique_ptr<Iterator> iter(reader.NewIterator(ro));
    iter->SeekToFirst();
    int count = 0;
    for (; iter->Valid(); iter->Next()) {
      count++;
    }
    ASSERT_OK(iter->status());
    // 4 Puts + 2 Merges visible; 2 Deletes hidden by DBIter.
    ASSERT_EQ(count, 6);
  }

  // Read with trie UDI using the logical (DB) iterator. This iterator hides
  // delete tombstones and resolves merges, so we expect 6 visible entries
  // (4 Puts + 2 Merges; 2 Deletes are hidden).
  {
    Options read_options;
    read_options.merge_operator = MergeOperators::CreateStringAppendOperator();
    BlockBasedTableOptions read_table_options;
    read_table_options.user_defined_index_factory = trie_factory_;
    read_table_options.index_mode =
        BlockBasedTableOptions::IndexMode::kStandardDefault;
    read_options.table_factory.reset(
        NewBlockBasedTableFactory(read_table_options));

    SstFileReader reader(read_options);
    ASSERT_OK(reader.Open(sst_path_));

    ReadOptions ro;
    ro.read_index = ReadOptions::ReadIndex::kPreferCustom;
    std::unique_ptr<Iterator> iter(reader.NewIterator(ro));

    // Full forward scan -- expect 6 logically visible entries.
    iter->SeekToFirst();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid())
        << "SeekToFirst invalid, status: " << iter->status().ToString();

    // Collect all visible keys.
    std::vector<std::string> visible_keys;
    for (; iter->Valid(); iter->Next()) {
      visible_keys.push_back(iter->key().ToString());
    }
    ASSERT_OK(iter->status());
    // 4 Puts + 2 Merges = 6 visible; 2 Deletes are hidden by DBIter.
    ASSERT_EQ(visible_keys.size(), 6u);
    // Verify the expected visible keys (deletes key_0004 and key_0007 skipped).
    std::vector<std::string> expected_visible = {
        "key_0001", "key_0002", "key_0003", "key_0005", "key_0006", "key_0008",
    };
    ASSERT_EQ(visible_keys, expected_visible);

    // Point seek to a merged key -- should find it.
    iter->Seek("key_0002");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_0002");

    // Point seek to a deleted key -- should advance past the tombstone.
    iter->Seek("key_0004");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_0005");

    // Point seek to the last deleted key -- should advance to key_0008.
    iter->Seek("key_0007");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_0008");
  }
}

TEST_F(TrieIndexSSTTest, LargeMixedKeyTypesWithTrieUDI) {
  // Larger test with many keys of different types to exercise multiple data
  // blocks and verify the trie index handles block boundaries correctly.
  Options options;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory = trie_factory_;
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;
  // Use small block size to force many data blocks, stressing the index.
  table_options.block_size = 128;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.Open(sst_path_));

  const int kNumKeys = 500;
  // Track all keys and their types, plus the subset visible via DBIter.
  std::vector<std::pair<std::string, char>> all_entries;
  std::vector<std::string>
      visible_keys;  // Keys visible via DBIter (non-delete)
  all_entries.reserve(kNumKeys);

  for (int i = 0; i < kNumKeys; i++) {
    char key_buf[32];
    snprintf(key_buf, sizeof(key_buf), "key_%06d", i);
    std::string key(key_buf);

    // Distribute types: 60% Put, 20% Delete, 20% Merge.
    if (i % 5 == 0) {
      ASSERT_OK(writer.Delete(key));
      all_entries.emplace_back(key, 'D');
      // Deletes are hidden by DBIter -- not added to visible_keys.
    } else if (i % 5 == 1) {
      char val_buf[32];
      snprintf(val_buf, sizeof(val_buf), "merge_%06d", i);
      ASSERT_OK(writer.Merge(key, Slice(val_buf)));
      all_entries.emplace_back(key, 'M');
      visible_keys.push_back(key);
    } else {
      char val_buf[32];
      snprintf(val_buf, sizeof(val_buf), "value_%06d", i);
      ASSERT_OK(writer.Put(key, Slice(val_buf)));
      all_entries.emplace_back(key, 'P');
      visible_keys.push_back(key);
    }
  }
  ASSERT_OK(writer.Finish());

  // Verify visible entries (Puts + Merges) exist using the native index via
  // the logical (DB) iterator without UDI. The native binary search index is
  // always present alongside the UDI, so we can verify the SST structure
  // with it. DBIter hides delete tombstones, so only visible entries counted.
  {
    Options read_options;
    read_options.merge_operator = MergeOperators::CreateStringAppendOperator();
    BlockBasedTableOptions read_table_options;
    read_options.table_factory.reset(
        NewBlockBasedTableFactory(read_table_options));

    SstFileReader reader(read_options);
    ASSERT_OK(reader.Open(sst_path_));

    ReadOptions ro;
    std::unique_ptr<Iterator> iter(reader.NewIterator(ro));
    iter->SeekToFirst();
    int count = 0;
    for (; iter->Valid(); iter->Next()) {
      count++;
    }
    ASSERT_OK(iter->status());
    // Visible entries = Puts + Merges (deletes hidden by DBIter).
    ASSERT_EQ(count, static_cast<int>(visible_keys.size()));
  }

  // Read with trie UDI via the logical (DB) iterator. Delete tombstones are
  // hidden, so we iterate only the visible keys (Puts + Merges).
  {
    Options read_options;
    read_options.merge_operator = MergeOperators::CreateStringAppendOperator();
    BlockBasedTableOptions read_table_options;
    read_table_options.user_defined_index_factory = trie_factory_;
    read_table_options.index_mode =
        BlockBasedTableOptions::IndexMode::kStandardDefault;
    read_options.table_factory.reset(
        NewBlockBasedTableFactory(read_table_options));

    SstFileReader reader(read_options);
    ASSERT_OK(reader.Open(sst_path_));

    ReadOptions ro;
    ro.read_index = ReadOptions::ReadIndex::kPreferCustom;
    std::unique_ptr<Iterator> iter(reader.NewIterator(ro));

    // Full forward scan -- only visible keys should appear.
    iter->SeekToFirst();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    int count = 0;
    for (; iter->Valid(); iter->Next()) {
      ASSERT_LT(count, static_cast<int>(visible_keys.size()))
          << "Too many visible keys";
      ASSERT_EQ(iter->key().ToString(), visible_keys[count])
          << "Visible key mismatch at " << count;
      count++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(count, static_cast<int>(visible_keys.size()));

    // Point seek to every 10th visible key to verify trie index correctness.
    for (int i = 0; i < static_cast<int>(visible_keys.size()); i += 10) {
      iter->Seek(visible_keys[i]);
      ASSERT_TRUE(iter->Valid()) << "Seek failed for " << visible_keys[i];
      ASSERT_EQ(iter->key().ToString(), visible_keys[i]);
    }

    // Point seek to deleted keys -- should advance past the tombstone.
    for (int i = 0; i < kNumKeys; i++) {
      if (all_entries[i].second == 'D') {
        iter->Seek(all_entries[i].first);
        // A deleted key should either advance to the next visible key or
        // reach end-of-file if it's the last key.
        if (iter->Valid()) {
          ASSERT_GT(iter->key().ToString(), all_entries[i].first)
              << "Seek to deleted key " << all_entries[i].first
              << " should have advanced past it";
        }
        ASSERT_OK(iter->status());
      }
    }
  }
}

TEST_F(TrieIndexFactoryTest, WrapperNextAndGetResultReturnsInternalKey) {
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));

  // Build a 3-block index: separators "a", "b", "c".
  std::string scratch;
  {
    IndexFactoryBuilder::BlockHandle h{0, 100};
    Slice next("b");
    builder->AddIndexEntry(Slice("a"), &next, h, &scratch, EntryCtx(100, 100));
  }
  {
    IndexFactoryBuilder::BlockHandle h{100, 100};
    Slice next("c");
    builder->AddIndexEntry(Slice("b"), &next, h, &scratch, EntryCtx(100, 100));
  }
  {
    IndexFactoryBuilder::BlockHandle h{200, 100};
    builder->AddIndexEntry(Slice("c"), nullptr, h, &scratch,
                           EntryCtx(100, 100));
  }

  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));

  std::unique_ptr<IndexFactoryReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));

  ReadOptions ro;
  auto udi_iter = reader->NewIterator(ro);
  // Wrap the UDI iterator in the adapter that converts to InternalIterator.
  IndexFactoryIteratorWrapper wrapper(std::move(udi_iter));

  // Seek to "a" -- constructs an internal key from user key "a".
  InternalKey seek_ikey;
  seek_ikey.Set(Slice("a"), kMaxSequenceNumber, kValueTypeForSeek);
  wrapper.Seek(Slice(*seek_ikey.const_rep()));
  ASSERT_TRUE(wrapper.Valid());
  ASSERT_OK(wrapper.status());

  // wrapper.key() must be an internal key: user_key("a") + 8 bytes suffix.
  Slice wrapper_key = wrapper.key();
  ASSERT_EQ(wrapper_key.size(), 1u + 8u)
      << "key() should be internal key (user_key + 8-byte footer)";
  ParsedInternalKey parsed;
  ASSERT_OK(ParseInternalKey(wrapper_key, &parsed, /*log_err_key=*/false));
  EXPECT_EQ(parsed.user_key.ToString(), "a");
  EXPECT_EQ(parsed.type, ValueType::kTypeValue);

  // Now test NextAndGetResult -- this is the method we fixed.
  IterateResult result;
  bool valid = wrapper.NextAndGetResult(&result);
  ASSERT_TRUE(valid);
  ASSERT_OK(wrapper.status());

  // result.key must be an internal key ("b" + 8-byte suffix), not a raw
  // user key ("b" alone). Returning a raw user key would cause the
  // BlockBasedTableIterator to misinterpret the key format.
  ASSERT_EQ(result.key.size(), 1u + 8u)
      << "NextAndGetResult key must be internal key (user_key + 8-byte "
         "footer), got size "
      << result.key.size();
  ASSERT_OK(ParseInternalKey(result.key, &parsed, /*log_err_key=*/false));
  EXPECT_EQ(parsed.user_key.ToString(), "b");
  EXPECT_EQ(parsed.type, ValueType::kTypeValue);

  // result.key must match wrapper.key() -- both views of the current key.
  EXPECT_EQ(result.key, wrapper.key());

  // Advance again and verify. The last block's separator is "c"
  // (the actual last key, no successor shortening for last block).
  valid = wrapper.NextAndGetResult(&result);
  ASSERT_TRUE(valid);
  ASSERT_OK(wrapper.status());
  ASSERT_EQ(result.key.size(), 1u + 8u);
  ASSERT_OK(ParseInternalKey(result.key, &parsed, /*log_err_key=*/false));
  EXPECT_EQ(parsed.user_key.ToString(), "c");
  EXPECT_EQ(result.key, wrapper.key());

  // One more advance -- past end.
  valid = wrapper.NextAndGetResult(&result);
  EXPECT_FALSE(valid);
}

// Runs on separators of different lengths ("ab" and "b") map every block to
// its own handle in key order, and Seek picks the right block of each run by
// seqno. In each run, the second block's last key is a prefix of the next
// block's first key ("ab" of "abc", "b" of "ba"), so FindShortestSeparator
// leaves it unchanged and the block joins the run.
TEST_F(TrieIndexFactoryTest, RunsOnPrefixKeysKeepBlockOrder) {
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));

  std::string scratch;
  Slice sep;

  // Block 0: last="ab", next="ab" (same-key boundary)
  // -> sep="ab", same_user_key=true, seqno=500
  {
    IndexFactoryBuilder::BlockHandle h{0, 100};
    Slice next("ab");
    sep = builder->AddIndexEntry(Slice("ab"), &next, h, &scratch,
                                 EntryCtx(500, 0));
    ASSERT_EQ(scratch, "ab") << "Block 0 separator";
  }
  // Block 1: last="ab", next="abc" (prefix -- FindShortestSeparator no-op)
  // -> sep="ab", edge-case match with prev sep, same_user_key=true, seqno=400
  {
    IndexFactoryBuilder::BlockHandle h{100, 100};
    Slice next("abc");
    sep = builder->AddIndexEntry(Slice("ab"), &next, h, &scratch,
                                 EntryCtx(400, 0));
    ASSERT_EQ(scratch, "ab") << "Block 1 separator (prefix edge case)";
  }
  // Block 2: last="abc", next="b"
  // FindShortestSeparator("abc","b"): diff_index=0, 'a' vs 'b',
  // limit.size()-1=0 so fallback: increment start[1] 'b'->'c' -> "ac"
  // -> sep="ac", different key, tag 0
  {
    IndexFactoryBuilder::BlockHandle h{200, 100};
    Slice next("b");
    sep = builder->AddIndexEntry(Slice("abc"), &next, h, &scratch,
                                 EntryCtx(100, 100));
    ASSERT_EQ(scratch, "ac") << "Block 2 separator";
  }
  // Block 3: last="b", next="b" (same-key boundary)
  // -> sep="b", same_user_key=true, seqno=300
  {
    IndexFactoryBuilder::BlockHandle h{300, 100};
    Slice next("b");
    sep = builder->AddIndexEntry(Slice("b"), &next, h, &scratch,
                                 EntryCtx(300, 0));
    ASSERT_EQ(scratch, "b") << "Block 3 separator";
  }
  // Block 4: last="b", next="ba" (prefix -- FindShortestSeparator no-op)
  // -> sep="b", edge-case match with prev sep, same_user_key=true, seqno=200
  {
    IndexFactoryBuilder::BlockHandle h{400, 100};
    Slice next("ba");
    sep = builder->AddIndexEntry(Slice("b"), &next, h, &scratch,
                                 EntryCtx(200, 0));
    ASSERT_EQ(scratch, "b") << "Block 4 separator (prefix edge case)";
  }
  // Block 5: last="ba", next="d"
  // FindShortestSeparator("ba","d"): diff_index=0, 'b' vs 'd',
  // start_byte+1='c' < 'd' -> increment and truncate -> "c"
  // -> sep="c", different key, tag 0
  {
    IndexFactoryBuilder::BlockHandle h{500, 100};
    Slice next("d");
    sep = builder->AddIndexEntry(Slice("ba"), &next, h, &scratch,
                                 EntryCtx(100, 100));
    ASSERT_EQ(scratch, "c") << "Block 5 separator";
  }
  // Block 6: last="d", next=null (last block, no successor shortening)
  // -> sep="d", different from prev "c", last block keeps its tag, seqno=100
  {
    IndexFactoryBuilder::BlockHandle h{600, 100};
    sep = builder->AddIndexEntry(Slice("d"), nullptr, h, &scratch,
                                 EntryCtx(100, 100));
  }

  // After Finish(), separators in key order with their blocks
  // {offset, tag seqno}:
  //   "ab": {0, 500}, {100, 400}
  //   "ac": {200, 0}
  //   "b":  {300, 300}, {400, 200}
  //   "c":  {500, 0}
  //   "d":  {600, 100}

  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));

  std::unique_ptr<IndexFactoryReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));

  ReadOptions ro;
  auto iter = reader->NewIterator(ro);
  IterateResult result;

  // --- Full forward scan: verify all block offsets in key order ---
  // Expected offsets: ab 0, ab 100, ac 200, b 300, b 400, c 500, d 600
  ASSERT_OK(iter->SeekToFirstAndGetResult(&result));
  EXPECT_EQ(result.key.ToString(), "ab");
  EXPECT_EQ(iter->value().offset, 0u) << "SeekToFirst: ab first block";

  ASSERT_OK(iter->NextAndGetResult(&result));
  EXPECT_EQ(result.key.ToString(), "ab");
  EXPECT_EQ(iter->value().offset, 100u) << "Next: ab second block";

  ASSERT_OK(iter->NextAndGetResult(&result));
  EXPECT_EQ(result.key.ToString(), "ac");
  EXPECT_EQ(iter->value().offset, 200u) << "Next: ac";

  ASSERT_OK(iter->NextAndGetResult(&result));
  EXPECT_EQ(result.key.ToString(), "b");
  EXPECT_EQ(iter->value().offset, 300u) << "Next: b first block";

  ASSERT_OK(iter->NextAndGetResult(&result));
  EXPECT_EQ(result.key.ToString(), "b");
  EXPECT_EQ(iter->value().offset, 400u) << "Next: b second block";

  ASSERT_OK(iter->NextAndGetResult(&result));
  EXPECT_EQ(result.key.ToString(), "c");
  EXPECT_EQ(iter->value().offset, 500u) << "Next: c";

  ASSERT_OK(iter->NextAndGetResult(&result));
  EXPECT_EQ(result.key.ToString(), "d");
  EXPECT_EQ(iter->value().offset, 600u) << "Next: d";

  ASSERT_OK(iter->NextAndGetResult(&result));
  EXPECT_EQ(result.bound_check_result, IterBoundCheck::kUnknown) << "past end";

  // --- Seek-based tests: verify each run's tags select the right block ---

  // "ab" first block: Seek("ab", kMax) -> offset=0
  ASSERT_OK(iter->SeekAndGetResult(Slice("ab"), &result,
                                   SeekCtx(kMaxSequenceNumber)));
  EXPECT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  EXPECT_EQ(iter->value().offset, 0u) << "Seek(ab,kMax): ab first block";

  // "ab" second block: Seek("ab", 400) -> offset=100, the first block of the
  // run whose tag is at most 400.
  ASSERT_OK(iter->SeekAndGetResult(Slice("ab"), &result, SeekCtx(400)));
  EXPECT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  EXPECT_EQ(iter->value().offset, 100u) << "Seek(ab,400): ab second block";

  // "ab" advance past run: Seek("ab", 50) -> advances to "ac" at offset=200
  // Both tags of the run are larger than 50, so Seek moves to the next
  // separator.
  ASSERT_OK(iter->SeekAndGetResult(Slice("ab"), &result, SeekCtx(50)));
  EXPECT_EQ(result.key.ToString(), "ac")
      << "Seek(ab,50): should advance past ab run";
  EXPECT_EQ(iter->value().offset, 200u) << "Seek(ab,50): expected ac (200)";

  // "b" first block: Seek("b", kMax) -> offset=300
  ASSERT_OK(
      iter->SeekAndGetResult(Slice("b"), &result, SeekCtx(kMaxSequenceNumber)));
  EXPECT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  EXPECT_EQ(iter->value().offset, 300u) << "Seek(b,kMax): b first block";

  // "b" second block: Seek("b", 200) -> offset=400, the first block of the
  // run whose tag is at most 200. Seek must not move on to "c".
  ASSERT_OK(iter->SeekAndGetResult(Slice("b"), &result, SeekCtx(200)));
  EXPECT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  EXPECT_EQ(result.key.ToString(), "b")
      << "Seek(b,200): must stay on b, not advance";
  EXPECT_EQ(iter->value().offset, 400u) << "Seek(b,200): b second block";

  // "b" advance past run: Seek("b", 50) -> advances to "c" at offset=500
  ASSERT_OK(iter->SeekAndGetResult(Slice("b"), &result, SeekCtx(50)));
  EXPECT_EQ(result.key.ToString(), "c")
      << "Seek(b,50): should advance past b run";
  EXPECT_EQ(iter->value().offset, 500u) << "Seek(b,50): expected c (500)";
}

TEST_F(TrieIndexFactoryTest, SeekToLastPrefixKeyOnlyTrie) {
  // SeekToLast and Prev when one separator is a prefix of the other. The
  // separators are "a" and "ab", so SeekToLast must land on "ab", the
  // largest separator, and Prev from there must land on "a".
  auto ctx = BuildTrieAndGetIterator({
      {"a", "ab", 0, 500, 0, 0},
      {"ab", "", 500, 500, 0, 0},
  });

  IterateResult result;

  // SeekToLast should land on "ab".
  ASSERT_OK(ctx.iter->SeekToLastAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(result.key.ToString(), "ab");

  // Prev should land on "a" (a prefix of "ab").
  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(result.key.ToString(), "a");

  // Prev again should be past the beginning.
  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_NE(result.bound_check_result, IterBoundCheck::kInbound);
}

TEST_F(TrieIndexFactoryTest, SeekToLastAndPrevWithPrefixKeys) {
  // Verifies SeekToLast and Prev when separators are prefixes of later ones.
  // The last keys "a" and "ab" are prefixes of the next block's first key,
  // so they stay unshortened: the separators are "a", "ab", "ac" and "b".
  auto ctx = BuildTrieAndGetIterator({
      {"a", "ab", 0, 500, 0, 0},
      {"ab", "abc", 500, 500, 0, 0},
      {"abc", "b", 1000, 500, 0, 0},
      {"b", "", 1500, 500, 0, 0},
  });

  IterateResult result;

  // SeekToLast should land on "b" (the largest separator).
  ASSERT_OK(ctx.iter->SeekToLastAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(result.key.ToString(), "b");

  // Full reverse scan. The trie stores shortened separators (via
  // FindShortestSeparator), so the exact keys depend on the comparator.
  std::vector<std::string> rev_keys;
  ASSERT_OK(ctx.iter->SeekToLastAndGetResult(&result));
  while (result.bound_check_result == IterBoundCheck::kInbound) {
    rev_keys.push_back(result.key.ToString());
    ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  }
  ASSERT_EQ(rev_keys.size(), 4u);

  // Full forward scan should match.
  std::vector<std::string> fwd_keys;
  ASSERT_OK(ctx.iter->SeekToFirstAndGetResult(&result));
  while (result.bound_check_result == IterBoundCheck::kInbound) {
    fwd_keys.push_back(result.key.ToString());
    ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  }
  std::vector<std::string> expected_rev(fwd_keys.rbegin(), fwd_keys.rend());
  ASSERT_EQ(rev_keys, expected_rev);
}

TEST_F(TrieIndexFactoryTest, LastBlockSeekWithRealSeqno) {
  // Verifies that seeking with a real seqno (not kMaxSequenceNumber) on the
  // last block's separator correctly finds the block. The last block stores
  // the real tag of the last key, so a seek with any seqno <= that tag
  // stays on this block (matching the standard index which stores the full
  // internal key for the last block's separator).
  //
  // The last block stores the real tag of its last key (not a sentinel),
  // so seeks with real seqnos correctly stay on this block.
  auto ctx = BuildTrieAndGetIterator({
      {"apple", "cherry", 0, 1000, 100, 50},
      {"cherry", "", 1000, 1000, 50, 0},
  });

  // Seek "cherry" with kMaxSequenceNumber -- should find block at offset 1000.
  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(ctx.iter.get(), Slice("cherry"),
                                           kMaxSequenceNumber, 1000));

  // Seek "cherry" with a real seqno (200) -- the last block's tag has
  // seqno=50. In internal key order, 200 > 50 means target is SMALLER (higher
  // seqno = smaller key). So target <= the block's last key -> should stay on
  // this block.
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("cherry"), 200, 1000));

  // Seek "cherry" with seqno=50 (equal to the block's tag) -- should stay.
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("cherry"), 50, 1000));

  // Seek "cherry" with seqno=10 (less than the tag's seqno=50) -- target is
  // LARGER in internal key order. target > the block's last key -> should
  // advance past.
  // But this is the last block -- no next block -> past end.
  IterateResult result;
  ASSERT_OK(ctx.iter->SeekAndGetResult(Slice("cherry"), &result, SeekCtx(10)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);
}

TEST_F(TrieIndexFactoryTest, IntermediateNonBoundarySeparatorNoAdvance) {
  // Verifies that seeking with any seqno on an intermediate non-boundary
  // separator does NOT advance past it. Intermediate non-boundary separators
  // store tag=0 (sentinel), making target_tag < 0 always false -> stays.
  // This matches the standard index's index_key_is_user_key=true mode where
  // equal user keys always match without seqno comparison.
  auto ctx = BuildTrieAndGetIterator({
      {"apple", "cherry", 0, 1000, 100, 50},
      {"cherry", "elderberry", 1000, 1000, 50, 1},
      {"elderberry", "", 2000, 1000, 1, 0},
  });

  // FindShortestSeparator("apple", "cherry") = "b" (shortened).
  // Seeking for "apple" should find block 0 regardless of seqno.
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("apple"), kMaxSequenceNumber, 0));
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("apple"), 1, 0));
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("apple"), 0, 0));

  // Seeking for "cherry" should find block 1 regardless of seqno.
  // The separator between block 0 and 1 is a shortened key (non-boundary).
  // "cherry" matches block 1's separator -- should stay with any seqno.
  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(ctx.iter.get(), Slice("cherry"),
                                           kMaxSequenceNumber, 1000));
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("cherry"), 1, 1000));
}

TEST_F(TrieIndexFactoryTest, NonBoundarySeparatorSeekWhenShorteningFails) {
  // Reproducer for GitHub issue #14561: when FindShortestSeparator cannot
  // shorten the separator (e.g., "acc" -> "acd" stays "acc"), the trie lands
  // on separator "acc" with tag=0 (non-boundary sentinel). The post-seek
  // correction must NOT advance past it regardless of the target seqno.
  //
  // The key arrangement:
  //   Block 0: separator = FindShortestSeparator("aaa","acc") = "ab"
  //   Block 1: separator = FindShortestSeparator("acc","acd") = "acc"
  //            (shortening fails because 'c'+1='d' is not < 'd')
  //   Block 2: last block, separator = "acd", real tag
  auto ctx = BuildTrieAndGetIterator({
      {"aaa", "acc", 0, 1000, 100, 50},
      {"acc", "acd", 1000, 1000, 50, 40},
      {"acd", "", 2000, 1000, 40, 0},
  });

  // Seek for "acc" with any seqno should find block 1 at offset 1000.
  // The separator "acc" has tag=0 (non-boundary sentinel), so
  // target_packed < 0 is always false -> no advancement occurs.
  //
  // Non-boundary separators use tag=0 (sentinel). For unsigned uint64_t,
  // target_packed < 0 is always false, so no advancement occurs. This
  // matches the standard index's user-key-only comparison semantics.
  // If the sentinel were kMaxSequenceNumber instead, target_packed < kMax
  // would be true for all real seqnos, causing incorrect advancement.
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("acc"), kMaxSequenceNumber, 1000));
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("acc"), 50, 1000));
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("acc"), 1, 1000));
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("acc"), 0, 1000));

  // Full forward scan verifies all blocks are reachable.
  AssertFullForwardScan(ctx.iter.get(), Slice("aaa"), {0, 1000, 2000});
}

TEST_F(TrieIndexFactoryTest, PrevWithinRun) {
  // Exercises PrevAndGetResult into and within a run: Prev from the block
  // after a run lands on the run's last block, and Prev within the run walks
  // back to its first block.
  auto ctx = BuildTrieAndGetIterator({
      // Blocks 0-2: user key "key" spans 3 blocks. Blocks 0-1 share
      // separator "key" (a run); block 2 gets separator "l".
      {"key", "key", 0, 1000, 300, 200},
      {"key", "key", 1000, 1000, 200, 100},
      {"key", "zzz", 2000, 1000, 100, 50},
      // Block 3: "zzz" last block.
      {"zzz", "", 3000, 1000, 50, 0},
  });

  IterateResult result;

  // Seek to "key"|100 -- both blocks of the "key" run have larger tags (300,
  // 200), so Seek moves to the next separator, "l" (block 2, offset 2000).
  ASSERT_OK(ctx.iter->SeekAndGetResult(Slice("key"), &result, SeekCtx(100)));
  ASSERT_EQ(ctx.iter->value().offset, 2000u);

  // Prev should go back to the last block of the "key" run (offset 1000).
  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 1000u);

  // Prev again: first block of the run (offset 0).
  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 0u);

  // Prev again: past the beginning.
  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_NE(result.bound_check_result, IterBoundCheck::kInbound);
}

TEST_F(TrieIndexFactoryTest, SeekToLastWithRun) {
  // SeekToLast when the last separator has a run of several blocks should
  // position at the last block of the run.
  auto ctx = BuildTrieAndGetIterator({
      // Block 0: "aaa" standalone.
      {"aaa", "zzz", 0, 1000, 100, 50},
      // Block 1-3: "zzz" spans 3 blocks, a run on the last separator.
      {"zzz", "zzz", 1000, 1000, 50, 30},
      {"zzz", "zzz", 2000, 1000, 30, 10},
      {"zzz", "", 3000, 1000, 10, 0},
  });

  IterateResult result;

  // SeekToLast should land on the last block of the run (offset 3000).
  ASSERT_OK(ctx.iter->SeekToLastAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(result.key.ToString(), "zzz");
  ASSERT_EQ(ctx.iter->value().offset, 3000u);

  // Prev within the run: offset 2000.
  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 2000u);

  // Prev within the run: offset 1000.
  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 1000u);

  // Prev to the previous separator: block 0 at offset 0.
  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 0u);

  // Prev past beginning.
  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_NE(result.bound_check_result, IterBoundCheck::kInbound);
}

TEST_F(TrieIndexFactoryTest, PrevAcrossSameUserKeyBlocks) {
  // Prev from a standalone block steps back one block at a time, including
  // across the two blocks of user key "aaa". Only block 0 keeps separator
  // "aaa"; block 1, the last "aaa" block, gets the shortened separator "b".
  auto ctx = BuildTrieAndGetIterator({
      // Block 0-1: "aaa" spans 2 blocks.
      {"aaa", "aaa", 0, 1000, 200, 100},
      {"aaa", "mmm", 1000, 1000, 100, 50},
      // Block 2: "mmm" standalone.
      {"mmm", "zzz", 2000, 1000, 50, 10},
      // Block 3: "zzz" standalone.
      {"zzz", "", 3000, 1000, 10, 0},
  });

  IterateResult result;

  // Seek to "zzz" (offset 3000).
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("zzz"), kMaxSequenceNumber, 3000));

  // Prev to "mmm" (offset 2000).
  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 2000u);

  // Prev to block 1 (offset 1000), the last "aaa" block, not block 0.
  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 1000u);

  // Prev to block 0: offset 0.
  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 0u);

  // Prev past beginning.
  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_NE(result.bound_check_result, IterBoundCheck::kInbound);
}

TEST_F(TrieIndexFactoryTest, SeekToFirstOnEmptyTrie) {
  // SeekToFirstAndGetResult on an empty trie should return kUnknown.
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();
  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));
  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));
  std::unique_ptr<IndexFactoryReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));
  ReadOptions ro;
  auto iter = reader->NewIterator(ro);

  IterateResult result;
  ASSERT_OK(iter->SeekToFirstAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);
}

TEST_F(TrieIndexFactoryTest, SeekToLastOnEmptyTrie) {
  // SeekToLastAndGetResult on an empty trie should return kUnknown.
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();
  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));
  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));
  std::unique_ptr<IndexFactoryReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));
  ReadOptions ro;
  auto iter = reader->NewIterator(ro);

  IterateResult result;
  ASSERT_OK(iter->SeekToLastAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);
}

TEST_F(TrieIndexFactoryTest, RunExhaustionThenScanThroughRun) {
  // When SeekAndGetResult moves past a run because every tag in it is
  // larger than the target, verify that subsequent Next() calls correctly
  // walk through a later run.
  //
  // Layout:
  //   Blocks 0-1: "key" same-key run (2 blocks)
  //   Block 2: separator "l" (shortened "key"->"zzz"), standalone
  //   Blocks 3-4: "zzz" same-key run (2 blocks)
  auto ctx = BuildTrieAndGetIterator({
      {"key", "key", 0, 1000, 300, 200},
      {"key", "key", 1000, 1000, 200, 100},
      {"key", "zzz", 2000, 1000, 100, 50},
      {"zzz", "zzz", 3000, 1000, 50, 30},
      {"zzz", "", 4000, 1000, 30, 0},
  });

  IterateResult result;

  // Seek "key"|1 -- seqno=1 is below both tags of the "key" run (300, 200),
  // so Seek moves to the next separator.
  // FindShortestSeparator("key", "zzz") = "l", so that separator is "l".
  ASSERT_OK(ctx.iter->SeekAndGetResult(Slice("key"), &result, SeekCtx(1)));
  ASSERT_EQ(result.key.ToString(), "l");
  ASSERT_EQ(ctx.iter->value().offset, 2000u);

  // Next advances to the first "zzz" block (offset 3000).
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.key.ToString(), "zzz");
  ASSERT_EQ(ctx.iter->value().offset, 3000u);

  // Next advances to the second "zzz" block (offset 4000).
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(ctx.iter->value().offset, 4000u);

  // Next past end.
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);
}

TEST_F(TrieIndexFactoryTest, AllScansExhaustedThenSeek) {
  // After all scan ranges are exhausted (current_scan_idx_ >= size), any
  // subsequent seek should return kOutOfBound.
  auto ctx = BuildTrieAndGetIterator({
      {"az", "c", 0, 500, 0, 0},
      {"cz", "e", 1000, 500, 0, 0},
      {"ez", "", 2000, 500, 0, 0},
  });

  // Two non-overlapping scan ranges: [a,b) and [c,d).
  ScanOptions scans[2] = {
      ScanOptions(Slice("a"), Slice("b")),
      ScanOptions(Slice("c"), Slice("d")),
  };
  ctx.iter->Prepare(scans, 2);

  IterateResult result;

  // Seek "a" -- within first scan range, kInbound.
  ASSERT_OK(ctx.iter->SeekAndGetResult(Slice("a"), &result,
                                       SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);

  // Seek "c" -- advances to second scan range, kInbound.
  ASSERT_OK(ctx.iter->SeekAndGetResult(Slice("c"), &result,
                                       SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);

  // Seek "e" -- past both scan ranges, kOutOfBound.
  ASSERT_OK(ctx.iter->SeekAndGetResult(Slice("e"), &result,
                                       SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kOutOfBound);
}

// ============================================================================
// Parallel compression protocol tests
// ============================================================================

TEST_F(TrieIndexFactoryTest, SupportsParallelAddEntry) {
  // The trie builder must opt into the parallel protocol so the table
  // builder routes EmitBlockForParallel through PrepareAddEntry /
  // FinishAddEntry instead of forcing a serial fallback.
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();
  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));
  EXPECT_TRUE(builder->SupportsParallelAddEntry());

  auto prep = builder->CreatePreparedAddEntry();
  EXPECT_NE(prep, nullptr);
}

TEST_F(TrieIndexFactoryTest, ParallelMatchesSerialOutput) {
  // Drive identical inputs through AddIndexEntry (serial) and through
  // CreatePreparedAddEntry / PrepareAddEntry / FinishAddEntry (parallel).
  // The two paths must produce byte-identical serialized index blocks
  // and identical iterator behavior -- otherwise parallel compression
  // with a trie index produces silently divergent SSTs.
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();

  // Mix of distinct user keys, same-user-key boundaries (where seqno
  // disambiguation matters), and a final block.
  struct Block {
    std::string last_key;
    std::string next_key;  // empty = last block (nullptr)
    uint64_t offset;
    uint64_t size;
    SequenceNumber last_seq;
    SequenceNumber first_seq;
  };
  const std::vector<Block> blocks = {
      {"aaa", "bbb", 0, 100, 100, 90},
      {"bbb", "bbb", 105, 100, 80, 70},  // same-user-key boundary
      {"bbb", "ccc", 210, 100, 60, 50},
      {"ccc", "ddd", 315, 100, 55, 45},
      // Four blocks split inside user key "kkk". The first three all get
      // separator "kkk" and a tag each, forming a run of three blocks,
      // which both paths must build identically. The fourth gets "l".
      {"kkk", "kkk", 420, 100, 44, 40},
      {"kkk", "kkk", 525, 100, 38, 34},
      {"kkk", "kkk", 630, 100, 32, 28},
      {"kkk", "lll", 735, 100, 26, 22},
      {"lll", "", 840, 100, 20, 0},  // last block
  };

  // --- Serial build ---
  std::unique_ptr<IndexFactoryBuilder> serial;
  ASSERT_OK(factory_->NewBuilder(option, serial));
  std::string scratch_serial;
  for (const auto& b : blocks) {
    IndexFactoryBuilder::BlockHandle h{b.offset, b.size};
    if (!b.next_key.empty()) {
      Slice next(b.next_key);
      serial->AddIndexEntry(Slice(b.last_key), &next, h, &scratch_serial,
                            EntryCtx(b.last_seq, b.first_seq));
    } else {
      serial->AddIndexEntry(Slice(b.last_key), nullptr, h, &scratch_serial,
                            EntryCtx(b.last_seq, 0));
    }
  }
  Slice serial_contents;
  ASSERT_OK(serial->Finish(&serial_contents));

  // --- Parallel build (Prepare on emit, Finish on writer) ---
  std::unique_ptr<IndexFactoryBuilder> par;
  ASSERT_OK(factory_->NewBuilder(option, par));
  ASSERT_TRUE(par->SupportsParallelAddEntry());
  std::vector<std::unique_ptr<IndexFactoryBuilder::PreparedAddEntry>> prepared;
  prepared.reserve(blocks.size());
  for (const auto& b : blocks) {
    auto p = par->CreatePreparedAddEntry();
    ASSERT_NE(p, nullptr);
    if (!b.next_key.empty()) {
      Slice next(b.next_key);
      par->PrepareAddEntry(Slice(b.last_key), &next,
                           EntryCtx(b.last_seq, b.first_seq), p.get());
    } else {
      par->PrepareAddEntry(Slice(b.last_key), nullptr, EntryCtx(b.last_seq, 0),
                           p.get());
    }
    prepared.push_back(std::move(p));
  }
  std::string scratch_parallel;
  for (size_t i = 0; i < blocks.size(); i++) {
    IndexFactoryBuilder::BlockHandle h{blocks[i].offset, blocks[i].size};
    par->FinishAddEntry(h, prepared[i].get(), &scratch_parallel,
                        /*skip_delta_encoding=*/false);
  }
  Slice parallel_contents;
  ASSERT_OK(par->Finish(&parallel_contents));

  EXPECT_EQ(serial_contents.ToString(), parallel_contents.ToString());

  // Verify the parallel output is also functionally correct -- round-trip
  // a few seeks through it.
  std::unique_ptr<IndexFactoryReader> reader;
  ASSERT_OK(factory_->NewReader(option, parallel_contents, reader));
  ReadOptions ro;
  auto iter = reader->NewIterator(ro);
  IterateResult result;
  ASSERT_OK(iter->SeekAndGetResult(Slice("aaa"), &result,
                                   SeekCtx(kMaxSequenceNumber)));
  EXPECT_EQ(iter->value().offset, 0u);
  ASSERT_OK(iter->SeekAndGetResult(Slice("lll"), &result,
                                   SeekCtx(kMaxSequenceNumber)));
  EXPECT_EQ(iter->value().offset, 840u);
  // Land inside the "kkk" run: the newest block in the run wins.
  ASSERT_OK(iter->SeekAndGetResult(Slice("kkk"), &result,
                                   SeekCtx(kMaxSequenceNumber)));
  EXPECT_EQ(iter->value().offset, 420u);
}

TEST_F(TrieIndexFactoryTest, ParallelPipelineOverlapsPrepareAndFinish) {
  // ParallelMatchesSerialOutput drives Prepare and Finish from one thread.
  // The real table builder calls PrepareAddEntry on the emit thread while
  // the BG writer thread is still finishing earlier blocks, which is the
  // arrangement the disjoint-state contract in index_factory.h is about.
  // Reproduce that overlap and require the output to match a serial build.
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();

  struct Block {
    std::string last_key;
    std::string next_key;  // empty = last block (nullptr)
    uint64_t offset;
    SequenceNumber last_seq;
    SequenceNumber first_seq;
  };
  // Ascending keys with periodic same-user-key runs, so the concurrent run
  // covers both the sentinel-tag and real-tag paths.
  std::vector<Block> blocks;
  const int kNumKeys = 200;
  uint64_t offset = 0;
  for (int i = 0; i < kNumKeys; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "key%06d", i);
    std::string key(buf);
    // Every 5th key is split across two blocks.
    const bool split = (i % 5 == 0);
    snprintf(buf, sizeof(buf), "key%06d", i + 1);
    std::string next(buf);
    if (split) {
      blocks.push_back({key, key, offset, 1000u - 2 * i, 999u - 2 * i});
      offset += 105;
    }
    blocks.push_back({key, next, offset, 900u - 2 * i, 899u - 2 * i});
    offset += 105;
  }
  blocks.back().next_key.clear();  // final block has no successor

  auto build_serial = [&]() {
    std::unique_ptr<IndexFactoryBuilder> b;
    EXPECT_OK(factory_->NewBuilder(option, b));
    std::string scratch;
    for (const auto& blk : blocks) {
      IndexFactoryBuilder::BlockHandle h{blk.offset, 100};
      if (!blk.next_key.empty()) {
        Slice next(blk.next_key);
        b->AddIndexEntry(Slice(blk.last_key), &next, h, &scratch,
                         EntryCtx(blk.last_seq, blk.first_seq));
      } else {
        b->AddIndexEntry(Slice(blk.last_key), nullptr, h, &scratch,
                         EntryCtx(blk.last_seq, 0));
      }
    }
    Slice contents;
    EXPECT_OK(b->Finish(&contents));
    return contents.ToString();
  };
  const std::string serial_contents = build_serial();

  // --- Concurrent build ---
  std::unique_ptr<IndexFactoryBuilder> par;
  ASSERT_OK(factory_->NewBuilder(option, par));
  ASSERT_TRUE(par->SupportsParallelAddEntry());

  // Bounded hand-off queue, sized like a small ring buffer so the emit
  // thread has to keep preparing while the writer thread is finishing.
  const size_t kQueueDepth = 4;
  struct Item {
    std::unique_ptr<IndexFactoryBuilder::PreparedAddEntry> prepared;
    IndexFactoryBuilder::BlockHandle handle;
  };
  std::deque<Item> queue;
  std::mutex mu;
  std::condition_variable not_full;
  std::condition_variable not_empty;
  bool done_preparing = false;

  port::Thread emitter([&]() {
    for (const auto& blk : blocks) {
      auto prepared = par->CreatePreparedAddEntry();
      if (!blk.next_key.empty()) {
        Slice next(blk.next_key);
        par->PrepareAddEntry(Slice(blk.last_key), &next,
                             EntryCtx(blk.last_seq, blk.first_seq),
                             prepared.get());
      } else {
        par->PrepareAddEntry(Slice(blk.last_key), nullptr,
                             EntryCtx(blk.last_seq, 0), prepared.get());
      }
      std::unique_lock<std::mutex> lock(mu);
      not_full.wait(lock, [&] { return queue.size() < kQueueDepth; });
      queue.push_back({std::move(prepared), {blk.offset, 100}});
      not_empty.notify_one();
    }
    std::lock_guard<std::mutex> lock(mu);
    done_preparing = true;
    not_empty.notify_one();
  });

  port::Thread writer([&]() {
    std::string scratch;
    for (;;) {
      Item item;
      {
        std::unique_lock<std::mutex> lock(mu);
        not_empty.wait(lock, [&] { return !queue.empty() || done_preparing; });
        if (queue.empty()) {
          return;
        }
        item = std::move(queue.front());
        queue.pop_front();
        not_full.notify_one();
      }
      par->FinishAddEntry(item.handle, item.prepared.get(), &scratch,
                          /*skip_delta_encoding=*/false);
    }
  });

  emitter.join();
  writer.join();

  Slice parallel_contents;
  ASSERT_OK(par->Finish(&parallel_contents));
  EXPECT_EQ(serial_contents, parallel_contents.ToString());

  // Every key must still land on a real data block.
  std::unique_ptr<IndexFactoryReader> reader;
  ASSERT_OK(factory_->NewReader(option, parallel_contents, reader));
  ReadOptions ro;
  auto iter = reader->NewIterator(ro);
  for (const auto& blk : blocks) {
    IterateResult result;
    ASSERT_OK(iter->SeekAndGetResult(Slice(blk.last_key), &result,
                                     SeekCtx(kMaxSequenceNumber)));
    ASSERT_EQ(iter->value().size, 100u) << blk.last_key;
  }
}

TEST_F(TrieIndexFactoryTest, ParallelEstimatedSizeStaysMonotonic) {
  // EstimatedSize() runs on the emit thread while FinishAddEntry commits
  // entries on the BG worker, and never decreases.
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();
  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));

  uint64_t last_size = builder->EstimatedSize();
  EXPECT_GT(last_size, 0u);

  for (int i = 0; i < 8; i++) {
    std::string key = "key";
    PutFixed32(&key, i);
    std::string next_key = "key";
    PutFixed32(&next_key, i + 1);

    auto p = builder->CreatePreparedAddEntry();
    Slice next(next_key);
    builder->PrepareAddEntry(Slice(key), &next, EntryCtx(100, 90), p.get());

    const uint64_t prepared_size = builder->EstimatedSize();
    EXPECT_GT(prepared_size, last_size)
        << "EstimatedSize should grow after Prepare";

    IndexFactoryBuilder::BlockHandle h{static_cast<uint64_t>(i * 105), 100};
    std::string scratch;
    builder->FinishAddEntry(h, p.get(), &scratch,
                            /*skip_delta_encoding=*/false);
    const uint64_t finished_size = builder->EstimatedSize();
    EXPECT_GE(finished_size, prepared_size)
        << "EstimatedSize should not shrink after FinishAddEntry";
    last_size = finished_size;
  }
}

TEST_F(TrieIndexFactoryTest, ParallelFinishWithoutPrepareFailsBuild) {
  // FinishAddEntry() on an entry that PrepareAddEntry() did not fill, or on
  // one that was already finished, has no separator to commit for its data
  // block. Skipping the block would leave its keys unreachable through the
  // index while the SST build succeeds, so Finish() must fail instead.
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();
  std::string scratch;

  for (bool finish_twice : {false, true}) {
    SCOPED_TRACE(finish_twice ? "entry finished twice"
                              : "entry never prepared");
    std::unique_ptr<IndexFactoryBuilder> builder;
    ASSERT_OK(factory_->NewBuilder(option, builder));

    // Block 0 holds keys up to "a".
    auto first = builder->CreatePreparedAddEntry();
    Slice next("b");
    builder->PrepareAddEntry(Slice("a"), &next, EntryCtx(100, 90), first.get());
    builder->FinishAddEntry({0, 100}, first.get(), &scratch,
                            /*skip_delta_encoding=*/false);

    // Block 1 (keys "b" and up) is committed without a prepared separator.
    if (finish_twice) {
      builder->FinishAddEntry({105, 100}, first.get(), &scratch,
                              /*skip_delta_encoding=*/false);
    } else {
      auto unfilled = builder->CreatePreparedAddEntry();
      builder->FinishAddEntry({105, 100}, unfilled.get(), &scratch,
                              /*skip_delta_encoding=*/false);
    }

    // Block 2 is the last block, holding keys up to "c".
    auto last = builder->CreatePreparedAddEntry();
    builder->PrepareAddEntry(Slice("c"), nullptr, EntryCtx(80, 0), last.get());
    builder->FinishAddEntry({210, 100}, last.get(), &scratch,
                            /*skip_delta_encoding=*/false);

    Slice contents;
    Status s = builder->Finish(&contents);
    ASSERT_EQ(s.code(), Status::Code::kInvalidArgument) << s.ToString();
  }
}

// ============================================================================
// Trie
// ============================================================================

class TrieTest : public testing::Test {
 protected:
  struct Block {
    std::string separator;
    TrieBlockHandle handle;
    uint64_t tag;
  };

  // Tag of a Seek target that lands on the first block of its key.
  const uint64_t kSeekTag =
      PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek);

  static uint64_t NumBlocks(const Trie& index) {
    return index.Trie::NumBlocks();
  }

  // Serializes blocks with the given bucket size, failing the test on error.
  static std::string Build(const std::vector<Block>& blocks,
                           uint32_t bucket_size) {
    TrieBuilder builder(bucket_size);
    for (const Block& b : blocks) {
      builder.AddBlock(b.separator, b.handle, b.tag);
    }
    std::string data;
    Status s = builder.Finish(&data);
    EXPECT_OK(s);
    return data;
  }

  // One block per separator, laid out back to back with random sizes and a
  // 5-byte gap between blocks.
  static std::vector<Block> ContiguousBlocks(
      const std::vector<std::string>& separators, Random* rnd) {
    std::vector<Block> blocks;
    uint64_t offset = rnd->Uniform(1000);
    for (const std::string& sep : separators) {
      const uint64_t size = 100 + rnd->Uniform(4000);
      blocks.push_back({sep, {offset, size}, 0});
      offset += size + 5;
    }
    return blocks;
  }

  // Blocks shaped like the factory's output: some separators span several
  // blocks with descending tags (the last block of such a run may have none),
  // and gaps are either uniform (derived sizes) or not (explicit sizes),
  // sometimes starting past 4 GiB.
  static std::vector<Block> RandomBlocks(
      const std::vector<std::string>& separators, Random* rnd) {
    std::vector<Block> blocks;
    const bool uniform_gaps = rnd->OneIn(2);
    uint64_t offset = rnd->OneIn(4) ? uint64_t{1} << (32 + rnd->Uniform(20))
                                    : rnd->Uniform(1000);
    for (const std::string& sep : separators) {
      const uint32_t run =
          rnd->OneIn(3) ? 2 + rnd->Uniform(rnd->OneIn(5) ? 40 : 5) : 1;
      uint64_t tag =
          PackSequenceAndType(1000 + rnd->Uniform(1 << 30), kTypeValue);
      for (uint32_t j = 0; j < run; j++) {
        const bool tagged = j + 1 < run || rnd->OneIn(3);
        const uint64_t size = 1 + rnd->Uniform(5000);
        blocks.push_back({sep, {offset, size}, tagged ? tag : 0});
        offset += size + (uniform_gaps ? 5 : rnd->Uniform(20));
        tag -= 1 + rnd->Uniform(256);
      }
    }
    return blocks;
  }

  // Sorted distinct non-empty keys from one of several shapes: a tiny
  // alphabet (deep shared prefixes, keys that prefix other keys), full-range
  // bytes (wide fanout), and either of those behind a long common prefix.
  static std::vector<std::string> RandomKeys(Random* rnd, size_t n) {
    const uint32_t shape = rnd->Uniform(4);
    const std::string common = shape >= 2 ? rnd->RandomString(30) : "";
    std::set<std::string> keys;
    while (keys.size() < n) {
      std::string k = common;
      const size_t len = 1 + rnd->Uniform(shape % 2 == 0 ? 12 : 3);
      for (size_t i = 0; i < len; i++) {
        k.push_back(shape % 2 == 0
                        ? static_cast<char>("ab\0\xff"[rnd->Uniform(4)])
                        : static_cast<char>(rnd->Uniform(256)));
      }
      keys.insert(k);
    }
    return {keys.begin(), keys.end()};
  }

  // Seek targets around each separator plus random strings.
  static std::vector<std::string> SeekTargets(
      const std::vector<std::string>& separators, Random* rnd) {
    std::vector<std::string> targets = {""};
    for (const std::string& sep : separators) {
      targets.push_back(sep);
      targets.push_back(sep + std::string(1, '\0'));
      targets.push_back(sep + "a");
      targets.push_back(
          sep.substr(0, rnd->Uniform(static_cast<int>(sep.size()) + 1)));
      if (!sep.empty()) {
        std::string bumped = sep;
        bumped.back() =
            static_cast<char>(bumped.back() + (rnd->OneIn(2) ? 1 : -1));
        targets.push_back(bumped);
      }
    }
    for (int i = 0; i < 100; i++) {
      targets.push_back(rnd->RandomBinaryString(rnd->Uniform(40)));
    }
    return targets;
  }

  // Index of the first block at or after (target, tag) in internal key
  // order, or blocks.size().
  static size_t ExpectedSeek(const std::vector<Block>& blocks,
                             const std::string& target, uint64_t tag) {
    for (size_t i = 0; i < blocks.size(); i++) {
      const int cmp = Slice(blocks[i].separator).compare(target);
      if (cmp > 0 ||
          (cmp == 0 && (blocks[i].tag == 0 || tag >= blocks[i].tag))) {
        return i;
      }
    }
    return blocks.size();
  }

  static void ExpectAt(const TrieIterator& it, const std::vector<Block>& blocks,
                       size_t i) {
    if (i == blocks.size()) {
      ASSERT_FALSE(it.Valid());
      return;
    }
    ASSERT_TRUE(it.Valid());
    ASSERT_EQ(it.key(), blocks[i].separator);
    ASSERT_EQ(it.value().offset, blocks[i].handle.offset);
    ASSERT_EQ(it.value().size, blocks[i].handle.size);
  }

  // Seek tags for user-key-only lookups and for an old snapshot.
  static std::vector<uint64_t> BasicSeekTags() {
    return {PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek),
            PackSequenceAndType(1, kValueTypeForSeek)};
  }

  // Checks a forward scan and Seek to every (target, tag) against the
  // blocks.
  static void Verify(const std::vector<Block>& blocks, uint32_t bucket_size,
                     const std::vector<std::string>& targets,
                     const std::vector<uint64_t>& tags) {
    SCOPED_TRACE("bucket_size=" + std::to_string(bucket_size));
    std::string data = Build(blocks, bucket_size);
    Trie index;
    ASSERT_OK(index.Init(data));
    ASSERT_EQ(NumBlocks(index), blocks.size());
    TrieIterator it(&index);
    ASSERT_OK(it.SeekToFirst());
    for (size_t i = 0; i <= blocks.size(); i++) {
      ASSERT_NO_FATAL_FAILURE(ExpectAt(it, blocks, i));
      ASSERT_OK(it.Next());
    }
    ASSERT_OK(it.SeekToLast());
    for (size_t i = blocks.size(); i-- > 0;) {
      ASSERT_NO_FATAL_FAILURE(ExpectAt(it, blocks, i));
      ASSERT_OK(it.Prev());
    }
    ASSERT_FALSE(it.Valid());
    for (const std::string& target : targets) {
      for (uint64_t tag : tags) {
        SCOPED_TRACE(Slice(target).ToString(true) + " tag " +
                     std::to_string(tag));
        ASSERT_OK(it.Seek(target, tag));
        const size_t expected = ExpectedSeek(blocks, target, tag);
        ASSERT_NO_FATAL_FAILURE(ExpectAt(it, blocks, expected));
        if (expected == blocks.size()) {
          continue;
        }
        // Step back and forth from the Seek position.
        ASSERT_OK(it.Prev());
        if (expected == 0) {
          ASSERT_FALSE(it.Valid());
          continue;
        }
        ASSERT_NO_FATAL_FAILURE(ExpectAt(it, blocks, expected - 1));
        ASSERT_OK(it.Next());
        ASSERT_NO_FATAL_FAILURE(ExpectAt(it, blocks, expected));
      }
    }
  }

  // Seeks to random (target, tag) pairs, then walks Next and Prev at random
  // across runs and bucket boundaries, checking every position.
  static void VerifyWalks(const std::vector<Block>& blocks,
                          uint32_t bucket_size,
                          const std::vector<std::string>& targets,
                          const std::vector<uint64_t>& tags, Random* rnd) {
    SCOPED_TRACE("bucket_size=" + std::to_string(bucket_size));
    std::string data = Build(blocks, bucket_size);
    Trie index;
    ASSERT_OK(index.Init(data));
    TrieIterator it(&index);
    for (int i = 0; i < 200; i++) {
      const std::string& target =
          targets[rnd->Uniform(static_cast<int>(targets.size()))];
      const uint64_t tag = tags[rnd->Uniform(static_cast<int>(tags.size()))];
      SCOPED_TRACE(Slice(target).ToString(true) + " tag " +
                   std::to_string(tag));
      ASSERT_OK(it.Seek(target, tag));
      size_t pos = ExpectedSeek(blocks, target, tag);
      ASSERT_NO_FATAL_FAILURE(ExpectAt(it, blocks, pos));
      for (int step = 0; step < 30 && pos < blocks.size(); step++) {
        if (rnd->OneIn(2)) {
          ASSERT_OK(it.Next());
          pos++;
        } else {
          ASSERT_OK(it.Prev());
          pos = pos == 0 ? blocks.size() : pos - 1;
        }
        ASSERT_NO_FATAL_FAILURE(ExpectAt(it, blocks, pos));
      }
    }
  }
};

TEST_F(TrieTest, EmptySeparator) {
  const uint32_t seed = TimeSeed();
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);
  std::vector<std::vector<std::string>> cases = {
      {""},
      {"", std::string(1, '\0')},
      {"", "a"},
      {"", "a", "ab", "b"},
  };
  for (int i = 0; i < 10; i++) {
    std::vector<std::string> keys = RandomKeys(&rnd, 1 + rnd.Uniform(100));
    keys.insert(keys.begin(), "");
    cases.push_back(keys);
  }
  for (const auto& keys : cases) {
    std::vector<Block> blocks = ContiguousBlocks(keys, &rnd);
    std::vector<std::string> targets = SeekTargets(keys, &rnd);
    for (uint32_t bucket_size : {1u, 2u, 8u}) {
      ASSERT_NO_FATAL_FAILURE(
          Verify(blocks, bucket_size, targets, BasicSeekTags()));
    }
  }
}

// One user key spanning several blocks repeats its separator; each block of
// the run carries the tag of its last key, newest first.
TEST_F(TrieTest, SameKeyRunsAndTags) {
  const uint32_t seed = TimeSeed();
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);
  for (int round = 0; round < 30; round++) {
    std::vector<std::string> keys = RandomKeys(&rnd, 1 + rnd.Uniform(300));
    std::vector<std::string> separators;
    std::vector<uint64_t> block_tags;
    for (const std::string& key : keys) {
      const uint32_t run = rnd.OneIn(3) ? 2 + rnd.Uniform(4) : 1;
      SequenceNumber seq = 1000 + rnd.Uniform(1000);
      for (uint32_t j = 0; j < run; j++) {
        separators.push_back(key);
        uint64_t tag = 0;
        if (run > 1 ? (j + 1 < run || rnd.OneIn(2)) : rnd.OneIn(8)) {
          tag = PackSequenceAndType(seq, kTypeValue);
        }
        block_tags.push_back(tag);
        seq -= 1 + rnd.Uniform(100);
      }
    }
    std::vector<Block> blocks = ContiguousBlocks(separators, &rnd);
    std::vector<uint64_t> tags = BasicSeekTags();
    tags.push_back(0);
    for (size_t i = 0; i < blocks.size(); i++) {
      blocks[i].tag = block_tags[i];
      if (block_tags[i] != 0) {
        tags.push_back(block_tags[i]);
        tags.push_back(block_tags[i] - 1);
        tags.push_back(block_tags[i] + 1);
      }
    }
    // Keep the cross product of targets and tags small.
    std::vector<std::string> targets = keys;
    targets.emplace_back();
    std::sort(tags.begin(), tags.end());
    tags.erase(std::unique(tags.begin(), tags.end()), tags.end());
    if (tags.size() > 60) {
      tags.resize(60);
    }
    for (uint32_t bucket_size : {1u, 3u, 8u}) {
      ASSERT_NO_FATAL_FAILURE(Verify(blocks, bucket_size, targets, tags));
    }
  }
}

TEST_F(TrieTest, LargeHandlesAndIrregularGaps) {
  const uint32_t seed = TimeSeed();
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);
  std::vector<std::string> keys = RandomKeys(&rnd, 500);
  std::vector<std::string> targets = SeekTargets(keys, &rnd);

  // Offsets past 4 GiB with a uniform gap between blocks.
  std::vector<Block> high = ContiguousBlocks(keys, &rnd);
  for (Block& b : high) {
    b.handle.offset += uint64_t{5} << 40;
  }
  // Gaps of varying size, as with block alignment padding.
  std::vector<Block> padded = ContiguousBlocks(keys, &rnd);
  uint64_t offset = 0;
  for (Block& b : padded) {
    b.handle.offset = offset;
    offset += b.handle.size + rnd.Uniform(4096);
  }
  // Blocks larger than 4 GiB.
  std::vector<Block> huge = ContiguousBlocks(keys, &rnd);
  offset = 0;
  for (Block& b : huge) {
    b.handle.offset = offset;
    b.handle.size = (uint64_t{1} << 33) + rnd.Uniform(1 << 20);
    offset += b.handle.size + 5;
  }
  for (const auto* blocks : {&high, &padded, &huge}) {
    ASSERT_NO_FATAL_FAILURE(Verify(*blocks, 8, targets, BasicSeekTags()));
  }
}

// A handle whose end does not fit in 64 bits is stored as is, with explicit
// sizes, whether or not it is the last block. (Sizes still have to fit one
// column, so they are all near the maximum.)
TEST_F(TrieTest, HandleEndPastUint64) {
  const uint64_t max = std::numeric_limits<uint64_t>::max();
  const std::vector<std::vector<Block>> cases = {
      {{"a", {5, max}, 0}},
      {{"a", {0, max}, 0}, {"b", {1, max}, 0}},
      {{"a", {0, max}, 0}, {"b", {1, max - 1}, 0}},
  };
  for (const std::vector<Block>& blocks : cases) {
    SCOPED_TRACE("num_blocks=" + std::to_string(blocks.size()));
    ASSERT_NO_FATAL_FAILURE(
        Verify(blocks, 8, {"", "a", "b", "c"}, BasicSeekTags()));
  }
}

// Column residuals are limited to 56 bits, far beyond any real SST.
TEST_F(TrieTest, ColumnRangeLimit) {
  TrieBuilder builder(8);
  builder.AddBlock("a", {0, 1}, 0);
  builder.AddBlock("b", {1, 1}, 0);
  builder.AddBlock("c", {uint64_t{1} << 60, 1}, 0);
  std::string data;
  ASSERT_EQ(builder.Finish(&data).code(), Status::Code::kNotSupported);
}

TEST_F(TrieTest, NoBlocks) {
  std::string data = Build({}, 8);
  Trie index;
  ASSERT_OK(index.Init(data));
  ASSERT_EQ(NumBlocks(index), 0u);
  TrieIterator it(&index);
  ASSERT_OK(it.SeekToFirst());
  ASSERT_FALSE(it.Valid());
  ASSERT_OK(it.SeekToLast());
  ASSERT_FALSE(it.Valid());
  ASSERT_OK(it.Seek("a", kSeekTag));
  ASSERT_FALSE(it.Valid());
}

// Version 1 was an earlier layout, so its indexes are rejected too. The
// version is checked before the header size, as other versions may have a
// shorter header.
TEST_F(TrieTest, RejectsOtherVersions) {
  std::string data = Build({{"a", {0, 10}, 0}}, 8);
  for (uint32_t version : {0u, 1u, 3u}) {
    SCOPED_TRACE(version);
    EncodeFixed32(&data[4], version);
    for (size_t len : {data.size(), size_t{8}}) {
      Trie index;
      ASSERT_EQ(index.Init(Slice(data.data(), len)).code(),
                Status::Code::kNotSupported);
    }
  }
}

// Init() rejects a max_key_len that the separator bytes stored in the index
// cannot reach.
TEST_F(TrieTest, RejectsMaxKeyLenBeyondContent) {
  const std::string data =
      Build({{"apple", {0, 100}, 0}, {"apricot", {105, 100}, 0}}, 8);
  Trie index;
  ASSERT_OK(index.Init(data));
  for (uint32_t max_key_len : {uint32_t{1} << 20, uint32_t{0xFFFFFFFF}}) {
    SCOPED_TRACE(max_key_len);
    std::string mutated = data;
    // Header field max_key_len.
    EncodeFixed32(&mutated[36], max_key_len);
    ASSERT_EQ(index.Init(mutated).code(), Status::Code::kCorruption);
  }
}

// The header's end offset is the index size, so bytes after it mean the
// header or the block boundaries are wrong.
TEST_F(TrieTest, RejectsBytesAfterEnd) {
  const std::string data =
      Build({{"apple", {0, 100}, 0}, {"apricot", {105, 100}, 0}}, 8);
  Trie index;
  ASSERT_OK(index.Init(data));
  for (size_t extra : {1, 8, 16, 17}) {
    SCOPED_TRACE(extra);
    const std::string longer = data + std::string(extra, '\0');
    ASSERT_EQ(index.Init(longer).code(), Status::Code::kCorruption);
  }
}

// Init() reads the header before it validates the sections, so a failure
// must not leave an iterator reading a half parsed index, nor a stale one
// from an earlier Init().
TEST_F(TrieTest, FailedInitLeavesIndexEmpty) {
  const std::string data =
      Build({{"apple", {0, 100}, 0}, {"apricot", {105, 100}, 0}}, 8);
  // The end offset no longer matches the size.
  const std::string longer = data + '\0';
  for (bool reinit : {false, true}) {
    SCOPED_TRACE(reinit);
    Trie index;
    if (reinit) {
      ASSERT_OK(index.Init(data));
    }
    ASSERT_EQ(index.Init(longer).code(), Status::Code::kCorruption);
    TrieIterator it(&index);
    ASSERT_OK(it.SeekToFirst());
    ASSERT_FALSE(it.Valid());
    ASSERT_OK(it.SeekToLast());
    ASSERT_FALSE(it.Valid());
    ASSERT_OK(it.Seek("apple", kSeekTag));
    ASSERT_FALSE(it.Valid());
  }
}

TEST_F(TrieTest, SeparatorsOutOfOrder) {
  TrieBuilder builder(8);
  builder.AddBlock("b", {0, 10}, 0);
  builder.AddBlock("a", {15, 10}, 0);
  std::string data;
  ASSERT_EQ(builder.Finish(&data).code(), Status::Code::kInvalidArgument);
}

// A block much larger than the others widens only the residuals of its own
// group of 64 offsets.
TEST_F(TrieTest, OutlierBlockWidensOnlyItsGroup) {
  const uint32_t seed = TimeSeed();
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);
  std::vector<std::string> keys;
  keys.reserve(2048);
  for (int i = 0; i < 2048; i++) {
    keys.push_back("key" + std::to_string(10000 + i));
  }
  std::vector<Block> blocks = ContiguousBlocks(keys, &rnd);
  const size_t uniform_size = Build(blocks, 8).size();
  blocks[1000].handle.size += 1 << 20;
  for (size_t i = 1001; i < blocks.size(); i++) {
    blocks[i].handle.offset += 1 << 20;
  }
  ASSERT_LE(Build(blocks, 8).size(), uniform_size + 64 * 56 / 8);
}

// Decreasing offsets would wrap a derived block size around. The writer
// refuses them. Init() does not read every offset, so the iterator reports
// Corruption when it reaches such a block.
TEST_F(TrieTest, DecreasingOffsets) {
  TrieBuilder builder(8);
  builder.AddBlock("a", {100, 10}, 0);
  builder.AddBlock("b", {50, 10}, 0);
  std::string data;
  ASSERT_EQ(builder.Finish(&data).code(), Status::Code::kInvalidArgument);

  const uint32_t seed = TimeSeed();
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);
  std::vector<std::string> keys;
  keys.reserve(128);
  for (int i = 0; i < 128; i++) {
    keys.push_back("key" + std::to_string(1000 + i));
  }
  data = Build(ContiguousBlocks(keys, &rnd), 8);
  // Zero the base of the second group of offsets, the header's fourth
  // section offset locating the offset table. Block 64 now starts before
  // block 63.
  const uint64_t offset_table = DecodeFixed64(&data[56 + 8 * 3]);
  EncodeFixed64(&data[offset_table + 8 + 16], 0);
  Trie index;
  ASSERT_OK(index.Init(data));
  TrieIterator it(&index);
  ASSERT_OK(it.SeekToFirst());
  for (int i = 0; i < 62; i++) {
    ASSERT_OK(it.Next());
  }
  ASSERT_EQ(it.key(), keys[62]);
  ASSERT_EQ(it.Next().code(), Status::Code::kCorruption);
  ASSERT_FALSE(it.Valid());
  ASSERT_EQ(it.Seek(keys[63], kSeekTag).code(), Status::Code::kCorruption);
  ASSERT_FALSE(it.Valid());
  ASSERT_OK(it.Seek(keys[64], kSeekTag));
  ASSERT_EQ(it.key(), keys[64]);
  ASSERT_EQ(it.Prev().code(), Status::Code::kCorruption);
  ASSERT_FALSE(it.Valid());
}

// The serialized bytes of a small index, written out by hand from the format
// description in trie.h. Changing them breaks existing SST files.
TEST_F(TrieTest, GoldenBytes) {
  std::string data = Build({{"ab", {0, 10}, 0}, {"ac", {15, 20}, 257}}, 8);
  std::string expected;
  ASSERT_TRUE(Slice(
                  // magic, version, flags, bucket_size
                  "45495254"
                  "02000000"
                  "00000000"
                  "08000000"
                  // num_blocks, num_keys
                  "0200000000000000"
                  "0200000000000000"
                  // prefix_len, max_key_len, id_width, trie_root
                  "01000000"
                  "02000000"
                  "01000000"
                  "00000000"
                  // size_gap
                  "0500000000000000"
                  // trie, bucket table, bucket data, offset table, size table
                  // (none), run table, tag table, end
                  "7900000000000000"
                  "7D00000000000000"
                  "9D00000000000000"
                  "9F00000000000000"
                  "0000000000000000"
                  "C700000000000000"
                  "CF00000000000000"
                  "E700000000000000"
                  // shared prefix "a"
                  "61"
                  // trie: terminal leaf, 1-byte pointers, path "b", bucket 0
                  "04"
                  "01"
                  "62"
                  "00"
                  // bucket table: 2 values as 0 + 2 * i, residuals of width 0
                  // from word 0, then padding
                  "0200000000000000"
                  "0000000000000000"
                  "02000000"
                  "00000000"
                  "0000000000000000"
                  // bucket data: "ac" after "ab" drops 1 byte and adds "c"
                  "11"
                  "63"
                  // offset table: offsets 0, 15 and 40 (35 plus the 5-byte gap)
                  // as -5 + 20 * i + residuals 5, 0, 5 of width 3 from word 0,
                  // then padding
                  "0300000000000000"
                  "FBFFFFFFFFFFFFFF"
                  "14000000"
                  "03000000"
                  "4501000000000000"
                  "0000000000000000"
                  // run table: empty
                  "0000000000000000"
                  // tag table: block 1 has tag 257
                  "0100000000000000"
                  "0100000000000000"
                  "0101000000000000")
                  .DecodeHex(&expected));
  ASSERT_EQ(Slice(data).ToString(true), Slice(expected).ToString(true));
}

// Init() does not check the separators inside buckets. The iterator
// bounds-checks each one as it decodes it and reports one that is out of
// bounds as Corruption, leaving itself invalid.
TEST_F(TrieTest, MalformedBucketEntryFailsOnRead) {
  std::vector<Block> blocks;
  blocks.reserve(4);
  for (uint64_t i = 0; i < 4; i++) {
    blocks.push_back({"k" + std::to_string(i), {i * 105, 100}, 0});
  }
  std::string data = Build(blocks, 8);
  // The first entry after bucket 0's head, located by the header's third
  // section offset. It now drops 15 bytes from a 1-byte suffix.
  const uint64_t bucket_data = DecodeFixed64(&data[56 + 8 * 2]);
  data[bucket_data] = '\xF0';
  Trie index;
  ASSERT_OK(index.Init(data));
  TrieIterator it(&index);
  ASSERT_OK(it.SeekToFirst());
  ASSERT_EQ(it.key(), "k0");
  ASSERT_EQ(it.Next().code(), Status::Code::kCorruption);
  ASSERT_FALSE(it.Valid());
  ASSERT_EQ(it.Seek("k2", kSeekTag).code(), Status::Code::kCorruption);
  ASSERT_FALSE(it.Valid());
  ASSERT_EQ(it.SeekToLast().code(), Status::Code::kCorruption);
  ASSERT_FALSE(it.Valid());
}

// Init() does not check bucket positions either. A position past the bucket
// data is clamped to its end, so the bucket reads as truncated.
TEST_F(TrieTest, BadBucketPositionFailsOnRead) {
  std::vector<Block> blocks;
  blocks.reserve(4);
  for (uint64_t i = 0; i < 4; i++) {
    blocks.push_back({"k" + std::to_string(i), {i * 105, 100}, 0});
  }
  std::string data = Build(blocks, 8);
  // The base of the only group of the bucket table, located by the header's
  // second section offset.
  const uint64_t bucket_table = DecodeFixed64(&data[56 + 8]);
  EncodeFixed64(&data[bucket_table + 8], uint64_t{1} << 40);
  Trie index;
  ASSERT_OK(index.Init(data));
  TrieIterator it(&index);
  // The bucket head comes from the trie, so only the entries after it fail.
  ASSERT_OK(it.SeekToFirst());
  ASSERT_EQ(it.key(), "k0");
  ASSERT_EQ(it.Next().code(), Status::Code::kCorruption);
  ASSERT_FALSE(it.Valid());
  ASSERT_EQ(it.Seek("k2", kSeekTag).code(), Status::Code::kCorruption);
  ASSERT_FALSE(it.Valid());
}

// Prev re-decodes the previous key's bucket from its head, so it meets a
// malformed entry the iterator has not decoded yet.
TEST_F(TrieTest, MalformedBucketEntryFailsOnPrev) {
  std::vector<Block> blocks;
  blocks.reserve(4);
  for (uint64_t i = 0; i < 4; i++) {
    blocks.push_back({"k" + std::to_string(i), {i * 105, 100}, 0});
  }
  // Buckets {k0, k1} and {k2, k3}. Break k1, the only entry of bucket 0.
  std::string data = Build(blocks, 2);
  const uint64_t bucket_data = DecodeFixed64(&data[56 + 8 * 2]);
  data[bucket_data] = '\xF0';
  Trie index;
  ASSERT_OK(index.Init(data));
  TrieIterator it(&index);
  // k2 is a bucket head, so the Seek decodes no entry.
  ASSERT_OK(it.Seek("k2", kSeekTag));
  ASSERT_EQ(it.key(), "k2");
  ASSERT_EQ(it.Prev().code(), Status::Code::kCorruption);
  ASSERT_FALSE(it.Valid());
}

// Malformed indexes must be rejected by Init() or iterate without crashing.
// Run under ASAN to catch out-of-bounds reads.
TEST_F(TrieTest, MalformedData) {
  const uint32_t seed = TimeSeed();
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);

  auto exercise = [&](const std::string& data) {
    Trie index;
    if (!OpenedOrRejected(index.Init(data))) {
      return;
    }
    TrieIterator it(&index);
    const uint64_t limit = 2 * NumBlocks(index) + 10;
    bool ok = OkOrCorruption(it.SeekToFirst());
    for (uint64_t i = 0; ok && it.Valid() && i < limit; i++) {
      // Sizes are derived here, and never wrap around.
      const TrieBlockHandle handle = it.value();
      ASSERT_GE(handle.offset + handle.size, handle.offset);
      ok = OkOrCorruption(it.Next());
    }
    ok = OkOrCorruption(it.SeekToLast());
    for (uint64_t i = 0; ok && it.Valid() && i < limit; i++) {
      ok = OkOrCorruption(it.Prev());
    }
    for (int i = 0; i < 20; i++) {
      ok = OkOrCorruption(
          it.Seek(rnd.RandomBinaryString(rnd.Uniform(12)), kSeekTag));
      if (ok && it.Valid()) {
        OkOrCorruption(it.Prev());
      }
    }
  };

  for (int round = 0; round < 20; round++) {
    std::vector<std::string> separators = RandomKeys(&rnd, 1 + rnd.Uniform(60));
    if (rnd.OneIn(2)) {
      separators.insert(
          separators.begin() + rnd.Uniform(static_cast<int>(separators.size())),
          separators[0]);
      std::sort(separators.begin(), separators.end());
    }
    std::vector<Block> blocks = ContiguousBlocks(separators, &rnd);
    blocks.back().tag = 1 << 8;
    const std::string data = Build(blocks, 1 + rnd.Uniform(8));

    for (size_t len = 0; len < data.size(); len++) {
      Trie index;
      ASSERT_EQ(index.Init(Slice(data.data(), len)).code(),
                Status::Code::kCorruption);
    }
    for (size_t pos = 0; pos < kTrieHeaderSize; pos++) {
      for (char v : {'\0', '\xff', static_cast<char>(data[pos] ^ 1)}) {
        std::string mutated = data;
        mutated[pos] = v;
        exercise(mutated);
      }
    }
    for (int i = 0; i < 300; i++) {
      exercise(FlipRandomBytes(data, &rnd));
    }
  }
}

TEST_F(TrieTest, RandomizedSeekAndScan) {
  const uint32_t seed = TimeSeed();
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);
  for (int round = 0; round < 40; round++) {
    const size_t n = 1 + rnd.Uniform(round < 20 ? 40 : 1500);
    std::vector<std::string> keys = RandomKeys(&rnd, n);
    std::vector<Block> blocks = ContiguousBlocks(keys, &rnd);
    std::vector<std::string> targets = SeekTargets(keys, &rnd);
    for (uint32_t bucket_size : {1u, 2u, 3u, 8u, 64u}) {
      ASSERT_NO_FATAL_FAILURE(
          Verify(blocks, bucket_size, targets, BasicSeekTags()));
    }
  }
}

// Like RandomizedSeekAndScan, with the shapes it leaves out: separators that
// span several blocks, tags, explicit sizes, offsets past 4 GiB, the empty
// key and a separator equal to the shared prefix. Also walks Next and Prev.
TEST_F(TrieTest, RandomizedRunsTagsAndSizes) {
  const uint32_t seed = TimeSeed();
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);
  for (int round = 0; round < 30; round++) {
    std::vector<std::string> keys =
        RandomKeys(&rnd, 1 + rnd.Uniform(round < 15 ? 30 : 200));
    const size_t shared = Slice(keys.front()).difference_offset(keys.back());
    if (rnd.OneIn(3) && shared < keys.front().size()) {
      keys.insert(keys.begin(), keys.front().substr(0, shared));
    } else if (rnd.OneIn(2)) {
      keys.insert(keys.begin(), "");
    }
    std::vector<Block> blocks = RandomBlocks(keys, &rnd);
    std::vector<std::string> targets = SeekTargets(keys, &rnd);
    std::vector<uint64_t> tags = BasicSeekTags();
    for (int i = 0; i < 2; i++) {
      const uint64_t tag =
          blocks[rnd.Uniform(static_cast<int>(blocks.size()))].tag;
      tags.insert(tags.end(), {tag, tag + 1, tag > 0 ? tag - 1 : 0});
    }
    for (uint32_t bucket_size : {1u, 2u, 3u, 8u, 64u}) {
      ASSERT_NO_FATAL_FAILURE(Verify(blocks, bucket_size, targets, tags));
      ASSERT_NO_FATAL_FAILURE(
          VerifyWalks(blocks, bucket_size, targets, tags, &rnd));
    }
  }
}

TEST_F(TrieTest, SingleBlock) {
  std::string data = Build({{"apple", {0, 4000}, 0}}, 8);
  Trie index;
  ASSERT_OK(index.Init(data));
  ASSERT_EQ(NumBlocks(index), 1u);
  TrieIterator it(&index);

  for (const char* target : {"", "aardvark", "apple"}) {
    SCOPED_TRACE(target);
    ASSERT_OK(it.Seek(target, kSeekTag));
    ASSERT_TRUE(it.Valid());
    ASSERT_EQ(it.key(), "apple");
    ASSERT_EQ(it.value().offset, 0u);
    ASSERT_EQ(it.value().size, 4000u);
  }
  for (const char* target : {"apples", "b"}) {
    SCOPED_TRACE(target);
    ASSERT_OK(it.Seek(target, kSeekTag));
    ASSERT_FALSE(it.Valid());
  }
}

}  // namespace trie_index
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
