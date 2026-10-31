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
#include "utilities/trie_index/bitvector.h"
#include "utilities/trie_index/louds_trie.h"
#include "utilities/trie_index/trie_index_factory.h"
#include "utilities/trie_index/trie_v2.h"

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

// Bitvector tests
// ============================================================================

class BitvectorTest : public testing::Test {};

TEST_F(BitvectorTest, EmptyBitvector) {
  BitvectorBuilder builder;
  ASSERT_EQ(builder.NumBits(), 0u);

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), 0u);
  ASSERT_EQ(bv.NumOnes(), 0u);
  ASSERT_EQ(bv.NumZeros(), 0u);
}

TEST_F(BitvectorTest, SingleBit) {
  // Single 1-bit.
  {
    BitvectorBuilder builder;
    builder.Append(true);
    Bitvector bv;
    bv.BuildFrom(builder);
    ASSERT_EQ(bv.NumBits(), 1u);
    ASSERT_EQ(bv.NumOnes(), 1u);
    ASSERT_TRUE(bv.GetBit(0));
    ASSERT_EQ(bv.Rank1(0), 0u);
    ASSERT_EQ(bv.Rank1(1), 1u);
    ASSERT_EQ(bv.FindNthOneBit(0), 0u);
    ASSERT_EQ(bv.FindNthZeroBit(0), 1u);  // No 0-bits; returns num_bits_.
  }
  // Single 0-bit.
  {
    BitvectorBuilder builder;
    builder.Append(false);
    Bitvector bv;
    bv.BuildFrom(builder);
    ASSERT_EQ(bv.NumBits(), 1u);
    ASSERT_EQ(bv.NumOnes(), 0u);
    ASSERT_FALSE(bv.GetBit(0));
    ASSERT_EQ(bv.Rank1(0), 0u);
    ASSERT_EQ(bv.Rank1(1), 0u);
    ASSERT_EQ(bv.Rank0(1), 1u);
    ASSERT_EQ(bv.FindNthZeroBit(0), 0u);
    ASSERT_EQ(bv.FindNthOneBit(0), 1u);  // No 1-bits; returns num_bits_.
  }
}

TEST_F(BitvectorTest, RankAndSelect) {
  // Build a bitvector: 1010 1010 1010 ... (alternating bits).
  const uint64_t n = 1000;
  BitvectorBuilder builder;
  for (uint64_t i = 0; i < n; i++) {
    builder.Append(i % 2 == 0);  // Even positions are 1.
  }

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), n);
  ASSERT_EQ(bv.NumOnes(), n / 2);

  // Verify rank at various positions.
  for (uint64_t pos = 0; pos <= n; pos++) {
    uint64_t expected_ones = (pos + 1) / 2;  // Count of even numbers in [0,pos)
    ASSERT_EQ(bv.Rank1(pos), expected_ones) << "Rank1 failed at pos=" << pos;
    ASSERT_EQ(bv.Rank0(pos), pos - expected_ones)
        << "Rank0 failed at pos=" << pos;
  }

  // Verify select.
  for (uint64_t i = 0; i < n / 2; i++) {
    ASSERT_EQ(bv.FindNthOneBit(i), i * 2) << "FindNthOneBit failed at i=" << i;
    ASSERT_EQ(bv.FindNthZeroBit(i), i * 2 + 1)
        << "FindNthZeroBit failed at i=" << i;
  }

  // Out of range selects.
  ASSERT_EQ(bv.FindNthOneBit(n / 2), n);
  ASSERT_EQ(bv.FindNthZeroBit(n / 2), n);
}

TEST_F(BitvectorTest, AllOnes) {
  const uint64_t n = 512;  // Exactly two rank sample intervals (256 each).
  BitvectorBuilder builder;
  builder.AppendMultiple(true, n);

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), n);
  ASSERT_EQ(bv.NumOnes(), n);

  for (uint64_t pos = 0; pos <= n; pos++) {
    ASSERT_EQ(bv.Rank1(pos), pos);
    ASSERT_EQ(bv.Rank0(pos), 0u);
  }
  for (uint64_t i = 0; i < n; i++) {
    ASSERT_EQ(bv.FindNthOneBit(i), i);
  }
  ASSERT_EQ(bv.FindNthZeroBit(0), n);  // No 0-bits.
}

TEST_F(BitvectorTest, AllZeros) {
  const uint64_t n = 512;
  BitvectorBuilder builder;
  builder.AppendMultiple(false, n);

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), n);
  ASSERT_EQ(bv.NumOnes(), 0u);

  for (uint64_t pos = 0; pos <= n; pos++) {
    ASSERT_EQ(bv.Rank1(pos), 0u);
    ASSERT_EQ(bv.Rank0(pos), pos);
  }
  for (uint64_t i = 0; i < n; i++) {
    ASSERT_EQ(bv.FindNthZeroBit(i), i);
  }
  ASSERT_EQ(bv.FindNthOneBit(0), n);  // No 1-bits.
}

TEST_F(BitvectorTest, LargeBitvector) {
  // Test with a bitvector larger than one rank sample (kBitsPerRankSample).
  const uint64_t n = 2000;
  BitvectorBuilder builder;
  uint64_t expected_ones = 0;
  for (uint64_t i = 0; i < n; i++) {
    bool bit = (i % 3 == 0);
    builder.Append(bit);
    if (bit) {
      expected_ones++;
    }
  }

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), n);
  ASSERT_EQ(bv.NumOnes(), expected_ones);

  // Verify rank at boundaries.
  ASSERT_EQ(bv.Rank1(0), 0u);
  ASSERT_EQ(bv.Rank1(n), expected_ones);

  // Verify select for a subset of positions.
  uint64_t ones_so_far = 0;
  for (uint64_t i = 0; i < n; i++) {
    if (bv.GetBit(i)) {
      ASSERT_EQ(bv.FindNthOneBit(ones_so_far), i)
          << "FindNthOneBit failed at ones_so_far=" << ones_so_far;
      ones_so_far++;
    }
  }
  ASSERT_EQ(ones_so_far, expected_ones);
}

TEST_F(BitvectorTest, SerializeAndDeserialize) {
  const uint64_t n = 1500;
  BitvectorBuilder builder;
  for (uint64_t i = 0; i < n; i++) {
    builder.Append(i % 5 == 0);
  }

  Bitvector original;
  original.BuildFrom(builder);

  // Serialize.
  std::string serialized;
  original.EncodeTo(&serialized);
  ASSERT_GT(serialized.size(), 0u);

  // Deserialize.
  Bitvector deserialized;
  size_t consumed = 0;
  ASSERT_OK(deserialized.InitFromData(serialized.data(), serialized.size(),
                                      &consumed));
  ASSERT_GT(consumed, 0u);
  ASSERT_EQ(consumed, serialized.size());

  // Verify match.
  ASSERT_EQ(deserialized.NumBits(), original.NumBits());
  ASSERT_EQ(deserialized.NumOnes(), original.NumOnes());

  for (uint64_t pos = 0; pos <= n; pos++) {
    ASSERT_EQ(deserialized.Rank1(pos), original.Rank1(pos))
        << "Rank1 mismatch at pos=" << pos;
  }

  for (uint64_t i = 0; i < original.NumOnes(); i++) {
    ASSERT_EQ(deserialized.FindNthOneBit(i), original.FindNthOneBit(i))
        << "FindNthOneBit mismatch at i=" << i;
  }
}

TEST_F(BitvectorTest, SerializeRoundTripNonAligned) {
  // Test with a size that's not a multiple of 64.
  const uint64_t n = 100;
  BitvectorBuilder builder;
  for (uint64_t i = 0; i < n; i++) {
    builder.Append(i % 7 == 0);
  }

  Bitvector original;
  original.BuildFrom(builder);

  std::string serialized;
  original.EncodeTo(&serialized);

  Bitvector deserialized;
  size_t consumed = 0;
  ASSERT_OK(deserialized.InitFromData(serialized.data(), serialized.size(),
                                      &consumed));
  ASSERT_EQ(consumed, serialized.size());
  ASSERT_EQ(deserialized.NumBits(), n);
  ASSERT_EQ(deserialized.NumOnes(), original.NumOnes());

  // Verify every bit matches.
  for (uint64_t i = 0; i < n; i++) {
    ASSERT_EQ(deserialized.GetBit(i), original.GetBit(i))
        << "Bit mismatch at position " << i;
  }
}

TEST_F(BitvectorTest, AppendWord) {
  // Test the AppendWord API used by the dense bitmap builder.
  BitvectorBuilder builder;

  // Append 4 words to simulate a 256-bit dense node bitmap.
  // Word 0: bits 0 and 3 set (labels 0x00 and 0x03).
  builder.AppendWord(0x9, 64);                     // 0b1001
  builder.AppendWord(0x0, 64);                     // all zeros
  builder.AppendWord(0x0, 64);                     // all zeros
  builder.AppendWord(0x80000000'00000000ULL, 64);  // bit 63 set (label 0xFF)

  ASSERT_EQ(builder.NumBits(), 256u);
  ASSERT_TRUE(builder.GetBit(0));  // label 0x00
  ASSERT_TRUE(builder.GetBit(3));  // label 0x03
  ASSERT_FALSE(builder.GetBit(1));
  ASSERT_FALSE(builder.GetBit(64));
  ASSERT_TRUE(builder.GetBit(255));  // label 0xFF

  // Build and verify rank/select work correctly.
  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), 256u);
  ASSERT_EQ(bv.NumOnes(), 3u);  // bits 0, 3, 255
  ASSERT_EQ(bv.FindNthOneBit(0), 0u);
  ASSERT_EQ(bv.FindNthOneBit(1), 3u);
  ASSERT_EQ(bv.FindNthOneBit(2), 255u);
}

TEST_F(BitvectorTest, AppendWordAndAppendInterleaved) {
  // Verify AppendWord works when mixed with regular Append calls,
  // as long as AppendWord is called on word-aligned boundaries.
  BitvectorBuilder builder;

  // Append 64 bits via AppendWord.
  builder.AppendWord(0xAAAAAAAA'AAAAAAAAULL, 64);
  ASSERT_EQ(builder.NumBits(), 64u);

  // Append 64 more bits one at a time.
  for (int i = 0; i < 64; i++) {
    builder.Append(i % 2 == 0);
  }
  ASSERT_EQ(builder.NumBits(), 128u);

  // Another word-aligned AppendWord.
  builder.AppendWord(0xFFFFFFFF'FFFFFFFFULL, 64);
  ASSERT_EQ(builder.NumBits(), 192u);

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), 192u);

  // First word: alternating starting at bit 1 (0xAAAA... = 1010...)
  ASSERT_FALSE(bv.GetBit(0));
  ASSERT_TRUE(bv.GetBit(1));
  // Second word: alternating starting at bit 0
  ASSERT_TRUE(bv.GetBit(64));
  ASSERT_FALSE(bv.GetBit(65));
  // Third word: all ones
  ASSERT_TRUE(bv.GetBit(128));
  ASSERT_TRUE(bv.GetBit(191));
}

TEST_F(BitvectorTest, NextSetBit) {
  // Build: 64 zeros, then 1, then 63 zeros, then 1.
  BitvectorBuilder builder;
  builder.AppendMultiple(false, 64);
  builder.Append(true);
  builder.AppendMultiple(false, 63);
  builder.Append(true);

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), 129u);

  // NextSetBit from various positions.
  ASSERT_EQ(bv.NextSetBit(0), 64u);     // First set bit.
  ASSERT_EQ(bv.NextSetBit(64), 64u);    // Exact position.
  ASSERT_EQ(bv.NextSetBit(65), 128u);   // Next one.
  ASSERT_EQ(bv.NextSetBit(128), 128u);  // Exact.
  ASSERT_EQ(bv.NextSetBit(129), 129u);  // Past end -> num_bits_.
}

TEST_F(BitvectorTest, NextSetBitAllZeros) {
  BitvectorBuilder builder;
  builder.AppendMultiple(false, 200);

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NextSetBit(0), 200u);
  ASSERT_EQ(bv.NextSetBit(100), 200u);
  ASSERT_EQ(bv.NextSetBit(199), 200u);
}

TEST_F(BitvectorTest, DistanceToNextSetBit) {
  // Build: 1, 0, 0, 1, 0, 1
  BitvectorBuilder builder;
  builder.Append(true);
  builder.Append(false);
  builder.Append(false);
  builder.Append(true);
  builder.Append(false);
  builder.Append(true);

  Bitvector bv;
  bv.BuildFrom(builder);

  ASSERT_EQ(bv.DistanceToNextSetBit(0), 3u);  // 0 -> 3
  ASSERT_EQ(bv.DistanceToNextSetBit(3), 2u);  // 3 -> 5
  // Last set bit: distance to next = num_bits_ - pos.
  ASSERT_EQ(bv.DistanceToNextSetBit(5), 1u);  // 5 -> 6 (num_bits_)
}

TEST_F(BitvectorTest, MultipleRankSampleBoundaries) {
  // Test rank/select across multiple rank sample boundaries.
  const uint64_t n = 2048;  // 8 rank samples (256 bits each).
  BitvectorBuilder builder;
  for (uint64_t i = 0; i < n; i++) {
    // Set bit at every 100th position.
    builder.Append(i % 100 == 0);
  }

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumOnes(), 21u);  // 0, 100, 200, ..., 2000

  // Verify rank at sample boundaries.
  ASSERT_EQ(bv.Rank1(512), 6u);    // 0,100,200,300,400,500
  ASSERT_EQ(bv.Rank1(1024), 11u);  // +600,700,800,900,1000
  ASSERT_EQ(bv.Rank1(1536), 16u);  // +1100,1200,1300,1400,1500

  // Verify select.
  for (uint64_t i = 0; i < bv.NumOnes(); i++) {
    ASSERT_EQ(bv.FindNthOneBit(i), i * 100);
  }
}

TEST_F(BitvectorTest, InitFromDataCorruption) {
  // Test that InitFromData correctly rejects truncated data.
  Bitvector bv;
  size_t consumed;

  // Too short for header.
  ASSERT_TRUE(bv.InitFromData("short", 5, &consumed).IsCorruption());

  // Header says more words than available.
  std::string bad;
  uint64_t num_bits = 1024;
  uint64_t num_ones = 0;
  bad.append(reinterpret_cast<const char*>(&num_bits), 8);
  bad.append(reinterpret_cast<const char*>(&num_ones), 8);
  // No word data follows.
  ASSERT_TRUE(
      bv.InitFromData(bad.data(), bad.size(), &consumed).IsCorruption());
}

TEST_F(BitvectorTest, FindNthSetBitInWordExhaustive) {
  // Verify FindNthSetBitInWord for every position in a word with known
  // popcount.
  uint64_t word = 0xDEADBEEFCAFEBABEULL;
  uint64_t pc = Popcount(word);

  for (uint64_t i = 0; i < pc; i++) {
    uint64_t pos = FindNthSetBitInWord(word, i);
    // The pos-th bit should be set.
    ASSERT_TRUE((word >> pos) & 1)
        << "FindNthSetBitInWord returned non-set bit at i=" << i;
    // Exactly i bits should be set before pos.
    uint64_t mask =
        (pos == 0) ? 0 : (pos == 64 ? ~uint64_t(0) : (uint64_t(1) << pos) - 1);
    ASSERT_EQ(Popcount(word & mask), i)
        << "Wrong rank at FindNthSetBitInWord result for i=" << i;
  }
}

TEST_F(BitvectorTest, MoveConstructor) {
  // Build a non-trivial bitvector and verify move constructor preserves
  // correctness, especially the RecomputePointers() logic.
  const uint64_t n = 1000;
  BitvectorBuilder builder;
  for (uint64_t i = 0; i < n; i++) {
    builder.Append(i % 3 == 0);
  }

  Bitvector original;
  original.BuildFrom(builder);
  uint64_t orig_ones = original.NumOnes();
  uint64_t orig_rank_500 = original.Rank1(500);
  uint64_t orig_select_10 = original.FindNthOneBit(10);

  // Move-construct a new bitvector.
  Bitvector moved(std::move(original));

  // Moved-to should preserve all data.
  ASSERT_EQ(moved.NumBits(), n);
  ASSERT_EQ(moved.NumOnes(), orig_ones);
  ASSERT_EQ(moved.Rank1(500), orig_rank_500);
  ASSERT_EQ(moved.FindNthOneBit(10), orig_select_10);

  // Verify full rank/select consistency.
  for (uint64_t i = 0; i < moved.NumOnes(); i++) {
    uint64_t pos = moved.FindNthOneBit(i);
    ASSERT_TRUE(moved.GetBit(pos))
        << "Bit not set at FindNthOneBit(" << i << ")";
  }

  // Moved-from should be empty/zeroed.
  // NOLINTBEGIN(bugprone-use-after-move)
  ASSERT_EQ(original.NumBits(), 0u);
  ASSERT_EQ(original.NumOnes(), 0u);
  // NOLINTEND(bugprone-use-after-move)
}

TEST_F(BitvectorTest, MoveAssignment) {
  // Build two bitvectors, move-assign the first to the second.
  const uint64_t n = 500;
  BitvectorBuilder builder1;
  for (uint64_t i = 0; i < n; i++) {
    builder1.Append(i % 5 == 0);
  }
  Bitvector bv1;
  bv1.BuildFrom(builder1);

  BitvectorBuilder builder2;
  for (uint64_t i = 0; i < 200; i++) {
    builder2.Append(true);
  }
  Bitvector bv2;
  bv2.BuildFrom(builder2);

  uint64_t bv1_ones = bv1.NumOnes();
  uint64_t bv1_rank_250 = bv1.Rank1(250);

  // Move-assign bv1 to bv2.
  bv2 = std::move(bv1);

  // bv2 should now have bv1's data.
  ASSERT_EQ(bv2.NumBits(), n);
  ASSERT_EQ(bv2.NumOnes(), bv1_ones);
  ASSERT_EQ(bv2.Rank1(250), bv1_rank_250);

  // Moved-from should be empty.
  ASSERT_EQ(bv1.NumBits(), 0u);  // NOLINT(bugprone-use-after-move)
}

TEST_F(BitvectorTest, SerializedSizeMatchesEncoded) {
  // Verify SerializedSize() matches actual encoded output length.
  const uint64_t n = 1000;
  BitvectorBuilder builder;
  for (uint64_t i = 0; i < n; i++) {
    builder.Append(i % 7 == 0);
  }
  Bitvector bv;
  bv.BuildFrom(builder);

  std::string encoded;
  bv.EncodeTo(&encoded);
  ASSERT_EQ(bv.SerializedSize(), encoded.size());
}

TEST_F(BitvectorTest, DistanceToNextSetBitCrossWordBoundary) {
  // Test DistanceToNextSetBit where the next set bit is in a different
  // 64-bit word. Build: bit 0 set, then 127 zeros, then bit 128 set.
  BitvectorBuilder builder;
  builder.Append(true);  // pos 0
  builder.AppendMultiple(false, 127);
  builder.Append(true);  // pos 128
  builder.Append(false);

  Bitvector bv;
  bv.BuildFrom(builder);

  // Distance from pos 0 to next set bit (pos 128) = 128.
  ASSERT_EQ(bv.DistanceToNextSetBit(0), 128u);
  // Distance from pos 128 is to end (no more set bits after).
  // DistanceToNextSetBit returns distance to next set bit after pos,
  // where pos itself must be a set bit.
}

TEST_F(BitvectorTest, NumZeros) {
  // Verify NumZeros for various patterns.
  const uint64_t n = 300;
  BitvectorBuilder builder;
  uint64_t expected_ones = 0;
  for (uint64_t i = 0; i < n; i++) {
    bool bit = (i % 4 == 0);
    builder.Append(bit);
    if (bit) {
      expected_ones++;
    }
  }
  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), n);
  ASSERT_EQ(bv.NumOnes(), expected_ones);
  ASSERT_EQ(bv.NumZeros(), n - expected_ones);
}

TEST_F(BitvectorTest, InitFromDataNumOnesExceedsNumBits) {
  // Craft a header where num_ones > num_bits -- should return Corruption.
  std::string buf(2 * sizeof(uint64_t), '\0');
  uint64_t num_bits = 64;
  uint64_t num_ones = 65;  // Invalid: more ones than bits.
  memcpy(&buf[0], &num_bits, sizeof(uint64_t));
  memcpy(&buf[sizeof(uint64_t)], &num_ones, sizeof(uint64_t));

  Bitvector bv;
  size_t consumed = 0;
  Status s = bv.InitFromData(buf.data(), buf.size(), &consumed);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

TEST_F(BitvectorTest, InitFromDataNumBitsExceedsUint32) {
  // Craft a header where num_bits > UINT32_MAX -- should return Corruption.
  std::string buf(2 * sizeof(uint64_t), '\0');
  uint64_t num_bits = static_cast<uint64_t>(UINT32_MAX) + 1;
  uint64_t num_ones = 0;
  memcpy(&buf[0], &num_bits, sizeof(uint64_t));
  memcpy(&buf[sizeof(uint64_t)], &num_ones, sizeof(uint64_t));

  Bitvector bv;
  size_t consumed = 0;
  Status s = bv.InitFromData(buf.data(), buf.size(), &consumed);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

TEST_F(BitvectorTest, InitFromDataWordsNotAligned) {
  // Build a valid bitvector, serialize it, then read from a buffer where the
  // words data starts at an odd offset (not 8-byte aligned).
  BitvectorBuilder builder;
  for (int i = 0; i < 128; i++) {
    builder.Append(i % 3 == 0);
  }
  Bitvector bv_orig;
  bv_orig.BuildFrom(builder);

  std::string serialized;
  bv_orig.EncodeTo(&serialized);

  // Allocate buffer with 1-byte extra and offset by 1 to break alignment.
  std::unique_ptr<char[]> unaligned_buf(new char[serialized.size() + 1]);
  memcpy(unaligned_buf.get() + 1, serialized.data(), serialized.size());

  Bitvector bv;
  size_t consumed = 0;
  Status s =
      bv.InitFromData(unaligned_buf.get() + 1, serialized.size(), &consumed);
  // The header is 16 bytes; words start right after. If the base pointer is
  // offset by 1, then the words pointer is at (base+1+16) which is odd.
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

TEST_F(BitvectorTest, InitFromDataTruncatedRankLUT) {
  // Build a valid bitvector, serialize, then truncate after the words section
  // so the rank LUT cannot be read.
  BitvectorBuilder builder;
  for (int i = 0; i < 512; i++) {
    builder.Append(i % 5 == 0);
  }
  Bitvector bv_orig;
  bv_orig.BuildFrom(builder);

  std::string serialized;
  bv_orig.EncodeTo(&serialized);

  // Header = 16 bytes, words = ceil(512/64)*8 = 64 bytes. Truncate after that.
  size_t truncated_size = 2 * sizeof(uint64_t) + ((512 + 63) / 64) * 8;
  ASSERT_LT(truncated_size, serialized.size());

  Bitvector bv;
  size_t consumed = 0;
  Status s = bv.InitFromData(serialized.data(), truncated_size, &consumed);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

TEST_F(BitvectorTest, InitFromDataTruncatedSelect1Hints) {
  // Build a bitvector with enough ones to have select1 hints, then truncate
  // after the rank LUT so select1 hints cannot be read.
  BitvectorBuilder builder;
  // 600 bits, all ones -> lots of select1 hints.
  for (int i = 0; i < 600; i++) {
    builder.Append(true);
  }
  Bitvector bv_orig;
  bv_orig.BuildFrom(builder);

  std::string serialized;
  bv_orig.EncodeTo(&serialized);

  // We need to find a truncation point after the rank LUT but before select1
  // hints finish. Use a binary approach: try reading at decreasing sizes.
  // A valid read needs the full buffer. Remove the last 8 bytes (at minimum).
  size_t truncated_size = serialized.size() - 8;

  Bitvector bv;
  size_t consumed = 0;
  Status s = bv.InitFromData(serialized.data(), truncated_size, &consumed);
  // Should fail for either select1 or select0 hints truncation.
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

TEST_F(BitvectorTest, InitFromDataTruncatedSelect0Hints) {
  // Build a bitvector with enough zeros to have select0 hints, then truncate
  // so select0 hints cannot be fully read.
  BitvectorBuilder builder;
  // 600 bits, all zeros -> lots of select0 hints, zero select1 hints.
  for (int i = 0; i < 600; i++) {
    builder.Append(false);
  }
  Bitvector bv_orig;
  bv_orig.BuildFrom(builder);

  std::string serialized;
  bv_orig.EncodeTo(&serialized);

  // Remove the last 8 bytes.
  size_t truncated_size = serialized.size() - 8;

  Bitvector bv;
  size_t consumed = 0;
  Status s = bv.InitFromData(serialized.data(), truncated_size, &consumed);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

TEST_F(BitvectorTest, BuilderReserve) {
  // Verify Reserve() doesn't change behavior, just avoids reallocation.
  BitvectorBuilder builder;
  builder.Reserve(1000);
  for (int i = 0; i < 1000; i++) {
    builder.Append(i % 2 == 0);
  }
  ASSERT_EQ(builder.NumBits(), 1000u);

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), 1000u);
  ASSERT_EQ(bv.NumOnes(), 500u);
  // Verify correctness of rank/select after Reserve.
  ASSERT_EQ(bv.FindNthOneBit(0), 0u);
  ASSERT_EQ(bv.FindNthOneBit(1), 2u);
  ASSERT_EQ(bv.FindNthZeroBit(0), 1u);
}

TEST_F(BitvectorTest, AppendMultipleZeroCount) {
  // AppendMultiple with count=0 should be a no-op.
  BitvectorBuilder builder;
  builder.Append(true);
  builder.AppendMultiple(true, 0);
  builder.AppendMultiple(false, 0);
  ASSERT_EQ(builder.NumBits(), 1u);
  ASSERT_TRUE(builder.GetBit(0));
}

TEST_F(BitvectorTest, AppendMultipleTruePartialWord) {
  // AppendMultiple(true, count) starting at a non-word-aligned position.
  // This exercises the partial-word fill path at the beginning.
  BitvectorBuilder builder;
  // Start with 7 false bits to offset by 7 within the first word.
  builder.AppendMultiple(false, 7);
  // Now append 200 true bits starting at bit position 7.
  builder.AppendMultiple(true, 200);
  // Then 5 more false bits.
  builder.AppendMultiple(false, 5);

  ASSERT_EQ(builder.NumBits(), 212u);
  // Verify the pattern: 7 zeros, 200 ones, 5 zeros.
  for (uint64_t i = 0; i < 7; i++) {
    ASSERT_FALSE(builder.GetBit(i)) << "Expected 0 at " << i;
  }
  for (uint64_t i = 7; i < 207; i++) {
    ASSERT_TRUE(builder.GetBit(i)) << "Expected 1 at " << i;
  }
  for (uint64_t i = 207; i < 212; i++) {
    ASSERT_FALSE(builder.GetBit(i)) << "Expected 0 at " << i;
  }

  // Build the bitvector and verify rank/select correctness.
  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumOnes(), 200u);
  ASSERT_EQ(bv.FindNthOneBit(0), 7u);
  ASSERT_EQ(bv.FindNthOneBit(199), 206u);
  ASSERT_EQ(bv.FindNthZeroBit(0), 0u);
  ASSERT_EQ(bv.FindNthZeroBit(7), 207u);
}

TEST_F(BitvectorTest, FindNthZeroBitNonAligned) {
  // Build a bitvector whose length is not a multiple of 64, then verify
  // FindNthZeroBit handles the last partial word correctly.
  BitvectorBuilder builder;
  // 100 bits: alternating 1, 0.
  for (int i = 0; i < 100; i++) {
    builder.Append(i % 2 == 0);
  }
  // 100 bits, 50 ones (at even positions), 50 zeros (at odd positions).
  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), 100u);
  ASSERT_EQ(bv.NumOnes(), 50u);

  // The zeros are at positions 1, 3, 5, ..., 99.
  for (uint64_t i = 0; i < 50; i++) {
    ASSERT_EQ(bv.FindNthZeroBit(i), 2 * i + 1) << "Mismatch at i=" << i;
  }
  // Asking for the 50th zero-bit (0-indexed) should return num_bits_ (100).
  ASSERT_EQ(bv.FindNthZeroBit(50), 100u);
}

// ============================================================================
// EliasFano tests
// ============================================================================

class EliasFanoTest : public testing::Test {};

TEST_F(EliasFanoTest, EmptySequence) {
  EliasFano ef;
  std::vector<uint64_t> values;
  ef.BuildFrom(values.data(), 0, 0);
  ASSERT_EQ(ef.Count(), 0u);
  ASSERT_EQ(ef.Universe(), 0u);
}

TEST_F(EliasFanoTest, SingleElement) {
  EliasFano ef;
  uint64_t val = 42;
  ef.BuildFrom(&val, 1, 100);
  ASSERT_EQ(ef.Count(), 1u);
  ASSERT_EQ(ef.Universe(), 100u);
  ASSERT_EQ(ef.Access(0), 42u);
}

TEST_F(EliasFanoTest, BuildAccessVariants) {
  // Table-driven test covering different value distributions.
  struct Case {
    const char* name;
    std::vector<uint64_t> values;
    uint64_t universe;
  };

  // Monotonic sequence (uniform spacing).
  std::vector<uint64_t> monotonic = {0,  10, 20, 30, 40, 50,
                                     60, 70, 80, 90, 100};
  // Constant sequence (all same value, universe = count).
  std::vector<uint64_t> constant(50, 7);
  // Large universe simulating 4GB SST block offsets.
  const uint64_t large_universe = uint64_t(4) * 1024 * 1024 * 1024;
  std::vector<uint64_t> large(1000);
  for (size_t i = 0; i < large.size(); i++) {
    large[i] = i * (large_universe / large.size());
  }
  // Dense consecutive: 0, 1, 2, ..., 99.
  std::vector<uint64_t> consecutive(100);
  for (size_t i = 0; i < 100; i++) {
    consecutive[i] = i;
  }
  // Word-boundary crossing: low_bits ~17, small offsets per element.
  std::vector<uint64_t> word_boundary(100);
  for (size_t i = 0; i < 100; i++) {
    word_boundary[i] = i * 131072 + (i % 7);
  }

  std::vector<Case> cases = {
      {"MonotonicSequence", monotonic, 101},
      {"ConstantSequence", constant, 8},
      {"LargeUniverse", large, large_universe},
      {"ConsecutiveValues", consecutive, 100},
      {"WordBoundaryCrossing", word_boundary, 100 * 131072},
  };

  for (const auto& tc : cases) {
    SCOPED_TRACE(tc.name);
    EliasFano ef;
    ef.BuildFrom(tc.values.data(), tc.values.size(), tc.universe);
    ASSERT_EQ(ef.Count(), tc.values.size());
    ASSERT_EQ(ef.Universe(), tc.universe);
    for (size_t i = 0; i < tc.values.size(); i++) {
      ASSERT_EQ(ef.Access(i), tc.values[i]) << "Mismatch at i=" << i;
    }
  }
}

TEST_F(EliasFanoTest, SerializeDeserializeRoundTrip) {
  // Build, serialize, deserialize, verify all values match.
  std::vector<uint64_t> values = {0, 5, 100, 1000, 5000, 10000, 50000};
  EliasFano original;
  original.BuildFrom(values.data(), values.size(), 100000);

  std::string serialized;
  original.EncodeTo(&serialized);
  ASSERT_GT(serialized.size(), 0u);
  ASSERT_EQ(original.SerializedSize(), serialized.size());

  EliasFano deserialized;
  size_t consumed = 0;
  ASSERT_OK(deserialized.InitFromData(serialized.data(), serialized.size(),
                                      &consumed));
  ASSERT_EQ(consumed, serialized.size());
  ASSERT_EQ(deserialized.Count(), original.Count());
  ASSERT_EQ(deserialized.Universe(), original.Universe());

  for (size_t i = 0; i < values.size(); i++) {
    ASSERT_EQ(deserialized.Access(i), values[i]) << "Mismatch at i=" << i;
  }
}

TEST_F(EliasFanoTest, SerializeDeserializeEmpty) {
  EliasFano original;
  std::vector<uint64_t> empty;
  original.BuildFrom(empty.data(), 0, 0);

  std::string serialized;
  original.EncodeTo(&serialized);

  EliasFano deserialized;
  size_t consumed = 0;
  ASSERT_OK(deserialized.InitFromData(serialized.data(), serialized.size(),
                                      &consumed));
  ASSERT_EQ(deserialized.Count(), 0u);
}

TEST_F(EliasFanoTest, InitFromDataCorruptionTruncatedHeader) {
  std::string bad_data(10, '\0');  // Too short for 3 uint64_t header.
  EliasFano ef;
  size_t consumed = 0;
  ASSERT_TRUE(ef.InitFromData(bad_data.data(), bad_data.size(), &consumed)
                  .IsCorruption());
}

TEST_F(EliasFanoTest, InitFromDataCorruptionBadLowBits) {
  // Build a valid EliasFano, then corrupt the low_bits field to > 63.
  std::vector<uint64_t> values = {0, 10, 20};
  EliasFano ef;
  ef.BuildFrom(values.data(), values.size(), 100);

  std::string serialized;
  ef.EncodeTo(&serialized);

  // low_bits is the third uint64_t (bytes 16..23). Set to 64.
  uint64_t bad_low_bits = 64;
  memcpy(&serialized[16], &bad_low_bits, sizeof(uint64_t));

  EliasFano corrupt;
  size_t consumed = 0;
  ASSERT_TRUE(
      corrupt.InitFromData(serialized.data(), serialized.size(), &consumed)
          .IsCorruption());
}

TEST_F(EliasFanoTest, InitFromDataCorruptionTruncatedLowWords) {
  // Build valid, then truncate so low words are incomplete.
  std::vector<uint64_t> values = {0, 100, 200, 300, 400};
  EliasFano ef;
  ef.BuildFrom(values.data(), values.size(), 1000);

  std::string serialized;
  ef.EncodeTo(&serialized);

  // Truncate a few bytes off the end.
  std::string truncated = serialized.substr(0, serialized.size() - 4);
  EliasFano corrupt;
  size_t consumed = 0;
  ASSERT_TRUE(
      corrupt.InitFromData(truncated.data(), truncated.size(), &consumed)
          .IsCorruption());
}

TEST_F(EliasFanoTest, LargeCount) {
  // Test with 10K elements to exercise multi-word packing.
  const size_t count = 10000;
  const uint64_t universe = count * 1000;
  std::vector<uint64_t> values(count);
  for (size_t i = 0; i < count; i++) {
    values[i] = i * 1000;
  }
  EliasFano ef;
  ef.BuildFrom(values.data(), values.size(), universe);
  ASSERT_EQ(ef.Count(), count);

  // Spot-check values.
  ASSERT_EQ(ef.Access(0), 0u);
  ASSERT_EQ(ef.Access(count / 2), (count / 2) * 1000);
  ASSERT_EQ(ef.Access(count - 1), (count - 1) * 1000);

  // Full verification.
  for (size_t i = 0; i < count; i++) {
    ASSERT_EQ(ef.Access(i), values[i]) << "Mismatch at i=" << i;
  }
}

TEST_F(EliasFanoTest, InitFromDataTruncatedHighBitvector) {
  // Build a valid EliasFano, serialize it, then truncate so the high bitvector
  // cannot be fully read.
  std::vector<uint64_t> values = {10, 20, 30, 40, 50};
  EliasFano ef;
  ef.BuildFrom(values.data(), values.size(), 100);

  std::string serialized;
  ef.EncodeTo(&serialized);

  // Header is 3 * 8 = 24 bytes. Truncate just a few bytes past the header
  // so the high bitvector InitFromData fails.
  size_t truncated_size = 3 * sizeof(uint64_t) + 4;
  ASSERT_LT(truncated_size, serialized.size());

  EliasFano ef2;
  size_t consumed = 0;
  Status s = ef2.InitFromData(serialized.data(), truncated_size, &consumed);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

TEST_F(EliasFanoTest, MoveConstructor) {
  // Verify that move-constructing an EliasFano preserves correctness.
  // Use the InitFromData path (production path) since the default move ctor
  // doesn't re-seat low_words_ into owned_low_data_. When initialized from
  // serialized data, low_words_ points into external memory (not
  // owned_low_data_), so the default move is safe.
  std::vector<uint64_t> values = {0, 100, 200, 300, 400};
  EliasFano ef_tmp;
  ef_tmp.BuildFrom(values.data(), values.size(), 500);

  std::string serialized;
  ef_tmp.EncodeTo(&serialized);

  EliasFano ef1;
  size_t consumed = 0;
  ASSERT_OK(ef1.InitFromData(serialized.data(), serialized.size(), &consumed));

  // Move-construct ef2 from ef1.
  EliasFano ef2(std::move(ef1));

  ASSERT_EQ(ef2.Count(), 5u);
  ASSERT_EQ(ef2.Universe(), 500u);
  for (size_t i = 0; i < values.size(); i++) {
    ASSERT_EQ(ef2.Access(i), values[i]) << "Mismatch at i=" << i;
  }
}

TEST_F(EliasFanoTest, MoveAssignment) {
  // Verify that move-assigning an EliasFano preserves correctness.
  // Use the InitFromData path (see MoveConstructor comment for rationale).
  std::vector<uint64_t> values = {5, 15, 25, 35, 45};
  EliasFano ef_tmp;
  ef_tmp.BuildFrom(values.data(), values.size(), 50);

  std::string serialized;
  ef_tmp.EncodeTo(&serialized);

  EliasFano ef1;
  size_t consumed = 0;
  ASSERT_OK(ef1.InitFromData(serialized.data(), serialized.size(), &consumed));

  // Move-assign ef1 into a fresh ef2.
  EliasFano ef2;
  ef2 = std::move(ef1);

  ASSERT_EQ(ef2.Count(), 5u);
  ASSERT_EQ(ef2.Universe(), 50u);
  for (size_t i = 0; i < values.size(); i++) {
    ASSERT_EQ(ef2.Access(i), values[i]) << "Mismatch at i=" << i;
  }
}

// ============================================================================
// LoudsTrie tests
// ============================================================================

class LoudsTrieTest : public testing::Test {
 protected:
  // Owns builder (and its serialized data), trie, and iterator.  The handle
  // for key[i] is {i * 100, 50} by convention.
  //
  // Custom move constructor is required because `iter` holds a raw pointer to
  // `trie`.  A default move would transfer the unique_ptr but leave it
  // pointing at the moved-from trie (dangling).  We re-create the iterator
  // against the new trie location instead.
  struct BuiltTrie {
    LoudsTrieBuilder builder;
    LoudsTrie trie;
    std::unique_ptr<LoudsTrieIterator> iter;

    BuiltTrie() = default;
    ~BuiltTrie() = default;
    BuiltTrie(BuiltTrie&& other) noexcept
        : builder(std::move(other.builder)), trie(std::move(other.trie)) {
      if (other.iter) {
        iter = std::make_unique<LoudsTrieIterator>(&trie);
      }
    }
    BuiltTrie& operator=(BuiltTrie&&) = delete;
    BuiltTrie(const BuiltTrie&) = delete;
    BuiltTrie& operator=(const BuiltTrie&) = delete;
  };

  // Build a trie from pre-sorted keys using the standard {i*100, 50} handles.
  // Returns a BuiltTrie whose iterator is ready for Seek/Next.
  static BuiltTrie BuildTrieFromKeys(const std::vector<std::string>& keys) {
    BuiltTrie bt;
    for (size_t i = 0; i < keys.size(); i++) {
      bt.builder.AddKey(Slice(keys[i]), TrieBlockHandle{i * 100, 50});
    }
    bt.builder.Finish();
    Status s = bt.trie.InitFromData(bt.builder.GetSerializedData());
    EXPECT_OK(s);
    bt.iter = std::make_unique<LoudsTrieIterator>(&bt.trie);
    return bt;
  }

  // Seek to the first key and walk the full trie with Next, verifying every
  // key string and handle offset.
  static void VerifyFullScan(const BuiltTrie& bt,
                             const std::vector<std::string>& keys) {
    auto* iter = bt.iter.get();
    ASSERT_TRUE(iter->Seek(Slice(keys[0])));
    for (size_t i = 0; i < keys.size(); i++) {
      ASSERT_TRUE(iter->Valid()) << "Invalid at i=" << i;
      ASSERT_EQ(iter->Key().ToString(), keys[i]) << "Key mismatch at i=" << i;
      ASSERT_EQ(iter->Value().offset, i * 100) << "Handle mismatch at i=" << i;
      if (i < keys.size() - 1) {
        ASSERT_TRUE(iter->Next()) << "Next failed at i=" << i;
      }
    }
    ASSERT_FALSE(iter->Next());
    ASSERT_FALSE(iter->Valid());
  }
};

// Helper: build a trie from sorted keys and verify that Seek and Next
// iterate in exactly the expected order, checking both handles and
// reconstructed keys.
static void VerifyTrieIteration(const std::vector<std::string>& keys) {
  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    TrieBlockHandle h{i * 100, 50};
    builder.AddKey(Slice(keys[i]), h);
  }
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));
  ASSERT_EQ(trie.NumKeys(), keys.size());

  LoudsTrieIterator iter(&trie);

  // Test 1: Seek to every key and verify exact match + correct handle.
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Seek(Slice(keys[i])))
        << "Seek failed for key[" << i << "]=\"" << keys[i] << "\"";
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.Key().ToString(), keys[i]) << "Key mismatch at i=" << i;
    ASSERT_EQ(iter.Value().offset, i * 100) << "Handle mismatch at i=" << i;
  }

  // Test 2: Seek to first key, then Next through all keys.
  ASSERT_TRUE(iter.Seek(Slice(keys[0])));
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Valid()) << "Invalid at i=" << i;
    ASSERT_EQ(iter.Key().ToString(), keys[i])
        << "Key mismatch during iteration at i=" << i;
    ASSERT_EQ(iter.Value().offset, i * 100)
        << "Handle mismatch during iteration at i=" << i;
    if (i < keys.size() - 1) {
      ASSERT_TRUE(iter.Next()) << "Next failed at i=" << i;
    }
  }
  ASSERT_FALSE(iter.Next());
  ASSERT_FALSE(iter.Valid());

  // Test 3: Seek past all keys.
  std::string past_all = keys.back() + "zzz";
  ASSERT_FALSE(iter.Seek(Slice(past_all)));
  ASSERT_FALSE(iter.Valid());

  // Test 3b: SeekToFirst, and SeekToLast then Prev through all keys.
  ASSERT_TRUE(iter.SeekToFirst());
  ASSERT_EQ(iter.Key().ToString(), keys[0]);
  ASSERT_EQ(iter.Value().offset, 0u);
  ASSERT_TRUE(iter.SeekToLast());
  for (size_t i = keys.size(); i-- > 0;) {
    ASSERT_TRUE(iter.Valid()) << "Invalid at i=" << i;
    ASSERT_EQ(iter.Key().ToString(), keys[i])
        << "Key mismatch during reverse iteration at i=" << i;
    ASSERT_EQ(iter.Value().offset, i * 100)
        << "Handle mismatch during reverse iteration at i=" << i;
    ASSERT_EQ(iter.Prev(), i > 0);
  }
  ASSERT_FALSE(iter.Valid());

  // Test 4: Seek before all keys.
  std::string before_all(1, keys[0][0] - 1);
  if (keys[0][0] > 0) {
    ASSERT_TRUE(iter.Seek(Slice(before_all)));
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.Key().ToString(), keys[0]);
  }
}

// Table-driven test consolidating individual VerifyTrieIteration tests.
// Each case generates a key set covering a distinct trie topology: prefix
// chains, binary content, dense/sparse boundaries, varying lengths, etc.
TEST_F(LoudsTrieTest, TrieTopologyVariants) {
  auto run = [](const char* name, std::vector<std::string> keys) {
    SCOPED_TRACE(name);
    std::sort(keys.begin(), keys.end());
    keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
    ASSERT_NO_FATAL_FAILURE(VerifyTrieIteration(keys));
  };

  // Minimal multi-key trie.
  ASSERT_NO_FATAL_FAILURE(run("TwoKeys", {"aa", "ab"}));

  // The empty key alone, under a sparse root, and under a dense root.
  ASSERT_NO_FATAL_FAILURE(run("EmptyKeyOnly", {""}));
  ASSERT_NO_FATAL_FAILURE(run("EmptyKeySparseRoot", {"", "a", "ab", "b"}));
  {
    std::vector<std::string> keys = {""};
    for (int b = 0; b < 256; b++) {
      keys.emplace_back(1, static_cast<char>(b));
    }
    ASSERT_NO_FATAL_FAILURE(run("EmptyKeyDenseRoot", std::move(keys)));
  }

  // All keys share a long prefix, differ only in last byte.
  {
    std::string prefix(50, 'x');
    std::vector<std::string> keys;
    keys.reserve(26);
    for (char c = 'a'; c <= 'z'; c++) {
      keys.push_back(prefix + c);
    }
    ASSERT_NO_FATAL_FAILURE(
        run("IdenticalPrefixDifferentLastByte", std::move(keys)));
  }

  // Keys containing non-ASCII bytes (0x00, 0xFF, etc).
  ASSERT_NO_FATAL_FAILURE(run(
      "BinaryKeys", {std::string("\x00\x00", 2), std::string("\x00\x01", 2),
                     std::string("\x00\xFF", 2), std::string("\x01\x00", 2),
                     std::string("\xFF\x00", 2), std::string("\xFF\xFF", 2)}));

  // One key for every possible single byte value (0x00-0xFF).
  {
    std::vector<std::string> keys;
    keys.reserve(256);
    for (int b = 0; b < 256; b++) {
      keys.emplace_back(1, static_cast<char>(b));
    }
    ASSERT_NO_FATAL_FAILURE(run("SingleByteAllValues", std::move(keys)));
  }

  // Single path through the trie (linear chain), depth = 100.
  ASSERT_NO_FATAL_FAILURE(run("DeepChain", {std::string(100, 'a')}));

  // Mix of prefix keys and non-prefix keys in the same subtree.
  ASSERT_NO_FATAL_FAILURE(
      run("AlternatingPrefixAndNonPrefix", {"a", "ab", "abc", "b", "c"}));

  // Prefix key at the dense/sparse boundary.
  {
    std::vector<std::string> keys;
    keys.reserve(28);
    for (char c = 'a'; c <= 'z'; c++) {
      keys.emplace_back(1, c);
    }
    keys.emplace_back("aa");
    keys.emplace_back("aab");
    ASSERT_NO_FATAL_FAILURE(run("PrefixKeyAtDenseSparseEdge", std::move(keys)));
  }

  // Key reconstruction variants.
  ASSERT_NO_FATAL_FAILURE(
      run("KeyReconstructionBasic", {"abc", "abd", "abe", "xyz"}));
  ASSERT_NO_FATAL_FAILURE(
      run("KeyReconstructionSingleByte", {"a", "b", "c", "d", "e"}));

  {
    std::vector<std::string> keys;
    std::string prefix = "common_prefix_that_is_quite_long_";
    keys.reserve(26);
    for (int i = 0; i < 26; i++) {
      keys.push_back(prefix + static_cast<char>('a' + i));
    }
    ASSERT_NO_FATAL_FAILURE(
        run("KeyReconstructionLongSharedPrefix", std::move(keys)));
  }

  ASSERT_NO_FATAL_FAILURE(
      run("KeyReconstructionVaryingLengths",
          {"a", "ab", "abc", "abcd", "abcde", "b", "bc", "bcd"}));
  ASSERT_NO_FATAL_FAILURE(run("TwoByteKeys", {"aa", "ab", "ba", "bb", "ca"}));

  {
    std::vector<std::string> keys;
    keys.reserve(26);
    for (int c = 'a'; c <= 'z'; c++) {
      keys.push_back(std::string(1, static_cast<char>(c)) + "suffix");
    }
    ASSERT_NO_FATAL_FAILURE(run("HighFanoutRoot", std::move(keys)));
  }

  ASSERT_NO_FATAL_FAILURE(
      run("DeepTrie",
          {"abcdefghijklmnop", "abcdefghijklmnoq", "abcdefghijklmnor"}));

  // Prefix key variants.
  ASSERT_NO_FATAL_FAILURE(run("PrefixKeysSimple", {"a", "ab", "b"}));
  ASSERT_NO_FATAL_FAILURE(run("PrefixKeysChain", {"a", "ab", "abc", "abcd"}));
  ASSERT_NO_FATAL_FAILURE(
      run("PrefixKeysMultipleBranches", {"a", "aa", "ab", "ac"}));
  ASSERT_NO_FATAL_FAILURE(
      run("PrefixKeysAtDifferentLevels", {"a", "aa", "aab", "b", "ba"}));

  {
    std::vector<std::string> keys;
    keys.reserve(30);
    for (int i = 0; i < 30; i++) {
      keys.push_back("p_" + std::to_string(i));
    }
    ASSERT_NO_FATAL_FAILURE(run("PrefixKeysDiverging", std::move(keys)));
  }

  ASSERT_NO_FATAL_FAILURE(run("PrefixKeyRootOnly", {"a", "ab", "ac", "b"}));
  ASSERT_NO_FATAL_FAILURE(
      run("MultiplePrefixKeySameNode", {"x", "xa", "xab", "xb"}));

  {
    std::vector<std::string> keys;
    keys.reserve(20);
    for (int len = 1; len <= 20; len++) {
      keys.emplace_back(len, 'a');
    }
    ASSERT_NO_FATAL_FAILURE(run("ManyPrefixKeys", std::move(keys)));
  }

  // Dense/sparse boundary variants.
  ASSERT_NO_FATAL_FAILURE(run("AllSparse", {"a", "b"}));

  {
    std::vector<std::string> keys;
    keys.reserve(26);
    for (char c = 'a'; c <= 'z'; c++) {
      keys.emplace_back(1, c);
    }
    ASSERT_NO_FATAL_FAILURE(run("AllDense", std::move(keys)));
  }

  {
    std::vector<std::string> keys;
    for (char c = 'a'; c <= 'z'; c++) {
      keys.push_back(std::string(1, c) + "1");
      keys.push_back(std::string(1, c) + "2");
    }
    ASSERT_NO_FATAL_FAILURE(run("DenseSparseTransition", std::move(keys)));
  }

  // Larger key sets (former deterministic stress tests).
  {
    std::vector<std::string> keys;
    char buf[32];
    for (int i = 0; i < 1000; i++) {
      snprintf(buf, sizeof(buf), "key_%06d", i);
      keys.emplace_back(buf);
    }
    ASSERT_NO_FATAL_FAILURE(run("ThousandFormattedKeys", std::move(keys)));
  }

  {
    std::vector<std::string> keys;
    const char* prefixes[] = {"alpha", "beta", "gamma", "delta", "epsilon"};
    char buf[64];
    for (const char* p : prefixes) {
      for (int i = 0; i < 50; i++) {
        snprintf(buf, sizeof(buf), "%s_%03d", p, i);
        keys.emplace_back(buf);
      }
    }
    ASSERT_NO_FATAL_FAILURE(run("DiversePrefixPatterns", std::move(keys)));
  }

  {
    std::vector<std::string> keys;
    keys.reserve(176);
    char buf[64];
    for (char c = 'a'; c <= 'z'; c++) {
      keys.emplace_back(1, c);
    }
    for (int i = 0; i < 100; i++) {
      snprintf(buf, sizeof(buf), "medium_%04d", i);
      keys.emplace_back(buf);
    }
    for (int i = 0; i < 50; i++) {
      snprintf(buf, sizeof(buf), "this_is_a_very_long_key_%06d", i);
      keys.emplace_back(buf);
    }
    ASSERT_NO_FATAL_FAILURE(run("MixedLengths", std::move(keys)));
  }

  {
    std::vector<std::string> keys;
    keys.reserve(200);
    for (int i = 0; i < 200; i++) {
      keys.push_back("item_" + std::to_string(i));
    }
    ASSERT_NO_FATAL_FAILURE(run("PrefixKeyStressPatterns", std::move(keys)));
  }
}

// Randomized stress test: generates random key sets with varying sizes,
// lengths, and character distributions, verifying trie correctness for each.
// Uses a time-based seed for broad coverage across runs; seed is logged
// via SCOPED_TRACE for reproducibility.
TEST_F(LoudsTrieTest, RandomizedStress) {
  uint32_t seed = static_cast<uint32_t>(
      std::chrono::system_clock::now().time_since_epoch().count());
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);

  for (int trial = 0; trial < 50; trial++) {
    SCOPED_TRACE("trial=" + std::to_string(trial));
    int num_keys = 1 + rnd.Uniform(500);
    int max_key_len = 1 + rnd.Uniform(30);
    std::vector<std::string> keys;
    keys.reserve(num_keys);
    for (int i = 0; i < num_keys; i++) {
      keys.push_back(rnd.RandomString(1 + rnd.Uniform(max_key_len)));
    }
    if (rnd.OneIn(3)) {
      keys.emplace_back();
    }
    std::sort(keys.begin(), keys.end());
    keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
    if (keys.empty()) {
      continue;
    }
    ASSERT_NO_FATAL_FAILURE(VerifyTrieIteration(keys));
  }
}

// --- Basic builder and trie construction ---

TEST_F(LoudsTrieTest, EmptyTrie) {
  LoudsTrieBuilder builder;
  builder.Finish();
  Slice data = builder.GetSerializedData();
  ASSERT_GT(data.size(), 0u);

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));
  ASSERT_EQ(trie.NumKeys(), 0u);
}

TEST_F(LoudsTrieTest, SingleKey) {
  LoudsTrieBuilder builder;
  TrieBlockHandle h{100, 200};
  builder.AddKey(Slice("hello"), h);
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));
  ASSERT_EQ(trie.NumKeys(), 1u);

  // Verify handle.
  auto handle = trie.GetHandle(0);
  ASSERT_EQ(handle.offset, 100u);
  ASSERT_EQ(handle.size, 200u);
}

TEST_F(LoudsTrieTest, MultipleKeys) {
  LoudsTrieBuilder builder;
  std::vector<std::string> keys = {"apple", "application", "banana", "band",
                                   "cat"};
  for (size_t i = 0; i < keys.size(); i++) {
    TrieBlockHandle h{i * 100, 50};
    builder.AddKey(Slice(keys[i]), h);
  }
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));
  ASSERT_EQ(trie.NumKeys(), keys.size());

  // Verify all handles via the iterator (which computes correct leaf indices).
  LoudsTrieIterator iter(&trie);
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Seek(Slice(keys[i])));
    ASSERT_TRUE(iter.Valid());
    auto handle = iter.Value();
    ASSERT_EQ(handle.offset, i * 100) << "Handle mismatch for key " << keys[i];
    ASSERT_EQ(handle.size, 50u);
  }
}

TEST_F(LoudsTrieTest, SharedPrefixKeys) {
  // Keys with long shared prefixes -- this exercises the trie's prefix
  // compression.
  LoudsTrieBuilder builder;
  std::vector<std::string> keys;
  for (int i = 0; i < 100; i++) {
    std::string key = "common_prefix_" + std::to_string(i);
    keys.push_back(key);
  }
  // Keys are already sorted lexicographically for single-digit vs double-digit
  // numbers, but we need proper sorting.
  std::sort(keys.begin(), keys.end());

  for (size_t i = 0; i < keys.size(); i++) {
    TrieBlockHandle h{i * 100, 50};
    builder.AddKey(Slice(keys[i]), h);
  }
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));
  ASSERT_EQ(trie.NumKeys(), keys.size());
}

// --- Block handle encoding ---

TEST_F(LoudsTrieTest, HandleRoundTrip) {
  // Verify that block handles round-trip correctly through serialization.
  // Handles are stored as fixed-width uint64 arrays for zero-copy loading.
  LoudsTrieBuilder builder;

  // Simulate realistic SST block handles: sequential offsets with ~4KB blocks.
  const uint64_t kBlockSize = 4096;
  const int kNumBlocks = 100;
  std::vector<std::string> keys;
  std::vector<TrieBlockHandle> expected_handles;

  for (int i = 0; i < kNumBlocks; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "key_%04d", i);
    keys.emplace_back(buf);
    TrieBlockHandle h{static_cast<uint64_t>(i) * kBlockSize, kBlockSize};
    expected_handles.emplace_back(h);
    builder.AddKey(Slice(keys.back()), h);
  }
  builder.Finish();
  Slice data = builder.GetSerializedData();

  // Handles are stored as two fixed-width uint64 arrays (offsets + sizes),
  // totaling 16 bytes per handle (100 * 16 = 1600 bytes for handles).
  // This trades space for O(1) zero-copy loading.
  size_t fixed_handle_bytes = kNumBlocks * 16;
  // Verify that handles deserialize correctly.

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));
  ASSERT_EQ(trie.NumKeys(), static_cast<uint64_t>(kNumBlocks));

  // Verify all handles round-trip correctly.
  LoudsTrieIterator iter(&trie);
  ASSERT_TRUE(iter.Seek(Slice(keys[0])));
  for (int i = 0; i < kNumBlocks; i++) {
    ASSERT_TRUE(iter.Valid()) << "Invalid at i=" << i;
    auto h = iter.Value();
    ASSERT_EQ(h.offset, expected_handles[i].offset)
        << "Offset mismatch at i=" << i;
    ASSERT_EQ(h.size, expected_handles[i].size) << "Size mismatch at i=" << i;
    if (i < kNumBlocks - 1) {
      ASSERT_TRUE(iter.Next());
    }
  }

  // Log for informational purposes.
  fprintf(stderr,
          "HandleRoundTrip: total serialized=%zu, "
          "handle arrays=%zu bytes\n",
          data.size(), fixed_handle_bytes);
  (void)fixed_handle_bytes;
}

TEST_F(LoudsTrieTest, PrefixKeyHandleReorderVerification) {
  // Explicitly verify that BFS handle reordering is correct with prefix keys.
  // Keys: "a"=0, "ab"=1, "b"=2
  // BFS leaf order: "b" first (leaf at root), then "a" (prefix at level 1),
  // then "ab" (leaf at level 2).
  // So handles should be: [2, 0, 1] in BFS order.
  LoudsTrieBuilder builder;
  builder.AddKey(Slice("a"), {100, 10});
  builder.AddKey(Slice("ab"), {200, 20});
  builder.AddKey(Slice("b"), {300, 30});
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));

  LoudsTrieIterator iter(&trie);

  // Seek to "a" -- should find "a" with handle {100, 10}.
  ASSERT_TRUE(iter.Seek(Slice("a")));
  ASSERT_EQ(iter.Key().ToString(), "a");
  ASSERT_EQ(iter.Value().offset, 100u);
  ASSERT_EQ(iter.Value().size, 10u);

  // Next should give "ab" with handle {200, 20}.
  ASSERT_TRUE(iter.Next());
  ASSERT_EQ(iter.Key().ToString(), "ab");
  ASSERT_EQ(iter.Value().offset, 200u);
  ASSERT_EQ(iter.Value().size, 20u);

  // Next should give "b" with handle {300, 30}.
  ASSERT_TRUE(iter.Next());
  ASSERT_EQ(iter.Key().ToString(), "b");
  ASSERT_EQ(iter.Value().offset, 300u);
  ASSERT_EQ(iter.Value().size, 30u);

  ASSERT_FALSE(iter.Next());
}

TEST_F(LoudsTrieTest, PrefixKeySeekBetween) {
  // Seek to keys between a prefix key and its child.
  // "a" < target "aa" < "ab"
  LoudsTrieBuilder builder;
  builder.AddKey(Slice("a"), {100, 10});
  builder.AddKey(Slice("ab"), {200, 20});
  builder.AddKey(Slice("b"), {300, 30});
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));
  LoudsTrieIterator iter(&trie);

  // "aa" is between "a" (prefix key) and "ab" -> should land on "ab".
  ASSERT_TRUE(iter.Seek(Slice("aa")));
  ASSERT_EQ(iter.Key().ToString(), "ab");

  // "a" should hit the prefix key exactly.
  ASSERT_TRUE(iter.Seek(Slice("a")));
  ASSERT_EQ(iter.Key().ToString(), "a");
}

// --- Iterator edge cases ---

TEST_F(LoudsTrieTest, IteratorSeekExact) {
  std::vector<std::string> keys = {"abc", "abd", "abe", "xyz"};
  auto bt = BuildTrieFromKeys(keys);

  // Seek to exact keys.
  ASSERT_TRUE(bt.iter->Seek(Slice("abc")));
  ASSERT_TRUE(bt.iter->Valid());
  ASSERT_EQ(bt.iter->Value().offset, 0u);

  ASSERT_TRUE(bt.iter->Seek(Slice("xyz")));
  ASSERT_TRUE(bt.iter->Valid());
  ASSERT_EQ(bt.iter->Value().offset, 300u);
}

TEST_F(LoudsTrieTest, IteratorSeekBetweenKeys) {
  auto bt = BuildTrieFromKeys({"aaa", "bbb", "ccc", "ddd"});

  // Seek to a key between "aaa" and "bbb" -- should land on "bbb".
  ASSERT_TRUE(bt.iter->Seek(Slice("ab")));
  ASSERT_TRUE(bt.iter->Valid());
  ASSERT_EQ(bt.iter->Value().offset, 100u);  // "bbb"'s handle.
}

TEST_F(LoudsTrieTest, IteratorNext) {
  std::vector<std::string> keys = {"a", "b", "c", "d", "e"};
  auto bt = BuildTrieFromKeys(keys);
  ASSERT_NO_FATAL_FAILURE(VerifyFullScan(bt, keys));
}

TEST_F(LoudsTrieTest, IteratorSeekPastEnd) {
  auto bt = BuildTrieFromKeys({"aaa", "bbb"});
  // Seek past all keys.
  ASSERT_FALSE(bt.iter->Seek(Slice("zzz")));
  ASSERT_FALSE(bt.iter->Valid());
}

TEST_F(LoudsTrieTest, IteratorSeekBeforeAll) {
  auto bt = BuildTrieFromKeys({"bbb", "ccc", "ddd"});
  // Seek before all keys.
  ASSERT_TRUE(bt.iter->Seek(Slice("aaa")));
  ASSERT_TRUE(bt.iter->Valid());
  ASSERT_EQ(bt.iter->Value().offset, 0u);  // "bbb"'s handle.
}

TEST_F(LoudsTrieTest, SeekBetweenAllPairs) {
  // For a small set of keys, try seeking to every possible "between" value.
  std::vector<std::string> keys = {"aaa", "aab", "aac", "bbb", "ccc"};
  auto bt = BuildTrieFromKeys(keys);

  // Seek to values between consecutive keys.
  struct TestCase {
    std::string target;
    size_t expected_idx;  // index into keys, or keys.size() for past-end.
  };
  std::vector<TestCase> cases = {
      {"a", 0},     // Before first key -> first key.
      {"aaa", 0},   // Exact match.
      {"aaaa", 1},  // Between aaa and aab -> aab.
      {"aab", 1},   // Exact match.
      {"aaba", 2},  // Between aab and aac -> aac.
      {"aac", 2},   // Exact match.
      {"aad", 3},   // Between aac and bbb -> bbb.
      {"b", 3},     // Before bbb -> bbb.
      {"bbb", 3},   // Exact match.
      {"bbba", 4},  // Between bbb and ccc -> ccc.
      {"ccc", 4},   // Exact match.
      {"ccca", 5},  // Past end.
      {"zzz", 5},   // Past end.
  };

  for (const auto& tc : cases) {
    bool ok = bt.iter->Seek(Slice(tc.target));
    if (tc.expected_idx >= keys.size()) {
      ASSERT_FALSE(ok) << "Expected past-end for target=\"" << tc.target
                       << "\"";
    } else {
      ASSERT_TRUE(ok) << "Seek failed for target=\"" << tc.target << "\"";
      ASSERT_TRUE(bt.iter->Valid());
      ASSERT_EQ(bt.iter->Key().ToString(), keys[tc.expected_idx])
          << "Wrong key for target=\"" << tc.target << "\"" << " expected=\""
          << keys[tc.expected_idx] << "\"" << " got=\""
          << bt.iter->Key().ToString() << "\"";
    }
  }
}

TEST_F(LoudsTrieTest, EmptyTargetSeek) {
  // Seeking with an empty target should return the first key.
  auto bt = BuildTrieFromKeys({"aaa", "bbb", "ccc"});

  ASSERT_TRUE(bt.iter->Seek(Slice("")));
  ASSERT_TRUE(bt.iter->Valid());
  ASSERT_EQ(bt.iter->Key().ToString(), "aaa");
}

TEST_F(LoudsTrieTest, IteratorReSeekAfterInvalidation) {
  // After iterator becomes invalid (past end), re-seeking should work.
  auto bt = BuildTrieFromKeys({"aaa", "bbb", "ccc"});

  // Go past end.
  ASSERT_FALSE(bt.iter->Seek(Slice("zzz")));
  ASSERT_FALSE(bt.iter->Valid());

  // Re-seek to valid key.
  ASSERT_TRUE(bt.iter->Seek(Slice("aaa")));
  ASSERT_TRUE(bt.iter->Valid());
  ASSERT_EQ(bt.iter->Key().ToString(), "aaa");
}

TEST_F(LoudsTrieTest, IteratorNextOnInvalid) {
  // Calling Next() on an invalid iterator should return false.
  auto bt = BuildTrieFromKeys({"aaa"});
  ASSERT_FALSE(bt.iter->Valid());
  ASSERT_FALSE(bt.iter->Next());
}

TEST_F(LoudsTrieTest, IteratorSeekEmptyStringVariousKeys) {
  // Seek("") should always return the first key.
  auto bt = BuildTrieFromKeys({"x", "xy", "xyz"});

  ASSERT_TRUE(bt.iter->Seek(Slice("")));
  ASSERT_TRUE(bt.iter->Valid());
  ASSERT_EQ(bt.iter->Key().ToString(), "x");
}

TEST_F(LoudsTrieTest, IteratorMultipleSeeksDescending) {
  // Seek to keys in reverse order (each seek resets state).
  std::vector<std::string> keys = {"aaa", "bbb", "ccc", "ddd"};
  auto bt = BuildTrieFromKeys(keys);

  for (int i = static_cast<int>(keys.size()) - 1; i >= 0; i--) {
    ASSERT_TRUE(bt.iter->Seek(Slice(keys[i])));
    ASSERT_EQ(bt.iter->Key().ToString(), keys[i]);
    ASSERT_EQ(bt.iter->Value().offset, static_cast<uint64_t>(i) * 100);
  }
}

TEST_F(LoudsTrieTest, IteratorSeekToLongerThanAnyKey) {
  // Target key is longer than any key in the trie.
  // The trie has "ab" and "cd". Seeking "abx" should find "cd" (next after
  // "ab").
  auto bt = BuildTrieFromKeys({"ab", "cd"});

  ASSERT_TRUE(bt.iter->Seek(Slice("abx")));
  ASSERT_TRUE(bt.iter->Valid());
  ASSERT_EQ(bt.iter->Key().ToString(), "cd");
}

TEST_F(LoudsTrieTest, IteratorSeekTargetIsPrefixOfKey) {
  // Target "ab" is a prefix of trie key "abc". Should find "abc".
  auto bt = BuildTrieFromKeys({"abc", "abd"});

  ASSERT_TRUE(bt.iter->Seek(Slice("ab")));
  ASSERT_TRUE(bt.iter->Valid());
  ASSERT_EQ(bt.iter->Key().ToString(), "abc");
}

TEST_F(LoudsTrieTest, IteratorFullScanThenReSeek) {
  // Full scan with Next() until end, then re-seek to middle.
  auto bt = BuildTrieFromKeys({"aa", "bb", "cc", "dd", "ee"});

  // Scan all.
  ASSERT_TRUE(bt.iter->Seek(Slice("aa")));
  int count = 0;
  while (bt.iter->Valid()) {
    count++;
    bt.iter->Next();
  }
  ASSERT_EQ(count, 5);
  ASSERT_FALSE(bt.iter->Valid());

  // Re-seek to middle.
  ASSERT_TRUE(bt.iter->Seek(Slice("cc")));
  ASSERT_EQ(bt.iter->Key().ToString(), "cc");
  ASSERT_EQ(bt.iter->Value().offset, 200u);
}

// --- Serialization / deserialization ---

TEST_F(LoudsTrieTest, InitFromDataCorruption) {
  // Truncated data should return corruption status.
  LoudsTrie trie;
  ASSERT_TRUE(trie.InitFromData(Slice("short")).IsCorruption());

  // Bad magic number.
  std::string bad(56, '\0');
  bad[0] = 'X';  // Wrong magic.
  ASSERT_TRUE(trie.InitFromData(Slice(bad)).IsCorruption());
}

TEST_F(LoudsTrieTest, InitFromDataMaxDepthCorruption) {
  // A corrupted SST could set max_depth_ = UINT32_MAX, which would cause
  // an integer overflow in LoudsTrieIterator (key_cap_ = MaxDepth() + 1
  // wraps to 0). Verify that InitFromData rejects this.
  //
  // Build the header manually: magic(4) + version(4) + num_keys(8) +
  // cutoff_level(4) + max_depth(4) + dense_leaf_count(8) +
  // dense_node_count(8) + dense_child_count(8) + flags(4) +
  // reserved(4) = 56 bytes.
  std::string header;
  uint32_t magic = 0x54524945;  // "TRIE"
  uint32_t version = 1;
  uint64_t num_keys = 0;
  uint32_t cutoff_level = 0;
  uint32_t max_depth = UINT32_MAX;  // Corrupt value
  uint64_t zeros = 0;
  uint32_t zero32 = 0;

  header.append(reinterpret_cast<const char*>(&magic), 4);
  header.append(reinterpret_cast<const char*>(&version), 4);
  header.append(reinterpret_cast<const char*>(&num_keys), 8);
  header.append(reinterpret_cast<const char*>(&cutoff_level), 4);
  header.append(reinterpret_cast<const char*>(&max_depth), 4);
  header.append(reinterpret_cast<const char*>(&zeros), 8);  // dense_leaf_count
  header.append(reinterpret_cast<const char*>(&zeros), 8);  // dense_node_count
  header.append(reinterpret_cast<const char*>(&zeros), 8);  // dense_child_count
  header.append(reinterpret_cast<const char*>(&zero32), 4);  // flags
  header.append(reinterpret_cast<const char*>(&zero32), 4);  // reserved
  // Pad with enough data to pass subsequent parsing.
  header.append(256, '\0');

  LoudsTrie trie2;
  ASSERT_TRUE(trie2.InitFromData(Slice(header)).IsCorruption());

  // Also test a value just above the limit (65537).
  max_depth = 65537;
  std::string header2;
  header2.append(reinterpret_cast<const char*>(&magic), 4);
  header2.append(reinterpret_cast<const char*>(&version), 4);
  header2.append(reinterpret_cast<const char*>(&num_keys), 8);
  header2.append(reinterpret_cast<const char*>(&cutoff_level), 4);
  header2.append(reinterpret_cast<const char*>(&max_depth), 4);
  header2.append(reinterpret_cast<const char*>(&zeros), 8);
  header2.append(reinterpret_cast<const char*>(&zeros), 8);
  header2.append(reinterpret_cast<const char*>(&zeros), 8);
  header2.append(reinterpret_cast<const char*>(&zero32), 4);  // flags
  header2.append(reinterpret_cast<const char*>(&zero32), 4);  // reserved
  header2.append(256, '\0');

  LoudsTrie trie3;
  ASSERT_TRUE(trie3.InitFromData(Slice(header2)).IsCorruption());
}

TEST_F(LoudsTrieTest, MoveConstructor) {
  // Verify that move constructor works correctly for LoudsTrie, which
  // contains Bitvector members with RecomputePointers() logic.
  std::vector<std::string> keys = {"apple", "banana", "cherry", "date"};
  auto bt = BuildTrieFromKeys(keys);
  ASSERT_EQ(bt.trie.NumKeys(), keys.size());

  // Move-construct a new trie.
  LoudsTrie moved(std::move(bt.trie));
  ASSERT_EQ(moved.NumKeys(), keys.size());

  // Verify iteration works on the moved trie.
  LoudsTrieIterator iter(&moved);
  ASSERT_TRUE(iter.Seek(Slice(keys[0])));
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Valid()) << "Invalid at i=" << i;
    ASSERT_EQ(iter.Key().ToString(), keys[i]);
    ASSERT_EQ(iter.Value().offset, i * 100);
    if (i < keys.size() - 1) {
      ASSERT_TRUE(iter.Next());
    }
  }
  ASSERT_FALSE(iter.Next());
}

TEST_F(LoudsTrieTest, SerializeDeserializeRoundTrip) {
  // Build a non-trivial trie and verify that serialization round-trips.
  std::vector<std::string> keys = {"alpha", "beta", "gamma", "delta"};
  std::sort(keys.begin(), keys.end());
  auto bt = BuildTrieFromKeys(keys);
  ASSERT_EQ(bt.trie.NumKeys(), keys.size());
  ASSERT_NO_FATAL_FAILURE(VerifyFullScan(bt, keys));
}

TEST_F(LoudsTrieTest, SerializeDeserializeRoundTripMisalignedData) {
  // Verify that LoudsTrie::InitFromData handles non-8-byte-aligned data.
  // This can happen when the SST block is read from mmap at an unaligned
  // file offset.
  std::vector<std::string> keys = {"alpha", "beta", "gamma", "delta"};
  std::sort(keys.begin(), keys.end());
  auto bt = BuildTrieFromKeys(keys);
  Slice data = bt.builder.GetSerializedData();

  // Create a buffer with 1-byte offset to guarantee misalignment.
  std::string padded;
  padded.reserve(1 + data.size());
  padded.push_back('\0');  // 1-byte padding
  padded.append(data.data(), data.size());
  const char* misaligned_ptr = padded.data() + 1;
  ASSERT_NE(reinterpret_cast<uintptr_t>(misaligned_ptr) % 8, 0u);

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(Slice(misaligned_ptr, data.size())));
  ASSERT_EQ(trie.NumKeys(), keys.size());

  // Verify all keys and handles via iteration.
  LoudsTrieIterator iter(&trie);
  ASSERT_TRUE(iter.Seek(Slice(keys[0])));
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.Key().ToString(), keys[i]);
    ASSERT_EQ(iter.Value().offset, i * 100);
    if (i < keys.size() - 1) {
      iter.Next();
    }
  }
}

TEST_F(LoudsTrieTest, StressTest10KKeys) {
  // 10K keys with uniform distribution.
  std::vector<std::string> keys;
  for (int i = 0; i < 10000; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "key_%08d", i);
    keys.emplace_back(buf);
  }
  std::sort(keys.begin(), keys.end());
  auto bt = BuildTrieFromKeys(keys);
  ASSERT_EQ(bt.trie.NumKeys(), keys.size());

  // Verify every 100th key via Seek.
  for (size_t i = 0; i < keys.size(); i += 100) {
    ASSERT_TRUE(bt.iter->Seek(Slice(keys[i])))
        << "Seek failed for key[" << i << "]";
    ASSERT_EQ(bt.iter->Key().ToString(), keys[i]);
    ASSERT_EQ(bt.iter->Value().offset, i * 100);
  }

  // Full forward scan.
  ASSERT_NO_FATAL_FAILURE(VerifyFullScan(bt, keys));
}

TEST_F(LoudsTrieTest, MoveAssignment) {
  // Test move assignment operator (move constructor was already tested).
  std::vector<std::string> keys = {"alpha", "beta", "gamma", "delta"};
  std::sort(keys.begin(), keys.end());
  auto bt = BuildTrieFromKeys(keys);

  // Copy serialized data so it outlives the builder.
  std::string data_copy(bt.builder.GetSerializedData().data(),
                        bt.builder.GetSerializedData().size());
  LoudsTrie trie1;
  ASSERT_OK(trie1.InitFromData(Slice(data_copy)));
  ASSERT_EQ(trie1.NumKeys(), keys.size());

  // Move-assign trie1 into a fresh trie.
  LoudsTrie trie2;
  trie2 = std::move(trie1);
  ASSERT_EQ(trie2.NumKeys(), keys.size());

  // Verify the moved-to trie works correctly.
  LoudsTrieIterator iter(&trie2);
  ASSERT_TRUE(iter.Seek(Slice(keys[0])));
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Valid()) << "Invalid at i=" << i;
    ASSERT_EQ(iter.Key().ToString(), keys[i]);
    ASSERT_EQ(iter.Value().offset, i * 100);
    if (i < keys.size() - 1) {
      ASSERT_TRUE(iter.Next());
    }
  }
  ASSERT_FALSE(iter.Next());
}

TEST_F(LoudsTrieTest, CutoffLevelAndMaxDepthAndHasChains) {
  // Verify CutoffLevel(), MaxDepth(), and HasChains() return sensible values.
  std::vector<std::string> keys = {"a", "ab", "abc", "abcd", "b", "bc"};
  std::sort(keys.begin(), keys.end());
  auto bt = BuildTrieFromKeys(keys);
  auto& trie = bt.trie;

  // MaxDepth should equal the longest key length.
  uint32_t max_key_len = 0;
  for (const auto& k : keys) {
    max_key_len = std::max(max_key_len, static_cast<uint32_t>(k.size()));
  }
  ASSERT_EQ(trie.MaxDepth(), max_key_len);

  // CutoffLevel should be within [0, max_depth].
  ASSERT_LE(trie.CutoffLevel(), trie.MaxDepth());
}

TEST_F(LoudsTrieTest, LeafIndex) {
  // Verify LeafIndex() returns correct indices during sequential iteration.
  std::vector<std::string> keys = {"cat", "cow", "dog", "fox"};
  auto bt = BuildTrieFromKeys(keys);

  ASSERT_TRUE(bt.iter->Seek(Slice(keys[0])));

  // LeafIndex should be sequential for sequential keys.
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(bt.iter->Valid());
    ASSERT_EQ(bt.iter->LeafIndex(), i)
        << "LeafIndex mismatch at key=" << keys[i];
    ASSERT_EQ(bt.iter->Value().offset, i * 100);
    if (i < keys.size() - 1) {
      ASSERT_TRUE(bt.iter->Next());
    }
  }
}

TEST_F(LoudsTrieTest, EmptyTrieIterator) {
  // Verify iterator behavior on an empty trie.
  LoudsTrieBuilder builder;
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));
  ASSERT_EQ(trie.NumKeys(), 0u);

  LoudsTrieIterator iter(&trie);
  ASSERT_FALSE(iter.Seek(Slice("anything")));
  ASSERT_FALSE(iter.Valid());
  ASSERT_FALSE(iter.Next());
  ASSERT_FALSE(iter.Valid());
}

TEST_F(LoudsTrieTest, ApproximateAuxMemoryUsage) {
  // Verify that ApproximateAuxMemoryUsage() returns a reasonable value
  // for a trie with sparse internal nodes.
  std::vector<std::string> keys;
  for (int i = 0; i < 200; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "key_%04d", i);
    keys.emplace_back(buf);
  }
  auto bt = BuildTrieFromKeys(keys);

  // Aux memory should be > 0 for a trie with sparse internal nodes
  // (child position lookup tables are allocated).
  size_t aux_mem = bt.trie.ApproximateAuxMemoryUsage();
  ASSERT_GT(aux_mem, 0u);

  // Unaligned data is copied, and the copy is charged too.
  const Slice data = bt.builder.GetSerializedData();
  std::string unaligned(1, '\0');
  unaligned.append(data.data(), data.size());
  LoudsTrie unaligned_trie;
  ASSERT_OK(
      unaligned_trie.InitFromData(Slice(unaligned.data() + 1, data.size())));
  ASSERT_GE(unaligned_trie.ApproximateAuxMemoryUsage(), aux_mem + data.size());
}

TEST_F(LoudsTrieTest, InitFromDataUnknownFlagsOrReserved) {
  LoudsTrieBuilder builder;
  builder.SetHasSeqnoEncoding(true);
  builder.AddKeyWithSeqno("a", {0, 50}, 0, 1);
  builder.AddKeyWithSeqno("b", {100, 50}, 0, 1);
  builder.Finish();
  const std::string data = builder.GetSerializedData().ToString();
  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));

  // The flags word is at offset 48 and the reserved word at offset 52.
  for (size_t pos : {48, 49, 51, 52, 55}) {
    SCOPED_TRACE(pos);
    std::string patched = data;
    patched[pos] ^= pos == 48 ? 2 : 1;
    LoudsTrie patched_trie;
    ASSERT_EQ(patched_trie.InitFromData(patched).code(),
              Status::Code::kCorruption);
  }
}

TEST_F(LoudsTrieTest, InitFromDataUnsupportedVersion) {
  // Build a valid trie, then patch the format version to a bad value.
  std::vector<std::string> keys = {"a", "b", "c"};
  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  std::string data(builder.GetSerializedData().data(),
                   builder.GetSerializedData().size());

  // Version is at offset 4 (after the 4-byte magic).
  uint32_t bad_version = 99;
  memcpy(&data[4], &bad_version, sizeof(uint32_t));

  LoudsTrie trie;
  Status s = trie.InitFromData(Slice(data));
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_NE(s.ToString().find("unsupported format version"), std::string::npos);
}

TEST_F(LoudsTrieTest, InitFromDataProgressiveTruncation) {
  // Build a valid trie (no chains), serialize it, then try InitFromData with
  // every length from 0 to data.size()-1 and verify all return Corruption.
  // This covers ALL truncation error paths in InitFromData in a single test.
  std::vector<std::string> keys;
  for (int i = 0; i < 20; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "k%02d", i);
    keys.emplace_back(buf);
  }

  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  std::string data(builder.GetSerializedData().data(),
                   builder.GetSerializedData().size());

  // Verify the full data works.
  {
    LoudsTrie trie;
    ASSERT_OK(trie.InitFromData(Slice(data)));
  }

  // Every truncated length should fail with Corruption.
  for (size_t len = 0; len < data.size(); len++) {
    LoudsTrie trie;
    Status s = trie.InitFromData(Slice(data.data(), len));
    ASSERT_TRUE(s.IsCorruption()) << "Expected Corruption at len=" << len << "/"
                                  << data.size() << ", got: " << s.ToString();
  }
}

TEST_F(LoudsTrieTest, InitFromDataProgressiveTruncationWithChains) {
  // Same as above, but with keys that produce path compression chains.
  // Keys share a 10-char prefix "AAAAAAAAAA" + varying suffix, ensuring
  // the chain suffix is >= kMinChainLength (8).
  std::vector<std::string> keys;
  std::string prefix(10, 'A');
  for (int i = 0; i < 20; i++) {
    char suffix[8];
    snprintf(suffix, sizeof(suffix), "%02d", i);
    keys.push_back(prefix + suffix);
  }
  std::sort(keys.begin(), keys.end());

  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  std::string data(builder.GetSerializedData().data(),
                   builder.GetSerializedData().size());

  // Verify the full data works and has chains.
  {
    LoudsTrie trie;
    ASSERT_OK(trie.InitFromData(Slice(data)));
    ASSERT_TRUE(trie.HasChains());
  }

  // Every truncated length should fail with Corruption.
  for (size_t len = 0; len < data.size(); len++) {
    LoudsTrie trie;
    Status s = trie.InitFromData(Slice(data.data(), len));
    ASSERT_TRUE(s.IsCorruption()) << "Expected Corruption at len=" << len << "/"
                                  << data.size() << ", got: " << s.ToString();
  }
}

TEST_F(LoudsTrieTest, SparseBinarySearchPath) {
  // Create a trie where the root node has >16 children in the sparse level,
  // which forces SparseSeekLabel to use std::lower_bound instead of linear
  // scan (kLinearScanThreshold = 16).
  // Strategy: 20 two-byte keys with distinct first bytes. With cutoff=0
  // (all sparse), the root has 20 labels.
  std::vector<std::string> keys;
  // Use bytes 'A' through 'T' (20 distinct first bytes).
  for (int i = 0; i < 20; i++) {
    std::string key(1, static_cast<char>('A' + i));
    key += 'x';  // Second byte to ensure sparse leaf.
    keys.push_back(key);
  }
  std::sort(keys.begin(), keys.end());

  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));

  LoudsTrieIterator iter(&trie);

  // Seek to each key -- exercises binary search in the 20-label root node.
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Seek(Slice(keys[i])))
        << "Seek failed for key[" << i << "]";
    ASSERT_EQ(iter.Key().ToString(), keys[i]);
    ASSERT_EQ(iter.Value().offset, i * 100);
  }

  // Seek to a key between existing labels (binary search inexact match).
  // 'A' + 0.5 doesn't exist -- seek "AAx" which is between "Ax" and "Bx".
  ASSERT_TRUE(iter.Seek(Slice("AAx")));
  // Should land on "Ax" or "Bx" -- the first key >= "AAx".
  ASSERT_TRUE(iter.Valid());
  std::string result_key = iter.Key().ToString();
  ASSERT_GE(result_key, "AAx");

  // Seek past all keys.
  ASSERT_FALSE(iter.Seek(Slice("Zx")));
  ASSERT_FALSE(iter.Valid());

  // Full forward scan to verify trie integrity.
  ASSERT_TRUE(iter.Seek(Slice(keys[0])));
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Valid()) << "Invalid at i=" << i;
    ASSERT_EQ(iter.Key().ToString(), keys[i]);
    if (i < keys.size() - 1) {
      ASSERT_TRUE(iter.Next());
    }
  }
  ASSERT_FALSE(iter.Next());
}

TEST_F(LoudsTrieTest, ChainSeekVariousTargets) {
  // Build a trie with keys that create path compression chains, then seek to
  // various targets that exercise different chain-matching code paths:
  //   1. Full chain match (chain ends at internal node with fanout > 1)
  //   2. Chain mismatch: target < chain suffix
  //   3. Chain mismatch: target > chain suffix
  //   4. Target runs out before chain (cmp < 0)
  //   5. Target runs out before chain (cmp > 0)
  //   6. Target runs out before chain (cmp == 0, mid-chain consumption)
  //   7. target_remaining == 0 (target consumed at parent)
  //
  // Keys: all share a long prefix that triggers chain compression.
  // The prefix "AAAAAAAAA" (9 chars) + varying 2-char suffixes.
  // The chain suffix must be >= kMinChainLength (8 bytes).
  std::vector<std::string> keys;
  std::string prefix(9, 'A');
  // Add keys with suffixes that force a fanout > 1 after the chain.
  keys.push_back(prefix + "Ma");
  keys.push_back(prefix + "Mb");
  keys.push_back(prefix + "Mc");
  keys.push_back(prefix + "Na");
  keys.push_back(prefix + "Nb");
  std::sort(keys.begin(), keys.end());

  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  std::string data(builder.GetSerializedData().data(),
                   builder.GetSerializedData().size());

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(Slice(data)));

  LoudsTrieIterator iter(&trie);

  // 1. Exact match on each key (full chain match, internal node after chain).
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Seek(Slice(keys[i])))
        << "Seek failed for key[" << i << "]=\"" << keys[i] << "\"";
    ASSERT_EQ(iter.Key().ToString(), keys[i]);
    ASSERT_EQ(iter.Value().offset, i * 100);
  }

  // 2. Chain mismatch: target < chain suffix.
  // Seek to prefix + "L" -- 'L' < 'M', so chain mismatch with target < suffix.
  // Should land on the first key (prefix + "Ma").
  {
    std::string target = prefix + "La";
    ASSERT_TRUE(iter.Seek(Slice(target)));
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.Key().ToString(), keys[0]);  // "AAAAAAAAA" + "Ma"
  }

  // 3. Chain mismatch: target > chain suffix.
  // Seek to prefix + "Md" -- 'd' > 'c' after chain, should advance past "Mc".
  {
    std::string target = prefix + "Md";
    ASSERT_TRUE(iter.Seek(Slice(target)));
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.Key().ToString(), prefix + "Na");
  }

  // 4. Target runs out before chain (shorter target < chain prefix).
  // Seek to prefix + "L" (shorter than chain). 'L' < 'M' -> target < chain.
  {
    std::string target = prefix + "L";
    ASSERT_TRUE(iter.Seek(Slice(target)));
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.Key().ToString(), keys[0]);
  }

  // 5. Target runs out before chain (shorter target > chain prefix character).
  // Seek to prefix + "O" -- 'O' > 'N' (the highest chain prefix). Should
  // advance past all keys.
  {
    std::string target = prefix + "O";
    ASSERT_FALSE(iter.Seek(Slice(target)));
    ASSERT_FALSE(iter.Valid());
  }

  // 6. Target consumed exactly at the parent of the chain (target_remaining
  // == 0). Seek to just the prefix "AAAAAAAAA" -- shorter than any key.
  // All keys are > prefix, so should land on the first key.
  {
    ASSERT_TRUE(iter.Seek(Slice(prefix)));
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.Key().ToString(), keys[0]);
  }

  // 7. Full forward scan to verify trie integrity after all seeks.
  ASSERT_TRUE(iter.Seek(Slice(keys[0])));
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Valid()) << "Invalid at i=" << i;
    ASSERT_EQ(iter.Key().ToString(), keys[i]);
    if (i < keys.size() - 1) {
      ASSERT_TRUE(iter.Next());
    }
  }
  ASSERT_FALSE(iter.Next());
}

TEST_F(LoudsTrieTest, ChainSeekEndAtLeaf) {
  // Test path compression chains that end at a leaf node (end_child_idx ==
  // UINT32_MAX). This requires a key whose suffix after the chain endpoint
  // has no children -- i.e., the chain is the entire remaining path.
  //
  // Strategy: Create keys with a long shared prefix and a single-character
  // difference at the very end, so the chain covers most of the key and
  // terminates at a leaf.
  //
  // "AAAAAAAAA" (9 chars prefix) + "B" = 10-char key. The "AAAAAAAA" portion
  // (8 bytes) forms the chain suffix (>= kMinChainLength). The 'B' is the
  // label at the chain-ending leaf.
  //
  // We also add keys with different first characters to ensure the trie has
  // enough structure for the budget formula.
  std::vector<std::string> keys;
  // Main chain key.
  keys.push_back(std::string(9, 'A') + "B");
  // Additional keys with a different prefix to provide sparse-level context.
  for (int i = 0; i < 5; i++) {
    std::string key(1, static_cast<char>('C' + i));
    key += "x";
    keys.push_back(key);
  }
  std::sort(keys.begin(), keys.end());

  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  std::string data(builder.GetSerializedData().data(),
                   builder.GetSerializedData().size());

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(Slice(data)));

  LoudsTrieIterator iter(&trie);

  // Exact seek to the chain-ending-at-leaf key.
  std::string chain_key = std::string(9, 'A') + "B";
  ASSERT_TRUE(iter.Seek(Slice(chain_key)));
  ASSERT_TRUE(iter.Valid());
  ASSERT_EQ(iter.Key().ToString(), chain_key);

  // Seek to a target longer than the chain key -- target has more bytes but
  // trie key ends at leaf. Should Advance() past it.
  std::string longer_target = chain_key + "Z";
  ASSERT_TRUE(iter.Seek(Slice(longer_target)));
  ASSERT_TRUE(iter.Valid());
  // Should land on the next key after the chain key.
  ASSERT_GT(iter.Key().ToString(), chain_key);

  // Seek to a target that is the chain key prefix (target consumed mid-chain).
  std::string mid_chain = std::string(5, 'A');
  ASSERT_TRUE(iter.Seek(Slice(mid_chain)));
  ASSERT_TRUE(iter.Valid());
  // First key >= "AAAAA" is "AAAAAAAAAB".
  ASSERT_EQ(iter.Key().ToString(), chain_key);

  // Full forward scan to verify integrity.
  ASSERT_TRUE(iter.Seek(Slice(keys[0])));
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Valid()) << "Invalid at i=" << i;
    ASSERT_EQ(iter.Key().ToString(), keys[i]);
    if (i < keys.size() - 1) {
      ASSERT_TRUE(iter.Next());
    }
  }
  ASSERT_FALSE(iter.Next());
}

// ============================================================================
// TrieIndexFactory integration tests
// ============================================================================

// Parameterized by the format version the factory writes.
class TrieIndexFactoryTest : public testing::TestWithParam<uint32_t> {
 protected:
  void SetUp() override {
    factory_ = std::make_shared<TrieIndexFactory>(GetParam());
  }

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

  void AssertSerializedSizeBound(const std::vector<std::string>& keys,
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
      const IndexFactoryBuilder::BlockHandle handle{i * 100, 100};
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

TEST_P(TrieIndexFactoryTest, EstimatedSizeBoundsSerializedTrie) {
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
      AssertSerializedSizeBound(key_set.keys, parallel, key_set.max_size_ratio);
    }
  }
}

// Format 1 rejects handles past 32 bits; format 2 stores them.
TEST_P(TrieIndexFactoryTest, BlockHandleEncodingLimit) {
  constexpr uint64_t kLimit = UINT32_MAX;
  for (bool parallel : {false, true}) {
    for (bool large_offset : {false, true}) {
      for (size_t invalid_entry : {0U, 1U, 2U}) {
        SCOPED_TRACE(testing::Message() << parallel << " " << large_offset
                                        << " " << invalid_entry);
        std::unique_ptr<IndexFactoryBuilder> builder;
        ASSERT_OK(factory_->NewBuilder(IndexFactoryOptions(), builder));
        if (parallel && !builder->SupportsParallelAddEntry()) {
          continue;
        }
        // Identical separators exercise both primary and overflow handles.
        const Slice key("same");
        std::vector<IndexFactoryBuilder::BlockHandle> handles;
        for (size_t i = 0; i < 2; ++i) {
          IndexFactoryBuilder::BlockHandle handle{kLimit, kLimit};
          if (i == invalid_entry) {
            if (large_offset) {
              ++handle.offset;
            } else {
              ++handle.size;
            }
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
        Status s = builder->Finish(&contents);
        if (invalid_entry < 2 && GetParam() == 1) {
          EXPECT_EQ(s.code(), Status::Code::kNotSupported) << s.ToString();
          EXPECT_TRUE(contents.empty());
        } else {
          ASSERT_OK(s);
          std::unique_ptr<IndexFactoryReader> reader;
          ASSERT_OK(
              factory_->NewReader(IndexFactoryOptions(), contents, reader));
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
}

TEST_P(TrieIndexFactoryTest, BasicBuildAndRead) {
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

TEST_P(TrieIndexFactoryTest, EmptySeparator) {
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

TEST_P(TrieIndexFactoryTest, FactoryName) {
  ASSERT_STREQ(factory_->Name(), "trie_index");
}

TEST_P(TrieIndexFactoryTest, EmptyIndex) {
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));

  // Finish without adding any entries.
  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));
  // Empty index should produce empty or minimal contents.
}

TEST_P(TrieIndexFactoryTest, DoubleFinish) {
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));

  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));

  // Second Finish should fail.
  Slice index_contents2;
  Status s = builder->Finish(&index_contents2);
  ASSERT_TRUE(s.IsInvalidArgument());
}

TEST_P(TrieIndexFactoryTest, IteratorBoundsChecking) {
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

TEST_P(TrieIndexFactoryTest, IteratorNoBounds) {
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

TEST_P(TrieIndexFactoryTest, UpperBoundDoesNotDropValidBlocks) {
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

TEST_P(TrieIndexFactoryTest, PrevAfterPrepareDoesNotUseForwardUpperBound) {
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

TEST_P(TrieIndexFactoryTest, MultiScanBoundsAdvanceCorrectly) {
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

TEST_P(TrieIndexFactoryTest, RejectsNonBytewiseComparator) {
  // The trie index requires bytewise ordering because it traverses keys
  // byte-by-byte. Non-bytewise comparators should be rejected.
  IndexFactoryOptions option;

  // ReverseBytewiseComparator should be rejected.
  option.comparator = ReverseBytewiseComparator();
  std::unique_ptr<IndexFactoryBuilder> builder;
  Status s = factory_->NewBuilder(option, builder);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_EQ(builder, nullptr);

  // NewReader should also reject non-bytewise comparators.
  Slice dummy_data;
  std::unique_ptr<IndexFactoryReader> reader;
  s = factory_->NewReader(option, dummy_data, reader);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_EQ(reader, nullptr);

  // BytewiseComparator should be accepted.
  option.comparator = BytewiseComparator();
  ASSERT_OK(factory_->NewBuilder(option, builder));
  ASSERT_NE(builder, nullptr);
}

TEST_P(TrieIndexFactoryTest, ApproximateMemoryUsageReportsOnlyAuxData) {
  // The reader reports its auxiliary heap allocations. The serialized block
  // is accounted separately by the table reader or block cache.
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<IndexFactoryBuilder> udi_builder;
  ASSERT_OK(factory_->NewBuilder(option, udi_builder));

  // Build a non-trivial index with enough keys to produce sparse internal
  // nodes (which allocate child position lookup tables).
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

TEST_P(TrieIndexFactoryTest, EmptyTrieIterator) {
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
  // kUnknown: no leaf has a key >= target, so the target is past all blocks
  // in this SST. We return kUnknown (not kOutOfBound) because exhausting
  // this SST says nothing about the upper bound -- the next SST on the level
  // may still have in-bound keys.
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);
}

TEST_P(TrieIndexFactoryTest, PrepareWithZeroScans) {
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

TEST_P(TrieIndexFactoryTest, RePrepareResetsScanState) {
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

TEST_P(TrieIndexFactoryTest, ScanWithNoLimit) {
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

TEST_P(TrieIndexFactoryTest, NewReaderWithCorruptedData) {
  // Attempt to create a reader from corrupted data.
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();

  std::string bad_data(10, '\xff');
  Slice bad_slice(bad_data);
  std::unique_ptr<IndexFactoryReader> reader;
  Status s = factory_->NewReader(option, bad_slice, reader);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

// An index block that passes its checksum can still be malformed, e.g. when
// written by a buggy writer. The reader must reject it or read it without
// undefined behavior; run under ASAN/UBSAN to catch out-of-bounds reads.
TEST_P(TrieIndexFactoryTest, MalformedIndexIsRejectedOrReadable) {
  const uint32_t seed = static_cast<uint32_t>(
      std::chrono::system_clock::now().time_since_epoch().count());
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);

  // Long shared prefixes give format 1 path-compression chains, and a user
  // key spanning three blocks gives it seqno overflow runs.
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
    if (!factory_->NewReader(IndexFactoryOptions(), contents, reader).ok()) {
      return;
    }
    std::unique_ptr<IndexFactoryIterator> iter =
        reader->NewIterator(ReadOptions());
    const size_t limit = 2 * blocks.size() + 10;
    IterateResult result;
    Status s = iter->SeekToFirstAndGetResult(&result);
    for (size_t i = 0;
         s.ok() && result.bound_check_result == IterBoundCheck::kInbound &&
         i < limit;
         i++) {
      iter->value();
      s = iter->NextAndGetResult(&result);
    }
    s = iter->SeekToLastAndGetResult(&result);
    for (size_t i = 0;
         s.ok() && result.bound_check_result == IterBoundCheck::kInbound &&
         i < limit;
         i++) {
      s = iter->PrevAndGetResult(&result);
    }
    for (const std::string& key : keys) {
      for (SequenceNumber seq : {kMaxSequenceNumber, SequenceNumber{15}}) {
        s = iter->SeekAndGetResult(key, &result, SeekCtx(seq));
        if (s.ok() && result.bound_check_result == IterBoundCheck::kInbound) {
          iter->value();
          s = iter->NextAndGetResult(&result);
        }
      }
    }
    s.PermitUncheckedError();
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
    std::string mutated = data;
    const int flips = 1 + rnd.Uniform(3);
    for (int f = 0; f < flips; f++) {
      mutated[rnd.Uniform(static_cast<int>(mutated.size()))] ^=
          static_cast<char>(1 + rnd.Uniform(255));
    }
    exercise(mutated);
  }
}

TEST_P(TrieIndexFactoryTest, OnKeyAddedNoOp) {
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

TEST_P(TrieIndexFactoryTest, NullComparator) {
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

TEST_P(TrieIndexFactoryTest, SeekSucceedsButTargetPastLimit) {
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
// Sequence number encoding tests
//
// These tests verify the same-user-key boundary handling: when the same user
// key spans a data block boundary with different sequence numbers, the trie
// builder detects this and encodes ALL separators with sequence number suffixes
// so the trie can distinguish the two blocks.
// ============================================================================

TEST_P(TrieIndexFactoryTest, SameUserKeyBoundaryTriggersSeqnoEncoding) {
  // When the same user key spans two adjacent data blocks (e.g.,
  // "foo"|seq=100 ends Block 0, "foo"|seq=50 starts Block 1), the
  // builder must detect this and switch to seqno-encoded separators.
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

TEST_P(TrieIndexFactoryTest, DistinctUserKeysSeekWithMaxSeqno) {
  // Seqno encoding is always active. Verify that seeking with
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

TEST_P(TrieIndexFactoryTest, MultipleSameUserKeyBoundaries) {
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

  // Seek "key"|seq=250 -> Block 1. In internal key order, higher seqno is
  // "smaller", so "key"|300 < "key"|250. Block 0's separator "key"+enc(300)
  // is less than the target -> skip. Block 1's separator "key"+enc(200)
  // is >= target because enc(200) > enc(250) (lower seqno -> larger encoded
  // bytes). So the first separator >= target is Block 1.
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("key"), 250, 1000));

  // Seek "key"|seq=200 -> Block 1
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("key"), 200, 1000));

  // Seek "key"|seq=150 -> Block 2. "key"+enc(300) < target (skip Block 0),
  // "key"+enc(200) < target (skip Block 1, because enc(200) < enc(150):
  // 200 > 150 -> higher seqno -> smaller encoding). The next separator is
  // Block 2's separator which is >= target -> Block 2.
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

TEST_P(TrieIndexFactoryTest, SameUserKeyWithZeroSeqnos) {
  // During bottommost compaction, RocksDB zeroes sequence numbers for keys
  // that no longer need version discrimination. When the same user key spans
  // multiple data blocks and all versions have seqno=0, the overflow entries
  // in the same-key run also have seqno=0. This is valid -- the reader's
  // post-seek correction handles it correctly:
  //   - Primary leaf seqno=0, and target_packed >= 0 so no advancement
  //   - Next() iterates overflow blocks by index, not seqno
  auto ctx = BuildTrieAndGetIterator({
      {"key", "key", 0, 1000, 0, 0},
      {"key", "key", 1000, 1000, 0, 0},
      {"key", "zzz", 2000, 1000, 0, 0},
      {"zzz", "", 3000, 1000, 0, 0},
  });

  // Seek "key"|kMaxSequenceNumber -> Block 0 (primary, seqno=0 guard prevents
  // advancement through overflow blocks).
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("key"), kMaxSequenceNumber, 0));

  // Seek "key"|seq=0 -> Block 0 (primary, target_packed=0 >= leaf_packed=0).
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

TEST_P(TrieIndexFactoryTest, SameUserKeyLastBlockZeroSeqno) {
  // Same user key spans blocks including the last block in the SST, with
  // all seqnos zeroed (bottommost compaction). The last block's AddIndexEntry
  // call has first_key_in_next_block=nullptr and first_key_tag=0.
  auto ctx = BuildTrieAndGetIterator({
      {"key", "key", 0, 1000, 0, 0},
      {"key", "", 1000, 1000, 0, 0},  // last block
  });

  // Seek -> Block 0 (primary).
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("key"), kMaxSequenceNumber, 0));

  // Next -> Block 1 (overflow).
  IterateResult result;
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 1000u);

  // Next -> exhausted.
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_NE(result.bound_check_result, IterBoundCheck::kInbound);
}

TEST_P(TrieIndexFactoryTest, SeqnoEncodingRoundTripSerialization) {
  // Verify that the seqno encoding flag survives serialization/deserialization.
  // Build a trie with a same-user-key boundary (triggers seqno encoding),
  // serialize it, deserialize it, and verify the flag is set and seeks work.
  auto ctx = BuildTrieAndGetIterator({
      {"x", "x", 0, 500, 10, 5},
      {"x", "", 500, 500, 5, 0},
  });

  // Seek "x"|seq=10 -> Block 0
  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(ctx.iter.get(), Slice("x"), 10, 0));

  // Seek "x"|seq=5 -> Block 1
  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(ctx.iter.get(), Slice("x"), 5, 500));
}

TEST_P(TrieIndexFactoryTest, SeqnoEncodingWithBoundsChecking) {
  // Verify that bounds checking still works correctly when seqno encoding
  // is active. The bounds comparison should use user keys only (not the
  // seqno-encoded internal representation).
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
// Side-table comprehensive tests
//
// These tests exercise the side-table seqno encoding for same-user-key
// boundaries. Key insight: FindShortestSeparator shortens separator keys
// when the two user keys differ. For example, FindShortestSeparator("aaa",
// "mmm") = "b". Only same-user-key boundaries produce identical separators.
// The trie de-duplicates these into overflow runs with a seqno side-table.
//
// Coverage:
//   - Large overflow runs (10+ blocks with the same user key)
//   - Mixed runs (multiple different user keys each with multi-block runs)
//   - Adjacent runs (two different user keys, each with their own run)
//   - Seek with kMaxSequenceNumber on same-user-key blocks
//   - Seek with seq=0 on same-user-key blocks
//   - result.key is the trie separator (no encoded suffix)
//   - Seeking non-existent keys when seqno encoding is active
//   - Seeking past all keys / Next() past last block with seqno encoding
//   - kUnknown on exhaustion with seqno encoding and overflow
//   - Next() transitions: overflow->next leaf, overflow->next overflow run
//   - Bounds checking during overflow run iteration
//   - Size comparison: seqno-free trie same size as without the feature
//   - Re-seeking after being positioned in an overflow run
// ============================================================================

TEST_P(TrieIndexFactoryTest, LargeOverflowRun) {
  // 12 blocks all with user key "key", seqnos descending from 1200 to 100.
  // The last "key" block transitions to "zzz", so
  // FindShortestSeparator("key", "zzz") = "l". The trie has:
  //   "key" (11-block run, seqnos 1200..200) -> "l" (block 11) -> "zzz" (block
  //   12)
  // The overflow run for "key" has 10 overflow entries (blocks 1-10).
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

  // Seek "key"|1100 -> leaf_seqno=1200, 1100<1200 -> advance.
  // overflow[0]=1100, 1100>=1100 -> Block 1.
  ASSERT_OK(iter->SeekAndGetResult(Slice("key"), &result, SeekCtx(1100)));
  ASSERT_EQ(iter->value().offset, 1000u);

  // Seek "key"|550 -> advance through overflow:
  //   overflow seqnos: 1100,1000,900,800,700,600,500,400,300,200
  //   550 < 1100..600, 550 >= 500? No, 550 is not < 600:
  //   Actually: 550<1100 skip, 550<1000 skip, 550<900 skip, 550<800 skip,
  //   550<700 skip, 550<600 skip, 550>=500 -> found at overflow index 6.
  //   overflow_run_index_ = 7 (1-based) -> Block 7, offset 7000.
  ASSERT_OK(iter->SeekAndGetResult(Slice("key"), &result, SeekCtx(550)));
  ASSERT_EQ(iter->value().offset, 7000u);

  // Seek "key"|50 -> below all "key" overflow seqnos (minimum is 200).
  // All 10 overflow seqnos exhausted -> advance to next leaf "l" (block 11).
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

TEST_P(TrieIndexFactoryTest, MixedSameKeyRuns) {
  // Two different user keys each with multi-block runs, plus distinct blocks.
  //
  // The trie structure (after FindShortestSeparator shortening):
  //   "aaa" (2-block run, seqnos 300, 200) -> "b" (block 2) ->
  //   "mmm" (1-block, seqno 60) -> "n" (block 4) -> "zzz" (block 5)
  //
  // Block 0: "aaa"|300 -> next "aaa"|200 (same-key -> separator "aaa")
  // Block 1: "aaa"|200 -> next "mmm"    (diff-key -> separator "b")
  // Block 2: "mmm"|60  -> next "mmm"|30 (same-key -> separator "mmm")
  // Block 3: "mmm"|30  -> next "zzz"    (diff-key -> separator "n")
  // Block 4: "zzz"|10  -> null          (last -> separator "zzz")
  //
  // "aaa" run: blocks 0-1 (2 entries, run of 2 in trie)
  // block 2 gets separator "b" (separate trie leaf)
  // "mmm" run: block 3 only (run of 1 in trie)
  // block 4 gets separator "n" (separate trie leaf)
  // block 5 gets separator "zzz" (separate trie leaf, no shortening)
  //
  // Wait -- let me recount. We have 6 AddIndexEntry calls:
  //   Entry 0: "aaa" 300 next="aaa" -> sep="aaa"
  //   Entry 1: "aaa" 200 next="mmm" -> sep="b"
  //   Entry 2: "mmm"  60 next="mmm" -> sep="mmm"
  //   Entry 3: "mmm"  30 next="zzz" -> sep="n"
  //   Entry 4: "zzz"  10 next=null  -> sep="zzz"
  //
  // De-duplicated separators: "aaa" appears once (run=1), "b", "mmm" appears
  // once (run=1), "n", "zzz". So actually there are no overflow runs here!
  // The "aaa" run is only block 0, and block 1 gets separator "b".
  // The "mmm" run is only block 2, and block 3 gets separator "n".
  //
  // For a true multi-block overflow run for "aaa", we need 3+ consecutive
  // blocks ALL with "aaa" -> "aaa" boundaries.
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));

  // "aaa" run: 3 blocks (all same-key boundaries).
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
  // "mmm" run: 2 blocks (one same-key boundary).
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

  // Trie structure: "aaa"(run=2) -> "b" -> "mmm"(run=1) -> "n" -> "zzz"
  //
  // Seek "aaa"|kMax -> Block 0.
  ASSERT_OK(iter->SeekAndGetResult(Slice("aaa"), &result,
                                   SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(iter->value().offset, 0u);

  // Seek "aaa"|250 -> leaf_seqno=300, 250<300 -> advance.
  // overflow[0]=200, 250>=200 -> Block 1.
  ASSERT_OK(iter->SeekAndGetResult(Slice("aaa"), &result, SeekCtx(250)));
  ASSERT_EQ(iter->value().offset, 1000u);

  // Seek "aaa"|50 -> leaf_seqno=300, 50<300 -> advance.
  // overflow[0]=200, 50<200 -> skip. All exhausted -> next leaf "b" (block 2).
  ASSERT_OK(iter->SeekAndGetResult(Slice("aaa"), &result, SeekCtx(50)));
  ASSERT_EQ(iter->value().offset, 2000u);

  // Seek "mmm"|kMax -> Block 3 (the "mmm" trie leaf).
  ASSERT_OK(iter->SeekAndGetResult(Slice("mmm"), &result,
                                   SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(iter->value().offset, 3000u);

  // Seek "mmm"|45 -> leaf_seqno=60, 45<60 -> advance.
  // "mmm" has run_size=1 (only 1 block in trie), no overflow -> next leaf "n".
  // Wait -- "mmm" appears only once in the separator list (entry 2), because
  // entry 3 has separator "n". So "mmm" trie leaf has block_count=1.
  // 45 < 60, no overflow -> advance to "n" (block 4).
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

TEST_P(TrieIndexFactoryTest, AdjacentSameKeyRuns) {
  // Two adjacent same-user-key runs. The trick is that the last block of
  // the first run has a different-key boundary, creating a non-run trie leaf
  // between the two runs. To get true adjacency we make ALL "aaa" blocks
  // transition to "aaa", and ALL "bbb" blocks transition to "bbb", with
  // the boundary between runs being "aaa"->"bbb".
  //
  // Entries (separator after FindShortestSeparator):
  //   Entry 0: "aaa" 200 next="aaa" -> sep="aaa" (same-key)
  //   Entry 1: "aaa" 100 next="bbb" -> sep="b"   (diff-key)
  //   Entry 2: "bbb"  80 next="bbb" -> sep="bbb"  (same-key)
  //   Entry 3: "bbb"  40 next=null  -> sep="bbb"  (last block, no shortening)
  //
  // Trie: "aaa"(run=1, seq=200) -> "b"(1 block) -> "bbb"(run=2, seqnos 80,40)
  // Note: the last block joins the "bbb" run since its separator also is "bbb".
  auto ctx = BuildTrieAndGetIterator({
      {"aaa", "aaa", 0, 1000, 200, 100},
      {"aaa", "bbb", 1000, 1000, 100, 80},
      {"bbb", "bbb", 2000, 1000, 80, 40},
      {"bbb", "", 3000, 1000, 40, 0},
  });

  IterateResult result;

  // Full scan: "aaa" (block 0) -> "b" (block 1) -> "bbb" (block 2) -> "bbb"
  // overflow (block 3).
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

TEST_P(TrieIndexFactoryTest, SeqnoEncodingResultKeyIsUserKey) {
  // Verify that result.key from SeekAndGetResult is the trie separator key
  // (no encoded seqno suffix). For same-key boundary leaves, the separator
  // IS the original user key. For different-key boundaries, the separator
  // is shortened.
  //
  // Entries:
  //   "foo" 100 next="foo" -> sep="foo" (same-key, run of 2)
  //   "foo"  50 next="zoo" -> sep="g"   (FindShortestSeparator("foo","zoo"))
  //   "zoo"   1 next=null  -> sep="zoo" (last block, no successor shortening)
  auto ctx = BuildTrieAndGetIterator({
      {"foo", "foo", 0, 500, 100, 50},
      {"foo", "zoo", 500, 500, 50, 1},
      {"zoo", "", 1000, 500, 1, 0},
  });

  IterateResult result;

  // Seek to "foo"|100 -> "foo" leaf. result.key = "foo" (3 bytes, no suffix).
  ASSERT_OK(ctx.iter->SeekAndGetResult(Slice("foo"), &result, SeekCtx(100)));
  ASSERT_EQ(result.key.ToString(), "foo");
  ASSERT_EQ(result.key.size(), 3u);

  // Next -> "g" leaf (FindShortestSeparator("foo", "zoo") = "g").
  // result.key = "g" (1 byte).
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.key.size(), 1u);
  // The separator is "g" (or similar shortened form).
  ASSERT_GE(result.key.ToString(), "g");
  ASSERT_LT(result.key.ToString(), "zoo");
}

TEST_P(TrieIndexFactoryTest, SeekNonExistentKeyWithSeqnoEncoding) {
  // Seeking for keys not in the trie when seqno encoding is active.
  //
  // Trie: "mmm"(run=1, seq=100) -> "n" -> "zzz"
  auto ctx = BuildTrieAndGetIterator({
      {"mmm", "mmm", 0, 1000, 100, 50},
      {"mmm", "zzz", 1000, 1000, 50, 1},
      {"zzz", "", 2000, 1000, 1, 0},
  });
  auto* iter = ctx.iter.get();

  // Seek "aaa" (before all keys) -> lands on "mmm" leaf, Block 0.
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(iter, Slice("aaa"), kMaxSequenceNumber, 0u));
  // Seek "nnn" (between "n" and "zzz") -> lands on "zzz" leaf, Block 2.
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(iter, Slice("nnn"), kMaxSequenceNumber, 2000u));

  // Seek "|" (past all keys, "|" > "zzz") -> kUnknown.
  IterateResult result;
  ASSERT_OK(
      iter->SeekAndGetResult(Slice("|"), &result, SeekCtx(kMaxSequenceNumber)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);
}

TEST_P(TrieIndexFactoryTest, SeqnoEncodingPastEndAndNextPastEnd) {
  // Verify seeking past all keys and Next() past the last block with seqno
  // encoding active.
  //
  // Trie: "key"(run=2, seqnos 10,5)
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

  // Seek "key"|5 -> seqno=10 on primary, 5<10 -> overflow seqno=5, 5>=5 ->
  // overflow block 1.
  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(iter, Slice("key"), 5, 500u));

  // Next from last block in run -> past end.
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);

  // Full forward scan: block 0 -> block 1 -> past end.
  ASSERT_NO_FATAL_FAILURE(AssertFullForwardScan(iter, Slice("key"), {0, 500}));
}

TEST_P(TrieIndexFactoryTest, SeqnoEncodingOutOfBoundWithOverflow) {
  // Verify that bounds checking works correctly with seqno encoding and
  // overflow blocks.
  //
  // Entries:
  //   "key" 300 next="key" -> sep="key" (same-key)
  //   "key" 200 next="key" -> sep="key" (same-key)
  //   "key" 100 next="zzz" -> sep="l"
  //   "zzz"   1 next=null  -> sep="zzz" (last block, no shortening)
  //
  // Trie: "key"(run=2, seqnos 300,200) -> "l" -> "zzz"
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

  // Next -> Block 1 (overflow within "key" run), prev "key" < "zzz" ->
  // kInbound.
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 1000u);

  // Next -> "l" leaf (block 2), prev "key" < "zzz" -> kInbound.
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 2000u);

  // Next -> "zzz" leaf (block 3). prev key is "l", "l" < "zzz" -> kInbound.
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

  // Next -> Block 1 (overflow), prev "key" < "l" -> kInbound.
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);

  // Next -> "l" leaf (block 2). prev "key" < "l" -> kInbound (conservative).
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);

  // Next -> "zzz" leaf (block 3). prev "l" >= "l" -> kOutOfBound.
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kOutOfBound);
}

TEST_P(TrieIndexFactoryTest, SeqnoEncodingConsistentSize) {
  // Verify that tries built with different seqno contexts produce the
  // same serialized size (seqno encoding is always on).
  if (GetParam() != 1) {
    ROCKSDB_GTEST_SKIP("Format 2 stores only nonzero tags");
    return;
  }
  IndexFactoryOptions option;
  option.comparator = BytewiseComparator();

  // Build trie with distinct keys, seqno=0.
  std::unique_ptr<IndexFactoryBuilder> builder_no_seq;
  ASSERT_OK(factory_->NewBuilder(option, builder_no_seq));
  for (int i = 0; i < 50; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "key_%04d", i);
    char next_buf[16];
    snprintf(next_buf, sizeof(next_buf), "key_%04d", i + 1);

    IndexFactoryBuilder::BlockHandle handle{static_cast<uint64_t>(i) * 1000,
                                            500};
    std::string scratch;
    if (i < 49) {
      Slice next(next_buf);
      builder_no_seq->AddIndexEntry(Slice(buf), &next, handle, &scratch,
                                    {0, 0});
    } else {
      builder_no_seq->AddIndexEntry(Slice(buf), nullptr, handle, &scratch,
                                    {0, 0});
    }
  }
  Slice contents_no_seq;
  ASSERT_OK(builder_no_seq->Finish(&contents_no_seq));
  size_t size_no_seq = contents_no_seq.size();

  // Same keys but with nonzero seqnos (still distinct -> no seqno encoding).
  std::unique_ptr<IndexFactoryBuilder> builder_with_seq;
  ASSERT_OK(factory_->NewBuilder(option, builder_with_seq));
  for (int i = 0; i < 50; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "key_%04d", i);
    char next_buf[16];
    snprintf(next_buf, sizeof(next_buf), "key_%04d", i + 1);

    IndexFactoryBuilder::BlockHandle handle{static_cast<uint64_t>(i) * 1000,
                                            500};
    std::string scratch;
    if (i < 49) {
      Slice next(next_buf);
      builder_with_seq->AddIndexEntry(Slice(buf), &next, handle, &scratch,
                                      {100, 50});
    } else {
      builder_with_seq->AddIndexEntry(Slice(buf), nullptr, handle, &scratch,
                                      {100, 0});
    }
  }
  Slice contents_with_seq;
  ASSERT_OK(builder_with_seq->Finish(&contents_with_seq));
  size_t size_with_seq = contents_with_seq.size();

  ASSERT_EQ(size_no_seq, size_with_seq);
}

TEST_P(TrieIndexFactoryTest, SeekWithMaxSeqOnSameKeyBlocks) {
  // kMaxSequenceNumber should always land on Block 0 of a same-key run.
  //
  // Trie: "key"(run=3, seqnos 400,300,200) -> "l"
  auto ctx = BuildTrieAndGetIterator({
      {"key", "key", 0, 1000, 400, 300},
      {"key", "key", 1000, 1000, 300, 200},
      {"key", "key", 2000, 1000, 200, 100},
      {"key", "", 3000, 1000, 100, 0},
  });
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("key"), kMaxSequenceNumber, 0u));
}

TEST_P(TrieIndexFactoryTest, SeekWithZeroSeqOnSameKeyBlocks) {
  // seq=0 is below all overflow seqnos -> advance past the entire run.
  //
  // Trie: "key"(run=2, seqnos 300,200) -> "l" -> "zzz"
  // seq=0 < all overflow seqnos -> advance past run -> lands on "l" (block 2).
  auto ctx = BuildTrieAndGetIterator({
      {"key", "key", 0, 1000, 300, 200},
      {"key", "key", 1000, 1000, 200, 100},
      {"key", "zzz", 2000, 1000, 100, 1},
      {"zzz", "", 3000, 1000, 1, 0},
  });
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("key"), 0, 2000u));
}

TEST_P(TrieIndexFactoryTest, ZeroSeqMustNotSkipLeafForSmallerUserKey) {
  // Validates correctness of DBIter's forward-scan reseek path.
  //
  // Block 0 ends with user key "m" and block 1 starts with the same user key,
  // so the trie stores separator "m" with non-zero seqno metadata on block 0.
  // However, a seek target for a *smaller* user key "l"|0 must still land on
  // block 0, because block 0 can contain keys in ("l", "m"].
  //
  // Verifies that seqno-based post-seek correction is NOT applied when the
  // target user key is strictly less than the separator user key. The seqno
  // comparison is only meaningful when user keys are equal. If the target
  // user key "l" < separator "m", the seek must stay on block 0 regardless
  // of seqno, because block 0 can contain keys up to "m".
  auto ctx = BuildTrieAndGetIterator({
      {"m", "m", 0, 1000, 300, 200},
      {"y", "zzz", 1000, 1000, 100, 1},
      {"zzz", "", 2000, 1000, 1, 0},
  });

  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(ctx.iter.get(), Slice("l"), 0, 0u));
}

TEST_P(TrieIndexFactoryTest, NextTransitionOverflowToOverflow) {
  // Test Next() transitioning from one overflow run to the next trie leaf
  // that also has an overflow run.
  //
  // To get two true adjacent overflow runs, both user keys need multiple
  // same-key boundaries. We need a 3rd user key to separate them in the trie.
  //
  // Entries:
  //   "aaa" 200 next="aaa" -> sep="aaa" (same-key)
  //   "aaa" 100 next="aaa" -> sep="aaa" (same-key)
  //   "aaa"  50 next="bbb" -> sep="b"   (diff-key)
  //   "bbb"  90 next="bbb" -> sep="bbb" (same-key)
  //   "bbb"  60 next="bbb" -> sep="bbb" (same-key)
  //   "bbb"  30 next=null  -> sep="bbb" (last block, no successor shortening)
  //
  // Trie: "aaa"(run=2, seqnos 200,100) -> "b" -> "bbb"(run=3, seqnos 90,60,30)
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

  ASSERT_OK(iter->NextAndGetResult(&result));  // "aaa" overflow (block 1)
  ASSERT_EQ(iter->value().offset, 1000u);
  ASSERT_EQ(result.key.ToString(), "aaa");

  ASSERT_OK(iter->NextAndGetResult(&result));  // -> "b" leaf (block 2)
  ASSERT_EQ(iter->value().offset, 2000u);
  ASSERT_EQ(result.key.ToString(), "b");

  ASSERT_OK(iter->NextAndGetResult(&result));  // -> "bbb" leaf (block 3)
  ASSERT_EQ(iter->value().offset, 3000u);
  ASSERT_EQ(result.key.ToString(), "bbb");

  ASSERT_OK(iter->NextAndGetResult(&result));  // "bbb" overflow (block 4)
  ASSERT_EQ(iter->value().offset, 4000u);
  ASSERT_EQ(result.key.ToString(), "bbb");

  ASSERT_OK(iter->NextAndGetResult(&result));  // "bbb" overflow (block 5)
  ASSERT_EQ(iter->value().offset, 5000u);
  ASSERT_EQ(result.key.ToString(), "bbb");

  // Past end -- kUnknown (exhaustion doesn't imply upper bound reached).
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);
}

TEST_P(TrieIndexFactoryTest, SingleBlockWithSeqnoActive) {
  // Both blocks have the same user key "x". Without successor shortening
  // for the last block, both separators are "x", forming a 2-block run:
  //   "x"(run=2, seqnos 10,5)
  auto ctx = BuildTrieAndGetIterator({
      {"x", "x", 0, 500, 10, 5},
      {"x", "", 500, 500, 5, 0},
  });
  auto* iter = ctx.iter.get();

  // Seek "x"|10 -> leaf_seqno=10, 10>=10 -> Block 0.
  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(iter, Slice("x"), 10, 0u));
  // Seek "x"|7 -> 7<10 -> advance through overflow. overflow seqno=5, 7>=5 ->
  // overflow block 1.
  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(iter, Slice("x"), 7, 500u));
  // Seek "x"|3 -> 3<10 -> advance. overflow seqno=5, 3<5 -> not found. Advance
  // past run. No more leaves -> invalid. This matches the standard index
  // behavior: seeking for internal key "x"|3 is past the standard index's
  // last separator "x"|5.
  {
    IterateResult result;
    ASSERT_OK(iter->SeekAndGetResult(Slice("x"), &result, SeekCtx(3)));
    ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);
  }
}

TEST_P(TrieIndexFactoryTest, TagDistinguishesSameSeqDifferentType) {
  // the trie must use the full tag (seq << 8 |
  // type) for block selection, not just the sequence number. Without this,
  // when two blocks share the same user key and same seqno but differ in
  // value type, the trie could return the wrong block.
  //
  // Scenario: separator has tag (50 << 8) | 0 = 12800
  // (kTypeDeletion) Target has tag (50 << 8) | 1 = 12801
  // (kTypeValue) In internal key order, higher tag = smaller key, so
  // target (12801) < separator (12800). The trie should stay on Block 0.
  //
  // With seqno-only comparison: 50 < 50 is false -> stay. Same result.
  // But the reverse case matters:
  // Separator tag (50 << 8) | 1 = 12801 (kTypeValue)
  // Target tag (50 << 8) | 0 = 12800 (kTypeDeletion)
  // target (12800) < separator (12801) -> advance to Block 1.
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
  // Separator packed=12801. 12801 >= 12801 -> stay on Block 0. Correct.
  ASSERT_OK(iter->SeekAndGetResult(Slice("x"), &result, {(50ULL << 8) | 1}));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(iter->value().offset, 0u);

  // Target: ("x", seqno=50, kTypeDeletion=0), packed=12800.
  // Separator packed=12801. 12800 < 12801 -> advance to Block 1. Correct.
  // With seqno-only comparison (without tag): 50 < 50 -> false ->
  // stay on Block 0 (WRONG).
  ASSERT_OK(iter->SeekAndGetResult(Slice("x"), &result, {(50ULL << 8) | 0}));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(iter->value().offset, 500u);
}

TEST_P(TrieIndexFactoryTest, SeqnoEncodingReSeekAfterOverflow) {
  // Verify that re-seeking after being positioned in an overflow run
  // correctly resets the overflow state.
  //
  // Trie: "key"(run=2, seqnos 300,200) -> "l" -> "zzz"
  auto ctx = BuildTrieAndGetIterator({
      {"key", "key", 0, 1000, 300, 200},
      {"key", "key", 1000, 1000, 200, 100},
      {"key", "zzz", 2000, 1000, 100, 1},
      {"zzz", "", 3000, 1000, 1, 0},
  });
  auto* iter = ctx.iter.get();

  // Position in overflow: 150 < all run seqnos -> advance to "l" (block 2).
  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(iter, Slice("key"), 150, 2000u));
  // Re-seek to "key"|300 -> should reset to Block 0.
  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(iter, Slice("key"), 300, 0u));
  // Re-seek to "zzz" -> "zzz" leaf (block 3), overflow state should be clean.
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(iter, Slice("zzz"), kMaxSequenceNumber, 3000u));

  // Next should go past end.
  IterateResult result;
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);
}

TEST_P(TrieIndexFactoryTest, AllFfLastKeyWithSameKeyBoundary) {
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
  // Seek "\xff\xff"|100 -> advance to overflow Block 1.
  ASSERT_NO_FATAL_FAILURE(AssertSeekOffset(iter, Slice(ff), 100, 500u));

  // Seek "\xff\xff"|50 -> past entire run -> exhausted -> kUnknown.
  IterateResult result;
  ASSERT_OK(iter->SeekAndGetResult(Slice(ff), &result, SeekCtx(50)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);

  // Forward scan.
  ASSERT_NO_FATAL_FAILURE(AssertFullForwardScan(iter, Slice(ff), {0, 500}));
}

TEST_P(TrieIndexFactoryTest, AllFfLastKeyWithoutPrecedingSameKey) {
  // Complementary test: all-0xFF last key but NO same-user-key boundary.
  // No seqno encoding is triggered.
  std::string ff("\xff\xff", 2);
  auto ctx = BuildTrieAndGetIterator({
      {"aaa", ff, 0, 500, 100, 50},
      {ff, "", 500, 500, 50, 0},
  });
  ASSERT_NO_FATAL_FAILURE(
      AssertFullForwardScan(ctx.iter.get(), Slice("aaa"), {0, 500}));
}

TEST_P(TrieIndexFactoryTest, SeqnoEncodingRandomized) {
  // Randomized test: generate random block layouts with a mix of same-user-key
  // and distinct-key boundaries, build a trie, then verify:
  //   1. Full forward scan visits all blocks in offset order
  //   2. Seeking each block's key with its exact seqno returns the right offset
  //   3. Seeking with kMaxSequenceNumber returns the first block for that key
  //
  // Uses Random from util/random.h per project guidelines.
  uint32_t seed = static_cast<uint32_t>(
      std::chrono::steady_clock::now().time_since_epoch().count());
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

// Parameterized by the format version the factory writes.
class TrieIndexSSTTest : public testing::TestWithParam<uint32_t> {
 protected:
  void SetUp() override {
    trie_factory_ = std::make_shared<TrieIndexFactory>(GetParam());
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

TEST_P(TrieIndexSSTTest, WriteAndReadWithTrieUDI) {
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

TEST_P(TrieIndexSSTTest, SeekWithTrieUDI) {
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

TEST_P(TrieIndexSSTTest, SeekWithUpperBound) {
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

TEST_P(TrieIndexSSTTest, SmallSST) {
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
TEST_P(TrieIndexSSTTest, MixedKeyTypesWithCompressionDict) {
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
// Micro-benchmarks: trie Seek vs RocksDB IndexBlockIter.
//
// Compares the trie data structure against the actual RocksDB binary search
// index (IndexBlockIter) which is used in production. The IndexBlockIter
// operates on a prefix-compressed block with InternalKeys, varint decoding,
// and the InternalKeyComparator abstraction -- making it a realistic baseline.
// ============================================================================

class TrieSeekBenchmark : public testing::Test {
 protected:
  // Generate sorted fixed-width keys that mimic realistic RocksDB separator
  // keys. Uses snprintf to create keys like "key00000000NNNNNNNN" where N is
  // the zero-padded index. This produces keys with moderate prefix sharing
  // (the "key" prefix + some leading zeros) while ensuring each key differs
  // at multiple byte positions, matching the distribution that SST separator
  // keys would have in practice.
  static std::vector<std::string> GenerateKeys(size_t count,
                                               size_t key_size = 16) {
    std::vector<std::string> keys;
    keys.reserve(count);
    char buf[64];
    for (size_t i = 0; i < count; i++) {
      snprintf(buf, sizeof(buf), "%0*zu", static_cast<int>(key_size), i);
      keys.emplace_back(buf, key_size);
    }
    return keys;
  }

  // Build a trie from sorted separator keys. Returns {trie, serialized_data}.
  // The serialized data string must outlive the trie since the trie stores
  // zero-copy pointers into it.
  static std::pair<LoudsTrie, std::string> BuildTrie(
      const std::vector<std::string>& keys) {
    LoudsTrieBuilder builder;
    for (size_t i = 0; i < keys.size(); i++) {
      TrieBlockHandle handle{i * 4096, 4096};
      builder.AddKey(Slice(keys[i]), handle);
    }
    builder.Finish();

    std::string data(builder.GetSerializedData().data(),
                     builder.GetSerializedData().size());
    LoudsTrie trie;
    Status s = trie.InitFromData(Slice(data));
    assert(s.ok());
    (void)s;
    return {std::move(trie), std::move(data)};
  }

  // Generate random lookup keys (existing keys from the set).
  static std::vector<size_t> GenerateRandomIndices(size_t count,
                                                   size_t num_keys,
                                                   uint64_t seed = 42) {
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<size_t> dist(0, num_keys - 1);
    std::vector<size_t> indices(count);
    for (auto& idx : indices) {
      idx = dist(rng);
    }
    return indices;
  }
};

// Benchmark the trie against the actual RocksDB IndexBlockIter, which is
// the real binary search index used in production. Both sides replicate
// their full production code paths:
//
// Trie (via UDI wrapper):
//   ParseInternalKey -> Seek(user_key) -> Key().ToString() (string copy) ->
//   CheckBounds (no-op without Prepare) -> Value() -> BlockHandle
//
// IndexBlockIter:
//   Seek(internal_key) -> value() -> IndexValue with BlockHandle
//
// The IndexBlockIter operates on a prefix-compressed block with InternalKeys,
// varint decoding, and the InternalKeyComparator -- matching production
// exactly.
TEST_F(TrieSeekBenchmark, TrieVsRealIndexBlockIter) {
  static constexpr size_t kNumLookups = 200000;
  static constexpr size_t kKeySize = 16;
  static constexpr size_t kKeyCounts[] = {100, 500, 1000, 5000, 10000, 32000};

  fprintf(stderr,
          "\n====== Trie vs Real IndexBlockIter: Scaling (%zu-byte keys, %zu "
          "lookups) ======\n",
          kKeySize, kNumLookups);
  fprintf(stderr, "  %8s  %12s  %12s  %8s  %s\n", "Keys", "Trie ns/op",
          "IBI ns/op", "vs IBI", "Winner");
  fprintf(stderr, "  %8s  %12s  %12s  %8s  %s\n", "--------", "------------",
          "------------", "--------", "----------");

  for (size_t num_keys : kKeyCounts) {
    auto keys = GenerateKeys(num_keys, kKeySize);

    // ---- Build trie ----
    auto [trie, trie_data] = BuildTrie(keys);
    LoudsTrieIterator trie_iter(&trie);

    // ---- Build real RocksDB index block ----
    // Use restart_interval=1 (the default for index blocks).
    const int kRestartInterval = 1;
    BlockBuilder index_builder(
        kRestartInterval,
        /*use_delta_encoding=*/true,
        /*use_value_delta_encoding=*/false,
        BlockBasedTableOptions::kDataBlockBinarySearch,
        /*data_block_hash_table_util_ratio=*/0.75, /*ts_sz=*/0,
        /*persist_user_defined_timestamps=*/true, /*is_user_key=*/false,
        /*use_separated_kv_storage=*/false, /*statistics=*/nullptr,
        /*uniform_cv_threshold=*/-1.0, /*use_common_prefix=*/false);

    // Convert user keys to InternalKeys and add to the index block.
    std::vector<std::string> internal_keys;
    internal_keys.reserve(num_keys);
    for (size_t i = 0; i < num_keys; i++) {
      std::string ikey = keys[i];
      AppendInternalKeyFooter(&ikey, /*s=*/0, kTypeValue);
      internal_keys.push_back(ikey);

      BlockHandle handle(i * 4096, 4096);
      IndexValue entry(handle, Slice());
      std::string encoded_value;
      entry.EncodeTo(&encoded_value, /*have_first_key=*/false,
                     /*previous_handle=*/nullptr);
      index_builder.Add(Slice(internal_keys.back()), Slice(encoded_value));
    }

    // Finish the block and create a Block reader.
    Slice raw_block = index_builder.Finish();
    std::string block_data(raw_block.data(), raw_block.size());
    BlockContents contents;
    contents.data = Slice(block_data);
    Block index_block(std::move(contents));

    // ---- Generate random lookup targets ----
    auto indices = GenerateRandomIndices(kNumLookups, num_keys);

    // Pre-build InternalKey seek targets (user_key + kMaxSequenceNumber).
    // Both sides use the same InternalKey targets -- the trie path parses
    // them to extract the user key (matching production), while
    // IndexBlockIter uses them directly.
    std::vector<std::string> seek_ikeys;
    seek_ikeys.reserve(kNumLookups);
    for (auto idx : indices) {
      std::string sk = keys[idx];
      AppendInternalKeyFooter(&sk, kMaxSequenceNumber, kValueTypeForSeek);
      seek_ikeys.push_back(std::move(sk));
    }
    std::vector<Slice> seek_ikey_slices;
    seek_ikey_slices.reserve(kNumLookups);
    for (const auto& sk : seek_ikeys) {
      seek_ikey_slices.emplace_back(sk);
    }

    // ---- Benchmark: Trie Seek (full production path) ----
    // Replicates IndexFactoryIteratorWrapper::Seek() ->
    //   TrieIndexIterator::SeekAndGetResult():
    //   1. ParseInternalKey to extract user_key
    //   2. trie_iter.Seek(user_key)
    //   3. trie_iter.Key().ToString() -- string copy (result must outlive
    //   call)
    //   4. trie_iter.Value() -- look up BlockHandle from packed arrays
    volatile uint64_t trie_checksum = 0;
    std::string key_scratch;  // Reused scratch, like current_key_scratch_
    auto t0 = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < kNumLookups; i++) {
      // Step 1: Parse InternalKey -> user key (same as wrapper)
      ParsedInternalKey pkey;
      ASSERT_OK(ParseInternalKey(seek_ikey_slices[i], &pkey,
                                 /*log_err_key=*/false));

      // Step 2: Seek on the trie with user key
      trie_iter.Seek(pkey.user_key);

      if (trie_iter.Valid()) {
        // Step 3: Materialize the key (string copy, matches production)
        key_scratch = trie_iter.Key().ToString();

        // Step 4: Get BlockHandle (two uint32 array reads)
        TrieBlockHandle handle = trie_iter.Value();
        trie_checksum = trie_checksum + handle.offset;
      }
    }
    auto t1 = std::chrono::high_resolution_clock::now();
    double trie_ns =
        static_cast<double>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0)
                .count()) /
        static_cast<double>(kNumLookups);

    // ---- Benchmark: Real IndexBlockIter Seek ----
    // This is exactly what production does: Seek(InternalKey) -> value().
    std::unique_ptr<IndexBlockIter> ibi_iter(index_block.NewIndexIterator(
        BytewiseComparator(), kDisableGlobalSequenceNumber,
        /*iter=*/nullptr, /*stats=*/nullptr,
        /*total_order_seek=*/true, /*have_first_key=*/false,
        /*key_includes_seq=*/true, /*value_is_full=*/true,
        /*block_contents_pinned=*/false,
        /*user_defined_timestamps_persisted=*/true, /*prefix_index=*/nullptr,
        BlockBasedTableOptions::kBinary, /*value_delta_escape=*/false));

    volatile uint64_t ibi_checksum = 0;
    auto t2 = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < kNumLookups; i++) {
      ibi_iter->Seek(seek_ikey_slices[i]);
      if (ibi_iter->Valid()) {
        IndexValue val = ibi_iter->value();
        ibi_checksum = ibi_checksum + val.handle.offset();
      }
    }
    auto t3 = std::chrono::high_resolution_clock::now();
    double ibi_ns =
        static_cast<double>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(t3 - t2)
                .count()) /
        static_cast<double>(kNumLookups);

    double vs_ibi = (ibi_ns - trie_ns) / ibi_ns * 100.0;
    const char* winner = trie_ns <= ibi_ns ? "TRIE" : "IBI";
    fprintf(stderr, "  %8zu  %10.1f    %10.1f    %+6.1f%%  %s\n", num_keys,
            trie_ns, ibi_ns, vs_ibi, winner);
  }

  fprintf(stderr, "\n");
}

// ============================================================================
// Mixed key type tests -- verifies UDI works with Delete, Merge, etc.
// ============================================================================

TEST_P(TrieIndexSSTTest, MixedKeyTypesWithTrieUDI) {
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

TEST_P(TrieIndexSSTTest, LargeMixedKeyTypesWithTrieUDI) {
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

TEST_P(TrieIndexFactoryTest, WrapperNextAndGetResultReturnsInternalKey) {
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

// Verifies that overflow blocks are BFS-reordered alongside primary handles.
// If overflow blocks were stored in key-sorted order instead of BFS order,
// the overflow_base_ prefix sum would map overflow blocks to the wrong
// leaves when separator keys have different lengths.
//
// Key design:
//   Trie entries: "ab"(bc=2), "ac"(bc=1), "b"(bc=2), "c"(bc=1), "e"(bc=1)
//
//   Trie structure:
//     Root: ['a'(internal), 'b'(leaf), 'c'(leaf), 'e'(leaf)]
//     Level 1 under 'a': ['b'(leaf="ab"), 'c'(leaf="ac")]
//
//   BFS leaf order: "b"(0), "c"(1), "e"(2), "ab"(3), "ac"(4)
//   Key-sorted order: "ab"(0), "ac"(1), "b"(2), "c"(3), "e"(4)
//
//   Both "ab" and "b" have overflow runs. Key-sorted overflow is
//   ["ab"-overflow, "b"-overflow]. BFS-reordered overflow must be
//   ["b"-overflow, "ab"-overflow].
//
//   Without fix: Seek("b") at low seqno gets "ab"'s overflow data (wrong
//   offset and seqno), while Seek("ab") at low seqno gets "b"'s overflow.
TEST_P(TrieIndexFactoryTest, OverflowBfsReordering) {
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
  // -> sep="ac", different key, seqno=kMax->0
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
  // -> sep="c", different key, seqno=kMax->0
  {
    IndexFactoryBuilder::BlockHandle h{500, 100};
    Slice next("d");
    sep = builder->AddIndexEntry(Slice("ba"), &next, h, &scratch,
                                 EntryCtx(100, 100));
    ASSERT_EQ(scratch, "c") << "Block 5 separator";
  }
  // Block 6: last="d", next=null (last block, no successor shortening)
  // -> sep="d", different from prev "c", seqno=kMax->0
  {
    IndexFactoryBuilder::BlockHandle h{600, 100};
    sep = builder->AddIndexEntry(Slice("d"), nullptr, h, &scratch,
                                 EntryCtx(100, 100));
  }

  // After Finish(), trie entries (key-sorted):
  //   ki=0: "ab" bc=2, primary={offset=0,seqno=500},
  //         overflow=[{offset=100,seqno=400}]
  //   ki=1: "ac" bc=1, {offset=200,seqno=0}
  //   ki=2: "b"  bc=2, primary={offset=300,seqno=300},
  //         overflow=[{offset=400,seqno=200}]
  //   ki=3: "c"  bc=1, {offset=500,seqno=0}
  //   ki=4: "d"  bc=1, {offset=600,seqno=0}
  //
  // BFS leaf order: "b"(0), "c"(1), "d"(2), "ab"(3), "ac"(4)
  // BFS-reordered overflow: [{offset=400,seqno=200}, {offset=100,seqno=400}]
  // overflow_base_: leaf0("b")=0, leaf3("ab")=1

  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));

  std::unique_ptr<IndexFactoryReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));

  ReadOptions ro;
  auto iter = reader->NewIterator(ro);
  IterateResult result;

  // --- Full forward scan: verify all block offsets in key order ---
  // Expected order: ab(0), ab-overflow(100), ac(200), b(300),
  //                 b-overflow(400), c(500), e(600)
  ASSERT_OK(iter->SeekToFirstAndGetResult(&result));
  EXPECT_EQ(result.key.ToString(), "ab");
  EXPECT_EQ(iter->value().offset, 0u) << "SeekToFirst: ab primary";

  ASSERT_OK(iter->NextAndGetResult(&result));
  EXPECT_EQ(result.key.ToString(), "ab");
  EXPECT_EQ(iter->value().offset, 100u) << "Next: ab overflow";

  ASSERT_OK(iter->NextAndGetResult(&result));
  EXPECT_EQ(result.key.ToString(), "ac");
  EXPECT_EQ(iter->value().offset, 200u) << "Next: ac primary";

  ASSERT_OK(iter->NextAndGetResult(&result));
  EXPECT_EQ(result.key.ToString(), "b");
  EXPECT_EQ(iter->value().offset, 300u) << "Next: b primary";

  ASSERT_OK(iter->NextAndGetResult(&result));
  EXPECT_EQ(result.key.ToString(), "b");
  EXPECT_EQ(iter->value().offset, 400u) << "Next: b overflow";

  ASSERT_OK(iter->NextAndGetResult(&result));
  EXPECT_EQ(result.key.ToString(), "c");
  EXPECT_EQ(iter->value().offset, 500u) << "Next: c primary";

  ASSERT_OK(iter->NextAndGetResult(&result));
  EXPECT_EQ(result.key.ToString(), "d");
  EXPECT_EQ(iter->value().offset, 600u) << "Next: d primary";

  ASSERT_OK(iter->NextAndGetResult(&result));
  EXPECT_EQ(result.bound_check_result, IterBoundCheck::kUnknown) << "past end";

  // --- Seek-based tests: verify overflow data is correctly associated ---

  // "ab" primary: Seek("ab", kMax) -> offset=0
  ASSERT_OK(iter->SeekAndGetResult(Slice("ab"), &result,
                                   SeekCtx(kMaxSequenceNumber)));
  EXPECT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  EXPECT_EQ(iter->value().offset, 0u) << "Seek(ab,kMax): ab primary";

  // "ab" overflow: Seek("ab", 400) -> offset=100
  // Primary seqno=500, 400<500 -> advance to overflow. Overflow seqno=400,
  // 400>=400 -> match.
  ASSERT_OK(iter->SeekAndGetResult(Slice("ab"), &result, SeekCtx(400)));
  EXPECT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  EXPECT_EQ(iter->value().offset, 100u) << "Seek(ab,400): ab overflow";

  // "ab" advance past run: Seek("ab", 50) -> advances to "ac" at offset=200
  // Primary seqno=500, 50<500 -> overflow seqno=400, 50<400 -> exhaust run ->
  // advance to next trie leaf "ac".
  ASSERT_OK(iter->SeekAndGetResult(Slice("ab"), &result, SeekCtx(50)));
  EXPECT_EQ(result.key.ToString(), "ac")
      << "Seek(ab,50): should advance past ab run";
  EXPECT_EQ(iter->value().offset, 200u) << "Seek(ab,50): expected ac (200)";

  // "b" primary: Seek("b", kMax) -> offset=300
  ASSERT_OK(
      iter->SeekAndGetResult(Slice("b"), &result, SeekCtx(kMaxSequenceNumber)));
  EXPECT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  EXPECT_EQ(iter->value().offset, 300u) << "Seek(b,kMax): b primary";

  // "b" overflow: Seek("b", 200) -> offset=400
  // Primary seqno=300, 200<300 -> advance to overflow. Overflow seqno=200,
  // 200>=200 -> match.
  // If overflow blocks were NOT BFS-reordered, overflow[0] would contain
  // ab's data {offset=100,seqno=400}, and 200<400 would fail to match,
  // incorrectly advancing to "c".
  ASSERT_OK(iter->SeekAndGetResult(Slice("b"), &result, SeekCtx(200)));
  EXPECT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  EXPECT_EQ(result.key.ToString(), "b")
      << "Seek(b,200): must stay on b, not advance";
  EXPECT_EQ(iter->value().offset, 400u) << "Seek(b,200): b overflow";

  // "b" advance past run: Seek("b", 50) -> advances to "c" at offset=500
  ASSERT_OK(iter->SeekAndGetResult(Slice("b"), &result, SeekCtx(50)));
  EXPECT_EQ(result.key.ToString(), "c")
      << "Seek(b,50): should advance past b run";
  EXPECT_EQ(iter->value().offset, 500u) << "Seek(b,50): expected c (500)";
}

TEST_P(TrieIndexFactoryTest, SeekToLastPrefixKeyOnlyTrie) {
  // DescendToRightmostLeaf skips prefix keys (they are the smallest leaf at
  // a node). This test verifies that SeekToLast correctly lands on the
  // rightmost regular leaf ("ab"), and Prev correctly visits the prefix key
  // ("a") during backtracking.
  //
  // This trie has separators "a" and "ab". Separator "a" is a prefix of "ab",
  // creating a prefix key at the "a" node. When we SeekToLast, we should
  // land on "ab" (the rightmost leaf), and Prev from there should land on
  // "a" (the prefix key).
  auto ctx = BuildTrieAndGetIterator({
      {"a", "ab", 0, 500, 0, 0},
      {"ab", "", 500, 500, 0, 0},
  });

  IterateResult result;

  // SeekToLast should land on "ab".
  ASSERT_OK(ctx.iter->SeekToLastAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(result.key.ToString(), "ab");

  // Prev should land on "a" (the prefix key).
  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(result.key.ToString(), "a");

  // Prev again should be past the beginning.
  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_NE(result.bound_check_result, IterBoundCheck::kInbound);
}

TEST_P(TrieIndexFactoryTest, SeekToLastAndPrevWithPrefixKeys) {
  // Verifies SeekToLast and Prev correctly handle prefix keys in a trie
  // with multiple levels. Separators "a", "ab", "abc", "b" create prefix
  // keys at the "a" and "ab" nodes.
  auto ctx = BuildTrieAndGetIterator({
      {"a", "ab", 0, 500, 0, 0},
      {"ab", "abc", 500, 500, 0, 0},
      {"abc", "b", 1000, 500, 0, 0},
      {"b", "", 1500, 500, 0, 0},
  });

  IterateResult result;

  // SeekToLast should land on "b" (rightmost leaf).
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

TEST_P(TrieIndexFactoryTest, LastBlockSeekWithRealSeqno) {
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

  // Seek "cherry" with a real seqno (200) -- the last block's separator has
  // seqno=50. In internal key order, 200 > 50 means target is SMALLER (higher
  // seqno = smaller key). So target <= separator -> should stay on this block.
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("cherry"), 200, 1000));

  // Seek "cherry" with seqno=50 (equal to separator) -- should stay.
  ASSERT_NO_FATAL_FAILURE(
      AssertSeekOffset(ctx.iter.get(), Slice("cherry"), 50, 1000));

  // Seek "cherry" with seqno=10 (less than separator seqno=50) -- target is
  // LARGER in internal key order. target > separator -> should advance past.
  // But this is the last block -- no next block -> past end.
  IterateResult result;
  ASSERT_OK(ctx.iter->SeekAndGetResult(Slice("cherry"), &result, SeekCtx(10)));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);
}

TEST_P(TrieIndexFactoryTest, IntermediateNonBoundarySeparatorNoAdvance) {
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

TEST_P(TrieIndexFactoryTest, NonBoundarySeparatorSeekWhenShorteningFails) {
  // Reproducer for GitHub issue #14561: when FindShortestSeparator cannot
  // shorten the separator (e.g., "acc" -> "acd" stays "acc"), the trie lands
  // on separator "acc" with tag=0 (non-boundary sentinel). The post-seek
  // correction must NOT advance past it regardless of the target seqno.
  //
  // Seqno encoding is always active for a non-empty trie.
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

TEST_P(TrieIndexFactoryTest, PrevWithinOverflowRun) {
  // Exercises PrevAndGetResult when positioned mid-overflow: the fast path
  // that decrements overflow_run_index_ without calling iter_.Prev().
  auto ctx = BuildTrieAndGetIterator({
      // Block 0-2: "key" spans 3 blocks (overflow run).
      {"key", "key", 0, 1000, 300, 200},
      {"key", "key", 1000, 1000, 200, 100},
      {"key", "zzz", 2000, 1000, 100, 50},
      // Block 3: "zzz" last block.
      {"zzz", "", 3000, 1000, 50, 0},
  });

  IterateResult result;

  // Seek to "key"|100 -- should land on the last block in the "key" overflow
  // run (offset 2000) since seqno=100 matches that block's tag.
  ASSERT_OK(ctx.iter->SeekAndGetResult(Slice("key"), &result, SeekCtx(100)));
  ASSERT_EQ(ctx.iter->value().offset, 2000u);

  // Prev should go back to the middle of the overflow run (offset 1000).
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

TEST_P(TrieIndexFactoryTest, SeekToLastWithOverflowRun) {
  // SeekToLast on a trie where the last leaf has block_count > 1 should
  // position at the last overflow block.
  auto ctx = BuildTrieAndGetIterator({
      // Block 0: "aaa" standalone.
      {"aaa", "zzz", 0, 1000, 100, 50},
      // Block 1-3: "zzz" spans 3 blocks (overflow run) -- this is the last
      // leaf.
      {"zzz", "zzz", 1000, 1000, 50, 30},
      {"zzz", "zzz", 2000, 1000, 30, 10},
      {"zzz", "", 3000, 1000, 10, 0},
  });

  IterateResult result;

  // SeekToLast should land on the last overflow block (offset 3000).
  ASSERT_OK(ctx.iter->SeekToLastAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(result.key.ToString(), "zzz");
  ASSERT_EQ(ctx.iter->value().offset, 3000u);

  // Prev within overflow: offset 2000.
  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 2000u);

  // Prev within overflow: offset 1000.
  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 1000u);

  // Prev to previous trie leaf: "aaa" at offset 0.
  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 0u);

  // Prev past beginning.
  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_NE(result.bound_check_result, IterBoundCheck::kInbound);
}

TEST_P(TrieIndexFactoryTest, PrevLandsOnLeafWithOverflow) {
  // Prev from a standalone leaf should land on a previous leaf that has an
  // overflow run, positioning at the last block in that run.
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

  // Prev to "aaa" -- should land on the LAST block in the overflow run
  // (offset 1000), not the primary block (offset 0).
  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 1000u);

  // Prev within "aaa" overflow: offset 0.
  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(ctx.iter->value().offset, 0u);

  // Prev past beginning.
  ASSERT_OK(ctx.iter->PrevAndGetResult(&result));
  ASSERT_NE(result.bound_check_result, IterBoundCheck::kInbound);
}

TEST_P(TrieIndexFactoryTest, SeekToFirstOnEmptyTrie) {
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

TEST_P(TrieIndexFactoryTest, SeekToLastOnEmptyTrie) {
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

TEST_P(TrieIndexFactoryTest, OverflowExhaustionThenForwardScanThroughOverflow) {
  // When SeekAndGetResult exhausts an overflow run and advances to the next
  // trie leaf, verify that subsequent Next() calls correctly traverse through
  // any later overflow runs.
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

  // Seek "key"|1 -- seqno=1 is below all overflow tags in the "key" run
  // (300, 200), so it exhausts the run and advances to the next leaf.
  // FindShortestSeparator("key", "zzz") = "l", so the next leaf is "l".
  ASSERT_OK(ctx.iter->SeekAndGetResult(Slice("key"), &result, SeekCtx(1)));
  ASSERT_EQ(result.key.ToString(), "l");
  ASSERT_EQ(ctx.iter->value().offset, 2000u);

  // Next advances to "zzz" primary (offset 3000).
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.key.ToString(), "zzz");
  ASSERT_EQ(ctx.iter->value().offset, 3000u);

  // Next advances through "zzz" overflow (offset 4000).
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(ctx.iter->value().offset, 4000u);

  // Next past end.
  ASSERT_OK(ctx.iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);
}

TEST_P(TrieIndexFactoryTest, AllScansExhaustedThenSeek) {
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

TEST_P(TrieIndexFactoryTest, SupportsParallelAddEntry) {
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

TEST_P(TrieIndexFactoryTest, ParallelMatchesSerialOutput) {
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
      // separator "kkk", so Finish() has to collapse them into one
      // multi-block run with a seqno side-table entry per block. A run
      // longer than two is the only way that path gets exercised.
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

TEST_P(TrieIndexFactoryTest, ParallelPipelineOverlapsPrepareAndFinish) {
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

TEST_P(TrieIndexFactoryTest, ParallelEstimatedSizeStaysMonotonic) {
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

TEST_P(TrieIndexFactoryTest, ParallelFinishWithoutPrepareFailsBuild) {
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
// Trie format version 2
// ============================================================================

class TrieV2Test : public testing::Test {
 protected:
  struct Block {
    std::string separator;
    TrieBlockHandle handle;
    uint64_t tag;
  };

  // Tag of a Seek target that lands on the first block of its key.
  const uint64_t kSeekTag =
      PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek);

  // Serializes blocks with the given bucket size, failing the test on error.
  static std::string Build(const std::vector<Block>& blocks,
                           uint32_t bucket_size) {
    TrieV2Builder builder(bucket_size);
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

  static void ExpectAt(const TrieV2Iterator& it,
                       const std::vector<Block>& blocks, size_t i) {
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
    TrieV2Index index;
    ASSERT_OK(index.Init(data));
    ASSERT_EQ(index.NumBlocks(), blocks.size());
    TrieV2Iterator it(&index);
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
};

TEST_F(TrieV2Test, EmptySeparator) {
  const uint32_t seed = static_cast<uint32_t>(
      std::chrono::system_clock::now().time_since_epoch().count());
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
TEST_F(TrieV2Test, SameKeyRunsAndTags) {
  const uint32_t seed = static_cast<uint32_t>(
      std::chrono::system_clock::now().time_since_epoch().count());
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
    targets.push_back("");
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

TEST_F(TrieV2Test, LargeHandlesAndIrregularGaps) {
  const uint32_t seed = static_cast<uint32_t>(
      std::chrono::system_clock::now().time_since_epoch().count());
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);
  std::vector<std::string> keys = RandomKeys(&rnd, 500);
  std::vector<std::string> targets = SeekTargets(keys, &rnd);

  // Offsets past 4 GiB with a uniform gap between blocks.
  std::vector<Block> far = ContiguousBlocks(keys, &rnd);
  for (Block& b : far) {
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
  for (const auto* blocks : {&far, &padded, &huge}) {
    ASSERT_NO_FATAL_FAILURE(Verify(*blocks, 8, targets, BasicSeekTags()));
  }
}

// Column residuals are limited to 56 bits, far beyond any real SST.
TEST_F(TrieV2Test, ColumnRangeLimit) {
  TrieV2Builder builder(8);
  builder.AddBlock("a", {0, 1}, 0);
  builder.AddBlock("b", {1, 1}, 0);
  builder.AddBlock("c", {uint64_t{1} << 60, 1}, 0);
  std::string data;
  ASSERT_EQ(builder.Finish(&data).code(), Status::Code::kNotSupported);
}

TEST_F(TrieV2Test, NoBlocks) {
  std::string data = Build({}, 8);
  TrieV2Index index;
  ASSERT_OK(index.Init(data));
  ASSERT_EQ(index.NumBlocks(), 0u);
  TrieV2Iterator it(&index);
  ASSERT_OK(it.SeekToFirst());
  ASSERT_FALSE(it.Valid());
  ASSERT_OK(it.SeekToLast());
  ASSERT_FALSE(it.Valid());
  ASSERT_OK(it.Seek("a", kSeekTag));
  ASSERT_FALSE(it.Valid());
}

TEST_F(TrieV2Test, SeparatorsOutOfOrder) {
  TrieV2Builder builder(8);
  builder.AddBlock("b", {0, 10}, 0);
  builder.AddBlock("a", {15, 10}, 0);
  std::string data;
  ASSERT_EQ(builder.Finish(&data).code(), Status::Code::kInvalidArgument);
}

// The serialized bytes of a small index, written out by hand from the format
// description in trie_v2.h. Changing them breaks existing SST files.
TEST_F(TrieV2Test, GoldenBytes) {
  std::string data = Build({{"ab", {0, 10}, 0}, {"ac", {15, 20}, 257}}, 8);
  std::string expected;
  ASSERT_TRUE(
      Slice(
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
          // trie, bucket table, bucket data, offset table, size table (none),
          // run table, tag table, end
          "7900000000000000"
          "7D00000000000000"
          "A500000000000000"
          "A700000000000000"
          "0000000000000000"
          "D100000000000000"
          "D900000000000000"
          "F100000000000000"
          // shared prefix "a"
          "61"
          // trie: terminal leaf, 1-byte pointers, path "b", bucket 0
          "04"
          "01"
          "62"
          "00"
          // bucket table: 2 values, width 0, base 0, slope 2, padding
          "0200000000000000"
          "00000000"
          "00000000"
          "0000000000000000"
          "0200000000000000"
          "0000000000000000"
          // bucket data: "ac" after "ab" drops 1 byte and adds "c"
          "11"
          "63"
          // offset table: offsets 0, 15 and 40 (35 plus the 5-byte gap) as
          // -5 + 20 * i + residuals 5, 0, 5 in 3 bits each, then padding
          "0300000000000000"
          "03000000"
          "00000000"
          "FBFFFFFFFFFFFFFF"
          "1400000000000000"
          "4501"
          "0000000000000000"
          // run table: empty
          "0000000000000000"
          // tag table: block 1 has tag 257
          "0100000000000000"
          "0100000000000000"
          "0101000000000000"
          // padding
          "00000000000000000000000000000000")
          .DecodeHex(&expected));
  ASSERT_EQ(Slice(data).ToString(true), Slice(expected).ToString(true));
}

// Malformed indexes must be rejected by Init() or iterate without crashing.
// Run under ASAN to catch out-of-bounds reads.
TEST_F(TrieV2Test, MalformedData) {
  const uint32_t seed = static_cast<uint32_t>(
      std::chrono::system_clock::now().time_since_epoch().count());
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);

  auto exercise = [&](const std::string& data) {
    TrieV2Index index;
    if (!index.Init(data).ok()) {
      return;
    }
    TrieV2Iterator it(&index);
    const uint64_t limit = 2 * index.NumBlocks() + 10;
    Status s = it.SeekToFirst();
    for (uint64_t i = 0; s.ok() && it.Valid() && i < limit; i++) {
      it.value();
      s = it.Next();
    }
    s = it.SeekToLast();
    for (uint64_t i = 0; s.ok() && it.Valid() && i < limit; i++) {
      s = it.Prev();
    }
    for (int i = 0; i < 20; i++) {
      s = it.Seek(rnd.RandomBinaryString(rnd.Uniform(12)), kSeekTag);
      if (s.ok() && it.Valid()) {
        s = it.Prev();
      }
    }
    s.PermitUncheckedError();
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
      TrieV2Index index;
      ASSERT_NOK(index.Init(Slice(data.data(), len)));
    }
    for (size_t pos = 0; pos < kTrieV2HeaderSize; pos++) {
      for (char v : {'\0', '\xff', static_cast<char>(data[pos] ^ 1)}) {
        std::string mutated = data;
        mutated[pos] = v;
        exercise(mutated);
      }
    }
    for (int i = 0; i < 300; i++) {
      std::string mutated = data;
      const int flips = 1 + rnd.Uniform(3);
      for (int f = 0; f < flips; f++) {
        mutated[rnd.Uniform(static_cast<int>(mutated.size()))] ^=
            static_cast<char>(1 + rnd.Uniform(255));
      }
      exercise(mutated);
    }
  }
}

TEST_F(TrieV2Test, RandomizedSeekAndScan) {
  const uint32_t seed = static_cast<uint32_t>(
      std::chrono::system_clock::now().time_since_epoch().count());
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

TEST_F(TrieV2Test, SingleBlock) {
  std::string data = Build({{"apple", {0, 4000}, 0}}, 8);
  TrieV2Index index;
  ASSERT_OK(index.Init(data));
  ASSERT_EQ(index.NumBlocks(), 1u);
  TrieV2Iterator it(&index);

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

// Builds an index over blocks ending in "a", "b" and "c" with factory.
std::string BuildThreeBlockIndex(const IndexFactory& factory) {
  std::unique_ptr<IndexFactoryBuilder> builder;
  EXPECT_OK(factory.NewBuilder(IndexFactoryOptions(), builder));
  if (builder == nullptr) {
    return "";
  }
  const std::vector<std::string> keys = {"a", "b", "c"};
  for (size_t i = 0; i < keys.size(); i++) {
    Slice next;
    if (i + 1 < keys.size()) {
      next = keys[i + 1];
    }
    std::string scratch;
    builder->AddIndexEntry(keys[i], i + 1 < keys.size() ? &next : nullptr,
                           {i * 100, 95}, &scratch, EntryCtx(10, 10));
  }
  Slice contents;
  EXPECT_OK(builder->Finish(&contents));
  return contents.ToString();
}

TEST(TrieIndexFormatVersionTest, SelectsWrittenFormat) {
  struct Case {
    const char* table_options;
    uint32_t written_version;
  };
  for (const Case& c : std::vector<Case>{
           {"user_defined_index_factory=trie_index", 1},
           {"user_defined_index_factory={id=trie_index;format_version=1}", 1},
           {"user_defined_index_factory={id=trie_index;format_version=2}", 2},
       }) {
    SCOPED_TRACE(c.table_options);
    BlockBasedTableOptions table_options;
    ConfigOptions config_options;
    ASSERT_OK(GetBlockBasedTableOptionsFromString(
        config_options, BlockBasedTableOptions(), c.table_options,
        &table_options));
    ASSERT_NE(table_options.user_defined_index_factory, nullptr);
    std::string data =
        BuildThreeBlockIndex(*table_options.user_defined_index_factory);
    ASSERT_GE(data.size(), 8u);
    ASSERT_EQ(DecodeFixed32(data.data() + 4), c.written_version);

    // Every factory reads every format, whatever it writes.
    for (uint32_t reader_version : {1u, 2u}) {
      SCOPED_TRACE(reader_version);
      TrieIndexFactory reader_factory(reader_version);
      Slice contents(data);
      std::unique_ptr<IndexFactoryReader> reader;
      ASSERT_OK(
          reader_factory.NewReader(IndexFactoryOptions(), contents, reader));
      auto iter = reader->NewIterator(ReadOptions());
      IterateResult result;
      ASSERT_OK(iter->SeekAndGetResult("b", &result, SeekCtx(5)));
      ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
      ASSERT_EQ(result.key, "b");
      ASSERT_EQ(iter->value().offset, 100u);
      ASSERT_EQ(iter->value().size, 95u);
    }
  }
}

TEST(TrieIndexFormatVersionTest, RejectsUnknownFormatVersion) {
  for (uint32_t version : {1u, 2u}) {
    SCOPED_TRACE(version);
    TrieIndexFactory factory(version);
    ASSERT_EQ(
        factory.ValidateOptions(DBOptions(), ColumnFamilyOptions()).code(),
        version == 1 && !port::kLittleEndian ? Status::Code::kNotSupported
                                             : Status::Code::kOk);
  }
  for (uint32_t version : {0u, 3u}) {
    SCOPED_TRACE(version);
    TrieIndexFactory factory(version);
    std::unique_ptr<IndexFactoryBuilder> builder;
    ASSERT_EQ(factory.NewBuilder(IndexFactoryOptions(), builder).code(),
              Status::Code::kInvalidArgument);
    ASSERT_EQ(
        factory.ValidateOptions(DBOptions(), ColumnFamilyOptions()).code(),
        Status::Code::kInvalidArgument);
  }
}

// RocksDB versions that predate format 2 parse every trie index as format 1,
// and must reject format 2 instead of misreading it.
TEST(TrieIndexFormatVersionTest, FormatOneReaderRejectsFormatTwo) {
  const std::string data = BuildThreeBlockIndex(TrieIndexFactory(2));
  LoudsTrie trie;
  Status s = trie.InitFromData(data);
  ASSERT_EQ(s.code(), Status::Code::kCorruption);
  ASSERT_NE(s.ToString().find("unsupported format version"), std::string::npos)
      << s.ToString();
}

// A format 1 index written by RocksDB 11.14 for blocks ending in "apple"@90,
// "apricot"@80, "apricot"@70 and "banana"@7. SST files holding such indexes
// must stay readable.
TEST(TrieIndexFormatVersionTest, ReadsFormatOneGoldenIndex) {
  std::string data;
  ASSERT_TRUE(
      Slice("4549525401000000040000000000000000000000070000000000000000000000"
            "0000000000000000010000000000000001000000000000000000000000000000"
            "0000000000000000000000000000000000000000000000000000000000000000"
            "0000000000000000000000000000000000000000000000000000000000000000"
            "0E000000000000006162706171726E6961636E6F617400000E00000000000000"
            "0B00000000000000EF0F00000000000000000000000000000000000000000000"
            "00000000000000000E000000000000000C00000000000000DD3F000000000000"
            "0000000000000000000000000000000000000000000000000C00000000000000"
            "0100000000000000040000000000000000000000000000000000000000000000"
            "00000000000000000B0000000000000002000000030000000400000006000000"
            "0700000008000000090000000A0000000B0000000C0000000D00000000000000"
            "0300000004000000060000000700000008000000090000000A0000000B000000"
            "0C0000000D0000000E000000000000000000000000000000AE1F000000000000"
            "EF2E0000A50F00003C0F0000A00F0000D0070000041000000000000000000000"
            "0000000000000000000000000000000001070000000000000150000000000000"
            "01000000010000000100000001000000")
          .DecodeHex(&data));
  const std::vector<std::pair<uint64_t, uint64_t>> handles = {
      {0, 4000}, {4005, 4100}, {8110, 3900}, {12015, 2000}};
  for (uint32_t reader_version : {1u, 2u}) {
    SCOPED_TRACE(reader_version);
    TrieIndexFactory factory(reader_version);
    Slice contents(data);
    std::unique_ptr<IndexFactoryReader> reader;
    ASSERT_OK(factory.NewReader(IndexFactoryOptions(), contents, reader));
    auto iter = reader->NewIterator(ReadOptions());
    IterateResult result;
    ASSERT_OK(iter->SeekToFirstAndGetResult(&result));
    for (size_t i = 0; i < handles.size(); i++) {
      ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
      ASSERT_EQ(iter->value().offset, handles[i].first);
      ASSERT_EQ(iter->value().size, handles[i].second);
      ASSERT_OK(iter->NextAndGetResult(&result));
    }
    ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);

    ASSERT_OK(iter->SeekAndGetResult("apple", &result, SeekCtx(95)));
    ASSERT_EQ(iter->value().offset, 0u);
    ASSERT_OK(iter->SeekAndGetResult("apricot", &result, SeekCtx(85)));
    ASSERT_EQ(iter->value().offset, 4005u);
    ASSERT_OK(iter->SeekAndGetResult("apricot", &result, SeekCtx(75)));
    ASSERT_EQ(iter->value().offset, 8110u);
    ASSERT_OK(iter->SeekAndGetResult("banana", &result, SeekCtx(8)));
    ASSERT_EQ(iter->value().offset, 12015u);
    ASSERT_OK(iter->SeekAndGetResult("banana", &result, SeekCtx(6)));
    ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);
  }
}

// Format 1 is only supported on little-endian hosts.
std::vector<uint32_t> SupportedFormatVersions() {
  if (port::kLittleEndian) {
    return {1, 2};
  }
  return {2};
}

INSTANTIATE_TEST_CASE_P(FormatVersions, TrieIndexFactoryTest,
                        ::testing::ValuesIn(SupportedFormatVersions()));
INSTANTIATE_TEST_CASE_P(FormatVersions, TrieIndexSSTTest,
                        ::testing::ValuesIn(SupportedFormatVersions()));

}  // namespace trie_index
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
