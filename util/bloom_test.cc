//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run this test... Skipping...\n");
  return 0;
}
#else

#include <array>
#include <cmath>
#include <vector>

#include "cache/cache_entry_roles.h"
#include "cache/cache_reservation_manager.h"
#include "memory/arena.h"
#include "port/jemalloc_helper.h"
#include "rocksdb/convenience.h"
#include "rocksdb/filter_policy.h"
#include "table/block_based/filter_policy_internal.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/gflags_compat.h"
#include "util/hash.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;

// The test is not fully designed for bits_per_key other than 10, but with
// this parameter you can easily explore the behavior of other bits_per_key.
// See also filter_bench.
DEFINE_int32(bits_per_key, 10, "");

namespace ROCKSDB_NAMESPACE {

namespace {
const std::string kLegacyBloom = test::LegacyBloomFilterPolicy::kClassName();
const std::string kFastLocalBloom =
    test::FastLocalBloomFilterPolicy::kClassName();
const std::string kStandard128Ribbon =
    test::Standard128RibbonFilterPolicy::kClassName();
}  // namespace

static const int kVerbose = 1;

static Slice Key(int i, char* buffer) {
  std::string s;
  PutFixed32(&s, static_cast<uint32_t>(i));
  memcpy(buffer, s.c_str(), sizeof(i));
  return Slice(buffer, sizeof(i));
}

static int NextLength(int length) {
  if (length < 10) {
    length += 1;
  } else if (length < 100) {
    length += 10;
  } else if (length < 1000) {
    length += 100;
  } else {
    length += 1000;
  }
  return length;
}

class FullBloomTest : public testing::TestWithParam<std::string> {
 protected:
  BlockBasedTableOptions table_options_;

 private:
  std::shared_ptr<const FilterPolicy>& policy_;
  std::unique_ptr<FilterBitsBuilder> bits_builder_;
  std::unique_ptr<FilterBitsReader> bits_reader_;
  std::unique_ptr<const char[]> buf_;
  size_t filter_size_;

 public:
  FullBloomTest() : policy_(table_options_.filter_policy), filter_size_(0) {
    ResetPolicy();
  }

  FilterBitsBuilder* GetFilterBitsBuilder() { return bits_builder_.get(); }

  const BloomLikeFilterPolicy* GetBloomLikeFilterPolicy() {
    // Throws on bad cast
    return &dynamic_cast<const BloomLikeFilterPolicy&>(*policy_);
  }

  void Reset() {
    bits_builder_.reset(BloomFilterPolicy::GetBuilderFromContext(
        FilterBuildingContext(table_options_)));
    bits_reader_.reset(nullptr);
    buf_.reset(nullptr);
    filter_size_ = 0;
  }

  void ResetPolicy(double bits_per_key) {
    policy_ = BloomLikeFilterPolicy::Create(GetParam(), bits_per_key);
    Reset();
  }

  void ResetPolicy() { ResetPolicy(FLAGS_bits_per_key); }

  void Add(const Slice& s) { bits_builder_->AddKey(s); }

  void OpenRaw(const Slice& s) {
    bits_reader_.reset(policy_->GetFilterBitsReader(s));
  }

  void Build() {
    Slice filter = bits_builder_->Finish(&buf_);
    bits_reader_.reset(policy_->GetFilterBitsReader(filter));
    filter_size_ = filter.size();
  }

  size_t FilterSize() const { return filter_size_; }

  Slice FilterData() { return Slice(buf_.get(), filter_size_); }

  int GetNumProbesFromFilterData() {
    assert(filter_size_ >= 5);
    int8_t raw_num_probes = static_cast<int8_t>(buf_.get()[filter_size_ - 5]);
    if (raw_num_probes == -1) {  // New bloom filter marker
      return static_cast<uint8_t>(buf_.get()[filter_size_ - 3]);
    } else {
      return raw_num_probes;
    }
  }

  int GetRibbonSeedFromFilterData() {
    assert(filter_size_ >= 5);
    // Check for ribbon marker
    assert(-2 == static_cast<int8_t>(buf_.get()[filter_size_ - 5]));
    return static_cast<uint8_t>(buf_.get()[filter_size_ - 4]);
  }

  bool Matches(const Slice& s) {
    if (bits_reader_ == nullptr) {
      Build();
    }
    return bits_reader_->MayMatch(s);
  }

  // Provides a kind of fingerprint on the Bloom filter's
  // behavior, for reasonbly high FP rates.
  uint64_t PackedMatches() {
    char buffer[sizeof(int)];
    uint64_t result = 0;
    for (int i = 0; i < 64; i++) {
      if (Matches(Key(i + 12345, buffer))) {
        result |= uint64_t{1} << i;
      }
    }
    return result;
  }

  // Provides a kind of fingerprint on the Bloom filter's
  // behavior, for lower FP rates.
  std::string FirstFPs(int count) {
    char buffer[sizeof(int)];
    std::string rv;
    int fp_count = 0;
    for (int i = 0; i < 1000000; i++) {
      // Pack four match booleans into each hexadecimal digit
      if (Matches(Key(i + 1000000, buffer))) {
        ++fp_count;
        rv += std::to_string(i);
        if (fp_count == count) {
          break;
        }
        rv += ',';
      }
    }
    return rv;
  }

  double FalsePositiveRate() {
    char buffer[sizeof(int)];
    int result = 0;
    for (int i = 0; i < 10000; i++) {
      if (Matches(Key(i + 1000000000, buffer))) {
        result++;
      }
    }
    return result / 10000.0;
  }
};

TEST_P(FullBloomTest, FilterSize) {
  // In addition to checking the consistency of space computation, we are
  // checking that denoted and computed doubles are interpreted as expected
  // as bits_per_key values.
  bool some_computed_less_than_denoted = false;
  // Note: to avoid unproductive configurations, bits_per_key < 0.5 is rounded
  // down to 0 (no filter), and 0.5 <= bits_per_key < 1.0 is rounded up to 1
  // bit per key (1000 millibits). Also, enforced maximum is 100 bits per key
  // (100000 millibits).
  for (auto bpk : std::vector<std::pair<double, int> >{{-HUGE_VAL, 0},
                                                       {-INFINITY, 0},
                                                       {0.0, 0},
                                                       {0.499, 0},
                                                       {0.5, 1000},
                                                       {1.234, 1234},
                                                       {3.456, 3456},
                                                       {9.5, 9500},
                                                       {10.0, 10000},
                                                       {10.499, 10499},
                                                       {21.345, 21345},
                                                       {99.999, 99999},
                                                       {1234.0, 100000},
                                                       {HUGE_VAL, 100000},
                                                       {INFINITY, 100000},
                                                       {NAN, 100000}}) {
    ResetPolicy(bpk.first);
    auto bfp = GetBloomLikeFilterPolicy();
    EXPECT_EQ(bpk.second, bfp->GetMillibitsPerKey());
    EXPECT_EQ((bpk.second + 500) / 1000, bfp->GetWholeBitsPerKey());

    double computed = bpk.first;
    // This transforms e.g. 9.5 -> 9.499999999999998, which we still
    // round to 10 for whole bits per key.
    computed += 0.5;
    computed /= 1234567.0;
    computed *= 1234567.0;
    computed -= 0.5;
    some_computed_less_than_denoted |= (computed < bpk.first);
    ResetPolicy(computed);
    bfp = GetBloomLikeFilterPolicy();
    EXPECT_EQ(bpk.second, bfp->GetMillibitsPerKey());
    EXPECT_EQ((bpk.second + 500) / 1000, bfp->GetWholeBitsPerKey());

    auto bits_builder = GetFilterBitsBuilder();
    if (bpk.second == 0) {
      ASSERT_EQ(bits_builder, nullptr);
      continue;
    }

    size_t n = 1;
    size_t space = 0;
    for (; n < 1000000; n += 1 + n / 1000) {
      // Ensure consistency between CalculateSpace and ApproximateNumEntries
      space = bits_builder->CalculateSpace(n);
      size_t n2 = bits_builder->ApproximateNumEntries(space);
      EXPECT_GE(n2, n);
      size_t space2 = bits_builder->CalculateSpace(n2);
      if (n > 12000 && GetParam() == kStandard128Ribbon) {
        // TODO(peterd): better approximation?
        EXPECT_GE(space2, space);
        EXPECT_LE(space2 * 0.998, space * 1.0);
      } else {
        EXPECT_EQ(space2, space);
      }
    }
    // Until size_t overflow
    for (; n < (n + n / 3); n += n / 3) {
      // Ensure space computation is not overflowing; capped is OK
      size_t space2 = bits_builder->CalculateSpace(n);
      EXPECT_GE(space2, space);
      space = space2;
    }
  }
  // Check that the compiler hasn't optimized our computation into nothing
  EXPECT_TRUE(some_computed_less_than_denoted);
  ResetPolicy();
}

TEST_P(FullBloomTest, FullEmptyFilter) {
  // Empty filter is not match, at this level
  ASSERT_TRUE(!Matches("hello"));
  ASSERT_TRUE(!Matches("world"));
}

TEST_P(FullBloomTest, FullSmall) {
  Add("hello");
  Add("world");
  ASSERT_TRUE(Matches("hello"));
  ASSERT_TRUE(Matches("world"));
  ASSERT_TRUE(!Matches("x"));
  ASSERT_TRUE(!Matches("foo"));
}

TEST_P(FullBloomTest, FullVaryingLengths) {
  // Match how this test was originally built
  table_options_.optimize_filters_for_memory = false;

  char buffer[sizeof(int)];

  // Count number of filters that significantly exceed the false positive rate
  int mediocre_filters = 0;
  int good_filters = 0;

  for (int length = 1; length <= 10000; length = NextLength(length)) {
    Reset();
    for (int i = 0; i < length; i++) {
      Add(Key(i, buffer));
    }
    Build();

    EXPECT_LE(FilterSize(), (size_t)((length * FLAGS_bits_per_key / 8) +
                                     CACHE_LINE_SIZE * 2 + 5));

    // All added keys must match
    for (int i = 0; i < length; i++) {
      ASSERT_TRUE(Matches(Key(i, buffer)))
          << "Length " << length << "; key " << i;
    }

    // Check false positive rate
    double rate = FalsePositiveRate();
    if (kVerbose >= 1) {
      fprintf(stderr, "False positives: %5.2f%% @ length = %6d ; bytes = %6d\n",
              rate * 100.0, length, static_cast<int>(FilterSize()));
    }
    if (FLAGS_bits_per_key == 10) {
      EXPECT_LE(rate, 0.02);  // Must not be over 2%
      if (rate > 0.0125) {
        mediocre_filters++;  // Allowed, but not too often
      } else {
        good_filters++;
      }
    }
  }
  if (kVerbose >= 1) {
    fprintf(stderr, "Filters: %d good, %d mediocre\n", good_filters,
            mediocre_filters);
  }
  EXPECT_LE(mediocre_filters, good_filters / 5);
}

TEST_P(FullBloomTest, OptimizeForMemory) {
  // Verify default option
  EXPECT_EQ(BlockBasedTableOptions().optimize_filters_for_memory, true);

  char buffer[sizeof(int)];
  for (bool offm : {true, false}) {
    table_options_.optimize_filters_for_memory = offm;
    ResetPolicy();
    Random32 rnd(12345);
    uint64_t total_size = 0;
    uint64_t total_mem = 0;
    int64_t total_keys = 0;
    double total_fp_rate = 0;
    constexpr int nfilters = 100;
    for (int i = 0; i < nfilters; ++i) {
      int nkeys = static_cast<int>(rnd.Uniformish(10000)) + 100;
      Reset();
      for (int j = 0; j < nkeys; ++j) {
        Add(Key(j, buffer));
      }
      Build();
      size_t size = FilterData().size();
      total_size += size;
      // optimize_filters_for_memory currently only has an effect with
      // malloc_usable_size support, but we run the rest of the test to ensure
      // no bad behavior without it.
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
      size = malloc_usable_size(const_cast<char*>(FilterData().data()));
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
      total_mem += size;
      total_keys += nkeys;
      total_fp_rate += FalsePositiveRate();
    }
    if (FLAGS_bits_per_key == 10) {
      EXPECT_LE(total_fp_rate / double{nfilters}, 0.011);
      EXPECT_GE(total_fp_rate / double{nfilters},
                CACHE_LINE_SIZE >= 256 ? 0.007 : 0.008);
    }

    int64_t ex_min_total_size = int64_t{FLAGS_bits_per_key} * total_keys / 8;
    if (GetParam() == kStandard128Ribbon) {
      // ~ 30% savings vs. Bloom filter
      ex_min_total_size = 7 * ex_min_total_size / 10;
    }
    EXPECT_GE(static_cast<int64_t>(total_size), ex_min_total_size);

    int64_t blocked_bloom_overhead = nfilters * (CACHE_LINE_SIZE + 5);
    if (GetParam() == kLegacyBloom) {
      // this config can add extra cache line to make odd number
      blocked_bloom_overhead += nfilters * CACHE_LINE_SIZE;
    }

    EXPECT_GE(total_mem, total_size);

    // optimize_filters_for_memory not implemented with legacy Bloom
    if (offm && GetParam() != kLegacyBloom) {
      // This value can include a small extra penalty for kExtraPadding
      fprintf(stderr, "Internal fragmentation (optimized): %g%%\n",
              (total_mem - total_size) * 100.0 / total_size);
      // Less than 1% internal fragmentation
      EXPECT_LE(total_mem, total_size * 101 / 100);
      // Up to 2% storage penalty
      EXPECT_LE(static_cast<int64_t>(total_size),
                ex_min_total_size * 102 / 100 + blocked_bloom_overhead);
    } else {
      fprintf(stderr, "Internal fragmentation (not optimized): %g%%\n",
              (total_mem - total_size) * 100.0 / total_size);
      // TODO: add control checks for more allocators?
#ifdef ROCKSDB_JEMALLOC
      fprintf(stderr, "Jemalloc detected? %d\n", HasJemalloc());
      if (HasJemalloc()) {
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
        // More than 5% internal fragmentation
        EXPECT_GE(total_mem, total_size * 105 / 100);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
      }
#endif  // ROCKSDB_JEMALLOC
      // No storage penalty, just usual overhead
      EXPECT_LE(static_cast<int64_t>(total_size),
                ex_min_total_size + blocked_bloom_overhead);
    }
  }
}

class ChargeFilterConstructionTest : public testing::Test {};
TEST_F(ChargeFilterConstructionTest, RibbonFilterFallBackOnLargeBanding) {
  constexpr std::size_t kCacheCapacity =
      8 * CacheReservationManagerImpl<
              CacheEntryRole::kFilterConstruction>::GetDummyEntrySize();
  constexpr std::size_t num_entries_for_cache_full = kCacheCapacity / 8;

  for (CacheEntryRoleOptions::Decision charge_filter_construction_mem :
       {CacheEntryRoleOptions::Decision::kEnabled,
        CacheEntryRoleOptions::Decision::kDisabled}) {
    bool will_fall_back = charge_filter_construction_mem ==
                          CacheEntryRoleOptions::Decision::kEnabled;

    BlockBasedTableOptions table_options;
    table_options.cache_usage_options.options_overrides.insert(
        {CacheEntryRole::kFilterConstruction,
         {/*.charged = */ charge_filter_construction_mem}});
    LRUCacheOptions lo;
    lo.capacity = kCacheCapacity;
    lo.num_shard_bits = 0;  // 2^0 shard
    lo.strict_capacity_limit = true;
    std::shared_ptr<Cache> cache(NewLRUCache(lo));
    table_options.block_cache = cache;
    table_options.filter_policy =
        BloomLikeFilterPolicy::Create(kStandard128Ribbon, FLAGS_bits_per_key);
    FilterBuildingContext ctx(table_options);
    std::unique_ptr<FilterBitsBuilder> filter_bits_builder(
        table_options.filter_policy->GetBuilderWithContext(ctx));

    char key_buffer[sizeof(int)];
    for (std::size_t i = 0; i < num_entries_for_cache_full; ++i) {
      filter_bits_builder->AddKey(Key(static_cast<int>(i), key_buffer));
    }

    std::unique_ptr<const char[]> buf;
    Slice filter = filter_bits_builder->Finish(&buf);

    // To verify Ribbon Filter fallbacks to Bloom Filter properly
    // based on cache charging result
    // See BloomFilterPolicy::GetBloomBitsReader re: metadata
    // -1 = Marker for newer Bloom implementations
    // -2 = Marker for Standard128 Ribbon
    if (will_fall_back) {
      EXPECT_EQ(filter.data()[filter.size() - 5], static_cast<char>(-1));
    } else {
      EXPECT_EQ(filter.data()[filter.size() - 5], static_cast<char>(-2));
    }

    if (charge_filter_construction_mem ==
        CacheEntryRoleOptions::Decision::kEnabled) {
      const size_t dummy_entry_num = static_cast<std::size_t>(std::ceil(
          filter.size() * 1.0 /
          CacheReservationManagerImpl<
              CacheEntryRole::kFilterConstruction>::GetDummyEntrySize()));
      EXPECT_GE(
          cache->GetPinnedUsage(),
          dummy_entry_num *
              CacheReservationManagerImpl<
                  CacheEntryRole::kFilterConstruction>::GetDummyEntrySize());
      EXPECT_LT(
          cache->GetPinnedUsage(),
          (dummy_entry_num + 1) *
              CacheReservationManagerImpl<
                  CacheEntryRole::kFilterConstruction>::GetDummyEntrySize());
    } else {
      EXPECT_EQ(cache->GetPinnedUsage(), 0);
    }
  }
}

namespace {
inline uint32_t SelectByCacheLineSize(uint32_t for64, uint32_t for128,
                                      uint32_t for256) {
  (void)for64;
  (void)for128;
  (void)for256;
#if CACHE_LINE_SIZE == 64
  return for64;
#elif CACHE_LINE_SIZE == 128
  return for128;
#elif CACHE_LINE_SIZE == 256
  return for256;
#else
#error "CACHE_LINE_SIZE unknown or unrecognized"
#endif
}
}  // namespace

// Ensure the implementation doesn't accidentally change in an
// incompatible way. This test doesn't check the reading side
// (FirstFPs/PackedMatches) for LegacyBloom because it requires the
// ability to read filters generated using other cache line sizes.
// See RawSchema.
TEST_P(FullBloomTest, Schema) {
  // Match how this test was originally built
  table_options_.optimize_filters_for_memory = false;

#define EXPECT_EQ_Bloom(a, b)               \
  {                                         \
    if (GetParam() != kStandard128Ribbon) { \
      EXPECT_EQ(a, b);                      \
    }                                       \
  }
#define EXPECT_EQ_Ribbon(a, b)              \
  {                                         \
    if (GetParam() == kStandard128Ribbon) { \
      EXPECT_EQ(a, b);                      \
    }                                       \
  }
#define EXPECT_EQ_FastBloom(a, b)        \
  {                                      \
    if (GetParam() == kFastLocalBloom) { \
      EXPECT_EQ(a, b);                   \
    }                                    \
  }
#define EXPECT_EQ_LegacyBloom(a, b)   \
  {                                   \
    if (GetParam() == kLegacyBloom) { \
      EXPECT_EQ(a, b);                \
    }                                 \
  }
#define EXPECT_EQ_NotLegacy(a, b)     \
  {                                   \
    if (GetParam() != kLegacyBloom) { \
      EXPECT_EQ(a, b);                \
    }                                 \
  }

  char buffer[sizeof(int)];

  // First do a small number of keys, where Ribbon config will fall back on
  // fast Bloom filter and generate the same data
  ResetPolicy(5);  // num_probes = 3
  for (int key = 0; key < 87; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ(GetNumProbesFromFilterData(), 3);

  EXPECT_EQ_NotLegacy(BloomHash(FilterData()), 4130687756U);

  EXPECT_EQ_NotLegacy("31,38,40,43,61,83,86,112,125,131", FirstFPs(10));

  // Now use enough keys so that changing bits / key by 1 is guaranteed to
  // change number of allocated cache lines. So keys > max cache line bits.

  // Note that the first attempted Ribbon seed is determined by the hash
  // of the first key added (for pseudorandomness in practice, determinism in
  // testing)

  ResetPolicy(2);  // num_probes = 1
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ_Bloom(GetNumProbesFromFilterData(), 1);
  EXPECT_EQ_Ribbon(GetRibbonSeedFromFilterData(), 61);

  EXPECT_EQ_LegacyBloom(
      BloomHash(FilterData()),
      SelectByCacheLineSize(1567096579, 1964771444, 2659542661U));
  EXPECT_EQ_FastBloom(BloomHash(FilterData()), 3817481309U);
  EXPECT_EQ_Ribbon(BloomHash(FilterData()), 1705851228U);

  EXPECT_EQ_FastBloom("11,13,17,25,29,30,35,37,45,53", FirstFPs(10));
  EXPECT_EQ_Ribbon("3,8,10,17,19,20,23,28,31,32", FirstFPs(10));

  ResetPolicy(3);  // num_probes = 2
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ_Bloom(GetNumProbesFromFilterData(), 2);
  EXPECT_EQ_Ribbon(GetRibbonSeedFromFilterData(), 61);

  EXPECT_EQ_LegacyBloom(
      BloomHash(FilterData()),
      SelectByCacheLineSize(2707206547U, 2571983456U, 218344685));
  EXPECT_EQ_FastBloom(BloomHash(FilterData()), 2807269961U);
  EXPECT_EQ_Ribbon(BloomHash(FilterData()), 1095342358U);

  EXPECT_EQ_FastBloom("4,15,17,24,27,28,29,53,63,70", FirstFPs(10));
  EXPECT_EQ_Ribbon("3,17,20,28,32,33,36,43,49,54", FirstFPs(10));

  ResetPolicy(5);  // num_probes = 3
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ_Bloom(GetNumProbesFromFilterData(), 3);
  EXPECT_EQ_Ribbon(GetRibbonSeedFromFilterData(), 61);

  EXPECT_EQ_LegacyBloom(
      BloomHash(FilterData()),
      SelectByCacheLineSize(515748486, 94611728, 2436112214U));
  EXPECT_EQ_FastBloom(BloomHash(FilterData()), 204628445U);
  EXPECT_EQ_Ribbon(BloomHash(FilterData()), 3971337699U);

  EXPECT_EQ_FastBloom("15,24,29,39,53,87,89,100,103,104", FirstFPs(10));
  EXPECT_EQ_Ribbon("3,33,36,43,67,70,76,78,84,102", FirstFPs(10));

  ResetPolicy(8);  // num_probes = 5
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ_Bloom(GetNumProbesFromFilterData(), 5);
  EXPECT_EQ_Ribbon(GetRibbonSeedFromFilterData(), 61);

  EXPECT_EQ_LegacyBloom(
      BloomHash(FilterData()),
      SelectByCacheLineSize(1302145999, 2811644657U, 756553699));
  EXPECT_EQ_FastBloom(BloomHash(FilterData()), 355564975U);
  EXPECT_EQ_Ribbon(BloomHash(FilterData()), 3651449053U);

  EXPECT_EQ_FastBloom("16,60,66,126,220,238,244,256,265,287", FirstFPs(10));
  EXPECT_EQ_Ribbon("33,187,203,296,300,322,411,419,547,582", FirstFPs(10));

  ResetPolicy(9);  // num_probes = 6
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ_Bloom(GetNumProbesFromFilterData(), 6);
  EXPECT_EQ_Ribbon(GetRibbonSeedFromFilterData(), 61);

  EXPECT_EQ_LegacyBloom(
      BloomHash(FilterData()),
      SelectByCacheLineSize(2092755149, 661139132, 1182970461));
  EXPECT_EQ_FastBloom(BloomHash(FilterData()), 2137566013U);
  EXPECT_EQ_Ribbon(BloomHash(FilterData()), 1005676675U);

  EXPECT_EQ_FastBloom("156,367,791,872,945,1015,1139,1159,1265", FirstFPs(9));
  EXPECT_EQ_Ribbon("33,187,203,296,411,419,604,612,615,619", FirstFPs(10));

  ResetPolicy(11);  // num_probes = 7
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ_Bloom(GetNumProbesFromFilterData(), 7);
  EXPECT_EQ_Ribbon(GetRibbonSeedFromFilterData(), 61);

  EXPECT_EQ_LegacyBloom(
      BloomHash(FilterData()),
      SelectByCacheLineSize(3755609649U, 1812694762, 1449142939));
  EXPECT_EQ_FastBloom(BloomHash(FilterData()), 2561502687U);
  EXPECT_EQ_Ribbon(BloomHash(FilterData()), 3129900846U);

  EXPECT_EQ_FastBloom("34,74,130,236,643,882,962,1015,1035,1110", FirstFPs(10));
  EXPECT_EQ_Ribbon("411,419,623,665,727,794,955,1052,1323,1330", FirstFPs(10));

  // This used to be 9 probes, but 8 is a better choice for speed,
  // especially with SIMD groups of 8 probes, with essentially no
  // change in FP rate.
  // FP rate @ 9 probes, old Bloom: 0.4321%
  // FP rate @ 9 probes, new Bloom: 0.1846%
  // FP rate @ 8 probes, new Bloom: 0.1843%
  ResetPolicy(14);  // num_probes = 8 (new), 9 (old)
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ_LegacyBloom(GetNumProbesFromFilterData(), 9);
  EXPECT_EQ_FastBloom(GetNumProbesFromFilterData(), 8);
  EXPECT_EQ_Ribbon(GetRibbonSeedFromFilterData(), 61);

  EXPECT_EQ_LegacyBloom(
      BloomHash(FilterData()),
      SelectByCacheLineSize(178861123, 379087593, 2574136516U));
  EXPECT_EQ_FastBloom(BloomHash(FilterData()), 3709876890U);
  EXPECT_EQ_Ribbon(BloomHash(FilterData()), 1855638875U);

  EXPECT_EQ_FastBloom("130,240,522,565,989,2002,2526,3147,3543", FirstFPs(9));
  EXPECT_EQ_Ribbon("665,727,1323,1755,3866,4232,4442,4492,4736", FirstFPs(9));

  // This used to be 11 probes, but 9 is a better choice for speed
  // AND accuracy.
  // FP rate @ 11 probes, old Bloom: 0.3571%
  // FP rate @ 11 probes, new Bloom: 0.0884%
  // FP rate @  9 probes, new Bloom: 0.0843%
  ResetPolicy(16);  // num_probes = 9 (new), 11 (old)
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ_LegacyBloom(GetNumProbesFromFilterData(), 11);
  EXPECT_EQ_FastBloom(GetNumProbesFromFilterData(), 9);
  EXPECT_EQ_Ribbon(GetRibbonSeedFromFilterData(), 61);

  EXPECT_EQ_LegacyBloom(
      BloomHash(FilterData()),
      SelectByCacheLineSize(1129406313, 3049154394U, 1727750964));
  EXPECT_EQ_FastBloom(BloomHash(FilterData()), 1087138490U);
  EXPECT_EQ_Ribbon(BloomHash(FilterData()), 459379967U);

  EXPECT_EQ_FastBloom("3299,3611,3916,6620,7822,8079,8482,8942", FirstFPs(8));
  EXPECT_EQ_Ribbon("727,1323,1755,4442,4736,5386,6974,7154,8222", FirstFPs(9));

  ResetPolicy(10);  // num_probes = 6, but different memory ratio vs. 9
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ_Bloom(GetNumProbesFromFilterData(), 6);
  EXPECT_EQ_Ribbon(GetRibbonSeedFromFilterData(), 61);

  EXPECT_EQ_LegacyBloom(
      BloomHash(FilterData()),
      SelectByCacheLineSize(1478976371, 2910591341U, 1182970461));
  EXPECT_EQ_FastBloom(BloomHash(FilterData()), 2498541272U);
  EXPECT_EQ_Ribbon(BloomHash(FilterData()), 1273231667U);

  EXPECT_EQ_FastBloom("16,126,133,422,466,472,813,1002,1035", FirstFPs(9));
  EXPECT_EQ_Ribbon("296,411,419,612,619,623,630,665,686,727", FirstFPs(10));

  ResetPolicy(10);
  for (int key = /*CHANGED*/ 1; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ_Bloom(GetNumProbesFromFilterData(), 6);
  EXPECT_EQ_Ribbon(GetRibbonSeedFromFilterData(), /*CHANGED*/ 184);

  EXPECT_EQ_LegacyBloom(
      BloomHash(FilterData()),
      SelectByCacheLineSize(4205696321U, 1132081253U, 2385981855U));
  EXPECT_EQ_FastBloom(BloomHash(FilterData()), 2058382345U);
  EXPECT_EQ_Ribbon(BloomHash(FilterData()), 3007790572U);

  EXPECT_EQ_FastBloom("16,126,133,422,466,472,813,1002,1035", FirstFPs(9));
  EXPECT_EQ_Ribbon("33,152,383,497,589,633,737,781,911,990", FirstFPs(10));

  ResetPolicy(10);
  for (int key = 1; key < /*CHANGED*/ 2088; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ_Bloom(GetNumProbesFromFilterData(), 6);
  EXPECT_EQ_Ribbon(GetRibbonSeedFromFilterData(), 184);

  EXPECT_EQ_LegacyBloom(
      BloomHash(FilterData()),
      SelectByCacheLineSize(2885052954U, 769447944, 4175124908U));
  EXPECT_EQ_FastBloom(BloomHash(FilterData()), 23699164U);
  EXPECT_EQ_Ribbon(BloomHash(FilterData()), 1942323379U);

  EXPECT_EQ_FastBloom("16,126,133,422,466,472,813,1002,1035", FirstFPs(9));
  EXPECT_EQ_Ribbon("33,95,360,589,737,911,990,1048,1081,1414", FirstFPs(10));

  // With new fractional bits_per_key, check that we are rounding to
  // whole bits per key for old Bloom filters but fractional for
  // new Bloom filter.
  ResetPolicy(9.5);
  for (int key = 1; key < 2088; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ_Bloom(GetNumProbesFromFilterData(), 6);
  EXPECT_EQ_Ribbon(GetRibbonSeedFromFilterData(), 184);

  EXPECT_EQ_LegacyBloom(
      BloomHash(FilterData()),
      /*SAME*/ SelectByCacheLineSize(2885052954U, 769447944, 4175124908U));
  EXPECT_EQ_FastBloom(BloomHash(FilterData()), 3166884174U);
  EXPECT_EQ_Ribbon(BloomHash(FilterData()), 1148258663U);

  EXPECT_EQ_FastBloom("126,156,367,444,458,791,813,976,1015", FirstFPs(9));
  EXPECT_EQ_Ribbon("33,54,95,360,589,693,737,911,990,1048", FirstFPs(10));

  ResetPolicy(10.499);
  for (int key = 1; key < 2088; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ_LegacyBloom(GetNumProbesFromFilterData(), 6);
  EXPECT_EQ_FastBloom(GetNumProbesFromFilterData(), 7);
  EXPECT_EQ_Ribbon(GetRibbonSeedFromFilterData(), 184);

  EXPECT_EQ_LegacyBloom(
      BloomHash(FilterData()),
      /*SAME*/ SelectByCacheLineSize(2885052954U, 769447944, 4175124908U));
  EXPECT_EQ_FastBloom(BloomHash(FilterData()), 4098502778U);
  EXPECT_EQ_Ribbon(BloomHash(FilterData()), 792138188U);

  EXPECT_EQ_FastBloom("16,236,240,472,1015,1045,1111,1409,1465", FirstFPs(9));
  EXPECT_EQ_Ribbon("33,95,360,589,737,990,1048,1081,1414,1643", FirstFPs(10));

  ResetPolicy();
}

// A helper class for testing custom or corrupt filter bits as read by
// built-in FilterBitsReaders.
struct RawFilterTester {
  // Buffer, from which we always return a tail Slice, so the
  // last five bytes are always the metadata bytes.
  std::array<char, 3000> data_{};
  // Points five bytes from the end
  char* metadata_ptr_;

  RawFilterTester() : metadata_ptr_(&*(data_.end() - 5)) {}

  Slice ResetNoFill(uint32_t len_without_metadata, uint32_t num_lines,
                    uint32_t num_probes) {
    metadata_ptr_[0] = static_cast<char>(num_probes);
    EncodeFixed32(metadata_ptr_ + 1, num_lines);
    uint32_t len = len_without_metadata + /*metadata*/ 5;
    assert(len <= data_.size());
    return Slice(metadata_ptr_ - len_without_metadata, len);
  }

  Slice Reset(uint32_t len_without_metadata, uint32_t num_lines,
              uint32_t num_probes, bool fill_ones) {
    data_.fill(fill_ones ? 0xff : 0);
    return ResetNoFill(len_without_metadata, num_lines, num_probes);
  }

  Slice ResetWeirdFill(uint32_t len_without_metadata, uint32_t num_lines,
                       uint32_t num_probes) {
    for (uint32_t i = 0; i < data_.size(); ++i) {
      data_[i] = static_cast<char>(0x7b7b >> (i % 7));
    }
    return ResetNoFill(len_without_metadata, num_lines, num_probes);
  }
};

TEST_P(FullBloomTest, RawSchema) {
  RawFilterTester cft;
  // Legacy Bloom configurations
  // Two probes, about 3/4 bits set: ~50% "FP" rate
  // One 256-byte cache line.
  OpenRaw(cft.ResetWeirdFill(256, 1, 2));
  EXPECT_EQ(uint64_t{11384799501900898790U}, PackedMatches());

  // Two 128-byte cache lines.
  OpenRaw(cft.ResetWeirdFill(256, 2, 2));
  EXPECT_EQ(uint64_t{10157853359773492589U}, PackedMatches());

  // Four 64-byte cache lines.
  OpenRaw(cft.ResetWeirdFill(256, 4, 2));
  EXPECT_EQ(uint64_t{7123594913907464682U}, PackedMatches());

  // Fast local Bloom configurations (marker 255 -> -1)
  // Two probes, about 3/4 bits set: ~50% "FP" rate
  // Four 64-byte cache lines.
  OpenRaw(cft.ResetWeirdFill(256, 2U << 8, 255));
  EXPECT_EQ(uint64_t{9957045189927952471U}, PackedMatches());

  // Ribbon configurations (marker 254 -> -2)

  // Even though the builder never builds configurations this
  // small (preferring Bloom), we can test that the configuration
  // can be read, for possible future-proofing.

  // 256 slots, one result column = 32 bytes (2 blocks, seed 0)
  // ~50% FP rate:
  // 0b0101010111110101010000110000011011011111100100001110010011101010
  OpenRaw(cft.ResetWeirdFill(32, 2U << 8, 254));
  EXPECT_EQ(uint64_t{6193930559317665002U}, PackedMatches());

  // 256 slots, three-to-four result columns = 112 bytes
  // ~ 1 in 10 FP rate:
  // 0b0000000000100000000000000000000001000001000000010000101000000000
  OpenRaw(cft.ResetWeirdFill(112, 2U << 8, 254));
  EXPECT_EQ(uint64_t{9007200345328128U}, PackedMatches());
}

TEST_P(FullBloomTest, CorruptFilters) {
  RawFilterTester cft;

  for (bool fill : {false, true}) {
    // Legacy Bloom configurations
    // Good filter bits - returns same as fill
    OpenRaw(cft.Reset(CACHE_LINE_SIZE, 1, 6, fill));
    ASSERT_EQ(fill, Matches("hello"));
    ASSERT_EQ(fill, Matches("world"));

    // Good filter bits - returns same as fill
    OpenRaw(cft.Reset(CACHE_LINE_SIZE * 3, 3, 6, fill));
    ASSERT_EQ(fill, Matches("hello"));
    ASSERT_EQ(fill, Matches("world"));

    // Good filter bits - returns same as fill
    // 256 is unusual but legal cache line size
    OpenRaw(cft.Reset(256 * 3, 3, 6, fill));
    ASSERT_EQ(fill, Matches("hello"));
    ASSERT_EQ(fill, Matches("world"));

    // Good filter bits - returns same as fill
    // 30 should be max num_probes
    OpenRaw(cft.Reset(CACHE_LINE_SIZE, 1, 30, fill));
    ASSERT_EQ(fill, Matches("hello"));
    ASSERT_EQ(fill, Matches("world"));

    // Good filter bits - returns same as fill
    // 1 should be min num_probes
    OpenRaw(cft.Reset(CACHE_LINE_SIZE, 1, 1, fill));
    ASSERT_EQ(fill, Matches("hello"));
    ASSERT_EQ(fill, Matches("world"));

    // Type 1 trivial filter bits - returns true as if FP by zero probes
    OpenRaw(cft.Reset(CACHE_LINE_SIZE, 1, 0, fill));
    ASSERT_TRUE(Matches("hello"));
    ASSERT_TRUE(Matches("world"));

    // Type 2 trivial filter bits - returns false as if built from zero keys
    OpenRaw(cft.Reset(0, 0, 6, fill));
    ASSERT_FALSE(Matches("hello"));
    ASSERT_FALSE(Matches("world"));

    // Type 2 trivial filter bits - returns false as if built from zero keys
    OpenRaw(cft.Reset(0, 37, 6, fill));
    ASSERT_FALSE(Matches("hello"));
    ASSERT_FALSE(Matches("world"));

    // Type 2 trivial filter bits - returns false as 0 size trumps 0 probes
    OpenRaw(cft.Reset(0, 0, 0, fill));
    ASSERT_FALSE(Matches("hello"));
    ASSERT_FALSE(Matches("world"));

    // Bad filter bits - returns true for safety
    // No solution to 0 * x == CACHE_LINE_SIZE
    OpenRaw(cft.Reset(CACHE_LINE_SIZE, 0, 6, fill));
    ASSERT_TRUE(Matches("hello"));
    ASSERT_TRUE(Matches("world"));

    // Bad filter bits - returns true for safety
    // Can't have 3 * x == 4 for integer x
    OpenRaw(cft.Reset(4, 3, 6, fill));
    ASSERT_TRUE(Matches("hello"));
    ASSERT_TRUE(Matches("world"));

    // Bad filter bits - returns true for safety
    // 97 bytes is not a power of two, so not a legal cache line size
    OpenRaw(cft.Reset(97 * 3, 3, 6, fill));
    ASSERT_TRUE(Matches("hello"));
    ASSERT_TRUE(Matches("world"));

    // Bad filter bits - returns true for safety
    // 65 bytes is not a power of two, so not a legal cache line size
    OpenRaw(cft.Reset(65 * 3, 3, 6, fill));
    ASSERT_TRUE(Matches("hello"));
    ASSERT_TRUE(Matches("world"));

    // Bad filter bits - returns false as if built from zero keys
    // < 5 bytes overall means missing even metadata
    OpenRaw(cft.Reset(static_cast<uint32_t>(-1), 3, 6, fill));
    ASSERT_FALSE(Matches("hello"));
    ASSERT_FALSE(Matches("world"));

    OpenRaw(cft.Reset(static_cast<uint32_t>(-5), 3, 6, fill));
    ASSERT_FALSE(Matches("hello"));
    ASSERT_FALSE(Matches("world"));

    // Bad filter bits - returns true for safety
    // num_lines * CACHE_LINE_SIZE only equals len modulo 2^32, so num_lines
    // must not be trusted to address the filter data
    OpenRaw(cft.Reset(CACHE_LINE_SIZE,
                      1 + static_cast<uint32_t>((uint64_t{1} << 32) /
                                                CACHE_LINE_SIZE),
                      6, fill));
    ASSERT_TRUE(Matches("hello"));
    ASSERT_TRUE(Matches("world"));

    // Dubious filter bits - returns same as fill (for now)
    // 31 is not a useful num_probes, nor generated by RocksDB unless directly
    // using filter bits API without BloomFilterPolicy.
    OpenRaw(cft.Reset(CACHE_LINE_SIZE, 1, 31, fill));
    ASSERT_EQ(fill, Matches("hello"));
    ASSERT_EQ(fill, Matches("world"));

    // Dubious filter bits - returns same as fill (for now)
    // Similar, with 127, largest positive char
    OpenRaw(cft.Reset(CACHE_LINE_SIZE, 1, 127, fill));
    ASSERT_EQ(fill, Matches("hello"));
    ASSERT_EQ(fill, Matches("world"));

    // Dubious filter bits - returns true (for now)
    // num_probes set to 128 / -128, lowest negative char
    // NB: Bug in implementation interprets this as negative and has same
    // effect as zero probes, but effectively reserves negative char values
    // for future use.
    OpenRaw(cft.Reset(CACHE_LINE_SIZE, 1, 128, fill));
    ASSERT_TRUE(Matches("hello"));
    ASSERT_TRUE(Matches("world"));

    // Dubious filter bits - returns true (for now)
    // Similar, with 253 / -3
    OpenRaw(cft.Reset(CACHE_LINE_SIZE, 1, 253, fill));
    ASSERT_TRUE(Matches("hello"));
    ASSERT_TRUE(Matches("world"));

    // #########################################################
    // Fast local Bloom configurations (marker 255 -> -1)
    // Good config with six probes
    OpenRaw(cft.Reset(CACHE_LINE_SIZE, 6U << 8, 255, fill));
    ASSERT_EQ(fill, Matches("hello"));
    ASSERT_EQ(fill, Matches("world"));

    // Becomes bad/reserved config (always true) if any other byte set
    OpenRaw(cft.Reset(CACHE_LINE_SIZE, (6U << 8) | 1U, 255, fill));
    ASSERT_TRUE(Matches("hello"));
    ASSERT_TRUE(Matches("world"));

    OpenRaw(cft.Reset(CACHE_LINE_SIZE, (6U << 8) | (1U << 16), 255, fill));
    ASSERT_TRUE(Matches("hello"));
    ASSERT_TRUE(Matches("world"));

    OpenRaw(cft.Reset(CACHE_LINE_SIZE, (6U << 8) | (1U << 24), 255, fill));
    ASSERT_TRUE(Matches("hello"));
    ASSERT_TRUE(Matches("world"));

    // Good config, max 30 probes
    OpenRaw(cft.Reset(CACHE_LINE_SIZE, 30U << 8, 255, fill));
    ASSERT_EQ(fill, Matches("hello"));
    ASSERT_EQ(fill, Matches("world"));

    // Bad/reserved config (always true) if more than 30
    OpenRaw(cft.Reset(CACHE_LINE_SIZE, 31U << 8, 255, fill));
    ASSERT_TRUE(Matches("hello"));
    ASSERT_TRUE(Matches("world"));

    OpenRaw(cft.Reset(CACHE_LINE_SIZE, 33U << 8, 255, fill));
    ASSERT_TRUE(Matches("hello"));
    ASSERT_TRUE(Matches("world"));

    OpenRaw(cft.Reset(CACHE_LINE_SIZE, 66U << 8, 255, fill));
    ASSERT_TRUE(Matches("hello"));
    ASSERT_TRUE(Matches("world"));

    OpenRaw(cft.Reset(CACHE_LINE_SIZE, 130U << 8, 255, fill));
    ASSERT_TRUE(Matches("hello"));
    ASSERT_TRUE(Matches("world"));
  }

  // #########################################################
  // Ribbon configurations (marker 254 -> -2)
  // ("fill" doesn't work to detect good configurations, we just
  // have to rely on TN probability)

  // Good: 2 blocks * 16 bytes / segment * 4 columns = 128 bytes
  // seed = 123
  OpenRaw(cft.Reset(128, (2U << 8) + 123U, 254, false));
  ASSERT_FALSE(Matches("hello"));
  ASSERT_FALSE(Matches("world"));

  // Good: 2 blocks * 16 bytes / segment * 8 columns = 256 bytes
  OpenRaw(cft.Reset(256, (2U << 8) + 123U, 254, false));
  ASSERT_FALSE(Matches("hello"));
  ASSERT_FALSE(Matches("world"));

  // Surprisingly OK: 5000 blocks (640,000 slots) in only 1024 bits
  // -> average close to 0 columns
  OpenRaw(cft.Reset(128, (5000U << 8) + 123U, 254, false));
  // *Almost* all FPs
  ASSERT_TRUE(Matches("hello"));
  ASSERT_TRUE(Matches("world"));
  // Need many queries to find a "true negative"
  for (int i = 0; Matches(std::to_string(i)); ++i) {
    ASSERT_LT(i, 1000);
  }

  // Bad: 1 block not allowed (for implementation detail reasons)
  OpenRaw(cft.Reset(128, (1U << 8) + 123U, 254, false));
  ASSERT_TRUE(Matches("hello"));
  ASSERT_TRUE(Matches("world"));

  // Bad: 0 blocks not allowed
  OpenRaw(cft.Reset(128, (0U << 8) + 123U, 254, false));
  ASSERT_TRUE(Matches("hello"));
  ASSERT_TRUE(Matches("world"));
}

INSTANTIATE_TEST_CASE_P(Full, FullBloomTest,
                        testing::Values(kLegacyBloom, kFastLocalBloom,
                                        kStandard128Ribbon));

static double GetEffectiveBitsPerKey(FilterBitsBuilder* builder) {
  union {
    uint64_t key_value = 0;
    char key_bytes[8];
  };

  const unsigned kNumKeys = 1000;

  Slice key_slice{key_bytes, 8};
  for (key_value = 0; key_value < kNumKeys; ++key_value) {
    builder->AddKey(key_slice);
  }

  std::unique_ptr<const char[]> buf;
  auto filter = builder->Finish(&buf);
  return filter.size() * /*bits per byte*/ 8 / (1.0 * kNumKeys);
}

static void SetTestingLevel(int levelish, FilterBuildingContext* ctx) {
  if (levelish == -1) {
    // Flush is treated as level -1 for this option but actually level 0
    ctx->level_at_creation = 0;
    ctx->reason = TableFileCreationReason::kFlush;
  } else {
    ctx->level_at_creation = levelish;
    ctx->reason = TableFileCreationReason::kCompaction;
  }
}

TEST(RibbonTest, RibbonTestLevelThreshold) {
  BlockBasedTableOptions opts;
  FilterBuildingContext ctx(opts);

  std::shared_ptr<FilterPolicy> reused{NewRibbonFilterPolicy(10)};

  // A few settings
  for (CompactionStyle cs : {kCompactionStyleLevel, kCompactionStyleUniversal,
                             kCompactionStyleFIFO, kCompactionStyleNone}) {
    ctx.compaction_style = cs;
    for (int bloom_before_level : {-1, 0, 1, 10, INT_MAX - 1, INT_MAX}) {
      SCOPED_TRACE("bloom_before_level=" + std::to_string(bloom_before_level));
      std::vector<std::shared_ptr<FilterPolicy> > policies;
      policies.emplace_back(NewRibbonFilterPolicy(10, bloom_before_level));

      if (bloom_before_level == 0) {
        // Also test new API default
        policies.emplace_back(NewRibbonFilterPolicy(10));
      }

      ASSERT_OK(reused->ConfigureOption({}, "bloom_before_level",
                                        std::to_string(bloom_before_level)));

      policies.push_back(reused);

      for (auto& policy : policies) {
        std::unique_ptr<FilterBitsBuilder> builder;
        if (bloom_before_level < INT_MAX) {
          // Claim to be generating filter for this level
          SetTestingLevel(bloom_before_level, &ctx);

          builder.reset(policy->GetBuilderWithContext(ctx));

          // Must be Ribbon (more space efficient than 10 bits per key)
          ASSERT_LT(GetEffectiveBitsPerKey(builder.get()), 8);
        }
        if (bloom_before_level >= 0) {
          // Claim to be generating filter for previous level
          SetTestingLevel(bloom_before_level - 1, &ctx);

          builder.reset(policy->GetBuilderWithContext(ctx));

          if (cs == kCompactionStyleLevel || cs == kCompactionStyleUniversal) {
            // Level is considered.
            // Must be Bloom (~ 10 bits per key)
            ASSERT_GT(GetEffectiveBitsPerKey(builder.get()), 9);
          } else if (bloom_before_level == INT_MAX) {
            // Force bloom option
            // Must be Bloom (~ 10 bits per key)
            ASSERT_GT(GetEffectiveBitsPerKey(builder.get()), 9);
          } else {
            // Level is ignored under non-traditional compaction styles.
            // Must be Ribbon (more space efficient than 10 bits per key)
            ASSERT_LT(GetEffectiveBitsPerKey(builder.get()), 8);
          }
        }

        // Like SST file writer
        ctx.level_at_creation = -1;
        ctx.reason = TableFileCreationReason::kSstFileWriter;

        builder.reset(policy->GetBuilderWithContext(ctx));

        if (bloom_before_level < INT_MAX) {
          // Must be Ribbon (more space efficient than 10 bits per key)
          ASSERT_LT(GetEffectiveBitsPerKey(builder.get()), 8);
        } else {
          // Force bloom option
          // Must be Bloom (~ 10 bits per key)
          ASSERT_GT(GetEffectiveBitsPerKey(builder.get()), 9);
        }
      }
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  ParseCommandLineFlags(&argc, &argv, true);

  return RUN_ALL_TESTS();
}

#endif  // GFLAGS
