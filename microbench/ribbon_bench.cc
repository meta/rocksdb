//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// this is a simple micro-benchmark for compare ribbon filter vs. other filter
// for more comprehensive, please check the dedicate util/filter_bench.
#include "benchmark/benchmark.h"
#include "table/block_based/filter_policy_internal.h"
#include "table/block_based/mock_block_based_table.h"

namespace ROCKSDB_NAMESPACE {

struct KeyMaker {
  explicit KeyMaker(size_t avg_size)
      : smallest_size_(avg_size),
        buf_size_(avg_size + 11),  // pad to vary key size and alignment
        buf_(new char[buf_size_]) {
    memset(buf_.get(), 0, buf_size_);
    assert(smallest_size_ > 8);
  }
  size_t smallest_size_;
  size_t buf_size_;
  std::unique_ptr<char[]> buf_;

  // Returns a unique(-ish) key based on the given parameter values. Each
  // call returns a Slice from the same buffer so previously returned
  // Slices should be considered invalidated.
  Slice Get(uint32_t filter_num, uint32_t val_num) const {
    size_t start = val_num % 4;
    size_t len = smallest_size_;
    // To get range [avg_size - 2, avg_size + 2]
    // use range [smallest_size, smallest_size + 4]
    len += FastRange32((val_num >> 5) * 1234567891, 5);
    char* data = buf_.get() + start;
    // Populate key data such that all data makes it into a key of at
    // least 8 bytes. We also don't want all the within-filter key
    // variance confined to a contiguous 32 bits, because then a 32 bit
    // hash function can "cheat" the false positive rate by
    // approximating a perfect hash.
    EncodeFixed32(data, val_num);
    EncodeFixed32(data + 4, filter_num + val_num);
    // ensure clearing leftovers from different alignment
    EncodeFixed32(data + 8, 0);
    return {data, len};
  }
};

// benchmark arguments:
// 0. filter impl (like filter_bench -impl)
// 1. filter config bits_per_key
// 2. average data key length
// 3. data entry number
static void CustomArguments(benchmark::internal::Benchmark* b) {
  const auto kImplCount =
      static_cast<int>(BloomLikeFilterPolicy::GetAllFixedImpls().size());
  for (int filter_impl = 0; filter_impl < kImplCount; ++filter_impl) {
    for (int bits_per_key : {10, 20}) {
      for (int key_len_avg : {10, 100}) {
        for (int64_t entry_num : {1 << 10, 1 << 20}) {
          b->Args({filter_impl, bits_per_key, key_len_avg, entry_num});
        }
      }
    }
  }
  b->ArgNames({"filter_impl", "bits_per_key", "key_len_avg", "entry_num"});
}

static void FilterBuild(benchmark::State& state) {
  // setup data
  auto filter = BloomLikeFilterPolicy::Create(
      BloomLikeFilterPolicy::GetAllFixedImpls().at(state.range(0)),
      static_cast<double>(state.range(1)));
  auto tester = std::make_unique<mock::MockBlockBasedTableTester>(filter);
  KeyMaker km(state.range(2));
  std::unique_ptr<const char[]> owner;
  const int64_t kEntryNum = state.range(3);
  auto rnd = Random32(12345);
  uint32_t filter_num = rnd.Next();
  // run the test
  for ([[maybe_unused]] auto _ : state) {
    std::unique_ptr<FilterBitsBuilder> builder(tester->GetBuilder());
    for (uint32_t i = 0; i < kEntryNum; i++) {
      builder->AddKey(km.Get(filter_num, i));
    }
    auto ret = builder->Finish(&owner);
    state.counters["size"] = static_cast<double>(ret.size());
  }
}
BENCHMARK(FilterBuild)->Apply(CustomArguments);

static void FilterQueryPositive(benchmark::State& state) {
  // setup data
  auto filter = BloomLikeFilterPolicy::Create(
      BloomLikeFilterPolicy::GetAllFixedImpls().at(state.range(0)),
      static_cast<double>(state.range(1)));
  auto tester = std::make_unique<mock::MockBlockBasedTableTester>(filter);
  KeyMaker km(state.range(2));
  std::unique_ptr<const char[]> owner;
  const int64_t kEntryNum = state.range(3);
  auto rnd = Random32(12345);
  uint32_t filter_num = rnd.Next();
  std::unique_ptr<FilterBitsBuilder> builder(tester->GetBuilder());
  for (uint32_t i = 0; i < kEntryNum; i++) {
    builder->AddKey(km.Get(filter_num, i));
  }
  auto data = builder->Finish(&owner);
  std::unique_ptr<FilterBitsReader> reader{filter->GetFilterBitsReader(data)};

  // run test
  uint32_t i = 0;
  for ([[maybe_unused]] auto _ : state) {
    i++;
    i = i % kEntryNum;
    reader->MayMatch(km.Get(filter_num, i));
  }
}
BENCHMARK(FilterQueryPositive)->Apply(CustomArguments);

static void FilterQueryNegative(benchmark::State& state) {
  // setup data
  auto filter = BloomLikeFilterPolicy::Create(
      BloomLikeFilterPolicy::GetAllFixedImpls().at(state.range(0)),
      static_cast<double>(state.range(1)));
  auto tester = std::make_unique<mock::MockBlockBasedTableTester>(filter);
  KeyMaker km(state.range(2));
  std::unique_ptr<const char[]> owner;
  const int64_t kEntryNum = state.range(3);
  auto rnd = Random32(12345);
  uint32_t filter_num = rnd.Next();
  std::unique_ptr<FilterBitsBuilder> builder(tester->GetBuilder());
  for (uint32_t i = 0; i < kEntryNum; i++) {
    builder->AddKey(km.Get(filter_num, i));
  }
  auto data = builder->Finish(&owner);
  std::unique_ptr<FilterBitsReader> reader{filter->GetFilterBitsReader(data)};

  // run test
  uint32_t i = 0;
  double fp_cnt = 0;
  for ([[maybe_unused]] auto _ : state) {
    i++;
    auto result = reader->MayMatch(km.Get(filter_num + 1, i));
    if (result) {
      fp_cnt++;
    }
  }
  state.counters["fp_pct"] =
      benchmark::Counter(fp_cnt * 100, benchmark::Counter::kAvgIterations);
}
BENCHMARK(FilterQueryNegative)->Apply(CustomArguments);

}  // namespace ROCKSDB_NAMESPACE

BENCHMARK_MAIN();
