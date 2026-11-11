//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run this test... Skipping...\n");
  return 0;
}
#else

#include <algorithm>
#include <atomic>
#include <cinttypes>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include "dynamic_bloom.h"
#include "memory/arena.h"
#include "port/port.h"
#include "rocksdb/system_clock.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/gflags_compat.h"
#include "util/stop_watch.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;

DEFINE_int32(bits_per_key, 10, "");
DEFINE_int32(num_probes, 6, "");
DEFINE_bool(enable_perf, false, "");

namespace ROCKSDB_NAMESPACE {

struct KeyMaker {
  uint64_t a;
  uint64_t b;

  // Sequential, within a hash function block
  inline Slice Seq(uint64_t i) {
    a = i;
    return Slice(reinterpret_cast<char*>(&a), sizeof(a));
  }
  // Not quite sequential, varies across hash function blocks
  inline Slice Nonseq(uint64_t i) {
    a = i;
    b = i * 123;
    return Slice(reinterpret_cast<char*>(this), sizeof(*this));
  }
  inline Slice Key(uint64_t i, bool nonseq) {
    return nonseq ? Nonseq(i) : Seq(i);
  }
};

class DynamicBloomTest : public testing::Test {};

TEST_F(DynamicBloomTest, EmptyFilter) {
  Arena arena;
  DynamicBloom bloom1(&arena, 100, 2);
  ASSERT_TRUE(!bloom1.MayContain("hello"));
  ASSERT_TRUE(!bloom1.MayContain("world"));

  DynamicBloom bloom2(&arena, CACHE_LINE_SIZE * 8 * 2 - 1, 2);
  ASSERT_TRUE(!bloom2.MayContain("hello"));
  ASSERT_TRUE(!bloom2.MayContain("world"));
}

TEST_F(DynamicBloomTest, Small) {
  Arena arena;
  DynamicBloom bloom1(&arena, 100, 2);
  bloom1.Add("hello");
  bloom1.Add("world");
  ASSERT_TRUE(bloom1.MayContain("hello"));
  ASSERT_TRUE(bloom1.MayContain("world"));
  ASSERT_TRUE(!bloom1.MayContain("x"));
  ASSERT_TRUE(!bloom1.MayContain("foo"));

  DynamicBloom bloom2(&arena, CACHE_LINE_SIZE * 8 * 2 - 1, 2);
  bloom2.Add("hello");
  bloom2.Add("world");
  ASSERT_TRUE(bloom2.MayContain("hello"));
  ASSERT_TRUE(bloom2.MayContain("world"));
  ASSERT_TRUE(!bloom2.MayContain("x"));
  ASSERT_TRUE(!bloom2.MayContain("foo"));
}

TEST_F(DynamicBloomTest, SmallConcurrentAdd) {
  Arena arena;
  DynamicBloom bloom1(&arena, 100, 2);
  bloom1.AddConcurrently("hello");
  bloom1.AddConcurrently("world");
  ASSERT_TRUE(bloom1.MayContain("hello"));
  ASSERT_TRUE(bloom1.MayContain("world"));
  ASSERT_TRUE(!bloom1.MayContain("x"));
  ASSERT_TRUE(!bloom1.MayContain("foo"));

  DynamicBloom bloom2(&arena, CACHE_LINE_SIZE * 8 * 2 - 1, 2);
  bloom2.AddConcurrently("hello");
  bloom2.AddConcurrently("world");
  ASSERT_TRUE(bloom2.MayContain("hello"));
  ASSERT_TRUE(bloom2.MayContain("world"));
  ASSERT_TRUE(!bloom2.MayContain("x"));
  ASSERT_TRUE(!bloom2.MayContain("foo"));
}

static uint32_t NextNum(uint32_t num) {
  if (num < 10) {
    num += 1;
  } else if (num < 100) {
    num += 10;
  } else if (num < 1000) {
    num += 100;
  } else {
    num = num * 26 / 10;
  }
  return num;
}

TEST_F(DynamicBloomTest, VaryingLengths) {
  KeyMaker km;

  // Count number of filters that significantly exceed the false positive rate
  int mediocre_filters = 0;
  int good_filters = 0;
  uint32_t num_probes = static_cast<uint32_t>(FLAGS_num_probes);

  fprintf(stderr, "bits_per_key: %d  num_probes: %d\n", FLAGS_bits_per_key,
          num_probes);

  // NB: FP rate impact of 32-bit hash is noticeable starting around 10M keys.
  // But that effect is hidden if using sequential keys (unique hashes).
  for (bool nonseq : {false, true}) {
    const uint32_t max_num = FLAGS_enable_perf ? 40000000 : 400000;
    for (uint32_t num = 1; num <= max_num; num = NextNum(num)) {
      uint32_t bloom_bits = 0;
      Arena arena;
      bloom_bits = num * FLAGS_bits_per_key;
      DynamicBloom bloom(&arena, bloom_bits, num_probes);
      for (uint64_t i = 0; i < num; i++) {
        bloom.Add(km.Key(i, nonseq));
        ASSERT_TRUE(bloom.MayContain(km.Key(i, nonseq)));
      }

      // All added keys must match
      for (uint64_t i = 0; i < num; i++) {
        ASSERT_TRUE(bloom.MayContain(km.Key(i, nonseq)));
      }

      // Check false positive rate
      int result = 0;
      for (uint64_t i = 0; i < 30000; i++) {
        if (bloom.MayContain(km.Key(i + 1000000000, nonseq))) {
          result++;
        }
      }
      double rate = result / 30000.0;

      fprintf(stderr,
              "False positives (%s keys): "
              "%5.2f%% @ num = %6u, bloom_bits = %6u\n",
              nonseq ? "nonseq" : "seq", rate * 100.0, num, bloom_bits);

      if (rate > 0.0125) {
        mediocre_filters++;  // Allowed, but not too often
      } else {
        good_filters++;
      }
    }
  }

  fprintf(stderr, "Filters: %d good, %d mediocre\n", good_filters,
          mediocre_filters);
  ASSERT_LE(mediocre_filters, good_filters / 25);
}

TEST_F(DynamicBloomTest, perf) {
  KeyMaker km;
  StopWatchNano timer(SystemClock::Default().get());
  uint32_t num_probes = static_cast<uint32_t>(FLAGS_num_probes);

  if (!FLAGS_enable_perf) {
    return;
  }

  for (uint32_t m = 1; m <= 8; ++m) {
    Arena arena;
    const uint32_t num_keys = m * 8 * 1024 * 1024;
    fprintf(stderr, "testing %" PRIu32 "M keys\n", m * 8);

    DynamicBloom std_bloom(&arena, num_keys * 10, num_probes);

    timer.Start();
    for (uint64_t i = 1; i <= num_keys; ++i) {
      std_bloom.Add(km.Seq(i));
    }

    uint64_t elapsed = timer.ElapsedNanos();
    fprintf(stderr, "dynamic bloom, avg add latency %3g\n",
            static_cast<double>(elapsed) / num_keys);

    uint32_t count = 0;
    timer.Start();
    for (uint64_t i = 1; i <= num_keys; ++i) {
      if (std_bloom.MayContain(km.Seq(i))) {
        ++count;
      }
    }
    ASSERT_EQ(count, num_keys);
    elapsed = timer.ElapsedNanos();
    assert(count > 0);
    fprintf(stderr, "dynamic bloom, avg query latency %3g\n",
            static_cast<double>(elapsed) / count);
  }
}

TEST_F(DynamicBloomTest, ConcurrentWithPerf) {
  uint32_t num_probes = static_cast<uint32_t>(FLAGS_num_probes);

  uint32_t m_limit = FLAGS_enable_perf ? 8 : 1;

  uint32_t num_threads = 4;
  std::vector<port::Thread> threads;

  // NB: Uses sequential keys for speed, but that hides the FP rate
  // impact of 32-bit hash, which is noticeable starting around 10M keys
  // when they vary across hashing blocks.
  for (uint32_t m = 1; m <= m_limit; ++m) {
    Arena arena;
    const uint32_t num_keys = m * 8 * 1024 * 1024;
    fprintf(stderr, "testing %" PRIu32 "M keys\n", m * 8);

    DynamicBloom std_bloom(&arena, num_keys * 10, num_probes);

    std::atomic<uint64_t> elapsed(0);

    std::function<void(size_t)> adder([&](size_t t) {
      KeyMaker km;
      StopWatchNano timer(SystemClock::Default().get());
      timer.Start();
      for (uint64_t i = 1 + t; i <= num_keys; i += num_threads) {
        std_bloom.AddConcurrently(km.Seq(i));
      }
      elapsed += timer.ElapsedNanos();
    });
    for (size_t t = 0; t < num_threads; ++t) {
      threads.emplace_back(adder, t);
    }
    while (threads.size() > 0) {
      threads.back().join();
      threads.pop_back();
    }

    fprintf(stderr,
            "dynamic bloom, avg parallel add latency %3g"
            " nanos/key\n",
            static_cast<double>(elapsed) / num_threads / num_keys);

    elapsed = 0;
    std::function<void(size_t)> hitter([&](size_t t) {
      KeyMaker km;
      StopWatchNano timer(SystemClock::Default().get());
      timer.Start();
      for (uint64_t i = 1 + t; i <= num_keys; i += num_threads) {
        bool f = std_bloom.MayContain(km.Seq(i));
        ASSERT_TRUE(f);
      }
      elapsed += timer.ElapsedNanos();
    });
    for (size_t t = 0; t < num_threads; ++t) {
      threads.emplace_back(hitter, t);
    }
    while (threads.size() > 0) {
      threads.back().join();
      threads.pop_back();
    }

    fprintf(stderr,
            "dynamic bloom, avg parallel hit latency %3g"
            " nanos/key\n",
            static_cast<double>(elapsed) / num_threads / num_keys);

    elapsed = 0;
    std::atomic<uint32_t> false_positives(0);
    std::function<void(size_t)> misser([&](size_t t) {
      KeyMaker km;
      StopWatchNano timer(SystemClock::Default().get());
      timer.Start();
      for (uint64_t i = num_keys + 1 + t; i <= 2 * num_keys; i += num_threads) {
        bool f = std_bloom.MayContain(km.Seq(i));
        if (f) {
          ++false_positives;
        }
      }
      elapsed += timer.ElapsedNanos();
    });
    for (size_t t = 0; t < num_threads; ++t) {
      threads.emplace_back(misser, t);
    }
    while (threads.size() > 0) {
      threads.back().join();
      threads.pop_back();
    }

    fprintf(stderr,
            "dynamic bloom, avg parallel miss latency %3g"
            " nanos/key, %f%% false positive rate\n",
            static_cast<double>(elapsed) / num_threads / num_keys,
            false_positives.load() * 100.0 / num_keys);
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
