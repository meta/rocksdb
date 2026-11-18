//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "memory/arena.h"

#ifndef OS_WIN
#include <sys/resource.h>
#endif
#include "port/jemalloc_helper.h"
#include "port/port.h"
#include "test_util/testharness.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

namespace {
const size_t kHugePageSize = 2 * 1024 * 1024;
}  // namespace
class ArenaTest : public testing::Test {};

TEST_F(ArenaTest, Empty) { Arena arena0; }

namespace {
bool CheckMemoryAllocated(size_t allocated, size_t expected) {
  // The value returned by Arena::MemoryAllocatedBytes() may be greater than
  // the requested memory. We choose a somewhat arbitrary upper bound of
  // max_expected = expected * 1.1 to detect critical overallocation.
  size_t max_expected = expected + expected / 10;
  return allocated >= expected && allocated <= max_expected;
}

void MemoryAllocatedBytesTest(size_t huge_page_size) {
  const int N = 17;
  size_t req_sz;           // requested size
  size_t bsz = 32 * 1024;  // block size
  size_t expected_memory_allocated;

  Arena arena(bsz, nullptr, huge_page_size);

  // requested size > quarter of a block:
  //   allocate requested size separately
  req_sz = 12 * 1024;
  for (int i = 0; i < N; i++) {
    arena.Allocate(req_sz);
  }
  expected_memory_allocated = req_sz * N + Arena::kInlineSize;
  ASSERT_PRED2(CheckMemoryAllocated, arena.MemoryAllocatedBytes(),
               expected_memory_allocated);

  arena.Allocate(Arena::kInlineSize - 1);

  // requested size < quarter of a block:
  //   allocate a block with the default size, then try to use unused part
  //   of the block. So one new block will be allocated for the first
  //   Allocate(99) call. All the remaining calls won't lead to new allocation.
  req_sz = 99;
  for (int i = 0; i < N; i++) {
    arena.Allocate(req_sz);
  }
  if (huge_page_size) {
    ASSERT_TRUE(
        CheckMemoryAllocated(arena.MemoryAllocatedBytes(),
                             expected_memory_allocated + bsz) ||
        CheckMemoryAllocated(arena.MemoryAllocatedBytes(),
                             expected_memory_allocated + huge_page_size));
  } else {
    expected_memory_allocated += bsz;
    ASSERT_PRED2(CheckMemoryAllocated, arena.MemoryAllocatedBytes(),
                 expected_memory_allocated);
  }

  // requested size > size of a block:
  //   allocate requested size separately
  expected_memory_allocated = arena.MemoryAllocatedBytes();
  req_sz = 8 * 1024 * 1024;
  for (int i = 0; i < N; i++) {
    arena.Allocate(req_sz);
  }
  expected_memory_allocated += req_sz * N;
  ASSERT_PRED2(CheckMemoryAllocated, arena.MemoryAllocatedBytes(),
               expected_memory_allocated);
}

// Make sure we didn't count the allocate but not used memory space in
// Arena::ApproximateMemoryUsage()
static void ApproximateMemoryUsageTest(size_t huge_page_size) {
  const size_t kBlockSize = 4096;
  const size_t kEntrySize = kBlockSize / 8;
  const size_t kZero = 0;
  Arena arena(kBlockSize, nullptr, huge_page_size);
  ASSERT_EQ(kZero, arena.ApproximateMemoryUsage());

  // allocate inline bytes
  const size_t kAlignUnit = alignof(max_align_t);
  EXPECT_TRUE(arena.IsInInlineBlock());
  arena.AllocateAligned(kAlignUnit);
  EXPECT_TRUE(arena.IsInInlineBlock());
  arena.AllocateAligned(Arena::kInlineSize / 2 - (2 * kAlignUnit));
  EXPECT_TRUE(arena.IsInInlineBlock());
  arena.AllocateAligned(Arena::kInlineSize / 2);
  EXPECT_TRUE(arena.IsInInlineBlock());
  ASSERT_EQ(arena.ApproximateMemoryUsage(), Arena::kInlineSize - kAlignUnit);
  ASSERT_PRED2(CheckMemoryAllocated, arena.MemoryAllocatedBytes(),
               Arena::kInlineSize);

  auto num_blocks = kBlockSize / kEntrySize;

  // first allocation
  arena.AllocateAligned(kEntrySize);
  EXPECT_FALSE(arena.IsInInlineBlock());
  auto mem_usage = arena.MemoryAllocatedBytes();
  if (huge_page_size) {
    ASSERT_TRUE(
        CheckMemoryAllocated(mem_usage, kBlockSize + Arena::kInlineSize) ||
        CheckMemoryAllocated(mem_usage, huge_page_size + Arena::kInlineSize));
  } else {
    ASSERT_PRED2(CheckMemoryAllocated, mem_usage,
                 kBlockSize + Arena::kInlineSize);
  }
  auto usage = arena.ApproximateMemoryUsage();
  ASSERT_LT(usage, mem_usage);
  for (size_t i = 1; i < num_blocks; ++i) {
    arena.AllocateAligned(kEntrySize);
    ASSERT_EQ(mem_usage, arena.MemoryAllocatedBytes());
    ASSERT_EQ(arena.ApproximateMemoryUsage(), usage + kEntrySize);
    EXPECT_FALSE(arena.IsInInlineBlock());
    usage = arena.ApproximateMemoryUsage();
  }
  if (huge_page_size) {
    ASSERT_TRUE(usage > mem_usage ||
                usage + huge_page_size - kBlockSize == mem_usage);
  } else {
    ASSERT_GT(usage, mem_usage);
  }
}

static void SimpleTest(size_t huge_page_size) {
  std::vector<std::pair<size_t, char*>> allocated;
  Arena arena(Arena::kMinBlockSize, nullptr, huge_page_size);
  const int N = 100000;
  size_t bytes = 0;
  Random rnd(301);
  for (int i = 0; i < N; i++) {
    size_t s;
    if (i % (N / 10) == 0) {
      s = i;
    } else {
      s = rnd.OneIn(4000)
              ? rnd.Uniform(6000)
              : (rnd.OneIn(10) ? rnd.Uniform(100) : rnd.Uniform(20));
    }
    if (s == 0) {
      // Our arena disallows size 0 allocations.
      s = 1;
    }
    char* r;
    if (rnd.OneIn(10)) {
      r = arena.AllocateAligned(s);
    } else {
      r = arena.Allocate(s);
    }

    for (unsigned int b = 0; b < s; b++) {
      // Fill the "i"th allocation with a known bit pattern
      r[b] = i % 256;
    }
    bytes += s;
    allocated.emplace_back(s, r);
    ASSERT_GE(arena.ApproximateMemoryUsage(), bytes);
    if (i > N / 10) {
      ASSERT_LE(arena.ApproximateMemoryUsage(), bytes * 1.10);
    }
  }
  for (unsigned int i = 0; i < allocated.size(); i++) {
    size_t num_bytes = allocated[i].first;
    const char* p = allocated[i].second;
    for (unsigned int b = 0; b < num_bytes; b++) {
      // Check the "i"th allocation for the known bit pattern
      ASSERT_EQ(int(p[b]) & 0xff, (int)(i % 256));
    }
  }
}
}  // namespace

TEST_F(ArenaTest, MemoryAllocatedBytes) {
  MemoryAllocatedBytesTest(0);
  MemoryAllocatedBytesTest(kHugePageSize);
}

TEST_F(ArenaTest, ApproximateMemoryUsage) {
  ApproximateMemoryUsageTest(0);
  ApproximateMemoryUsageTest(kHugePageSize);
}

TEST_F(ArenaTest, Simple) {
  SimpleTest(0);
  SimpleTest(kHugePageSize);
}

// Number of minor page faults since last call
size_t PopMinorPageFaultCount() {
#ifdef RUSAGE_SELF
  static long prev = 0;
  struct rusage usage{};
  EXPECT_EQ(getrusage(RUSAGE_SELF, &usage), 0);
  size_t rv = usage.ru_minflt - prev;
  prev = usage.ru_minflt;
  return rv;
#else
  // Conservative
  return SIZE_MAX;
#endif  // RUSAGE_SELF
}

TEST(MmapTest, AllocateLazyZeroed) {
  // Doesn't have to be page aligned
  constexpr size_t len = 1234567;    // in bytes
  constexpr size_t count = len / 8;  // in uint64_t objects
  // Implicit conversion move
  TypedMemMapping<uint64_t> pre_arr = MemMapping::AllocateLazyZeroed(len);
  // Move from same type
  TypedMemMapping<uint64_t> arr = std::move(pre_arr);

  ASSERT_NE(arr.Get(), nullptr);
  ASSERT_EQ(arr.Get(), &arr[0]);
  ASSERT_EQ(arr.Get(), arr.MemMapping::Get());

  ASSERT_EQ(arr.Length(), len);
  ASSERT_EQ(arr.Count(), count);

  // Start counting page faults
  PopMinorPageFaultCount();

  // Access half of the allocation
  size_t i = 0;
  for (; i < count / 2; ++i) {
    ASSERT_EQ(arr[i], 0);
    arr[i] = i;
  }

  // Appropriate page faults (maybe more)
  size_t faults = PopMinorPageFaultCount();
  ASSERT_GE(faults, len / 2 / port::kPageSize);

  // Access rest of the allocation
  for (; i < count; ++i) {
    ASSERT_EQ(arr[i], 0);
    arr[i] = i;
  }

  // Appropriate page faults (maybe more)
  faults = PopMinorPageFaultCount();
  ASSERT_GE(faults, len / 2 / port::kPageSize);

  // Verify data
  for (i = 0; i < count; ++i) {
    ASSERT_EQ(arr[i], i);
  }
}

TEST_F(ArenaTest, UnmappedAllocation) {
  // Verify that it's possible to get unmapped pages in large allocations,
  // for memory efficiency and to ensure we don't accidentally waste time &
  // space initializing the memory.

#ifdef ROCKSDB_JEMALLOC
  // With Jemalloc config.fill, the pages are written to before we get them
  uint8_t fill = 0;
  size_t fill_sz = sizeof(fill);
  mallctl("config.fill", &fill, &fill_sz, nullptr, 0);
  if (fill) {
    ROCKSDB_GTEST_BYPASS("Test skipped because of config.fill==true");
    return;
  }
#endif  // ROCKSDB_JEMALLOC

  // This block size value is smaller than the smallest x86 huge page size,
  // so should not be fulfilled by a transparent huge page mapping.
  constexpr size_t kBlockSize = 1U << 20;
  Arena arena(kBlockSize);

  // The allocator might give us back recycled memory for a while, but
  // shouldn't last forever.
  for (int i = 0;; ++i) {
    char* p = arena.Allocate(kBlockSize);

    // Start counting page faults
    PopMinorPageFaultCount();

    // Overwrite the whole allocation
    for (size_t j = 0; j < kBlockSize; ++j) {
      p[j] = static_cast<char>(j & 255);
    }

    size_t faults = PopMinorPageFaultCount();
    if (faults >= kBlockSize * 3 / 4 / port::kPageSize) {
      // Most of the access generated page faults => GOOD
      break;
    }
    // Should have succeeded after enough tries
    ASSERT_LT(i, 1000);
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
