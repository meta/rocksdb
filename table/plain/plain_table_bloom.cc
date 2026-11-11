//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/plain/plain_table_bloom.h"

#include <algorithm>
#include <string>

#include "memory/allocator.h"
#include "util/dynamic_bloom.h"

namespace ROCKSDB_NAMESPACE {

namespace {

uint32_t GetTotalBitsForLocality(uint32_t total_bits) {
  uint32_t num_blocks =
      (total_bits + CACHE_LINE_SIZE * 8 - 1) / (CACHE_LINE_SIZE * 8);

  // Make num_blocks an odd number to make sure more bits are involved
  // when determining which block.
  if (num_blocks % 2 == 0) {
    num_blocks++;
  }

  return num_blocks * (CACHE_LINE_SIZE * 8);
}
}  // namespace

PlainTableBloomV1::PlainTableBloomV1(uint32_t num_probes)
    : kTotalBits(0), kNumBlocks(0), kNumProbes(num_probes), data_(nullptr) {}

void PlainTableBloomV1::SetRawData(char* raw_data, uint32_t total_bits,
                                   uint32_t num_blocks) {
  data_ = raw_data;
  kTotalBits = total_bits;
  kNumBlocks = num_blocks;
}

void PlainTableBloomV1::SetTotalBits(Allocator* allocator, uint32_t total_bits,
                                     uint32_t locality,
                                     size_t huge_page_tlb_size,
                                     Logger* logger) {
  kTotalBits = (locality > 0) ? GetTotalBitsForLocality(total_bits)
                              : (total_bits + 7) / 8 * 8;
  kNumBlocks = (locality > 0) ? (kTotalBits / (CACHE_LINE_SIZE * 8)) : 0;

  assert(kNumBlocks > 0 || kTotalBits > 0);
  assert(kNumProbes > 0);

  uint32_t sz = kTotalBits / 8;
  if (kNumBlocks > 0) {
    sz += CACHE_LINE_SIZE - 1;
  }
  assert(allocator);

  char* raw = allocator->AllocateAligned(sz, huge_page_tlb_size, logger);
  memset(raw, 0, sz);
  auto cache_line_offset = reinterpret_cast<uintptr_t>(raw) % CACHE_LINE_SIZE;
  if (kNumBlocks > 0 && cache_line_offset > 0) {
    raw += CACHE_LINE_SIZE - cache_line_offset;
  }
  data_ = raw;
}

void BloomBlockBuilder::AddKeysHashes(
    const std::vector<uint32_t>& keys_hashes) {
  for (auto hash : keys_hashes) {
    bloom_.AddHash(hash);
  }
}

Slice BloomBlockBuilder::Finish() { return bloom_.GetRawData(); }

}  // namespace ROCKSDB_NAMESPACE
