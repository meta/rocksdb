// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <array>
#include <atomic>

#include "rocksdb/slice.h"
#include "table/multiget_context.h"
#include "util/atomic.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

class Slice;
class Allocator;
class Logger;

// A Bloom filter intended only to be used in memory, never serialized in a way
// that could lead to schema incompatibility. Supports opt-in lock-free
// concurrent access.
//
// This implementation is also intended for applications generally preferring
// speed vs. maximum accuracy: roughly 0.9x BF op latency for 1.1x FP rate.
// For 1% FP rate, that means that the latency of a look-up triggered by an FP
// should be less than roughly 100x the cost of a Bloom filter op.
//
// For simplicity and performance, the current implementation requires
// num_probes to be a multiple of two and <= 10.
//
class DynamicBloom {
 public:
  // allocator: pass allocator to bloom filter, hence trace the usage of memory
  // total_bits: fixed total bits for the bloom
  // num_probes: number of hash probes for a single key
  // hash_func:  customized hash function
  // huge_page_tlb_size:  if >0, try to allocate bloom bytes from huge page TLB
  //                      within this page size. Need to reserve huge pages for
  //                      it to be allocated, like:
  //                         sysctl -w vm.nr_hugepages=20
  //                     See linux doc Documentation/vm/hugetlbpage.txt
  explicit DynamicBloom(Allocator* allocator, uint32_t total_bits,
                        uint32_t num_probes = 6, size_t huge_page_tlb_size = 0,
                        Logger* logger = nullptr);

  ~DynamicBloom() = default;

  // Assuming single thread adding to the DynamicBloom
  void Add(const Slice& key);

  // Like Add, but may be called concurrently with other functions. Does not
  // establish happens-before relationship with other functions so requires some
  // external mechanism to ensure other threads can see the change.
  void AddConcurrently(const Slice& key);

  // Assuming single threaded access to this function.
  void AddHash(uint32_t hash);

  // Like AddHash, but may be called concurrently with other functions. Does not
  // establish happens-before relationship with other functions so requires some
  // external mechanism to ensure other threads can see the change.
  void AddHashConcurrently(uint32_t hash);

  // Multithreaded access to this function is OK
  bool MayContain(const Slice& key) const;

  void MayContain(int num_keys, Slice* keys, bool* may_match) const;

  // Multithreaded access to this function is OK
  bool MayContainHash(uint32_t hash) const;

  void Prefetch(uint32_t h);

 private:
  // Length of the structure, in 64-bit words. For this structure, "word"
  // will always refer to 64-bit words.
  uint32_t kLen;
  // We make the k probes in pairs, two for each 64-bit read/write. Thus,
  // this stores k/2, the number of words to double-probe.
  const uint32_t kNumDoubleProbes;

  RelaxedAtomic<uint64_t>* data_;

  // or_func(ptr, mask) should effect *ptr |= mask with the appropriate
  // concurrency safety, working with bytes.
  template <typename OrFunc>
  void AddHash(uint32_t hash, const OrFunc& or_func);

  bool DoubleProbe(uint32_t h32, size_t a) const;
};

inline void DynamicBloom::Add(const Slice& key) { AddHash(BloomHash(key)); }

inline void DynamicBloom::AddConcurrently(const Slice& key) {
  AddHashConcurrently(BloomHash(key));
}

inline void DynamicBloom::AddHash(uint32_t hash) {
  AddHash(hash, [](RelaxedAtomic<uint64_t>* ptr, uint64_t mask) {
    ptr->StoreRelaxed(ptr->LoadRelaxed() | mask);
  });
}

inline void DynamicBloom::AddHashConcurrently(uint32_t hash) {
  AddHash(hash, [](RelaxedAtomic<uint64_t>* ptr, uint64_t mask) {
    // Happens-before between AddHash and MaybeContains is handled by
    // access to versions_->LastSequence(), so all we have to do here is
    // avoid races (so we don't give the compiler a license to mess up
    // our code) and not lose bits.  std::memory_order_relaxed is enough
    // for that.
    if ((mask & ptr->LoadRelaxed()) != mask) {
      ptr->FetchOrRelaxed(mask);
    }
  });
}

inline bool DynamicBloom::MayContain(const Slice& key) const {
  return (MayContainHash(BloomHash(key)));
}

inline void DynamicBloom::MayContain(int num_keys, Slice* keys,
                                     bool* may_match) const {
  std::array<uint32_t, MultiGetContext::MAX_BATCH_SIZE> hashes;
  std::array<size_t, MultiGetContext::MAX_BATCH_SIZE> byte_offsets;
  for (int i = 0; i < num_keys; ++i) {
    hashes[i] = BloomHash(keys[i]);
    size_t a = FastRange32(hashes[i], kLen);
    PREFETCH(data_ + a, 0, 3);
    byte_offsets[i] = a;
  }

  for (int i = 0; i < num_keys; i++) {
    may_match[i] = DoubleProbe(hashes[i], byte_offsets[i]);
  }
}

#if defined(_MSC_VER)
#pragma warning(push)
// local variable is initialized but not referenced
#pragma warning(disable : 4189)
#endif
inline void DynamicBloom::Prefetch(uint32_t h32) {
  size_t a = FastRange32(h32, kLen);
  PREFETCH(data_ + a, 0, 3);
}
#if defined(_MSC_VER)
#pragma warning(pop)
#endif

// Speed hacks in this implementation:
// * Uses fastrange instead of %
// * Minimum logic to determine first (and all) probed memory addresses.
//   (Uses constant bit-xor offsets from the starting probe address.)
// * (Major) Two probes per 64-bit memory fetch/write.
//   Code simplification / optimization: only allow even number of probes.
// * Very fast and effective (murmur-like) hash expansion/re-mixing. (At
// least on recent CPUs, integer multiplication is very cheap. Each 64-bit
// remix provides five pairs of bit addresses within a uint64_t.)
//   Code simplification / optimization: only allow up to 10 probes, from a
//   single 64-bit remix.
//
// The FP rate penalty for this implementation, vs. standard Bloom filter, is
// roughly 1.12x on top of the 1.15x penalty for a 512-bit cache-local Bloom.
// This implementation does not explicitly use the cache line size, but is
// effectively cache-local (up to 16 probes) because of the bit-xor offsetting.
//
// NB: could easily be upgraded to support a 64-bit hash and
// total_bits > 2^32 (512MB). (The latter is a bad idea without the former,
// because of false positives.)

inline bool DynamicBloom::MayContainHash(uint32_t h32) const {
  size_t a = FastRange32(h32, kLen);
  PREFETCH(data_ + a, 0, 3);
  return DoubleProbe(h32, a);
}

inline bool DynamicBloom::DoubleProbe(uint32_t h32, size_t byte_offset) const {
  // Expand/remix with 64-bit golden ratio
  uint64_t h = 0x9e3779b97f4a7c13ULL * h32;
  for (unsigned i = 0;; ++i) {
    // Two bit probes per uint64_t probe
    uint64_t mask =
        ((uint64_t)1 << (h & 63)) | ((uint64_t)1 << ((h >> 6) & 63));
    uint64_t val = data_[byte_offset ^ i].LoadRelaxed();
    if (i + 1 >= kNumDoubleProbes) {
      return (val & mask) == mask;
    } else if ((val & mask) != mask) {
      return false;
    }
    h = (h >> 12) | (h << 52);
  }
}

template <typename OrFunc>
inline void DynamicBloom::AddHash(uint32_t h32, const OrFunc& or_func) {
  size_t a = FastRange32(h32, kLen);
  PREFETCH(data_ + a, 0, 3);
  // Expand/remix with 64-bit golden ratio
  uint64_t h = 0x9e3779b97f4a7c13ULL * h32;
  for (unsigned i = 0;; ++i) {
    // Two bit probes per uint64_t probe
    uint64_t mask =
        ((uint64_t)1 << (h & 63)) | ((uint64_t)1 << ((h >> 6) & 63));
    or_func(&data_[a ^ i], mask);
    if (i + 1 >= kNumDoubleProbes) {
      return;
    }
    h = (h >> 12) | (h << 52);
  }
}

}  // namespace ROCKSDB_NAMESPACE
