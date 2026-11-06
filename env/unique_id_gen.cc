//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "env/unique_id_gen.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <random>

#include "port/lang.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/version.h"
#include "util/hash.h"

#ifdef __SSE4_2__
#ifdef _WIN32
#include <intrin.h>
#define _rdtsc() __rdtsc()
#else
#include <x86intrin.h>
#endif
#else
#include "rocksdb/system_clock.h"
#endif

namespace ROCKSDB_NAMESPACE {

namespace {

struct GenerateRawUniqueIdOpts {
  Env* env = Env::Default();
  bool exclude_port_uuid = false;
  bool exclude_env_details = false;
  bool exclude_random_device = false;
};

// Each of these "tracks" below should be sufficient for generating 128 bits
// of entropy, after hashing the raw bytes. The tracks are separable for
// testing purposes, but in production we combine as many tracks as possible
// to ensure quality results even if some environments have degraded
// capabilities or quality in some APIs.
//
// This approach has not been validated for use in cryptography. The goal is
// generating globally unique values with high probability without coordination
// between instances.
//
// Linux performance: EntropyTrackRandomDevice is much faster than
// EntropyTrackEnvDetails, which is much faster than EntropyTrackPortUuid.

struct EntropyTrackPortUuid {
  std::array<char, 36> uuid;

  void Populate(const GenerateRawUniqueIdOpts& opts) {
    if (opts.exclude_port_uuid) {
      return;
    }
    std::string s;
    port::GenerateRfcUuid(&s);
    if (s.size() >= uuid.size()) {
      std::copy_n(s.begin(), uuid.size(), uuid.begin());
    }
  }
};

struct EntropyTrackEnvDetails {
  std::array<char, 64> hostname_buf;
  int64_t process_id;
  uint64_t thread_id;
  int64_t unix_time;
  uint64_t nano_time;

  void Populate(const GenerateRawUniqueIdOpts& opts) {
    if (opts.exclude_env_details) {
      return;
    }
    opts.env->GetHostName(hostname_buf.data(), hostname_buf.size())
        .PermitUncheckedError();
    process_id = port::GetProcessID();
    thread_id = opts.env->GetThreadID();
    opts.env->GetCurrentTime(&unix_time).PermitUncheckedError();
    nano_time = opts.env->NowNanos();
  }
};

struct EntropyTrackRandomDevice {
  using RandType = std::random_device::result_type;
  static constexpr size_t kNumRandVals =
      /* generous bits */ 192U / (8U * sizeof(RandType));
  std::array<RandType, kNumRandVals> rand_vals;

  void Populate(const GenerateRawUniqueIdOpts& opts) {
    if (opts.exclude_random_device) {
      return;
    }
    std::random_device r;
    for (auto& val : rand_vals) {
      val = r();
    }
  }
};

struct Entropy {
  uint64_t version_identifier;
  EntropyTrackRandomDevice et1;
  EntropyTrackEnvDetails et2;
  EntropyTrackPortUuid et3;

  void Populate(const GenerateRawUniqueIdOpts& opts) {
    // If we change the format of what goes into the entropy inputs, it's
    // conceivable there could be a physical collision in the hash input
    // even though they are logically different. This value should change
    // if there's a change to the "schema" here, including byte order.
    version_identifier = (uint64_t{ROCKSDB_MAJOR} << 32) +
                         (uint64_t{ROCKSDB_MINOR} << 16) +
                         uint64_t{ROCKSDB_PATCH};
    et1.Populate(opts);
    et2.Populate(opts);
    et3.Populate(opts);
  }
};

void GenerateRawUniqueIdImpl(uint64_t* a, uint64_t* b,
                             const GenerateRawUniqueIdOpts& opts) {
  Entropy e;
  std::memset(&e, 0, sizeof(e));
  e.Populate(opts);
  Hash2x64(reinterpret_cast<const char*>(&e), sizeof(e), a, b);
}

}  // namespace

void GenerateRawUniqueId(uint64_t* a, uint64_t* b, bool exclude_port_uuid) {
  GenerateRawUniqueIdOpts opts;
  opts.exclude_port_uuid = exclude_port_uuid;
  assert(!opts.exclude_env_details);
  assert(!opts.exclude_random_device);
  GenerateRawUniqueIdImpl(a, b, opts);
}

#ifndef NDEBUG
void TEST_GenerateRawUniqueId(uint64_t* a, uint64_t* b, bool exclude_port_uuid,
                              bool exclude_env_details,
                              bool exclude_random_device) {
  GenerateRawUniqueIdOpts opts;
  opts.exclude_port_uuid = exclude_port_uuid;
  opts.exclude_env_details = exclude_env_details;
  opts.exclude_random_device = exclude_random_device;
  GenerateRawUniqueIdImpl(a, b, opts);
}
#endif

void SemiStructuredUniqueIdGen::Reset() {
  saved_process_id_ = port::GetProcessID();
  GenerateRawUniqueId(&base_upper_, &base_lower_);
  counter_ = 0;
}

void SemiStructuredUniqueIdGen::GenerateNext(uint64_t* upper, uint64_t* lower) {
  if (port::GetProcessID() == saved_process_id_) {
    // Safe to increment the atomic for guaranteed uniqueness within this
    // process lifetime. Xor slightly better than +. See
    // https://github.com/pdillinger/unique_id
    *lower = base_lower_ ^ counter_.fetch_add(1);
    *upper = base_upper_;
  } else {
    // There must have been a fork() or something. Rather than attempting to
    // update in a thread-safe way, simply fall back on GenerateRawUniqueId.
    GenerateRawUniqueId(upper, lower);
  }
}

void UnpredictableUniqueIdGen::Reset() {
  for (size_t i = 0; i < pool_.size(); i += 2) {
    assert(i + 1 < pool_.size());
    uint64_t a;
    uint64_t b;
    GenerateRawUniqueId(&a, &b);
    pool_[i] = a;
    pool_[i + 1] = b;
  }
}

void UnpredictableUniqueIdGen::GenerateNext(uint64_t* upper, uint64_t* lower) {
  uint64_t extra_entropy;
  // Use timing information (if available) to add to entropy. (Not a disaster
  // if unavailable on some platforms. High performance is important.)
#ifdef __SSE4_2__  // More than enough to guarantee rdtsc instruction
  extra_entropy = static_cast<uint64_t>(__rdtsc());
#else
  extra_entropy = SystemClock::Default()->NowNanos();
#endif

  GenerateNextWithEntropy(upper, lower, extra_entropy);
}

void UnpredictableUniqueIdGen::GenerateNextWithEntropy(uint64_t* upper,
                                                       uint64_t* lower,
                                                       uint64_t extra_entropy) {
  // To efficiently ensure unique inputs to the hash function in the presence
  // of multithreading, we do not require atomicity on the whole entropy pool,
  // but instead only a piece of it (a 64-bit counter) that is sufficient to
  // guarantee uniqueness.
  uint64_t count = counter_.fetch_add(1, std::memory_order_relaxed);
  uint64_t a = count;
  uint64_t b = extra_entropy;
  // Invoking the hash function several times avoids copying all the inputs
  // to a contiguous, non-atomic buffer.
  BijectiveHash2x64(a, b, &a, &b);  // Based on XXH128

  // In hashing the rest of the pool with that, we don't need to worry about
  // races, but use atomic operations for sanitizer-friendliness.
  for (size_t i = 0; i < pool_.size(); i += 2) {
    assert(i + 1 < pool_.size());
    a ^= pool_[i].load(std::memory_order_relaxed);
    b ^= pool_[i + 1].load(std::memory_order_relaxed);
    BijectiveHash2x64(a, b, &a, &b);  // Based on XXH128
  }

  // Return result
  *lower = a;
  *upper = b;

  // Add some back into pool. We don't really care that there's a race in
  // storing the result back and another thread computing the next value.
  // It's just an entropy pool.
  pool_[count & (pool_.size() - 1)].fetch_add(a, std::memory_order_relaxed);
}

#ifndef NDEBUG
UnpredictableUniqueIdGen::UnpredictableUniqueIdGen(TEST_ZeroInitialized) {
  for (auto& p : pool_) {
    p.store(0);
  }
  counter_.store(0);
}
#endif

}  // namespace ROCKSDB_NAMESPACE
