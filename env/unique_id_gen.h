//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// This file is for functions that generate unique identifiers by
// (at least in part) by extracting novel entropy or sources of uniqueness
// from the execution environment. (By contrast, random.h is for algorithmic
// pseudorandomness.)
//
// These functions could eventually migrate to public APIs, such as in Env.

#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <type_traits>

#include "port/port.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// Generates a new 128-bit identifier that is universally unique
// (with high probability) for each call. The result is split into
// two 64-bit pieces. This function has NOT been validated for use in
// cryptography.
//
// This is used in generating DB session IDs and by Env::GenerateUniqueId
// (used for DB IDENTITY) if the platform does not provide a generator of
// RFC 4122 UUIDs or fails somehow. (Set exclude_port_uuid=true if this
// function is used as a fallback for GenerateRfcUuid, because no need
// trying it again.)
void GenerateRawUniqueId(uint64_t* a, uint64_t* b,
                         bool exclude_port_uuid = false);

#ifndef NDEBUG
// A version of above with options for challenge testing
void TEST_GenerateRawUniqueId(uint64_t* a, uint64_t* b, bool exclude_port_uuid,
                              bool exclude_env_details,
                              bool exclude_random_device);
#endif

// Generates globally unique ids with lower probability of any collisions
// vs. each unique id being independently random (GenerateRawUniqueId).
// We call this "semi-structured" because between different
// SemiStructuredUniqueIdGen objects, the IDs are separated by random
// intervals (unstructured), but within a single SemiStructuredUniqueIdGen
// object, the generated IDs are trivially related (structured). See
// https://github.com/pdillinger/unique_id for how this improves probability
// of no collision. In short, if we have n SemiStructuredUniqueIdGen
// objects each generating m IDs, the first collision is expected at
// around n = sqrt(2^128 / m), equivalently n * sqrt(m) = 2^64,
// rather than n * m = 2^64 for fully random IDs.
class SemiStructuredUniqueIdGen {
 public:
  // Initializes with random starting state (from GenerateRawUniqueId)
  SemiStructuredUniqueIdGen() { Reset(); }
  // Re-initializes, but not thread safe
  void Reset();

  // Assuming no fork(), `lower` is guaranteed unique from one call
  // to the next (thread safe).
  void GenerateNext(uint64_t* upper, uint64_t* lower);

  // For generating smaller values. Will cycle through all the possibilities
  // before repeating.
  template <typename T>
  T GenerateNext() {
    static_assert(sizeof(T) <= sizeof(uint64_t));
    static_assert(std::is_integral_v<T>);
    uint64_t ignore;
    uint64_t val;
    GenerateNext(&ignore, &val);
    return static_cast<T>(val);
  }

  uint64_t GetBaseUpper() const { return base_upper_; }

 private:
  uint64_t base_upper_ = 0;
  uint64_t base_lower_ = 0;
  std::atomic<uint64_t> counter_;
  int64_t saved_process_id_ = 0;
};

// A unique id generator that should provide reasonable security against
// predicting the output from previous outputs, but is NOT known to be
// cryptographically secure. Unlike std::random_device, this is guaranteed
// not to block once initialized.
class ALIGN_AS(CACHE_LINE_SIZE) UnpredictableUniqueIdGen {
 public:
  // Initializes with random starting state (from several GenerateRawUniqueId)
  UnpredictableUniqueIdGen() { Reset(); }
  // Re-initializes, but not thread safe
  void Reset();

  // Generate next probabilistically unique value. Thread safe. Uses timing
  // information to add to the entropy pool.
  void GenerateNext(uint64_t* upper, uint64_t* lower);

  // Explicitly include given value for entropy pool instead of timing
  // information.
  void GenerateNextWithEntropy(uint64_t* upper, uint64_t* lower,
                               uint64_t extra_entropy);

#ifndef NDEBUG
  struct TEST_ZeroInitialized {};
  explicit UnpredictableUniqueIdGen(TEST_ZeroInitialized);
  std::atomic<uint64_t>& TEST_counter() { return counter_; }
#endif
 private:
  // 256 bit entropy pool
  std::array<std::atomic<uint64_t>, 4> pool_;
  // Counter to ensure unique hash inputs
  std::atomic<uint64_t> counter_;
};

}  // namespace ROCKSDB_NAMESPACE
