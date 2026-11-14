//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// Structured key-value mode (--structured_key_value) replaces the default key
// and value generators with schema-like keys and versioned row values, so that
// table formats that decode rows into columns (for example, a table factory
// selected with --table_factory_uri) see a realistic mix of row shapes. Keys
// still map one-to-one and in order to the expected-state key indexes, and
// values are still regenerated from their value base for verification.
//
// Keys. The key index space is split into contiguous key spaces of
// --structured_keys_per_key_space keys. Key space r has a type t in
// [0, kMaxStructuredKeySpaceTypes), drawn deterministically from r with the
// weights of --structured_key_space_type_weights, and the 4-byte key-space id
// r * kMaxStructuredKeySpaceTypes + t, so ids increase with r and every key
// names the type of its key space. With offset o of the key in its key space,
// all integers big-endian:
//   type 0:  [key-space id: 4][o: 8]                                12 bytes
//   type 1+: [key-space id: 4][o / fanout: 8][o % fanout: 8]        20 bytes
// where fanout is --structured_key_fanout.
//
// Values. Byte 0-3 hold the value base as GenerateValue() writes it, and byte
// 4 holds the row version v, drawn deterministically from the value base with
// the weights of --structured_value_version_weights. The layout of the rest
// depends on v % 4:
//   0:        opaque bytes (an unparseable layout)
//   1, 2, 3:  [time: 4, little-endian][null flags: 1][field 1][field 2]
// Field 1 is present when v % 4 is 1 or 2, field 2 when v % 4 is 2 or 3. Bit
// 0 of the null flags marks a present field 1 as null and bit 1 a present
// field 2. A present, non-null field is [length: 1][bytes], and may be empty.
// Absent and null fields have no bytes.

inline constexpr uint32_t kMaxStructuredKeySpaceTypes = 8;
inline constexpr size_t kStructuredKeySpaceIdSize = 4;
inline constexpr size_t kStructuredSingleFieldKeySize = 12;
inline constexpr size_t kStructuredTwoFieldKeySize = 20;
inline constexpr size_t kStructuredValueVersionOffset = 4;

struct StructuredKeyValueContext {
  uint64_t keys_per_key_space = 0;
  uint64_t key_fanout = 0;
  // Running sums of the key-space type weights, one per type.
  std::vector<uint64_t> key_space_type_weight_sums;
  std::vector<uint8_t> value_versions;
  // Running sums of the value version weights, one per version.
  std::vector<uint64_t> value_version_weight_sums;
  size_t value_size_mult = 0;
};

extern StructuredKeyValueContext structured_kv_ctx;

// Initializes structured_kv_ctx from the arguments. 'max_key' bounds the key
// indexes, so that every key-space id fits in 4 bytes.
Status InitStructuredKeyValueContext(uint64_t keys_per_key_space,
                                     uint64_t key_fanout,
                                     const std::string& key_space_type_weights,
                                     const std::string& value_version_weights,
                                     size_t value_size_mult, int64_t max_key);

// Returns the key of 'key_index'.
std::string StructuredKey(uint64_t key_index);

// Returns in '*key_index' the index of 'key', or false when 'key' is not a key
// that StructuredKey() returns.
bool StructuredKeyToIndex(const Slice& key, uint64_t* key_index);

// Returns the smallest key index whose key is at least 'key', for any byte
// string 'key', such as a prefix or the successor of a prefix. The result may
// exceed every key index in use.
uint64_t StructuredKeyLowerBound(const Slice& key);

// Writes the value of 'value_base' to 'v', which holds 'max_sz' bytes, followed
// by a NUL, and returns its size.
size_t GenerateStructuredValue(uint32_t value_base, char* v, size_t max_sz);

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
