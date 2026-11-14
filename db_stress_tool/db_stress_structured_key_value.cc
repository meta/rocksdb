//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS
#include "db_stress_tool/db_stress_structured_key_value.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <limits>

#include "util/cast_util.h"
#include "util/coding_lean.h"

namespace ROCKSDB_NAMESPACE {

StructuredKeyValueContext structured_kv_ctx;

namespace {

constexpr uint64_t kValueVersionSeed = 0x5bd1e995;
constexpr uint32_t kTimeMultiplier = 2654435761u;
constexpr size_t kMaxFieldSize = 40;
constexpr size_t kMinOpaqueValueSize = 8;
constexpr uint8_t kField1NullFlag = 1;
constexpr uint8_t kField2NullFlag = 2;

// The splitmix64 finalizer, which spreads key-space indexes and value bases
// evenly over the weights.
uint64_t Mix(uint64_t x) {
  x += 0x9E3779B97F4A7C15ULL;
  x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
  x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
  return x ^ (x >> 31);
}

size_t PickWeighted(const std::vector<uint64_t>& weight_sums, uint64_t hash) {
  assert(!weight_sums.empty() && weight_sums.back() > 0);
  const uint64_t point = hash % weight_sums.back();
  return static_cast<size_t>(
      std::upper_bound(weight_sums.begin(), weight_sums.end(), point) -
      weight_sums.begin());
}

uint32_t KeySpaceType(uint64_t key_space) {
  return static_cast<uint32_t>(PickWeighted(
      structured_kv_ctx.key_space_type_weight_sums, Mix(key_space)));
}

void AppendBigEndian(uint64_t value, size_t bytes, std::string* out) {
  for (size_t i = bytes; i > 0; --i) {
    out->push_back(static_cast<char>((value >> (8 * (i - 1))) & 0xffu));
  }
}

uint64_t DecodeBigEndian(const char* data, size_t bytes) {
  uint64_t value = 0;
  for (size_t i = 0; i < bytes; ++i) {
    unsigned char byte;
    std::memcpy(&byte, data + i, 1);
    value = (value << 8) | byte;
  }
  return value;
}

void PutByte(char* out, uint8_t byte) { std::memcpy(out, &byte, 1); }

// Parses "w0,w1,..." into running sums of the weights.
Status ParseWeights(const std::string& text, size_t max_entries,
                    std::vector<uint64_t>* weight_sums) {
  weight_sums->clear();
  uint64_t sum = 0;
  size_t pos = 0;
  while (pos <= text.size()) {
    const size_t end = std::min(text.find(',', pos), text.size());
    const std::string entry = text.substr(pos, end - pos);
    if (entry.empty() ||
        entry.find_first_not_of("0123456789") != std::string::npos ||
        entry.size() > 9) {
      return Status::InvalidArgument("Bad weight", entry);
    }
    sum += std::stoull(entry);
    weight_sums->push_back(sum);
    pos = end + 1;
  }
  if (weight_sums->size() > max_entries) {
    return Status::InvalidArgument("Too many weights", text);
  }
  if (sum == 0) {
    return Status::InvalidArgument("Weights must not all be zero", text);
  }
  return Status::OK();
}

// Parses "version:weight,..." into versions and running sums of the weights.
Status ParseVersionWeights(const std::string& text,
                           std::vector<uint8_t>* versions,
                           std::vector<uint64_t>* weight_sums) {
  versions->clear();
  std::string weights;
  size_t pos = 0;
  while (pos <= text.size()) {
    const size_t end = std::min(text.find(',', pos), text.size());
    const std::string entry = text.substr(pos, end - pos);
    const size_t colon = entry.find(':');
    const std::string version = entry.substr(0, colon);
    if (colon == std::string::npos || version.empty() ||
        version.find_first_not_of("0123456789") != std::string::npos ||
        version.size() > 3) {
      return Status::InvalidArgument("Bad version weight", entry);
    }
    const uint64_t parsed = std::stoull(version);
    if (parsed < 1 || parsed > std::numeric_limits<uint8_t>::max()) {
      return Status::InvalidArgument("Version must be in [1, 255]", entry);
    }
    versions->push_back(static_cast<uint8_t>(parsed));
    if (!weights.empty()) {
      weights.push_back(',');
    }
    weights.append(entry.substr(colon + 1));
    pos = end + 1;
  }
  return ParseWeights(weights, versions->size(), weight_sums);
}

size_t FieldSize(uint64_t factor) {
  return std::min(kMaxFieldSize, static_cast<size_t>(factor) *
                                     structured_kv_ctx.value_size_mult / 2);
}

size_t AppendField(uint32_t value_base, uint32_t field, size_t size, char* v) {
  PutByte(v, static_cast<uint8_t>(size));
  for (size_t i = 0; i < size; ++i) {
    PutByte(v + 1 + i, static_cast<uint8_t>(value_base + 31 * field +
                                            7 * static_cast<uint32_t>(i)));
  }
  return size + 1;
}

}  // namespace

Status InitStructuredKeyValueContext(uint64_t keys_per_key_space,
                                     uint64_t key_fanout,
                                     const std::string& key_space_type_weights,
                                     const std::string& value_version_weights,
                                     size_t value_size_mult, int64_t max_key) {
  StructuredKeyValueContext ctx;
  if (keys_per_key_space == 0 ||
      keys_per_key_space > std::numeric_limits<uint32_t>::max()) {
    return Status::InvalidArgument(
        "structured_keys_per_key_space must be in [1, 2^32)");
  }
  if (key_fanout == 0 || key_fanout > std::numeric_limits<uint32_t>::max()) {
    return Status::InvalidArgument(
        "structured_key_fanout must be in [1, 2^32)");
  }
  const uint64_t last_key_space =
      max_key > 0 ? lossless_cast<uint64_t>(max_key - 1) / keys_per_key_space
                  : 0;
  if (last_key_space >= (uint64_t{1} << 32) / kMaxStructuredKeySpaceTypes) {
    return Status::InvalidArgument(
        "max_key / structured_keys_per_key_space is too large for 4-byte "
        "key-space ids");
  }
  ctx.keys_per_key_space = keys_per_key_space;
  ctx.key_fanout = key_fanout;
  ctx.value_size_mult = value_size_mult;
  Status s = ParseWeights(key_space_type_weights, kMaxStructuredKeySpaceTypes,
                          &ctx.key_space_type_weight_sums);
  if (s.ok()) {
    s = ParseVersionWeights(value_version_weights, &ctx.value_versions,
                            &ctx.value_version_weight_sums);
  }
  if (s.ok()) {
    structured_kv_ctx = std::move(ctx);
  }
  return s;
}

std::string StructuredKey(uint64_t key_index) {
  const uint64_t key_space = key_index / structured_kv_ctx.keys_per_key_space;
  const uint64_t offset = key_index % structured_kv_ctx.keys_per_key_space;
  const uint32_t type = KeySpaceType(key_space);
  std::string key;
  key.reserve(kStructuredTwoFieldKeySize);
  AppendBigEndian(key_space * kMaxStructuredKeySpaceTypes + type,
                  kStructuredKeySpaceIdSize, &key);
  if (type == 0) {
    AppendBigEndian(offset, sizeof(uint64_t), &key);
  } else {
    AppendBigEndian(offset / structured_kv_ctx.key_fanout, sizeof(uint64_t),
                    &key);
    AppendBigEndian(offset % structured_kv_ctx.key_fanout, sizeof(uint64_t),
                    &key);
  }
  return key;
}

bool StructuredKeyToIndex(const Slice& key, uint64_t* key_index) {
  if (key.size() < kStructuredKeySpaceIdSize) {
    return false;
  }
  const uint64_t id = DecodeBigEndian(key.data(), kStructuredKeySpaceIdSize);
  const uint64_t key_space = id / kMaxStructuredKeySpaceTypes;
  const uint64_t type = id % kMaxStructuredKeySpaceTypes;
  if (type != KeySpaceType(key_space)) {
    return false;
  }
  const char* fields = key.data() + kStructuredKeySpaceIdSize;
  uint64_t offset = 0;
  if (type == 0) {
    if (key.size() != kStructuredSingleFieldKeySize) {
      return false;
    }
    offset = DecodeBigEndian(fields, sizeof(uint64_t));
  } else {
    if (key.size() != kStructuredTwoFieldKeySize) {
      return false;
    }
    const uint64_t high = DecodeBigEndian(fields, sizeof(uint64_t));
    const uint64_t low =
        DecodeBigEndian(fields + sizeof(uint64_t), sizeof(uint64_t));
    if (low >= structured_kv_ctx.key_fanout ||
        high >= structured_kv_ctx.keys_per_key_space) {
      return false;
    }
    offset = high * structured_kv_ctx.key_fanout + low;
  }
  if (offset >= structured_kv_ctx.keys_per_key_space) {
    return false;
  }
  *key_index = key_space * structured_kv_ctx.keys_per_key_space + offset;
  return true;
}

uint64_t StructuredKeyLowerBound(const Slice& key) {
  const uint64_t keys_per_key_space = structured_kv_ctx.keys_per_key_space;
  const uint64_t fanout = structured_kv_ctx.key_fanout;
  // Pads the key with zeros to the longest key, which only moves it down to
  // the first key it is a prefix of.
  char padded[kStructuredTwoFieldKeySize] = {};
  std::memcpy(padded, key.data(),
              std::min(key.size(), kStructuredTwoFieldKeySize));
  const uint64_t id = DecodeBigEndian(padded, kStructuredKeySpaceIdSize);
  const uint64_t key_space = id / kMaxStructuredKeySpaceTypes;
  const uint64_t expected_id =
      key_space * kMaxStructuredKeySpaceTypes + KeySpaceType(key_space);
  if (id < expected_id) {
    return key_space * keys_per_key_space;
  }
  if (id > expected_id) {
    return (key_space + 1) * keys_per_key_space;
  }

  const char* fields = padded + kStructuredKeySpaceIdSize;
  uint64_t offset = 0;
  size_t key_size = kStructuredSingleFieldKeySize;
  // Whether the padded key equals the key of 'offset' over the length of that
  // key, which a longer input then exceeds.
  bool exact = true;
  if (id % kMaxStructuredKeySpaceTypes == 0) {
    offset = DecodeBigEndian(fields, sizeof(uint64_t));
  } else {
    key_size = kStructuredTwoFieldKeySize;
    const uint64_t high = DecodeBigEndian(fields, sizeof(uint64_t));
    const uint64_t low =
        DecodeBigEndian(fields + sizeof(uint64_t), sizeof(uint64_t));
    if (high >= (keys_per_key_space + fanout - 1) / fanout) {
      return (key_space + 1) * keys_per_key_space;
    }
    exact = low < fanout;
    offset = high * fanout + std::min(low, fanout);
  }
  if (offset >= keys_per_key_space) {
    return (key_space + 1) * keys_per_key_space;
  }
  if (exact && key.size() > key_size) {
    ++offset;
  }
  return key_space * keys_per_key_space + offset;
}

size_t GenerateStructuredValue(uint32_t value_base, char* v, size_t max_sz) {
  const uint8_t version = structured_kv_ctx.value_versions[PickWeighted(
      structured_kv_ctx.value_version_weight_sums,
      Mix(value_base ^ kValueVersionSeed))];
  std::memcpy(v, &value_base, sizeof(value_base));
  PutByte(v + kStructuredValueVersionOffset, version);
  size_t size = kStructuredValueVersionOffset + 1;
  const uint32_t layout = version % 4;
  if (layout == 0) {
    const size_t opaque_size =
        std::max(kMinOpaqueValueSize, static_cast<size_t>(value_base % 3 + 1) *
                                          structured_kv_ctx.value_size_mult);
    for (; size < opaque_size; ++size) {
      PutByte(v + size,
              static_cast<uint8_t>(value_base ^ static_cast<uint32_t>(size)));
    }
  } else {
    EncodeFixed32(v + size, value_base * kTimeMultiplier);
    size += sizeof(uint32_t);
    const bool has_field1 = layout != 3;
    const bool has_field2 = layout != 1;
    const bool field1_null = has_field1 && (value_base >> 8) % 8 == 0;
    const bool field2_null = has_field2 && (value_base >> 12) % 8 == 0;
    PutByte(v + size,
            static_cast<uint8_t>((field1_null ? kField1NullFlag : 0) |
                                 (field2_null ? kField2NullFlag : 0)));
    ++size;
    if (has_field1 && !field1_null) {
      size +=
          AppendField(value_base, 1, FieldSize(value_base % 3 + 1), v + size);
    }
    if (has_field2 && !field2_null) {
      size += AppendField(value_base, 2, FieldSize((value_base >> 4) % 3),
                          v + size);
    }
  }
  assert(size < max_sz);
  (void)max_sz;
  v[size] = '\0';
  return size;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
