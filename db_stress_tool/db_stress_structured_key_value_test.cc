//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS
#include "db_stress_tool/db_stress_structured_key_value.h"

#include <algorithm>
#include <cstring>
#include <ctime>
#include <set>
#include <string>
#include <vector>

#include "port/stack_trace.h"
#include "test_util/testharness.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

namespace {

struct KeyConfig {
  uint64_t keys_per_key_space;
  uint64_t key_fanout;
  const char* key_space_type_weights;
};

constexpr KeyConfig kKeyConfigs[] = {
    {1, 1, "1"},          {1, 16, "0,1"},    {5, 3, "2,3,2"},
    {16, 1, "1,1,1,1"},   {16, 16, "2,3,2"}, {7, 2, "1,0,0,0,0,0,0,1"},
    {256, 16, "0,0,1,1"},
};

constexpr int64_t kKeySpacesPerConfig = 24;
constexpr char kVersionWeights[] = "1:1";

void InitKeys(const KeyConfig& config, int64_t max_key) {
  ASSERT_OK(InitStructuredKeyValueContext(
      config.keys_per_key_space, config.key_fanout,
      config.key_space_type_weights, kVersionWeights,
      /*value_size_mult=*/32, max_key));
}

std::vector<std::string> AllKeys(int64_t max_key) {
  std::vector<std::string> keys;
  for (int64_t i = 0; i < max_key; ++i) {
    keys.push_back(StructuredKey(static_cast<uint64_t>(i)));
  }
  return keys;
}

// Returns the smallest byte string greater than every string that starts with
// 'prefix', or an empty string when there is none.
std::string PrefixSuccessor(std::string prefix) {
  while (!prefix.empty() && static_cast<unsigned char>(prefix.back()) == 0xff) {
    prefix.pop_back();
  }
  if (!prefix.empty()) {
    prefix.back() = static_cast<char>(prefix.back() + 1);
  }
  return prefix;
}

void ExpectLowerBound(const std::vector<std::string>& keys,
                      const std::string& probe) {
  const auto expected = static_cast<uint64_t>(
      std::lower_bound(keys.begin(), keys.end(), probe) - keys.begin());
  const uint64_t actual = StructuredKeyLowerBound(probe);
  SCOPED_TRACE("probe=" + Slice(probe).ToString(/*hex=*/true));
  if (expected < keys.size()) {
    EXPECT_EQ(actual, expected);
  } else {
    EXPECT_GE(actual, keys.size());
  }
}

}  // namespace

TEST(StructuredKeyValueTest, KeysRoundTripInOrder) {
  for (const KeyConfig& config : kKeyConfigs) {
    SCOPED_TRACE(std::string("weights=") + config.key_space_type_weights);
    const int64_t max_key =
        kKeySpacesPerConfig * static_cast<int64_t>(config.keys_per_key_space);
    InitKeys(config, max_key);
    const std::vector<std::string> keys = AllKeys(max_key);
    for (size_t i = 0; i < keys.size(); ++i) {
      ASSERT_TRUE(keys[i].size() == kStructuredSingleFieldKeySize ||
                  keys[i].size() == kStructuredTwoFieldKeySize);
      uint64_t index = 0;
      ASSERT_TRUE(StructuredKeyToIndex(keys[i], &index));
      ASSERT_EQ(index, i);
      if (i > 0) {
        ASSERT_LT(keys[i - 1], keys[i]);
      }
    }
  }
}

TEST(StructuredKeyValueTest, RejectsKeysItDoesNotGenerate) {
  InitKeys(kKeyConfigs[4], kKeySpacesPerConfig * 16);
  const std::string key = StructuredKey(17);
  uint64_t index = 0;
  for (size_t size = 0; size < key.size(); ++size) {
    EXPECT_FALSE(StructuredKeyToIndex(key.substr(0, size), &index));
  }
  EXPECT_FALSE(StructuredKeyToIndex(key + '\0', &index));
  std::string wrong_type = key;
  wrong_type[3] = static_cast<char>(wrong_type[3] ^ 1);
  EXPECT_FALSE(StructuredKeyToIndex(wrong_type, &index));
}

TEST(StructuredKeyValueTest, LowerBoundOfPrefixes) {
  for (const KeyConfig& config : kKeyConfigs) {
    SCOPED_TRACE(std::string("weights=") + config.key_space_type_weights);
    const int64_t max_key =
        kKeySpacesPerConfig * static_cast<int64_t>(config.keys_per_key_space);
    InitKeys(config, max_key);
    const std::vector<std::string> keys = AllKeys(max_key);
    for (const std::string& key : keys) {
      for (size_t size = 0; size <= key.size(); ++size) {
        const std::string prefix = key.substr(0, size);
        ExpectLowerBound(keys, prefix);
        ExpectLowerBound(keys, PrefixSuccessor(prefix));
      }
      ExpectLowerBound(keys, key + '\0');
    }
  }
}

TEST(StructuredKeyValueTest, LowerBoundOfRandomBytes) {
  const uint32_t seed = static_cast<uint32_t>(time(nullptr));
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);
  for (const KeyConfig& config : kKeyConfigs) {
    SCOPED_TRACE(std::string("weights=") + config.key_space_type_weights);
    const int64_t max_key =
        kKeySpacesPerConfig * static_cast<int64_t>(config.keys_per_key_space);
    InitKeys(config, max_key);
    const std::vector<std::string> keys = AllKeys(max_key);
    for (int i = 0; i < 2000; ++i) {
      // Start most probes from a real key so that they land among the keys.
      std::string probe = keys[rnd.Uniform(static_cast<int>(keys.size()))];
      probe.resize(rnd.Uniform(static_cast<int>(probe.size()) + 4));
      for (size_t j = rnd.Uniform(static_cast<int>(probe.size()) + 1);
           j < probe.size(); ++j) {
        probe[j] = static_cast<char>(rnd.Uniform(256));
      }
      ExpectLowerBound(keys, probe);
    }
  }
}

TEST(StructuredKeyValueTest, ValuesFollowTheirVersion) {
  ASSERT_OK(InitStructuredKeyValueContext(
      16, 16, "1", "1:1,2:1,3:1,4:1,6:1,255:1", /*value_size_mult=*/32,
      /*max_key=*/16));
  const std::set<uint8_t> versions{1, 2, 3, 4, 6, 255};
  std::set<uint8_t> seen;
  for (uint32_t value_base = 0; value_base < 4096; ++value_base) {
    char value[100];
    const size_t size =
        GenerateStructuredValue(value_base, value, sizeof(value));
    ASSERT_LT(size, sizeof(value));
    char again[100];
    ASSERT_EQ(GenerateStructuredValue(value_base, again, sizeof(again)), size);
    ASSERT_EQ(Slice(value, size), Slice(again, size));
    uint32_t stored_base = 0;
    memcpy(&stored_base, value, sizeof(stored_base));
    ASSERT_EQ(stored_base, value_base);
    const auto version =
        static_cast<uint8_t>(value[kStructuredValueVersionOffset]);
    ASSERT_EQ(versions.count(version), 1);
    seen.insert(version);
  }
  EXPECT_EQ(seen, versions);
}

TEST(StructuredKeyValueTest, RejectsBadConfigurations) {
  struct BadConfig {
    uint64_t keys_per_key_space;
    uint64_t key_fanout;
    const char* key_space_type_weights;
    const char* value_version_weights;
    int64_t max_key;
  };
  constexpr BadConfig kBadConfigs[] = {
      {0, 16, "1", "1:1", 100},
      {16, 0, "1", "1:1", 100},
      {1, 16, "1", "1:1", int64_t{1} << 30},
      {16, 16, "", "1:1", 100},
      {16, 16, "1,,1", "1:1", 100},
      {16, 16, "1,x", "1:1", 100},
      {16, 16, "0,0", "1:1", 100},
      {16, 16, "1,1,1,1,1,1,1,1,1", "1:1", 100},
      {16, 16, "1", "", 100},
      {16, 16, "1", "1", 100},
      {16, 16, "1", "0:1", 100},
      {16, 16, "1", "256:1", 100},
      {16, 16, "1", "1:0", 100},
  };
  for (const BadConfig& config : kBadConfigs) {
    SCOPED_TRACE(std::string(config.key_space_type_weights) + " / " +
                 config.value_version_weights);
    EXPECT_EQ(InitStructuredKeyValueContext(
                  config.keys_per_key_space, config.key_fanout,
                  config.key_space_type_weights, config.value_version_weights,
                  /*value_size_mult=*/32, config.max_key)
                  .code(),
              Status::Code::kInvalidArgument);
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <cstdio>

int main() {
  fprintf(stderr, "SKIPPED as db_stress requires GFLAGS\n");
  return 0;
}
#endif  // GFLAGS
