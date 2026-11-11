//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/coding.h"

#include <initializer_list>

#include "test_util/testharness.h"
#include "util/prefix_varint.h"

namespace ROCKSDB_NAMESPACE {

class Coding {};

TEST(Coding, Fixed16) {
  std::string s;
  for (uint16_t v = 0; v < 0xFFFF; v++) {
    PutFixed16(&s, v);
  }

  const char* p = s.data();
  for (uint16_t v = 0; v < 0xFFFF; v++) {
    uint16_t actual = DecodeFixed16(p);
    ASSERT_EQ(v, actual);
    p += sizeof(uint16_t);
  }
}

TEST(Coding, Fixed32) {
  std::string s;
  for (uint32_t v = 0; v < 100000; v++) {
    PutFixed32(&s, v);
  }

  const char* p = s.data();
  for (uint32_t v = 0; v < 100000; v++) {
    uint32_t actual = DecodeFixed32(p);
    ASSERT_EQ(v, actual);
    p += sizeof(uint32_t);
  }
}

TEST(Coding, Fixed64) {
  std::string s;
  for (int power = 0; power <= 63; power++) {
    uint64_t v = static_cast<uint64_t>(1) << power;
    PutFixed64(&s, v - 1);
    PutFixed64(&s, v + 0);
    PutFixed64(&s, v + 1);
  }

  const char* p = s.data();
  for (int power = 0; power <= 63; power++) {
    uint64_t v = static_cast<uint64_t>(1) << power;
    uint64_t actual = 0;
    actual = DecodeFixed64(p);
    ASSERT_EQ(v - 1, actual);
    p += sizeof(uint64_t);

    actual = DecodeFixed64(p);
    ASSERT_EQ(v + 0, actual);
    p += sizeof(uint64_t);

    actual = DecodeFixed64(p);
    ASSERT_EQ(v + 1, actual);
    p += sizeof(uint64_t);
  }
}

// Test that encoding routines generate little-endian encodings
TEST(Coding, EncodingOutput) {
  std::string dst;
  PutFixed32(&dst, 0x04030201);
  ASSERT_EQ(4U, dst.size());
  ASSERT_EQ(0x01, static_cast<int>(dst[0]));
  ASSERT_EQ(0x02, static_cast<int>(dst[1]));
  ASSERT_EQ(0x03, static_cast<int>(dst[2]));
  ASSERT_EQ(0x04, static_cast<int>(dst[3]));

  dst.clear();
  PutFixed64(&dst, 0x0807060504030201ull);
  ASSERT_EQ(8U, dst.size());
  ASSERT_EQ(0x01, static_cast<int>(dst[0]));
  ASSERT_EQ(0x02, static_cast<int>(dst[1]));
  ASSERT_EQ(0x03, static_cast<int>(dst[2]));
  ASSERT_EQ(0x04, static_cast<int>(dst[3]));
  ASSERT_EQ(0x05, static_cast<int>(dst[4]));
  ASSERT_EQ(0x06, static_cast<int>(dst[5]));
  ASSERT_EQ(0x07, static_cast<int>(dst[6]));
  ASSERT_EQ(0x08, static_cast<int>(dst[7]));
}

TEST(Coding, Varint32) {
  std::string s;
  for (uint32_t i = 0; i < (32 * 32); i++) {
    uint32_t v = (i / 32) << (i % 32);
    PutVarint32(&s, v);
  }

  const char* p = s.data();
  const char* limit = p + s.size();
  for (uint32_t i = 0; i < (32 * 32); i++) {
    uint32_t expected = (i / 32) << (i % 32);
    uint32_t actual = 0;
    const char* start = p;
    p = GetVarint32Ptr(p, limit, &actual);
    ASSERT_TRUE(p != nullptr);
    ASSERT_EQ(expected, actual);
    ASSERT_EQ(VarintLength(actual), p - start);
  }
  ASSERT_EQ(p, s.data() + s.size());
}

TEST(Coding, Varint64) {
  // Construct the list of values to check
  std::vector<uint64_t> values;
  // Some special values
  values.push_back(0);
  values.push_back(100);
  values.push_back(~uint64_t{0});
  values.push_back(~uint64_t{0} - 1);
  for (uint32_t k = 0; k < 64; k++) {
    // Test values near powers of two
    const uint64_t power = 1ull << k;
    values.push_back(power);
    values.push_back(power - 1);
    values.push_back(power + 1);
  }

  std::string s;
  for (unsigned int i = 0; i < values.size(); i++) {
    PutVarint64(&s, values[i]);
  }

  const char* p = s.data();
  const char* limit = p + s.size();
  for (unsigned int i = 0; i < values.size(); i++) {
    ASSERT_TRUE(p < limit);
    uint64_t actual = 0;
    const char* start = p;
    p = GetVarint64Ptr(p, limit, &actual);
    ASSERT_TRUE(p != nullptr);
    ASSERT_EQ(values[i], actual);
    ASSERT_EQ(VarintLength(actual), p - start);
  }
  ASSERT_EQ(p, limit);
}

TEST(Coding, Varint32Overflow) {
  uint32_t result;
  std::string input("\x81\x82\x83\x84\x85\x11");
  ASSERT_TRUE(GetVarint32Ptr(input.data(), input.data() + input.size(),
                             &result) == nullptr);
}

TEST(Coding, Varint32Truncation) {
  uint32_t large_value = (1u << 31) + 100;
  std::string s;
  PutVarint32(&s, large_value);
  uint32_t result;
  for (unsigned int len = 0; len + 1 < s.size(); len++) {
    ASSERT_TRUE(GetVarint32Ptr(s.data(), s.data() + len, &result) == nullptr);
  }
  ASSERT_TRUE(GetVarint32Ptr(s.data(), s.data() + s.size(), &result) !=
              nullptr);
  ASSERT_EQ(large_value, result);
}

TEST(Coding, Varint64Overflow) {
  uint64_t result;
  std::string input("\x81\x82\x83\x84\x85\x81\x82\x83\x84\x85\x11");
  ASSERT_TRUE(GetVarint64Ptr(input.data(), input.data() + input.size(),
                             &result) == nullptr);
}

TEST(Coding, Varint64Truncation) {
  uint64_t large_value = (1ull << 63) + 100ull;
  std::string s;
  PutVarint64(&s, large_value);
  uint64_t result;
  for (unsigned int len = 0; len + 1 < s.size(); len++) {
    ASSERT_TRUE(GetVarint64Ptr(s.data(), s.data() + len, &result) == nullptr);
  }
  ASSERT_TRUE(GetVarint64Ptr(s.data(), s.data() + s.size(), &result) !=
              nullptr);
  ASSERT_EQ(large_value, result);
}

TEST(Coding, Strings) {
  std::string s;
  PutLengthPrefixedSlice(&s, Slice(""));
  PutLengthPrefixedSlice(&s, Slice("foo"));
  PutLengthPrefixedSlice(&s, Slice("bar"));
  PutLengthPrefixedSlice(&s, Slice(std::string(200, 'x')));

  Slice input(s);
  Slice v;
  ASSERT_TRUE(GetLengthPrefixedSlice(&input, &v));
  ASSERT_EQ("", v.ToString());
  ASSERT_TRUE(GetLengthPrefixedSlice(&input, &v));
  ASSERT_EQ("foo", v.ToString());
  ASSERT_TRUE(GetLengthPrefixedSlice(&input, &v));
  ASSERT_EQ("bar", v.ToString());
  ASSERT_TRUE(GetLengthPrefixedSlice(&input, &v));
  ASSERT_EQ(std::string(200, 'x'), v.ToString());
  ASSERT_EQ("", input.ToString());
}

// Keep PrefixVarint-specific helpers and vectors with the PrefixVarint tests
// below so the legacy coding tests stay grouped above.
namespace {

template <typename T>
struct PrefixVarintTestCase {
  T value;
  uint32_t length;
  std::string encoded;
};

template <typename T>
struct PrefixVarintTraits;

template <>
struct PrefixVarintTraits<uint32_t> {
  static constexpr size_t kMaxLength = kMaxPrefixVarint32Length;

  static uint32_t Length(uint32_t value) { return PrefixVarint32Length(value); }
  static char* Encode(char* dst, uint32_t value) {
    return EncodePrefixVarint32(dst, value);
  }
  static void Put(std::string* dst, uint32_t value) {
    PutPrefixVarint32(dst, value);
  }
  static const char* GetPtr(const char* p, const char* limit, uint32_t* value) {
    return GetPrefixVarint32Ptr(p, limit, value);
  }
  static bool Get(Slice* input, uint32_t* value) {
    return GetPrefixVarint32(input, value);
  }
  static uint32_t AddlByteCount(char first_byte) {
    return PrefixVarint32AddlByteCount(first_byte);
  }
  static bool Decode(char first_byte, const char* addl_bytes,
                     size_t addl_byte_count, uint32_t* value) {
    return DecodePrefixVarint32(first_byte, addl_bytes, addl_byte_count, value);
  }
};

template <>
struct PrefixVarintTraits<uint64_t> {
  static constexpr size_t kMaxLength = kMaxPrefixVarint64Length;

  static uint32_t Length(uint64_t value) { return PrefixVarint64Length(value); }
  static char* Encode(char* dst, uint64_t value) {
    return EncodePrefixVarint64(dst, value);
  }
  static void Put(std::string* dst, uint64_t value) {
    PutPrefixVarint64(dst, value);
  }
  static const char* GetPtr(const char* p, const char* limit, uint64_t* value) {
    return GetPrefixVarint64Ptr(p, limit, value);
  }
  static bool Get(Slice* input, uint64_t* value) {
    return GetPrefixVarint64(input, value);
  }
  static uint32_t AddlByteCount(char first_byte) {
    return PrefixVarint64AddlByteCount(first_byte);
  }
  static bool Decode(char first_byte, const char* addl_bytes,
                     size_t addl_byte_count, uint64_t* value) {
    return DecodePrefixVarint64(first_byte, addl_bytes, addl_byte_count, value);
  }
};

std::string ByteString(std::initializer_list<unsigned char> bytes) {
  std::string out;
  out.reserve(bytes.size());
  for (unsigned char b : bytes) {
    out.push_back(static_cast<char>(b));
  }
  return out;
}

template <typename T>
std::string EncodePrefixVarintToString(T value) {
  char buf[PrefixVarintTraits<T>::kMaxLength];
  char* end = PrefixVarintTraits<T>::Encode(buf, value);
  return std::string(buf, static_cast<size_t>(end - buf));
}

std::string EncodeInvalidPrefixVarint32Overflow() {
  char buf[kMaxPrefixVarint32Length];
  uint64_t encoded = (uint64_t{1} << 37) | 0x10u;
  for (size_t i = 0; i < sizeof(buf); ++i) {
    buf[i] = static_cast<char>(encoded >> (i * 8));
  }
  return std::string(buf, sizeof(buf));
}

template <typename T, size_t N>
void AssertPrefixVarintRoundTrip(const PrefixVarintTestCase<T> (&cases)[N]) {
  std::string encoded_values;
  for (const auto& tc : cases) {
    ASSERT_EQ(tc.length, PrefixVarintTraits<T>::Length(tc.value));
    ASSERT_EQ(tc.encoded, EncodePrefixVarintToString(tc.value));
    PrefixVarintTraits<T>::Put(&encoded_values, tc.value);
  }

  const char* p = encoded_values.data();
  const char* limit = p + encoded_values.size();
  for (const auto& tc : cases) {
    T actual = 0;
    const char* start = p;
    p = PrefixVarintTraits<T>::GetPtr(p, limit, &actual);
    ASSERT_TRUE(p != nullptr);
    ASSERT_EQ(tc.value, actual);
    ASSERT_EQ(tc.length, p - start);
  }
  ASSERT_EQ(p, limit);

  Slice input(encoded_values);
  for (const auto& tc : cases) {
    T actual = 0;
    ASSERT_TRUE(PrefixVarintTraits<T>::Get(&input, &actual));
    ASSERT_EQ(tc.value, actual);
  }
  ASSERT_TRUE(input.empty());
}

template <typename T, size_t N>
void AssertPrefixVarintDiskReadApi(const PrefixVarintTestCase<T> (&cases)[N]) {
  for (const auto& tc : cases) {
    const std::string encoded = EncodePrefixVarintToString(tc.value);
    ASSERT_EQ(tc.length, encoded.size());

    const uint32_t addl_byte_count =
        PrefixVarintTraits<T>::AddlByteCount(encoded[0]);
    ASSERT_EQ(tc.length - 1, addl_byte_count);

    T actual = 0;
    ASSERT_TRUE(PrefixVarintTraits<T>::Decode(encoded[0], encoded.data() + 1,
                                              addl_byte_count, &actual));
    ASSERT_EQ(tc.value, actual);
    if (addl_byte_count > 0) {
      ASSERT_FALSE(PrefixVarintTraits<T>::Decode(encoded[0], encoded.data() + 1,
                                                 addl_byte_count - 1, &actual));
    }
  }
}

template <typename T>
void AssertPrefixVarintTruncation(T value) {
  const std::string encoded = EncodePrefixVarintToString(value);
  T actual = 0;
  for (size_t len = 0; len + 1 < encoded.size(); ++len) {
    ASSERT_TRUE(PrefixVarintTraits<T>::GetPtr(
                    encoded.data(), encoded.data() + len, &actual) == nullptr);
  }
  ASSERT_TRUE(PrefixVarintTraits<T>::GetPtr(encoded.data(),
                                            encoded.data() + encoded.size(),
                                            &actual) != nullptr);
  ASSERT_EQ(value, actual);
}

const auto& PrefixVarint32TestCases() {
  static const PrefixVarintTestCase<uint32_t> kCases[] = {
      {0, 1, ByteString({0x01})},
      {1, 1, ByteString({0x03})},
      {127, 1, ByteString({0xFF})},
      {128, 2, ByteString({0x02, 0x02})},
      {255, 2, ByteString({0xFE, 0x03})},
      {16383, 2, ByteString({0xFE, 0xFF})},
      {16384, 3, ByteString({0x04, 0x00, 0x02})},
      {(1u << 21) - 1, 3, ByteString({0xFC, 0xFF, 0xFF})},
      {(1u << 21), 4, ByteString({0x08, 0x00, 0x00, 0x02})},
      {(1u << 28) - 1, 4, ByteString({0xF8, 0xFF, 0xFF, 0xFF})},
      {(1u << 28), 5, ByteString({0x10, 0x00, 0x00, 0x00, 0x02})},
      {~uint32_t{0}, 5, ByteString({0xF0, 0xFF, 0xFF, 0xFF, 0x1F})},
  };
  return kCases;
}

const auto& PrefixVarint64TestCases() {
  static const PrefixVarintTestCase<uint64_t> kCases[] = {
      {0, 1, ByteString({0x01})},
      {1, 1, ByteString({0x03})},
      {127, 1, ByteString({0xFF})},
      {128, 2, ByteString({0x02, 0x02})},
      {16383, 2, ByteString({0xFE, 0xFF})},
      {(uint64_t{1} << 21), 4, ByteString({0x08, 0x00, 0x00, 0x02})},
      {(uint64_t{1} << 28), 5, ByteString({0x10, 0x00, 0x00, 0x00, 0x02})},
      {(uint64_t{1} << 35), 6,
       ByteString({0x20, 0x00, 0x00, 0x00, 0x00, 0x02})},
      {(uint64_t{1} << 42), 7,
       ByteString({0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02})},
      {(uint64_t{1} << 49), 8,
       ByteString({0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02})},
      {(uint64_t{1} << 56) - 1, 8,
       ByteString({0x80, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})},
      {(uint64_t{1} << 56), 9,
       ByteString({0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01})},
      {~uint64_t{0}, 9,
       ByteString({0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})},
  };
  return kCases;
}

template <uint32_t kMinimumBytes>
void TestPrefixVarint64MinimumBytes(const std::vector<uint64_t>& values) {
  for (uint64_t value : values) {
    SCOPED_TRACE("value=" + std::to_string(value) +
                 " kMinimumBytes=" + std::to_string(kMinimumBytes));
    char buf[kMaxPrefixVarint64Length];
    char* end = EncodePrefixVarint64<kMinimumBytes>(buf, value);
    uint32_t encoded_len = static_cast<uint32_t>(end - buf);
    uint32_t natural_len = PrefixVarint64Length(value);
    ASSERT_EQ(encoded_len, std::max(natural_len, kMinimumBytes));

    // Verify via split-decode API
    uint32_t addl = PrefixVarint64AddlByteCount(buf[0]);
    ASSERT_EQ(addl + 1, encoded_len);
    uint64_t decoded = ~uint64_t{0};
    ASSERT_TRUE(DecodePrefixVarint64(buf[0], buf + 1, addl, &decoded));
    ASSERT_EQ(value, decoded);

    // Verify via GetPtr API
    decoded = ~uint64_t{0};
    const char* next = GetPrefixVarint64Ptr(buf, end, &decoded);
    ASSERT_EQ(next, end);
    ASSERT_EQ(value, decoded);

    // Verify via Slice API
    decoded = ~uint64_t{0};
    Slice slice(buf, encoded_len);
    ASSERT_TRUE(GetPrefixVarint64(&slice, &decoded));
    ASSERT_EQ(value, decoded);
    ASSERT_TRUE(slice.empty());
  }
}

}  // namespace

// Keep PrefixVarint tests after the legacy coding tests so the two varint
// families stay visually separated.
TEST(Coding, PrefixVarint32) {
  for (const auto& tc : PrefixVarint32TestCases()) {
    ASSERT_EQ(tc.length, VarintLength(tc.value));
  }
  AssertPrefixVarintRoundTrip(PrefixVarint32TestCases());
}

TEST(Coding, PrefixVarint32DiskReadApi) {
  AssertPrefixVarintDiskReadApi(PrefixVarint32TestCases());
  ASSERT_EQ(kInvalidPrefixVarint32AddlByteCount,
            PrefixVarint32AddlByteCount('\0'));
  ASSERT_EQ(kInvalidPrefixVarint32AddlByteCount,
            PrefixVarint32AddlByteCount('\x20'));
  ASSERT_EQ(kInvalidPrefixVarint32AddlByteCount,
            PrefixVarint32AddlByteCount('\x40'));
  ASSERT_EQ(kInvalidPrefixVarint32AddlByteCount,
            PrefixVarint32AddlByteCount('\x80'));
}

TEST(Coding, PrefixVarint64) {
  for (const auto& tc : PrefixVarint64TestCases()) {
    const uint32_t expected_length =
        tc.value < (uint64_t{1} << 56) ? VarintLength(tc.value) : 9;
    ASSERT_EQ(tc.length, expected_length);
  }
  AssertPrefixVarintRoundTrip(PrefixVarint64TestCases());
}

TEST(Coding, PrefixVarint64DiskReadApi) {
  AssertPrefixVarintDiskReadApi(PrefixVarint64TestCases());
  ASSERT_EQ(8u, PrefixVarint64AddlByteCount('\0'));
  ASSERT_EQ(7u, PrefixVarint64AddlByteCount('\x80'));
}

TEST(Coding, PrefixVarint32Overflow) {
  uint32_t result = 0;
  const std::string input = EncodeInvalidPrefixVarint32Overflow();
  ASSERT_FALSE(DecodePrefixVarint32(input[0], input.data() + 1,
                                    input.size() - 1, &result));
  ASSERT_TRUE(GetPrefixVarint32Ptr(input.data(), input.data() + input.size(),
                                   &result) == nullptr);
}

TEST(Coding, PrefixVarint32Truncation) {
  AssertPrefixVarintTruncation(~uint32_t{0});
}

TEST(Coding, PrefixVarint64Truncation) {
  AssertPrefixVarintTruncation(~uint64_t{0});
}

TEST(Coding, PrefixVarint64ImproperEncoding) {
  // Values spanning each natural encoding length
  const std::vector<uint64_t> values = {
      0,
      1,
      63,
      127,  // 1-byte natural
      128,
      255,
      16383,  // 2-byte natural
      16384,
      (1ull << 21) - 1,  // 3-byte natural
      (1ull << 21),
      (1ull << 28) - 1,  // 4-byte natural
      (1ull << 28),
      (1ull << 35) - 1,  // 5-byte natural
      (1ull << 35),
      (1ull << 42) - 1,  // 6-byte natural
      (1ull << 42),
      (1ull << 49) - 1,  // 7-byte natural
      (1ull << 49),
      (1ull << 56) - 1,  // 8-byte natural
      (1ull << 56),
      ~uint64_t{0},  // 9-byte natural
  };

  TestPrefixVarint64MinimumBytes<2>(values);
  TestPrefixVarint64MinimumBytes<3>(values);
  TestPrefixVarint64MinimumBytes<4>(values);
  TestPrefixVarint64MinimumBytes<5>(values);
  TestPrefixVarint64MinimumBytes<6>(values);
  TestPrefixVarint64MinimumBytes<7>(values);
  TestPrefixVarint64MinimumBytes<8>(values);

  // Verify specific byte patterns for some improper encodings
  auto check_bytes = [](const char* buf, const char* end,
                        const std::string& expected) {
    ASSERT_EQ(expected, std::string(buf, static_cast<size_t>(end - buf)));
  };

  // value=0 with kMinimumBytes=2: (0 << 2) | (1 << 1) = 0x02
  {
    char buf[kMaxPrefixVarint64Length];
    char* end = EncodePrefixVarint64<2>(buf, 0);
    check_bytes(buf, end, ByteString({0x02, 0x00}));
  }
  // value=0 with kMinimumBytes=3: (0 << 3) | (1 << 2) = 0x04
  {
    char buf[kMaxPrefixVarint64Length];
    char* end = EncodePrefixVarint64<3>(buf, 0);
    check_bytes(buf, end, ByteString({0x04, 0x00, 0x00}));
  }
  // value=1 with kMinimumBytes=2: (1 << 2) | (1 << 1) = 0x06
  {
    char buf[kMaxPrefixVarint64Length];
    char* end = EncodePrefixVarint64<2>(buf, 1);
    check_bytes(buf, end, ByteString({0x06, 0x00}));
  }
  // value=127 with kMinimumBytes=2: (127 << 2) | (1 << 1) = 0x01FE
  {
    char buf[kMaxPrefixVarint64Length];
    char* end = EncodePrefixVarint64<2>(buf, 127);
    check_bytes(buf, end, ByteString({0xFE, 0x01}));
  }
  // value=0 with kMinimumBytes=8: (0 << 8) | (1 << 7) = 0x80
  {
    char buf[kMaxPrefixVarint64Length];
    char* end = EncodePrefixVarint64<8>(buf, 0);
    check_bytes(buf, end,
                ByteString({0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}));
  }
  // value=1 with kMinimumBytes=8: (1 << 8) | (1 << 7) = 0x0180
  {
    char buf[kMaxPrefixVarint64Length];
    char* end = EncodePrefixVarint64<8>(buf, 1);
    check_bytes(buf, end,
                ByteString({0x80, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}));
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
