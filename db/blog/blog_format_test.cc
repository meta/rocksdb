//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blog/blog_format.h"

#include <cstdint>
#include <cstring>
#include <string>

#include "table/format.h"
#include "test_util/testharness.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/prefix_varint.h"

namespace ROCKSDB_NAMESPACE {

class BlogFormatTest : public testing::Test {};

// --- Property encoding/decoding ---

TEST_F(BlogFormatTest, EncodeDecodePropertiesEmpty) {
  BlogPropertyMap props;
  std::string encoded;
  EncodeBlogProperties(props, &encoded);
  ASSERT_EQ(encoded.size(), 0u);

  BlogPropertyMap decoded;
  Slice input(encoded);
  ASSERT_OK(DecodeBlogProperties(&input, &decoded));
  ASSERT_TRUE(decoded.empty());
}

TEST_F(BlogFormatTest, EncodeDecodePropertiesMultiple) {
  BlogPropertyMap props = {
      {"CompressionCompatibilityName", "zstd"},
      {"role", "wal"},
      {"compressionSettings", "level=3"},
  };
  std::string encoded;
  EncodeBlogProperties(props, &encoded);
  ASSERT_GT(encoded.size(), 0u);

  BlogPropertyMap decoded;
  Slice input(encoded);
  ASSERT_OK(DecodeBlogProperties(&input, &decoded));
  ASSERT_EQ(decoded.size(), 3u);
  ASSERT_EQ(decoded[0].first, "CompressionCompatibilityName");
  ASSERT_EQ(decoded[0].second, "zstd");
  ASSERT_EQ(decoded[1].first, "role");
  ASSERT_EQ(decoded[1].second, "wal");
  ASSERT_EQ(decoded[2].first, "compressionSettings");
  ASSERT_EQ(decoded[2].second, "level=3");
}

TEST_F(BlogFormatTest, DecodePropertiesTruncated) {
  // Write a name but no value
  std::string encoded;
  PutLengthPrefixedSlice(&encoded, "key");
  // Truncate before value
  BlogPropertyMap decoded;
  Slice input(encoded);
  ASSERT_TRUE(DecodeBlogProperties(&input, &decoded).IsCorruption());
}

// --- IsBlogRequiredProperty ---

TEST_F(BlogFormatTest, IsBlogRequiredProperty) {
  ASSERT_TRUE(IsBlogRequiredProperty("CompressionCompatibilityName"));
  ASSERT_TRUE(IsBlogRequiredProperty("TimestampSize"));
  ASSERT_TRUE(IsBlogRequiredProperty("A"));
  ASSERT_FALSE(IsBlogRequiredProperty("role"));
  ASSERT_FALSE(IsBlogRequiredProperty("compressionSettings"));
  ASSERT_FALSE(IsBlogRequiredProperty("a"));
  ASSERT_FALSE(IsBlogRequiredProperty(""));
}

// --- Escape sequence generation ---

TEST_F(BlogFormatTest, GenerateRandomFieldsConstraints) {
  // Generate many escape sequences and verify constraints
  for (int i = 0; i < 100; ++i) {
    BlogFileHeader header;
    header.GenerateRandomFields();

    // First byte must not be 0x00 or 0xFF
    uint8_t first = static_cast<uint8_t>(header.escape_sequence[0]);
    ASSERT_NE(first, 0x00u) << "iteration " << i;
    ASSERT_NE(first, 0xFFu) << "iteration " << i;

    // Derived bytes must match
    ASSERT_TRUE(BlogFileHeader::VerifyEscapeSequence(header.escape_sequence))
        << "iteration " << i;
  }
}

TEST_F(BlogFormatTest, IncarnationIdNonzero) {
  BlogFileHeader header;
  header.GenerateRandomFields();
  // incarnation_id is first 4 bytes little-endian; since byte[0] is not 0,
  // the incarnation_id must be nonzero.
  ASSERT_NE(header.incarnation_id(), 0u);
}

TEST_F(BlogFormatTest, VerifyEscapeSequenceRejectsInvalid) {
  char seq[kBlogEscapeSequenceSize] = {};
  // All zeros: first byte is 0x00 -> invalid
  ASSERT_FALSE(BlogFileHeader::VerifyEscapeSequence(seq));

  // Valid first byte but wrong derived part
  seq[0] = 0x42;
  seq[1] = 0x13;
  // Derived part is random garbage, likely won't match
  ASSERT_FALSE(BlogFileHeader::VerifyEscapeSequence(seq));
}

TEST_F(BlogFormatTest, HeuristicEscapeSequenceDetection) {
  // Generate a valid escape sequence and verify we can detect it
  // heuristically (by checking derived bytes against random part).
  BlogFileHeader header;
  header.GenerateRandomFields();

  // VerifyEscapeSequence does exactly this heuristic check
  ASSERT_TRUE(BlogFileHeader::VerifyEscapeSequence(header.escape_sequence));

  // Corrupt one random byte -> should fail verification
  char corrupted[kBlogEscapeSequenceSize];
  memcpy(corrupted, header.escape_sequence, kBlogEscapeSequenceSize);
  corrupted[2] ^= 0x42;
  // Might still pass if the corruption happens to produce a valid derived
  // part, but astronomically unlikely
  // (We don't ASSERT_FALSE here because of the tiny probability)
}

// --- BlogFileHeader encode/decode ---

TEST_F(BlogFormatTest, HeaderEncodeDecodeMinimal) {
  BlogFileHeader header;
  header.checksum_type = kXXH3;
  header.compact_record_type = kBlogWriteBatchRecord;
  header.GenerateRandomFields();

  std::string encoded;
  header.EncodeTo(&encoded);
  ASSERT_GE(encoded.size(), kBlogFileFixedHeaderSize);

  BlogFileHeader decoded;
  Slice input(encoded);
  ASSERT_OK(decoded.DecodeFrom(&input));

  ASSERT_EQ(decoded.schema_version, header.schema_version);
  ASSERT_EQ(decoded.checksum_type, header.checksum_type);
  ASSERT_EQ(decoded.compact_record_type, header.compact_record_type);
  ASSERT_EQ(memcmp(decoded.escape_sequence, header.escape_sequence,
                   kBlogEscapeSequenceSize),
            0);
  ASSERT_TRUE(decoded.properties.empty());
  ASSERT_EQ(input.size(), 0u);  // all bytes consumed
}

TEST_F(BlogFormatTest, HeaderEncodeDecodeWithProperties) {
  BlogFileHeader header;
  header.checksum_type = kCRC32c;
  header.compact_record_type = kBlogBlobRecord;
  header.GenerateRandomFields();
  header.SetProperty("CompressionCompatibilityName", "lz4");
  header.SetProperty("role", "blob");
  header.SetProperty("compressionSettings", "level=1");

  std::string encoded;
  header.EncodeTo(&encoded);

  BlogFileHeader decoded;
  Slice input(encoded);
  ASSERT_OK(decoded.DecodeFrom(&input));

  ASSERT_EQ(decoded.GetProperty("CompressionCompatibilityName"), "lz4");
  ASSERT_EQ(decoded.GetProperty("role"), "blob");
  ASSERT_EQ(decoded.GetProperty("compressionSettings"), "level=1");
  ASSERT_EQ(input.size(), 0u);
}

TEST_F(BlogFormatTest, HeaderRejectsUnknownRequiredProperty) {
  BlogFileHeader header;
  header.GenerateRandomFields();
  header.SetProperty("FutureRequired", "something");

  std::string encoded;
  header.EncodeTo(&encoded);

  BlogFileHeader decoded;
  Slice input(encoded);
  Status s = decoded.DecodeFrom(&input);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
}

TEST_F(BlogFormatTest, HeaderRejectsNewerSchemaVersion) {
  BlogFileHeader header;
  header.GenerateRandomFields();

  std::string encoded;
  header.EncodeTo(&encoded);

  // Tamper with schema version byte (offset 12) to a future version.
  // Must also recompute the fixed header checksum (bytes [36-39]) with
  // context modifier using the incarnation_id.
  encoded[12] = static_cast<char>(kBlogCurrentSchemaVersion + 1);
  uint32_t new_checksum =
      ComputeBuiltinChecksum(header.checksum_type, encoded.data(), 36) +
      ChecksumModifierForContext(header.incarnation_id(), 0);
  EncodeFixed32(&encoded[36], new_checksum);

  BlogFileHeader decoded;
  Slice input(encoded);
  Status s = decoded.DecodeFrom(&input);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_NE(s.ToString().find("newer than supported"), std::string::npos)
      << s.ToString();
}

TEST_F(BlogFormatTest, HeaderCurrentSchemaVersionAccepted) {
  BlogFileHeader header;
  header.GenerateRandomFields();
  ASSERT_EQ(header.schema_version, kBlogCurrentSchemaVersion);

  std::string encoded;
  header.EncodeTo(&encoded);

  BlogFileHeader decoded;
  Slice input(encoded);
  ASSERT_OK(decoded.DecodeFrom(&input));
  ASSERT_EQ(decoded.schema_version, kBlogCurrentSchemaVersion);
}

TEST_F(BlogFormatTest, TypedPropertyRoundTrip) {
  BlogFileHeader header;
  header.GenerateRandomFields();
  header.SetUint64Property("fileNumber", 12345678ULL);
  header.SetUint32Property("columnFamilyId", 42);

  std::string encoded;
  header.EncodeTo(&encoded);

  BlogFileHeader decoded;
  Slice input(encoded);
  ASSERT_OK(decoded.DecodeFrom(&input));

  uint64_t fn = 0;
  ASSERT_TRUE(decoded.GetUint64Property("fileNumber", &fn));
  ASSERT_EQ(fn, 12345678ULL);

  uint32_t cf_id = 0;
  ASSERT_TRUE(decoded.GetUint32Property("columnFamilyId", &cf_id));
  ASSERT_EQ(cf_id, 42u);

  // Missing property returns false
  uint64_t missing = 0;
  ASSERT_FALSE(decoded.GetUint64Property("nonexistent", &missing));
}

TEST_F(BlogFormatTest, HeaderAcceptsUnknownIgnorableProperty) {
  BlogFileHeader header;
  header.GenerateRandomFields();
  header.SetProperty("futureIgnorable", "something");

  std::string encoded;
  header.EncodeTo(&encoded);

  BlogFileHeader decoded;
  Slice input(encoded);
  ASSERT_OK(decoded.DecodeFrom(&input));
  ASSERT_EQ(decoded.GetProperty("futureIgnorable"), "something");
}

TEST_F(BlogFormatTest, HeaderChecksumCorruption) {
  BlogFileHeader header;
  header.GenerateRandomFields();

  std::string encoded;
  header.EncodeTo(&encoded);

  // Corrupt a byte in the fixed header
  encoded[14] ^= 0xFF;

  BlogFileHeader decoded;
  Slice input(encoded);
  ASSERT_TRUE(decoded.DecodeFrom(&input).IsCorruption());
}

TEST_F(BlogFormatTest, HeaderPropertyChecksumCorruption) {
  BlogFileHeader header;
  header.GenerateRandomFields();
  header.SetProperty("role", "wal");

  std::string encoded;
  header.EncodeTo(&encoded);

  // Corrupt a byte in the property section (after fixed header)
  if (encoded.size() > kBlogFileFixedHeaderSize + 2) {
    encoded[kBlogFileFixedHeaderSize + 2] ^= 0xFF;
  }

  BlogFileHeader decoded;
  Slice input(encoded);
  Status s = decoded.DecodeFrom(&input);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

TEST_F(BlogFormatTest, HeaderSetPropertyOverwrites) {
  BlogFileHeader header;
  header.SetProperty("role", "wal");
  ASSERT_EQ(header.GetProperty("role"), "wal");

  header.SetProperty("role", "blob");
  ASSERT_EQ(header.GetProperty("role"), "blob");
  // Should not have duplicate entries
  int count = 0;
  for (const auto& [k, v] : header.properties) {
    if (k == "role") {
      ++count;
    }
  }
  ASSERT_EQ(count, 1);
}

TEST_F(BlogFormatTest, HeaderMagicValues) {
  BlogFileHeader header;
  header.GenerateRandomFields();

  std::string encoded;
  header.EncodeTo(&encoded);

  // Verify magic bytes
  ASSERT_GE(encoded.size(), kBlogFileMagicSize);
  ASSERT_EQ(memcmp(encoded.data(), kBlogFileMagic, kBlogFileMagicSize), 0);
}

// --- Footer locator ---

TEST_F(BlogFormatTest, FooterLocatorEncodeDecodeEmpty) {
  BlogFileFooterLocator locator;

  std::string encoded;
  locator.EncodeTo(&encoded);
  ASSERT_EQ(encoded.size(), 0u);

  BlogFileFooterLocator decoded;
  ASSERT_OK(decoded.DecodeFrom(Slice(encoded)));
  ASSERT_TRUE(decoded.entries.empty());
}

TEST_F(BlogFormatTest, FooterLocatorEncodeDecodeMultiple) {
  BlogFileFooterLocator locator;
  locator.entries.push_back({kBlogFooterPropertiesRecord, 50});
  locator.entries.push_back({kBlogFooterIndexRecord, 85});

  std::string encoded;
  locator.EncodeTo(&encoded);
  ASSERT_EQ(encoded.size(), 10u);  // 2 * (1 + 4)

  BlogFileFooterLocator decoded;
  ASSERT_OK(decoded.DecodeFrom(Slice(encoded)));
  ASSERT_EQ(decoded.entries.size(), 2u);
  ASSERT_EQ(decoded.entries[0].record_type, kBlogFooterPropertiesRecord);
  ASSERT_EQ(decoded.entries[0].relative_offset_4B, 50u);
  ASSERT_EQ(decoded.entries[1].record_type, kBlogFooterIndexRecord);
  ASSERT_EQ(decoded.entries[1].relative_offset_4B, 85u);
}

// --- Footer properties ---

TEST_F(BlogFormatTest, FooterPropertiesEncodeDecodeRoundTrip) {
  BlogFileFooterProperties props;
  props.SetBlobCount(42);
  props.SetBlobPayloadBytes(123456);
  props.SetBlobCompressedBytes(50000);
  props.SetBlobUncompressedBytes(80000);
  props.SetBlobOverheadBytes(3456);
  SmallEnumSet<CompressionType, kDisableCompressionOption> comp_types(
      kZSTD, kLZ4Compression);
  props.SetCompressionTypes(comp_types);
  props.SetSequenceRange(100, 200);

  std::string encoded;
  props.EncodeTo(&encoded);

  BlogFileFooterProperties decoded;
  ASSERT_OK(decoded.DecodeFrom(Slice(encoded)));
  ASSERT_EQ(decoded.properties.size(), 8u);
  // Verify compressionTypes encodes as sorted bytes
  for (const auto& [name, value] : decoded.properties) {
    if (name == "compressionTypes") {
      ASSERT_EQ(value.size(), 2u);
      ASSERT_EQ(static_cast<uint8_t>(value[0]), 4u);  // kLZ4Compression
      ASSERT_EQ(static_cast<uint8_t>(value[1]), 7u);  // kZSTD
    }
  }
}

// --- Padding ---

TEST_F(BlogFormatTest, PaddingLastByteZero) {
  // When last byte is 0x00, must pad with 0xFF, at least 1 byte
  BlogPadding pad(0x00, 0);
  ASSERT_GE(pad.count, 1u);
  ASSERT_EQ(lossless_cast<uint8_t>(pad.byte), 0xFFu);
  ASSERT_EQ(pad.count % 4, 0u);
}

TEST_F(BlogFormatTest, PaddingLastByteFF) {
  // When last byte is 0xFF, must pad with 0x00, at least 1 byte
  BlogPadding pad(0xFF, 0);
  ASSERT_GE(pad.count, 1u);
  ASSERT_EQ(lossless_cast<uint8_t>(pad.byte), 0x00u);
  ASSERT_EQ(pad.count % 4, 0u);
}

TEST_F(BlogFormatTest, PaddingLastByteOther) {
  // When last byte is something else, pad only if needed for alignment
  BlogPadding pad1(0x42, 0);
  ASSERT_EQ(pad1.count, 0u);

  BlogPadding pad2(0x42, 1);
  ASSERT_EQ(pad2.count, 3u);
  ASSERT_EQ(lossless_cast<uint8_t>(pad2.byte), 0x00u);
}

TEST_F(BlogFormatTest, PaddingAlignment) {
  for (size_t offset = 0; offset < 8; ++offset) {
    BlogPadding pad(0x42, offset);
    size_t after = offset + pad.count;
    ASSERT_EQ(after % 4, 0u) << "offset=" << offset;
  }
}

TEST_F(BlogFormatTest, PaddingAppendTo) {
  std::string dst;
  BlogPadding pad(0x00, 0);
  pad.AppendTo(&dst);
  ASSERT_EQ(dst.size(), pad.count);
  for (size_t i = 0; i < pad.count; ++i) {
    ASSERT_EQ(static_cast<uint8_t>(dst[i]), 0xFFu);
  }
}

TEST_F(BlogFormatTest, PaddingAllowsStrongerAlignment) {
  BlogPadding pad(0x42, 100, kBlogFlushFriendlyAlignment,
                  /*prefer_0xff_when_legal=*/true);
  ASSERT_EQ((100u + pad.count) % kBlogFlushFriendlyAlignment, 0u);
  ASSERT_EQ(lossless_cast<uint8_t>(pad.byte), 0xFFu);
}

TEST_F(BlogFormatTest, IsValidPaddingAcceptsSupportedAlignments) {
  const uint8_t last_meaningful_byte = 0x42;
  const uint64_t current_offset = 100;

  const uint32_t min_pad = ComputeBlogPaddingLength(
      last_meaningful_byte, current_offset, kBlogMinRecordAlignment);
  ASSERT_TRUE(IsValidBlogPadding(last_meaningful_byte, current_offset, min_pad,
                                 /*pad_byte=*/0x00));

  const uint32_t flush_pad = ComputeBlogPaddingLength(
      last_meaningful_byte, current_offset, kBlogFlushFriendlyAlignment);
  ASSERT_TRUE(IsValidBlogPadding(last_meaningful_byte, current_offset,
                                 flush_pad, /*pad_byte=*/0xFF));

  const uint32_t page_pad = ComputeBlogPaddingLength(
      last_meaningful_byte, current_offset, kBlogPageFriendlyAlignment);
  ASSERT_TRUE(IsValidBlogPadding(last_meaningful_byte, current_offset, page_pad,
                                 /*pad_byte=*/0x00));
  ASSERT_FALSE(IsValidBlogPadding(last_meaningful_byte, current_offset,
                                  /*count=*/5, /*pad_byte=*/0x00));
}

// --- Improper (non-minimal) prefix varint ---

TEST_F(BlogFormatTest, ImproperPrefixVarintZero) {
  char buf[kMaxPrefixVarint64Length];
  char* end = EncodePrefixVarint64<4>(buf, 0);
  size_t len = static_cast<size_t>(end - buf);
  ASSERT_GE(len, 4u);

  uint64_t value;
  ASSERT_TRUE(DecodePrefixVarint64(buf[0], buf + 1, len - 1, &value));
  ASSERT_EQ(value, 0u);
}

TEST_F(BlogFormatTest, ImproperPrefixVarintSmallValue) {
  char buf[kMaxPrefixVarint64Length];
  char* end = EncodePrefixVarint64<4>(buf, 42);
  size_t len = static_cast<size_t>(end - buf);
  ASSERT_GE(len, 4u);

  uint64_t value;
  ASSERT_TRUE(DecodePrefixVarint64(buf[0], buf + 1, len - 1, &value));
  ASSERT_EQ(value, 42u);
}

TEST_F(BlogFormatTest, ImproperPrefixVarintLargeValue) {
  char buf[kMaxPrefixVarint64Length];
  char* end = EncodePrefixVarint64<5>(buf, 100000);
  size_t len = static_cast<size_t>(end - buf);
  ASSERT_GE(len, 5u);

  uint64_t value;
  ASSERT_TRUE(DecodePrefixVarint64(buf[0], buf + 1, len - 1, &value));
  ASSERT_EQ(value, 100000u);
}

TEST_F(BlogFormatTest, ImproperPrefixVarintNaturallyLargeEnough) {
  uint64_t original = 1ULL << 35;
  char buf[kMaxPrefixVarint64Length];
  char* end = EncodePrefixVarint64<4>(buf, original);
  size_t len = static_cast<size_t>(end - buf);
  // Natural encoding is 6 bytes, already >= 4
  ASSERT_EQ(len, 6u);

  uint64_t value;
  ASSERT_TRUE(DecodePrefixVarint64(buf[0], buf + 1, len - 1, &value));
  ASSERT_EQ(value, original);
}

TEST_F(BlogFormatTest, UnspecifiedSizePrefixVarint) {
  std::string encoded;
  PutPrefixVarint64(&encoded, kBlogUnspecifiedSize);
  // 2^63 - 1 encodes to 9 bytes in prefix varint (0x00 + fixed64)
  ASSERT_EQ(encoded.size(), 9u);

  Slice input(encoded);
  uint64_t value;
  ASSERT_TRUE(GetPrefixVarint64(&input, &value));
  ASSERT_EQ(value, kBlogUnspecifiedSize);
}

// --- Checksum ---

TEST_F(BlogFormatTest, ComputeBlogRecordChecksumBasic) {
  const char* data = "hello world";
  size_t size = strlen(data);

  // With no context (incarnation_id = 0), should match standard checksum
  uint32_t cksum = ComputeBlogRecordChecksum(
      kXXH3, data, size, static_cast<char>(kNoCompression), 0, 0);
  uint32_t expected = ComputeBuiltinChecksumWithLastByte(
      kXXH3, data, size, static_cast<char>(kNoCompression));
  ASSERT_EQ(cksum, expected);
}

TEST_F(BlogFormatTest, ComputeBlogRecordChecksumWithContext) {
  const char* data = "hello world";
  size_t size = strlen(data);

  uint32_t cksum1 = ComputeBlogRecordChecksum(
      kXXH3, data, size, static_cast<char>(kNoCompression), 0x12345678, 100);
  uint32_t cksum2 = ComputeBlogRecordChecksum(
      kXXH3, data, size, static_cast<char>(kNoCompression), 0x12345678, 200);

  // Different offsets should produce different checksums
  ASSERT_NE(cksum1, cksum2);

  // Different incarnation IDs should produce different checksums
  uint32_t cksum3 = ComputeBlogRecordChecksum(
      kXXH3, data, size, static_cast<char>(kNoCompression), 0xABCDEF01, 100);
  ASSERT_NE(cksum1, cksum3);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
