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

class BlogFormatTest : public testing::Test {
 protected:
  static std::string MakeFooterLocatorRecord(const BlogFileHeader& header,
                                             uint64_t file_offset,
                                             uint32_t incarnation_id,
                                             uint64_t declared_length,
                                             const std::string& payload) {
    return MakeFooterLocatorRecordWithCompression(
        header, file_offset, incarnation_id, declared_length, payload,
        kNoCompression, kNoCompression);
  }

  static std::string MakeFooterLocatorRecordWithCompression(
      const BlogFileHeader& header, uint64_t file_offset,
      uint32_t incarnation_id, uint64_t declared_length,
      const std::string& payload, CompressionType prefix_comp,
      CompressionType trailer_comp) {
    std::string record(header.escape_sequence, kBlogEscapeSequenceSize);
    char varint[kMaxPrefixVarint64Length];
    const char* varint_end = EncodePrefixVarint64<3>(varint, declared_length);
    std::string prefix(varint, lossless_cast<size_t>(varint_end - varint));
    prefix.push_back(lossless_cast<char>(kBlogFooterLocatorRecord));
    prefix.push_back(lossless_cast<char>(prefix_comp));
    const uint32_t prefix_checksum =
        ComputeBuiltinChecksum(header.checksum_type, prefix.data(),
                               prefix.size()) +
        ChecksumModifierForContext(incarnation_id, file_offset);
    record.append(prefix);
    PutFixed32(&record, prefix_checksum);
    const uint64_t payload_file_offset = file_offset + record.size();
    record.append(payload);
    record.push_back(lossless_cast<char>(trailer_comp));
    PutFixed32(&record, ComputeBlogRecordChecksum(
                            header.checksum_type, payload.data(),
                            payload.size(), lossless_cast<char>(trailer_comp),
                            incarnation_id, payload_file_offset));
    return record;
  }
};

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

  ASSERT_TRUE(IsBlogIgnorableProperty("role"));
  ASSERT_TRUE(IsBlogIgnorableProperty("a"));
  ASSERT_FALSE(IsBlogIgnorableProperty("TimestampSize"));
  ASSERT_FALSE(IsBlogIgnorableProperty("1invalid"));
  ASSERT_FALSE(IsBlogIgnorableProperty(""));
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

TEST_F(BlogFormatTest, WalEscapeSequenceBindsLogNumber) {
  // Empty session id exercises the random fallback.
  for (const std::string& db_session_id :
       {std::string(), std::string("0123456789ABCDEFGHIJ")}) {
    BlogFileHeader header;
    header.compact_record_type = kBlogWriteBatchRecord;
    header.GenerateWalFromUniqueId("db-id", db_session_id, /*log_number=*/123);
    ASSERT_TRUE(BlogFileHeader::VerifyWalEscapeSequence(header.escape_sequence,
                                                        /*log_number=*/123));
    ASSERT_FALSE(BlogFileHeader::VerifyWalEscapeSequence(header.escape_sequence,
                                                         /*log_number=*/124));
    ASSERT_FALSE(BlogFileHeader::VerifyEscapeSequence(header.escape_sequence));

    // GenerateWalFromUniqueId records dbId/dbSessionId as ignorable
    // properties, which EncodeTo leaves out of the header section.
    std::string encoded;
    header.EncodeTo(&encoded);
    Slice input(encoded);
    BlogFileHeader decoded;
    ASSERT_OK(decoded.DecodeFrom(&input));
  }
}

TEST_F(BlogFormatTest, NonWalHeaderRejectsWalBoundEscapeSequence) {
  BlogFileHeader header;
  header.compact_record_type = kBlogWriteBatchRecord;
  header.GenerateWalFromUniqueId("db-id", "0123456789ABCDEFGHIJ",
                                 /*log_number=*/123);
  header.compact_record_type = kBlogBlobRecord;
  std::string encoded;
  header.EncodeTo(&encoded);
  Slice input(encoded);
  BlogFileHeader decoded;
  Status s = decoded.DecodeFrom(&input);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_NE(s.ToString().find("invalid escape sequence"), std::string::npos);
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
  ASSERT_TRUE(decoded.required_properties.empty());
  ASSERT_TRUE(decoded.ignorable_properties.empty());
  ASSERT_EQ(input.size(), 0u);  // all bytes consumed
}

TEST_F(BlogFormatTest, HeaderEncodeDecodeWithProperties) {
  BlogFileHeader header;
  header.checksum_type = kCRC32c;
  header.compact_record_type = kBlogBlobRecord;
  header.GenerateRandomFields();
  header.SetProperty("CompressionCompatibilityName", "lz4");
  header.SetProperty("PredecessorWalInfo", "some-info");
  header.SetUint32Property("TimestampSize", 8);

  std::string encoded;
  header.EncodeTo(&encoded);

  BlogFileHeader decoded;
  Slice input(encoded);
  ASSERT_OK(decoded.DecodeFrom(&input));

  ASSERT_EQ(decoded.GetProperty("CompressionCompatibilityName"), "lz4");
  ASSERT_EQ(decoded.GetProperty("PredecessorWalInfo"), "some-info");
  uint32_t ts = 0;
  ASSERT_TRUE(decoded.GetUint32Property("TimestampSize", &ts));
  ASSERT_EQ(ts, 8u);
  ASSERT_EQ(input.size(), 0u);
}

TEST_F(BlogFormatTest, HeaderEncodeDecodeWithIgnorableProperties) {
  BlogFileHeader header;
  header.checksum_type = kXXH3;
  header.compact_record_type = kBlogBlobRecord;
  header.GenerateRandomFields();
  header.SetProperty("CompressionCompatibilityName", "BuiltinV2");
  // Ignorable properties must not break header decode; they travel in body
  // records, not the header property section.
  header.SetProperty("role", "blob");
  std::string encoded;
  header.EncodeTo(&encoded);
  BlogFileHeader decoded;
  Slice input(encoded);
  ASSERT_OK(decoded.DecodeFrom(&input));
  // Only required properties survive the header round-trip; ignorable ones
  // are body-record content decoded separately by the reader.
  ASSERT_EQ(decoded.GetProperty("CompressionCompatibilityName"), "BuiltinV2");
  ASSERT_TRUE(decoded.ignorable_properties.empty());
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
  // In-memory round-trip for ignorable typed properties (not via header
  // encoding, since the header section holds only required uppercase props).
  BlogFileHeader header;
  header.GenerateRandomFields();
  header.SetUint64Property("fileNumber", 12345678ULL);
  header.SetUint32Property("columnFamilyId", 42);

  uint64_t fn = 0;
  ASSERT_TRUE(header.GetUint64Property("fileNumber", &fn));
  ASSERT_EQ(fn, 12345678ULL);

  uint32_t cf_id = 0;
  ASSERT_TRUE(header.GetUint32Property("columnFamilyId", &cf_id));
  ASSERT_EQ(cf_id, 42u);

  // Missing property returns false
  uint64_t missing = 0;
  ASSERT_FALSE(header.GetUint64Property("nonexistent", &missing));

  // Header encoding round-trip for required typed property.
  BlogFileHeader header2;
  header2.GenerateRandomFields();
  header2.SetUint32Property("TimestampSize", 8);
  std::string encoded;
  header2.EncodeTo(&encoded);
  BlogFileHeader decoded;
  Slice input(encoded);
  ASSERT_OK(decoded.DecodeFrom(&input));
  uint32_t ts = 0;
  ASSERT_TRUE(decoded.GetUint32Property("TimestampSize", &ts));
  ASSERT_EQ(ts, 8u);
}

TEST_F(BlogFormatTest, HeaderRejectsIgnorableProperty) {
  BlogFileHeader header;
  header.GenerateRandomFields();
  // Bypass the setters to simulate a corrupt/foreign file whose header
  // section contains a lowercase property. Our own writer can no longer
  // produce this: EncodeTo only encodes required_properties.
  header.required_properties.emplace_back("futureIgnorable", "something");

  std::string encoded;
  header.EncodeTo(&encoded);

  BlogFileHeader decoded;
  Slice input(encoded);
  Status s = decoded.DecodeFrom(&input);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_NE(s.ToString().find("ignorable property in header"),
            std::string::npos)
      << s.ToString();
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

TEST_F(BlogFormatTest, DecodeFromBufferVerifiesChecksumBeforePropertySize) {
  BlogFileHeader header;
  header.GenerateRandomFields();
  header.SetProperty("role", "wal");

  std::string encoded;
  header.EncodeTo(&encoded);

  // An intact fixed header without its property section needs more data.
  BlogFileHeader decoded;
  size_t additional_bytes_needed = 0;
  Slice fixed_only(encoded.data(), kBlogFileFixedHeaderSize);
  ASSERT_TRUE(decoded.DecodeFromBuffer(&fixed_only, &additional_bytes_needed)
                  .IsIncomplete());
  ASSERT_EQ(additional_bytes_needed, encoded.size() - kBlogFileFixedHeaderSize);

  // A corrupted property section size that points past the buffer fails the
  // fixed header checksum instead of asking for more data.
  EncodeFixed32(&encoded[32], 1 << 20);
  Slice corrupted(encoded);
  Status s = decoded.DecodeFromBuffer(&corrupted, &additional_bytes_needed);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

TEST_F(BlogFormatTest, HeaderPropertyChecksumCorruption) {
  BlogFileHeader header;
  header.GenerateRandomFields();
  header.SetProperty("CompressionCompatibilityName", "wal");

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

TEST_F(BlogFormatTest, HeaderPropertySettersOverwrite) {
  BlogFileHeader header;
  header.SetProperty("role", "wal");
  ASSERT_EQ(header.GetProperty("role"), "wal");

  header.SetProperty("role", "blob");
  ASSERT_EQ(header.GetProperty("role"), "blob");
  // Should not have duplicate entries
  int count = 0;
  for (const auto& [k, v] : header.ignorable_properties) {
    if (k == "role") {
      ++count;
    }
  }
  ASSERT_EQ(count, 1);

  header.SetProperty("PredecessorWalInfo", "a");
  header.SetProperty("PredecessorWalInfo", "b");
  ASSERT_EQ(header.GetProperty("PredecessorWalInfo"), "b");
  count = 0;
  for (const auto& [k, v] : header.required_properties) {
    if (k == "PredecessorWalInfo") {
      ++count;
    }
  }
  ASSERT_EQ(count, 1);

  // Uppercase names land in required_properties, lowercase in ignorable.
  ASSERT_EQ(header.required_properties.size(), 1u);
  ASSERT_EQ(header.required_properties[0].first, "PredecessorWalInfo");
  ASSERT_EQ(header.ignorable_properties.size(), 1u);
  ASSERT_EQ(header.ignorable_properties[0].first, "role");
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

TEST_F(BlogFormatTest, FooterLocatorRejectsUnboundedLength) {
  // Zero context and kNoChecksum avoid a huge payload checksum scan if the
  // bounds check regresses. On wraparound, trailer reads would target the
  // zero-filled prefix checksum or payload instead of rejecting the length.
  BlogFileHeader header;
  header.checksum_type = kNoChecksum;
  header.escape_sequence[0] = 0x42;
  const std::string payload(5, '\0');
  const uint64_t invalid_lengths[] = {6,
                                      kBlogUnspecifiedSize,
                                      UINT64_MAX - 4,
                                      UINT64_MAX - 3,
                                      UINT64_MAX - 2,
                                      UINT64_MAX - 1,
                                      UINT64_MAX};
  for (const uint64_t length : invalid_lengths) {
    SCOPED_TRACE("length=" + std::to_string(length));
    const std::string record =
        MakeFooterLocatorRecord(header, 0, 0, length, payload);
    uint64_t locator_offset = 123;
    ASSERT_FALSE(VerifyBlogFooterLocator(
        record.data(), record.size(), 0, header.escape_sequence,
        header.checksum_type, 0, nullptr, &locator_offset));
    ASSERT_EQ(locator_offset, 123u);
  }

  std::string short_trailer = MakeFooterLocatorRecord(header, 0, 0, 1, "x");
  short_trailer.resize(short_trailer.size() - 2);
  ASSERT_FALSE(VerifyBlogFooterLocator(
      short_trailer.data(), short_trailer.size(), 0, header.escape_sequence,
      header.checksum_type, 0, nullptr, nullptr));
}

TEST_F(BlogFormatTest, FooterLocatorBoundedPayloadRoundTrip) {
  BlogFileFooterLocator locator;
  locator.entries.push_back({kBlogFooterPropertiesRecord, 50});
  std::string payload;
  locator.EncodeTo(&payload);
  const uint64_t file_offset = 100;
  for (const ChecksumType checksum_type : {kNoChecksum, kCRC32c, kXXH3}) {
    SCOPED_TRACE("checksum=" + std::to_string(checksum_type));
    BlogFileHeader header;
    header.GenerateRandomFields();
    header.checksum_type = checksum_type;
    const std::string record = MakeFooterLocatorRecord(
        header, file_offset, header.incarnation_id(), payload.size(), payload);
    BlogFileFooterLocator decoded;
    uint64_t locator_offset = 0;
    ASSERT_TRUE(VerifyBlogFooterLocator(
        record.data(), record.size(), file_offset, header.escape_sequence,
        checksum_type, header.incarnation_id(), &decoded, &locator_offset));
    ASSERT_EQ(locator_offset, file_offset);
    ASSERT_EQ(decoded.entries.size(), 1u);
    ASSERT_EQ(decoded.entries[0].record_type, locator.entries[0].record_type);
    ASSERT_EQ(decoded.entries[0].relative_offset_4B,
              locator.entries[0].relative_offset_4B);
  }
}

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

// --- Header validation (spec: Fixed Header) ---

TEST_F(BlogFormatTest, HeaderRejectsZeroSchemaVersion) {
  BlogFileHeader header;
  header.GenerateRandomFields();
  std::string encoded;
  header.EncodeTo(&encoded);
  encoded[12] = 0;
  uint32_t new_checksum =
      ComputeBuiltinChecksum(header.checksum_type, encoded.data(), 36) +
      ChecksumModifierForContext(header.incarnation_id(), 0);
  EncodeFixed32(&encoded[36], new_checksum);
  BlogFileHeader decoded;
  Slice input(encoded);
  ASSERT_TRUE(decoded.DecodeFrom(&input).IsCorruption());
}

TEST_F(BlogFormatTest, HeaderRejectsUnsupportedChecksumType) {
  BlogFileHeader header;
  header.GenerateRandomFields();
  std::string encoded;
  header.EncodeTo(&encoded);
  encoded[13] = static_cast<char>(0x7F);
  // Checksum will be computed with invalid type, but our code rejects
  // before checksum verification, so no need to fix checksum.
  BlogFileHeader decoded;
  Slice input(encoded);
  ASSERT_TRUE(decoded.DecodeFrom(&input).IsCorruption());
}

TEST_F(BlogFormatTest, HeaderRejectsInvalidCompactRecordType) {
  // Meta type (0x01) not allowed as compact
  {
    BlogFileHeader header;
    header.GenerateRandomFields();
    header.compact_record_type = kBlogIgnorablePropertiesRecord;
    std::string encoded;
    header.EncodeTo(&encoded);
    // Fix header checksum after tampering? EncodeTo already used invalid
    // type, so checksum is correct for that value. We just decode.
    BlogFileHeader decoded;
    Slice input(encoded);
    ASSERT_TRUE(decoded.DecodeFrom(&input).IsCorruption());
  }
  // Footer type not allowed
  {
    BlogFileHeader header;
    header.GenerateRandomFields();
    header.compact_record_type = kBlogFooterLocatorRecord;
    std::string encoded;
    header.EncodeTo(&encoded);
    BlogFileHeader decoded;
    Slice input(encoded);
    ASSERT_TRUE(decoded.DecodeFrom(&input).IsCorruption());
  }
  // Reserved body type (0x14)
  {
    BlogFileHeader header;
    header.GenerateRandomFields();
    header.compact_record_type = static_cast<BlogRecordType>(0x14);
    std::string encoded;
    header.EncodeTo(&encoded);
    BlogFileHeader decoded;
    Slice input(encoded);
    ASSERT_TRUE(decoded.DecodeFrom(&input).IsNotSupported());
  }
}

TEST_F(BlogFormatTest, HeaderRejectsUnknownFlags) {
  BlogFileHeader header;
  header.GenerateRandomFields();
  header.flags = 0x02;
  std::string encoded;
  header.EncodeTo(&encoded);
  BlogFileHeader decoded;
  Slice input(encoded);
  ASSERT_TRUE(decoded.DecodeFrom(&input).IsNotSupported());
}

TEST_F(BlogFormatTest, HeaderRejectsNonZeroReserved) {
  BlogFileHeader header;
  header.GenerateRandomFields();
  std::string encoded;
  header.EncodeTo(&encoded);
  encoded[26] = 0x42;
  uint32_t new_checksum =
      ComputeBuiltinChecksum(header.checksum_type, encoded.data(), 36) +
      ChecksumModifierForContext(header.incarnation_id(), 0);
  EncodeFixed32(&encoded[36], new_checksum);
  BlogFileHeader decoded;
  Slice input(encoded);
  ASSERT_TRUE(decoded.DecodeFrom(&input).IsNotSupported());
}

TEST_F(BlogFormatTest, HeaderRejectsNonZeroPropCompType) {
  BlogFileHeader header;
  header.GenerateRandomFields();
  std::string encoded;
  header.EncodeTo(&encoded);
  // Property section trailer compression_type is at offset 40+prop_data_size
  uint32_t prop_size = DecodeFixed32(encoded.data() + 32);
  size_t comp_offset = kBlogFileFixedHeaderSize + prop_size - 5;
  encoded[comp_offset] = lossless_cast<char>(kLZ4Compression);
  // Recompute property section checksum with new comp type
  const char* prop_data = encoded.data() + kBlogFileFixedHeaderSize;
  size_t prop_data_size = prop_size - 5;
  uint32_t new_prop_cksum = ComputeBuiltinChecksumWithLastByte(
      header.checksum_type, prop_data, prop_data_size, encoded[comp_offset]);
  new_prop_cksum += ChecksumModifierForContext(header.incarnation_id(),
                                               kBlogFileFixedHeaderSize);
  EncodeFixed32(&encoded[comp_offset + 1], new_prop_cksum);
  BlogFileHeader decoded;
  Slice input(encoded);
  ASSERT_TRUE(decoded.DecodeFrom(&input).IsCorruption());
}

// --- Property decoding validation ---

TEST_F(BlogFormatTest, DecodePropertiesRejectsEmptyName) {
  std::string encoded;
  PutPrefixVarint32(&encoded, 0);
  PutPrefixVarint32(&encoded, 3);
  encoded.append("val");
  Slice input(encoded);
  BlogPropertyMap props;
  ASSERT_TRUE(DecodeBlogProperties(&input, &props).IsCorruption());
}

TEST_F(BlogFormatTest, DecodePropertiesRejectsDuplicate) {
  BlogPropertyMap props = {{"role", "wal"}, {"role", "blob"}};
  std::string encoded;
  EncodeBlogProperties(props, &encoded);
  Slice input(encoded);
  BlogPropertyMap decoded;
  ASSERT_TRUE(DecodeBlogProperties(&input, &decoded).IsCorruption());
}

TEST_F(BlogFormatTest, TypedPropertyRejectsTrailingBytes) {
  BlogFileHeader header;
  header.GenerateRandomFields();
  std::string bad;
  PutPrefixVarint64(&bad, 42);
  bad.push_back(0xFF);  // trailing garbage
  header.SetProperty("fileNumber", bad);
  // Get helper should reject trailing bytes
  uint64_t v = 0;
  ASSERT_FALSE(header.GetUint64Property("fileNumber", &v));
}

// --- Footer properties validation ---

TEST_F(BlogFormatTest, FooterPropertiesRejectsRequiredUppercase) {
  BlogFileFooterProperties props;
  props.properties.emplace_back("FutureRequired", "x");
  std::string encoded;
  props.EncodeTo(&encoded);
  BlogFileFooterProperties decoded;
  ASSERT_TRUE(decoded.DecodeFrom(Slice(encoded)).IsNotSupported());
}

// --- Record trailer validation ---

TEST_F(BlogFormatTest, VerifyRecordTrailerRejectsInvalidCompType) {
  const char* payload = "hello";
  size_t payload_size = 5;
  char trailer[5];
  trailer[0] = static_cast<char>(0x08);  // reserved compression type
  uint32_t checksum = ComputeBlogRecordChecksum(kXXH3, payload, payload_size,
                                                trailer[0], 0x1234, 100);
  EncodeFixed32(trailer + 1, checksum);
  // Build a buffer where VerifyBlogRecordTrailer expects payload+trailer
  std::string buf(payload, payload_size);
  buf.append(trailer, 5);
  CompressionType ct;
  Status s = VerifyBlogRecordTrailer(kXXH3, buf.data(), payload_size, 0x1234,
                                     100, &ct);
  ASSERT_TRUE(s.IsCorruption());
}

TEST_F(BlogFormatTest, HeaderRejectsInvalidStreamingCompressionType) {
  const struct {
    const char* value;
    bool corruption;
    bool not_supported;
  } cases[] = {
      {"", true, false},    {"0", true, false},  {"07x", true, false},
      {"zz", true, false},  {"08", true, false}, {"7f", true, false},
      {"00", false, true},  {"01", false, true}, {"80", false, true},
      {"07", false, false},
  };
  for (const auto& test_case : cases) {
    SCOPED_TRACE(test_case.value);
    BlogFileHeader header;
    header.GenerateRandomFields();
    header.SetProperty(kBlogPropWriteBatchStreamingCompressionType,
                       test_case.value);
    std::string encoded;
    header.EncodeTo(&encoded);
    BlogFileHeader decoded;
    Slice input(encoded);
    const Status s = decoded.DecodeFrom(&input);
    if (test_case.corruption) {
      ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    } else if (test_case.not_supported) {
      ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
    } else {
      ASSERT_OK(s);
    }
  }
}

TEST_F(BlogFormatTest, HeaderRejectsInvalidTimestampSize) {
  BlogFileHeader header;
  header.GenerateRandomFields();
  header.SetProperty(kBlogPropTimestampSize, "not-a-varint\xff\xff");
  std::string encoded;
  header.EncodeTo(&encoded);
  BlogFileHeader decoded;
  Slice input(encoded);
  ASSERT_TRUE(decoded.DecodeFrom(&input).IsCorruption());
}

TEST_F(BlogFormatTest, FooterLocatorRejectsInvalidEntryType) {
  BlogFileFooterLocator locator;
  locator.entries.push_back(
      {lossless_cast<BlogRecordType>(uint8_t{0x10}), 10});  // body, not footer
  std::string encoded;
  locator.EncodeTo(&encoded);
  BlogFileFooterLocator decoded;
  ASSERT_TRUE(decoded.DecodeFrom(Slice(encoded)).IsCorruption());
}

TEST_F(BlogFormatTest, FooterLocatorRejectsUnknownFooterType) {
  BlogFileFooterLocator locator;
  locator.entries.push_back({lossless_cast<BlogRecordType>(uint8_t{0xF1}), 10});
  std::string encoded;
  locator.EncodeTo(&encoded);
  BlogFileFooterLocator decoded;
  ASSERT_TRUE(decoded.DecodeFrom(Slice(encoded)).IsNotSupported());
}

TEST_F(BlogFormatTest, FooterLocatorRejectsZeroOffset) {
  BlogFileFooterLocator locator;
  locator.entries.push_back({kBlogFooterPropertiesRecord, 0});
  std::string encoded;
  locator.EncodeTo(&encoded);
  BlogFileFooterLocator decoded;
  ASSERT_TRUE(decoded.DecodeFrom(Slice(encoded)).IsCorruption());
}

TEST_F(BlogFormatTest, FooterLocatorRejectsSelfReference) {
  BlogFileFooterLocator locator;
  locator.entries.push_back({kBlogFooterLocatorRecord, 10});
  std::string encoded;
  locator.EncodeTo(&encoded);
  BlogFileFooterLocator decoded;
  ASSERT_TRUE(decoded.DecodeFrom(Slice(encoded)).IsCorruption());
}

TEST_F(BlogFormatTest, FooterPropertiesRejectsInvalidCompressionTypes) {
  // Unsorted
  {
    BlogFileFooterProperties props;
    std::string unsorted;
    unsorted.push_back(lossless_cast<char>(kZSTD));
    unsorted.push_back(lossless_cast<char>(kLZ4Compression));
    props.properties.emplace_back("compressionTypes", unsorted);
    std::string encoded;
    props.EncodeTo(&encoded);
    BlogFileFooterProperties decoded;
    ASSERT_TRUE(decoded.DecodeFrom(Slice(encoded)).IsCorruption());
  }
  // Includes kNoCompression
  {
    BlogFileFooterProperties props;
    std::string bad;
    bad.push_back(lossless_cast<char>(kNoCompression));
    props.properties.emplace_back("compressionTypes", bad);
    std::string encoded;
    props.EncodeTo(&encoded);
    BlogFileFooterProperties decoded;
    ASSERT_TRUE(decoded.DecodeFrom(Slice(encoded)).IsCorruption());
  }
  // Invalid type 0x08
  {
    BlogFileFooterProperties props;
    std::string bad;
    bad.push_back(static_cast<char>(0x08));
    props.properties.emplace_back("compressionTypes", bad);
    std::string encoded;
    props.EncodeTo(&encoded);
    BlogFileFooterProperties decoded;
    ASSERT_TRUE(decoded.DecodeFrom(Slice(encoded)).IsCorruption());
  }
}

TEST_F(BlogFormatTest, FooterPropertiesRejectsInvalidBlobCount) {
  BlogFileFooterProperties props;
  props.properties.emplace_back("blobCount", "not-varint\xff");
  std::string encoded;
  props.EncodeTo(&encoded);
  BlogFileFooterProperties decoded;
  ASSERT_TRUE(decoded.DecodeFrom(Slice(encoded)).IsCorruption());
}

TEST_F(BlogFormatTest, FooterPropertiesRejectsMinGreaterMax) {
  BlogFileFooterProperties props;
  props.SetSequenceRange(200, 100);
  std::string encoded;
  props.EncodeTo(&encoded);
  BlogFileFooterProperties decoded;
  ASSERT_TRUE(decoded.DecodeFrom(Slice(encoded)).IsCorruption());
}

TEST_F(BlogFormatTest, HeaderRejectsCaseFlippedRequiredProperty) {
  for (ChecksumType checksum : {kXXH3, kNoChecksum}) {
    BlogFileHeader header;
    header.GenerateRandomFields();
    header.checksum_type = checksum;
    header.SetProperty(kBlogPropPredecessorWalInfo, "predecessor");
    std::string encoded;
    header.EncodeTo(&encoded);
    const size_t property_offset = encoded.find(kBlogPropPredecessorWalInfo);
    ASSERT_NE(property_offset, std::string::npos);
    encoded[property_offset] ^= 0x20;  // Uppercase P becomes lowercase p.
    BlogFileHeader decoded;
    Slice input(encoded);
    const Status s = decoded.DecodeFrom(&input);
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    if (checksum == kNoChecksum) {
      ASSERT_NE(s.ToString().find("ignorable property in header"),
                std::string::npos)
          << s.ToString();
    }
  }
}

TEST_F(BlogFormatTest, HeaderTimestampSizeValueBoundaries) {
  std::string trailing;
  PutPrefixVarint64(&trailing, 8);
  trailing.push_back('x');
  const std::string invalid[] = {std::string(), std::string(1, '\0'), trailing};
  for (const std::string& value : invalid) {
    BlogFileHeader header;
    header.GenerateRandomFields();
    header.SetProperty(kBlogPropTimestampSize, value);
    std::string encoded;
    header.EncodeTo(&encoded);
    BlogFileHeader decoded;
    Slice input(encoded);
    const Status s = decoded.DecodeFrom(&input);
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  }
  BlogFileHeader header;
  header.GenerateRandomFields();
  header.SetUint64Property(kBlogPropTimestampSize, 0);
  std::string encoded;
  header.EncodeTo(&encoded);
  BlogFileHeader decoded;
  Slice input(encoded);
  ASSERT_OK(decoded.DecodeFrom(&input));
}

TEST_F(BlogFormatTest, FooterLocatorRejectsMalformedRecordWithoutOutput) {
  const struct {
    uint64_t declared_length;
    std::string payload;
    CompressionType prefix_comp;
    CompressionType trailer_comp;
  } cases[] = {
      {0, "", kNoCompression, kNoCompression},
      {1, "x", kNoCompression, kNoCompression},
      {5, std::string(5, '\0'), kNoCompression, kNoCompression},
      {5, std::string("\xf9\x01\x00\x00\x00", 5), kNoCompression,
       kNoCompression},
      {5, std::string("\xf8\x01\x00\x00\x00", 5),
       lossless_cast<CompressionType>(uint8_t{0x08}), kNoCompression},
      {5, std::string("\xf8\x01\x00\x00\x00", 5), kSnappyCompression,
       kNoCompression},
  };
  for (ChecksumType checksum : {kXXH3, kNoChecksum}) {
    BlogFileHeader header;
    header.GenerateRandomFields();
    header.checksum_type = checksum;
    for (const auto& test_case : cases) {
      const std::string record = MakeFooterLocatorRecordWithCompression(
          header, 0, header.incarnation_id(), test_case.declared_length,
          test_case.payload, test_case.prefix_comp, test_case.trailer_comp);
      for (bool decode_output : {false, true}) {
        BlogFileFooterLocator decoded;
        uint64_t offset = 123;
        ASSERT_FALSE(VerifyBlogFooterLocator(
            record.data(), record.size(), 0, header.escape_sequence, checksum,
            header.incarnation_id(), decode_output ? &decoded : nullptr,
            &offset));
        ASSERT_EQ(offset, 123u);
      }
    }
  }
}

TEST_F(BlogFormatTest, FooterPropertiesCompressionTypeBoundaries) {
  const std::string invalid[] = {
      std::string(1, lossless_cast<char>(kStreamingCompressionSentinel)),
      std::string(2, lossless_cast<char>(kZSTD)),
  };
  for (const std::string& value : invalid) {
    BlogFileFooterProperties props;
    props.properties.emplace_back("compressionTypes", value);
    std::string encoded;
    props.EncodeTo(&encoded);
    BlogFileFooterProperties decoded;
    const Status s = decoded.DecodeFrom(encoded);
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  }
  BlogFileFooterProperties props;
  props.properties.emplace_back(
      "compressionTypes",
      std::string(1, lossless_cast<char>(kFirstCustomCompression)));
  std::string encoded;
  props.EncodeTo(&encoded);
  BlogFileFooterProperties decoded;
  ASSERT_OK(decoded.DecodeFrom(encoded));
}

TEST_F(BlogFormatTest, FooterPropertiesNumericValueBoundaries) {
  const char* names[] = {"blobCount",           "blobPayloadBytes",
                         "blobCompressedBytes", "blobUncompressedBytes",
                         "blobOverheadBytes",   "minSequence",
                         "maxSequence"};
  std::string trailing;
  PutPrefixVarint64(&trailing, 42);
  trailing.push_back('x');
  const std::string invalid[] = {std::string(), std::string(1, '\0'), trailing};
  for (const char* name : names) {
    SCOPED_TRACE(name);
    for (const std::string& value : invalid) {
      BlogFileFooterProperties props;
      props.properties.emplace_back(name, value);
      std::string encoded;
      props.EncodeTo(&encoded);
      BlogFileFooterProperties decoded;
      const Status s = decoded.DecodeFrom(encoded);
      ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    }
    std::string value;
    PutPrefixVarint64(&value, UINT64_MAX);
    BlogFileFooterProperties props;
    props.properties.emplace_back(name, value);
    std::string encoded;
    props.EncodeTo(&encoded);
    BlogFileFooterProperties decoded;
    ASSERT_OK(decoded.DecodeFrom(encoded));
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
