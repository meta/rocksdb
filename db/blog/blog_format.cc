//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blog/blog_format.h"

#include <array>
#include <cassert>
#include <cstring>

#include "env/unique_id_gen.h"
#include "rocksdb/version.h"
#include "table/format.h"
#include "table/unique_id_impl.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/fastrange.h"
#include "util/prefix_varint.h"
#include "util/xxhash.h"

namespace ROCKSDB_NAMESPACE {

// --- Property encoding/decoding ---

void EncodeBlogProperties(const BlogPropertyMap& props, std::string* dst) {
  for (const auto& [name, value] : props) {
    PutPrefixVarint32(dst, static_cast<uint32_t>(name.size()));
    dst->append(name);
    PutPrefixVarint32(dst, static_cast<uint32_t>(value.size()));
    dst->append(value);
  }
}

Status DecodeBlogProperties(Slice* input, BlogPropertyMap* props) {
  while (input->size() > 0) {
    uint32_t name_len;
    if (!GetPrefixVarint32(input, &name_len) || input->size() < name_len) {
      return Status::Corruption("Blog properties: truncated property name");
    }
    std::string name(input->data(), name_len);
    input->remove_prefix(name_len);

    uint32_t value_len;
    if (!GetPrefixVarint32(input, &value_len) || input->size() < value_len) {
      return Status::Corruption("Blog properties: truncated property value");
    }
    std::string value(input->data(), value_len);
    input->remove_prefix(value_len);

    props->emplace_back(std::move(name), std::move(value));
  }
  return Status::OK();
}

// --- BlogFileHeader ---

uint32_t BlogFileHeader::incarnation_id() const {
  return DecodeFixed32(escape_sequence);
}

void BlogFileHeader::EncodeTo(std::string* dst) const {
  // --- Fixed header (40 bytes) ---
  // Bytes [0-11]: File magic
  dst->append(kBlogFileMagic, kBlogFileMagicSize);
  // Byte [12]: Schema version
  dst->push_back(static_cast<char>(schema_version));
  // Byte [13]: Checksum type
  dst->push_back(static_cast<char>(checksum_type));
  // Byte [14]: Compact record type
  dst->push_back(static_cast<char>(compact_record_type));
  // Byte [15]: Flags
  dst->push_back(static_cast<char>(flags));
  // Bytes [16-25]: Escape sequence
  dst->append(escape_sequence, kBlogEscapeSequenceSize);
  // Bytes [26-31]: Reserved
  for (int i = 0; i < 6; ++i) {
    dst->push_back(0);
  }

  // Encode properties into a temporary buffer to measure size.
  std::string prop_buf;
  EncodeBlogProperties(properties, &prop_buf);
  char prop_comp_type = static_cast<char>(kNoCompression);

  // Build property section: [properties][compression_type][checksum]
  uint32_t prop_checksum = ComputeBuiltinChecksumWithLastByte(
      checksum_type, prop_buf.data(), prop_buf.size(), prop_comp_type);
  prop_checksum +=
      ChecksumModifierForContext(incarnation_id(), kBlogFileFixedHeaderSize);
  std::string prop_section = std::move(prop_buf);
  prop_section.push_back(prop_comp_type);
  PutFixed32(&prop_section, prop_checksum);

  // Bytes [32-35]: Property section size (trailer included, no padding)
  PutFixed32(dst, static_cast<uint32_t>(prop_section.size()));

  // Bytes [36-39]: Fixed header checksum (covers bytes [0, 36))
  // Uses context checksum with incarnation_id at offset 0 for
  // defense-in-depth (misplaced data detection).
  assert(dst->size() == 36);
  uint32_t header_checksum =
      ComputeBuiltinChecksum(checksum_type, dst->data(), dst->size()) +
      ChecksumModifierForContext(incarnation_id(), 0);
  PutFixed32(dst, header_checksum);

  // Property section follows the fixed header
  assert(dst->size() == kBlogFileFixedHeaderSize);
  dst->append(prop_section);
}

Status BlogFileHeader::DecodeFrom(Slice* input) {
  if (input->size() < kBlogFileFixedHeaderSize) {
    return Status::Corruption("Blog header too short: have " +
                              std::to_string(input->size()) + " bytes, need " +
                              std::to_string(kBlogFileFixedHeaderSize));
  }

  const char* p = input->data();

  // Verify magic
  if (memcmp(p, kBlogFileMagic, kBlogFileMagicSize) != 0) {
    return Status::Corruption("Blog header: bad magic (first 4 bytes: 0x" +
                              Slice(p, 4).ToString(true) + ")");
  }

  // Parse fields needed for checksum computation before verifying.
  checksum_type = static_cast<ChecksumType>(p[13]);
  memcpy(escape_sequence, p + 16, kBlogEscapeSequenceSize);

  // Verify fixed header checksum FIRST, before trusting any other fields.
  uint32_t prop_section_size = DecodeFixed32(p + 32);
  uint32_t stored_header_checksum = DecodeFixed32(p + 36);
  uint32_t computed_header_checksum =
      ComputeBuiltinChecksum(checksum_type, p, 36) +
      ChecksumModifierForContext(incarnation_id(), 0);
  if (stored_header_checksum != computed_header_checksum) {
    char computed_buf[4];
    EncodeFixed32(computed_buf, computed_header_checksum);
    return Status::Corruption(
        "Blog header: fixed header checksum mismatch (stored: 0x" +
        Slice(p + 36, 4).ToString(true) + ", computed: 0x" +
        Slice(computed_buf, 4).ToString(true) + ")");
  }

  // Now that checksum is verified, validate the remaining fields.
  schema_version = static_cast<uint8_t>(p[12]);
  if (schema_version > kBlogCurrentSchemaVersion) {
    return Status::NotSupported("Blog header: schema version " +
                                std::to_string(schema_version) +
                                " is newer than supported version " +
                                std::to_string(kBlogCurrentSchemaVersion));
  }
  compact_record_type = static_cast<BlogRecordType>(p[14]);
  flags = static_cast<uint8_t>(p[15]);

  if (!VerifyEscapeSequence(escape_sequence)) {
    return Status::Corruption(
        "Blog header: escape sequence derived bytes mismatch (first byte: 0x" +
        Slice(escape_sequence, 1).ToString(true) + ")");
  }

  // Advance past fixed header
  input->remove_prefix(kBlogFileFixedHeaderSize);

  // Read property section
  if (input->size() < prop_section_size) {
    return Status::Corruption("Blog header: truncated property section (have " +
                              std::to_string(input->size()) + " bytes, need " +
                              std::to_string(prop_section_size) + ")");
  }

  if (prop_section_size < kBlogBlockTrailerSize) {
    return Status::Corruption("Blog header: property section too small (" +
                              std::to_string(prop_section_size) +
                              " bytes, min " +
                              std::to_string(kBlogBlockTrailerSize) + ")");
  }

  // Property section layout: [properties][comp_type:1B][checksum:4B]
  const char* prop_data = input->data();
  size_t prop_data_size = prop_section_size - kBlogBlockTrailerSize;
  char prop_comp_type = prop_data[prop_data_size];
  uint32_t stored_prop_checksum = DecodeFixed32(prop_data + prop_data_size + 1);
  uint32_t computed_prop_checksum = ComputeBuiltinChecksumWithLastByte(
      checksum_type, prop_data, prop_data_size, prop_comp_type);
  computed_prop_checksum +=
      ChecksumModifierForContext(incarnation_id(), kBlogFileFixedHeaderSize);
  if (stored_prop_checksum != computed_prop_checksum) {
    return Status::Corruption(
        "Blog header: property section checksum mismatch (prop_size=" +
        std::to_string(prop_data_size) + ", comp_type=0x" +
        Slice(&prop_comp_type, 1).ToString(true) + ")");
  }

  // Decode properties
  Slice prop_slice(prop_data, prop_data_size);
  properties.clear();
  Status s = DecodeBlogProperties(&prop_slice, &properties);
  if (!s.ok()) {
    return s;
  }

  // Check for unrecognized required properties
  for (const auto& [name, value] : properties) {
    if (IsBlogRequiredProperty(name)) {
      if (name != kBlogPropCompressionCompatibilityName &&
          name != kBlogPropTimestampSize &&
          name != kBlogPropPredecessorWalInfo &&
          name != kBlogPropWriteBatchStreamingCompressionType) {
        return Status::NotSupported(
            "Blog header: unrecognized required property: \"" + name + "\"");
      }
    }
  }

  input->remove_prefix(prop_section_size);
  return Status::OK();
}

void BlogFileHeader::SetEscapeSequenceFromU64(uint64_t v) {
  // Byte 0: derived from top 4 bytes via FastRange32 into [1, 254],
  // avoiding 0x00 and 0xFF (which are padding bytes).
  uint32_t top = static_cast<uint32_t>(v >> 32);
  escape_sequence[0] = static_cast<char>(FastRange32(top, 254) + 1);
  assert(escape_sequence[0] != '\0');
  assert(escape_sequence[0] != '\xFF');

  // Bytes 1-5: lowest 5 bytes as-is (little-endian).
  EncodeFixed32(escape_sequence + 1, static_cast<uint32_t>(v));
  escape_sequence[5] = static_cast<char>(v >> 32);
  // (byte at index 5 overlaps with top byte used for FastRange32, but
  // that's fine -- it's just input entropy, not the derived byte 0)

  // Bytes 6-9: XXH3-derived suffix for heuristic detection. This would enable
  // us to scan through a file missing its header and, with some hashing effort,
  // detect escape sequences with high accuracy (expect 1 false positive per
  // ~16GB; further validation/pruning can be done trying to parse records
  // and/or picking the most common candidate escape sequence).
  uint64_t derived = XXH3_64bits_withSeed(
      escape_sequence, kBlogEscapeSeqRandomPartSize, kBlogEscapeSeqSeed);
  EncodeFixed32(escape_sequence + kBlogEscapeSeqRandomPartSize,
                static_cast<uint32_t>(derived));
}

void BlogFileHeader::GenerateFromUniqueId(const std::string& db_id,
                                          const std::string& db_session_id,
                                          uint64_t file_number) {
  if (db_id.empty() || db_session_id.empty() || file_number == 0) {
    GenerateRandomFields();
    return;
  }

  // Derive escape sequence from the same unique ID scheme as SST files.
  UniqueId64x2 unique_id;
  Status s =
      GetSstInternalUniqueId(db_id, db_session_id, file_number, &unique_id);
  if (!s.ok()) {
    GenerateRandomFields();
    return;
  }

  // The internal unique ID is intentionally not fully random/mixed. Mix it
  // so that it is.
  InternalUniqueIdToExternal(&unique_id);

  SetEscapeSequenceFromU64(unique_id[0]);

  // Store the inputs as ignorable header properties for debugging/tracking.
  SetProperty(kBlogPropDbId, db_id);
  SetProperty(kBlogPropDbSessionId, db_session_id);
  SetUint64Property(kBlogPropFileNumber, file_number);
}

void BlogFileHeader::GenerateRandomFields() {
  uint64_t rand_a;
  uint64_t rand_b;
  GenerateRawUniqueId(&rand_a, &rand_b);
  // Use rand_a for escape sequence, rand_b is unused extra entropy.
  (void)rand_b;
  SetEscapeSequenceFromU64(rand_a);
}

void BlogFileHeader::SetDiagnosticProperties(
    const std::string& db_host_id, uint32_t column_family_id,
    const std::string& column_family_name, uint64_t creation_time) {
  if (!db_host_id.empty()) {
    SetProperty(kBlogPropDbHostId, db_host_id);
  }
  if (column_family_id != kNoCfId) {
    SetUint32Property(kBlogPropColumnFamilyId, column_family_id);
  }
  if (!column_family_name.empty()) {
    SetProperty(kBlogPropColumnFamilyName, column_family_name);
  }
  if (creation_time > 0) {
    SetUint64Property(kBlogPropCreationTime, creation_time);
  }
  SetProperty(kBlogPropCreator, GetRocksVersionAsString());
}

bool BlogFileHeader::VerifyEscapeSequence(const char* seq) {
  uint8_t first = static_cast<uint8_t>(seq[0]);
  if (first == 0x00 || first == 0xFF) {
    return false;
  }
  // Verify derived part
  uint64_t derived = XXH3_64bits_withSeed(seq, kBlogEscapeSeqRandomPartSize,
                                          kBlogEscapeSeqSeed);
  uint32_t expected = static_cast<uint32_t>(derived);
  uint32_t actual = DecodeFixed32(seq + kBlogEscapeSeqRandomPartSize);
  return expected == actual;
}

void BlogFileHeader::SetProperty(const std::string& name,
                                 const std::string& value) {
  for (auto& [k, v] : properties) {
    if (k == name) {
      v = value;
      return;
    }
  }
  properties.emplace_back(name, value);
}

std::string BlogFileHeader::GetProperty(const std::string& name) const {
  for (const auto& [k, v] : properties) {
    if (k == name) {
      return v;
    }
  }
  return "";
}

bool BlogFileHeader::HasProperty(const std::string& name) const {
  for (const auto& [k, v] : properties) {
    if (k == name) {
      return true;
    }
  }
  return false;
}

void BlogFileHeader::SetUint64Property(const std::string& name,
                                       uint64_t value) {
  std::string encoded;
  PutPrefixVarint64(&encoded, value);
  SetProperty(name, encoded);
}

void BlogFileHeader::SetUint32Property(const std::string& name,
                                       uint32_t value) {
  std::string encoded;
  PutPrefixVarint32(&encoded, value);
  SetProperty(name, encoded);
}

bool BlogFileHeader::GetUint64Property(const std::string& name,
                                       uint64_t* value) const {
  std::string raw = GetProperty(name);
  if (raw.empty()) {
    return false;
  }
  Slice s(raw);
  return GetPrefixVarint64(&s, value);
}

bool BlogFileHeader::GetUint32Property(const std::string& name,
                                       uint32_t* value) const {
  std::string raw = GetProperty(name);
  if (raw.empty()) {
    return false;
  }
  Slice s(raw);
  return GetPrefixVarint32(&s, value);
}

Status BlogFileHeader::DecodeFromBuffer(Slice* buffer,
                                        size_t* additional_bytes_needed) {
  if (buffer->size() < kBlogFileFixedHeaderSize) {
    return Status::Corruption("Blog header: buffer too small for fixed header");
  }

  // Check if the full header (fixed + property section) fits in the buffer.
  uint32_t prop_section_size = DecodeFixed32(buffer->data() + 32);
  size_t total_header_size = kBlogFileFixedHeaderSize + prop_section_size;

  if (buffer->size() < total_header_size) {
    if (additional_bytes_needed) {
      *additional_bytes_needed = total_header_size - buffer->size();
    }
    return Status::Incomplete(
        "Blog header: need more data for property section",
        std::to_string(total_header_size));
  }

  // Full header is available. Decode it.
  Slice header_slice(buffer->data(), total_header_size);
  Status s = DecodeFrom(&header_slice);
  if (s.ok()) {
    buffer->remove_prefix(total_header_size);
  }
  return s;
}

// --- Footer locator verification ---

bool VerifyBlogFooterLocator(const char* buffer, size_t buffer_size,
                             uint64_t buffer_file_offset,
                             const char* expected_escape_seq,
                             ChecksumType checksum_type,
                             uint32_t incarnation_id,
                             BlogFileFooterLocator* locator_out,
                             uint64_t* locator_file_offset_out) {
  // Scan backward in 4-byte steps for the escape sequence.
  // Buffer must start at a 4-byte-aligned file offset. Start from the last
  // 4-byte-aligned position in the buffer (buffer_size may not be a multiple
  // of 4 because the footer locator record is not padded).
  for (size_t pos = buffer_size & ~size_t{3}; pos >= 4; pos -= 4) {
    size_t esc_start = pos - 4;
    uint8_t first_byte = static_cast<uint8_t>(buffer[esc_start]);
    if (first_byte == 0x00 || first_byte == 0xFF) {
      continue;  // padding byte
    }
    // Check if this is our escape sequence
    if (esc_start + kBlogEscapeSequenceSize > buffer_size ||
        memcmp(buffer + esc_start, expected_escape_seq,
               kBlogEscapeSequenceSize) != 0) {
      continue;  // not our escape sequence
    }

    // Found escape sequence. Parse the full-format record that follows.
    // Full format: [escape_seq:10B] [varint length:3+B] [type:1B]
    //              [compression_type:1B] [prefix_checksum:4B]
    //              [payload:length B] [comp:1B] [checksum:4B]
    const char* rec = buffer + esc_start + kBlogEscapeSequenceSize;
    size_t remaining = buffer_size - esc_start - kBlogEscapeSequenceSize;

    // Parse prefix varint length (must be >= 3 bytes for full format)
    uint64_t length = 0;
    const char* varint_end =
        GetPrefixVarint64Ptr(rec, rec + remaining, &length);
    if (!varint_end) {
      continue;  // truncated varint
    }
    size_t varint_len = static_cast<size_t>(varint_end - rec);
    if (varint_len <= kBlogCompactVarintMaxBytes) {
      continue;  // not full format
    }

    // Need type(1) + comp(1) + prefix_checksum(4) after varint
    size_t after_varint = remaining - varint_len;
    if (after_varint < 6) {
      continue;  // truncated prefix
    }

    BlogRecordType type =
        static_cast<BlogRecordType>(static_cast<uint8_t>(varint_end[0]));
    if (type != kBlogFooterLocatorRecord) {
      continue;  // not a footer locator
    }

    // Verify prefix checksum over (varint + type + compression_type)
    size_t prefix_data_size = varint_len + 2;  // varint + type + comp
    uint64_t esc_file_offset = buffer_file_offset + esc_start;
    uint32_t stored_prefix_checksum = DecodeFixed32(varint_end + 2);
    uint32_t computed_prefix_checksum =
        ComputeBuiltinChecksum(checksum_type, rec, prefix_data_size) +
        ChecksumModifierForContext(incarnation_id, esc_file_offset);
    if (stored_prefix_checksum != computed_prefix_checksum) {
      continue;  // prefix checksum mismatch
    }

    // If length > 0, verify the payload trailer checksum
    if (length > 0) {
      size_t after_prefix = after_varint - 6;  // after type+comp+prefix_cksum
      if (after_prefix < length + kBlogBlockTrailerSize) {
        continue;  // truncated payload or trailer
      }
      const char* payload = varint_end + 6;
      uint64_t payload_file_offset = buffer_file_offset + esc_start +
                                     kBlogEscapeSequenceSize + varint_len + 6;
      CompressionType actual_comp;
      Status s = VerifyBlogRecordTrailer(
          checksum_type, payload, static_cast<size_t>(length), incarnation_id,
          payload_file_offset, &actual_comp);
      if (!s.ok()) {
        continue;  // trailer checksum mismatch
      }
    }

    // Valid footer locator record found.
    if (locator_out && length > 0) {
      const char* payload = varint_end + 6;
      if (!locator_out->DecodeFrom(Slice(payload, static_cast<size_t>(length)))
               .ok()) {
        continue;  // malformed locator payload
      }
    }
    if (locator_file_offset_out) {
      *locator_file_offset_out = buffer_file_offset + esc_start;
    }
    return true;
  }
  return false;
}

// --- BlogFileFooterLocator ---

void BlogFileFooterLocator::EncodeTo(std::string* dst) const {
  for (const auto& entry : entries) {
    dst->push_back(static_cast<char>(entry.record_type));
    PutFixed32(dst, entry.relative_offset_4B);
  }
}

Status BlogFileFooterLocator::DecodeFrom(const Slice& input) {
  Slice s = input;
  entries.clear();
  while (s.size() >= 5) {  // 1 byte type + 4 bytes offset
    entries.push_back(
        {.record_type = static_cast<BlogRecordType>(static_cast<uint8_t>(s[0])),
         .relative_offset_4B = DecodeFixed32(s.data() + 1)});
    s.remove_prefix(5);
  }
  if (s.size() != 0) {
    return Status::Corruption(
        "Blog footer locator: trailing bytes after entries");
  }
  return Status::OK();
}

// --- BlogFileFooterProperties ---

void BlogFileFooterProperties::SetBlobCount(uint64_t count) {
  std::string val;
  PutPrefixVarint64(&val, count);
  properties.emplace_back("blobCount", std::move(val));
}

void BlogFileFooterProperties::SetBlobPayloadBytes(uint64_t bytes) {
  std::string val;
  PutPrefixVarint64(&val, bytes);
  properties.emplace_back("blobPayloadBytes", std::move(val));
}

void BlogFileFooterProperties::SetBlobCompressedBytes(uint64_t bytes) {
  std::string val;
  PutPrefixVarint64(&val, bytes);
  properties.emplace_back("blobCompressedBytes", std::move(val));
}

void BlogFileFooterProperties::SetBlobUncompressedBytes(uint64_t bytes) {
  std::string val;
  PutPrefixVarint64(&val, bytes);
  properties.emplace_back("blobUncompressedBytes", std::move(val));
}

void BlogFileFooterProperties::SetBlobOverheadBytes(uint64_t bytes) {
  std::string val;
  PutPrefixVarint64(&val, bytes);
  properties.emplace_back("blobOverheadBytes", std::move(val));
}

void BlogFileFooterProperties::SetCompressionTypes(
    const SmallEnumSet<CompressionType, kDisableCompressionOption>& types) {
  std::string val;
  for (CompressionType t : types) {
    val.push_back(static_cast<char>(t));
  }
  properties.emplace_back("compressionTypes", std::move(val));
}

void BlogFileFooterProperties::SetSequenceRange(uint64_t min_seq,
                                                uint64_t max_seq) {
  std::string min_val;
  PutPrefixVarint64(&min_val, min_seq);
  properties.emplace_back("minSequence", std::move(min_val));
  std::string max_val;
  PutPrefixVarint64(&max_val, max_seq);
  properties.emplace_back("maxSequence", std::move(max_val));
}

void BlogFileFooterProperties::EncodeTo(std::string* dst) const {
  EncodeBlogProperties(properties, dst);
}

Status BlogFileFooterProperties::DecodeFrom(const Slice& input) {
  Slice s = input;
  properties.clear();
  return DecodeBlogProperties(&s, &properties);
}

// --- Padding ---

uint32_t ComputeBlogPaddingLength(uint8_t last_meaningful_byte,
                                  uint64_t current_offset, uint32_t alignment) {
  assert(alignment >= kBlogMinRecordAlignment);
  assert((alignment & (alignment - 1)) == 0);

  const uint64_t count64 =
      BitwiseAnd(uint64_t{0} - current_offset, uint64_t{alignment - 1});
  assert(count64 < alignment);
  uint32_t count = static_cast<uint32_t>(count64);
  if (count == 0 &&
      (last_meaningful_byte == 0x00 || last_meaningful_byte == 0xFF)) {
    count = alignment;
  }
  return count;
}

uint8_t ChooseBlogPaddingByte(uint8_t last_meaningful_byte,
                              bool prefer_0xff_when_legal) {
  if (last_meaningful_byte == 0x00) {
    return 0xFF;
  }
  if (last_meaningful_byte == 0xFF) {
    return 0x00;
  }
  return prefer_0xff_when_legal ? 0xFF : 0x00;
}

bool IsValidBlogPadding(uint8_t last_meaningful_byte, uint64_t current_offset,
                        uint32_t count, uint8_t pad_byte) {
  if (count == 0) {
    return true;
  }

  if (pad_byte != 0x00 && pad_byte != 0xFF) {
    return false;
  }

  if (last_meaningful_byte == 0x00 && pad_byte != 0xFF) {
    return false;
  }
  if (last_meaningful_byte == 0xFF && pad_byte != 0x00) {
    return false;
  }

  static constexpr std::array<uint32_t, 3> kAllowedAlignments{
      kBlogMinRecordAlignment, kBlogFlushFriendlyAlignment,
      kBlogPageFriendlyAlignment};
  for (uint32_t alignment : kAllowedAlignments) {
    if (count == ComputeBlogPaddingLength(last_meaningful_byte, current_offset,
                                          alignment)) {
      return true;
    }
  }
  return false;
}

BlogPadding::BlogPadding(uint8_t last_meaningful_byte, size_t current_offset,
                         uint32_t alignment, bool prefer_0xff_when_legal) {
  count = ComputeBlogPaddingLength(last_meaningful_byte,
                                   uint64_t{current_offset}, alignment);
  if (count > 0) {
    byte = lossless_cast<char>(
        ChooseBlogPaddingByte(last_meaningful_byte, prefer_0xff_when_legal));
  }
}

// --- Checksum ---

uint32_t ComputeBlogRecordChecksum(ChecksumType type, const char* data,
                                   size_t data_size, char compression_type_byte,
                                   uint32_t base_context_checksum,
                                   uint64_t record_offset) {
  uint32_t checksum = ComputeBuiltinChecksumWithLastByte(type, data, data_size,
                                                         compression_type_byte);
  checksum += ChecksumModifierForContext(base_context_checksum, record_offset);
  return checksum;
}

// --- Record trailer verification ---

Status VerifyBlogRecordTrailer(ChecksumType checksum_type, const char* payload,
                               size_t payload_size, uint32_t incarnation_id,
                               uint64_t payload_file_offset,
                               CompressionType* actual_compression_type) {
  // Trailer is the 5 bytes immediately after the payload:
  // [compression_type: 1B][checksum: 4B]
  const char* trailer = payload + payload_size;
  char comp_byte = trailer[0];
  uint32_t stored_checksum = DecodeFixed32(trailer + 1);
  uint32_t computed_checksum =
      ComputeBlogRecordChecksum(checksum_type, payload, payload_size, comp_byte,
                                incarnation_id, payload_file_offset);
  if (stored_checksum != computed_checksum) {
    return Status::Corruption("Blog record checksum mismatch");
  }
  if (actual_compression_type) {
    *actual_compression_type =
        static_cast<CompressionType>(static_cast<uint8_t>(comp_byte));
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
