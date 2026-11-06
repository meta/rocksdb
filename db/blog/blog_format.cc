//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blog/blog_format.h"

#include <cassert>
#include <cstring>

#include "env/unique_id_gen.h"
#include "options/options_helper.h"
#include "rocksdb/version.h"
#include "table/format.h"
#include "table/unique_id_impl.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/fastrange.h"
#include "util/prefix_varint.h"
#include "util/xxhash.h"

namespace ROCKSDB_NAMESPACE {

namespace {
bool HasValidEscapeSequenceFirstByte(const char* seq) {
  uint8_t first = static_cast<uint8_t>(seq[0]);
  return first != 0x00 && first != 0xFF;
}
}  // namespace

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
    if (name_len == 0) {
      return Status::Corruption("Blog properties: empty property name");
    }
    std::string name(input->data(), name_len);
    input->remove_prefix(name_len);

    uint32_t value_len;
    if (!GetPrefixVarint32(input, &value_len) || input->size() < value_len) {
      return Status::Corruption("Blog properties: truncated property value");
    }
    std::string value(input->data(), value_len);
    input->remove_prefix(value_len);

    // Reject duplicate property names within a single section.
    for (const auto& [existing_name, _] : *props) {
      if (existing_name == name) {
        return Status::Corruption(
            "Blog properties: duplicate property name \"" + name + "\"");
      }
    }

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

  // Encode required properties into a temporary buffer to measure size.
  // Ignorable properties are body-record content and are never encoded here.
  std::string prop_buf;
  EncodeBlogProperties(required_properties, &prop_buf);
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

Status BlogFileHeader::VerifyFixedHeaderChecksum(const char* fixed_header) {
  const char* p = fixed_header;
  ChecksumType type = static_cast<ChecksumType>(p[13]);
  if (!IsSupportedChecksumType(type)) {
    return Status::Corruption("Blog header: unsupported checksum type: " +
                              std::to_string(lossless_cast<int>(p[13])));
  }
  // Blog spec lists only kNoChecksum, kCRC32c, kXXH3 as expected, but we
  // accept any supported checksum type for forward compatibility.
  uint32_t incarnation_id = DecodeFixed32(p + 16);
  uint32_t stored_header_checksum = DecodeFixed32(p + 36);
  uint32_t computed_header_checksum =
      ComputeBuiltinChecksum(type, p, 36) +
      ChecksumModifierForContext(incarnation_id, 0);
  if (stored_header_checksum != computed_header_checksum) {
    char computed_buf[4];
    EncodeFixed32(computed_buf, computed_header_checksum);
    return Status::Corruption(
        "Blog header: fixed header checksum mismatch (stored: 0x" +
        Slice(p + 36, 4).ToString(true) + ", computed: 0x" +
        Slice(computed_buf, 4).ToString(true) + ")");
  }
  return Status::OK();
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
  Status checksum_status = VerifyFixedHeaderChecksum(p);
  if (!checksum_status.ok()) {
    return checksum_status;
  }
  uint32_t prop_section_size =
      DecodeFixed32(p + kBlogFilePropertySectionSizeOffset);

  // Now that checksum is verified, validate the remaining fields.
  schema_version = static_cast<uint8_t>(p[12]);
  if (schema_version == 0) {
    return Status::Corruption(
        "Blog header: schema version 0 is reserved for legacy blob files");
  }
  if (schema_version > kBlogCurrentSchemaVersion) {
    return Status::NotSupported("Blog header: schema version " +
                                std::to_string(schema_version) +
                                " is newer than supported version " +
                                std::to_string(kBlogCurrentSchemaVersion));
  }
  compact_record_type = static_cast<BlogRecordType>(p[14]);
  if (!IsBlogBodyRecordType(compact_record_type)) {
    return Status::Corruption("Blog header: compact_record_type 0x" +
                              Slice(p + 14, 1).ToString(true) +
                              " is not a body record type");
  }
  if (!IsKnownBlogRecordType(compact_record_type)) {
    return Status::NotSupported("Blog header: unknown compact_record_type 0x" +
                                Slice(p + 14, 1).ToString(true));
  }
  flags = static_cast<uint8_t>(p[15]);
  if ((flags & ~lossless_cast<uint8_t>(kBlogFileRecycled)) != 0) {
    return Status::NotSupported("Blog header: unknown flags 0x" +
                                Slice(p + 15, 1).ToString(true));
  }
  // Reserved bytes [26-31] must be zero.
  for (int i = 26; i < 32; ++i) {
    if (p[i] != 0) {
      return Status::NotSupported(
          "Blog header: non-zero reserved byte at offset " + std::to_string(i));
    }
  }

  const bool escape_sequence_valid =
      is_wal() ? HasValidEscapeSequenceFirstByte(escape_sequence)
               : VerifyEscapeSequence(escape_sequence);
  if (!escape_sequence_valid) {
    return Status::Corruption(
        "Blog header: invalid escape sequence (first byte: 0x" +
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
  if (lossless_cast<uint8_t>(prop_comp_type) !=
      lossless_cast<uint8_t>(kNoCompression)) {
    return Status::Corruption(
        "Blog header: property section compression type must be "
        "kNoCompression, "
        "got 0x" +
        Slice(&prop_comp_type, 1).ToString(true));
  }
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
  required_properties.clear();
  // The header section carries no ignorable properties; reset those too so
  // a reused object cannot leak a previous file's merged lookup state.
  ignorable_properties.clear();
  Status s = DecodeBlogProperties(&prop_slice, &required_properties);
  if (!s.ok()) {
    return s;
  }

  for (const auto& [name, value] : required_properties) {
    // Header must contain only required (uppercase) properties; ignorable
    // (lowercase) properties belong in kBlogIgnorablePropertiesRecord.
    // Our own writer can no longer produce such a file (see EncodeTo), so
    // this guards against corrupt or foreign files.
    if (!IsBlogRequiredProperty(name)) {
      return Status::Corruption(
          "Blog header: ignorable property in header: \"" + name + "\"");
    }
    // Check for unrecognized required properties
    if (name != kBlogPropCompressionCompatibilityName &&
        name != kBlogPropTimestampSize && name != kBlogPropPredecessorWalInfo &&
        name != kBlogPropWriteBatchStreamingCompressionType) {
      return Status::NotSupported(
          "Blog header: unrecognized required property: \"" + name + "\"");
    }
    // Validate known required property values.
    if (name == kBlogPropWriteBatchStreamingCompressionType) {
      // Must be exactly 2-digit hex of CompressionType (e.g. "07" for ZSTD).
      if (value.size() != 2) {
        return Status::Corruption(
            "Blog header: WriteBatchStreamingCompressionType must be 2-digit "
            "hex, got size " +
            std::to_string(value.size()));
      }
      std::string decoded_type;
      if (!Slice(value).DecodeHex(&decoded_type)) {
        return Status::Corruption(
            "Blog header: WriteBatchStreamingCompressionType not hex: \"" +
            value + "\"");
      }
      const uint8_t comp = lossless_cast<uint8_t>(decoded_type[0]);
      if (!IsValidBlogCompressionType(comp) ||
          comp == kStreamingCompressionSentinel) {
        return Status::Corruption(
            "Blog header: WriteBatchStreamingCompressionType invalid "
            "compression type 0x" +
            value);
      }
      // Currently only ZSTD is supported for streaming compression.
      if (comp != kZSTD) {
        return Status::NotSupported(
            "Blog header: WriteBatchStreamingCompressionType only ZSTD "
            "(0x07) currently supported, got 0x" +
            value);
      }
    } else if (name == kBlogPropTimestampSize) {
      // Must be a valid prefix-varint64.
      Slice ts_slice(value);
      uint64_t ts_val = 0;
      if (!GetPrefixVarint64(&ts_slice, &ts_val) || !ts_slice.empty()) {
        return Status::Corruption(
            "Blog header: TimestampSize not a valid prefix varint");
      }
    }
  }

  input->remove_prefix(prop_section_size);
  return Status::OK();
}

void BlogFileHeader::SetEscapeSequenceFromU64(uint64_t v, uint64_t seed) {
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
  uint64_t derived =
      XXH3_64bits_withSeed(escape_sequence, kBlogEscapeSeqRandomPartSize, seed);
  EncodeFixed32(escape_sequence + kBlogEscapeSeqRandomPartSize,
                static_cast<uint32_t>(derived));
}

void BlogFileHeader::GenerateFromUniqueId(const std::string& db_id,
                                          const std::string& db_session_id,
                                          uint64_t file_number) {
  GenerateFromUniqueIdWithSeed(db_id, db_session_id, file_number,
                               kBlogEscapeSeqSeed);
}

void BlogFileHeader::GenerateWalFromUniqueId(const std::string& db_id,
                                             const std::string& db_session_id,
                                             uint64_t log_number) {
  GenerateFromUniqueIdWithSeed(db_id, db_session_id, log_number,
                               BlogWalEscapeSeqSeed(log_number));
}

void BlogFileHeader::GenerateFromUniqueIdWithSeed(
    const std::string& db_id, const std::string& db_session_id,
    uint64_t file_number, uint64_t seed) {
  if (db_id.empty() || db_session_id.empty() || file_number == 0) {
    GenerateRandomFieldsWithSeed(seed);
    return;
  }

  // Derive escape sequence from the same unique ID scheme as SST files.
  UniqueId64x2 unique_id;
  Status s =
      GetSstInternalUniqueId(db_id, db_session_id, file_number, &unique_id);
  if (!s.ok()) {
    GenerateRandomFieldsWithSeed(seed);
    return;
  }

  // The internal unique ID is intentionally not fully random/mixed. Mix it
  // so that it is.
  InternalUniqueIdToExternal(&unique_id);

  SetEscapeSequenceFromU64(unique_id[0], seed);

  // Record the IDs as ignorable properties for debugging/tracking.
  SetProperty(kBlogPropDbId, db_id);
  SetProperty(kBlogPropDbSessionId, db_session_id);
}

void BlogFileHeader::GenerateRandomFields() {
  GenerateRandomFieldsWithSeed(kBlogEscapeSeqSeed);
}

void BlogFileHeader::GenerateRandomFieldsWithSeed(uint64_t seed) {
  uint64_t rand_a;
  uint64_t rand_b;
  GenerateRawUniqueId(&rand_a, &rand_b);
  // Use rand_a for escape sequence, rand_b is unused extra entropy.
  (void)rand_b;
  SetEscapeSequenceFromU64(rand_a, seed);
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
  return VerifyEscapeSequenceWithSeed(seq, kBlogEscapeSeqSeed);
}

bool BlogFileHeader::VerifyWalEscapeSequence(const char* seq,
                                             uint64_t log_number) {
  return VerifyEscapeSequenceWithSeed(seq, BlogWalEscapeSeqSeed(log_number));
}

bool BlogFileHeader::VerifyEscapeSequenceWithSeed(const char* seq,
                                                  uint64_t seed) {
  if (!HasValidEscapeSequenceFirstByte(seq)) {
    return false;
  }
  // Verify derived part
  uint64_t derived =
      XXH3_64bits_withSeed(seq, kBlogEscapeSeqRandomPartSize, seed);
  uint32_t expected = static_cast<uint32_t>(derived);
  uint32_t actual = DecodeFixed32(seq + kBlogEscapeSeqRandomPartSize);
  return expected == actual;
}

namespace {
void SetPropertyInMap(BlogPropertyMap* map, const std::string& name,
                      const std::string& value) {
  for (auto& [k, v] : *map) {
    if (k == name) {
      v = value;
      return;
    }
  }
  map->emplace_back(name, value);
}

const std::string* FindPropertyInMaps(const BlogPropertyMap& required,
                                      const BlogPropertyMap& ignorable,
                                      const std::string& name) {
  for (const auto& [k, v] : required) {
    if (k == name) {
      return &v;
    }
  }
  for (const auto& [k, v] : ignorable) {
    if (k == name) {
      return &v;
    }
  }
  return nullptr;
}
}  // namespace

void BlogFileHeader::SetProperty(const std::string& name,
                                 const std::string& value) {
  if (IsBlogRequiredProperty(name)) {
    SetPropertyInMap(&required_properties, name, value);
  } else {
    assert(IsBlogIgnorableProperty(name));
    SetPropertyInMap(&ignorable_properties, name, value);
  }
}

std::string BlogFileHeader::GetProperty(const std::string& name) const {
  const std::string* v =
      FindPropertyInMaps(required_properties, ignorable_properties, name);
  return v != nullptr ? *v : "";
}

bool BlogFileHeader::HasProperty(const std::string& name) const {
  return FindPropertyInMaps(required_properties, ignorable_properties, name) !=
         nullptr;
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
  return GetPrefixVarint64(&s, value) && s.empty();
}

bool BlogFileHeader::GetUint32Property(const std::string& name,
                                       uint32_t* value) const {
  std::string raw = GetProperty(name);
  if (raw.empty()) {
    return false;
  }
  Slice s(raw);
  return GetPrefixVarint32(&s, value) && s.empty();
}

Status BlogFileHeader::DecodeFromBuffer(Slice* buffer,
                                        size_t* additional_bytes_needed) {
  if (buffer->size() < kBlogFileFixedHeaderSize) {
    return Status::Corruption("Blog header: buffer too small for fixed header");
  }
  // Early magic check before trusting prop_section_size.
  if (!IsBlogFormat(buffer->data(), buffer->size())) {
    return Status::Corruption("Blog header: bad magic in DecodeFromBuffer");
  }
  Status checksum_status = VerifyFixedHeaderChecksum(buffer->data());
  if (!checksum_status.ok()) {
    return checksum_status;
  }
  // Check if the full header (fixed + property section) fits in the buffer.
  uint32_t prop_section_size =
      DecodeFixed32(buffer->data() + kBlogFilePropertySectionSizeOffset);
  // Sanity cap: property section should be reasonably small (properties are
  // diagnostic/required metadata). Reject absurd sizes to avoid OOM.
  if (prop_section_size < kBlogBlockTrailerSize ||
      prop_section_size > kBlogMaxPropertySectionSize) {
    return Status::Corruption("Blog header: invalid property section size " +
                              std::to_string(prop_section_size));
  }
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
    // Validate compression type in prefix (must be valid and typically
    // kNoCompression for locator).
    uint8_t prefix_comp = lossless_cast<uint8_t>(varint_end[1]);
    if (!IsValidBlogCompressionType(prefix_comp)) {
      continue;  // invalid compression type
    }
    if (length == 0) {
      continue;  // locator must have payload (empty is invalid)
    }
    if (length == kBlogUnspecifiedSize) {
      continue;  // locator cannot be unspecified-size
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

    // Verify the payload trailer checksum and ensure record ends at EOF.
    {
      size_t after_prefix = after_varint - 6;  // after type+comp+prefix_cksum
      if (after_prefix < kBlogBlockTrailerSize ||
          length > after_prefix - kBlogBlockTrailerSize) {
        continue;  // truncated payload or trailer
      }
      // Locator must be the last record: trailer must end exactly at buffer
      // end.
      if (after_prefix != length + kBlogBlockTrailerSize) {
        continue;  // trailing bytes after locator
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
      if (lossless_cast<uint8_t>(actual_comp) != prefix_comp) {
        continue;  // pre-payload and trailer compression mismatch
      }
      // Payload must be multiple of 5 (entry size).
      if (length % 5 != 0) {
        continue;  // malformed locator payload size
      }
    }

    BlogFileFooterLocator decoded_locator;
    const char* payload = varint_end + 6;
    Status decode_status = decoded_locator.DecodeFrom(
        Slice(payload, lossless_cast<size_t>(length)));
    if (!decode_status.ok()) {
      continue;
    }

    // Valid footer locator record found.
    if (locator_out) {
      *locator_out = std::move(decoded_locator);
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
    BlogRecordType rt =
        lossless_cast<BlogRecordType>(lossless_cast<uint8_t>(s[0]));
    uint32_t rel = DecodeFixed32(s.data() + 1);
    if (!IsBlogFooterRecordType(rt)) {
      return Status::Corruption("Blog footer locator: entry record_type 0x" +
                                Slice(s.data(), 1).ToString(true) +
                                " is not a footer type");
    }
    if (!IsKnownBlogRecordType(rt)) {
      return Status::NotSupported(
          "Blog footer locator: unknown footer record_type 0x" +
          Slice(s.data(), 1).ToString(true));
    }
    if (rt == kBlogFooterLocatorRecord) {
      return Status::Corruption(
          "Blog footer locator: entry must not reference locator itself");
    }
    if (rel == 0) {
      return Status::Corruption(
          "Blog footer locator: relative_offset must be non-zero");
    }
    entries.push_back({.record_type = rt, .relative_offset_4B = rel});
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
  Status st = DecodeBlogProperties(&s, &properties);
  if (!st.ok()) {
    return st;
  }
  // Validate known footer properties.
  for (const auto& [name, value] : properties) {
    if (name == "compressionTypes") {
      // Must be sorted raw bytes, unique, excluding kNoCompression and
      // kStreamingCompressionSentinel, and each must be a valid type.
      for (size_t i = 0; i < value.size(); ++i) {
        uint8_t comp = lossless_cast<uint8_t>(value[i]);
        if (!IsValidBlogCompressionType(comp) || comp == kNoCompression ||
            comp == kStreamingCompressionSentinel) {
          return Status::Corruption(
              "Blog footer properties: compressionTypes contains invalid "
              "type 0x" +
              Slice(value.data() + i, 1).ToString(true));
        }
        if (i > 0 && lossless_cast<uint8_t>(value[i]) <=
                         lossless_cast<uint8_t>(value[i - 1])) {
          return Status::Corruption(
              "Blog footer properties: compressionTypes not sorted/unique");
        }
      }
    } else if (name == "blobCount" || name == "blobPayloadBytes" ||
               name == "blobCompressedBytes" ||
               name == "blobUncompressedBytes" || name == "blobOverheadBytes" ||
               name == "minSequence" || name == "maxSequence") {
      Slice vs(value);
      uint64_t v = 0;
      if (!GetPrefixVarint64(&vs, &v) || !vs.empty()) {
        return Status::Corruption("Blog footer properties: property \"" + name +
                                  "\" not a valid prefix varint");
      }
    } else if (IsBlogRequiredProperty(name)) {
      return Status::NotSupported(
          "Blog footer properties: unrecognized required property \"" + name +
          "\"");
    }
  }
  // Cross-field: minSequence <= maxSequence if both present.
  bool has_min = false, has_max = false;
  uint64_t min_seq = 0, max_seq = 0;
  for (const auto& [name, value] : properties) {
    if (name == "minSequence") {
      Slice vs(value);
      has_min = GetPrefixVarint64(&vs, &min_seq) && vs.empty();
    } else if (name == "maxSequence") {
      Slice vs(value);
      has_max = GetPrefixVarint64(&vs, &max_seq) && vs.empty();
    }
  }
  if (has_min && has_max && min_seq > max_seq) {
    return Status::Corruption(
        "Blog footer properties: minSequence > maxSequence");
  }
  return Status::OK();
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
                                   uint64_t payload_file_offset) {
  uint32_t checksum = ComputeBuiltinChecksumWithLastByte(type, data, data_size,
                                                         compression_type_byte);
  checksum +=
      ChecksumModifierForContext(base_context_checksum, payload_file_offset);
  return checksum;
}

struct BlogRecordChecksumWriter::State {
  // Only the member matching the checksum type passed to Init() is used.
  // Init() always (re)initializes the active member before any Update().
  union {
    uint32_t crc32c;
    XXH32_state_t xxh32;
    XXH64_state_t xxh64;
    XXH3_state_t xxh3;
  };
};

BlogRecordChecksumWriter::BlogRecordChecksumWriter(ChecksumType type,
                                                   uint32_t incarnation_id)
    : type_(type), incarnation_id_(incarnation_id) {}
BlogRecordChecksumWriter::~BlogRecordChecksumWriter() = default;
BlogRecordChecksumWriter::BlogRecordChecksumWriter(BlogRecordChecksumWriter&&) =
    default;
BlogRecordChecksumWriter& BlogRecordChecksumWriter::operator=(
    BlogRecordChecksumWriter&&) = default;

void BlogRecordChecksumWriter::Init(uint64_t payload_file_offset) {
  payload_file_offset_ = payload_file_offset;
  if (state_ == nullptr) {
    state_ = std::make_unique<State>();
  }
  switch (type_) {
    case kCRC32c:
      state_->crc32c = 0;
      break;
    case kxxHash:
      XXH32_reset(&state_->xxh32, 0);
      break;
    case kxxHash64:
      XXH64_reset(&state_->xxh64, 0);
      break;
    case kXXH3:
      XXH3_64bits_reset(&state_->xxh3);
      break;
    default:  // including kNoChecksum
      break;
  }
}

void BlogRecordChecksumWriter::Update(const char* data, size_t size) {
  assert(state_ != nullptr);
  switch (type_) {
    case kCRC32c:
      state_->crc32c = crc32c::Extend(state_->crc32c, data, size);
      break;
    case kxxHash:
      XXH32_update(&state_->xxh32, data, size);
      break;
    case kxxHash64:
      XXH64_update(&state_->xxh64, data, size);
      break;
    case kXXH3: {
      // Update outside assert(): an NDEBUG build must still hash the input.
      const XXH_errorcode rc = XXH3_64bits_update(&state_->xxh3, data, size);
      assert(rc == XXH_OK);
      (void)rc;
      break;
    }
    default:  // including kNoChecksum
      break;
  }
}

uint32_t BlogRecordChecksumWriter::Finalize(char compression_type_byte) {
  assert(state_ != nullptr);
  uint32_t checksum = 0;
  switch (type_) {
    case kCRC32c: {
      const uint32_t crc =
          crc32c::Extend(state_->crc32c, &compression_type_byte, 1);
      checksum = crc32c::Mask(crc);
      break;
    }
    case kxxHash: {
      XXH32_update(&state_->xxh32, &compression_type_byte, 1);
      checksum = XXH32_digest(&state_->xxh32);
      break;
    }
    case kxxHash64: {
      XXH64_update(&state_->xxh64, &compression_type_byte, 1);
      checksum = Lower32of64(XXH64_digest(&state_->xxh64));
      break;
    }
    case kXXH3: {
      // Same custom last-byte handling as
      // ComputeBuiltinChecksumWithLastByte: digest the payload, then mix in
      // the trailing byte. The prime must match table/format.cc; the
      // blog_format_test equivalence test guards it.
      constexpr uint32_t kRandomPrime = 0x6b9083d9;
      checksum = Lower32of64(XXH3_64bits_digest(&state_->xxh3)) ^
                 lossless_cast<uint8_t>(compression_type_byte) * kRandomPrime;
      break;
    }
    default:  // including kNoChecksum
      break;
  }
  return checksum +
         ChecksumModifierForContext(incarnation_id_, payload_file_offset_);
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
  if (!IsValidBlogCompressionType(lossless_cast<uint8_t>(comp_byte))) {
    return Status::Corruption(
        "Blog record trailer: invalid compression type 0x" +
        Slice(&comp_byte, 1).ToString(true));
  }
  uint32_t stored_checksum = DecodeFixed32(trailer + 1);
  uint32_t computed_checksum =
      ComputeBlogRecordChecksum(checksum_type, payload, payload_size, comp_byte,
                                incarnation_id, payload_file_offset);
  if (stored_checksum != computed_checksum) {
    return Status::Corruption("Blog record checksum mismatch");
  }
  if (actual_compression_type) {
    *actual_compression_type =
        lossless_cast<CompressionType>(lossless_cast<uint8_t>(comp_byte));
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
