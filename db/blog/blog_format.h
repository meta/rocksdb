//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <algorithm>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/compression_type.h"
#include "rocksdb/data_structure.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "util/cast_util.h"
#include "util/prefix_varint.h"

namespace ROCKSDB_NAMESPACE {

// Record types used in blog files.
// Types 0x01-0x0F are blog meta records (may appear in header, body, or
// footer regions). Types 0x10-0x7F are body records. Types 0xF0-0xFF are
// footer records (in expected order of occurrence, with gaps for future
// additions).
enum BlogRecordType : uint8_t {
  // --- Blog meta records (0x01-0x0F) ---
  kBlogIgnorablePropertiesRecord = 0x01,  // ignorable named properties
                                          // (diagnostic/debugging data)
  // 0x02-0x0F: reserved for future blog meta record types

  // --- Body records (0x10-0x7F) ---
  kBlogBlobRecord = 0x10,
  kBlogPreambleStartRecord = 0x11,
  kBlogWriteBatchRecord = 0x12,
  kBlogManifestRecord = 0x13,  // manifest (version edit) record (placeholder)
  // 0x14-0x7F: reserved for future body record types

  // --- Footer records (0xF0-0xFF) ---
  kBlogFooterIndexRecord = 0xF0,  // sparse index or similar large metadata
  // 0xF1-0xF3: reserved for future footer data records
  kBlogFooterPropertiesRecord = 0xF8,  // small metadata (named properties)
  // 0xF9-0xFB: reserved
  kBlogFooterFileChecksumInfo = 0xFC,  // full file checksum state dump, so
                                       // the file checksum can be inferred
                                       // from just reading the tail
  // 0xFD-0xFE: reserved
  kBlogFooterLocatorRecord = 0xFF,  // relative offsets to other footer
                                    // records; must be last
};

// --- Constants ---

// 12-byte file magic: "Blog" (ASCII) followed by XXH3_64bits("Blog", 4).
// Used as an opaque identifier for blog format files.
static constexpr char kBlogFileMagic[12] = {
    '\x42', '\x6C', '\x6F', '\x67',                                 // "Blog"
    '\x9F', '\xFC', '\x41', '\x52', '\xF7', '\xF2', '\x2C', '\x49'  // XXH3_64
};
static constexpr size_t kBlogFileMagicSize = 12;

// Fixed header size in bytes (before property section).
static constexpr size_t kBlogFileFixedHeaderSize = 40;

// Escape sequence: 6 random bytes + 4 derived bytes.
static constexpr size_t kBlogEscapeSequenceSize = 10;
static constexpr size_t kBlogEscapeSeqRandomPartSize = 6;

// Seed for deriving bytes [6-9] of escape sequence via
// XXH3_64bits_withSeed(random_6_bytes, 6, seed), where seed is
// kBlogEscapeSeqSeed for non-WAL files and BlogWalEscapeSeqSeed(log_number)
// for WAL files. Chosen arbitrarily; must never change once the format is
// released.
static constexpr uint64_t kBlogEscapeSeqSeed =
    0x52636B73426C6F67ULL;  // "RcksBlg\0"

inline uint64_t BlogWalEscapeSeqSeed(uint64_t log_number) {
  return kBlogEscapeSeqSeed + log_number;
}

// Sentinel for unspecified-size records. Encodes to 9 bytes in prefix varint,
// which always triggers the full record format.
static constexpr uint64_t kBlogUnspecifiedSize = (uint64_t{1} << 63) - 1;

// Maximum payload size that encodes to a compact-format prefix varint
// (<= 2 bytes). Prefix varint of 16383 uses 2 bytes; 16384 uses 3 bytes.
// Records larger than this get full format with prefix checksum protection.
static constexpr uint64_t kBlogMaxCompactPayloadSize = (1u << 14) - 1;

// Record format is determined by the prefix-varint-encoded length field:
//   - value=0 (1 byte): Trivial format (type + checksum, no payload)
//   - value 1-16383 (1-2 bytes): Compact format (payload + trailer, no type)
//   - 3+ byte prefix varint: Full format (type + comp + prefix_checksum +
//     payload + trailer)
// Compact format prefix varint threshold: varints of this many bytes or
// fewer use compact format (no type byte, no prefix checksum).
static constexpr size_t kBlogCompactVarintMaxBytes = 2;

// Block trailer size: 1 byte compression_type + 4 bytes checksum.
static constexpr size_t kBlogBlockTrailerSize = 5;

// Named property sections are intentionally bounded to prevent unreasonable
// allocations while opening a file. Includes the enclosing block trailer.
static constexpr uint32_t kBlogMaxPropertySectionSize = 16 * 1024 * 1024;

// Writers always keep record starts 4-byte aligned. They may optionally pad
// farther to stronger alignments for flush/sync friendliness.
static constexpr uint32_t kBlogMinRecordAlignment = 4;
static constexpr uint32_t kBlogFlushFriendlyAlignment = 512;
static constexpr uint32_t kBlogPageFriendlyAlignment = 4096;

// Compute the canonical on-disk size of a blog record (framing + payload +
// trailer + 4-byte-alignment padding) given the payload size and whether
// compact format is available. Test-only noncanonical encodings can be larger;
// exact write-time accounting for those comes from BlogFileWriter.
inline uint64_t ComputeBlogRecordSize(uint64_t payload_size,
                                      bool compact_eligible) {
  uint64_t size_before_padding;
  if (payload_size == 0) {
    // Trivial: escape_seq(10) + varint(1) + type(1) + cksum(4) = 16
    return 16;  // already 4-byte aligned
  } else if (compact_eligible && payload_size <= kBlogMaxCompactPayloadSize) {
    // Compact: escape_seq(10) + prefix_varint(1-2) + payload + trailer(5)
    size_before_padding =
        payload_size + PrefixVarint64Length(payload_size) + 15;
  } else {
    // Full: escape_seq(10) + prefix_varint(V) + type(1) + comp(1) +
    // prefix_cksum(4) + payload + trailer(5). V >= 3.
    uint64_t varint_len =
        std::max(uint32_t{3}, PrefixVarint64Length(payload_size));
    size_before_padding = payload_size + varint_len + 21;
  }
  // Pad to 4-byte alignment
  return (size_before_padding + 3) & ~uint64_t{3};
}

// Returns whether a record payload of `payload_size` bytes starting at file
// offset `payload_file_offset` fits inside a blog file of `file_size` bytes,
// accounting for the 5-byte trailer that always follows a payload on disk.
//
// Random-access readers call this before issuing a read so that an offset
// outside the file they opened is reported as corruption rather than reaching
// the file layer. That distinction matters: a reader holding a stale, shorter
// view of a file that is still being appended to (direct-write blob files) is
// recovered by reopening it, and that recovery is keyed on Corruption.
//
// This is a bound, not a framing check: it does not know where records
// actually begin, so it can accept an offset that no record starts at. The
// record trailer checksum remains the authority on payload validity.
inline bool IsBlogPayloadWithinFile(uint64_t payload_file_offset,
                                    uint64_t payload_size, uint64_t file_size) {
  // A payload cannot begin before the fixed header, the property section that
  // always follows it (at minimum its own 5-byte trailer), and the shortest
  // possible record prefix (escape sequence + a one-byte length varint).
  constexpr uint64_t kMinPayloadOffset = kBlogFileFixedHeaderSize +
                                         kBlogBlockTrailerSize +
                                         kBlogEscapeSequenceSize + 1;
  if (payload_file_offset < kMinPayloadOffset) {
    return false;
  }
  if (file_size < kBlogBlockTrailerSize) {
    return false;
  }
  // The payload and its trailer must both be inside the file. Written without
  // addition so a hostile payload_size cannot overflow.
  const uint64_t max_payload_end = file_size - kBlogBlockTrailerSize;
  if (payload_size > max_payload_end) {
    return false;
  }
  return payload_file_offset <= max_payload_end - payload_size;
}

// Estimated footer size for file-rolling decisions. A typical footer contains
// a properties record (~80-100 bytes of varint-encoded stats) and a locator
// record (~29 bytes), plus framing overhead. This is an approximation; the
// actual size depends on how many properties are populated.
static constexpr size_t kBlogEstimatedFooterSize = 128;

// kStreamingCompressionSentinel (0x7F) is defined in compression_type.h
// and used in blog record trailers to indicate streaming compression.
// A record with kNoCompression in the trailer remains individually
// uncompressed even when streaming is active (dynamic on/off).

// --- Named Properties ---

// Named property: string key -> string value.
// Case convention: Uppercase first letter = required (reader must recognize
// or reject the file). Lowercase first letter = ignorable.
using BlogPropertyMap = std::vector<std::pair<std::string, std::string>>;

// Encode properties as a sequence of length-prefixed name/value pairs.
// No count prefix; section is terminated by reaching the end of the slice.
void EncodeBlogProperties(const BlogPropertyMap& props, std::string* dst);

// Decode properties from a slice, consuming all bytes.
Status DecodeBlogProperties(Slice* input, BlogPropertyMap* props);

// Returns true if a property name starts with an uppercase letter,
// meaning the reader must recognize it or reject the file.
inline bool IsBlogRequiredProperty(const Slice& name) {
  return name.size() > 0 && name[0] >= 'A' && name[0] <= 'Z';
}

// Returns true if a property name starts with a lowercase ASCII letter,
// meaning the reader may ignore an unrecognized property.
inline bool IsBlogIgnorableProperty(const Slice& name) {
  return name.size() > 0 && name[0] >= 'a' && name[0] <= 'z';
}

// Returns true if the given record type is a known (non-reserved) blog
// record type per the spec table.
inline bool IsKnownBlogRecordType(BlogRecordType type) {
  switch (type) {
    case kBlogIgnorablePropertiesRecord:
    case kBlogBlobRecord:
    case kBlogPreambleStartRecord:
    case kBlogWriteBatchRecord:
    case kBlogManifestRecord:
    case kBlogFooterIndexRecord:
    case kBlogFooterPropertiesRecord:
    case kBlogFooterFileChecksumInfo:
    case kBlogFooterLocatorRecord:
      return true;
    default:
      return false;
  }
}

// Returns true if the record type is a body record (0x10-0x7F).
inline bool IsBlogBodyRecordType(BlogRecordType type) {
  uint8_t v = lossless_cast<uint8_t>(type);
  return v >= 0x10 && v <= 0x7F;
}

// Returns true if the record type is a footer record (0xF0-0xFF).
inline bool IsBlogFooterRecordType(BlogRecordType type) {
  uint8_t v = lossless_cast<uint8_t>(type);
  return v >= 0xF0;
}

// Returns true if the compression type byte is a valid value for a blog
// record trailer (builtin, custom, sentinel; excludes reserved and
// kDisableCompressionOption).
inline bool IsValidBlogCompressionType(uint8_t comp) {
  if (comp <= kLastBuiltinCompression) {
    return true;
  }
  if (comp == kStreamingCompressionSentinel) {
    return true;
  }
  if (comp >= kFirstCustomCompression && comp <= kLastCustomCompression) {
    return true;
  }
  return false;
}

// Known property names. Required properties (uppercase first letter) live in
// the physical header property section; ignorable properties (lowercase
// first letter) travel in kBlogIgnorablePropertiesRecord body records and
// are never part of the header section.
static constexpr const char* kBlogPropCompressionCompatibilityName =
    "CompressionCompatibilityName";
static constexpr const char* kBlogPropTimestampSize = "TimestampSize";
static constexpr const char* kBlogPropPredecessorWalInfo = "PredecessorWalInfo";
// Required (uppercase) when streaming compression is active for WriteBatch
// records. Value is a 2-digit hex encoding of the CompressionType used
// (e.g. "07" for kZSTD). The presence of this property means WriteBatch
// record payloads with kStreamingCompressionSentinel in their trailer
// are part of a streaming compression context that spans across records.
static constexpr const char* kBlogPropWriteBatchStreamingCompressionType =
    "WriteBatchStreamingCompressionType";
static constexpr const char* kBlogPropRole = "role";
static constexpr const char* kBlogPropCompressionSettings =
    "compressionSettings";
static constexpr const char* kBlogPropCreationTime = "creationTime";
static constexpr const char* kBlogPropCreator = "creator";
// Ignorable properties for file identity and diagnostics.
static constexpr const char* kBlogPropDbId = "dbId";
static constexpr const char* kBlogPropDbSessionId = "dbSessionId";
static constexpr const char* kBlogPropDbHostId = "dbHostId";
static constexpr const char* kBlogPropColumnFamilyId = "columnFamilyId";
static constexpr const char* kBlogPropColumnFamilyName = "columnFamilyName";

// --- BlogFileHeader ---

// Bit flags for BlogFileHeader::flags (byte [15] in the fixed header).
enum BlogFileHeaderFlags : uint8_t {
  kBlogFileRecycled = 0x01,  // File may contain trailing stale data from a
                             // previous incarnation. Recovery should tolerate
                             // trailing data that doesn't match our escape
                             // sequence (recycled/stale, not corruption).
};

// Schema version 0 is reserved outside the file format to denote legacy blob
// files that predate blog format. Blog files therefore start at schema
// version 1.
static constexpr uint8_t kBlogCurrentSchemaVersion = 1;

struct BlogFileHeader {
  uint8_t schema_version = kBlogCurrentSchemaVersion;
  ChecksumType checksum_type = kXXH3;
  BlogRecordType compact_record_type = kBlogBlobRecord;
  uint8_t flags = 0;  // BlogFileHeaderFlags, byte [15] in fixed header
  // 10-byte escape sequence: bytes [0-5] random (byte[0] not 0x00/0xFF),
  // bytes [6-9] = lower32(XXH3_64bits_withSeed(bytes[0-5], seed)). See
  // kBlogEscapeSeqSeed.
  char escape_sequence[kBlogEscapeSequenceSize] = {};

  bool is_recycled() const { return flags & kBlogFileRecycled; }
  bool is_wal() const { return compact_record_type == kBlogWriteBatchRecord; }

  // Required (uppercase) properties. These are the only properties encoded
  // in the physical header property section by EncodeTo().
  BlogPropertyMap required_properties;
  // Ignorable (lowercase) properties. These are NOT part of the physical
  // header: on write, BlogFileWriter::WriteHeader emits them as the initial
  // kBlogIgnorablePropertiesRecord body record; on read,
  // BlogFileReader::ReadRecord accumulates them here as ignorable properties
  // records are consumed.
  BlogPropertyMap ignorable_properties;

  // Incarnation ID = first 4 bytes of escape_sequence (little-endian via
  // DecodeFixed32). Guaranteed nonzero since byte[0] is not 0x00.
  uint32_t incarnation_id() const;

  // Encode the full header (fixed prefix + property section + checksums)
  // into dst. Only required_properties are encoded; ignorable_properties
  // are body-record content and never appear in the header section.
  // required_properties is encoded as-is: case placement is enforced by
  // SetProperty, not here.
  void EncodeTo(std::string* dst) const;

  // Decode from input. Consumes the bytes read. WAL escape sequence derived
  // bytes are not verified because the log number is not known here; see
  // VerifyWalEscapeSequence.
  Status DecodeFrom(Slice* input);

  // Verify the fixed header checksum, which covers bytes [0, 36) including
  // the property section size, after checking that the checksum type is
  // supported. fixed_header must point to at least kBlogFileFixedHeaderSize
  // bytes. Call this before trusting any fixed header field, such as the
  // property section size.
  static Status VerifyFixedHeaderChecksum(const char* fixed_header);

  // Generate escape_sequence from the file's unique ID (db_id, db_session_id,
  // file_number), using the same scheme as SST internal unique IDs. Falls back
  // to GenerateRandomFields() if any input is empty/zero. Also records the
  // IDs as ignorable dbId/dbSessionId properties (body-record content, not
  // part of the physical header).
  void GenerateFromUniqueId(const std::string& db_id,
                            const std::string& db_session_id,
                            uint64_t file_number);
  void GenerateWalFromUniqueId(const std::string& db_id,
                               const std::string& db_session_id,
                               uint64_t log_number);

  // Set optional diagnostic properties for debugging. All fields are
  // ignorable (lowercase) and so are emitted in an ignorable properties
  // body record, not the header section. Empty strings and sentinel values
  // are skipped.
  static constexpr uint32_t kNoCfId = UINT32_MAX;
  void SetDiagnosticProperties(const std::string& db_host_id,
                               uint32_t column_family_id = kNoCfId,
                               const std::string& column_family_name = "",
                               uint64_t creation_time = 0);

  // Generate escape_sequence from random bytes. Appropriate when unique ID
  // inputs are not available.
  void GenerateRandomFields();

 private:
  void GenerateFromUniqueIdWithSeed(const std::string& db_id,
                                    const std::string& db_session_id,
                                    uint64_t file_number, uint64_t seed);
  void GenerateRandomFieldsWithSeed(uint64_t seed);
  // Populate escape_sequence from a 64 bits of entropy.
  void SetEscapeSequenceFromU64(uint64_t v, uint64_t seed);
  static bool VerifyEscapeSequenceWithSeed(const char* seq, uint64_t seed);

 public:
  // Verify that the escape sequence's derived bytes match.
  static bool VerifyEscapeSequence(const char* seq);
  static bool VerifyWalEscapeSequence(const char* seq, uint64_t log_number);

  // Check if a buffer starts with the blog file magic number.
  static bool IsBlogFormat(const char* data, size_t size) {
    return size >= kBlogFileMagicSize &&
           memcmp(data, kBlogFileMagic, kBlogFileMagicSize) == 0;
  }

  // Decode a blog file header from a buffer that may or may not contain
  // the full header. Returns OK and consumes the header bytes on success.
  // If the buffer contains the fixed header but not the full property
  // section, returns the number of additional bytes needed in
  // *additional_bytes_needed and returns Status::Incomplete().
  // This enables single-read header parsing from a ~4 KiB buffer.
  Status DecodeFromBuffer(Slice* buffer, size_t* additional_bytes_needed);

  // Property setters. The name's first letter selects the map: uppercase
  // names are required properties, lowercase names are ignorable.
  void SetProperty(const std::string& name, const std::string& value);
  void SetUint64Property(const std::string& name, uint64_t value);
  void SetUint32Property(const std::string& name, uint32_t value);

  // Property getters. These search required_properties first, then
  // ignorable_properties, so callers can look up either kind by name. Since
  // SetProperty places each name by case, a name lives in at most one map;
  // for direct-inserted duplicates the map searched first wins.
  std::string GetProperty(const std::string& name) const;
  bool GetUint64Property(const std::string& name, uint64_t* value) const;
  bool GetUint32Property(const std::string& name, uint32_t* value) const;
  bool HasProperty(const std::string& name) const;
};

// --- Footer types ---

struct BlogFooterLocatorEntry {
  BlogRecordType record_type;
  uint32_t relative_offset_4B;
};

struct BlogFileFooterLocator {
  std::vector<BlogFooterLocatorEntry> entries;

  // Encode as the payload of a footer locator record.
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& input);
};

// Verify that a blog file's trailing data contains a valid footer locator
// record. Scans backward from the end of the buffer for the escape sequence,
// then parses and verifies the full record (varint length, type, prefix
// checksum, payload, trailer checksum). The buffer must start at a 4-byte-
// aligned file offset. Returns true if a valid footer locator record is found.
// If locator_out is non-null, the parsed locator is stored there on success.
// If locator_file_offset_out is non-null, the file offset of the locator
// record's escape sequence is stored there on success.
bool VerifyBlogFooterLocator(const char* buffer, size_t buffer_size,
                             uint64_t buffer_file_offset,
                             const char* expected_escape_seq,
                             ChecksumType checksum_type,
                             uint32_t incarnation_id,
                             BlogFileFooterLocator* locator_out = nullptr,
                             uint64_t* locator_file_offset_out = nullptr);

struct BlogFileFooterProperties {
  BlogPropertyMap properties;

  // Convenience setters that encode values into the property map.
  // All blob-related properties start with "blob" and are only emitted
  // when the file contains blob records (count > 0).

  // blobCount: number of blob records
  void SetBlobCount(uint64_t count);
  // blobPayloadBytes: total on-disk payload bytes (compressed where applicable)
  void SetBlobPayloadBytes(uint64_t bytes);
  // blobCompressedBytes: total compressed payload bytes (only blobs that were
  // actually compressed, i.e. compression_type != kNoCompression)
  void SetBlobCompressedBytes(uint64_t bytes);
  // blobUncompressedBytes: total original (pre-compression) bytes for the
  // same blobs counted by blobCompressedBytes
  void SetBlobUncompressedBytes(uint64_t bytes);
  // blobOverheadBytes: total record framing bytes (escape sequence, varint,
  // trailer, padding) across all blob records
  void SetBlobOverheadBytes(uint64_t bytes);
  // compressionTypes: set of CompressionType values used in records
  // (excluding kNoCompression and kStreamingCompressionSentinel),
  // encoded as sorted raw bytes. File-level, not blob-specific.
  void SetCompressionTypes(
      const SmallEnumSet<CompressionType, kDisableCompressionOption>& types);

  void SetSequenceRange(uint64_t min_seq, uint64_t max_seq);

  // Encode as named properties (no 5-byte trailer; that comes from
  // the enclosing full record format).
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& input);
};

// --- Padding helpers ---

// Computes and holds padding to reach the next 4-byte-aligned offset.
// The pad byte value is chosen to differ from last_meaningful_byte.
struct BlogPadding {
  char byte = 0;
  uint32_t count = 0;

  BlogPadding(uint8_t last_meaningful_byte, size_t current_offset,
              uint32_t alignment = kBlogMinRecordAlignment,
              bool prefer_0xff_when_legal = false);

  // Append padding to a string (convenience for non-hot paths).
  void AppendTo(std::string* dst) const { dst->append(count, byte); }
};

// Compute the number of padding bytes needed after a record or header section
// ending at `current_offset`, rounded up to `alignment`.
uint32_t ComputeBlogPaddingLength(uint8_t last_meaningful_byte,
                                  uint64_t current_offset, uint32_t alignment);

// Returns the pad byte to use. For trailing 0x00/0xFF bytes the opposite value
// is required; otherwise either 0x00 or 0xFF is legal, with 0x00 preferred by
// default.
uint8_t ChooseBlogPaddingByte(uint8_t last_meaningful_byte,
                              bool prefer_0xff_when_legal);

// --- Checksum helpers ---

// Compute a context-aware checksum over data + compression_type byte,
// suitable for a blog record trailer. Uses the same algorithm and context
// checksum approach as block-based SST files. The context offset is the file
// offset of data, i.e. the record payload rather than its escape sequence.
uint32_t ComputeBlogRecordChecksum(ChecksumType type, const char* data,
                                   size_t data_size, char compression_type_byte,
                                   uint32_t base_context_checksum,
                                   uint64_t payload_file_offset);

// Verify a blog record's 5-byte trailer (compression_type + checksum).
// payload points to the record payload of payload_size bytes; the trailer
// immediately follows. Returns OK if checksum matches, Corruption otherwise.
// On success, *actual_compression_type is set from the trailer.
Status VerifyBlogRecordTrailer(ChecksumType checksum_type, const char* payload,
                               size_t payload_size, uint32_t incarnation_id,
                               uint64_t payload_file_offset,
                               CompressionType* actual_compression_type);

}  // namespace ROCKSDB_NAMESPACE
