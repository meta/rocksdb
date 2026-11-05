//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blog/blog_reader.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>

#include "port/likely.h"
#include "table/format.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/prefix_varint.h"

namespace ROCKSDB_NAMESPACE {

namespace {

std::string MakeFileOffsetString(uint64_t file_offset) {
  return "file offset " + std::to_string(file_offset);
}

Status AppendContext(const Status& s, const std::string& context) {
  if (s.getState() == nullptr) {
    return Status(s.code(), s.subcode(), s.severity(), context);
  }
  return Status::CopyAppendMessage(s, "; ", context);
}

Status AppendFileOffset(const Status& s, uint64_t file_offset) {
  if (s.getState() == nullptr) {
    return Status(s.code(), s.subcode(), s.severity(),
                  MakeFileOffsetString(file_offset));
  }
  return Status::CopyAppendMessage(s, " at ",
                                   MakeFileOffsetString(file_offset));
}

std::string MakeRecordChecksumMismatchMessage(const std::string& kind,
                                              uint32_t stored_checksum,
                                              uint32_t computed_checksum,
                                              ChecksumType checksum_type,
                                              uint64_t file_offset,
                                              size_t size) {
  return kind + ": stored = " + std::to_string(stored_checksum) +
         ", computed = " + std::to_string(computed_checksum) +
         ", type = " + std::to_string(static_cast<int>(checksum_type)) +
         " in blog file offset " + std::to_string(file_offset) + " size " +
         std::to_string(size);
}

std::string MakeUnspecifiedSizeChecksumMismatchMessage(
    uint64_t record_offset, uint64_t candidate_escape_sequence_offset,
    size_t candidate_payload_size, size_t candidate_count) {
  return "unspecified-size record checksum mismatch for record at file "
         "offset " +
         std::to_string(record_offset) +
         " at candidate escape sequence file offset " +
         std::to_string(candidate_escape_sequence_offset) +
         ", candidate payload size " + std::to_string(candidate_payload_size) +
         "; escape-sequence candidates checked=" +
         std::to_string(candidate_count);
}

std::string MakeUnspecifiedSizeTruncatedMessage(
    uint64_t record_offset, uint64_t candidate_escape_sequence_offset,
    size_t available_bytes) {
  return "unspecified-size record truncated: record at file offset " +
         std::to_string(record_offset) + " has " +
         std::to_string(available_bytes) +
         " bytes before the next boundary at file offset " +
         std::to_string(candidate_escape_sequence_offset) +
         ", leaving no room for a trailer";
}

// memcmp(bytes, escape_sequence, kBlogEscapeSequenceSize) == 0, as two
// fixed-width loads that compile inline.
bool MatchesEscapeSequence(const char* bytes, const char* escape_sequence) {
  static_assert(kBlogEscapeSequenceSize == 10);
  uint64_t bytes_head;
  uint64_t escape_head;
  uint16_t bytes_tail;
  uint16_t escape_tail;
  memcpy(&bytes_head, bytes, sizeof(bytes_head));
  memcpy(&escape_head, escape_sequence, sizeof(escape_head));
  memcpy(&bytes_tail, bytes + 8, sizeof(bytes_tail));
  memcpy(&escape_tail, escape_sequence + 8, sizeof(escape_tail));
  return bytes_head == escape_head && bytes_tail == escape_tail;
}

}  // namespace

BlogFileReader::BlogFileReader(std::unique_ptr<SequentialFileReader>&& file,
                               Reporter* reporter, bool verify_checksums)
    : BlogFileReader(std::move(file), reporter, verify_checksums,
                     std::string()) {}

BlogFileReader::BlogFileReader(std::unique_ptr<SequentialFileReader>&& file,
                               Reporter* reporter, bool verify_checksums,
                               std::string&& initial_data)
    : window_(std::move(file), std::move(initial_data)),
      reporter_(reporter),
      verify_checksums_(verify_checksums) {}

Status BlogFileReader::ReadHeader(BlogFileHeader* header) {
  assert(!header_read_);
  // Nothing is accepted until the whole header decodes, so an incomplete
  // header is retried from the same position once the file grows.
  BlogFileHeader decoded;
  size_t header_size = 0;
  Status s = ReadHeaderInternal(&decoded, &header_size);
  if (!s.ok()) {
    return s;
  }
  Accept(header_size);
  body_end_offset_ = window_.ParseOffset();
  *header = decoded;
  header_ = decoded;
  header_read_ = true;
  incarnation_id_ = header_.incarnation_id();
  has_compression_compatibility_name_ =
      header_.HasProperty(kBlogPropCompressionCompatibilityName);
  has_streaming_compression_type_ =
      header_.HasProperty(kBlogPropWriteBatchStreamingCompressionType);
  // Compact lengths are never 0 or unspecified, so only the type itself can
  // fail ValidateRecordType for a compact record.
  compact_type_valid_ = IsKnownBlogRecordType(header_.compact_record_type) &&
                        header_.compact_record_type != kBlogPreambleStartRecord;
  return Status::OK();
}

Status BlogFileReader::ReadHeaderInternal(BlogFileHeader* header,
                                          size_t* header_size) {
  assert(!header_read_);
  const uint64_t fixed_offset = window_.ParseOffset();
  Slice fixed;
  Status s = window_.Peek(kBlogFileFixedHeaderSize, &fixed);
  if (!s.ok()) {
    return AppendContext(s, "while reading blog fixed header");
  }
  Slice fixed_input = fixed;
  size_t prop_section_size = 0;
  s = header->DecodeFromBuffer(&fixed_input, &prop_section_size);
  if (!s.IsIncomplete()) {
    assert(!s.ok());
    return AppendContext(AppendFileOffset(s, fixed_offset),
                         "while validating blog fixed header");
  }
  Slice full;
  s = window_.Peek(kBlogFileFixedHeaderSize + prop_section_size, &full);
  if (!s.ok()) {
    return AppendContext(s, "while reading blog header property section");
  }
  Slice full_input = full;
  s = header->DecodeFrom(&full_input);
  if (!s.ok()) {
    return AppendContext(AppendFileOffset(s, fixed_offset),
                         "while decoding blog header");
  }
  *header_size = kBlogFileFixedHeaderSize + prop_section_size;
  return Status::OK();
}

Status BlogFileReader::ReadRecord(BlogRecordType* type, Slice* payload,
                                  std::string* /*scratch*/,
                                  uint64_t* record_offset,
                                  CompressionType* record_compression_type) {
  // Drop accounting is per call.
  last_dropped_bytes_ = 0;
  while (true) {
    failure_stage_ = BlogReadFailureStage::kEscapeSequenceNotFound;
    failure_in_footer_record_ = false;
    size_t gap_bytes = 0;
    Status pad_status = SkipPadding(&gap_bytes);
    last_gap_bytes_ = gap_bytes;
    last_gap_known_ = pad_status.ok();
    last_record_end_known_ = false;
    if (!pad_status.ok()) {
      return pad_status;
    }
    Status record_status = ReadOneRecord(type, payload, record_offset,
                                         record_compression_type, gap_bytes);
    if (!record_status.ok()) {
      // The recovery hints describe a damaged record; a torn or unreadable
      // one leaves the scan to start at the cursor.
      last_gap_known_ = record_status.IsCorruption();
      return record_status;
    }
    if (*type != kBlogIgnorablePropertiesRecord) {
      return Status::OK();
    }
    // Ignorable properties were decoded and merged pre-commit; skip delivery
    // and read on.
  }
}

Status BlogFileReader::ReadOneRecord(BlogRecordType* type, Slice* payload,
                                     uint64_t* record_offset,
                                     CompressionType* record_compression_type,
                                     size_t gap_bytes) {
  if (!header_read_) {
    return Status::Corruption("Blog reader: header not read at " +
                              MakeFileOffsetString(window_.ParseOffset()));
  }
  const uint64_t rec_offset = window_.ParseOffset() + gap_bytes;
  assert(rec_offset % kBlogMinRecordAlignment == 0);

  Slice view;
  Status s = window_.Peek(gap_bytes + kBlogEscapeSequenceSize, &view);
  if (!s.ok()) {
    if (s.IsIncomplete()) {
      const size_t available =
          view.size() > gap_bytes ? view.size() - gap_bytes : 0;
      if (available == 0) {
        return Status::NotFound("Blog reader: clean EOF at " +
                                MakeFileOffsetString(rec_offset));
      }
      // A recycled file's EOF is the previous incarnation's, so a partial
      // escape here is either stale data or our own torn write with no stale
      // bytes after it. A full-length peek cannot tell those apart either, so
      // classify it like a full-length escape mismatch (stale tail).
      if (header_.is_recycled()) {
        return ReportCorruption(
            available, "stale recycled tail at " +
                           MakeFileOffsetString(rec_offset) +
                           " (insufficient escape sequence candidate bytes)");
      }
      return Status::Incomplete("Blog reader: incomplete escape sequence at " +
                                MakeFileOffsetString(rec_offset));
    }
    return AppendContext(s, "while reading escape sequence at " +
                                MakeFileOffsetString(rec_offset));
  }
  const Slice esc(view.data() + gap_bytes, kBlogEscapeSequenceSize);
  if (!MatchesEscapeSequence(esc.data(), header_.escape_sequence)) {
    return ReportCorruption(
        kBlogEscapeSequenceSize,
        "escape sequence mismatch at " + MakeFileOffsetString(rec_offset) +
            " (first bytes: 0x" +
            Slice(esc.data(), std::min(esc.size(), size_t{4})).ToString(true) +
            ")");
  }
  failure_stage_ = BlogReadFailureStage::kRecordValidationFailed;
  if (record_offset) {
    *record_offset = rec_offset;
  }

  uint64_t length = 0;
  size_t varint_len = 0;
  Status varint_status = ReadPrefixVarint64(gap_bytes + kBlogEscapeSequenceSize,
                                            &length, &varint_len);
  if (!varint_status.ok()) {
    return AppendContext(varint_status,
                         "while reading record length varint for record at " +
                             MakeFileOffsetString(rec_offset));
  }
  const size_t prefix_rel = gap_bytes + kBlogEscapeSequenceSize + varint_len;

  size_t helper_extent = 0;
  Status body_status =
      ReadRecordBody(rec_offset, prefix_rel, varint_len, length, type, payload,
                     record_compression_type, &helper_extent);
  if (body_status.IsIncomplete()) {
    // Torn tail or overrunning length: only a later boundary tells them apart.
    return ResolveTornRecord(rec_offset, gap_bytes, body_status);
  }
  if (!body_status.ok()) {
    return body_status;
  }
  failure_stage_ = BlogReadFailureStage::kRecordValidationFailed;
  // The record extent is known; failures below resume past it when trusted.
  const size_t record_end_rel =
      gap_bytes + kBlogEscapeSequenceSize + varint_len + helper_extent;
  // Trivial extents are exact; sized ones need authenticated lengths.
  const bool length_trusted = length == 0 || LengthsAuthenticated();
  Status sequence_status = ValidateRecordSequence(*type, rec_offset);
  if (!sequence_status.ok()) {
    NoteFailedRecordEnd(record_end_rel, length_trusted);
    return sequence_status;
  }
  if (*type == kBlogIgnorablePropertiesRecord) {
    // Pre-commit: failures leave the cursor before the record for retry/scan.
    Status properties_status = AbsorbIgnorableProperties(*payload, rec_offset);
    if (!properties_status.ok()) {
      NoteFailedRecordEnd(record_end_rel, length_trusted);
      return properties_status;
    }
  }
  Accept(record_end_rel);
  if (!IsBlogFooterRecordType(*type)) {
    body_end_offset_ = length == kBlogUnspecifiedSize
                           ? rec_offset + kBlogEscapeSequenceSize + varint_len +
                                 kBlogFullFormatPrefixRestSize +
                                 payload->size() + kBlogBlockTrailerSize
                           : window_.ParseOffset();
  }
  return Status::OK();
}

inline Status BlogFileReader::ReadRecordBody(
    uint64_t rec_offset, size_t prefix_rel, size_t varint_len, uint64_t length,
    BlogRecordType* type, Slice* payload,
    CompressionType* record_compression_type, size_t* helper_extent) {
  if (length == 0) {
    return ReadTrivialRecord(rec_offset, prefix_rel, varint_len, type, payload,
                             record_compression_type, helper_extent);
  }
  if (varint_len <= kBlogCompactVarintMaxBytes) {
    return ReadCompactRecord(rec_offset, prefix_rel, varint_len, length, type,
                             payload, record_compression_type, helper_extent);
  }
  if (length == kBlogUnspecifiedSize) {
    return ReadUnspecifiedSizeRecord(rec_offset, prefix_rel, varint_len, type,
                                     payload, record_compression_type,
                                     helper_extent);
  }
  return ReadFullFormatRecord(rec_offset, prefix_rel, varint_len, length, type,
                              payload, record_compression_type, helper_extent);
}

Status BlogFileReader::ReadFullFormatPrefix(
    uint64_t rec_offset, size_t prefix_rel, size_t varint_len,
    BlogRecordType* rec_type, CompressionType* prefix_compression_type) {
  Slice view;
  Status s = window_.Peek(prefix_rel + kBlogFullFormatPrefixRestSize, &view);
  if (!s.ok()) {
    return AppendContext(s, "while reading full record prefix for record at " +
                                MakeFileOffsetString(rec_offset));
  }
  const Slice rest(view.data() + prefix_rel, kBlogFullFormatPrefixRestSize);
  *rec_type = static_cast<BlogRecordType>(static_cast<uint8_t>(rest[0]));
  *prefix_compression_type =
      lossless_cast<CompressionType>(lossless_cast<uint8_t>(rest[1]));
  if (verify_checksums_) {
    const Slice covered(view.data() + (prefix_rel - varint_len),
                        varint_len + 2);
    const uint32_t stored_prefix_checksum = DecodeFixed32(rest.data() + 2);
    const uint32_t computed_prefix_checksum =
        ComputeBuiltinChecksum(header_.checksum_type, covered.data(),
                               covered.size()) +
        ChecksumModifierForContext(incarnation_id_, rec_offset);
    if (stored_prefix_checksum != computed_prefix_checksum) {
      return ReportCorruption(
          varint_len + kBlogFullFormatPrefixRestSize,
          MakeRecordChecksumMismatchMessage(
              "prefix checksum mismatch", stored_prefix_checksum,
              computed_prefix_checksum, header_.checksum_type, rec_offset,
              covered.size()));
    }
  }
  return Status::OK();
}

Status BlogFileReader::ReadTrivialRecord(
    uint64_t rec_offset, size_t prefix_rel, size_t varint_len,
    BlogRecordType* type, Slice* payload,
    CompressionType* record_compression_type, size_t* helper_extent) {
  if (varint_len != 1) {
    return ReportCorruption(
        varint_len, "zero-length record has nontrivial length encoding at " +
                        MakeFileOffsetString(rec_offset));
  }
  Slice view;
  Status s = window_.Peek(prefix_rel + kBlogTrivialRecordTrailerSize, &view);
  if (!s.ok()) {
    return AppendContext(s,
                         "while reading trivial record trailer for record at " +
                             MakeFileOffsetString(rec_offset));
  }
  const Slice rest(view.data() + prefix_rel, kBlogTrivialRecordTrailerSize);
  const BlogRecordType rec_type =
      static_cast<BlogRecordType>(static_cast<uint8_t>(rest[0]));
  if (verify_checksums_) {
    const Slice covered(view.data() + (prefix_rel - varint_len),
                        varint_len + 1);
    const uint32_t stored_checksum = DecodeFixed32(rest.data() + 1);
    const uint32_t computed_checksum =
        ComputeBuiltinChecksum(header_.checksum_type, covered.data(),
                               covered.size()) +
        ChecksumModifierForContext(incarnation_id_, rec_offset);
    if (stored_checksum != computed_checksum) {
      return ReportCorruption(
          varint_len + kBlogTrivialRecordTrailerSize,
          MakeRecordChecksumMismatchMessage("trivial record checksum mismatch",
                                            stored_checksum, computed_checksum,
                                            header_.checksum_type, rec_offset,
                                            covered.size()));
    }
  }
  s = ValidateRecordType(rec_type, 0, varint_len, rec_offset);
  if (!s.ok()) {
    return s;
  }
  *type = rec_type;
  payload->clear();
  if (record_compression_type) {
    *record_compression_type = kNoCompression;
  }
  *helper_extent = kBlogTrivialRecordTrailerSize;
  return Status::OK();
}

Status BlogFileReader::ReadCompactRecord(
    uint64_t rec_offset, size_t prefix_rel, size_t varint_len, uint64_t length,
    BlogRecordType* type, Slice* payload,
    CompressionType* record_compression_type, size_t* helper_extent) {
  const BlogRecordType rec_type = header_.compact_record_type;
  if (UNLIKELY(!compact_type_valid_)) {
    Status type_status =
        ValidateRecordType(rec_type, length, varint_len, rec_offset);
    if (!type_status.ok()) {
      return type_status;
    }
  }
  Status length_status = ValidateCompactLength(length, varint_len, rec_offset);
  if (!length_status.ok()) {
    return length_status;
  }
  const size_t payload_size = lossless_cast<size_t>(length);
  assert(payload_size <= kBlogMaxCompactPayloadSize);
  Slice view;
  Status peek_status =
      window_.Peek(prefix_rel + payload_size + kBlogBlockTrailerSize, &view);
  if (!peek_status.ok()) {
    return AppendContext(
        peek_status, std::string(view.size() < prefix_rel + payload_size
                                     ? "while reading payload"
                                     : "while reading trailer") +
                         " for record at " + MakeFileOffsetString(rec_offset));
  }
  const uint64_t payload_file_offset = window_.ParseOffset() + prefix_rel;
  const Slice payload_bytes(view.data() + prefix_rel, payload_size);
  const Slice trailer(view.data() + prefix_rel + payload_size,
                      kBlogBlockTrailerSize);
  if (verify_checksums_) {
    const uint32_t stored_checksum = DecodeFixed32(trailer.data() + 1);
    const uint32_t computed_checksum = ComputeBlogRecordChecksum(
        header_.checksum_type, payload_bytes.data(), payload_bytes.size(),
        trailer[0], incarnation_id_, payload_file_offset);
    if (stored_checksum != computed_checksum) {
      return ReportCorruption(
          payload_size + kBlogBlockTrailerSize,
          MakeRecordChecksumMismatchMessage(
              "record checksum mismatch", stored_checksum, computed_checksum,
              header_.checksum_type, rec_offset, payload_size));
    }
  }
  Status compression_status = ValidateCompressionType(
      lossless_cast<uint8_t>(trailer[0]), rec_type, rec_offset);
  if (!compression_status.ok()) {
    NoteFailedRecordEnd(prefix_rel + payload_size + kBlogBlockTrailerSize,
                        LengthsAuthenticated());
    return compression_status;
  }
  *payload = payload_bytes;
  if (record_compression_type) {
    *record_compression_type =
        lossless_cast<CompressionType>(lossless_cast<uint8_t>(trailer[0]));
  }
  *type = rec_type;
  *helper_extent = payload_size + kBlogBlockTrailerSize;
  return Status::OK();
}

Status BlogFileReader::ReadFullFormatRecord(
    uint64_t rec_offset, size_t prefix_rel, size_t varint_len, uint64_t length,
    BlogRecordType* type, Slice* payload,
    CompressionType* record_compression_type, size_t* helper_extent) {
  BlogRecordType rec_type = kBlogBlobRecord;
  CompressionType prefix_compression_type = kNoCompression;
  Status s = ReadFullFormatPrefix(rec_offset, prefix_rel, varint_len, &rec_type,
                                  &prefix_compression_type);
  if (!s.ok()) {
    return s;
  }
  failure_in_footer_record_ =
      LengthsAuthenticated() && IsBlogFooterRecordType(rec_type);
  s = ValidateRecordType(rec_type, length, varint_len, rec_offset);
  if (!s.ok()) {
    return s;
  }
  s = ValidateCompressionType(lossless_cast<uint8_t>(prefix_compression_type),
                              rec_type, rec_offset);
  if (!s.ok()) {
    return s;
  }
  if (length > kBlogMaxRecordPayloadSize) {
    return ReportCorruption(varint_len,
                            "record payload length exceeds reader limit at " +
                                MakeFileOffsetString(rec_offset));
  }
  const size_t payload_size = lossless_cast<size_t>(length);
  const size_t body_rel = prefix_rel + kBlogFullFormatPrefixRestSize;
  if (LengthsAuthenticated()) {
    // Authenticated lengths may reserve up front; anything else grows
    // progressively, bounded by short reads.
    const size_t record_bytes = body_rel + payload_size + kBlogBlockTrailerSize;
    if (!window_.Reserve(record_bytes)) {
      return ReportCorruption(record_bytes, "record length " +
                                                std::to_string(record_bytes) +
                                                " exceeds maximum string size");
    }
  }
  Slice view;
  s = window_.Peek(body_rel + payload_size + kBlogBlockTrailerSize, &view);
  if (!s.ok()) {
    return AppendContext(s, std::string(view.size() < body_rel + payload_size
                                            ? "while reading payload"
                                            : "while reading trailer") +
                                " for record at " +
                                MakeFileOffsetString(rec_offset));
  }
  const uint64_t payload_file_offset = window_.ParseOffset() + body_rel;
  const Slice payload_bytes(view.data() + body_rel, payload_size);
  const Slice trailer(view.data() + body_rel + payload_size,
                      kBlogBlockTrailerSize);
  if (verify_checksums_) {
    const uint32_t stored_checksum = DecodeFixed32(trailer.data() + 1);
    const uint32_t computed_checksum = ComputeBlogRecordChecksum(
        header_.checksum_type, payload_bytes.data(), payload_bytes.size(),
        trailer[0], incarnation_id_, payload_file_offset);
    if (stored_checksum != computed_checksum) {
      return ReportCorruption(
          payload_size + kBlogBlockTrailerSize,
          MakeRecordChecksumMismatchMessage(
              "record checksum mismatch", stored_checksum, computed_checksum,
              header_.checksum_type, rec_offset, payload_size));
    }
  }
  s = ValidateCompressionType(lossless_cast<uint8_t>(trailer[0]), rec_type,
                              rec_offset);
  if (!s.ok()) {
    NoteFailedRecordEnd(body_rel + payload_size + kBlogBlockTrailerSize,
                        LengthsAuthenticated());
    return s;
  }
  const CompressionType trailer_compression_type =
      lossless_cast<CompressionType>(lossless_cast<uint8_t>(trailer[0]));
  if (trailer_compression_type != prefix_compression_type) {
    NoteFailedRecordEnd(body_rel + payload_size + kBlogBlockTrailerSize,
                        LengthsAuthenticated());
    return ReportCorruption(
        payload_size + kBlogBlockTrailerSize,
        "full record prefix/trailer compression type mismatch at " +
            MakeFileOffsetString(rec_offset));
  }
  if (record_compression_type) {
    *record_compression_type = trailer_compression_type;
  }
  *type = rec_type;
  *helper_extent =
      kBlogFullFormatPrefixRestSize + payload_size + kBlogBlockTrailerSize;
  if (rec_type == kBlogFooterLocatorRecord) {
    // Trailing bytes after a locator belong to no record of ours; report
    // them as a missing boundary so recycled tails route as stale, not damage.
    failure_stage_ = BlogReadFailureStage::kEscapeSequenceNotFound;
    // Bytes after an intact locator are not part of it.
    failure_in_footer_record_ = false;
    s = RequireExactEOF(rec_offset, prefix_rel + *helper_extent);
    if (!s.ok()) {
      NoteFailedRecordEnd(prefix_rel + *helper_extent, LengthsAuthenticated());
      return s;
    }
  }
  // Assigned after RequireExactEOF, which may compact or grow the window.
  *payload = Slice(window_.Retained().data() + body_rel, payload_size);
  return Status::OK();
}

Status BlogFileReader::ResolveTornRecord(uint64_t rec_offset, size_t gap_bytes,
                                         const Status& torn_status) {
  assert(torn_status.IsIncomplete());
  // An overrunning length is corruption (writer moved past); a clean
  // truncation stays Incomplete. Prefix-less records cannot overrun.
  Slice record_bytes = window_.Retained();
  record_bytes.remove_prefix(std::min(gap_bytes, record_bytes.size()));
  Slice overrun_bytes;
  if (LengthOverranRecordBoundary(record_bytes, rec_offset, &overrun_bytes)) {
    return ReportCorruption(
        overrun_bytes.size(),
        "record length overruns a later record boundary at " +
            MakeFileOffsetString(rec_offset));
  }
  return torn_status;
}

Status BlogFileReader::AbsorbIgnorableProperties(const Slice& payload,
                                                 uint64_t rec_offset) {
  BlogPropertyMap properties;
  Slice property_input(payload);
  Status s = DecodeBlogProperties(&property_input, &properties);
  if (!s.ok() || !property_input.empty()) {
    return ReportCorruption(payload.size(),
                            "invalid ignorable properties record at " +
                                MakeFileOffsetString(rec_offset));
  }
  for (const auto& [name, value] : properties) {
    if (!IsBlogIgnorableProperty(name)) {
      return ReportCorruption(
          payload.size(),
          "required property in ignorable properties record at " +
              MakeFileOffsetString(rec_offset));
    }
    header_.SetProperty(name, value);
  }
  return Status::OK();
}

Status BlogFileReader::ValidateRecordType(BlogRecordType type, uint64_t length,
                                          size_t varint_len,
                                          uint64_t record_offset) {
  if (!IsKnownBlogRecordType(type)) {
    const uint8_t raw_type = lossless_cast<uint8_t>(type);
    const bool in_record_class_range =
        raw_type != 0x00 && (raw_type <= 0x7F || raw_type >= 0xF0);
    if (in_record_class_range) {
      return Status::NotSupported("Blog reader: reserved record type " +
                                  std::to_string(raw_type) + " at " +
                                  MakeFileOffsetString(record_offset));
    }
    return ReportCorruption(
        varint_len, "invalid record type " + std::to_string(raw_type) + " at " +
                        MakeFileOffsetString(record_offset));
  }

  if (type == kBlogPreambleStartRecord && length != 0) {
    return ReportCorruption(
        varint_len, "preamble-start record must use trivial format at " +
                        MakeFileOffsetString(record_offset));
  }
  if (type == kBlogFooterLocatorRecord &&
      (length == 0 || length == kBlogUnspecifiedSize)) {
    return ReportCorruption(
        varint_len,
        "footer locator must have a specified, non-zero payload at " +
            MakeFileOffsetString(record_offset));
  }
  return Status::OK();
}

Status BlogFileReader::ValidateCompactLength(uint64_t length, size_t varint_len,
                                             uint64_t record_offset) {
  if (LIKELY(PrefixVarint64Length(length) == varint_len)) {
    return Status::OK();
  }
  return ReportNonCanonicalCompactLength(length, varint_len, record_offset);
}

Status BlogFileReader::ReportNonCanonicalCompactLength(uint64_t length,
                                                       size_t varint_len,
                                                       uint64_t record_offset) {
  return ReportCorruption(varint_len,
                          "compact record length " + std::to_string(length) +
                              " has non-canonical prefix varint encoding at " +
                              MakeFileOffsetString(record_offset));
}

Status BlogFileReader::ValidateCompressionType(uint8_t compression_type,
                                               BlogRecordType record_type,
                                               uint64_t record_offset) {
  // Uncompressed records are valid for every record type and header.
  if (LIKELY(compression_type == kNoCompression)) {
    return Status::OK();
  }
  return ValidateCompressedType(compression_type, record_type, record_offset);
}

Status BlogFileReader::ValidateCompressedType(uint8_t compression_type,
                                              BlogRecordType record_type,
                                              uint64_t record_offset) {
  if (!IsValidBlogCompressionType(compression_type)) {
    return ReportCorruption(1, "invalid compression type " +
                                   std::to_string(compression_type) + " at " +
                                   MakeFileOffsetString(record_offset));
  }

  if (compression_type == kStreamingCompressionSentinel &&
      (record_type != kBlogWriteBatchRecord ||
       !has_streaming_compression_type_)) {
    return ReportCorruption(
        1,
        "streaming compression is only valid for WriteBatch records with "
        "WriteBatchStreamingCompressionType at " +
            MakeFileOffsetString(record_offset));
  }

  if (compression_type != kNoCompression &&
      !has_compression_compatibility_name_) {
    return ReportCorruption(
        1, "compressed record without CompressionCompatibilityName at " +
               MakeFileOffsetString(record_offset));
  }
  return Status::OK();
}

Status BlogFileReader::ValidateRecordSequence(BlogRecordType type,
                                              uint64_t record_offset) {
  if (LIKELY(!IsBlogFooterRecordType(type) && !footer_started_)) {
    return Status::OK();
  }
  return ValidateFooterSequence(type, record_offset);
}

Status BlogFileReader::ValidateFooterSequence(BlogRecordType type,
                                              uint64_t record_offset) {
  if (!IsBlogFooterRecordType(type)) {
    if (footer_started_) {
      return ReportCorruption(0, "non-footer record after footer began at " +
                                     MakeFileOffsetString(record_offset));
    }
    return Status::OK();
  }

  footer_started_ = true;
  switch (type) {
    case kBlogFooterIndexRecord:
      if (footer_properties_seen_ || footer_checksum_info_seen_ ||
          footer_locator_seen_) {
        return ReportCorruption(0, "footer index record out of order at " +
                                       MakeFileOffsetString(record_offset));
      }
      break;
    case kBlogFooterPropertiesRecord:
      if (footer_properties_seen_ || footer_checksum_info_seen_ ||
          footer_locator_seen_) {
        return ReportCorruption(
            0, "duplicate or out-of-order footer properties record at " +
                   MakeFileOffsetString(record_offset));
      }
      footer_properties_seen_ = true;
      break;
    case kBlogFooterFileChecksumInfo:
      if (footer_checksum_info_seen_ || footer_locator_seen_) {
        return ReportCorruption(
            0, "duplicate or out-of-order footer checksum-info record at " +
                   MakeFileOffsetString(record_offset));
      }
      footer_checksum_info_seen_ = true;
      break;
    case kBlogFooterLocatorRecord:
      if (footer_locator_seen_) {
        return ReportCorruption(0, "duplicate footer locator record at " +
                                       MakeFileOffsetString(record_offset));
      }
      footer_locator_seen_ = true;
      break;
    default:
      assert(false);
  }
  return Status::OK();
}

Status BlogFileReader::FindNextEscapeFrom(size_t from_rel, size_t limit_rel,
                                          size_t* found_rel) {
  const uint64_t misalignment =
      (window_.ParseOffset() + from_rel) % kBlogMinRecordAlignment;
  size_t pos = misalignment == 0
                   ? from_rel
                   : from_rel + lossless_cast<size_t>(kBlogMinRecordAlignment -
                                                      misalignment);
  while (true) {
    const Slice view = window_.Retained();
    const size_t end = std::min(view.size(), limit_rel);
    for (; pos + kBlogEscapeSequenceSize <= end;
         pos += kBlogMinRecordAlignment) {
      if (MatchesEscapeSequence(view.data() + pos, header_.escape_sequence)) {
        *found_rel = pos;
        return Status::OK();
      }
    }
    if (pos + kBlogEscapeSequenceSize > limit_rel) {
      return Status::NotFound(
          "escape sequence not found within record bound at " +
          MakeFileOffsetString(window_.ParseOffset() + limit_rel));
    }
    Slice extended;
    Status s = window_.Peek(pos + kBlogEscapeSequenceSize, &extended);
    if (s.IsIncomplete()) {
      return Status::NotFound("escape sequence not found before EOF at " +
                              MakeFileOffsetString(window_.ParseOffset()));
    }
    if (!s.ok()) {
      return AppendContext(s, "while scanning for escape sequence");
    }
  }
}

Status BlogFileReader::ScanRecycledTail(size_t from_rel,
                                        uint64_t* found_offset) {
  if (!recycled_tail_scan_) {
    recycled_tail_scan_.reset(new RecycledTailScan);
  }
  RecycledTailScan& scan = *recycled_tail_scan_;
  const uint64_t origin = window_.ParseOffset();
  // Only Refresh(offset) returns the cursor to an origin past which a
  // boundary was found. A boundary at origin itself proves nothing about the
  // record there.
  if (scan.origin == origin && scan.found_offset > origin) {
    Status s = window_.Refresh(scan.found_offset);
    if (!s.ok()) {
      return AppendContext(s, "while returning to recycled tail boundary");
    }
    Accept(0);
    *found_offset = scan.found_offset;
    return Status::OK();
  }
  const bool resuming = scan.origin == origin;
  // The writer may have written a boundary into skipped bytes since they were
  // scanned, so a boundary found past them is only confirmed by rescanning
  // them, without a read limit. The rescan stops at the first boundary, at
  // the latest the one already found: bytes before a boundary of this
  // incarnation are never rewritten.
  const bool rescanning = resuming && scan.found_offset == origin;
  scan.origin = origin;
  scan.found_offset = rescanning ? origin : 0;
  const size_t max_reads = window_.CanReopen() && !rescanning ? 1 : SIZE_MAX;
  const Slice retained = window_.Retained();
  const uint64_t retained_end = origin + retained.size();
  if (window_.ReadOffset() != retained_end) {
    // An earlier call read past the retained bytes; its buffer carries on,
    // but a refreshable input may have rewritten the bytes it scanned.
    return ContinueRecycledTailScan(max_reads, window_.CanReopen(),
                                    found_offset);
  }
  const size_t pos = FindAlignedEscapeSequence(retained, origin, from_rel);
  if (pos + kBlogEscapeSequenceSize <= retained.size()) {
    Accept(pos);
    *found_offset = scan.found_offset = window_.ParseOffset();
    return Status::OK();
  }
  const bool skip =
      !rescanning && resuming && scan.resume_offset > retained_end;
  if (skip) {
    // An earlier poll already scanned past the retained bytes.
    Status s = window_.SkipAhead(scan.resume_offset - retained_end);
    if (!s.ok()) {
      return AppendContext(s, "while resuming recycled tail scan");
    }
  } else {
    scan.resume_offset = origin + pos;
    memcpy(scan.buffer, retained.data() + pos, retained.size() - pos);
  }
  return ContinueRecycledTailScan(max_reads, skip, found_offset);
}

Status BlogFileReader::ContinueRecycledTailScan(size_t max_reads,
                                                bool rescan_on_match,
                                                uint64_t* found_offset) {
  RecycledTailScan& scan = *recycled_tail_scan_;
  for (size_t reads = 1;; ++reads) {
    const size_t carried =
        lossless_cast<size_t>(window_.ReadOffset() - scan.resume_offset);
    // Too short to hold an escape sequence, so nothing to scan before reading.
    assert(carried < kBlogEscapeSequenceSize);
    Slice bytes;
    Status s = window_.ReadAhead(RecycledTailScan::kReadSize, &bytes,
                                 scan.buffer + carried);
    if (!s.ok() && !s.IsIncomplete()) {
      return AppendContext(s, "while scanning recycled tail");
    }
    const size_t size = carried + bytes.size();
    const size_t pos = FindAlignedEscapeSequence(Slice(scan.buffer, size),
                                                 scan.resume_offset, 0);
    const bool matched = pos + kBlogEscapeSequenceSize <= size;
    if (matched && rescan_on_match) {
      scan.found_offset = scan.origin;
      return Status::NotFound("recycled tail boundary at " +
                              MakeFileOffsetString(scan.resume_offset + pos) +
                              " follows skipped bytes; rescanning from " +
                              MakeFileOffsetString(scan.origin));
    }
    if (matched) {
      *found_offset = scan.found_offset = scan.resume_offset + pos;
      window_.AdoptReadAhead(*found_offset,
                             Slice(scan.buffer + pos, size - pos));
      Accept(0);
      return Status::OK();
    }
    memmove(scan.buffer, scan.buffer + pos, size - pos);
    scan.resume_offset += pos;
    if (s.IsIncomplete()) {
      scan.origin = 0;
      return Status::NotFound("escape sequence not found before EOF at " +
                              MakeFileOffsetString(window_.ReadOffset()));
    }
    if (reads == max_reads) {
      return Status::NotFound("recycled tail scan paused at " +
                              MakeFileOffsetString(scan.resume_offset));
    }
  }
}

inline size_t BlogFileReader::ComputeRecycledRecordScanLimit() const {
  if (!CanRefresh() || !last_gap_known_ ||
      failure_stage_ != BlogReadFailureStage::kRecordValidationFailed) {
    return SIZE_MAX;
  }
  const Slice view = window_.Retained();
  const size_t varint_rel = last_gap_bytes_ + kBlogEscapeSequenceSize;
  if (varint_rel >= view.size()) {
    return SIZE_MAX;
  }
  uint64_t length = 0;
  const char* end = GetPrefixVarint64Ptr(view.data() + varint_rel,
                                         view.data() + view.size(), &length);
  if (end == nullptr) {
    return SIZE_MAX;
  }
  const size_t varint_len =
      lossless_cast<size_t>(end - (view.data() + varint_rel));
  const size_t prefix_rel = varint_rel + varint_len;
  size_t extent = 0;
  if (length == 0) {
    extent = kBlogTrivialRecordTrailerSize;
  } else if (varint_len <= kBlogCompactVarintMaxBytes) {
    extent = kBlogMaxCompactPayloadSize + kBlogBlockTrailerSize;
  } else if (length != kBlogUnspecifiedSize && LengthsAuthenticated() &&
             view.size() >= prefix_rel + kBlogFullFormatPrefixRestSize &&
             length <= kBlogMaxRecordPayloadSize) {
    const uint32_t expected =
        ComputeBuiltinChecksum(header_.checksum_type, view.data() + varint_rel,
                               varint_len + 2) +
        ChecksumModifierForContext(incarnation_id_,
                                   window_.ParseOffset() + last_gap_bytes_);
    if (DecodeFixed32(view.data() + prefix_rel + 2) == expected) {
      extent = kBlogFullFormatPrefixRestSize + lossless_cast<size_t>(length) +
               kBlogBlockTrailerSize;
    }
  }
  constexpr size_t kSuffixSize =
      kBlogPageFriendlyAlignment + kBlogEscapeSequenceSize;
  if (extent != 0 && prefix_rel <= SIZE_MAX - kSuffixSize &&
      extent <= SIZE_MAX - kSuffixSize - prefix_rel) {
    return prefix_rel + extent + kSuffixSize;
  }
  return SIZE_MAX;
}

Status BlogFileReader::ScanForEscapeSequence(uint64_t* found_offset) {
  // Without a matched escape the boundary may sit one quantum past the gap
  // (a flipped padding word ends the gap early); with one, skip past it.
  size_t from_rel = 0;
  if (last_gap_known_) {
    from_rel = last_gap_bytes_ +
               (failure_stage_ == BlogReadFailureStage::kEscapeSequenceNotFound
                    ? kBlogMinRecordAlignment
                    : kBlogEscapeSequenceSize);
  }
  if (last_record_end_known_) {
    from_rel = std::max(from_rel, last_record_end_rel_);
  }
  size_t limit_rel = SIZE_MAX;
  if (header_.is_recycled()) {
    limit_rel = ComputeRecycledRecordScanLimit();
    if (limit_rel == SIZE_MAX) {
      return ScanRecycledTail(from_rel, found_offset);
    }
  }
  size_t found_rel = 0;
  Status s = FindNextEscapeFrom(from_rel, limit_rel, &found_rel);
  if (!s.ok()) {
    return s;
  }
  Accept(found_rel);
  *found_offset = window_.ParseOffset();
  return Status::OK();
}

inline Status BlogFileReader::SkipPadding(size_t* gap_bytes) {
  // Common case: the gap ends inside the retained bytes at a record start.
  const Slice view = window_.Retained();
  for (size_t pos = 0; pos < view.size(); ++pos) {
    const uint8_t byte = lossless_cast<uint8_t>(view[pos]);
    if (byte == 0x00 || byte == 0xFF) {
      continue;
    }
    if (LIKELY((window_.ParseOffset() + pos) % kBlogMinRecordAlignment == 0)) {
      *gap_bytes = pos;
      return Status::OK();
    }
    break;
  }
  return SkipPaddingSlow(gap_bytes);
}

Status BlogFileReader::SkipPaddingSlow(size_t* gap_bytes) {
  *gap_bytes = 0;
  size_t pos = 0;
  while (true) {
    const Slice view = window_.Retained();
    for (; pos < view.size(); ++pos) {
      const uint8_t byte = lossless_cast<uint8_t>(view[pos]);
      if (byte == 0x00 || byte == 0xFF) {
        continue;
      }
      const uint64_t byte_offset = window_.ParseOffset() + pos;
      if (byte_offset % kBlogMinRecordAlignment != 0) {
        // Records start aligned, and growth cannot repair a byte that is
        // neither padding nor a record start.
        return ReportCorruption(view.size() - pos,
                                "non-padding byte inside alignment gap at " +
                                    MakeFileOffsetString(byte_offset));
      }
      // Next record, or aligned garbage for the record parse to reject. The
      // gap advance folds into the record's accept.
      *gap_bytes = pos;
      return Status::OK();
    }
    // Every retained byte is padding; look at the next one.
    Slice extended;
    Status s = window_.Peek(pos + 1, &extended);
    if (s.IsIncomplete()) {
      break;
    }
    if (!s.ok()) {
      return AppendContext(s, "while scanning padding");
    }
  }
  const uint64_t end_offset = window_.ParseOffset() + pos;
  if (pos > 0 && end_offset % kBlogMinRecordAlignment != 0) {
    return Status::Incomplete("torn padding at " +
                              MakeFileOffsetString(end_offset));
  }
  *gap_bytes = pos;
  if (!header_.is_recycled()) {
    Accept(pos);
  }
  return Status::NotFound("padding extends to EOF at " +
                          MakeFileOffsetString(end_offset));
}

Status BlogFileReader::RequireExactEOF(uint64_t record_offset,
                                       size_t record_end_rel) {
  Slice view;
  Status s = window_.Peek(record_end_rel + 1, &view);
  if (s.ok()) {
    return ReportCorruption(
        1, "footer locator is not the last byte sequence in the file at " +
               MakeFileOffsetString(record_offset));
  }
  if (s.IsIncomplete() && view.size() <= record_end_rel) {
    return Status::OK();
  }
  return AppendContext(s, "while checking for data after footer locator at " +
                              MakeFileOffsetString(record_offset));
}

void BlogFileReader::Accept(size_t n) {
  window_.Advance(n);
  last_gap_known_ = false;
  last_record_end_known_ = false;
}

bool BlogFileReader::LengthsAuthenticated() const {
  return verify_checksums_ && header_.checksum_type != kNoChecksum;
}

void BlogFileReader::NoteFailedRecordEnd(size_t end_rel, bool length_trusted) {
  if (length_trusted) {
    last_record_end_rel_ = end_rel;
    last_record_end_known_ = true;
  }
}

inline Status BlogFileReader::ReadPrefixVarint64(size_t offset, uint64_t* value,
                                                 size_t* varint_len) {
  const Slice retained = window_.Retained();
  if (LIKELY(retained.size() >= offset + kMaxPrefixVarint64Length)) {
    // Every encoding fits, so the decode cannot run short.
    const char* const bytes = retained.data() + offset;
    const uint32_t addl = PrefixVarint64AddlByteCount(bytes[0]);
    *varint_len = size_t{1} + addl;
    const bool decoded = DecodePrefixVarint64(bytes[0], bytes + 1, addl, value);
    assert(decoded);
    (void)decoded;
    return Status::OK();
  }
  return ReadPrefixVarint64Slow(offset, value, varint_len);
}

Status BlogFileReader::ReadPrefixVarint64Slow(size_t offset, uint64_t* value,
                                              size_t* varint_len) {
  Slice view;
  Status s = window_.Peek(offset + 1, &view);
  if (!s.ok()) {
    return s;
  }
  const char first_byte = view.data()[offset];
  const uint32_t addl = PrefixVarint64AddlByteCount(first_byte);
  if (addl > 0) {
    Status addl_status = window_.Peek(offset + 1 + addl, &view);
    if (!addl_status.ok()) {
      return addl_status;
    }
  }
  *varint_len = size_t{1} + addl;
  if (!DecodePrefixVarint64(first_byte, view.data() + offset + 1, addl,
                            value)) {
    return ReportCorruption(
        *varint_len,
        "invalid prefix varint ending at " +
            MakeFileOffsetString(window_.ParseOffset() + offset + *varint_len));
  }
  return Status::OK();
}

size_t BlogFileReader::RecordPrefixSize(const Slice& record_bytes) const {
  if (record_bytes.size() <= kBlogEscapeSequenceSize ||
      !MatchesEscapeSequence(record_bytes.data(), header_.escape_sequence)) {
    return 0;
  }
  const char* const varint = record_bytes.data() + kBlogEscapeSequenceSize;
  const size_t available = record_bytes.size() - kBlogEscapeSequenceSize;
  const uint32_t addl_bytes = PrefixVarint64AddlByteCount(*varint);
  const size_t varint_size = size_t{addl_bytes} + 1;
  const size_t prefix_rest_size = varint_size > kBlogCompactVarintMaxBytes
                                      ? kBlogFullFormatPrefixRestSize
                                      : 0;
  if (varint_size + prefix_rest_size > available) {
    return 0;
  }
  uint64_t length = 0;
  if (!DecodePrefixVarint64(*varint, varint + 1, addl_bytes, &length) ||
      length == 0 || length == kBlogUnspecifiedSize) {
    return 0;
  }
  return kBlogEscapeSequenceSize + varint_size + prefix_rest_size;
}

size_t BlogFileReader::FindAlignedEscapeSequence(const Slice& bytes,
                                                 uint64_t bytes_file_offset,
                                                 size_t pos) const {
  const uint64_t misalignment =
      (bytes_file_offset + pos) % kBlogMinRecordAlignment;
  if (misalignment != 0) {
    pos += lossless_cast<size_t>(kBlogMinRecordAlignment - misalignment);
  }
  for (; pos + kBlogEscapeSequenceSize <= bytes.size();
       pos += kBlogMinRecordAlignment) {
    if (MatchesEscapeSequence(bytes.data() + pos, header_.escape_sequence)) {
      return pos;
    }
  }
  return std::min(pos, bytes.size());
}

bool BlogFileReader::ContainsAlignedEscapeSequence(
    const Slice& bytes, uint64_t bytes_file_offset) const {
  return FindAlignedEscapeSequence(bytes, bytes_file_offset, 0) +
             kBlogEscapeSequenceSize <=
         bytes.size();
}

bool BlogFileReader::LengthOverranRecordBoundary(const Slice& record_bytes,
                                                 uint64_t record_offset,
                                                 Slice* overrun_bytes) const {
  const size_t prefix_size = RecordPrefixSize(record_bytes);
  if (prefix_size == 0) {
    return false;
  }
  const Slice bytes(record_bytes.data() + prefix_size,
                    record_bytes.size() - prefix_size);
  if (!ContainsAlignedEscapeSequence(bytes, record_offset + prefix_size)) {
    return false;
  }
  *overrun_bytes = bytes;
  return true;
}

Status BlogFileReader::TryEndUnspecifiedSizeRecord(UnspecifiedSizeScan* scan,
                                                   const Slice& payload_bytes,
                                                   size_t candidate_offset,
                                                   bool record_evidence,
                                                   bool* terminated) {
  assert(scan != nullptr);
  assert(terminated != nullptr);
  assert(candidate_offset <= payload_bytes.size());

  *terminated = false;
  // Bytes this record would span if the boundary turns out to be damaged.
  const size_t boundary_bytes =
      candidate_offset + (record_evidence ? kBlogEscapeSequenceSize : 0);

  // The trailer sits immediately before the trailing 0x00/0xFF run.
  size_t pad_len = 0;
  if (candidate_offset > 0) {
    uint8_t pad_byte =
        lossless_cast<uint8_t>(payload_bytes[candidate_offset - 1]);
    if (pad_byte == 0x00 || pad_byte == 0xFF) {
      pad_len = 1;
      while (pad_len < candidate_offset &&
             lossless_cast<uint8_t>(
                 payload_bytes[candidate_offset - 1 - pad_len]) == pad_byte) {
        ++pad_len;
      }
    }
  }

  if (record_evidence && !scan->saw_candidate) {
    scan->saw_candidate = true;
    scan->first_candidate_offset = candidate_offset;
  }

  if (record_evidence) {
    // Every examined candidate counts toward the scan bound, including ones
    // without room for a trailer: those still prove the record is torn.
    ++scan->candidate_count;
  }

  if (candidate_offset < pad_len + kBlogBlockTrailerSize) {
    // An escape with no trailer room proves a torn record (something real
    // starts here); a bare EOF stays retryable.
    if (record_evidence) {
      scan->saw_truncated_candidate = true;
    }
    return Status::OK();
  }

  const size_t trailer_pos = candidate_offset - pad_len - kBlogBlockTrailerSize;
  const size_t payload_size = trailer_pos;
  const char* trailer = payload_bytes.data() + trailer_pos;
  const uint32_t stored_checksum = DecodeFixed32(trailer + 1);
  const uint32_t computed_checksum = ComputeBlogRecordChecksum(
      header_.checksum_type, payload_bytes.data(), payload_size, trailer[0],
      incarnation_id_, scan->payload_file_offset);
  if (record_evidence) {
    // End of file is not an escape-sequence candidate, so it leaves the
    // diagnostics reported on failure untouched.
    scan->last_candidate_escape_sequence_offset =
        scan->payload_file_offset + candidate_offset;
    scan->last_candidate_payload_size = payload_size;
  }
  if (stored_checksum != computed_checksum) {
    // Escapes occur in payloads and torn records end anywhere; keep scanning
    // until a trailer checksum authenticates a boundary.
    scan->saw_checksum_mismatch |= record_evidence;
    return Status::OK();
  }
  // With kNoChecksum the stored value is only the context modifier,
  // independent of payload length, so any matching trailer is accepted.
  const uint8_t trailer_compression_byte = lossless_cast<uint8_t>(trailer[0]);
  Status validation = ValidateCompressionType(
      trailer_compression_byte, scan->record_type, scan->record_offset);
  if (!validation.ok()) {
    return validation;
  }
  const CompressionType trailer_compression_type =
      lossless_cast<CompressionType>(trailer_compression_byte);
  if (trailer_compression_type != scan->prefix_compression_type) {
    return ReportCorruption(
        boundary_bytes,
        "unspecified-size record prefix/trailer compression type mismatch "
        "at " +
            MakeFileOffsetString(scan->record_offset));
  }

  // Report the accepted extent for the caller to advance past; retained
  // bytes need no pushback, so at end of file there is nothing to do.
  *scan->payload = Slice(payload_bytes.data(), payload_size);
  if (scan->record_compression_type) {
    *scan->record_compression_type = trailer_compression_type;
  }
  scan->accepted_extent = scan->payload_rel + candidate_offset;
  *terminated = true;
  return Status::OK();
}

Status BlogFileReader::ReportTornUnspecifiedSizeRecord(
    UnspecifiedSizeScan* scan, uint64_t record_offset) {
  assert(scan != nullptr);
  assert(scan->saw_checksum_mismatch || scan->saw_truncated_candidate);
  // Recovery resumes at the first rejected candidate via the outer scan.
  const size_t dropped_bytes = scan->first_candidate_offset;
  if (!scan->saw_checksum_mismatch) {
    // No candidate survived long enough to test a checksum: the record ends
    // before any trailer could fit.
    return ReportCorruption(dropped_bytes, MakeUnspecifiedSizeTruncatedMessage(
                                               record_offset,
                                               scan->payload_file_offset +
                                                   scan->first_candidate_offset,
                                               scan->first_candidate_offset));
  }
  return ReportCorruption(
      dropped_bytes,
      MakeUnspecifiedSizeChecksumMismatchMessage(
          record_offset, scan->last_candidate_escape_sequence_offset,
          scan->last_candidate_payload_size, scan->candidate_count));
}

Status BlogFileReader::ReadUnspecifiedSizeRecord(
    uint64_t rec_offset, size_t prefix_rel, size_t varint_len,
    BlogRecordType* type, Slice* payload,
    CompressionType* record_compression_type, size_t* helper_extent) {
  assert(payload != nullptr);

  BlogRecordType rec_type = kBlogBlobRecord;
  CompressionType prefix_compression_type = kNoCompression;
  Status s = ReadFullFormatPrefix(rec_offset, prefix_rel, varint_len, &rec_type,
                                  &prefix_compression_type);
  if (!s.ok()) {
    return s;
  }
  s = ValidateRecordType(rec_type, kBlogUnspecifiedSize, varint_len,
                         rec_offset);
  if (!s.ok()) {
    return s;
  }
  s = ValidateCompressionType(lossless_cast<uint8_t>(prefix_compression_type),
                              rec_type, rec_offset);
  if (!s.ok()) {
    return s;
  }
  *type = rec_type;

  UnspecifiedSizeScan scan;
  scan.record_offset = rec_offset;
  scan.payload_rel = prefix_rel + kBlogFullFormatPrefixRestSize;
  scan.payload_file_offset = window_.ParseOffset() + scan.payload_rel;
  scan.record_type = rec_type;
  scan.prefix_compression_type = prefix_compression_type;
  scan.payload = payload;
  scan.record_compression_type = record_compression_type;

  // Only 4-byte-aligned file offsets can start the next record; the core
  // aligns the search start up.
  size_t pos = scan.payload_rel;
  while (true) {
    size_t found_rel = 0;
    s = FindNextEscapeFrom(pos, SIZE_MAX, &found_rel);
    // The prefix read retained everything before the payload, and the window
    // only grows mid-parse, so the payload base is always retained.
    const Slice retained = window_.Retained();
    assert(retained.size() >= scan.payload_rel);
    const Slice payload_bytes(retained.data() + scan.payload_rel,
                              retained.size() - scan.payload_rel);
    if (!s.ok()) {
      if (!s.IsNotFound()) {
        // I/O errors fail fast: unlike a torn tail, they will not heal.
        return AppendContext(
            s, "while reading unspecified-size record starting at " +
                   MakeFileOffsetString(rec_offset));
      }
      // No self-describing end: clean EOF is the last boundary that can
      // terminate it (the post-crash WAL tail).
      bool terminated = false;
      Status eof_status = TryEndUnspecifiedSizeRecord(
          &scan, payload_bytes, payload_bytes.size(),
          /*record_evidence=*/false, &terminated);
      if (!eof_status.ok() || terminated) {
        if (terminated) {
          *helper_extent = scan.accepted_extent - prefix_rel;
        }
        return eof_status;
      }
      // A torn record cannot authenticate its trailer. Retryable tails
      // keep Incomplete so the whole record is retried once the file grows.
      if (scan.saw_checksum_mismatch || scan.saw_truncated_candidate) {
        return ReportTornUnspecifiedSizeRecord(&scan, rec_offset);
      }
      return AppendContext(
          Status::Incomplete("torn unspecified-size record tail at " +
                             MakeFileOffsetString(window_.ParseOffset() +
                                                  scan.payload_rel +
                                                  payload_bytes.size())),
          "while reading unspecified-size record starting at " +
              MakeFileOffsetString(rec_offset));
    }

    // A candidate boundary; authenticate it before trusting it, since the
    // payload itself can contain escape sequences.
    const size_t candidate_offset = found_rel - scan.payload_rel;
    bool terminated = false;
    Status candidate_status =
        TryEndUnspecifiedSizeRecord(&scan, payload_bytes, candidate_offset,
                                    /*record_evidence=*/true, &terminated);
    if (!candidate_status.ok() || terminated) {
      if (terminated) {
        *helper_extent = scan.accepted_extent - prefix_rel;
      }
      return candidate_status;
    }
    if (scan.candidate_count > kBlogMaxUnspecifiedSizeScanCandidates) {
      // Too many rejected candidates: the record is torn, with later
      // records' escape sequences acting as candidates. Stop here instead of
      // scanning to end of file: every candidate re-checksums the whole
      // payload so far. Bytes past this point stay retained for recovery.
      break;
    }
    pos = found_rel + kBlogMinRecordAlignment;
  }
  return ReportTornUnspecifiedSizeRecord(&scan, rec_offset);
}

Status BlogFileReader::ReportCorruption(size_t bytes, const std::string& msg) {
  last_dropped_bytes_ = bytes;
  Status s = Status::Corruption("Blog reader", msg);
  if (reporter_) {
    reporter_->Corruption(bytes, s);
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
