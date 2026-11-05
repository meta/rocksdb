//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blog/blog_reader.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>

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
    size_t candidate_payload_size, uint32_t stored_checksum,
    uint32_t computed_checksum, ChecksumType checksum_type,
    size_t candidate_count) {
  return "unspecified-size record checksum mismatch: stored = " +
         std::to_string(stored_checksum) +
         ", computed = " + std::to_string(computed_checksum) +
         ", type = " + std::to_string(static_cast<int>(checksum_type)) +
         " for record at file offset " + std::to_string(record_offset) +
         " at candidate escape sequence file offset " +
         std::to_string(candidate_escape_sequence_offset) +
         ", candidate payload size " + std::to_string(candidate_payload_size) +
         "; escape-sequence candidates checked=" +
         std::to_string(candidate_count);
}

}  // namespace

BlogFileReader::BlogFileReader(std::unique_ptr<SequentialFileReader>&& file,
                               Reporter* reporter, bool verify_checksums)
    : seq_file_(std::move(file)),
      reporter_(reporter),
      verify_checksums_(verify_checksums) {}

Status BlogFileReader::ReadHeader(BlogFileHeader* header) {
  assert(!header_read_);

  // Read the fixed header to get property_section_size
  const uint64_t fixed_header_offset = current_offset();
  std::string fixed_buf;
  Slice fixed_slice;
  Status s = ReadBytes(kBlogFileFixedHeaderSize, &fixed_slice, &fixed_buf);
  if (!s.ok()) {
    return AppendContext(s, "while reading blog fixed header");
  }

  // Validate the fields needed to determine the remaining header size before
  // trusting property_section_size for an allocation. DecodeFromBuffer()
  // checks the magic, the fixed header checksum, and the property-section
  // bounds before returning the number of missing bytes.
  size_t prop_section_size = 0;
  Slice fixed_input(fixed_slice);
  s = header->DecodeFromBuffer(&fixed_input, &prop_section_size);
  if (!s.IsIncomplete()) {
    assert(!s.ok());
    return AppendContext(AppendFileOffset(s, fixed_header_offset),
                         "while validating blog fixed header");
  }

  // Read the property section
  std::string prop_buf;
  Slice prop_slice;
  s = ReadBytes(prop_section_size, &prop_slice, &prop_buf);
  if (!s.ok()) {
    return AppendContext(s, "while reading blog header property section");
  }

  // Combine into a single buffer for decoding
  std::string full_header;
  full_header.append(fixed_slice.data(), fixed_slice.size());
  full_header.append(prop_slice.data(), prop_slice.size());

  Slice input(full_header);
  s = header->DecodeFrom(&input);
  if (!s.ok()) {
    return AppendContext(AppendFileOffset(s, fixed_header_offset),
                         "while decoding blog header");
  }

  header_ = *header;
  header_read_ = true;
  has_compression_compatibility_name_ =
      header_.HasProperty(kBlogPropCompressionCompatibilityName);
  has_streaming_compression_type_ =
      header_.HasProperty(kBlogPropWriteBatchStreamingCompressionType);
  return Status::OK();
}

Status BlogFileReader::ReadRecord(BlogRecordType* type, Slice* payload,
                                  std::string* scratch, uint64_t* record_offset,
                                  CompressionType* record_compression_type) {
  while (true) {
    if (eof_ && pending_input_.empty()) {
      return Status::NotFound("Blog reader: clean EOF at " +
                              MakeFileOffsetString(current_offset()));
    }
    // Skip the padding gap after the previously delivered header or record.
    // Torn padding reports Incomplete (retry once the file grows) without
    // withholding the complete record or header already delivered.
    Status pad_status = SkipPadding();
    if (!pad_status.ok()) {
      return pad_status;
    }
    Status s = ReadOneRecord(type, payload, scratch, record_offset,
                             record_compression_type);
    if (!s.ok() || *type != kBlogIgnorablePropertiesRecord) {
      return s;
    }

    BlogPropertyMap properties;
    Slice property_input(*payload);
    s = DecodeBlogProperties(&property_input, &properties);
    if (!s.ok() || !property_input.empty()) {
      return ReportCorruption(payload->size(),
                              "invalid ignorable properties record at " +
                                  MakeFileOffsetString(record_offset == nullptr
                                                           ? last_record_offset_
                                                           : *record_offset));
    }
    for (const auto& [name, value] : properties) {
      if (!IsBlogIgnorableProperty(name)) {
        return ReportCorruption(
            payload->size(),
            "required property in ignorable properties record at " +
                MakeFileOffsetString(record_offset == nullptr
                                         ? last_record_offset_
                                         : *record_offset));
      }
      header_.SetProperty(name, value);
    }
  }
}

Status BlogFileReader::ReadOneRecord(BlogRecordType* type, Slice* payload,
                                     std::string* scratch,
                                     uint64_t* record_offset,
                                     CompressionType* record_compression_type) {
  if (!header_read_) {
    return Status::Corruption("Blog reader: header not read at " +
                              MakeFileOffsetString(current_offset()));
  }

  Status s;
  scratch->clear();

  std::string esc_buf;
  const uint64_t escape_sequence_offset =
      offset_ - lossless_cast<uint64_t>(pending_input_.size());
  Slice esc_slice;
  s = ReadBytes(kBlogEscapeSequenceSize, &esc_slice, &esc_buf);
  if (!s.ok()) {
    if (s.IsIncomplete()) {
      eof_ = true;
      if (!esc_slice.empty()) {
        return Status::Incomplete(
            "Blog reader: incomplete escape sequence at " +
            MakeFileOffsetString(escape_sequence_offset));
      }
      return Status::NotFound("Blog reader: clean EOF at " +
                              MakeFileOffsetString(escape_sequence_offset));
    }
    return AppendContext(s, "while reading escape sequence at " +
                                MakeFileOffsetString(escape_sequence_offset));
  }
  if (esc_slice.data() != esc_buf.data()) {
    esc_buf.assign(esc_slice.data(), esc_slice.size());
  }

  // Verify escape sequence matches
  if (memcmp(esc_buf.data(), header_.escape_sequence,
             kBlogEscapeSequenceSize) != 0) {
    return ReportCorruption(
        kBlogEscapeSequenceSize,
        "escape sequence mismatch at " +
            MakeFileOffsetString(escape_sequence_offset) + " (first bytes: 0x" +
            Slice(esc_buf.data(), std::min(esc_buf.size(), size_t{4}))
                .ToString(true) +
            ")");
  }

  const uint64_t rec_offset = escape_sequence_offset;
  assert(rec_offset % kBlogMinRecordAlignment == 0);
  if (record_offset) {
    *record_offset = rec_offset;
  }
  last_record_offset_ = rec_offset;

  // Read varint length. scratch receives the raw varint bytes for use
  // in prefix checksum verification (avoids fragile re-encoding).
  uint64_t length = 0;
  s = ReadPrefixVarint64(&length, scratch);
  if (!s.ok()) {
    if (s.IsIncomplete()) {
      eof_ = true;
    }
    return AppendContext(s,
                         "while reading record length varint for record at " +
                             MakeFileOffsetString(rec_offset));
  }
  size_t varint_len = scratch->size();

  BlogRecordType rec_type;
  std::string prefix_rest_buf;

  if (length == 0) {
    // Trivial format: [varint(0): 1B] [type: 1B] [checksum: 4B]
    if (varint_len != 1) {
      return ReportCorruption(
          varint_len, "zero-length record has nontrivial length encoding at " +
                          MakeFileOffsetString(rec_offset));
    }
    Slice trivial_rest_slice;
    s = ReadBytes(5, &trivial_rest_slice,
                  &prefix_rest_buf);  // type(1) + cksum(4)
    if (!s.ok()) {
      if (s.IsIncomplete()) {
        eof_ = true;
      }
      return AppendContext(
          s, "while reading trivial record trailer for record at " +
                 MakeFileOffsetString(rec_offset));
    }

    rec_type = static_cast<BlogRecordType>(
        static_cast<uint8_t>(trivial_rest_slice[0]));

    if (verify_checksums_) {
      scratch->push_back(trivial_rest_slice[0]);  // type
      uint32_t stored_checksum = DecodeFixed32(trivial_rest_slice.data() + 1);
      uint32_t computed_checksum =
          ComputeBuiltinChecksum(header_.checksum_type, scratch->data(),
                                 scratch->size()) +
          ChecksumModifierForContext(header_.incarnation_id(), rec_offset);
      if (stored_checksum != computed_checksum) {
        return ReportCorruption(
            varint_len + 5,
            MakeRecordChecksumMismatchMessage(
                "trivial record checksum mismatch", stored_checksum,
                computed_checksum, header_.checksum_type, rec_offset,
                scratch->size()));
      }
    }

    s = ValidateRecordType(rec_type, length, varint_len, rec_offset);
    if (!s.ok()) {
      return s;
    }

    *type = rec_type;
    payload->clear();
    if (record_compression_type) {
      *record_compression_type = kNoCompression;
    }
    s = ValidateRecordSequence(rec_type, rec_offset);
    if (!s.ok()) {
      return s;
    }
    return Status::OK();
  }

  bool is_full_format = (varint_len > kBlogCompactVarintMaxBytes);
  CompressionType prefix_compression_type = kNoCompression;

  if (is_full_format) {
    // Full format: read type + compression_type + prefix_checksum
    Slice prefix_rest_slice;
    s = ReadBytes(6, &prefix_rest_slice, &prefix_rest_buf);  // 1+1+4
    if (!s.ok()) {
      if (s.IsIncomplete()) {
        eof_ = true;
      }
      return AppendContext(s,
                           "while reading full record prefix for record at " +
                               MakeFileOffsetString(rec_offset));
    }

    rec_type =
        static_cast<BlogRecordType>(static_cast<uint8_t>(prefix_rest_slice[0]));
    prefix_compression_type = lossless_cast<CompressionType>(
        lossless_cast<uint8_t>(prefix_rest_slice[1]));

    if (verify_checksums_) {
      scratch->push_back(prefix_rest_slice[0]);  // type
      scratch->push_back(prefix_rest_slice[1]);  // comp_type

      uint32_t stored_prefix_checksum =
          DecodeFixed32(prefix_rest_slice.data() + 2);
      uint32_t computed_prefix_checksum =
          ComputeBuiltinChecksum(header_.checksum_type, scratch->data(),
                                 scratch->size()) +
          ChecksumModifierForContext(header_.incarnation_id(), rec_offset);
      if (stored_prefix_checksum != computed_prefix_checksum) {
        return ReportCorruption(
            varint_len + 6,
            MakeRecordChecksumMismatchMessage(
                "prefix checksum mismatch", stored_prefix_checksum,
                computed_prefix_checksum, header_.checksum_type, rec_offset,
                scratch->size()));
      }
    }
  } else {
    rec_type = header_.compact_record_type;
  }

  s = ValidateRecordType(rec_type, length, varint_len, rec_offset);
  if (!s.ok()) {
    return s;
  }
  if (is_full_format) {
    s = ValidateCompressionType(lossless_cast<uint8_t>(prefix_compression_type),
                                rec_type, rec_offset);
    if (!s.ok()) {
      return s;
    }
  }

  if (length == kBlogUnspecifiedSize) {
    s = ReadUnspecifiedSizeRecord(payload, scratch, rec_offset, rec_type,
                                  prefix_compression_type,
                                  record_compression_type);
    if (!s.ok()) {
      return s;
    }
  } else {
    const size_t payload_size = lossless_cast<size_t>(length);
    if (payload_size > scratch->max_size()) {
      return ReportCorruption(
          varint_len, "record payload length exceeds string max_size at " +
                          MakeFileOffsetString(rec_offset));
    }

    // Read payload
    scratch->clear();
    Slice payload_slice;
    const uint64_t payload_file_offset = current_offset();
    s = ReadBytes(payload_size, &payload_slice, scratch);
    if (!s.ok()) {
      if (s.IsIncomplete()) {
        eof_ = true;
      }
      return AppendContext(s, "while reading payload for record at " +
                                  MakeFileOffsetString(rec_offset));
    }
    if (payload_slice.data() != scratch->data()) {
      scratch->assign(payload_slice.data(), payload_slice.size());
    }

    // Read 5-byte trailer
    Slice trailer_slice;
    std::string trailer_buf;
    s = ReadBytes(kBlogBlockTrailerSize, &trailer_slice, &trailer_buf);
    if (!s.ok()) {
      if (s.IsIncomplete()) {
        eof_ = true;
      }
      return AppendContext(s, "while reading trailer for record at " +
                                  MakeFileOffsetString(rec_offset));
    }

    char trailer_comp_type = trailer_slice[0];
    uint32_t stored_checksum = DecodeFixed32(trailer_slice.data() + 1);

    if (verify_checksums_) {
      uint32_t computed_checksum = ComputeBlogRecordChecksum(
          header_.checksum_type, scratch->data(), scratch->size(),
          trailer_comp_type, header_.incarnation_id(), payload_file_offset);
      if (stored_checksum != computed_checksum) {
        return ReportCorruption(payload_size + kBlogBlockTrailerSize,
                                MakeRecordChecksumMismatchMessage(
                                    "record checksum mismatch", stored_checksum,
                                    computed_checksum, header_.checksum_type,
                                    rec_offset, lossless_cast<size_t>(length)));
      }
    }

    s = ValidateCompressionType(lossless_cast<uint8_t>(trailer_comp_type),
                                rec_type, rec_offset);
    if (!s.ok()) {
      return s;
    }
    const CompressionType trailer_compression_type =
        lossless_cast<CompressionType>(
            lossless_cast<uint8_t>(trailer_comp_type));
    if (is_full_format && trailer_compression_type != prefix_compression_type) {
      return ReportCorruption(
          payload_size + kBlogBlockTrailerSize,
          "full record prefix/trailer compression type mismatch at " +
              MakeFileOffsetString(rec_offset));
    }

    *payload = Slice(*scratch);
    if (record_compression_type) {
      *record_compression_type = trailer_compression_type;
    }

    if (rec_type == kBlogFooterLocatorRecord) {
      s = RequireExactEOF(rec_offset);
      if (!s.ok()) {
        return s;
      }
    }
  }

  s = ValidateRecordSequence(rec_type, rec_offset);
  if (!s.ok()) {
    return s;
  }
  *type = rec_type;

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

Status BlogFileReader::ValidateCompressionType(uint8_t compression_type,
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

Status BlogFileReader::ScanForEscapeSequence(uint64_t* found_offset) {
  // Scan forward 4 bytes at a time (escape sequences are 32-bit aligned).
  // Check first 4 bytes of escape sequence, then verify remaining 6 bytes.
  uint32_t first4;
  memcpy(&first4, header_.escape_sequence, 4);

  std::string buf;
  // Align to 4-byte boundary first
  uint64_t logical_offset =
      offset_ - lossless_cast<uint64_t>(pending_input_.size());
  size_t remainder = logical_offset % kBlogMinRecordAlignment;
  if (remainder != 0) {
    size_t skip = kBlogMinRecordAlignment - remainder;
    Slice skip_slice;
    Status s = ReadBytes(skip, &skip_slice, &buf);
    if (!s.ok()) {
      if (s.IsIncomplete()) {
        return Status::NotFound("escape sequence not found before EOF at " +
                                MakeFileOffsetString(current_offset()));
      }
      return AppendContext(s, "while aligning scan for escape sequence");
    }
  }

  while (!eof_) {
    const uint64_t candidate_offset =
        offset_ - lossless_cast<uint64_t>(pending_input_.size());
    Slice chunk_slice;
    Status s = ReadBytes(kBlogMinRecordAlignment, &chunk_slice, &buf);
    if (!s.ok()) {
      if (s.IsIncomplete()) {
        return Status::NotFound("escape sequence not found before EOF at " +
                                MakeFileOffsetString(current_offset()));
      }
      return AppendContext(s, "while scanning for escape sequence");
    }

    uint32_t candidate;
    memcpy(&candidate, chunk_slice.data(), 4);
    if (candidate == first4) {
      // Potential match - verify remaining 6 bytes
      Slice rest_slice;
      std::string rest_buf;
      s = ReadBytes(kBlogEscapeSequenceSize - 4, &rest_slice, &rest_buf);
      if (!s.ok()) {
        if (s.IsIncomplete()) {
          return Status::NotFound("escape sequence not found before EOF at " +
                                  MakeFileOffsetString(current_offset()));
        }
        return AppendContext(s,
                             "while verifying candidate escape sequence at " +
                                 MakeFileOffsetString(candidate_offset));
      }

      if (memcmp(rest_slice.data(), header_.escape_sequence + 4,
                 kBlogEscapeSequenceSize - 4) == 0) {
        PrependPendingInput(rest_slice);
        PrependPendingInput(chunk_slice);
        *found_offset = candidate_offset;
        return Status::OK();
      }
      // Advance only by the four-byte aligned candidate prefix. Put the six
      // lookahead bytes back so an overlapping aligned escape sequence is not
      // skipped.
      PrependPendingInput(rest_slice);
    }
  }
  return Status::NotFound("escape sequence not found before EOF at " +
                          MakeFileOffsetString(current_offset()));
}

Status BlogFileReader::SkipPadding() {
  // Advance past the padding gap (0x00 or 0xFF bytes) to the next record.
  // Padding carries no semantic content and the writer constructs it
  // correctly, so gap bytes are skipped without validating uniformity or any
  // first/mandatory byte. Records only start aligned, so a non-padding byte
  // anywhere else is reported immediately; whatever follows at an aligned
  // offset is left for record parsing to authenticate.
  // Torn padding returns Incomplete so the caller retries once the file
  // grows; only an exact end of file is clean (NotFound).
  //
  // Align first with an exact-sized read: record starts are always aligned,
  // so the bytes to the next boundary are required (a short read is torn),
  // while at an aligned offset zero gap bytes are legal. A fixed-size chunk
  // read from an unaligned position cannot make that distinction.
  const size_t remainder = current_offset() % kBlogMinRecordAlignment;
  if (remainder != 0) {
    Slice pad_slice;
    std::string pad_buf;
    Status s =
        ReadBytes(kBlogMinRecordAlignment - remainder, &pad_slice, &pad_buf);
    if (!s.ok() && !s.IsIncomplete()) {
      return AppendContext(s, "while aligning padding scan");
    }
    size_t i = 0;
    while (i < pad_slice.size()) {
      const uint8_t byte = lossless_cast<uint8_t>(pad_slice[i]);
      if (byte != 0x00 && byte != 0xFF) {
        break;
      }
      ++i;
    }
    if (i < pad_slice.size()) {
      // Garbage before the boundary: records can only start aligned, so no
      // record the parse could accept begins here. Report immediately.
      return ReportCorruption(
          pad_slice.size() - i,
          "non-padding byte before aligned record boundary at " +
              MakeFileOffsetString(current_offset() - pad_slice.size() + i));
    }
    if (s.IsIncomplete()) {
      return Status::Incomplete("torn padding at " +
                                MakeFileOffsetString(current_offset()));
    }
  }

  while (true) {
    Slice chunk_slice;
    std::string chunk_buf;
    Status s = ReadBytes(kBlogMinRecordAlignment, &chunk_slice, &chunk_buf);
    if (!s.ok() && !s.IsIncomplete()) {
      return AppendContext(s, "while scanning padding");
    }
    size_t i = 0;
    while (i < chunk_slice.size()) {
      const uint8_t byte = lossless_cast<uint8_t>(chunk_slice[i]);
      if (byte != 0x00 && byte != 0xFF) {
        break;
      }
      ++i;
    }
    if (i < chunk_slice.size()) {
      if (i > 0) {
        // The chunk began at an aligned offset, so this byte is unaligned
        // and no record the parse could accept begins here. Report
        // immediately, as before the boundary above; growth cannot repair a
        // byte that is neither padding nor a record start.
        return ReportCorruption(
            chunk_slice.size() - i,
            "non-padding byte inside alignment gap at " +
                MakeFileOffsetString(current_offset() - chunk_slice.size() +
                                     i));
      }
      // Next record (or aligned garbage for the record parse to reject).
      // Stashing a torn escape likewise leaves the Incomplete to the record
      // read.
      PrependPendingInput(
          Slice(chunk_slice.data() + i, chunk_slice.size() - i));
      return Status::OK();
    }
    if (!chunk_slice.empty()) {
      if (s.IsIncomplete()) {
        return Status::Incomplete("torn padding at " +
                                  MakeFileOffsetString(current_offset()));
      }
      continue;
    }
    return Status::NotFound("padding extends to EOF at " +
                            MakeFileOffsetString(current_offset()));
  }
}

Status BlogFileReader::RequireExactEOF(uint64_t record_offset) {
  Slice extra_slice;
  std::string extra_buf;
  Status s = ReadBytes(1, &extra_slice, &extra_buf);
  if (s.ok()) {
    return ReportCorruption(
        1, "footer locator is not the last byte sequence in the file at " +
               MakeFileOffsetString(record_offset));
  }
  if (s.IsIncomplete() && extra_slice.empty()) {
    return Status::OK();
  }
  return AppendContext(s, "while checking for data after footer locator at " +
                              MakeFileOffsetString(record_offset));
}

Status BlogFileReader::ReadBytes(size_t n, Slice* result,
                                 std::string* scratch) {
  scratch->clear();
  size_t copied = 0;
  if (!pending_input_.empty()) {
    copied = std::min(n, pending_input_.size());
    scratch->resize(copied);
    memcpy(scratch->data(), pending_input_.data(), copied);
    pending_input_.erase(0, copied);
  }
  if (copied == n) {
    *result = Slice(scratch->data(), n);
    return Status::OK();
  }

  Status s =
      ReadInChunks(seq_file_.get(), scratch, n - copied, &offset_, &eof_);
  if (!s.ok()) {
    return s;
  }
  *result = Slice(scratch->data(), n);
  return Status::OK();
}

Status BlogFileReader::ReadInChunks(SequentialFileReader* file,
                                    std::string* scratch, size_t remaining,
                                    uint64_t* offset, bool* eof) {
  // Initial file-read chunk size. Reads smaller than this complete in a
  // single iteration, exactly like a direct read, so multi-megabyte sized
  // payloads cost one underlying read; the chunk doubles per round so larger
  // reads copy linearly. Chunking (instead of pre-sizing the full request)
  // bounds a corrupt huge length to what is actually present: allocation
  // grows with bytes read and stops at the first short read.
  constexpr size_t kInitialChunkSize = 8 << 20;
  size_t chunk = kInitialChunkSize;
  while (remaining > 0) {
    const size_t want = std::min(remaining, chunk);
    const size_t have = scratch->size();
    scratch->resize(have + want);
    Slice read_result;
    IOStatus ios =
        file->Read(want, &read_result, scratch->data() + have, Env::IO_TOTAL);
    if (!ios.ok()) {
      return AppendContext(AppendFileOffset(static_cast<Status>(ios), *offset),
                           "while reading " + std::to_string(want) + " bytes");
    }
    if (read_result.size() < want) {
      *eof = true;
      *offset += lossless_cast<uint64_t>(read_result.size());
      scratch->resize(have + read_result.size());
      // Return Incomplete -- the caller (ReadRecord) determines whether
      // this is an acceptable incomplete tail vs. genuine corruption.
      return Status::Incomplete(
          "Blog reader: unexpected EOF at " + MakeFileOffsetString(*offset) +
          " while reading " + std::to_string(remaining) + " bytes");
    }
    if (read_result.data() != scratch->data() + have) {
      memcpy(scratch->data() + have, read_result.data(), read_result.size());
    }
    *offset += want;
    remaining -= want;
    if (chunk < remaining && chunk <= SIZE_MAX / 2) {
      chunk *= 2;
    }
  }
  return Status::OK();
}

Status BlogFileReader::ReadPrefixVarint64(uint64_t* value,
                                          std::string* scratch) {
  // Prefix varint: first byte determines the total length. Read the first
  // byte, then read exactly the additional bytes needed. The raw bytes are
  // stored in scratch for checksum verification.
  scratch->clear();
  std::string byte_buf;
  Slice first_slice;
  Status s = ReadBytes(1, &first_slice, &byte_buf);
  if (!s.ok()) {
    return s;
  }
  char first_byte = first_slice[0];
  scratch->push_back(first_byte);

  uint32_t addl = PrefixVarint64AddlByteCount(first_byte);
  if (addl > 0) {
    Slice addl_slice;
    std::string addl_buf;
    s = ReadBytes(addl, &addl_slice, &addl_buf);
    if (!s.ok()) {
      return s;
    }
    scratch->append(addl_slice.data(), addl_slice.size());
  }

  if (!DecodePrefixVarint64(first_byte, scratch->data() + 1,
                            scratch->size() - 1, value)) {
    return ReportCorruption(scratch->size(),
                            "invalid prefix varint ending at " +
                                MakeFileOffsetString(current_offset()));
  }
  return Status::OK();
}

void BlogFileReader::PrependPendingInput(const Slice& input) {
  pending_input_.insert(0, input.data(), input.size());
}

Status BlogFileReader::ReadUnspecifiedSizeRecord(
    Slice* payload, std::string* scratch, uint64_t record_offset,
    BlogRecordType record_type, CompressionType prefix_compression_type,
    CompressionType* record_compression_type) {
  assert(payload != nullptr);
  assert(scratch != nullptr);

  constexpr size_t kReadChunkSize = 4;
  const uint64_t payload_file_offset = current_offset();
  scratch->clear();

  // scratch is payload-relative: scratch[i] corresponds to file offset
  // payload_file_offset + i. Only 4-byte-aligned file offsets can start the
  // next record, so candidate_offset is always advanced in 4-byte steps.
  // Each iteration appends one 4-byte chunk. After the initial warmup reads,
  // that makes exactly one new aligned candidate fully available to examine.
  size_t candidate_offset = lossless_cast<size_t>(
      BitwiseAnd(uint64_t{0} - payload_file_offset, uint64_t{3}));
  bool saw_checksum_mismatch = false;
  size_t candidate_count = 0;
  uint64_t last_candidate_escape_sequence_offset = 0;
  size_t last_candidate_payload_size = 0;
  uint32_t last_stored_checksum = 0;
  uint32_t last_computed_checksum = 0;
  while (true) {
    Slice chunk_slice;
    std::string chunk_buf;
    Status s = ReadBytes(kReadChunkSize, &chunk_slice, &chunk_buf);
    if (!s.ok()) {
      if (saw_checksum_mismatch && s.IsIncomplete()) {
        return ReportCorruption(
            scratch->size(),
            MakeUnspecifiedSizeChecksumMismatchMessage(
                record_offset, last_candidate_escape_sequence_offset,
                last_candidate_payload_size, last_stored_checksum,
                last_computed_checksum, header_.checksum_type,
                candidate_count));
      }
      if (s.IsIncomplete()) {
        eof_ = true;
      }
      return AppendContext(
          s, "while reading unspecified-size record starting at " +
                 MakeFileOffsetString(record_offset));
    }
    scratch->append(chunk_slice.data(), chunk_slice.size());

    if (candidate_offset + kBlogEscapeSequenceSize > scratch->size()) {
      // Keep reading until the current aligned candidate is fully buffered.
      continue;
    }

    if (memcmp(scratch->data() + candidate_offset, header_.escape_sequence,
               kBlogEscapeSequenceSize) != 0) {
      // This aligned position is not an escape-sequence candidate.
      candidate_offset += 4;
      continue;
    }

    // If padding exists, it is exactly the trailing run of 0x00 or 0xFF
    // immediately before the next escape sequence. The 5-byte trailer must
    // sit immediately before that run.
    size_t pad_len = 0;
    if (candidate_offset > 0) {
      uint8_t pad_byte =
          lossless_cast<uint8_t>((*scratch)[candidate_offset - 1]);
      if (pad_byte == 0x00 || pad_byte == 0xFF) {
        pad_len = 1;
        while (pad_len < candidate_offset &&
               lossless_cast<uint8_t>(
                   (*scratch)[candidate_offset - 1 - pad_len]) == pad_byte) {
          ++pad_len;
        }
      }
    }

    if (candidate_offset < pad_len + kBlogBlockTrailerSize) {
      // This candidate leaves no room for the required 5-byte trailer.
      candidate_offset += 4;
      continue;
    }

    const size_t trailer_pos =
        candidate_offset - pad_len - kBlogBlockTrailerSize;
    const size_t payload_size = trailer_pos;
    ++candidate_count;
    last_candidate_escape_sequence_offset =
        payload_file_offset + candidate_offset;
    const char* trailer = scratch->data() + trailer_pos;
    const uint32_t stored_checksum = DecodeFixed32(trailer + 1);
    const uint32_t computed_checksum = ComputeBlogRecordChecksum(
        header_.checksum_type, scratch->data(), payload_size, trailer[0],
        header_.incarnation_id(), payload_file_offset);
    last_candidate_payload_size = payload_size;
    last_stored_checksum = stored_checksum;
    last_computed_checksum = computed_checksum;
    if (stored_checksum != computed_checksum) {
      saw_checksum_mismatch = true;
      if (candidate_count > kBlogMaxUnspecifiedSizeScanCandidates) {
        return ReportCorruption(
            candidate_offset + kBlogEscapeSequenceSize,
            MakeUnspecifiedSizeChecksumMismatchMessage(
                record_offset, last_candidate_escape_sequence_offset,
                last_candidate_payload_size, last_stored_checksum,
                last_computed_checksum, header_.checksum_type,
                candidate_count));
      }
      // An escape sequence can legitimately occur in the payload. Continue
      // until a candidate is authenticated by the trailer checksum, EOF is
      // reached, or kBlogMaxUnspecifiedSizeScanCandidates is exceeded.
      candidate_offset += 4;
      continue;
    }

    const uint8_t trailer_compression_byte = lossless_cast<uint8_t>(trailer[0]);
    Status validation = ValidateCompressionType(trailer_compression_byte,
                                                record_type, record_offset);
    if (!validation.ok()) {
      return validation;
    }
    const CompressionType trailer_compression_type =
        lossless_cast<CompressionType>(trailer_compression_byte);
    if (trailer_compression_type != prefix_compression_type) {
      return ReportCorruption(
          candidate_offset + kBlogEscapeSequenceSize,
          "unspecified-size record prefix/trailer compression type mismatch "
          "at " +
              MakeFileOffsetString(record_offset));
    }

    PrependPendingInput(Slice(scratch->data() + candidate_offset,
                              scratch->size() - candidate_offset));
    scratch->resize(payload_size);
    *payload = Slice(*scratch);
    if (record_compression_type) {
      *record_compression_type = trailer_compression_type;
    }
    return Status::OK();
  }
}

Status BlogFileReader::ReportCorruption(size_t bytes, const std::string& msg) {
  Status s = Status::Corruption("Blog reader", msg);
  if (reporter_) {
    reporter_->Corruption(bytes, s);
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
