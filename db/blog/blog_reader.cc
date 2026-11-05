//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blog/blog_reader.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <vector>

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

std::string FormatCandidatePayloadSizes(
    const std::vector<size_t>& candidate_payload_sizes) {
  std::string result = "[";
  for (size_t i = 0; i < candidate_payload_sizes.size(); ++i) {
    if (i > 0) {
      result += ",";
    }
    result += std::to_string(candidate_payload_sizes[i]);
  }
  result += "]";
  return result;
}

std::string MakeUnspecifiedSizeChecksumMismatchMessage(
    uint64_t record_offset, uint64_t candidate_escape_sequence_offset,
    size_t candidate_payload_size, uint32_t stored_checksum,
    uint32_t computed_checksum, ChecksumType checksum_type,
    const std::vector<size_t>& candidate_payload_sizes) {
  return "unspecified-size record checksum mismatch: stored = " +
         std::to_string(stored_checksum) +
         ", computed = " + std::to_string(computed_checksum) +
         ", type = " + std::to_string(static_cast<int>(checksum_type)) +
         " for record at file offset " + std::to_string(record_offset) +
         " at candidate escape sequence file offset " +
         std::to_string(candidate_escape_sequence_offset) +
         ", candidate payload size " + std::to_string(candidate_payload_size) +
         "; candidate payload sizes checked=" +
         FormatCandidatePayloadSizes(candidate_payload_sizes);
}

std::string MakeUnspecifiedSizeChecksumMismatchAtEOFMessage(
    uint64_t record_offset, uint64_t scan_limit_offset,
    size_t candidate_payload_size, uint32_t stored_checksum,
    uint32_t computed_checksum, ChecksumType checksum_type,
    const std::vector<size_t>& candidate_payload_sizes) {
  return "unspecified-size record checksum mismatch: stored = " +
         std::to_string(stored_checksum) +
         ", computed = " + std::to_string(computed_checksum) +
         ", type = " + std::to_string(static_cast<int>(checksum_type)) +
         " for record at file offset " + std::to_string(record_offset) +
         " while scanning up to file offset " +
         std::to_string(scan_limit_offset) + ", last candidate payload size " +
         std::to_string(candidate_payload_size) +
         "; candidate payload sizes checked=" +
         FormatCandidatePayloadSizes(candidate_payload_sizes);
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
  const uint64_t fixed_header_offset = offset_;
  std::string fixed_buf;
  Slice fixed_slice;
  Status s = ReadBytes(kBlogFileFixedHeaderSize, &fixed_slice, &fixed_buf);
  if (!s.ok()) {
    return AppendContext(s, "while reading blog fixed header");
  }

  // Peek at property_section_size from bytes [32-35]
  uint32_t prop_section_size = DecodeFixed32(fixed_slice.data() + 32);

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

  // Skip padding after header to reach the first record's escape sequence.
  // Padding may be arbitrarily long (for stronger alignment or recycled
  // files of all zeros). EOF here is fine (header-only file, no records).
  s = SkipPadding();
  if (!s.ok() && !s.IsNotFound()) {
    return AppendContext(s, "while skipping padding after blog header");
  }
  return Status::OK();
}

Status BlogFileReader::ReadRecord(BlogRecordType* type, Slice* payload,
                                  std::string* scratch, uint64_t* record_offset,
                                  CompressionType* record_compression_type) {
  if (!header_read_) {
    return Status::Corruption("Blog reader: header not read at " +
                              MakeFileOffsetString(offset_));
  }
  if (eof_) {
    return Status::NotFound("Blog reader: clean EOF at " +
                            MakeFileOffsetString(offset_));
  }

  Status s;
  scratch->clear();

  std::string esc_buf;
  const bool had_pending_input = !pending_input_.empty();
  const uint64_t escape_sequence_offset =
      offset_ - lossless_cast<uint64_t>(pending_input_.size());
  Slice esc_slice;
  s = ReadBytes(kBlogEscapeSequenceSize, &esc_slice, &esc_buf);
  if (!s.ok()) {
    if (s.IsIncomplete()) {
      eof_ = true;
      if (had_pending_input) {
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
    assert(varint_len == 1);
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

    *type = rec_type;
    payload->clear();
    if (record_compression_type) {
      *record_compression_type = kNoCompression;
    }
    s = SkipPadding();
    if (!s.ok() && !s.IsNotFound()) {
      return AppendContext(s,
                           "while skipping padding after trivial record at " +
                               MakeFileOffsetString(rec_offset));
    }
    return Status::OK();
  }

  bool is_full_format = (varint_len > kBlogCompactVarintMaxBytes);

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

  *type = rec_type;

  if (length == kBlogUnspecifiedSize) {
    s = ReadUnspecifiedSizeRecord(payload, scratch, rec_offset,
                                  record_compression_type);
    if (!s.ok()) {
      return s;
    }
  } else {
    // Read payload
    scratch->clear();
    Slice payload_slice;
    s = ReadBytes(static_cast<size_t>(length), &payload_slice, scratch);
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
      const uint64_t payload_file_offset =
          offset_ - kBlogBlockTrailerSize - length;
      uint32_t computed_checksum = ComputeBlogRecordChecksum(
          header_.checksum_type, scratch->data(), scratch->size(),
          trailer_comp_type, header_.incarnation_id(), payload_file_offset);
      if (stored_checksum != computed_checksum) {
        return ReportCorruption(
            static_cast<size_t>(length) + kBlogBlockTrailerSize,
            MakeRecordChecksumMismatchMessage(
                "record checksum mismatch", stored_checksum, computed_checksum,
                header_.checksum_type, rec_offset,
                lossless_cast<size_t>(length)));
      }
    }

    *payload = Slice(*scratch);
    if (record_compression_type) {
      *record_compression_type = static_cast<CompressionType>(
          lossless_cast<uint8_t>(trailer_comp_type));
    }

    // Skip padding to next record.
    s = SkipPadding();
    if (!s.ok() && !s.IsNotFound()) {
      return AppendContext(s, "while skipping padding after record at " +
                                  MakeFileOffsetString(rec_offset));
    }
  }

  // Silently skip ignorable properties records -- they carry diagnostic
  // data that readers don't need for correctness.
  if (*type == kBlogIgnorablePropertiesRecord) {
    return ReadRecord(type, payload, scratch, record_offset,
                      record_compression_type);
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
  size_t remainder = offset_ % 4;
  if (remainder != 0) {
    size_t skip = 4 - remainder;
    Slice skip_slice;
    Status s = ReadBytes(skip, &skip_slice, &buf);
    if (!s.ok()) {
      if (s.IsIncomplete()) {
        return Status::NotFound("escape sequence not found before EOF at " +
                                MakeFileOffsetString(offset_));
      }
      return AppendContext(s, "while aligning scan for escape sequence");
    }
  }

  while (!eof_) {
    Slice chunk_slice;
    Status s = ReadBytes(4, &chunk_slice, &buf);
    if (!s.ok()) {
      if (s.IsIncomplete()) {
        return Status::NotFound("escape sequence not found before EOF at " +
                                MakeFileOffsetString(offset_));
      }
      return AppendContext(s, "while scanning for escape sequence");
    }

    uint32_t candidate;
    memcpy(&candidate, chunk_slice.data(), 4);
    if (candidate == first4) {
      // Potential match - verify remaining 6 bytes
      uint64_t candidate_offset = offset_ - 4;
      Slice rest_slice;
      std::string rest_buf;
      s = ReadBytes(kBlogEscapeSequenceSize - 4, &rest_slice, &rest_buf);
      if (!s.ok()) {
        if (s.IsIncomplete()) {
          return Status::NotFound("escape sequence not found before EOF at " +
                                  MakeFileOffsetString(offset_));
        }
        return AppendContext(s,
                             "while verifying candidate escape sequence at " +
                                 MakeFileOffsetString(candidate_offset));
      }

      if (memcmp(rest_slice.data(), header_.escape_sequence + 4,
                 kBlogEscapeSequenceSize - 4) == 0) {
        *found_offset = candidate_offset;
        return Status::OK();
      }
      // False match, continue scanning. But we consumed 6 extra bytes that
      // might not be aligned. Re-align.
      remainder = offset_ % 4;
      if (remainder != 0) {
        size_t skip = 4 - remainder;
        Slice skip_s;
        s = ReadBytes(skip, &skip_s, &buf);
        if (!s.ok()) {
          if (s.IsIncomplete()) {
            return Status::NotFound("escape sequence not found before EOF at " +
                                    MakeFileOffsetString(offset_));
          }
          return AppendContext(s, "while re-aligning scan after candidate at " +
                                      MakeFileOffsetString(candidate_offset));
        }
      }
    }
  }
  return Status::NotFound("escape sequence not found before EOF at " +
                          MakeFileOffsetString(offset_));
}

Status BlogFileReader::SkipPadding() {
  // Skip arbitrarily long padding (0x00 or 0xFF bytes). Padding is
  // 32-bit aligned. Since the escape sequence's first byte is guaranteed
  // to be neither 0x00 nor 0xFF, we align to 4 bytes then scan 4-byte
  // chunks until we find one that starts with a non-padding byte.
  size_t remainder = offset_ % 4;
  if (remainder != 0) {
    Slice skip_slice;
    std::string skip_buf;
    Status s = ReadBytes(4 - remainder, &skip_slice, &skip_buf);
    if (!s.ok()) {
      if (s.IsIncomplete()) {
        return Status::NotFound("padding extends to EOF at " +
                                MakeFileOffsetString(offset_));
      }
      return AppendContext(s, "while aligning padding scan");
    }
  }
  while (true) {
    Slice chunk_slice;
    std::string chunk_buf;
    Status s = ReadBytes(4, &chunk_slice, &chunk_buf);
    if (!s.ok()) {
      if (s.IsIncomplete()) {
        return Status::NotFound("padding extends to EOF at " +
                                MakeFileOffsetString(offset_));
      }
      return AppendContext(s, "while scanning padding");
    }
    uint8_t first = static_cast<uint8_t>(chunk_slice[0]);
    if (first != 0x00 && first != 0xFF) {
      // Found the start of the next escape sequence.
      pending_input_.assign(chunk_slice.data(), 4);
      return Status::OK();
    }
  }
}

Status BlogFileReader::ReadBytes(size_t n, Slice* result,
                                 std::string* scratch) {
  scratch->resize(n);
  size_t copied = 0;
  if (!pending_input_.empty()) {
    copied = std::min(n, pending_input_.size());
    memcpy(scratch->data(), pending_input_.data(), copied);
    pending_input_.erase(0, copied);
  }
  if (copied == n) {
    *result = Slice(scratch->data(), n);
    return Status::OK();
  }

  Slice read_result;
  IOStatus ios = seq_file_->Read(n - copied, &read_result,
                                 scratch->data() + copied, Env::IO_TOTAL);
  if (!ios.ok()) {
    return AppendContext(
        AppendFileOffset(static_cast<Status>(ios), offset_),
        "while reading " + std::to_string(n - copied) + " bytes");
  }
  if (read_result.size() < n - copied) {
    eof_ = true;
    // Return Incomplete -- the caller (ReadRecord) determines whether
    // this is an acceptable incomplete tail vs. genuine corruption.
    return Status::Incomplete(
        "Blog reader: unexpected EOF at " +
        MakeFileOffsetString(offset_ +
                             lossless_cast<uint64_t>(read_result.size())) +
        " while reading " + std::to_string(n) + " bytes");
  }
  if (read_result.data() != scratch->data() + copied) {
    memcpy(scratch->data() + copied, read_result.data(), read_result.size());
  }
  offset_ += n - copied;
  *result = Slice(scratch->data(), n);
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
    return ReportCorruption(
        scratch->size(),
        "invalid prefix varint ending at " + MakeFileOffsetString(offset_));
  }
  return Status::OK();
}

Status BlogFileReader::ReadUnspecifiedSizeRecord(
    Slice* payload, std::string* scratch, uint64_t record_offset,
    CompressionType* record_compression_type) {
  assert(payload != nullptr);
  assert(scratch != nullptr);

  constexpr size_t kReadChunkSize = 4;
  constexpr uint32_t kMaxEscapeSequenceLookaheadOnChecksumMismatch = 3;
  const uint64_t payload_file_offset = offset_;
  scratch->clear();

  // scratch is payload-relative: scratch[i] corresponds to file offset
  // payload_file_offset + i. Only 4-byte-aligned file offsets can start the
  // next record, so candidate_offset is always advanced in 4-byte steps.
  // Each iteration appends one 4-byte chunk. After the initial warmup reads,
  // that makes exactly one new aligned candidate fully available to examine.
  size_t candidate_offset = lossless_cast<size_t>(
      BitwiseAnd(uint64_t{0} - payload_file_offset, uint64_t{3}));
  bool saw_checksum_mismatch = false;
  uint32_t remaining_checksum_mismatch_lookahead = 0;
  std::vector<size_t> candidate_payload_sizes_checked;
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
            MakeUnspecifiedSizeChecksumMismatchAtEOFMessage(
                record_offset, payload_file_offset + scratch->size(),
                last_candidate_payload_size, last_stored_checksum,
                last_computed_checksum, header_.checksum_type,
                candidate_payload_sizes_checked));
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
    candidate_payload_sizes_checked.push_back(payload_size);
    const char* trailer = scratch->data() + trailer_pos;
    const uint32_t stored_checksum = DecodeFixed32(trailer + 1);
    const uint32_t computed_checksum = ComputeBlogRecordChecksum(
        header_.checksum_type, scratch->data(), payload_size, trailer[0],
        header_.incarnation_id(), payload_file_offset);
    last_candidate_payload_size = payload_size;
    last_stored_checksum = stored_checksum;
    last_computed_checksum = computed_checksum;
    if (stored_checksum != computed_checksum) {
      if (!saw_checksum_mismatch) {
        saw_checksum_mismatch = true;
        remaining_checksum_mismatch_lookahead =
            kMaxEscapeSequenceLookaheadOnChecksumMismatch;
      } else if (remaining_checksum_mismatch_lookahead == 0) {
        return ReportCorruption(
            candidate_offset + kBlogEscapeSequenceSize,
            MakeUnspecifiedSizeChecksumMismatchMessage(
                record_offset, payload_file_offset + candidate_offset,
                payload_size, stored_checksum, computed_checksum,
                header_.checksum_type, candidate_payload_sizes_checked));
      } else {
        --remaining_checksum_mismatch_lookahead;
      }
      // Treat this as a likely false positive inside the payload until the
      // bounded checksum-mismatch lookahead is exhausted.
      candidate_offset += 4;
      continue;
    }

    pending_input_.assign(scratch->data() + candidate_offset,
                          scratch->size() - candidate_offset);
    scratch->resize(payload_size);
    *payload = Slice(*scratch);
    if (record_compression_type) {
      *record_compression_type =
          static_cast<CompressionType>(lossless_cast<uint8_t>(trailer[0]));
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
