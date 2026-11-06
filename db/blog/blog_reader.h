//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "db/blog/blog_format.h"
#include "file/random_access_file_reader.h"
#include "file/sequence_file_reader.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// Maximum number of escape-sequence candidates examined while scanning for
// the end of one unspecified-size record. Past this many rejections the
// record is declared torn: embedded escapes are 2^-80 per position in
// non-adversarial data, so a healthy record never gets here. The bound keeps
// torn scans from consuming the rest of the file, which would cost a
// full-payload checksum per later record plus a whole-remainder rewind.
constexpr size_t kBlogMaxUnspecifiedSizeScanCandidates = 64;

// Reads blog-format files sequentially. Random blob reads and backward footer
// discovery use the format helpers and their respective file readers.
class BlogFileReader {
 public:
  // Error reporter, same pattern as log::Reader::Reporter.
  class Reporter {
   public:
    virtual ~Reporter() = default;
    // `bytes` is the approximate number of bytes logically dropped because the
    // reader could not continue past the corruption, matching
    // log::Reader::Reporter semantics.
    virtual void Corruption(size_t bytes, const Status& status) = 0;
  };

  // Construct for sequential reading (WAL replay, forward scan).
  BlogFileReader(std::unique_ptr<SequentialFileReader>&& file,
                 Reporter* reporter, bool verify_checksums);
  // Construct after `initial_data` has already been read from `file`.
  BlogFileReader(std::unique_ptr<SequentialFileReader>&& file,
                 Reporter* reporter, bool verify_checksums,
                 std::string&& initial_data);

  ~BlogFileReader() = default;

  // Take ownership of a reporter (e.g., an adapter wrapping another type).
  void SetOwnedReporter(std::unique_ptr<Reporter> reporter) {
    owned_reporter_ = std::move(reporter);
    reporter_ = owned_reporter_.get();
  }

  // Read and validate the file header. Must be called first. Stops at the
  // start of the padding after the header; the next ReadRecord() skips that
  // padding before reading the first record.
  Status ReadHeader(BlogFileHeader* header);

  // Read the next record sequentially. Returns:
  //   OK - record read successfully
  //   NotFound - clean EOF (no more records, only padding/zeros remain)
  //   Incomplete - partial record at tail (crash mid-write)
  //   Corruption - data integrity error
  // On OK, *type is the record type and *payload contains the payload.
  // *scratch is used as backing storage for payload.
  Status ReadRecord(BlogRecordType* type, Slice* payload, std::string* scratch,
                    uint64_t* record_offset = nullptr,
                    CompressionType* record_compression_type = nullptr);

  // Scan forward from the current position for the next occurrence of
  // the file's escape sequence. Used for WAL recovery and unspecified-
  // size record parsing.
  // On success, *found_offset is the file offset of the escape sequence and
  // the sequence remains logically unconsumed, so the next ReadRecord() starts
  // at that escape sequence.
  Status ScanForEscapeSequence(uint64_t* found_offset);

  bool IsEOF() const { return eof_; }
  void UnmarkEOF() { eof_ = false; }
  uint64_t LastRecordOffset() const { return last_record_offset_; }
  uint64_t current_offset() const { return offset_ - pending_input_.size(); }
  SequentialFileReader* file() { return seq_file_.get(); }
  const BlogFileHeader& header() const { return header_; }

 private:
  // Read one record, including ignorable-properties records that the public
  // ReadRecord() interface skips.
  Status ReadOneRecord(BlogRecordType* type, Slice* payload,
                       std::string* scratch, uint64_t* record_offset,
                       CompressionType* record_compression_type);

  // Advance past the padding gap (0x00 or 0xFF bytes) to the next record,
  // stashing the first non-padding bytes in pending_input_ for the record
  // read that follows. Gap bytes are skipped without validation; record
  // parsing authenticates whatever follows. Returns OK once positioned at
  // the next record, NotFound on clean EOF (zero gap bytes are legal at an
  // aligned offset), Incomplete on a torn gap, Corruption on garbage before
  // an alignment boundary, or an error Status on I/O failure.
  Status SkipPadding();

  // Require the current logical position to be the exact end of the file.
  Status RequireExactEOF(uint64_t record_offset);

  // Validate record-level semantics that do not depend on the payload.
  Status ValidateRecordType(BlogRecordType type, uint64_t length,
                            size_t varint_len, uint64_t record_offset);
  Status ValidateCompressionType(uint8_t compression_type,
                                 BlogRecordType record_type,
                                 uint64_t record_offset);

  // Validate and advance the body/footer record-order state.
  Status ValidateRecordSequence(BlogRecordType type, uint64_t record_offset);

  // Read exactly n bytes sequentially. Returns Incomplete on a short read and
  // makes the partial bytes available through result/scratch.
  Status ReadBytes(size_t n, Slice* result, std::string* scratch);

  // Read `remaining` bytes from `file`, appending them to `scratch` in
  // progressively larger chunks. Advances *offset by the bytes read and
  // sets *eof if EOF is hit first. Returns Incomplete on short read; the
  // caller decides whether that is an acceptable tail or corruption.
  static Status ReadInChunks(SequentialFileReader* file, std::string* scratch,
                             size_t remaining, uint64_t* offset, bool* eof);

  // Read a prefix varint64 from the sequential stream.
  Status ReadPrefixVarint64(uint64_t* value, std::string* scratch);

  void PrependPendingInput(const Slice& input);

  // Read the payload/trailer/padding of a full-format unspecified-size record
  // by scanning forward for aligned candidate escape sequences. For each
  // candidate, derive the only possible padding length (the exact trailing run
  // of 0x00/0xFF immediately before it) and validate the trailer checksum.
  // Embedded escape sequences are skipped until a candidate is authenticated
  // by its trailer checksum.
  Status ReadUnspecifiedSizeRecord(Slice* payload, std::string* scratch,
                                   uint64_t record_offset,
                                   BlogRecordType record_type,
                                   CompressionType prefix_compression_type,
                                   CompressionType* record_compression_type);

  // Report corruption and return a Corruption status. `bytes` is the
  // approximate number of bytes logically dropped because parsing cannot
  // continue past the corruption.
  Status ReportCorruption(size_t bytes, const std::string& msg);

  std::unique_ptr<SequentialFileReader> seq_file_;
  Reporter* reporter_;
  std::unique_ptr<Reporter> owned_reporter_;  // optional, for adapter lifetime
  bool verify_checksums_;
  BlogFileHeader header_;
  bool header_read_ = false;
  bool has_compression_compatibility_name_ = false;
  bool has_streaming_compression_type_ = false;
  bool eof_ = false;
  uint64_t offset_ = 0;
  uint64_t last_record_offset_ = 0;

  bool footer_started_ = false;
  bool footer_properties_seen_ = false;
  bool footer_checksum_info_seen_ = false;
  bool footer_locator_seen_ = false;

  // Bytes already read from seq_file_ but not yet logically consumed.
  std::string pending_input_;
};

}  // namespace ROCKSDB_NAMESPACE
