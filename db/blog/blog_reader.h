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

// Reads blog-format files sequentially (WAL replay) or at random offsets
// (blob lookups). Supports footer reading by backward scanning.
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

  ~BlogFileReader() = default;

  // Take ownership of a reporter (e.g., an adapter wrapping another type).
  void SetOwnedReporter(std::unique_ptr<Reporter> reporter) {
    owned_reporter_ = std::move(reporter);
    reporter_ = owned_reporter_.get();
  }

  // Read and validate the file header. Must be called first.
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
  // On success, *found_offset is the file offset of the escape sequence.
  Status ScanForEscapeSequence(uint64_t* found_offset);

  bool IsEOF() const { return eof_; }
  uint64_t LastRecordOffset() const { return last_record_offset_; }
  uint64_t current_offset() const { return offset_; }
  const BlogFileHeader& header() const { return header_; }

 private:
  // Skip arbitrarily long padding (0x00 or 0xFF bytes) after a record or
  // header. Aligns to 4-byte boundary first, then scans 4-byte chunks
  // until finding one whose first byte is not 0x00/0xFF. Stashes that
  // chunk in pending_input_ for the next ReadRecord call.
  // Returns OK on success, NotFound on clean EOF (no more records),
  // or an error Status on I/O failure.
  Status SkipPadding();

  // Read exactly n bytes sequentially. Returns Corruption on short read.
  Status ReadBytes(size_t n, Slice* result, std::string* scratch);

  // Read a prefix varint64 from the sequential stream.
  Status ReadPrefixVarint64(uint64_t* value, std::string* scratch);

  // Read the payload/trailer/padding of a full-format unspecified-size record
  // by scanning forward for aligned candidate escape sequences. For each
  // candidate, derive the only possible padding length (the exact trailing run
  // of 0x00/0xFF immediately before it) and validate the trailer checksum.
  // After the first checksum mismatch, only a small fixed number of later
  // escape-sequence candidates are considered before reporting corruption.
  Status ReadUnspecifiedSizeRecord(Slice* payload, std::string* scratch,
                                   uint64_t record_offset,
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
  bool eof_ = false;
  uint64_t offset_ = 0;
  uint64_t last_record_offset_ = 0;

  // Bytes already read from seq_file_ but not yet logically consumed.
  std::string pending_input_;
};

}  // namespace ROCKSDB_NAMESPACE
