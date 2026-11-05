//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "db/blog/blog_format.h"
#include "db/blog/blog_input_window.h"
#include "file/random_access_file_reader.h"
#include "file/sequence_file_reader.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// Whether a failed BlogFileReader::ReadRecord() had positively identified a
// record boundary of this file incarnation before it failed.
enum class BlogReadFailureStage {
  // Garbage, a misaligned match, or damage in the padding gap before the
  // boundary. In a recycled file these bytes may predate this incarnation.
  kEscapeSequenceNotFound,
  // A boundary of this incarnation was identified; later bytes failed.
  kRecordValidationFailed,
};

// Maximum number of escape-sequence candidates examined while scanning for
// the end of one unspecified-size record. Past this many rejections the
// record is declared torn: embedded escapes are 2^-80 per position in
// non-adversarial data, so healthy records never get here.
constexpr size_t kBlogMaxUnspecifiedSizeScanCandidates = 64;

// Largest sized-record payload a reader accepts. Far beyond any real record,
// and small enough that a payload plus any record overhead, read-ahead, or
// file offset stays well below every 64-bit std::string limit and SIZE_MAX.
constexpr uint64_t kBlogMaxRecordPayloadSize = uint64_t{1} << 60;

// Reads blog-format files sequentially. Random blob reads and backward footer
// discovery use the format helpers and their respective file readers.
class BlogFileReader {
 public:
  // Error reporter, same pattern as log::Reader::Reporter.
  class Reporter {
   public:
    virtual ~Reporter() = default;
    // `bytes` is the approximate number of bytes logically dropped because
    // the reader could not continue past the corruption.
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

  // Read and validate the file header. Must be called first.
  Status ReadHeader(BlogFileHeader* header);

  // Read the next record sequentially. Returns:
  //   OK - record read successfully
  //   NotFound - clean EOF (no more records, only padding/zeros remain)
  //   Incomplete - partial record at tail (crash mid-write)
  //   Corruption - data integrity error
  // On OK, *type is the record type and *payload views the payload in the
  // retained window; it stays valid only until the next ReadRecord() call.
  // *scratch is currently unused and left untouched.
  Status ReadRecord(BlogRecordType* type, Slice* payload, std::string* scratch,
                    uint64_t* record_offset = nullptr,
                    CompressionType* record_compression_type = nullptr);

  // Valid only when the last ReadRecord() returned Corruption or NotSupported.
  BlogReadFailureStage last_failure_stage() const { return failure_stage_; }

  // Valid only when the last ReadRecord() failed: whether it failed inside a
  // record whose authenticated prefix names a footer record type. Never set
  // without authenticated lengths, since the type byte is then untrusted, nor
  // for bytes following an intact footer locator.
  bool last_failure_in_footer_record() const {
    return failure_in_footer_record_;
  }

  // Scan forward from the parse cursor for the next aligned occurrence of
  // the file's escape sequence, for WAL recovery after a failed record. The
  // search starts past the failed record's trusted extent when known, else
  // past its matched escape -- or one quantum past the gap when no escape
  // matched; without a known gap the first match wins.
  // On success the cursor advances past the skipped garbage and parks before
  // the boundary for the next ReadRecord(); no advance on failure. Never
  // Incomplete: a torn tail holds no verifiable boundary (NotFound). Recycled
  // refreshable inputs stop at the failed record's format-derived bound when
  // available. Otherwise each call inspects one more 32 KiB chunk, restarting
  // from the failed record after EOF, so NotFound can also mean the scan must
  // continue on a later poll; Refresh() preserves that progress while
  // re-reading the unaccepted frontier. A boundary found past bytes scanned on
  // earlier calls is returned only after the next call rescans them, which
  // can read up to that boundary at once, so success always reports the first
  // boundary past the failed record.
  Status ScanForEscapeSequence(uint64_t* found_offset);

  // Bytes the last ReadRecord() call reported dropped; reset every call
  // (torn tails, clean EOF, and I/O errors leave it zero).
  size_t last_dropped_bytes() const { return last_dropped_bytes_; }

  // Discard unaccepted bytes and reopen the file at the first unaccepted
  // byte, so the next read observes the file's current contents. For inputs
  // whose unaccepted bytes may be overwritten or replaced, such as recycled
  // live WALs. Accepted bytes are never re-read. Requires CanRefresh().
  Status Refresh() { return window_.Refresh(); }
  // Restore a saved parse cursor after a recovery scan, using fresh input.
  // A boundary already found past the record at offset is kept, so scanning
  // again from there returns it without re-reading the skipped bytes.
  Status Refresh(uint64_t offset) { return window_.Refresh(offset); }
  uint64_t current_offset() const { return window_.ParseOffset(); }
  bool CanRefresh() const { return window_.CanReopen(); }

  // End of the last accepted non-footer trailer, or the header trailer.
  uint64_t body_end_offset() const { return body_end_offset_; }
  SequentialFileReader* file() { return window_.file(); }
  const BlogFileHeader& header() const { return header_; }

 private:
  friend class BlogWriterTest;

  // Parse and validate the file header from retained bytes without
  // advancing; *header_size is the caller's advance on success. Incomplete
  // mid-header (the outer layer retries on growth); errors otherwise.
  Status ReadHeaderInternal(BlogFileHeader* header, size_t* header_size);

  // Read one record, including ignorable-properties records that the public
  // ReadRecord() skips. The record starts gap_bytes past the parse cursor;
  // on success the cursor advances past gap and record atomically, and
  // nothing moves on failure.
  Status ReadOneRecord(BlogRecordType* type, Slice* payload,
                       uint64_t* record_offset,
                       CompressionType* record_compression_type,
                       size_t gap_bytes);
  // Dispatch to the record-format reader selected by the length prefix.
  Status ReadRecordBody(uint64_t rec_offset, size_t prefix_rel,
                        size_t varint_len, uint64_t length,
                        BlogRecordType* type, Slice* payload,
                        CompressionType* record_compression_type,
                        size_t* helper_extent);
  // Happy path for a trivial-format record (zero length): type + checksum.
  // prefix_rel is the parse-relative offset past escape sequence and length
  // varint. Reports the post-varint record extent in *helper_extent.
  Status ReadTrivialRecord(uint64_t rec_offset, size_t prefix_rel,
                           size_t varint_len, BlogRecordType* type,
                           Slice* payload,
                           CompressionType* record_compression_type,
                           size_t* helper_extent);
  // Happy path for a compact-format record: payload + trailer with the
  // header's implicit record type. See ReadTrivialRecord for conventions.
  Status ReadCompactRecord(uint64_t rec_offset, size_t prefix_rel,
                           size_t varint_len, uint64_t length,
                           BlogRecordType* type, Slice* payload,
                           CompressionType* record_compression_type,
                           size_t* helper_extent);
  // Happy path for a sized full-format record: shared prefix plus payload
  // and trailer. See ReadTrivialRecord for conventions.
  Status ReadFullFormatRecord(uint64_t rec_offset, size_t prefix_rel,
                              size_t varint_len, uint64_t length,
                              BlogRecordType* type, Slice* payload,
                              CompressionType* record_compression_type,
                              size_t* helper_extent);
  // Shared prefix of sized and unspecified-size full-format records: type,
  // compression type, and prefix checksum, verified when checksums are on.
  Status ReadFullFormatPrefix(uint64_t rec_offset, size_t prefix_rel,
                              size_t varint_len, BlogRecordType* rec_type,
                              CompressionType* prefix_compression_type);
  // Slow path for a torn sized read: an overrunning length is corruption,
  // a clean truncation stays Incomplete. No-op without a length prefix.
  Status ResolveTornRecord(uint64_t rec_offset, size_t gap_bytes,
                           const Status& torn_status);
  // Decode, validate, and merge an ignorable-properties record payload into
  // the header. Runs pre-commit: failures leave the cursor before the record.
  Status AbsorbIgnorableProperties(const Slice& payload, uint64_t rec_offset);

  // Peek past the padding gap (0x00 or 0xFF) to the next record; report
  // its length in *gap_bytes (0 unless OK or NotFound). Gap bytes skip
  // validation; the record parse authenticates what follows (OK covers
  // aligned garbage it will reject). NotFound when the input ends at the
  // last accepted byte or after an aligned gap; Incomplete on a torn gap;
  // Corruption on garbage before an alignment boundary. Peek-only, except
  // that a terminal gap is accepted in files that are not recycled (no
  // record can later start inside it), to bound the window on long zero
  // tails.
  Status SkipPadding(size_t* gap_bytes);
  // SkipPadding() when the gap does not end at a record start inside the
  // retained bytes: extends the input or reports the failure.
  Status SkipPaddingSlow(size_t* gap_bytes);

  // Require the file to end exactly at parse-relative record_end_rel.
  Status RequireExactEOF(uint64_t record_offset, size_t record_end_rel);

  // Validate record-level semantics that do not depend on the payload.
  Status ValidateRecordType(BlogRecordType type, uint64_t length,
                            size_t varint_len, uint64_t record_offset);
  Status ValidateCompressionType(uint8_t compression_type,
                                 BlogRecordType record_type,
                                 uint64_t record_offset);
  // Compact records forbid overlong length varints (full-format allows
  // them); only the encoding needs checking, the range is structural.
  Status ValidateCompactLength(uint64_t length, size_t varint_len,
                               uint64_t record_offset);
  Status ReportNonCanonicalCompactLength(uint64_t length, size_t varint_len,
                                         uint64_t record_offset);
  // ValidateCompressionType() for a type other than kNoCompression.
  Status ValidateCompressedType(uint8_t compression_type,
                                BlogRecordType record_type,
                                uint64_t record_offset);

  // Validate and advance the body/footer record-order state.
  Status ValidateRecordSequence(BlogRecordType type, uint64_t record_offset);
  // ValidateRecordSequence() once a footer record is involved.
  Status ValidateFooterSequence(BlogRecordType type, uint64_t record_offset);

  // Commit n retained bytes as accepted, invalidating the recovery hints
  // left by the latest failed attempt.
  void Accept(size_t n);

  // First aligned occurrence of the file's escape sequence at or after
  // parse-relative from_rel (aligned up) that ends by parse-relative
  // limit_rel (SIZE_MAX for none). Never advances. OK reports the match in
  // *found_rel; NotFound when the input or limit ends first; errors
  // propagate.
  Status FindNextEscapeFrom(size_t from_rel, size_t limit_rel,
                            size_t* found_rel);

  // Parse-relative scan end, including padding and the next escape sequence.
  // Returns SIZE_MAX when the failed record's format provides no usable bound.
  // Known gap: trivial and compact bounds trust an unauthenticated length
  // varint, so corruption that makes a larger record decode as one of them
  // can hide the next boundary and stall a tailing reader on every poll.
  size_t ComputeRecycledRecordScanLimit() const;

  // Recovery scan without a format-derived bound: the retained bytes in
  // place, then the input past them through a fixed-size buffer, one
  // kReadSize chunk per call for refreshable inputs and through EOF
  // otherwise. Progress persists across calls for the same failed record. A
  // boundary found past bytes skipped as scanned on earlier calls is
  // confirmed by rescanning them on the next call.
  Status ScanRecycledTail(size_t from_rel, uint64_t* found_offset);
  // Read on past the bytes buffered from resume_offset and scan them, up to
  // max_reads kReadSize chunks. With rescan_on_match, a boundary found is
  // not returned but marks found_offset for a rescan instead.
  Status ContinueRecycledTailScan(size_t max_reads, bool rescan_on_match,
                                  uint64_t* found_offset);

  struct RecycledTailScan {
    static constexpr size_t kReadSize = 32 * 1024;
    // Parse offset of the failed record the scan describes. Reset to 0 at
    // EOF, so the next pass after a refresh restarts from the record.
    uint64_t origin = 0;
    // File offset of buffer[0], where scanning resumes. While input has been
    // read past the retained bytes, buffer holds it from here to the read
    // offset; after a refresh, the scan skips ahead to here.
    uint64_t resume_offset = 0;
    // Boundary found, when past origin. Flushed bytes of the current
    // incarnation are never rewritten, so it stays valid while origin is
    // unaccepted. Equal to origin while a boundary found past skipped bytes
    // awaits the rescan that confirms the first one.
    uint64_t found_offset = 0;
    char buffer[kReadSize + kBlogEscapeSequenceSize];
  };

  // True when record lengths are authenticated by checksums (verification on
  // with a real checksum type).
  bool LengthsAuthenticated() const;
  // Remember the failed record's parse-relative end for the recovery scan
  // when its length is trusted (authenticated or exact).
  void NoteFailedRecordEnd(size_t end_rel, bool length_trusted);

  // Peek a prefix varint64 at parse-relative offset. Short reads return raw
  // for the caller to contextualize; undecodable bytes are Corruption.
  Status ReadPrefixVarint64(size_t offset, uint64_t* value, size_t* varint_len);
  // ReadPrefixVarint64() when fewer than kMaxPrefixVarint64Length bytes are
  // retained past offset.
  Status ReadPrefixVarint64Slow(size_t offset, uint64_t* value,
                                size_t* varint_len);

  // Escape-through-prefix size when `record_bytes` starts a compact or sized
  // full-format record, else 0.
  size_t RecordPrefixSize(const Slice& record_bytes) const;

  // Position of the first escape sequence in `bytes` at or after `pos` that
  // starts at an aligned file offset, where bytes[0] is at
  // `bytes_file_offset`. Without one, the first aligned position where a
  // match could still complete once more bytes follow, capped at
  // bytes.size().
  size_t FindAlignedEscapeSequence(const Slice& bytes,
                                   uint64_t bytes_file_offset,
                                   size_t pos) const;

  bool ContainsAlignedEscapeSequence(const Slice& bytes,
                                     uint64_t bytes_file_offset) const;

  // True when the record's length swallowed a later record boundary (vs. a
  // cleanly truncated tail); `overrun_bytes` receives the swallowed bytes.
  bool LengthOverranRecordBoundary(const Slice& record_bytes,
                                   uint64_t record_offset,
                                   Slice* overrun_bytes) const;

  // Happy path for a full-format unspecified-size record: verify the shared
  // prefix, then scan forward for aligned escape candidates, validating each
  // by the only trailer that fits (immediately before the trailing pad run).
  // Embedded escapes fail validation and are skipped; clean EOF terminates
  // the record the same way. See ReadTrivialRecord for conventions.
  Status ReadUnspecifiedSizeRecord(uint64_t rec_offset, size_t prefix_rel,
                                   size_t varint_len, BlogRecordType* type,
                                   Slice* payload,
                                   CompressionType* record_compression_type,
                                   size_t* helper_extent);

  // Per-record state for the unspecified-size scan over the retained
  // window. On accept the payload views the window directly; no copy.
  struct UnspecifiedSizeScan {
    uint64_t record_offset = 0;
    uint64_t payload_file_offset = 0;
    // Retained-relative payload start (parse_pos_-relative).
    size_t payload_rel = 0;
    BlogRecordType record_type = kBlogBlobRecord;
    CompressionType prefix_compression_type = kNoCompression;
    Slice* payload = nullptr;
    CompressionType* record_compression_type = nullptr;
    // Parse-relative end of the accepted record (payload + trailer +
    // padding). Valid only when a boundary test reports terminated.
    size_t accepted_extent = 0;

    // Every examined candidate counts, including ones rejected for lack of
    // trailer room; the last_candidate_* fields locate the latest tested.
    size_t candidate_count = 0;
    bool saw_checksum_mismatch = false;
    uint64_t last_candidate_escape_sequence_offset = 0;
    size_t last_candidate_payload_size = 0;

    // Payload-relative offset of the first escape-sequence candidate. When
    // the scan fails, recovery resumes there via the outer boundary scan.
    bool saw_candidate = false;
    size_t first_candidate_offset = 0;

    // A candidate left no trailer room: the record is truncated before what
    // follows, so the tail cannot be a clean truncation.
    bool saw_truncated_candidate = false;
  };

  // Test whether the payload ends at payload-relative `candidate_offset`
  // within `payload_bytes`. Non-OK means the boundary authenticated but the
  // record is invalid; on OK, *terminated tells accept (payload views the
  // window, accepted_extent set) from keep-scanning. EOF never records
  // evidence, so a clean tail stays retryable.
  Status TryEndUnspecifiedSizeRecord(UnspecifiedSizeScan* scan,
                                     const Slice& payload_bytes,
                                     size_t candidate_offset,
                                     bool record_evidence, bool* terminated);

  // Report a torn record; recovery resumes at the first candidate via the
  // outer scan. Called at EOF or past kBlogMaxUnspecifiedSizeScanCandidates
  // without an authenticated boundary.
  Status ReportTornUnspecifiedSizeRecord(UnspecifiedSizeScan* scan,
                                         uint64_t record_offset);

  // Report corruption and return it. `bytes` is the approximate number of
  // bytes logically dropped because parsing cannot continue past it.
  Status ReportCorruption(size_t bytes, const std::string& msg);

  BlogInputWindow window_;
  // Allocated by the first ScanRecycledTail(), then reused.
  std::unique_ptr<RecycledTailScan> recycled_tail_scan_;
  Reporter* reporter_;
  std::unique_ptr<Reporter> owned_reporter_;  // optional, for adapter lifetime
  bool verify_checksums_;
  BlogFileHeader header_;
  uint64_t body_end_offset_ = 0;
  // header_.incarnation_id(), cached for per-record checksums.
  uint32_t incarnation_id_ = 0;
  bool header_read_ = false;
  bool has_compression_compatibility_name_ = false;
  bool has_streaming_compression_type_ = false;
  bool compact_type_valid_ = false;
  // Set once the latest ReadRecord() attempt authenticates a footer-type
  // prefix; reset per attempt.
  bool failure_in_footer_record_ = false;
  BlogReadFailureStage failure_stage_ =
      BlogReadFailureStage::kEscapeSequenceNotFound;
  // Set by ReportCorruption(); reset per ReadRecord() call.
  size_t last_dropped_bytes_ = 0;

  bool footer_started_ = false;
  bool footer_properties_seen_ = false;
  bool footer_checksum_info_seen_ = false;
  bool footer_locator_seen_ = false;

  // The pad gap peeked by the latest ReadRecord() attempt, so
  // ScanForEscapeSequence() skips the failed record's own escape. Any
  // advance invalidates it.
  size_t last_gap_bytes_ = 0;
  bool last_gap_known_ = false;
  // Parse-relative end of the latest attempt's record, when its extent came
  // from a trusted length, so ScanForEscapeSequence() resumes past the whole
  // record instead of walking its payload. Any advance invalidates it.
  size_t last_record_end_rel_ = 0;
  bool last_record_end_known_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
