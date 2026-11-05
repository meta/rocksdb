//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "db/blog/blog_format.h"
#include "file/writable_file_writer.h"
#include "rocksdb/data_structure.h"
#include "rocksdb/io_status.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

struct TEST_BlogNoncanonicalConfig {
  bool enabled = false;
  uint64_t seed = 0;
  uint32_t force_full_record_one_in = 8;
  uint32_t overlong_full_varint_one_in = 8;
  // Ignored by writers constructed with BlogUnspecifiedSizeRecords::
  // kDisallowed, which cannot use the encoding at all. Recycled WALs are the
  // case that matters; see that enum.
  uint32_t unspecified_size_record_one_in = 16;
  uint32_t shuffle_properties_one_in = 4;
  uint32_t split_ignorable_properties_one_in = 4;
  uint32_t inject_auxiliary_record_one_in = 16;
  uint32_t extra_padding_512b_one_in = 16;
  uint32_t extra_padding_4k_one_in = 64;
  uint32_t prefer_padding_with_0xff_one_in = 8;
  uint32_t max_auxiliary_payload_bytes = 32;
};

// Test-only global configuration for writing accepted-but-noncanonical blog
// encodings. Callers must set or reset it before concurrent file creation.
void TEST_SetBlogNoncanonicalConfig(const TEST_BlogNoncanonicalConfig& config);
const TEST_BlogNoncanonicalConfig& TEST_GetBlogNoncanonicalConfig();

class TEST_BlogNoncanonicalConfigScope {
 public:
  explicit TEST_BlogNoncanonicalConfigScope(
      const TEST_BlogNoncanonicalConfig& config);
  ~TEST_BlogNoncanonicalConfigScope();

  TEST_BlogNoncanonicalConfigScope(const TEST_BlogNoncanonicalConfigScope&) =
      delete;
  TEST_BlogNoncanonicalConfigScope& operator=(
      const TEST_BlogNoncanonicalConfigScope&) = delete;
  TEST_BlogNoncanonicalConfigScope(TEST_BlogNoncanonicalConfigScope&&) = delete;
  TEST_BlogNoncanonicalConfigScope& operator=(
      TEST_BlogNoncanonicalConfigScope&&) = delete;

 private:
  TEST_BlogNoncanonicalConfig prev_;
};

// Whether a writer may encode a record with an unspecified size, i.e. with
// the kBlogUnspecifiedSize sentinel in place of its length.
//
// Such a record is the one record shape that is not self-delimiting: its end
// is the next 32-bit-aligned escape sequence, or the end of the file,
// whichever comes first. Both boundaries have to be available to the reader,
// which rules the encoding out for a file that can be read while its trailing
// bytes are neither absent nor padding. Today that means exactly one thing: a
// recycled WAL, whose previous incarnation's bytes remain past the logical
// end, carry a different escape sequence, and are indistinguishable from
// payload. There is no reader-side remedy -- the file simply does not record
// where the last record stopped -- so the writer must not create the shape.
//
// Everything else may use it: a blob file, whose records are only ever read
// by random access at a known offset, and a non-recycled WAL or MANIFEST,
// whose physical end of file is exactly its last written byte (see
// PosixFileSystem::OptimizeForLogWrite / OptimizeForManifestWrite, which force
// buffered writes and fallocate with FALLOC_FL_KEEP_SIZE).
enum class BlogUnspecifiedSizeRecords : uint8_t {
  kDisallowed,
  kAllowed,
};

// Writes blog-format files (WAL or blob). The caller constructs a
// BlogFileHeader, calls WriteHeader(), then adds records via
// AddBlobRecord/AddWriteBatchRecord, and optionally calls WriteFooter() before
// closing.
//
// Format selection (compact vs full) is automatic based on payload size and
// record type. See blog_format.h for the wire format details.
class BlogFileWriter {
 public:
  BlogFileWriter(std::unique_ptr<WritableFileWriter>&& dest,
                 const BlogFileHeader& header,
                 BlogUnspecifiedSizeRecords unspecified_size_records,
                 bool manual_flush = false);
  ~BlogFileWriter();

  // Write the file header. Must be called exactly once, before any records.
  IOStatus WriteHeader(const WriteOptions& wo);

  // Write a blob record. Selects compact or full format automatically.
  // The payload must not be empty.
  // On success, *blob_offset is the file offset of the payload start.
  // uncompressed_size: original size before compression (for footer stats).
  // Pass payload.size() when comp_type == kNoCompression.
  IOStatus AddBlobRecord(const WriteOptions& wo, const Slice& payload,
                         CompressionType comp_type, uint64_t* blob_offset,
                         uint64_t uncompressed_size);

  // Write a WriteBatch record. Selects compact or full format automatically.
  IOStatus AddWriteBatchRecord(const WriteOptions& wo, const Slice& wb_data,
                               CompressionType comp_type = kNoCompression);

  // Write a record whose payload size is not known upfront, using the
  // unspecified-size length encoding so payload chunks can stream directly
  // to the file. Begin() emits the record prefix; each chunk is then passed
  // to AppendUnspecifiedSizeChunk() in file order; Finish() emits the
  // trailer, padding, and flush. Refused when this writer was constructed
  // with BlogUnspecifiedSizeRecords::kDisallowed. Only body record types may
  // be used. A failed chunk or Finish() leaves a torn record that the reader
  // reports as corruption; the caller must then start the next record with a
  // fresh Begin(), like any other failed record.
  IOStatus BeginUnspecifiedSizeRecord(const WriteOptions& wo,
                                      BlogRecordType type,
                                      CompressionType comp_type);
  IOStatus AppendUnspecifiedSizeChunk(const WriteOptions& wo,
                                      const Slice& chunk);
  IOStatus FinishUnspecifiedSizeRecord(const WriteOptions& wo);

  // Write a preamble-start record (stub). Uses trivial format (length=0).
  IOStatus AddPreambleStartRecord(const WriteOptions& wo);

  // Write an ignorable properties record. Can appear anywhere in the file
  // body, including when re-opening an existing file (e.g. manifest reuse)
  // to record updated diagnostic properties. Only ignorable (lowercase)
  // properties may be passed.
  IOStatus AddIgnorablePropertiesRecord(const WriteOptions& wo,
                                        const BlogPropertyMap& props);

  // Write WAL user-defined timestamp sizes encoded as a
  // UserDefinedTimestampSizeRecord payload.
  IOStatus AddUserDefinedTimestampSizeRecord(const WriteOptions& wo,
                                             const Slice& payload);

  // Seal the file: write a footer properties record holding `properties` plus
  // the statistics this writer tracked, then a footer locator that references
  // it. `properties` must not contain the writer-tracked statistics. No
  // records can be added afterwards.
  IOStatus WriteFooter(const WriteOptions& wo,
                       const BlogFileFooterProperties& properties);

  IOStatus WriteBuffer(const WriteOptions& wo);
  IOStatus Sync(const WriteOptions& wo, bool use_fsync = false);
  IOStatus Close(const WriteOptions& wo);

  uint64_t current_offset() const { return offset_; }
  // End of the last non-footer record's checksum trailer, or the header
  // property-section trailer if no body records have been written.
  uint64_t body_end_offset() const { return body_end_offset_; }
  WritableFileWriter* file() { return dest_.get(); }
  const WritableFileWriter* file() const { return dest_.get(); }
  // If closing the writer through file(), call this afterwards to modify
  // this object's state to reflect that. Returns true if the destination file
  // has been closed. If it hasn't been closed, returns false with no change.
  bool PublishIfClosed();
  bool BufferIsEmpty() const;
  // True while a write loss leaves offset_ out of step with the bytes actually
  // flushed, which then mark where the file ends. The next record resyncs.
  bool HasUnresyncedLostWrites() const {
    return !dest_->use_direct_io() && dest_->BufferIsEmpty() &&
           dest_->GetFlushedSize() != offset_;
  }
  // Remains true after offset resynchronization following a write loss.
  bool HasLostWrites() const {
    return lost_writes_ || HasUnresyncedLostWrites();
  }
  std::string GetFileChecksum() { return dest_->GetFileChecksum(); }
  const char* GetFileChecksumFuncName() const {
    return dest_->GetFileChecksumFuncName();
  }
  // Test-only escape hatch for constructing malformed files.
  WritableFileWriter* TEST_file() { return file(); }
  const BlogFileHeader& header() const { return header_; }
  uint64_t last_blob_record_size() const { return last_blob_record_size_; }
  uint64_t last_auxiliary_record_size() const {
    return last_auxiliary_record_size_;
  }

  // Exact physical growth the next AddBlobRecord() call would cause,
  // including any injected test-only auxiliary record.
  uint64_t EstimateNextBlobWritePhysicalGrowth(const Slice& payload,
                                               CompressionType comp_type) const;

  // Accumulated blob record stats for footer properties.
  struct BlobStats {
    uint64_t count = 0;
    uint64_t payload_bytes =
        0;  // on-disk payload (compressed where applicable)
    uint64_t compressed_bytes =
        0;  // compressed size (only actually-compressed blobs)
    uint64_t uncompressed_bytes = 0;  // original size of those same blobs
    uint64_t overhead_bytes =
        0;  // framing: escape seq, varint, trailer, padding
  };
  const BlobStats& blob_stats() const { return blob_stats_; }

  using CompressionTypeSet =
      SmallEnumSet<CompressionType, kDisableCompressionOption>;

  // Set of CompressionType values used across all records in the file
  // (excluding kNoCompression and kStreamingCompressionSentinel).
  const CompressionTypeSet& compression_type_set() const {
    return compression_type_set_;
  }

 private:
  struct RecordPlan {
    bool use_compact = false;
    bool use_unspecified_size = false;
    uint32_t full_varint_min_bytes = uint32_t{kBlogCompactVarintMaxBytes} + 1;
    uint32_t padding_alignment = kBlogMinRecordAlignment;
    bool prefer_padding_with_0xff = false;
  };

  // Escape sequence, longest length varint, type, compression type, and
  // prefix checksum.
  static constexpr size_t kMaxFullRecordPrefixSize =
      kBlogEscapeSequenceSize + kMaxPrefixVarint64Length + 2 + sizeof(uint32_t);
  // Records with payloads up to this size are assembled in one stack buffer
  // so the whole record reaches the file in a single append.
  static constexpr size_t kMaxInlinePayloadSize = 1024;

  // Write a record. Chooses compact format when varint fits in <= 3 bytes
  // and type matches compact_record_type, unless force_full is true.
  IOStatus AddRecordInternal(const IOOptions& opts, BlogRecordType type,
                             const Slice& payload, CompressionType comp_type,
                             uint64_t* payload_offset, bool force_full,
                             bool allow_unspecified_size,
                             uint64_t* record_size);

  IOStatus EmitRecordWithPlan(const IOOptions& opts, BlogRecordType type,
                              const Slice& payload, CompressionType comp_type,
                              const RecordPlan& plan, uint64_t* payload_offset);

  // Emit a trivial-format record (length=0, no payload).
  IOStatus EmitTrivialRecord(const IOOptions& opts, BlogRecordType type,
                             const RecordPlan& plan);

  // Emit a compact-format record.
  IOStatus EmitCompactRecord(const IOOptions& opts, const Slice& payload,
                             CompressionType comp_type, const RecordPlan& plan,
                             uint64_t* payload_offset);

  // Emit a full-format record.
  IOStatus EmitFullRecord(const IOOptions& opts, BlogRecordType type,
                          const Slice& payload, CompressionType comp_type,
                          const RecordPlan& plan, uint64_t* payload_offset);

  // Write a full-format record prefix (escape sequence, length varint,
  // type, pre-payload compression type, prefix checksum) for a record
  // starting at the current offset_ to `dst`, which must have room for
  // kMaxFullRecordPrefixSize bytes. Returns the prefix length.
  // `payload_size` is ignored when `plan.use_unspecified_size` is set.
  size_t BuildFullRecordPrefix(char* dst, BlogRecordType type,
                               CompressionType comp_type,
                               const RecordPlan& plan,
                               uint64_t payload_size) const;

  // Emit the caller-built record prefix, then payload, 5-byte trailer
  // (compression_type + checksum), and padding. If skip_padding is true, no
  // padding is emitted (used for the last record).
  IOStatus EmitPrefixPayloadTrailerPadding(
      const IOOptions& opts, const char* prefix, size_t prefix_size,
      const Slice& payload, CompressionType comp_type, const RecordPlan& plan,
      uint64_t* payload_offset, bool skip_padding);

  IOStatus EmitPadding(const IOOptions& opts, const BlogPadding& pad);
  IOStatus MaybeFlush(const IOOptions& opts);
  IOStatus MaybeEmitAuxiliaryRecord(const IOOptions& opts);

  RecordPlan PlanRecord(BlogRecordType type, const Slice& payload,
                        bool force_full, bool allow_unspecified_size,
                        uint64_t record_ordinal) const;
  uint64_t EstimateRecordSize(const Slice& payload, CompressionType comp_type,
                              BlogRecordType type, const RecordPlan& plan,
                              uint64_t record_offset, bool skip_padding) const;
  bool IsBlobRoleFile() const;
  bool ShouldEmitAuxiliaryRecord(uint64_t record_ordinal) const;
  bool ShouldSplitIgnorableProperties() const;
  std::string BuildAuxiliaryPayload(uint64_t record_ordinal) const;

  // A failed write can drop buffered bytes that offset_ already counts (see
  // WritableFileWriter::WriteBuffered), or a failed append can have flushed
  // the part of its data that filled the buffer, which offset_ never counts.
  // A caller may then clear the error and keep appending. If that happened,
  // re-derives offset_ from the bytes actually flushed, padding to
  // kBlogMinRecordAlignment when the loss ended mid-record, so later records
  // carry checksums for their real offsets. A failed append can also leave
  // part of a record in place with nothing lost; then pads offset_ to
  // kBlogMinRecordAlignment so the next record starts aligned.
  IOStatus ResyncOffsetAfterLostWrites(const IOOptions& opts);

  // Append raw bytes to the file and advance offset_.
  IOStatus EmitBytes(const IOOptions& opts, const char* data, size_t len);

  std::unique_ptr<WritableFileWriter> dest_;
  BlogFileHeader header_;
  uint64_t offset_ = 0;
  uint64_t body_end_offset_ = 0;
  BlogUnspecifiedSizeRecords unspecified_size_records_;
  // State for an in-progress unspecified-size record session. Begin() resets
  // it; only a torn session may be abandoned without Finish().
  bool streaming_record_active_ = false;
  bool streaming_record_broken_ = false;
  CompressionType streaming_record_comp_type_ = kNoCompression;
  uint64_t streaming_record_payload_size_ = 0;
  BlogRecordChecksumWriter streaming_record_checksum_;
  bool manual_flush_;
  bool header_written_ = false;
  bool lost_writes_ = false;
  // Set once WriteFooter() starts. Footer records are only written while
  // sealed; all other records only before.
  bool sealed_ = false;
  uint64_t record_ordinal_ = 0;
  BlobStats blob_stats_;
  CompressionTypeSet compression_type_set_;
  uint64_t last_blob_record_size_ = 0;
  uint64_t last_auxiliary_record_size_ = 0;
  uint64_t header_end_offset_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE
