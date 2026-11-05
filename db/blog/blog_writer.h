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

// Writes blog-format files (WAL or blob). The caller constructs a
// BlogFileHeader, calls WriteHeader(), then adds records via
// AddBlobRecord/AddWriteBatchRecord, and optionally appends footer records
// before closing.
//
// Format selection (compact vs full) is automatic based on payload size and
// record type. See blog_format.h for the wire format details.
class BlogFileWriter {
 public:
  BlogFileWriter(std::unique_ptr<WritableFileWriter>&& dest,
                 const BlogFileHeader& header, bool manual_flush = false);
  ~BlogFileWriter();

  // Write the file header. Must be called exactly once, before any records.
  IOStatus WriteHeader(const WriteOptions& wo);

  // Write a blob record. Selects compact or full format automatically.
  // On success, *blob_offset is the file offset of the payload start.
  // uncompressed_size: original size before compression (for footer stats).
  // Pass payload.size() when comp_type == kNoCompression.
  IOStatus AddBlobRecord(const WriteOptions& wo, const Slice& payload,
                         CompressionType comp_type, uint64_t* blob_offset,
                         uint64_t uncompressed_size);

  // Write a WriteBatch record. Selects compact or full format automatically.
  IOStatus AddWriteBatchRecord(const WriteOptions& wo, const Slice& wb_data,
                               CompressionType comp_type = kNoCompression);

  // Write a preamble-start record (stub). Uses trivial format (length=0).
  IOStatus AddPreambleStartRecord(const WriteOptions& wo);

  // Write an ignorable properties record. Can appear anywhere in the file
  // body, including when re-opening an existing file (e.g. manifest reuse)
  // to record updated diagnostic properties. Only ignorable (lowercase)
  // properties are allowed; required (uppercase) properties are rejected.
  IOStatus AddIgnorablePropertiesRecord(const WriteOptions& wo,
                                        const BlogPropertyMap& props);

  // Write a footer index record (full format).
  IOStatus AddFooterIndexRecord(const WriteOptions& wo,
                                const Slice& index_data);

  // Write a footer properties record (full format).
  IOStatus AddFooterPropertiesRecord(const WriteOptions& wo,
                                     const BlogFileFooterProperties& props);

  // Write a footer locator record (full format). Must be the last record.
  IOStatus AddFooterLocatorRecord(const WriteOptions& wo,
                                  const BlogFileFooterLocator& locator);

  IOStatus WriteBuffer(const WriteOptions& wo);
  IOStatus Sync(const WriteOptions& wo, bool use_fsync = false);
  IOStatus Close(const WriteOptions& wo);

  uint64_t current_offset() const { return offset_; }
  WritableFileWriter* file() { return dest_.get(); }
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

  // Write a record. Chooses compact format when varint fits in <= 3 bytes
  // and type matches compact_record_type, unless force_full is true.
  IOStatus AddRecordInternal(const WriteOptions& wo, BlogRecordType type,
                             const Slice& payload, CompressionType comp_type,
                             uint64_t* payload_offset, bool force_full,
                             bool allow_unspecified_size,
                             uint64_t* record_size);

  IOStatus EmitRecordWithPlan(const WriteOptions& wo, BlogRecordType type,
                              const Slice& payload, CompressionType comp_type,
                              const RecordPlan& plan, uint64_t* payload_offset);

  // Emit a trivial-format record (length=0, no payload).
  IOStatus EmitTrivialRecord(const WriteOptions& wo, BlogRecordType type,
                             const RecordPlan& plan);

  // Emit a compact-format record.
  IOStatus EmitCompactRecord(const WriteOptions& wo, const Slice& payload,
                             CompressionType comp_type, const RecordPlan& plan,
                             uint64_t* payload_offset);

  // Emit a full-format record.
  IOStatus EmitFullRecord(const WriteOptions& wo, BlogRecordType type,
                          const Slice& payload, CompressionType comp_type,
                          const RecordPlan& plan, uint64_t* payload_offset);

  // Emit payload, 5-byte trailer (compression_type + checksum), and padding.
  // If skip_padding is true, no padding is emitted (used for the last record).
  IOStatus EmitPayloadTrailerPadding(
      const WriteOptions& wo, const Slice& payload, CompressionType comp_type,
      const RecordPlan& plan, uint64_t* payload_offset, bool skip_padding);

  IOStatus EmitPadding(const WriteOptions& wo, const BlogPadding& pad);
  IOStatus MaybeFlush(const WriteOptions& wo);
  IOStatus MaybeEmitAuxiliaryRecord(const WriteOptions& wo);

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

  // Append raw bytes to the file and advance offset_.
  IOStatus EmitBytes(const WriteOptions& wo, const Slice& data);
  IOStatus EmitBytes(const WriteOptions& wo, const char* data, size_t len);

  std::unique_ptr<WritableFileWriter> dest_;
  BlogFileHeader header_;
  uint64_t offset_ = 0;
  bool manual_flush_;
  bool header_written_ = false;
  uint64_t record_ordinal_ = 0;
  BlobStats blob_stats_;
  CompressionTypeSet compression_type_set_;
  uint64_t last_blob_record_size_ = 0;
  uint64_t last_auxiliary_record_size_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE
