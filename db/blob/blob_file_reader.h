//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>
#include <memory>

#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_read_request.h"
#include "db/blog/blog_format.h"
#include "file/random_access_file_reader.h"
#include "rocksdb/advanced_compression.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/rocksdb_namespace.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

class Status;
struct ImmutableOptions;
struct FileOptions;
class HistogramImpl;
struct ReadOptions;
class Slice;
class FilePrefetchBuffer;
class BlobContents;
class Statistics;

class BlobFileReader {
 public:
  static Status Create(const ImmutableOptions& immutable_options,
                       const ReadOptions& read_options,
                       const FileOptions& file_options,
                       uint32_t column_family_id,
                       HistogramImpl* blob_file_read_hist,
                       uint64_t blob_file_number,
                       const std::shared_ptr<IOTracer>& io_tracer,
                       std::unique_ptr<BlobFileReader>* reader) {
    return Create(immutable_options, read_options, file_options,
                  column_family_id, blob_file_read_hist, blob_file_number,
                  io_tracer, /*configured_compression_manager=*/nullptr,
                  /*skip_footer_validation=*/false, reader);
  }

  static Status Create(const ImmutableOptions& immutable_options,
                       const ReadOptions& read_options,
                       const FileOptions& file_options,
                       uint32_t column_family_id,
                       HistogramImpl* blob_file_read_hist,
                       uint64_t blob_file_number,
                       const std::shared_ptr<IOTracer>& io_tracer,
                       CompressionManager* configured_compression_manager,
                       std::unique_ptr<BlobFileReader>* reader) {
    return Create(immutable_options, read_options, file_options,
                  column_family_id, blob_file_read_hist, blob_file_number,
                  io_tracer, configured_compression_manager,
                  /*skip_footer_validation=*/false, reader);
  }

  // Allows opening in-flight direct-write blob files by optionally skipping
  // footer validation.
  static Status Create(
      const ImmutableOptions& immutable_options,
      const ReadOptions& read_options, const FileOptions& file_options,
      uint32_t column_family_id, HistogramImpl* blob_file_read_hist,
      uint64_t blob_file_number, const std::shared_ptr<IOTracer>& io_tracer,
      bool skip_footer_validation, std::unique_ptr<BlobFileReader>* reader) {
    return Create(immutable_options, read_options, file_options,
                  column_family_id, blob_file_read_hist, blob_file_number,
                  io_tracer, /*configured_compression_manager=*/nullptr,
                  skip_footer_validation, reader);
  }

  static Status Create(
      const ImmutableOptions& immutable_options,
      const ReadOptions& read_options, const FileOptions& file_options,
      uint32_t column_family_id, HistogramImpl* blob_file_read_hist,
      uint64_t blob_file_number, const std::shared_ptr<IOTracer>& io_tracer,
      CompressionManager* configured_compression_manager,
      bool skip_footer_validation, std::unique_ptr<BlobFileReader>* reader);

  BlobFileReader(const BlobFileReader&) = delete;
  BlobFileReader& operator=(const BlobFileReader&) = delete;
  BlobFileReader(BlobFileReader&&) = delete;
  BlobFileReader& operator=(BlobFileReader&&) = delete;

  ~BlobFileReader();

  Status GetBlob(const ReadOptions& read_options, const Slice& user_key,
                 uint64_t offset, uint64_t value_size,
                 CompressionType compression_type,
                 FilePrefetchBuffer* prefetch_buffer,
                 MemoryAllocator* allocator,
                 std::unique_ptr<BlobContents>* result,
                 uint64_t* bytes_read) const;

  // Reads a byte sub-range [range_offset, range_offset + range_length) of an
  // *uncompressed* blob's value directly from the file, reading only those
  // bytes. Unlike GetBlob it does not read the record header/key, does not
  // verify the whole-record checksum (a strict sub-range cannot cover it), and
  // never decompresses -- so it is only valid when this reader's blob file is
  // uncompressed (checked). `offset` and `value_size` are the blob value's file
  // offset and full size (from the BlobIndex); the caller must ensure
  // range_offset + range_length <= value_size. On success `*result` owns a copy
  // of the requested bytes and `*bytes_read` (when non-null) is the number of
  // bytes read from the file.
  Status GetBlobRange(const ReadOptions& read_options, const Slice& user_key,
                      uint64_t offset, uint64_t value_size,
                      uint64_t range_offset, size_t range_length,
                      MemoryAllocator* allocator,
                      std::unique_ptr<BlobContents>* result,
                      uint64_t* bytes_read) const;

  // offsets must be sorted in ascending order by caller.
  void MultiGetBlob(
      const ReadOptions& read_options, MemoryAllocator* allocator,
      autovector<std::pair<BlobReadRequest*, std::unique_ptr<BlobContents>>>&
          blob_reqs,
      uint64_t* bytes_read) const;

  // Byte-range (partial) multi-read counterpart of MultiGetBlob: for each
  // request reads only its sub-range [offset + range_offset, +range_length) of
  // an *uncompressed* blob value, coalescing all requests into a single
  // MultiRead. Unlike MultiGetBlob it reads no record header/key, verifies no
  // whole-record checksum (a strict sub-range cannot cover it), and never
  // decompresses -- so it is only valid when this reader's blob file is
  // uncompressed (checked). Each result owns a copy of its requested bytes.
  // Requests must be sorted ascending by effective read offset (offset +
  // range_offset) by the caller.
  void MultiGetBlobRange(
      const ReadOptions& read_options,
      autovector<std::pair<BlobRangeReadRequest*,
                           std::unique_ptr<BlobContents>>>& blob_reqs,
      uint64_t* bytes_read) const;

  bool IsBlogFormat() const { return is_blog_format_; }
  // Legacy blob files store one file-wide compression type in the header.
  // Blog files compress per record, so this accessor is only meaningful for
  // legacy files.
  CompressionType GetLegacyCompressionType() const {
    assert(!is_blog_format_);
    return compression_type_;
  }
  uint64_t GetFileSize() const { return file_size_; }
  uint8_t GetBlogSchemaVersion() const { return blog_schema_version_; }
  Slice GetBlogFileIdentity() const {
    return is_blog_format_
               ? Slice(blog_escape_sequence_, kBlogEscapeSequenceSize)
               : Slice();
  }

  // Verifies the file facts observed when this reader was opened against
  // MANIFEST metadata for the same blob file: the physical file size, the
  // legacy-vs-blog format, the blog schema version, and the file identity.
  // Returns Corruption describing the first mismatch, or OK when everything
  // matches.
  //
  // An expected physical size of zero means "unknown" (MANIFESTs written
  // before the size was tracked) and skips the size check. A legacy expected
  // schema version (kLegacyBlobFileSchemaVersion) requires a legacy file and
  // ignores the expected identity; otherwise a blog file with the same
  // schema version and file identity is required.
  Status VerifyAgainstManifestMetadata(
      uint64_t expected_physical_size, uint8_t expected_schema_version,
      const Slice& expected_file_identity) const;

 private:
  // `has_footer` tracks whether offset validation should reserve footer space.
  BlobFileReader(std::unique_ptr<RandomAccessFileReader>&& file_reader,
                 uint64_t file_size, SystemClock* clock, Statistics* statistics,
                 bool has_footer);

  // `skip_footer_size_check` is used for direct-write files that are still
  // missing their footer at open time.
  static Status OpenFile(const ImmutableOptions& immutable_options,
                         const FileOptions& file_opts,
                         HistogramImpl* blob_file_read_hist,
                         uint64_t blob_file_number,
                         const std::shared_ptr<IOTracer>& io_tracer,
                         uint64_t* file_size,
                         std::unique_ptr<RandomAccessFileReader>* file_reader,
                         bool skip_footer_size_check);

  // Read and validate the file header. Detects blog format vs legacy format,
  // setting is_blog_format_ and related fields accordingly. For legacy format,
  // validates column_family_id and sets compression_type_. For blog format,
  // resolves the per-file compatible compression manager needed by ReadFooter.
  Status ReadHeader(
      const ReadOptions& read_options, uint32_t column_family_id,
      CompressionManager* configured_compression_manager,
      std::shared_ptr<CompressionManager>* blog_compression_manager);

  // Validate the file footer. For legacy format, reads and validates the
  // BlobLogFooter. For blog format, scans backward for the footer locator
  // record to verify the file was cleanly sealed and optionally refine the
  // decompressor based on footer properties.
  Status ReadFooter(
      const ReadOptions& read_options,
      const std::shared_ptr<CompressionManager>& blog_compression_manager);

  using Buffer = std::unique_ptr<char[]>;

  static Status ReadFromFile(const RandomAccessFileReader* file_reader,
                             const ReadOptions& read_options,
                             uint64_t read_offset, size_t read_size,
                             Statistics* statistics, Slice* slice, Buffer* buf,
                             AlignedBuffer* direct_io_buffer);

  static Status VerifyLegacyBlobRecord(const Slice& record_slice,
                                       const Slice& user_key,
                                       uint64_t value_size);

  // Verify a blob record's checksum, dispatching to the format-appropriate
  // method. On success, sets *actual_compression_type from the record.
  Status VerifyBlobRecord(const Slice& record_slice, const Slice& user_key,
                          uint64_t value_size, uint64_t value_file_offset,
                          CompressionType* actual_compression_type) const;

  static Status UncompressBlobIfNeeded(const Slice& value_slice,
                                       CompressionType compression_type,
                                       Decompressor* decompressor,
                                       MemoryAllocator* allocator,
                                       SystemClock* clock,
                                       Statistics* statistics,
                                       std::unique_ptr<BlobContents>* result);

  std::unique_ptr<RandomAccessFileReader> file_reader_;
  uint64_t file_size_;
  CompressionType compression_type_ = kNoCompression;
  std::shared_ptr<Decompressor> decompressor_;
  SystemClock* clock_;
  Statistics* statistics_;
  // False when the reader was opened before the blob file footer was written.
  bool has_footer_;
  // True when the file uses blog format instead of legacy blob log format.
  bool is_blog_format_ = false;
  uint8_t blog_schema_version_ = kLegacyBlobFileSchemaVersion;
  // For blog format: fields from the file header used for record checksum
  // verification and footer validation.
  uint32_t blog_incarnation_id_ = 0;
  ChecksumType blog_checksum_type_ = kNoChecksum;
  char blog_escape_sequence_[10] = {};  // kBlogEscapeSequenceSize
};

}  // namespace ROCKSDB_NAMESPACE
