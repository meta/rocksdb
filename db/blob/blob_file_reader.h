//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>
#include <memory>
#include <string>
#include <vector>

#include "db/blob/blob_read_request.h"
#include "file/random_access_file_reader.h"
#include "rocksdb/advanced_compression.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/rocksdb_namespace.h"
#include "table/internal_iterator.h"
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
class BlobIndex;
struct BlockBasedTableOptions;

struct StoredBlobRelocationReadRequest {
  uint64_t offset = 0;
  uint64_t stored_value_size = 0;
  CompressionType compression = kNoCompression;
  uint32_t stable_checksum_version = 0;
  uint32_t stable_checksum = 0;
  std::string* stored_payload = nullptr;
  Status* status = nullptr;
  uint32_t stored_checksum = 0;
};

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
                  io_tracer, /*skip_footer_validation=*/false, reader);
  }

  // Allows opening in-flight direct-write blob files by optionally skipping
  // footer validation.
  static Status Create(
      const ImmutableOptions& immutable_options,
      const ReadOptions& read_options, const FileOptions& file_options,
      uint32_t column_family_id, HistogramImpl* blob_file_read_hist,
      uint64_t blob_file_number, const std::shared_ptr<IOTracer>& io_tracer,
      bool skip_footer_validation, std::unique_ptr<BlobFileReader>* reader);

  static Status CreateRelocationFile(
      const ImmutableOptions& immutable_options,
      const ReadOptions& read_options, const FileOptions& file_options,
      HistogramImpl* blob_file_read_hist, uint32_t column_family_id,
      uint64_t blob_file_number, uint64_t expected_file_size,
      uint64_t expected_tail_size, uint32_t expected_directory_digest,
      const std::string& current_db_session_id,
      const BlockBasedTableOptions& table_options,
      CompressionManager* compression_manager,
      uint8_t block_protection_bytes_per_key,
      const std::shared_ptr<IOTracer>& io_tracer,
      std::unique_ptr<BlobFileReader>* reader);

  // The factory is scoped to the owning BlobFileCache, not one physical file.
  // NewTableReader retains its own reservation/cache state after this returns.
  static Status CreateRelocationFileWithFactory(
      const ImmutableOptions& immutable_options,
      const ReadOptions& read_options, const FileOptions& file_options,
      HistogramImpl* blob_file_read_hist, uint32_t column_family_id,
      uint64_t blob_file_number, uint64_t expected_file_size,
      uint64_t expected_tail_size, uint32_t expected_directory_digest,
      const std::string& current_db_session_id, const TableFactory& factory,
      CompressionManager* compression_manager,
      uint8_t block_protection_bytes_per_key,
      const std::shared_ptr<IOTracer>& io_tracer,
      std::unique_ptr<BlobFileReader>* reader);

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

  Status GetBlob(const ReadOptions& read_options, const Slice& user_key,
                 uint64_t offset, uint64_t value_size,
                 CompressionType compression_type,
                 FilePrefetchBuffer* prefetch_buffer,
                 MemoryAllocator* allocator,
                 std::unique_ptr<BlobContents>* result, uint64_t* bytes_read,
                 const BlobIndex* stable_blob_index) const;

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

  // Reads the exact stored payload bytes of a normal identity-root record,
  // verifies them by decompressing and checking the stable logical checksum,
  // and returns the still-compressed representation for relocation. The user
  // key is intentionally unnecessary because stable integrity is carried by
  // the feature BlobIndex.
  Status GetStoredBlobForRelocation(
      const ReadOptions& read_options, uint64_t offset,
      uint64_t stored_value_size, CompressionType compression_type,
      uint32_t stable_checksum_version, uint32_t stable_checksum,
      uint64_t max_stored_value_size, uint64_t max_logical_value_size,
      std::string* stored_payload, uint64_t* bytes_read) const;

  // Bounded MultiRead counterpart. Requests must be sorted by offset.
  void MultiGetStoredBlobsForRelocation(
      const ReadOptions& read_options, size_t num_requests,
      StoredBlobRelocationReadRequest* requests, uint64_t max_stored_value_size,
      uint64_t max_logical_value_size, uint64_t* bytes_read) const;

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

  Status GetBlobFromRelocationFile(const ReadOptions& read_options,
                                   const BlobIndex& blob_index,
                                   uint64_t range_offset, size_t range_length,
                                   PinnableSlice* result,
                                   uint64_t* bytes_read) const;

  void MultiGetBlobFromRelocationFile(const ReadOptions& read_options,
                                      size_t num_reads,
                                      RelocationBlobReadRequest* requests,
                                      uint64_t* bytes_read) const;

  bool IsRelocationFile() const { return relocation_file_state_ != nullptr; }
  uint32_t GetRelocationDirectoryDigest() const;
  uint32_t GetRelocationColumnFamilyId() const;
  uint64_t GetRelocationTailSize() const;

  CompressionType GetCompressionType() const { return compression_type_; }

  uint64_t GetFileSize() const { return file_size_; }

 private:
  // `has_footer` tracks whether offset validation should reserve footer space.
  BlobFileReader(std::unique_ptr<RandomAccessFileReader>&& file_reader,
                 uint64_t file_size, CompressionType compression_type,
                 std::shared_ptr<Decompressor> decompressor, SystemClock* clock,
                 Statistics* statistics, bool has_footer);

  struct RelocationFileState;
  BlobFileReader(uint64_t file_size, SystemClock* clock, Statistics* statistics,
                 std::unique_ptr<RelocationFileState> relocation_file_state);

  // `skip_footer_size_check` is used for direct-write files that are still
  // missing their footer at open time.
  static Status OpenFile(const ImmutableOptions& immutable_options,
                         const FileOptions& file_opts,
                         HistogramImpl* blob_file_read_hist,
                         uint64_t blob_file_number,
                         const std::shared_ptr<IOTracer>& io_tracer,
                         uint64_t* file_size,
                         std::unique_ptr<RandomAccessFileReader>* file_reader,
                         bool skip_footer_size_check, bool is_relocation_file);

  std::unique_ptr<InternalIterator> NewRelocationFileLookupIterator(
      ReadOptions* lookup_options) const;

  Status FindRelocationFileBlobIndex(InternalIterator* iterator,
                                     const BlobIndex& stable_blob_index,
                                     BlobIndex* same_file_blob_index) const;

  static Status ReadHeader(const RandomAccessFileReader* file_reader,
                           const ReadOptions& read_options,
                           uint32_t column_family_id, Statistics* statistics,
                           CompressionType* compression_type);

  static Status ReadFooter(const RandomAccessFileReader* file_reader,
                           const ReadOptions& read_options, uint64_t file_size,
                           Statistics* statistics);

  using Buffer = std::unique_ptr<char[]>;

  static Status ReadFromFile(const RandomAccessFileReader* file_reader,
                             const ReadOptions& read_options,
                             uint64_t read_offset, size_t read_size,
                             Statistics* statistics, Slice* slice, Buffer* buf,
                             AlignedBuffer* direct_io_buffer);

  static Status VerifyBlob(const Slice& record_slice, const Slice& user_key,
                           uint64_t value_size);

  std::unique_ptr<RandomAccessFileReader> file_reader_;
  uint64_t file_size_;
  CompressionType compression_type_;
  std::shared_ptr<Decompressor> decompressor_;
  SystemClock* clock_;
  Statistics* statistics_;
  // False when the reader was opened before the blob file footer was written.
  bool has_footer_;
  std::unique_ptr<RelocationFileState> relocation_file_state_;
};

}  // namespace ROCKSDB_NAMESPACE
