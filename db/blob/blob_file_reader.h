//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>
#include <memory>
#include <optional>
#include <string>

#include "db/blob/blob_gen2_format.h"
#include "db/blob/blob_read_request.h"
#include "db/blob/blob_relocation_file.h"
#include "db/dbformat.h"
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
class BlockBasedTable;
class BlockCacheTracer;
class BlobRelocationFileReader;
class TableReader;

// One value that indirect blob GC reads from a normal blob file with
// MultiGetStoredBlobsForRelocation(). GC fills the input fields from a
// BlobRelocationLiveValue of its live set (see db/blob/blob_relocation_gc.h).
// For a value in a relocation file, see StoredRelocationFileReadRequest.
struct StoredNormalBlobReadRequest {
  // Offset of the stored value in the blob file (not of its record header).
  uint64_t offset = 0;
  uint64_t stored_value_size = 0;
  CompressionType compression = kNoCompression;

  // Output: set to the stored bytes, which live in the call's
  // CoalescedReadBuffers. If null, the value is read and checked with the
  // stored CRC, but its bytes are not kept. Indirect blob GC always passes a
  // non-null pointer.
  Slice* stored_payload = nullptr;

  // Output: the result of this request. A null `status` skips the request.
  Status* status = nullptr;

  // Input: the expected stored CRC of the value. The read fails with
  // Corruption if the stored bytes do not match it.
  uint32_t stored_checksum = 0;
};

// One value to read from a relocation file in
// BlobRelocationFileReader::MultiGetStoredBlobs(). The read finds the value by
// its stable identity (origin, original offset).
struct StoredRelocationFileReadRequest {
  uint64_t origin_file_number = 0;
  uint64_t original_value_offset = 0;
  uint64_t stored_value_size = 0;
  CompressionType compression = kNoCompression;

  // The logical CRC. The read needs it only to build the BlobIndex that it
  // looks up. It is not checked, because the value is not decompressed.
  uint32_t stable_checksum = 0;

  // Output: set to the stored bytes, which live in the call's
  // CoalescedReadBuffers. If null, the value is only checked.
  Slice* stored_payload = nullptr;

  // Output: the result of this request. A null `status` skips the request.
  Status* status = nullptr;

  // Input: the expected stored CRC of the value (see "Stored CRC" in the
  // overview of db/blob/blob_relocation_gc.h). The read fails with Corruption
  // if the stored bytes do not match it.
  uint32_t stored_checksum = 0;
};

// Reads one blob file of either kind. BlobFileCache stores both kinds under one
// cache entry type.
//   - Blob log file: GetBlob(), GetBlobRange(), MultiGetBlob() and
//     MultiGetBlobRange() read it.
//   - Relocation file: relocation() is not null, and only relocation() may be
//     used. The four read functions above are for blob log files only.
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

  // Wraps an open relocation file; see BlobRelocationFileReader.
  explicit BlobFileReader(std::unique_ptr<BlobRelocationFileReader> relocation);

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
                 std::unique_ptr<BlobContents>* result, uint64_t* bytes_read,
                 std::optional<uint32_t>* value_crc32c = nullptr) const;

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

  // Streams the exact stored bytes of a value in a normal blob file to
  // `consumer` in bounded chunks, and checks them with the stored CRC. It never
  // decompresses and never fills a cache. The consumer's output is valid only
  // if the final status is OK. For relocation files, see
  // BlobRelocationFileReader::StreamStoredBlob.
  Status StreamStoredBlobForRelocation(const ReadOptions& read_options,
                                       const BlobIndex& blob_index,
                                       const BlobChunkConsumer& consumer) const;

  // Reads the exact stored payload bytes of normal indirect-origin records and
  // checks them with the stored checksum, without decompressing. The user key
  // is not needed, because the kIndirectBlob index carries the checksums.
  //   - Order: requests must be sorted by offset.
  //   - Value limit: a request whose stored size is larger than
  //     `max_stored_value_size` gets MemoryLimit.
  //   - Merging: neighbor values share one read when the gap between them is
  //     at most `coalesce_bytes`, and one read is at most
  //     `max_stored_value_size` (see ReadCoalescedRanges()).
  //   - Buffer limit: the requested bytes plus `max_stored_value_size`. A
  //     request whose read would go past it is not read and gets
  //     Status::Incomplete. The caller retries it in a later call.
  //   - Null payload: a request with a null stored_payload is only checked;
  //     its stored bytes are not returned. Other payloads point into `buffers`.
  //   - Counters: BLOB_DB_BLOB_FILE_BYTES_READ and blob_read_byte count the
  //     bytes read, merged gaps included.
  void MultiGetStoredBlobsForRelocation(const ReadOptions& read_options,
                                        uint64_t coalesce_bytes,
                                        size_t num_requests,
                                        StoredNormalBlobReadRequest* requests,
                                        uint64_t max_stored_value_size,
                                        CoalescedReadBuffers* buffers,
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

  // The relocation file reader, or null for a blob log file.
  const BlobRelocationFileReader* relocation() const {
    return relocation_.get();
  }

  CompressionType GetCompressionType() const { return compression_type_; }

  uint64_t GetFileSize() const { return file_size_; }

 private:
  // `has_footer` tracks whether offset validation should reserve footer space.
  BlobFileReader(std::unique_ptr<RandomAccessFileReader>&& file_reader,
                 uint64_t file_size, CompressionType compression_type,
                 std::shared_ptr<Decompressor> decompressor, SystemClock* clock,
                 Statistics* statistics, bool has_footer);

  // BlobRelocationFileReader::Open() uses OpenFile() to open the file.
  friend class BlobRelocationFileReader;

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

  static Status ReadHeader(const RandomAccessFileReader* file_reader,
                           const ReadOptions& read_options,
                           uint32_t column_family_id, uint64_t blob_file_number,
                           Statistics* statistics,
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

  // Checks the record header and the CRC over key + value. With a non-null
  // `value_crc32c`, computes the key and value CRCs separately, combines them
  // for the record check, and returns the value CRC.
  static Status VerifyBlob(const Slice& record_slice, const Slice& user_key,
                           uint64_t value_size,
                           std::optional<uint32_t>* value_crc32c = nullptr);

  std::unique_ptr<RandomAccessFileReader> file_reader_;
  uint64_t file_size_;
  CompressionType compression_type_;
  std::shared_ptr<Decompressor> decompressor_;
  SystemClock* clock_;
  Statistics* statistics_;
  // False when the reader was opened before the blob file footer was written.
  bool has_footer_;
  std::unique_ptr<BlobRelocationFileReader> relocation_;
};

// Reads one relocation file (format in db/blob/blob_relocation_file.h).
// BlobSource uses it to read the values of indirect BlobIndexes that resolve
// to this file. It opens the file as a block-based table behind the 8-byte
// prefix. It is immutable after Open(), so several threads can read with it.
//
// How a read works (MultiGetBlob()):
//   1. BlockBasedTable::MultiGetRawEntries() finds the location entry of each
//      requested value, using the lookup key (origin, original offset). When
//      pin_blob_relocation_location_blocks is on and the reader keeps its
//      location blocks in memory, MultiGetRawEntriesPinned() does it instead.
//   2. CheckEntry() decodes the entry and turns the place of the value into a
//      same-file BlobIndex (record offset and stored size).
//   3. BlockBasedTable::MultiGetSameFileBlob() reads the record. Normal SSTs
//      use the same function to read embedded blobs. Here it runs without a
//      BlobSource. A record has the same format as an embedded blob (the
//      SimpleGen2Blob format, see db/blob/blob_gen2_format.h). Which
//      checksums a read checks is in the table under "Reads" in
//      db/blob/blob_relocation_file.h.
class BlobRelocationFileReader {
 public:
  // Opens relocation file `file` of `column_family_id`. The caller passes
  // the expected file size, tail size and directory digest in `file` (the
  // file's identity, see BlobPhysicalFile).
  //
  // Errors:
  //   - InvalidArgument: `file` is not marked as a relocation file, or its
  //     file number, size or tail size is missing, the tail is larger than
  //     the file, or `db_session_id` is empty.
  //   - Corruption: the file does not match `file` (size, tail size, file
  //     number or directory digest) or `column_family_id`, the table
  //     properties have no db session id, the table properties or the
  //     directory are missing or bad, the directory totals do not match the
  //     embedded blob stats property of the table
  //     (kEmbeddedBlobSstStatsPropertyName in table/embedded_blob_sst.h: record
  //     count and payload bytes), or the table's entry count (num_entries) is
  //     not the number of location entries that the directory implies.
  //
  // `factory` is built from MakeBlobRelocationTableOptions() and is shared by
  // the owning BlobFileCache. `compression_manager` and
  // `block_protection_bytes_per_key` configure the table reader.
  // `block_cache_tracer` (may be nullptr) records the block reads of the
  // table reader in a block cache trace, with caller kBlobRelocationFileRead.
  //
  // With `pin_location_blocks` (the column family option
  // pin_blob_relocation_location_blocks), Open reads every location block and
  // keeps it until the reader is closed, so later lookups need no index or
  // block cache lookup. With a block cache, the blocks are held through their
  // cache handles, so they are charged and not evicted. Without one, they are
  // kept in the reader's memory. If the blocks cannot be kept (the block cache
  // cannot charge them, or a read fails), Open logs a warning and still
  // succeeds, and the reader does normal lookups.
  static Status Open(
      const ImmutableOptions& immutable_options,
      const ReadOptions& read_options, const FileOptions& file_options,
      HistogramImpl* blob_file_read_hist, uint32_t column_family_id,
      const BlobPhysicalFile& file, const std::string& db_session_id,
      const TableFactory& factory, CompressionManager* compression_manager,
      uint8_t block_protection_bytes_per_key, bool pin_location_blocks,
      const std::shared_ptr<IOTracer>& io_tracer,
      BlockCacheTracer* block_cache_tracer,
      std::unique_ptr<BlobRelocationFileReader>* reader);

  BlobRelocationFileReader(const BlobRelocationFileReader&) = delete;
  BlobRelocationFileReader& operator=(const BlobRelocationFileReader&) = delete;

  ~BlobRelocationFileReader();

  // Reads the values of indirect BlobIndexes from this file. A request reads
  // the whole value when range_length == kWholeBlobLength, and decompresses a
  // compressed value. Otherwise it reads the range of an uncompressed value. A
  // range of a compressed value is not supported and gets Corruption.
  //
  // Steps:
  //   1. Check each request (BlobIndex and range). A bad request gets
  //      Corruption, or InvalidArgument for an out-of-bounds range, and is
  //      skipped.
  //   2. Sort the valid requests in key order (origin, original offset).
  //   3. For each batch of up to MultiGetContext::MAX_BATCH_SIZE (32)
  //      requests, look up all keys of the batch. A data block in the block
  //      cache needs no read. The missing data blocks are read together: one
  //      plain read when one block is missing, or else one batched read. Then
  //      check each location entry found and read the records.
  //
  // It does not check the logical CRC. The caller (BlobSource) does. Each
  // request gets its own status: Corruption when the entry is missing or does
  // not match. With read_tier == kBlockCacheTier, a request whose data block
  // is not cached gets Incomplete, also when its entry is missing.
  //
  // `*bytes_read` is an estimate. For each request that succeeded and was not
  // copied from a duplicate, it adds the record bytes (stored size +
  // kSimpleGen2BlobTrailerSize) for a whole read, and the range length for a
  // range read. It does not count the index and data block reads. Values are
  // owned buffers allocated from `allocator` (may be null).
  void MultiGetBlob(const ReadOptions& read_options, MemoryAllocator* allocator,
                    size_t num_reads, RelocationBlobReadRequest* requests,
                    uint64_t* bytes_read) const;

  // Reads values by their stable identity (origin, original offset). When
  // stored_payload is set, it returns their exact stored bytes, for indirect
  // blob GC to copy into a new relocation file. A request with a null
  // stored_payload is only checked. Returned payloads point into `buffers`.
  //   - Order: requests can be in any order; the read sorts them.
  //   - Records: each read covers whole records, trailer included. Neighbor
  //     records share one read when the gap between them is at most
  //     `coalesce_bytes` (see ReadCoalescedRanges()).
  //   - Limits: `max_stored_value_size` has more uses here than in
  //     BlobFileReader::MultiGetStoredBlobsForRelocation(). A request whose
  //     stored size is larger than it gets MemoryLimit, and so does a request
  //     that would bring the total stored bytes of the call past it. It is
  //     also the limit of one read and sets the buffer limit (see
  //     BlockBasedTable::MultiGetStoredSameFileBlob(), where it is named
  //     `max_stored_bytes`). A request past the buffer limit gets
  //     Status::Incomplete, and the caller retries it in a later call.
  //   - Counters: unlike the normal-file read, these reads update no blob read
  //     ticker (BLOB_DB_BLOB_FILE_BYTES_READ) and no blob_read_* perf counter.
  void MultiGetStoredBlobs(const ReadOptions& read_options,
                           uint64_t coalesce_bytes, size_t num_requests,
                           StoredRelocationFileReadRequest* requests,
                           uint64_t max_stored_value_size,
                           CoalescedReadBuffers* buffers,
                           uint64_t* bytes_read) const;

  // Like BlobFileReader::StreamStoredBlobForRelocation, for the value of
  // indirect `blob_index` in this file.
  Status StreamStoredBlob(const ReadOptions& read_options,
                          const BlobIndex& blob_index,
                          const BlobChunkConsumer& consumer) const;

  uint32_t column_family_id() const { return column_family_id_; }
  uint64_t file_size() const { return file_size_; }
  uint64_t tail_size() const { return tail_size_; }
  uint32_t directory_digest() const { return directory_digest_; }
  uint64_t total_entries() const { return total_entries_; }
  uint64_t total_record_bytes() const { return total_record_bytes_; }
  // The memory of the kept location blocks, or 0 when they are not kept (see
  // `pin_location_blocks` in Open()).
  size_t pinned_location_block_bytes() const;

 private:
  BlobRelocationFileReader(uint32_t column_family_id, uint64_t file_size);
  // The location blocks that Open() keeps; defined in the .cc file.
  struct PinnedLocationBlocks;

  // Returns an iterator over the table that yields the raw location entries
  // (packed groups, see db/blob/blob_relocation_file.h). Adjusts
  // `*lookup_options` (no bounds, no readahead, no async I/O).
  // `*lookup_options` must outlive the iterator.
  std::unique_ptr<InternalIterator> NewLookupIterator(
      ReadOptions* lookup_options) const;

  // The last location entry that CheckEntry() decoded, so that a batch of
  // sorted lookups decodes each entry once.
  struct DecodedGroupCache {
    bool valid = false;
    std::string internal_key;
    // The origin of the entry, from its key.
    uint64_t origin_file_number = 0;
    BlobRelocationGroup group;
  };

  // Finds the location entry of `stable_blob_index` and builds the same-file
  // BlobIndex of its record in `*same_file_blob_index` (see CheckEntry()).
  // When `cache` holds an entry of the same origin whose offsets cover the
  // requested one, it uses that entry and does not seek. Returns the
  // PrepareLookup() error when the request is not valid. Returns Corruption
  // when the value is not in the file ("Blob relocation entry is missing"),
  // when the entry is malformed, or when its stored size does not match.
  // Returns the iterator status on an I/O error.
  Status FindBlobIndex(InternalIterator* iterator,
                       const BlobIndex& stable_blob_index,
                       BlobIndex* same_file_blob_index,
                       DecodedGroupCache* cache) const;

  BlockBasedTable* table() const;

  // Checks before the lookup: the request (BlobIndex and range) is valid. The
  // location entry check after the lookup (CheckEntry()) proves the value is
  // in this file.
  Status PrepareLookup(const BlobIndex& stable_blob_index,
                       uint64_t range_offset, size_t range_length) const;

  // Writes the internal lookup key of `stable_blob_index` into `lookup_key`:
  // the 16-byte relocation key (origin, original offset) plus the 8-byte
  // internal key footer of a seek (kMaxSequenceNumber, kValueTypeForSeek), 24
  // bytes in total. The stored entries use sequence 0 and kTypeValue, so the
  // seek lands on the first entry whose relocation key is at or after the
  // lookup key.
  static void EncodeLookupKey(const BlobIndex& stable_blob_index,
                              char* lookup_key);

  // Checks after the lookup: the location entry found (the first entry at or
  // after the lookup key) has the requested origin and holds the requested
  // original offset, and the value's stored size matches `stable_blob_index`.
  // Returns Corruption ("Blob relocation entry is missing") when the origin or
  // the offset is not there. Builds the same-file BlobIndex of the record in
  // `*same_file_blob_index`, with the compression of `stable_blob_index`. The
  // location entry stores no compression type, so the record read checks the
  // compression against the record's compression marker. `cache` may be null.
  static Status CheckEntry(const BlobIndex& stable_blob_index,
                           const Slice& internal_key, const Slice& value,
                           BlobIndex* same_file_blob_index,
                           DecodedGroupCache* cache);

  const uint32_t column_family_id_;
  const uint64_t file_size_;
  uint64_t tail_size_ = 0;
  // Must outlive table_reader_.
  InternalKeyComparator internal_comparator_;
  std::unique_ptr<TableReader> table_reader_;
  // Set only when Open() kept the location blocks. Declared after
  // table_reader_, so it is released first.
  std::unique_ptr<PinnedLocationBlocks> pinned_location_blocks_;
  // Directory totals and digest; the directory itself is not kept.
  uint64_t total_entries_ = 0;
  uint64_t total_record_bytes_ = 0;
  uint32_t directory_digest_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE
