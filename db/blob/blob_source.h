//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>
#include <memory>

#include "cache/cache_key.h"
#include "cache/typed_cache.h"
#include "db/blob/blob_contents.h"
#include "db/blob/blob_file_cache.h"
#include "db/blob/blob_read_request.h"
#include "rocksdb/cache.h"
#include "rocksdb/rocksdb_namespace.h"
#include "table/block_based/cachable_entry.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

struct ImmutableOptions;
struct MutableCFOptions;
class Status;
class FilePrefetchBuffer;
class Slice;
class RandomAccessFileReader;
enum ChecksumType : char;

// One whole-record request for BlobSource::MultiGetSimpleGen2Blob (the batched
// counterpart of GetSimpleGen2Blob). The per-file context (base cache key,
// file, checksum context) is passed once to the batch call; these are the
// per-record inputs and outputs.
struct SimpleGen2BlobReadRequest {
  uint64_t record_offset = 0;
  uint64_t payload_size = 0;
  CompressionType expected_compression = kNoCompression;
  PinnableSlice* result = nullptr;
  Status* status = nullptr;
};

// One sub-range request for BlobSource::MultiGetSimpleGen2BlobRange (the
// batched counterpart of GetSimpleGen2BlobRange).
struct SimpleGen2BlobRangeReadRequest {
  uint64_t record_offset = 0;
  uint64_t payload_size = 0;
  uint64_t range_offset = 0;
  size_t range_length = 0;
  CompressionType expected_compression = kNoCompression;
  PinnableSlice* result = nullptr;
  Status* status = nullptr;
};

// BlobSource is a class that provides universal access to blobs, regardless of
// whether they are in the blob cache, secondary cache, or (remote) storage.
// Depending on user settings, it always fetch blobs from multi-tier cache and
// storage with minimal cost.
class BlobSource {
 public:
  // NOTE: db_id, db_session_id, and blob_file_cache are saved by reference or
  // pointer.
  BlobSource(const ImmutableOptions& immutable_options,
             const MutableCFOptions& mutable_cf_options,
             const std::string& db_id, const std::string& db_session_id,
             BlobFileCache* blob_file_cache);

  BlobSource(const BlobSource&) = delete;
  BlobSource& operator=(const BlobSource&) = delete;
  BlobSource(BlobSource&&) = delete;
  BlobSource& operator=(BlobSource&&) = delete;

  ~BlobSource();

  // Read a blob from the underlying cache or one blob file.
  //
  // If successful, returns ok and sets "*value" to the newly retrieved
  // uncompressed blob. If there was an error while fetching the blob, sets
  // "*value" to empty and returns a non-ok status.
  //
  // Note: For consistency, whether the blob is found in the cache or on disk,
  // sets "*bytes_read" to the size of on-disk (possibly compressed) blob
  // record.
  Status GetBlob(const ReadOptions& read_options, const Slice& user_key,
                 const BlobFileOpenInfo& blob_file, uint64_t offset,
                 uint64_t file_size, uint64_t value_size,
                 CompressionType compression_type,
                 FilePrefetchBuffer* prefetch_buffer, PinnableSlice* value,
                 uint64_t* bytes_read);

  // Reads a byte sub-range [range_offset, range_offset + range_length) of an
  // *uncompressed* blob's value, reading only those bytes on a cache miss.
  //
  // First probes the blob cache for the whole value. On a hit, pins the cache
  // handle into *value and points it at the requested sub-range (zero-copy, no
  // disk I/O). On a miss, reads only the requested bytes from the blob file
  // (via BlobFileReader::GetBlobRange -- no record-header/key read and no
  // whole-record checksum verification) and pins the owned buffer into *value.
  //
  // Unlike GetBlob, a partial read never inserts into the blob cache: the cache
  // entry is keyed per (file, offset) and holds the *whole* BlobContents, so a
  // partial value would violate that invariant. `compression_type` must be
  // kNoCompression (a strict sub-range of a compressed blob cannot be
  // decompressed in isolation); callers that need a compressed column, a
  // whole-column read, or checksum verification use GetBlob and slice instead.
  //
  // The caller must ensure range_offset + range_length <= value_size. On a miss
  // *bytes_read (when non-null) is the number of bytes read from the file; on a
  // hit it is 0.
  Status GetBlobRange(const ReadOptions& read_options, const Slice& user_key,
                      const BlobFileOpenInfo& blob_file, uint64_t offset,
                      uint64_t file_size, uint64_t value_size,
                      CompressionType compression_type, uint64_t range_offset,
                      size_t range_length, PinnableSlice* value,
                      uint64_t* bytes_read);

  // Reads a SimpleGen2Blob payload (see db/blob/blob_gen2_format.h) through the
  // blob value cache and BLOB_DB_* statistics. This is the counterpart to
  // GetBlob() for the second-generation blob record format, which is read
  // directly from a RandomAccessFileReader rather than from a traditional blob
  // file.
  //
  // The cache key is derived from the SimpleGen2Blob format itself, not chosen
  // by the caller: it is GetSimpleGen2BlobCacheKey(base_cache_key,
  // record_offset), the same offset scheme block-based SST blocks use. The
  // caller supplies only its file's `base_cache_key` (db_id / db_session_id /
  // file_number). This keeps blob records collision-free with the file's data
  // blocks even when the blob cache and block cache are the same cache.
  //
  // `file`, `record_offset`, `payload_size`, `checksum_type`,
  // `base_context_checksum`, and `expected_compression` are the inputs to the
  // SimpleGen2Blob reader used on a cache miss (see
  // ReadAndVerifySimpleGen2BlobRecord). The on-disk record size (payload +
  // trailer) is reported via `*bytes_read` (when non-null) and the
  // BLOB_DB_BLOB_FILE_BYTES_READ / blob_read_byte counters, consistently on
  // both cache hits and misses.
  //
  // On a cache hit, pins the cached value into `*value` (no copy). On a miss,
  // reads + verifies the record into a cache-allocator buffer, records the
  // per-read stats, inserts it into the cache (when configured and fill_cache
  // is set), and pins it into `*value`. If blob_cache_ is not configured, the
  // record is still read and read stats recorded, just without a cache
  // lookup/insert.
  Status GetSimpleGen2Blob(const ReadOptions& read_options,
                           const OffsetableCacheKey& base_cache_key,
                           RandomAccessFileReader* file, uint64_t record_offset,
                           uint64_t payload_size, ChecksumType checksum_type,
                           uint32_t base_context_checksum,
                           CompressionType expected_compression,
                           PinnableSlice* value, uint64_t* bytes_read);

  // Reads a byte sub-range [range_offset, range_offset + range_length) of an
  // *uncompressed* SimpleGen2Blob payload, reading only those bytes on a cache
  // miss. This is the embedded (same-file) counterpart of GetBlobRange().
  //
  // First probes the blob cache for the whole payload (keyed exactly as
  // GetSimpleGen2Blob). On a hit, pins the cache handle into *value and points
  // it at the requested sub-range (zero-copy, no disk I/O). On a miss, reads
  // only the requested bytes from the file (via ReadSimpleGen2BlobRange -- no
  // trailer read and no checksum verification) and pins the owned buffer.
  //
  // Like GetBlobRange, a partial read never inserts into the blob cache (the
  // cache entry holds the whole payload). `expected_compression` must be
  // kNoCompression. The caller must ensure range_offset + range_length <=
  // payload_size. On a miss *bytes_read (when non-null) is the number of bytes
  // read from the file; on a hit it is 0.
  Status GetSimpleGen2BlobRange(const ReadOptions& read_options,
                                const OffsetableCacheKey& base_cache_key,
                                RandomAccessFileReader* file,
                                uint64_t record_offset, uint64_t payload_size,
                                ChecksumType checksum_type,
                                uint32_t base_context_checksum,
                                CompressionType expected_compression,
                                uint64_t range_offset, size_t range_length,
                                PinnableSlice* value, uint64_t* bytes_read);

  // Batched counterpart of GetSimpleGen2Blob: resolves `num_records` whole
  // embedded (SimpleGen2Blob) records from one file, coalescing all cache-miss
  // reads into a single MultiRead. The per-file context (base cache key, file,
  // checksum type, base context checksum) is shared by all records; per-record
  // inputs/outputs are in `reqs`. Each record independently probes the blob
  // cache and, on a miss, is read + verified from the file; whole records are
  // cache-filled (when configured and fill_cache) exactly as GetSimpleGen2Blob.
  // Per-record outcome is written to *reqs[i].status.
  void MultiGetSimpleGen2Blob(const ReadOptions& read_options,
                              const OffsetableCacheKey& base_cache_key,
                              RandomAccessFileReader* file,
                              ChecksumType checksum_type,
                              uint32_t base_context_checksum,
                              size_t num_records,
                              SimpleGen2BlobReadRequest* reqs);

  // Batched counterpart of GetSimpleGen2BlobRange: resolves `num_records`
  // uncompressed embedded sub-ranges from one file, coalescing cache-miss reads
  // into a single MultiRead. Like GetSimpleGen2BlobRange, each record probes
  // the whole-payload cache (slicing on a hit) and a partial read is never
  // inserted into the cache. Per-record outcome is in *reqs[i].status.
  void MultiGetSimpleGen2BlobRange(const ReadOptions& read_options,
                                   const OffsetableCacheKey& base_cache_key,
                                   RandomAccessFileReader* file,
                                   size_t num_records,
                                   SimpleGen2BlobRangeReadRequest* reqs);

  // Read multiple blobs from the underlying cache or blob file(s).
  //
  // If successful, returns ok and sets "result" in the elements of "blob_reqs"
  // to the newly retrieved uncompressed blobs. If there was an error while
  // fetching one of blobs, sets its "result" to empty and sets its
  // corresponding "status" to a non-ok status.
  //
  // Note:
  //  - The main difference between this function and MultiGetBlobFromOneFile is
  //    that this function can read multiple blobs from multiple blob files.
  //
  //  - For consistency, whether the blob is found in the cache or on disk, sets
  //  "*bytes_read" to the total size of on-disk (possibly compressed) blob
  //  records.
  void MultiGetBlob(const ReadOptions& read_options,
                    autovector<BlobFileReadRequests>& blob_reqs,
                    uint64_t* bytes_read);

  // Read multiple blobs from the underlying cache or one blob file.
  //
  // If successful, returns ok and sets "result" in the elements of "blob_reqs"
  // to the newly retrieved uncompressed blobs. If there was an error while
  // fetching one of blobs, sets its "result" to empty and sets its
  // corresponding "status" to a non-ok status.
  //
  // Note:
  //  - The main difference between this function and MultiGetBlob is that this
  //  function is only used for the case where the demanded blobs are stored in
  //  one blob file. MultiGetBlob will call this function multiple times if the
  //  demanded blobs are stored in multiple blob files.
  //
  //  - For consistency, whether the blob is found in the cache or on disk, sets
  //  "*bytes_read" to the total size of on-disk (possibly compressed) blob
  //  records.
  void MultiGetBlobFromOneFile(const ReadOptions& read_options,
                               const BlobFileOpenInfo& blob_file,
                               uint64_t file_size,
                               autovector<BlobReadRequest>& blob_reqs,
                               uint64_t* bytes_read);

  // Byte-range (partial) multi-read counterpart of MultiGetBlob: for each
  // request reads only its sub-range of an *uncompressed* blob value, from the
  // blob cache (whole-value hit, sliced) or coalesced disk reads (one MultiRead
  // per file). Like GetBlobRange, a partial read never fills the blob cache.
  // Groups requests by file and dispatches each file via
  // MultiGetBlobRangeFromOneFile. Per-request outcome is in
  // *blob_reqs[..].status.
  void MultiGetBlobRange(const ReadOptions& read_options,
                         autovector<BlobFileRangeReadRequests>& blob_reqs,
                         uint64_t* bytes_read);

  // Byte-range (partial) multi-read counterpart of MultiGetBlobFromOneFile for
  // a single blob file. See MultiGetBlobRange.
  void MultiGetBlobRangeFromOneFile(const ReadOptions& read_options,
                                    const BlobFileOpenInfo& blob_file,
                                    uint64_t file_size,
                                    autovector<BlobRangeReadRequest>& blob_reqs,
                                    uint64_t* bytes_read);

  inline Status GetBlobFileReader(
      const ReadOptions& read_options, const BlobFileOpenInfo& blob_file,
      CacheHandleGuard<BlobFileReader>* blob_file_reader) {
    return blob_file_cache_->GetBlobFileReader(read_options, blob_file,
                                               blob_file_reader);
  }

  inline Cache* GetBlobCache() const { return blob_cache_.get(); }

  bool TEST_BlobInCache(uint64_t file_number, uint64_t file_size,
                        uint64_t offset, size_t* charge = nullptr) const;

  // For TypedSharedCacheInterface
  void Create(BlobContents** out, const char* buf, size_t size,
              MemoryAllocator* alloc);

  using SharedCacheInterface =
      FullTypedSharedCacheInterface<BlobContents, BlobContentsCreator>;
  using TypedHandle = SharedCacheInterface::TypedHandle;

 private:
  Status GetBlobFromCache(const Slice& cache_key,
                          CacheHandleGuard<BlobContents>* cached_blob) const;

  Status PutBlobIntoCache(const Slice& cache_key,
                          std::unique_ptr<BlobContents>* blob,
                          CacheHandleGuard<BlobContents>* cached_blob) const;

  static void PinCachedBlob(CacheHandleGuard<BlobContents>* cached_blob,
                            PinnableSlice* value);

  static void PinOwnedBlob(std::unique_ptr<BlobContents>* owned_blob,
                           PinnableSlice* value);

  TypedHandle* GetEntryFromCache(const Slice& key) const;

  Status InsertEntryIntoCache(const Slice& key, BlobContents* value,
                              TypedHandle** cache_handle,
                              Cache::Priority priority) const;

  inline CacheKey GetCacheKey(uint64_t file_number, uint64_t /*file_size*/,
                              uint64_t offset) const {
    OffsetableCacheKey base_cache_key(db_id_, db_session_id_, file_number);
    return base_cache_key.WithOffset(offset);
  }

  const std::string& db_id_;
  const std::string& db_session_id_;

  Statistics* statistics_;

  // A cache to store blob file reader.
  BlobFileCache* blob_file_cache_;

  // A cache to store uncompressed blobs.
  mutable SharedCacheInterface blob_cache_;

  // The control option of how the cache tiers will be used. Currently rocksdb
  // support block/blob cache (volatile tier) and secondary cache (this tier
  // isn't strictly speaking a non-volatile tier since the compressed cache in
  // this tier is in volatile memory).
  const CacheTier lowest_used_cache_tier_;
};

}  // namespace ROCKSDB_NAMESPACE
