//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>
#include <memory>
#include <mutex>
#include <optional>

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
class Decompressor;
class CompressionManager;
class SystemClock;
enum ChecksumType : char;

// Indirect values use the stable blob cache; see the "Stable blob cache"
// comment at stable_blob_cache_ below.

// Returns the stable blob cache key of an indirect value (a value that a
// kIndirectBlob or kIndirectFlattenedBlob BlobIndex points to). The key is
// built from db_id, the session id, the origin file number and the original
// offset. It does not include the generation or the physical file and offset,
// so moving a value into a relocation file neither duplicates nor invalidates
// its cache entry. This is the reference (slow) derivation, for tests. Reads
// use StableBlobCacheKeyBuilder, which gives the same keys faster.
CacheKey TEST_GetStableBlobCacheKey(const std::string& db_id,
                                    const std::string& db_session_id,
                                    uint64_t origin_file_number,
                                    uint64_t original_value_offset);

// Gives the same keys as TEST_GetStableBlobCacheKey, but hashes db_id and the
// session and chooses the domain (see ChooseStableKeyDomain in blob_source.cc)
// once, at construction. Each Get() only mixes in the origin and the offset.
// The builder does not change after construction, so several threads can use it
// at the same time.
class StableBlobCacheKeyBuilder {
 public:
  StableBlobCacheKeyBuilder(const std::string& db_id,
                            const std::string& db_session_id);
  CacheKey Get(uint64_t origin_file_number,
               uint64_t original_value_offset) const;

 private:
  // GetSstInternalUniqueId(db_id, session, file_number) is
  // {session_lower_, db_hash_ ^ file_number}.
  uint64_t session_lower_ = 0;
  uint64_t db_hash_ = 0;
  // ReverseBits(domain), xored into the second word of the internal id.
  uint64_t domain_bits_ = 0;
  // OffsetableCacheKey treats a zero session_lower in a special way, so the
  // fast path would give keys that differ from the reference derivation. A
  // session id that decodes to session_lower == 0 (DBImpl does not create one)
  // therefore uses the reference derivation.
  std::string slow_db_id_;
  std::string slow_db_session_id_;
  bool use_slow_path_ = false;
};

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

  // Looks up the value of one indirect BlobIndex in the stable blob cache only.
  // Readers (Version) call it first, before they look up any file, so a hit
  // needs no blob file lookup. On a hit it pins the cached value (or the range
  // at `range_offset` of `range_length` bytes; kWholeBlobLength selects the
  // whole value) into `*value` and returns OK. A range of a compressed value
  // returns Corruption first, before the cache is checked, also when there is
  // no stable cache. Otherwise it returns NotFound when there is no stable
  // cache or the value is not cached, InvalidArgument for a range outside the
  // value, and Corruption for a damaged entry from the secondary cache.
  //
  // A hit is not checked against `blob_index` (no fingerprint, file or
  // relocation check). This is safe for these reasons:
  // - The stable key includes db_id and db_session_id, so entries from before a
  //   reopen never match.
  // - A blob file never changes after it is written, and the DB never reuses
  //   a file number. So (origin, original offset) names the same bytes for the
  //   life of the DB.
  // - Outside tests, a relocation file is marked lost only when the open
  //   replays a loss record, so no entry of this session can belong to a lost
  //   file.
  Status GetIndirectBlobFromCache(const BlobIndex& blob_index,
                                  uint64_t range_offset, size_t range_length,
                                  PinnableSlice* value) const;

  // Reads the value of one indirect BlobIndex from `file`, the physical file
  // that the caller picked: a normal blob file, or a relocation file when
  // `file.is_relocation` is set. The caller (Version) picks the file; this
  // class never chooses one. The stable cache key uses only the stable identity
  // of the value (origin, original offset).
  // - Range of a compressed value: returns Corruption, as in base RocksDB,
  //   before any cache lookup or read. A byte range of a compressed value
  //   cannot be decompressed alone, so callers read the whole value and slice
  //   it.
  // - Stable cache hit: the same as GetIndirectBlobFromCache(). With
  //   `cache_checked`, the caller already looked up the cache and missed, so
  //   this function does not look it up again.
  // - Whole value: reads and decompresses the whole value. Its logical CRC is
  //   checked when verify_checksums is set or when it is inserted into the
  //   stable cache (fill_cache with a blob cache).
  // - Uncompressed range: reads only the range bytes; no CRC check and no cache
  //   fill. Callers send only strict parts of a value (see
  //   ReadPathBlobResolver::ResolveColumnRange), and a range is never turned
  //   into a whole read.
  // - A kBlockCacheTier read that misses the cache returns Incomplete.
  // `*bytes_read` is the size read from the physical file (0 on a cache hit).
  // For a relocation file it is an estimate: the record size (or the range
  // length for an uncompressed range read), without the index and data block
  // reads. `mutable_cf_options` configure the table reader when a relocation
  // file is opened.
  Status GetIndirectBlob(const ReadOptions& read_options, const Slice& user_key,
                         const BlobIndex& blob_index,
                         const BlobPhysicalFile& file,
                         const MutableCFOptions& mutable_cf_options,
                         uint64_t range_offset, size_t range_length,
                         FilePrefetchBuffer* prefetch_buffer,
                         PinnableSlice* value, uint64_t* bytes_read,
                         bool cache_checked);

  // Batched counterpart of GetIndirectBlob for requests that all read the same
  // physical file. Each request gets the same contract as GetIndirectBlob and
  // its own status (IndirectBlobReadRequest::cache_checked plays the role of
  // `cache_checked`), with one difference. When a read from a normal blob file
  // fails with Corruption, GetIndirectBlob drops the cached file reader and
  // tries once more with a new reader, for whole-value reads and for
  // uncompressed range reads. This function does that only for whole-value
  // reads, so an uncompressed range read returns the Corruption. Both fail
  // closed. For a normal blob file, the reads are sorted and split into batches
  // whose file ranges do not overlap, and an exact duplicate shares the read of
  // its source. `*bytes_read` is the sum of the read sizes of the requests that
  // read OK, so a duplicate adds its own size. For a relocation file it is an
  // estimate: the sum of the record sizes (or range lengths for uncompressed
  // range reads), without the index and data block reads.
  void MultiGetIndirectBlobFromOneFile(
      const ReadOptions& read_options, const BlobPhysicalFile& file,
      const MutableCFOptions& mutable_cf_options, size_t num_requests,
      IndirectBlobReadRequest* requests, uint64_t* bytes_read);

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
  // is set), and pins it into `*value`. A compressed record is decompressed
  // into a new buffer, and the cache holds the decompressed value. If
  // blob_cache_ is not configured, the record is still read and read stats
  // recorded, just without a cache lookup/insert. Incomplete on a
  // kBlockCacheTier miss.
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
  // embedded (SimpleGen2Blob) records from one file, coalescing cache-miss
  // reads into MultiRead batches. The per-file context (base cache key, file,
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
  // into MultiRead batches. Like GetSimpleGen2BlobRange, each record probes
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

  // Inserts a value that flush just wrote into the blob cache
  // (prepopulate_blob_cache = kFlushOnly). A legacy value (a value that a
  // kBlob BlobIndex points to) is keyed by its physical file and offset. An
  // indirect value, given its BlobIndex `fingerprint`, is keyed by its stable
  // identity (origin = the new file, original offset). The caller computed
  // the logical CRC in `fingerprint` from this same value, so the insert does
  // not compute it again. Unlike a read, the insert does not check for an
  // existing entry under the per-key admission mutex
  // (stable_cache_admission_mutex_): no other entry can exist yet for a value
  // in a new file. No-op without a blob cache.
  Status PrepopulateFlushBlob(const Slice& value, uint64_t blob_file_number,
                              uint64_t blob_offset,
                              const StableBlobFingerprint* fingerprint) const;

  bool TEST_BlobInCache(uint64_t file_number, uint64_t file_size,
                        uint64_t offset, size_t* charge = nullptr) const;

  // For TypedSharedCacheInterface
  void Create(BlobContents** out, const char* buf, size_t size,
              MemoryAllocator* alloc);

  using SharedCacheInterface =
      FullTypedSharedCacheInterface<BlobContents, BlobContentsCreator>;
  using TypedHandle = SharedCacheInterface::TypedHandle;

 private:
  using StableSharedCacheInterface =
      FullTypedSharedCacheInterface<StableBlobContents,
                                    StableBlobContentsCreator>;
  using StableTypedHandle = StableSharedCacheInterface::TypedHandle;

  Status GetBlobFromCache(const Slice& cache_key,
                          CacheHandleGuard<BlobContents>* cached_blob) const;

  Status PutBlobIntoCache(const Slice& cache_key,
                          std::unique_ptr<BlobContents>* blob,
                          CacheHandleGuard<BlobContents>* cached_blob) const;

  // Looks up the stable cache entry for `cache_key`. Returns NotFound on a
  // miss. An invalid entry (promoted from damaged secondary-cache bytes) is
  // erased and returns Corruption. The entry is not compared with the reader's
  // BlobIndex (see GetIndirectBlobFromCache() for why). With `primary_only`,
  // the secondary cache is not probed.
  Status GetStableBlobFromCache(
      const Slice& cache_key, CacheHandleGuard<StableBlobContents>* cached_blob,
      bool record_statistics, bool primary_only) const;

  // Inserts `*logical_value`, whose logical CRC the caller has checked, under
  // `cache_key` and pins it in `cached_blob`. The entry adopts the buffer (no
  // copy); `*logical_value` is empty afterwards. The entry is built outside the
  // per-key admission mutex; under it there is only a primary-cache lookup and
  // the insert, so a good entry is never replaced by a conflicting one. If the
  // primary cache already has an entry under `cache_key`, that entry is
  // returned, and the new one is dropped. No comparison is needed, because one
  // stable key names one immutable value.
  Status PutStableBlobIntoCache(
      const Slice& cache_key, const BlobIndex& blob_index,
      std::unique_ptr<BlobContents>* logical_value,
      CacheHandleGuard<StableBlobContents>* cached_blob) const;

  // Inserts `*contents` under `cache_key` and records the cache statistics.
  // With `cached_blob`, pins the entry there; otherwise releases it. On success
  // `*contents` is empty.
  Status InsertStableBlob(
      const Slice& cache_key, std::unique_ptr<StableBlobContents>* contents,
      CacheHandleGuard<StableBlobContents>* cached_blob) const;

  // Finishes one indirect read after the physical read, and pins the result
  // into `result`. Takes ownership of `*physical_value`.
  // - `materialized_whole` is false: `*physical_value` holds the requested
  //   uncompressed range and is pinned as is.
  // - `materialized_whole` is true: `*physical_value` holds the whole value.
  //   1. Check the logical CRC when verify_checksums is set or when the value
  //      goes into the stable cache. Use `value_crc32c` if the reader already
  //      computed the CRC32C of the uncompressed value in the same pass as its
  //      record check: BlobFileReader::GetBlob() for blob log files
  //      (BlobReadRequest::value_crc32c), or the no-BlobSource
  //      BlockBasedTable::MultiGetSameFileBlob() for relocation files
  //      (SimpleGen2ReadExtras::payload_crc32c in SameFileBlobStandaloneRead).
  //      Then the value is not read a second time.
  //   2. If fill_cache is set and `stable_cache_key` is not null (it is null
  //      without a stable cache), insert the value and pin the cache entry.
  //   3. Otherwise pin the whole value.
  Status FinishIndirectBlobRead(const ReadOptions& read_options,
                                const BlobIndex& blob_index,
                                const CacheKey* stable_cache_key,
                                bool materialized_whole,
                                std::unique_ptr<BlobContents>* physical_value,
                                const std::optional<uint32_t>& value_crc32c,
                                PinnableSlice* result) const;

  static void PinCachedBlob(CacheHandleGuard<BlobContents>* cached_blob,
                            PinnableSlice* value);

  static void PinOwnedBlob(std::unique_ptr<BlobContents>* owned_blob,
                           PinnableSlice* value);

  // Moves the cache handle into `value` and points it at the whole stable value
  // or at [range_offset, range_offset + range_length).
  static void PinStableCachedBlob(
      CacheHandleGuard<StableBlobContents>* cached_blob, uint64_t range_offset,
      size_t range_length, PinnableSlice* value);
  // Checks that the range fits in the cached value, then pins it into
  // `*value`. Returns InvalidArgument for a range outside the value.
  static Status PinStableCacheHit(
      CacheHandleGuard<StableBlobContents>* cached_blob, uint64_t range_offset,
      size_t range_length, PinnableSlice* value);

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
  // Times the decompression of SimpleGen2 records.
  SystemClock* clock_;

  // A cache to store blob file reader.
  BlobFileCache* blob_file_cache_;

  // Serializes the final insert into the stable cache for one stable identity
  // (see PutStableBlobIntoCache). A hash of the cache key picks one of 128
  // mutex stripes (1 << 7). 128 is an estimate that keeps lock contention low
  // at a small memory cost; it was not measured. Each stripe takes one cache
  // line, so the stripes use 8 KB on x86. Every column family has a
  // BlobSource, so the stripes are built on first use, together with
  // stable_key_builder_ (see InitStableCacheState()). A column family that
  // never reads or caches an indirect value does not pay for them.
  static constexpr size_t kNumberOfAdmissionMutexStripes = 1 << 7;
  mutable std::unique_ptr<Striped<CacheAlignedWrapper<port::Mutex>>>
      stable_cache_admission_mutex_;

  // The blob cache that both typed interfaces below use: the configured blob
  // cache, or a ChargedCache wrapper of it when blob cache memory is charged to
  // the block cache.
  std::shared_ptr<Cache> blob_cache_storage_;

  // A cache to store uncompressed blobs.
  mutable SharedCacheInterface blob_cache_;

  // Stable blob cache: the cache entries of indirect values.
  //
  // Background: an indirect value keeps its stable identity (origin, original
  // offset) when indirect blob GC moves it. So its cache key (the stable cache
  // key) uses only that identity, plus db_id and the session
  // (StableBlobCacheKeyBuilder), and a cached value stays valid when the value
  // moves. The entries have their own type (StableBlobContents, which can be
  // saved to a secondary cache). They share the configured blob cache and its
  // charge policy with legacy (kBlob) values, which are keyed by physical file
  // and offset.
  //
  // Life of an entry:
  //   1. Read: GetIndirectBlobFromCache() looks up the key before any blob
  //      file. A hit is used without a check against the BlobIndex (see
  //      there for why).
  //   2. Insert after a file read: PutStableBlobIntoCache() runs after the
  //      logical CRC check. It locks the admission mutex that a hash of the
  //      key picks (one of kNumberOfAdmissionMutexStripes (128) stripes; see
  //      stable_cache_admission_mutex_). Then it looks up the primary cache
  //      again and inserts only on a miss. So an entry that another thread
  //      inserted first is kept.
  //   3. Insert at flush: PrepopulateFlushBlob() inserts a new value directly
  //      (InsertStableBlob()).
  //   4. Secondary cache: when an entry moves to a secondary cache, its cache
  //      item helper writes a header (with its own CRC) and then the value.
  //      When the secondary cache loads an entry back into the primary cache
  //      (a promoted entry), StableBlobContents::CreateFromSaved() checks the
  //      header CRC and the value CRC. If a check fails, the cache item
  //      helper's Create returns Corruption, so the secondary cache drops the
  //      entry and the lookup is a miss. The read then goes to the blob file.
  mutable StableSharedCacheInterface stable_blob_cache_;

  // Returns the stable cache key of (origin, original offset). Requires a
  // stable cache.
  CacheKey StableCacheKey(uint64_t origin_file_number,
                          uint64_t original_value_offset) const;

  // Derives stable cache keys. Built on first use, not at construction:
  // DBImpl sets db_id_ during recovery, after the column families (and their
  // BlobSource) are created, and reads start only after recovery.
  mutable std::once_flag stable_key_builder_once_;
  mutable std::unique_ptr<StableBlobCacheKeyBuilder> stable_key_builder_;

  // Builds stable_key_builder_ and stable_cache_admission_mutex_ once, under
  // stable_key_builder_once_.
  void InitStableCacheState() const;

  // SimpleGen2 records can mix built-in blob compression types in one table.
  // Created once; Decompressor is safe for concurrent use.
  std::shared_ptr<Decompressor> simple_gen2_decompressor_;

  // The control option of how the cache tiers will be used. Currently rocksdb
  // support block/blob cache (volatile tier) and secondary cache (this tier
  // isn't strictly speaking a non-volatile tier since the compressed cache in
  // this tier is in volatile memory).
  const CacheTier lowest_used_cache_tier_;
};

}  // namespace ROCKSDB_NAMESPACE
