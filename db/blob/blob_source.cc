//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_source.h"

#include <cassert>
#include <string>
#include <vector>

#include "cache/cache_reservation_manager.h"
#include "cache/charged_cache.h"
#include "db/blob/blob_contents.h"
#include "db/blob/blob_file_reader.h"
#include "db/blob/blob_gen2_format.h"
#include "db/blob/blob_log_format.h"
#include "file/random_access_file_reader.h"
#include "memory/memory_allocator_impl.h"
#include "monitoring/statistics_impl.h"
#include "options/cf_options.h"
#include "table/get_context.h"
#include "table/multiget_context.h"

namespace ROCKSDB_NAMESPACE {

namespace {

Status AppendBlobRefreshRetryFailure(const Status& stale_status,
                                     const Status& retry_status) {
  assert(stale_status.IsCorruption());
  assert(!retry_status.ok());
  if (retry_status.IsCorruption()) {
    return retry_status;
  }
  return Status::CopyAppendMessage(
      stale_status, "; refresh retry failed: ", retry_status.ToString());
}

// Runs read(reader) against the blob-file reader for `file_number`. On a
// Corruption -- which a stale cached reader (e.g. the physical file was
// replaced) can surface -- evicts the cached reader, reopens it uncached,
// retries read() once, and refreshes the cache on success. Shared by the
// single-blob read paths (GetBlob / GetBlobRange) so the stale-file recovery
// lives in one place.
//
// `read` is invoked with a BlobFileReader* and returns a Status; because it may
// run twice, it must re-initialize any output it populates on each call (and is
// taken by const reference rather than forwarded, since it is not moved-from).
template <typename ReadFn>
Status ReadBlobWithReaderRetry(BlobFileCache* blob_file_cache,
                               const ReadOptions& read_options,
                               const BlobFileOpenInfo& blob_file,
                               const ReadFn& read) {
  CacheHandleGuard<BlobFileReader> blob_file_reader;
  Status s = blob_file_cache->GetBlobFileReader(read_options, blob_file,
                                                &blob_file_reader);
  if (!s.ok()) {
    return s;
  }
  assert(blob_file_reader.GetValue());

  s = read(blob_file_reader.GetValue());
  if (!s.IsCorruption()) {
    return s;
  }

  const Status stale_status = s;
  blob_file_reader.Reset();
  blob_file_cache->Evict(blob_file.file_number);

  std::unique_ptr<BlobFileReader> fresh_reader;
  s = blob_file_cache->OpenBlobFileReaderUncached(
      read_options, blob_file, &fresh_reader,
      /*allow_footer_skip_retry=*/false);
  if (!s.ok()) {
    return AppendBlobRefreshRetryFailure(stale_status, s);
  }

  s = read(fresh_reader.get());
  if (!s.ok()) {
    return AppendBlobRefreshRetryFailure(stale_status, s);
  }

  CacheHandleGuard<BlobFileReader> ignored_reader;
  blob_file_cache
      ->RefreshBlobFileReader(blob_file, &fresh_reader, &ignored_reader)
      .PermitUncheckedError();
  return s;
}

// Records lazy wide-column read metrics for one actual storage read issued
// while resolving a lazy result (attributed via Env::IOActivity::kLazyResolve):
// `bytes_read` bytes were read from storage. Covers both whole-column and
// partial reads; not called for cache hits (no storage read).
void RecordLazyRead(Statistics* statistics, uint64_t bytes_read) {
  RecordTick(statistics, BLOB_DB_LAZY_READ_COUNT);
  RecordTick(statistics, BLOB_DB_LAZY_READ_BYTES, bytes_read);
}

// Additionally records the partial-read metrics for one actual partial
// (byte-range) read: `bytes_read` bytes were fetched instead of the column's
// full `value_size`. Call alongside RecordLazyRead (a partial read is also a
// lazy read).
void RecordLazyPartialRead(Statistics* statistics, uint64_t bytes_read,
                           uint64_t value_size) {
  RecordTick(statistics, BLOB_DB_LAZY_PARTIAL_READ_COUNT);
  if (value_size > bytes_read) {
    RecordTick(statistics, BLOB_DB_LAZY_PARTIAL_BYTES_SAVED,
               value_size - bytes_read);
  }
}

// Pins the sub-range [range_offset, range_offset + range_length) of a cache-hit
// whole blob value into *value (clamped: an offset at/past the end yields
// empty, length is clamped to the remainder), keeping the cached bytes alive by
// transferring the cache handle into *value. Shared by the range-read cache-hit
// paths (GetBlobRange / GetSimpleGen2BlobRange).
void PinCacheHitSubRange(CacheHandleGuard<BlobContents>* blob_handle,
                         uint64_t range_offset, size_t range_length,
                         PinnableSlice* value) {
  const Slice full = blob_handle->GetValue()->data();
  Slice sub;
  if (range_offset < full.size()) {
    const size_t off = static_cast<size_t>(range_offset);
    const size_t avail = full.size() - off;
    const size_t len = range_length > avail ? avail : range_length;
    sub = Slice(full.data() + off, len);
  }  // else: offset at/past end -> empty (not an error)

  value->Reset();
  constexpr Cleanable* cleanable = nullptr;
  value->PinSlice(sub, cleanable);
  blob_handle->TransferTo(value);
}

}  // namespace

BlobSource::BlobSource(const ImmutableOptions& immutable_options,
                       const MutableCFOptions& mutable_cf_options,
                       const std::string& db_id,
                       const std::string& db_session_id,
                       BlobFileCache* blob_file_cache)
    : db_id_(db_id),
      db_session_id_(db_session_id),
      statistics_(immutable_options.statistics.get()),
      blob_file_cache_(blob_file_cache),
      blob_cache_(immutable_options.blob_cache),
      lowest_used_cache_tier_(immutable_options.lowest_used_cache_tier) {
  auto bbto =
      mutable_cf_options.table_factory->GetOptions<BlockBasedTableOptions>();
  if (bbto &&
      bbto->cache_usage_options.options_overrides.at(CacheEntryRole::kBlobCache)
              .charged == CacheEntryRoleOptions::Decision::kEnabled) {
    blob_cache_ = SharedCacheInterface{std::make_shared<ChargedCache>(
        immutable_options.blob_cache, bbto->block_cache)};
  }
}

BlobSource::~BlobSource() = default;

Status BlobSource::GetBlobFromCache(
    const Slice& cache_key, CacheHandleGuard<BlobContents>* cached_blob) const {
  assert(blob_cache_);
  assert(!cache_key.empty());
  assert(cached_blob);
  assert(cached_blob->IsEmpty());

  Cache::Handle* cache_handle = nullptr;
  cache_handle = GetEntryFromCache(cache_key);
  if (cache_handle != nullptr) {
    *cached_blob =
        CacheHandleGuard<BlobContents>(blob_cache_.get(), cache_handle);

    assert(cached_blob->GetValue());

    PERF_COUNTER_ADD(blob_cache_hit_count, 1);
    PERF_COUNTER_ADD(blob_cache_read_byte, cached_blob->GetValue()->size());
    RecordTick(statistics_, BLOB_DB_CACHE_HIT);
    RecordTick(statistics_, BLOB_DB_CACHE_BYTES_READ,
               cached_blob->GetValue()->size());

    return Status::OK();
  }

  RecordTick(statistics_, BLOB_DB_CACHE_MISS);

  return Status::NotFound("Blob not found in cache");
}

Status BlobSource::PutBlobIntoCache(
    const Slice& cache_key, std::unique_ptr<BlobContents>* blob,
    CacheHandleGuard<BlobContents>* cached_blob) const {
  assert(blob_cache_);
  assert(!cache_key.empty());
  assert(blob);
  assert(*blob);
  assert(cached_blob);
  assert(cached_blob->IsEmpty());

  TypedHandle* cache_handle = nullptr;
  const Status s = InsertEntryIntoCache(cache_key, blob->get(), &cache_handle,
                                        Cache::Priority::BOTTOM);
  if (s.ok()) {
    [[maybe_unused]] BlobContents* released_blob = blob->release();

    assert(cache_handle != nullptr);
    *cached_blob =
        CacheHandleGuard<BlobContents>(blob_cache_.get(), cache_handle);

    assert(cached_blob->GetValue());

    RecordTick(statistics_, BLOB_DB_CACHE_ADD);
    RecordTick(statistics_, BLOB_DB_CACHE_BYTES_WRITE,
               cached_blob->GetValue()->size());

  } else {
    RecordTick(statistics_, BLOB_DB_CACHE_ADD_FAILURES);
  }

  return s;
}

BlobSource::TypedHandle* BlobSource::GetEntryFromCache(const Slice& key) const {
  return blob_cache_.LookupFull(key, nullptr /* context */,
                                Cache::Priority::BOTTOM, statistics_,
                                lowest_used_cache_tier_);
}

void BlobSource::PinCachedBlob(CacheHandleGuard<BlobContents>* cached_blob,
                               PinnableSlice* value) {
  assert(cached_blob);
  assert(cached_blob->GetValue());
  assert(value);

  // To avoid copying the cached blob into the buffer provided by the
  // application, we can simply transfer ownership of the cache handle to
  // the target PinnableSlice. This has the potential to save a lot of
  // CPU, especially with large blob values.

  value->Reset();

  constexpr Cleanable* cleanable = nullptr;
  value->PinSlice(cached_blob->GetValue()->data(), cleanable);

  cached_blob->TransferTo(value);
}

void BlobSource::PinOwnedBlob(std::unique_ptr<BlobContents>* owned_blob,
                              PinnableSlice* value) {
  assert(owned_blob);
  assert(*owned_blob);
  assert(value);

  BlobContents* const blob = owned_blob->release();
  assert(blob);

  value->Reset();
  value->PinSlice(
      blob->data(),
      [](void* arg1, void* /* arg2 */) {
        delete static_cast<BlobContents*>(arg1);
      },
      blob, nullptr);
}

Status BlobSource::InsertEntryIntoCache(const Slice& key, BlobContents* value,
                                        TypedHandle** cache_handle,
                                        Cache::Priority priority) const {
  return blob_cache_.InsertFull(key, value, value->ApproximateMemoryUsage(),
                                cache_handle, priority,
                                lowest_used_cache_tier_);
}

Status BlobSource::GetBlob(const ReadOptions& read_options,
                           const Slice& user_key,
                           const BlobFileOpenInfo& blob_file, uint64_t offset,
                           uint64_t file_size, uint64_t value_size,
                           CompressionType compression_type,
                           FilePrefetchBuffer* prefetch_buffer,
                           PinnableSlice* value, uint64_t* bytes_read) {
  assert(value);

  Status s;

  const CacheKey cache_key =
      GetCacheKey(blob_file.file_number, file_size, offset);

  CacheHandleGuard<BlobContents> blob_handle;

  // First, try to get the blob from the cache
  //
  // If blob cache is enabled, we'll try to read from it.
  if (blob_cache_) {
    Slice key = cache_key.AsSlice();
    s = GetBlobFromCache(key, &blob_handle);
    if (s.ok()) {
      PinCachedBlob(&blob_handle, value);

      // For consistency, the size of on-disk (possibly compressed) blob record
      // is assigned to bytes_read.
      uint64_t adjustment =
          read_options.verify_checksums
              ? BlobLogRecord::CalculateAdjustmentForRecordHeader(
                    user_key.size())
              : 0;
      assert(offset >= adjustment);

      uint64_t record_size = value_size + adjustment;
      if (bytes_read) {
        *bytes_read = record_size;
      }
      return s;
    }
  }

  assert(blob_handle.IsEmpty());

  const bool no_io = read_options.read_tier == kBlockCacheTier;
  if (no_io) {
    s = Status::Incomplete("Cannot read blob(s): no disk I/O allowed");
    return s;
  }

  // Can't find the blob from the cache. Since I/O is allowed, read from the
  // file.
  std::unique_ptr<BlobContents> blob_contents;

  {
    MemoryAllocator* const allocator =
        (blob_cache_ && read_options.fill_cache)
            ? blob_cache_.get()->memory_allocator()
            : nullptr;

    uint64_t read_size = 0;
    s = ReadBlobWithReaderRetry(
        blob_file_cache_, read_options, blob_file, [&](BlobFileReader* reader) {
          if (compression_type != reader->GetCompressionType()) {
            return Status::Corruption(
                "Compression type mismatch when reading blob");
          }
          blob_contents.reset();
          read_size = 0;
          return reader->GetBlob(read_options, user_key, offset, value_size,
                                 compression_type, prefetch_buffer, allocator,
                                 &blob_contents, &read_size);
        });
    if (!s.ok()) {
      return s;
    }
    if (bytes_read) {
      *bytes_read = read_size;
    }
    // Whole-column read on the lazy resolve path (partial reads go through
    // GetBlobRange). Counts the storage read; partial reads are not counted
    // here.
    if (read_options.io_activity == Env::IOActivity::kLazyResolve) {
      RecordLazyRead(statistics_, read_size);
    }
  }

  if (blob_cache_ && read_options.fill_cache) {
    // If filling cache is allowed and a cache is configured, try to put the
    // blob to the cache.
    Slice key = cache_key.AsSlice();
    s = PutBlobIntoCache(key, &blob_contents, &blob_handle);
    if (!s.ok()) {
      return s;
    }

    PinCachedBlob(&blob_handle, value);
  } else {
    PinOwnedBlob(&blob_contents, value);
  }

  assert(s.ok());
  return s;
}

Status BlobSource::GetBlobRange(const ReadOptions& read_options,
                                const Slice& user_key,
                                const BlobFileOpenInfo& blob_file,
                                uint64_t offset, uint64_t file_size,
                                uint64_t value_size,
                                CompressionType compression_type,
                                uint64_t range_offset, size_t range_length,
                                PinnableSlice* value, uint64_t* bytes_read) {
  assert(value);
  // Partial reads are for uncompressed blobs only; the caller decides this (a
  // compressed column takes the whole-value GetBlob path and slices).
  assert(compression_type == kNoCompression);
  // Range reads are (currently) only issued while resolving a lazy result, so
  // the lazy read stats below are recorded unconditionally (unlike GetBlob,
  // which gates on the activity). Enforce that invariant here.
  assert(read_options.io_activity == Env::IOActivity::kLazyResolve);

  Status s;

  const CacheKey cache_key =
      GetCacheKey(blob_file.file_number, file_size, offset);

  CacheHandleGuard<BlobContents> blob_handle;

  // First, probe the blob cache for the whole value. On a hit, slice the
  // requested sub-range out of the cached value while pinning the cache handle
  // (zero-copy, no disk read). A partial read never inserts into the cache.
  if (blob_cache_) {
    Slice key = cache_key.AsSlice();
    s = GetBlobFromCache(key, &blob_handle);
    if (s.ok()) {
      PinCacheHitSubRange(&blob_handle, range_offset, range_length, value);
      if (bytes_read) {
        *bytes_read = 0;  // served from cache; no disk read
      }
      return s;
    }
  }

  assert(blob_handle.IsEmpty());

  const bool no_io = read_options.read_tier == kBlockCacheTier;
  if (no_io) {
    s = Status::Incomplete("Cannot read blob(s): no disk I/O allowed");
    return s;
  }

  // Cache miss: read only the requested sub-range from the file. The result is
  // never inserted into the blob cache (see the header comment).
  std::unique_ptr<BlobContents> blob_contents;
  {
    // No cache-fill allocator: a partial value is never inserted into the
    // cache.
    constexpr MemoryAllocator* allocator = nullptr;

    uint64_t read_size = 0;
    // Reuses the stale-reader retry shared with GetBlob. A range read skips
    // whole-record checksum verification, so the retry handles the stale-file
    // case (offset/size mismatch surfacing as Corruption), not a payload
    // checksum failure.
    s = ReadBlobWithReaderRetry(
        blob_file_cache_, read_options, blob_file, [&](BlobFileReader* reader) {
          if (compression_type != reader->GetCompressionType()) {
            return Status::Corruption(
                "Compression type mismatch when reading blob");
          }
          blob_contents.reset();
          read_size = 0;
          return reader->GetBlobRange(read_options, user_key, offset,
                                      value_size, range_offset, range_length,
                                      allocator, &blob_contents, &read_size);
        });
    if (!s.ok()) {
      return s;
    }
    if (bytes_read) {
      *bytes_read = read_size;
    }
    RecordLazyRead(statistics_, read_size);
    RecordLazyPartialRead(statistics_, read_size, value_size);
  }

  PinOwnedBlob(&blob_contents, value);

  assert(s.ok());
  return s;
}

Status BlobSource::GetSimpleGen2Blob(
    const ReadOptions& read_options, const OffsetableCacheKey& base_cache_key,
    RandomAccessFileReader* file, uint64_t record_offset, uint64_t payload_size,
    ChecksumType checksum_type, uint32_t base_context_checksum,
    CompressionType expected_compression, PinnableSlice* value,
    uint64_t* bytes_read) {
  assert(value);
  assert(file);

  const uint64_t record_size = payload_size + kSimpleGen2BlobTrailerSize;

  // The cache key is derived from the SimpleGen2Blob format (shared scheme with
  // block-based SST blocks); see GetSimpleGen2BlobCacheKey.
  const CacheKey cache_key =
      GetSimpleGen2BlobCacheKey(base_cache_key, record_offset);

  Status s;

  CacheHandleGuard<BlobContents> blob_handle;

  // First, try to get the blob from the cache.
  if (blob_cache_) {
    Slice key = cache_key.AsSlice();
    s = GetBlobFromCache(key, &blob_handle);
    if (s.ok()) {
      PinCachedBlob(&blob_handle, value);

      // For consistency, the on-disk record size is assigned to bytes_read on
      // both cache hits and misses.
      if (bytes_read) {
        *bytes_read = record_size;
      }
      return s;
    }
  }

  assert(blob_handle.IsEmpty());

  const bool no_io = read_options.read_tier == kBlockCacheTier;
  if (no_io) {
    s = Status::Incomplete("Cannot read blob(s): no disk I/O allowed");
    return s;
  }

  // Cache miss (or no cache configured). Read the record into a buffer
  // allocated from the blob cache's memory allocator when we intend to insert
  // it, exposing the uncompressed payload as BlobContents (the trailer just
  // sits unused at the tail of the buffer).
  MemoryAllocator* const allocator = (blob_cache_ && read_options.fill_cache)
                                         ? blob_cache_.get()->memory_allocator()
                                         : nullptr;

  CacheAllocationPtr buf =
      AllocateBlock(static_cast<size_t>(record_size), allocator);
  s = ReadAndVerifySimpleGen2BlobRecord(
      read_options, file, record_offset, static_cast<size_t>(payload_size),
      static_cast<size_t>(record_size), checksum_type, base_context_checksum,
      expected_compression, buf.get());
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<BlobContents> blob_contents(
      new BlobContents(std::move(buf), static_cast<size_t>(payload_size)));

  // Record the per-read statistics (mirrors BlobFileReader::GetBlob).
  RecordTick(statistics_, BLOB_DB_BLOB_FILE_BYTES_READ, record_size);
  PERF_COUNTER_ADD(blob_read_count, 1);
  PERF_COUNTER_ADD(blob_read_byte, record_size);
  if (bytes_read) {
    *bytes_read = record_size;
  }
  // Whole-column read on the lazy resolve path (partial reads go through
  // GetSimpleGen2BlobRange).
  if (read_options.io_activity == Env::IOActivity::kLazyResolve) {
    RecordLazyRead(statistics_, record_size);
  }

  if (blob_cache_ && read_options.fill_cache) {
    // If filling cache is allowed and a cache is configured, try to put the
    // blob into the cache.
    Slice key = cache_key.AsSlice();
    s = PutBlobIntoCache(key, &blob_contents, &blob_handle);
    if (!s.ok()) {
      return s;
    }

    PinCachedBlob(&blob_handle, value);
  } else {
    PinOwnedBlob(&blob_contents, value);
  }

  assert(s.ok());
  return s;
}

Status BlobSource::GetSimpleGen2BlobRange(
    const ReadOptions& read_options, const OffsetableCacheKey& base_cache_key,
    RandomAccessFileReader* file, uint64_t record_offset, uint64_t payload_size,
    ChecksumType /*checksum_type*/, uint32_t /*base_context_checksum*/,
    CompressionType expected_compression, uint64_t range_offset,
    size_t range_length, PinnableSlice* value, uint64_t* bytes_read) {
  assert(value);
  assert(file);
  // Partial reads are for uncompressed payloads only; the caller decides this
  // (a compressed column takes the whole-payload GetSimpleGen2Blob path +
  // slice).
  assert(expected_compression == kNoCompression);
  // Range reads are (currently) only issued while resolving a lazy result, so
  // the lazy read stats below are recorded unconditionally (unlike
  // GetSimpleGen2Blob, which gates on the activity). Enforce that invariant
  // here.
  assert(read_options.io_activity == Env::IOActivity::kLazyResolve);

  const CacheKey cache_key =
      GetSimpleGen2BlobCacheKey(base_cache_key, record_offset);

  Status s;

  CacheHandleGuard<BlobContents> blob_handle;

  // First, probe the blob cache for the whole payload. On a hit, slice the
  // requested sub-range out of the cached payload while pinning the cache
  // handle (zero-copy, no disk read). A partial read never inserts into the
  // cache.
  if (blob_cache_) {
    Slice key = cache_key.AsSlice();
    s = GetBlobFromCache(key, &blob_handle);
    if (s.ok()) {
      PinCacheHitSubRange(&blob_handle, range_offset, range_length, value);
      if (bytes_read) {
        *bytes_read = 0;  // served from cache; no disk read
      }
      return s;
    }
  }

  assert(blob_handle.IsEmpty());

  const bool no_io = read_options.read_tier == kBlockCacheTier;
  if (no_io) {
    s = Status::Incomplete("Cannot read blob(s): no disk I/O allowed");
    return s;
  }

  // Cache miss: read only the requested sub-range from the file. The result is
  // never inserted into the blob cache (a partial payload cannot represent the
  // whole-record cache entry).
  CacheAllocationPtr buf = AllocateBlock(range_length, /*allocator=*/nullptr);
  s = ReadSimpleGen2BlobRange(read_options, file, record_offset,
                              static_cast<size_t>(payload_size), range_offset,
                              range_length, expected_compression, buf.get());
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<BlobContents> blob_contents(
      new BlobContents(std::move(buf), range_length));

  RecordTick(statistics_, BLOB_DB_BLOB_FILE_BYTES_READ, range_length);
  PERF_COUNTER_ADD(blob_read_count, 1);
  PERF_COUNTER_ADD(blob_read_byte, range_length);
  RecordLazyRead(statistics_, range_length);
  RecordLazyPartialRead(statistics_, range_length, payload_size);
  if (bytes_read) {
    *bytes_read = range_length;
  }

  PinOwnedBlob(&blob_contents, value);

  assert(s.ok());
  return s;
}

void BlobSource::MultiGetSimpleGen2Blob(
    const ReadOptions& read_options, const OffsetableCacheKey& base_cache_key,
    RandomAccessFileReader* file, ChecksumType checksum_type,
    uint32_t base_context_checksum, size_t num_records,
    SimpleGen2BlobReadRequest* reqs) {
  assert(file);
  if (num_records == 0) {
    return;
  }

  const bool no_io = read_options.read_tier == kBlockCacheTier;
  MemoryAllocator* const allocator = (blob_cache_ && read_options.fill_cache)
                                         ? blob_cache_.get()->memory_allocator()
                                         : nullptr;

  // Records that missed the cache and must be read from disk, in read order;
  // each owns its buffer until we build a BlobContents from it below.
  struct Pending {
    size_t idx;
    CacheAllocationPtr buf;
    uint64_t record_size;
  };
  std::vector<Pending> pending;
  std::vector<SimpleGen2RecordReadRequest> read_reqs;

  for (size_t i = 0; i < num_records; ++i) {
    SimpleGen2BlobReadRequest& req = reqs[i];
    assert(req.result);
    assert(req.status);

    const uint64_t record_size = req.payload_size + kSimpleGen2BlobTrailerSize;
    const CacheKey cache_key =
        GetSimpleGen2BlobCacheKey(base_cache_key, req.record_offset);

    if (blob_cache_) {
      CacheHandleGuard<BlobContents> blob_handle;
      const Status cs = GetBlobFromCache(cache_key.AsSlice(), &blob_handle);
      if (cs.ok()) {
        *req.status = cs;
        PinCachedBlob(&blob_handle, req.result);
        continue;
      }
    }

    if (no_io) {
      *req.status =
          Status::Incomplete("Cannot read blob(s): no disk I/O allowed");
      continue;
    }

    CacheAllocationPtr buf =
        AllocateBlock(static_cast<size_t>(record_size), allocator);
    read_reqs.push_back(SimpleGen2RecordReadRequest{
        req.record_offset, static_cast<size_t>(req.payload_size),
        static_cast<size_t>(record_size), req.expected_compression, buf.get(),
        req.status});
    pending.push_back(Pending{i, std::move(buf), record_size});
  }

  if (read_reqs.empty()) {
    return;
  }

  ReadAndVerifySimpleGen2BlobRecords(read_options, file, checksum_type,
                                     base_context_checksum, read_reqs.size(),
                                     read_reqs.data());

  for (size_t k = 0; k < pending.size(); ++k) {
    SimpleGen2BlobReadRequest& req = reqs[pending[k].idx];
    if (!req.status->ok()) {
      continue;
    }
    const uint64_t record_size = pending[k].record_size;
    std::unique_ptr<BlobContents> blob_contents(new BlobContents(
        std::move(pending[k].buf), static_cast<size_t>(req.payload_size)));

    RecordTick(statistics_, BLOB_DB_BLOB_FILE_BYTES_READ, record_size);
    PERF_COUNTER_ADD(blob_read_count, 1);
    PERF_COUNTER_ADD(blob_read_byte, record_size);
    if (read_options.io_activity == Env::IOActivity::kLazyResolve) {
      RecordLazyRead(statistics_, record_size);
    }

    if (blob_cache_ && read_options.fill_cache) {
      const CacheKey cache_key =
          GetSimpleGen2BlobCacheKey(base_cache_key, req.record_offset);
      CacheHandleGuard<BlobContents> blob_handle;
      const Status ps =
          PutBlobIntoCache(cache_key.AsSlice(), &blob_contents, &blob_handle);
      if (!ps.ok()) {
        *req.status = ps;
      } else {
        PinCachedBlob(&blob_handle, req.result);
      }
    } else {
      PinOwnedBlob(&blob_contents, req.result);
    }
  }
}

void BlobSource::MultiGetSimpleGen2BlobRange(
    const ReadOptions& read_options, const OffsetableCacheKey& base_cache_key,
    RandomAccessFileReader* file, size_t num_records,
    SimpleGen2BlobRangeReadRequest* reqs) {
  assert(file);
  // Range reads are only issued while resolving a lazy result (partial reads
  // are lazy-only), so the lazy read stats below are recorded unconditionally.
  assert(read_options.io_activity == Env::IOActivity::kLazyResolve);
  if (num_records == 0) {
    return;
  }

  const bool no_io = read_options.read_tier == kBlockCacheTier;

  struct Pending {
    size_t idx;
    CacheAllocationPtr buf;
  };
  std::vector<Pending> pending;
  std::vector<SimpleGen2RangeReadRequest> read_reqs;

  for (size_t i = 0; i < num_records; ++i) {
    SimpleGen2BlobRangeReadRequest& req = reqs[i];
    assert(req.result);
    assert(req.status);

    const CacheKey cache_key =
        GetSimpleGen2BlobCacheKey(base_cache_key, req.record_offset);

    // Probe the whole-payload cache; on a hit slice the requested sub-range.
    if (blob_cache_) {
      CacheHandleGuard<BlobContents> blob_handle;
      const Status cs = GetBlobFromCache(cache_key.AsSlice(), &blob_handle);
      if (cs.ok()) {
        *req.status = cs;
        PinCacheHitSubRange(&blob_handle, req.range_offset, req.range_length,
                            req.result);
        continue;
      }
    }

    if (no_io) {
      *req.status =
          Status::Incomplete("Cannot read blob(s): no disk I/O allowed");
      continue;
    }

    // No cache-fill allocator: a partial payload is never inserted.
    CacheAllocationPtr buf =
        AllocateBlock(req.range_length, /*allocator=*/nullptr);
    read_reqs.push_back(SimpleGen2RangeReadRequest{
        req.record_offset, static_cast<size_t>(req.payload_size),
        req.range_offset, req.range_length, req.expected_compression, buf.get(),
        req.status});
    pending.push_back(Pending{i, std::move(buf)});
  }

  if (read_reqs.empty()) {
    return;
  }

  ReadSimpleGen2BlobRanges(read_options, file, read_reqs.size(),
                           read_reqs.data());

  for (size_t k = 0; k < pending.size(); ++k) {
    SimpleGen2BlobRangeReadRequest& req = reqs[pending[k].idx];
    if (!req.status->ok()) {
      continue;
    }
    std::unique_ptr<BlobContents> blob_contents(
        new BlobContents(std::move(pending[k].buf), req.range_length));

    RecordTick(statistics_, BLOB_DB_BLOB_FILE_BYTES_READ, req.range_length);
    PERF_COUNTER_ADD(blob_read_count, 1);
    PERF_COUNTER_ADD(blob_read_byte, req.range_length);
    RecordLazyRead(statistics_, req.range_length);
    RecordLazyPartialRead(statistics_, req.range_length, req.payload_size);

    PinOwnedBlob(&blob_contents, req.result);
  }
}

void BlobSource::MultiGetBlob(const ReadOptions& read_options,
                              autovector<BlobFileReadRequests>& blob_reqs,
                              uint64_t* bytes_read) {
  assert(blob_reqs.size() > 0);

  uint64_t total_bytes_read = 0;
  uint64_t bytes_read_in_file = 0;

  for (auto& [blob_file, file_size, blob_reqs_in_file] : blob_reqs) {
    // sort blob_reqs_in_file by file offset.
    std::sort(
        blob_reqs_in_file.begin(), blob_reqs_in_file.end(),
        [](const BlobReadRequest& lhs, const BlobReadRequest& rhs) -> bool {
          return lhs.offset < rhs.offset;
        });

    MultiGetBlobFromOneFile(read_options, blob_file, file_size,
                            blob_reqs_in_file, &bytes_read_in_file);

    total_bytes_read += bytes_read_in_file;
  }

  if (bytes_read) {
    *bytes_read = total_bytes_read;
  }
}

void BlobSource::MultiGetBlobFromOneFile(const ReadOptions& read_options,
                                         const BlobFileOpenInfo& blob_file,
                                         uint64_t /*file_size*/,
                                         autovector<BlobReadRequest>& blob_reqs,
                                         uint64_t* bytes_read) {
  const size_t num_blobs = blob_reqs.size();
  assert(num_blobs > 0);
  assert(num_blobs <= MultiGetContext::MAX_BATCH_SIZE);

#ifndef NDEBUG
  for (size_t i = 0; i < num_blobs - 1; ++i) {
    assert(blob_reqs[i].offset <= blob_reqs[i + 1].offset);
  }
#endif  // !NDEBUG

  using Mask = uint64_t;
  Mask cache_hit_mask = 0;

  uint64_t total_bytes = 0;
  const uint64_t file_number = blob_file.file_number;
  const OffsetableCacheKey base_cache_key(db_id_, db_session_id_, file_number);

  if (blob_cache_) {
    size_t cached_blob_count = 0;
    for (size_t i = 0; i < num_blobs; ++i) {
      auto& req = blob_reqs[i];

      CacheHandleGuard<BlobContents> blob_handle;
      const CacheKey cache_key = base_cache_key.WithOffset(req.offset);
      const Slice key = cache_key.AsSlice();

      const Status s = GetBlobFromCache(key, &blob_handle);

      if (s.ok()) {
        assert(req.status);
        *req.status = s;

        PinCachedBlob(&blob_handle, req.result);

        // Update the counter for the number of valid blobs read from the cache.
        ++cached_blob_count;

        // For consistency, the size of each on-disk (possibly compressed) blob
        // record is accumulated to total_bytes.
        uint64_t adjustment =
            read_options.verify_checksums
                ? BlobLogRecord::CalculateAdjustmentForRecordHeader(
                      req.user_key->size())
                : 0;
        assert(req.offset >= adjustment);
        total_bytes += req.len + adjustment;
        cache_hit_mask |= (Mask{1} << i);  // cache hit
      }
    }

    // All blobs were read from the cache.
    if (cached_blob_count == num_blobs) {
      if (bytes_read) {
        *bytes_read = total_bytes;
      }
      return;
    }
  }

  const bool no_io = read_options.read_tier == kBlockCacheTier;
  if (no_io) {
    for (size_t i = 0; i < num_blobs; ++i) {
      if (!(cache_hit_mask & (Mask{1} << i))) {
        BlobReadRequest& req = blob_reqs[i];
        assert(req.status);

        *req.status =
            Status::Incomplete("Cannot read blob(s): no disk I/O allowed");
      }
    }
    return;
  }

  {
    // Find the rest of blobs from the file since I/O is allowed.
    autovector<std::pair<BlobReadRequest*, std::unique_ptr<BlobContents>>>
        _blob_reqs;
    uint64_t _bytes_read = 0;

    for (size_t i = 0; i < num_blobs; ++i) {
      if (!(cache_hit_mask & (Mask{1} << i))) {
        _blob_reqs.emplace_back(&blob_reqs[i], std::unique_ptr<BlobContents>());
      }
    }

    CacheHandleGuard<BlobFileReader> blob_file_reader;
    Status s = blob_file_cache_->GetBlobFileReader(read_options, blob_file,
                                                   &blob_file_reader);
    if (!s.ok()) {
      for (size_t i = 0; i < _blob_reqs.size(); ++i) {
        BlobReadRequest* const req = _blob_reqs[i].first;
        assert(req);
        assert(req->status);

        *req->status = s;
      }
      return;
    }

    assert(blob_file_reader.GetValue());

    MemoryAllocator* const allocator =
        (blob_cache_ && read_options.fill_cache)
            ? blob_cache_.get()->memory_allocator()
            : nullptr;

    blob_file_reader.GetValue()->MultiGetBlob(read_options, allocator,
                                              _blob_reqs, &_bytes_read);

    bool needs_reader_refresh = false;
    for (const auto& blob_req : _blob_reqs) {
      BlobReadRequest* const req = blob_req.first;
      assert(req != nullptr);
      assert(req->status != nullptr);
      if (req->status->IsCorruption()) {
        needs_reader_refresh = true;
        break;
      }
    }

    if (needs_reader_refresh) {
      blob_file_reader.Reset();
      blob_file_cache_->Evict(file_number);

      std::unique_ptr<BlobFileReader> fresh_reader;
      const Status open_status = blob_file_cache_->OpenBlobFileReaderUncached(
          read_options, blob_file, &fresh_reader,
          /*allow_footer_skip_retry=*/false);
      if (!open_status.ok()) {
        // Only the corrupted requests fail. The other requests in the batch
        // already read their blobs, so fall through to pin them and update
        // the stats below.
        for (const auto& blob_req : _blob_reqs) {
          BlobReadRequest* const req = blob_req.first;
          assert(req != nullptr);
          assert(req->status != nullptr);
          if (req->status->IsCorruption()) {
            *req->status =
                AppendBlobRefreshRetryFailure(*req->status, open_status);
          }
        }
      } else {
        autovector<std::pair<BlobReadRequest*, std::unique_ptr<BlobContents>>>
            retry_blob_reqs;
        autovector<Status> stale_statuses;
        for (auto& blob_req : _blob_reqs) {
          BlobReadRequest* const req = blob_req.first;
          assert(req != nullptr);
          assert(req->status != nullptr);
          if (!req->status->IsCorruption()) {
            continue;
          }

          stale_statuses.emplace_back(*req->status);
          *req->status = Status::OK();
          blob_req.second.reset();
          retry_blob_reqs.emplace_back(req, std::unique_ptr<BlobContents>());
        }

        uint64_t refreshed_bytes_read = 0;
        fresh_reader->MultiGetBlob(read_options, allocator, retry_blob_reqs,
                                   &refreshed_bytes_read);
        _bytes_read += refreshed_bytes_read;

        bool install_fresh_reader = false;
        for (size_t i = 0; i < retry_blob_reqs.size(); ++i) {
          auto& retried_blob_req = retry_blob_reqs[i];
          BlobReadRequest* const retried_req = retried_blob_req.first;
          assert(retried_req != nullptr);
          if (retried_req->status->ok()) {
            install_fresh_reader = true;
          } else {
            *retried_req->status = AppendBlobRefreshRetryFailure(
                stale_statuses[i], *retried_req->status);
          }

          for (auto& blob_req : _blob_reqs) {
            if (blob_req.first != retried_req) {
              continue;
            }

            blob_req.second = std::move(retried_blob_req.second);
            break;
          }
        }

        if (install_fresh_reader) {
          CacheHandleGuard<BlobFileReader> ignored_reader;
          blob_file_cache_
              ->RefreshBlobFileReader(blob_file, &fresh_reader, &ignored_reader)
              .PermitUncheckedError();
        }
      }
    }

    if (blob_cache_ && read_options.fill_cache) {
      // If filling cache is allowed and a cache is configured, try to put
      // the blob(s) to the cache.
      for (auto& [req, blob_contents] : _blob_reqs) {
        assert(req);

        if (req->status->ok()) {
          CacheHandleGuard<BlobContents> blob_handle;
          const CacheKey cache_key = base_cache_key.WithOffset(req->offset);
          const Slice key = cache_key.AsSlice();
          s = PutBlobIntoCache(key, &blob_contents, &blob_handle);
          if (!s.ok()) {
            *req->status = s;
          } else {
            PinCachedBlob(&blob_handle, req->result);
          }
        }
      }
    } else {
      for (auto& [req, blob_contents] : _blob_reqs) {
        assert(req);

        if (req->status->ok()) {
          PinOwnedBlob(&blob_contents, req->result);
        }
      }
    }

    // Whole-column reads on the lazy resolve path count as lazy reads (mirrors
    // BlobSource::GetBlob, which records only on a disk read, not a cache hit).
    // Gated on the activity so regular MultiGet is unaffected; the misses read
    // above are exactly the disk reads for these blob columns.
    if (read_options.io_activity == Env::IOActivity::kLazyResolve) {
      for (auto& [req, blob_contents] : _blob_reqs) {
        assert(req);
        if (req->status->ok()) {
          const uint64_t adjustment =
              read_options.verify_checksums
                  ? BlobLogRecord::CalculateAdjustmentForRecordHeader(
                        req->user_key->size())
                  : 0;
          RecordLazyRead(statistics_, req->len + adjustment);
        }
      }
    }

    total_bytes += _bytes_read;
    if (bytes_read) {
      *bytes_read = total_bytes;
    }
  }
}

void BlobSource::MultiGetBlobRange(
    const ReadOptions& read_options,
    autovector<BlobFileRangeReadRequests>& blob_reqs, uint64_t* bytes_read) {
  assert(blob_reqs.size() > 0);

  uint64_t total_bytes_read = 0;
  uint64_t bytes_read_in_file = 0;

  for (auto& [blob_file, file_size, blob_reqs_in_file] : blob_reqs) {
    // Sort by effective read offset so the file system layer can coalesce
    // adjacent sub-range reads within one MultiRead.
    std::sort(blob_reqs_in_file.begin(), blob_reqs_in_file.end(),
              [](const BlobRangeReadRequest& lhs,
                 const BlobRangeReadRequest& rhs) -> bool {
                return lhs.offset + lhs.range_offset <
                       rhs.offset + rhs.range_offset;
              });

    MultiGetBlobRangeFromOneFile(read_options, blob_file, file_size,
                                 blob_reqs_in_file, &bytes_read_in_file);

    total_bytes_read += bytes_read_in_file;
    bytes_read_in_file = 0;  // reset: some early-return paths do not write it
  }

  if (bytes_read) {
    *bytes_read = total_bytes_read;
  }
}

void BlobSource::MultiGetBlobRangeFromOneFile(
    const ReadOptions& read_options, const BlobFileOpenInfo& blob_file,
    uint64_t /*file_size*/, autovector<BlobRangeReadRequest>& blob_reqs,
    uint64_t* bytes_read) {
  const size_t num_blobs = blob_reqs.size();
  assert(num_blobs > 0);
  assert(num_blobs <= MultiGetContext::MAX_BATCH_SIZE);
  // Range reads are only issued while resolving a lazy result (partial reads
  // are lazy-only), so the lazy read stats below are recorded unconditionally.
  assert(read_options.io_activity == Env::IOActivity::kLazyResolve);

  using Mask = uint64_t;
  Mask cache_hit_mask = 0;

  uint64_t total_bytes = 0;
  const uint64_t file_number = blob_file.file_number;
  const OffsetableCacheKey base_cache_key(db_id_, db_session_id_, file_number);

  // Probe the whole-value cache per request; on a hit, slice the requested
  // sub-range (zero-copy, no disk read). A partial read is never cache-filled.
  if (blob_cache_) {
    size_t cached_blob_count = 0;
    for (size_t i = 0; i < num_blobs; ++i) {
      BlobRangeReadRequest& req = blob_reqs[i];
      assert(req.status);

      CacheHandleGuard<BlobContents> blob_handle;
      const CacheKey cache_key = base_cache_key.WithOffset(req.offset);
      const Status s = GetBlobFromCache(cache_key.AsSlice(), &blob_handle);
      if (s.ok()) {
        *req.status = s;
        PinCacheHitSubRange(&blob_handle, req.range_offset, req.range_length,
                            req.result);
        ++cached_blob_count;
        cache_hit_mask |= (Mask{1} << i);  // hit -> no disk read
      }
    }

    if (cached_blob_count == num_blobs) {
      if (bytes_read) {
        *bytes_read = 0;
      }
      return;
    }
  }

  const bool no_io = read_options.read_tier == kBlockCacheTier;
  if (no_io) {
    for (size_t i = 0; i < num_blobs; ++i) {
      if (!(cache_hit_mask & (Mask{1} << i))) {
        assert(blob_reqs[i].status);
        *blob_reqs[i].status =
            Status::Incomplete("Cannot read blob(s): no disk I/O allowed");
      }
    }
    return;
  }

  autovector<std::pair<BlobRangeReadRequest*, std::unique_ptr<BlobContents>>>
      _blob_reqs;
  for (size_t i = 0; i < num_blobs; ++i) {
    if (!(cache_hit_mask & (Mask{1} << i))) {
      _blob_reqs.emplace_back(&blob_reqs[i], std::unique_ptr<BlobContents>());
    }
  }

  // Lazy range reads run against a pinned SuperVersion (max_open_files == -1),
  // and blob file numbers are never reused, so the stale cached-reader race
  // that MultiGetBlobFromOneFile guards against cannot occur here; a Corruption
  // is simply surfaced per request. Hence no reader-refresh retry and no
  // cache-fill (a partial value cannot represent the whole-record cache entry).
  CacheHandleGuard<BlobFileReader> blob_file_reader;
  Status s = blob_file_cache_->GetBlobFileReader(read_options, blob_file,
                                                 &blob_file_reader);
  if (!s.ok()) {
    for (auto& blob_req : _blob_reqs) {
      assert(blob_req.first);
      assert(blob_req.first->status);
      *blob_req.first->status = s;
    }
    return;
  }
  assert(blob_file_reader.GetValue());

  uint64_t _bytes_read = 0;
  blob_file_reader.GetValue()->MultiGetBlobRange(read_options, _blob_reqs,
                                                 &_bytes_read);

  for (auto& [req, blob_contents] : _blob_reqs) {
    assert(req);
    if (req->status->ok()) {
      PinOwnedBlob(&blob_contents, req->result);
      RecordLazyRead(statistics_, req->range_length);
      RecordLazyPartialRead(statistics_, req->range_length, req->value_size);
    }
  }

  total_bytes += _bytes_read;
  if (bytes_read) {
    *bytes_read = total_bytes;
  }
}

bool BlobSource::TEST_BlobInCache(uint64_t file_number, uint64_t file_size,
                                  uint64_t offset, size_t* charge) const {
  const CacheKey cache_key = GetCacheKey(file_number, file_size, offset);
  const Slice key = cache_key.AsSlice();

  CacheHandleGuard<BlobContents> blob_handle;
  const Status s = GetBlobFromCache(key, &blob_handle);

  if (s.ok() && blob_handle.GetValue() != nullptr) {
    if (charge) {
      const Cache* const cache = blob_handle.GetCache();
      assert(cache);

      Cache::Handle* const handle = blob_handle.GetCacheHandle();
      assert(handle);

      *charge = cache->GetUsage(handle);
    }

    return true;
  }

  return false;
}

}  // namespace ROCKSDB_NAMESPACE
