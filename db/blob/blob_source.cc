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
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/version_edit.h"
#include "file/random_access_file_reader.h"
#include "memory/memory_allocator_impl.h"
#include "monitoring/statistics_impl.h"
#include "options/cf_options.h"
#include "table/get_context.h"
#include "table/multiget_context.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/math.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

namespace {

Status ValidateIndirectBlobIndexForRead(const BlobIndex& blob_index,
                                        bool is_relocation_file) {
  if (!blob_index.IsIndirect()) {
    return Status::InvalidArgument("Expected indirect blob index");
  }
  Status status = blob_index.Validate();
  if (!status.ok()) {
    return status;
  }
  if (!CompressionTypeSupported(blob_index.compression()) ||
      (is_relocation_file &&
       blob_index.size() > kMaxSimpleGen2BlobPayloadSize)) {
    return Status::Corruption(
        "Indirect blob size or compression is not supported");
  }
  return Status::OK();
}

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

// Partitions already sorted request indexes into the minimum first-fit set of
// batches whose physical byte intervals do not overlap. FileSystem::MultiRead
// requires that contract, while duplicate stable references and overlapping
// lazy ranges are valid API inputs.
template <typename GetOffset, typename GetLength>
std::vector<std::vector<size_t>> PartitionNonOverlappingReadBatches(
    const std::vector<size_t>& sorted_indexes, const GetOffset& get_offset,
    const GetLength& get_length) {
  std::vector<std::vector<size_t>> batches;
  std::vector<uint64_t> batch_ends;
  for (size_t index : sorted_indexes) {
    const uint64_t offset = get_offset(index);
    const uint64_t length = get_length(index);
    const uint64_t end = length > std::numeric_limits<uint64_t>::max() - offset
                             ? std::numeric_limits<uint64_t>::max()
                             : offset + length;
    size_t batch_index = batches.size();
    for (size_t i = 0; i < batches.size(); ++i) {
      if (batch_ends[i] <= offset) {
        batch_index = i;
        break;
      }
    }
    if (batch_index == batches.size()) {
      batches.emplace_back();
      batch_ends.push_back(end);
    } else {
      batch_ends[batch_index] = end;
    }
    batches[batch_index].push_back(index);
  }
  return batches;
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

CacheKey GetStableBlobCacheKey(const std::string& db_id,
                               const std::string& db_session_id,
                               uint64_t origin_file_number,
                               uint64_t original_value_offset) {
  static_assert(kFileNumberMask == 0x3fffffffffffffffULL);
  assert(origin_file_number != 0 && origin_file_number <= kFileNumberMask);
  OffsetableCacheKey physical_base(db_id, db_session_id, origin_file_number);
  UniqueId64x2 id = physical_base.ToInternalUniqueId();
  // For this DB/session all physical raw first words have the same low two
  // bits P: ReverseBits(file_number) cannot change them. OffsetableCacheKey
  // swaps a zero raw first word with ReverseBits(session_lower), however.
  // A fixed bit-63 domain alone is therefore not enough to separate final
  // keys at arbitrary offsets. Choose an invalid-file-number domain whose
  // raw first word has nonzero low bits Q different from P and, when P=0,
  // also from the possible swapped physical first word's low bits.
  const uint64_t physical_low_bits =
      (DownwardInvolution(id[0]) ^ ReverseBits(id[1])) & 3;
  const uint64_t swapped_low_bits = ReverseBits(id[0]) & 3;
  for (uint64_t domain = 1; domain <= 3; ++domain) {
    const uint64_t stable_low_bits = physical_low_bits ^ domain;
    if (stable_low_bits != 0 &&
        (physical_low_bits != 0 || stable_low_bits != swapped_low_bits)) {
      // At most two of the three domains are forbidden. This selection
      // depends only on the session, not the origin's lower 62 bits. Stable
      // first words never swap and remain distinct for distinct origins.
      // Offsets change only the second word; reservations have first word 0.
      // Cross-session uniqueness retains the ordinary cache-key guarantee.
      id[1] ^= ReverseBits(domain);
      return OffsetableCacheKey::FromInternalUniqueId(&id).WithOffset(
          original_value_offset);
    }
  }
  assert(false);
  return CacheKey();
}

std::shared_ptr<Cache> GetBlobCacheWithReservation(
    const ImmutableOptions& immutable_options,
    const MutableCFOptions& mutable_cf_options) {
  std::shared_ptr<Cache> blob_cache = immutable_options.blob_cache;
  const BlockBasedTableOptions* const bbto =
      mutable_cf_options.table_factory->GetOptions<BlockBasedTableOptions>();
  if (blob_cache && bbto &&
      bbto->cache_usage_options.options_overrides.at(CacheEntryRole::kBlobCache)
              .charged == CacheEntryRoleOptions::Decision::kEnabled) {
    blob_cache = std::make_shared<ChargedCache>(blob_cache, bbto->block_cache);
  }
  return blob_cache;
}

BlobSource::BlobSource(const ImmutableOptions& immutable_options,
                       const MutableCFOptions& mutable_cf_options,
                       const std::string& db_id,
                       const std::string& db_session_id,
                       BlobFileCache* blob_file_cache)
    : db_id_(db_id),
      db_session_id_(db_session_id),
      statistics_(immutable_options.statistics.get()),
      clock_(immutable_options.clock),
      blob_file_cache_(blob_file_cache),
      blob_cache_storage_(
          GetBlobCacheWithReservation(immutable_options, mutable_cf_options)),
      blob_cache_(blob_cache_storage_),
      stable_blob_cache_(blob_cache_storage_),
      simple_gen2_decompressor_(
          GetBuiltinV2CompressionManager()->GetDecompressor()),
      lowest_used_cache_tier_(immutable_options.lowest_used_cache_tier) {}

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

Status BlobSource::GetStableBlobFromCache(
    const Slice& cache_key, const BlobIndex& blob_index,
    CacheHandleGuard<StableBlobContents>* cached_blob,
    bool record_statistics) const {
  assert(stable_blob_cache_);
  assert(blob_index.IsIndirect());
  assert(cached_blob != nullptr);
  assert(cached_blob->IsEmpty());

  StableTypedHandle* const cache_handle = stable_blob_cache_.LookupFull(
      cache_key, nullptr /* context */, Cache::Priority::BOTTOM, statistics_,
      lowest_used_cache_tier_);
  if (cache_handle == nullptr) {
    if (record_statistics) {
      RecordTick(statistics_, BLOB_DB_CACHE_MISS);
    }
    return Status::NotFound("Stable blob not found in cache");
  }

  *cached_blob = CacheHandleGuard<StableBlobContents>(stable_blob_cache_.get(),
                                                      cache_handle);
  const StableBlobContents* const contents = cached_blob->GetValue();
  assert(contents != nullptr);
  Status status = contents->validation_status();
  if (!status.ok()) {
    cached_blob->Reset();
    stable_blob_cache_.get()->Erase(cache_key);
    return status;
  }
  if (contents->stored_value_size() != blob_index.size() ||
      contents->compression() != blob_index.compression() ||
      contents->stable_checksum_version() !=
          blob_index.stable_checksum_version() ||
      contents->stable_checksum() != blob_index.stable_checksum() ||
      contents->stored_checksum() != blob_index.stored_checksum()) {
    // The incumbent is valid for a different fingerprint. Preserve it: the
    // caller's metadata is the conflicting input and must not poison the
    // stable identity for subsequent readers.
    cached_blob->Reset();
    return Status::Corruption("Conflicting stable blob cache metadata");
  }

  if (record_statistics) {
    PERF_COUNTER_ADD(blob_cache_hit_count, 1);
    PERF_COUNTER_ADD(blob_cache_read_byte, contents->size());
    RecordTick(statistics_, BLOB_DB_CACHE_HIT);
    RecordTick(statistics_, BLOB_DB_CACHE_BYTES_READ, contents->size());
  }
  return Status::OK();
}

Status BlobSource::PutStableBlobIntoCache(
    const Slice& cache_key, const BlobIndex& blob_index,
    const Slice& logical_value,
    CacheHandleGuard<StableBlobContents>* cached_blob) const {
  assert(stable_blob_cache_);
  assert(blob_index.IsIndirect());
  assert(cached_blob != nullptr);
  assert(cached_blob->IsEmpty());

  MutexLock admission_lock(
      &blob_file_cache_->GetStableCacheAdmissionMutex(cache_key));
  Status status = GetStableBlobFromCache(cache_key, blob_index, cached_blob,
                                         /*record_statistics=*/false);
  if (status.ok()) {
    return status;
  }
  if (!status.IsNotFound()) {
    return status;
  }

  // FinishIndirectBlobRead has validated the logical CRC before admission.
  // Allocate/copy only after the serialized incumbent check: concurrent
  // readers of the same stable identity need not build redundant cache entries.
  std::unique_ptr<StableBlobContents> contents;
  status = StableBlobContents::CreateWithVerifiedValue(
      logical_value, blob_index.size(), blob_index.compression(),
      blob_index.stable_checksum_version(), blob_index.stable_checksum(),
      blob_index.stored_checksum(),
      stable_blob_cache_.get()->memory_allocator(), &contents);
  if (!status.ok()) {
    return status;
  }

  StableTypedHandle* cache_handle = nullptr;
  status = stable_blob_cache_.InsertFull(
      cache_key, contents.get(), contents->ApproximateMemoryUsage(),
      &cache_handle, Cache::Priority::BOTTOM, lowest_used_cache_tier_);
  if (!status.ok()) {
    RecordTick(statistics_, BLOB_DB_CACHE_ADD_FAILURES);
    return status;
  }

  [[maybe_unused]] StableBlobContents* const released = contents.release();
  assert(cache_handle != nullptr);
  *cached_blob = CacheHandleGuard<StableBlobContents>(stable_blob_cache_.get(),
                                                      cache_handle);
  RecordTick(statistics_, BLOB_DB_CACHE_ADD);
  RecordTick(statistics_, BLOB_DB_CACHE_BYTES_WRITE,
             cached_blob->GetValue()->size());
  return Status::OK();
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

void BlobSource::PinStableCachedBlob(
    CacheHandleGuard<StableBlobContents>* cached_blob, uint64_t range_offset,
    size_t range_length, PinnableSlice* value) {
  assert(cached_blob != nullptr);
  assert(cached_blob->GetValue() != nullptr);
  assert(value != nullptr);

  const Slice full = cached_blob->GetValue()->data();
  Slice selected = full;
  if (range_length != kWholeBlobLength) {
    assert(range_offset <= full.size());
    assert(range_length <= full.size() - range_offset);
    selected =
        Slice(full.data() + static_cast<size_t>(range_offset), range_length);
  }
  value->Reset();
  value->PinSlice(selected, static_cast<Cleanable*>(nullptr));
  cached_blob->TransferTo(value);
}

Status BlobSource::FinishIndirectBlobRead(
    const ReadOptions& read_options, const BlobIndex& blob_index,
    const Slice& stable_cache_key, bool materialized_whole,
    uint64_t range_offset, size_t range_length, PinnableSlice* physical_value,
    PinnableSlice* result) const {
  assert(blob_index.IsIndirect());
  assert(physical_value != nullptr);
  assert(result != nullptr);
  if (!materialized_whole) {
    result->PinSlice(*physical_value, physical_value);
    return Status::OK();
  }
  if ((read_options.verify_checksums ||
       (stable_blob_cache_ && read_options.fill_cache)) &&
      crc32c::Value(physical_value->data(), physical_value->size()) !=
          blob_index.stable_checksum()) {
    return Status::Corruption("Stable blob checksum mismatch");
  }

  const size_t logical_size = physical_value->size();
  if (range_length != kWholeBlobLength &&
      (range_offset > logical_size ||
       range_length > logical_size - static_cast<size_t>(range_offset))) {
    return Status::InvalidArgument("Stable blob range is out of bounds");
  }

  if (stable_blob_cache_ && read_options.fill_cache) {
    CacheHandleGuard<StableBlobContents> cached_blob;
    Status status = PutStableBlobIntoCache(stable_cache_key, blob_index,
                                           *physical_value, &cached_blob);
    if (!status.ok()) {
      return status;
    }
    PinStableCachedBlob(&cached_blob, range_offset, range_length, result);
  } else if (range_length == kWholeBlobLength) {
    result->PinSlice(*physical_value, physical_value);
  } else {
    const Slice selected(
        physical_value->data() + static_cast<size_t>(range_offset),
        range_length);
    result->PinSlice(selected, physical_value);
  }
  return Status::OK();
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

Status BlobSource::GetIndirectBlob(
    const ReadOptions& read_options, const Slice& user_key,
    const BlobIndex& blob_index, const BlobFileOpenInfo& physical_file,
    uint64_t physical_file_size, bool is_relocation_file,
    uint32_t relocation_directory_digest,
    CompressionManager* relocation_compression_manager,
    uint8_t block_protection_bytes_per_key, uint64_t range_offset,
    size_t range_length, FilePrefetchBuffer* prefetch_buffer,
    PinnableSlice* value, uint64_t* bytes_read) {
  assert(value != nullptr);
  value->Reset();
  if (bytes_read != nullptr) {
    *bytes_read = 0;
  }
  Status status =
      ValidateIndirectBlobIndexForRead(blob_index, is_relocation_file);
  if (!status.ok()) {
    return status;
  }
  if (range_length != kWholeBlobLength &&
      blob_index.compression() == kNoCompression &&
      (range_offset > blob_index.size() ||
       static_cast<uint64_t>(range_length) >
           blob_index.size() - range_offset)) {
    return Status::InvalidArgument("Stable blob range is out of bounds");
  }
  if (physical_file.file_number == 0 || physical_file_size == 0) {
    return Status::Corruption("Invalid indirect blob physical file");
  }

  const CacheKey stable_key = GetStableBlobCacheKey(
      db_id_, db_session_id_, blob_index.origin_file_number(),
      blob_index.original_value_offset());
  CacheHandleGuard<StableBlobContents> cached_blob;
  if (stable_blob_cache_) {
    status =
        GetStableBlobFromCache(stable_key.AsSlice(), blob_index, &cached_blob,
                               /*record_statistics=*/true);
    if (status.ok()) {
      const size_t logical_size = cached_blob.GetValue()->size();
      if (range_length != kWholeBlobLength &&
          (range_offset > logical_size ||
           range_length > logical_size - static_cast<size_t>(range_offset))) {
        return Status::InvalidArgument("Stable blob range is out of bounds");
      }
      PinStableCachedBlob(&cached_blob, range_offset, range_length, value);
      return Status::OK();
    }
    if (!status.IsNotFound()) {
      return status;
    }
  }
  if (read_options.read_tier == kBlockCacheTier) {
    return Status::Incomplete("Cannot read indirect blob: no disk I/O allowed");
  }

  const bool materialize_whole = range_length == kWholeBlobLength ||
                                 blob_index.compression() != kNoCompression;
  PinnableSlice physical_value;
  uint64_t physical_bytes_read = 0;
  if (is_relocation_file) {
    CacheHandleGuard<BlobFileReader> reader;
    status = blob_file_cache_->GetRelocationFileReader(
        read_options, physical_file, physical_file_size,
        relocation_directory_digest, db_session_id_,
        relocation_compression_manager, block_protection_bytes_per_key,
        &reader);
    if (!status.ok()) {
      return status;
    }
    status = reader.GetValue()->GetBlobFromRelocationFile(
        read_options, blob_index, materialize_whole ? 0 : range_offset,
        materialize_whole ? kWholeBlobLength : range_length, &physical_value,
        &physical_bytes_read);
  } else {
    std::unique_ptr<BlobContents> contents;
    status = ReadBlobWithReaderRetry(
        blob_file_cache_, read_options, physical_file,
        [&](BlobFileReader* reader) {
          if (reader->GetCompressionType() != blob_index.compression()) {
            return Status::Corruption(
                "Compression type mismatch when reading indirect blob");
          }
          contents.reset();
          physical_bytes_read = 0;
          if (materialize_whole) {
            return reader->GetBlob(
                read_options, user_key, blob_index.original_value_offset(),
                blob_index.size(), blob_index.compression(), prefetch_buffer,
                /*allocator=*/nullptr, &contents, &physical_bytes_read,
                &blob_index);
          }
          return reader->GetBlobRange(
              read_options, user_key, blob_index.original_value_offset(),
              blob_index.size(), range_offset, range_length,
              /*allocator=*/nullptr, &contents, &physical_bytes_read);
        });
    if (status.ok()) {
      PinOwnedBlob(&contents, &physical_value);
    }
  }
  if (!status.ok()) {
    return status;
  }
  if (bytes_read != nullptr) {
    *bytes_read = physical_bytes_read;
  }
  if (is_relocation_file) {
    RecordTick(statistics_, BLOB_DB_BLOB_FILE_BYTES_READ, physical_bytes_read);
    PERF_COUNTER_ADD(blob_read_count, 1);
    PERF_COUNTER_ADD(blob_read_byte, physical_bytes_read);
  }
  // Both normal BlobFileReader and the standalone relocation table perform
  // the physical read without BlobSource's lazy attribution. Stable cache
  // hits returned above and therefore never enter these counters.
  if (read_options.io_activity == Env::IOActivity::kLazyResolve) {
    RecordLazyRead(statistics_, physical_bytes_read);
    if (!materialize_whole) {
      RecordLazyPartialRead(statistics_, physical_bytes_read,
                            blob_index.size());
    }
  }

  return FinishIndirectBlobRead(read_options, blob_index, stable_key.AsSlice(),
                                materialize_whole, range_offset, range_length,
                                &physical_value, value);
}

void BlobSource::MultiGetIndirectBlobFromOneFile(
    const ReadOptions& read_options, const BlobFileOpenInfo& physical_file,
    uint64_t physical_file_size, bool is_relocation_file,
    uint32_t relocation_directory_digest,
    CompressionManager* relocation_compression_manager,
    uint8_t block_protection_bytes_per_key, size_t num_requests,
    IndirectBlobReadRequest* requests, uint64_t* bytes_read) {
  if (bytes_read != nullptr) {
    *bytes_read = 0;
  }
  if (num_requests == 0) {
    return;
  }
  assert(requests != nullptr);
  if (physical_file.file_number == 0 || physical_file_size == 0) {
    for (size_t i = 0; i < num_requests; ++i) {
      assert(requests[i].status != nullptr);
      *requests[i].status =
          Status::Corruption("Invalid indirect blob physical file");
    }
    return;
  }
  if (num_requests > MultiGetContext::MAX_BATCH_SIZE) {
    uint64_t total_bytes_read = 0;
    for (size_t begin = 0; begin < num_requests;
         begin += MultiGetContext::MAX_BATCH_SIZE) {
      const size_t count =
          std::min(num_requests - begin,
                   static_cast<size_t>(MultiGetContext::MAX_BATCH_SIZE));
      uint64_t chunk_bytes_read = 0;
      MultiGetIndirectBlobFromOneFile(
          read_options, physical_file, physical_file_size, is_relocation_file,
          relocation_directory_digest, relocation_compression_manager,
          block_protection_bytes_per_key, count, requests + begin,
          &chunk_bytes_read);
      if (chunk_bytes_read >
          std::numeric_limits<uint64_t>::max() - total_bytes_read) {
        for (size_t i = begin; i < num_requests; ++i) {
          assert(requests[i].status != nullptr);
          *requests[i].status =
              Status::Corruption("Indirect blob read byte count overflow");
        }
        return;
      }
      total_bytes_read += chunk_bytes_read;
    }
    if (bytes_read != nullptr) {
      *bytes_read = total_bytes_read;
    }
    return;
  }

  std::vector<CacheKey> stable_keys(num_requests);
  std::vector<PinnableSlice> physical_values(num_requests);
  std::vector<uint8_t> materialized_whole(num_requests, 0);
  std::vector<size_t> pending;
  pending.reserve(num_requests);
  for (size_t i = 0; i < num_requests; ++i) {
    IndirectBlobReadRequest& request = requests[i];
    assert(request.blob_index != nullptr);
    assert(request.result != nullptr);
    assert(request.status != nullptr);
    request.result->Reset();
    *request.status = ValidateIndirectBlobIndexForRead(*request.blob_index,
                                                       is_relocation_file);
    if (!request.status->ok()) {
      continue;
    }
    stable_keys[i] = GetStableBlobCacheKey(
        db_id_, db_session_id_, request.blob_index->origin_file_number(),
        request.blob_index->original_value_offset());
    if (stable_blob_cache_) {
      CacheHandleGuard<StableBlobContents> cached_blob;
      *request.status = GetStableBlobFromCache(
          stable_keys[i].AsSlice(), *request.blob_index, &cached_blob,
          /*record_statistics=*/true);
      if (request.status->ok()) {
        const size_t logical_size = cached_blob.GetValue()->size();
        if (request.range_length != kWholeBlobLength &&
            (request.range_offset > logical_size ||
             request.range_length >
                 logical_size - static_cast<size_t>(request.range_offset))) {
          *request.status =
              Status::InvalidArgument("Stable blob range is out of bounds");
          continue;
        }
        PinStableCachedBlob(&cached_blob, request.range_offset,
                            request.range_length, request.result);
        continue;
      }
      if (!request.status->IsNotFound()) {
        continue;
      }
    }
    *request.status = Status::OK();
    materialized_whole[i] =
        request.range_length == kWholeBlobLength ||
                request.blob_index->compression() != kNoCompression
            ? 1
            : 0;
    if (materialized_whole[i] == 0 &&
        (request.range_offset > request.blob_index->size() ||
         static_cast<uint64_t>(request.range_length) >
             request.blob_index->size() - request.range_offset)) {
      *request.status =
          Status::InvalidArgument("Stable blob range is out of bounds");
      continue;
    }
    pending.push_back(i);
  }
  if (pending.empty()) {
    return;
  }
  if (read_options.read_tier == kBlockCacheTier) {
    for (size_t index : pending) {
      *requests[index].status =
          Status::Incomplete("Cannot read indirect blob: no disk I/O allowed");
    }
    return;
  }

  uint64_t physical_bytes_read = 0;
  if (is_relocation_file) {
    CacheHandleGuard<BlobFileReader> reader;
    Status open_status = blob_file_cache_->GetRelocationFileReader(
        read_options, physical_file, physical_file_size,
        relocation_directory_digest, db_session_id_,
        relocation_compression_manager, block_protection_bytes_per_key,
        &reader);
    if (!open_status.ok()) {
      for (size_t index : pending) {
        *requests[index].status = open_status;
      }
      return;
    }
    std::vector<RelocationBlobReadRequest> relocation_requests;
    relocation_requests.reserve(pending.size());
    for (size_t index : pending) {
      IndirectBlobReadRequest& request = requests[index];
      relocation_requests.push_back(RelocationBlobReadRequest{
          request.blob_index,
          materialized_whole[index] != 0 ? 0 : request.range_offset,
          materialized_whole[index] != 0 ? kWholeBlobLength
                                         : request.range_length,
          &physical_values[index], request.status});
    }
    reader.GetValue()->MultiGetBlobFromRelocationFile(
        read_options, relocation_requests.size(), relocation_requests.data(),
        &physical_bytes_read);

    size_t successful_reads = 0;
    for (size_t index : pending) {
      if (requests[index].status->ok()) {
        ++successful_reads;
      }
    }
    RecordTick(statistics_, BLOB_DB_BLOB_FILE_BYTES_READ, physical_bytes_read);
    PERF_COUNTER_ADD(blob_read_count, successful_reads);
    PERF_COUNTER_ADD(blob_read_byte, physical_bytes_read);
  } else {
    CacheHandleGuard<BlobFileReader> reader;
    Status open_status = blob_file_cache_->GetBlobFileReader(
        read_options, physical_file, &reader);
    if (!open_status.ok()) {
      for (size_t index : pending) {
        *requests[index].status = open_status;
      }
      return;
    }

    std::vector<size_t> whole_indices;
    std::vector<size_t> range_indices;
    whole_indices.reserve(pending.size());
    range_indices.reserve(pending.size());
    for (size_t index : pending) {
      const BlobIndex& blob_index = *requests[index].blob_index;
      if (reader.GetValue()->GetCompressionType() != blob_index.compression()) {
        *requests[index].status = Status::Corruption(
            "Compression type mismatch when reading indirect blob");
      } else if (blob_index.size() > std::numeric_limits<size_t>::max()) {
        *requests[index].status =
            Status::Corruption("Indirect blob size is too large");
      } else if (materialized_whole[index] != 0) {
        whole_indices.push_back(index);
      } else {
        range_indices.push_back(index);
      }
    }
    std::sort(
        whole_indices.begin(), whole_indices.end(),
        [requests, &read_options](size_t left, size_t right) {
          const IndirectBlobReadRequest& lhs = requests[left];
          const IndirectBlobReadRequest& rhs = requests[right];
          const uint64_t left_adjustment =
              read_options.verify_checksums
                  ? BlobLogRecord::CalculateAdjustmentForRecordHeader(
                        lhs.user_key.size())
                  : 0;
          const uint64_t right_adjustment =
              read_options.verify_checksums
                  ? BlobLogRecord::CalculateAdjustmentForRecordHeader(
                        rhs.user_key.size())
                  : 0;
          const uint64_t left_offset =
              lhs.blob_index->original_value_offset() >= left_adjustment
                  ? lhs.blob_index->original_value_offset() - left_adjustment
                  : 0;
          const uint64_t right_offset =
              rhs.blob_index->original_value_offset() >= right_adjustment
                  ? rhs.blob_index->original_value_offset() - right_adjustment
                  : 0;
          return left_offset < right_offset;
        });
    std::sort(
        range_indices.begin(), range_indices.end(),
        [requests](size_t left, size_t right) {
          const IndirectBlobReadRequest& lhs = requests[left];
          const IndirectBlobReadRequest& rhs = requests[right];
          const uint64_t left_offset =
              lhs.range_offset > std::numeric_limits<uint64_t>::max() -
                                     lhs.blob_index->original_value_offset()
                  ? std::numeric_limits<uint64_t>::max()
                  : lhs.blob_index->original_value_offset() + lhs.range_offset;
          const uint64_t right_offset =
              rhs.range_offset > std::numeric_limits<uint64_t>::max() -
                                     rhs.blob_index->original_value_offset()
                  ? std::numeric_limits<uint64_t>::max()
                  : rhs.blob_index->original_value_offset() + rhs.range_offset;
          return left_offset < right_offset;
        });

    std::vector<BlobReadRequest> whole_requests;
    whole_requests.reserve(whole_indices.size());
    for (size_t index : whole_indices) {
      IndirectBlobReadRequest& request = requests[index];
      whole_requests.emplace_back(
          request.user_key, request.blob_index->original_value_offset(),
          static_cast<size_t>(request.blob_index->size()),
          request.blob_index->compression(), /*result=*/nullptr,
          request.status);
      whole_requests.back().stable_blob_index = request.blob_index;
    }
    std::vector<size_t> whole_positions;
    whole_positions.reserve(whole_requests.size());
    for (size_t i = 0; i < whole_requests.size(); ++i) {
      whole_positions.push_back(i);
    }
    const std::vector<std::vector<size_t>> whole_batches =
        PartitionNonOverlappingReadBatches(
            whole_positions,
            [&whole_requests, &read_options](size_t position) {
              const BlobReadRequest& request = whole_requests[position];
              const uint64_t adjustment =
                  read_options.verify_checksums
                      ? BlobLogRecord::CalculateAdjustmentForRecordHeader(
                            request.user_key->size())
                      : 0;
              return request.offset >= adjustment ? request.offset - adjustment
                                                  : 0;
            },
            [&whole_requests, &read_options](size_t position) {
              const BlobReadRequest& request = whole_requests[position];
              const uint64_t adjustment =
                  read_options.verify_checksums
                      ? BlobLogRecord::CalculateAdjustmentForRecordHeader(
                            request.user_key->size())
                      : 0;
              return static_cast<uint64_t>(request.len) >
                             std::numeric_limits<uint64_t>::max() - adjustment
                         ? std::numeric_limits<uint64_t>::max()
                         : static_cast<uint64_t>(request.len) + adjustment;
            });
    uint64_t whole_bytes_read = 0;
    bool read_byte_count_overflow = false;
    for (const std::vector<size_t>& batch : whole_batches) {
      autovector<std::pair<BlobReadRequest*, std::unique_ptr<BlobContents>>>
          whole_reads;
      for (size_t position : batch) {
        whole_reads.emplace_back(&whole_requests[position],
                                 std::unique_ptr<BlobContents>());
      }
      uint64_t batch_bytes_read = 0;
      reader.GetValue()->MultiGetBlob(read_options, /*allocator=*/nullptr,
                                      whole_reads, &batch_bytes_read);
      if (batch_bytes_read >
          std::numeric_limits<uint64_t>::max() - whole_bytes_read) {
        read_byte_count_overflow = true;
      } else {
        whole_bytes_read += batch_bytes_read;
      }
      for (size_t i = 0; i < batch.size(); ++i) {
        const size_t position = batch[i];
        if (whole_requests[position].status->ok()) {
          PinOwnedBlob(&whole_reads[i].second,
                       &physical_values[whole_indices[position]]);
        }
      }
    }

    std::vector<BlobRangeReadRequest> range_requests;
    range_requests.reserve(range_indices.size());
    for (size_t index : range_indices) {
      IndirectBlobReadRequest& request = requests[index];
      range_requests.emplace_back(
          request.user_key, request.blob_index->original_value_offset(),
          request.blob_index->size(), request.range_offset,
          request.range_length, /*result=*/nullptr, request.status);
    }
    std::vector<size_t> range_positions;
    range_positions.reserve(range_requests.size());
    for (size_t i = 0; i < range_requests.size(); ++i) {
      range_positions.push_back(i);
    }
    const std::vector<std::vector<size_t>> range_batches =
        PartitionNonOverlappingReadBatches(
            range_positions,
            [&range_requests](size_t position) {
              const BlobRangeReadRequest& request = range_requests[position];
              return request.range_offset >
                             std::numeric_limits<uint64_t>::max() -
                                 request.offset
                         ? std::numeric_limits<uint64_t>::max()
                         : request.offset + request.range_offset;
            },
            [&range_requests](size_t position) {
              return static_cast<uint64_t>(
                  range_requests[position].range_length);
            });
    uint64_t range_bytes_read = 0;
    for (const std::vector<size_t>& batch : range_batches) {
      autovector<
          std::pair<BlobRangeReadRequest*, std::unique_ptr<BlobContents>>>
          range_reads;
      for (size_t position : batch) {
        range_reads.emplace_back(&range_requests[position],
                                 std::unique_ptr<BlobContents>());
      }
      uint64_t batch_bytes_read = 0;
      reader.GetValue()->MultiGetBlobRange(read_options, range_reads,
                                           &batch_bytes_read);
      if (batch_bytes_read >
          std::numeric_limits<uint64_t>::max() - range_bytes_read) {
        read_byte_count_overflow = true;
      } else {
        range_bytes_read += batch_bytes_read;
      }
      for (size_t i = 0; i < batch.size(); ++i) {
        const size_t position = batch[i];
        if (range_requests[position].status->ok()) {
          PinOwnedBlob(&range_reads[i].second,
                       &physical_values[range_indices[position]]);
        }
      }
    }
    if (read_byte_count_overflow ||
        range_bytes_read >
            std::numeric_limits<uint64_t>::max() - whole_bytes_read) {
      for (size_t index : pending) {
        *requests[index].status =
            Status::Corruption("Indirect blob read byte count overflow");
      }
      return;
    }
    physical_bytes_read = whole_bytes_read + range_bytes_read;
  }

  for (size_t index : pending) {
    IndirectBlobReadRequest& request = requests[index];
    if (!request.status->ok()) {
      continue;
    }
    if (read_options.io_activity == Env::IOActivity::kLazyResolve) {
      const bool whole = materialized_whole[index] != 0;
      const uint64_t adjustment =
          is_relocation_file
              ? kSimpleGen2BlobTrailerSize
              : (read_options.verify_checksums
                     ? BlobLogRecord::CalculateAdjustmentForRecordHeader(
                           request.user_key.size())
                     : 0);
      const uint64_t read_size = whole ? request.blob_index->size() + adjustment
                                       : request.range_length;
      RecordLazyRead(statistics_, read_size);
      if (!whole) {
        RecordLazyPartialRead(statistics_, read_size,
                              request.blob_index->size());
      }
    }
    *request.status = FinishIndirectBlobRead(
        read_options, *request.blob_index, stable_keys[index].AsSlice(),
        materialized_whole[index] != 0, request.range_offset,
        request.range_length, &physical_values[index], request.result);
  }
  if (bytes_read != nullptr) {
    *bytes_read = physical_bytes_read;
  }
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

  std::unique_ptr<BlobContents> blob_contents;
  if (expected_compression == kNoCompression) {
    blob_contents.reset(
        new BlobContents(std::move(buf), static_cast<size_t>(payload_size)));
  } else {
    s = UncompressBlobContentsIfNeeded(
        Slice(buf.get(), static_cast<size_t>(payload_size)),
        expected_compression, simple_gen2_decompressor_.get(), allocator,
        clock_, statistics_, &blob_contents);
    if (!s.ok()) {
      return s;
    }
  }

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
    ChecksumType checksum_type, uint32_t base_context_checksum,
    CompressionType expected_compression, uint64_t range_offset,
    size_t range_length, PinnableSlice* value, uint64_t* bytes_read) {
  assert(value);
  assert(file);
  // Range reads are (currently) only issued while resolving a lazy result, so
  // the lazy read stats below are recorded unconditionally (unlike
  // GetSimpleGen2Blob, which gates on the activity). Enforce that invariant
  // here.
  assert(read_options.io_activity == Env::IOActivity::kLazyResolve);

  if (expected_compression != kNoCompression) {
    PinnableSlice whole_value;
    Status whole_status =
        GetSimpleGen2Blob(read_options, base_cache_key, file, record_offset,
                          payload_size, checksum_type, base_context_checksum,
                          expected_compression, &whole_value, bytes_read);
    if (!whole_status.ok()) {
      return whole_status;
    }
    if (range_offset > whole_value.size() ||
        range_length > whole_value.size() - range_offset) {
      return Status::InvalidArgument("Compressed blob range is out of bounds");
    }
    const Slice subrange(whole_value.data() + static_cast<size_t>(range_offset),
                         range_length);
    value->Reset();
    value->PinSlice(subrange, &whole_value);
    return Status::OK();
  }

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

    // Indirect callers already probed their stable-ID cache. Do not populate
    // a second physical-key entry, which would lose the stored-byte fingerprint
    // and duplicate cached values after each relocation.
    if (blob_cache_ && req.stable_blob_index == nullptr) {
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
    if (read_options.verify_checksums && req.stable_blob_index != nullptr) {
      *req.status = req.stable_blob_index->VerifyStoredChecksum(
          Slice(pending[k].buf.get(), static_cast<size_t>(req.payload_size)));
      if (!req.status->ok()) {
        continue;
      }
    }
    std::unique_ptr<BlobContents> blob_contents;
    if (req.expected_compression == kNoCompression) {
      blob_contents.reset(new BlobContents(
          std::move(pending[k].buf), static_cast<size_t>(req.payload_size)));
    } else {
      *req.status = UncompressBlobContentsIfNeeded(
          Slice(pending[k].buf.get(), static_cast<size_t>(req.payload_size)),
          req.expected_compression, simple_gen2_decompressor_.get(), allocator,
          clock_, statistics_, &blob_contents);
      if (!req.status->ok()) {
        continue;
      }
    }

    RecordTick(statistics_, BLOB_DB_BLOB_FILE_BYTES_READ, record_size);
    PERF_COUNTER_ADD(blob_read_count, 1);
    PERF_COUNTER_ADD(blob_read_byte, record_size);
    if (read_options.io_activity == Env::IOActivity::kLazyResolve) {
      RecordLazyRead(statistics_, record_size);
    }

    if (blob_cache_ && read_options.fill_cache &&
        req.stable_blob_index == nullptr) {
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
    RandomAccessFileReader* file, ChecksumType checksum_type,
    uint32_t base_context_checksum, size_t num_records,
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

    if (req.expected_compression != kNoCompression) {
      *req.status = GetSimpleGen2BlobRange(
          read_options, base_cache_key, file, req.record_offset,
          req.payload_size, checksum_type, base_context_checksum,
          req.expected_compression, req.range_offset, req.range_length,
          req.result, /*bytes_read=*/nullptr);
      continue;
    }

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
      s = blob_file_cache_->OpenBlobFileReaderUncached(
          read_options, blob_file, &fresh_reader,
          /*allow_footer_skip_retry=*/false);
      if (!s.ok()) {
        for (const auto& blob_req : _blob_reqs) {
          BlobReadRequest* const req = blob_req.first;
          assert(req != nullptr);
          assert(req->status != nullptr);
          if (req->status->IsCorruption()) {
            *req->status = AppendBlobRefreshRetryFailure(*req->status, s);
          }
        }
        return;
      }

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
