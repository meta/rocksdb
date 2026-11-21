//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_source.h"

#include <array>
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
#include "db/blob/same_file_blob_reader.h"
#include "db/version_edit.h"
#include "file/random_access_file_reader.h"
#include "logging/logging.h"
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

// Start and length of the file read for a whole value at `value_offset` in a
// blob log file. With checksum verification the read also covers the record
// header in front of the value.
std::pair<uint64_t, uint64_t> WholeBlobReadExtent(
    const ReadOptions& read_options, size_t user_key_size,
    uint64_t value_offset, uint64_t value_size) {
  const uint64_t adjustment =
      read_options.verify_checksums
          ? BlobLogRecord::CalculateAdjustmentForRecordHeader(user_key_size)
          : 0;
  return {value_offset >= adjustment ? value_offset - adjustment : 0,
          value_size > std::numeric_limits<uint64_t>::max() - adjustment
              ? std::numeric_limits<uint64_t>::max()
              : value_size + adjustment};
}

constexpr size_t kMaxIndirectBatch = MultiGetContext::MAX_BATCH_SIZE;

// One planned file read: its (start, length) and the index of its request.
struct PlannedRead {
  uint64_t start = 0;
  uint64_t length = 0;
  size_t request = 0;
  // A copy shares the source's record check, which includes the user key, so
  // only reads for the same user key are duplicates.
  Slice user_key;
};
using PlannedReads = autovector<PlannedRead, kMaxIndirectBatch>;
using ReadBatch = autovector<size_t, kMaxIndirectBatch>;
using ReadBatches = autovector<ReadBatch, 2>;
// (request, request whose result it copies) for exact duplicate reads.
using DuplicateReads = autovector<std::pair<size_t, size_t>, kMaxIndirectBatch>;

// Sorts `reads` by (start, length, user key) and splits them into the fewest
// batches whose reads do not overlap, because FileSystem::MultiRead requires
// non-overlapping requests. When no reads overlap (the usual case) there is one
// batch. The sort puts equal reads next to each other, so an exact duplicate of
// the previous read (same start, length and user key) is not read again; it
// goes to `duplicates`. Batches hold request indexes.
void PlanReadBatches(PlannedReads* reads, ReadBatches* batches,
                     DuplicateReads* duplicates) {
  std::sort(reads->begin(), reads->end(),
            [](const PlannedRead& lhs, const PlannedRead& rhs) {
              if (lhs.start != rhs.start || lhs.length != rhs.length) {
                return std::make_pair(lhs.start, lhs.length) <
                       std::make_pair(rhs.start, rhs.length);
              }
              return lhs.user_key.compare(rhs.user_key) < 0;
            });
  autovector<uint64_t, 2> batch_ends;
  const PlannedRead* previous = nullptr;
  for (const PlannedRead& read : *reads) {
    if (previous != nullptr && previous->start == read.start &&
        previous->length == read.length &&
        previous->user_key == read.user_key) {
      duplicates->emplace_back(read.request, previous->request);
      continue;
    }
    previous = &read;
    const uint64_t end =
        read.length > std::numeric_limits<uint64_t>::max() - read.start
            ? std::numeric_limits<uint64_t>::max()
            : read.start + read.length;
    size_t batch = batch_ends.size();
    for (size_t i = 0; i < batch_ends.size(); ++i) {
      if (batch_ends[i] <= read.start) {
        batch = i;
        break;
      }
    }
    if (batch == batch_ends.size()) {
      batches->emplace_back();
      batch_ends.push_back(end);
    } else {
      batch_ends[batch] = end;
    }
    (*batches)[batch].push_back(read.request);
  }
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
//
// `check(reader)` runs first on the cached reader. A metadata error it returns
// (for example a compression mismatch) is returned as is, without the evict and
// retry.
template <typename CheckFn, typename ReadFn>
Status ReadBlobWithReaderRetry(BlobFileCache* blob_file_cache,
                               const ReadOptions& read_options,
                               const BlobFileOpenInfo& blob_file,
                               const CheckFn& check, const ReadFn& read) {
  CacheHandleGuard<BlobFileReader> blob_file_reader;
  Status s = blob_file_cache->GetBlobFileReader(read_options, blob_file,
                                                &blob_file_reader);
  if (!s.ok()) {
    return s;
  }
  assert(blob_file_reader.GetValue());

  s = check(blob_file_reader.GetValue());
  if (!s.ok()) {
    return s;
  }
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

template <typename ReadFn>
Status ReadBlobWithReaderRetry(BlobFileCache* blob_file_cache,
                               const ReadOptions& read_options,
                               const BlobFileOpenInfo& blob_file,
                               const ReadFn& read) {
  return ReadBlobWithReaderRetry(
      blob_file_cache, read_options, blob_file,
      [](BlobFileReader*) { return Status::OK(); }, read);
}

// Reads `requests` in the batches planned from `reads` (see PlanReadBatches)
// with read_batch(batch_reads, &bytes_read). Each value goes to
// *value_of(request); an exact duplicate gets its own copy, allocated from
// `allocator`, and is reported to on_duplicate(request) (it was not read from
// storage). Returns the bytes read.
template <typename Request, typename ReadBatchFn, typename ValueFn,
          typename DuplicateFn>
uint64_t ReadInPlannedBatches(autovector<Request, kMaxIndirectBatch>* requests,
                              PlannedReads* reads, MemoryAllocator* allocator,
                              const ReadBatchFn& read_batch,
                              const ValueFn& value_of,
                              const DuplicateFn& on_duplicate) {
  ReadBatches batches;
  DuplicateReads duplicates;
  PlanReadBatches(reads, &batches, &duplicates);
  uint64_t total_bytes_read = 0;
  for (const ReadBatch& batch : batches) {
    autovector<std::pair<Request*, std::unique_ptr<BlobContents>>> batch_reads;
    for (size_t request : batch) {
      batch_reads.emplace_back(&(*requests)[request],
                               std::unique_ptr<BlobContents>());
    }
    uint64_t batch_bytes_read = 0;
    read_batch(batch_reads, &batch_bytes_read);
    total_bytes_read += batch_bytes_read;
    for (size_t i = 0; i < batch.size(); ++i) {
      if ((*requests)[batch[i]].status->ok()) {
        *value_of(batch[i]) = std::move(batch_reads[i].second);
      }
    }
  }
  for (const auto& [duplicate, source] : duplicates) {
    on_duplicate(duplicate);
    *(*requests)[duplicate].status = *(*requests)[source].status;
    if ((*requests)[source].status->ok()) {
      const Slice value = (*value_of(source))->data();
      *value_of(duplicate) = std::make_unique<BlobContents>(
          AllocateAndCopyBlock(value, allocator), value.size());
    }
  }
  return total_bytes_read;
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

namespace {
// Returns the domain (1, 2 or 3) of the stable cache keys of this DB and
// session. A stable key must never equal a physical blob cache key (the key of
// a physical file and offset) of the same DB and session.
//
// Why: stable keys live in the blob cache next to physical blob keys and
// embedded blob keys. When the blob cache is also the block cache, they also
// live next to SST block keys and cache reservation keys. A collision would
// return the bytes of another value. SST block keys and embedded blob keys are
// built the same way as physical blob keys: OffsetableCacheKey with a real file
// number. For keys of this DB and session, this proof applies to all of them
// ("physical key" below). Keys of another session differ as for any other cache
// key (see the end of this comment). For the key layout, see OffsetableCacheKey
// in cache/cache_key.h.
//
// Stable keys and physical blob keys are both built by OffsetableCacheKey from
// an internal id `id`, where id[0] = session_lower and id[1] = H ^ file
// number. H depends only on db_id and the session.
//
// Terms:
// - First word: the first 64 bits of the cache key. OffsetableCacheKey sets it
//   to DownwardInvolution(id[0]) ^ ReverseBits(id[1]). The offset changes only
//   the second word.
// - P: the low two bits of a physical key's first word. ReverseBits moves the
//   top two bits of id[1] to the low two bits. File numbers are at most
//   kFileNumberMask (62 bits), so they never set those top two bits, and P is
//   the same for every file of this DB and session.
// - Domain (1, 2 or 3): a value that the stable key xors into the top two bits
//   of id[1], like a file number that no real file can have. The low two bits
//   of a stable first word are then P ^ domain.
//
// OffsetableCacheKey swaps a zero first word with the second word,
// ReverseBits(session_lower). So when P is 0, a physical first word can also
// have the low bits of that swapped word. One fixed domain is therefore not
// enough. The loop picks the first domain whose stable low bits are not 0 and,
// when P is 0, also differ from the low bits of the swapped word. At most one
// of the three domains is rejected: the domain equal to P when P != 0, or the
// domain that gives the swapped word's low bits when P == 0. The choice depends
// only on db_id and the session, not on the origin. A stable first word is
// never 0, so it is never swapped, and distinct origins give distinct first
// words. Reserved keys (CacheKey::CreateUnique*) have a first word of 0, so a
// stable key never equals one either. Keys of different sessions differ as for
// any other cache key.
uint64_t ChooseStableKeyDomain(const UniqueId64x2& id) {
  const uint64_t physical_low_bits =
      (DownwardInvolution(id[0]) ^ ReverseBits(id[1])) & 3;
  const uint64_t swapped_low_bits = ReverseBits(id[0]) & 3;
  for (uint64_t domain = 1; domain <= 3; ++domain) {
    const uint64_t stable_low_bits = physical_low_bits ^ domain;
    if (stable_low_bits != 0 &&
        (physical_low_bits != 0 || stable_low_bits != swapped_low_bits)) {
      return domain;
    }
  }
  assert(false);
  return 1;
}
}  // namespace

CacheKey GetStableBlobCacheKey(const std::string& db_id,
                               const std::string& db_session_id,
                               uint64_t origin_file_number,
                               uint64_t original_value_offset) {
  static_assert(kFileNumberMask == 0x3fffffffffffffffULL);
  static_assert(BlobIndex::kMaxFileNumber == kFileNumberMask);
  assert(origin_file_number != 0 && origin_file_number <= kFileNumberMask);
  OffsetableCacheKey physical_base(db_id, db_session_id, origin_file_number);
  UniqueId64x2 id = physical_base.ToInternalUniqueId();
  id[1] ^= ReverseBits(ChooseStableKeyDomain(id));
  return OffsetableCacheKey::FromInternalUniqueId(&id).WithOffset(
      original_value_offset);
}

// Applies the configured blob-cache memory charging policy, so reads and flush
// prepopulation are accounted against the block cache when charging is enabled.
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

}  // namespace

CacheKey TEST_GetStableBlobCacheKey(const std::string& db_id,
                                    const std::string& db_session_id,
                                    uint64_t origin_file_number,
                                    uint64_t original_value_offset) {
  return GetStableBlobCacheKey(db_id, db_session_id, origin_file_number,
                               original_value_offset);
}

StableBlobCacheKeyBuilder::StableBlobCacheKeyBuilder(
    const std::string& db_id, const std::string& db_session_id) {
  UniqueId64x2 id;
  Status s = GetSstInternalUniqueId(db_id, db_session_id, /*file_number=*/0,
                                    &id, /*force=*/true);
  assert(s.ok());
  s.PermitUncheckedError();
  session_lower_ = id[0];
  db_hash_ = id[1];
  if (session_lower_ == 0) {
    slow_db_id_ = db_id;
    slow_db_session_id_ = db_session_id;
    use_slow_path_ = true;
    return;
  }
  // The domain depends only on db_id and the session: file numbers are at most
  // kFileNumberMask, so they do not change the top two bits of id[1].
  domain_bits_ = ReverseBits(ChooseStableKeyDomain(id));
}

CacheKey StableBlobCacheKeyBuilder::Get(uint64_t origin_file_number,
                                        uint64_t original_value_offset) const {
  assert(origin_file_number != 0 && origin_file_number <= kFileNumberMask);
  if (use_slow_path_) {
    return GetStableBlobCacheKey(slow_db_id_, slow_db_session_id_,
                                 origin_file_number, original_value_offset);
  }
  UniqueId64x2 id{session_lower_, db_hash_ ^ origin_file_number ^ domain_bits_};
  return OffsetableCacheKey::FromInternalUniqueId(&id).WithOffset(
      original_value_offset);
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
      stable_cache_admission_mutex_(kNumberOfAdmissionMutexStripes),
      blob_cache_storage_(
          GetBlobCacheWithReservation(immutable_options, mutable_cf_options)),
      blob_cache_(blob_cache_storage_),
      stable_blob_cache_(blob_cache_storage_),
      simple_gen2_decompressor_(
          GetBuiltinV2CompressionManager()->GetDecompressor()),
      lowest_used_cache_tier_(immutable_options.lowest_used_cache_tier) {}

CacheKey BlobSource::StableCacheKey(uint64_t origin_file_number,
                                    uint64_t original_value_offset) const {
  assert(stable_blob_cache_);
  std::call_once(stable_key_builder_once_, [this]() {
    stable_key_builder_ =
        std::make_unique<StableBlobCacheKeyBuilder>(db_id_, db_session_id_);
  });
  CacheKey key =
      stable_key_builder_->Get(origin_file_number, original_value_offset);
  // The builder's key must equal the slow path's key for the current db_id and
  // session id.
  assert(key.AsSlice() == GetStableBlobCacheKey(db_id_, db_session_id_,
                                                origin_file_number,
                                                original_value_offset)
                              .AsSlice());
  return key;
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

Status BlobSource::GetStableBlobFromCache(
    const Slice& cache_key, CacheHandleGuard<StableBlobContents>* cached_blob,
    bool record_statistics, bool primary_only) const {
  assert(stable_blob_cache_);
  assert(cached_blob != nullptr);
  assert(cached_blob->IsEmpty());

  StableTypedHandle* const cache_handle =
      primary_only
          ? stable_blob_cache_.Lookup(cache_key)
          : stable_blob_cache_.LookupFull(cache_key, nullptr /* context */,
                                          Cache::Priority::BOTTOM, statistics_,
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
    std::unique_ptr<BlobContents>* logical_value,
    CacheHandleGuard<StableBlobContents>* cached_blob) const {
  assert(stable_blob_cache_);
  assert(blob_index.IsIndirect());
  assert(logical_value != nullptr && *logical_value != nullptr);
  assert(cached_blob != nullptr);
  assert(cached_blob->IsEmpty());

  // FinishIndirectBlobRead has checked the logical CRC. The entry adopts the
  // read buffer, so there is no copy, and it is built outside the lock.
  const size_t value_size = (*logical_value)->size();
  std::unique_ptr<StableBlobContents> contents;
  Status status = StableBlobContents::CreateWithVerifiedAllocation(
      (*logical_value)->ReleaseAllocation(), value_size,
      blob_index.fingerprint(), &contents);
  logical_value->reset();
  if (!status.ok()) {
    return status;
  }

  // This lock is taken only on a miss, after a file read. Between the check
  // for an existing entry and the insert, it keeps a second read of the same
  // value from inserting another entry, so readers keep using the first one.
  // The first lookup already checked the secondary cache, so this lookup checks
  // only the primary cache.
  MutexLock admission_lock(&stable_cache_admission_mutex_.Get(cache_key));
  status = GetStableBlobFromCache(cache_key, cached_blob,
                                  /*record_statistics=*/false,
                                  /*primary_only=*/true);
  if (status.ok() || !status.IsNotFound()) {
    return status;
  }

  return InsertStableBlob(cache_key, &contents, cached_blob);
}

Status BlobSource::InsertStableBlob(
    const Slice& cache_key, std::unique_ptr<StableBlobContents>* contents,
    CacheHandleGuard<StableBlobContents>* cached_blob) const {
  assert(contents != nullptr && *contents != nullptr);
  const size_t value_size = (*contents)->size();
  StableTypedHandle* cache_handle = nullptr;
  const Status status = stable_blob_cache_.InsertFull(
      cache_key, contents->get(), (*contents)->ApproximateMemoryUsage(),
      &cache_handle, Cache::Priority::BOTTOM, lowest_used_cache_tier_);
  if (!status.ok()) {
    RecordTick(statistics_, BLOB_DB_CACHE_ADD_FAILURES);
    return status;
  }

  [[maybe_unused]] StableBlobContents* const released = contents->release();
  assert(cache_handle != nullptr);
  CacheHandleGuard<StableBlobContents> guard(stable_blob_cache_.get(),
                                             cache_handle);
  if (cached_blob != nullptr) {
    *cached_blob = std::move(guard);
  }
  RecordTick(statistics_, BLOB_DB_CACHE_ADD);
  RecordTick(statistics_, BLOB_DB_CACHE_BYTES_WRITE, value_size);
  return Status::OK();
}

Status BlobSource::PrepopulateFlushBlob(
    const Slice& value, uint64_t blob_file_number, uint64_t blob_offset,
    const StableBlobFingerprint* fingerprint) const {
  if (!blob_cache_) {
    return Status::OK();
  }
  if (fingerprint == nullptr) {
    const CacheKey cache_key =
        GetCacheKey(blob_file_number, /*file_size=*/0, blob_offset);
    const Status s = blob_cache_.InsertSaved(
        cache_key.AsSlice(), value, nullptr /*context*/,
        Cache::Priority::BOTTOM, lowest_used_cache_tier_);
    if (s.ok()) {
      RecordTick(statistics_, BLOB_DB_CACHE_ADD);
      RecordTick(statistics_, BLOB_DB_CACHE_BYTES_WRITE, value.size());
    } else {
      RecordTick(statistics_, BLOB_DB_CACHE_ADD_FAILURES);
    }
    return s;
  }
  std::unique_ptr<StableBlobContents> contents;
  Status s = StableBlobContents::CreateWithVerifiedValue(
      value, *fingerprint, stable_blob_cache_.get()->memory_allocator(),
      &contents);
  if (!s.ok()) {
    RecordTick(statistics_, BLOB_DB_CACHE_ADD_FAILURES);
    return s;
  }
  const CacheKey cache_key = StableCacheKey(blob_file_number, blob_offset);
  return InsertStableBlob(cache_key.AsSlice(), &contents,
                          /*cached_blob=*/nullptr);
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

Status BlobSource::PinStableCacheHit(
    CacheHandleGuard<StableBlobContents>* cached_blob, uint64_t range_offset,
    size_t range_length, PinnableSlice* value) {
  const size_t logical_size = cached_blob->GetValue()->size();
  if (range_length != kWholeBlobLength &&
      (range_offset > logical_size ||
       range_length > logical_size - static_cast<size_t>(range_offset))) {
    return Status::InvalidArgument("Stable blob range is out of bounds");
  }
  PinStableCachedBlob(cached_blob, range_offset, range_length, value);
  return Status::OK();
}

Status BlobSource::GetIndirectBlobFromCache(const BlobIndex& blob_index,
                                            uint64_t range_offset,
                                            size_t range_length,
                                            PinnableSlice* value) const {
  assert(value != nullptr);
  assert(blob_index.IsIndirect());
  value->Reset();
  if (range_length != kWholeBlobLength &&
      blob_index.compression() != kNoCompression) {
    // A range of a compressed value is not supported, also on a cache hit (see
    // GetIndirectBlob()). Checking here means that no caller can miss it.
    return Status::Corruption("Cannot range-read a compressed blob");
  }
  if (!stable_blob_cache_) {
    return Status::NotFound("No stable blob cache");
  }
  const CacheKey stable_key = StableCacheKey(
      blob_index.origin_file_number(), blob_index.original_value_offset());
  CacheHandleGuard<StableBlobContents> cached_blob;
  const Status status = GetStableBlobFromCache(
      stable_key.AsSlice(), &cached_blob,
      /*record_statistics=*/true, /*primary_only=*/false);
  if (!status.ok()) {
    return status;
  }
  return PinStableCacheHit(&cached_blob, range_offset, range_length, value);
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
    const CacheKey* stable_cache_key, bool materialized_whole,
    std::unique_ptr<BlobContents>* physical_value,
    const std::optional<uint32_t>& value_crc32c, PinnableSlice* result) const {
  assert(blob_index.IsIndirect());
  assert(physical_value != nullptr && *physical_value != nullptr);
  assert(result != nullptr);
  if (!materialized_whole) {
    PinOwnedBlob(physical_value, result);
    return Status::OK();
  }
  const bool fill_stable_cache =
      stable_cache_key != nullptr && read_options.fill_cache;
  const Slice value = (*physical_value)->data();
  if (read_options.verify_checksums || fill_stable_cache) {
    // The reader may have computed the CRC while it checked the record.
    const uint32_t checksum = value_crc32c.has_value()
                                  ? *value_crc32c
                                  : crc32c::Value(value.data(), value.size());
    if (checksum != blob_index.stable_checksum()) {
      return Status::Corruption(
          "Stable blob checksum mismatch",
          "generation " + std::to_string(blob_index.generation_file_number()) +
              ", origin " + std::to_string(blob_index.origin_file_number()) +
              ", original offset " +
              std::to_string(blob_index.original_value_offset()));
    }
  }

  if (fill_stable_cache) {
    CacheHandleGuard<StableBlobContents> cached_blob;
    Status status = PutStableBlobIntoCache(
        stable_cache_key->AsSlice(), blob_index, physical_value, &cached_blob);
    if (!status.ok()) {
      return status;
    }
    PinStableCachedBlob(&cached_blob, /*range_offset=*/0, kWholeBlobLength,
                        result);
    return Status::OK();
  }
  PinOwnedBlob(physical_value, result);
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
    const BlobIndex& blob_index, const BlobPhysicalFile& file,
    const MutableCFOptions& mutable_cf_options, uint64_t range_offset,
    size_t range_length, FilePrefetchBuffer* prefetch_buffer,
    PinnableSlice* value, uint64_t* bytes_read, bool cache_checked) {
  assert(value != nullptr);
  value->Reset();
  if (bytes_read != nullptr) {
    *bytes_read = 0;
  }
  if (range_length != kWholeBlobLength &&
      blob_index.compression() != kNoCompression) {
    // As in base RocksDB, a range of a compressed value is not supported, also
    // when the stable cache holds the value: a byte range cannot be
    // decompressed alone. Callers read the whole value and slice it.
    return Status::Corruption("Cannot range-read a compressed blob");
  }
  Status status;
  if (!cache_checked) {
    status =
        GetIndirectBlobFromCache(blob_index, range_offset, range_length, value);
    if (!status.IsNotFound()) {
      return status;
    }
  }
  status = ValidateIndirectBlobIndexForRead(blob_index, file.is_relocation);
  if (!status.ok()) {
    return status;
  }
  if (range_length != kWholeBlobLength &&
      (range_offset > blob_index.size() ||
       static_cast<uint64_t>(range_length) >
           blob_index.size() - range_offset)) {
    return Status::InvalidArgument("Stable blob range is out of bounds");
  }
  if (file.open_info.file_number == 0 || file.file_size == 0) {
    return Status::Corruption("Invalid indirect blob physical file");
  }

  // The stable cache key is derived only when there is a stable cache. It is
  // used to fill the cache after the read.
  CacheKey stable_key;
  if (stable_blob_cache_) {
    stable_key = StableCacheKey(blob_index.origin_file_number(),
                                blob_index.original_value_offset());
  }
  if (read_options.read_tier == kBlockCacheTier) {
    return Status::Incomplete("Cannot read indirect blob: no disk I/O allowed");
  }

  const bool materialize_whole = range_length == kWholeBlobLength;
  // A value that will be cached ends up in a buffer from the cache's allocator,
  // so the cache entry can adopt the buffer without another copy. A relocation
  // record is read directly into that buffer. A normal blob file value is read
  // into a local buffer and copied (or decompressed) into it once, as in base.
  MemoryAllocator* const allocator =
      stable_blob_cache_ && read_options.fill_cache
          ? stable_blob_cache_.get()->memory_allocator()
          : nullptr;
  // The reader computes the value CRC while it checks the record, when it can
  // (whole uncompressed values).
  std::optional<uint32_t> value_crc32c;
  std::optional<uint32_t>* const value_crc32c_out =
      materialize_whole && blob_index.compression() == kNoCompression
          ? &value_crc32c
          : nullptr;
  std::unique_ptr<BlobContents> physical_value;
  uint64_t physical_bytes_read = 0;
  if (file.is_relocation) {
    CacheHandleGuard<BlobFileReader> reader;
    status = blob_file_cache_->GetRelocationFileReader(
        read_options, file, mutable_cf_options, &reader);
    if (!status.ok()) {
      return status;
    }
    SameFileBlobStandaloneRead output;
    RelocationBlobReadRequest request;
    request.blob_index = &blob_index;
    request.range_offset = materialize_whole ? 0 : range_offset;
    request.range_length = materialize_whole ? kWholeBlobLength : range_length;
    request.status = &status;
    request.output = &output;
    reader.GetValue()->relocation()->MultiGetBlob(read_options, allocator,
                                                  /*num_reads=*/1, &request,
                                                  &physical_bytes_read);
    physical_value = std::move(output.contents);
    if (value_crc32c_out != nullptr) {
      value_crc32c = output.gen2.payload_crc32c;
    }
  } else {
    status = ReadBlobWithReaderRetry(
        blob_file_cache_, read_options, file.open_info,
        [&](BlobFileReader* reader) {
          // A metadata error: an evict and reopen cannot fix it.
          return reader->GetCompressionType() == blob_index.compression()
                     ? Status::OK()
                     : Status::Corruption(
                           "Compression type mismatch when reading indirect "
                           "blob");
        },
        [&](BlobFileReader* reader) {
          physical_value.reset();
          value_crc32c.reset();
          physical_bytes_read = 0;
          if (materialize_whole) {
            return reader->GetBlob(read_options, user_key,
                                   blob_index.original_value_offset(),
                                   blob_index.size(), blob_index.compression(),
                                   prefetch_buffer, allocator, &physical_value,
                                   &physical_bytes_read, value_crc32c_out);
          }
          return reader->GetBlobRange(
              read_options, user_key, blob_index.original_value_offset(),
              blob_index.size(), range_offset, range_length,
              /*allocator=*/nullptr, &physical_value, &physical_bytes_read);
        });
  }
  if (!status.ok()) {
    return status;
  }
  if (bytes_read != nullptr) {
    *bytes_read = physical_bytes_read;
  }
  // BlobFileReader records these for blob log files. The relocation reader
  // reads through the table reader, which does not record them, so BlobSource
  // records them here.
  if (file.is_relocation) {
    RecordTick(statistics_, BLOB_DB_BLOB_FILE_BYTES_READ, physical_bytes_read);
    PERF_COUNTER_ADD(blob_read_count, 1);
    PERF_COUNTER_ADD(blob_read_byte, physical_bytes_read);
  }
  // BlobFileReader and the relocation table reader do not record lazy-read
  // statistics, so BlobSource records them here. A stable cache hit returns
  // earlier, so it is not counted.
  if (read_options.io_activity == Env::IOActivity::kLazyResolve) {
    RecordLazyRead(statistics_, physical_bytes_read);
    if (!materialize_whole) {
      RecordLazyPartialRead(statistics_, physical_bytes_read,
                            blob_index.size());
    }
  }

  return FinishIndirectBlobRead(
      read_options, blob_index, stable_blob_cache_ ? &stable_key : nullptr,
      materialize_whole, &physical_value, value_crc32c, value);
}

void BlobSource::MultiGetIndirectBlobFromOneFile(
    const ReadOptions& read_options, const BlobPhysicalFile& file,
    const MutableCFOptions& mutable_cf_options, size_t num_requests,
    IndirectBlobReadRequest* requests, uint64_t* bytes_read) {
  if (bytes_read != nullptr) {
    *bytes_read = 0;
  }
  if (num_requests == 0) {
    return;
  }
  assert(requests != nullptr);
  if (file.open_info.file_number == 0 || file.file_size == 0) {
    for (size_t i = 0; i < num_requests; ++i) {
      assert(requests[i].status != nullptr);
      *requests[i].status =
          Status::Corruption("Invalid indirect blob physical file");
    }
    return;
  }
  // One caller, Version::MultiGetBlob, does not limit the number of requests
  // per file. The fixed-size arrays below hold at most kMaxIndirectBatch
  // (MultiGetContext::MAX_BATCH_SIZE, 32) requests, so a larger call is split
  // into chunks of that size. A chunk cannot overflow the uint64_t byte counts
  // below: a relocation value is at most kMaxSimpleGen2BlobPayloadSize (about
  // 4 GiB), and a normal blob file read must fit inside the file.
  if (num_requests > kMaxIndirectBatch) {
    uint64_t total_bytes_read = 0;
    for (size_t begin = 0; begin < num_requests; begin += kMaxIndirectBatch) {
      const size_t count = std::min(num_requests - begin, kMaxIndirectBatch);
      uint64_t chunk_bytes_read = 0;
      MultiGetIndirectBlobFromOneFile(read_options, file, mutable_cf_options,
                                      count, requests + begin,
                                      &chunk_bytes_read);
      total_bytes_read += chunk_bytes_read;
    }
    if (bytes_read != nullptr) {
      *bytes_read = total_bytes_read;
    }
    return;
  }

  // A request that needs a file read, with its physical result.
  struct Pending {
    size_t request = 0;
    // Whole value, else an uncompressed range read alone.
    bool whole = false;
    // Set only with a stable cache.
    CacheKey stable_key;
    // The read outputs:
    // - out.contents: the physical value.
    // - out.gen2.payload_crc32c: set by the reader when it computed the value
    //   CRC while checking the record.
    // - out.gen2.copied: true when the value was copied from an identical read
    //   in the batch, not read from storage.
    SameFileBlobStandaloneRead out;
  };
  autovector<Pending, kMaxIndirectBatch> pending;
  const bool no_io = read_options.read_tier == kBlockCacheTier;
  for (size_t i = 0; i < num_requests; ++i) {
    IndirectBlobReadRequest& request = requests[i];
    assert(request.blob_index != nullptr);
    assert(request.result != nullptr);
    assert(request.status != nullptr);
    const BlobIndex& blob_index = *request.blob_index;
    request.result->Reset();
    if (request.range_length != kWholeBlobLength &&
        blob_index.compression() != kNoCompression) {
      // A range of a compressed value is not supported (see GetIndirectBlob()).
      *request.status =
          Status::Corruption("Cannot range-read a compressed blob");
      continue;
    }
    if (!request.cache_checked) {
      *request.status =
          GetIndirectBlobFromCache(blob_index, request.range_offset,
                                   request.range_length, request.result);
      if (!request.status->IsNotFound()) {
        continue;
      }
    }
    *request.status =
        ValidateIndirectBlobIndexForRead(blob_index, file.is_relocation);
    if (!request.status->ok()) {
      continue;
    }
    CacheKey stable_key;
    if (stable_blob_cache_) {
      stable_key = StableCacheKey(blob_index.origin_file_number(),
                                  blob_index.original_value_offset());
    }
    const bool whole = request.range_length == kWholeBlobLength;
    if (!whole && (request.range_offset > blob_index.size() ||
                   static_cast<uint64_t>(request.range_length) >
                       blob_index.size() - request.range_offset)) {
      *request.status =
          Status::InvalidArgument("Stable blob range is out of bounds");
      continue;
    }
    if (no_io) {
      *request.status =
          Status::Incomplete("Cannot read indirect blob: no disk I/O allowed");
      continue;
    }
    *request.status = Status::OK();
    pending.emplace_back();
    pending.back().request = i;
    pending.back().whole = whole;
    pending.back().stable_key = stable_key;
  }
  if (pending.empty()) {
    return;
  }

  // A value that will be cached ends up in a buffer from the cache's allocator,
  // so the cache entry can adopt the buffer without another copy. A relocation
  // record is read directly into that buffer. A normal blob file value is read
  // into a local buffer and copied (or decompressed) into it once, as in base.
  MemoryAllocator* const allocator =
      stable_blob_cache_ && read_options.fill_cache
          ? stable_blob_cache_.get()->memory_allocator()
          : nullptr;
  const auto value_crc32c_out = [requests](Pending& p) {
    return p.whole && requests[p.request].blob_index->compression() ==
                          kNoCompression
               ? &p.out.gen2.payload_crc32c
               : nullptr;
  };
  uint64_t physical_bytes_read = 0;
  if (file.is_relocation) {
    CacheHandleGuard<BlobFileReader> reader;
    Status open_status = blob_file_cache_->GetRelocationFileReader(
        read_options, file, mutable_cf_options, &reader);
    if (!open_status.ok()) {
      for (const Pending& p : pending) {
        *requests[p.request].status = open_status;
      }
      return;
    }
    // At most kMaxIndirectBatch requests, kept contiguous for the reader.
    std::array<RelocationBlobReadRequest, kMaxIndirectBatch>
        relocation_requests;
    size_t relocation_count = 0;
    for (Pending& p : pending) {
      const IndirectBlobReadRequest& request = requests[p.request];
      RelocationBlobReadRequest& relocation_request =
          relocation_requests[relocation_count++];
      relocation_request.blob_index = request.blob_index;
      relocation_request.range_offset = p.whole ? 0 : request.range_offset;
      relocation_request.range_length =
          p.whole ? kWholeBlobLength : request.range_length;
      relocation_request.status = request.status;
      relocation_request.output = &p.out;
    }
    reader.GetValue()->relocation()->MultiGetBlob(
        read_options, allocator, relocation_count, relocation_requests.data(),
        &physical_bytes_read);
    RecordTick(statistics_, BLOB_DB_BLOB_FILE_BYTES_READ, physical_bytes_read);
    PERF_COUNTER_ADD(blob_read_byte, physical_bytes_read);
  } else {
    CacheHandleGuard<BlobFileReader> reader;
    Status open_status = blob_file_cache_->GetBlobFileReader(
        read_options, file.open_info, &reader);
    if (!open_status.ok()) {
      for (const Pending& p : pending) {
        *requests[p.request].status = open_status;
      }
      return;
    }
    // Requests and their planned reads; *_owner maps a request to its index in
    // `pending`.
    autovector<BlobReadRequest, kMaxIndirectBatch> whole_requests;
    autovector<BlobRangeReadRequest, kMaxIndirectBatch> range_requests;
    autovector<size_t, kMaxIndirectBatch> whole_owner;
    autovector<size_t, kMaxIndirectBatch> range_owner;
    PlannedReads whole_reads;
    PlannedReads range_reads;
    for (size_t position = 0; position < pending.size(); ++position) {
      Pending& p = pending[position];
      IndirectBlobReadRequest& request = requests[p.request];
      const BlobIndex& blob_index = *request.blob_index;
      if (reader.GetValue()->GetCompressionType() != blob_index.compression()) {
        *request.status = Status::Corruption(
            "Compression type mismatch when reading indirect blob");
        continue;
      }
      if (blob_index.size() > std::numeric_limits<size_t>::max()) {
        *request.status = Status::Corruption("Indirect blob size is too large");
        continue;
      }
      const uint64_t offset = blob_index.original_value_offset();
      if (p.whole) {
        whole_requests.emplace_back(
            request.user_key, offset, static_cast<size_t>(blob_index.size()),
            blob_index.compression(), /*result=*/nullptr, request.status);
        if (read_options.verify_checksums) {
          whole_requests.back().value_crc32c = value_crc32c_out(p);
        }
        const auto extent = WholeBlobReadExtent(
            read_options, request.user_key.size(), offset, blob_index.size());
        whole_reads.push_back({extent.first, extent.second,
                               whole_requests.size() - 1, request.user_key});
        whole_owner.push_back(position);
      } else {
        range_requests.emplace_back(request.user_key, offset, blob_index.size(),
                                    request.range_offset, request.range_length,
                                    /*result=*/nullptr, request.status);
        range_reads.push_back({offset + request.range_offset,
                               request.range_length, range_requests.size() - 1,
                               request.user_key});
        range_owner.push_back(position);
      }
    }
    physical_bytes_read += ReadInPlannedBatches(
        &whole_requests, &whole_reads, allocator,
        [&](autovector<std::pair<BlobReadRequest*,
                                 std::unique_ptr<BlobContents>>>& batch_reads,
            uint64_t* batch_bytes_read) {
          reader.GetValue()->MultiGetBlob(read_options, allocator, batch_reads,
                                          batch_bytes_read);
        },
        [&](size_t request) {
          return &pending[whole_owner[request]].out.contents;
        },
        [&](size_t request) {
          Pending& p = pending[whole_owner[request]];
          p.out.gen2.copied = true;
          // The copy has no CRC from the reader; FinishIndirectBlobRead
          // computes the value CRC again.
          p.out.gen2.payload_crc32c.reset();
        });
    physical_bytes_read += ReadInPlannedBatches(
        &range_requests, &range_reads, /*allocator=*/nullptr,
        [&](autovector<std::pair<BlobRangeReadRequest*,
                                 std::unique_ptr<BlobContents>>>& batch_reads,
            uint64_t* batch_bytes_read) {
          reader.GetValue()->MultiGetBlobRange(read_options, batch_reads,
                                               batch_bytes_read);
        },
        [&](size_t request) {
          return &pending[range_owner[request]].out.contents;
        },
        [&](size_t request) {
          pending[range_owner[request]].out.gen2.copied = true;
        });
    // A Corruption of a whole read may come from a stale cached reader (for
    // example, the file was replaced). Like MultiGetBlobFromOneFile, those
    // reads are retried once with a newly opened reader. Range reads are not
    // retried, like MultiGetBlobRangeFromOneFile.
    autovector<size_t, kMaxIndirectBatch> stale;
    for (size_t r = 0; r < whole_requests.size(); ++r) {
      if (whole_requests[r].status->IsCorruption()) {
        stale.push_back(r);
      }
    }
    if (!stale.empty()) {
      reader.Reset();
      blob_file_cache_->Evict(file.open_info.file_number);
      std::unique_ptr<BlobFileReader> fresh_reader;
      const Status reopen_status = blob_file_cache_->OpenBlobFileReaderUncached(
          read_options, file.open_info, &fresh_reader,
          /*allow_footer_skip_retry=*/false);
      if (!reopen_status.ok()) {
        for (size_t r : stale) {
          *whole_requests[r].status = AppendBlobRefreshRetryFailure(
              *whole_requests[r].status, reopen_status);
        }
      } else {
        autovector<BlobReadRequest, kMaxIndirectBatch> retry_requests;
        autovector<Status, kMaxIndirectBatch> stale_statuses;
        PlannedReads retry_reads;
        for (size_t r : stale) {
          BlobReadRequest& request = whole_requests[r];
          stale_statuses.push_back(*request.status);
          // Only a failed retry reads the stale status.
          stale_statuses.back().PermitUncheckedError();
          *request.status = Status::OK();
          // The first pass may have copied this read from an equal read. The
          // retry reads it again, so it counts as a read unless the retry
          // itself copies it.
          pending[whole_owner[r]].out.gen2.copied = false;
          pending[whole_owner[r]].out.gen2.payload_crc32c.reset();
          retry_requests.push_back(request);
          const auto extent =
              WholeBlobReadExtent(read_options, request.user_key->size(),
                                  request.offset, request.len);
          retry_reads.push_back({extent.first, extent.second,
                                 retry_requests.size() - 1, *request.user_key});
        }
        physical_bytes_read += ReadInPlannedBatches(
            &retry_requests, &retry_reads, allocator,
            [&](autovector<
                    std::pair<BlobReadRequest*, std::unique_ptr<BlobContents>>>&
                    batch_reads,
                uint64_t* batch_bytes_read) {
              fresh_reader->MultiGetBlob(read_options, allocator, batch_reads,
                                         batch_bytes_read);
            },
            [&](size_t k) {
              return &pending[whole_owner[stale[k]]].out.contents;
            },
            [&](size_t k) {
              Pending& p = pending[whole_owner[stale[k]]];
              p.out.gen2.copied = true;
              p.out.gen2.payload_crc32c.reset();
            });
        bool install_fresh_reader = false;
        for (size_t k = 0; k < stale.size(); ++k) {
          Status* const status = retry_requests[k].status;
          if (status->ok()) {
            install_fresh_reader = true;
          } else {
            *status = AppendBlobRefreshRetryFailure(stale_statuses[k], *status);
          }
        }
        if (install_fresh_reader) {
          CacheHandleGuard<BlobFileReader> ignored_reader;
          blob_file_cache_
              ->RefreshBlobFileReader(file.open_info, &fresh_reader,
                                      &ignored_reader)
              .PermitUncheckedError();
        }
      }
    }
  }

  size_t successful_reads = 0;
  for (Pending& p : pending) {
    IndirectBlobReadRequest& request = requests[p.request];
    if (!request.status->ok()) {
      continue;
    }
    successful_reads += p.out.gen2.copied ? 0 : 1;
    if (read_options.io_activity == Env::IOActivity::kLazyResolve &&
        !p.out.gen2.copied) {
      // For a relocation file this is an estimate: the range length for an
      // uncompressed range read, else the record size (value plus
      // kSimpleGen2BlobTrailerSize, 5 bytes). It does not count the index and
      // data block reads.
      const BlobIndex& blob_index = *request.blob_index;
      const uint64_t read_size =
          !p.whole ? request.range_length
          : file.is_relocation
              ? blob_index.size() + kSimpleGen2BlobTrailerSize
              : WholeBlobReadExtent(read_options, request.user_key.size(),
                                    blob_index.original_value_offset(),
                                    blob_index.size())
                    .second;
      RecordLazyRead(statistics_, read_size);
      if (!p.whole) {
        RecordLazyPartialRead(statistics_, read_size, blob_index.size());
      }
    }
    *request.status = FinishIndirectBlobRead(
        read_options, *request.blob_index,
        stable_blob_cache_ ? &p.stable_key : nullptr, p.whole, &p.out.contents,
        p.out.gen2.payload_crc32c, request.result);
  }
  if (file.is_relocation) {
    PERF_COUNTER_ADD(blob_read_count, successful_reads);
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
  // it. An uncompressed payload is exposed as BlobContents in place (the
  // trailer sits unused at the tail of the buffer); a compressed payload is
  // decompressed into a new buffer.
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
    // Outputs of the read. extras.copied marks a copy of a duplicate request's
    // read, which counts no read statistics.
    SimpleGen2ReadExtras extras;
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
  for (size_t k = 0; k < pending.size(); ++k) {
    read_reqs[k].extras = &pending[k].extras;
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

    if (!pending[k].extras.copied) {
      RecordTick(statistics_, BLOB_DB_BLOB_FILE_BYTES_READ, record_size);
      PERF_COUNTER_ADD(blob_read_count, 1);
      PERF_COUNTER_ADD(blob_read_byte, record_size);
      if (read_options.io_activity == Env::IOActivity::kLazyResolve) {
        RecordLazyRead(statistics_, record_size);
      }
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
    // Outputs of the read. extras.copied marks a copy of a duplicate request's
    // read, which counts no read statistics.
    SimpleGen2ReadExtras extras;
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
  for (size_t k = 0; k < pending.size(); ++k) {
    read_reqs[k].extras = &pending[k].extras;
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

    if (!pending[k].extras.copied) {
      RecordTick(statistics_, BLOB_DB_BLOB_FILE_BYTES_READ, req.range_length);
      PERF_COUNTER_ADD(blob_read_count, 1);
      PERF_COUNTER_ADD(blob_read_byte, req.range_length);
      RecordLazyRead(statistics_, req.range_length);
      RecordLazyPartialRead(statistics_, req.range_length, req.payload_size);
    }

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
