//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_cache.h"

#include <cassert>
#include <memory>

#include "db/blob/blob_file_reader.h"
#include "logging/logging.h"
#include "options/cf_options.h"
#include "rocksdb/cache.h"
#include "rocksdb/slice.h"
#include "test_util/sync_point.h"
#include "trace_replay/io_tracer.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

BlobFileCache::BlobFileCache(Cache* cache,
                             const ImmutableOptions* immutable_options,
                             const FileOptions* file_options,
                             uint32_t column_family_id,
                             HistogramImpl* blob_file_read_hist,
                             const std::shared_ptr<IOTracer>& io_tracer)
    : cache_(cache),
      mutex_(kNumberOfMutexStripes),
      immutable_options_(immutable_options),
      file_options_(file_options),
      column_family_id_(column_family_id),
      blob_file_read_hist_(blob_file_read_hist),
      io_tracer_(io_tracer) {
  assert(cache_);
  assert(immutable_options_);
  assert(file_options_);
}

Status BlobFileCache::GetBlobFileReader(
    const ReadOptions& read_options, const BlobFileOpenInfo& blob_file,
    CompressionManager* configured_compression_manager,
    CacheHandleGuard<BlobFileReader>* blob_file_reader,
    bool allow_footer_skip_retry) {
  assert(blob_file_reader);
  assert(blob_file_reader->IsEmpty());

  const uint64_t blob_file_number = blob_file.file_number;

  // NOTE: sharing same Cache with table_cache
  const Slice key = GetSliceForKey(&blob_file_number);

  assert(cache_);

  TypedHandle* handle = cache_.Lookup(key);
  if (handle) {
    *blob_file_reader = cache_.Guard(handle);
    return Status::OK();
  }

  TEST_SYNC_POINT("BlobFileCache::GetBlobFileReader:DoubleCheck");

  // Check again while holding mutex
  MutexLock lock(&mutex_.Get(key));

  handle = cache_.Lookup(key);
  if (handle) {
    *blob_file_reader = cache_.Guard(handle);
    return Status::OK();
  }

  assert(immutable_options_);
  Statistics* const statistics = immutable_options_->stats;

  RecordTick(statistics, NO_FILE_OPENS);

  std::unique_ptr<BlobFileReader> reader;

  {
    Status s = OpenBlobFileReader(read_options, blob_file,
                                  configured_compression_manager,
                                  allow_footer_skip_retry, &reader);
    if (!s.ok()) {
      ROCKS_LOG_WARN(immutable_options_->logger,
                     "BlobFileCache open failed for blob file %" PRIu64
                     " in CF %u: %s",
                     blob_file_number, column_family_id_, s.ToString().c_str());
      RecordTick(statistics, NO_FILE_ERRORS);
      return s;
    }
  }

  {
    constexpr size_t charge = 1;

    const Status s = cache_.Insert(key, reader.get(), charge, &handle);
    if (!s.ok()) {
      RecordTick(statistics, NO_FILE_ERRORS);
      return s;
    }
  }

  [[maybe_unused]] BlobFileReader* released_reader = reader.release();

  *blob_file_reader = cache_.Guard(handle);

  return Status::OK();
}

Status BlobFileCache::OpenBlobFileReaderUncached(
    const ReadOptions& read_options, const BlobFileOpenInfo& blob_file,
    CompressionManager* configured_compression_manager,
    std::unique_ptr<BlobFileReader>* blob_file_reader,
    bool allow_footer_skip_retry) {
  assert(blob_file_reader);
  assert(!*blob_file_reader);

  Statistics* const statistics = immutable_options_->stats;
  RecordTick(statistics, NO_FILE_OPENS);

  Status s = OpenBlobFileReader(read_options, blob_file,
                                configured_compression_manager,
                                allow_footer_skip_retry, blob_file_reader);
  if (!s.ok()) {
    RecordTick(statistics, NO_FILE_ERRORS);
  }
  return s;
}

void BlobFileCache::RegisterBlobFileChecksum(
    const BlobFileOpenInfo& blob_file) {
  assert(blob_file.file_checksum.empty() ==
         blob_file.file_checksum_func_name.empty());
  if (blob_file.file_checksum.empty()) {
    return;
  }

  const Slice key = GetSliceForKey(&blob_file.file_number);
  MutexLock cache_lock(&mutex_.Get(key));
  {
    MutexLock checksum_lock(&blob_file_checksums_mutex_);
    BlobFileChecksum& checksum = blob_file_checksums_[blob_file.file_number];
    checksum.value = blob_file.file_checksum.ToString();
    checksum.function_name = blob_file.file_checksum_func_name.ToString();
  }

  // Readers opened while the file was active did not carry finalized metadata
  // and may also have observed a footer-less file size.
  cache_.get()->Erase(key);
}

void BlobFileCache::UnregisterBlobFileChecksum(uint64_t blob_file_number) {
  MutexLock checksum_lock(&blob_file_checksums_mutex_);
  blob_file_checksums_.erase(blob_file_number);
}

Status BlobFileCache::OpenBlobFileReader(
    const ReadOptions& read_options, const BlobFileOpenInfo& blob_file,
    CompressionManager* configured_compression_manager,
    bool allow_footer_skip_retry,
    std::unique_ptr<BlobFileReader>* blob_file_reader) {
  assert(file_options_);
  assert(blob_file.file_checksum.empty() ==
         blob_file.file_checksum_func_name.empty());

  FileOptions file_options = *file_options_;
  if (!blob_file.file_checksum.empty()) {
    file_options.file_checksum = blob_file.file_checksum.ToString();
    file_options.file_checksum_func_name =
        blob_file.file_checksum_func_name.ToString();
  } else {
    MutexLock checksum_lock(&blob_file_checksums_mutex_);
    const auto it = blob_file_checksums_.find(blob_file.file_number);
    if (it != blob_file_checksums_.end()) {
      file_options.file_checksum = it->second.value;
      file_options.file_checksum_func_name = it->second.function_name;
    } else {
      file_options.file_checksum.clear();
      file_options.file_checksum_func_name.clear();
    }
  }

  Status s = BlobFileReader::Create(
      *immutable_options_, read_options, file_options, column_family_id_,
      blob_file_read_hist_, blob_file.file_number, io_tracer_,
      configured_compression_manager,
      /*skip_footer_validation=*/false, blob_file_reader);
  if (!s.ok() && s.IsCorruption() && allow_footer_skip_retry) {
    blob_file_reader->reset();
    s = BlobFileReader::Create(
        *immutable_options_, read_options, file_options, column_family_id_,
        blob_file_read_hist_, blob_file.file_number, io_tracer_,
        configured_compression_manager,
        /*skip_footer_validation=*/true, blob_file_reader);
  }
  return s;
}

Status BlobFileCache::InsertBlobFileReader(
    uint64_t blob_file_number,
    std::unique_ptr<BlobFileReader>* blob_file_reader,
    CacheHandleGuard<BlobFileReader>* cached_blob_file_reader) {
  assert(blob_file_reader);
  assert(*blob_file_reader);
  assert(cached_blob_file_reader);
  assert(cached_blob_file_reader->IsEmpty());

  const Slice key = GetSliceForKey(&blob_file_number);
  // Serialize refreshes for the same blob file. The cache API does not expose
  // a conditional replace, so refresh is intentionally modeled as
  // Lookup/optional Erase/Insert under this per-key mutex.
  MutexLock lock(&mutex_.Get(key));

  TypedHandle* handle = cache_.Lookup(key);
  if (handle) {
    *cached_blob_file_reader = cache_.Guard(handle);
    blob_file_reader->reset();
    return Status::OK();
  }

  constexpr size_t charge = 1;
  Status s = cache_.Insert(key, blob_file_reader->get(), charge, &handle);
  if (!s.ok()) {
    RecordTick(immutable_options_->stats, NO_FILE_ERRORS);
    return s;
  }

  // Ownership transferred to the cache.
  [[maybe_unused]] BlobFileReader* released_reader =
      blob_file_reader->release();
  *cached_blob_file_reader = cache_.Guard(handle);
  return Status::OK();
}

Status BlobFileCache::RefreshBlobFileReader(
    const BlobFileOpenInfo& blob_file,
    std::unique_ptr<BlobFileReader>* blob_file_reader,
    CacheHandleGuard<BlobFileReader>* cached_blob_file_reader) {
  assert(blob_file_reader);
  assert(*blob_file_reader);
  assert(cached_blob_file_reader);
  assert(cached_blob_file_reader->IsEmpty());

  const Slice key = GetSliceForKey(&blob_file.file_number);
  MutexLock lock(&mutex_.Get(key));

  TypedHandle* handle = cache_.Lookup(key);

  if (blob_file.file_checksum.empty()) {
    MutexLock checksum_lock(&blob_file_checksums_mutex_);
    if (blob_file_checksums_.find(blob_file.file_number) !=
        blob_file_checksums_.end()) {
      // This reader can have been opened just before finalization registered
      // checksum metadata. Never let such a racing refresh repopulate the
      // cache.
      if (handle) {
        *cached_blob_file_reader = cache_.Guard(handle);
      }
      blob_file_reader->reset();
      return Status::OK();
    }
  }

  if (handle) {
    BlobFileReader* const cached_reader = cache_.Value(handle);
    assert(cached_reader != nullptr);

    // Active direct-write blob files can grow between refresh attempts. Keep
    // whichever reader observed the larger on-disk size so an older refresh
    // cannot overwrite a newer one that another thread already installed.
    if (cached_reader->GetFileSize() >= (*blob_file_reader)->GetFileSize()) {
      *cached_blob_file_reader = cache_.Guard(handle);
      blob_file_reader->reset();
      return Status::OK();
    }

    cache_.Release(handle);
    cache_.get()->Erase(key);
  }

  constexpr size_t charge = 1;
  Status s = cache_.Insert(key, blob_file_reader->get(), charge, &handle);
  if (!s.ok()) {
    RecordTick(immutable_options_->stats, NO_FILE_ERRORS);
    return s;
  }

  // Ownership transferred to the cache.
  [[maybe_unused]] BlobFileReader* released_reader =
      blob_file_reader->release();
  *cached_blob_file_reader = cache_.Guard(handle);
  return Status::OK();
}

void BlobFileCache::Evict(uint64_t blob_file_number) {
  // NOTE: sharing same Cache with table_cache
  const Slice key = GetSliceForKey(&blob_file_number);

  assert(cache_);

  MutexLock lock(&mutex_.Get(key));
  cache_.get()->Erase(key);
}

}  // namespace ROCKSDB_NAMESPACE
