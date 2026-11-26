//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_cache.h"

#include <cassert>
#include <memory>

#include "db/blob/blob_file_reader.h"
#include "db/blob/blob_relocation_file.h"
#include "logging/logging.h"
#include "options/cf_options.h"
#include "rocksdb/cache.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "table/adaptive/adaptive_table_factory.h"
#include "table/block_based/block_based_table_factory.h"
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
    : BlobFileCache(cache, immutable_options, file_options, column_family_id,
                    blob_file_read_hist, io_tracer,
                    /*db_session_id=*/std::string(),
                    /*block_cache_tracer=*/nullptr) {}

BlobFileCache::BlobFileCache(
    Cache* cache, const ImmutableOptions* immutable_options,
    const FileOptions* file_options, uint32_t column_family_id,
    HistogramImpl* blob_file_read_hist,
    const std::shared_ptr<IOTracer>& io_tracer,
    const std::string& db_session_id, BlockCacheTracer* block_cache_tracer)
    : cache_(cache),
      mutex_(kNumberOfMutexStripes),
      immutable_options_(immutable_options),
      file_options_(file_options),
      column_family_id_(column_family_id),
      blob_file_read_hist_(blob_file_read_hist),
      io_tracer_(io_tracer),
      db_session_id_(db_session_id),
      block_cache_tracer_(block_cache_tracer) {
  assert(cache_);
  assert(immutable_options_);
  assert(file_options_);
}

BlobFileCache::~BlobFileCache() = default;

port::Mutex* BlobFileCache::GetStripeMutex(FileKind kind, const Slice& key) {
  if (kind == FileKind::kBlobFile) {
    return &mutex_.Get(key);
  }
  std::call_once(relocation_mutex_once_, [this]() {
    relocation_mutex_ =
        std::make_unique<Striped<CacheAlignedWrapper<port::Mutex>>>(
            kNumberOfRelocationMutexStripes);
  });
  return &relocation_mutex_->Get(key);
}

Status BlobFileCache::GetRelocationTableFactory(
    const MutableCFOptions& mutable_cf_options,
    std::shared_ptr<const TableFactory>* factory) {
  assert(factory != nullptr);
  MutexLock lock(&relocation_factory_mutex_);
  if (relocation_factory_ == nullptr ||
      relocation_factory_source_ != mutable_cf_options.table_factory) {
    const BlockBasedTableOptions* const table_options =
        GetBlockBasedTableWriterOptions(mutable_cf_options.table_factory.get());
    if (table_options == nullptr) {
      return Status::NotSupported(
          "Blob relocation requires a block-based table factory");
    }
    relocation_factory_ = std::make_shared<BlockBasedTableFactory>(
        MakeBlobRelocationTableOptions(*table_options));
    relocation_factory_source_ = mutable_cf_options.table_factory;
  }
  *factory = relocation_factory_;
  return Status::OK();
}

template <typename ValidateFn, typename OpenFn>
Status BlobFileCache::GetBlobFileReaderImpl(
    const ReadOptions& read_options, uint64_t blob_file_number, FileKind kind,
    const ValidateFn& validate, const OpenFn& open,
    CacheHandleGuard<BlobFileReader>* blob_file_reader) {
  assert(blob_file_reader);
  assert(blob_file_reader->IsEmpty());

  // NOTE: sharing same Cache with table_cache
  const Slice key = GetSliceForKey(&blob_file_number);

  assert(cache_);

  const auto use_cached = [&](TypedHandle* handle) {
    const Status validation = validate(*cache_.Value(handle));
    if (!validation.ok()) {
      cache_.Release(handle);
      return validation;
    }
    *blob_file_reader = cache_.Guard(handle);
    return Status::OK();
  };

  TypedHandle* handle = cache_.Lookup(key);
  if (handle) {
    return use_cached(handle);
  }

  TEST_SYNC_POINT("BlobFileCache::GetBlobFileReader:DoubleCheck");

  // Check again while holding mutex
  MutexLock lock(GetStripeMutex(kind, key));

  handle = cache_.Lookup(key);
  if (handle) {
    return use_cached(handle);
  }

  if (read_options.read_tier == kBlockCacheTier) {
    return Status::Incomplete(
        "Cannot open blob file reader: no disk I/O allowed");
  }

  assert(immutable_options_);
  Statistics* const statistics = immutable_options_->stats;
  if (kind == FileKind::kRelocationFile) {
    MutexLock failed_lock(&failed_relocation_opens_mutex_);
    const UnorderedMap<uint64_t, FailedRelocationOpen>::iterator failed =
        failed_relocation_opens_.find(blob_file_number);
    if (failed != failed_relocation_opens_.end()) {
      if (immutable_options_->clock->NowMicros() <
          failed->second.expires_micros) {
        RecordTick(statistics, BLOB_RELOCATION_FAILED_OPEN_CACHE_HITS);
        return failed->second.status;
      }
      failed_relocation_opens_.erase(failed);
    }
  }

  RecordTick(statistics, NO_FILE_OPENS);

  std::unique_ptr<BlobFileReader> reader;

  {
    Status s = open(&reader);
    if (!s.ok()) {
      ROCKS_LOG_WARN(immutable_options_->logger,
                     "BlobFileCache open failed for blob file %" PRIu64
                     " in CF %u: %s",
                     blob_file_number, column_family_id_, s.ToString().c_str());
      RecordTick(statistics, NO_FILE_ERRORS);
      if (kind == FileKind::kRelocationFile) {
        RecordTick(statistics, BLOB_RELOCATION_FILE_OPEN_FAILURES);
      }
      // Remember only errors that come from the file itself. A missing file is
      // NotFound or IOError(PathNotFound). See failed_relocation_opens_ for why
      // other errors are not remembered.
      if (kind == FileKind::kRelocationFile &&
          (s.IsCorruption() || s.IsNotFound() || s.IsPathNotFound())) {
        MutexLock failed_lock(&failed_relocation_opens_mutex_);
        FailedRelocationOpen& failed =
            failed_relocation_opens_[blob_file_number];
        failed =
            FailedRelocationOpen{s, immutable_options_->clock->NowMicros() +
                                        kFailedRelocationOpenMicros};
        // The kept copy may expire or be forgotten without being returned.
        failed.status.PermitUncheckedError();
      }
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

Status BlobFileCache::GetBlobFileReader(
    const ReadOptions& read_options, const BlobFileOpenInfo& blob_file,
    CacheHandleGuard<BlobFileReader>* blob_file_reader,
    bool allow_footer_skip_retry) {
  return GetBlobFileReaderImpl(
      read_options, blob_file.file_number, FileKind::kBlobFile,
      [](const BlobFileReader& reader) {
        return reader.relocation() == nullptr
                   ? Status::OK()
                   : Status::Corruption("Blob file reader metadata mismatch");
      },
      [&](std::unique_ptr<BlobFileReader>* reader) {
        return OpenBlobFileReader(read_options, blob_file,
                                  allow_footer_skip_retry, reader);
      },
      blob_file_reader);
}

Status BlobFileCache::GetRelocationFileReader(
    const ReadOptions& read_options, const BlobPhysicalFile& file,
    const MutableCFOptions& mutable_cf_options,
    CacheHandleGuard<BlobFileReader>* blob_file_reader) {
  return GetBlobFileReaderImpl(
      read_options, file.open_info.file_number, FileKind::kRelocationFile,
      [&](const BlobFileReader& reader) {
        const BlobRelocationFileReader* const relocation = reader.relocation();
        if (relocation == nullptr ||
            relocation->column_family_id() != column_family_id_ ||
            relocation->file_size() != file.file_size ||
            relocation->tail_size() != file.tail_size ||
            relocation->directory_digest() != file.directory_digest) {
          return Status::Corruption("Blob file reader metadata mismatch");
        }
        return Status::OK();
      },
      [&](std::unique_ptr<BlobFileReader>* reader) {
        return OpenRelocationFileReader(read_options, file, mutable_cf_options,
                                        reader);
      },
      blob_file_reader);
}

Status BlobFileCache::OpenRelocationFileReaderUncached(
    const ReadOptions& read_options, const BlobPhysicalFile& file,
    const MutableCFOptions& mutable_cf_options,
    std::unique_ptr<BlobFileReader>* blob_file_reader) {
  assert(blob_file_reader);
  assert(!*blob_file_reader);
  Statistics* const statistics = immutable_options_->stats;
  RecordTick(statistics, NO_FILE_OPENS);
  const Status s = OpenRelocationFileReader(
      read_options, file, mutable_cf_options, blob_file_reader);
  if (!s.ok()) {
    ROCKS_LOG_WARN(immutable_options_->logger,
                   "BlobFileCache uncached open failed for relocation file "
                   "%" PRIu64 " in CF %u: %s",
                   file.open_info.file_number, column_family_id_,
                   s.ToString().c_str());
    RecordTick(statistics, NO_FILE_ERRORS);
    RecordTick(statistics, BLOB_RELOCATION_FILE_OPEN_FAILURES);
  }
  return s;
}

Status BlobFileCache::OpenRelocationFileReader(
    const ReadOptions& read_options, const BlobPhysicalFile& file,
    const MutableCFOptions& mutable_cf_options,
    std::unique_ptr<BlobFileReader>* blob_file_reader) {
  if (db_session_id_.empty()) {
    return Status::NotSupported(
        "Blob relocation file reads need a DB session id");
  }
  std::shared_ptr<const TableFactory> factory;
  Status s = GetRelocationTableFactory(mutable_cf_options, &factory);
  if (!s.ok()) {
    return s;
  }
  std::unique_ptr<BlobRelocationFileReader> relocation;
  s = BlobRelocationFileReader::Open(
      *immutable_options_, read_options, GetFileOptions(file.open_info),
      blob_file_read_hist_, column_family_id_, file, db_session_id_,
      std::move(factory), mutable_cf_options.compression_manager.get(),
      mutable_cf_options.block_protection_bytes_per_key,
      mutable_cf_options.pin_blob_relocation_location_blocks, io_tracer_,
      block_cache_tracer_, &relocation);
  if (s.ok()) {
    blob_file_reader->reset(new BlobFileReader(std::move(relocation)));
  }
  return s;
}

Status BlobFileCache::OpenBlobFileReaderUncached(
    const ReadOptions& read_options, const BlobFileOpenInfo& blob_file,
    std::unique_ptr<BlobFileReader>* blob_file_reader,
    bool allow_footer_skip_retry) {
  return OpenBlobFileReaderUncached(read_options, blob_file, blob_file_reader,
                                    allow_footer_skip_retry,
                                    /*prefer_direct_reads=*/false);
}

Status BlobFileCache::OpenBlobFileReaderUncached(
    const ReadOptions& read_options, const BlobFileOpenInfo& blob_file,
    std::unique_ptr<BlobFileReader>* blob_file_reader,
    bool allow_footer_skip_retry, bool prefer_direct_reads) {
  assert(blob_file_reader);
  assert(!*blob_file_reader);

  Statistics* const statistics = immutable_options_->stats;
  RecordTick(statistics, NO_FILE_OPENS);

  Status s;
  FileOptions file_options = GetFileOptions(blob_file);
  // See OpenBlobFileReaderUncached() in blob_file_cache.h.
  if (prefer_direct_reads && !file_options.use_mmap_reads) {
    file_options.use_direct_reads = true;
    s = BlobFileReader::Create(
        *immutable_options_, read_options, file_options, column_family_id_,
        blob_file_read_hist_, blob_file.file_number, io_tracer_,
        /*skip_footer_validation=*/false, blob_file_reader);
    if (s.ok()) {
      return s;
    }
    // The file system may not support direct reads; the buffered open below
    // reports any real error again.
    blob_file_reader->reset();
  }
  s = OpenBlobFileReader(read_options, blob_file, allow_footer_skip_retry,
                         blob_file_reader);
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
  MutexLock cache_lock(GetStripeMutex(FileKind::kBlobFile, key));
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

FileOptions BlobFileCache::GetFileOptions(const BlobFileOpenInfo& blob_file) {
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
  return file_options;
}

Status BlobFileCache::OpenBlobFileReader(
    const ReadOptions& read_options, const BlobFileOpenInfo& blob_file,
    bool allow_footer_skip_retry,
    std::unique_ptr<BlobFileReader>* blob_file_reader) {
  const FileOptions file_options = GetFileOptions(blob_file);
  Status s = BlobFileReader::Create(
      *immutable_options_, read_options, file_options, column_family_id_,
      blob_file_read_hist_, blob_file.file_number, io_tracer_,
      /*skip_footer_validation=*/false, blob_file_reader);
  if (!s.ok() && s.IsCorruption() && allow_footer_skip_retry) {
    blob_file_reader->reset();
    s = BlobFileReader::Create(
        *immutable_options_, read_options, file_options, column_family_id_,
        blob_file_read_hist_, blob_file.file_number, io_tracer_,
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
  MutexLock lock(GetStripeMutex(FileKind::kBlobFile, key));

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
  MutexLock lock(GetStripeMutex(FileKind::kBlobFile, key));

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

  MutexLock lock(GetStripeMutex(FileKind::kBlobFile, key));
#ifndef NDEBUG
  // Only for normal blob files (see the declaration).
  if (TypedHandle* const handle = cache_.Lookup(key)) {
    assert(cache_.Value(handle)->relocation() == nullptr);
    cache_.Release(handle);
  }
#endif  // NDEBUG
  cache_.get()->Erase(key);
}

void BlobFileCache::ForgetFailedRelocationOpen(uint64_t file_number) {
  MutexLock failed_lock(&failed_relocation_opens_mutex_);
  failed_relocation_opens_.erase(file_number);
}

}  // namespace ROCKSDB_NAMESPACE
