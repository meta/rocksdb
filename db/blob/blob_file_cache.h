//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "cache/typed_cache.h"
#include "db/blob/blob_file_reader.h"
#include "db/blob/blob_read_request.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/table.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

class Cache;
struct ImmutableOptions;
struct FileOptions;
class HistogramImpl;
class Status;
class Slice;
class IOTracer;

class BlobFileCache {
 public:
  BlobFileCache(Cache* cache, const ImmutableOptions* immutable_options,
                const FileOptions* file_options, uint32_t column_family_id,
                HistogramImpl* blob_file_read_hist,
                const std::shared_ptr<IOTracer>& io_tracer,
                const BlockBasedTableOptions* relocation_file_table_options);
  BlobFileCache(const BlobFileCache&) = delete;
  BlobFileCache& operator=(const BlobFileCache&) = delete;
  BlobFileCache(BlobFileCache&&) = delete;
  BlobFileCache& operator=(BlobFileCache&&) = delete;

  ~BlobFileCache() = default;

  // Returns a cached reader for `blob_file`, opening and caching it on miss.
  // Checksum metadata is forwarded to the FileSystem through FileOptions when
  // the file is opened. If `allow_footer_skip_retry` is true, a
  // footer-validation corruption retries once without requiring a footer.
  Status GetBlobFileReader(const ReadOptions& read_options,
                           const BlobFileOpenInfo& blob_file,
                           CacheHandleGuard<BlobFileReader>* blob_file_reader,
                           bool allow_footer_skip_retry = false);

  Status GetRelocationFileReader(
      const ReadOptions& read_options, const BlobFileOpenInfo& blob_file,
      uint64_t expected_file_size, uint32_t expected_directory_digest,
      const std::string& current_db_session_id,
      CompressionManager* compression_manager,
      uint8_t block_protection_bytes_per_key,
      CacheHandleGuard<BlobFileReader>* blob_file_reader);

  // Opens a blob file reader without inserting it into the cache.
  Status OpenBlobFileReaderUncached(
      const ReadOptions& read_options, const BlobFileOpenInfo& blob_file,
      std::unique_ptr<BlobFileReader>* blob_file_reader,
      bool allow_footer_skip_retry = false);

  // Makes finalized checksum metadata available to direct-write fallback reads
  // whose pinned Version predates the blob-file addition. Any cached
  // footer-less reader is evicted as part of the same per-file transition.
  void RegisterBlobFileChecksum(const BlobFileOpenInfo& blob_file);

  // Stops retaining checksum metadata once no direct-write fallback can refer
  // to the file. Manifest-backed callers continue to provide it directly.
  void UnregisterBlobFileChecksum(uint64_t blob_file_number);

  // Inserts a freshly opened uncached reader unless another thread already
  // cached the same blob file.
  Status InsertBlobFileReader(
      uint64_t blob_file_number,
      std::unique_ptr<BlobFileReader>* blob_file_reader,
      CacheHandleGuard<BlobFileReader>* cached_blob_file_reader);

  // Installs a refresh-opened reader into the cache. If another thread has
  // already cached a reader opened on at least as large a file size, keep that
  // reader instead so a racing refresh cannot reintroduce an older active-file
  // view after the blob file grows.
  Status RefreshBlobFileReader(
      const BlobFileOpenInfo& blob_file,
      std::unique_ptr<BlobFileReader>* blob_file_reader,
      CacheHandleGuard<BlobFileReader>* cached_blob_file_reader);

  // Called when a blob file is obsolete to ensure it is removed from the cache
  // to avoid effectively leaking the open file and assicated memory
  void Evict(uint64_t blob_file_number);

  uint32_t GetColumnFamilyId() const { return column_family_id_; }

  // Serializes final stable-cache admission for one logical blob identity.
  // The physical read stays outside this lock; callers recheck the cache while
  // holding it so conflicting fingerprints cannot blindly replace each other.
  port::Mutex& GetStableCacheAdmissionMutex(const Slice& stable_key) {
    return stable_cache_admission_mutex_.Get(stable_key);
  }

  // Used to identify cache entries for blob files (not normally useful)
  static const Cache::CacheItemHelper* GetHelper() {
    return CacheInterface::GetBasicHelper();
  }

 private:
  Status GetBlobFileReaderImpl(
      const ReadOptions& read_options, const BlobFileOpenInfo& blob_file,
      bool allow_footer_skip_retry, uint64_t expected_relocation_file_size,
      uint32_t expected_directory_digest,
      const std::string& current_db_session_id,
      CompressionManager* compression_manager,
      uint8_t block_protection_bytes_per_key,
      CacheHandleGuard<BlobFileReader>* blob_file_reader);

  Status OpenBlobFileReader(const ReadOptions& read_options,
                            const BlobFileOpenInfo& blob_file,
                            bool allow_footer_skip_retry,
                            uint64_t expected_relocation_file_size,
                            uint32_t expected_directory_digest,
                            const std::string& current_db_session_id,
                            CompressionManager* compression_manager,
                            uint8_t block_protection_bytes_per_key,
                            std::unique_ptr<BlobFileReader>* blob_file_reader);

  using CacheInterface =
      BasicTypedCacheInterface<BlobFileReader, CacheEntryRole::kMisc>;
  using TypedHandle = CacheInterface::TypedHandle;
  CacheInterface cache_;
  // Note: mutex_ below is used to guard against multiple threads racing to open
  // the same file.
  Striped<CacheAlignedWrapper<port::Mutex>> mutex_;
  Striped<CacheAlignedWrapper<port::Mutex>> stable_cache_admission_mutex_;
  const ImmutableOptions* immutable_options_;
  const FileOptions* file_options_;
  uint32_t column_family_id_;
  HistogramImpl* blob_file_read_hist_;
  std::shared_ptr<IOTracer> io_tracer_;
  std::unique_ptr<BlockBasedTableOptions> relocation_file_table_options_;
  std::once_flag relocation_factory_once_;
  std::unique_ptr<TableFactory> relocation_factory_;

  struct BlobFileChecksum {
    std::string value;
    std::string function_name;
  };
  std::unordered_map<uint64_t, BlobFileChecksum> blob_file_checksums_;
  mutable port::Mutex blob_file_checksums_mutex_;

  static constexpr size_t kNumberOfMutexStripes = 1 << 7;
};

}  // namespace ROCKSDB_NAMESPACE
