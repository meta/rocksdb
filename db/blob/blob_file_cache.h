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
#include "util/hash_containers.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

class Cache;
struct ImmutableOptions;
struct FileOptions;
class HistogramImpl;
class Status;
class Slice;
class IOTracer;
struct MutableCFOptions;
class TableFactory;

class BlobFileCache {
 public:
  // The base signature. It passes no DB session id, so the cache opens normal
  // blob files only; a relocation file read returns NotSupported. Because such
  // a cache opens no relocation file, it needs no block cache tracer.
  BlobFileCache(Cache* cache, const ImmutableOptions* immutable_options,
                const FileOptions* file_options, uint32_t column_family_id,
                HistogramImpl* blob_file_read_hist,
                const std::shared_ptr<IOTracer>& io_tracer);

  // A cache that can also open relocation files. `db_session_id` is passed to
  // the relocation readers it opens; an empty one makes relocation file reads
  // return NotSupported. `block_cache_tracer` is the DB's block cache tracer
  // (the same one the table cache gets), so a block cache trace includes the
  // block reads of relocation files. It may be nullptr. The table options of
  // relocation files come from the column family options of each open (see
  // GetRelocationFileReader()).
  BlobFileCache(Cache* cache, const ImmutableOptions* immutable_options,
                const FileOptions* file_options, uint32_t column_family_id,
                HistogramImpl* blob_file_read_hist,
                const std::shared_ptr<IOTracer>& io_tracer,
                const std::string& db_session_id,
                BlockCacheTracer* block_cache_tracer);

  BlobFileCache(const BlobFileCache&) = delete;
  BlobFileCache& operator=(const BlobFileCache&) = delete;
  BlobFileCache(BlobFileCache&&) = delete;
  BlobFileCache& operator=(BlobFileCache&&) = delete;

  ~BlobFileCache();

  // Returns a cached reader for `blob_file`, opening and caching it on miss.
  // Checksum metadata is forwarded to the FileSystem through FileOptions when
  // the file is opened. If `allow_footer_skip_retry` is true, a
  // footer-validation corruption retries once without requiring a footer.
  // If the reader is not cached and `read_options.read_tier` is
  // kBlockCacheTier, returns Incomplete, because opening the file needs disk
  // I/O. If the cached reader of this file number is a relocation file reader,
  // returns Corruption.
  Status GetBlobFileReader(const ReadOptions& read_options,
                           const BlobFileOpenInfo& blob_file,
                           CacheHandleGuard<BlobFileReader>* blob_file_reader,
                           bool allow_footer_skip_retry = false);

  // Like GetBlobFileReader, but for relocation file `file` (see
  // db/blob/blob_relocation_file.h). The file size, tail size and directory
  // digest in `file` are the expected identity of the file. The code that
  // installs the file recorded them, and BlobRelocationFileReader::Open checks
  // the file against them.
  // - Cache hit: if the cached reader is not a relocation file reader of this
  //   column family, or its identity differs from `file`, returns Corruption.
  // - Cache miss: returns Incomplete for a kBlockCacheTier read, like
  //   GetBlobFileReader. If an open of this file failed less than 1 second ago
  //   (kFailedRelocationOpenMicros) with an error from the file itself
  //   (Corruption, NotFound or IOError(PathNotFound)), returns that error again
  //   without opening the file (see failed_relocation_opens_). Otherwise opens
  //   the file. The table options, compression manager and block protection
  //   of `mutable_cf_options` configure the table reader, so a SetOptions()
  //   change applies to relocation files opened after it. Returns
  //   NotSupported if the cache has no DB session id or the table factory of
  //   `mutable_cf_options` is not block-based.
  Status GetRelocationFileReader(
      const ReadOptions& read_options, const BlobPhysicalFile& file,
      const MutableCFOptions& mutable_cf_options,
      CacheHandleGuard<BlobFileReader>* blob_file_reader);

  // Indirect blob GC opens its relocation inputs with this function, so that a
  // reader opened with GC's options (no pinned location blocks) never serves
  // user reads. It opens a reader of relocation file `file` and does not put it
  // into the cache. It does not use or update the remembered failed opens
  // (failed_relocation_opens_). BlobRelocationFileReader::Open checks the file
  // against the identity in `file`. Returns NotSupported if the cache has no
  // DB session id or the column family's table factory is not block-based.
  Status OpenRelocationFileReaderUncached(
      const ReadOptions& read_options, const BlobPhysicalFile& file,
      const MutableCFOptions& mutable_cf_options,
      std::unique_ptr<BlobFileReader>* blob_file_reader);

  // The base signature. It passes prefer_direct_reads = false (the file is
  // opened as configured).
  Status OpenBlobFileReaderUncached(
      const ReadOptions& read_options, const BlobFileOpenInfo& blob_file,
      std::unique_ptr<BlobFileReader>* blob_file_reader,
      bool allow_footer_skip_retry = false);

  // Opens a blob file reader without inserting it into the cache. With
  // `prefer_direct_reads`, the file is opened for direct reads when mmap reads
  // are off. If that open fails (for example, the file system does not support
  // direct reads), the file is opened as configured.
  Status OpenBlobFileReaderUncached(
      const ReadOptions& read_options, const BlobFileOpenInfo& blob_file,
      std::unique_ptr<BlobFileReader>* blob_file_reader,
      bool allow_footer_skip_retry, bool prefer_direct_reads);

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
  //
  // Only for normal blob files: it takes the stripe mutex of a normal blob
  // file (mutex_), not relocation_mutex_. PurgeObsoleteFiles() evicts the
  // reader of a relocation file with TableCache::Evict(), which takes no
  // stripe mutex (this cache shares its Cache with the table cache).
  void Evict(uint64_t blob_file_number);

  // Removes the remembered failed open of relocation file `file_number`, if
  // any (see failed_relocation_opens_). The deleter of a Version-owned
  // relocation file calls it, with or without the DB mutex (see
  // VersionSet::FreeBlobRelocationState()). It takes only
  // failed_relocation_opens_mutex_. PurgeObsoleteFiles() evicts the cached
  // reader later (see Evict()).
  void ForgetFailedRelocationOpen(uint64_t file_number);

  // Used to identify cache entries for blob files (not normally useful)
  static const Cache::CacheItemHelper* GetHelper() {
    return CacheInterface::GetBasicHelper();
  }

 private:
  // The kind of a file in this cache. It picks the stripe mutex set (see
  // relocation_mutex_).
  enum class FileKind { kBlobFile, kRelocationFile };

  // The stripe mutex of the file with cache key `key` and kind `kind`.
  port::Mutex* GetStripeMutex(FileKind kind, const Slice& key);

  // Returns in `*factory` the table factory to open a relocation file with:
  // MakeBlobRelocationTableOptions() of the block-based table options in
  // `mutable_cf_options.table_factory`. It reuses the last factory while the
  // column family keeps the same table factory (see relocation_factory_).
  // Returns NotSupported if the table factory is not block-based.
  Status GetRelocationTableFactory(
      const MutableCFOptions& mutable_cf_options,
      std::shared_ptr<const TableFactory>* factory);

  // Shared by GetBlobFileReader and GetRelocationFileReader. On a cache hit,
  // returns the cached reader of `blob_file_number` if validate(reader) returns
  // OK, and the validate status otherwise. On a miss, returns Incomplete for a
  // kBlockCacheTier read. Otherwise it calls open(&reader) under the stripe
  // mutex of the file and caches the result. For a relocation file, a recent
  // failed open that came from the file itself is returned again without
  // opening the file (see failed_relocation_opens_).
  template <typename ValidateFn, typename OpenFn>
  Status GetBlobFileReaderImpl(
      const ReadOptions& read_options, uint64_t blob_file_number, FileKind kind,
      const ValidateFn& validate, const OpenFn& open,
      CacheHandleGuard<BlobFileReader>* blob_file_reader);

  // Returns the FileOptions to open `blob_file` with, including its checksum
  // metadata.
  FileOptions GetFileOptions(const BlobFileOpenInfo& blob_file);

  Status OpenBlobFileReader(const ReadOptions& read_options,
                            const BlobFileOpenInfo& blob_file,
                            bool allow_footer_skip_retry,
                            std::unique_ptr<BlobFileReader>* blob_file_reader);
  Status OpenRelocationFileReader(
      const ReadOptions& read_options, const BlobPhysicalFile& file,
      const MutableCFOptions& mutable_cf_options,
      std::unique_ptr<BlobFileReader>* blob_file_reader);

  using CacheInterface =
      BasicTypedCacheInterface<BlobFileReader, CacheEntryRole::kMisc>;
  using TypedHandle = CacheInterface::TypedHandle;
  CacheInterface cache_;
  // Note: mutex_ below is used to guard against multiple threads racing to open
  // the same file.
  Striped<CacheAlignedWrapper<port::Mutex>> mutex_;
  // The stripe mutexes of relocation files, separate from mutex_ (which normal
  // blob files use). A relocation file open can hold its stripe mutex for a
  // long time, because with pin_blob_relocation_location_blocks it reads all
  // location blocks. Evict() of a normal blob file runs under the DB mutex
  // (from the file's deleter), so it must never wait for such an open.
  // kNumberOfRelocationMutexStripes (32) is enough because relocation opens
  // are rare (a reader stays open after the first open), and a collision of
  // two relocation opens only delays one read. Each stripe takes one cache
  // line (CACHE_LINE_SIZE, 64 bytes on x86 and 128 bytes on aarch64). So the
  // 32 stripes use 2 KB per BlobFileCache on x86, against 8 KB for the 128
  // stripes of mutex_. Every column family has a BlobFileCache, so the stripes
  // are built on the first relocation open (relocation_mutex_once_). A column
  // family without relocation files does not pay for them.
  std::once_flag relocation_mutex_once_;
  std::unique_ptr<Striped<CacheAlignedWrapper<port::Mutex>>> relocation_mutex_;
  // Recent failed relocation file opens, keyed by file number. Many reads can
  // be routed to one bad relocation file. With this map, they get the same
  // error without opening the file again until the entry expires after
  // kFailedRelocationOpenMicros (1 second). The value is an estimate, not
  // measured: it is long enough to absorb a burst of reads routed to the same
  // bad file, and short enough that a repaired or replaced file is tried again
  // soon.
  // - Kept: errors that come from the file itself. These are Corruption
  //   (including an identity or directory digest mismatch), NotFound and
  //   IOError(PathNotFound).
  // - Not kept: other IOErrors, TimedOut, Busy and Aborted. They can be
  //   short-lived or caused by the deadline of one reader.
  // ForgetFailedRelocationOpen() also removes the entry. Guarded by
  // failed_relocation_opens_mutex_.
  static constexpr uint64_t kFailedRelocationOpenMicros = 1000 * 1000;
  struct FailedRelocationOpen {
    Status status;
    uint64_t expires_micros = 0;
  };
  port::Mutex failed_relocation_opens_mutex_;
  UnorderedMap<uint64_t, FailedRelocationOpen> failed_relocation_opens_;
  const ImmutableOptions* immutable_options_;
  const FileOptions* file_options_;
  uint32_t column_family_id_;
  HistogramImpl* blob_file_read_hist_;
  std::shared_ptr<IOTracer> io_tracer_;
  // The DB session id. Each relocation table reader gets it as
  // TableReaderOptions::cur_db_session_id.
  const std::string db_session_id_;
  // Passed to each relocation table reader as
  // TableReaderOptions::block_cache_tracer. May be nullptr.
  BlockCacheTracer* const block_cache_tracer_;
  // Table factory for relocation files, built on the first relocation open
  // from the column family's table factory (relocation_factory_source_), and
  // built again when SetOptions() gives the column family a new table factory.
  // A relocation reader does not keep the factory: its table reader keeps its
  // own copy of the table options. Holding relocation_factory_source_ keeps
  // its address from being reused by another factory. After SetOptions(), it
  // keeps the old factory (and its block cache) alive until the next
  // relocation open. Both are guarded by relocation_factory_mutex_.
  port::Mutex relocation_factory_mutex_;
  std::shared_ptr<TableFactory> relocation_factory_source_;
  std::shared_ptr<const TableFactory> relocation_factory_;

  struct BlobFileChecksum {
    std::string value;
    std::string function_name;
  };
  std::unordered_map<uint64_t, BlobFileChecksum> blob_file_checksums_;
  mutable port::Mutex blob_file_checksums_mutex_;

  static constexpr size_t kNumberOfMutexStripes = 1 << 7;
  static constexpr size_t kNumberOfRelocationMutexStripes = 32;
};

}  // namespace ROCKSDB_NAMESPACE
