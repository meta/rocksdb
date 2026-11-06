//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#pragma once
#include <cstdint>
#include <string>
#include <vector>

#include "cache/typed_cache.h"
#include "db/dbformat.h"
#include "db/range_del_aggregator.h"
#include "options/cf_options.h"
#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/table_reader.h"
#include "trace_replay/block_cache_tracer.h"
#include "util/coro_utils.h"

namespace ROCKSDB_NAMESPACE {

class Env;
class Arena;
struct FileDescriptor;
class GetContext;
class HistogramImpl;
class BlobSource;

struct TableCacheOpenOptions {
  // Open a new TableReader owned by the returned iterator instead of reusing
  // the pinned reader or shared TableCache entry.
  bool open_ephemeral_table_reader = false;

  // Avoid inserting open-time metadata blocks into the shared block cache.
  bool avoid_shared_metadata_cache = false;

  // Disable loading/accessing filter blocks for this open/iterator.
  bool skip_filters = false;
};

// Manages caching for TableReader objects for a column family. The actual
// cache is allocated separately and passed to the constructor. TableCache
// wraps around the underlying SST file readers by providing Get(),
// MultiGet() and NewIterator() methods that hide the instantiation,
// caching and access to the TableReader. The main purpose of this is
// performance - by caching the TableReader, it avoids unnecessary file opens
// and object allocation and instantiation. One exception is compaction, where
// a new TableReader may be instantiated - see NewIterator() comments
//
// Another service provided by TableCache is managing the row cache - if the
// DB is configured with a row cache, and the lookup key is present in the row
// cache, lookup is very fast. The row cache is obtained from
// ioptions.row_cache
class TableCache {
 public:
  TableCache(const ImmutableOptions& ioptions,
             const FileOptions* storage_options, Cache* cache,
             BlockCacheTracer* const block_cache_tracer,
             const std::shared_ptr<IOTracer>& io_tracer,
             const std::string& db_session_id, bool fast_sst_open = false,
             std::string column_family_name = "");
  ~TableCache();

  // Sets the BlobSource used to route same-file ("embedded") blob reads of SSTs
  // opened through this cache via the blob value cache + BLOB_DB_* statistics.
  // Both the TableCache and the BlobSource are owned by the same
  // ColumnFamilyData and share its lifetime, so the raw pointer is safe.
  // Called once at CFD setup, after the BlobSource is constructed. Left unset
  // (nullptr) in non-DB contexts (e.g. repair), where embedded reads fall back
  // to a direct (uncached) read.
  void SetBlobSource(BlobSource* blob_source) { blob_source_ = blob_source; }

  // Cache interface for table cache
  using CacheInterface =
      BasicTypedCacheInterface<TableReader, CacheEntryRole::kMisc>;
  using TypedHandle = CacheInterface::TypedHandle;

  // Cache interface for row cache
  using RowCacheInterface =
      BasicTypedCacheInterface<std::string, CacheEntryRole::kMisc>;
  using RowHandle = RowCacheInterface::TypedHandle;

  // Return an iterator for the specified file number (the corresponding
  // file length must be exactly "file_size" bytes).  If "table_reader_ptr"
  // is non-nullptr, also sets "*table_reader_ptr" to point to the Table object
  // underlying the returned iterator, or nullptr if no Table object underlies
  // the returned iterator.  The returned "*table_reader_ptr" object is owned
  // by the cache and should not be deleted, and is valid for as long as the
  // returned iterator is live. `table_reader_ptr` must be nullptr when
  // `open_options.open_ephemeral_table_reader` is set because that reader is
  // owned by iterator cleanup rather than by the cache.
  // If !options.ignore_range_deletions, and range_del_iter is non-nullptr,
  // then range_del_iter is set to a TruncatedRangeDelIterator for range
  // tombstones in the SST file corresponding to the specified file number. The
  // upper/lower bounds for the TruncatedRangeDelIterator are set to the SST
  // file's boundary.
  // @param options Must outlive the returned iterator.
  // @param range_del_agg If non-nullptr, adds range deletions to the
  //    aggregator. If an error occurs, returns it in a NewErrorInternalIterator
  // @param caller Identifies the high-level caller for table-reader behavior
  //               such as compaction preparation.
  // @param skip_filters Disables loading/accessing the filter block
  // @param level The level this table is at, -1 for "not set / don't know"
  // @param range_del_read_seqno If non-nullptr, will be used to create
  // *range_del_iter.
  // @param open_options Optional table-open policy overrides. The caller must
  //                       not combine `open_ephemeral_table_reader` with
  //                       `ReadOptions::read_tier = kBlockCacheTier` (no_io);
  //                       opening an ephemeral reader inherently requires I/O.
  InternalIterator* NewIterator(
      const ReadOptions& options, const FileOptions& toptions,
      const InternalKeyComparator& internal_comparator,
      const FileMetaData& file_meta, RangeDelAggregator* range_del_agg,
      const MutableCFOptions& mutable_cf_options,
      TableReader** table_reader_ptr, HistogramImpl* file_read_hist,
      TableReaderCaller caller, Arena* arena, bool skip_filters, int level,
      size_t max_file_size_for_l0_meta_pin,
      const InternalKey* smallest_compaction_key,
      const InternalKey* largest_compaction_key, bool allow_unprepared_value,
      const SequenceNumber* range_del_read_seqno = nullptr,
      std::unique_ptr<TruncatedRangeDelIterator>* range_del_iter = nullptr,
      bool maybe_pin_table_handle = false,
      // If non-null, and the table reader is newly opened (not cached),
      // retrieves file open metadata via GetFileOpenMetadata().
      std::string* file_open_metadata = nullptr,
      const TableCacheOpenOptions& open_options = TableCacheOpenOptions());

  // If a seek to internal key "k" in specified file finds an entry,
  // call get_context->SaveValue() repeatedly until
  // it returns false. As a side effect, it will insert the TableReader
  // into the cache and potentially evict another entry
  // @param get_context Context for get operation. The result of the lookup
  //                    can be retrieved by calling get_context->State()
  // @param file_read_hist If non-nullptr, the file reader statistics are
  //                       recorded
  // @param skip_filters Disables loading/accessing the filter block
  // @param level The level this table is at, -1 for "not set / don't know"
  DECLARE_SYNC_AND_ASYNC(Status, Get, const ReadOptions& options,
                         const InternalKeyComparator& internal_comparator,
                         const FileMetaData& file_meta, const Slice& k,
                         GetContext* get_context,
                         const MutableCFOptions& mutable_cf_options,
                         HistogramImpl* file_read_hist = nullptr,
                         bool skip_filters = false, int level = -1,
                         size_t max_file_size_for_l0_meta_pin = 0);

  // Return the range delete tombstone iterator of the file specified by
  // `file_meta`.
  Status GetRangeTombstoneIterator(
      const ReadOptions& options,
      const InternalKeyComparator& internal_comparator,
      const FileMetaData& file_meta, const MutableCFOptions& mutable_cf_options,
      std::unique_ptr<FragmentedRangeTombstoneIterator>* out_iter);

  // Call table reader's MultiGetFilter to use the bloom filter to filter out
  // keys. Returns Status::NotSupported() if row cache needs to be checked.
  // If the table cache is looked up to get the table reader, the cache handle
  // is returned in table_handle. This handle should be passed back to
  // MultiGet() so it can be released.
  Status MultiGetFilter(const ReadOptions& options,
                        const InternalKeyComparator& internal_comparator,
                        const FileMetaData& file_meta,
                        const MutableCFOptions& mutable_cf_options,
                        HistogramImpl* file_read_hist, int level,
                        MultiGetContext::Range* mget_range,
                        TypedHandle** table_handle);

  // If a seek to internal key "k" in specified file finds an entry,
  // call get_context->SaveValue() repeatedly until
  // it returns false. As a side effect, it will insert the TableReader
  // into the cache and potentially evict another entry
  // @param mget_range Pointer to the structure describing a batch of keys to
  //                   be looked up in this table file. The result is stored
  //                   in the embedded GetContext
  // @param skip_filters Disables loading/accessing the filter block
  // @param level The level this table is at, -1 for "not set / don't know"
  DECLARE_SYNC_AND_ASYNC(Status, MultiGet, const ReadOptions& options,
                         const InternalKeyComparator& internal_comparator,
                         const FileMetaData& file_meta,
                         const MultiGetContext::Range* mget_range,
                         const MutableCFOptions& mutable_cf_options,
                         HistogramImpl* file_read_hist = nullptr,
                         bool skip_filters = false,
                         bool skip_range_deletions = false, int level = -1,
                         TypedHandle* table_handle = nullptr);

  // Evict any entry for the specified file number. ReleaseObsolete() is
  // preferred for cleaning up from obsolete files.
  static void Evict(Cache* cache, uint64_t file_number);

  // Handles releasing, erasing, etc. of what should be the last reference
  // to an obsolete file. `handle` may be nullptr if no prior handle is known.
  static void ReleaseObsolete(Cache* cache, uint64_t file_number,
                              Cache::Handle* handle,
                              uint32_t uncache_aggressiveness);

  // Return handle to an existing cache entry if there is one
  static Cache::Handle* Lookup(Cache* cache, uint64_t file_number);

  // Look up the TableReader for the given file in the cache, or open the file
  // and create a new TableReader if not cached. On success, sets *table_reader
  // to point to the TableReader (owned by the cache) and *handle to the cache
  // handle (caller must release via cache_.Release() unless pin_table_handle is
  // true). If the table reader is already pinned on file_meta, returns it
  // directly without a cache lookup.
  //
  // @param no_io If true, returns Status::Incomplete() when the table is not
  //              already in cache rather than reading from disk.
  // @param skip_filters Disables loading/accessing the filter block.
  // @param level The LSM level of this table, -1 if not specified.
  // @param pin_table_handle If true, pins the table reader on file_meta so
  //              future lookups bypass the cache. *handle is set to nullptr
  //              on return in this case.
  // @param fresh_table_reader_owner If non-null, FindTable always opens a new
  //              TableReader (skipping the pinned-reader fast path and the
  //              shared cache) and writes ownership into this unique_ptr.
  //              `*handle` will be nullptr on return and `*table_reader` will
  //              point to the freshly allocated reader. Callers that pass this
  //              are responsible for keeping the unique_ptr alive for the
  //              lifetime of any iterators built on top.
  // @param open_options Optional table-open policy overrides (e.g.
  //              avoid_shared_metadata_cache, skip_filters).
  //              `open_options.open_ephemeral_table_reader` must be set if and
  //              only if `fresh_table_reader_owner` is non-null.
  Status FindTable(
      const ReadOptions& ro, const FileOptions& toptions,
      const InternalKeyComparator& internal_comparator,
      const FileMetaData& file_meta, TypedHandle**,
      const MutableCFOptions& mutable_cf_options, TableReader** table_reader,
      const bool no_io = false, HistogramImpl* file_read_hist = nullptr,
      bool skip_filters = false, int level = -1,
      bool prefetch_index_and_filter_in_cache = true,
      size_t max_file_size_for_l0_meta_pin = 0,
      Temperature file_temperature = Temperature::kUnknown,
      bool pin_table_handle = false, std::string* file_open_metadata = nullptr,
      std::unique_ptr<TableReader>* fresh_table_reader_owner = nullptr,
      const TableCacheOpenOptions& open_options = TableCacheOpenOptions());

  // Get the table properties of a given table.
  // @no_io: indicates if we should load table to the cache if it is not present
  //         in table cache yet.
  // @level, @file_read_hist, @prefetch_index_and_filter_in_cache: as for
  //         FindTable(), applied if the table is opened here. Pass -1 and
  //         nullptr if the table's level is unknown.
  // @returns: `properties` will be reset on success. Please note that we will
  //            return Status::Incomplete() if table is not present in cache and
  //            we set `no_io` to be true.
  Status GetTableProperties(const FileOptions& toptions,
                            const ReadOptions& read_options,
                            const InternalKeyComparator& internal_comparator,
                            const FileMetaData& file_meta,
                            std::shared_ptr<const TableProperties>* properties,
                            const MutableCFOptions& mutable_cf_options,
                            bool no_io, int level,
                            HistogramImpl* file_read_hist,
                            bool prefetch_index_and_filter_in_cache);

  Status ApproximateKeyAnchors(const ReadOptions& ro,
                               const InternalKeyComparator& internal_comparator,
                               const FileMetaData& file_meta,
                               const MutableCFOptions& mutable_cf_options,
                               std::vector<TableReader::Anchor>& anchors);

  // Return total memory usage of the table reader of the file.
  // 0 if table reader of the file is not loaded.
  size_t GetMemoryUsageByTableReader(
      const FileOptions& toptions, const ReadOptions& read_options,
      const InternalKeyComparator& internal_comparator,
      const FileMetaData& file_meta,
      const MutableCFOptions& mutable_cf_options);

  // Returns approximated offset of a key in a file represented by fd.
  uint64_t ApproximateOffsetOf(const ReadOptions& read_options,
                               const Slice& key, const FileMetaData& file_meta,
                               TableReaderCaller caller,
                               const InternalKeyComparator& internal_comparator,
                               const MutableCFOptions& mutable_cf_options);

  // Returns approximated data size between start and end keys in a file
  // represented by fd (the start key must not be greater than the end key).
  uint64_t ApproximateSize(const ReadOptions& read_options, const Slice& start,
                           const Slice& end, const FileMetaData& file_meta,
                           TableReaderCaller caller,
                           const InternalKeyComparator& internal_comparator,
                           const MutableCFOptions& mutable_cf_options);

  CacheInterface& get_cache() { return cache_; }

  const FileOptions& file_options() const { return file_options_; }

  // Capacity of the backing Cache that indicates infinite TableCache capacity.
  // For example when max_open_files is -1 we set the backing Cache to this.
  static const int kInfiniteCapacity = 0x400000;

  // The tables opened with this TableCache will be immortal, i.e., their
  // lifetime is as long as that of the DB.
  void SetTablesAreImmortal() {
    if (cache_.get()->GetCapacity() >= kInfiniteCapacity) {
      immortal_tables_ = true;
    }
  }

  // Re-evaluates should_pin_table_handles_ from the current cache capacity.
  // Must be called after the underlying cache capacity changes (e.g. via
  // SetDBOptions changing max_open_files).
  void UpdateShouldPinTableHandles() {
    should_pin_table_handles_ =
        cache_.get()->GetCapacity() >= kInfiniteCapacity;
  }

  void SetFastSstOpen(bool enabled) {
    fast_sst_open_.store(enabled, std::memory_order_relaxed);
  }

 private:
  // Build a table reader
  Status GetTableReader(const ReadOptions& ro, const FileOptions& file_options,
                        const InternalKeyComparator& internal_comparator,
                        const FileMetaData& file_meta, bool sequential_mode,
                        HistogramImpl* file_read_hist,
                        std::unique_ptr<TableReader>* table_reader,
                        const MutableCFOptions& mutable_cf_options,
                        bool skip_filters = false, int level = -1,
                        bool prefetch_index_and_filter_in_cache = true,
                        size_t max_file_size_for_l0_meta_pin = 0,
                        Temperature file_temperature = Temperature::kUnknown,
                        std::string* file_open_metadata = nullptr,
                        bool avoid_shared_metadata_cache = false);

  // Update the max_covering_tombstone_seq in the GetContext for each key based
  // on the range deletions in the table
  void UpdateRangeTombstoneSeqnums(const ReadOptions& options, TableReader* t,
                                   MultiGetContext::Range& table_range);

  // Create a key prefix for looking up the row cache. The prefix is of the
  // format row_cache_id + fd_number + seq_no. Later, the user key can be
  // appended to form the full key
  // Return the sequence number that determines the visibility of row_cache_key
  uint64_t CreateRowCacheKeyPrefix(const ReadOptions& options,
                                   const FileDescriptor& fd,
                                   const Slice& internal_key,
                                   GetContext* get_context,
                                   IterKey& row_cache_key);

  // Helper function to lookup the row cache for a key. It appends the
  // user key to row_cache_key at offset prefix_size
  bool GetFromRowCache(const Slice& user_key, IterKey& row_cache_key,
                       size_t prefix_size, GetContext* get_context,
                       Status* read_status,
                       SequenceNumber seq_no = kMaxSequenceNumber);

  const ImmutableOptions& ioptions_;
  const FileOptions& file_options_;
  CacheInterface cache_;
  std::string row_cache_id_;
  bool immortal_tables_;
  bool should_pin_table_handles_;
  std::atomic<bool> fast_sst_open_;
  BlockCacheTracer* const block_cache_tracer_;
  Striped<CacheAlignedWrapper<port::Mutex>> loader_mutex_;
  std::shared_ptr<IOTracer> io_tracer_;
  std::string db_session_id_;
  std::string column_family_name_;
  // Owned by the same ColumnFamilyData; see SetBlobSource(). nullptr in non-DB
  // contexts (e.g. repair).
  BlobSource* blob_source_ = nullptr;
};

}  // namespace ROCKSDB_NAMESPACE
