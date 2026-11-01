//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "cache/cache_entry_roles.h"
#include "cache/cache_key.h"
#include "cache/cache_reservation_manager.h"
#include "db/blob/same_file_blob_reader.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/seqno_to_time_mapping.h"
#include "file/filename.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table_properties.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_cache.h"
#include "table/block_based/block_type.h"
#include "table/block_based/cachable_entry.h"
#include "table/block_based/filter_block.h"
#include "table/block_based/uncompression_dict_reader.h"
#include "table/embedded_blob_sst.h"
#include "table/format.h"
#include "table/persistent_cache_options.h"
#include "table/table_properties_internal.h"
#include "table/table_reader.h"
#include "table/two_level_iterator.h"
#include "trace_replay/block_cache_tracer.h"
#include "util/aligned_buffer.h"
#include "util/atomic.h"
#include "util/cast_util.h"
#include "util/coro_utils.h"
#include "util/hash_containers.h"

namespace ROCKSDB_NAMESPACE {

class Cache;
class FilterBlockReader;
class FullFilterBlockReader;
class Footer;
class InternalKeyComparator;
class Iterator;
class FSRandomAccessFile;
class TableCache;
class TableReader;
class WritableFile;
class BlobIndex;
class BlobSource;
struct BlockBasedTableOptions;
struct EnvOptions;
struct ReadOptions;
class GetContext;

using KVPairBlock = std::vector<std::pair<std::string, std::string>>;

// Calls the provider and checks that a successful allocation returned a lease
// satisfying RocksDB's size, alignment, data pointer, and cleanup contract.
Status AllocateReadScopedBlockBuffer(
    ReadScopedBlockBufferProvider& block_buffer_provider, size_t requested_size,
    size_t requested_alignment, ReadScopedBlockBufferProvider::Lease* lease);

// Allocates a read-scoped provider lease and exposes it as an AlignedBuffer
// external allocation. The AlignedBuffer owner keeps a cleanup reference so the
// provider allocation remains valid while direct I/O can write into it.
Status AllocateReadScopedAlignedBuffer(
    ReadScopedBlockBufferProvider& block_buffer_provider, size_t size,
    size_t alignment, ReadScopedBlockBufferProvider::Lease* lease,
    AlignedBuffer::ExternalAllocation* out);

// Returns an allocator whose direct-I/O allocation comes from the read-scoped
// provider. The caller must keep `lease` alive until the file reader
// synchronously invokes the allocator.
AlignedBuffer::Allocator MakeReadScopedAlignedBufferAllocator(
    ReadScopedBlockBufferProviderRef block_buffer_provider,
    ReadScopedBlockBufferProvider::Lease* lease);

// Resolves the read-scoped provider configured for this read. Returns
// std::nullopt when no provider is configured, or when mmap reads are enabled
// because mmap file reads can ignore RocksDB-provided scratch buffers.
ReadScopedBlockBufferProviderRef GetReadScopedBlockBufferProvider(
    const ReadOptions& ro, bool allow_mmap_reads);

// Centralizes the data-block cache decision for iterator and MultiScan paths.
// A read-scoped provider skips data-block cache lookup and insertion because it
// supplies alternate read-scope backing for supported data-block reads.
bool ShouldUseDataBlockCacheForIterator(
    const BlockBasedTableOptions& table_options, const ReadOptions& ro,
    bool allow_mmap_reads);

// Copies block bytes into RocksDB-owned heap storage and records that ownership
// in BlockContents. `src` may include a block trailer while `data_size` is the
// payload size.
Status CopyBufferToHeapBlockContents(Slice src, size_t data_size,
                                     MemoryAllocator* allocator,
                                     BlockContents* out_contents);

// Copies block bytes into read-scoped provider storage and records that
// cleanup in BlockContents. `src` may include a block trailer while
// `data_size` is the payload size.
Status CopyBufferToReadScopedBlockContents(
    Slice src, size_t data_size,
    ReadScopedBlockBufferProvider& block_buffer_provider,
    BlockContents* out_contents);

// Reader class for BlockBasedTable format.
// For the format of BlockBasedTable refer to
// https://github.com/meta/rocksdb/wiki/Rocksdb-BlockBasedTable-Format.
// This is the default table type. Data is chucked into fixed size blocks and
// each block in-turn stores entries. When storing data, we can compress and/or
// encode data efficiently within a block, which often results in a much smaller
// data size compared with the raw data size. As for the record retrieval, we'll
// first locate the block where target record may reside, then read the block to
// memory, and finally search that record within the block. Of course, to avoid
// frequent reads of the same block, we introduced the block cache to keep the
// loaded blocks in the memory.
class BlockBasedTable : public TableReader, public SameFileBlobReader {
 public:
  static const std::string kObsoleteFilterBlockPrefix;
  static const std::string kFullFilterBlockPrefix;
  static const std::string kPartitionedFilterBlockPrefix;

  // 1-byte compression type + 32-bit checksum
  static constexpr size_t kBlockTrailerSize = 5;

  // Attempt to open the table that is stored in bytes [0..file_size)
  // of "file", and read the metadata entries necessary to allow
  // retrieving data from the table.
  //
  // If successful, returns ok and sets "*table_reader" to the newly opened
  // table.  The client should delete "*table_reader" when no longer needed.
  // If there was an error while initializing the table, sets "*table_reader"
  // to nullptr and returns a non-ok status.
  //
  // @param file must remain live while this Table is in use.
  // @param prefetch_index_and_filter_in_cache can be used to disable
  // prefetching of
  //    index and filter blocks into block cache at startup
  // @param skip_filters Disables loading/accessing the filter block. Overrides
  //    prefetch_index_and_filter_in_cache, so filter will be skipped if both
  //    are set.
  // @param force_direct_prefetch if true, always prefetching to RocksDB
  //    buffer, rather than calling RandomAccessFile::Prefetch().
  // @param avoid_shared_metadata_cache if true, open-time index/filter/
  //    dictionary reads must not insert into the shared block cache.
  static Status Open(
      const ReadOptions& ro, const ImmutableOptions& ioptions,
      const EnvOptions& env_options,
      const BlockBasedTableOptions& table_options,
      const InternalKeyComparator& internal_key_comparator,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      uint8_t block_protection_bytes_per_key,
      std::unique_ptr<TableReader>* table_reader, uint64_t tail_size,
      std::shared_ptr<CacheReservationManager> table_reader_cache_res_mgr =
          nullptr,
      const std::shared_ptr<const SliceTransform>& prefix_extractor = nullptr,
      UnownedPtr<CompressionManager> compression_manager = nullptr,
      bool prefetch_index_and_filter_in_cache = true, bool skip_filters = false,
      int level = -1, const bool immortal_table = false,
      const SequenceNumber largest_seqno = 0,
      bool force_direct_prefetch = false,
      TailPrefetchStats* tail_prefetch_stats = nullptr,
      BlockCacheTracer* const block_cache_tracer = nullptr,
      size_t max_file_size_for_l0_meta_pin = 0,
      const std::string& cur_db_session_id = "", uint64_t cur_file_num = 0,
      UniqueId64x2 expected_unique_id = {},
      const bool user_defined_timestamps_persisted = true,
      bool avoid_shared_metadata_cache = false,
      BlobSource* blob_source = nullptr);

  bool PrefixRangeMayMatch(const Slice& internal_key,
                           const ReadOptions& read_options,
                           const SliceTransform* options_prefix_extractor,
                           const bool need_upper_bound_check,
                           BlockCacheLookupContext* lookup_context,
                           bool* filter_checked) const;

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  // @param read_options Must outlive the returned iterator.
  // @param skip_filters Disables loading/accessing the filter block
  // compaction_readahead_size: its value will only be used if caller =
  // kCompaction.
  InternalIterator* NewIterator(const ReadOptions&,
                                const SliceTransform* prefix_extractor,
                                Arena* arena, bool skip_filters,
                                TableReaderCaller caller,
                                size_t compaction_readahead_size = 0,
                                bool allow_unprepared_value = false) override;

  FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      const ReadOptions& read_options) override;

  FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      SequenceNumber read_seqno, const Slice* timestamp) override;

  // @param skip_filters Disables loading/accessing the filter block
  DECLARE_SYNC_AND_ASYNC_OVERRIDE(Status, Get, const ReadOptions& readOptions,
                                  const Slice& key, GetContext* get_context,
                                  const SliceTransform* prefix_extractor,
                                  bool skip_filters = false);

  // Whether this SST contains embedded (same-file) blob records, as advertised
  // by the embedded blob stats property.
  bool HasEmbeddedBlobRecords() const;

  // Reads a single same-file blob record referenced by `blob_index`.
  Status ResolveEmbeddedBlob(const ReadOptions& read_options,
                             const BlobIndex& blob_index,
                             std::string* value) const;

  // Like ResolveEmbeddedBlob, but reads the payload into a heap buffer owned by
  // `value` (via a registered cleanup) and pins it, avoiding a copy. Use on the
  // Get()/MultiGet() path so the resolved value can be pinned into the output.
  Status ResolveEmbeddedBlobPinned(const ReadOptions& read_options,
                                   const BlobIndex& blob_index,
                                   PinnableSlice* value) const;

  // Like ResolveEmbeddedBlobPinned, but routes the read through the CFD's
  // BlobSource so the payload is served from / inserted into the blob value
  // cache and BLOB_DB_* statistics are recorded. BlobSource derives the cache
  // key from the SimpleGen2Blob format (the same offset scheme as the SST's
  // data blocks; see GetSimpleGen2BlobCacheKey), so embedded blob records stay
  // collision-free with data blocks even when the blob cache and block cache
  // are shared. Must only be called when rep_->blob_source_ is non-null.
  Status ResolveEmbeddedBlobCached(const ReadOptions& read_options,
                                   const BlobIndex& blob_index,
                                   PinnableSlice* value) const;

  // Range-read counterpart of ResolveEmbeddedBlobCached: resolve only
  // [range_offset, range_offset + range_length) of an uncompressed same-file
  // payload (skipping the trailer + checksum verification, and never inserting
  // into the blob cache). The caller must ensure range_offset + range_length <=
  // blob_index.size(). Requires a BlobSource (rep_->blob_source_ != nullptr):
  // byte-range embedded reads are only issued on the lazy read path, which
  // always has one, so there is no direct-pinned range variant.
  Status ResolveEmbeddedBlobRangeCached(const ReadOptions& read_options,
                                        const BlobIndex& blob_index,
                                        uint64_t range_offset,
                                        size_t range_length,
                                        PinnableSlice* value) const;

  // SameFileBlobReader: resolve a same-file blob reference (whole value or a
  // sub-range) for GetContext's zero-copy wide-column resolution on the
  // Get()/MultiGet() path and for the lazy read path. A whole-value read routes
  // through the blob cache (ResolveEmbeddedBlobCached) when a BlobSource is
  // wired, else a direct pinned read (ResolveEmbeddedBlobPinned); a sub-range
  // read routes through ResolveEmbeddedBlobRangeCached (requires a BlobSource).
  // See BlobVerifyPolicy / kWholeBlobLength for the range + verify semantics.
  Status GetSameFileBlob(const ReadOptions& read_options,
                         const BlobIndex& blob_index, uint64_t range_offset,
                         size_t range_length, BlobVerifyPolicy verify_policy,
                         PinnableSlice* value) const override;

  // Batched counterpart of GetSameFileBlob: coalesces the cache-miss reads of
  // multiple embedded (same-file) records in this SST into a single MultiRead
  // (one for whole records, one for sub-ranges). Force-verify reads
  // (kVerifyIfPresent) fall back to per-record GetSameFileBlob.
  void MultiGetSameFileBlob(const ReadOptions& read_options, size_t num_reads,
                            SameFileBlobReadRequest* reqs) const override;

  // If `value` is a same-file BlobIndex, materializes the referenced payload
  // and updates `resolved_internal_key` to the corresponding value type. Leaves
  // `resolved`. When `pinned_value` is non-null, a whole-value same-file blob
  // is pinned into it without a copy (and `*value_pinned` is set); otherwise
  // the value is built into `resolved_value`. Wide-column entities are always
  // built into `resolved_value`.
  //
  // When `skip_wide_column_entities` is true, wide-column entities are left
  // unresolved (returned raw, `resolved` stays false) so the Get()/MultiGet()
  // path can hand the raw entity to GetContext for zero-copy same-file column
  // resolution. The iterator path leaves it false to keep re-serializing.
  Status MaybeResolveEmbeddedValue(
      const ReadOptions& read_options, const Slice& internal_key,
      const Slice& value, std::string* resolved_internal_key,
      std::string* resolved_value, bool* resolved,
      PinnableSlice* pinned_value = nullptr, bool* value_pinned = nullptr,
      bool skip_wide_column_entities = false) const;

  // Reusable scratch for resolving embedded values across a Get()/MultiGet()
  // loop, so the hot loop performs no per-entry allocation. Construct one per
  // Get()/MultiGet() call and pass it to ResolveEmbeddedValueForGet().
  struct EmbeddedValueGetScratch {
    std::string key_buf;
    std::string value_buf;
    PinnableSlice pinned_value;
  };

  // Resolves the embedded blob / wide-column value for one Get()/MultiGet()
  // entry and selects the Cleanable to hand to GetContext::SaveValue.
  // `block_pinner` is the data block iterator's value pinner (or nullptr). On
  // return, *key_to_save / *value_to_save are the slices to save and
  // *value_pinner is the pinner to use: the zero-copy blob payload pinner (in
  // `scratch`), nullptr for a copied wide-column value, or `block_pinner` when
  // nothing needed resolving. Only call this when the table actually has
  // embedded blob records (see Rep::has_embedded_blobs); the common
  // non-embedded path should skip it entirely.
  //
  // When `defer_wide_column_entities` is true, a wide-column entity is returned
  // raw (not pre-resolved / re-serialized) so GetContext can resolve its
  // same-file blob columns zero-copy via an EmbeddedAwareBlobFetcher. Callers
  // must then pass this table as the SameFileBlobReader to
  // GetContext::SaveValue. Whole-value same-file blobs are still resolved here.
  Status ResolveEmbeddedValueForGet(const ReadOptions& read_options,
                                    const Slice& key, const Slice& value,
                                    EmbeddedValueGetScratch* scratch,
                                    Cleanable* block_pinner, Slice* key_to_save,
                                    Slice* value_to_save,
                                    Cleanable** value_pinner,
                                    bool defer_wide_column_entities) const;

  // Validates a same-file `blob_index` and returns the payload and full record
  // (payload + trailer) sizes. Shared by the ResolveEmbeddedBlob* variants.
  Status ValidateEmbeddedBlobIndex(const BlobIndex& blob_index,
                                   size_t* payload_size,
                                   size_t* record_size) const;

  Status MultiGetFilter(const ReadOptions& read_options,
                        const SliceTransform* prefix_extractor,
                        MultiGetRange* mget_range) override;

  DECLARE_SYNC_AND_ASYNC_OVERRIDE(void, MultiGet,
                                  const ReadOptions& readOptions,
                                  const MultiGetContext::Range* mget_range,
                                  const SliceTransform* prefix_extractor,
                                  bool skip_filters = false);

  // Pre-fetch the disk blocks that correspond to the key range specified by
  // (kbegin, kend). The call will return error status in the event of
  // IO or iteration error.
  Status Prefetch(const ReadOptions& read_options, const Slice* begin,
                  const Slice* end) override;

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file). The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  uint64_t ApproximateOffsetOf(const ReadOptions& read_options,
                               const Slice& key,
                               TableReaderCaller caller) override;

  // Given start and end keys, return the approximate data size in the file
  // between the keys. The returned value is in terms of file bytes, and so
  // includes effects like compression of the underlying data.
  // The start key must not be greater than the end key.
  uint64_t ApproximateSize(const ReadOptions& read_options, const Slice& start,
                           const Slice& end, TableReaderCaller caller) override;

  Status ApproximateKeyAnchors(const ReadOptions& read_options,
                               std::vector<Anchor>& anchors) override;

  bool EraseFromCache(const BlockHandle& handle) const;

  bool TEST_BlockInCache(const BlockHandle& handle) const;

  // Returns true if the block for the specified key is in cache.
  // REQUIRES: key is in this table && block cache enabled
  bool TEST_KeyInCache(const ReadOptions& options, const Slice& key);

  void TEST_GetDataBlockHandle(const ReadOptions& options, const Slice& key,
                               BlockHandle& handle);

  // Set up the table for Compaction. Might change some parameters with
  // posix_fadvise
  void SetupForCompaction() override;

  std::shared_ptr<const TableProperties> GetTableProperties() const override;

  const SeqnoToTimeMapping& GetSeqnoToTimeMapping() const;

  size_t ApproximateMemoryUsage() const override;

  // convert SST file to a human readable form
  Status DumpTable(WritableFile* out_file,
                   bool show_sequence_number_type = false) override;

  Status VerifyChecksum(const ReadOptions& readOptions,
                        TableReaderCaller caller,
                        bool meta_blocks_only = false) override;

  void MarkObsolete(uint32_t uncache_aggressiveness) override;

  ~BlockBasedTable() override;

  bool TEST_FilterBlockInCache() const;
  bool TEST_IndexBlockInCache() const;

  // IndexReader is the interface that provides the functionality for index
  // access.
  class IndexReader {
   public:
    virtual ~IndexReader() = default;

    // Create an iterator for index access. If iter is null, then a new object
    // is created on the heap, and the callee will have the ownership.
    // If a non-null iter is passed in, it may be used, and the returned value
    // is either the same as iter or a new on-heap object.
    // In the latter case the return value points to a different object then
    // iter, and the callee has the ownership of the returned object.
    //
    // Under all circumstances, the caller MUST use the returned iterator
    // for further operations. If the returned iterator != iter, then the
    // caller MUST ensure that iter stays in scope until the returned
    // iterator is destroyed.
    virtual InternalIteratorBase<IndexValue>* NewIterator(
        const ReadOptions& read_options, bool disable_prefix_seek,
        IndexBlockIter* iter, GetContext* get_context,
        BlockCacheLookupContext* lookup_context) = 0;

    // Report an approximation of how much memory has been used other than
    // memory that was allocated in block cache.
    virtual size_t ApproximateMemoryUsage() const = 0;
    // Cache the dependencies of the index reader (e.g. the partitions
    // of a partitioned index).
    virtual Status CacheDependencies(
        const ReadOptions& /*ro*/, bool /* pin */,
        FilePrefetchBuffer* /* tail_prefetch_buffer */) {
      return Status::OK();
    }
    virtual void EraseFromCacheBeforeDestruction(
        uint32_t /*uncache_aggressiveness*/) {}
  };

  class IndexReaderCommon;

  static void SetupBaseCacheKey(const TableProperties* properties,
                                const std::string& cur_db_session_id,
                                uint64_t cur_file_number,
                                OffsetableCacheKey* out_base_cache_key,
                                bool* out_is_stable = nullptr);

  static CacheKey GetCacheKey(const OffsetableCacheKey& base_cache_key,
                              const BlockHandle& handle);

  static void UpdateCacheInsertionMetrics(BlockType block_type,
                                          GetContext* get_context, size_t usage,
                                          bool redundant,
                                          Statistics* const statistics);

  Statistics* GetStatistics() const;
  bool IsLastLevel() const;

  // Get the size to read from storage for a BlockHandle. size_t because we
  // are about to load into memory.
  static inline size_t BlockSizeWithTrailer(const BlockHandle& handle) {
    return static_cast<size_t>(handle.size() + kBlockTrailerSize);
  }

  // It is the caller's responsibility to make sure that this is called with
  // block-based table serialized block contents, which contains the compression
  // byte in the trailer after `block_size`.
  static inline CompressionType GetBlockCompressionType(const char* block_data,
                                                        size_t block_size) {
    return static_cast<CompressionType>(block_data[block_size]);
  }
  static inline CompressionType GetBlockCompressionType(
      const BlockContents& contents) {
    assert(contents.has_trailer);
    return GetBlockCompressionType(contents.data.data(), contents.data.size());
  }

  // Retrieve all key value pairs from data blocks in the table.
  // The key retrieved are internal keys.
  Status GetKVPairsFromDataBlocks(const ReadOptions& read_options,
                                  std::vector<KVPairBlock>* kv_pair_blocks);

  // Look up the block cache for the specified block.
  // out_parsed_block is set to nullptr if the block is not found in the cache.
  template <typename TBlocklike>
  Status LookupAndPinBlocksInCache(
      const ReadOptions& ro, const BlockHandle& handle,
      CachableEntry<TBlocklike>* out_parsed_block) const;

  // Create the block given in `block_contents` and insert it into block cache.
  // `out_parsed_block` points to the inserted block if successful.
  template <typename TBlocklike>
  Status CreateAndPinBlockInCache(
      const ReadOptions& ro, const BlockHandle& handle,
      UnownedPtr<Decompressor> decomp, BlockContents* block_contents,
      CachableEntry<TBlocklike>* out_parsed_block) const;

  struct Rep;

  Rep* get_rep() { return rep_; }
  const Rep* get_rep() const { return rep_; }

  // input_iter: if it is not null, update this one and return it as Iterator
  DECLARE_SYNC_AND_ASYNC_TEMPLATE_CONST(
      template <typename TBlockIter>, TBlockIter*, NewDataBlockIterator,
      const ReadOptions& ro, const BlockHandle& block_handle,
      TBlockIter* input_iter, BlockType block_type, GetContext* get_context,
      BlockCacheLookupContext* lookup_context,
      FilePrefetchBuffer* prefetch_buffer, bool for_compaction, bool async_read,
      Status& s, bool use_block_cache_for_lookup);

  // input_iter: if it is not null, update this one and return it as Iterator
  DECLARE_SYNC_AND_ASYNC_TEMPLATE_CONST(template <typename TBlockIter>,
                                        TBlockIter*, NewDataBlockIterator,
                                        const ReadOptions& ro,
                                        CachableEntry<Block>& block,
                                        TBlockIter* input_iter, Status s);

  class PartitionedIndexIteratorState;

  template <typename TBlocklike>
  friend class FilterBlockReaderCommon;

  friend class PartitionIndexReader;

  friend class UncompressionDictReader;

 protected:
  Rep* rep_;
  explicit BlockBasedTable(Rep* rep, BlockCacheTracer* const block_cache_tracer)
      : rep_(rep), block_cache_tracer_(block_cache_tracer) {}
  // No copying allowed
  explicit BlockBasedTable(const TableReader&) = delete;
  void operator=(const TableReader&) = delete;

 private:
  friend class MockedBlockBasedTable;
  friend class BlockBasedTableReaderTestVerifyChecksum_ChecksumMismatch_Test;
  BlockCacheTracer* const block_cache_tracer_;

  void UpdateCacheHitMetrics(BlockType block_type, GetContext* get_context,
                             size_t usage) const;
  void UpdateCacheMissMetrics(BlockType block_type,
                              GetContext* get_context) const;

  // Either Block::NewDataIterator() or Block::NewIndexIterator().
  template <typename TBlockIter>
  static TBlockIter* InitBlockIterator(const Rep* rep, Block* block,
                                       BlockType block_type,
                                       TBlockIter* input_iter,
                                       bool block_contents_pinned);

  // If block cache enabled (compressed or uncompressed), looks for the block
  // identified by handle in (1) uncompressed cache, (2) compressed cache, and
  // then (3) file. If found, inserts into the cache(s) that were searched
  // unsuccessfully (e.g., if found in file, will add to both uncompressed and
  // compressed caches if they're enabled).
  //
  // @param block_entry value is set to the uncompressed block if found. If
  //    in uncompressed block cache, also sets cache_handle to reference that
  //    block.
  DECLARE_SYNC_AND_ASYNC_TEMPLATE_CONST(
      template <typename TBlocklike>, BlocklikeStatus<TBlocklike>,
      MaybeReadBlockAndLoadToCache, FilePrefetchBuffer* prefetch_buffer,
      const ReadOptions& ro, const BlockHandle& handle,
      UnownedPtr<Decompressor> decomp, bool for_compaction,
      CachableEntry<TBlocklike>* block_entry, GetContext* get_context,
      BlockCacheLookupContext* lookup_context, BlockContents* contents,
      bool async_read, bool use_block_cache_for_lookup);

  // Similar to the above, with one crucial difference: it will retrieve the
  // block from the file even if there are no caches configured (assuming the
  // read options allow I/O).
  DECLARE_SYNC_AND_ASYNC_TEMPLATE_CONST(
      template <typename TBlocklike>, BlocklikeStatus<TBlocklike>,
      RetrieveBlock, FilePrefetchBuffer* prefetch_buffer, const ReadOptions& ro,
      const BlockHandle& handle, UnownedPtr<Decompressor> decomp,
      CachableEntry<TBlocklike>* block_entry, GetContext* get_context,
      BlockCacheLookupContext* lookup_context, bool for_compaction,
      bool use_cache, bool async_read, bool use_block_cache_for_lookup);

  template <typename TBlocklike>
  WithBlocklikeCheck<void, TBlocklike> SaveLookupContextOrTraceRecord(
      const Slice& block_key, bool is_cache_hit, const ReadOptions& ro,
      const TBlocklike* parsed_block_value,
      BlockCacheLookupContext* lookup_context) const;

  void FinishTraceRecord(const BlockCacheLookupContext& lookup_context,
                         const Slice& block_key, const Slice& referenced_key,
                         bool does_referenced_key_exist,
                         uint64_t referenced_data_size) const;

  DECLARE_SYNC_AND_ASYNC_CONST(
      void, RetrieveMultipleBlocks, const ReadOptions& options,
      const MultiGetRange* batch,
      const autovector<BlockHandle, MultiGetContext::MAX_BATCH_SIZE>* handles,
      Status* statuses, CachableEntry<Block_kData>* results, char* scratch,
      UnownedPtr<Decompressor> decomp, bool use_fs_scratch);

  // Get the iterator from the index reader.
  //
  // If input_iter is not set, return a new Iterator.
  // If input_iter is set, try to update it and return it as Iterator.
  // However note that in some cases the returned iterator may be different
  // from input_iter. In such case the returned iterator should be freed.
  //
  // Note: ErrorIterator with Status::Incomplete shall be returned if all the
  // following conditions are met:
  //  1. We enabled table_options.cache_index_and_filter_blocks.
  //  2. index is not present in block cache.
  //  3. We disallowed any io to be performed, that is, read_options ==
  //     kBlockCacheTier
  InternalIteratorBase<IndexValue>* NewIndexIterator(
      const ReadOptions& read_options, bool disable_prefix_seek,
      IndexBlockIter* input_iter, GetContext* get_context,
      BlockCacheLookupContext* lookup_context) const;

  template <typename TBlocklike>
  Cache::Priority GetCachePriority() const;

  // Read block cache from block caches (if set): block_cache.
  // On success, Status::OK with be returned and @block will be populated with
  // pointer to the block as well as its block handle.
  // @param uncompression_dict Data for presetting the compression library's
  //    dictionary.
  template <typename TBlocklike>
  WithBlocklikeCheck<Status, TBlocklike> GetDataBlockFromCache(
      const Slice& cache_key, BlockCacheInterface<TBlocklike> block_cache,
      CachableEntry<TBlocklike>* block, GetContext* get_context,
      UnownedPtr<Decompressor> decomp) const;

  // Put a maybe compressed block to the corresponding block caches.
  // This method will perform decompression against block_contents if needed
  // and then populate the block caches.
  // On success, Status::OK will be returned; also @block will be populated with
  // uncompressed block and its cache handle.
  //
  // Allocated memory managed by block_contents will be transferred to
  // PutDataBlockToCache(). After the call, the object will be invalid.
  // @param uncompression_dict Data for presetting the compression library's
  //    dictionary.
  template <typename TBlocklike>
  WithBlocklikeCheck<Status, TBlocklike> PutDataBlockToCache(
      const Slice& cache_key, BlockCacheInterface<TBlocklike> block_cache,
      CachableEntry<TBlocklike>* cached_block,
      BlockContents&& uncompressed_block_contents,
      BlockContents&& compressed_block_contents,
      CompressionType block_comp_type, UnownedPtr<Decompressor> decomp,
      MemoryAllocator* memory_allocator, GetContext* get_context) const;

  // Calls (*handle_result)(arg, ...) repeatedly, starting with the entry found
  // after a call to Seek(key), until handle_result returns false.
  // May not make such a call if filter policy says that key is not present.
  friend class TableCache;
  friend class BlockBasedTableBuilder;

  // Create a index reader based on the index type stored in the table.
  // Optionally, user can pass a preloaded meta_index_iter for the index that
  // need to access extra meta blocks for index construction. This parameter
  // helps avoid re-reading meta index block if caller already created one.
  Status CreateIndexReader(const ReadOptions& ro,
                           FilePrefetchBuffer* prefetch_buffer,
                           InternalIterator* preloaded_meta_index_iter,
                           bool use_cache, bool prefetch, bool pin,
                           BlockCacheLookupContext* lookup_context,
                           std::unique_ptr<IndexReader>* index_reader);

  bool FullFilterKeyMayMatch(FilterBlockReader* filter, const Slice& user_key,
                             const SliceTransform* prefix_extractor,
                             GetContext* get_context,
                             BlockCacheLookupContext* lookup_context,
                             const ReadOptions& read_options) const;

  void FullFilterKeysMayMatch(FilterBlockReader* filter, MultiGetRange* range,
                              const SliceTransform* prefix_extractor,
                              BlockCacheLookupContext* lookup_context,
                              const ReadOptions& read_options) const;

  // If force_direct_prefetch is true, always prefetching to RocksDB
  //    buffer, rather than calling RandomAccessFile::Prefetch().
  static Status PrefetchTail(
      const ReadOptions& ro, const ImmutableOptions& ioptions,
      RandomAccessFileReader* file, uint64_t file_size,
      bool force_direct_prefetch, TailPrefetchStats* tail_prefetch_stats,
      const bool prefetch_all, const bool preload_all,
      std::unique_ptr<FilePrefetchBuffer>* prefetch_buffer, Statistics* stats,
      uint64_t tail_size, Logger* const logger);
  Status ReadMetaIndexBlock(const ReadOptions& ro,
                            FilePrefetchBuffer* prefetch_buffer,
                            std::unique_ptr<Block>* metaindex_block,
                            std::unique_ptr<InternalIterator>* iter);
  Status ReadPropertiesBlock(const ReadOptions& ro,
                             FilePrefetchBuffer* prefetch_buffer,
                             InternalIterator* meta_iter,
                             const SequenceNumber largest_seqno);
  Status ReadRangeDelBlock(const ReadOptions& ro,
                           FilePrefetchBuffer* prefetch_buffer,
                           InternalIterator* meta_iter,
                           const InternalKeyComparator& internal_comparator,
                           BlockCacheLookupContext* lookup_context);
  // If index and filter blocks do not need to be pinned, `prefetch_all`
  // determines whether they will be read and added to cache. When
  // `avoid_shared_metadata_cache` is set, open-time metadata reads avoid the
  // shared block cache regardless of pinning/prefetch policy.
  Status PrefetchIndexAndFilterBlocks(
      const ReadOptions& ro, FilePrefetchBuffer* prefetch_buffer,
      InternalIterator* meta_iter, BlockBasedTable* new_table,
      bool prefetch_all, const BlockBasedTableOptions& table_options,
      const int level, size_t file_size, size_t max_file_size_for_l0_meta_pin,
      bool avoid_shared_metadata_cache,
      BlockCacheLookupContext* lookup_context);

  static BlockType GetBlockTypeForMetaBlockByName(const Slice& meta_block_name);

  Status VerifyChecksumInMetaBlocks(const ReadOptions& read_options,
                                    InternalIteratorBase<Slice>* index_iter);
  Status VerifyChecksumInBlocks(const ReadOptions& read_options,
                                InternalIteratorBase<IndexValue>* index_iter);

  // Create the filter from the filter block.
  std::unique_ptr<FilterBlockReader> CreateFilterBlockReader(
      const ReadOptions& ro, FilePrefetchBuffer* prefetch_buffer,
      bool use_cache, bool prefetch, bool pin,
      BlockCacheLookupContext* lookup_context);

  // Size of all data blocks, maybe approximate
  uint64_t GetApproximateDataSize();

  // Given an iterator return its offset in data block section of file.
  uint64_t ApproximateDataOffsetOf(
      const InternalIteratorBase<IndexValue>& index_iter,
      uint64_t data_size) const;

  // Helper functions for DumpTable()
  Status DumpIndexBlock(std::ostream& out_stream);
  Status DumpDataBlocks(std::ostream& out_stream,
                        bool show_sequence_number_type = false);
  void DumpKeyValue(const Slice& key, const Slice& value,
                    std::ostream& out_stream,
                    bool show_sequence_number_type = false);
  void DumpBlockChecksumInfo(const BlockHandle& block_handle,
                             const ReadOptions& read_options,
                             const char* block_name,
                             std::ostream& out_stream) const;

  // Returns false if prefix_extractor exists and is compatible with that used
  // in building the table file, otherwise true.
  bool PrefixExtractorChanged(const SliceTransform* prefix_extractor) const;

  bool TimestampMayMatch(const ReadOptions& read_options) const;

  bool BlockTypeMaybeCompressed(BlockType type) const {
    return type != BlockType::kFilter &&
           type != BlockType::kCompressionDictionary &&
           type != BlockType::kUserDefinedIndex;
  }

  // A cumulative data block file read in MultiGet lower than this size will
  // use a stack buffer
  static constexpr size_t kMultiGetReadStackBufSize = 8192;

  friend class PartitionedFilterBlockReader;
  friend class PartitionedFilterBlockTest;
  friend class DBBasicTest_MultiGetIOBufferOverrun_Test;
  friend class ReadSet;
  friend class IODispatcherTest;
};

// Maintaining state of a two-level iteration on a partitioned index structure.
class BlockBasedTable::PartitionedIndexIteratorState
    : public TwoLevelIteratorState {
 public:
  PartitionedIndexIteratorState(
      const BlockBasedTable* table,
      UnorderedMap<uint64_t, CachableEntry<Block>>* block_map);
  InternalIteratorBase<IndexValue>* NewSecondaryIterator(
      const BlockHandle& index_value) override;

 private:
  // Don't own table_
  const BlockBasedTable* table_;
  UnorderedMap<uint64_t, CachableEntry<Block>>* block_map_;
};

// Stores all the properties associated with a BlockBasedTable.
// These are immutable.
struct BlockBasedTable::Rep {
  Rep(const ImmutableOptions& _ioptions, const EnvOptions& _env_options,
      const BlockBasedTableOptions& _table_opt,
      const InternalKeyComparator& _internal_comparator, bool skip_filters,
      uint64_t _file_size, int _level, const bool _immortal_table,
      const bool _user_defined_timestamps_persisted = true)
      : ioptions(_ioptions),
        env_options(_env_options),
        table_options(_table_opt),
        filter_policy(skip_filters ? nullptr : _table_opt.filter_policy.get()),
        internal_comparator(_internal_comparator),
        filter_type(FilterType::kNoFilter),
        index_type(BlockBasedTableOptions::IndexType::kBinarySearch),
        whole_key_filtering(_table_opt.whole_key_filtering),
        prefix_filtering(true),
        global_seqno(kDisableGlobalSequenceNumber),
        file_size(_file_size),
        level(_level),
        immortal_table(_immortal_table),
        user_defined_timestamps_persisted(_user_defined_timestamps_persisted),
        fs_prefetch_support(CheckFSFeatureSupport(
            _ioptions.fs.get(), FSSupportedOps::kFSPrefetch)) {}
  // No copying allowed
  Rep(const Rep&) = delete;
  Rep& operator=(const Rep&) = delete;
  Rep(Rep&&) = delete;
  Rep& operator=(Rep&&) = delete;

  ~Rep() { status.PermitUncheckedError(); }
  const ImmutableOptions& ioptions;
  const EnvOptions& env_options;
  const BlockBasedTableOptions table_options;
  const FilterPolicy* const filter_policy;
  const InternalKeyComparator& internal_comparator;
  Status status;
  std::unique_ptr<RandomAccessFileReader> file;
  OffsetableCacheKey base_cache_key;
  PersistentCacheOptions persistent_cache_options;

  // Footer contains the fixed table information
  Footer footer;

  // Declared before index_reader so both outlive the custom index reader.
  CachableEntry<Block_kUserDefinedIndex> udi_block;
  // This table's view of udi_block, passed to IndexFactory::NewReader(). The
  // reader may retain or advance it without changing a shared cached block.
  Slice udi_index_contents;

  std::unique_ptr<IndexReader> index_reader;
  std::unique_ptr<FilterBlockReader> filter;
  std::unique_ptr<UncompressionDictReader> uncompression_dict_reader;

  enum class FilterType {
    kNoFilter,
    kFullFilter,
    kPartitionedFilter,
  };
  FilterType filter_type;
  BlockHandle filter_handle;
  BlockHandle compression_dict_handle;

  std::shared_ptr<const TableProperties> table_properties;
  SeqnoToTimeMapping seqno_to_time_mapping;
  BlockHandle index_handle;
  BlockBasedTableOptions::IndexType index_type;
  bool whole_key_filtering;
  bool prefix_filtering;
  std::shared_ptr<const SliceTransform> table_prefix_extractor;

  std::shared_ptr<FragmentedRangeTombstoneList> fragmented_range_dels;

  // Context for block cache CreateCallback
  BlockCreateContext create_context;

  // If global_seqno is used, all Keys in this file will have the same
  // seqno with value `global_seqno`.
  //
  // A value of kDisableGlobalSequenceNumber means that this feature is disabled
  // and every key have it's own seqno.
  SequenceNumber global_seqno;

  // Size of the table file on disk
  uint64_t file_size;

  // the level when the table is opened, could potentially change when trivial
  // move is involved
  int level;

  // the timestamp range of table
  // Points into memory owned by TableProperties. This would need to change if
  // TableProperties become subject to cache eviction.
  Slice min_timestamp;
  Slice max_timestamp;

  // If blocks might be compressed, refers to a decompressor that can decompress
  // them. (nullptr -> no blocks compressed)  However, if (data) blocks are
  // dictionary compressed, a dictionary-aware decompressor is needed, which
  // might live in the block cache.
  std::shared_ptr<Decompressor> decompressor;

  // These describe how index is encoded.
  bool index_has_first_key = false;
  bool index_key_includes_seq = true;
  bool index_value_is_full = true;

  // Restart intervals read from table properties (0 if not available)
  uint32_t data_block_restart_interval = 0;
  uint32_t index_block_restart_interval = 0;

  // If true, then data blocks have keys and values separated.
  bool separate_key_value_in_data_block = false;
  // Whether this SST contains embedded (same-file) blob records, detected from
  // the presence of the embedded blob stats property. Same-file BlobIndex
  // references are only valid when this is true.
  bool has_embedded_blobs = false;

  // BlobSource for routing same-file ("embedded") blob reads through the blob
  // value cache + BLOB_DB_* statistics. Owned by the ColumnFamilyData; this
  // reader is owned (via TableCache) by the same CFD, so the raw pointer is
  // lifetime-safe. nullptr for non-DB openers (SstFileReader, sst_dump,
  // repair, external-file ingestion prevalidation, etc.); in that case
  // embedded reads fall back to a direct (uncached) read.
  BlobSource* blob_source_ = nullptr;

  // Whether block checksums in metadata blocks were verified on open.
  // This is only to mostly maintain current dubious behavior of VerifyChecksum
  // with respect to index blocks, but only when the checksum was previously
  // verified.
  bool verify_checksum_set_on_open = false;

  const bool immortal_table;
  // Whether the user key contains user-defined timestamps. If this is false and
  // the running user comparator has a non-zero timestamp size, a min timestamp
  // of this size will be padded to each user key while parsing blocks whenever
  // it applies.  This includes the keys in data block, index block for data
  // block, top-level index for index partitions (if index type is
  // `kTwoLevelIndexSearch`), top-level index for filter partitions (if using
  // partitioned filters), the `first_internal_key` in `IndexValue`, the
  // `end_key` for range deletion entries.
  const bool user_defined_timestamps_persisted;

  const bool fs_prefetch_support;

  // Set to >0 when the file is known to be obsolete and should have its block
  // cache entries evicted on close. NOTE: when the file becomes obsolete,
  // there could be multiple table cache references that all mark this file as
  // obsolete. An atomic resolves the race quite reasonably. Even in the rare
  // case of such a race, they will most likely be storing the same value.
  RelaxedAtomic<uint32_t> uncache_aggressiveness{0};

  std::unique_ptr<CacheReservationManager::CacheReservationHandle>
      table_reader_cache_res_handle = nullptr;

  SequenceNumber get_global_seqno(BlockType block_type) const {
    return (block_type == BlockType::kFilterPartitionIndex ||
            block_type == BlockType::kCompressionDictionary)
               ? kDisableGlobalSequenceNumber
               : global_seqno;
  }

  uint64_t cf_id_for_tracing() const {
    return table_properties
               ? table_properties->column_family_id
               : ROCKSDB_NAMESPACE::TablePropertiesCollectorFactory::Context::
                     kUnknownColumnFamily;
  }

  Slice cf_name_for_tracing() const {
    return table_properties ? table_properties->column_family_name
                            : BlockCacheTraceHelper::kUnknownColumnFamilyName;
  }

  uint32_t level_for_tracing() const { return level >= 0 ? level : UINT32_MAX; }

  uint64_t sst_number_for_tracing() const {
    return file ? TableFileNameToNumber(file->file_name()) : UINT64_MAX;
  }
  void CreateFilePrefetchBuffer(
      const ReadaheadParams& readahead_params,
      std::unique_ptr<FilePrefetchBuffer>* fpb,
      const std::function<void(bool, uint64_t&, uint64_t&)>& readaheadsize_cb,
      FilePrefetchBufferUsage usage) const {
    fpb->reset(new FilePrefetchBuffer(
        readahead_params, !ioptions.allow_mmap_reads /* enable */,
        false /* track_min_offset */, ioptions.fs.get(), ioptions.clock,
        ioptions.stats, readaheadsize_cb, usage));
  }

  void CreateFilePrefetchBufferIfNotExists(
      const ReadaheadParams& readahead_params,
      std::unique_ptr<FilePrefetchBuffer>* fpb,
      const std::function<void(bool, uint64_t&, uint64_t&)>& readaheadsize_cb,
      FilePrefetchBufferUsage usage = FilePrefetchBufferUsage::kUnknown) const {
    if (!(*fpb)) {
      CreateFilePrefetchBuffer(readahead_params, fpb, readaheadsize_cb, usage);
    }
  }

  std::size_t ApproximateMemoryUsage() const {
    std::size_t usage = 0;
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
    usage += malloc_usable_size(const_cast<BlockBasedTable::Rep*>(this));
#else
    usage += sizeof(*this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
    return usage;
  }
};

// This is an adapter class for `WritableFile` to be used for `std::ostream`.
// The adapter wraps a `WritableFile`, which can be passed to a `std::ostream`
// constructor for storing streaming data.
// Note:
//  * This adapter doesn't provide any buffering, each write is forwarded to
//    `WritableFile->Append()` directly.
//  * For a failed write, the user needs to check the status by `ostream.good()`
class WritableFileStringStreamAdapter : public std::stringbuf {
 public:
  explicit WritableFileStringStreamAdapter(WritableFile* writable_file)
      : file_(writable_file) {}

  // Override overflow() to handle `sputc()`. There are cases that will not go
  // through `xsputn()` e.g. `std::endl` or an unsigned long long is written by
  // `os.put()` directly and will call `sputc()` By internal implementation:
  //    int_type __CLR_OR_THIS_CALL sputc(_Elem _Ch) {  // put a character
  //        return 0 < _Pnavail() ? _Traits::to_int_type(*_Pninc() = _Ch) :
  //        overflow(_Traits::to_int_type(_Ch));
  //    }
  // As we explicitly disabled buffering (_Pnavail() is always 0), every write,
  // not captured by xsputn(), becomes an overflow here.
  int overflow(int ch = EOF) override {
    if (ch != EOF) {
      Status s = file_->Append(Slice((char*)&ch, 1));
      if (s.ok()) {
        return ch;
      }
    }
    return EOF;
  }

  std::streamsize xsputn(char const* p, std::streamsize n) override {
    Status s = file_->Append(Slice(p, n));
    if (!s.ok()) {
      return 0;
    }
    return n;
  }

 private:
  WritableFile* file_;
};

}  // namespace ROCKSDB_NAMESPACE
