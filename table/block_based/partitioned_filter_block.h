//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <deque>
#include <list>
#include <string>
#include <unordered_map>

#include "block_cache.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "table/block_based/block.h"
#include "table/block_based/filter_block_reader_common.h"
#include "table/block_based/full_filter_block.h"
#include "table/block_based/index_builder.h"
#include "util/atomic.h"
#include "util/autovector.h"
#include "util/hash_containers.h"

namespace ROCKSDB_NAMESPACE {
class InternalKeyComparator;

class PartitionedFilterBlockBuilder : public FullFilterBlockBuilder {
 public:
  explicit PartitionedFilterBlockBuilder(
      const SliceTransform* prefix_extractor, bool whole_key_filtering,
      FilterBitsBuilder* filter_bits_builder, int index_block_restart_interval,
      const bool use_value_delta_encoding, uint32_t format_version,
      PartitionedIndexBuilder* const partitioned_index_builder,
      const uint32_t partition_size, size_t ts_sz,
      const bool persist_user_defined_timestamps,
      bool decouple_from_index_partitions, bool use_common_prefix = false);

  ~PartitionedFilterBlockBuilder() override;

  void Add(const Slice& key_without_ts) override;
  void AddWithPrevKey(const Slice& key_without_ts,
                      const Slice& prev_key_without_ts) override;
  bool IsEmpty() const override {
    return filter_bits_builder_->EstimateEntriesAdded() == 0 &&
           filters_.empty();
  }

  size_t EstimateEntriesAdded() override;
  size_t CurrentFilterSizeEstimate() override;
  void OnDataBlockFinalized(uint64_t num_data_blocks) override;

  void PrevKeyBeforeFinish(const Slice& prev_key_without_ts) override;
  Status Finish(const BlockHandle& last_partition_block_handle, Slice* filter,
                std::unique_ptr<const char[]>* filter_owner = nullptr) override;

  void ResetFilterBitsBuilder() override {
    filters_.clear();
    total_added_in_built_ = 0;
    index_on_filter_block_builder_.Reset();
    index_on_filter_block_builder_without_seq_.Reset();
    FullFilterBlockBuilder::ResetFilterBitsBuilder();
  }

  // For PartitionFilter, optional post-verifing the filter is done
  // as part of PartitionFilterBlockBuilder::Finish
  // to avoid implementation complexity of doing it elsewhere.
  // Therefore we are skipping it in here.
  Status MaybePostVerifyFilter(const Slice& /* filter_content */) override {
    return Status::OK();
  }

 protected:
  // Needs to be thread-safe to be invoked from background worker
  // thread when parallel compression is enabled.
  void UpdateFilterSizeEstimate(uint64_t num_data_blocks) override;

 private:  // fns
  // Whether to cut a filter block before the next key
  bool DecideCutAFilterBlock();
  void CutAFilterBlock(const Slice* next_key, const Slice* next_prefix,
                       const Slice& prev_key);

  void AddImpl(const Slice& key_without_ts, const Slice& prev_key_without_ts);

 private:  // data
  // Supplies the separator format and, unless decoupled, partition boundaries.
  // Never null: a partitioned filter is created alongside a partitioned index.
  PartitionedIndexBuilder* const partitioned_index_builder_;
  const size_t ts_sz_;
  const bool decouple_from_index_partitions_;

  // Filter data
  struct FilterEntry {
    std::string ikey;  // internal key or separator *after* this filter
    std::unique_ptr<const char[]> filter_owner;
    Slice filter;
  };
  std::deque<FilterEntry> filters_;  // list of partitioned filters and keys
                                     // used in building the index
  // Running total of completed filter partition sizes to avoid
  // iterating over filters_ deque, which can be concurrently modified by
  // the main thread when parallel compression is enabled.
  RelaxedAtomic<size_t> completed_partitions_size_{0};

  // The desired number of keys per partition
  uint32_t keys_per_partition_;
  // According to the bits builders, how many keys/prefixes added
  // in all the filters we have fully built
  uint64_t total_added_in_built_ = 0;

  // Set to the first non-okay status if any of the filter
  // partitions experiences construction error.
  // If partitioned_filters_construction_status_ is non-okay,
  // then the whole partitioned filters should not be used.
  Status partitioned_filters_construction_status_;

  // For Add without prev key
  std::string prev_key_without_ts_;

  // Cached filter size estimate for hot path performance - updated only when
  // data blocks are written for meaningful estimate updates.
  // Must be atomic since UpdateFilterSizeEstimate() can be called from
  // background worker threads when parallel compression is enabled.
  RelaxedAtomic<size_t> estimated_filter_size_{0};

#ifndef NDEBUG
  // For verifying accurate previous keys are provided by the caller, so that
  // release code can be fast
  bool DEBUG_add_with_prev_key_called_ = false;
  std::string DEBUG_prev_key_without_ts_;
#endif  // NDEBUG

  // ===== State for Finish() =====

  // top-level index builder on internal keys
  BlockBuilder index_on_filter_block_builder_;
  // same for user keys
  BlockBuilder index_on_filter_block_builder_without_seq_;
  // For delta-encoding handles
  BlockHandle last_encoded_handle_ = BlockHandle::NullBlockHandle();
  const bool use_value_delta_encoding_;
  // format_version >= 8 value-delta codec for the index on filter partitions
  // (see IndexValue::EncodeTo). Must match what the reader of this block uses.
  const bool value_delta_escape_;
  // True if we are between two calls to Finish(), because we have returned
  // the filter at the front of filters_ but haven't yet added it to the
  // partition index.
  bool finishing_front_filter_ = false;
};

class PartitionedFilterBlockReader
    : public FilterBlockReaderCommon<Block_kFilterPartitionIndex> {
 public:
  PartitionedFilterBlockReader(
      const BlockBasedTable* t,
      CachableEntry<Block_kFilterPartitionIndex>&& filter_block);

  static std::unique_ptr<FilterBlockReader> Create(
      const BlockBasedTable* table, const ReadOptions& ro,
      FilePrefetchBuffer* prefetch_buffer, bool use_cache, bool prefetch,
      bool pin, BlockCacheLookupContext* lookup_context);

  bool KeyMayMatch(const Slice& key, const Slice* const const_ikey_ptr,
                   GetContext* get_context,
                   BlockCacheLookupContext* lookup_context,
                   const ReadOptions& read_options) override;
  void KeysMayMatch(MultiGetRange* range,
                    BlockCacheLookupContext* lookup_context,
                    const ReadOptions& read_options) override;

  bool PrefixMayMatch(const Slice& prefix, const Slice* const const_ikey_ptr,
                      GetContext* get_context,
                      BlockCacheLookupContext* lookup_context,
                      const ReadOptions& read_options) override;
  void PrefixesMayMatch(MultiGetRange* range,
                        const SliceTransform* prefix_extractor,
                        BlockCacheLookupContext* lookup_context,
                        const ReadOptions& read_options) override;

  size_t ApproximateMemoryUsage() const override;

 private:
  BlockHandle GetFilterPartitionHandle(
      const CachableEntry<Block_kFilterPartitionIndex>& filter_block,
      const Slice& entry) const;
  // Initializes `iter` over the top-level filter partition index in
  // `filter_block`, for use with `SeekFilterPartitionHandle`.
  void NewFilterPartitionIndexIterator(
      const CachableEntry<Block_kFilterPartitionIndex>& filter_block,
      IndexBlockIter* iter) const;
  BlockHandle SeekFilterPartitionHandle(IndexBlockIter* iter,
                                        const Slice& entry) const;
  Status GetFilterPartitionBlock(
      FilePrefetchBuffer* prefetch_buffer, const BlockHandle& handle,
      GetContext* get_context, BlockCacheLookupContext* lookup_context,
      const ReadOptions& read_options,
      CachableEntry<ParsedFullFilterBlock>* filter_block) const;

  using FilterFunction = bool (FullFilterBlockReader::*)(
      const Slice& slice, const Slice* const const_ikey_ptr,
      GetContext* get_context, BlockCacheLookupContext* lookup_context,
      const ReadOptions& read_options);
  bool MayMatch(const Slice& slice, const Slice* const_ikey_ptr,
                GetContext* get_context,
                BlockCacheLookupContext* lookup_context,
                const ReadOptions& read_options,
                FilterFunction filter_function) const;
  using FilterManyFunction = void (FullFilterBlockReader::*)(
      MultiGetRange* range, const SliceTransform* prefix_extractor,
      BlockCacheLookupContext* lookup_context, const ReadOptions& read_options);
  void MayMatch(MultiGetRange* range, const SliceTransform* prefix_extractor,
                BlockCacheLookupContext* lookup_context,
                const ReadOptions& read_options,
                FilterManyFunction filter_function) const;
  void MayMatchPartition(MultiGetRange* range,
                         const SliceTransform* prefix_extractor,
                         BlockHandle filter_handle,
                         BlockCacheLookupContext* lookup_context,
                         const ReadOptions& read_options,
                         FilterManyFunction filter_function) const;
  Status CacheDependencies(const ReadOptions& ro, bool pin,
                           FilePrefetchBuffer* tail_prefetch_buffer) override;
  void EraseFromCacheBeforeDestruction(
      uint32_t /*uncache_aggressiveness*/) override;

  const InternalKeyComparator* internal_comparator() const;
  bool index_key_includes_seq() const;
  bool index_value_is_full() const;
  bool index_value_delta_escape() const;
  bool user_defined_timestamps_persisted() const;

 protected:
  // For partition blocks pinned in cache. Can be a subset of blocks
  // in case some fail insertion on attempt to pin.
  UnorderedMap<uint64_t, CachableEntry<ParsedFullFilterBlock>> filter_map_;
};

}  // namespace ROCKSDB_NAMESPACE
