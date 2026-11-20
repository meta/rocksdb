//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <vector>

#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "table/block_based/filter_block_reader_common.h"
#include "table/block_based/filter_policy_internal.h"
#include "table/block_based/parsed_full_filter_block.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

class FilterPolicy;
class FilterBitsBuilder;
class FilterBitsReader;

// A FullFilterBlockBuilder is used to construct a full filter for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
// The format of full filter block is:
// +----------------------------------------------------------------+
// |              full filter for all keys in sst file              |
// +----------------------------------------------------------------+
// The full filter can be very large. At the end of it, we put
// num_probes: how many hash functions are used in bloom filter
//
class FullFilterBlockBuilder : public FilterBlockBuilder {
 public:
  explicit FullFilterBlockBuilder(const SliceTransform* prefix_extractor,
                                  bool whole_key_filtering,
                                  FilterBitsBuilder* filter_bits_builder);
  // No copying allowed
  FullFilterBlockBuilder(const FullFilterBlockBuilder&) = delete;
  void operator=(const FullFilterBlockBuilder&) = delete;
  FullFilterBlockBuilder(FullFilterBlockBuilder&&) = delete;
  void operator=(FullFilterBlockBuilder&&) = delete;

  // bits_builder is created in filter_policy, it should be passed in here
  // directly. and be deleted here
  ~FullFilterBlockBuilder() override {}

  void Add(const Slice& key_without_ts) override;
  void AddWithPrevKey(const Slice& key_without_ts,
                      const Slice& prev_key_without_ts) override;

  bool IsEmpty() const override {
    return filter_bits_builder_->EstimateEntriesAdded() == 0;
  }
  size_t EstimateEntriesAdded() override;
  size_t CurrentFilterSizeEstimate() override;
  void OnDataBlockFinalized(uint64_t num_data_blocks) override;
  Status Finish(const BlockHandle& last_partition_block_handle, Slice* filter,
                std::unique_ptr<const char[]>* filter_owner = nullptr) override;
  using FilterBlockBuilder::Finish;

  void ResetFilterBitsBuilder() override { filter_bits_builder_.reset(); }

  Status MaybePostVerifyFilter(const Slice& filter_content) override {
    return filter_bits_builder_->MaybePostVerify(filter_content);
  }

 protected:
  const SliceTransform* prefix_extractor() const { return prefix_extractor_; }
  bool whole_key_filtering() const { return whole_key_filtering_; }

  std::unique_ptr<FilterBitsBuilder> filter_bits_builder_;

  void UpdateFilterSizeEstimate(uint64_t num_data_blocks_written) override;

 private:
  // important: all of these might point to invalid addresses
  // at the time of destruction of this filter block. destructor
  // should NOT dereference them.
  const SliceTransform* const prefix_extractor_;
  const bool whole_key_filtering_;
  std::unique_ptr<const char[]> filter_data_;

  size_t estimated_filter_size_ = 0;
};

// A FilterBlockReader is used to parse filter from SST table.
// KeyMayMatch and PrefixMayMatch would trigger filter checking
class FullFilterBlockReader
    : public FilterBlockReaderCommon<ParsedFullFilterBlock> {
 public:
  FullFilterBlockReader(const BlockBasedTable* t,
                        CachableEntry<ParsedFullFilterBlock>&& filter_block);

  static std::unique_ptr<FilterBlockReader> Create(
      const BlockBasedTable* table, const ReadOptions& ro,
      FilePrefetchBuffer* prefetch_buffer, bool use_cache, bool prefetch,
      bool pin, BlockCacheLookupContext* lookup_context);

  bool KeyMayMatch(const Slice& key, const Slice* const const_ikey_ptr,
                   GetContext* get_context,
                   BlockCacheLookupContext* lookup_context,
                   const ReadOptions& read_options) override;

  bool PrefixMayMatch(const Slice& prefix, const Slice* const const_ikey_ptr,
                      GetContext* get_context,
                      BlockCacheLookupContext* lookup_context,
                      const ReadOptions& read_options) override;

  void KeysMayMatch(MultiGetRange* range,
                    BlockCacheLookupContext* lookup_context,
                    const ReadOptions& read_options) override;
  // Used in partitioned filter code
  void KeysMayMatch2(MultiGetRange* range,
                     const SliceTransform* /*prefix_extractor*/,
                     BlockCacheLookupContext* lookup_context,
                     const ReadOptions& read_options) {
    KeysMayMatch(range, lookup_context, read_options);
  }

  void PrefixesMayMatch(MultiGetRange* range,
                        const SliceTransform* prefix_extractor,
                        BlockCacheLookupContext* lookup_context,
                        const ReadOptions& read_options) override;
  size_t ApproximateMemoryUsage() const override;

 private:
  bool MayMatch(const Slice& entry, GetContext* get_context,
                BlockCacheLookupContext* lookup_context,
                const ReadOptions& read_options) const;
  void MayMatch(MultiGetRange* range, const SliceTransform* prefix_extractor,
                BlockCacheLookupContext* lookup_context,
                const ReadOptions& read_options) const;
};

}  // namespace ROCKSDB_NAMESPACE
