//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once

#include <cassert>

#include "table/block_based/cachable_entry.h"
#include "table/block_based/filter_block.h"

namespace ROCKSDB_NAMESPACE {

class BlockBasedTable;
class FilePrefetchBuffer;

// Encapsulates common functionality for the various filter block reader
// implementations. Provides access to the filter block regardless of whether
// it is owned by the reader or stored in the cache, or whether it is pinned
// in the cache or not.
template <typename TBlocklike>
class FilterBlockReaderCommon : public FilterBlockReader {
 public:
  FilterBlockReaderCommon(const BlockBasedTable* t,
                          CachableEntry<TBlocklike>&& filter_block)
      : table_(t), filter_block_(std::move(filter_block)) {
    assert(table_);
    const SliceTransform* const prefix_extractor = table_prefix_extractor();
    if (prefix_extractor) {
      full_length_enabled_ =
          prefix_extractor->FullLengthEnabled(&prefix_extractor_full_length_);
    }
  }

  bool RangeMayExist(const Slice* iterate_upper_bound, const Slice& user_key,
                     const SliceTransform* prefix_extractor,
                     const Comparator* comparator,
                     const Slice* const const_ikey_ptr, bool* filter_checked,
                     bool need_upper_bound_check,
                     BlockCacheLookupContext* lookup_context,
                     const ReadOptions& read_options) override;

  void EraseFromCacheBeforeDestruction(
      uint32_t /*uncache_aggressiveness*/) override;

 protected:
  static Status ReadFilterBlock(const BlockBasedTable* table,
                                FilePrefetchBuffer* prefetch_buffer,
                                const ReadOptions& read_options, bool use_cache,
                                GetContext* get_context,
                                BlockCacheLookupContext* lookup_context,
                                CachableEntry<TBlocklike>* filter_block);

  const BlockBasedTable* table() const { return table_; }
  const SliceTransform* table_prefix_extractor() const;
  bool whole_key_filtering() const;
  bool cache_filter_blocks() const;

  Status GetOrReadFilterBlock(GetContext* get_context,
                              BlockCacheLookupContext* lookup_context,
                              CachableEntry<TBlocklike>* filter_block,
                              const ReadOptions& read_options) const;

  // Like GetOrReadFilterBlock(), but if this reader does not hold the filter
  // block, looks for it only in the primary tier of the block cache (see
  // BlockBasedTable::LookupBlockInPrimaryCache()). Leaves `filter_block` empty
  // if the filter block is not found there.
  Status GetFilterBlockInMemory(CachableEntry<TBlocklike>* filter_block) const;

  // The filter block that this reader holds for its whole life (owned, or
  // pinned in the block cache), or nullptr if it looks the block up when
  // needed.
  TBlocklike* held_filter_block() const { return filter_block_.GetValue(); }

  size_t ApproximateFilterBlockMemoryUsage() const;

 private:
  bool IsFilterCompatible(const Slice* iterate_upper_bound, const Slice& prefix,
                          const Comparator* comparator) const;

 private:
  const BlockBasedTable* table_;
  CachableEntry<TBlocklike> filter_block_;
  size_t prefix_extractor_full_length_ = 0;
  bool full_length_enabled_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
