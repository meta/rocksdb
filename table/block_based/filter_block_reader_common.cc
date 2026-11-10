//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "table/block_based/filter_block_reader_common.h"

#include "block_cache.h"
#include "monitoring/perf_context_imp.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/parsed_full_filter_block.h"

namespace ROCKSDB_NAMESPACE {

template <typename TBlocklike>
Status FilterBlockReaderCommon<TBlocklike>::ReadFilterBlock(
    const BlockBasedTable* table, FilePrefetchBuffer* prefetch_buffer,
    const ReadOptions& read_options, bool use_cache, GetContext* get_context,
    BlockCacheLookupContext* lookup_context,
    CachableEntry<TBlocklike>* filter_block) {
  PERF_TIMER_GUARD(read_filter_block_nanos);

  assert(table);
  assert(filter_block);
  assert(filter_block->IsEmpty());

  const BlockBasedTable::Rep* const rep = table->get_rep();
  assert(rep);

  const Status s = table->RetrieveBlock(
      prefetch_buffer, read_options, rep->filter_handle,
      /* decomp */ nullptr, filter_block, get_context, lookup_context,
      /* for_compaction */ false, use_cache,
      /* async_read */ false, /* use_block_cache_for_lookup */ true);

  return s;
}

template <typename TBlocklike>
const SliceTransform*
FilterBlockReaderCommon<TBlocklike>::table_prefix_extractor() const {
  assert(table_);

  const BlockBasedTable::Rep* const rep = table_->get_rep();
  assert(rep);

  return rep->prefix_filtering ? rep->table_prefix_extractor.get() : nullptr;
}

template <typename TBlocklike>
bool FilterBlockReaderCommon<TBlocklike>::whole_key_filtering() const {
  assert(table_);
  assert(table_->get_rep());

  return table_->get_rep()->whole_key_filtering;
}

template <typename TBlocklike>
bool FilterBlockReaderCommon<TBlocklike>::cache_filter_blocks() const {
  assert(table_);
  assert(table_->get_rep());

  return table_->get_rep()->table_options.cache_index_and_filter_blocks;
}

template <typename TBlocklike>
Status FilterBlockReaderCommon<TBlocklike>::GetOrReadFilterBlock(
    GetContext* get_context, BlockCacheLookupContext* lookup_context,
    CachableEntry<TBlocklike>* filter_block,
    const ReadOptions& read_options) const {
  assert(filter_block);

  if (!filter_block_.IsEmpty()) {
    filter_block->SetUnownedValue(filter_block_.GetValue());
    return Status::OK();
  }

  return ReadFilterBlock(table_, nullptr /* prefetch_buffer */, read_options,
                         cache_filter_blocks(), get_context, lookup_context,
                         filter_block);
}

template <typename TBlocklike>
Status FilterBlockReaderCommon<TBlocklike>::GetFilterBlockInMemory(
    CachableEntry<TBlocklike>* filter_block) const {
  assert(filter_block);
  assert(filter_block->IsEmpty());

  if (!filter_block_.IsEmpty()) {
    filter_block->SetUnownedValue(filter_block_.GetValue());
    return Status::OK();
  }
  // Without cache_index_and_filter_blocks, a filter block that this reader
  // does not hold could only be read from the file.
  if (!cache_filter_blocks()) {
    return Status::OK();
  }
  return table_->LookupBlockInPrimaryCache(table_->get_rep()->filter_handle,
                                           filter_block);
}

template <typename TBlocklike>
size_t FilterBlockReaderCommon<TBlocklike>::ApproximateFilterBlockMemoryUsage()
    const {
  assert(!filter_block_.GetOwnValue() || filter_block_.GetValue() != nullptr);
  return filter_block_.GetOwnValue()
             ? filter_block_.GetValue()->ApproximateMemoryUsage()
             : 0;
}

template <typename TBlocklike>
bool FilterBlockReaderCommon<TBlocklike>::RangeMayExist(
    const Slice* iterate_upper_bound, const Slice& user_key_without_ts,
    const SliceTransform* prefix_extractor, const Comparator* comparator,
    const Slice* const const_ikey_ptr, bool* filter_checked,
    bool need_upper_bound_check, BlockCacheLookupContext* lookup_context,
    const ReadOptions& read_options) {
  if (!prefix_extractor || !prefix_extractor->InDomain(user_key_without_ts)) {
    *filter_checked = false;
    return true;
  }
  Slice prefix = prefix_extractor->Transform(user_key_without_ts);
  if (need_upper_bound_check &&
      !IsFilterCompatible(iterate_upper_bound, prefix, comparator)) {
    *filter_checked = false;
    return true;
  } else {
    *filter_checked = true;
    return PrefixMayMatch(prefix, const_ikey_ptr,
                          /* get_context */ nullptr, lookup_context,
                          read_options);
  }
}

template <typename TBlocklike>
bool FilterBlockReaderCommon<TBlocklike>::IsFilterCompatible(
    const Slice* iterate_upper_bound, const Slice& prefix,
    const Comparator* comparator) const {
  // Try to reuse the bloom filter in the SST table if prefix_extractor in
  // mutable_cf_options has changed. If range [user_key, upper_bound) all
  // share the same prefix then we may still be able to use the bloom filter.
  const SliceTransform* const prefix_extractor = table_prefix_extractor();
  if (iterate_upper_bound != nullptr && prefix_extractor) {
    if (!prefix_extractor->InDomain(*iterate_upper_bound)) {
      return false;
    }
    Slice upper_bound_xform = prefix_extractor->Transform(*iterate_upper_bound);
    // first check if user_key and upper_bound all share the same prefix
    if (comparator->CompareWithoutTimestamp(prefix, false, upper_bound_xform,
                                            false) != 0) {
      // second check if user_key's prefix is the immediate predecessor of
      // upper_bound and have the same length. If so, we know for sure all
      // keys in the range [user_key, upper_bound) share the same prefix.
      // Also need to make sure upper_bound are full length to ensure
      // correctness
      if (!full_length_enabled_ ||
          iterate_upper_bound->size() != prefix_extractor_full_length_ ||
          !comparator->IsSameLengthImmediateSuccessor(prefix,
                                                      *iterate_upper_bound)) {
        return false;
      }
    }
    return true;
  } else {
    return false;
  }
}

template <typename TBlocklike>
void FilterBlockReaderCommon<TBlocklike>::EraseFromCacheBeforeDestruction(
    uint32_t uncache_aggressiveness) {
  if (uncache_aggressiveness > 0) {
    if (filter_block_.IsCached()) {
      filter_block_.ResetEraseIfLastRef();
    } else {
      table()->EraseFromCache(table()->get_rep()->filter_handle);
    }
  }
}

// Explicitly instantiate templates for both "blocklike" types we use.
// This makes it possible to keep the template definitions in the .cc file.
template class FilterBlockReaderCommon<Block_kFilterPartitionIndex>;
template class FilterBlockReaderCommon<ParsedFullFilterBlock>;

}  // namespace ROCKSDB_NAMESPACE
