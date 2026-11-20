//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#pragma once

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <vector>

#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "table/format.h"
#include "table/multiget_context.h"
#include "trace_replay/block_cache_tracer.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

const uint64_t kNotValid = ULLONG_MAX;
class FilterPolicy;

class GetContext;
using MultiGetRange = MultiGetContext::Range;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table, or partitioned into smaller filters.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      Add* Finish
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder() {}
  // No copying allowed
  FilterBlockBuilder(const FilterBlockBuilder&) = delete;
  void operator=(const FilterBlockBuilder&) = delete;
  FilterBlockBuilder(FilterBlockBuilder&&) = delete;
  void operator=(FilterBlockBuilder&&) = delete;

  virtual ~FilterBlockBuilder() {}

  // Add a key to current filter.
  virtual void Add(const Slice& key_without_ts) = 0;
  // A potentially more efficient version of Add(), though you cannot go back
  // to Add() after using AddWithPrevKey() on a FilterBlockBuilder.
  // prev_key_without_ts should be the empty Slice for the first key added
  // (regardless of comparator; e.g. for bootstrapping delta encoding).
  // More detail: The previous key is used when filters are key-range
  // partitioned, and the PartitionedFilterBlockBuilder doesn't need to buffer
  // the previous key when it is provided by calling this function.
  virtual void AddWithPrevKey(const Slice& key_without_ts,
                              const Slice& /*prev_key_without_ts*/) = 0;

  virtual bool IsEmpty() const = 0;  // Empty == none added
  // For reporting stats on how many entries the builder considered unique
  virtual size_t EstimateEntriesAdded() = 0;

  // Returns an estimate of the current filter size based on the builder's
  // state. Implementations should cache the estimate and update it via
  // UpdateFilterSizeEstimate() to avoid recalculating on every key add.
  //
  // Can be called at any time during table construction, even before calling
  // Finish(). Used during table construction to determine when to cut files.
  virtual size_t CurrentFilterSizeEstimate() = 0;

  // Provides a hook for filter builder when a data block is finalized, such as
  // to update cached filter size estimates.
  virtual void OnDataBlockFinalized(uint64_t /* num_data_blocks */) {}

  // When using AddWithPrevKey, this must be called before Finish(). (May also
  // be called without AddWithPrevKey, but prev_key_without_ts must be
  // accurate regardless.)
  virtual void PrevKeyBeforeFinish(const Slice& /*prev_key_without_ts*/) {}

  // Generate a filter block. Returns OK if finished, or Incomplete if more
  // filters are needed (partitioned filter). In the latter case, subsequent
  // calls require the BlockHandle of the most recently generated and written
  // filter, in last_partition_block_handle.
  //
  // If filter_owner is not nullptr, Finish() may transfer ownership of
  // underlying filter data to the caller,  so that it can be freed as soon as
  // possible. BlockBasedFilterBlock will ignore this parameter.
  //
  // For either OK or Incomplete, *filter is set to point to the next filter
  // bytes, which survive until either this is destroyed, *filter_owner is
  // destroyed, or next call to Finish.
  virtual Status Finish(
      const BlockHandle& last_partition_block_handle, Slice* filter,
      std::unique_ptr<const char[]>* filter_owner = nullptr) = 0;

  // This is called when finishes using the FilterBitsBuilder
  // in order to release memory usage and cache charge
  // associated with it timely
  virtual void ResetFilterBitsBuilder() {}

  // To optionally post-verify the filter returned from
  // FilterBlockBuilder::Finish.
  // Return Status::OK() if skipped.
  virtual Status MaybePostVerifyFilter(const Slice& /* filter_content */) {
    return Status::OK();
  }

#ifndef NDEBUG
  Slice TEST_Finish() {  // Generate Filter
    const BlockHandle empty_handle;
    Slice filter;
    Status status = Finish(empty_handle, &filter);
    assert(status.ok());
    return filter;
  }
#endif  // NDEBUG

 protected:
  // Update cached filter size estimate. Subclasses should override to update
  // estimates based on their internal state.
  virtual void UpdateFilterSizeEstimate(uint64_t /* num_data_blocks */) {}
};

// A FilterBlockReader is used to parse filter from SST table.
// KeyMayMatch and PrefixMayMatch would trigger filter checking
//
// BlockBased/Full FilterBlock would be called in the same way.
class FilterBlockReader {
 public:
  FilterBlockReader() = default;
  virtual ~FilterBlockReader() = default;

  FilterBlockReader(const FilterBlockReader&) = delete;
  FilterBlockReader& operator=(const FilterBlockReader&) = delete;
  FilterBlockReader(FilterBlockReader&&) = delete;
  FilterBlockReader& operator=(FilterBlockReader&&) = delete;

  /**
   * Normally filters are built on only the user keys and the InternalKey is not
   * needed for a query. The index in PartitionedFilterBlockReader however is
   * built upon InternalKey and must be provided via const_ikey_ptr when running
   * queries.
   */
  virtual bool KeyMayMatch(const Slice& key, const Slice* const const_ikey_ptr,
                           GetContext* get_context,
                           BlockCacheLookupContext* lookup_context,
                           const ReadOptions& read_options) = 0;

  virtual void KeysMayMatch(MultiGetRange* range,
                            BlockCacheLookupContext* lookup_context,
                            const ReadOptions& read_options) {
    for (auto iter = range->begin(); iter != range->end(); ++iter) {
      const Slice ukey_without_ts = iter->ukey_without_ts;
      const Slice ikey = iter->ikey;
      GetContext* const get_context = iter->get_context;
      if (!KeyMayMatch(ukey_without_ts, &ikey, get_context, lookup_context,
                       read_options)) {
        range->SkipKey(iter);
      }
    }
  }

  /**
   * Similar to KeyMayMatch
   */
  virtual bool PrefixMayMatch(const Slice& prefix,
                              const Slice* const const_ikey_ptr,
                              GetContext* get_context,
                              BlockCacheLookupContext* lookup_context,
                              const ReadOptions& read_options) = 0;

  virtual void PrefixesMayMatch(MultiGetRange* range,
                                const SliceTransform* prefix_extractor,
                                BlockCacheLookupContext* lookup_context,
                                const ReadOptions& read_options) {
    for (auto iter = range->begin(); iter != range->end(); ++iter) {
      const Slice ukey_without_ts = iter->ukey_without_ts;
      const Slice ikey = iter->ikey;
      GetContext* const get_context = iter->get_context;
      if (prefix_extractor->InDomain(ukey_without_ts) &&
          !PrefixMayMatch(prefix_extractor->Transform(ukey_without_ts), &ikey,
                          get_context, lookup_context, read_options)) {
        range->SkipKey(iter);
      }
    }
  }

  virtual size_t ApproximateMemoryUsage() const = 0;

  // convert this object to a human readable form
  virtual std::string ToString() const {
    std::string error_msg("Unsupported filter \n");
    return error_msg;
  }

  virtual Status CacheDependencies(
      const ReadOptions& /*ro*/, bool /*pin*/,
      FilePrefetchBuffer* /* tail_prefetch_buffer */) {
    return Status::OK();
  }

  virtual void EraseFromCacheBeforeDestruction(
      uint32_t /*uncache_aggressiveness*/) {}

  virtual bool RangeMayExist(const Slice* /*iterate_upper_bound*/,
                             const Slice& user_key_without_ts,
                             const SliceTransform* prefix_extractor,
                             const Comparator* /*comparator*/,
                             const Slice* const const_ikey_ptr,
                             bool* filter_checked, bool need_upper_bound_check,
                             BlockCacheLookupContext* lookup_context,
                             const ReadOptions& read_options) = 0;
};

}  // namespace ROCKSDB_NAMESPACE
