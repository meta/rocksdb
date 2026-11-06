//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/block_based/partitioned_filter_block.h"

#include <utility>

#include "block_cache.h"
#include "block_type.h"
#include "file/random_access_file_reader.h"
#include "logging/logging.h"
#include "monitoring/perf_context_imp.h"
#include "port/malloc.h"
#include "port/port.h"
#include "rocksdb/filter_policy.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_reader.h"

namespace ROCKSDB_NAMESPACE {

PartitionedFilterBlockBuilder::PartitionedFilterBlockBuilder(
    const SliceTransform* _prefix_extractor, bool whole_key_filtering,
    FilterBitsBuilder* filter_bits_builder, int index_block_restart_interval,
    const bool use_value_delta_encoding, uint32_t format_version,
    PartitionedIndexBuilder* const partitioned_index_builder,
    const uint32_t partition_size, size_t ts_sz,
    const bool persist_user_defined_timestamps,
    bool decouple_from_index_partitions, bool use_common_prefix)
    : FullFilterBlockBuilder(_prefix_extractor, whole_key_filtering,
                             filter_bits_builder),
      partitioned_index_builder_(partitioned_index_builder),
      ts_sz_(ts_sz),
      decouple_from_index_partitions_(decouple_from_index_partitions),
      index_on_filter_block_builder_(
          index_block_restart_interval, true /*use_delta_encoding*/,
          use_value_delta_encoding,
          BlockBasedTableOptions::kDataBlockBinarySearch /* index_type */,
          0.75 /* data_block_hash_table_util_ratio */, ts_sz,
          persist_user_defined_timestamps, false /* is_user_key */,
          /*use_separated_kv_storage=*/false, /*statistics=*/nullptr,
          /*uniform_cv_threshold=*/-1.0, use_common_prefix),
      index_on_filter_block_builder_without_seq_(
          index_block_restart_interval, true /*use_delta_encoding*/,
          use_value_delta_encoding,
          BlockBasedTableOptions::kDataBlockBinarySearch /* index_type */,
          0.75 /* data_block_hash_table_util_ratio */, ts_sz,
          persist_user_defined_timestamps, true /* is_user_key */,
          /*use_separated_kv_storage=*/false, /*statistics=*/nullptr,
          /*uniform_cv_threshold=*/-1.0, use_common_prefix),
      use_value_delta_encoding_(use_value_delta_encoding),
      value_delta_escape_(FormatVersionUsesValueDeltaEscape(format_version)) {
  // Compute keys_per_partition_
  keys_per_partition_ = static_cast<uint32_t>(
      filter_bits_builder_->ApproximateNumEntries(partition_size));
  if (keys_per_partition_ < 1) {
    // partition_size (minus buffer, ~10%) might be smaller than minimum
    // filter size, sometimes based on cache line size. Try to find that
    // minimum size without CalculateSpace (not necessarily available).
    uint32_t larger = std::max(partition_size + 4, uint32_t{16});
    for (;;) {
      keys_per_partition_ = static_cast<uint32_t>(
          filter_bits_builder_->ApproximateNumEntries(larger));
      if (keys_per_partition_ >= 1) {
        break;
      }
      larger += larger / 4;
      if (larger > 100000) {
        // might be a broken implementation. substitute something reasonable:
        // 1 key / byte.
        keys_per_partition_ = partition_size;
        break;
      }
    }
  }
  if (keys_per_partition_ > 1 && prefix_extractor()) {
    // Correct for adding next prefix in CutAFilterBlock *after* checking
    // against this threshold
    keys_per_partition_--;
  }
}

PartitionedFilterBlockBuilder::~PartitionedFilterBlockBuilder() {
  partitioned_filters_construction_status_.PermitUncheckedError();
}

bool PartitionedFilterBlockBuilder::DecideCutAFilterBlock() {
  size_t added = filter_bits_builder_->EstimateEntriesAdded();
  if (decouple_from_index_partitions_) {
    // NOTE: Can't just use ==, because estimated might be incremented by more
    // than one.
    return added >= keys_per_partition_;
  } else {
    // NOTE: Can't just use ==, because estimated might be incremented by more
    // than one.
    if (added >= keys_per_partition_) {
      // Currently only index builder is in charge of cutting a partition. We
      // keep requesting until it is granted.
      partitioned_index_builder_->RequestPartitionCut();
    }
    return partitioned_index_builder_->ShouldCutFilterBlock();
  }
}

void PartitionedFilterBlockBuilder::CutAFilterBlock(const Slice* next_key,
                                                    const Slice* next_prefix,
                                                    const Slice& prev_key) {
  // When there is a next partition, add the prefix of the first key in the
  // next partition before closing this one out. This is needed to support
  // prefix Seek, because there could exist a key k where
  // * last_key < k < next_key
  // * prefix(last_key) != prefix(k)
  // * prefix(k) == prefix(next_key)
  // * seeking to k lands in this partition, not the next
  // in which case the iterator needs to find next_key despite starting in
  // the partition before it. (This fixes a bug in the original implementation
  // of format_version=3.)
  if (next_prefix) {
    if (whole_key_filtering()) {
      // NOTE: At the end of building filter bits, we need a special case for
      // treating prefix as an "alt" entry. See AddKeyAndAlt() comment. This is
      // a reasonable hack for that.
      filter_bits_builder_->AddKeyAndAlt(*next_prefix, *next_prefix);
    } else {
      filter_bits_builder_->AddKey(*next_prefix);
    }
  }

  // Cut the partition
  total_added_in_built_ += filter_bits_builder_->EstimateEntriesAdded();
  std::unique_ptr<const char[]> filter_data;
  Status filter_construction_status = Status::OK();
  Slice filter =
      filter_bits_builder_->Finish(&filter_data, &filter_construction_status);
  if (filter_construction_status.ok()) {
    filter_construction_status = filter_bits_builder_->MaybePostVerify(filter);
  }
  std::string ikey;
  if (decouple_from_index_partitions_) {
    if (ts_sz_ > 0) {
      AppendKeyWithMinTimestamp(&ikey, prev_key, ts_sz_);
    } else {
      ikey = prev_key.ToString();
    }
    AppendInternalKeyFooter(&ikey, /*seqno*/ 0, ValueType::kTypeDeletion);
  } else {
    ikey = partitioned_index_builder_->GetPartitionKey();
  }
  filters_.push_back({std::move(ikey), std::move(filter_data), filter});
  completed_partitions_size_.FetchAddRelaxed(filter.size());
  partitioned_filters_construction_status_.UpdateIfOk(
      filter_construction_status);

  // If we are building another filter partition, the last prefix in the
  // previous partition should be added to support prefix SeekForPrev.
  // (Analogous to above fix for prefix Seek.)
  if (next_key && prefix_extractor() &&
      prefix_extractor()->InDomain(prev_key)) {
    // NOTE: At the beginning of building filter bits, we don't need a special
    // case for treating prefix as an "alt" entry.
    // See DBBloomFilterTest.FilterBitsBuilderDedup
    filter_bits_builder_->AddKey(prefix_extractor()->Transform(prev_key));
  }
}

void PartitionedFilterBlockBuilder::Add(const Slice& key_without_ts) {
  assert(!DEBUG_add_with_prev_key_called_);
  AddImpl(key_without_ts, prev_key_without_ts_);
  prev_key_without_ts_.assign(key_without_ts.data(), key_without_ts.size());
}

void PartitionedFilterBlockBuilder::AddWithPrevKey(
    const Slice& key_without_ts, const Slice& prev_key_without_ts) {
#ifndef NDEBUG
  if (!DEBUG_add_with_prev_key_called_) {
    assert(prev_key_without_ts.compare(prev_key_without_ts_) == 0);
    DEBUG_add_with_prev_key_called_ = true;
  } else {
    assert(prev_key_without_ts.compare(DEBUG_prev_key_without_ts_) == 0);
  }
  DEBUG_prev_key_without_ts_.assign(key_without_ts.data(),
                                    key_without_ts.size());
#endif
  AddImpl(key_without_ts, prev_key_without_ts);
}

void PartitionedFilterBlockBuilder::AddImpl(const Slice& key_without_ts,
                                            const Slice& prev_key_without_ts) {
  // When filter partitioning is coupled to index partitioning, we need to
  // check for cutting a block even if we aren't adding anything this time.
  bool cut = DecideCutAFilterBlock();
  if (prefix_extractor() && prefix_extractor()->InDomain(key_without_ts)) {
    Slice prefix = prefix_extractor()->Transform(key_without_ts);
    if (cut) {
      CutAFilterBlock(&key_without_ts, &prefix, prev_key_without_ts);
    }
    if (whole_key_filtering()) {
      filter_bits_builder_->AddKeyAndAlt(key_without_ts, prefix);
    } else {
      filter_bits_builder_->AddKey(prefix);
    }
  } else {
    if (cut) {
      CutAFilterBlock(&key_without_ts, nullptr /*no prefix*/,
                      prev_key_without_ts);
    }
    if (whole_key_filtering()) {
      filter_bits_builder_->AddKey(key_without_ts);
    }
  }
}

size_t PartitionedFilterBlockBuilder::EstimateEntriesAdded() {
  return total_added_in_built_ + filter_bits_builder_->EstimateEntriesAdded();
}

size_t PartitionedFilterBlockBuilder::CurrentFilterSizeEstimate() {
  size_t active_partition_size =
      filter_bits_builder_->EstimateEntriesAdded() * 2;  // 2 bytes per key

  return estimated_filter_size_.LoadRelaxed() + active_partition_size;
}

void PartitionedFilterBlockBuilder::OnDataBlockFinalized(
    uint64_t num_data_blocks) {
  UpdateFilterSizeEstimate(num_data_blocks);
}

void PartitionedFilterBlockBuilder::UpdateFilterSizeEstimate(
    uint64_t num_data_blocks) {
  size_t partitions_size = completed_partitions_size_.LoadRelaxed();

  // Reserve space if no partitions have been cut
  size_t active_filter_estimate = 0;
  if (partitions_size == 0) {
    size_t avg_bytes_per_entry =
        2;  // 2 bytes per entry, approx 15 bits per key

    // Estimate using keys_per_partition_ since we expect to cut the first
    // partition once it reaches approx. this many entries.
    active_filter_estimate = keys_per_partition_ * avg_bytes_per_entry;

    // Add a 2x buffer (for top-level index, etc.)
    active_filter_estimate = active_filter_estimate * 2;
  }
  size_t filter_estimate = std::max(partitions_size, active_filter_estimate);

  // Estimate top-level partition index size
  if (partitioned_index_builder_->separator_is_key_plus_seq()) {
    filter_estimate += index_on_filter_block_builder_.CurrentSizeEstimate();
  } else {
    filter_estimate +=
        index_on_filter_block_builder_without_seq_.CurrentSizeEstimate();
  }

  // Reserve filter space for the next data block
  size_t reserved = 0;
  if (num_data_blocks > 0) {
    reserved = (filter_estimate / num_data_blocks) *
               2;  // 2x average size per data block
    estimated_filter_size_.StoreRelaxed(filter_estimate + reserved);
  } else {
    estimated_filter_size_.StoreRelaxed(filter_estimate);
  }
}

void PartitionedFilterBlockBuilder::PrevKeyBeforeFinish(
    const Slice& prev_key_without_ts) {
  assert(prev_key_without_ts.compare(DEBUG_add_with_prev_key_called_
                                         ? DEBUG_prev_key_without_ts_
                                         : prev_key_without_ts_) == 0);
  if (filter_bits_builder_->EstimateEntriesAdded() > 0) {
    CutAFilterBlock(nullptr /*no next key*/, nullptr /*no next prefix*/,
                    prev_key_without_ts);
  }
}

Status PartitionedFilterBlockBuilder::Finish(
    const BlockHandle& last_partition_block_handle, Slice* filter,
    std::unique_ptr<const char[]>* filter_owner) {
  if (finishing_front_filter_) {
    assert(!filters_.empty());
    auto& e = filters_.front();

    assert(last_partition_block_handle != BlockHandle{});
    // Record the handle of the last written filter block in the index
    std::string handle_encoding;
    last_partition_block_handle.EncodeTo(&handle_encoding);
    std::string handle_delta_encoding;
    // NOTE: must go through IndexValue::EncodeTo, whose encoding depends on
    // format_version; readers decode this block with IndexValue::DecodeFrom.
    // Nothing to delta against for the first entry, whose value BlockBuilder
    // writes in full anyway.
    if (use_value_delta_encoding_ && !last_encoded_handle_.IsNull()) {
      IndexValue(last_partition_block_handle, Slice())
          .EncodeTo(&handle_delta_encoding, /*have_first_key=*/false,
                    &last_encoded_handle_, value_delta_escape_);
    }
    last_encoded_handle_ = last_partition_block_handle;
    const Slice handle_delta_encoding_slice(handle_delta_encoding);

    // NOTE: WriteBatch guarantees keys < 4GB; handle values are also small
    index_on_filter_block_builder_.Add(e.ikey, handle_encoding,
                                       handle_delta_encoding_slice);
    if (!partitioned_index_builder_->separator_is_key_plus_seq()) {
      index_on_filter_block_builder_without_seq_.Add(
          ExtractUserKey(e.ikey), handle_encoding, handle_delta_encoding_slice);
    }

    filters_.pop_front();
  } else {
    assert(last_partition_block_handle == BlockHandle{});
    if (filter_bits_builder_->EstimateEntriesAdded() > 0) {
      // PrevKeyBeforeFinish was not called
      assert(!DEBUG_add_with_prev_key_called_);
      CutAFilterBlock(nullptr, nullptr, prev_key_without_ts_);
    }
    // Nothing uncommitted
    assert(filter_bits_builder_->EstimateEntriesAdded() == 0);
  }

  Status s = partitioned_filters_construction_status_;
  assert(!s.IsIncomplete());

  if (s.ok()) {
    // If there is no filter partition left, then return the index on filter
    // partitions
    if (UNLIKELY(filters_.empty())) {
      if (!index_on_filter_block_builder_.empty()) {
        // Simplest to just add them all at the end
        if (partitioned_index_builder_->separator_is_key_plus_seq()) {
          *filter = index_on_filter_block_builder_.Finish();
        } else {
          *filter = index_on_filter_block_builder_without_seq_.Finish();
        }
      } else {
        // This is the rare case where no key was added to the filter
        *filter = Slice{};
      }
    } else {
      // Return the next filter partition in line and set Incomplete() status to
      // indicate we expect more calls to Finish
      s = Status::Incomplete();
      finishing_front_filter_ = true;

      auto& e = filters_.front();
      if (filter_owner != nullptr) {
        *filter_owner = std::move(e.filter_owner);
      }
      *filter = e.filter;
    }
  }
  return s;
}

PartitionedFilterBlockReader::PartitionedFilterBlockReader(
    const BlockBasedTable* t,
    CachableEntry<Block_kFilterPartitionIndex>&& filter_block)
    : FilterBlockReaderCommon(t, std::move(filter_block)) {}

std::unique_ptr<FilterBlockReader> PartitionedFilterBlockReader::Create(
    const BlockBasedTable* table, const ReadOptions& ro,
    FilePrefetchBuffer* prefetch_buffer, bool use_cache, bool prefetch,
    bool pin, BlockCacheLookupContext* lookup_context) {
  assert(table);
  assert(table->get_rep());
  assert(!pin || prefetch);

  CachableEntry<Block_kFilterPartitionIndex> filter_block;
  if (prefetch || !use_cache) {
    const Status s = ReadFilterBlock(table, prefetch_buffer, ro, use_cache,
                                     nullptr /* get_context */, lookup_context,
                                     &filter_block);
    if (!s.ok()) {
      IGNORE_STATUS_IF_ERROR(s);
      return std::unique_ptr<FilterBlockReader>();
    }

    if (use_cache && !pin) {
      filter_block.Reset();
    }
  }

  return std::unique_ptr<FilterBlockReader>(
      new PartitionedFilterBlockReader(table, std::move(filter_block)));
}

bool PartitionedFilterBlockReader::KeyMayMatch(
    const Slice& key, const Slice* const const_ikey_ptr,
    GetContext* get_context, BlockCacheLookupContext* lookup_context,
    const ReadOptions& read_options) {
  assert(const_ikey_ptr != nullptr);
  if (!whole_key_filtering()) {
    return true;
  }

  return MayMatch(key, const_ikey_ptr, get_context, lookup_context,
                  read_options, &FullFilterBlockReader::KeyMayMatch);
}

void PartitionedFilterBlockReader::KeysMayMatch(
    MultiGetRange* range, BlockCacheLookupContext* lookup_context,
    const ReadOptions& read_options) {
  if (!whole_key_filtering()) {
    return;  // Any/all may match
  }

  MayMatch(range, nullptr, lookup_context, read_options,
           &FullFilterBlockReader::KeysMayMatch2);
}

bool PartitionedFilterBlockReader::PrefixMayMatch(
    const Slice& prefix, const Slice* const const_ikey_ptr,
    GetContext* get_context, BlockCacheLookupContext* lookup_context,
    const ReadOptions& read_options) {
  assert(const_ikey_ptr != nullptr);
  return MayMatch(prefix, const_ikey_ptr, get_context, lookup_context,
                  read_options, &FullFilterBlockReader::PrefixMayMatch);
}

void PartitionedFilterBlockReader::PrefixesMayMatch(
    MultiGetRange* range, const SliceTransform* prefix_extractor,
    BlockCacheLookupContext* lookup_context, const ReadOptions& read_options) {
  assert(prefix_extractor);
  MayMatch(range, prefix_extractor, lookup_context, read_options,
           &FullFilterBlockReader::PrefixesMayMatch);
}

void PartitionedFilterBlockReader::NewFilterPartitionIndexIterator(
    const CachableEntry<Block_kFilterPartitionIndex>& filter_block,
    IndexBlockIter* iter) const {
  const InternalKeyComparator* const comparator = internal_comparator();
  Statistics* kNullStats = nullptr;
  filter_block.GetValue()->NewIndexIterator(
      comparator->user_comparator(),
      table()->get_rep()->get_global_seqno(BlockType::kFilterPartitionIndex),
      iter, kNullStats, true /* total_order_seek */, false /* have_first_key */,
      index_key_includes_seq(), index_value_is_full(),
      false /* block_contents_pinned */, user_defined_timestamps_persisted(),
      nullptr /* prefix_index */, BlockBasedTableOptions::kBinary,
      index_value_delta_escape());
}

BlockHandle PartitionedFilterBlockReader::SeekFilterPartitionHandle(
    IndexBlockIter* iter, const Slice& entry) const {
  iter->Seek(entry);
  if (UNLIKELY(!iter->Valid())) {
    // entry is larger than all the keys. However its prefix might still be
    // present in the last partition. If this is called by PrefixMayMatch this
    // is necessary for correct behavior. Otherwise it is unnecessary but safe.
    // Assuming this is an unlikely case for full key search, the performance
    // overhead should be negligible.
    iter->SeekToLast();
  }
  assert(iter->Valid());
  BlockHandle fltr_blk_handle = iter->value().handle;
  return fltr_blk_handle;
}

BlockHandle PartitionedFilterBlockReader::GetFilterPartitionHandle(
    const CachableEntry<Block_kFilterPartitionIndex>& filter_block,
    const Slice& entry) const {
  IndexBlockIter iter;
  NewFilterPartitionIndexIterator(filter_block, &iter);
  return SeekFilterPartitionHandle(&iter, entry);
}

Status PartitionedFilterBlockReader::GetFilterPartitionBlock(
    FilePrefetchBuffer* prefetch_buffer, const BlockHandle& fltr_blk_handle,
    GetContext* get_context, BlockCacheLookupContext* lookup_context,
    const ReadOptions& read_options,
    CachableEntry<ParsedFullFilterBlock>* filter_block) const {
  assert(table());
  assert(filter_block);
  assert(filter_block->IsEmpty());

  if (!filter_map_.empty()) {
    auto iter = filter_map_.find(fltr_blk_handle.offset());
    // This is a possible scenario since block cache might not have had space
    // for the partition
    if (iter != filter_map_.end()) {
      filter_block->SetUnownedValue(iter->second.GetValue());
      return Status::OK();
    }
  }

  // The top-level partition index is timed by ReadFilterBlock(). Filter
  // partitions bypass that helper, so account for their cache or SST reads
  // here.
  PERF_TIMER_GUARD(read_filter_block_nanos);

  const Status s = table()->RetrieveBlock(
      prefetch_buffer, read_options, fltr_blk_handle,
      /* decomp */ nullptr, filter_block, get_context, lookup_context,
      /* for_compaction */ false, /* use_cache */ true,
      /* async_read */ false, /* use_block_cache_for_lookup */ true);

  return s;
}

bool PartitionedFilterBlockReader::MayMatch(
    const Slice& slice, const Slice* const_ikey_ptr, GetContext* get_context,
    BlockCacheLookupContext* lookup_context, const ReadOptions& read_options,
    FilterFunction filter_function) const {
  CachableEntry<Block_kFilterPartitionIndex> filter_block;
  Status s = GetOrReadFilterBlock(get_context, lookup_context, &filter_block,
                                  read_options);
  if (UNLIKELY(!s.ok())) {
    IGNORE_STATUS_IF_ERROR(s);
    return true;
  }

  if (UNLIKELY(filter_block.GetValue()->size() == 0)) {
    return true;
  }

  auto filter_handle = GetFilterPartitionHandle(filter_block, *const_ikey_ptr);
  if (UNLIKELY(filter_handle.size() == 0)) {  // key is out of range
    return false;
  }

  CachableEntry<ParsedFullFilterBlock> filter_partition_block;
  s = GetFilterPartitionBlock(nullptr /* prefetch_buffer */, filter_handle,
                              get_context, lookup_context, read_options,
                              &filter_partition_block);
  if (UNLIKELY(!s.ok())) {
    IGNORE_STATUS_IF_ERROR(s);
    return true;
  }

  FullFilterBlockReader filter_partition(table(),
                                         std::move(filter_partition_block));
  return (filter_partition.*filter_function)(slice, const_ikey_ptr, get_context,
                                             lookup_context, read_options);
}

bool PartitionedFilterBlockReader::KeyMayMatchInMemory(
    const Slice& key, const Slice* const const_ikey_ptr) {
  assert(const_ikey_ptr != nullptr);
  if (!whole_key_filtering()) {
    return true;
  }
  return MayMatchInMemory(key, const_ikey_ptr,
                          &FullFilterBlockReader::KeyMayMatchInMemory);
}

bool PartitionedFilterBlockReader::PrefixMayMatchInMemory(
    const Slice& prefix, const Slice* const const_ikey_ptr) {
  assert(const_ikey_ptr != nullptr);
  return MayMatchInMemory(prefix, const_ikey_ptr,
                          &FullFilterBlockReader::PrefixMayMatchInMemory);
}

bool PartitionedFilterBlockReader::MayMatchInMemory(
    const Slice& slice, const Slice* const_ikey_ptr,
    InMemoryFilterFunction filter_function) const {
  CachableEntry<Block_kFilterPartitionIndex> filter_block;
  Status s = GetFilterBlockInMemory(&filter_block);
  if (!s.ok() || filter_block.GetValue() == nullptr ||
      filter_block.GetValue()->size() == 0) {
    return true;
  }

  const BlockHandle filter_handle =
      GetFilterPartitionHandle(filter_block, *const_ikey_ptr);
  if (UNLIKELY(filter_handle.size() == 0)) {  // key is out of range
    return false;
  }

  CachableEntry<ParsedFullFilterBlock> filter_partition_block;
  const decltype(filter_map_)::const_iterator pinned =
      filter_map_.find(filter_handle.offset());
  if (pinned != filter_map_.end()) {
    filter_partition_block.SetUnownedValue(pinned->second.GetValue());
  } else {
    s = table()->LookupBlockInPrimaryCache(filter_handle,
                                           &filter_partition_block);
    if (!s.ok() || filter_partition_block.GetValue() == nullptr) {
      return true;
    }
  }

  FullFilterBlockReader filter_partition(table(),
                                         std::move(filter_partition_block));
  return (filter_partition.*filter_function)(slice, const_ikey_ptr);
}

void PartitionedFilterBlockReader::MayMatch(
    MultiGetRange* range, const SliceTransform* prefix_extractor,
    BlockCacheLookupContext* lookup_context, const ReadOptions& read_options,
    FilterManyFunction filter_function) const {
  CachableEntry<Block_kFilterPartitionIndex> filter_block;
  Status s = GetOrReadFilterBlock(range->begin()->get_context, lookup_context,
                                  &filter_block, read_options);
  if (UNLIKELY(!s.ok())) {
    IGNORE_STATUS_IF_ERROR(s);
    return;  // Any/all may match
  }

  if (UNLIKELY(filter_block.GetValue()->size() == 0)) {
    return;  // Any/all may match
  }

  IndexBlockIter filter_index_iter;
  NewFilterPartitionIndexIterator(filter_block, &filter_index_iter);

  auto start_iter_same_handle = range->begin();
  BlockHandle prev_filter_handle = BlockHandle::NullBlockHandle();

  // For all keys mapping to same partition (must be adjacent in sorted order)
  // share block cache lookup and use full filter multiget on the partition
  // filter.
  for (auto iter = start_iter_same_handle; iter != range->end(); ++iter) {
    BlockHandle this_filter_handle =
        SeekFilterPartitionHandle(&filter_index_iter, iter->ikey);
    if (!prev_filter_handle.IsNull() &&
        this_filter_handle != prev_filter_handle) {
      MultiGetRange subrange(*range, start_iter_same_handle, iter);
      MayMatchPartition(&subrange, prefix_extractor, prev_filter_handle,
                        lookup_context, read_options, filter_function);
      range->AddSkipsFrom(subrange);
      start_iter_same_handle = iter;
    }
    if (UNLIKELY(this_filter_handle.size() == 0)) {
      // Empty filter partition. Keys outside the prefix extractor's domain
      // are never added to a prefix filter, so only other keys can be ruled
      // out.
      if (prefix_extractor == nullptr ||
          prefix_extractor->InDomain(iter->ukey_without_ts)) {
        range->SkipKey(iter);
      }
      prev_filter_handle = BlockHandle::NullBlockHandle();
    } else {
      prev_filter_handle = this_filter_handle;
    }
  }
  if (!prev_filter_handle.IsNull()) {
    MultiGetRange subrange(*range, start_iter_same_handle, range->end());
    MayMatchPartition(&subrange, prefix_extractor, prev_filter_handle,
                      lookup_context, read_options, filter_function);
    range->AddSkipsFrom(subrange);
  }
}

void PartitionedFilterBlockReader::MayMatchPartition(
    MultiGetRange* range, const SliceTransform* prefix_extractor,
    BlockHandle filter_handle, BlockCacheLookupContext* lookup_context,
    const ReadOptions& read_options, FilterManyFunction filter_function) const {
  CachableEntry<ParsedFullFilterBlock> filter_partition_block;
  Status s = GetFilterPartitionBlock(
      nullptr /* prefetch_buffer */, filter_handle, range->begin()->get_context,
      lookup_context, read_options, &filter_partition_block);
  if (UNLIKELY(!s.ok())) {
    IGNORE_STATUS_IF_ERROR(s);
    return;  // Any/all may match
  }

  FullFilterBlockReader filter_partition(table(),
                                         std::move(filter_partition_block));
  (filter_partition.*filter_function)(range, prefix_extractor, lookup_context,
                                      read_options);
}

size_t PartitionedFilterBlockReader::ApproximateMemoryUsage() const {
  size_t usage = ApproximateFilterBlockMemoryUsage();
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
  usage += malloc_usable_size(const_cast<PartitionedFilterBlockReader*>(this));
#else
  usage += sizeof(*this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
  return usage;
  // TODO(myabandeh): better estimation for filter_map_ size
}

// TODO(myabandeh): merge this with the same function in IndexReader
Status PartitionedFilterBlockReader::CacheDependencies(
    const ReadOptions& ro, bool pin, FilePrefetchBuffer* tail_prefetch_buffer) {
  assert(table());

  const BlockBasedTable::Rep* const rep = table()->get_rep();
  assert(rep);

  BlockCacheLookupContext lookup_context{TableReaderCaller::kPrefetch};

  CachableEntry<Block_kFilterPartitionIndex> filter_block;

  Status s = GetOrReadFilterBlock(nullptr /* get_context */, &lookup_context,
                                  &filter_block, ro);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(rep->ioptions.logger,
                    "Error retrieving top-level filter block while trying to "
                    "cache filter partitions: %s",
                    s.ToString().c_str());
    return s;
  }

  // Before read partitions, prefetch them to avoid lots of IOs
  assert(filter_block.GetValue());

  IndexBlockIter biter;
  const InternalKeyComparator* const comparator = internal_comparator();
  Statistics* kNullStats = nullptr;
  filter_block.GetValue()->NewIndexIterator(
      comparator->user_comparator(),
      rep->get_global_seqno(BlockType::kFilterPartitionIndex), &biter,
      kNullStats, true /* total_order_seek */, false /* have_first_key */,
      index_key_includes_seq(), index_value_is_full(),
      false /* block_contents_pinned */, user_defined_timestamps_persisted(),
      nullptr /* prefix_index */, BlockBasedTableOptions::kBinary,
      index_value_delta_escape());
  // Index partitions are assumed to be consecuitive. Prefetch them all.
  // Read the first block offset
  biter.SeekToFirst();
  BlockHandle handle = biter.value().handle;
  uint64_t prefetch_off = handle.offset();

  // Read the last block's offset
  biter.SeekToLast();
  handle = biter.value().handle;
  uint64_t last_off =
      handle.offset() + handle.size() + BlockBasedTable::kBlockTrailerSize;
  uint64_t prefetch_len = last_off - prefetch_off;
  std::unique_ptr<FilePrefetchBuffer> prefetch_buffer;
  if (tail_prefetch_buffer == nullptr || !tail_prefetch_buffer->Enabled() ||
      tail_prefetch_buffer->GetPrefetchOffset() > prefetch_off) {
    rep->CreateFilePrefetchBuffer(ReadaheadParams(), &prefetch_buffer,
                                  /*readaheadsize_cb*/ nullptr,
                                  /*usage=*/FilePrefetchBufferUsage::kUnknown);

    IOOptions opts;
    IODebugContext dbg;
    s = rep->file->PrepareIOOptions(ro, opts, &dbg);
    if (s.ok()) {
      s = prefetch_buffer->Prefetch(opts, rep->file.get(), prefetch_off,
                                    static_cast<size_t>(prefetch_len));
    }
    if (!s.ok()) {
      return s;
    }
  }
  // After prefetch, read the partitions one by one
  for (biter.SeekToFirst(); biter.Valid(); biter.Next()) {
    handle = biter.value().handle;

    CachableEntry<ParsedFullFilterBlock> block;
    // TODO: Support counter batch update for partitioned index and
    // filter blocks
    s = table()->MaybeReadBlockAndLoadToCache(
        prefetch_buffer ? prefetch_buffer.get() : tail_prefetch_buffer, ro,
        handle, /* dict */ nullptr,
        /* for_compaction */ false, &block, nullptr /* get_context */,
        &lookup_context, nullptr /* contents */, false,
        /* use_block_cache_for_lookup */ true);
    if (!s.ok()) {
      return s;
    }
    assert(s.ok() || block.GetValue() == nullptr);

    if (block.GetValue() != nullptr) {
      if (block.IsCached()) {
        if (pin) {
          filter_map_[handle.offset()] = std::move(block);
        }
      }
    }
  }
  return biter.status();
}

void PartitionedFilterBlockReader::EraseFromCacheBeforeDestruction(
    uint32_t uncache_aggressiveness) {
  // NOTE: essentially a copy of
  // PartitionIndexReader::EraseFromCacheBeforeDestruction
  if (uncache_aggressiveness > 0) {
    CachableEntry<Block_kFilterPartitionIndex> top_level_block;

    ReadOptions ro;
    ro.read_tier = ReadTier::kBlockCacheTier;
    GetOrReadFilterBlock(/*get_context=*/nullptr,
                         /*lookup_context=*/nullptr, &top_level_block, ro)
        .PermitUncheckedError();

    if (!filter_map_.empty()) {
      // All partitions present if any
      for (auto& e : filter_map_) {
        e.second.ResetEraseIfLastRef();
      }
    } else if (!top_level_block.IsEmpty()) {
      IndexBlockIter biter;
      const InternalKeyComparator* const comparator = internal_comparator();
      Statistics* kNullStats = nullptr;
      top_level_block.GetValue()->NewIndexIterator(
          comparator->user_comparator(),
          table()->get_rep()->get_global_seqno(
              BlockType::kFilterPartitionIndex),
          &biter, kNullStats, true /* total_order_seek */,
          false /* have_first_key */, index_key_includes_seq(),
          index_value_is_full(), false /* block_contents_pinned */,
          user_defined_timestamps_persisted(), nullptr /* prefix_index */,
          BlockBasedTableOptions::kBinary, index_value_delta_escape());

      UncacheAggressivenessAdvisor advisor(uncache_aggressiveness);
      for (biter.SeekToFirst(); biter.Valid() && advisor.ShouldContinue();
           biter.Next()) {
        bool erased = table()->EraseFromCache(biter.value().handle);
        advisor.Report(erased);
      }
      biter.status().PermitUncheckedError();
    }
    top_level_block.ResetEraseIfLastRef();
  }
  // Might be needed to un-cache a pinned top-level block
  FilterBlockReaderCommon<Block_kFilterPartitionIndex>::
      EraseFromCacheBeforeDestruction(uncache_aggressiveness);
}

const InternalKeyComparator* PartitionedFilterBlockReader::internal_comparator()
    const {
  assert(table());
  assert(table()->get_rep());

  return &table()->get_rep()->internal_comparator;
}

bool PartitionedFilterBlockReader::index_key_includes_seq() const {
  assert(table());
  assert(table()->get_rep());

  return table()->get_rep()->index_key_includes_seq;
}

bool PartitionedFilterBlockReader::index_value_is_full() const {
  assert(table());
  assert(table()->get_rep());

  return table()->get_rep()->index_value_is_full;
}

bool PartitionedFilterBlockReader::index_value_delta_escape() const {
  assert(table());
  assert(table()->get_rep());

  return FormatVersionUsesValueDeltaEscape(
      table()->get_rep()->footer.format_version());
}

bool PartitionedFilterBlockReader::user_defined_timestamps_persisted() const {
  assert(table());
  assert(table()->get_rep());

  return table()->get_rep()->user_defined_timestamps_persisted;
}
}  // namespace ROCKSDB_NAMESPACE
