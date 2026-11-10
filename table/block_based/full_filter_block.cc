//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/block_based/full_filter_block.h"

#include <array>

#include "block_type.h"
#include "monitoring/perf_context_imp.h"
#include "port/malloc.h"
#include "port/port.h"
#include "rocksdb/filter_policy.h"
#include "table/block_based/block_based_table_reader.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

FullFilterBlockBuilder::FullFilterBlockBuilder(
    const SliceTransform* _prefix_extractor, bool whole_key_filtering,
    FilterBitsBuilder* filter_bits_builder)
    : prefix_extractor_(_prefix_extractor),
      whole_key_filtering_(whole_key_filtering) {
  assert(filter_bits_builder != nullptr);
  filter_bits_builder_.reset(filter_bits_builder);
}

size_t FullFilterBlockBuilder::EstimateEntriesAdded() {
  return filter_bits_builder_->EstimateEntriesAdded();
}

void FullFilterBlockBuilder::OnDataBlockFinalized(uint64_t num_data_blocks) {
  UpdateFilterSizeEstimate(num_data_blocks);
}

size_t FullFilterBlockBuilder::CurrentFilterSizeEstimate() {
  return estimated_filter_size_;
}

void FullFilterBlockBuilder::UpdateFilterSizeEstimate(
    uint64_t num_data_blocks) {
  size_t entries_added = filter_bits_builder_->EstimateEntriesAdded();

  if (entries_added == 0) {
    estimated_filter_size_ = 0;
    return;
  }

  size_t filter_size = filter_bits_builder_->CalculateSpace(entries_added);

  // Reserve filter space for next data block ~2x the average.
  size_t buffer_size = 0;
  if (num_data_blocks > 0) {
    buffer_size = (filter_size / num_data_blocks) * 2;
    estimated_filter_size_ = filter_size + buffer_size;
  } else {
    estimated_filter_size_ = filter_size;
  }
}

void FullFilterBlockBuilder::AddWithPrevKey(
    const Slice& key_without_ts, const Slice& /*prev_key_without_ts*/) {
  FullFilterBlockBuilder::Add(key_without_ts);
}

void FullFilterBlockBuilder::Add(const Slice& key_without_ts) {
  if (prefix_extractor_ && prefix_extractor_->InDomain(key_without_ts)) {
    Slice prefix = prefix_extractor_->Transform(key_without_ts);
    if (whole_key_filtering_) {
      filter_bits_builder_->AddKeyAndAlt(key_without_ts, prefix);
    } else {
      filter_bits_builder_->AddKey(prefix);
    }
  } else if (whole_key_filtering_) {
    filter_bits_builder_->AddKey(key_without_ts);
  }
}

Status FullFilterBlockBuilder::Finish(
    const BlockHandle& /*last_partition_block_handle*/, Slice* filter,
    std::unique_ptr<const char[]>* filter_owner) {
  Status s = Status::OK();
  *filter = filter_bits_builder_->Finish(
      filter_owner ? filter_owner : &filter_data_, &s);
  return s;
}

FullFilterBlockReader::FullFilterBlockReader(
    const BlockBasedTable* t,
    CachableEntry<ParsedFullFilterBlock>&& filter_block)
    : FilterBlockReaderCommon(t, std::move(filter_block)) {}

bool FullFilterBlockReader::KeyMayMatch(const Slice& key,
                                        const Slice* const /*const_ikey_ptr*/,
                                        GetContext* get_context,
                                        BlockCacheLookupContext* lookup_context,
                                        const ReadOptions& read_options) {
  if (!whole_key_filtering()) {
    return true;
  }
  return MayMatch(key, get_context, lookup_context, read_options);
}

std::unique_ptr<FilterBlockReader> FullFilterBlockReader::Create(
    const BlockBasedTable* table, const ReadOptions& ro,
    FilePrefetchBuffer* prefetch_buffer, bool use_cache, bool prefetch,
    bool pin, BlockCacheLookupContext* lookup_context) {
  assert(table);
  assert(table->get_rep());
  assert(!pin || prefetch);

  CachableEntry<ParsedFullFilterBlock> filter_block;
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
      new FullFilterBlockReader(table, std::move(filter_block)));
}

bool FullFilterBlockReader::PrefixMayMatch(
    const Slice& prefix, const Slice* const /*const_ikey_ptr*/,
    GetContext* get_context, BlockCacheLookupContext* lookup_context,
    const ReadOptions& read_options) {
  return MayMatch(prefix, get_context, lookup_context, read_options);
}

bool FullFilterBlockReader::MayMatch(const Slice& entry,
                                     GetContext* get_context,
                                     BlockCacheLookupContext* lookup_context,
                                     const ReadOptions& read_options) const {
  CachableEntry<ParsedFullFilterBlock> filter_block;

  const Status s = GetOrReadFilterBlock(get_context, lookup_context,
                                        &filter_block, read_options);
  if (!s.ok()) {
    IGNORE_STATUS_IF_ERROR(s);
    return true;
  }

  assert(filter_block.GetValue());

  FilterBitsReader* const filter_bits_reader =
      filter_block.GetValue()->filter_bits_reader();

  if (filter_bits_reader) {
    if (filter_bits_reader->MayMatch(entry)) {
      PERF_COUNTER_ADD(bloom_sst_hit_count, 1);
      return true;
    } else {
      PERF_COUNTER_ADD(bloom_sst_miss_count, 1);
      return false;
    }
  }
  return true;
}

bool FullFilterBlockReader::KeyMayMatchInMemory(
    const Slice& key, const Slice* const /*const_ikey_ptr*/) {
  if (!whole_key_filtering()) {
    return true;
  }
  return MayMatchInMemory(key);
}

bool FullFilterBlockReader::PrefixMayMatchInMemory(
    const Slice& prefix, const Slice* const /*const_ikey_ptr*/) {
  return MayMatchInMemory(prefix);
}

FilterBitsReader* FullFilterBlockReader::HeldFilterBitsReader() const {
  const ParsedFullFilterBlock* const filter_block = held_filter_block();
  return filter_block == nullptr ? nullptr : filter_block->filter_bits_reader();
}

bool FullFilterBlockReader::MayMatchInMemory(const Slice& entry) const {
  CachableEntry<ParsedFullFilterBlock> filter_block;
  const Status s = GetFilterBlockInMemory(&filter_block);
  if (!s.ok() || filter_block.GetValue() == nullptr) {
    return true;
  }
  FilterBitsReader* const filter_bits_reader =
      filter_block.GetValue()->filter_bits_reader();
  return filter_bits_reader == nullptr || filter_bits_reader->MayMatch(entry);
}

void FullFilterBlockReader::KeysMayMatch(
    MultiGetRange* range, BlockCacheLookupContext* lookup_context,
    const ReadOptions& read_options) {
  if (!whole_key_filtering()) {
    // Simply return. Don't skip any key - consider all keys as likely to be
    // present
    return;
  }
  MayMatch(range, nullptr, lookup_context, read_options);
}

void FullFilterBlockReader::PrefixesMayMatch(
    MultiGetRange* range, const SliceTransform* prefix_extractor,
    BlockCacheLookupContext* lookup_context, const ReadOptions& read_options) {
  MayMatch(range, prefix_extractor, lookup_context, read_options);
}

void FullFilterBlockReader::MayMatch(MultiGetRange* range,
                                     const SliceTransform* prefix_extractor,
                                     BlockCacheLookupContext* lookup_context,
                                     const ReadOptions& read_options) const {
  CachableEntry<ParsedFullFilterBlock> filter_block;

  const Status s = GetOrReadFilterBlock(
      range->begin()->get_context, lookup_context, &filter_block, read_options);
  if (!s.ok()) {
    IGNORE_STATUS_IF_ERROR(s);
    return;
  }

  assert(filter_block.GetValue());

  FilterBitsReader* const filter_bits_reader =
      filter_block.GetValue()->filter_bits_reader();

  if (!filter_bits_reader) {
    return;
  }

  // We need to use an array instead of autovector for may_match since
  // &may_match[0] doesn't work for autovector<bool> (compiler error). So
  // declare both keys and may_match as arrays, which is also slightly less
  // expensive compared to autovector
  std::array<Slice*, MultiGetContext::MAX_BATCH_SIZE> keys;
  std::array<bool, MultiGetContext::MAX_BATCH_SIZE> may_match = {{true}};
  autovector<Slice, MultiGetContext::MAX_BATCH_SIZE> prefixes;
  int num_keys = 0;
  MultiGetRange filter_range(*range, range->begin(), range->end());
  for (auto iter = filter_range.begin(); iter != filter_range.end(); ++iter) {
    if (!prefix_extractor) {
      keys[num_keys++] = &iter->ukey_without_ts;
    } else if (prefix_extractor->InDomain(iter->ukey_without_ts)) {
      prefixes.emplace_back(prefix_extractor->Transform(iter->ukey_without_ts));
      keys[num_keys++] = &prefixes.back();
    } else {
      filter_range.SkipKey(iter);
    }
  }

  filter_bits_reader->MayMatch(num_keys, keys.data(), may_match.data());

  int i = 0;
  for (auto iter = filter_range.begin(); iter != filter_range.end(); ++iter) {
    if (!may_match[i]) {
      // Update original MultiGet range to skip this key. The filter_range
      // was temporarily used just to skip keys not in prefix_extractor domain
      range->SkipKey(iter);
      PERF_COUNTER_ADD(bloom_sst_miss_count, 1);
    } else {
      // PERF_COUNTER_ADD(bloom_sst_hit_count, 1);
      PerfContext* perf_ctx = get_perf_context();
      perf_ctx->bloom_sst_hit_count++;
    }
    ++i;
  }
}

size_t FullFilterBlockReader::ApproximateMemoryUsage() const {
  size_t usage = ApproximateFilterBlockMemoryUsage();
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
  usage += malloc_usable_size(const_cast<FullFilterBlockReader*>(this));
#else
  usage += sizeof(*this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
  return usage;
}

}  // namespace ROCKSDB_NAMESPACE
