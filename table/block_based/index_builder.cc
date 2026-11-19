//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/block_based/index_builder.h"

#include <cassert>
#include <cinttypes>
#include <list>
#include <string>

#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "rocksdb/flush_block_policy.h"
#include "table/block_based/partitioned_filter_block.h"
#include "table/format.h"

namespace ROCKSDB_NAMESPACE {

// Create a index builder based on its type.
IndexBuilder* IndexBuilder::CreateIndexBuilder(
    BlockBasedTableOptions::IndexType index_type,
    const InternalKeyComparator* comparator,
    const InternalKeySliceTransform* int_key_slice_transform,
    const bool use_value_delta_encoding,
    const BlockBasedTableOptions& table_opt, size_t ts_sz,
    const bool persist_user_defined_timestamps, Statistics* statistics,
    bool use_common_prefix_leaf, bool use_common_prefix_top) {
  IndexBuilder* result = nullptr;
  switch (index_type) {
    case BlockBasedTableOptions::kBinarySearch: {
      result = new ShortenedIndexBuilder(
          comparator, table_opt.index_block_restart_interval,
          table_opt.format_version, use_value_delta_encoding,
          table_opt.index_shortening, /* include_first_key */ false, ts_sz,
          persist_user_defined_timestamps, statistics,
          table_opt.uniform_cv_threshold, use_common_prefix_leaf);
      break;
    }
    case BlockBasedTableOptions::kHashSearch: {
      // Currently kHashSearch is incompatible with index_block_restart_interval
      // > 1
      assert(table_opt.index_block_restart_interval == 1);
      result = new HashIndexBuilder(
          comparator, int_key_slice_transform,
          table_opt.index_block_restart_interval, table_opt.format_version,
          use_value_delta_encoding, table_opt.index_shortening, ts_sz,
          persist_user_defined_timestamps, table_opt.uniform_cv_threshold,
          use_common_prefix_leaf);
      break;
    }
    case BlockBasedTableOptions::kTwoLevelIndexSearch: {
      result = PartitionedIndexBuilder::CreateIndexBuilder(
          comparator, use_value_delta_encoding, table_opt, ts_sz,
          persist_user_defined_timestamps, statistics, use_common_prefix_top,
          use_common_prefix_leaf);
      break;
    }
    case BlockBasedTableOptions::kBinarySearchWithFirstKey: {
      result = new ShortenedIndexBuilder(
          comparator, table_opt.index_block_restart_interval,
          table_opt.format_version, use_value_delta_encoding,
          table_opt.index_shortening, /* include_first_key */ true, ts_sz,
          persist_user_defined_timestamps, statistics,
          table_opt.uniform_cv_threshold, use_common_prefix_leaf);
      break;
    }
    default: {
      assert(false && "Do not recognize the index type ");
      break;
    }
  }
  return result;
}

Slice ShortenedIndexBuilder::FindShortestInternalKeySeparator(
    const Comparator& comparator, const Slice& start, const Slice& limit,
    std::string* scratch) {
  // Attempt to shorten the user portion of the key
  Slice user_start = ExtractUserKey(start);
  Slice user_limit = ExtractUserKey(limit);
  scratch->assign(user_start.data(), user_start.size());
  comparator.FindShortestSeparator(scratch, user_limit);
  assert(comparator.Compare(user_start, *scratch) <= 0);
  assert(comparator.Compare(user_start, user_limit) >= 0 ||
         comparator.Compare(*scratch, user_limit) < 0);
  if (scratch->size() <= user_start.size() &&
      comparator.Compare(user_start, *scratch) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(scratch,
               PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(InternalKeyComparator(&comparator).Compare(start, *scratch) < 0);
    assert(InternalKeyComparator(&comparator).Compare(*scratch, limit) < 0);
    return *scratch;
  } else {
    return start;
  }
}

Slice ShortenedIndexBuilder::FindShortInternalKeySuccessor(
    const Comparator& comparator, const Slice& key, std::string* scratch) {
  Slice user_key = ExtractUserKey(key);
  scratch->assign(user_key.data(), user_key.size());
  comparator.FindShortSuccessor(scratch);
  assert(comparator.Compare(user_key, *scratch) <= 0);
  if (scratch->size() <= user_key.size() &&
      comparator.Compare(user_key, *scratch) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(scratch,
               PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(InternalKeyComparator(&comparator).Compare(key, *scratch) < 0);
    return *scratch;
  } else {
    return key;
  }
}

void ShortenedIndexBuilder::UpdateIndexSizeEstimate() {
  uint64_t current_size =
      must_use_separator_with_seq_.LoadRelaxed()
          ? index_block_builder_.CurrentSizeEstimate()
          : index_block_builder_without_seq_.CurrentSizeEstimate();

  uint64_t final_estimate = current_size;
  if (num_index_entries_ > 0) {
    // Add buffer to generously account (in most cases) for the next index entry
    final_estimate += (2 * (current_size / num_index_entries_));
  }
  estimated_index_size_.StoreRelaxed(final_estimate);
}

PartitionedIndexBuilder* PartitionedIndexBuilder::CreateIndexBuilder(
    const InternalKeyComparator* comparator,
    const bool use_value_delta_encoding,
    const BlockBasedTableOptions& table_opt, size_t ts_sz,
    const bool persist_user_defined_timestamps, Statistics* statistics,
    bool use_common_prefix_top, bool use_common_prefix_sub) {
  return new PartitionedIndexBuilder(
      comparator, table_opt, use_value_delta_encoding, ts_sz,
      persist_user_defined_timestamps, statistics, use_common_prefix_top,
      use_common_prefix_sub);
}

PartitionedIndexBuilder::PartitionedIndexBuilder(
    const InternalKeyComparator* comparator,
    const BlockBasedTableOptions& table_opt,
    const bool use_value_delta_encoding, size_t ts_sz,
    const bool persist_user_defined_timestamps, Statistics* statistics,
    bool use_common_prefix_top, bool use_common_prefix_sub)
    : IndexBuilder(comparator, ts_sz, persist_user_defined_timestamps),
      index_block_builder_(
          table_opt.index_block_restart_interval, true /*use_delta_encoding*/,
          use_value_delta_encoding,
          BlockBasedTableOptions::kDataBlockBinarySearch /* index_type */,
          0.75 /* data_block_hash_table_util_ratio */, ts_sz,
          persist_user_defined_timestamps, false /* is_user_key */,
          /*use_separated_kv_storage=*/false, /*statistics=*/nullptr,
          /*uniform_cv_threshold=*/-1.0, use_common_prefix_top),
      index_block_builder_without_seq_(
          table_opt.index_block_restart_interval, true /*use_delta_encoding*/,
          use_value_delta_encoding,
          BlockBasedTableOptions::kDataBlockBinarySearch /* index_type */,
          0.75 /* data_block_hash_table_util_ratio */, ts_sz,
          persist_user_defined_timestamps, true /* is_user_key */,
          /*use_separated_kv_storage=*/false, /*statistics=*/nullptr,
          /*uniform_cv_threshold=*/-1.0, use_common_prefix_top),
      sub_index_builder_(nullptr),
      table_opt_(table_opt),
      // We start by false. After each partition we revise the value based on
      // what the sub_index_builder has decided. If the feature is disabled
      // entirely, this will be set to true after switching the first
      // sub_index_builder. Otherwise, it could be set to true even one of the
      // sub_index_builders could not safely exclude seq from the keys, then it
      // wil be enforced on all sub_index_builders on ::Finish.
      must_use_separator_with_seq_(false),
      use_value_delta_encoding_(use_value_delta_encoding),
      value_delta_escape_(
          FormatVersionUsesValueDeltaEscape(table_opt.format_version)),
      use_common_prefix_sub_(use_common_prefix_sub),
      statistics_(statistics) {
  MakeNewSubIndexBuilder();
}

void PartitionedIndexBuilder::MakeNewSubIndexBuilder() {
  auto new_builder = std::make_unique<ShortenedIndexBuilder>(
      comparator_, table_opt_.index_block_restart_interval,
      table_opt_.format_version, use_value_delta_encoding_,
      table_opt_.index_shortening, /* include_first_key */ false, ts_sz_,
      persist_user_defined_timestamps_, statistics_,
      table_opt_.uniform_cv_threshold, use_common_prefix_sub_);
  sub_index_builder_ = new_builder.get();
  // Start next partition entry, where we will modify the key
  entries_.push_back({{}, std::move(new_builder)});

  BlockBuilder* builder_to_monitor;
  // Set sub_index_builder_->must_use_separator_with_seq_ to true if
  // must_use_separator_with_seq_ is true (internal-key mode) (set to false by
  // default on Creation) so that flush policy can point to
  // sub_index_builder_->index_block_builder_
  if (must_use_separator_with_seq_.LoadRelaxed()) {
    sub_index_builder_->must_use_separator_with_seq_.StoreRelaxed(true);
    builder_to_monitor = &sub_index_builder_->index_block_builder_;
  } else {
    builder_to_monitor = &sub_index_builder_->index_block_builder_without_seq_;
  }

  if (flush_policy_ == nullptr) {
    // Note: some partitions could be sub-optimal since sub_index_builder_
    // could later reset must_use_separator_with_seq_ but the probability and
    // impact of that are low.
    flush_policy_ = NewFlushBlockBySizePolicy(table_opt_.metadata_block_size,
                                              table_opt_.block_size_deviation,
                                              *builder_to_monitor);
  } else {
    flush_policy_->Retarget(*builder_to_monitor);
  }
  partition_cut_requested_ = false;
}

void PartitionedIndexBuilder::RequestPartitionCut() {
  partition_cut_requested_ = true;
}

std::unique_ptr<IndexBuilder::PreparedIndexEntry>
PartitionedIndexBuilder::CreatePreparedIndexEntry() {
  // Fortunately, for ShortenedIndexBuilder, we can prepare an entry from one
  // similarly configured builder and finish it at another.
  return entries_.front().value->CreatePreparedIndexEntry();
}
void PartitionedIndexBuilder::PrepareIndexEntry(
    const Slice& last_key_in_current_block,
    const Slice* first_key_in_next_block, PreparedIndexEntry* out) {
  // Fortunately, for ShortenedIndexBuilder, we can prepare an entry from one
  // similarly configured builder and finish it at another. We just have to
  // keep in mind that this first sub builder keeps track of the original
  // must_use_separator_with_seq_ in the pipeline that is then propagated.
  return entries_.front().value->PrepareIndexEntry(
      last_key_in_current_block, first_key_in_next_block, out);
}

void PartitionedIndexBuilder::MaybeFlush(const Slice& index_key,
                                         const BlockHandle& index_value) {
  bool do_flush = !sub_index_builder_->index_block_builder_.empty() &&
                  (partition_cut_requested_ ||
                   flush_policy_->Update(
                       index_key, EncodedBlockHandle(index_value).AsSlice()));
  if (do_flush) {
    assert(entries_.back().value.get() == sub_index_builder_);

    // Update estimate of completed partitions when a partition is flushed
    estimated_completed_partitions_size_.FetchAddRelaxed(
        sub_index_builder_->CurrentIndexSizeEstimate());

    cut_filter_block = true;
    MakeNewSubIndexBuilder();
  }
}

void PartitionedIndexBuilder::FinishIndexEntry(const BlockHandle& block_handle,
                                               PreparedIndexEntry* base_entry,
                                               bool skip_delta_encoding) {
  using SPIE = ShortenedIndexBuilder::ShortenedPreparedIndexEntry;
  SPIE* entry = static_cast<SPIE*>(base_entry);

  MaybeFlush(entry->separator_with_seq, block_handle);

  sub_index_builder_->FinishIndexEntry(block_handle, base_entry,
                                       skip_delta_encoding);
  std::swap(entries_.back().key, entry->separator_with_seq);

  // Update cached size estimate when data blocks are finalized for more
  // accurate tail size estimation. This is needed for parallel compression
  // which uses FinishIndexEntry() instead of AddIndexEntry().
  UpdateIndexSizeEstimate();

  if (!must_use_separator_with_seq_.LoadRelaxed() &&
      entry->must_use_separator_with_seq) {
    // We need to apply !must_use_separator_with_seq to all sub-index builders
    must_use_separator_with_seq_.StoreRelaxed(true);
    flush_policy_->Retarget(sub_index_builder_->index_block_builder_);
  }
  // NOTE: not compatible with coupled partitioned filters so don't need to
  // cut_filter_block
}

Slice PartitionedIndexBuilder::AddIndexEntry(
    const Slice& last_key_in_current_block,
    const Slice* first_key_in_next_block, const BlockHandle& block_handle,
    std::string* separator_scratch, bool skip_delta_encoding) {
  // At least when running without parallel compression, maintain behavior of
  // avoiding a last index partition with just one entry
  if (first_key_in_next_block) {
    MaybeFlush(last_key_in_current_block, block_handle);
  }

  auto sep = sub_index_builder_->AddIndexEntry(
      last_key_in_current_block, first_key_in_next_block, block_handle,
      separator_scratch, skip_delta_encoding);
  entries_.back().key.assign(sep.data(), sep.size());

  // Update cached size estimate when data blocks are finalized for more
  // accurate tail size estimation. This ensures the estimate reflects current
  // state after each data block is added.
  UpdateIndexSizeEstimate();

  if (!must_use_separator_with_seq_.LoadRelaxed() &&
      sub_index_builder_->must_use_separator_with_seq_.LoadRelaxed()) {
    // We need to apply !must_use_separator_with_seq to all sub-index builders
    must_use_separator_with_seq_.StoreRelaxed(true);
    flush_policy_->Retarget(sub_index_builder_->index_block_builder_);
  }
  if (UNLIKELY(first_key_in_next_block == nullptr)) {
    // no more keys
    cut_filter_block = true;
  }
  return sep;
}

Status PartitionedIndexBuilder::Finish(
    IndexBlocks* index_blocks, const BlockHandle& last_partition_block_handle) {
  if (partition_cnt_ == 0) {
    sub_index_builder_ = nullptr;
    if (!entries_.empty()) {
      // Remove the last entry if it is empty
      if (entries_.back().value->index_block_builder_.empty()) {
        assert(entries_.back().key.empty());
        entries_.pop_back();
      }
      partition_cnt_ = entries_.size();
    }
  }
  if (finishing_indexes_ == true) {
    Entry& last_entry = entries_.front();
    EncodedBlockHandle handle_encoding(last_partition_block_handle);
    std::string handle_delta_encoding;
    // NOTE: must go through IndexValue::EncodeTo, whose encoding depends on
    // format_version; readers decode this block with IndexValue::DecodeFrom.
    // Nothing to delta against for the first entry; it is a restart point
    // (shared == 0), so BlockBuilder writes its value in full and never
    // consults this (empty) delta.
    if (use_value_delta_encoding_ && !last_encoded_handle_.IsNull()) {
      IndexValue(last_partition_block_handle, Slice())
          .EncodeTo(&handle_delta_encoding, /*have_first_key=*/false,
                    &last_encoded_handle_, value_delta_escape_);
    }
    last_encoded_handle_ = last_partition_block_handle;
    const Slice handle_delta_encoding_slice(handle_delta_encoding);
    // NOTE: WriteBatch guarantees keys < 4GB; handle values are also small
    index_block_builder_.Add(last_entry.key, handle_encoding.AsSlice(),
                             handle_delta_encoding_slice);
    if (!must_use_separator_with_seq_.LoadRelaxed()) {
      index_block_builder_without_seq_.Add(ExtractUserKey(last_entry.key),
                                           handle_encoding.AsSlice(),
                                           handle_delta_encoding_slice);
    }
    entries_.pop_front();
  }
  // If there is no sub_index left, then return the 2nd level index.
  if (UNLIKELY(entries_.empty())) {
    if (must_use_separator_with_seq_.LoadRelaxed()) {
      index_blocks->index_block_contents = index_block_builder_.Finish();
      num_uniform_index_blocks_ += index_block_builder_.IsUniform() ? 1 : 0;
    } else {
      index_blocks->index_block_contents =
          index_block_builder_without_seq_.Finish();
      num_uniform_index_blocks_ +=
          index_block_builder_without_seq_.IsUniform() ? 1 : 0;
    }
    top_level_index_size_ = index_blocks->index_block_contents.size();
    index_size_ += top_level_index_size_;
    return Status::OK();
  } else {
    // Finish the next partition index in line and Incomplete() to indicate we
    // expect more calls to Finish
    Entry& entry = entries_.front();
    // Apply the policy to all sub-indexes
    entry.value->must_use_separator_with_seq_.StoreRelaxed(
        must_use_separator_with_seq_.LoadRelaxed());
    auto s = entry.value->Finish(index_blocks);
    num_uniform_index_blocks_ += entry.value->NumUniformIndexBlocks();
    index_size_ += index_blocks->index_block_contents.size();
    finishing_indexes_ = true;
    return s.ok() ? Status::Incomplete() : s;
  }
}

size_t PartitionedIndexBuilder::NumPartitions() const { return partition_cnt_; }

void PartitionedIndexBuilder::UpdateIndexSizeEstimate() {
  uint64_t total_size = 0;

  // Ignore last entry which is a placeholder for the partition being built
  size_t completed_partitions = entries_.size() > 0 ? entries_.size() - 1 : 0;

  // Use running estimate of completed partitions instead of IndexSize() which
  // is only available after calling Finish().
  uint64_t completed_partitions_size =
      estimated_completed_partitions_size_.LoadRelaxed();
  total_size += completed_partitions_size;

  // Add current active partition size if it exists
  uint64_t current_sub_index_size = 0;
  if (sub_index_builder_ != nullptr) {
    current_sub_index_size = sub_index_builder_->CurrentIndexSizeEstimate();
    total_size += current_sub_index_size;
  }

  // Add buffer for top-level index and next partition
  uint64_t buffer_size = 0;
  if (completed_partitions > 0) {
    // Calculate top-level index size. Each top-level entry consists of:
    // separator key (~20-50 bytes) + BlockHandle (~20 bytes) + overhead
    // Estimate ~70 bytes per top-level entry as a reasonable average
    auto estimated_top_level_size = completed_partitions * 70;
    total_size += completed_partitions * 70;

    // Buffer for next partition + next top-level entry
    uint64_t avg_partition_size =
        completed_partitions_size / completed_partitions;
    uint64_t avg_top_level_entry_size =
        estimated_top_level_size / completed_partitions;

    buffer_size = 2 * (avg_partition_size + avg_top_level_entry_size);
    total_size += buffer_size;
  } else if (sub_index_builder_ != nullptr) {
    // For the first partition, estimate using the current partition's state
    buffer_size = 2 * current_sub_index_size;
    total_size += buffer_size;
  }
  estimated_index_size_.StoreRelaxed(total_size);
}

}  // namespace ROCKSDB_NAMESPACE
