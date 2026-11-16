//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#include <deque>

#include "db/seqno_to_time_mapping.h"
#include "rocksdb/io_dispatcher.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_prefetcher.h"
#include "table/block_based/multi_scan_index_iterator.h"
#include "table/block_based/reader_common.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {
// Iterates over the contents of BlockBasedTable.
class BlockBasedTableIterator : public InternalIteratorBase<Slice> {
  // compaction_readahead_size: its value will only be used if for_compaction =
  // true
  // @param read_options Must outlive this iterator.
 public:
  BlockBasedTableIterator(
      const BlockBasedTable* table, const ReadOptions& read_options,
      const InternalKeyComparator& icomp,
      std::unique_ptr<InternalIteratorBase<IndexValue>>&& index_iter,
      bool check_filter, bool need_upper_bound_check,
      const SliceTransform* prefix_extractor, TableReaderCaller caller,
      size_t compaction_readahead_size = 0, bool allow_unprepared_value = false)
      : index_iter_(std::move(index_iter)),
        table_(table),
        read_options_(read_options),
        icomp_(icomp),
        user_comparator_(icomp.user_comparator()),
        pinned_iters_mgr_(nullptr),
        prefix_extractor_(prefix_extractor),
        lookup_context_(caller),
        block_prefetcher_(
            compaction_readahead_size,
            table_->get_rep()->table_options.initial_auto_readahead_size),
        allow_unprepared_value_(allow_unprepared_value),
        check_filter_(check_filter),
        need_upper_bound_check_(need_upper_bound_check),
        async_read_in_progress_(false),
        is_last_level_(table->IsLastLevel()),
        block_iter_points_to_real_block_(false) {
    multi_scan_status_.PermitUncheckedError();
  }
  // No copying allowed
  BlockBasedTableIterator(const BlockBasedTableIterator&) = delete;
  BlockBasedTableIterator& operator=(const BlockBasedTableIterator&) = delete;
  BlockBasedTableIterator(BlockBasedTableIterator&&) = delete;
  BlockBasedTableIterator& operator=(BlockBasedTableIterator&&) = delete;

  ~BlockBasedTableIterator() override { ClearBlockHandles(); }

  void Seek(const Slice& target) override;
  void SeekForPrev(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() final;
  bool NextAndGetResult(IterateResult* result) override;
  void Prev() override;
  bool Valid() const override {
    return !is_out_of_bound_ && multi_scan_status_.ok() &&
           (is_at_first_key_from_index_ ||
            (block_iter_points_to_real_block_ && block_iter_.Valid()));
  }

  // For block cache readahead lookup scenario -
  // If is_at_first_key_from_index_ is true, InitDataBlock hasn't been
  // called. It means block_handles is empty and index_ point to current block.
  // So index_iter_ can be accessed directly.
  Slice key() const override {
    assert(Valid());
    if (is_at_first_key_from_index_) {
      assert(!multi_scan_read_set_);
      return index_iter_->value().first_internal_key;
    } else {
      return block_iter_.key();
    }
  }
  Slice user_key() const override {
    assert(Valid());
    if (is_at_first_key_from_index_) {
      return ExtractUserKey(index_iter_->value().first_internal_key);
    } else {
      return block_iter_.user_key();
    }
  }

  bool PrepareValue() override {
    assert(Valid());

    if (!is_at_first_key_from_index_) {
      return true;
    }

    return const_cast<BlockBasedTableIterator*>(this)
        ->MaterializeCurrentBlock();
  }

  uint64_t write_unix_time() const override {
    assert(Valid());
    ParsedInternalKey pikey;
    SequenceNumber seqno;
    const SeqnoToTimeMapping& seqno_to_time_mapping =
        table_->GetSeqnoToTimeMapping();
    Status s = ParseInternalKey(key(), &pikey, /*log_err_key=*/false);
    if (!s.ok()) {
      return std::numeric_limits<uint64_t>::max();
    } else if (kUnknownSeqnoBeforeAll == pikey.sequence) {
      return kUnknownTimeBeforeAll;
    } else if (seqno_to_time_mapping.Empty()) {
      return std::numeric_limits<uint64_t>::max();
    } else if (kTypeValuePreferredSeqno == pikey.type) {
      seqno = ParsePackedValueForSeqno(value());
    } else {
      seqno = pikey.sequence;
    }
    return seqno_to_time_mapping.GetProximalTimeBeforeSeqno(seqno);
  }

  Slice value() const override {
    // PrepareValue() must have been called.
    assert(!is_at_first_key_from_index_);
    assert(Valid());

    if (seek_stat_state_ & kReportOnUseful) {
      bool filter_used = (seek_stat_state_ & kFilterUsed) != 0;
      RecordTick(
          table_->GetStatistics(),
          filter_used
              ? (is_last_level_ ? LAST_LEVEL_SEEK_DATA_USEFUL_FILTER_MATCH
                                : NON_LAST_LEVEL_SEEK_DATA_USEFUL_FILTER_MATCH)
              : (is_last_level_ ? LAST_LEVEL_SEEK_DATA_USEFUL_NO_FILTER
                                : NON_LAST_LEVEL_SEEK_DATA_USEFUL_NO_FILTER));
      seek_stat_state_ = kDataBlockReadSinceLastSeek;
    }

    return block_iter_.value();
  }
  Status status() const override {
    if (!multi_scan_status_.ok()) {
      return multi_scan_status_;
    }
    // In case of block cache readahead lookup, it won't add the block to
    // block_handles if it's index is invalid. So index_iter_->status check can
    // be skipped.
    // Prefix index set status to NotFound when the prefix does not exist.
    if (IsIndexAtCurr() && !index_iter_->status().ok() &&
        !index_iter_->status().IsNotFound()) {
      return index_iter_->status();
    } else if (block_iter_points_to_real_block_) {
      // This is the common case.
      return block_iter_.status();
    } else if (async_read_in_progress_) {
      assert(!multi_scan_read_set_);
      return Status::TryAgain("Async read in progress");
    } else {
      return Status::OK();
    }
  }

  inline IterBoundCheck UpperBoundCheckResult() override {
    if (is_out_of_bound_) {
      return IterBoundCheck::kOutOfBound;
    } else if (block_upper_bound_check_ ==
               BlockUpperBound::kUpperBoundBeyondCurBlock) {
      assert(!is_out_of_bound_);
      return IterBoundCheck::kInbound;
    } else {
      return IterBoundCheck::kUnknown;
    }
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
  }
  bool IsKeyPinned() const override {
    // Our key comes either from block_iter_'s current key
    // or index_iter_'s current *value*.
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           ((is_at_first_key_from_index_ && index_iter_->IsValuePinned()) ||
            (block_iter_points_to_real_block_ && block_iter_.IsKeyPinned()));
  }
  bool IsValuePinned() const override {
    assert(!is_at_first_key_from_index_);
    assert(Valid());

    // BlockIter::IsValuePinned() is always true. No need to check
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           block_iter_points_to_real_block_;
  }

  // Pinning this iterator keeps alive the reader (through a table cache
  // handle), the current block, and any index block a key() may point into.
  // In an immortal table the reader outlives the Version anyway, and a block
  // read from its source (such as mmap) stays valid without the Block object;
  // so if nothing else is held, deleting the iterator frees nothing a returned
  // slice points into. MultiScan is excluded since the iterator owns its
  // ReadSet.
  bool IsSafeToDropWhilePinned() const override {
    const BlockBasedTable::Rep* rep = table_->get_rep();
    return rep->immortal_table && !rep->index_has_first_key && !HasCleanups() &&
           !multi_scan_read_set_ &&
           (!block_iter_points_to_real_block_ ||
            IsDataBlockFromImmortalSource());
  }

  void ResetDataIter() {
    if (block_iter_points_to_real_block_) {
      if (pinned_iters_mgr_ != nullptr && pinned_iters_mgr_->PinningEnabled() &&
          !IsDataBlockFromImmortalSource()) {
        TEST_SYNC_POINT("BlockBasedTableIterator::ResetDataIter:DelegateBlock");
        block_iter_.DelegateCleanupsTo(pinned_iters_mgr_);
      }
      block_iter_.Invalidate(Status::OK());
      block_iter_points_to_real_block_ = false;
    }
    block_upper_bound_check_ = BlockUpperBound::kUnknown;
  }

  void SavePrevIndexValue() {
    if (block_iter_points_to_real_block_ && IsIndexAtCurr()) {
      // Reseek. If they end up with the same data block, we shouldn't re-fetch
      // the same data block.
      prev_block_offset_ = index_iter_->value().handle.offset();
    }
  }

  void GetReadaheadState(ReadaheadFileInfo* readahead_file_info) override {
    if (block_prefetcher_.prefetch_buffer() != nullptr &&
        read_options_.adaptive_readahead) {
      block_prefetcher_.prefetch_buffer()->GetReadaheadState(
          &(readahead_file_info->data_block_readahead_info));
      if (index_iter_) {
        index_iter_->GetReadaheadState(readahead_file_info);
      }
    }
  }

  void SetReadaheadState(ReadaheadFileInfo* readahead_file_info) override {
    if (read_options_.adaptive_readahead) {
      block_prefetcher_.SetReadaheadState(
          &(readahead_file_info->data_block_readahead_info));
      if (index_iter_) {
        index_iter_->SetReadaheadState(readahead_file_info);
      }
    }
  }

  void Prepare(const MultiScanArgs* scan_opts) override;

  FilePrefetchBuffer* prefetch_buffer() {
    return block_prefetcher_.prefetch_buffer();
  }

  std::unique_ptr<InternalIteratorBase<IndexValue>> index_iter_;

  bool TEST_IsBlockPinnedByMultiScan(size_t block_idx) {
    if (!multi_scan_read_set_) {
      return false;
    }
    return multi_scan_read_set_->IsBlockAvailable(block_idx);
  }

 private:
  // Pinned contents without a cache handle means an unowned block of an
  // immortal table (see NewDataBlockIterator): the bytes belong to the
  // reader's source and outlive the Block object.
  bool IsDataBlockFromImmortalSource() const {
    return block_iter_.IsValuePinned() && block_iter_.cache_handle() == nullptr;
  }

  enum class IterDirection {
    kForward,
    kBackward,
  };
  // This enum indicates whether the upper bound falls into current block
  // or beyond.
  //   +-------------+
  //   |  cur block  |       <-- (1)
  //   +-------------+
  //                         <-- (2)
  //  --- <boundary key> ---
  //                         <-- (3)
  //   +-------------+
  //   |  next block |       <-- (4)
  //        ......
  //
  // When the block is smaller than <boundary key>, kUpperBoundInCurBlock
  // is the value to use. The examples are (1) or (2) in the graph. It means
  // all keys in the next block or beyond will be out of bound. Keys within
  // the current block may or may not be out of bound.
  // When the block is larger or equal to <boundary key>,
  // kUpperBoundBeyondCurBlock is to be used. The examples are (3) and (4)
  // in the graph. It means that all keys in the current block is within the
  // upper bound and keys in the next block may or may not be within the uppder
  // bound.
  // If the boundary key hasn't been checked against the upper bound,
  // kUnknown can be used.
  enum class BlockUpperBound : uint8_t {
    kUpperBoundInCurBlock,
    kUpperBoundBeyondCurBlock,
    kUnknown,
  };

  // State bits for collecting stats on seeks and whether they returned useful
  // results.
  enum SeekStatState : uint8_t {
    kNone = 0,
    // Most recent seek checked prefix filter (or similar future feature)
    kFilterUsed = 1 << 0,
    // Already recorded that a data block was accessed since the last seek.
    kDataBlockReadSinceLastSeek = 1 << 1,
    // Have not yet recorded that a value() was accessed.
    kReportOnUseful = 1 << 2,
  };

  // BlockHandleInfo is used to store the info needed when block cache lookup
  // ahead is enabled to tune readahead_size.
  struct BlockHandleInfo {
    void SetFirstInternalKey(const Slice& key) {
      if (key.empty()) {
        return;
      }
      size_t size = key.size();
      buf_ = std::unique_ptr<char[]>(new char[size]);
      memcpy(buf_.get(), key.data(), size);
      first_internal_key_ = Slice(buf_.get(), size);
    }

    BlockHandle handle_;
    bool is_cache_hit_ = false;
    CachableEntry<Block> cachable_entry_;
    Slice first_internal_key_;
    std::unique_ptr<char[]> buf_;
  };

  bool IsIndexAtCurr() const { return is_index_at_curr_block_; }

  inline bool IsReverseMultiScan() const {
    return multi_scan_read_set_ && multi_scan_index_iter_ &&
           multi_scan_index_iter_->scan_opts()->reverse;
  }

  const BlockBasedTable* table_;
  const ReadOptions& read_options_;
  const InternalKeyComparator& icomp_;
  UserComparatorWrapper user_comparator_;
  PinnedIteratorsManager* pinned_iters_mgr_;
  DataBlockIter block_iter_;
  const SliceTransform* prefix_extractor_;
  uint64_t prev_block_offset_ = std::numeric_limits<uint64_t>::max();
  BlockCacheLookupContext lookup_context_;

  BlockPrefetcher block_prefetcher_;

  // It stores all the block handles that are lookuped in cache ahead when
  // BlockCacheLookupForReadAheadSize is called. Since index_iter_ may point to
  // different blocks when readahead_size is calculated in
  // BlockCacheLookupForReadAheadSize, to avoid index_iter_ reseek,
  // block_handles_ is used.
  // `block_handles_` is lazily constructed to save CPU when it is unused
  std::unique_ptr<std::deque<BlockHandleInfo>> block_handles_;

  // Prefix of the seek key from the last Seek() under prefix_same_as_start,
  // for readahead trimming so no data blocks containing keys of a different
  // prefix are prefetched. Empty disables prefix-based trimming.
  std::string seek_key_prefix_for_readahead_trimming_;

  const bool allow_unprepared_value_;
  // How current data block's boundary key with the next block is compared with
  // iterate upper bound.
  BlockUpperBound block_upper_bound_check_ = BlockUpperBound::kUnknown;
  // True if we're standing at the first key of a block, and we haven't loaded
  // that block yet. A call to PrepareValue() will trigger loading the block.
  bool is_at_first_key_from_index_ = false;
  bool check_filter_;
  // TODO(Zhongyi): pick a better name
  bool need_upper_bound_check_;

  bool async_read_in_progress_;

  mutable SeekStatState seek_stat_state_ = SeekStatState::kNone;
  bool is_last_level_;

  // If set to true, it'll lookup in the cache ahead to estimate the readahead
  // size based on cache hit and miss.
  bool readahead_cache_lookup_ = false;

  bool is_index_out_of_bound_ = false;

  // Used in case of auto_readahead_size to disable the block_cache lookup if
  // direction is reversed from forward to backward. In case of backward
  // direction, SeekForPrev or Prev might call Seek from db_iter. So direction
  // is used to disable the lookup.
  IterDirection direction_ = IterDirection::kForward;

  //*** BEGIN States used by both regular scan and multiscan

  // True if block_iter_ is initialized and points to the same block
  // as index iterator.
  bool block_iter_points_to_real_block_;
  // See InternalIteratorBase::IsOutOfBound().
  bool is_out_of_bound_ = false;

  // During cache lookup to find readahead size, index_iter_ is iterated and it
  // can point to a different block.
  // If Prepare() is called, index_iter_ is used to prefetch data blocks for the
  // multiscan, so is_index_at_curr_block_ will be false.
  // Whether index is expected to match the current data_block_iter_.
  bool is_index_at_curr_block_ = true;

  // *** END States used by both regular scan and multiscan

  // *** BEGIN MultiScan related states ***
  Status multi_scan_status_;
  // ReadSet from IODispatcher, set during Prepare(). When non-null, MultiScan
  // is active and index_iter_ points to a MultiScanIndexIterator.
  std::shared_ptr<ReadSet> multi_scan_read_set_;
  // Raw pointer into index_iter_ when it's a MultiScanIndexIterator.
  MultiScanIndexIterator* multi_scan_index_iter_ = nullptr;
  // Original index iterator saved during Prepare(), restored on backward ops.
  std::unique_ptr<InternalIteratorBase<IndexValue>> original_index_iter_;
  // Maximum prefetchable block index.
  size_t prefetch_max_idx_ = 0;
  // *** END MultiScan related states ***

  // Reset MultiScan state and restore the original index iterator.
  void ResetMultiScan() {
    multi_scan_read_set_.reset();
    multi_scan_index_iter_ = nullptr;
    prefetch_max_idx_ = 0;
    // Discard any MultiScan error (e.g. PrefetchLimitReached) since we're
    // falling back to regular iteration.
    multi_scan_status_.PermitUncheckedError();
    multi_scan_status_ = Status::OK();
    if (original_index_iter_) {
      index_iter_ = std::move(original_index_iter_);
    }
  }

  void SeekSecondPass(const Slice* target);

  // If `target` is null, seek to first.
  void SeekImpl(const Slice* target, bool async_prefetch);

  void InitDataBlock();
  void AsyncInitDataBlock(bool is_first_pass);
  bool MaterializeCurrentBlock();
  void FindKeyForward();
  void FindBlockForward();
  void FindKeyBackward();
  void CheckOutOfBound();

  // Check if data block is fully within iterate_upper_bound.
  //
  // Note MyRocks may update iterate bounds between seek. To workaround it,
  // we need to check and update data_block_within_upper_bound_ accordingly.
  void CheckDataBlockWithinUpperBound();

  bool CheckPrefixMayMatch(const Slice& ikey, IterDirection direction,
                           bool* filter_checked) {
    if (need_upper_bound_check_ && direction == IterDirection::kBackward) {
      // Upper bound check isn't sufficient for backward direction to
      // guarantee the same result as total order, so disable prefix
      // check.
      return true;
    }
    if (check_filter_ &&
        !table_->PrefixRangeMayMatch(ikey, read_options_, prefix_extractor_,
                                     need_upper_bound_check_, &lookup_context_,
                                     filter_checked)) {
      // TODO remember the iterator is invalidated because of prefix
      // match. This can avoid the upper level file iterator to falsely
      // believe the position is the end of the SST file and move to
      // the first key of the next file.
      ResetDataIter();
      return false;
    }
    return true;
  }

  // *** BEGIN APIs relevant to auto tuning of readahead_size ***

  // This API is called to lookup the data blocks ahead in the cache to tune
  // the start and end offsets passed.
  void BlockCacheLookupForReadAheadSize(bool read_curr_block,
                                        uint64_t& start_offset,
                                        uint64_t& end_offset);

  void ResetBlockCacheLookupVar() {
    is_index_out_of_bound_ = false;
    readahead_cache_lookup_ = false;
    ClearBlockHandles();
  }

  bool IsNextBlockOutOfReadaheadBound() {
    assert(index_iter_->Valid());
    const Slice& index_iter_user_key = index_iter_->user_key();
    // If curr block's index key >= iterate_upper_bound, it means all the keys
    // in next block or above are out of bound.
    bool out_of_upper_bound =
        read_options_.iterate_upper_bound != nullptr &&
        (user_comparator_.CompareWithoutTimestamp(
             index_iter_user_key,
             /*a_has_ts=*/true, *read_options_.iterate_upper_bound,
             /*b_has_ts=*/false) >= 0
             ? true
             : false);
    if (out_of_upper_bound) {
      return true;
    }

    // If curr block's index key has a different prefix from the seek key's, it
    // means all the keys in next block or above has a different prefix from the
    // seek key's.
    bool out_of_prefix_bound =
        (read_options_.prefix_same_as_start &&
         !seek_key_prefix_for_readahead_trimming_.empty() &&
         (prefix_extractor_->InDomain(index_iter_user_key)
              ? (prefix_extractor_->Transform(index_iter_user_key)
                     .compare(seek_key_prefix_for_readahead_trimming_) != 0)
              : user_comparator_.CompareWithoutTimestamp(
                    index_iter_user_key,
                    /*a_has_ts=*/true, seek_key_prefix_for_readahead_trimming_,
                    /*b_has_ts=*/false) > 0));

    if (out_of_prefix_bound) {
      return true;
    }

    return false;
  }

  void ClearBlockHandles() {
    if (block_handles_ != nullptr) {
      block_handles_->clear();
    }
  }

  // Reset prev_block_offset_. If index_iter_ has moved ahead, it won't get
  // accurate prev_block_offset_.
  void ResetPreviousBlockOffset() {
    prev_block_offset_ = std::numeric_limits<uint64_t>::max();
  }

  bool DoesContainBlockHandles() {
    return block_handles_ != nullptr && !block_handles_->empty();
  }

  void InitializeStartAndEndOffsets(bool read_curr_block,
                                    bool& found_first_miss_block,
                                    uint64_t& start_updated_offset,
                                    uint64_t& end_updated_offset,
                                    size_t& prev_handles_size);
  // *** END APIs relevant to auto tuning of readahead_size ***

  // *** BEGIN APIs relevant to multiscan ***

  Status CollectBlockHandles(
      const std::vector<ScanOptions>& scan_opts,
      std::vector<BlockHandle>* scan_block_handles,
      std::vector<std::tuple<size_t, size_t>>* block_index_ranges_per_scan,
      std::vector<std::string>* data_block_boundary_keys);

  // *** END APIs relevant to multiscan ***
};
}  // namespace ROCKSDB_NAMESPACE
