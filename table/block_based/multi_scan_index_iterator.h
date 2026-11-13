//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <limits>
#include <string>
#include <tuple>
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/io_dispatcher.h"
#include "table/format.h"
#include "table/internal_iterator.h"
#include "util/user_comparator_wrapper.h"

namespace ROCKSDB_NAMESPACE {

class MultiScanArgs;
class Statistics;

// MultiScanIndexIterator wraps the block handle list produced by
// Prepare()/CollectBlockHandles() and presents it as an
// InternalIteratorBase<IndexValue>. This allows BlockBasedTableIterator
// to use the same SeekImpl()/FindBlockForward() code path for both
// regular iteration and MultiScan.
//
// The iterator supports forward Seek()/Next() or reverse SeekForPrev()/Prev()
// depending on MultiScanArgs::reverse.
class MultiScanIndexIterator : public InternalIteratorBase<IndexValue> {
 public:
  // scan_opts and icomp must outlive this iterator. read_set is shared.
  MultiScanIndexIterator(
      std::vector<BlockHandle>&& block_handles,
      std::vector<std::string>&& data_block_separators,
      std::vector<std::tuple<size_t, size_t>>&& block_index_ranges_per_scan,
      const MultiScanArgs* scan_opts, std::shared_ptr<ReadSet> read_set,
      size_t prefetch_start_idx, size_t prefetch_max_idx,
      const InternalKeyComparator& icomp, Statistics* statistics);

  MultiScanIndexIterator(
      std::vector<BlockHandle>&& block_handles,
      std::vector<std::string>&& data_block_separators,
      std::vector<std::vector<size_t>>&& block_indices_per_scan,
      std::vector<bool>&& range_reaches_file_end,
      const MultiScanArgs* scan_opts, std::shared_ptr<ReadSet> read_set,
      size_t prefetch_start_idx, size_t prefetch_max_idx,
      const InternalKeyComparator& icomp, Statistics* statistics);

  ~MultiScanIndexIterator() override;

  // Non-copyable, non-movable.
  MultiScanIndexIterator(const MultiScanIndexIterator&) = delete;
  MultiScanIndexIterator& operator=(const MultiScanIndexIterator&) = delete;
  MultiScanIndexIterator(MultiScanIndexIterator&&) = delete;
  MultiScanIndexIterator& operator=(MultiScanIndexIterator&&) = delete;

  // Forward-only seek. target must be >= prev_seek_key_.
  void Seek(const Slice& target) override;

  // Move to the next block. Handles scan range boundaries.
  void Next() override;

  // Move to the first block of the first scan range.
  void SeekToFirst() override;

  void SeekForPrev(const Slice& target) override;
  void SeekToLast() override;
  void Prev() override;

  bool Valid() const override { return valid_; }

  // Returns an internal key built from the current block's separator
  // with kMaxSequenceNumber.
  Slice key() const override;

  // Returns the user key separator for the current block.
  Slice user_key() const override;

  // Returns IndexValue with the current block handle and empty
  // first_internal_key (disables is_at_first_key_from_index_ optimization).
  IndexValue value() const override;

  Status status() const override { return status_; }

  const MultiScanArgs* scan_opts() const { return scan_opts_; }

  // Returns the current index into the ReadSet, or prefetch size if the
  // current block was not prefetched.
  size_t current_read_set_index();

  // Returns the max_prefetch_size from scan options.
  uint64_t GetMaxPrefetchSize() const;

  size_t GetPrefetchBlockCount() const {
    return prefetch_max_idx_ - prefetch_start_idx_;
  }

  // Returns true if the last Next() crossed a scan range boundary.
  // Only valid immediately after Next(); reset to false on the next Seek().
  bool IsScanRangeExhausted() const { return scan_range_exhausted_; }

  // Returns true if there are more scan ranges after the current one.
  bool HasMoreScanRanges() const;

  bool RetainsPrefetchedBlocks() const { return arbitrary_ranges_; }

  // Stop retention bookkeeping for the current block after ReadSet transfers
  // a foreground synchronous read to the data iterator.
  void MarkCurrentBlockUnretained();

 private:
  // Release blocks from from_idx (inclusive) to to_idx (exclusive),
  // counting wasted prefetched blocks.
  void ReleaseBlocks(size_t from_idx, size_t to_idx);
  void ReleaseBlock(size_t block_idx);
  bool ReleasePrefetchedBlock(size_t block_idx);
  void ShrinkUnreleasedPrefetchBounds();
  bool IsPrefetchedBlock(size_t block_idx) const;
  void CountUnreleasedPrefetchedBlocksAsWasted();

  // Find the correct scan range and block for an unexpected seek target
  // (target doesn't match expected scan range start).
  void SeekToBlock(const Slice* user_seek_target);

  // Position at block_idx after releasing any skipped blocks.
  void SeekToBlockIdx(size_t block_idx);
  void SeekToBlockIdxReverse(size_t block_idx);

  // Mark the current scan range as exhausted. If more ranges remain,
  // positions at the next range's start (stays valid for out-of-bound
  // detection). If this is the last range, becomes invalid.
  void SetExhausted();

  void SeekArbitrary(const Slice& target);
  void NextArbitrary();
  void ActivateArbitraryRange(size_t scan_idx, const Slice& target);
  void PositionInArbitraryRange(const Slice& target);
  void RetireArbitraryRange();
  void FinishArbitraryRange();
  void ReleaseArbitraryBlocks();
  size_t FindLocalRange(size_t logical_range_index) const;
  void SetArbitraryRangeExhausted();

  std::vector<BlockHandle> block_handles_;
  std::vector<std::string> data_block_separators_;
  std::vector<std::tuple<size_t, size_t>> block_index_ranges_per_scan_;
  std::vector<std::vector<size_t>> block_indices_per_scan_;
  std::vector<bool> range_reaches_file_end_;
  const MultiScanArgs* scan_opts_;
  std::shared_ptr<ReadSet> read_set_;
  size_t prefetch_start_idx_;
  size_t prefetch_max_idx_;
  size_t first_unreleased_prefetch_idx_;
  size_t one_past_last_unreleased_prefetch_idx_;
  std::vector<bool> released_prefetch_blocks_;
  size_t unreleased_prefetch_blocks_;
  std::vector<size_t> remaining_range_uses_;
  std::vector<bool> used_prefetch_blocks_;
  const InternalKeyComparator& icomp_;
  UserComparatorWrapper user_comparator_;
  Statistics* statistics_;

  size_t cur_idx_ = 0;
  size_t next_scan_idx_ = 0;
  size_t cur_scan_idx_ = 0;
  bool valid_ = false;
  Status status_;
  std::string prev_seek_key_;
  size_t wasted_blocks_count_ = 0;
  bool scan_range_exhausted_ = false;
  bool arbitrary_ranges_ = false;
  size_t active_scan_idx_ = std::numeric_limits<size_t>::max();
  size_t active_scan_block_pos_ = 0;
  bool active_range_retired_ = false;
  uint64_t observed_seek_generation_ = 0;
  mutable std::string cur_key_buf_;
};

}  // namespace ROCKSDB_NAMESPACE
