//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/block_based/multi_scan_index_iterator.h"

#include <algorithm>

#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics_impl.h"
#include "rocksdb/options.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

MultiScanIndexIterator::MultiScanIndexIterator(
    std::vector<BlockHandle>&& block_handles,
    std::vector<std::string>&& data_block_separators,
    std::vector<std::tuple<size_t, size_t>>&& block_index_ranges_per_scan,
    const MultiScanArgs* scan_opts, std::shared_ptr<ReadSet> read_set,
    size_t prefetch_start_idx, size_t prefetch_max_idx,
    const InternalKeyComparator& icomp, Statistics* statistics)
    : block_handles_(std::move(block_handles)),
      data_block_separators_(std::move(data_block_separators)),
      block_index_ranges_per_scan_(std::move(block_index_ranges_per_scan)),
      scan_opts_(scan_opts),
      read_set_(std::move(read_set)),
      prefetch_start_idx_(prefetch_start_idx),
      prefetch_max_idx_(prefetch_max_idx),
      first_unreleased_prefetch_idx_(prefetch_start_idx),
      one_past_last_unreleased_prefetch_idx_(prefetch_max_idx),
      released_prefetch_blocks_(prefetch_max_idx - prefetch_start_idx, false),
      unreleased_prefetch_blocks_(prefetch_max_idx - prefetch_start_idx),
      icomp_(icomp),
      user_comparator_(icomp.user_comparator()),
      statistics_(statistics) {}

MultiScanIndexIterator::MultiScanIndexIterator(
    std::vector<BlockHandle>&& block_handles,
    std::vector<std::string>&& data_block_separators,
    std::vector<std::vector<size_t>>&& block_indices_per_scan,
    std::vector<bool>&& range_reaches_file_end,
    const MultiScanArgs* scan_opts, std::shared_ptr<ReadSet> read_set,
    size_t prefetch_start_idx, size_t prefetch_max_idx,
    const InternalKeyComparator& icomp, Statistics* statistics)
    : block_handles_(std::move(block_handles)),
      data_block_separators_(std::move(data_block_separators)),
      block_indices_per_scan_(std::move(block_indices_per_scan)),
      range_reaches_file_end_(std::move(range_reaches_file_end)),
      scan_opts_(scan_opts),
      read_set_(std::move(read_set)),
      prefetch_start_idx_(prefetch_start_idx),
      prefetch_max_idx_(prefetch_max_idx),
      first_unreleased_prefetch_idx_(prefetch_start_idx),
      one_past_last_unreleased_prefetch_idx_(prefetch_max_idx),
      released_prefetch_blocks_(prefetch_max_idx - prefetch_start_idx, false),
      unreleased_prefetch_blocks_(prefetch_max_idx - prefetch_start_idx),
      remaining_range_uses_(block_handles_.size(), 0),
      used_prefetch_blocks_(prefetch_max_idx - prefetch_start_idx, false),
      icomp_(icomp),
      user_comparator_(icomp.user_comparator()),
      statistics_(statistics),
      arbitrary_ranges_(true) {
  for (const auto& block_indices : block_indices_per_scan_) {
    for (const size_t block_index : block_indices) {
      ++remaining_range_uses_[block_index];
    }
  }
}

MultiScanIndexIterator::~MultiScanIndexIterator() {
  if (arbitrary_ranges_) {
    ReleaseArbitraryBlocks();
  } else {
    CountUnreleasedPrefetchedBlocksAsWasted();
  }
  if (statistics_ && wasted_blocks_count_ > 0) {
    RecordTick(statistics_, MULTISCAN_PREFETCH_BLOCKS_WASTED,
               wasted_blocks_count_);
  }
  // Report the prefetch IO the ReadSet performed for this Prepare. Recorded at
  // teardown (not in Prepare) so async and memory-deferred prefetches, which
  // complete during iteration, are included in the counts.
  if (read_set_) {
    const uint64_t blocks_from_cache = read_set_->GetNumCacheHits();
    const uint64_t blocks_prefetched = read_set_->GetNumBlocksPrefetched();
    const uint64_t prefetch_bytes = read_set_->GetPrefetchBytes();
    const uint64_t io_requests = read_set_->GetNumIORequests();
    const uint64_t io_coalesced_nonadjacent =
        read_set_->GetNumCoalescedNonadjacent();
    if (statistics_) {
      RecordTick(statistics_, MULTISCAN_BLOCKS_FROM_CACHE, blocks_from_cache);
      RecordTick(statistics_, MULTISCAN_BLOCKS_PREFETCHED, blocks_prefetched);
      RecordTick(statistics_, MULTISCAN_PREFETCH_BYTES, prefetch_bytes);
      RecordTick(statistics_, MULTISCAN_IO_REQUESTS, io_requests);
      RecordTick(statistics_, MULTISCAN_IO_COALESCED_NONADJACENT,
                 io_coalesced_nonadjacent);
    }
    PERF_COUNTER_ADD(multiscan_blocks_from_cache, blocks_from_cache);
    PERF_COUNTER_ADD(multiscan_blocks_prefetched, blocks_prefetched);
    PERF_COUNTER_ADD(multiscan_prefetch_bytes, prefetch_bytes);
    PERF_COUNTER_ADD(multiscan_io_requests, io_requests);
    PERF_COUNTER_ADD(multiscan_io_coalesced_nonadjacent,
                     io_coalesced_nonadjacent);
  }
  // Release any remaining pinned blocks
  if (read_set_) {
    for (size_t i = first_unreleased_prefetch_idx_;
         i < one_past_last_unreleased_prefetch_idx_ &&
         unreleased_prefetch_blocks_ > 0;
         ++i) {
      ReleasePrefetchedBlock(i);
    }
  }
}

void MultiScanIndexIterator::CountUnreleasedPrefetchedBlocksAsWasted() {
  if (unreleased_prefetch_blocks_ == 0) {
    return;
  }

  for (size_t i = first_unreleased_prefetch_idx_;
       i < one_past_last_unreleased_prefetch_idx_; ++i) {
    if (!IsPrefetchedBlock(i) ||
        released_prefetch_blocks_[i - prefetch_start_idx_]) {
      continue;
    }
    if (valid_ && !scan_range_exhausted_ && i == cur_idx_) {
      continue;
    }
    ++wasted_blocks_count_;
  }
}

void MultiScanIndexIterator::ReleaseArbitraryBlocks() {
  for (size_t block_index = prefetch_start_idx_;
       block_index < prefetch_max_idx_; ++block_index) {
    const size_t prefetch_index = block_index - prefetch_start_idx_;
    if (released_prefetch_blocks_[prefetch_index]) {
      continue;
    }
    if (!used_prefetch_blocks_[prefetch_index]) {
      ++wasted_blocks_count_;
    }
    ReleasePrefetchedBlock(block_index);
  }
}

void MultiScanIndexIterator::RetireArbitraryRange() {
  if (active_scan_idx_ >= block_indices_per_scan_.size() ||
      active_range_retired_) {
    return;
  }
  for (const size_t block_index : block_indices_per_scan_[active_scan_idx_]) {
    assert(remaining_range_uses_[block_index] > 0);
    --remaining_range_uses_[block_index];
    if (remaining_range_uses_[block_index] != 0 ||
        !IsPrefetchedBlock(block_index)) {
      continue;
    }
    const size_t prefetch_index = block_index - prefetch_start_idx_;
    if (!used_prefetch_blocks_[prefetch_index]) {
      ++wasted_blocks_count_;
    }
    ReleasePrefetchedBlock(block_index);
  }
  active_range_retired_ = true;
}

void MultiScanIndexIterator::FinishArbitraryRange() {
  RetireArbitraryRange();
  active_scan_idx_ = std::numeric_limits<size_t>::max();
}

size_t MultiScanIndexIterator::FindLocalRange(
    size_t logical_range_index) const {
  for (size_t i = 0; i < block_indices_per_scan_.size(); ++i) {
    if (scan_opts_->GetLogicalRangeIndex(i) == logical_range_index) {
      return i;
    }
  }
  return std::numeric_limits<size_t>::max();
}

bool MultiScanIndexIterator::IsPrefetchedBlock(size_t block_idx) const {
  return block_idx >= prefetch_start_idx_ && block_idx < prefetch_max_idx_;
}

size_t MultiScanIndexIterator::current_read_set_index() {
  if (!IsPrefetchedBlock(cur_idx_)) {
    return prefetch_max_idx_ - prefetch_start_idx_;
  }
  if (arbitrary_ranges_) {
    used_prefetch_blocks_[cur_idx_ - prefetch_start_idx_] = true;
  }
  return cur_idx_ - prefetch_start_idx_;
}

bool MultiScanIndexIterator::HasMoreScanRanges() const {
  if (scan_opts_->reverse) {
    return false;
  }
  if (arbitrary_ranges_) {
    return active_scan_idx_ < range_reaches_file_end_.size() &&
           !range_reaches_file_end_[active_scan_idx_];
  }
  return next_scan_idx_ < block_index_ranges_per_scan_.size();
}

void MultiScanIndexIterator::ReleaseBlock(size_t block_idx) {
  ReleasePrefetchedBlock(block_idx);
}

bool MultiScanIndexIterator::ReleasePrefetchedBlock(size_t block_idx) {
  if (!IsPrefetchedBlock(block_idx) || unreleased_prefetch_blocks_ == 0) {
    return false;
  }

  const size_t read_set_idx = block_idx - prefetch_start_idx_;
  if (released_prefetch_blocks_[read_set_idx]) {
    return false;
  }

  assert(read_set_);
  read_set_->ReleaseBlock(read_set_idx);
  released_prefetch_blocks_[read_set_idx] = true;
  --unreleased_prefetch_blocks_;
  ShrinkUnreleasedPrefetchBounds();
  return true;
}

void MultiScanIndexIterator::MarkCurrentBlockUnretained() {
  assert(arbitrary_ranges_);
  TEST_SYNC_POINT(
      "MultiScanIndexIterator::MarkCurrentBlockUnretained");
  ReleasePrefetchedBlock(cur_idx_);
}

void MultiScanIndexIterator::ShrinkUnreleasedPrefetchBounds() {
  while (first_unreleased_prefetch_idx_ <
             one_past_last_unreleased_prefetch_idx_ &&
         released_prefetch_blocks_[first_unreleased_prefetch_idx_ -
                                   prefetch_start_idx_]) {
    ++first_unreleased_prefetch_idx_;
  }
  while (first_unreleased_prefetch_idx_ <
             one_past_last_unreleased_prefetch_idx_ &&
         released_prefetch_blocks_[one_past_last_unreleased_prefetch_idx_ - 1 -
                                   prefetch_start_idx_]) {
    --one_past_last_unreleased_prefetch_idx_;
  }
}

void MultiScanIndexIterator::ReleaseBlocks(size_t from_idx, size_t to_idx) {
  const size_t release_from = std::max(from_idx, prefetch_start_idx_);
  const size_t release_to = std::min(to_idx, prefetch_max_idx_);
  if (release_from >= release_to || unreleased_prefetch_blocks_ == 0) {
    return;
  }

  for (size_t i = release_from; i < release_to; ++i) {
    if (ReleasePrefetchedBlock(i)) {
      wasted_blocks_count_++;
    }
  }
}

void MultiScanIndexIterator::Seek(const Slice& target) {
  if (!status_.ok()) {
    return;
  }
  if (scan_opts_->reverse) {
    status_ = Status::InvalidArgument("Seek called on reverse MultiScan");
    RecordTick(statistics_, MULTISCAN_SEEK_ERRORS);
    valid_ = false;
    return;
  }
  if (arbitrary_ranges_) {
    SeekArbitrary(target);
    return;
  }

  // Reset scan range exhaustion flag on Seek
  scan_range_exhausted_ = false;

  // Check the case where there are no ranges prepared
  if (scan_opts_->size() == 0) {
    valid_ = false;
    return;
  }

  // Enforce forward-only seek
  if (!prev_seek_key_.empty() && icomp_.Compare(target, prev_seek_key_) <= 0) {
    // Seek key is not moving forward -- keep current position
    return;
  }
  prev_seek_key_ = target.ToString();

  const auto& scan_ranges = scan_opts_->GetScanRanges();
  Slice user_seek_target = ExtractUserKey(target);

  // Allow reseek on the start of the last prepared range
  next_scan_idx_ =
      std::min(next_scan_idx_, block_index_ranges_per_scan_.size() - 1);

  auto compare_next_scan_start_result =
      user_comparator_.CompareWithoutTimestamp(
          user_seek_target, /*a_has_ts=*/true,
          scan_ranges[next_scan_idx_].range.start.value(),
          /*b_has_ts=*/false);

  // There are 3 different Cases we need to handle:
  // The following diagram explains different seek targets seeking at various
  // positions on the table, while the next_scan_idx_ points to PreparedRange 2.
  //
  // next_scan_idx_: ------------------+
  //                                   |
  //                                   v
  // table:     : __[PreparedRange 1]__[PreparedRange 2]__[PreparedRange 3]__
  // Seek target: <----- Case 1 ------>^<------------- Case 2 -------------->
  //                                   |
  //                                 Case 3
  //
  // Case 1: seek before the start of next prepared range. This could happen
  //    due to too many delete tombstones triggering reseek or delete range.
  // Case 2: seek after the start of next prepared range.
  //    This could happen due to seek key adjustment from delete range file.
  // Case 3: seek at the beginning of a prepared range (expected case)

  if (compare_next_scan_start_result < 0) {
    // Case 1: Seek before the start of the next prepared range
    if (next_scan_idx_ == 0) {
      // Should not happen -- seek before first prepared range
      assert(false && "Seek target before the first prepared range");
      valid_ = false;
      return;
    }
    auto seek_target_before_previous_prepared_range =
        user_comparator_.CompareWithoutTimestamp(
            user_seek_target, /*a_has_ts=*/true,
            scan_ranges[next_scan_idx_ - 1].range.start.value(),
            /*b_has_ts=*/false) < 0;
    assert(!seek_target_before_previous_prepared_range);
    if (seek_target_before_previous_prepared_range) {
      status_ = Status::InvalidArgument(
          "Seek target is before the previous prepared range at index " +
          std::to_string(next_scan_idx_));
      RecordTick(statistics_, MULTISCAN_SEEK_ERRORS);
      valid_ = false;
      return;
    }
    // Seek within a gap -- advance to the right scan range and find block
    SeekToBlock(&user_seek_target);
  } else if (compare_next_scan_start_result > 0) {
    // Case 2: Seek after the start of the next prepared range
    SeekToBlock(&user_seek_target);
  } else {
    // Case 3: Seek at the beginning of a prepared range (expected case)
    assert(next_scan_idx_ < block_index_ranges_per_scan_.size());
    auto [cur_scan_start_idx, cur_scan_end_idx] =
        block_index_ranges_per_scan_[next_scan_idx_];
    ++next_scan_idx_;

    if (cur_scan_start_idx >= cur_scan_end_idx) {
      // No blocks are prepared for this range at current file
      SetExhausted();
      return;
    }

    // max_sequential_skip_in_iterations can trigger a reseek on the start
    // key of a scan range, even though we're already past cur_scan_start_idx
    size_t block_idx = std::max(cur_scan_start_idx, cur_idx_);
    if (block_idx >= cur_scan_end_idx) {
      // cur_idx_ has advanced past this range's blocks (e.g., due to a
      // reseek after all ranges were exhausted). Treat as exhausted.
      SetExhausted();
      return;
    }
    SeekToBlockIdx(block_idx);
  }
}

void MultiScanIndexIterator::SeekArbitrary(const Slice& target) {
  scan_range_exhausted_ = false;
  const auto& seek_state = scan_opts_->GetSeekState();
  if (seek_state && seek_state->generation != observed_seek_generation_) {
    FinishArbitraryRange();
    observed_seek_generation_ = seek_state->generation;
    const size_t local_range =
        FindLocalRange(seek_state->logical_range_index);
    if (local_range == std::numeric_limits<size_t>::max()) {
      valid_ = false;
      return;
    }
    ActivateArbitraryRange(local_range, target);
    return;
  }

  const Slice user_seek_target = ExtractUserKey(target);
  if (!seek_state && next_scan_idx_ < block_indices_per_scan_.size() &&
      user_comparator_.CompareWithoutTimestamp(
          user_seek_target, /*a_has_ts=*/true,
          *scan_opts_->GetScanRanges()[next_scan_idx_].range.start,
          /*b_has_ts=*/false) == 0) {
    FinishArbitraryRange();
    ActivateArbitraryRange(next_scan_idx_, target);
    ++next_scan_idx_;
    return;
  }

  if (active_scan_idx_ >= block_indices_per_scan_.size()) {
    valid_ = false;
    return;
  }
  PositionInArbitraryRange(target);
}

void MultiScanIndexIterator::ActivateArbitraryRange(size_t scan_idx,
                                                     const Slice& target) {
  assert(scan_idx < block_indices_per_scan_.size());
  active_scan_idx_ = scan_idx;
  active_scan_block_pos_ = 0;
  active_range_retired_ = false;
  PositionInArbitraryRange(target);
}

void MultiScanIndexIterator::PositionInArbitraryRange(const Slice& target) {
  assert(active_scan_idx_ < block_indices_per_scan_.size());
  const auto& block_indices = block_indices_per_scan_[active_scan_idx_];
  const Slice user_seek_target = ExtractUserKey(target);
  size_t block_position = 0;
  while (block_position < block_indices.size() &&
         user_comparator_.CompareWithoutTimestamp(
             user_seek_target, /*a_has_ts=*/true,
             data_block_separators_[block_indices[block_position]],
             /*b_has_ts=*/false) > 0) {
    ++block_position;
  }
  if (block_position == block_indices.size()) {
    SetArbitraryRangeExhausted();
    return;
  }

  active_scan_block_pos_ = block_position;
  cur_idx_ = block_indices[block_position];
  if (!IsPrefetchedBlock(cur_idx_)) {
    valid_ = false;
    if (scan_opts_->max_prefetch_size > 0) {
      status_ = Status::PrefetchLimitReached();
    }
    return;
  }
  valid_ = true;
}

void MultiScanIndexIterator::SetArbitraryRangeExhausted() {
  scan_range_exhausted_ = true;
  valid_ = false;
  RetireArbitraryRange();
}

void MultiScanIndexIterator::SeekToBlock(const Slice* user_seek_target) {
  const auto& scan_ranges = scan_opts_->GetScanRanges();

  // Advance next_scan_idx_ past ranges whose start key <= seek target
  while (next_scan_idx_ < block_index_ranges_per_scan_.size() &&
         user_comparator_.CompareWithoutTimestamp(
             *user_seek_target, /*a_has_ts=*/true,
             scan_ranges[next_scan_idx_].range.start.value(),
             /*b_has_ts=*/false) >= 0) {
    next_scan_idx_++;
  }

  assert(next_scan_idx_ > 0);
  auto cur_scan_idx = next_scan_idx_ - 1;
  auto [cur_scan_start_idx, cur_scan_end_idx] =
      block_index_ranges_per_scan_[cur_scan_idx];

  if (cur_scan_start_idx >= cur_scan_end_idx) {
    SetExhausted();
    return;
  }

  // Release blocks from current position to cur_scan_start_idx (wasted)
  ReleaseBlocks(cur_idx_, cur_scan_start_idx);

  // Find the correct block within the range using linear search on separators
  size_t block_idx = std::max(cur_scan_start_idx, cur_idx_);
  while (block_idx < data_block_separators_.size() &&
         user_comparator_.CompareWithoutTimestamp(
             *user_seek_target, /*a_has_ts=*/true,
             data_block_separators_[block_idx],
             /*b_has_ts=*/false) > 0) {
    if (IsPrefetchedBlock(block_idx)) {
      wasted_blocks_count_++;
    }
    ReleaseBlock(block_idx);
    block_idx++;
  }

  if (block_idx >= data_block_separators_.size()) {
    // All remaining blocks were released above. Update cur_idx_ so the
    // destructor does not double-release them.
    cur_idx_ = block_handles_.size();
    SetExhausted();
    return;
  }

  // Update cur_idx_ before calling SeekToBlockIdx since we've already
  // released all blocks up to block_idx above. This prevents SeekToBlockIdx's
  // ReleaseBlocks(cur_idx_, block_idx) from double-releasing.
  cur_idx_ = block_idx;
  SeekToBlockIdx(block_idx);
}

void MultiScanIndexIterator::SeekToBlockIdx(size_t block_idx) {
  assert(cur_idx_ <= block_idx);

  // Release any blocks between cur_idx_ and block_idx (wasted)
  ReleaseBlocks(cur_idx_, block_idx);

  cur_idx_ = block_idx;
  valid_ = true;
}

void MultiScanIndexIterator::SeekToBlockIdxReverse(size_t block_idx) {
  if (cur_idx_ > block_idx && cur_idx_ < block_handles_.size()) {
    ReleaseBlocks(block_idx + 1, cur_idx_ + 1);
  }

  cur_idx_ = block_idx;
  if (!IsPrefetchedBlock(cur_idx_)) {
    valid_ = false;
    if (scan_opts_->max_prefetch_size > 0) {
      status_ = Status::PrefetchLimitReached();
    }
    return;
  }
  valid_ = true;
}

void MultiScanIndexIterator::SetExhausted() {
  scan_range_exhausted_ = true;
  if (next_scan_idx_ < block_index_ranges_per_scan_.size()) {
    // More ranges remain -- signal out-of-bound for current range.
    valid_ = true;
    // Position at the start of the next range so that the next Seek()
    // can find it. We need to be "valid" so that FindBlockForward sets
    // is_out_of_bound_ = true.
    auto [start, end] = block_index_ranges_per_scan_[next_scan_idx_];
    if (start < end) {
      cur_idx_ = start;
      return;
    }
    valid_ = false;
  } else {
    // Last range -- natural EOF. Don't set out-of-bound so LevelIterator
    // advances to the next file.
    valid_ = false;
  }
}

void MultiScanIndexIterator::Next() {
  assert(valid_);
  if (arbitrary_ranges_) {
    NextArbitrary();
    return;
  }

  // Release current block
  ReleaseBlock(cur_idx_);
  ++cur_idx_;

  // Check if we've crossed a scan range boundary
  if (next_scan_idx_ > 0) {
    auto cur_scan_end_idx =
        std::get<1>(block_index_ranges_per_scan_[next_scan_idx_ - 1]);
    if (cur_idx_ >= cur_scan_end_idx) {
      // Current scan range is exhausted
      SetExhausted();
      return;
    }
  }

  // Check prefetch limit
  if (!IsPrefetchedBlock(cur_idx_)) {
    valid_ = false;
    if (scan_opts_->max_prefetch_size > 0) {
      status_ = Status::PrefetchLimitReached();
    }
    return;
  }

  // Still within current range, valid
  valid_ = true;
}

void MultiScanIndexIterator::NextArbitrary() {
  assert(active_scan_idx_ < block_indices_per_scan_.size());
  const auto& block_indices = block_indices_per_scan_[active_scan_idx_];
  ++active_scan_block_pos_;
  if (active_scan_block_pos_ >= block_indices.size()) {
    SetArbitraryRangeExhausted();
    return;
  }
  cur_idx_ = block_indices[active_scan_block_pos_];
  if (!IsPrefetchedBlock(cur_idx_)) {
    valid_ = false;
    if (scan_opts_->max_prefetch_size > 0) {
      status_ = Status::PrefetchLimitReached();
    }
    return;
  }
  valid_ = true;
}

void MultiScanIndexIterator::SeekToFirst() {
  if (block_index_ranges_per_scan_.empty()) {
    valid_ = false;
    return;
  }

  cur_idx_ = 0;
  next_scan_idx_ = 0;
  prev_seek_key_.clear();
  wasted_blocks_count_ = 0;
  status_ = Status::OK();

  auto [start, end] = block_index_ranges_per_scan_[0];
  if (start >= end) {
    valid_ = false;
    return;
  }
  cur_idx_ = start;
  next_scan_idx_ = 1;
  valid_ = true;
}

void MultiScanIndexIterator::SeekForPrev(const Slice& target) {
  if (!scan_opts_->reverse || !status_.ok()) {
    valid_ = false;
    return;
  }

  scan_range_exhausted_ = false;
  if (scan_opts_->size() == 0) {
    valid_ = false;
    return;
  }

  const auto& scan_ranges = scan_opts_->GetScanRanges();
  Slice user_seek_target = ExtractUserKey(target);
  size_t scan_idx = scan_ranges.size();
  for (size_t i = scan_ranges.size(); i > 0; --i) {
    const size_t candidate = i - 1;
    const auto& range = scan_ranges[candidate].range;
    if (!range.start.has_value() || !range.limit.has_value()) {
      continue;
    }
    const int cmp_start = user_comparator_.CompareWithoutTimestamp(
        user_seek_target, /*a_has_ts=*/true, range.start.value(),
        /*b_has_ts=*/false);
    if (cmp_start >= 0) {
      scan_idx = candidate;
      break;
    }
  }

  if (scan_idx == scan_ranges.size()) {
    status_ = Status::InvalidArgument(
        "SeekForPrev target is outside prepared ranges");
    RecordTick(statistics_, MULTISCAN_SEEK_ERRORS);
    valid_ = false;
    return;
  }

  cur_scan_idx_ = scan_idx;
  auto [cur_scan_start_idx, cur_scan_end_idx] =
      block_index_ranges_per_scan_[cur_scan_idx_];
  if (cur_scan_start_idx >= cur_scan_end_idx) {
    valid_ = false;
    return;
  }

  size_t block_idx = cur_scan_start_idx;
  while (block_idx + 1 < cur_scan_end_idx &&
         user_comparator_.CompareWithoutTimestamp(
             user_seek_target, /*a_has_ts=*/true,
             data_block_separators_[block_idx], /*b_has_ts=*/false) > 0) {
    ++block_idx;
  }

  SeekToBlockIdxReverse(block_idx);
}

void MultiScanIndexIterator::SeekToLast() {
  if (!scan_opts_->reverse) {
    valid_ = false;
    return;
  }
  for (size_t i = block_index_ranges_per_scan_.size(); i > 0; --i) {
    const size_t scan_idx = i - 1;
    auto [start, end] = block_index_ranges_per_scan_[scan_idx];
    if (start < end) {
      cur_scan_idx_ = scan_idx;
      SeekToBlockIdxReverse(end - 1);
      return;
    }
  }
  valid_ = false;
}

void MultiScanIndexIterator::Prev() {
  assert(valid_);
  if (!scan_opts_->reverse) {
    valid_ = false;
    return;
  }

  ReleaseBlock(cur_idx_);
  auto [cur_scan_start_idx, cur_scan_end_idx] =
      block_index_ranges_per_scan_[cur_scan_idx_];
  (void)cur_scan_end_idx;
  if (cur_idx_ <= cur_scan_start_idx) {
    // Reverse scan range transitions are driven by DBIter's lower bound and
    // the next SeekForPrev(), so this path does not need the forward
    // out-of-bound signal from SetExhausted().
    valid_ = false;
    return;
  }

  --cur_idx_;
  if (!IsPrefetchedBlock(cur_idx_)) {
    valid_ = false;
    if (scan_opts_->max_prefetch_size > 0) {
      status_ = Status::PrefetchLimitReached();
    }
    return;
  }
  valid_ = true;
}

Slice MultiScanIndexIterator::key() const {
  assert(valid_);
  assert(cur_idx_ < data_block_separators_.size());

  // Build internal key: user_key + pack(kMaxSequenceNumber, kValueTypeForSeek)
  cur_key_buf_.clear();
  AppendInternalKey(&cur_key_buf_,
                    ParsedInternalKey(data_block_separators_[cur_idx_],
                                      kMaxSequenceNumber, kValueTypeForSeek));
  return Slice(cur_key_buf_);
}

Slice MultiScanIndexIterator::user_key() const {
  assert(valid_);
  assert(cur_idx_ < data_block_separators_.size());
  return Slice(data_block_separators_[cur_idx_]);
}

IndexValue MultiScanIndexIterator::value() const {
  assert(valid_);
  assert(cur_idx_ < block_handles_.size());
  // Return IndexValue with empty first_internal_key to disable
  // is_at_first_key_from_index_ optimization
  return IndexValue(block_handles_[cur_idx_], Slice());
}

uint64_t MultiScanIndexIterator::GetMaxPrefetchSize() const {
  return scan_opts_->max_prefetch_size;
}

}  // namespace ROCKSDB_NAMESPACE
