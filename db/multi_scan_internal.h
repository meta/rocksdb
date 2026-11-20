//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

// Shared by the iterators in one non-canonical prepared scan. DBIter advances
// the generation for each caller-visible range transition. Internal reseeks
// observe the same generation and continue within the active range.
struct MultiScanSeekState {
  // Incremented once for each caller-visible range seek.
  uint64_t generation = 0;
  // Index of the active range in the caller's original request.
  size_t logical_range_index = 0;
};

// Internal access to state that must travel with MultiScanArgs while a request
// is pruned from DB scope to levels, files, and tables.
class MultiScanArgsHelper {
 public:
  static bool HasSortedNonOverlappingRanges(const MultiScanArgs& args) {
    if (args.comp_ == nullptr) {
      return false;
    }
    for (size_t i = 0; i < args.original_ranges_.size(); ++i) {
      if (!args.original_ranges_[i].range.start.has_value()) {
        return false;
      }
      if (i == 0) {
        continue;
      }
      if (!args.original_ranges_[i - 1].range.limit.has_value() ||
          args.comp_->CompareWithoutTimestamp(
              *args.original_ranges_[i].range.start,
              /*a_has_ts=*/false, *args.original_ranges_[i - 1].range.limit,
              /*b_has_ts=*/false) < 0) {
        return false;
      }
    }
    return true;
  }

  static bool IsNonCanonical(const MultiScanArgs& args) {
    return args.seek_state_ != nullptr ||
           (args.allow_unsorted_or_overlapping_ranges &&
            !HasSortedNonOverlappingRanges(args));
  }

  static void InitializeSeekState(MultiScanArgs* args) {
    assert(args != nullptr);
    args->seek_state_ = std::make_shared<MultiScanSeekState>();
  }

  static void BeginLogicalSeek(MultiScanArgs* args,
                               size_t logical_range_index) {
    assert(args != nullptr);
    std::shared_ptr<MultiScanSeekState> seek_state = GetSeekState(*args);
    if (seek_state) {
      seek_state->logical_range_index = logical_range_index;
      ++seek_state->generation;
    }
  }

  static std::shared_ptr<MultiScanSeekState> GetSeekState(
      const MultiScanArgs& args) {
    return std::static_pointer_cast<MultiScanSeekState>(args.seek_state_);
  }

  static size_t GetLogicalRangeIndex(const MultiScanArgs& args,
                                     size_t local_range_index) {
    return args.logical_range_indices_.empty()
               ? local_range_index
               : args.logical_range_indices_[local_range_index];
  }

  static void ReserveLogicalRangeIndices(MultiScanArgs* args, size_t size) {
    assert(args != nullptr);
    args->logical_range_indices_.reserve(size);
  }

  static void InsertForPrepare(MultiScanArgs* args,
                               const ScanOptions& scan_options,
                               size_t logical_range_index) {
    assert(args != nullptr);
    args->original_ranges_.push_back(scan_options);
    if (args->seek_state_) {
      args->logical_range_indices_.push_back(logical_range_index);
    }
  }
};

}  // namespace ROCKSDB_NAMESPACE
