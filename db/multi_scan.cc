//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cassert>
#include <memory>
#include <string>
#include <vector>

#include "db/multi_scan_internal.h"
#include "file/file_util.h"
#include "rocksdb/db.h"
#include "rocksdb/file_system.h"
#include "rocksdb/iterator.h"
#include "rocksdb/snapshot.h"

namespace ROCKSDB_NAMESPACE {

using MultiScanIterator = MultiScan::MultiScanIterator;

namespace {

Status ValidateScanOptionsForMultiScan(const MultiScanArgs& multiscan_opts) {
  if (multiscan_opts.empty()) {
    return Status::InvalidArgument("Empty MultiScanArgs");
  }

  const Comparator* comparator = multiscan_opts.GetComparator();
  if (comparator == nullptr) {
    return Status::InvalidArgument("MultiScanArgs requires comparator");
  }

  const std::vector<ScanOptions>& scan_opts = multiscan_opts.GetScanRanges();
  for (size_t i = 0; i < scan_opts.size(); ++i) {
    const RangeOpt& scan_range = scan_opts[i].range;
    if (!scan_range.start.has_value()) {
      return Status::InvalidArgument("Scan has no start key at index " +
                                     std::to_string(i));
    }
    if (multiscan_opts.reverse && !scan_range.limit.has_value()) {
      return Status::InvalidArgument(
          "Reverse MultiScan requires limit at index " + std::to_string(i));
    }
    if (scan_range.limit.has_value() &&
        comparator->CompareWithoutTimestamp(
            *scan_range.start, /*a_has_ts=*/false, *scan_range.limit,
            /*b_has_ts=*/false) >= 0) {
      return Status::InvalidArgument(
          "Scan start key is large or equal than limit at index " +
          std::to_string(i));
    }
  }

  return Status::OK();
}

bool HasMixedBounds(const MultiScanArgs& scan_opts) {
  const bool first_has_limit =
      scan_opts.GetScanRanges().front().range.limit.has_value();
  for (const ScanOptions& range : scan_opts.GetScanRanges()) {
    if (range.range.limit.has_value() != first_has_limit) {
      return true;
    }
  }
  return false;
}

MultiScanArgs BuildSingleRangeArgs(const MultiScanArgs& all_scan_opts,
                                   size_t range_index, bool noncanonical) {
  MultiScanArgs range_args(all_scan_opts.GetComparator());
  range_args.CopyConfigFrom(all_scan_opts);
  if (noncanonical) {
    MultiScanArgsHelper::InitializeSeekState(&range_args);
  }
  const ScanOptions& range = all_scan_opts.GetScanRanges()[range_index];
  if (range.range.limit.has_value()) {
    range_args.insert(*range.range.start, *range.range.limit,
                      range.property_bag);
  } else {
    range_args.insert(*range.range.start, range.property_bag);
  }
  return range_args;
}

}  // namespace

MultiScan::MultiScan(const ReadOptions& read_options,
                     const MultiScanArgs& scan_opts, DB* db,
                     ColumnFamilyHandle* cfh)
    : read_options_(read_options),
      scan_opts_([&] {
        assert(db != nullptr);
        MultiScanArgs options = scan_opts;
        if (options.use_async_io &&
            !CheckFSFeatureSupport(db->GetFileSystem(),
                                   FSSupportedOps::kAsyncIO)) {
          options.use_async_io = false;
        }
        return options;
      }()),
      db_(db),
      cfh_(cfh) {
  assert(db_ != nullptr);
  const Status validate_status = ValidateScanOptionsForMultiScan(scan_opts_);
  if (!validate_status.ok()) {
    db_iter_.reset(NewErrorIterator(validate_status));
    return;
  }

  noncanonical_ = MultiScanArgsHelper::IsNonCanonical(scan_opts_);
  const bool mixed_bounds = HasMixedBounds(scan_opts_);
  prepare_each_range_ = mixed_bounds && noncanonical_;
  if (prepare_each_range_ && read_options_.snapshot == nullptr) {
    owned_snapshot_ = std::make_unique<ManagedSnapshot>(db_);
    if (owned_snapshot_->snapshot() == nullptr) {
      db_iter_.reset(NewErrorIterator(Status::NotSupported(
          "Mixed-bound MultiScan requires snapshot support")));
      return;
    }
    read_options_.snapshot = owned_snapshot_->snapshot();
  }

  const ScanOptions& first_scan = scan_opts_.GetScanRanges().front();
  if (scan_opts_.reverse) {
    lower_bound_ = *first_scan.range.start;
    read_options_.iterate_lower_bound = &lower_bound_;
    upper_bound_ = *first_scan.range.limit;
    read_options_.iterate_upper_bound = &upper_bound_;
  } else if (first_scan.range.limit.has_value()) {
    upper_bound_ = *first_scan.range.limit;
    read_options_.iterate_upper_bound = &upper_bound_;
  } else {
    read_options_.iterate_upper_bound = nullptr;
  }

  db_iter_.reset(db_->NewIterator(read_options_, cfh_));
  if (prepare_each_range_) {
    MultiScanArgs first_range =
        BuildSingleRangeArgs(scan_opts_, 0, noncanonical_);
    db_iter_->Prepare(first_range);
  } else if (!mixed_bounds) {
    db_iter_->Prepare(scan_opts_);
  }
}

MultiScan::~MultiScan() = default;

void MultiScanIterator::PrepareCurrentRange() {
  MultiScanArgs range_args =
      BuildSingleRangeArgs(all_scan_opts_, idx_, noncanonical_);
  db_iter_->Prepare(range_args);
}

void MultiScanIterator::SeekCurrentRange() {
  const ScanOptions& scan_opt = scan_opts_[idx_];
  if (scan_opt.range.limit.has_value()) {
    *upper_bound_ = *scan_opt.range.limit;
    read_options_.iterate_upper_bound = upper_bound_;
  } else {
    read_options_.iterate_upper_bound = nullptr;
  }

  if (reverse_) {
    *lower_bound_ = *scan_opt.range.start;
    read_options_.iterate_lower_bound = lower_bound_;
    assert(scan_opt.range.limit.has_value());
    db_iter_->SeekForPrev(*scan_opt.range.limit);
  } else {
    db_iter_->Seek(*scan_opt.range.start);
  }
}

MultiScanIterator& MultiScanIterator::operator++() {
  status_ = db_iter_->status();
  if (!status_.ok()) {
    throw MultiScanException(status_);
  }

  if (!valid_) {
    throw std::logic_error("Incrementing exhausted MultiScan iterator");
  }
  ++idx_;
  if (idx_ >= scan_opts_.size()) {
    valid_ = false;
    return *this;
  }

  if (prepare_each_range_ ||
      scan_opts_[idx_].range.limit.has_value() !=
          (read_options_.iterate_upper_bound != nullptr)) {
    if (scan_opts_[idx_].range.limit.has_value()) {
      read_options_.iterate_upper_bound = upper_bound_;
    } else {
      read_options_.iterate_upper_bound = nullptr;
    }
    db_iter_.reset(db_->NewIterator(read_options_, cfh_));
    scan_.Reset(db_iter_.get(), reverse_);
    if (prepare_each_range_) {
      PrepareCurrentRange();
      status_ = db_iter_->status();
      if (!status_.ok()) {
        throw MultiScanException(status_);
      }
    }
  }
  SeekCurrentRange();
  status_ = db_iter_->status();
  if (!status_.ok()) {
    throw MultiScanException(status_);
  }
  return *this;
}

}  // namespace ROCKSDB_NAMESPACE
