//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <cassert>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "file/file_util.h"
#include "rocksdb/db.h"
#include "rocksdb/file_system.h"
#include "rocksdb/iterator.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/table.h"

namespace ROCKSDB_NAMESPACE {

using MultiScanIterator = MultiScan::MultiScanIterator;

namespace {

using StagedEntry = std::pair<std::string, std::string>;
using StagedRanges = std::vector<std::vector<StagedEntry>>;

// Adapts caller-ordered staged ranges to the single Iterator consumed by the
// existing nested MultiScan iterators. Each outer Seek selects the next range.
class StagedMultiScanIterator final : public Iterator {
 public:
  explicit StagedMultiScanIterator(StagedRanges&& ranges)
      : ranges_(std::move(ranges)) {}

  bool Valid() const override { return valid_; }

  void SeekToFirst() override { SelectRange(/*reverse=*/false, 0); }

  void SeekToLast() override { SelectRange(/*reverse=*/true, 0); }

  void Seek(const Slice& /*target*/) override {
    SelectNextRange(/*reverse=*/false);
  }

  void SeekForPrev(const Slice& /*target*/) override {
    SelectNextRange(/*reverse=*/true);
  }

  void Next() override {
    assert(valid_);
    ++entry_index_;
    valid_ = entry_index_ < ranges_[range_index_].size();
  }

  void Prev() override {
    assert(valid_);
    if (entry_index_ == 0) {
      valid_ = false;
    } else {
      --entry_index_;
    }
  }

  Slice key() const override {
    assert(valid_);
    return ranges_[range_index_][entry_index_].first;
  }

  Slice value() const override {
    assert(valid_);
    return ranges_[range_index_][entry_index_].second;
  }

  Status status() const override { return Status::OK(); }

 private:
  void SelectNextRange(bool reverse) {
    if (next_range_index_ >= ranges_.size()) {
      valid_ = false;
      return;
    }
    SelectRange(reverse, next_range_index_++);
  }

  void SelectRange(bool reverse, size_t range_index) {
    if (range_index >= ranges_.size()) {
      valid_ = false;
      return;
    }
    range_index_ = range_index;
    valid_ = !ranges_[range_index_].empty();
    if (valid_) {
      entry_index_ = reverse ? ranges_[range_index_].size() - 1 : 0;
    }
  }

  StagedRanges ranges_;
  size_t next_range_index_ = 0;
  size_t range_index_ = 0;
  size_t entry_index_ = 0;
  bool valid_ = false;
};

int CompareUserKeys(const Comparator& comparator, const Slice& lhs,
                    const Slice& rhs) {
  return comparator.CompareWithoutTimestamp(lhs, /*a_has_ts=*/false, rhs,
                                            /*b_has_ts=*/false);
}

Status ValidateScanOptionsForMultiScan(const MultiScanArgs& multiscan_opts) {
  if (multiscan_opts.empty()) {
    return Status::InvalidArgument("Empty MultiScanArgs");
  }

  const Comparator* comparator_ptr = multiscan_opts.GetComparator();
  if (comparator_ptr == nullptr) {
    return Status::InvalidArgument("MultiScanArgs requires comparator");
  }
  const Comparator& comparator = *comparator_ptr;

  const std::vector<ScanOptions>& scan_opts = multiscan_opts.GetScanRanges();
  for (size_t i = 0; i < scan_opts.size(); ++i) {
    const auto& scan_range = scan_opts[i].range;
    if (!scan_range.start.has_value()) {
      return Status::InvalidArgument("Scan has no start key at index " +
                                     std::to_string(i));
    }
    if (multiscan_opts.reverse && !scan_range.limit.has_value()) {
      return Status::InvalidArgument(
          "Reverse MultiScan requires limit at index " + std::to_string(i));
    }
    if (scan_range.limit.has_value() &&
        comparator.CompareWithoutTimestamp(
            scan_range.start.value(), /*a_has_ts=*/false,
            scan_range.limit.value(), /*b_has_ts=*/false) >= 0) {
      return Status::InvalidArgument(
          "Scan start key is large or equal than limit at index " +
          std::to_string(i));
    }
  }

  return Status::OK();
}

bool HasSortedNonOverlappingRanges(const MultiScanArgs& multiscan_opts) {
  const Comparator* comparator_ptr = multiscan_opts.GetComparator();
  assert(comparator_ptr != nullptr);
  const Comparator& comparator = *comparator_ptr;
  const std::vector<ScanOptions>& scan_opts = multiscan_opts.GetScanRanges();
  for (size_t i = 1; i < scan_opts.size(); ++i) {
    const auto& previous_limit = scan_opts[i - 1].range.limit;
    assert(scan_opts[i].range.start.has_value());
    if (!previous_limit.has_value() ||
        comparator.CompareWithoutTimestamp(
            scan_opts[i].range.start.value(), /*a_has_ts=*/false,
            previous_limit.value(), /*b_has_ts=*/false) < 0) {
      return false;
    }
  }
  return true;
}

std::vector<size_t> GetRangesSortedByStart(
    const MultiScanArgs& multiscan_opts) {
  std::vector<size_t> indices(multiscan_opts.size());
  std::iota(indices.begin(), indices.end(), 0);
  const Comparator* comparator_ptr = multiscan_opts.GetComparator();
  assert(comparator_ptr != nullptr);
  const Comparator& comparator = *comparator_ptr;
  const auto& ranges = multiscan_opts.GetScanRanges();
  std::stable_sort(indices.begin(), indices.end(),
                   [&comparator, &ranges](size_t lhs, size_t rhs) {
                     return CompareUserKeys(comparator,
                                            *ranges[lhs].range.start,
                                            *ranges[rhs].range.start) < 0;
                   });
  return indices;
}

MultiScanArgs BuildPhysicalScanArgs(const MultiScanArgs& logical_args,
                                    const std::vector<size_t>& sorted_indices) {
  const Comparator* comparator_ptr = logical_args.GetComparator();
  assert(comparator_ptr != nullptr);
  const Comparator& comparator = *comparator_ptr;
  const auto& logical_ranges = logical_args.GetScanRanges();
  MultiScanArgs physical_args(&comparator);
  physical_args.CopyConfigFrom(logical_args);
  physical_args.allow_unsorted_or_overlapping_ranges = false;
  physical_args.reverse = false;

  const bool has_unbounded_range = std::any_of(
      logical_ranges.begin(), logical_ranges.end(),
      [](const ScanOptions& range) { return !range.range.limit.has_value(); });
  if (has_unbounded_range) {
    physical_args.insert(*logical_ranges[sorted_indices.front()].range.start);
    return physical_args;
  }

  std::vector<RangeOpt> physical_ranges;
  physical_ranges.reserve(logical_ranges.size());

  for (size_t logical_index : sorted_indices) {
    const RangeOpt& range = logical_ranges[logical_index].range;
    assert(range.start.has_value());
    assert(range.limit.has_value());
    if (physical_ranges.empty() ||
        CompareUserKeys(comparator, *range.start,
                        *physical_ranges.back().limit) > 0) {
      physical_ranges.push_back(range);
      continue;
    }
    if (CompareUserKeys(comparator, *range.limit,
                        *physical_ranges.back().limit) > 0) {
      physical_ranges.back().limit = range.limit;
    }
  }

  physical_args.reserve(physical_ranges.size());
  for (const RangeOpt& range : physical_ranges) {
    physical_args.insert(*range.start, *range.limit);
  }
  return physical_args;
}

Status StageRangesIndependently(const ReadOptions& read_options,
                                const MultiScanArgs& logical_args, DB& db,
                                ColumnFamilyHandle* cfh,
                                StagedRanges& staged_ranges) {
  const Comparator* comparator_ptr = logical_args.GetComparator();
  assert(comparator_ptr != nullptr);
  const Comparator& comparator = *comparator_ptr;
  const auto& logical_ranges = logical_args.GetScanRanges();
  std::unique_ptr<ManagedSnapshot> snapshot;
  const Snapshot* read_snapshot = read_options.snapshot;
  if (read_snapshot == nullptr) {
    snapshot = std::make_unique<ManagedSnapshot>(&db);
    read_snapshot = snapshot->snapshot();
    if (read_snapshot == nullptr) {
      return Status::NotSupported(
          "MultiScan range staging requires snapshot support");
    }
  }

  staged_ranges.clear();
  staged_ranges.resize(logical_ranges.size());
  for (size_t range_index = 0; range_index < logical_ranges.size();
       ++range_index) {
    const ScanOptions& scan_options = logical_ranges[range_index];
    const RangeOpt& range = scan_options.range;
    Slice lower_bound;
    Slice upper_bound;
    ReadOptions logical_read_options = read_options;
    logical_read_options.snapshot = read_snapshot;
    if (logical_args.reverse) {
      assert(range.limit.has_value());
      lower_bound = *range.start;
      upper_bound = *range.limit;
      logical_read_options.iterate_lower_bound = &lower_bound;
      logical_read_options.iterate_upper_bound = &upper_bound;
    } else {
      logical_read_options.iterate_lower_bound =
          read_options.iterate_lower_bound;
      if (range.limit.has_value()) {
        upper_bound = *range.limit;
        logical_read_options.iterate_upper_bound = &upper_bound;
      } else {
        logical_read_options.iterate_upper_bound = nullptr;
      }
    }

    std::unique_ptr<Iterator> iter(db.NewIterator(logical_read_options, cfh));
    MultiScanArgs physical_args(&comparator);
    physical_args.CopyConfigFrom(logical_args);
    physical_args.allow_unsorted_or_overlapping_ranges = false;
    if (range.limit.has_value()) {
      physical_args.insert(*range.start, *range.limit,
                           scan_options.property_bag);
    } else {
      physical_args.insert(*range.start, scan_options.property_bag);
    }
    iter->Prepare(physical_args);
    if (!iter->status().ok()) {
      return iter->status();
    }

    if (logical_args.reverse) {
      iter->SeekForPrev(*range.limit);
      while (iter->Valid() &&
             CompareUserKeys(comparator, iter->key(), *range.limit) >= 0) {
        iter->Prev();
      }
    } else {
      iter->Seek(*range.start);
    }

    while (iter->Valid() &&
           CompareUserKeys(comparator, iter->key(), *range.start) >= 0 &&
           (!range.limit.has_value() ||
            CompareUserKeys(comparator, iter->key(), *range.limit) < 0)) {
      const std::string key = iter->key().ToString();
      if (!iter->PrepareValue()) {
        return iter->status();
      }
      staged_ranges[range_index].emplace_back(key, iter->value().ToString());
      if (logical_args.reverse) {
        iter->Prev();
      } else {
        iter->Next();
      }
    }
    if (logical_args.reverse) {
      std::reverse(staged_ranges[range_index].begin(),
                   staged_ranges[range_index].end());
    }
    if (!iter->status().ok()) {
      return iter->status();
    }
  }
  return Status::OK();
}

bool HasRangePropertyBag(const MultiScanArgs& logical_args) {
  return std::any_of(
      logical_args.GetScanRanges().begin(), logical_args.GetScanRanges().end(),
      [](const ScanOptions& range) { return range.property_bag.has_value(); });
}

Status StageArbitraryRanges(const ReadOptions& read_options,
                            const MultiScanArgs& logical_args, DB& db,
                            ColumnFamilyHandle* cfh,
                            StagedRanges& staged_ranges) {
  if (read_options.prefix_same_as_start || HasRangePropertyBag(logical_args)) {
    return StageRangesIndependently(read_options, logical_args, db, cfh,
                                    staged_ranges);
  }

  const Comparator* comparator_ptr = logical_args.GetComparator();
  assert(comparator_ptr != nullptr);
  const Comparator& comparator = *comparator_ptr;
  const auto& logical_ranges = logical_args.GetScanRanges();
  const std::vector<size_t> sorted_indices =
      GetRangesSortedByStart(logical_args);
  MultiScanArgs physical_args =
      BuildPhysicalScanArgs(logical_args, sorted_indices);

  ReadOptions physical_read_options = read_options;
  Slice upper_bound;
  if (logical_args.reverse) {
    physical_read_options.iterate_lower_bound = nullptr;
  }
  const ScanOptions& first_physical_range =
      physical_args.GetScanRanges().front();
  if (first_physical_range.range.limit.has_value()) {
    upper_bound = *first_physical_range.range.limit;
    physical_read_options.iterate_upper_bound = &upper_bound;
  } else {
    physical_read_options.iterate_upper_bound = nullptr;
  }
  std::unique_ptr<Iterator> physical_iter(
      db.NewIterator(physical_read_options, cfh));
  physical_iter->Prepare(physical_args);
  if (!physical_iter->status().ok()) {
    return physical_iter->status();
  }

  staged_ranges.clear();
  staged_ranges.resize(logical_ranges.size());
  std::vector<size_t> active_ranges;
  active_ranges.reserve(logical_ranges.size());
  size_t next_range_index = 0;

  for (const ScanOptions& physical_range : physical_args.GetScanRanges()) {
    if (physical_range.range.limit.has_value()) {
      upper_bound = *physical_range.range.limit;
    }
    physical_iter->Seek(*physical_range.range.start);
    while (physical_iter->Valid()) {
      const Slice key = physical_iter->key();
      while (next_range_index < sorted_indices.size() &&
             CompareUserKeys(
                 comparator,
                 *logical_ranges[sorted_indices[next_range_index]].range.start,
                 key) <= 0) {
        active_ranges.push_back(sorted_indices[next_range_index++]);
      }
      active_ranges.erase(
          std::remove_if(active_ranges.begin(), active_ranges.end(),
                         [&](size_t range_index) {
                           const OptSlice& limit =
                               logical_ranges[range_index].range.limit;
                           return limit.has_value() &&
                                  CompareUserKeys(comparator, *limit, key) <= 0;
                         }),
          active_ranges.end());

      const std::string key_copy = key.ToString();
      if (!physical_iter->PrepareValue()) {
        return physical_iter->status();
      }
      const std::string value_copy = physical_iter->value().ToString();
      for (size_t range_index : active_ranges) {
        staged_ranges[range_index].emplace_back(key_copy, value_copy);
      }
      physical_iter->Next();
    }
    if (!physical_iter->status().ok()) {
      return physical_iter->status();
    }
  }
  return Status::OK();
}

}  // namespace

MultiScan::MultiScan(const ReadOptions& read_options,
                     const MultiScanArgs& scan_opts, DB* db,
                     ColumnFamilyHandle* cfh)
    : read_options_(read_options),
      scan_opts_([&] {
        assert(db != nullptr);
        // Disable async IO if the filesystem does not support it, consistent
        // with how ArenaWrappedDBIter::Init() handles ReadOptions::async_io.
        auto opts = scan_opts;
        if (opts.use_async_io &&
            !CheckFSFeatureSupport(db->GetFileSystem(),
                                   FSSupportedOps::kAsyncIO)) {
          opts.use_async_io = false;
        }
        return opts;
      }()),
      db_(db),
      cfh_(cfh) {
  assert(db_ != nullptr);
  DB& database = *db_;
  Status validate_status = ValidateScanOptionsForMultiScan(scan_opts_);
  if (!validate_status.ok()) {
    db_iter_.reset(NewErrorIterator(validate_status));
    return;
  }

  const bool arbitrary_ranges =
      scan_opts_.allow_unsorted_or_overlapping_ranges &&
      !HasSortedNonOverlappingRanges(scan_opts_);
  if (arbitrary_ranges) {
    const std::shared_ptr<TableFactory> table_factory =
        database.GetOptions(cfh_).table_factory;
    if (table_factory != nullptr &&
        !table_factory->SupportsMultiScan(scan_opts_)) {
      db_iter_.reset(NewErrorIterator(Status::NotSupported(
          "Table factory does not support unsorted or overlapping "
          "MultiScan ranges")));
      return;
    }
    StagedRanges staged_ranges;
    Status stage_status = StageArbitraryRanges(read_options_, scan_opts_,
                                               database, cfh_, staged_ranges);
    if (!stage_status.ok()) {
      db_iter_.reset(NewErrorIterator(stage_status));
      return;
    }
    db_iter_ =
        std::make_unique<StagedMultiScanIterator>(std::move(staged_ranges));
    staged_results_ = true;
    return;
  }

  bool slow_path = false;
  // Setup read_options bounds based on the first scan. Subsequent scans will
  // update the bounds and allocate a new DB iterator as necessary.
  {
    const ScanOptions& first_scan = scan_opts_.GetScanRanges()[0];
    if (scan_opts_.reverse) {
      lower_bound_ = *first_scan.range.start;
      read_options_.iterate_lower_bound = &lower_bound_;
      upper_bound_ = *first_scan.range.limit;
      read_options_.iterate_upper_bound = &upper_bound_;
    } else if (first_scan.range.limit) {
      upper_bound_ = *first_scan.range.limit;
      read_options_.iterate_upper_bound = &upper_bound_;
    } else {
      read_options_.iterate_upper_bound = nullptr;
    }
  }
  for (const auto& opts : scan_opts_.GetScanRanges()) {
    // Check that all the ScanOptions either specify an upper bound or not. If
    // its mixed we take the slow path which avoids calling Prepare: we have to
    // reallocate the Iterator with updated read_options everytime we switch
    // between upper bound or no upper bound, which complicates Prepare.
    if (opts.range.limit.has_value() !=
        scan_opts_.GetScanRanges()[0].range.limit.has_value()) {
      slow_path = true;
      break;
    }
  }
  db_iter_.reset(db->NewIterator(read_options_, cfh));
  if (!slow_path) {
    db_iter_->Prepare(scan_opts_);
  }
}

void MultiScanIterator::SeekCurrentRange() {
  const ScanOptions& scan_opt = scan_opts_[idx_];
  if (scan_opt.range.limit) {
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

  // Check if we need to update read_options_
  if (!staged_results_ && scan_opts_[idx_].range.limit.has_value() !=
                              (read_options_.iterate_upper_bound != nullptr)) {
    if (scan_opts_[idx_].range.limit) {
      read_options_.iterate_upper_bound = upper_bound_;
    } else {
      read_options_.iterate_upper_bound = nullptr;
    }
    db_iter_.reset(db_->NewIterator(read_options_, cfh_));
    scan_.Reset(db_iter_.get(), reverse_);
  }
  SeekCurrentRange();
  status_ = db_iter_->status();
  if (!status_.ok()) {
    throw MultiScanException(status_);
  }
  return *this;
}

}  // namespace ROCKSDB_NAMESPACE
