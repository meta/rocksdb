//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once

#include <deque>
#include <string>
#include <vector>

#include "db/merge_context.h"
#include "db/range_del_aggregator.h"
#include "db/snapshot_checker.h"
#include "db/wide/wide_column_serialization.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "rocksdb/wide_columns.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

class Comparator;
class Iterator;
class Logger;
class MergeOperator;
class Statistics;
class SystemClock;
class BlobFetcher;
class PrefetchBufferCollection;
struct CompactionIterationStats;

class MergeHelper {
 public:
  MergeHelper(Env* env, const Comparator* user_comparator,
              const MergeOperator* user_merge_operator,
              const CompactionFilter* compaction_filter,
              CompactionFilter::MergeOrder merge_order, Logger* logger,
              bool assert_valid_internal_key, SequenceNumber latest_snapshot,
              const SnapshotChecker* snapshot_checker = nullptr, int level = 0,
              Statistics* stats = nullptr,
              const std::atomic<bool>* shutting_down = nullptr);

  // Wrappers around MergeOperator::FullMergeV3() that record perf statistics.
  // Set `update_num_ops_stats` to true if it is from a user read so that
  // the corresponding statistics are updated.
  // Returns one of the following statuses:
  // - OK: Entries were successfully merged.
  // - Corruption: Merge operator reported unsuccessful merge. The scope of the
  //   damage will be stored in `*op_failure_scope` when `op_failure_scope` is
  //   not nullptr

  // Empty tag types to disambiguate overloads
  struct NoBaseValueTag {};
  static constexpr NoBaseValueTag kNoBaseValue{};

  struct PlainBaseValueTag {};
  static constexpr PlainBaseValueTag kPlainBaseValue{};

  struct WideBaseValueTag {};
  static constexpr WideBaseValueTag kWideBaseValue{};

  template <typename... ResultTs>
  static Status TimedFullMerge(const MergeOperator* merge_operator,
                               const Slice& key, NoBaseValueTag,
                               const std::vector<Slice>& operands,
                               Logger* logger, Statistics* statistics,
                               SystemClock* clock, bool update_num_ops_stats,
                               MergeOperator::OpFailureScope* op_failure_scope,
                               ResultTs... results) {
    MergeOperator::MergeOperationInputV3::ExistingValue existing_value;

    return TimedFullMergeImpl(
        merge_operator, key, std::move(existing_value), operands, logger,
        statistics, clock, update_num_ops_stats, op_failure_scope, results...);
  }

  template <typename... ResultTs>
  static Status TimedFullMerge(
      const MergeOperator* merge_operator, const Slice& key, PlainBaseValueTag,
      const Slice& value, const std::vector<Slice>& operands, Logger* logger,
      Statistics* statistics, SystemClock* clock, bool update_num_ops_stats,
      MergeOperator::OpFailureScope* op_failure_scope, ResultTs... results) {
    MergeOperator::MergeOperationInputV3::ExistingValue existing_value(value);

    return TimedFullMergeImpl(
        merge_operator, key, std::move(existing_value), operands, logger,
        statistics, clock, update_num_ops_stats, op_failure_scope, results...);
  }

  template <typename... ResultTs>
  static Status TimedFullMerge(
      const MergeOperator* merge_operator, const Slice& key, WideBaseValueTag,
      const Slice& entity, const std::vector<Slice>& operands, Logger* logger,
      Statistics* statistics, SystemClock* clock, bool update_num_ops_stats,
      MergeOperator::OpFailureScope* op_failure_scope, ResultTs... results) {
    MergeOperator::MergeOperationInputV3::ExistingValue existing_value;

    Slice entity_copy(entity);
    WideColumns existing_columns;

    const Status s = WideColumnSerialization::DeserializeSimple(
        entity_copy, existing_columns);
    if (!s.ok()) {
      return s;
    }

    existing_value = std::move(existing_columns);

    return TimedFullMergeImpl(
        merge_operator, key, std::move(existing_value), operands, logger,
        statistics, clock, update_num_ops_stats, op_failure_scope, results...);
  }

  template <typename... ResultTs>
  static Status TimedFullMerge(const MergeOperator* merge_operator,
                               const Slice& key, WideBaseValueTag,
                               const WideColumns& columns,
                               const std::vector<Slice>& operands,
                               Logger* logger, Statistics* statistics,
                               SystemClock* clock, bool update_num_ops_stats,
                               MergeOperator::OpFailureScope* op_failure_scope,
                               ResultTs... results) {
    MergeOperator::MergeOperationInputV3::ExistingValue existing_value(columns);

    return TimedFullMergeImpl(
        merge_operator, key, std::move(existing_value), operands, logger,
        statistics, clock, update_num_ops_stats, op_failure_scope, results...);
  }

  // During compaction, merge entries until we hit
  //     - a corrupted key
  //     - a Put/Delete,
  //     - a different user key,
  //     - a specific sequence number (snapshot boundary),
  //     - REMOVE_AND_SKIP_UNTIL returned from compaction filter,
  //  or - the end of iteration
  //
  // The result(s) of the merge can be accessed in `MergeHelper::keys()` and
  // `MergeHelper::values()`, which are invalidated the next time `MergeUntil()`
  // is called. `MergeOutputIterator` is specially designed to iterate the
  // results of a `MergeHelper`'s most recent `MergeUntil()`.
  //
  // iter: (IN)  points to the first merge type entry
  //       (OUT) points to the first entry not included in the merge process
  // range_del_agg: (IN) filters merge operands covered by range tombstones.
  // stop_before: (IN) a sequence number that merge should not cross.
  //                   0 means no restriction
  // at_bottom:   (IN) true if the iterator covers the bottem level, which means
  //                   we could reach the start of the history of this user key.
  // allow_data_in_errors: (IN) if true, data details will be displayed in
  //                   error/log messages.
  // blob_fetcher: (IN) blob fetcher object for the compaction's input version.
  // prefetch_buffers: (IN/OUT) a collection of blob file prefetch buffers
  //                            used for compaction readahead.
  // c_iter_stats: (OUT) compaction iteration statistics.
  //
  // Returns one of the following statuses:
  // - OK: Entries were successfully merged.
  // - MergeInProgress: Output consists of merge operands only.
  // - Corruption: Merge operator reported unsuccessful merge or a corrupted
  //   key has been encountered and not expected (applies only when compiling
  //   with asserts removed).
  // - ShutdownInProgress: interrupted by shutdown (*shutting_down == true).
  //
  // REQUIRED: The first key in the input is not corrupted.
  Status MergeUntil(InternalIterator* iter,
                    CompactionRangeDelAggregator* range_del_agg,
                    const SequenceNumber stop_before, const bool at_bottom,
                    const bool allow_data_in_errors,
                    const BlobFetcher* blob_fetcher,
                    const std::string* const full_history_ts_low,
                    PrefetchBufferCollection* prefetch_buffers,
                    CompactionIterationStats* c_iter_stats);

  // Filters a merge operand using the compaction filter specified
  // in the constructor. Returns the decision that the filter made.
  // Uses compaction_filter_value_ and compaction_filter_skip_until_ for the
  // optional outputs of compaction filter.
  // user_key includes timestamp if user-defined timestamp is enabled.
  CompactionFilter::Decision FilterMerge(const Slice& user_key,
                                         const Slice& value_slice);

  // Query the merge result
  // These are valid until the next MergeUntil call
  // If the merging was successful:
  //   - keys() contains a single element with the latest sequence number of
  //     the merges. The type will be Put or Merge. See IMPORTANT 1 note, below.
  //   - values() contains a single element with the result of merging all the
  //     operands together
  //
  //   IMPORTANT 1: the key type could change after the MergeUntil call.
  //        Put/Delete + Merge + ... + Merge => Put
  //        Merge + ... + Merge => Merge
  //
  // If the merge operator is not associative, and if a Put/Delete is not found
  // then the merging will be unsuccessful. In this case:
  //   - keys() contains the list of internal keys seen in order of iteration.
  //   - values() contains the list of values (merges) seen in the same order.
  //              values() is parallel to keys() so that the first entry in
  //              keys() is the key associated with the first entry in values()
  //              and so on. These lists will be the same length.
  //              All of these pairs will be merges over the same user key.
  //              See IMPORTANT 2 note below.
  //
  //   IMPORTANT 2: The entries were traversed in order from BACK to FRONT.
  //                So keys().back() was the first key seen by iterator.
  // TODO: Re-style this comment to be like the first one
  const std::deque<std::string>& keys() const { return keys_; }

  enum class Outcome { kUncombined, kFull, kPartial };
  Outcome outcome() const { return outcome_; }
  bool aggregate_eligible() const { return aggregate_eligible_; }
  bool history_exhausted() const { return history_exhausted_; }
  CompactionFilter::MergeOrder merge_order() const { return merge_order_; }
  bool OperandEligible(SequenceNumber seq) const {
    return seq > latest_snapshot_;
  }
  const std::vector<Slice>& values() const {
    return merge_context_.GetOperands();
  }
  uint64_t TotalFilterTime() const { return total_filter_time_; }
  bool HasOperator() const { return user_merge_operator_ != nullptr; }

  // If compaction filter returned REMOVE_AND_SKIP_UNTIL, this method will
  // return true and fill *until with the key to which we should skip.
  // If true, keys() and values() are empty.
  bool FilteredUntil(Slice* skip_until) const {
    if (!has_compaction_filter_skip_until_) {
      return false;
    }
    assert(compaction_filter_ != nullptr);
    assert(skip_until != nullptr);
    assert(compaction_filter_skip_until_.Valid());
    *skip_until = compaction_filter_skip_until_.Encode();
    return true;
  }

 private:
  Env* env_;
  SystemClock* clock_;
  const Comparator* user_comparator_;
  const MergeOperator* user_merge_operator_;
  const CompactionFilter* compaction_filter_;
  const CompactionFilter::MergeOrder merge_order_;
  Outcome outcome_ = Outcome::kUncombined;
  bool aggregate_eligible_ = true;
  bool history_exhausted_ = false;
  const std::atomic<bool>* shutting_down_;
  Logger* logger_;
  bool assert_valid_internal_key_;  // enforce no internal key corruption?
  bool allow_single_operand_;
  SequenceNumber latest_snapshot_;
  const SnapshotChecker* const snapshot_checker_;
  int level_;

  // the scratch area that holds the result of MergeUntil
  // valid up to the next MergeUntil call

  // Keeps track of the sequence of keys seen
  std::deque<std::string> keys_;
  // Parallel with keys_; stores the operands
  mutable MergeContext merge_context_;

  StopWatchNano<> filter_timer_;
  uint64_t total_filter_time_;
  Statistics* stats_;

  bool has_compaction_filter_skip_until_ = false;
  std::string compaction_filter_value_;
  InternalKey compaction_filter_skip_until_;

  bool IsShuttingDown() {
    // This is a best-effort facility, so memory_order_relaxed is sufficient.
    return shutting_down_ && shutting_down_->load(std::memory_order_relaxed);
  }

  template <typename Visitor>
  static Status TimedFullMergeCommonImpl(
      const MergeOperator* merge_operator, const Slice& key,
      MergeOperator::MergeOperationInputV3::ExistingValue&& existing_value,
      const std::vector<Slice>& operands, Logger* logger,
      Statistics* statistics, SystemClock* clock, bool update_num_ops_stats,
      MergeOperator::OpFailureScope* op_failure_scope, Visitor&& visitor);

  // Variant that exposes the merge result directly (in serialized form for wide
  // columns) as well as its value type. Used by iterator and compaction.
  static Status TimedFullMergeImpl(
      const MergeOperator* merge_operator, const Slice& key,
      MergeOperator::MergeOperationInputV3::ExistingValue&& existing_value,
      const std::vector<Slice>& operands, Logger* logger,
      Statistics* statistics, SystemClock* clock, bool update_num_ops_stats,
      MergeOperator::OpFailureScope* op_failure_scope, std::string* result,
      Slice* result_operand, ValueType* result_type);

  // Variant that exposes the merge result translated into the form requested by
  // the client. (For example, if the result is a wide-column structure but the
  // client requested the results in plain-value form, the value of the default
  // column is returned.) Used by point lookups.
  static Status TimedFullMergeImpl(
      const MergeOperator* merge_operator, const Slice& key,
      MergeOperator::MergeOperationInputV3::ExistingValue&& existing_value,
      const std::vector<Slice>& operands, Logger* logger,
      Statistics* statistics, SystemClock* clock, bool update_num_ops_stats,
      MergeOperator::OpFailureScope* op_failure_scope,
      std::string* result_value, PinnableWideColumns* result_entity);
};

// MergeOutputIterator can be used to iterate over the result of a merge.
class MergeOutputIterator {
 public:
  // The MergeOutputIterator is bound to a MergeHelper instance.
  explicit MergeOutputIterator(const MergeHelper* merge_helper);

  // Seeks to the first record in the output.
  void SeekToFirst();
  // Advances to the next record in the output.
  void Next();
  void Invalidate() {
    it_keys_ = merge_helper_->keys().rend();
    it_values_ = merge_helper_->values().rend();
  }

  Slice key() { return Slice(*it_keys_); }
  Slice value() { return Slice(*it_values_); }
  bool Valid() const { return it_keys_ != merge_helper_->keys().rend(); }

 private:
  const MergeHelper* merge_helper_;
  std::deque<std::string>::const_reverse_iterator it_keys_;
  std::vector<Slice>::const_reverse_iterator it_values_;
};

}  // namespace ROCKSDB_NAMESPACE
