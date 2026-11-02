//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/merge_helper.h"

#include <string>

#include "db/blob/blob_fetcher.h"
#include "db/blob/blob_index.h"
#include "db/blob/prefetch_buffer_collection.h"
#include "db/compaction/compaction_iteration_stats.h"
#include "db/dbformat.h"
#include "db/wide/wide_column_serialization.h"
#include "db/wide/wide_columns_helper.h"
#include "logging/logging.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics_impl.h"
#include "port/likely.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/system_clock.h"
#include "table/format.h"
#include "table/internal_iterator.h"
#include "util/overload.h"

namespace ROCKSDB_NAMESPACE {

MergeHelper::MergeHelper(Env* env, const Comparator* user_comparator,
                         const MergeOperator* user_merge_operator,
                         const CompactionFilter* compaction_filter,
                         CompactionFilter::MergeOrder merge_order,
                         Logger* logger, bool assert_valid_internal_key,
                         SequenceNumber latest_snapshot,
                         const SnapshotChecker* snapshot_checker, int level,
                         Statistics* stats,
                         const std::atomic<bool>* shutting_down)
    : env_(env),
      clock_(env->GetSystemClock().get()),
      user_comparator_(user_comparator),
      user_merge_operator_(user_merge_operator),
      compaction_filter_(compaction_filter),
      merge_order_(merge_order),
      shutting_down_(shutting_down),
      logger_(logger),
      assert_valid_internal_key_(assert_valid_internal_key),
      allow_single_operand_(false),
      latest_snapshot_(latest_snapshot),
      snapshot_checker_(snapshot_checker),
      level_(level),
      keys_(),
      filter_timer_(clock_),
      total_filter_time_(0U),
      stats_(stats) {
  assert(user_comparator_ != nullptr);
  if (user_merge_operator_) {
    allow_single_operand_ = user_merge_operator_->AllowSingleOperand();
  }
}

template <typename Visitor>
Status MergeHelper::TimedFullMergeCommonImpl(
    const MergeOperator* merge_operator, const Slice& key,
    MergeOperator::MergeOperationInputV3::ExistingValue&& existing_value,
    const std::vector<Slice>& operands, Logger* logger, Statistics* statistics,
    SystemClock* clock, bool update_num_ops_stats,
    MergeOperator::OpFailureScope* op_failure_scope, Visitor&& visitor) {
  assert(merge_operator);
  assert(!operands.empty());

  if (update_num_ops_stats) {
    RecordInHistogram(statistics, READ_NUM_MERGE_OPERANDS,
                      static_cast<uint64_t>(operands.size()));
  }

  const MergeOperator::MergeOperationInputV3 merge_in(
      key, std::move(existing_value), operands, logger);
  MergeOperator::MergeOperationOutputV3 merge_out;

  bool success = false;

  {
    StopWatchNano timer(clock, statistics != nullptr);
    PERF_TIMER_GUARD(merge_operator_time_nanos);

    success = merge_operator->FullMergeV3(merge_in, &merge_out);

    RecordTick(statistics, MERGE_OPERATION_TOTAL_TIME,
               statistics ? timer.ElapsedNanos() : 0);
  }

  if (!success) {
    RecordTick(statistics, NUMBER_MERGE_FAILURES);

    if (op_failure_scope) {
      *op_failure_scope = merge_out.op_failure_scope;
      // Apply default per merge_operator.h
      if (*op_failure_scope == MergeOperator::OpFailureScope::kDefault) {
        *op_failure_scope = MergeOperator::OpFailureScope::kTryMerge;
      }
    }

    return Status::Corruption(Status::SubCode::kMergeOperatorFailed);
  }

  return std::visit(std::forward<Visitor>(visitor),
                    std::move(merge_out.new_value));
}

Status MergeHelper::TimedFullMergeImpl(
    const MergeOperator* merge_operator, const Slice& key,
    MergeOperator::MergeOperationInputV3::ExistingValue&& existing_value,
    const std::vector<Slice>& operands, Logger* logger, Statistics* statistics,
    SystemClock* clock, bool update_num_ops_stats,
    MergeOperator::OpFailureScope* op_failure_scope, std::string* result,
    Slice* result_operand, ValueType* result_type) {
  assert(result);
  assert(result_type);

  auto visitor = overload{
      [&](std::string&& new_value) -> Status {
        *result_type = kTypeValue;

        if (result_operand) {
          *result_operand = Slice(nullptr, 0);
        }

        *result = std::move(new_value);

        return Status::OK();
      },
      [&](MergeOperator::MergeOperationOutputV3::NewColumns&& new_columns)
          -> Status {
        *result_type = kTypeWideColumnEntity;

        if (result_operand) {
          *result_operand = Slice(nullptr, 0);
        }

        result->clear();

        WideColumns sorted_columns;
        sorted_columns.reserve(new_columns.size());

        for (const auto& column : new_columns) {
          sorted_columns.emplace_back(column.first, column.second);
        }

        WideColumnsHelper::SortColumns(sorted_columns);

        return WideColumnSerialization::Serialize(sorted_columns, *result);
      },
      [&](Slice&& operand) -> Status {
        *result_type = kTypeValue;

        if (result_operand) {
          *result_operand = operand;
          result->clear();
        } else {
          result->assign(operand.data(), operand.size());
        }

        return Status::OK();
      }};

  Status s = TimedFullMergeCommonImpl(
      merge_operator, key, std::move(existing_value), operands, logger,
      statistics, clock, update_num_ops_stats, op_failure_scope,
      std::move(visitor));
  if (s.ok()) {
    // Check merge result fits in uint32_t (a BlockBuilder assumption)
    size_t result_size = (result_operand && !result_operand->empty())
                             ? result_operand->size()
                             : result->size();
    if (UNLIKELY(result_size > std::numeric_limits<uint32_t>::max())) {
      return Status::Corruption("Merge result exceeds 4GB limit");
    }
  }
  return s;
}

Status MergeHelper::TimedFullMergeImpl(
    const MergeOperator* merge_operator, const Slice& key,
    MergeOperator::MergeOperationInputV3::ExistingValue&& existing_value,
    const std::vector<Slice>& operands, Logger* logger, Statistics* statistics,
    SystemClock* clock, bool update_num_ops_stats,
    MergeOperator::OpFailureScope* op_failure_scope, std::string* result_value,
    PinnableWideColumns* result_entity) {
  assert(result_value || result_entity);
  assert(!result_value || !result_entity);

  auto visitor = overload{
      [&](std::string&& new_value) -> Status {
        if (result_value) {
          *result_value = std::move(new_value);

          return Status::OK();
        }

        assert(result_entity);
        result_entity->SetPlainValue(std::move(new_value));

        return Status::OK();
      },
      [&](MergeOperator::MergeOperationOutputV3::NewColumns&& new_columns)
          -> Status {
        if (result_value) {
          if (!new_columns.empty() &&
              new_columns.front().first == kDefaultWideColumnName) {
            *result_value = std::move(new_columns.front().second);
          } else {
            result_value->clear();
          }

          return Status::OK();
        }

        assert(result_entity);

        WideColumns sorted_columns;
        sorted_columns.reserve(new_columns.size());

        for (const auto& column : new_columns) {
          sorted_columns.emplace_back(column.first, column.second);
        }

        WideColumnsHelper::SortColumns(sorted_columns);

        std::string result;
        const Status s =
            WideColumnSerialization::Serialize(sorted_columns, result);
        if (!s.ok()) {
          result_entity->Reset();
          return s;
        }

        return result_entity->SetWideColumnValue(std::move(result));
      },
      [&](Slice&& operand) -> Status {
        if (result_value) {
          result_value->assign(operand.data(), operand.size());

          return Status::OK();
        }

        assert(result_entity);
        result_entity->SetPlainValue(operand);

        return Status::OK();
      }};

  Status s = TimedFullMergeCommonImpl(
      merge_operator, key, std::move(existing_value), operands, logger,
      statistics, clock, update_num_ops_stats, op_failure_scope,
      std::move(visitor));
  if (s.ok()) {
    // Check merge result fits in uint32_t (a BlockBuilder assumption). For an
    // entity result this is the V1-serialized size that will actually be
    // stored, so use SerializedSizeV1() rather than payload_size().
    size_t result_size = result_value
                             ? result_value->size()
                             : WideColumnSerialization::SerializedSizeV1(
                                   result_entity->columns());
    if (UNLIKELY(result_size > std::numeric_limits<uint32_t>::max())) {
      return Status::Corruption("Merge result exceeds 4GB");
    }
  }
  return s;
}

// PRE:  iter points to the first merge type entry
// POST: iter points to the first entry beyond the merge process (or the end)
//       keys_, operands_ are updated to reflect the merge result.
//       keys_ stores the list of keys encountered while merging.
//       operands_ stores the list of merge operands encountered while merging.
//       keys_[i] corresponds to operands_[i] for each i.
//
// TODO: Avoid the snapshot stripe map lookup in CompactionRangeDelAggregator
// and just pass the StripeRep corresponding to the stripe being merged.
Status MergeHelper::MergeUntil(InternalIterator* iter,
                               CompactionRangeDelAggregator* range_del_agg,
                               const SequenceNumber stop_before,
                               const bool at_bottom,
                               const bool allow_data_in_errors,
                               const BlobFetcher* blob_fetcher,
                               const std::string* const full_history_ts_low,
                               PrefetchBufferCollection* prefetch_buffers,
                               CompactionIterationStats* c_iter_stats) {
  // Get a copy of the internal key, before it's invalidated by iter->Next()
  // Also maintain the list of merge operands seen.
  assert(HasOperator());
  keys_.clear();
  merge_context_.Clear();
  has_compaction_filter_skip_until_ = false;
  outcome_ = Outcome::kUncombined;
  aggregate_eligible_ = true;
  assert(user_merge_operator_);
  assert(user_comparator_);
  const size_t ts_sz = user_comparator_->timestamp_size();
  if (full_history_ts_low) {
    assert(ts_sz > 0);
    assert(ts_sz == full_history_ts_low->size());
  }
  bool first_key = true;

  // We need to parse the internal key again as the parsed key is
  // backed by the internal key!
  // Assume no internal key corruption as it has been successfully parsed
  // by the caller.
  // original_key_is_iter variable is just caching the information:
  // original_key_is_iter == (iter->key().ToString() == original_key)
  bool original_key_is_iter = true;
  std::string original_key = iter->key().ToString();
  // Important:
  // orig_ikey is backed by original_key if keys_.empty()
  // orig_ikey is backed by keys_.back() if !keys_.empty()
  ParsedInternalKey orig_ikey;

  Status s = ParseInternalKey(original_key, &orig_ikey, allow_data_in_errors);
  assert(s.ok());
  if (!s.ok()) {
    return s;
  }

  assert(kTypeMerge == orig_ikey.type);

  bool hit_the_next_user_key = false;
  int cmp_with_full_history_ts_low = 0;
  for (; iter->Valid(); iter->Next(), original_key_is_iter = false) {
    if (IsShuttingDown()) {
      s = Status::ShutdownInProgress();
      return s;
    }
    // Skip range tombstones emitted by the compaction iterator.
    if (iter->IsDeleteRangeSentinelKey()) {
      continue;
    }

    ParsedInternalKey ikey;
    assert(keys_.size() == merge_context_.GetNumOperands());

    Status pik_status =
        ParseInternalKey(iter->key(), &ikey, allow_data_in_errors);
    Slice ts;
    if (pik_status.ok()) {
      ts = ExtractTimestampFromUserKey(ikey.user_key, ts_sz);
      if (full_history_ts_low) {
        cmp_with_full_history_ts_low =
            user_comparator_->CompareTimestamp(ts, *full_history_ts_low);
      }
    }
    if (!pik_status.ok()) {
      // stop at corrupted key
      if (assert_valid_internal_key_) {
        return pik_status;
      }
      break;
    } else if (first_key) {
      // If user-defined timestamp is enabled, we expect both user key and
      // timestamps are equal, as a sanity check.
      assert(user_comparator_->Equal(ikey.user_key, orig_ikey.user_key));
      first_key = false;
    } else if (!user_comparator_->EqualWithoutTimestamp(ikey.user_key,
                                                        orig_ikey.user_key) ||
               (ts_sz > 0 &&
                !user_comparator_->Equal(ikey.user_key, orig_ikey.user_key) &&
                cmp_with_full_history_ts_low >= 0)) {
      // 1) hit a different user key, or
      // 2) user-defined timestamp is enabled, and hit a version of user key NOT
      // eligible for GC, then stop right here.
      hit_the_next_user_key = true;
      break;
    } else if (stop_before > 0 && ikey.sequence <= stop_before &&
               LIKELY(snapshot_checker_ == nullptr ||
                      snapshot_checker_->CheckInSnapshot(ikey.sequence,
                                                         stop_before) !=
                          SnapshotCheckerResult::kNotInSnapshot)) {
      // hit an entry that's possibly visible by the previous snapshot, can't
      // touch that
      break;
    }

    // At this point we are guaranteed that we need to process this key.

    assert(IsValueType(ikey.type));
    if (ikey.type != kTypeMerge) {
      // hit a put/delete/single delete
      //   => merge the put value or a nullptr with operands_
      //   => store result in operands_.back() (and update keys_.back())
      //   => change the entry type for keys_.back()
      // We are done! Success!

      // If there are no operands, just return the Status::OK(). That will cause
      // the compaction iterator to write out the key we're currently at, which
      // is the put/delete we just encountered.
      if (keys_.empty()) {
        return s;
      }

      // TODO: if we're in compaction and it's a put, it would be nice to run
      // compaction filter on it.
      std::string merge_result;
      ValueType merge_result_type = kTypeValue;
      MergeOperator::OpFailureScope op_failure_scope =
          MergeOperator::OpFailureScope::kDefault;

      if (range_del_agg &&
          range_del_agg->ShouldDelete(
              ikey, RangeDelPositioningMode::kForwardTraversal)) {
        s = TimedFullMerge(user_merge_operator_, ikey.user_key, kNoBaseValue,
                           merge_context_.GetOperands(), logger_, stats_,
                           clock_, /* update_num_ops_stats */ false,
                           &op_failure_scope, &merge_result,
                           /* result_operand */ nullptr, &merge_result_type);
      } else if (ikey.type == kTypeValue) {
        s = TimedFullMerge(user_merge_operator_, ikey.user_key, kPlainBaseValue,
                           iter->value(), merge_context_.GetOperands(), logger_,
                           stats_, clock_, /* update_num_ops_stats */ false,
                           &op_failure_scope, &merge_result,
                           /* result_operand */ nullptr, &merge_result_type);
      } else if (ikey.type == kTypeValuePreferredSeqno) {
        // When a TimedPut is merged with some merge operands, its original
        // write time info is obsolete and removed, and the merge result is a
        // kTypeValue.
        Slice unpacked_value = ParsePackedValueForValue(iter->value());
        s = TimedFullMerge(user_merge_operator_, ikey.user_key, kPlainBaseValue,
                           unpacked_value, merge_context_.GetOperands(),
                           logger_, stats_, clock_,
                           /* update_num_ops_stats */ false, &op_failure_scope,
                           &merge_result,
                           /* result_operand */ nullptr, &merge_result_type);

      } else if (ikey.type == kTypeBlobIndex) {
        BlobIndex blob_index;

        s = blob_index.DecodeFrom(iter->value());
        if (!s.ok()) {
          return s;
        }

        FilePrefetchBuffer* prefetch_buffer =
            prefetch_buffers ? prefetch_buffers->GetOrCreatePrefetchBuffer(
                                   blob_index.file_number())
                             : nullptr;

        uint64_t bytes_read = 0;

        assert(blob_fetcher);

        PinnableSlice blob_value;
        s = blob_fetcher->FetchBlob(ikey.user_key, blob_index, prefetch_buffer,
                                    &blob_value, &bytes_read);
        if (!s.ok()) {
          return s;
        }

        if (c_iter_stats) {
          ++c_iter_stats->num_blobs_read;
          c_iter_stats->total_blob_bytes_read += bytes_read;
        }

        s = TimedFullMerge(user_merge_operator_, ikey.user_key, kPlainBaseValue,
                           blob_value, merge_context_.GetOperands(), logger_,
                           stats_, clock_, /* update_num_ops_stats */ false,
                           &op_failure_scope, &merge_result,
                           /* result_operand */ nullptr, &merge_result_type);
      } else if (ikey.type == kTypeWideColumnEntity) {
        // Resolve V2 entity blob columns if present, since TimedFullMerge
        // only supports V1 format.
        std::string resolved_entity;
        Slice effective_entity;
        s = WideColumnSerialization::ResolveEntityForMerge(
            iter->value(), ikey.user_key, blob_fetcher, prefetch_buffers,
            resolved_entity, effective_entity);
        if (s.ok()) {
          s = TimedFullMerge(
              user_merge_operator_, ikey.user_key, kWideBaseValue,
              effective_entity, merge_context_.GetOperands(), logger_, stats_,
              clock_, /* update_num_ops_stats */ false, &op_failure_scope,
              &merge_result, /* result_operand */ nullptr, &merge_result_type);
        }
      } else {
        s = TimedFullMerge(user_merge_operator_, ikey.user_key, kNoBaseValue,
                           merge_context_.GetOperands(), logger_, stats_,
                           clock_, /* update_num_ops_stats */ false,
                           &op_failure_scope, &merge_result,
                           /* result_operand */ nullptr, &merge_result_type);
      }

      // We store the result in keys_.back() and operands_.back()
      // if nothing went wrong (i.e.: no operand corruption on disk)
      if (s.ok()) {
        outcome_ = Outcome::kFull;
        // The original key encountered
        original_key = std::move(keys_.back());

        assert(merge_result_type == kTypeValue ||
               merge_result_type == kTypeWideColumnEntity);
        orig_ikey.type = merge_result_type;
        UpdateInternalKey(&original_key, orig_ikey.sequence, orig_ikey.type);

        keys_.clear();
        merge_context_.Clear();
        keys_.emplace_front(std::move(original_key));
        merge_context_.PushOperand(merge_result);

        // move iter to the next entry
        iter->Next();
      } else if (op_failure_scope ==
                 MergeOperator::OpFailureScope::kMustMerge) {
        // Change to `Status::MergeInProgress()` to denote output consists of
        // merge operands only. Leave `iter` at the non-merge entry so it will
        // be output after.
        s = Status::MergeInProgress();
      }
      return s;
    } else {
      // hit a merge
      //   => if there is a compaction filter, apply it.
      //   => check for range tombstones covering the operand
      //   => merge the operand into the front of the operands_ list
      //      if not filtered
      //   => then continue because we haven't yet seen a Put/Delete.
      //
      // Keep queuing keys and operands until we either meet a put / delete
      // request or later did a partial merge.

      Slice value_slice = iter->value();
      // add an operand to the list if:
      // 1) it's included in one of the snapshots. in that case we *must* write
      // it out, no matter what compaction filter says
      // 2) it's not filtered by a compaction filter
      CompactionFilter::Decision filter =
          merge_order_ == CompactionFilter::MergeOrder::kMergeBeforeFilter ||
                  ikey.sequence <= latest_snapshot_
              ? CompactionFilter::Decision::kKeep
              : FilterMerge(orig_ikey.user_key, value_slice);
      // FIXME: should also check for kRemove here
      if (filter != CompactionFilter::Decision::kRemoveAndSkipUntil &&
          range_del_agg != nullptr &&
          range_del_agg->ShouldDelete(
              iter->key(), RangeDelPositioningMode::kForwardTraversal)) {
        filter = CompactionFilter::Decision::kRemove;
      }
      if (filter == CompactionFilter::Decision::kKeep ||
          filter == CompactionFilter::Decision::kChangeValue) {
        aggregate_eligible_ &= OperandEligible(ikey.sequence);
        if (original_key_is_iter) {
          // this is just an optimization that saves us one memcpy
          keys_.emplace_front(original_key);
        } else {
          keys_.emplace_front(iter->key().ToString());
        }
        if (keys_.size() == 1) {
          // we need to re-anchor the orig_ikey because it was anchored by
          // original_key before
          pik_status =
              ParseInternalKey(keys_.back(), &orig_ikey, allow_data_in_errors);
          pik_status.PermitUncheckedError();
          assert(pik_status.ok());
        }
        if (filter == CompactionFilter::Decision::kKeep) {
          merge_context_.PushOperand(
              value_slice, iter->IsValuePinned() /* operand_pinned */);
        } else {
          assert(filter == CompactionFilter::Decision::kChangeValue);
          // Compaction filter asked us to change the operand from value_slice
          // to compaction_filter_value_.
          merge_context_.PushOperand(compaction_filter_value_, false);
        }
      } else if (filter == CompactionFilter::Decision::kRemoveAndSkipUntil) {
        // Compaction filter asked us to remove this key altogether
        // (not just this operand), along with some keys following it.
        keys_.clear();
        merge_context_.Clear();
        has_compaction_filter_skip_until_ = true;
        return s;
      }
    }
  }

  if (cmp_with_full_history_ts_low >= 0) {
    size_t num_merge_operands = merge_context_.GetNumOperands();
    if (ts_sz && num_merge_operands > 1) {
      // We do not merge merge operands with different timestamps if they are
      // not eligible for GC.
      ROCKS_LOG_ERROR(logger_, "ts_sz=%d, %d merge oprands",
                      static_cast<int>(ts_sz),
                      static_cast<int>(num_merge_operands));
      assert(false);
    }
  }

  if (merge_context_.GetNumOperands() == 0) {
    // we filtered out all the merge operands
    return s;
  }

  // We are sure we have seen this key's entire history if:
  // at_bottom == true (this does not necessarily mean it is the bottommost
  // layer, but rather that we are confident the key does not appear on any of
  // the lower layers, at_bottom == false doesn't mean it does appear, just
  // that we can't be sure, see Compaction::IsBottommostLevel for details)
  // AND
  // we have either encountered another key or end of key history on this
  // layer.
  // Note that if user-defined timestamp is enabled, we need some extra caution
  // here: if full_history_ts_low is nullptr, or it's not null but the key's
  // timestamp is greater than or equal to full_history_ts_low, it means this
  // key cannot be dropped. We may not have seen the beginning of the key.
  //
  // When these conditions are true we are able to merge all the keys
  // using full merge.
  //
  // For these cases we are not sure about, we simply miss the opportunity
  // to combine the keys. Since VersionSet::SetupOtherInputs() always makes
  // sure that all merge-operands on the same level get compacted together,
  // this will simply lead to these merge operands moving to the next level.
  bool surely_seen_the_beginning =
      (hit_the_next_user_key || !iter->Valid()) && at_bottom &&
      (ts_sz == 0 || cmp_with_full_history_ts_low < 0);
  if (surely_seen_the_beginning) {
    // do a final merge with nullptr as the existing value and say
    // bye to the merge type (it's now converted to a Put)
    assert(kTypeMerge == orig_ikey.type);
    assert(merge_context_.GetNumOperands() >= 1);
    assert(merge_context_.GetNumOperands() == keys_.size());
    std::string merge_result;
    ValueType merge_result_type = kTypeValue;
    MergeOperator::OpFailureScope op_failure_scope =
        MergeOperator::OpFailureScope::kDefault;
    s = TimedFullMerge(user_merge_operator_, orig_ikey.user_key, kNoBaseValue,
                       merge_context_.GetOperands(), logger_, stats_, clock_,
                       /* update_num_ops_stats */ false, &op_failure_scope,
                       &merge_result,
                       /* result_operand */ nullptr, &merge_result_type);
    if (s.ok()) {
      outcome_ = Outcome::kFull;
      // The original key encountered
      // We are certain that keys_ is not empty here (see assertions couple of
      // lines before).
      original_key = std::move(keys_.back());

      assert(merge_result_type == kTypeValue ||
             merge_result_type == kTypeWideColumnEntity);
      orig_ikey.type = merge_result_type;
      UpdateInternalKey(&original_key, orig_ikey.sequence, orig_ikey.type);

      keys_.clear();
      merge_context_.Clear();
      keys_.emplace_front(std::move(original_key));
      merge_context_.PushOperand(merge_result);
    } else if (op_failure_scope == MergeOperator::OpFailureScope::kMustMerge) {
      // Change to `Status::MergeInProgress()` to denote output consists of
      // merge operands only.
      s = Status::MergeInProgress();
    }
  } else {
    // We haven't seen the beginning of the key nor a Put/Delete.
    // Attempt to use the user's associative merge function to
    // merge the stacked merge operands into a single operand.
    s = Status::MergeInProgress();
    if (merge_context_.GetNumOperands() >= 2 ||
        (allow_single_operand_ && merge_context_.GetNumOperands() == 1)) {
      bool merge_success = false;
      std::string merge_result;
      {
        StopWatchNano timer(clock_, stats_ != nullptr);
        PERF_TIMER_GUARD(merge_operator_time_nanos);
        merge_success = user_merge_operator_->PartialMergeMulti(
            orig_ikey.user_key,
            std::deque<Slice>(merge_context_.GetOperands().begin(),
                              merge_context_.GetOperands().end()),
            &merge_result, logger_);
        RecordTick(stats_, MERGE_OPERATION_TOTAL_TIME,
                   stats_ ? timer.ElapsedNanosSafe() : 0);
      }
      if (merge_success) {
        // Merging of operands (associative merge) was successful.
        // Replace operands with the merge result
        if (UNLIKELY(merge_result.size() >
                     std::numeric_limits<uint32_t>::max())) {
          s = Status::Corruption("PartialMerge result exceeds 4GB limit");
          return s;
        }
        outcome_ = Outcome::kPartial;
        merge_context_.Clear();
        merge_context_.PushOperand(merge_result);
        keys_.erase(keys_.begin(), keys_.end() - 1);
      }
    }
  }

  return s;
}

MergeOutputIterator::MergeOutputIterator(const MergeHelper* merge_helper)
    : merge_helper_(merge_helper) {
  it_keys_ = merge_helper_->keys().rend();
  it_values_ = merge_helper_->values().rend();
}

void MergeOutputIterator::SeekToFirst() {
  const auto& keys = merge_helper_->keys();
  const auto& values = merge_helper_->values();
  assert(keys.size() == values.size());
  it_keys_ = keys.rbegin();
  it_values_ = values.rbegin();
}

void MergeOutputIterator::Next() {
  ++it_keys_;
  ++it_values_;
}

CompactionFilter::Decision MergeHelper::FilterMerge(const Slice& user_key,
                                                    const Slice& value_slice) {
  if (compaction_filter_ == nullptr) {
    return CompactionFilter::Decision::kKeep;
  }
  if (stats_ != nullptr && ShouldReportDetailedTime(env_, stats_)) {
    filter_timer_.Start();
  }
  compaction_filter_value_.clear();
  compaction_filter_skip_until_.Clear();
  auto ret = compaction_filter_->FilterV4(
      level_, user_key, CompactionFilter::ValueType::kMergeOperand,
      &value_slice, /* existing_columns */ nullptr, &compaction_filter_value_,
      /* new_columns */ nullptr, compaction_filter_skip_until_.rep(),
      /* blob_resolver */ nullptr);
  if (ret == CompactionFilter::Decision::kRemoveAndSkipUntil) {
    if (user_comparator_->Compare(*compaction_filter_skip_until_.rep(),
                                  user_key) <= 0) {
      // Invalid skip_until returned from compaction filter.
      // Keep the key as per FilterV2/V3/V4 documentation.
      ret = CompactionFilter::Decision::kKeep;
    } else {
      compaction_filter_skip_until_.ConvertFromUserKey(kMaxSequenceNumber,
                                                       kValueTypeForSeek);
    }
  }
  if (stats_ != nullptr && ShouldReportDetailedTime(env_, stats_)) {
    total_filter_time_ += filter_timer_.ElapsedNanosSafe();
  }
  return ret;
}

}  // namespace ROCKSDB_NAMESPACE
