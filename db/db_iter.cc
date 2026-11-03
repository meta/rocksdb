//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_iter.h"

#include <limits>
#include <string>
#include <vector>

#include "db/blob/blob_fetcher.h"
#include "db/blob/blob_file_partition_manager.h"
#include "db/blob/blob_index.h"
#include "db/dbformat.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/pinned_iterators_manager.h"
#include "db/wide/wide_column_serialization.h"
#include "db/wide/wide_columns_helper.h"
#include "file/filename.h"
#include "logging/logging.h"
#include "memory/arena.h"
#include "monitoring/perf_context_imp.h"
#include "options/options_helper.h"
#include "port/likely.h"
#include "rocksdb/env.h"
#include "rocksdb/io_dispatcher.h"
#include "rocksdb/iterator.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/system_clock.h"
#include "table/internal_iterator.h"
#include "table/iterator_wrapper.h"
#include "trace_replay/trace_replay.h"
#include "util/mutexlock.h"
#include "util/string_util.h"
#include "util/user_comparator_wrapper.h"

namespace ROCKSDB_NAMESPACE {

namespace {

bool HasFullTimestampVisibility(const ReadOptions& read_options) {
  if (read_options.iter_start_ts != nullptr) {
    return false;
  }
  if (read_options.timestamp == nullptr) {
    return true;
  }
  const Slice ts = *read_options.timestamp;
  for (size_t i = 0; i < ts.size(); ++i) {
    if (static_cast<unsigned char>(ts[i]) != 0xff) {
      return false;
    }
  }
  return true;
}

}  // namespace

DBIter::DBIter(Env* _env, const ReadOptions& read_options,
               const ImmutableOptions& ioptions,
               const MutableCFOptions& mutable_cf_options,
               const Comparator* cmp, InternalIterator* iter,
               const Version* version, SequenceNumber s, bool arena_mode,
               ReadCallback* read_callback, DBImpl* db_impl,
               ColumnFamilyData* cfd, bool expose_blob_index,
               ReadOnlyMemTable* active_mem)
    : prefix_extractor_(mutable_cf_options.prefix_extractor.get()),
      env_(_env),
      clock_(ioptions.clock),
      logger_(ioptions.logger),
      user_comparator_(cmp),
      merge_operator_(ioptions.merge_operator.get()),
      iter_(iter),
      // Enable the direct-write blob fallback only when this CF has a
      // write-path partition manager; otherwise the fetcher stays on the
      // version-backed path.
      blob_state_(version, read_options, cfd ? cfd->blob_file_cache() : nullptr,
                  cfd != nullptr && cfd->blob_partition_manager() != nullptr),
      read_callback_(read_callback),
      sequence_(s),
      value_columns_state_(version, read_options, cfd),
      statistics_(ioptions.stats),
      max_skip_(mutable_cf_options.max_sequential_skip_in_iterations),
      max_skippable_internal_keys_(read_options.max_skippable_internal_keys),
      num_internal_keys_skipped_(0),
      iterate_lower_bound_(read_options.iterate_lower_bound),
      iterate_upper_bound_(read_options.iterate_upper_bound),
      trace_read_options_(read_options),
      trace_db_(db_impl),
      trace_cf_id_(cfd != nullptr ? cfd->GetID() : 0),
      has_trace_state_(db_impl != nullptr && cfd != nullptr),
      ingest_sst_lock_(cfd != nullptr ? &cfd->GetIngestSstLock() : nullptr),
      version_number_(version != nullptr ? version->GetVersionNumber() : 0),
      timestamp_ub_(read_options.timestamp),
      timestamp_lb_(read_options.iter_start_ts),
      timestamp_size_(timestamp_ub_ ? timestamp_ub_->size() : 0),
      active_mem_(active_mem),
      memtable_seqno_lb_(kMaxSequenceNumber),
      memtable_op_scan_flush_trigger_(0),
      avg_op_scan_flush_trigger_(0),
      iter_step_since_seek_(1),
      mem_hidden_op_scanned_since_seek_(0),
      contiguous_tombstone_count_(0),
      direction_(kForward),
      valid_(false),
      current_entry_is_merged_(false),
      can_skip_zero_sequence_duplicates_(
          version != nullptr &&
          !version->storage_info()->MayHaveMultipleZeroSequenceRuns()),
      prefix_same_as_start_(
          prefix_extractor_ ? read_options.prefix_same_as_start : false),
      pin_thru_lifetime_(read_options.pin_data),
      expect_total_order_inner_iter_(prefix_extractor_ == nullptr ||
                                     read_options.total_order_seek ||
                                     read_options.auto_prefix_mode),
      // Read-path range conversion assumes the scan can observe all interior
      // live keys. table_filter can hide whole SSTs, read tiers other than
      // kReadAllTier can skip SSTs or uncached blocks, and timestamp filtering
      // can hide newer UDT versions unless the read is at max timestamp with no
      // lower timestamp bound. Legacy prefix iterators without
      // prefix_same_as_start do not guarantee complete scans, so conversion
      // must stay disabled for the iterator lifetime.
      min_tombstones_for_range_conversion_(
          active_mem != nullptr && !HasTableFilter(read_options) &&
                  read_options.read_tier == kReadAllTier &&
                  (expect_total_order_inner_iter_ || prefix_same_as_start_) &&
                  HasFullTimestampVisibility(read_options)
              ? mutable_cf_options.min_tombstones_for_range_conversion
              : 0),
      expose_blob_index_(expose_blob_index),
      allow_unprepared_value_(read_options.allow_unprepared_value),
      arena_mode_(arena_mode) {
  RecordTick(statistics_, NO_ITERATOR_CREATED);
  if (pin_thru_lifetime_) {
    pinned_iters_mgr_.StartPinning();
  }
  if (iter_.iter()) {
    iter_.iter()->SetPinnedItersMgr(&pinned_iters_mgr_);
  }
  status_.PermitUncheckedError();
  assert(timestamp_size_ ==
         user_comparator_.user_comparator()->timestamp_size());
  // prefix_seek_opt_in_only should force total_order_seek whereever the caller
  // is duplicating the original ReadOptions
  assert(!ioptions.prefix_seek_opt_in_only || read_options.total_order_seek);
  if (active_mem_) {
    // FIXME: GetEarliestSequenceNumber() may return a seqno that is one smaller
    // than the smallest seqno in the memtable. This violates its comment and
    // entries with that seqno may not be in the active memtable. Before it's
    // fixed, we use GetFirstSequenceNumber() for more accurate result.
    memtable_seqno_lb_ = active_mem_->IsEmpty()
                             ? active_mem_->GetEarliestSequenceNumber()
                             : active_mem_->GetFirstSequenceNumber();
    memtable_op_scan_flush_trigger_ =
        mutable_cf_options.memtable_op_scan_flush_trigger;
    if (memtable_op_scan_flush_trigger_) {
      // avg_op_scan_flush_trigger_ requires memtable_op_scan_flush_trigger_ > 0
      avg_op_scan_flush_trigger_ =
          mutable_cf_options.memtable_avg_op_scan_flush_trigger;
    }
  } else {
    // memtable_op_scan_flush_trigger_ and avg_op_scan_flush_trigger_ are
    // initialized to 0(disabled) as default.
  }
}

Status DBIter::GetProperty(std::string prop_name, std::string* prop) {
  if (prop == nullptr) {
    return Status::InvalidArgument("prop is nullptr");
  }
  if (prop_name == "rocksdb.iterator.super-version-number") {
    // First try to pass the value returned from inner iterator.
    return iter_.iter()->GetProperty(prop_name, prop);
  } else if (prop_name == "rocksdb.iterator.is-key-pinned") {
    if (valid_) {
      *prop = (pin_thru_lifetime_ && saved_key_.IsKeyPinned()) ? "1" : "0";
    } else {
      *prop = "Iterator is not valid.";
    }
    return Status::OK();
  } else if (prop_name == "rocksdb.iterator.is-value-pinned") {
    if (valid_) {
      *prop = (pin_thru_lifetime_ && iter_.Valid() &&
               iter_.value().data() == value_columns_state_->value().data())
                  ? "1"
                  : "0";
    } else {
      *prop = "Iterator is not valid.";
    }
    return Status::OK();
  } else if (prop_name == "rocksdb.iterator.internal-key") {
    *prop = saved_key_.GetUserKey().ToString();
    return Status::OK();
  } else if (prop_name == "rocksdb.iterator.write-time") {
    PutFixed64(prop, saved_write_unix_time_);
    return Status::OK();
  }
  return Status::InvalidArgument("Unidentified property.");
}

bool DBIter::ParseKey(ParsedInternalKey* ikey) {
  Status s = ParseInternalKey(iter_.key(), ikey, false /* log_err_key */);
  if (!s.ok()) {
    status_ = Status::Corruption("In DBIter: ", s.getState());
    valid_ = false;
    ROCKS_LOG_ERROR(logger_, "In DBIter: %s", status_.getState());
    return false;
  } else {
    return true;
  }
}

void DBIter::Next() {
  assert(valid_);
  assert(status_.ok());

  PERF_COUNTER_ADD(iter_next_count, 1);
  PERF_CPU_TIMER_GUARD(iter_next_cpu_nanos, clock_);
  // Release temporarily pinned blocks from last operation
  ReleaseTempPinnedData();
  ResetBlobData();
  ResetValueAndColumns();
  local_stats_.skip_count_ += num_internal_keys_skipped_;
  local_stats_.skip_count_--;
  num_internal_keys_skipped_ = 0;
  iter_step_since_seek_++;
  bool ok = true;
  if (direction_ == kReverse) {
    is_key_seqnum_zero_ = false;
    ResetContiguousTombstoneTracking();
    if (!ReverseToForward()) {
      ok = false;
    }
  } else if (!current_entry_is_merged_) {
    // If the current value is not a merge, the iter position is the
    // current key, which is already returned. We can safely issue a
    // Next() without checking the current key.
    // If the current key is a merge, very likely iter already points
    // to the next internal position.
    assert(iter_.Valid());
    iter_.Next();
    PERF_COUNTER_ADD(internal_key_skipped_count, 1);
  }

  local_stats_.next_count_++;
  if (ok && iter_.Valid()) {
    ClearSavedValue();

    FindNextUserEntry(true /* skipping the current user key */);
  } else {
    is_key_seqnum_zero_ = false;
    valid_ = false;
  }
  if (statistics_ != nullptr && valid_) {
    local_stats_.next_found_count_++;
    local_stats_.bytes_read_ += (key().size() + value().size());
  }
}

Status DBIter::BlobReader::RetrieveAndSetBlobValue(const Slice& user_key,
                                                   const Slice& blob_index) {
  assert(blob_value_.empty());

  if (!blob_fetcher_.CanResolve()) {
    return Status::Corruption("Encountered unexpected blob index.");
  }

  // TODO: plumb Env::IOPriority
  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr uint64_t* bytes_read = nullptr;
  return blob_fetcher_.FetchBlob(user_key, blob_index, prefetch_buffer,
                                 &blob_value_, bytes_read);
}

bool DBIter::SetValueAndColumnsFromBlobImpl(const Slice& user_key,
                                            const Slice& blob_index) {
  const Status s =
      blob_state_.mut()->reader.RetrieveAndSetBlobValue(user_key, blob_index);
  if (!s.ok()) {
    status_ = s;
    valid_ = false;
    blob_state_.mut()->is_blob = false;
    return false;
  }

  SetValueAndColumnsFromPlain(blob_state_->reader.GetBlobValue());

  return true;
}

bool DBIter::SetValueAndColumnsFromBlob(const Slice& user_key,
                                        const Slice& blob_index) {
  assert(!blob_state_->is_blob);
  blob_state_.mut()->is_blob = true;

  if (expose_blob_index_) {
    SetValueAndColumnsFromPlain(blob_index);
    return true;
  }

  if (allow_unprepared_value_) {
    assert(value_columns_state_->value().empty());
    assert(value_columns_state_->wide_columns().empty());

    assert(blob_state_->lazy_blob_index.empty());
    blob_state_.mut()->lazy_blob_index = blob_index;

    return true;
  }

  return SetValueAndColumnsFromBlobImpl(user_key, blob_index);
}

bool DBIter::SetValueAndColumnsFromEntity(Slice slice) {
  // Auto-marks dirty via mut() up front since every successful path below
  // populates wide_columns_ (and possibly the lazy entity/blob column
  // vectors).
  auto& state = *value_columns_state_.mut();
  state.AssertReadyForEntity();

  // Fast path: if no blob columns, use the simpler Deserialize
  bool has_blob_columns = false;
  {
    const Status s_hbc =
        WideColumnSerialization::HasBlobColumns(slice, has_blob_columns);
    if (!s_hbc.ok()) {
      status_ = s_hbc;
      valid_ = false;
      return false;
    }
  }
  if (LIKELY(!has_blob_columns)) {
    WideColumns& wide_columns = state.wide_columns();
    const Status s =
        WideColumnSerialization::DeserializeSimple(slice, wide_columns);

    if (!s.ok()) {
      status_ = s;
      valid_ = false;
      state.ClearWideColumns();
      return false;
    }

    state.MaybeSetValueFromMaterializedDefaultColumn();
    return true;
  }

  // Entity has blob columns.
  // First, copy the serialized data to the saved entity buffer so that column
  // name/value Slices remain valid after the internal iterator moves.
  // Guard: if slice already aliases that saved buffer (e.g., when called from
  // SetValueAndColumnsFromMergeResult), skip the redundant copy to
  // avoid self-aliased std::string::assign (undefined behavior).
  state.SaveEntitySliceIfNeeded(slice);

  {
    const Slice entity = state.PrepareForLazyEntityDeserialize();
    const Status s = WideColumnSerialization::Deserialize(
        entity, state.lazy_entity_columns(), &state.lazy_blob_columns());

    if (!s.ok()) {
      status_ = s;
      valid_ = false;
      state.ClearLazyEntity();
      return false;
    }
  }

  // Iterator positions must expose fully prepared values and columns once
  // Valid() becomes true, so resolve and materialize all blob columns here.
  state.BindLazyEntity(saved_key_.GetUserKey());
  if (!MaterializeLazyEntityColumns()) {
    state.ClearLazyEntity();
    return false;
  }
  state.MaybeSetValueFromMaterializedDefaultColumn();

  return true;
}

bool DBIter::MaterializeLazyEntityColumns() const {
  const auto& state = *value_columns_state_;
  if (state.lazy_entity_columns().empty() || !state.wide_columns().empty()) {
    return true;
  }

  std::lock_guard<std::mutex> lock(state.lazy_entity_columns_mutex());
  if (state.lazy_entity_columns().empty() || !state.wide_columns().empty()) {
    return true;
  }

  DBIter* const mutable_this = const_cast<DBIter*>(this);
  auto& mutable_state = *mutable_this->value_columns_state_.mut();
  WideColumns materialized_columns;
  materialized_columns.reserve(state.lazy_entity_columns().size());
  for (const auto& col : state.lazy_entity_columns()) {
    materialized_columns.emplace_back(col.name(), col.value());
  }

  for (const auto& blob_col : state.lazy_blob_columns()) {
    Slice resolved_value;
    const Status s = mutable_state.entity_blob_resolver().ResolveColumn(
        blob_col.first, &resolved_value);
    if (!s.ok()) {
      mutable_this->status_ = s;
      mutable_this->valid_ = false;
      mutable_state.wide_columns().clear();
      return false;
    }

    materialized_columns[blob_col.first].value() = resolved_value;
  }

  mutable_state.wide_columns() = std::move(materialized_columns);
  return true;
}

bool DBIter::SetValueAndColumnsFromMergeResult(const Status& merge_status,
                                               ValueType result_type) {
  if (!merge_status.ok()) {
    valid_ = false;
    status_ = merge_status;
    return false;
  }

  if (result_type == kTypeWideColumnEntity) {
    if (!SetValueAndColumnsFromEntity(value_columns_state_->saved_value())) {
      assert(!valid_);
      return false;
    }

    valid_ = true;
    return true;
  }

  assert(result_type == kTypeValue);
  SetValueAndColumnsFromPlain(pinned_value_.data()
                                  ? pinned_value_
                                  : value_columns_state_->saved_value());
  valid_ = true;
  return true;
}

bool DBIter::PrepareValue() {
  assert(valid_);

  if (blob_state_->lazy_blob_index.empty()) {
    return true;
  }

  assert(allow_unprepared_value_);
  assert(blob_state_->is_blob);

  const bool result = SetValueAndColumnsFromBlobImpl(
      saved_key_.GetUserKey(), blob_state_->lazy_blob_index);

  blob_state_.mut()->lazy_blob_index.clear();

  return result;
}

// PRE: saved_key_ has the current user key if skipping_saved_key
// POST: saved_key_ should have the next user key if valid_,
//       if the current entry is a result of merge
//           current_entry_is_merged_ => true
//           the saved merge buffer   => the merged value
//
// NOTE: In between, saved_key_ can point to a user key that has
//       a delete marker or a sequence number higher than sequence_
//       saved_key_ MUST have a proper user_key before calling this function
//
// The prefix parameter, if not null, indicates that we need to iterate
// within the prefix, and the iterator needs to be made invalid, if no
// more entry for the prefix can be found.
bool DBIter::FindNextUserEntry(bool skipping_saved_key) {
  PERF_TIMER_GUARD(find_next_user_entry_time);
  return FindNextUserEntryInternal(skipping_saved_key);
}

// Actual implementation of DBIter::FindNextUserEntry()
bool DBIter::FindNextUserEntryInternal(bool skipping_saved_key) {
  // Loop until we hit an acceptable entry to yield
  assert(iter_.Valid());
  assert(status_.ok());
  assert(direction_ == kForward);
  current_entry_is_merged_ = false;

  // How many times in a row we have skipped an entry with user key less than
  // or equal to saved_key_. We could skip these entries either because
  // sequence numbers were too high or because skipping_saved_key = true.
  // What saved_key_ contains throughout this method:
  //  - if skipping_saved_key : saved_key_ contains the key that we need
  //                            to skip, and we haven't seen any keys greater
  //                            than that,
  //  - if num_skipped > 0    : saved_key_ contains the key that we have skipped
  //                            num_skipped times, and we haven't seen any keys
  //                            greater than that,
  //  - none of the above     : saved_key_ can contain anything, it doesn't
  //                            matter.
  uint64_t num_skipped = 0;
  // For write unprepared, the target sequence number in reseek could be larger
  // than the snapshot, and thus needs to be skipped again. This could result in
  // an infinite loop of reseeks. To avoid that, we limit the number of reseeks
  // to one.
  bool reseek_done = false;

  uint64_t mem_hidden_op_scanned = 0;
  do {
    const bool previous_zero = is_key_seqnum_zero_;
    if (!ParseKey(&ikey_)) {
      is_key_seqnum_zero_ = false;
      return false;
    }
    Slice user_key_without_ts =
        StripTimestampFromUserKey(ikey_.user_key, timestamp_size_);

    is_key_seqnum_zero_ = ikey_.sequence == 0;

    assert(iterate_upper_bound_ == nullptr ||
           iter_.UpperBoundCheckResult() != IterBoundCheck::kInbound ||
           user_comparator_.CompareWithoutTimestamp(
               user_key_without_ts, /*a_has_ts=*/false, *iterate_upper_bound_,
               /*b_has_ts=*/false) < 0);
    if (iterate_upper_bound_ != nullptr &&
        iter_.UpperBoundCheckResult() != IterBoundCheck::kInbound &&
        user_comparator_.CompareWithoutTimestamp(
            user_key_without_ts, /*a_has_ts=*/false, *iterate_upper_bound_,
            /*b_has_ts=*/false) >= 0) {
      break;
    }

    assert(!prefix_.has_value() || prefix_extractor_ != nullptr);
    if (!PrefixCheck(user_key_without_ts)) {
      // Insert any pending tombstone run using the last tracked delete
      // (saved_key_) as the end key.  We cannot use the current key's
      // prefix as the boundary because bloom filters may have hidden
      // entire prefixes between the seek prefix and the current key.
      // The tombstone covers n-1 of n deletes; the last remains as a
      // point delete.
      FlushPendingTombstoneRun(saved_key_.GetUserKey());
      if (prefix_same_as_start_) {
        break;
      }
    }

    if (TooManyInternalKeysSkipped()) {
      return false;
    }

    assert(ikey_.user_key.size() >= timestamp_size_);
    Slice ts = timestamp_size_ > 0 ? ExtractTimestampFromUserKey(
                                         ikey_.user_key, timestamp_size_)
                                   : Slice();
    bool more_recent = false;
    if (IsVisible(ikey_.sequence, ts, &more_recent)) {
      // A zero-sequence entry ends its key only when at most one sorted run
      // can contain zero sequences. Unknown layouts retain duplicate checking.
      if ((!previous_zero || !can_skip_zero_sequence_duplicates_ ||
           timestamp_size_ > 0) &&
          skipping_saved_key &&
          CompareKeyForSkip(ikey_.user_key, saved_key_.GetUserKey()) <= 0) {
        num_skipped++;  // skip this entry
        PERF_COUNTER_ADD(internal_key_skipped_count, 1);
        MarkMemtableForFlushForPerOpTrigger(mem_hidden_op_scanned);
      } else {
        assert(!skipping_saved_key ||
               CompareKeyForSkip(ikey_.user_key, saved_key_.GetUserKey()) > 0);
        num_skipped = 0;
        reseek_done = false;
        switch (ikey_.type) {
          case kTypeDeletion:
          case kTypeDeletionWithTimestamp:
          case kTypeSingleDeletion:
            // Arrange to skip all upcoming entries for this key since
            // they are hidden by this deletion.
            if (timestamp_lb_) {
              saved_key_.SetInternalKey(ikey_);
              valid_ = true;
              return true;
            } else {
              saved_key_.SetUserKey(
                  ikey_.user_key, !pin_thru_lifetime_ ||
                                      !iter_.iter()->IsKeyPinned() /* copy */);
              skipping_saved_key = true;
              PERF_COUNTER_ADD(internal_delete_skipped_count, 1);
              MarkMemtableForFlushForPerOpTrigger(mem_hidden_op_scanned);
              // Track contiguous tombstones for range conversion.
              // Skip if outside seek prefix -- the top-of-loop check
              // flushed any pending run, but we must also avoid starting
              // a new run outside the prefix.
              if (min_tombstones_for_range_conversion_ > 0 &&
                  PrefixCheck(user_key_without_ts)) {
                TrackContiguousTombstone(ikey_.user_key,
                                         /*always_update_first_key=*/false);
              }
            }
            break;
          case kTypeValue:
          case kTypeValuePreferredSeqno:
          case kTypeBlobIndex:
          case kTypeWideColumnEntity:
            if (!PrepareValueInternal()) {
              return false;
            }
            FlushPendingTombstoneRun(ikey_.user_key);
            if (timestamp_lb_) {
              saved_key_.SetInternalKey(ikey_);
            } else {
              saved_key_.SetUserKey(
                  ikey_.user_key, !pin_thru_lifetime_ ||
                                      !iter_.iter()->IsKeyPinned() /* copy */);
            }

            if (ikey_.type == kTypeBlobIndex) {
              if (!SetValueAndColumnsFromBlob(ikey_.user_key, iter_.value())) {
                return false;
              }
            } else if (ikey_.type == kTypeWideColumnEntity) {
              if (!SetValueAndColumnsFromEntity(iter_.value())) {
                return false;
              }
            } else {
              assert(ikey_.type == kTypeValue ||
                     ikey_.type == kTypeValuePreferredSeqno);
              Slice value = iter_.value();
              saved_write_unix_time_ = iter_.write_unix_time();
              if (ikey_.type == kTypeValuePreferredSeqno) {
                value = ParsePackedValueForValue(value);
              }
              SetValueAndColumnsFromPlain(value);
            }

            valid_ = true;
            return true;
          case kTypeMerge:
            if (!PrepareValueInternal()) {
              return false;
            }
            FlushPendingTombstoneRun(ikey_.user_key);
            saved_key_.SetUserKey(
                ikey_.user_key,
                !pin_thru_lifetime_ || !iter_.iter()->IsKeyPinned() /* copy */);
            // By now, we are sure the current ikey is going to yield a value
            current_entry_is_merged_ = true;
            valid_ = true;
            return MergeValuesNewToOld();  // Go to a different state machine
          default:
            valid_ = false;
            status_ = Status::Corruption(
                "Unknown value type: " +
                std::to_string(static_cast<unsigned int>(ikey_.type)));
            return false;
        }
      }
    } else {
      if (more_recent) {
        PERF_COUNTER_ADD(internal_recent_skipped_count, 1);
      }

      // This key was inserted after our snapshot was taken or skipped by
      // timestamp range. If this happens too many times in a row for the same
      // user key, we want to seek to the target sequence number.
      int cmp = user_comparator_.CompareWithoutTimestamp(
          ikey_.user_key, saved_key_.GetUserKey());
      if (cmp == 0 || (skipping_saved_key && cmp < 0)) {
        num_skipped++;
      } else {
        saved_key_.SetUserKey(
            ikey_.user_key,
            !iter_.iter()->IsKeyPinned() || !pin_thru_lifetime_ /* copy */);
        skipping_saved_key = false;
        num_skipped = 0;
        reseek_done = false;
      }
    }

    // If we have sequentially iterated via numerous equal keys, then it's
    // better to seek so that we can avoid too many key comparisons.
    //
    // To avoid infinite loops, do not reseek if we have already attempted to
    // reseek previously.
    //
    // TODO(lth): If we reseek to sequence number greater than ikey_.sequence,
    // then it does not make sense to reseek as we would actually land further
    // away from the desired key. There is opportunity for optimization here.
    // This could be a long-running operation due to tombstones, etc.
    bool aborted = ROCKSDB_THREAD_YIELD_CHECK_ABORT();
    if (UNLIKELY(aborted)) {
      valid_ = false;
      status_ = Status::Aborted("Query abort.");
      return false;
    }

    if (num_skipped > max_skip_ && !reseek_done) {
      is_key_seqnum_zero_ = false;
      num_skipped = 0;
      reseek_done = true;
      std::string last_key;
      if (skipping_saved_key) {
        // We're looking for the next user-key but all we see are the same
        // user-key with decreasing sequence numbers. Fast forward to
        // sequence number 0 and type deletion (the smallest type).
        if (timestamp_size_ == 0) {
          AppendInternalKey(
              &last_key,
              ParsedInternalKey(saved_key_.GetUserKey(), 0, kTypeDeletion));
        } else {
          const std::string kTsMin(timestamp_size_, '\0');
          AppendInternalKeyWithDifferentTimestamp(
              &last_key,
              ParsedInternalKey(saved_key_.GetUserKey(), 0, kTypeDeletion),
              kTsMin);
        }
        // Don't set skipping_saved_key = false because we may still see more
        // user-keys equal to saved_key_.
      } else {
        // We saw multiple entries with this user key and sequence numbers
        // higher than sequence_. Fast forward to sequence_.
        // Note that this only covers a case when a higher key was overwritten
        // many times since our snapshot was taken, not the case when a lot of
        // different keys were inserted after our snapshot was taken.
        if (timestamp_size_ == 0) {
          AppendInternalKey(
              &last_key, ParsedInternalKey(saved_key_.GetUserKey(), sequence_,
                                           kValueTypeForSeek));
        } else {
          AppendInternalKeyWithDifferentTimestamp(
              &last_key,
              ParsedInternalKey(saved_key_.GetUserKey(), sequence_,
                                kValueTypeForSeek),
              *timestamp_ub_);
        }
      }
      iter_.Seek(last_key);
      RecordTick(statistics_, NUMBER_OF_RESEEKS_IN_ITERATION);
    } else {
      iter_.Next();
    }
  } while (iter_.Valid());

  // If we accumulated tombstones, use the last tracked tombstone as the
  // exclusive end key. It may be more optimal to use iterator upper bound if it
  // exists, but the current iterator API makes that dangerous as upper bound
  // points to user memory which is not guaranteed immutable.
  if (contiguous_tombstone_count_ > 0 && iter_.status().ok()) {
    // It is unsafe to use iter_.key() here even when iter_.Valid() and the key
    // is within the seek prefix. This is because memtable iterators are still
    // valid past the upper bound, but sst iterators are not. So iter_.key() can
    // point to a memtable entry that has skipped past real live entries in
    // ssts.
    assert(PrefixCheck(saved_key_.GetUserKey()));
    MaybeInsertRangeTombstone(saved_key_.GetUserKey());
  }
  ResetContiguousTombstoneTracking();

  valid_ = false;
  return iter_.status().ok();
}

// Merge values of the same user key starting from the current iter_ position
// Scan from the newer entries to older entries.
// PRE: iter_.key() points to the first merge type entry
//      saved_key_ stores the user key
//      iter_.PrepareValue() has been called
// POST: the saved merge buffer has the merged value for the user key
//       iter_ points to the next entry (or invalid)
bool DBIter::MergeValuesNewToOld() {
  if (!merge_operator_) {
    ROCKS_LOG_ERROR(logger_, "Options::merge_operator is null.");
    status_ = Status::InvalidArgument("merge_operator_ must be set.");
    valid_ = false;
    return false;
  }

  // Temporarily pin the blocks that hold merge operands
  TempPinData();
  merge_context_.Clear();
  // Start the merge process by pushing the first operand
  merge_context_.PushOperand(
      iter_.value(), iter_.iter()->IsValuePinned() /* operand_pinned */);
  PERF_COUNTER_ADD(internal_merge_count, 1);

  TEST_SYNC_POINT("DBIter::MergeValuesNewToOld:PushedFirstOperand");

  ParsedInternalKey ikey;
  while (true) {
    // This could be a long-running operation due to merge operands, etc.
    bool aborted = ROCKSDB_THREAD_YIELD_CHECK_ABORT();
    if (UNLIKELY(aborted)) {
      valid_ = false;
      status_ = Status::Aborted("Query abort.");
      return false;
    }

    iter_.Next();
    if (!iter_.Valid()) {
      break;
    }

    TEST_SYNC_POINT("DBIter::MergeValuesNewToOld:SteppedToNextOperand");
    if (!ParseKey(&ikey)) {
      return false;
    }

    if (!user_comparator_.EqualWithoutTimestamp(ikey.user_key,
                                                saved_key_.GetUserKey())) {
      // hit the next user key, stop right here
      break;
    }
    if (kTypeDeletion == ikey.type || kTypeSingleDeletion == ikey.type ||
        kTypeDeletionWithTimestamp == ikey.type) {
      // hit a delete with the same user key, stop right here
      // iter_ is positioned after delete
      iter_.Next();
      break;
    }
    if (!PrepareValueInternal()) {
      return false;
    }

    if (kTypeValue == ikey.type || kTypeValuePreferredSeqno == ikey.type) {
      Slice value = iter_.value();
      saved_write_unix_time_ = iter_.write_unix_time();
      if (kTypeValuePreferredSeqno == ikey.type) {
        value = ParsePackedValueForValue(value);
      }
      // hit a put or put equivalent, merge the put value with operands and
      // store the final result in the saved merge buffer. We are done!
      if (!MergeWithPlainBaseValue(value, ikey.user_key)) {
        return false;
      }
      // iter_ is positioned after put
      iter_.Next();
      if (!iter_.status().ok()) {
        valid_ = false;
        return false;
      }
      return true;
    } else if (kTypeMerge == ikey.type) {
      // hit a merge, add the value as an operand and run associative merge.
      // when complete, add result to operands and continue.
      merge_context_.PushOperand(
          iter_.value(), iter_.iter()->IsValuePinned() /* operand_pinned */);
      PERF_COUNTER_ADD(internal_merge_count, 1);
    } else if (kTypeBlobIndex == ikey.type) {
      if (!MergeWithBlobBaseValue(iter_.value(), ikey.user_key)) {
        return false;
      }

      // iter_ is positioned after put
      iter_.Next();
      if (!iter_.status().ok()) {
        valid_ = false;
        return false;
      }

      return true;
    } else if (kTypeWideColumnEntity == ikey.type) {
      if (!MergeWithWideColumnBaseValue(iter_.value(), ikey.user_key)) {
        return false;
      }

      // iter_ is positioned after put
      iter_.Next();
      if (!iter_.status().ok()) {
        valid_ = false;
        return false;
      }

      return true;
    } else {
      valid_ = false;
      status_ = Status::Corruption(
          "Unrecognized value type: " +
          std::to_string(static_cast<unsigned int>(ikey.type)));
      return false;
    }
  }

  if (!iter_.status().ok()) {
    valid_ = false;
    return false;
  }

  // we either exhausted all internal keys under this user key, or hit
  // a deletion marker.
  // feed null as the existing value to the merge operator, such that
  // client can differentiate this scenario and do things accordingly.
  if (!MergeWithNoBaseValue(saved_key_.GetUserKey())) {
    return false;
  }
  assert(status_.ok());
  return true;
}

void DBIter::Prev() {
  assert(valid_);
  assert(status_.ok());

  PERF_COUNTER_ADD(iter_prev_count, 1);
  PERF_CPU_TIMER_GUARD(iter_prev_cpu_nanos, clock_);
  ReleaseTempPinnedData();
  ResetBlobData();
  ResetValueAndColumns();
  ResetInternalKeysSkippedCounter();
  bool ok = true;
  if (direction_ == kForward) {
    ResetContiguousTombstoneTracking();
    if (!ReverseToBackward()) {
      ok = false;
    }
    // Transitioning to reverse: current key is the end bound
    if (ok && min_tombstones_for_range_conversion_ > 0) {
      range_tomb_end_key_.SetUserKey(saved_key_.GetUserKey(),
                                     !saved_key_.IsKeyPinned());
    }
  }
  if (ok) {
    ClearSavedValue();

    PrevInternal();
  }

  if (statistics_ != nullptr) {
    local_stats_.prev_count_++;
    if (valid_) {
      local_stats_.prev_found_count_++;
      local_stats_.bytes_read_ += (key().size() + value().size());
    }
  }
}

bool DBIter::ReverseToForward() {
  assert(iter_.status().ok());

  // When moving backwards, iter_ is positioned on _previous_ key, which may
  // not exist or may have different prefix than the current key().
  // If that's the case, seek iter_ to current key.
  if (!expect_total_order_inner_iter() || !iter_.Valid()) {
    std::string last_key;
    if (timestamp_size_ == 0) {
      AppendInternalKey(
          &last_key, ParsedInternalKey(saved_key_.GetUserKey(),
                                       kMaxSequenceNumber, kValueTypeForSeek));
    } else {
      // TODO: pre-create kTsMax.
      const std::string kTsMax(timestamp_size_, '\xff');
      AppendInternalKeyWithDifferentTimestamp(
          &last_key,
          ParsedInternalKey(saved_key_.GetUserKey(), kMaxSequenceNumber,
                            kValueTypeForSeek),
          kTsMax);
    }
    iter_.Seek(last_key);
    RecordTick(statistics_, NUMBER_OF_RESEEKS_IN_ITERATION);
  }

  direction_ = kForward;
  // Skip keys less than the current key() (a.k.a. saved_key_).
  while (iter_.Valid()) {
    ParsedInternalKey ikey;
    if (!ParseKey(&ikey)) {
      return false;
    }
    if (user_comparator_.Compare(ikey.user_key, saved_key_.GetUserKey()) >= 0) {
      return true;
    }
    iter_.Next();
  }

  if (!iter_.status().ok()) {
    valid_ = false;
    return false;
  }

  return true;
}

// Move iter_ to the key before saved_key_.
bool DBIter::ReverseToBackward() {
  assert(iter_.status().ok());

  // When current_entry_is_merged_ is true, iter_ may be positioned on the next
  // key, which may not exist or may have prefix different from current.
  // If that's the case, seek to saved_key_.
  if (current_entry_is_merged_ &&
      (!expect_total_order_inner_iter() || !iter_.Valid())) {
    IterKey last_key;
    // Using kMaxSequenceNumber and kValueTypeForSeek
    // (not kValueTypeForSeekForPrev) to seek to a key strictly smaller
    // than saved_key_.
    last_key.SetInternalKey(ParsedInternalKey(
        saved_key_.GetUserKey(), kMaxSequenceNumber, kValueTypeForSeek));
    if (!expect_total_order_inner_iter()) {
      iter_.SeekForPrev(last_key.GetInternalKey());
    } else {
      // Some iterators may not support SeekForPrev(), so we avoid using it
      // when prefix seek mode is disabled. This is somewhat expensive
      // (an extra Prev(), as well as an extra change of direction of iter_),
      // so we may need to reconsider it later.
      iter_.Seek(last_key.GetInternalKey());
      if (!iter_.Valid() && iter_.status().ok()) {
        iter_.SeekToLast();
      }
    }
    RecordTick(statistics_, NUMBER_OF_RESEEKS_IN_ITERATION);
  }

  direction_ = kReverse;
  return FindUserKeyBeforeSavedKey();
}

void DBIter::PrevInternal() {
  // Capture saved_key_ (previous live key) into range_tomb_end_key_ before
  // saved_key_ is overwritten below.
  if (min_tombstones_for_range_conversion_ > 0) {
    range_tomb_end_key_.Swap(saved_key_);
  }

  while (iter_.Valid()) {
    saved_key_.SetUserKey(
        ExtractUserKey(iter_.key()),
        !iter_.iter()->IsKeyPinned() || !pin_thru_lifetime_ /* copy */);

    assert(!prefix_.has_value() || prefix_extractor_ != nullptr);
    Slice saved_key_without_ts =
        StripTimestampFromUserKey(saved_key_.GetUserKey(), timestamp_size_);
    // When prefix filtering is active, insert any pending tombstone run
    // before we leave the seek prefix.
    if (!PrefixCheck(saved_key_without_ts)) {
      // Insert any pending tombstone run before leaving the seek prefix.
      // Only insert if end_key (previous live key) is within the seek prefix.
      if (range_tomb_end_key_.Size() > 0) {
        FlushPendingTombstoneRun(range_tomb_end_key_.GetUserKey(),
                                 /*check_prefix_match=*/true);
      }
      if (prefix_same_as_start_) {
        valid_ = false;
        return;
      }
    }

    assert(iterate_lower_bound_ == nullptr || iter_.MayBeOutOfLowerBound() ||
           user_comparator_.CompareWithoutTimestamp(
               saved_key_.GetUserKey(), /*a_has_ts=*/true,
               *iterate_lower_bound_, /*b_has_ts=*/false) >= 0);
    if (iterate_lower_bound_ != nullptr && iter_.MayBeOutOfLowerBound() &&
        user_comparator_.CompareWithoutTimestamp(
            saved_key_.GetUserKey(), /*a_has_ts=*/true, *iterate_lower_bound_,
            /*b_has_ts=*/false) < 0) {
      // We've iterated earlier than the user-specified lower bound.
      if (range_tomb_end_key_.Size() > 0) {
        FlushPendingTombstoneRun(range_tomb_end_key_.GetUserKey(),
                                 /*check_prefix_match=*/true);
      }
      valid_ = false;
      return;
    }

    bool found_visible = false;
    if (!FindValueForCurrentKey(found_visible)) {  // assigns valid_
      return;
    }

    // Track contiguous tombstones for reverse range tombstone conversion.
    // Only track when FindValueForCurrentKey found a visible entry
    // (found_visible == true).  When no visible entry exists (all seqno >
    // snapshot), the key doesn't exist at this snapshot and must not be
    // treated as a tombstone.  Additionally, ikey_ is only updated when a
    // visible entry is found, so reading ikey_.sequence without this guard
    // would use a stale value.
    if (min_tombstones_for_range_conversion_ > 0 &&
        range_tomb_end_key_.Size() > 0 && timestamp_lb_ == nullptr) {
      if (!valid_ && found_visible && PrefixCheck(saved_key_without_ts)) {
        // Key was deleted and is within the seek prefix -- track it.
        TrackContiguousTombstone(saved_key_.GetUserKey(),
                                 /*always_update_first_key=*/true);
      } else if (valid_) {
        // Live key breaks the run.
        FlushPendingTombstoneRun(range_tomb_end_key_.GetUserKey(),
                                 /*check_prefix_match=*/true);
      }
    }

    // Whether or not we found a value for current key, we need iter_ to end up
    // on a smaller key.
    if (!FindUserKeyBeforeSavedKey()) {
      return;
    }

    if (valid_) {
      // Found the value.
      return;
    }

    if (TooManyInternalKeysSkipped(false)) {
      return;
    }
  }

  if (range_tomb_end_key_.Size() > 0) {
    FlushPendingTombstoneRun(range_tomb_end_key_.GetUserKey(),
                             /*check_prefix_match=*/true);
  }

  // We haven't found any key - iterator is not valid
  valid_ = false;
}

// Used for backwards iteration.
// Looks at the entries with user key saved_key_ and finds the most up-to-date
// value for it, or executes a merge, or determines that the value was deleted.
// Sets valid_ to true if the value is found and is ready to be presented to
// the user through value().
// Sets valid_ to false if the value was deleted or no visible entry exists.
// Sets ikey_ to the last visible entry's internal key.  When found_visible
// is false, ikey_ is not updated and may contain stale data.
// Sets found_visible to true if at least one entry passed the IsVisible()
// check (seqno <= snapshot).  When false, no entry was visible -- the key
// does not exist at this snapshot and should not be treated as a tombstone.
// Returns false if an error occurred, and !status().ok() and !valid_.
//
// PRE: iter_ is positioned on the last entry with user key equal to saved_key_.
// POST: iter_ is positioned on one of the entries equal to saved_key_, or on
//       the entry just before them, or on the entry just after them.
bool DBIter::FindValueForCurrentKey(bool& found_visible) {
  found_visible = false;
  assert(iter_.Valid());
  merge_context_.Clear();
  current_entry_is_merged_ = false;
  // last entry before merge (could be kTypeDeletion,
  // kTypeDeletionWithTimestamp, kTypeSingleDeletion, kTypeValue
  // kTypeBlobIndex, kTypeWideColumnEntity or kTypeValuePreferredSeqno)
  ValueType last_not_merge_type = kTypeDeletion;
  ValueType last_key_entry_type = kTypeDeletion;

  // If false, it indicates that we have not seen any valid entry, even though
  // last_key_entry_type is initialized to kTypeDeletion.
  bool valid_entry_seen = false;

  // Temporarily pin blocks that hold (merge operands / the value)
  ReleaseTempPinnedData();
  TempPinData();
  size_t num_skipped = 0;
  while (iter_.Valid()) {
    ParsedInternalKey ikey;
    if (!ParseKey(&ikey)) {
      return false;
    }

    if (!user_comparator_.EqualWithoutTimestamp(ikey.user_key,
                                                saved_key_.GetUserKey())) {
      // Found a smaller user key, thus we are done with current user key.
      break;
    }

    assert(ikey.user_key.size() >= timestamp_size_);
    Slice ts;
    if (timestamp_size_ > 0) {
      ts = Slice(ikey.user_key.data() + ikey.user_key.size() - timestamp_size_,
                 timestamp_size_);
    }

    bool visible = IsVisible(ikey.sequence, ts);
    if (!visible &&
        (timestamp_lb_ == nullptr ||
         user_comparator_.CompareTimestamp(ts, *timestamp_ub_) > 0)) {
      // Found an invisible version of the current user key, and it must have
      // a higher sequence number or timestamp. Therefore, we are done with the
      // current user key.
      break;
    }

    // Entry survived the visibility check -- at least one visible version
    // exists for this user key.
    found_visible = true;

    if (!ts.empty()) {
      saved_timestamp_.assign(ts.data(), ts.size());
    }

    if (TooManyInternalKeysSkipped()) {
      return false;
    }

    // This user key has lots of entries.
    // We're going from old to new, and it's taking too long. Let's do a Seek()
    // and go from new to old. This helps when a key was overwritten many times.
    if (num_skipped >= max_skip_) {
      return FindValueForCurrentKeyUsingSeek();
    }

    if (!PrepareValueInternal()) {
      return false;
    }

    if (timestamp_lb_ != nullptr) {
      saved_key_.SetInternalKey(ikey);
    } else if (user_comparator_.Compare(ikey.user_key,
                                        saved_key_.GetUserKey()) < 0) {
      saved_key_.SetUserKey(
          ikey.user_key,
          !pin_thru_lifetime_ || !iter_.iter()->IsKeyPinned() /* copy */);
    }

    // Ensure ikey_ is only set to VISIBLE keys.
    ikey_ = ikey;
    valid_entry_seen = true;
    last_key_entry_type = ikey.type;
    switch (last_key_entry_type) {
      case kTypeValue:
      case kTypeValuePreferredSeqno:
      case kTypeBlobIndex:
      case kTypeWideColumnEntity:
        if (iter_.iter()->IsValuePinned()) {
          saved_write_unix_time_ = iter_.write_unix_time();
          if (last_key_entry_type == kTypeValuePreferredSeqno) {
            pinned_value_ = ParsePackedValueForValue(iter_.value());
          } else {
            pinned_value_ = iter_.value();
          }
        } else {
          valid_ = false;
          status_ = Status::NotSupported(
              "Backward iteration not supported if underlying iterator's value "
              "cannot be pinned.");
        }
        merge_context_.Clear();
        last_not_merge_type = last_key_entry_type;
        if (!status_.ok()) {
          return false;
        }
        break;
      case kTypeDeletion:
      case kTypeDeletionWithTimestamp:
      case kTypeSingleDeletion:
        merge_context_.Clear();
        last_not_merge_type = last_key_entry_type;
        PERF_COUNTER_ADD(internal_delete_skipped_count, 1);
        break;
      case kTypeMerge: {
        assert(merge_operator_ != nullptr);
        merge_context_.PushOperandBack(
            iter_.value(), iter_.iter()->IsValuePinned() /* operand_pinned */);
        PERF_COUNTER_ADD(internal_merge_count, 1);
      } break;
      default:
        valid_ = false;
        status_ = Status::Corruption(
            "Unknown value type: " +
            std::to_string(static_cast<unsigned int>(last_key_entry_type)));
        return false;
    }

    PERF_COUNTER_ADD(internal_key_skipped_count, 1);

    // This could be a long-running operation due to tombstones, etc.
    bool aborted = ROCKSDB_THREAD_YIELD_CHECK_ABORT();
    if (UNLIKELY(aborted)) {
      valid_ = false;
      status_ = Status::Aborted("Query abort.");
      return false;
    }

    iter_.Prev();
    ++num_skipped;

    if (visible && timestamp_lb_ != nullptr) {
      // If timestamp_lb_ is not nullptr, we do not have to look further for
      // another internal key. We can return this current internal key. Yet we
      // still keep the invariant that iter_ is positioned before the returned
      // key.
      break;
    }
  }

  if (!iter_.status().ok()) {
    valid_ = false;
    return false;
  }

  if (!valid_entry_seen) {
    // Since we haven't seen any valid entry, last_key_entry_type remains
    // unchanged and the same as its initial value.
    assert(last_key_entry_type == kTypeDeletion);
    assert(last_not_merge_type == kTypeDeletion);
    valid_ = false;
    return true;
  }

  if (timestamp_lb_ != nullptr) {
    assert(last_key_entry_type == ikey_.type);
  }

  switch (last_key_entry_type) {
    case kTypeDeletion:
    case kTypeDeletionWithTimestamp:
    case kTypeSingleDeletion:
      if (timestamp_lb_ == nullptr) {
        valid_ = false;
      } else {
        valid_ = true;
      }
      return true;
    case kTypeMerge:
      current_entry_is_merged_ = true;
      if (last_not_merge_type == kTypeDeletion ||
          last_not_merge_type == kTypeSingleDeletion ||
          last_not_merge_type == kTypeDeletionWithTimestamp) {
        if (!MergeWithNoBaseValue(saved_key_.GetUserKey())) {
          return false;
        }
        return true;
      } else if (last_not_merge_type == kTypeBlobIndex) {
        if (!MergeWithBlobBaseValue(pinned_value_, saved_key_.GetUserKey())) {
          return false;
        }

        return true;
      } else if (last_not_merge_type == kTypeWideColumnEntity) {
        if (!MergeWithWideColumnBaseValue(pinned_value_,
                                          saved_key_.GetUserKey())) {
          return false;
        }

        return true;
      } else {
        assert(last_not_merge_type == kTypeValue ||
               last_not_merge_type == kTypeValuePreferredSeqno);
        if (!MergeWithPlainBaseValue(pinned_value_, saved_key_.GetUserKey())) {
          return false;
        }
        return true;
      }
    case kTypeValue:
    case kTypeValuePreferredSeqno:
      SetValueAndColumnsFromPlain(pinned_value_);

      break;
    case kTypeBlobIndex:
      if (!SetValueAndColumnsFromBlob(saved_key_.GetUserKey(), pinned_value_)) {
        return false;
      }
      break;
    case kTypeWideColumnEntity:
      if (!SetValueAndColumnsFromEntity(pinned_value_)) {
        return false;
      }
      break;
    default:
      valid_ = false;
      status_ = Status::Corruption(
          "Unknown value type: " +
          std::to_string(static_cast<unsigned int>(last_key_entry_type)));
      return false;
  }
  valid_ = true;
  return true;
}

// This function is used in FindValueForCurrentKey.
// We use Seek() function instead of Prev() to find necessary value
// TODO: This is very similar to FindNextUserEntry() and MergeValuesNewToOld().
//       Would be nice to reuse some code.
bool DBIter::FindValueForCurrentKeyUsingSeek() {
  // FindValueForCurrentKey will enable pinning before calling
  // FindValueForCurrentKeyUsingSeek()
  assert(pinned_iters_mgr_.PinningEnabled());
  std::string last_key;
  if (0 == timestamp_size_) {
    AppendInternalKey(&last_key,
                      ParsedInternalKey(saved_key_.GetUserKey(), sequence_,
                                        kValueTypeForSeek));
  } else {
    AppendInternalKeyWithDifferentTimestamp(
        &last_key,
        ParsedInternalKey(saved_key_.GetUserKey(), sequence_,
                          kValueTypeForSeek),
        timestamp_lb_ == nullptr ? *timestamp_ub_ : *timestamp_lb_);
  }
  iter_.Seek(last_key);
  RecordTick(statistics_, NUMBER_OF_RESEEKS_IN_ITERATION);

  // In case read_callback presents, the value we seek to may not be visible.
  // Find the next value that's visible.
  ParsedInternalKey ikey;

  while (true) {
    if (!iter_.Valid()) {
      valid_ = false;
      return iter_.status().ok();
    }

    if (!ParseKey(&ikey)) {
      return false;
    }
    assert(ikey.user_key.size() >= timestamp_size_);
    Slice ts;
    if (timestamp_size_ > 0) {
      ts = Slice(ikey.user_key.data() + ikey.user_key.size() - timestamp_size_,
                 timestamp_size_);
    }

    if (!user_comparator_.EqualWithoutTimestamp(ikey.user_key,
                                                saved_key_.GetUserKey())) {
      // No visible values for this key, even though FindValueForCurrentKey()
      // has seen some. This is possible if we're using a tailing iterator, and
      // the entries were discarded in a compaction.
      valid_ = false;
      return true;
    }

    if (IsVisible(ikey.sequence, ts)) {
      break;
    }

    // This could be a long-running operation due to tombstones, etc.
    bool aborted = ROCKSDB_THREAD_YIELD_CHECK_ABORT();
    if (UNLIKELY(aborted)) {
      valid_ = false;
      status_ = Status::Aborted("Query abort.");
      return false;
    }

    iter_.Next();
  }

  // Keep ikey_ in sync with the entry found by the seek.
  ikey_ = ikey;
  TEST_SYNC_POINT_CALLBACK(
      "DBIter::FindValueForCurrentKeyUsingSeek:ikey_updated", &ikey_);

  if (ikey.type == kTypeDeletion || ikey.type == kTypeSingleDeletion ||
      kTypeDeletionWithTimestamp == ikey.type) {
    if (timestamp_lb_ == nullptr) {
      valid_ = false;
    } else {
      valid_ = true;
      saved_key_.SetInternalKey(ikey);
    }
    return true;
  }
  if (!PrepareValueInternal()) {
    return false;
  }
  if (timestamp_size_ > 0) {
    Slice ts = ExtractTimestampFromUserKey(ikey.user_key, timestamp_size_);
    saved_timestamp_.assign(ts.data(), ts.size());
  }
  if (ikey.type == kTypeValue || ikey.type == kTypeValuePreferredSeqno ||
      ikey.type == kTypeBlobIndex || ikey.type == kTypeWideColumnEntity) {
    assert(iter_.iter()->IsValuePinned());
    saved_write_unix_time_ = iter_.write_unix_time();
    if (ikey.type == kTypeValuePreferredSeqno) {
      pinned_value_ = ParsePackedValueForValue(iter_.value());
    } else {
      pinned_value_ = iter_.value();
    }
    if (ikey.type == kTypeBlobIndex) {
      if (!SetValueAndColumnsFromBlob(ikey.user_key, pinned_value_)) {
        return false;
      }
    } else if (ikey.type == kTypeWideColumnEntity) {
      if (!SetValueAndColumnsFromEntity(pinned_value_)) {
        return false;
      }
    } else {
      assert(ikey.type == kTypeValue || ikey.type == kTypeValuePreferredSeqno);
      SetValueAndColumnsFromPlain(pinned_value_);
    }

    if (timestamp_lb_ != nullptr) {
      saved_key_.SetInternalKey(ikey);
    } else {
      saved_key_.SetUserKey(
          ikey.user_key,
          !pin_thru_lifetime_ || !iter_.iter()->IsKeyPinned() /* copy */);
    }

    valid_ = true;
    return true;
  }

  // kTypeMerge. We need to collect all kTypeMerge values and save them
  // in operands
  assert(ikey.type == kTypeMerge);
  current_entry_is_merged_ = true;
  merge_context_.Clear();
  merge_context_.PushOperand(
      iter_.value(), iter_.iter()->IsValuePinned() /* operand_pinned */);
  PERF_COUNTER_ADD(internal_merge_count, 1);

  while (true) {
    // This could be a long-running operation due to merge operands, etc.
    bool aborted = ROCKSDB_THREAD_YIELD_CHECK_ABORT();
    if (UNLIKELY(aborted)) {
      valid_ = false;
      status_ = Status::Aborted("Query abort.");
      return false;
    }

    iter_.Next();

    if (!iter_.Valid()) {
      if (!iter_.status().ok()) {
        valid_ = false;
        return false;
      }
      break;
    }
    if (!ParseKey(&ikey)) {
      return false;
    }
    if (!user_comparator_.EqualWithoutTimestamp(ikey.user_key,
                                                saved_key_.GetUserKey())) {
      break;
    }
    if (ikey.type == kTypeDeletion || ikey.type == kTypeSingleDeletion ||
        ikey.type == kTypeDeletionWithTimestamp) {
      break;
    }
    if (!PrepareValueInternal()) {
      return false;
    }

    if (ikey.type == kTypeValue || ikey.type == kTypeValuePreferredSeqno) {
      Slice value = iter_.value();
      if (ikey.type == kTypeValuePreferredSeqno) {
        value = ParsePackedValueForValue(value);
      }
      if (!MergeWithPlainBaseValue(value, saved_key_.GetUserKey())) {
        return false;
      }
      return true;
    } else if (ikey.type == kTypeMerge) {
      merge_context_.PushOperand(
          iter_.value(), iter_.iter()->IsValuePinned() /* operand_pinned */);
      PERF_COUNTER_ADD(internal_merge_count, 1);
    } else if (ikey.type == kTypeBlobIndex) {
      if (!MergeWithBlobBaseValue(iter_.value(), saved_key_.GetUserKey())) {
        return false;
      }

      return true;
    } else if (ikey.type == kTypeWideColumnEntity) {
      if (!MergeWithWideColumnBaseValue(iter_.value(),
                                        saved_key_.GetUserKey())) {
        return false;
      }

      return true;
    } else {
      valid_ = false;
      status_ = Status::Corruption(
          "Unknown value type: " +
          std::to_string(static_cast<unsigned int>(ikey.type)));
      return false;
    }
  }

  if (!MergeWithNoBaseValue(saved_key_.GetUserKey())) {
    return false;
  }

  // Make sure we leave iter_ in a good state. If it's valid and we don't care
  // about prefixes, that's already good enough. Otherwise it needs to be
  // seeked to the current key.
  if (!expect_total_order_inner_iter() || !iter_.Valid()) {
    if (!expect_total_order_inner_iter()) {
      iter_.SeekForPrev(last_key);
    } else {
      iter_.Seek(last_key);
      if (!iter_.Valid() && iter_.status().ok()) {
        iter_.SeekToLast();
      }
    }
    RecordTick(statistics_, NUMBER_OF_RESEEKS_IN_ITERATION);
  }

  valid_ = true;
  return true;
}

bool DBIter::MergeWithNoBaseValue(const Slice& user_key) {
  // `op_failure_scope` (an output parameter) is not provided (set to nullptr)
  // since a failure must be propagated regardless of its value.
  ValueType result_type;
  const Status s = MergeHelper::TimedFullMerge(
      merge_operator_, user_key, MergeHelper::kNoBaseValue,
      merge_context_.GetOperands(), logger_, statistics_, clock_,
      /* update_num_ops_stats */ true, /* op_failure_scope */ nullptr,
      &value_columns_state_.mut()->saved_value(), &pinned_value_, &result_type);
  return SetValueAndColumnsFromMergeResult(s, result_type);
}

bool DBIter::MergeWithPlainBaseValue(const Slice& value,
                                     const Slice& user_key) {
  // `op_failure_scope` (an output parameter) is not provided (set to nullptr)
  // since a failure must be propagated regardless of its value.
  ValueType result_type;
  const Status s = MergeHelper::TimedFullMerge(
      merge_operator_, user_key, MergeHelper::kPlainBaseValue, value,
      merge_context_.GetOperands(), logger_, statistics_, clock_,
      /* update_num_ops_stats */ true, /* op_failure_scope */ nullptr,
      &value_columns_state_.mut()->saved_value(), &pinned_value_, &result_type);
  return SetValueAndColumnsFromMergeResult(s, result_type);
}

bool DBIter::MergeWithBlobBaseValue(const Slice& blob_index,
                                    const Slice& user_key) {
  assert(!blob_state_->is_blob);

  if (expose_blob_index_) {
    status_ =
        Status::NotSupported("Legacy BlobDB does not support merge operator.");
    valid_ = false;
    return false;
  }

  const Status s =
      blob_state_.mut()->reader.RetrieveAndSetBlobValue(user_key, blob_index);
  if (!s.ok()) {
    status_ = s;
    valid_ = false;
    return false;
  }

  valid_ = true;

  if (!MergeWithPlainBaseValue(blob_state_->reader.GetBlobValue(), user_key)) {
    return false;
  }

  blob_state_.Reset();

  return true;
}

bool DBIter::MergeWithWideColumnBaseValue(const Slice& entity,
                                          const Slice& user_key) {
  // Resolve V2 entity blob columns if present, since TimedFullMerge only
  // supports V1 format.
  std::string resolved_entity;
  Slice effective_entity;
  Status s_resolve = WideColumnSerialization::ResolveEntityForMerge(
      entity, user_key, &blob_state_->reader.blob_fetcher(),
      nullptr /* prefetch_buffers */, resolved_entity, effective_entity);
  if (!s_resolve.ok()) {
    status_ = std::move(s_resolve);
    valid_ = false;
    return false;
  }

  // `op_failure_scope` (an output parameter) is not provided (set to nullptr)
  // since a failure must be propagated regardless of its value.
  ValueType result_type;
  Status s = MergeHelper::TimedFullMerge(
      merge_operator_, user_key, MergeHelper::kWideBaseValue, effective_entity,
      merge_context_.GetOperands(), logger_, statistics_, clock_,
      /* update_num_ops_stats */ true, /* op_failure_scope */ nullptr,
      &value_columns_state_.mut()->saved_value(), &pinned_value_, &result_type);
  return SetValueAndColumnsFromMergeResult(s, result_type);
}

// Move backwards until the key smaller than saved_key_.
// Changes valid_ only if return value is false.
bool DBIter::FindUserKeyBeforeSavedKey() {
  assert(status_.ok());
  size_t num_skipped = 0;
  while (iter_.Valid()) {
    ParsedInternalKey ikey;
    if (!ParseKey(&ikey)) {
      return false;
    }

    if (CompareKeyForSkip(ikey.user_key, saved_key_.GetUserKey()) < 0) {
      return true;
    }

    if (TooManyInternalKeysSkipped()) {
      return false;
    }

    assert(ikey.sequence != kMaxSequenceNumber);
    assert(ikey.user_key.size() >= timestamp_size_);
    Slice ts;
    if (timestamp_size_ > 0) {
      ts = Slice(ikey.user_key.data() + ikey.user_key.size() - timestamp_size_,
                 timestamp_size_);
    }
    if (!IsVisible(ikey.sequence, ts)) {
      PERF_COUNTER_ADD(internal_recent_skipped_count, 1);
    } else {
      PERF_COUNTER_ADD(internal_key_skipped_count, 1);
    }

    // This could be a long-running operation due to tombstones, etc.
    bool aborted = ROCKSDB_THREAD_YIELD_CHECK_ABORT();
    if (UNLIKELY(aborted)) {
      valid_ = false;
      status_ = Status::Aborted("Query abort.");
      return false;
    }

    if (num_skipped >= max_skip_) {
      num_skipped = 0;
      std::string last_key;
      if (timestamp_size_ == 0) {
        AppendInternalKey(&last_key, ParsedInternalKey(saved_key_.GetUserKey(),
                                                       kMaxSequenceNumber,
                                                       kValueTypeForSeek));
      } else {
        // TODO: pre-create kTsMax.
        const std::string kTsMax(timestamp_size_, '\xff');
        AppendInternalKeyWithDifferentTimestamp(
            &last_key,
            ParsedInternalKey(saved_key_.GetUserKey(), kMaxSequenceNumber,
                              kValueTypeForSeek),
            kTsMax);
      }
      // It would be more efficient to use SeekForPrev() here, but some
      // iterators may not support it.
      iter_.Seek(last_key);
      RecordTick(statistics_, NUMBER_OF_RESEEKS_IN_ITERATION);
      if (!iter_.Valid()) {
        break;
      }
    } else {
      ++num_skipped;
    }

    iter_.Prev();
  }

  if (!iter_.status().ok()) {
    valid_ = false;
    return false;
  }

  return true;
}

void DBIter::TrackContiguousTombstone(const Slice& user_key,
                                      bool always_update_first_key) {
  if (always_update_first_key || contiguous_tombstone_count_ == 0) {
    range_tomb_first_key_.SetUserKey(user_key, true /* copy */);
  }
  contiguous_tombstone_count_++;
}

void DBIter::FlushPendingTombstoneRun(const Slice& end_key,
                                      bool check_prefix_match) {
  if (contiguous_tombstone_count_ == 0) {
    return;
  }

  if (check_prefix_match) {
    Slice end_key_without_ts =
        StripTimestampFromUserKey(end_key, timestamp_size_);
    if (PrefixCheck(end_key_without_ts)) {
      MaybeInsertRangeTombstone(end_key);
    }
  } else {
    MaybeInsertRangeTombstone(end_key);
  }

  ResetContiguousTombstoneTracking();
}

void DBIter::MaybeInsertRangeTombstone(const Slice& end_key) {
  if (contiguous_tombstone_count_ < min_tombstones_for_range_conversion_) {
    return;
  }

  if (active_mem_ == nullptr) {
    return;
  }

  assert(PrefixCheck(range_tomb_first_key_.GetUserKey()));
  assert(PrefixCheck(end_key));
  assert(user_comparator_.Compare(range_tomb_first_key_.GetUserKey(),
                                  end_key) <= 0);

  auto earliest_seq = active_mem_->GetEarliestSequenceNumber();
  // Skip if the iterator's snapshot predates the memtable. Otherwise entries
  // added with seqno between sequence_ and earliest_seq will be unintentionally
  // covered.
  if (sequence_ < earliest_seq) {
    RecordTick(statistics_, READ_PATH_RANGE_TOMBSTONES_DISCARDED);
    return;
  }

  // Insert at the read sequence so the converted tombstone is visible only
  // to readers that could already observe the deletion run.
  SequenceNumber insert_seq = sequence_;

  // Skip if the insertion seq could shadow prepared-but-uncommitted writes.
  if (read_callback_ != nullptr &&
      insert_seq >= read_callback_->min_uncommitted()) {
    RecordTick(statistics_, READ_PATH_RANGE_TOMBSTONES_DISCARDED);
    return;
  }

  // Check if the memtable already has a range tombstone covering [start, end).
  {
    ReadOptions ro;
    // Assumption is that this should be relatively cheap as other read requests
    // should be building the cached core fragmented list.
    std::unique_ptr<FragmentedRangeTombstoneIterator> range_iter(
        active_mem_->NewRangeTombstoneIterator(ro, sequence_,
                                               false /* immutable_memtable */));
    if (range_iter) {
      range_iter->Seek(range_tomb_first_key_.GetUserKey());
      if (range_iter->Valid() &&
          user_comparator_.Compare(range_iter->start_key(),
                                   range_tomb_first_key_.GetUserKey()) <= 0 &&
          user_comparator_.Compare(range_iter->end_key(), end_key) >= 0) {
        RecordTick(statistics_, READ_PATH_RANGE_TOMBSTONES_DISCARDED);
        return;
      }
    }
  }

  assert(ingest_sst_lock_ != nullptr);
  if (active_mem_->AddLogicallyRedundantRangeTombstone(
          insert_seq, range_tomb_first_key_.GetUserKey(), end_key,
          version_number_, *ingest_sst_lock_)) {
    RecordTick(statistics_, READ_PATH_RANGE_TOMBSTONES_INSERTED);
    ROCKS_LOG_DEBUG(logger_,
                    "Inserted range tombstone [%s, %s) @ seq %" PRIu64
                    " (count=%" PRIu32 ", snapshot=%" PRIu64 ")",
                    range_tomb_first_key_.GetUserKey().ToString(true).c_str(),
                    end_key.ToString(true).c_str(), insert_seq,
                    contiguous_tombstone_count_, sequence_);
  } else {
    RecordTick(statistics_, READ_PATH_RANGE_TOMBSTONES_DISCARDED);
  }
}

bool DBIter::TooManyInternalKeysSkipped(bool increment) {
  if ((max_skippable_internal_keys_ > 0) &&
      (num_internal_keys_skipped_ > max_skippable_internal_keys_)) {
    valid_ = false;
    status_ = Status::Incomplete("Too many internal keys skipped.");
    return true;
  } else if (increment) {
    num_internal_keys_skipped_++;
  }
  return false;
}

bool DBIter::IsVisible(SequenceNumber sequence, const Slice& ts,
                       bool* more_recent) {
  // Remember that comparator orders preceding timestamp as larger.
  // TODO(yanqin): support timestamp in read_callback_.
  bool visible_by_seq = (read_callback_ == nullptr)
                            ? sequence <= sequence_
                            : read_callback_->IsVisible(sequence);

  bool visible_by_ts =
      (timestamp_ub_ == nullptr ||
       user_comparator_.CompareTimestamp(ts, *timestamp_ub_) <= 0) &&
      (timestamp_lb_ == nullptr ||
       user_comparator_.CompareTimestamp(ts, *timestamp_lb_) >= 0);

  if (more_recent) {
    *more_recent = !visible_by_seq;
  }
  return visible_by_seq && visible_by_ts;
}

void DBIter::SetSavedKeyToSeekTarget(const Slice& target) {
  is_key_seqnum_zero_ = false;
  SequenceNumber seq = sequence_;
  saved_key_.Clear();
  saved_key_.SetInternalKey(target, seq, kValueTypeForSeek, timestamp_ub_);

  if (iterate_lower_bound_ != nullptr &&
      user_comparator_.CompareWithoutTimestamp(
          saved_key_.GetUserKey(), /*a_has_ts=*/true, *iterate_lower_bound_,
          /*b_has_ts=*/false) < 0) {
    // Seek key is smaller than the lower bound.
    saved_key_.Clear();
    saved_key_.SetInternalKey(*iterate_lower_bound_, seq, kValueTypeForSeek,
                              timestamp_ub_);
  }
}

void DBIter::SetSavedKeyToSeekForPrevTarget(const Slice& target) {
  is_key_seqnum_zero_ = false;
  saved_key_.Clear();
  // now saved_key is used to store internal key.
  saved_key_.SetInternalKey(target, 0 /* sequence_number */,
                            kValueTypeForSeekForPrev, timestamp_ub_);

  if (timestamp_size_ > 0) {
    const std::string kTsMin(timestamp_size_, '\0');
    Slice ts = kTsMin;
    saved_key_.UpdateInternalKey(
        /*seq=*/0, kValueTypeForSeekForPrev,
        timestamp_lb_ == nullptr ? &ts : timestamp_lb_);
  }

  if (iterate_upper_bound_ != nullptr &&
      user_comparator_.CompareWithoutTimestamp(
          saved_key_.GetUserKey(), /*a_has_ts=*/true, *iterate_upper_bound_,
          /*b_has_ts=*/false) >= 0) {
    saved_key_.Clear();
    saved_key_.SetInternalKey(*iterate_upper_bound_, kMaxSequenceNumber,
                              kValueTypeForSeekForPrev, timestamp_ub_);
    if (timestamp_size_ > 0) {
      const std::string kTsMax(timestamp_size_, '\xff');
      Slice ts = kTsMax;
      saved_key_.UpdateInternalKey(kMaxSequenceNumber, kValueTypeForSeekForPrev,
                                   &ts);
    }
  }
}

Status DBIter::ValidateScanOptions(const MultiScanArgs& multiscan_opts) const {
  if (multiscan_opts.empty()) {
    return Status::InvalidArgument("Empty MultiScanArgs");
  }

  if (multiscan_opts.rolling_prefetch &&
      (!multiscan_opts.use_async_io || multiscan_opts.reverse ||
       multiscan_opts.rolling_prefetch_window_size == 0)) {
    ROCKS_LOG_WARN(
        logger_,
        "Ignoring MultiScanArgs::rolling_prefetch because its async, "
        "forward, nonzero-window prerequisites are not satisfied");
  }

  const std::vector<ScanOptions>& scan_opts = multiscan_opts.GetScanRanges();
  const bool has_limit = scan_opts.front().range.limit.has_value();
  if (multiscan_opts.reverse && !has_limit) {
    return Status::InvalidArgument("Reverse MultiScan requires limit");
  }
  if (!has_limit && scan_opts.size() > 1) {
    return Status::InvalidArgument("Scan has no upper bound");
  }

  for (size_t i = 0; i < scan_opts.size(); ++i) {
    const auto& scan_range = scan_opts[i].range;
    if (!scan_range.start.has_value()) {
      return Status::InvalidArgument("Scan has no start key at index " +
                                     std::to_string(i));
    }

    if (scan_range.limit.has_value()) {
      if (user_comparator_.CompareWithoutTimestamp(
              scan_range.start.value(), /*a_has_ts=*/false,
              scan_range.limit.value(), /*b_has_ts=*/false) >= 0) {
        return Status::InvalidArgument(
            "Scan start key is large or equal than limit at index " +
            std::to_string(i));
      }
    }

    if (i > 0) {
      if (!scan_range.limit.has_value()) {
        // multiple scan without limit scan ranges
        return Status::InvalidArgument("Scan has no upper bound at index " +
                                       std::to_string(i));
      }

      const auto& last_end_key = scan_opts[i - 1].range.limit.value();
      if (user_comparator_.CompareWithoutTimestamp(
              scan_range.start.value(), /*a_has_ts=*/false, last_end_key,
              /*b_has_ts=*/false) < 0) {
        return Status::InvalidArgument("Overlapping ranges at index " +
                                       std::to_string(i));
      }
    }
  }
  return Status::OK();
}

Status DBIter::SetScanOptionsForPrepare(const MultiScanArgs& scan_opts) {
  // Prepare() discards the current position, because child iterators may
  // release the data blocks that key() and value() point into.
  valid_ = false;
  status_ = ValidateScanOptions(scan_opts);
  if (!status_.ok()) {
    return status_;
  }
  std::optional<MultiScanArgs> new_scan_opts;
  new_scan_opts.emplace(scan_opts);
  scan_opts_.swap(new_scan_opts);
  scan_index_ = 0;

  // Create a shared IODispatcher if not provided. This allows all
  // BlockBasedTableIterators in this scan to share a single dispatcher,
  // enabling better IO coordination and future rate limiting.
  if (!scan_opts_.value().io_dispatcher) {
    scan_opts_->io_dispatcher.reset(NewIODispatcher());
  }
  return Status::OK();
}

void DBIter::PrepareInternalChildren() {
  if (!scan_opts_.has_value() || !status_.ok()) {
    return;
  }

  if (scan_opts_.value().HasBoundedScanRanges()) {
    iter_.Prepare(&scan_opts_.value());
  } else {
    iter_.Prepare(nullptr);
  }
}

void DBIter::Prepare(const MultiScanArgs& scan_opts) {
  if (SetScanOptionsForPrepare(scan_opts).ok()) {
    PrepareInternalChildren();
  }
}

void DBIter::Seek(const Slice& target) {
  PERF_COUNTER_ADD(iter_seek_count, 1);
  PERF_CPU_TIMER_GUARD(iter_seek_cpu_nanos, clock_);
  StopWatch sw(clock_, statistics_, DB_SEEK);

  if (scan_opts_.has_value()) {
    if (scan_opts_->reverse) {
      status_ = Status::InvalidArgument("Seek called on reverse MultiScan");
      valid_ = false;
      return;
    }

    // Validate the seek target is as expected in the previously prepared range
    auto const& scan_ranges = scan_opts_.value().GetScanRanges();
    if (scan_index_ >= scan_ranges.size()) {
      status_ = Status::InvalidArgument(
          "Seek called after exhausting all of the scan ranges");
      valid_ = false;
      return;
    }

    // Validate start key of next prepare range matches the seek target
    auto const& range = scan_ranges[scan_index_];
    auto const& start = range.range.start;
    assert(start.has_value());
    if (user_comparator_.CompareWithoutTimestamp(target, *start) != 0) {
      status_ = Status::InvalidArgument(
          "Seek target does not match the start of the next prepared range at "
          "index " +
          std::to_string(scan_index_));
      valid_ = false;
      return;
    }

    // validate the upper bound is set to the same value of limit, if limit
    // exists
    auto const& limit = range.range.limit;
    if (limit.has_value()) {
      if (iterate_upper_bound_ == nullptr ||
          user_comparator_.CompareWithoutTimestamp(
              limit.value(), *iterate_upper_bound_) != 0) {
        status_ = Status::InvalidArgument(
            "Upper bound is not set to the same limit value of the next "
            "prepared range at index " +
            std::to_string(scan_index_));
        valid_ = false;
        return;
      }
    }
    scan_index_++;
  }

  if (has_trace_state_) {
    // TODO: What do we do if this returns an error?
    Slice lower_bound;
    Slice upper_bound;
    if (iterate_lower_bound_ != nullptr) {
      lower_bound = *iterate_lower_bound_;
    } else {
      lower_bound = Slice("");
    }
    if (iterate_upper_bound_ != nullptr) {
      upper_bound = *iterate_upper_bound_;
    } else {
      upper_bound = Slice("");
    }
    trace_db_
        ->TraceIteratorSeek(trace_cf_id_, target, lower_bound, upper_bound,
                            trace_read_options_)
        .PermitUncheckedError();
  }

  status_ = Status::OK();
  ResetSeekState();

  MarkMemtableForFlushForAvgTrigger();

  // Seek the inner iterator based on the target key.
  {
    PERF_TIMER_GUARD(seek_internal_seek_time);

    SetSavedKeyToSeekTarget(target);
    iter_.Seek(saved_key_.GetInternalKey());

    RecordTick(statistics_, NUMBER_DB_SEEK);
  }
  if (!iter_.Valid()) {
    valid_ = false;
    return;
  }
  direction_ = kForward;

  // Now the inner iterator is placed to the target position. From there,
  // we need to find out the next key that is visible to the user.
  ClearSavedValue();
  if (ShouldSetPrefix(target)) {
    prefix_.emplace();
    prefix_->SetUserKey(prefix_extractor_->Transform(target));
  }
  FindNextUserEntry(false /* not skipping saved_key */);
  if (!valid_) {
    prefix_.reset();
  }
  if (!valid_) {
    return;
  }

  // Updating stats and perf context counters.
  if (statistics_ != nullptr) {
    // Decrement since we don't want to count this key as skipped
    RecordTick(statistics_, NUMBER_DB_SEEK_FOUND);
    RecordTick(statistics_, ITER_BYTES_READ, key().size() + value().size());
  }
  PERF_COUNTER_ADD(iter_read_bytes, key().size() + value().size());
}

void DBIter::SeekForPrev(const Slice& target) {
  PERF_COUNTER_ADD(iter_seek_count, 1);
  PERF_CPU_TIMER_GUARD(iter_seek_cpu_nanos, clock_);
  StopWatch sw(clock_, statistics_, DB_SEEK);

  if (scan_opts_.has_value()) {
    if (!scan_opts_->reverse) {
      status_ =
          Status::InvalidArgument("SeekForPrev called on forward MultiScan");
      valid_ = false;
      return;
    }

    auto const& scan_ranges = scan_opts_.value().GetScanRanges();
    if (scan_index_ >= scan_ranges.size()) {
      status_ = Status::InvalidArgument(
          "SeekForPrev called after exhausting all of the scan ranges");
      valid_ = false;
      return;
    }

    auto const& range = scan_ranges[scan_index_];
    auto const& limit = range.range.limit;
    assert(limit.has_value());
    if (!limit.has_value() ||
        user_comparator_.CompareWithoutTimestamp(target, *limit) != 0) {
      status_ = Status::InvalidArgument(
          "SeekForPrev target does not match the limit of the next prepared "
          "range at index " +
          std::to_string(scan_index_));
      valid_ = false;
      return;
    }

    auto const& start = range.range.start;
    assert(start.has_value());
    if (iterate_lower_bound_ == nullptr ||
        user_comparator_.CompareWithoutTimestamp(start.value(),
                                                 *iterate_lower_bound_) != 0) {
      status_ = Status::InvalidArgument(
          "Lower bound is not set to the same start value of the next "
          "prepared range at index " +
          std::to_string(scan_index_));
      valid_ = false;
      return;
    }
    if (iterate_upper_bound_ == nullptr ||
        user_comparator_.CompareWithoutTimestamp(limit.value(),
                                                 *iterate_upper_bound_) != 0) {
      status_ = Status::InvalidArgument(
          "Upper bound is not set to the same limit value of the next "
          "prepared range at index " +
          std::to_string(scan_index_));
      valid_ = false;
      return;
    }

    scan_index_++;
  }

  if (has_trace_state_) {
    // TODO: What do we do if this returns an error?
    Slice lower_bound;
    Slice upper_bound;
    if (iterate_lower_bound_ != nullptr) {
      lower_bound = *iterate_lower_bound_;
    } else {
      lower_bound = Slice("");
    }
    if (iterate_upper_bound_ != nullptr) {
      upper_bound = *iterate_upper_bound_;
    } else {
      upper_bound = Slice("");
    }
    trace_db_
        ->TraceIteratorSeekForPrev(trace_cf_id_, target, lower_bound,
                                   upper_bound, trace_read_options_)
        .PermitUncheckedError();
  }

  status_ = Status::OK();
  ResetSeekState();

  MarkMemtableForFlushForAvgTrigger();

  // Seek the inner iterator based on the target key.
  {
    PERF_TIMER_GUARD(seek_internal_seek_time);
    SetSavedKeyToSeekForPrevTarget(target);
    iter_.SeekForPrev(saved_key_.GetInternalKey());
    RecordTick(statistics_, NUMBER_DB_SEEK);
  }
  if (!iter_.Valid()) {
    valid_ = false;
    return;
  }
  direction_ = kReverse;

  // Now the inner iterator is placed to the target position. From there,
  // we need to find out the first key that is visible to the user in the
  // backward direction.
  ClearSavedValue();
  if (ShouldSetPrefix(target)) {
    prefix_.emplace();
    prefix_->SetUserKey(prefix_extractor_->Transform(target));
  }
  PrevInternal();
  if (!valid_) {
    prefix_.reset();
  }
  // Set end key for first Prev() call's tombstone tracking
  if (valid_ && min_tombstones_for_range_conversion_ > 0) {
    range_tomb_end_key_.SetUserKey(saved_key_.GetUserKey(),
                                   !saved_key_.IsKeyPinned());
  }

  // Report stats and perf context.
  if (statistics_ != nullptr && valid_) {
    RecordTick(statistics_, NUMBER_DB_SEEK_FOUND);
    RecordTick(statistics_, ITER_BYTES_READ, key().size() + value().size());
    PERF_COUNTER_ADD(iter_read_bytes, key().size() + value().size());
  }
}

void DBIter::SeekToFirst() {
  if (iterate_lower_bound_ != nullptr) {
    Seek(*iterate_lower_bound_);
    return;
  }
  PERF_COUNTER_ADD(iter_seek_count, 1);
  PERF_CPU_TIMER_GUARD(iter_seek_cpu_nanos, clock_);
  // Don't use iter_::Seek() if we set a prefix extractor
  // because prefix seek will be used.
  if (!expect_total_order_inner_iter()) {
    max_skip_ = std::numeric_limits<uint64_t>::max();
  }
  status_ = Status::OK();
  // if iterator is empty, this status_ could be unchecked.
  status_.PermitUncheckedError();
  direction_ = kForward;
  ResetSeekState();

  MarkMemtableForFlushForAvgTrigger();
  ClearSavedValue();
  is_key_seqnum_zero_ = false;

  {
    PERF_TIMER_GUARD(seek_internal_seek_time);
    iter_.SeekToFirst();
  }

  RecordTick(statistics_, NUMBER_DB_SEEK);
  if (iter_.Valid()) {
    saved_key_.SetUserKey(
        ExtractUserKey(iter_.key()),
        !iter_.iter()->IsKeyPinned() || !pin_thru_lifetime_ /* copy */);
    assert(!prefix_.has_value());
    FindNextUserEntry(false /* not skipping saved_key */);
    if (statistics_ != nullptr) {
      if (valid_) {
        RecordTick(statistics_, NUMBER_DB_SEEK_FOUND);
        RecordTick(statistics_, ITER_BYTES_READ, key().size() + value().size());
        PERF_COUNTER_ADD(iter_read_bytes, key().size() + value().size());
      }
    }
  } else {
    valid_ = false;
  }
  if (valid_) {
    Slice user_key_without_ts =
        StripTimestampFromUserKey(saved_key_.GetUserKey(), timestamp_size_);
    if (ShouldSetPrefix(user_key_without_ts)) {
      prefix_.emplace();
      prefix_->SetUserKey(prefix_extractor_->Transform(user_key_without_ts));
    }
  }
}

void DBIter::SeekToLast() {
  if (iterate_upper_bound_ != nullptr) {
    // Seek to last key strictly less than ReadOptions.iterate_upper_bound.
    SeekForPrev(*iterate_upper_bound_);
#ifndef NDEBUG
    Slice k = Valid() ? key() : Slice();
    if (Valid() && timestamp_size_ > 0 && timestamp_lb_) {
      k.remove_suffix(kNumInternalBytes + timestamp_size_);
    }
    assert(!Valid() || user_comparator_.CompareWithoutTimestamp(
                           k, /*a_has_ts=*/false, *iterate_upper_bound_,
                           /*b_has_ts=*/false) < 0);
#endif
    return;
  }

  PERF_COUNTER_ADD(iter_seek_count, 1);
  PERF_CPU_TIMER_GUARD(iter_seek_cpu_nanos, clock_);
  // Don't use iter_::Seek() if we set a prefix extractor
  // because prefix seek will be used.
  if (!expect_total_order_inner_iter()) {
    max_skip_ = std::numeric_limits<uint64_t>::max();
  }
  status_ = Status::OK();
  // if iterator is empty, this status_ could be unchecked.
  status_.PermitUncheckedError();
  direction_ = kReverse;
  ResetSeekState();

  MarkMemtableForFlushForAvgTrigger();
  ClearSavedValue();
  is_key_seqnum_zero_ = false;

  // Clear stale saved_key_ so PrevInternal()'s Swap does not pollute
  // range_tomb_end_key_ with a key from a previous seek operation.
  saved_key_.Clear();

  {
    PERF_TIMER_GUARD(seek_internal_seek_time);
    iter_.SeekToLast();
  }
  assert(!prefix_.has_value());
  PrevInternal();
  // Set end key for first Prev() call's tombstone tracking
  if (valid_ && min_tombstones_for_range_conversion_ > 0) {
    range_tomb_end_key_.SetUserKey(saved_key_.GetUserKey(),
                                   !saved_key_.IsKeyPinned());
  }
  if (statistics_ != nullptr) {
    RecordTick(statistics_, NUMBER_DB_SEEK);
    if (valid_) {
      RecordTick(statistics_, NUMBER_DB_SEEK_FOUND);
      RecordTick(statistics_, ITER_BYTES_READ, key().size() + value().size());
      PERF_COUNTER_ADD(iter_read_bytes, key().size() + value().size());
    }
  }
  if (valid_) {
    Slice user_key_without_ts =
        StripTimestampFromUserKey(saved_key_.GetUserKey(), timestamp_size_);
    if (ShouldSetPrefix(user_key_without_ts)) {
      prefix_.emplace();
      prefix_->SetUserKey(prefix_extractor_->Transform(user_key_without_ts));
    }
  }
}
}  // namespace ROCKSDB_NAMESPACE
