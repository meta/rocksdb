//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/compaction/compaction_iterator.h"

#include <iterator>
#include <limits>
#include <memory>

#include "db/blob/blob_fetcher.h"
#include "db/blob/blob_file_builder.h"
#include "db/blob/blob_index.h"
#include "db/blob/prefetch_buffer_collection.h"
#include "db/snapshot_checker.h"
#include "db/wide/blob_column_resolver_util.h"
#include "db/wide/wide_column_serialization.h"
#include "db/wide/wide_columns_helper.h"
#include "logging/logging.h"
#include "port/likely.h"
#include "rocksdb/listener.h"
#include "table/internal_iterator.h"
#include "test_util/sync_point.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

// CompactionBlobResolver implementation
void CompactionBlobResolver::Init(BlobFetcher* blob_fetcher,
                                  PrefetchBufferCollection* prefetch_buffers,
                                  CompactionIterationStats* iter_stats) {
  blob_fetcher_ = blob_fetcher;
  prefetch_buffers_ = prefetch_buffers;
  iter_stats_ = iter_stats;
}

void CompactionBlobResolver::Reset(
    const Slice& user_key, const std::vector<WideColumn>* columns,
    const std::vector<std::pair<size_t, BlobIndex>>* blob_columns,
    bool track_resolve_error) {
  user_key_ = user_key;
  columns_ = columns;
  blob_columns_ = blob_columns;
  track_resolve_error_ = track_resolve_error;
  resolved_cache_.clear();
  resolve_error_.reset();
}

Status CompactionBlobResolver::ResolveColumn(size_t column_index,
                                             Slice* resolved_value) {
  Status status = Status::OK();
  if (columns_ == nullptr || column_index >= columns_->size()) {
    status = Status::InvalidArgument("Column index out of bounds");
  } else {
    const BlobIndex* blob_index_ptr =
        blob_resolver_util::FindBlobColumn(blob_columns_, column_index);

    if (blob_index_ptr == nullptr) {
      // Not a blob column - return the inline value directly
      *resolved_value = (*columns_)[column_index].value();
    } else {
      // Check if we've already resolved this column
      PinnableSlice* cached =
          blob_resolver_util::FindInCache(resolved_cache_, column_index);
      if (cached != nullptr) {
        *resolved_value = Slice(*cached);
      } else if (blob_fetcher_ == nullptr) {
        status = Status::NotSupported("Blob fetcher not available");
      } else {
        const BlobIndex& blob_index = *blob_index_ptr;

        // Handle inlined blobs (e.g., from legacy stacked BlobDB with TTL)
        if (blob_index.IsInlined()) {
          *resolved_value = blob_resolver_util::CacheInlinedBlob(
              resolved_cache_, column_index, blob_index);
        } else {
          FilePrefetchBuffer* prefetch_buffer =
              prefetch_buffers_ ? prefetch_buffers_->GetOrCreatePrefetchBuffer(
                                      blob_index.file_number())
                                : nullptr;

          uint64_t bytes_read = 0;
          resolved_cache_.emplace_back(column_index,
                                       std::make_unique<PinnableSlice>());
          auto& new_entry = resolved_cache_.back();

          status =
              blob_fetcher_->FetchBlob(user_key_, blob_index, prefetch_buffer,
                                       new_entry.second.get(), &bytes_read);
          if (!status.ok()) {
            resolved_cache_.pop_back();
          } else {
            if (iter_stats_ != nullptr) {
              ++iter_stats_->num_blobs_read;
              iter_stats_->total_blob_bytes_read += bytes_read;
            }

            *resolved_value = Slice(*new_entry.second);
          }
        }
      }
    }
  }
  if (!status.ok() && track_resolve_error_ && !resolve_error_.has_value()) {
    resolve_error_.emplace(status);
  }
  return status;
}

bool CompactionBlobResolver::IsBlobColumn(size_t column_index) const {
  if (columns_ == nullptr || column_index >= columns_->size()) {
    return false;
  }
  return blob_resolver_util::IsBlobColumnIndex(blob_columns_, column_index);
}

size_t CompactionBlobResolver::NumColumns() const {
  if (columns_ == nullptr) {
    return 0;
  }
  return columns_->size();
}

CompactionIterator::CompactionIterator(
    InternalIterator* input, const Comparator* cmp, MergeHelper* merge_helper,
    SequenceNumber last_sequence, std::vector<SequenceNumber>* snapshots,
    SequenceNumber earliest_snapshot,
    SequenceNumber earliest_write_conflict_snapshot,
    SequenceNumber job_snapshot, const SnapshotChecker* snapshot_checker,
    Env* env, bool report_detailed_time,
    CompactionRangeDelAggregator* range_del_agg,
    BlobFileBuilder* blob_file_builder, bool allow_data_in_errors,
    bool enforce_single_del_contracts,
    const std::atomic<bool>& manual_compaction_canceled,
    bool must_count_input_entries, const Compaction* compaction,
    const CompactionFilter* compaction_filter,
    const std::atomic<bool>* shutting_down,
    const std::shared_ptr<Logger> info_log,
    const std::string* full_history_ts_low,
    std::optional<SequenceNumber> preserve_seqno_min,
    const Version* input_version, Env::IOActivity blob_read_io_activity,
    std::optional<uint64_t> blob_gc_cutoff_file_number)
    : CompactionIterator(
          input, cmp, merge_helper, last_sequence, snapshots, earliest_snapshot,
          earliest_write_conflict_snapshot, job_snapshot, snapshot_checker, env,
          report_detailed_time, range_del_agg, blob_file_builder,
          allow_data_in_errors, enforce_single_del_contracts,
          manual_compaction_canceled,
          compaction ? std::make_unique<RealCompaction>(compaction) : nullptr,
          must_count_input_entries, compaction_filter, shutting_down, info_log,
          full_history_ts_low, preserve_seqno_min, input_version,
          blob_read_io_activity, blob_gc_cutoff_file_number) {}

CompactionIterator::CompactionIterator(
    InternalIterator* input, const Comparator* cmp, MergeHelper* merge_helper,
    SequenceNumber /*last_sequence*/, std::vector<SequenceNumber>* snapshots,
    SequenceNumber earliest_snapshot,
    SequenceNumber earliest_write_conflict_snapshot,
    SequenceNumber job_snapshot, const SnapshotChecker* snapshot_checker,
    Env* env, bool report_detailed_time,
    CompactionRangeDelAggregator* range_del_agg,
    BlobFileBuilder* blob_file_builder, bool allow_data_in_errors,
    bool enforce_single_del_contracts,
    const std::atomic<bool>& manual_compaction_canceled,
    std::unique_ptr<CompactionProxy> compaction, bool must_count_input_entries,
    const CompactionFilter* compaction_filter,
    const std::atomic<bool>* shutting_down,
    const std::shared_ptr<Logger> info_log,
    const std::string* full_history_ts_low,
    std::optional<SequenceNumber> preserve_seqno_min,
    const Version* input_version, Env::IOActivity blob_read_io_activity,
    std::optional<uint64_t> blob_gc_cutoff_file_number)
    : input_(input, cmp, must_count_input_entries),
      cmp_(cmp),
      merge_helper_(merge_helper),
      snapshots_(snapshots),
      earliest_write_conflict_snapshot_(earliest_write_conflict_snapshot),
      job_snapshot_(job_snapshot),
      snapshot_checker_(snapshot_checker),
      env_(env),
      clock_(env_->GetSystemClock().get()),
      report_detailed_time_(report_detailed_time),
      range_del_agg_(range_del_agg),
      blob_file_builder_(blob_file_builder),
      compaction_(std::move(compaction)),
      compaction_filter_(compaction_filter),
      filter_supports_v4_(
          compaction_filter_ ? compaction_filter_->SupportsFilterV4() : false),
      shutting_down_(shutting_down),
      manual_compaction_canceled_(manual_compaction_canceled),
      bottommost_level_(compaction_ && compaction_->bottommost_level() &&
                        !compaction_->allow_ingest_behind()),
      // snapshots_ cannot be nullptr, but we will assert later in the body of
      // the constructor.
      visible_at_tip_(snapshots_ ? snapshots_->empty() : false),
      earliest_snapshot_(earliest_snapshot),
      info_log_(info_log),
      allow_data_in_errors_(allow_data_in_errors),
      enforce_single_del_contracts_(enforce_single_del_contracts),
      timestamp_size_(cmp_ ? cmp_->timestamp_size() : 0),
      full_history_ts_low_(full_history_ts_low),
      current_user_key_sequence_(0),
      current_user_key_snapshot_(0),
      merge_out_iter_(merge_helper_),
      blob_garbage_collection_cutoff_file_number_(
          blob_gc_cutoff_file_number.has_value()
              ? *blob_gc_cutoff_file_number
              : ComputeBlobGarbageCollectionCutoffFileNumber(
                    compaction_.get())),
      blob_fetcher_(CreateBlobFetcherIfNeeded(compaction_.get(), input_version,
                                              blob_read_io_activity)),
      prefetch_buffers_(
          CreatePrefetchBufferCollectionIfNeeded(compaction_.get())),
      current_key_committed_(false),
      cmp_with_history_ts_low_(0),
      level_(compaction_ == nullptr ? 0 : compaction_->level()),
      preserve_seqno_after_(preserve_seqno_min.value_or(earliest_snapshot)) {
  assert(snapshots_ != nullptr);
  assert(preserve_seqno_after_ <= earliest_snapshot_);

  TEST_SYNC_POINT_CALLBACK(
      "CompactionIterator::BlobGarbageCollectionCutoffFileNumber",
      &blob_garbage_collection_cutoff_file_number_);

  if (compaction_ != nullptr) {
    level_ptrs_ = std::vector<size_t>(compaction_->number_levels(), 0);
  }
#ifndef NDEBUG
  // findEarliestVisibleSnapshot assumes this ordering.
  for (size_t i = 1; i < snapshots_->size(); ++i) {
    assert(snapshots_->at(i - 1) < snapshots_->at(i));
  }
  assert(timestamp_size_ == 0 || !full_history_ts_low_ ||
         timestamp_size_ == full_history_ts_low_->size());
#endif
  input_.SetPinnedItersMgr(&pinned_iters_mgr_);
  blob_resolver_.Init(blob_fetcher_.get(), prefetch_buffers_.get(),
                      &iter_stats_);
  // The default `merge_until_status_` does not need to be checked since it is
  // overwritten as soon as `MergeUntil()` is called
  merge_until_status_.PermitUncheckedError();
  TEST_SYNC_POINT_CALLBACK("CompactionIterator:AfterInit", compaction_.get());
}

CompactionIterator::~CompactionIterator() {
  // input_ Iterator lifetime is longer than pinned_iters_mgr_ lifetime
  input_.SetPinnedItersMgr(nullptr);
}

void CompactionIterator::SetBlobFetcher(const Version* version,
                                        BlobFileCache* blob_file_cache,
                                        Env::IOActivity io_activity,
                                        bool allow_write_path_fallback) {
  if (blob_fetcher_ != nullptr || version == nullptr) {
    return;
  }

  ReadOptions read_options;
  read_options.io_activity = io_activity;
  read_options.fill_cache = false;

  blob_fetcher_ = std::make_unique<OwningVersionBlobFetcher>(
      version, std::move(read_options), blob_file_cache,
      allow_write_path_fallback);
  blob_resolver_.Init(blob_fetcher_.get(), prefetch_buffers_.get(),
                      &iter_stats_);
}

void CompactionIterator::ResetRecordCounts() {
  iter_stats_.num_record_drop_user = 0;
  iter_stats_.num_record_drop_hidden = 0;
  iter_stats_.num_record_drop_obsolete = 0;
  iter_stats_.num_record_drop_range_del = 0;
  iter_stats_.num_range_del_drop_obsolete = 0;
  iter_stats_.num_optimized_del_drop_obsolete = 0;
}

void CompactionIterator::SeekToFirst() {
  SeekToFirstWithoutPreparingOutput();
  PrepareOutputIfNeeded();
}

void CompactionIterator::SeekToFirstWithoutPreparingOutput() {
  NextFromInput();
}

void CompactionIterator::Next() {
  NextImpl();
  PrepareOutputIfNeeded();
}

void CompactionIterator::NextWithoutPreparingOutput() { NextImpl(); }

void CompactionIterator::NextImpl() {
  output_prepared_ = false;
  // If there is a merge output, return it before continuing to process the
  // input.
  if (merge_out_iter_.Valid()) {
    merge_out_iter_.Next();

    // Check if we returned all records of the merge output.
    if (merge_out_iter_.Valid()) {
      key_ = merge_out_iter_.key();
      value_ = merge_out_iter_.value();
      Status s = ParseInternalKey(key_, &ikey_, allow_data_in_errors_);
      // MergeUntil stops when it encounters a corrupt key and does not
      // include them in the result, so we expect the keys here to be valid.
      if (!s.ok()) {
        // FIXME: should fail compaction after this fatal logging.
        ROCKS_LOG_FATAL(
            info_log_, "Invalid ikey %s in compaction. %s",
            allow_data_in_errors_ ? key_.ToString(true).c_str() : "hidden",
            s.getState());
        assert(false);
      }

      // Keep current_key_ in sync.
      if (0 == timestamp_size_) {
        current_key_.UpdateInternalKey(ikey_.sequence, ikey_.type);
      } else {
        Slice ts = ikey_.GetTimestamp(timestamp_size_);
        current_key_.UpdateInternalKey(ikey_.sequence, ikey_.type, &ts);
      }
      key_ = current_key_.GetInternalKey();
      ikey_.user_key = current_key_.GetUserKey();
      validity_info_.SetValid(ValidContext::kMerge1);
    } else {
      if (merge_until_status_.IsMergeInProgress()) {
        // `Status::MergeInProgress()` tells us that the previous `MergeUntil()`
        // produced only merge operands. Those merge operands were accessed and
        // written out using `merge_out_iter_`. Since `merge_out_iter_` is
        // exhausted at this point, all merge operands have been written out.
        //
        // Still, there may be a base value (PUT, DELETE, SINGLEDEL, etc.) that
        // needs to be written out. Normally, `CompactionIterator` would skip it
        // on the basis that it has already output something in the same
        // snapshot stripe. To prevent this, we reset `has_current_user_key_` to
        // trick the future iteration from finding out the snapshot stripe is
        // unchanged.
        has_current_user_key_ = false;
      }
      // We consumed all pinned merge operands, release pinned iterators
      pinned_iters_mgr_.ReleasePinnedData();
      // MergeHelper moves the iterator to the first record after the merged
      // records, so even though we reached the end of the merge output, we do
      // not want to advance the iterator.
      NextFromInput();
    }
  } else {
    // Only advance the input iterator if there is no merge output and the
    // iterator is not already at the next record.
    if (!at_next_) {
      AdvanceInputIter();
    }
    NextFromInput();
  }

  if (Valid()) {
    // Record that we've outputted a record for the current key.
    has_outputted_key_ = true;
  }
}

void CompactionIterator::PrepareOutputIfNeeded() {
  if (!output_prepared_) {
    PrepareOutput();
    output_prepared_ = true;
  }
}

bool CompactionIterator::InvokeFilterIfNeeded(bool* need_skip,
                                              Slice* skip_until) {
  if (!compaction_filter_) {
    return true;
  }

  if (ikey_.type != kTypeValue && ikey_.type != kTypeBlobIndex &&
      ikey_.type != kTypeWideColumnEntity) {
    return true;
  }

  CompactionFilter::Decision decision =
      CompactionFilter::Decision::kUndetermined;
  CompactionFilter::ValueType value_type =
      ikey_.type == kTypeValue ? CompactionFilter::ValueType::kValue
      : ikey_.type == kTypeBlobIndex
          ? CompactionFilter::ValueType::kBlobIndex
          : CompactionFilter::ValueType::kWideColumnEntity;

  // Hack: pass internal key to BlobIndexCompactionFilter since it needs
  // to get sequence number.
  assert(compaction_filter_);
  const Slice& filter_key =
      (ikey_.type != kTypeBlobIndex ||
       !compaction_filter_->IsStackedBlobDbInternalCompactionFilter())
          ? ikey_.user_key
          : key_;

  compaction_filter_value_.clear();
  compaction_filter_skip_until_.Clear();

  std::vector<std::pair<std::string, std::string>> new_columns;

  {
    StopWatchNano timer(clock_, report_detailed_time_);

    if (ikey_.type == kTypeBlobIndex) {
      decision = compaction_filter_->FilterBlobByKey(
          level_, filter_key, &compaction_filter_value_,
          compaction_filter_skip_until_.rep());
      if (decision == CompactionFilter::Decision::kUndetermined &&
          !compaction_filter_->IsStackedBlobDbInternalCompactionFilter()) {
        if (!blob_fetcher_) {
          status_ = Status::NotSupported(
              "Blob-backed value filtering requires blob read support in the "
              "current table-file creation context");
          validity_info_.Invalidate();
          return false;
        }

        TEST_SYNC_POINT_CALLBACK(
            "CompactionIterator::InvokeFilterIfNeeded::TamperWithBlobIndex",
            &value_);

        // For integrated BlobDB impl, CompactionIterator reads blob value.
        // For Stacked BlobDB impl, the corresponding CompactionFilter's
        // FilterV2 method should read the blob value.
        BlobIndex blob_index;
        Status s = blob_index.DecodeFrom(value_);
        if (!s.ok()) {
          status_ = s;
          validity_info_.Invalidate();
          return false;
        }

        FilePrefetchBuffer* prefetch_buffer =
            prefetch_buffers_ ? prefetch_buffers_->GetOrCreatePrefetchBuffer(
                                    blob_index.file_number())
                              : nullptr;

        uint64_t bytes_read = 0;

        s = blob_fetcher_->FetchBlob(ikey_.user_key, blob_index,
                                     prefetch_buffer, &blob_value_,
                                     &bytes_read);
        if (!s.ok()) {
          status_ = s;
          validity_info_.Invalidate();
          return false;
        }

        ++iter_stats_.num_blobs_read;
        iter_stats_.total_blob_bytes_read += bytes_read;

        value_type = CompactionFilter::ValueType::kValue;
      }
    }

    if (decision == CompactionFilter::Decision::kUndetermined) {
      const Slice* existing_val = nullptr;
      const WideColumns* existing_col = nullptr;

      // Reuse member variable to avoid per-key heap allocation.
      filter_existing_columns_.clear();
      WideColumnBlobResolver* blob_resolver_ptr = nullptr;

      // Use member variables for entity column storage to avoid per-key
      // heap allocations. Cleared at start of each use.
      entity_columns_.clear();
      entity_blob_columns_.clear();
      // Storage for eagerly resolved blob values used by the FilterV3
      // compatibility path. Keep this in the same scope as the FilterV4()
      // invocation below because filter_existing_columns_ stores Slices into
      // these buffers.
      autovector<PinnableSlice, 4> resolved_blobs_storage;

      if (ikey_.type != kTypeWideColumnEntity) {
        if (!blob_value_.empty()) {
          existing_val = &blob_value_;
        } else {
          existing_val = &value_;
        }
      } else {
        // Check if entity has blob columns that need resolution
        bool has_blob_columns = false;
        {
          Status s_hbc =
              WideColumnSerialization::HasBlobColumns(value_, has_blob_columns);
          if (!s_hbc.ok()) {
            status_ = s_hbc;
            validity_info_.Invalidate();
            return false;
          }
        }
        if (UNLIKELY(has_blob_columns)) {
          // Entity has blob columns. Deserialize() populates
          // entity_columns_ with the full column set; blob-backed entries still
          // carry the serialized BlobIndex bytes in value(). The companion
          // entity_blob_columns_ side list identifies which column indexes are
          // blob references and provides the decoded BlobIndex objects.
          Slice input_copy = value_;
          Status s = WideColumnSerialization::Deserialize(
              input_copy, entity_columns_, &entity_blob_columns_);
          if (!s.ok()) {
            status_ = s;
            validity_info_.Invalidate();
            return false;
          }
          // Mark as deserialized so PrepareOutput can skip re-deserialization
          // if the filter returns kKeep.
          entity_deserialized_ = true;

          // Build filter_existing_columns_ with raw values (blob columns
          // have blob index as value, not the actual blob data)
          for (size_t i = 0; i < entity_columns_.size(); ++i) {
            filter_existing_columns_.emplace_back(entity_columns_[i].name(),
                                                  entity_columns_[i].value());
          }

          if (filter_supports_v4_) {
            // Reset member blob resolver for this entity - filter can call
            // resolver->ResolveColumn() to fetch blob values on-demand
            blob_resolver_.Reset(ikey_.user_key, &entity_columns_,
                                 &entity_blob_columns_,
                                 /*track_resolve_error=*/true);
            blob_resolver_ptr = &blob_resolver_;
          } else {
            // FilterV3 compatibility: eagerly resolve all blob columns so
            // the filter sees actual values (not raw BlobIndex bytes).
            // resolved_blobs_storage is in the outer scope so the data
            // outlives filter_existing_columns_ (which holds Slices into
            // it).
            resolved_blobs_storage.resize(entity_blob_columns_.size());
            for (size_t bi = 0; bi < entity_blob_columns_.size(); ++bi) {
              const size_t col_idx = entity_blob_columns_[bi].first;
              const BlobIndex& blob_idx = entity_blob_columns_[bi].second;

              if (blob_idx.IsInlined()) {
                resolved_blobs_storage[bi].PinSelf(blob_idx.value());
              } else {
                FilePrefetchBuffer* prefetch_buffer =
                    prefetch_buffers_
                        ? prefetch_buffers_->GetOrCreatePrefetchBuffer(
                              blob_idx.file_number())
                        : nullptr;
                uint64_t bytes_read = 0;
                assert(blob_fetcher_);
                Status s_fetch = blob_fetcher_->FetchBlob(
                    ikey_.user_key, blob_idx, prefetch_buffer,
                    &resolved_blobs_storage[bi], &bytes_read);
                if (!s_fetch.ok()) {
                  status_ = s_fetch;
                  validity_info_.Invalidate();
                  return false;
                }
                ++iter_stats_.num_blobs_read;
                iter_stats_.total_blob_bytes_read += bytes_read;
              }

              // Replace the blob index value in filter_existing_columns_
              // with the resolved blob value
              filter_existing_columns_[col_idx] =
                  WideColumn(entity_columns_[col_idx].name(),
                             Slice(resolved_blobs_storage[bi]));
            }
          }
        } else {
          // No blob columns, use fast path
          Slice value_copy = value_;
          const Status s = WideColumnSerialization::DeserializeSimple(
              value_copy, filter_existing_columns_);

          if (!s.ok()) {
            status_ = s;
            validity_info_.Invalidate();
            return false;
          }
        }

        existing_col = &filter_existing_columns_;
      }

      // existing_val / existing_col point into value_, blob_value_,
      // entity_columns_, and resolved_blobs_storage, all of which stay alive
      // until this call returns.
      decision = compaction_filter_->FilterV4(
          level_, filter_key, value_type, existing_val, existing_col,
          &compaction_filter_value_, &new_columns,
          compaction_filter_skip_until_.rep(), blob_resolver_ptr);

      if (blob_resolver_ptr != nullptr) {
        Status resolve_status = blob_resolver_.resolve_status();
        if (!resolve_status.ok()) {
          // Keep lazy FilterV4 failure semantics aligned with the eager
          // FilterV3 compatibility path: if blob resolution fails while the
          // filter is inspecting the entry, fail compaction even if the
          // filter returned kKeep after noticing the error.
          status_ = std::move(resolve_status);
          validity_info_.Invalidate();
          return false;
        }
      }
    }

    iter_stats_.total_filter_time +=
        env_ != nullptr && report_detailed_time_ ? timer.ElapsedNanos() : 0;
  }

  if (decision == CompactionFilter::Decision::kUndetermined) {
    // Should not reach here, since FilterV2/V3/V4 should never return
    // kUndetermined.
    status_ = Status::NotSupported(
        "FilterV2/V3/V4 should never return kUndetermined");
    validity_info_.Invalidate();
    return false;
  }

  if (decision == CompactionFilter::Decision::kRemoveAndSkipUntil &&
      cmp_->Compare(*compaction_filter_skip_until_.rep(), ikey_.user_key) <=
          0) {
    // Can't skip to a key smaller than the current one.
    // Keep the key as per FilterV2/V3/V4 documentation.
    decision = CompactionFilter::Decision::kKeep;
  }

  if (decision == CompactionFilter::Decision::kRemove) {
    // convert the current key to a delete; key_ is pointing into
    // current_key_ at this point, so updating current_key_ updates key()
    ikey_.type = kTypeDeletion;
    current_key_.UpdateInternalKey(ikey_.sequence, kTypeDeletion);
    // no value associated with delete
    value_.clear();
    iter_stats_.num_record_drop_user++;
  } else if (decision == CompactionFilter::Decision::kPurge) {
    // convert the current key to a single delete; key_ is pointing into
    // current_key_ at this point, so updating current_key_ updates key()
    ikey_.type = kTypeSingleDeletion;
    current_key_.UpdateInternalKey(ikey_.sequence, kTypeSingleDeletion);
    // no value associated with single delete
    value_.clear();
    iter_stats_.num_record_drop_user++;
  } else if (decision == CompactionFilter::Decision::kChangeValue) {
    if (ikey_.type != kTypeValue) {
      ikey_.type = kTypeValue;
      current_key_.UpdateInternalKey(ikey_.sequence, kTypeValue);
    }

    value_ = compaction_filter_value_;
  } else if (decision == CompactionFilter::Decision::kRemoveAndSkipUntil) {
    *need_skip = true;
    compaction_filter_skip_until_.ConvertFromUserKey(kMaxSequenceNumber,
                                                     kValueTypeForSeek);
    *skip_until = compaction_filter_skip_until_.Encode();
  } else if (decision == CompactionFilter::Decision::kChangeBlobIndex) {
    // Only the StackableDB-based BlobDB impl's compaction filter should return
    // kChangeBlobIndex. Decision about rewriting blob and changing blob index
    // in the integrated BlobDB impl is made in subsequent call to
    // PrepareOutput() and its callees.
    if (!compaction_filter_->IsStackedBlobDbInternalCompactionFilter()) {
      status_ = Status::NotSupported(
          "Only stacked BlobDB's internal compaction filter can return "
          "kChangeBlobIndex.");
      validity_info_.Invalidate();
      return false;
    }

    if (ikey_.type != kTypeBlobIndex) {
      ikey_.type = kTypeBlobIndex;
      current_key_.UpdateInternalKey(ikey_.sequence, kTypeBlobIndex);
    }

    value_ = compaction_filter_value_;
  } else if (decision == CompactionFilter::Decision::kIOError) {
    if (!compaction_filter_->IsStackedBlobDbInternalCompactionFilter()) {
      status_ = Status::NotSupported(
          "CompactionFilter for integrated BlobDB should not return kIOError");
      validity_info_.Invalidate();
      return false;
    }

    status_ = Status::IOError("Failed to access blob during compaction filter");
    validity_info_.Invalidate();
    return false;
  } else if (decision == CompactionFilter::Decision::kChangeWideColumnEntity) {
    WideColumns sorted_columns;
    sorted_columns.reserve(new_columns.size());

    for (const auto& column : new_columns) {
      sorted_columns.emplace_back(column.first, column.second);
    }

    WideColumnsHelper::SortColumns(sorted_columns);

    {
      const Status s = WideColumnSerialization::Serialize(
          sorted_columns, compaction_filter_value_);
      if (!s.ok()) {
        status_ = s;
        validity_info_.Invalidate();
        return false;
      }
    }

    if (ikey_.type != kTypeWideColumnEntity) {
      ikey_.type = kTypeWideColumnEntity;
      current_key_.UpdateInternalKey(ikey_.sequence, kTypeWideColumnEntity);
    }

    value_ = compaction_filter_value_;
    // Value changed, force re-deserialization in PrepareOutput
    entity_deserialized_ = false;
  }

  if (UNLIKELY(compaction_filter_value_.size() >
               std::numeric_limits<uint32_t>::max())) {
    status_ = Status::Corruption(
        "CompactionFilter result value size exceeds 4GB limit");
    validity_info_.Invalidate();
    return false;
  }

  return true;
}

void CompactionIterator::NextFromInput() {
  at_next_ = false;
  validity_info_.Invalidate();
  entity_deserialized_ = false;

  while (!Valid() && input_.Valid() && !IsPausingManualCompaction() &&
         !IsShuttingDown()) {
    // A filtered-out key can advance directly to the next input record without
    // returning to PrepareOutput(), so clear the per-record deserialization
    // cache at the start of each loop iteration.
    entity_deserialized_ = false;
    key_ = input_.key();
    value_ = input_.value();
    blob_value_.Reset();
    iter_stats_.num_input_records++;
    is_range_del_ = input_.IsDeleteRangeSentinelKey();

    Status pik_status = ParseInternalKey(key_, &ikey_, allow_data_in_errors_);
    if (!pik_status.ok()) {
      iter_stats_.num_input_corrupt_records++;

      // Always fail compaction when encountering corrupted internal keys
      status_ = pik_status;
      return;
    }
    if (has_consumed_earlier_entries_of_current_key_ &&
        !cmp_->EqualWithoutTimestamp(ikey_.user_key, current_user_key_)) {
      // current_user_key_ remains usable when lookahead or merge handling
      // clears has_current_user_key_.
      has_consumed_earlier_entries_of_current_key_ = false;
    }
    TEST_SYNC_POINT_CALLBACK("CompactionIterator:ProcessKV", &ikey_);
    if (is_range_del_) {
      validity_info_.SetValid(kRangeDeletion);
      break;
    }
    // Update input statistics
    if (ikey_.type == kTypeDeletion || ikey_.type == kTypeSingleDeletion ||
        ikey_.type == kTypeDeletionWithTimestamp) {
      iter_stats_.num_input_deletion_records++;
    } else if (ikey_.type == kTypeValuePreferredSeqno) {
      iter_stats_.num_input_timed_put_records++;
    }
    iter_stats_.total_input_raw_key_bytes += key_.size();
    iter_stats_.total_input_raw_value_bytes += value_.size();

    // If need_skip is true, we should seek the input iterator
    // to internal key skip_until and continue from there.
    bool need_skip = false;
    // Points either into compaction_filter_skip_until_ or into
    // merge_helper_->compaction_filter_skip_until_.
    Slice skip_until;

    bool user_key_equal_without_ts = false;
    int cmp_ts = 0;
    if (has_current_user_key_) {
      user_key_equal_without_ts =
          cmp_->EqualWithoutTimestamp(ikey_.user_key, current_user_key_);
      // if timestamp_size_ > 0, then curr_ts_ has been initialized by a
      // previous key.
      cmp_ts = timestamp_size_ ? cmp_->CompareTimestamp(
                                     ExtractTimestampFromUserKey(
                                         ikey_.user_key, timestamp_size_),
                                     curr_ts_)
                               : 0;
    }

    // Check whether the user key changed. After this if statement current_key_
    // is a copy of the current input key (maybe converted to a delete by the
    // compaction filter). ikey_.user_key is pointing to the copy.
    if (!has_current_user_key_ || !user_key_equal_without_ts || cmp_ts != 0) {
      // First occurrence of this user key
      // Copy key for output
      key_ = current_key_.SetInternalKey(key_, &ikey_);

      int prev_cmp_with_ts_low =
          !full_history_ts_low_ ? 0
          : curr_ts_.empty()
              ? 0
              : cmp_->CompareTimestamp(curr_ts_, *full_history_ts_low_);

      // If timestamp_size_ > 0, then copy from ikey_ to curr_ts_ for the use
      // in next iteration to compare with the timestamp of next key.
      UpdateTimestampAndCompareWithFullHistoryLow();

      // If
      // (1) !has_current_user_key_, OR
      // (2) timestamp is disabled, OR
      // (3) all history will be preserved, OR
      // (4) user key (excluding timestamp) is different from previous key, OR
      // (5) timestamp is NO older than *full_history_ts_low_, OR
      // (6) timestamp is the largest one older than full_history_ts_low_,
      // then current_user_key_ must be treated as a different user key.
      // This means, if a user key (excluding ts) is the same as the previous
      // user key, and its ts is older than *full_history_ts_low_, then we
      // consider this key for GC, e.g. it may be dropped if certain conditions
      // match.
      if (!has_current_user_key_ || !timestamp_size_ || !full_history_ts_low_ ||
          !user_key_equal_without_ts || cmp_with_history_ts_low_ >= 0 ||
          prev_cmp_with_ts_low >= 0) {
        // Initialize for future comparison for rule (A) and etc.
        current_user_key_sequence_ = kMaxSequenceNumber;
        current_user_key_snapshot_ = 0;
        has_current_user_key_ = true;
      }
      current_user_key_ = ikey_.user_key;

      has_outputted_key_ = false;

      last_key_seq_zeroed_ = false;

      current_key_committed_ = KeyCommitted(ikey_.sequence);

      // Apply the compaction filter to the first committed version of the user
      // key.
      if (current_key_committed_ &&
          !InvokeFilterIfNeeded(&need_skip, &skip_until)) {
        break;
      }
    } else {
      // Update the current key to reflect the new sequence number/type without
      // copying the user key.
      // TODO(rven): Compaction filter does not process keys in this path
      // Need to have the compaction filter process multiple versions
      // if we have versions on both sides of a snapshot
      current_key_.UpdateInternalKey(ikey_.sequence, ikey_.type);
      key_ = current_key_.GetInternalKey();
      ikey_.user_key = current_key_.GetUserKey();

      // Note that newer version of a key is ordered before older versions. If a
      // newer version of a key is committed, so as the older version. No need
      // to query snapshot_checker_ in that case.
      if (UNLIKELY(!current_key_committed_)) {
        assert(snapshot_checker_ != nullptr);
        current_key_committed_ = KeyCommitted(ikey_.sequence);
        // Apply the compaction filter to the first committed version of the
        // user key.
        if (current_key_committed_ &&
            !InvokeFilterIfNeeded(&need_skip, &skip_until)) {
          break;
        }
      }
    }

    if (UNLIKELY(!current_key_committed_)) {
      assert(snapshot_checker_ != nullptr);
      validity_info_.SetValid(ValidContext::kCurrentKeyUncommitted);
      break;
    }

    // If there are no snapshots, then this kv affect visibility at tip.
    // Otherwise, search though all existing snapshots to find the earliest
    // snapshot that is affected by this kv.
    SequenceNumber last_sequence = current_user_key_sequence_;
    current_user_key_sequence_ = ikey_.sequence;
    SequenceNumber last_snapshot = current_user_key_snapshot_;
    SequenceNumber prev_snapshot = 0;  // 0 means no previous snapshot
    current_user_key_snapshot_ =
        visible_at_tip_
            ? earliest_snapshot_
            : findEarliestVisibleSnapshot(ikey_.sequence, &prev_snapshot);

    if (need_skip) {
      // This case is handled below.
    } else if (clear_and_output_next_key_) {
      // In the previous iteration we encountered a single delete that we could
      // not compact out.  We will keep this Put, but can drop it's data.
      // (See Optimization 3, below.)
      if (ikey_.type != kTypeValue && ikey_.type != kTypeBlobIndex &&
          ikey_.type != kTypeWideColumnEntity &&
          ikey_.type != kTypeValuePreferredSeqno) {
        ROCKS_LOG_FATAL(info_log_, "Unexpected key %s for compaction output",
                        ikey_.DebugString(allow_data_in_errors_, true).c_str());
        assert(false);
      }
      if (current_user_key_snapshot_ < last_snapshot) {
        ROCKS_LOG_FATAL(info_log_,
                        "key %s, current_user_key_snapshot_ (%" PRIu64
                        ") < last_snapshot (%" PRIu64 ")",
                        ikey_.DebugString(allow_data_in_errors_, true).c_str(),
                        current_user_key_snapshot_, last_snapshot);
        assert(false);
      }

      if (ikey_.type == kTypeBlobIndex || ikey_.type == kTypeWideColumnEntity ||
          ikey_.type == kTypeValuePreferredSeqno) {
        ikey_.type = kTypeValue;
        current_key_.UpdateInternalKey(ikey_.sequence, ikey_.type);
      }

      value_.clear();
      validity_info_.SetValid(ValidContext::kKeepSDAndClearPut);
      clear_and_output_next_key_ = false;
    } else if (ikey_.type == kTypeSingleDeletion) {
      // We can compact out a SingleDelete if:
      // 1) We encounter the corresponding PUT -OR- we know that this key
      //    doesn't appear past this output level and  we are not in
      //    ingest_behind mode.
      // =AND=
      // 2) We've already returned a record in this snapshot -OR-
      //    there are no earlier earliest_write_conflict_snapshot.
      //
      // A note about 2) above:
      // we try to determine whether there is any earlier write conflict
      // checking snapshot by calling DefinitelyInSnapshot() with seq and
      // earliest_write_conflict_snapshot as arguments. For write-prepared
      // and write-unprepared transactions, if earliest_write_conflict_snapshot
      // is evicted from WritePreparedTxnDB::commit_cache, then
      // DefinitelyInSnapshot(seq, earliest_write_conflict_snapshot) returns
      // false, even if the seq is actually visible within
      // earliest_write_conflict_snapshot. Consequently, CompactionIterator
      // may try to zero out its sequence number, thus hitting assertion error
      // in debug mode or cause incorrect DBIter return result.
      // We observe that earliest_write_conflict_snapshot >= earliest_snapshot,
      // and the seq zeroing logic depends on
      // DefinitelyInSnapshot(seq, earliest_snapshot). Therefore, if we cannot
      // determine whether seq is **definitely** in
      // earliest_write_conflict_snapshot, then we can additionally check if
      // seq is definitely in earliest_snapshot. If the latter holds, then the
      // former holds too.
      //
      // Rule 1 is needed for SingleDelete correctness.  Rule 2 is needed to
      // allow Transactions to do write-conflict checking (if we compacted away
      // all keys, then we wouldn't know that a write happened in this
      // snapshot).  If there is no earlier snapshot, then we know that there
      // are no active transactions that need to know about any writes.
      //
      // Optimization 3:
      // If we encounter a SingleDelete followed by a PUT and Rule 2 is NOT
      // true, then we must output a SingleDelete.  In this case, we will decide
      // to also output the PUT.  While we are compacting less by outputting the
      // PUT now, hopefully this will lead to better compaction in the future
      // when Rule 2 is later true (Ie, We are hoping we can later compact out
      // both the SingleDelete and the Put, while we couldn't if we only
      // outputted the SingleDelete now).
      // In this case, we can save space by removing the PUT's value as it will
      // never be read.
      //
      // Deletes and Merges are not supported on the same key that has a
      // SingleDelete as it is not possible to correctly do any partial
      // compaction of such a combination of operations.  The result of mixing
      // those operations for a given key is documented as being undefined.  So
      // we can choose how to handle such a combinations of operations.  We will
      // try to compact out as much as we can in these cases.
      // We will report counts on these anomalous cases.
      //
      // Note: If timestamp is enabled, then record will be eligible for
      // deletion, only if, along with above conditions (Rule 1 and Rule 2)
      // full_history_ts_low_ is specified and timestamp for that key is less
      // than *full_history_ts_low_. If it's not eligible for deletion, then we
      // will output the SingleDelete. For Optimization 3 also, if
      // full_history_ts_low_ is specified and timestamp for the key is less
      // than *full_history_ts_low_ then only optimization will be applied.

      // The easiest way to process a SingleDelete during iteration is to peek
      // ahead at the next key.
      const bool is_timestamp_eligible_for_gc =
          (timestamp_size_ == 0 ||
           (full_history_ts_low_ && cmp_with_history_ts_low_ < 0));

      ParsedInternalKey next_ikey;
      AdvanceInputIterWithoutOutput();
      while (input_.Valid() && input_.IsDeleteRangeSentinelKey() &&
             ParseInternalKey(input_.key(), &next_ikey, allow_data_in_errors_)
                 .ok() &&
             cmp_->EqualWithoutTimestamp(ikey_.user_key, next_ikey.user_key)) {
        // skip range tombstone start keys with the same user key
        // since they are not "real" point keys.
        AdvanceInputIterWithoutOutput();
      }

      // Check whether the next key exists, is not corrupt, and is the same key
      // as the single delete.
      if (input_.Valid() &&
          ParseInternalKey(input_.key(), &next_ikey, allow_data_in_errors_)
              .ok() &&
          cmp_->EqualWithoutTimestamp(ikey_.user_key, next_ikey.user_key)) {
        assert(!input_.IsDeleteRangeSentinelKey());
#ifndef NDEBUG
        const Compaction* c =
            compaction_ ? compaction_->real_compaction() : nullptr;
#endif
        TEST_SYNC_POINT_CALLBACK(
            "CompactionIterator::NextFromInput:SingleDelete:1",
            const_cast<Compaction*>(c));
        if (last_key_seq_zeroed_) {
          // Drop SD and the next key since they are both in the last
          // snapshot (since last key has seqno zeroed).
          ++iter_stats_.num_record_drop_hidden;
          ++iter_stats_.num_record_drop_obsolete;
          assert(bottommost_level_);
          AdvanceInputIterWithoutOutput();
        } else if (prev_snapshot == 0 ||
                   DefinitelyNotInSnapshot(next_ikey.sequence, prev_snapshot)) {
          // Check whether the next key belongs to the same snapshot as the
          // SingleDelete.

          TEST_SYNC_POINT_CALLBACK(
              "CompactionIterator::NextFromInput:SingleDelete:2", nullptr);
          if (next_ikey.type == kTypeSingleDeletion) {
            // We encountered two SingleDeletes for same key in a row. This
            // could be due to unexpected user input. If write-(un)prepared
            // transaction is used, this could also be due to releasing an old
            // snapshot between a Put and its matching SingleDelete.
            // Skip the first SingleDelete and let the next iteration decide
            // how to handle the second SingleDelete.

            // First SingleDelete has been skipped since we already called
            // input_.Next().
            ++iter_stats_.num_record_drop_obsolete;
            ++iter_stats_.num_single_del_mismatch;
          } else if (next_ikey.type == kTypeDeletion) {
            std::ostringstream oss;
            oss << "Found SD and type: " << static_cast<int>(next_ikey.type)
                << " on the same key, violating the contract "
                   "of SingleDelete. Check your application to make sure the "
                   "application does not mix SingleDelete and Delete for "
                   "the same key. If you are using "
                   "write-prepared/write-unprepared transactions, and use "
                   "SingleDelete to delete certain keys, then make sure "
                   "TransactionDBOptions::rollback_deletion_type_callback is "
                   "configured properly. Mixing SD and DEL can lead to "
                   "undefined behaviors";
            ++iter_stats_.num_record_drop_obsolete;
            ++iter_stats_.num_single_del_mismatch;
            if (enforce_single_del_contracts_) {
              ROCKS_LOG_ERROR(info_log_, "%s", oss.str().c_str());
              validity_info_.Invalidate();
              status_ = Status::Corruption(oss.str());
              return;
            }
            ROCKS_LOG_WARN(info_log_, "%s", oss.str().c_str());
          } else if (!is_timestamp_eligible_for_gc) {
            // We cannot drop the SingleDelete as timestamp is enabled, and
            // timestamp of this key is greater than or equal to
            // *full_history_ts_low_. We will output the SingleDelete.
            validity_info_.SetValid(ValidContext::kKeepTsHistory);
          } else if (has_outputted_key_ ||
                     DefinitelyInSnapshot(ikey_.sequence,
                                          earliest_write_conflict_snapshot_) ||
                     (earliest_snapshot_ < earliest_write_conflict_snapshot_ &&
                      DefinitelyInSnapshot(ikey_.sequence,
                                           earliest_snapshot_))) {
            // Found a matching value, we can drop the single delete and the
            // value.  It is safe to drop both records since we've already
            // outputted a key in this snapshot, or there is no earlier
            // snapshot (Rule 2 above).

            // Note: it doesn't matter whether the second key is a Put or if it
            // is an unexpected Merge or Delete.  We will compact it out
            // either way. We will maintain counts of how many mismatches
            // happened
            if (next_ikey.type != kTypeValue &&
                next_ikey.type != kTypeBlobIndex &&
                next_ikey.type != kTypeWideColumnEntity &&
                next_ikey.type != kTypeValuePreferredSeqno) {
              ++iter_stats_.num_single_del_mismatch;
            }

            ++iter_stats_.num_record_drop_hidden;
            ++iter_stats_.num_record_drop_obsolete;
            // Already called input_.Next() once.  Call it a second time to
            // skip past the second key.
            AdvanceInputIterWithoutOutput();
          } else {
            // Found a matching value, but we cannot drop both keys since
            // there is an earlier snapshot and we need to leave behind a record
            // to know that a write happened in this snapshot (Rule 2 above).
            // Clear the value and output the SingleDelete. (The value will be
            // outputted on the next iteration.)

            // Setting valid_ to true will output the current SingleDelete
            validity_info_.SetValid(ValidContext::kKeepSDForConflictCheck);

            // Set up the Put to be outputted in the next iteration.
            // (Optimization 3).
            clear_and_output_next_key_ = true;
            TEST_SYNC_POINT_CALLBACK(
                "CompactionIterator::NextFromInput:KeepSDForWW",
                /*arg=*/nullptr);
          }
        } else {
          // We hit the next snapshot without hitting a put, so the iterator
          // returns the single delete.
          validity_info_.SetValid(ValidContext::kKeepSDForSnapshot);
          TEST_SYNC_POINT_CALLBACK(
              "CompactionIterator::NextFromInput:SingleDelete:3",
              const_cast<Compaction*>(c));
        }
      } else {
        // We are at the end of the input, could not parse the next key, or hit
        // a different key. The iterator returns the single delete if the key
        // possibly exists beyond the current output level.  We set
        // has_current_user_key to false so that if the iterator is at the next
        // key, we do not compare it again against the previous key at the next
        // iteration. If the next key is corrupt, we return before the
        // comparison, so the value of has_current_user_key does not matter.
        has_current_user_key_ = false;
        if (compaction_ != nullptr && !compaction_->allow_ingest_behind() &&
            DefinitelyInSnapshot(ikey_.sequence, earliest_snapshot_) &&
            compaction_->KeyNotExistsBeyondOutputLevel(ikey_.user_key,
                                                       &level_ptrs_) &&
            is_timestamp_eligible_for_gc) {
          // Key doesn't exist outside of this range.
          // Can compact out this SingleDelete.
          ++iter_stats_.num_record_drop_obsolete;
          ++iter_stats_.num_single_del_fallthru;
          if (!bottommost_level_) {
            ++iter_stats_.num_optimized_del_drop_obsolete;
          }
        } else if (last_key_seq_zeroed_) {
          // Sequence number zeroing requires bottommost_level_, which is
          // false with ingest_behind.
          assert(!compaction_->allow_ingest_behind());
          // Skip.
          ++iter_stats_.num_record_drop_hidden;
          ++iter_stats_.num_record_drop_obsolete;
          assert(bottommost_level_);
        } else {
          // Output SingleDelete
          validity_info_.SetValid(ValidContext::kKeepSD);
        }
      }

      if (Valid()) {
        at_next_ = true;
      }
    } else if (last_sequence != kMaxSequenceNumber &&
               (last_snapshot == current_user_key_snapshot_ ||
                last_snapshot < current_user_key_snapshot_)) {
      // rule (A):
      // If the earliest snapshot is which this key is visible in
      // is the same as the visibility of a previous instance of the
      // same key, then this kv is not visible in any snapshot.
      // Hidden by an newer entry for same user key
      //
      // Note: Dropping this key will not affect TransactionDB write-conflict
      // checking since there has already been a record returned for this key
      // in this snapshot.
      // When ingest_behind is enabled, it's ok that we drop an overwritten
      // Delete here. The overwritting key still covers whatever that will be
      // ingested. Note that we will not drop SingleDelete here as SingleDelte
      // is handled entirely in its own if clause. This is important, see
      // example: from new to old: SingleDelete_1, PUT_1, SingleDelete_2, PUT_2,
      // where all operations are on the same key and PUT_2 is ingested with
      // ingest_behind=true. If SingleDelete_2 is dropped due to being compacted
      // together with PUT_1, and then PUT_1 is compacted away together with
      // SingleDelete_1, PUT_2 can incorrectly becomes visible.
      if (last_sequence < current_user_key_sequence_) {
        ROCKS_LOG_FATAL(info_log_,
                        "key %s, last_sequence (%" PRIu64
                        ") < current_user_key_sequence_ (%" PRIu64 ")",
                        ikey_.DebugString(allow_data_in_errors_, true).c_str(),
                        last_sequence, current_user_key_sequence_);
        assert(false);
      }

      ++iter_stats_.num_record_drop_hidden;
      AdvanceInputIterWithoutOutput();
    } else if (compaction_ != nullptr &&
               (ikey_.type == kTypeDeletion ||
                (ikey_.type == kTypeDeletionWithTimestamp &&
                 cmp_with_history_ts_low_ < 0)) &&
               !compaction_->allow_ingest_behind() &&
               DefinitelyInSnapshot(ikey_.sequence, earliest_snapshot_) &&
               compaction_->KeyNotExistsBeyondOutputLevel(ikey_.user_key,
                                                          &level_ptrs_)) {
      // TODO(noetzli): This is the only place where we use compaction_
      // (besides the constructor). We should probably get rid of this
      // dependency and find a way to do similar filtering during flushes.
      //
      // For this user key:
      // (1) there is no data in higher levels
      // (2) data in lower levels will have larger sequence numbers
      // (3) data in layers that are being compacted here and have
      //     smaller sequence numbers will be dropped in the next
      //     few iterations of this loop (by rule (A) above).
      // Therefore this deletion marker is obsolete and can be dropped.
      //
      // Note:  Dropping this Delete will not affect TransactionDB
      // write-conflict checking since it is earlier than any snapshot.
      //
      // It seems that we can also drop deletion later than earliest snapshot
      // given that:
      // (1) The deletion is earlier than earliest_write_conflict_snapshot, and
      // (2) No value exist earlier than the deletion.
      //
      // Note also that a deletion marker of type kTypeDeletionWithTimestamp
      // will be treated as a different user key unless the timestamp is older
      // than *full_history_ts_low_.
      ++iter_stats_.num_record_drop_obsolete;
      if (!bottommost_level_) {
        ++iter_stats_.num_optimized_del_drop_obsolete;
      }
      AdvanceInputIterWithoutOutput();
    } else if ((ikey_.type == kTypeDeletion ||
                (ikey_.type == kTypeDeletionWithTimestamp &&
                 cmp_with_history_ts_low_ < 0)) &&
               bottommost_level_) {
      assert(compaction_);
      assert(!compaction_->allow_ingest_behind());  // bottommost_level_ is true
      // Handle the case where we have a delete key at the bottom most level
      // We can skip outputting the key iff there are no subsequent puts for
      // this key
      assert(compaction_->KeyNotExistsBeyondOutputLevel(ikey_.user_key,
                                                        &level_ptrs_));
      ParsedInternalKey next_ikey;
      AdvanceInputIterWithoutOutput();
#ifndef NDEBUG
      const Compaction* c =
          compaction_ ? compaction_->real_compaction() : nullptr;
#endif
      TEST_SYNC_POINT_CALLBACK(
          "CompactionIterator::NextFromInput:BottommostDelete:1",
          const_cast<Compaction*>(c));
      // Skip over all versions of this key that happen to occur in the same
      // snapshot range as the delete.
      //
      // Note that a deletion marker of type kTypeDeletionWithTimestamp will be
      // considered to have a different user key unless the timestamp is older
      // than *full_history_ts_low_.
      //
      // Range tombstone start keys are skipped as they are not "real" keys.
      while (!IsPausingManualCompaction() && !IsShuttingDown() &&
             input_.Valid() &&
             (ParseInternalKey(input_.key(), &next_ikey, allow_data_in_errors_)
                  .ok()) &&
             cmp_->EqualWithoutTimestamp(ikey_.user_key, next_ikey.user_key) &&
             (prev_snapshot == 0 || input_.IsDeleteRangeSentinelKey() ||
              DefinitelyNotInSnapshot(next_ikey.sequence, prev_snapshot))) {
        AdvanceInputIterWithoutOutput();
      }
      // If you find you still need to output a row with this key, we need to
      // output the delete too
      if (input_.Valid() &&
          (ParseInternalKey(input_.key(), &next_ikey, allow_data_in_errors_)
               .ok()) &&
          cmp_->EqualWithoutTimestamp(ikey_.user_key, next_ikey.user_key)) {
        validity_info_.SetValid(ValidContext::kKeepDel);
        at_next_ = true;
      }
    } else if (ikey_.type == kTypeValuePreferredSeqno &&
               DefinitelyInSnapshot(ikey_.sequence, earliest_snapshot_) &&
               (bottommost_level_ ||
                (compaction_ != nullptr &&
                 compaction_->KeyNotExistsBeyondOutputLevel(ikey_.user_key,
                                                            &level_ptrs_)))) {
      // FIXME: it's possible that we are setting sequence number to 0 as
      // preferred sequence number here. If cf_ingest_behind is enabled, this
      // may fail ingestions since they expect all keys above the last level
      // to have non-zero sequence number. We should probably not allow seqno
      // zeroing here.
      //
      // This section that attempts to swap preferred sequence number will not
      // be invoked if this is a CompactionIterator created for flush, since
      // `compaction_` will be nullptr and it's not bottommost either.
      //
      // The entries with the same user key and smaller sequence numbers are
      // all in this earliest snapshot range to be iterated. Since those entries
      // will be hidden by this entry [rule A], it's safe to swap in the
      // preferred seqno now.
      //
      // It's otherwise not safe to swap in the preferred seqno since it's
      // possible for entries in earlier snapshots to have sequence number that
      // is smaller than this entry's sequence number but bigger than this
      // entry's preferred sequence number. Swapping in the preferred sequence
      // number will break the internal key ordering invariant for this key.
      //
      // A special case involving range deletion is handled separately below.
      auto [unpacked_value, preferred_seqno] =
          ParsePackedValueWithSeqno(value_);
      assert(preferred_seqno < ikey_.sequence || ikey_.sequence == 0);
      if (range_del_agg_->ShouldDelete(
              key_, RangeDelPositioningMode::kForwardTraversal)) {
        ++iter_stats_.num_record_drop_hidden;
        ++iter_stats_.num_record_drop_range_del;
        AdvanceInputIterWithoutOutput();
      } else {
        InternalKey ikey_after_swap(ikey_.user_key,
                                    std::min(preferred_seqno, ikey_.sequence),
                                    kTypeValue);
        Slice ikey_after_swap_slice(*ikey_after_swap.rep());
        if (range_del_agg_->ShouldDelete(
                ikey_after_swap_slice,
                RangeDelPositioningMode::kForwardTraversal)) {
          // A range tombstone that doesn't cover this kTypeValuePreferredSeqno
          // entry will end up covering the entry, so it's not safe to swap
          // preferred sequence number. In this case, we output the entry as is.
          validity_info_.SetValid(ValidContext::kNewUserKey);
        } else {
          if (ikey_.sequence != 0) {
            iter_stats_.num_timed_put_swap_preferred_seqno++;
            ikey_.sequence = preferred_seqno;
          }
          ikey_.type = kTypeValue;
          current_key_.UpdateInternalKey(ikey_.sequence, ikey_.type);
          key_ = current_key_.GetInternalKey();
          ikey_.user_key = current_key_.GetUserKey();
          value_ = unpacked_value;
          validity_info_.SetValid(ValidContext::kSwapPreferredSeqno);
        }
      }
    } else if (ikey_.type == kTypeMerge) {
      if (!merge_helper_->HasOperator()) {
        status_ = Status::InvalidArgument(
            "merge_operator is not properly initialized.");
        return;
      }

      pinned_iters_mgr_.StartPinning();

      // We know the merge type entry is not hidden, otherwise we would
      // have hit (A)
      // We encapsulate the merge related state machine in a different
      // object to minimize change to the existing flow.
      has_consumed_earlier_entries_of_current_key_ = true;
      merge_until_status_ = merge_helper_->MergeUntil(
          &input_, range_del_agg_, prev_snapshot, bottommost_level_,
          allow_data_in_errors_, blob_fetcher_.get(), full_history_ts_low_,
          prefetch_buffers_.get(), &iter_stats_);
      merge_out_iter_.SeekToFirst();

      if (!merge_until_status_.ok() &&
          !merge_until_status_.IsMergeInProgress()) {
        status_ = merge_until_status_;
        return;
      } else if (merge_out_iter_.Valid()) {
        // NOTE: key, value, and ikey_ refer to old entries.
        //       These will be correctly set below.
        key_ = merge_out_iter_.key();
        value_ = merge_out_iter_.value();
        pik_status = ParseInternalKey(key_, &ikey_, allow_data_in_errors_);
        // MergeUntil stops when it encounters a corrupt key and does not
        // include them in the result, so we expect the keys here to valid.
        if (!pik_status.ok()) {
          ROCKS_LOG_FATAL(
              info_log_, "Invalid key %s in compaction. %s",
              allow_data_in_errors_ ? key_.ToString(true).c_str() : "hidden",
              pik_status.getState());
          assert(false);
        }
        // Keep current_key_ in sync.
        current_key_.UpdateInternalKey(ikey_.sequence, ikey_.type);
        key_ = current_key_.GetInternalKey();
        ikey_.user_key = current_key_.GetUserKey();
        validity_info_.SetValid(ValidContext::kMerge2);
      } else {
        // all merge operands were filtered out. reset the user key, since the
        // batch consumed by the merge operator should not shadow any keys
        // coming after the merges
        has_current_user_key_ = false;
        pinned_iters_mgr_.ReleasePinnedData();

        if (merge_helper_->FilteredUntil(&skip_until)) {
          need_skip = true;
        }
      }
    } else {
      // 1. new user key -OR-
      // 2. different snapshot stripe
      // If user-defined timestamp is enabled, we consider keys for GC if they
      // are below history_ts_low_. CompactionRangeDelAggregator::ShouldDelete()
      // only considers range deletions that are at or below history_ts_low_ and
      // trim_ts_. We drop keys here that are below history_ts_low_ and are
      // covered by a range tombstone that is at or below history_ts_low_ and
      // trim_ts.
      bool should_delete = false;
      if (!timestamp_size_ || cmp_with_history_ts_low_ < 0) {
        should_delete = range_del_agg_->ShouldDelete(
            key_, RangeDelPositioningMode::kForwardTraversal);
      }
      if (should_delete) {
        ++iter_stats_.num_record_drop_hidden;
        ++iter_stats_.num_record_drop_range_del;
        AdvanceInputIterWithoutOutput();
      } else {
        validity_info_.SetValid(ValidContext::kNewUserKey);
      }
    }

    if (need_skip) {
      SkipUntil(skip_until);
    }
  }

  if (status_.ok()) {
    if (!Valid() && IsShuttingDown()) {
      status_ = Status::ShutdownInProgress();
    } else if (IsPausingManualCompaction()) {
      status_ = Status::Incomplete(Status::SubCode::kManualCompactionPaused);
    } else if (!input_.Valid() && input_.status().IsCorruption()) {
      // Propagate corruption status from memtable iterator
      status_ = input_.status();
    }
  }
}

bool CompactionIterator::ExtractLargeValueIfNeededImpl() {
  if (!blob_file_builder_) {
    return false;
  }

  blob_index_.clear();
  const Status s = blob_file_builder_->Add(user_key(), value_, &blob_index_);

  if (!s.ok()) {
    status_ = s;
    validity_info_.Invalidate();

    return false;
  }

  if (blob_index_.empty()) {
    return false;
  }

  value_ = blob_index_;

  return true;
}

void CompactionIterator::ExtractLargeValueIfNeeded() {
  assert(ikey_.type == kTypeValue);

  if (!ExtractLargeValueIfNeededImpl()) {
    return;
  }

  ikey_.type = kTypeBlobIndex;
  current_key_.UpdateInternalKey(ikey_.sequence, ikey_.type);
}

void CompactionIterator::GarbageCollectBlobIfNeeded() {
  assert(ikey_.type == kTypeBlobIndex);

  if (!compaction_) {
    return;
  }

  // GC for integrated BlobDB
  if (compaction_->enable_blob_garbage_collection()) {
    TEST_SYNC_POINT_CALLBACK(
        "CompactionIterator::GarbageCollectBlobIfNeeded::TamperWithBlobIndex",
        &value_);

    BlobIndex blob_index;

    {
      const Status s = blob_index.DecodeFrom(value_);

      if (!s.ok()) {
        status_ = s;
        validity_info_.Invalidate();

        return;
      }
    }

    if (blob_index.file_number() >=
        blob_garbage_collection_cutoff_file_number_) {
      return;
    }

    FilePrefetchBuffer* prefetch_buffer =
        prefetch_buffers_ ? prefetch_buffers_->GetOrCreatePrefetchBuffer(
                                blob_index.file_number())
                          : nullptr;

    uint64_t bytes_read = 0;

    {
      assert(blob_fetcher_);

      const Status s = blob_fetcher_->FetchBlob(
          user_key(), blob_index, prefetch_buffer, &blob_value_, &bytes_read);

      if (!s.ok()) {
        status_ = s;
        validity_info_.Invalidate();

        return;
      }
    }

    ++iter_stats_.num_blobs_read;
    iter_stats_.total_blob_bytes_read += bytes_read;

    ++iter_stats_.num_blobs_relocated;
    iter_stats_.total_blob_bytes_relocated += blob_index.size();

    value_ = blob_value_;

    if (ExtractLargeValueIfNeededImpl()) {
      return;
    }

    ikey_.type = kTypeValue;
    current_key_.UpdateInternalKey(ikey_.sequence, ikey_.type);

    return;
  }

  // GC for stacked BlobDB
  if (compaction_filter_ &&
      compaction_filter_->IsStackedBlobDbInternalCompactionFilter()) {
    const auto blob_decision = compaction_filter_->PrepareBlobOutput(
        user_key(), value_, &compaction_filter_value_);

    if (blob_decision == CompactionFilter::BlobDecision::kCorruption) {
      status_ =
          Status::Corruption("Corrupted blob reference encountered during GC");
      validity_info_.Invalidate();

      return;
    }

    if (blob_decision == CompactionFilter::BlobDecision::kIOError) {
      status_ = Status::IOError("Could not relocate blob during GC");
      validity_info_.Invalidate();

      return;
    }

    if (blob_decision == CompactionFilter::BlobDecision::kChangeValue) {
      value_ = compaction_filter_value_;

      return;
    }
  }
}

// Wide-column entity compaction flow after InvokeFilterIfNeeded():
//   1. PrepareOutput() is the only caller of the helper block below.
//   2. PrepareOutput() ensures entity_columns_/entity_blob_columns_ describe
//      the current entity, either by reusing the filter-time deserialization or
//      by deserializing once itself.
//   3. PrepareOutput() then picks exactly one follow-up path:
//        - inline-only entity:
//            ExtractLargeColumnValuesIfNeeded()
//          This is the flush/compaction extraction path. It may rewrite large
//          inline columns into blob references.
//        - entity already containing blob references:
//            GarbageCollectEntityBlobsIfNeeded()
//          This is the blob-GC path. It only fetches / relocates references
//          that fall below the current GC cutoff.
// Both helpers leave value_ unchanged when no rewrite is needed.
void CompactionIterator::ExtractLargeColumnValuesIfNeeded() {
  assert(ikey_.type == kTypeWideColumnEntity);

  // Check if blob extraction is enabled
  if (!blob_file_builder_) {
    return;
  }

  // Deserialize using member variables (already populated by PrepareOutput
  // for the combined deserialization path). If not yet populated, deserialize
  // here.
  if (entity_columns_.empty()) {
    Slice entity_slice = value_;
    entity_columns_.clear();
    entity_blob_columns_.clear();
    Status s = WideColumnSerialization::Deserialize(
        entity_slice, entity_columns_, &entity_blob_columns_);
    if (!s.ok()) {
      status_ = s;
      validity_info_.Invalidate();
      return;
    }
  }

  // IMPORTANT: If there are already blob columns, we skip extraction entirely.
  // This means new large inline columns added to such entities (e.g., via
  // PutEntity after a previous PutEntity that already had blob columns) will
  // NOT be extracted to blob files until the entity undergoes a full rewrite
  // (e.g., via compaction that GCs all existing blob refs).
  //
  // This is a deliberate simplification: supporting partial extraction
  // (extracting only new large inline columns while preserving existing
  // blob refs) would require merging old and new blob column sets during
  // serialization, and the benefit is marginal since full rewrites happen
  // naturally during blob GC.
  if (UNLIKELY(!entity_blob_columns_.empty())) {
    return;
  }

  // Try to extract large column values to blob files
  // Track which columns were extracted and their blob indices
  std::vector<std::pair<size_t, BlobIndex>> new_blob_columns;
  std::string temp_blob_index;

  for (size_t i = 0; i < entity_columns_.size(); ++i) {
    const Slice& col_value = entity_columns_[i].value();

    // Clear the temporary buffer for each column
    temp_blob_index.clear();

    // Try to add the column value to blob file
    // blob_file_builder_->Add() will check min_blob_size internally
    // and only write to blob file if the value is large enough
    const Status add_status =
        blob_file_builder_->Add(user_key(), col_value, &temp_blob_index);
    if (!add_status.ok()) {
      status_ = add_status;
      validity_info_.Invalidate();
      return;
    }

    // If blob_index is not empty, the value was extracted to a blob file
    if (!temp_blob_index.empty()) {
      BlobIndex blob_idx;
      const Status decode_status = blob_idx.DecodeFrom(temp_blob_index);
      if (!decode_status.ok()) {
        status_ = decode_status;
        validity_info_.Invalidate();
        return;
      }
      new_blob_columns.emplace_back(i, blob_idx);
    }
  }

  // If no columns were extracted, nothing to do
  if (new_blob_columns.empty()) {
    return;
  }

  // Re-serialize the entity with blob indices using the Slice-based overload
  // to avoid copying column names and values to strings.
  entity_wide_columns_.clear();
  entity_wide_columns_.reserve(entity_columns_.size());
  for (const auto& col : entity_columns_) {
    entity_wide_columns_.emplace_back(col.name(), col.value());
  }

  rewritten_entity_.clear();
  const Status serialize_status = WideColumnSerialization::SerializeV2(
      entity_wide_columns_, new_blob_columns, rewritten_entity_);
  if (!serialize_status.ok()) {
    status_ = serialize_status;
    validity_info_.Invalidate();
    return;
  }

  // Update value_ to point to the rewritten entity
  value_ = rewritten_entity_;
}

bool CompactionIterator::FetchBlobsNeedingGC(
    const std::vector<WideColumn>& /*columns*/,
    const std::vector<std::pair<size_t, BlobIndex>>& blob_columns,
    std::vector<std::pair<size_t, PinnableSlice>>* fetched_blob_values) {
  assert(fetched_blob_values != nullptr);

  for (const auto& blob_col : blob_columns) {
    const size_t col_idx = blob_col.first;
    const BlobIndex& blob_index = blob_col.second;

    // Skip inlined blobs - they don't need GC
    if (blob_index.IsInlined()) {
      continue;
    }

    // Check if this blob file needs garbage collection
    if (blob_index.file_number() >=
        blob_garbage_collection_cutoff_file_number_) {
      continue;
    }

    // This blob needs to be relocated - fetch its value
    FilePrefetchBuffer* prefetch_buffer =
        prefetch_buffers_ ? prefetch_buffers_->GetOrCreatePrefetchBuffer(
                                blob_index.file_number())
                          : nullptr;

    uint64_t bytes_read = 0;
    assert(blob_fetcher_);

    PinnableSlice blob_value;
    Status s = blob_fetcher_->FetchBlob(user_key(), blob_index, prefetch_buffer,
                                        &blob_value, &bytes_read);
    if (!s.ok()) {
      status_ = std::move(s);
      validity_info_.Invalidate();
      return false;
    }

    ++iter_stats_.num_blobs_read;
    iter_stats_.total_blob_bytes_read += bytes_read;

    fetched_blob_values->emplace_back(col_idx, std::move(blob_value));
  }

  return !fetched_blob_values->empty();
}

std::vector<std::pair<size_t, BlobIndex>>
CompactionIterator::RelocateBlobValues(
    const std::vector<std::pair<size_t, PinnableSlice>>& fetched_blob_values) {
  std::vector<std::pair<size_t, BlobIndex>> new_blob_columns;

  if (blob_file_builder_) {
    for (const auto& fv : fetched_blob_values) {
      const size_t col_idx = fv.first;
      const Slice col_value(fv.second);

      std::string temp_blob_index;
      Status status =
          blob_file_builder_->Add(user_key(), col_value, &temp_blob_index);
      if (!status.ok()) {
        status_ = status;
        validity_info_.Invalidate();
        new_blob_columns.clear();
        break;
      }

      if (!temp_blob_index.empty()) {
        BlobIndex blob_idx;
        status = blob_idx.DecodeFrom(temp_blob_index);
        if (!status.ok()) {
          status_ = status;
          validity_info_.Invalidate();
          new_blob_columns.clear();
          break;
        }
        new_blob_columns.emplace_back(col_idx, blob_idx);

        // Track relocation stats here (after successful relocation)
        ++iter_stats_.num_blobs_relocated;
        iter_stats_.total_blob_bytes_relocated += col_value.size();
      }
      // If temp_blob_index is empty, the value will be inlined
    }
  }

  return new_blob_columns;
}

void CompactionIterator::SerializeEntityAfterGC(
    const std::vector<WideColumn>& columns,
    const std::vector<std::pair<size_t, BlobIndex>>& original_blob_columns,
    const std::vector<std::pair<size_t, PinnableSlice>>& fetched_blob_values,
    const std::vector<std::pair<size_t, BlobIndex>>& new_blob_columns) {
  // Collect all blob columns for final serialization: include non-GC'd
  // originals and newly relocated. Use linear scans since these
  // collections are typically small (<5 elements).
  std::vector<std::pair<size_t, BlobIndex>> final_blob_columns;
  for (const auto& blob_col : original_blob_columns) {
    // Check if this column was fetched (GC'd) via linear scan
    bool was_fetched = false;
    for (const auto& fv : fetched_blob_values) {
      if (fv.first == blob_col.first) {
        was_fetched = true;
        break;
      }
    }
    if (!was_fetched) {
      // This blob column wasn't GC'd - keep original
      final_blob_columns.push_back(blob_col);
    }
  }
  // Add newly relocated blobs
  for (const auto& nb : new_blob_columns) {
    final_blob_columns.push_back(nb);
  }

  // Build WideColumns for serialization, replacing fetched blob values
  // with their resolved inline values.
  WideColumns wide_columns;
  wide_columns.reserve(columns.size());

  for (size_t i = 0; i < columns.size(); ++i) {
    // Linear scan to find if this column was fetched
    const PinnableSlice* fetched_value = nullptr;
    for (const auto& fv : fetched_blob_values) {
      if (fv.first == i) {
        fetched_value = &fv.second;
        break;
      }
    }

    if (fetched_value != nullptr) {
      // This blob column was fetched for GC; use the resolved inline value.
      wide_columns.emplace_back(columns[i].name(), Slice(*fetched_value));
    } else {
      // For inline columns, this is the actual value. For non-fetched blob
      // columns, this contains the raw serialized blob index, which is fine
      // because SerializeV2() re-encodes from the BlobIndex
      // object in final_blob_columns, ignoring this value for blob-type
      // columns.
      wide_columns.emplace_back(columns[i].name(), columns[i].value());
    }
  }

  // Serialize the entity
  rewritten_entity_.clear();
  Status s;
  if (final_blob_columns.empty()) {
    s = WideColumnSerialization::Serialize(wide_columns, rewritten_entity_);
  } else {
    s = WideColumnSerialization::SerializeV2(wide_columns, final_blob_columns,
                                             rewritten_entity_);
  }

  if (!s.ok()) {
    status_ = s;
    validity_info_.Invalidate();
    return;
  }

  value_ = rewritten_entity_;
}

void CompactionIterator::GarbageCollectEntityBlobsIfNeeded() {
  assert(ikey_.type == kTypeWideColumnEntity);

  if (!compaction_ || !compaction_->enable_blob_garbage_collection()) {
    return;
  }

  // This is the second branch in the flow above: the entity already has V2
  // blob references, so compaction considers only relocating the subset of
  // referenced blob files that are old enough for blob GC.

  // Use member variables already populated by PrepareOutput's combined
  // deserialization path. If not yet populated, deserialize here.
  if (entity_columns_.empty()) {
    Slice entity_slice = value_;
    entity_columns_.clear();
    entity_blob_columns_.clear();
    Status s = WideColumnSerialization::Deserialize(
        entity_slice, entity_columns_, &entity_blob_columns_);
    if (!s.ok()) {
      status_ = s;
      validity_info_.Invalidate();
      return;
    }
  }

  // The caller (PrepareOutput) already verified blob columns exist.
  assert(!entity_blob_columns_.empty());
  if (entity_blob_columns_.empty()) {
    return;
  }

  // Fetch blobs that need GC
  std::vector<std::pair<size_t, PinnableSlice>> fetched_blob_values;
  if (!FetchBlobsNeedingGC(entity_columns_, entity_blob_columns_,
                           &fetched_blob_values)) {
    // Either no blobs needed GC (status_ is OK) or an error occurred
    // (status_ is already set by FetchBlobsNeedingGC). In both cases,
    // we return without modifying the entity.
    return;
  }

  // Relocate fetched values to new blob files
  std::vector<std::pair<size_t, BlobIndex>> new_blob_columns =
      RelocateBlobValues(fetched_blob_values);
  if (!status_.ok()) {
    return;  // Error occurred during relocation
  }

  // Serialize the final entity
  SerializeEntityAfterGC(entity_columns_, entity_blob_columns_,
                         fetched_blob_values, new_blob_columns);
}

void CompactionIterator::PrepareOutput() {
  if (Valid()) {
    if (LIKELY(!is_range_del_)) {
      if (ikey_.type == kTypeValue) {
        ExtractLargeValueIfNeeded();
      } else if (ikey_.type == kTypeBlobIndex) {
        GarbageCollectBlobIfNeeded();
      } else if (ikey_.type == kTypeWideColumnEntity) {
        // If entity was already deserialized (e.g., by InvokeFilterIfNeeded)
        // and the filter returned kKeep, skip redundant re-deserialization.
        if (!entity_deserialized_) {
          TEST_SYNC_POINT(
              "CompactionIterator::PrepareOutput:DeserializeEntity");
          // Deserialize entity once into member variables, then decide between
          // blob GC and extraction based on whether blob columns exist.
          // This avoids the double parse of HasBlobColumns() + Deserialize().
          entity_columns_.clear();
          entity_blob_columns_.clear();
          Slice entity_slice = value_;
          {
            Status s_deser = WideColumnSerialization::Deserialize(
                entity_slice, entity_columns_, &entity_blob_columns_);
            if (!s_deser.ok()) {
              status_ = s_deser;
              validity_info_.Invalidate();
              return;
            }
          }
        } else {
          TEST_SYNC_POINT(
              "CompactionIterator::PrepareOutput:SkipDeserializeEntity");
        }
        entity_deserialized_ = false;  // Reset for next iteration
        if (UNLIKELY(!entity_blob_columns_.empty())) {
          GarbageCollectEntityBlobsIfNeeded();
        } else {
          ExtractLargeColumnValuesIfNeeded();
        }
      }
    }

    // Zeroing out the sequence number leads to better compression.
    // If this is the bottommost level (no files in lower levels)
    // and the earliest snapshot is larger than this seqno
    // and the userkey differs from the last userkey in compaction
    // then we can squash the seqno to zero.
    //
    // This is safe for TransactionDB write-conflict checking since transactions
    // only care about sequence number larger than any active snapshots.
    //
    // Can we do the same for levels above bottom level as long as
    // KeyNotExistsBeyondOutputLevel() return true?
    if (Valid() && bottommost_level_ &&
        DefinitelyInSnapshot(ikey_.sequence, earliest_snapshot_) &&
        ikey_.type != kTypeMerge && current_key_committed_ && !is_range_del_) {
      assert(compaction_ != nullptr && !compaction_->allow_ingest_behind());
      if (ikey_.type == kTypeDeletion ||
          (ikey_.type == kTypeSingleDeletion && timestamp_size_ == 0)) {
        ROCKS_LOG_FATAL(
            info_log_,
            "Unexpected key %s for seq-zero optimization. "
            "earliest_snapshot %" PRIu64
            ", earliest_write_conflict_snapshot %" PRIu64
            " job_snapshot %" PRIu64
            ". timestamp_size: %d full_history_ts_low_ %s. validity %x",
            ikey_.DebugString(allow_data_in_errors_, true).c_str(),
            earliest_snapshot_, earliest_write_conflict_snapshot_,
            job_snapshot_, static_cast<int>(timestamp_size_),
            full_history_ts_low_ != nullptr
                ? Slice(*full_history_ts_low_).ToString(true).c_str()
                : "null",
            validity_info_.rep);
        assert(false);
      }

      bool zeroed_seqno = false;
      // Whether the sequence number can actually be zeroed out here is decided
      // by the shared BottommostSeqnoCanBeZeroed() predicate, which the
      // bottommost-file marking logic
      // (VersionStorageInfo::ComputeBottommostFilesMarkedForCompaction) also
      // consults. Keeping both on one predicate ensures a file is only marked
      // for a compaction that can make progress; otherwise the file would be
      // re-marked forever (infinite compaction loop).
      if (BottommostSeqnoCanBeZeroed(
              ikey_.sequence, preserve_seqno_after_, timestamp_size_,
              full_history_ts_low_ != nullptr, cmp_with_history_ts_low_ < 0)) {
        if (!timestamp_size_) {
          current_key_.UpdateInternalKey(0, ikey_.type);
        } else {
          // For UDT, the seqno and timestamp could only be zeroed out after the
          // key is below history_ts_low_.
          // For the same user key (excluding timestamp), the timestamp-based
          // history can be collapsed to save some space if the timestamp is
          // older than *full_history_ts_low_.
          const std::string kTsMin(timestamp_size_, static_cast<char>(0));
          const Slice ts_slice = kTsMin;
          ikey_.SetTimestamp(ts_slice);
          current_key_.UpdateInternalKey(0, ikey_.type, &ts_slice);
        }
        zeroed_seqno = true;
      }

      if (zeroed_seqno) {
        ikey_.sequence = 0;
        last_key_seq_zeroed_ = true;
        TEST_SYNC_POINT_CALLBACK("CompactionIterator::PrepareOutput:ZeroingSeq",
                                 &ikey_);
      }
    }
  }
}

inline SequenceNumber CompactionIterator::findEarliestVisibleSnapshot(
    SequenceNumber in, SequenceNumber* prev_snapshot) {
  assert(snapshots_->size());
  if (snapshots_->size() == 0) {
    ROCKS_LOG_FATAL(info_log_,
                    "No snapshot left in findEarliestVisibleSnapshot");
  }
  auto snapshots_iter =
      std::lower_bound(snapshots_->begin(), snapshots_->end(), in);
  assert(prev_snapshot != nullptr);
  if (snapshots_iter == snapshots_->begin()) {
    *prev_snapshot = 0;
  } else {
    *prev_snapshot = *std::prev(snapshots_iter);
    if (*prev_snapshot >= in) {
      ROCKS_LOG_FATAL(info_log_,
                      "*prev_snapshot (%" PRIu64 ") >= in (%" PRIu64
                      ") in findEarliestVisibleSnapshot",
                      *prev_snapshot, in);
      assert(false);
    }
  }
  if (snapshot_checker_ == nullptr) {
    return snapshots_iter != snapshots_->end() ? *snapshots_iter
                                               : kMaxSequenceNumber;
  }
  bool has_released_snapshot = !released_snapshots_.empty();
  for (; snapshots_iter != snapshots_->end(); ++snapshots_iter) {
    auto cur = *snapshots_iter;
    if (in > cur) {
      ROCKS_LOG_FATAL(info_log_,
                      "in (%" PRIu64 ") > cur (%" PRIu64
                      ") in findEarliestVisibleSnapshot",
                      in, cur);
      assert(false);
    }
    // Skip if cur is in released_snapshots.
    if (has_released_snapshot && released_snapshots_.count(cur) > 0) {
      continue;
    }
    auto res = snapshot_checker_->CheckInSnapshot(in, cur);
    if (res == SnapshotCheckerResult::kInSnapshot) {
      return cur;
    } else if (res == SnapshotCheckerResult::kSnapshotReleased) {
      released_snapshots_.insert(cur);
    }
    *prev_snapshot = cur;
  }
  return kMaxSequenceNumber;
}

uint64_t CompactionIterator::ComputeBlobGarbageCollectionCutoffFileNumber(
    const CompactionProxy* compaction) {
  if (!compaction) {
    return 0;
  }

  if (!compaction->enable_blob_garbage_collection()) {
    return 0;
  }

  const Version* const version = compaction->input_version();
  assert(version);

  const VersionStorageInfo* const storage_info = version->storage_info();
  assert(storage_info);

  const auto& blob_files = storage_info->GetBlobFiles();

  const size_t cutoff_index = static_cast<size_t>(
      compaction->blob_garbage_collection_age_cutoff() * blob_files.size());

  if (cutoff_index >= blob_files.size()) {
    return std::numeric_limits<uint64_t>::max();
  }

  const auto& meta = blob_files[cutoff_index];
  assert(meta);

  return meta->GetBlobFileNumber();
}

std::unique_ptr<BlobFetcher> CompactionIterator::CreateBlobFetcherIfNeeded(
    const CompactionProxy* compaction, const Version* input_version,
    Env::IOActivity blob_read_io_activity) {
  const Version* const version =
      compaction != nullptr ? compaction->input_version() : input_version;
  if (version == nullptr) {
    return nullptr;
  }

  ReadOptions read_options;
  read_options.io_activity = compaction != nullptr
                                 ? Env::IOActivity::kCompaction
                                 : blob_read_io_activity;
  read_options.fill_cache = false;

  BlobFileCache* blob_file_cache = nullptr;
  bool allow_write_path_fallback = false;
  if (compaction == nullptr) {
    allow_write_path_fallback = true;
    auto* cfd = version->cfd();
    if (cfd != nullptr) {
      blob_file_cache = cfd->blob_file_cache();
    }
  }

  return std::unique_ptr<BlobFetcher>(
      new OwningVersionBlobFetcher(version, std::move(read_options),
                                   blob_file_cache, allow_write_path_fallback));
}

std::unique_ptr<PrefetchBufferCollection>
CompactionIterator::CreatePrefetchBufferCollectionIfNeeded(
    const CompactionProxy* compaction) {
  if (!compaction) {
    return nullptr;
  }

  if (!compaction->input_version()) {
    return nullptr;
  }

  if (compaction->allow_mmap_reads()) {
    return nullptr;
  }

  const uint64_t readahead_size = compaction->blob_compaction_readahead_size();
  if (!readahead_size) {
    return nullptr;
  }

  return std::unique_ptr<PrefetchBufferCollection>(
      new PrefetchBufferCollection(readahead_size));
}

}  // namespace ROCKSDB_NAMESPACE
