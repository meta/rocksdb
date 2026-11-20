//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/coro_utils.h"

#if defined(WITHOUT_COROUTINES) || \
    (defined(USE_COROUTINES) && defined(WITH_COROUTINES))

namespace ROCKSDB_NAMESPACE {

DEFINE_SYNC_AND_ASYNC(void, Version::Get)
(const ReadOptions& read_options, const LookupKey& k, PinnableSlice* value,
 PinnableWideColumns* columns, std::string* timestamp, Status* status,
 MergeContext* merge_context, SequenceNumber* max_covering_tombstone_seq,
 PinnedIteratorsManager* pinned_iters_mgr, bool* value_found, bool* key_exists,
 SequenceNumber* seq, ReadCallback* callback, bool* is_blob, bool do_merge,
 const SameFileBlobReader** lazy_columns_same_file_reader) {
  Slice ikey = k.internal_key();
  Slice user_key = k.user_key();

  assert(status->ok() || status->IsMergeInProgress());

  if (key_exists != nullptr) {
    // will falsify below if not found
    *key_exists = true;
  }

  uint64_t tracing_get_id = BlockCacheTraceHelper::kReservedGetId;
  if (vset_ && vset_->block_cache_tracer_ &&
      vset_->block_cache_tracer_->is_tracing_enabled()) {
    tracing_get_id = vset_->block_cache_tracer_->NextGetId();
  }

  // Note: the old StackableDB-based BlobDB passes in
  // GetImplOptions::is_blob_index; for the integrated BlobDB implementation, we
  // need to provide it here.
  bool is_blob_index = false;
  bool* const is_blob_to_use = is_blob ? is_blob : &is_blob_index;
  VersionBlobFetcher blob_fetcher(this, read_options);

  assert(pinned_iters_mgr);
  GetContext get_context(
      user_comparator(), merge_operator_, info_log_, db_statistics_,
      status->ok() ? GetContext::kNotFound : GetContext::kMerge, user_key,
      do_merge ? value : nullptr, do_merge ? columns : nullptr,
      do_merge ? timestamp : nullptr, value_found, merge_context, do_merge,
      max_covering_tombstone_seq, clock_, seq,
      merge_operator_ ? pinned_iters_mgr : nullptr, callback, is_blob_to_use,
      tracing_get_id, &blob_fetcher, lazy_columns_same_file_reader);

  // Pin blocks that we read to hold merge operands
  if (merge_operator_) {
    pinned_iters_mgr->StartPinning();
  }

  FilePicker fp(user_key, ikey, &storage_info_.level_files_brief_,
                storage_info_.num_non_empty_levels_,
                &storage_info_.file_indexer_, user_comparator(),
                internal_comparator());
  FdWithKeyRange* f = fp.GetNextFile();

  while (f != nullptr) {
    if (*max_covering_tombstone_seq > 0) {
      // The remaining files we look at will only contain covered keys, so we
      // stop here.
      break;
    }
    if (get_context.sample()) {
      sample_file_read_inc(f->file_metadata);
    }

#if defined(WITHOUT_COROUTINES)
    bool timer_enabled =
        GetPerfLevel() >= PerfLevel::kEnableTimeExceptForMutex &&
        get_perf_context()->per_level_perf_context_enabled;
    StopWatchNano timer(clock_, timer_enabled /* auto_start */);
#endif
    *status =
        CO_AWAIT(table_cache_->Get, read_options, *internal_comparator(),
                 *f->file_metadata, ikey, &get_context, mutable_cf_options_,
                 cfd_->internal_stats()->GetFileReadHist(fp.GetHitFileLevel()),
                 IsFilterSkipped(static_cast<int>(fp.GetHitFileLevel()),
                                 fp.IsHitFileLastInLevel()),
                 fp.GetHitFileLevel(), max_file_size_for_l0_meta_pin_);
    // TODO: examine the behavior for corrupted key
#if defined(WITHOUT_COROUTINES)
    if (timer_enabled) {
      PERF_COUNTER_BY_LEVEL_ADD(get_from_table_nanos, timer.ElapsedNanos(),
                                fp.GetHitFileLevel());
    }
#endif
    if (!status->ok()) {
      if (db_statistics_ != nullptr) {
        get_context.ReportCounters();
      }
      CO_RETURN;
    }

    // report the counters before returning
    if (get_context.State() != GetContext::kNotFound &&
        get_context.State() != GetContext::kMerge &&
        db_statistics_ != nullptr) {
      get_context.ReportCounters();
    }
    switch (get_context.State()) {
      case GetContext::kNotFound:
        // Keep searching in other files
        if (get_context.sample()) {
          sample_collapsible_entry_file_read_inc(f->file_metadata);
        }
        break;
      case GetContext::kMerge:
        // TODO: update per-level perfcontext user_key_return_count for kMerge
        if (get_context.sample()) {
          sample_collapsible_entry_file_read_inc(f->file_metadata);
        }
        break;
      case GetContext::kFound:
        if (fp.GetHitFileLevel() == 0) {
          RecordTick(db_statistics_, GET_HIT_L0);
        } else if (fp.GetHitFileLevel() == 1) {
          RecordTick(db_statistics_, GET_HIT_L1);
        } else if (fp.GetHitFileLevel() >= 2) {
          RecordTick(db_statistics_, GET_HIT_L2_AND_UP);
        }

        PERF_COUNTER_BY_LEVEL_ADD(user_key_return_count, 1,
                                  fp.GetHitFileLevel());

        if (is_blob_index && do_merge && (value || columns)) {
          Slice blob_index =
              value ? *value
                    : WideColumnsHelper::GetDefaultColumn(columns->columns());

          TEST_SYNC_POINT_CALLBACK("Version::Get::TamperWithBlobIndex",
                                   &blob_index);

          constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;

          PinnableSlice result;

          constexpr uint64_t* bytes_read = nullptr;

          *status = blob_fetcher.FetchBlob(get_context.ukey_to_get_blob_value(),
                                           blob_index, prefetch_buffer, &result,
                                           bytes_read);
          if (!status->ok()) {
            if (status->IsIncomplete()) {
              get_context.MarkKeyMayExist();
            }
            CO_RETURN;
          }

          if (value) {
            *value = std::move(result);
          } else {
            assert(columns);
            columns->SetPlainValue(std::move(result));
          }
        }

        CO_RETURN;
      case GetContext::kDeleted:
        // Use empty error message for speed
        *status = Status::NotFound();
        if (get_context.sample()) {
          sample_collapsible_entry_file_read_inc(f->file_metadata);
        }
        CO_RETURN;
      case GetContext::kCorrupt:
        *status = Status::Corruption("corrupted key for ", user_key);
        CO_RETURN;
      case GetContext::kUnexpectedBlobIndex:
        ROCKS_LOG_ERROR(info_log_, "Encounter unexpected blob index.");
        *status = Status::NotSupported(
            "Encounter unexpected blob index. Please open DB with "
            "ROCKSDB_NAMESPACE::blob_db::BlobDB instead.");
        CO_RETURN;
      case GetContext::kMergeOperatorFailed:
        *status = Status::Corruption(Status::SubCode::kMergeOperatorFailed);
        CO_RETURN;
    }
    f = fp.GetNextFile();
  }
  if (db_statistics_ != nullptr) {
    get_context.ReportCounters();
  }
  if (GetContext::kMerge == get_context.State()) {
    if (!do_merge) {
      *status = Status::OK();
      CO_RETURN;
    }
    if (!merge_operator_) {
      *status = Status::InvalidArgument(
          "merge_operator is not properly initialized.");
      CO_RETURN;
    }
    // merge_operands are in saver and we hit the beginning of the key history
    // do a final merge of nullptr and operands;
    if (value || columns) {
      // `op_failure_scope` (an output parameter) is not provided (set to
      // nullptr) since a failure must be propagated regardless of its value.
      *status = MergeHelper::TimedFullMerge(
          merge_operator_, user_key, MergeHelper::kNoBaseValue,
          merge_context->GetOperands(), info_log_, db_statistics_, clock_,
          /* update_num_ops_stats */ true, /* op_failure_scope */ nullptr,
          value ? value->GetSelf() : nullptr, columns);
      if (status->ok()) {
        if (LIKELY(value != nullptr)) {
          value->PinSelf();
        }
      }
    }
  } else {
    if (key_exists != nullptr) {
      *key_exists = false;
    }
    *status = Status::NotFound();  // Use an empty error message for speed
  }
  CO_RETURN;
}

// Lookup a batch of keys in a single SST file
DEFINE_SYNC_AND_ASYNC(Status, Version::MultiGetFromSST)
(const ReadOptions& read_options, MultiGetRange file_range, int hit_file_level,
 bool skip_filters, bool skip_range_deletions, FdWithKeyRange* f,
 std::unordered_map<uint64_t, BlobReadContexts>& blob_ctxs,
 TableCache::TypedHandle* table_handle, uint64_t& num_filter_read,
 uint64_t& num_index_read, uint64_t& num_sst_read) {
  Status s;
#if defined(WITHOUT_COROUTINES)
  bool timer_enabled = GetPerfLevel() >= PerfLevel::kEnableTimeExceptForMutex &&
                       get_perf_context()->per_level_perf_context_enabled;
  StopWatchNano timer(clock_, timer_enabled /* auto_start */);
#endif
  s = CO_AWAIT(table_cache_->MultiGet, read_options, *internal_comparator(),
               *f->file_metadata, &file_range, mutable_cf_options_,
               cfd_->internal_stats()->GetFileReadHist(hit_file_level),
               skip_filters, skip_range_deletions, hit_file_level,
               table_handle);
  // TODO: examine the behavior for corrupted key
#if defined(WITHOUT_COROUTINES)
  if (timer_enabled) {
    PERF_COUNTER_BY_LEVEL_ADD(get_from_table_nanos, timer.ElapsedNanos(),
                              hit_file_level);
  }
#endif
  if (!s.ok()) {
    // TODO: Set status for individual keys appropriately
    for (auto iter = file_range.begin(); iter != file_range.end(); ++iter) {
      *iter->s = s;
      file_range.MarkKeyDone(iter);
    }
    CO_RETURN s;
  }
  uint64_t batch_size = 0;
  for (auto iter = file_range.begin(); s.ok() && iter != file_range.end();
       ++iter) {
    GetContext& get_context = *iter->get_context;
    Status* status = iter->s;
    // The Status in the KeyContext takes precedence over GetContext state
    // Status may be an error if there were any IO errors in the table
    // reader. We never expect Status to be NotFound(), as that is
    // determined by get_context
    assert(!status->IsNotFound());
    if (!status->ok()) {
      file_range.MarkKeyDone(iter);
      continue;
    }

    if (get_context.sample()) {
      sample_file_read_inc(f->file_metadata);
      if (get_context.State() == GetContext::kNotFound ||
          get_context.State() == GetContext::kMerge ||
          get_context.State() == GetContext::kDeleted) {
        sample_collapsible_entry_file_read_inc(f->file_metadata);
      }
    }
    batch_size++;
    num_index_read += get_context.get_context_stats_.num_index_read;
    num_filter_read += get_context.get_context_stats_.num_filter_read;
    num_sst_read += get_context.get_context_stats_.num_sst_read;
    // Reset these stats since they're specific to a level
    get_context.get_context_stats_.num_index_read = 0;
    get_context.get_context_stats_.num_filter_read = 0;
    get_context.get_context_stats_.num_sst_read = 0;

    // report the counters before returning
    if (get_context.State() != GetContext::kNotFound &&
        get_context.State() != GetContext::kMerge &&
        db_statistics_ != nullptr) {
      get_context.ReportCounters();
    } else {
      if (iter->max_covering_tombstone_seq > 0) {
        // The remaining files we look at will only contain covered keys, so
        // we stop here for this key
        file_range.SkipKey(iter);
      }
    }
    switch (get_context.State()) {
      case GetContext::kNotFound:
        // Keep searching in other files
        break;
      case GetContext::kMerge:
        // TODO: update per-level perfcontext user_key_return_count for kMerge
        break;
      case GetContext::kFound:
        if (hit_file_level == 0) {
          RecordTick(db_statistics_, GET_HIT_L0);
        } else if (hit_file_level == 1) {
          RecordTick(db_statistics_, GET_HIT_L1);
        } else if (hit_file_level >= 2) {
          RecordTick(db_statistics_, GET_HIT_L2_AND_UP);
        }

        PERF_COUNTER_BY_LEVEL_ADD(user_key_return_count, 1, hit_file_level);

        file_range.MarkKeyDone(iter);

        if (iter->is_blob_index) {
          BlobIndex blob_index;
          Status tmp_s;

          if (iter->value) {
            TEST_SYNC_POINT_CALLBACK("Version::MultiGet::TamperWithBlobIndex",
                                     &(*iter));

            tmp_s = blob_index.DecodeFrom(*(iter->value));

          } else {
            assert(iter->columns);

            tmp_s = blob_index.DecodeFrom(
                WideColumnsHelper::GetDefaultColumn(iter->columns->columns()));
          }

          if (tmp_s.ok()) {
            const uint64_t blob_file_num = blob_index.file_number();
            blob_ctxs[blob_file_num].emplace_back(blob_index, &*iter);
          } else {
            *(iter->s) = tmp_s;
          }
        } else {
          if (iter->value) {
            file_range.AddValueSize(iter->value->size());
          } else {
            assert(iter->columns);
            file_range.AddValueSize(iter->columns->payload_size());
          }

          if (file_range.GetValueSize() > read_options.value_size_soft_limit) {
            s = Status::Aborted();
            break;
          }
        }
        continue;
      case GetContext::kDeleted:
        // Use empty error message for speed
        *status = Status::NotFound();
        file_range.MarkKeyDone(iter);
        continue;
      case GetContext::kCorrupt:
        *status =
            Status::Corruption("corrupted key for ", iter->lkey->user_key());
        file_range.MarkKeyDone(iter);
        continue;
      case GetContext::kUnexpectedBlobIndex:
        ROCKS_LOG_ERROR(info_log_, "Encounter unexpected blob index.");
        *status = Status::NotSupported(
            "Encounter unexpected blob index. Please open DB with "
            "ROCKSDB_NAMESPACE::blob_db::BlobDB instead.");
        file_range.MarkKeyDone(iter);
        continue;
      case GetContext::kMergeOperatorFailed:
        *status = Status::Corruption(Status::SubCode::kMergeOperatorFailed);
        file_range.MarkKeyDone(iter);
        continue;
    }
  }

  RecordInHistogram(db_statistics_, SST_BATCH_SIZE, batch_size);
  CO_RETURN s;
}

DEFINE_SYNC_AND_ASYNC(void, Version::MultiGet)
(const ReadOptions& read_options, MultiGetRange* range,
 ReadCallback* callback) {
  PinnedIteratorsManager pinned_iters_mgr;

  // Pin blocks that we read to hold merge operands
  if (merge_operator_) {
    pinned_iters_mgr.StartPinning();
  }
  uint64_t tracing_mget_id = BlockCacheTraceHelper::kReservedGetId;

  if (vset_ && vset_->block_cache_tracer_ &&
      vset_->block_cache_tracer_->is_tracing_enabled()) {
    tracing_mget_id = vset_->block_cache_tracer_->NextGetId();
  }
  // Even though we know the batch size won't be > MAX_BATCH_SIZE,
  // use autovector in order to avoid unnecessary construction of GetContext
  // objects, which is expensive
  autovector<GetContext, 16> get_ctx;
  VersionBlobFetcher blob_fetcher(this, read_options);
  for (auto iter = range->begin(); iter != range->end(); ++iter) {
    assert(iter->s->ok() || iter->s->IsMergeInProgress());
    get_ctx.emplace_back(
        user_comparator(), merge_operator_, info_log_, db_statistics_,
        iter->s->ok() ? GetContext::kNotFound : GetContext::kMerge,
        iter->ukey_with_ts, iter->value, iter->columns, iter->timestamp,
        nullptr, &(iter->merge_context), true,
        &iter->max_covering_tombstone_seq, clock_, nullptr,
        merge_operator_ ? &pinned_iters_mgr : nullptr, callback,
        &iter->is_blob_index, tracing_mget_id, &blob_fetcher);
    if (callback != nullptr && callback->GetMetadataReadBounds() != nullptr) {
      get_ctx.back().SetNewerVersionResult(&iter->newer_version_present);
    }
    // MergeInProgress status, if set, has been transferred to the get_context
    // state, so we set status to ok here. From now on, the iter status will
    // be used for IO errors, and get_context state will be used for any
    // key level errors
    *(iter->s) = Status::OK();
  }
  int get_ctx_index = 0;
  for (auto iter = range->begin(); iter != range->end();
       ++iter, get_ctx_index++) {
    iter->get_context = &(get_ctx[get_ctx_index]);
  }

  Status s;
  // blob_file => [[blob_idx, it], ...]
  std::unordered_map<uint64_t, BlobReadContexts> blob_ctxs;
  MultiGetRange keys_with_blobs_range(*range, range->begin(), range->end());
#if defined(WITHOUT_COROUTINES)
  // Posix async IO support depends on the calling thread because its io_uring
  // instances are thread-local. Check on the thread executing MultiGet rather
  // than caching the result when the Version is constructed.
  const bool use_async_io = using_coroutines() && read_options.async_io &&
                            CheckFSFeatureSupport(env_->GetFileSystem().get(),
                                                  FSSupportedOps::kAsyncIO);
#endif  // WITHOUT_COROUTINES
#if defined(WITHOUT_COROUTINES) && defined(USE_COROUTINES)
  // optimize_multiget_for_io overlaps reads across levels via blockingWait; it
  // is only used by the synchronous variant. The coroutine variant overlaps
  // within a level via co_await below.
  if (use_async_io && read_options.optimize_multiget_for_io) {
    s = MultiGetAsync(read_options, range, &blob_ctxs);
  } else
#endif  // WITHOUT_COROUTINES && USE_COROUTINES
  {
    MultiGetRange file_picker_range(*range, range->begin(), range->end());
    FilePickerMultiGet fp(&file_picker_range, &storage_info_.level_files_brief_,
                          storage_info_.num_non_empty_levels_,
                          &storage_info_.file_indexer_, user_comparator(),
                          internal_comparator());
    FdWithKeyRange* f = fp.GetNextFileInLevel();
    uint64_t num_index_read = 0;
    uint64_t num_filter_read = 0;
    uint64_t num_sst_read = 0;
    uint64_t num_level_read = 0;

    int prev_level = -1;

    while (!fp.IsSearchEnded()) {
      // This will be set to true later if we actually look up in a file in L0.
      // For per level stats purposes, an L0 file is treated as a level
      bool dump_stats_for_l0_file = false;

      // Avoid using the coroutine version if we're looking in a L0 file, since
      // L0 files won't be parallelized anyway. The regular synchronous version
      // is faster.
#ifdef WITH_COROUTINES
      if (fp.GetHitFileLevel() == 0 || !fp.RemainingOverlapInLevel()) {
#else
      if (!use_async_io || fp.GetHitFileLevel() == 0 ||
          !fp.RemainingOverlapInLevel()) {
#endif
        if (f) {
          bool skip_filters =
              IsFilterSkipped(static_cast<int>(fp.GetHitFileLevel()),
                              fp.IsHitFileLastInLevel());
          s = CO_AWAIT(MultiGetFromSST, read_options, fp.CurrentFileRange(),
                       fp.GetHitFileLevel(), skip_filters,
                       /*skip_range_deletions=*/false, f, blob_ctxs,
                       /*table_handle=*/nullptr, num_filter_read,
                       num_index_read, num_sst_read);
#ifdef WITH_COROUTINES
          // Count this per-file coroutine read. The overlap branch below
          // records the same ticker for its batch of parallel reads.
          RecordTick(db_statistics_, MULTIGET_COROUTINE_COUNT);
#endif
          if (fp.GetHitFileLevel() == 0) {
            dump_stats_for_l0_file = true;
          }
        }
        if (s.ok()) {
          f = fp.GetNextFileInLevel();
        }
#if USE_COROUTINES
      } else {
        std::vector<folly::coro::Task<Status>> mget_tasks;
        while (f != nullptr) {
          MultiGetRange file_range = fp.CurrentFileRange();
          TableCache::TypedHandle* table_handle = nullptr;
          bool skip_filters =
              IsFilterSkipped(static_cast<int>(fp.GetHitFileLevel()),
                              fp.IsHitFileLastInLevel());
          bool skip_range_deletions = false;
          if (!skip_filters) {
            Status status = table_cache_->MultiGetFilter(
                read_options, *internal_comparator(), *f->file_metadata,
                mutable_cf_options_,
                cfd_->internal_stats()->GetFileReadHist(fp.GetHitFileLevel()),
                fp.GetHitFileLevel(), &file_range, &table_handle);
            skip_range_deletions = true;
            if (status.ok()) {
              skip_filters = true;
            } else if (!status.IsNotSupported()) {
              s = status;
            }
          }

          if (!s.ok()) {
            break;
          }

          if (!file_range.empty()) {
            mget_tasks.emplace_back(MultiGetFromSSTCoroutine(
                read_options, file_range, fp.GetHitFileLevel(), skip_filters,
                skip_range_deletions, f, blob_ctxs, table_handle,
                num_filter_read, num_index_read, num_sst_read));
          }
          if (fp.KeyMaySpanNextFile()) {
            break;
          }
          f = fp.GetNextFileInLevel();
        }
        if (mget_tasks.size() > 0) {
          RecordTick(db_statistics_, MULTIGET_COROUTINE_COUNT,
                     mget_tasks.size());
          // Collect all results so far.
#ifdef WITH_COROUTINES
          std::vector<Status> statuses = co_await folly::coro::co_nothrow(
              folly::coro::collectAllRange(std::move(mget_tasks)));
#else
          std::vector<Status> statuses =
              folly::coro::blockingWait(co_withExecutor(
                  &range->context()->executor(),
                  folly::coro::collectAllRange(std::move(mget_tasks))));
#endif
          if (s.ok()) {
            for (Status stat : statuses) {
              if (!stat.ok()) {
                s = std::move(stat);
                break;
              }
            }
          }

          if (s.ok() && fp.KeyMaySpanNextFile()) {
            f = fp.GetNextFileInLevel();
          }
        }
#endif  // USE_COROUTINES
      }
      // If bad status or we found final result for all the keys
      if (!s.ok() || file_picker_range.empty()) {
        break;
      }
      if (!f) {
        // Reached the end of this level. Prepare the next level
        fp.PrepareNextLevelForSearch();
        if (!fp.IsSearchEnded()) {
          // Its possible there is no overlap on this level and f is nullptr
          f = fp.GetNextFileInLevel();
        }
        if (dump_stats_for_l0_file ||
            (prev_level != 0 && prev_level != (int)fp.GetHitFileLevel())) {
          // Dump the stats if the search has moved to the next level and
          // reset for next level.
          if (num_filter_read + num_index_read) {
            RecordInHistogram(db_statistics_,
                              NUM_INDEX_AND_FILTER_BLOCKS_READ_PER_LEVEL,
                              num_index_read + num_filter_read);
          }
          if (num_sst_read) {
            RecordInHistogram(db_statistics_, NUM_SST_READ_PER_LEVEL,
                              num_sst_read);
            num_level_read++;
          }
          num_filter_read = 0;
          num_index_read = 0;
          num_sst_read = 0;
        }
        prev_level = fp.GetHitFileLevel();
      }
    }

    // Dump stats for most recent level
    if (num_filter_read + num_index_read) {
      RecordInHistogram(db_statistics_,
                        NUM_INDEX_AND_FILTER_BLOCKS_READ_PER_LEVEL,
                        num_index_read + num_filter_read);
    }
    if (num_sst_read) {
      RecordInHistogram(db_statistics_, NUM_SST_READ_PER_LEVEL, num_sst_read);
      num_level_read++;
    }
    if (num_level_read) {
      RecordInHistogram(db_statistics_, NUM_LEVEL_READ_PER_MULTIGET,
                        num_level_read);
    }
  }

  if (!blob_ctxs.empty()) {
    MultiGetBlob(read_options, keys_with_blobs_range, blob_ctxs);
  }

  // Process any left over keys
  for (auto iter = range->begin(); s.ok() && iter != range->end(); ++iter) {
    GetContext& get_context = *iter->get_context;
    Status* status = iter->s;
    Slice user_key = iter->lkey->user_key();

    if (db_statistics_ != nullptr) {
      get_context.ReportCounters();
    }
    if (GetContext::kMerge == get_context.State()) {
      if (!merge_operator_) {
        *status = Status::InvalidArgument(
            "merge_operator is not properly initialized.");
        range->MarkKeyDone(iter);
        continue;
      }
      // merge_operands are in saver and we hit the beginning of the key history
      // do a final merge of nullptr and operands;
      // `op_failure_scope` (an output parameter) is not provided (set to
      // nullptr) since a failure must be propagated regardless of its value.
      *status = MergeHelper::TimedFullMerge(
          merge_operator_, user_key, MergeHelper::kNoBaseValue,
          iter->merge_context.GetOperands(), info_log_, db_statistics_, clock_,
          /* update_num_ops_stats */ true, /* op_failure_scope */ nullptr,
          iter->value ? iter->value->GetSelf() : nullptr, iter->columns);
      if (LIKELY(iter->value != nullptr)) {
        iter->value->PinSelf();
        range->AddValueSize(iter->value->size());
      } else {
        assert(iter->columns);
        range->AddValueSize(iter->columns->payload_size());
      }

      range->MarkKeyDone(iter);
      if (range->GetValueSize() > read_options.value_size_soft_limit) {
        s = Status::Aborted();
        break;
      }
    } else {
      range->MarkKeyDone(iter);
      *status = Status::NotFound();  // Use an empty error message for speed
    }
  }

  for (auto iter = range->begin(); iter != range->end(); ++iter) {
    range->MarkKeyDone(iter);
    *(iter->s) = s;
  }

  CO_RETURN;
}
}  // namespace ROCKSDB_NAMESPACE
#endif
