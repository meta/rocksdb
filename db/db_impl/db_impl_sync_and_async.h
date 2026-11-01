//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/coro_utils.h"

#if defined(WITHOUT_COROUTINES) || \
    (defined(USE_COROUTINES) && defined(WITH_COROUTINES))

namespace ROCKSDB_NAMESPACE {

DEFINE_SYNC_AND_ASYNC(Status, DBImpl::GetImpl)
(const ReadOptions& read_options, ColumnFamilyHandle* column_family,
 const Slice& key, PinnableSlice* value) {
  CO_RETURN CO_AWAIT(GetImpl, read_options, column_family, key, value,
                     /*timestamp=*/nullptr);
}

DEFINE_SYNC_AND_ASYNC(Status, DBImpl::Get)
(const ReadOptions& _read_options, ColumnFamilyHandle* column_family,
 const Slice& key, PinnableSlice* value, std::string* timestamp) {
  assert(value != nullptr);
  value->Reset();

  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kGet) {
    CO_RETURN Status::InvalidArgument(
        "Can only call Get with `ReadOptions::io_activity` is "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kGet`");
  }

  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kGet;
  }

  CO_RETURN CO_AWAIT(GetImpl, read_options, column_family, key, value,
                     timestamp);
}

DEFINE_SYNC_AND_ASYNC(Status, DBImpl::GetEntity)
(const ReadOptions& _read_options, ColumnFamilyHandle* column_family,
 const Slice& key, PinnableWideColumns* columns) {
  if (!column_family) {
    CO_RETURN Status::InvalidArgument(
        "Cannot call GetEntity without a column family handle");
  }
  if (!columns) {
    CO_RETURN Status::InvalidArgument(
        "Cannot call GetEntity without a PinnableWideColumns object");
  }
  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kGetEntity) {
    CO_RETURN Status::InvalidArgument(
        "Can only call GetEntity with `ReadOptions::io_activity` set to "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kGetEntity`");
  }
  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kGetEntity;
  }
  columns->Reset();

  GetImplOptions get_impl_options;
  get_impl_options.column_family = column_family;
  get_impl_options.columns = columns;

  CO_RETURN CO_AWAIT(GetImpl, read_options, key, get_impl_options);
}

DEFINE_SYNC_AND_ASYNC(Status, DBImpl::GetEntity)
(const ReadOptions& _read_options, const Slice& key,
 PinnableAttributeGroups* result) {
  if (!result) {
    CO_RETURN Status::InvalidArgument(
        "Cannot call GetEntity without PinnableAttributeGroups object");
  }
  Status s;
  const size_t num_column_families = result->size();
  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kGetEntity) {
    s = Status::InvalidArgument(
        "Can only call GetEntity with `ReadOptions::io_activity` set to "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kGetEntity`");
    for (size_t i = 0; i < num_column_families; ++i) {
      (*result)[i].SetStatus(s);
    }
    CO_RETURN s;
  }
  // return early if no CF was passed in
  if (num_column_families == 0) {
    CO_RETURN s;
  }
  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kGetEntity;
  }
  std::vector<Slice> keys;
  std::vector<ColumnFamilyHandle*> column_families;
  for (size_t i = 0; i < num_column_families; ++i) {
    // If any of the CFH is null, break early since the entire query will fail
    if (!(*result)[i].column_family()) {
      s = Status::InvalidArgument(
          "DB failed to query because one or more group(s) have null column "
          "family handle");
      (*result)[i].SetStatus(
          Status::InvalidArgument("Column family handle cannot be null"));
      break;
    }
    // Adding the same key slice for different CFs
    keys.emplace_back(key);
    column_families.emplace_back((*result)[i].column_family());
  }
  if (!s.ok()) {
    for (size_t i = 0; i < num_column_families; ++i) {
      if ((*result)[i].status().ok()) {
        (*result)[i].SetStatus(
            Status::Incomplete("DB not queried due to invalid argument(s) in "
                               "one or more of the attribute groups"));
      }
    }
    CO_RETURN s;
  }
  std::vector<PinnableWideColumns> columns(num_column_families);
  std::vector<Status> statuses(num_column_families);
  CO_AWAIT(MultiGetCommon, read_options, num_column_families,
           column_families.data(), keys.data(),
           /* values */ nullptr, columns.data(),
           /* timestamps */ nullptr, statuses.data(),
           /* newer_version_present */ nullptr,
           /* sorted_input */ false);
  // Set results
  for (size_t i = 0; i < num_column_families; ++i) {
    (*result)[i].Reset();
    (*result)[i].SetStatus(statuses[i]);
    (*result)[i].SetColumns(std::move(columns[i]));
  }
  CO_RETURN s;
}

DEFINE_SYNC_AND_ASYNC(Status, DBImpl::GetImpl)
(const ReadOptions& read_options, ColumnFamilyHandle* column_family,
 const Slice& key, PinnableSlice* value, std::string* timestamp) {
  GetImplOptions get_impl_options;
  get_impl_options.column_family = column_family;
  get_impl_options.value = value;
  get_impl_options.timestamp = timestamp;

  Status s = CO_AWAIT(GetImpl, read_options, key, get_impl_options);
  CO_RETURN s;
}

DEFINE_SYNC_AND_ASYNC(Status, DBImpl::GetImpl)
(const ReadOptions& read_options, const Slice& key,
 GetImplOptions& get_impl_options) {
  assert(get_impl_options.value != nullptr ||
         get_impl_options.merge_operands != nullptr ||
         get_impl_options.columns != nullptr);

  assert(get_impl_options.column_family);

  if (get_impl_options.newer_version_present != nullptr &&
      read_options.snapshot != nullptr &&
      get_impl_options.callback != nullptr) {
    CO_RETURN Status::NotSupported(
        "Newer-version metadata is not supported with a read callback");
  }

  if (read_options.timestamp) {
    const Status s = FailIfTsMismatchCf(get_impl_options.column_family,
                                        *(read_options.timestamp));
    if (!s.ok()) {
      CO_RETURN s;
    }
  } else {
    const Status s = FailIfCfHasTs(get_impl_options.column_family);
    if (!s.ok()) {
      CO_RETURN s;
    }
  }

  // Clear the timestamps for returning results so that we can distinguish
  // between tombstone or key that has never been written
  if (get_impl_options.timestamp) {
    get_impl_options.timestamp->clear();
  }

  GetWithTimestampReadCallback read_cb(0);  // Will call Refresh

#if defined(WITHOUT_COROUTINES)
  PERF_CPU_TIMER_GUARD(get_cpu_nanos, immutable_db_options_.clock);
#endif
  StopWatch sw(immutable_db_options_.clock, stats_, DB_GET);
  PERF_TIMER_GUARD(get_snapshot_time);

  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(
      get_impl_options.column_family);
  auto cfd = cfh->cfd();

  if (tracer_) {
    // TODO: This mutex should be removed later, to improve performance when
    // tracing is enabled.
    InstrumentedMutexLock lock(&trace_mutex_);
    if (tracer_) {
      // TODO: maybe handle the tracing status?
      tracer_->Get(get_impl_options.column_family, key, read_options)
          .PermitUncheckedError();
    }
  }

  MergeContext merge_context;
  PrepareGetMergeOperands(get_impl_options, &merge_context);

  // Acquire SuperVersion (or, for a batched lazy read, use the shared one the
  // batch already referenced -- see GetImplOptions::lazy_columns_shared_sv).
  const bool own_super_version =
      get_impl_options.lazy_columns_shared_sv == nullptr;
  SuperVersion* sv;
#if defined(WITH_COROUTINES)
  if (own_super_version) {
    sv = cfd->GetReferencedSuperVersion(this);
  } else {
    sv = get_impl_options.lazy_columns_shared_sv;
  }
  Defer sv_cleanup([&] {
    if (own_super_version) {
      CleanupSuperVersion(sv);
    }
  });
#else
  if (own_super_version) {
    sv = GetAndRefSuperVersion(cfd);
  } else {
    sv = get_impl_options.lazy_columns_shared_sv;
  }
  Defer sv_cleanup([&] {
    if (own_super_version) {
      ReturnAndCleanupSuperVersion(cfd, sv);
    }
  });
#endif
  if (read_options.timestamp && read_options.timestamp->size() > 0) {
    const Status s =
        FailIfReadCollapsedHistory(cfd, sv, *(read_options.timestamp));
    if (!s.ok()) {
      CO_RETURN s;
    }
  }

  TEST_SYNC_POINT_CALLBACK("DBImpl::GetImpl:AfterAcquireSv", nullptr);
  TEST_SYNC_POINT("DBImpl::GetImpl:1");
  TEST_SYNC_POINT("DBImpl::GetImpl:2");

  SequenceNumber snapshot;
  if (!own_super_version) {
    // Batched lazy read: use the batch's consistent sequence number (derived
    // once, with the shared SuperVersion, by MultiCFSnapshot).
    snapshot = get_impl_options.lazy_columns_snapshot_seq;
  } else if (read_options.snapshot != nullptr) {
    if (get_impl_options.callback) {
      // Already calculated based on read_options.snapshot
      snapshot = get_impl_options.callback->max_visible_seq();
    } else {
      snapshot =
          static_cast<const SnapshotImpl*>(read_options.snapshot)->number_;
    }
  } else {
    // Note that the snapshot is assigned AFTER referencing the super
    // version because otherwise a flush happening in between may compact away
    // data for the snapshot, so the reader would see neither data that was be
    // visible to the snapshot before compaction nor the newer data inserted
    // afterwards.
    snapshot = GetLastPublishedSequence();
    if (get_impl_options.callback) {
      // The unprep_seqs are not published for write unprepared, so it could be
      // that max_visible_seq is larger. Seek to the std::max of the two.
      // However, we still want our callback to contain the actual snapshot so
      // that it can do the correct visibility filtering.
      get_impl_options.callback->Refresh(snapshot);

      // Internally, WriteUnpreparedTxnReadCallback::Refresh would set
      // max_visible_seq = max(max_visible_seq, snapshot)
      //
      // Currently, the commented out assert is broken by
      // InvalidSnapshotReadCallback, but if write unprepared recovery followed
      // the regular transaction flow, then this special read callback would not
      // be needed.
      //
      // assert(callback->max_visible_seq() >= snapshot);
      snapshot = get_impl_options.callback->max_visible_seq();
    }
  }
  SequenceNumber lookup_snapshot = snapshot;
  const bool newer_version_present_requested =
      get_impl_options.newer_version_present != nullptr &&
      read_options.snapshot != nullptr;
  const SequenceNumber newer_version_upper_bound_seq =
      newer_version_present_requested ? GetLastPublishedSequence() : snapshot;
  const bool track_newer_versions = newer_version_present_requested &&
                                    snapshot < newer_version_upper_bound_seq;
  if (track_newer_versions) {
    lookup_snapshot = newer_version_upper_bound_seq;
  }
  // If timestamp is used, we use read callback to ensure <key,t,s> is returned
  // only if t <= read_opts.timestamp and s <= snapshot.
  // HACK: temporarily overwrite input struct field but restore
  SaveAndRestore<ReadCallback*> restore_callback(&get_impl_options.callback);
  const Comparator* ucmp = get_impl_options.column_family->GetComparator();
  assert(ucmp);
  if (ucmp->timestamp_size() > 0) {
    assert(!get_impl_options
                .callback);  // timestamp with callback is not supported
    read_cb.Refresh(snapshot);
    get_impl_options.callback = &read_cb;
  } else if (track_newer_versions && get_impl_options.callback == nullptr) {
    read_cb.Refresh(snapshot);
    get_impl_options.callback = &read_cb;
  }
  if (track_newer_versions) {
    read_cb.EnableNewerVersionTracking(snapshot, newer_version_upper_bound_seq,
                                       get_impl_options.newer_version_present);
  }
  TEST_SYNC_POINT("DBImpl::GetImpl:3");
  TEST_SYNC_POINT("DBImpl::GetImpl:4");

  SequenceNumber max_covering_tombstone_seq = 0;

  Status s;
  // First look in the memtable, then in the immutable memtable (if any).
  // s is both in/out. When in, s could either be OK or MergeInProgress.
  // merge_operands will contain the sequence of merges in the latter case.
  LookupKey lkey(key, lookup_snapshot, read_options.timestamp);
  PERF_TIMER_STOP(get_snapshot_time);

  bool skip_memtable = (read_options.read_tier == kPersistedTier &&
                        has_unpersisted_data_.load(std::memory_order_relaxed));
  bool done = false;
  bool is_blob_index = false;
  bool* is_blob_ptr = get_impl_options.is_blob_index;
  auto* partition_mgr = cfd->blob_partition_manager();
  std::string timestamp_storage;
  std::string* timestamp = nullptr;
  if (ucmp->timestamp_size() > 0) {
    timestamp = get_impl_options.timestamp != nullptr
                    ? get_impl_options.timestamp
                    : (partition_mgr != nullptr ? &timestamp_storage : nullptr);
  }
  if (partition_mgr != nullptr && !is_blob_ptr && get_impl_options.get_value) {
    is_blob_ptr = &is_blob_index;
  }
  const bool resolve_direct_write_value =
      partition_mgr != nullptr && (is_blob_ptr == &is_blob_index);
  std::optional<VersionBlobFetcher> memtable_blob_fetcher;
  if (partition_mgr != nullptr) {
    memtable_blob_fetcher.emplace(sv->current, read_options,
                                  cfd->blob_file_cache(),
                                  /*allow_write_path_fallback=*/true);
  }
  const BlobFetcher* memtable_blob_fetcher_ptr =
      memtable_blob_fetcher ? &*memtable_blob_fetcher : nullptr;
  if (!skip_memtable) {
    // Get value associated with key
    if (get_impl_options.get_value) {
      if (sv->mem->Get(lkey,
                       get_impl_options.value
                           ? get_impl_options.value->GetSelf()
                           : nullptr,
                       get_impl_options.columns, timestamp, &s, &merge_context,
                       &max_covering_tombstone_seq, read_options,
                       false /* immutable_memtable */,
                       get_impl_options.callback, is_blob_ptr,
                       /*do_merge=*/true, memtable_blob_fetcher_ptr)) {
        done = true;
        PostprocessDirectWriteValueRead(
            read_options, key, timestamp, resolve_direct_write_value,
            memtable_blob_fetcher_ptr, get_impl_options.value,
            get_impl_options.columns, &s, &is_blob_index,
            get_impl_options.value_found);

        RecordTick(stats_, MEMTABLE_HIT);
      } else if ((s.ok() || s.IsMergeInProgress()) &&
                 sv->imm->Get(lkey,
                              get_impl_options.value
                                  ? get_impl_options.value->GetSelf()
                                  : nullptr,
                              get_impl_options.columns, timestamp, &s,
                              &merge_context, &max_covering_tombstone_seq,
                              read_options, get_impl_options.callback,
                              is_blob_ptr, memtable_blob_fetcher_ptr)) {
        done = true;
        PostprocessDirectWriteValueRead(
            read_options, key, timestamp, resolve_direct_write_value,
            memtable_blob_fetcher_ptr, get_impl_options.value,
            get_impl_options.columns, &s, &is_blob_index,
            get_impl_options.value_found);

        RecordTick(stats_, MEMTABLE_HIT);
      }
    } else {
      // Get Merge Operands associated with key, Merge Operands should not be
      // merged and raw values should be returned to the user.
      if (sv->mem->Get(lkey, /*value=*/nullptr, /*columns=*/nullptr,
                       /*timestamp=*/nullptr, &s, &merge_context,
                       &max_covering_tombstone_seq, read_options,
                       false /* immutable_memtable */, nullptr, nullptr, false,
                       memtable_blob_fetcher_ptr)) {
        done = true;
        RecordTick(stats_, MEMTABLE_HIT);
      } else if ((s.ok() || s.IsMergeInProgress()) &&
                 sv->imm->GetMergeOperands(
                     lkey, &s, &merge_context, &max_covering_tombstone_seq,
                     read_options, memtable_blob_fetcher_ptr)) {
        done = true;
        RecordTick(stats_, MEMTABLE_HIT);
      }
    }
    if (!s.ok() && !s.IsMergeInProgress() && !s.IsNotFound()) {
      assert(done);
      CO_RETURN s;
    }
  }
  TEST_SYNC_POINT("DBImpl::GetImpl:PostMemTableGet:0");
  TEST_SYNC_POINT("DBImpl::GetImpl:PostMemTableGet:1");
  PinnedIteratorsManager pinned_iters_mgr;
  if (!done) {
    PERF_TIMER_GUARD(get_from_output_files_time);
    CO_AWAIT(
        sv->current->Get, read_options, lkey, get_impl_options.value,
        get_impl_options.columns, timestamp, &s, &merge_context,
        &max_covering_tombstone_seq, &pinned_iters_mgr,
        get_impl_options.get_value ? get_impl_options.value_found : nullptr,
        nullptr, nullptr,
        get_impl_options.get_value ? get_impl_options.callback : nullptr,
        get_impl_options.get_value ? is_blob_ptr : nullptr,
        get_impl_options.get_value,
        get_impl_options.lazy_columns_same_file_reader);
    if (get_impl_options.get_value && resolve_direct_write_value) {
      assert(memtable_blob_fetcher_ptr != nullptr);
      std::string blob_lookup_key_storage;
      MaybeResolveDirectWriteValue(
          read_options,
          GetBlobLookupUserKey(key, timestamp, &blob_lookup_key_storage),
          resolve_direct_write_value, *memtable_blob_fetcher_ptr,
          get_impl_options.value, get_impl_options.columns, &s, &is_blob_index,
          get_impl_options.value_found);
    }
    RecordTick(stats_, MEMTABLE_MISS);
  }

  {
    PERF_TIMER_GUARD(get_post_process_time);

    RecordTick(stats_, NUMBER_KEYS_READ);
    size_t size = 0;
    if (s.ok()) {
      const auto& merge_threshold = read_options.merge_operand_count_threshold;
      if (merge_threshold.has_value() &&
          merge_context.GetNumOperands() > merge_threshold.value()) {
        s = Status::OkMergeOperandThresholdExceeded();
      }

      if (get_impl_options.get_value) {
        if (get_impl_options.value) {
          size = get_impl_options.value->size();
        } else if (get_impl_options.columns) {
          size = get_impl_options.columns->payload_size();
        }
      } else {
        // Keep an OK subcode set above, such as
        // OkMergeOperandThresholdExceeded, unless finalization fails.
        Status finalize_status = FinalizeGetMergeOperands(
            get_impl_options, sv, &merge_context, &pinned_iters_mgr, &size);
        if (!finalize_status.ok()) {
          s = std::move(finalize_status);
        }
      }
      RecordTick(stats_, BYTES_READ, size);
      PERF_COUNTER_ADD(get_read_bytes, size);
    }

    if (get_impl_options.lazy_columns_version != nullptr && s.ok()) {
      // Lazy result (GetEntityLazy / MultiGetEntityLazy): the entity's blob
      // references were left unresolved. Hand back the Version they must be
      // resolved against (for both the single-key and batched paths).
      *get_impl_options.lazy_columns_version = sv->current;
    }
    if (get_impl_options.lazy_columns_pin != nullptr && s.ok()) {
      // Single-key GetEntityLazy: transfer an extra SuperVersion reference into
      // the result's pin, so the result -- and thus deferred blob reads -- stay
      // valid after this call returns (as an iterator's SuperVersion pin does).
      // The reference is released when the pin's cleanup runs (result
      // destruction); the borrowed reference for this call is dropped by the
      // sv_cleanup Defer. The batched path passes no pin here (it uses
      // lazy_columns_shared_sv) because the batch holds one shared pin per
      // column family instead.
      TransferSuperVersionPin(sv, get_impl_options.lazy_columns_pin);
    }
    RecordInHistogram(stats_, BYTES_PER_READ, size);
  }
  CO_RETURN s;
}

// The actual implementation of batched MultiGet. Parameters -
// start_key - Index in the sorted_keys vector to start processing from
// num_keys - Number of keys to lookup, starting with sorted_keys[start_key]
// sorted_keys - The entire batch of sorted keys for this CF
//
// The per key status is returned in the KeyContext structures pointed to by
// sorted_keys. An overall Status is also returned, with the only possible
// values being Status::OK() and Status::TimedOut(). The latter indicates
// that the call exceeded read_options.deadline
DEFINE_SYNC_AND_ASYNC(Status, DBImpl::MultiGetImpl)
(const ReadOptions& read_options, size_t start_key, size_t num_keys,
 autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE>* sorted_keys,
 SuperVersion* super_version, SequenceNumber snapshot, ReadCallback* callback) {
#if defined(WITHOUT_COROUTINES)
  PERF_CPU_TIMER_GUARD(get_cpu_nanos, immutable_db_options_.clock);
#endif  // defined(WITHOUT_COROUTINES)
  StopWatch sw(immutable_db_options_.clock, stats_, DB_MULTIGET);

  assert(sorted_keys);
  assert(start_key + num_keys <= sorted_keys->size());
  if (num_keys == 0) {
    CO_RETURN Status::OK();
  }
  auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(
      (*sorted_keys)[start_key]->column_family);
  ColumnFamilyData* cfd = cfh->cfd();
  auto* partition_mgr = cfd->blob_partition_manager();
  std::optional<VersionBlobFetcher> memtable_blob_fetcher;
  if (partition_mgr != nullptr) {
    memtable_blob_fetcher.emplace(super_version->current, read_options,
                                  cfd->blob_file_cache(),
                                  /*allow_write_path_fallback=*/true);
  }
  const BlobFetcher* memtable_blob_fetcher_ptr =
      memtable_blob_fetcher ? &*memtable_blob_fetcher : nullptr;

#ifdef WITH_COROUTINES
  constexpr bool kUseCoroRead = true;
#else
  constexpr bool kUseCoroRead = false;
#endif

  // Clear the timestamps for returning results so that we can distinguish
  // between tombstone or key that has never been written
  for (size_t i = start_key; i < start_key + num_keys; ++i) {
    KeyContext* kctx = (*sorted_keys)[i];
    if (kctx->timestamp) {
      kctx->timestamp->clear();
    }
  }

  // For each of the given keys, apply the entire "get" process as follows:
  // First look in the memtable, then in the immutable memtable (if any).
  // s is both in/out. When in, s could either be OK or MergeInProgress.
  // merge_operands will contain the sequence of merges in the latter case.
  size_t keys_left = num_keys;
  Status s;
  uint64_t curr_value_size = 0;
  while (keys_left) {
    if (read_options.deadline.count() &&
        immutable_db_options_.clock->NowMicros() >
            static_cast<uint64_t>(read_options.deadline.count())) {
      s = Status::TimedOut();
      break;
    }

    size_t batch_size = (keys_left > MultiGetContext::MAX_BATCH_SIZE)
                            ? MultiGetContext::MAX_BATCH_SIZE
                            : keys_left;
    MultiGetContext ctx(sorted_keys, start_key + num_keys - keys_left,
                        batch_size, snapshot, read_options, GetFileSystem(),
                        stats_, kUseCoroRead);
    MultiGetRange range = ctx.GetMultiGetRange();
    range.AddValueSize(curr_value_size);
    bool lookup_current = true;

    keys_left -= batch_size;
    for (auto mget_iter = range.begin(); mget_iter != range.end();
         ++mget_iter) {
      mget_iter->merge_context.Clear();
      *mget_iter->s = Status::OK();
    }

    bool skip_memtable =
        (read_options.read_tier == kPersistedTier &&
         has_unpersisted_data_.load(std::memory_order_relaxed));
    if (!skip_memtable) {
      super_version->mem->MultiGet(read_options, &range, callback,
                                   false /* immutable_memtable */,
                                   memtable_blob_fetcher_ptr);
      if (!range.empty()) {
        super_version->imm->MultiGet(read_options, &range, callback,
                                     memtable_blob_fetcher_ptr);
      }
      if (!range.empty()) {
        uint64_t left = range.KeysLeft();
        RecordTick(stats_, MEMTABLE_MISS, left);
      } else {
        lookup_current = false;
      }
    }
    if (lookup_current) {
      PERF_TIMER_GUARD(get_from_output_files_time);
      CO_AWAIT(super_version->current->MultiGet, read_options, &range,
               callback);
    }
    curr_value_size = range.GetValueSize();
    if (curr_value_size > read_options.value_size_soft_limit) {
      s = Status::Aborted();
      break;
    }

    // This could be a long-running operation
    bool aborted = ROCKSDB_THREAD_YIELD_CHECK_ABORT();
    if (aborted) {
      s = Status::Aborted("Query abort.");
      break;
    }
  }

  // Post processing (decrement reference counts and record statistics)
  PERF_TIMER_GUARD(get_post_process_time);
  size_t num_found = 0;
  uint64_t bytes_read = 0;
  // value_size_soft_limit was enforced above (and inside per-file MultiGet) on
  // pre-resolution sizes. Direct-write blob references, though, are resolved
  // below -- after that check counted only their encoded blob-index bytes.
  // Enforce the limit on them here too, but only abort a key once the bytes
  // already returned by this MultiGet exceed the limit. That way at least one
  // key is always read even if it alone exceeds the limit ("always make
  // progress"), so a caller retrying aborted keys cannot loop forever on a
  // single oversized value.
  const bool enforce_direct_write_soft_limit =
      partition_mgr != nullptr &&
      read_options.value_size_soft_limit !=
          std::numeric_limits<uint64_t>::max() &&
      read_options.read_tier != kBlockCacheTier;
  for (size_t i = start_key; i < start_key + num_keys - keys_left; ++i) {
    KeyContext* key = (*sorted_keys)[i];
    assert(key);
    assert(key->s);

    if (partition_mgr != nullptr && key->s->ok()) {
      const bool has_unresolved_direct_write =
          key->is_blob_index ||
          (key->columns != nullptr &&
           !PinnableWideColumnsHelper::GetUnresolvedBlobColumnIndices(
                *key->columns)
                .empty());
      if (enforce_direct_write_soft_limit && has_unresolved_direct_write &&
          bytes_read > read_options.value_size_soft_limit) {
        // Prior keys in this MultiGet have already returned more than the soft
        // limit, so skip fetching this key's (possibly large) blob value(s).
        if (key->value != nullptr) {
          key->value->Reset();
        }
        if (key->columns != nullptr) {
          key->columns->Reset();
        }
        key->is_blob_index = false;
        *key->s = Status::Aborted();
      } else {
        assert(memtable_blob_fetcher_ptr != nullptr);
        std::string blob_lookup_key_storage;
        MaybeResolveDirectWriteValue(
            read_options,
            GetBlobLookupUserKey(*key->key, key->timestamp,
                                 &blob_lookup_key_storage),
            /*resolve_direct_write_value=*/true, *memtable_blob_fetcher_ptr,
            key->value, key->columns, key->s, &key->is_blob_index);
      }
    }

    if (key->s->ok()) {
      const auto& merge_threshold = read_options.merge_operand_count_threshold;
      if (merge_threshold.has_value() &&
          key->merge_context.GetNumOperands() > merge_threshold) {
        *(key->s) = Status::OkMergeOperandThresholdExceeded();
      }

      if (key->value) {
        bytes_read += key->value->size();
      } else {
        assert(key->columns);
        bytes_read += key->columns->payload_size();
      }

      num_found++;
    }
  }
  if (keys_left) {
    assert(s.IsTimedOut() || s.IsAborted());
    for (size_t i = start_key + num_keys - keys_left; i < start_key + num_keys;
         ++i) {
      KeyContext* key = (*sorted_keys)[i];
      *key->s = s;
    }
  }

  RecordTick(stats_, NUMBER_MULTIGET_CALLS);
  RecordTick(stats_, NUMBER_MULTIGET_KEYS_READ, num_keys);
  RecordTick(stats_, NUMBER_MULTIGET_KEYS_FOUND, num_found);
  RecordTick(stats_, NUMBER_MULTIGET_BYTES_READ, bytes_read);
  RecordInHistogram(stats_, BYTES_PER_MULTIGET, bytes_read);
  PERF_COUNTER_ADD(multiget_read_bytes, bytes_read);
  PERF_TIMER_STOP(get_post_process_time);

  CO_RETURN s;
}

DEFINE_SYNC_AND_ASYNC(void, DBImpl::MultiGetCommon)
(const ReadOptions& read_options, const size_t num_keys,
 ColumnFamilyHandle** column_families, const Slice* keys, PinnableSlice* values,
 PinnableWideColumns* columns, std::string* timestamps, Status* statuses,
 std::vector<uint8_t>* newer_version_present, const bool sorted_input) {
  if (num_keys == 0) {
    CO_RETURN;
  }
  bool should_fail = false;
  for (size_t i = 0; i < num_keys; ++i) {
    ColumnFamilyHandle* cfh = column_families[i];
    if (read_options.timestamp) {
      statuses[i] = FailIfTsMismatchCf(cfh, *(read_options.timestamp));
      if (!statuses[i].ok()) {
        should_fail = true;
      }
    } else {
      statuses[i] = FailIfCfHasTs(cfh);
      if (!statuses[i].ok()) {
        should_fail = true;
      }
    }
  }
  if (should_fail) {
    for (size_t i = 0; i < num_keys; ++i) {
      if (statuses[i].ok()) {
        statuses[i] = Status::Incomplete(
            "DB not queried due to invalid argument(s) in the same MultiGet");
      }
    }
    CO_RETURN;
  }

  if (tracer_) {
    // TODO: This mutex should be removed later, to improve performance when
    // tracing is enabled.
    InstrumentedMutexLock lock(&trace_mutex_);
    if (tracer_) {
      // TODO: maybe handle the tracing status?
      tracer_->MultiGet(num_keys, column_families, keys, read_options)
          .PermitUncheckedError();
    }
  }

  autovector<KeyContext, MultiGetContext::MAX_BATCH_SIZE> key_context;
  key_context.reserve(num_keys);
  autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE> sorted_keys;
  sorted_keys.resize(num_keys);
  for (size_t i = 0; i < num_keys; ++i) {
    PinnableSlice* val = nullptr;
    PinnableWideColumns* col = nullptr;

    if (values) {
      val = &values[i];
      val->Reset();
    } else {
      assert(columns);

      col = &columns[i];
      col->Reset();
    }

    key_context.emplace_back(column_families[i], keys[i], val, col,
                             timestamps ? &timestamps[i] : nullptr,
                             &statuses[i]);
  }
  for (size_t i = 0; i < num_keys; ++i) {
    sorted_keys[i] = &key_context[i];
  }
  PrepareMultiGetKeys(num_keys, sorted_input, &sorted_keys);

  autovector<MultiGetKeyRangePerCf, MultiGetContext::MAX_BATCH_SIZE>
      key_range_per_cf;
  autovector<ColumnFamilySuperVersionPair, MultiGetContext::MAX_BATCH_SIZE>
      cf_sv_pairs;
  size_t cf_start = 0;
  ColumnFamilyHandle* cf = sorted_keys[0]->column_family;

  for (size_t i = 0; i < num_keys; ++i) {
    KeyContext* key_ctx = sorted_keys[i];
    if (key_ctx->column_family != cf) {
      key_range_per_cf.emplace_back(cf_start, i - cf_start);
      cf_sv_pairs.emplace_back(cf, nullptr);
      cf_start = i;
      cf = key_ctx->column_family;
    }
  }

  key_range_per_cf.emplace_back(cf_start, num_keys - cf_start);
  cf_sv_pairs.emplace_back(cf, nullptr);

  SequenceNumber consistent_seqnum = kMaxSequenceNumber;
  bool sv_from_thread_local = false;
#ifdef WITH_COROUTINES
  // The coroutine read path may suspend mid-MultiGet and resume on a shared
  // executor thread, which is incompatible with the thread-local SuperVersion
  // cache: it marks the thread's slot kSVInUse for the duration of the read, so
  // a second coroutine resuming on the same thread would trip the kSVInUse
  // assertion. Take an independent reference instead, which releases the
  // thread-local slot before any suspension point.
  constexpr bool kExtraSvRef = true;
#else
  constexpr bool kExtraSvRef = false;
#endif
  Status s = MultiCFSnapshot<autovector<ColumnFamilySuperVersionPair,
                                        MultiGetContext::MAX_BATCH_SIZE>>(
      read_options, nullptr,
      [](autovector<ColumnFamilySuperVersionPair,
                    MultiGetContext::MAX_BATCH_SIZE>::iterator& cf_iter) {
        return &(*cf_iter);
      },
      &cf_sv_pairs, kExtraSvRef, &consistent_seqnum, &sv_from_thread_local);

  if (!s.ok()) {
    for (size_t i = 0; i < num_keys; ++i) {
      if (statuses[i].ok()) {
        statuses[i] = s;
      }
    }
    CO_RETURN;
  }

  GetWithTimestampReadCallback timestamp_read_callback(0);
  ReadCallback* read_callback = nullptr;
  SequenceNumber lookup_seqnum = consistent_seqnum;
  const bool newer_version_present_requested =
      newer_version_present != nullptr && read_options.snapshot != nullptr;
  const SequenceNumber newer_version_upper_bound_seq =
      newer_version_present_requested ? GetLastPublishedSequence()
                                      : consistent_seqnum;
  const bool track_newer_versions =
      newer_version_present_requested &&
      consistent_seqnum < newer_version_upper_bound_seq;
  if (track_newer_versions) {
    lookup_seqnum = newer_version_upper_bound_seq;
  }
  if ((read_options.timestamp && read_options.timestamp->size() > 0) ||
      track_newer_versions) {
    timestamp_read_callback.Refresh(consistent_seqnum);
    if (track_newer_versions) {
      timestamp_read_callback.EnableNewerVersionTracking(
          consistent_seqnum, newer_version_upper_bound_seq,
          /*single_key_result=*/nullptr);
    }
    read_callback = &timestamp_read_callback;
  }

  assert(key_range_per_cf.size() == cf_sv_pairs.size());
  auto key_range_per_cf_iter = key_range_per_cf.begin();
  auto cf_sv_pair_iter = cf_sv_pairs.begin();
  while (key_range_per_cf_iter != key_range_per_cf.end() &&
         cf_sv_pair_iter != cf_sv_pairs.end()) {
    s = CO_AWAIT(MultiGetImpl, read_options, key_range_per_cf_iter->start,
                 key_range_per_cf_iter->num_keys, &sorted_keys,
                 cf_sv_pair_iter->super_version, lookup_seqnum, read_callback);
    if (!s.ok()) {
      break;
    }
    ++key_range_per_cf_iter;
    ++cf_sv_pair_iter;
  }
  if (!s.ok()) {
    assert(s.IsTimedOut() || s.IsAborted());
    for (++key_range_per_cf_iter;
         key_range_per_cf_iter != key_range_per_cf.end();
         ++key_range_per_cf_iter) {
      for (size_t i = key_range_per_cf_iter->start;
           i < key_range_per_cf_iter->start + key_range_per_cf_iter->num_keys;
           ++i) {
        *sorted_keys[i]->s = s;
      }
    }
  }

  for (const auto& cf_sv_pair : cf_sv_pairs) {
    if (sv_from_thread_local && !kExtraSvRef) {
      ReturnAndCleanupSuperVersion(cf_sv_pair.cfd, cf_sv_pair.super_version);
    } else {
      TEST_SYNC_POINT("DBImpl::MultiCFSnapshot::BeforeLastTryUnRefSV");
      CleanupSuperVersion(cf_sv_pair.super_version);
    }
  }
  CopyNewerVersionPresent(key_context, newer_version_present);
}

DEFINE_SYNC_AND_ASYNC(void, DBImpl::MultiGet)
(const ReadOptions& _read_options, const size_t num_keys,
 ColumnFamilyHandle** column_families, const Slice* keys, PinnableSlice* values,
 std::string* timestamps, Status* statuses, const bool sorted_input) {
  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kMultiGet) {
    Status s = Status::InvalidArgument(
        "Can only call MultiGet with `ReadOptions::io_activity` is "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kMultiGet`");
    for (size_t i = 0; i < num_keys; ++i) {
      if (statuses[i].ok()) {
        statuses[i] = s;
      }
    }
    CO_RETURN;
  }
  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kMultiGet;
  }
  CO_AWAIT(MultiGetCommon, read_options, num_keys, column_families, keys,
           values,
           /* columns */ nullptr, timestamps, statuses,
           /* newer_version_present */ nullptr, sorted_input);
  CO_RETURN;
}

}  // namespace ROCKSDB_NAMESPACE

#endif
