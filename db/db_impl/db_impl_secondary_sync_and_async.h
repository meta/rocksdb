//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/coro_utils.h"

#if defined(WITHOUT_COROUTINES) || \
    (defined(USE_COROUTINES) && defined(WITH_COROUTINES))

namespace ROCKSDB_NAMESPACE {

DEFINE_SYNC_AND_ASYNC(Status, DBImplSecondary::GetImpl)
(const ReadOptions& read_options, const Slice& key,
 GetImplOptions& get_impl_options) {
  assert(get_impl_options.value != nullptr ||
         get_impl_options.columns != nullptr ||
         get_impl_options.merge_operands != nullptr);
  assert(get_impl_options.column_family);

  if (get_impl_options.newer_version_present != nullptr) {
    if (read_options.snapshot != nullptr) {
      CO_RETURN Status::NotSupported(
          "GetWithMetadata is not supported in secondary DB mode");
    }
    get_impl_options.newer_version_present = nullptr;
  }

  Status s;

  if (read_options.timestamp) {
    s = FailIfTsMismatchCf(get_impl_options.column_family,
                           *(read_options.timestamp));
    if (!s.ok()) {
      CO_RETURN s;
    }
  } else {
    s = FailIfCfHasTs(get_impl_options.column_family);
    if (!s.ok()) {
      CO_RETURN s;
    }
  }

  // Clear the timestamps for returning results so that we can distinguish
  // between tombstone or key that has never been written
  if (get_impl_options.timestamp) {
    get_impl_options.timestamp->clear();
  }

#if defined(WITHOUT_COROUTINES)
  PERF_CPU_TIMER_GUARD(get_cpu_nanos, immutable_db_options_.clock);
#endif
  StopWatch sw(immutable_db_options_.clock, stats_, DB_GET);
  PERF_TIMER_GUARD(get_snapshot_time);

  const Comparator* ucmp = get_impl_options.column_family->GetComparator();
  assert(ucmp);
  // For a batched lazy read (MultiGetEntityLazy) use the sequence number the
  // batch fixed (see GetImplOptions::lazy_columns_shared_sv); otherwise this
  // instance's last sequence.
  SequenceNumber snapshot = get_impl_options.lazy_columns_shared_sv != nullptr
                                ? get_impl_options.lazy_columns_snapshot_seq
                                : versions_->LastSequence();
  GetWithTimestampReadCallback read_cb(snapshot);
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(
      get_impl_options.column_family);
  auto cfd = cfh->cfd();
  bool is_blob_index = false;
  bool* is_blob_ptr = get_impl_options.is_blob_index;
  std::string timestamp_storage;
  std::string* ts = nullptr;
  if (ucmp->timestamp_size() > 0) {
    ts = get_impl_options.timestamp != nullptr
             ? get_impl_options.timestamp
             : (get_impl_options.get_value ? &timestamp_storage : nullptr);
  }
  if (!is_blob_ptr && get_impl_options.get_value) {
    is_blob_ptr = &is_blob_index;
  }
  const bool resolve_blob_backed_memtable_value =
      get_impl_options.get_value && (is_blob_ptr == &is_blob_index);
  if (tracer_) {
    InstrumentedMutexLock lock(&trace_mutex_);
    if (tracer_) {
      tracer_->Get(get_impl_options.column_family, key, read_options);
    }
  }

  MergeContext merge_context;
  PrepareGetMergeOperands(get_impl_options, &merge_context);

  // Acquire SuperVersion, or use the batch's shared one for a batched lazy read
  // (see GetImplOptions::lazy_columns_shared_sv), skipping the per-key
  // reference and its release.
  const bool own_super_version =
      get_impl_options.lazy_columns_shared_sv == nullptr;
  SuperVersion* super_version;
  if (own_super_version) {
#if defined(WITH_COROUTINES)
    super_version = cfd->GetReferencedSuperVersion(this);
#else
    super_version = GetAndRefSuperVersion(cfd);
#endif
  } else {
    super_version = get_impl_options.lazy_columns_shared_sv;
  }
  if (read_options.timestamp && read_options.timestamp->size() > 0) {
    s = FailIfReadCollapsedHistory(cfd, super_version,
                                   *(read_options.timestamp));
    if (!s.ok()) {
      if (own_super_version) {
#if defined(WITH_COROUTINES)
        CleanupSuperVersion(super_version);
#else
        ReturnAndCleanupSuperVersion(cfd, super_version);
#endif
      }
      CO_RETURN s;
    }
  }
  SequenceNumber max_covering_tombstone_seq = 0;
  LookupKey lkey(key, snapshot, read_options.timestamp);
  PERF_TIMER_STOP(get_snapshot_time);
  bool done = false;
  std::optional<VersionBlobFetcher> memtable_blob_fetcher;
  if (cfd->ioptions().enable_blob_direct_write ||
      cfd->GetLatestMutableCFOptions().enable_blob_files) {
    // Catch-up can rebuild older blob references into memtables after mutable
    // blob-file settings change, so keep blob resolution available whenever
    // either blob knob indicates it may be needed.
    memtable_blob_fetcher.emplace(super_version->current, read_options,
                                  cfd->blob_file_cache(),
                                  /*allow_write_path_fallback=*/true);
  }
  const BlobFetcher* memtable_blob_fetcher_ptr =
      memtable_blob_fetcher ? &*memtable_blob_fetcher : nullptr;

  // Look up starts here
  if (get_impl_options.get_value) {
    if (super_version->mem->Get(
            lkey,
            get_impl_options.value ? get_impl_options.value->GetSelf()
                                   : nullptr,
            get_impl_options.columns, ts, &s, &merge_context,
            &max_covering_tombstone_seq, read_options,
            false /* immutable_memtable */, &read_cb, is_blob_ptr,
            /*do_merge=*/true, memtable_blob_fetcher_ptr)) {
      done = true;
      s = DBImpl::PostprocessMemtableValueRead(
          key, ts, resolve_blob_backed_memtable_value,
          memtable_blob_fetcher_ptr, get_impl_options.value,
          get_impl_options.columns, std::move(s), &is_blob_index,
          get_impl_options.value_found);
      RecordTick(stats_, MEMTABLE_HIT);
    } else if ((s.ok() || s.IsMergeInProgress()) &&
               super_version->imm->Get(
                   lkey,
                   get_impl_options.value ? get_impl_options.value->GetSelf()
                                          : nullptr,
                   get_impl_options.columns, ts, &s, &merge_context,
                   &max_covering_tombstone_seq, read_options, &read_cb,
                   is_blob_ptr, memtable_blob_fetcher_ptr)) {
      done = true;
      s = DBImpl::PostprocessMemtableValueRead(
          key, ts, resolve_blob_backed_memtable_value,
          memtable_blob_fetcher_ptr, get_impl_options.value,
          get_impl_options.columns, std::move(s), &is_blob_index,
          get_impl_options.value_found);
      RecordTick(stats_, MEMTABLE_HIT);
    }
  } else {
    // GetMergeOperands
    if (super_version->mem->Get(
            lkey,
            get_impl_options.value ? get_impl_options.value->GetSelf()
                                   : nullptr,
            get_impl_options.columns, ts, &s, &merge_context,
            &max_covering_tombstone_seq, read_options,
            false /* immutable_memtable */, &read_cb,
            /*is_blob_index=*/nullptr, /*do_merge=*/false,
            memtable_blob_fetcher_ptr)) {
      done = true;
      RecordTick(stats_, MEMTABLE_HIT);
    } else if ((s.ok() || s.IsMergeInProgress()) &&
               super_version->imm->GetMergeOperands(
                   lkey, &s, &merge_context, &max_covering_tombstone_seq,
                   read_options, memtable_blob_fetcher_ptr)) {
      done = true;
      RecordTick(stats_, MEMTABLE_HIT);
    }
  }
  if (!s.ok() && !s.IsMergeInProgress() && !s.IsNotFound()) {
    assert(done);
    if (own_super_version) {
#if defined(WITH_COROUTINES)
      CleanupSuperVersion(super_version);
#else
      ReturnAndCleanupSuperVersion(cfd, super_version);
#endif
    }
    CO_RETURN s;
  }
  // SST-backed operands borrow block memory pinned here, so this must outlive
  // FinalizeGetMergeOperands below.
  PinnedIteratorsManager pinned_iters_mgr;
  if (!done) {
    PERF_TIMER_GUARD(get_from_output_files_time);
    CO_AWAIT(super_version->current->Get, read_options, lkey,
             get_impl_options.value, get_impl_options.columns, ts, &s,
             &merge_context, &max_covering_tombstone_seq, &pinned_iters_mgr,
             /*value_found*/ nullptr,
             /*key_exists*/ nullptr, /*seq*/ nullptr, &read_cb,
             /*is_blob*/ nullptr,
             /*do_merge=*/get_impl_options.get_value,
             get_impl_options.lazy_columns_same_file_reader);
    RecordTick(stats_, MEMTABLE_MISS);
  }
  {
    PERF_TIMER_GUARD(get_post_process_time);
    if (get_impl_options.lazy_columns_version != nullptr && s.ok()) {
      // Lazy result (GetEntityLazy / MultiGetEntityLazy): hand back the Version
      // the entity's blob references must be resolved against (both the
      // single-key and batched paths).
      *get_impl_options.lazy_columns_version = super_version->current;
    }
    if (get_impl_options.lazy_columns_pin != nullptr && s.ok()) {
      // Single-key GetEntityLazy: take a SuperVersion pin for the result before
      // the borrowed reference below is released, so the result -- and deferred
      // blob reads -- stay valid after this call (as an iterator's pin does).
      // The batched path passes no pin (it uses lazy_columns_shared_sv); the
      // batch holds one shared pin per column family instead.
      TransferSuperVersionPin(super_version, get_impl_options.lazy_columns_pin);
    }
    RecordTick(stats_, NUMBER_KEYS_READ);
    size_t size = 0;
    // Mirror DBImpl::GetImpl: only produce merge-operand output and count bytes
    // read on success. A non-OK status leaves outputs cleared (see
    // PostprocessMemtableValueRead) and records no read throughput.
    if (s.ok()) {
      if (get_impl_options.value) {
        size = get_impl_options.value->size();
      } else if (get_impl_options.columns) {
        size = get_impl_options.columns->payload_size();
      } else if (get_impl_options.merge_operands) {
        s = FinalizeGetMergeOperands(get_impl_options, super_version,
                                     &merge_context, &pinned_iters_mgr, &size);
      }
      RecordTick(stats_, BYTES_READ, size);
      RecordTimeToHistogram(stats_, BYTES_PER_READ, size);
      PERF_COUNTER_ADD(get_read_bytes, size);
    }
    // Finalization may copy from the SuperVersion or transfer its ownership to
    // returned operands, so release this call's reference only afterwards.
    // Memtable operands are borrowed slices, and a concurrent
    // TryCatchUpWithPrimary can retire their memtable, leaving this reference
    // as the last one keeping it alive.
    if (own_super_version) {
#if defined(WITH_COROUTINES)
      CleanupSuperVersion(super_version);
#else
      ReturnAndCleanupSuperVersion(cfd, super_version);
#endif
    }
  }
  CO_RETURN s;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // defined(WITHOUT_COROUTINES) ||
        // (defined(USE_COROUTINES) && defined(WITH_COROUTINES))
