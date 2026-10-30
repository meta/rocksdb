//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/db_impl/db_impl.h"

#include <algorithm>
#include <array>
#include <cstdint>

#include "db/db_impl/db_impl_metadata.h"

#include <cinttypes>
#include <cstdio>
#include <deque>
#include <forward_list>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "db/arena_wrapped_db_iter.h"
#include "db/attribute_group_iterator_impl.h"
#include "db/blob/blob_fetcher.h"
#include "db/blob/blob_file_partition_manager.h"
#include "db/blob/blob_index.h"
#include "db/builder.h"
#include "db/coalescing_iterator.h"
#include "db/compaction/compaction_job.h"
#include "db/convenience_impl.h"
#include "db/db_info_dumper.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/error_handler.h"
#include "db/event_helpers.h"
#include "db/external_sst_file_ingestion_job.h"
#include "db/flush_job.h"
#include "db/forward_iterator.h"
#include "db/import_column_family_job.h"
#include "db/job_context.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/malloc_stats.h"
#include "db/memtable.h"
#include "db/memtable_list.h"
#include "db/merge_context.h"
#include "db/periodic_task_scheduler.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/table_cache.h"
#include "db/table_properties_collector.h"
#include "db/version_set.h"
#include "db/wal_iterator_impl.h"
#include "db/wide/lazy_wide_columns_helper.h"
#include "db/wide/wide_column_serialization.h"
#include "db/wide/wide_columns_helper.h"
#include "db/write_batch_internal.h"
#include "db/write_callback.h"
#include "env/unique_id_gen.h"
#include "file/delete_scheduler.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "file/sst_file_manager_impl.h"
#include "logging/auto_roll_logger.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"
#include "monitoring/in_memory_stats_history.h"
#include "monitoring/instrumented_mutex.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/persistent_stats_history.h"
#include "monitoring/thread_status_updater.h"
#include "monitoring/thread_status_util.h"
#include "options/cf_options.h"
#include "options/options_helper.h"
#include "options/options_parser.h"
#include "rocksdb/lazy_wide_columns.h"
#ifdef ROCKSDB_JEMALLOC
#include "port/jemalloc_helper.h"
#endif
#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/statistics.h"
#include "rocksdb/stats_history.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/version.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/get_context.h"
#include "table/merging_iterator.h"
#include "table/multiget_context.h"
#include "table/prepared_file_info.h"
#include "table/sst_file_dumper.h"
#include "table/table_builder.h"
#include "table/two_level_iterator.h"
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "trace_replay/trace_replay.h"
#include "util/autovector.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/coro_stats_util.h"
#include "util/crc32c.h"
#include "util/defer.h"
#include "util/distributed_mutex.h"
#include "util/hash_containers.h"
#include "util/mutexlock.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/udt_util.h"
#include "utilities/trace/replayer_impl.h"

#if USE_COROUTINES
#include "folly/Executor.h"
#include "folly/coro/Nothrow.h"
#include "folly/coro/Task.h"
#include "folly/executors/IOExecutor.h"
#include "folly/io/async/EventBase.h"
#include "rocksdb/coro_db.h"
#endif  // USE_COROUTINES

namespace ROCKSDB_NAMESPACE {

const std::string kDefaultColumnFamilyName("default");
const std::string kPersistentStatsColumnFamilyName(
    "___rocksdb_stats_history___");
void DumpRocksDBBuildVersion(Logger* log);

CompressionType GetCompressionFlush(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options) {
  // Compressing memtable flushes might not help unless the sequential load
  // optimization is used for leveled compaction. Otherwise the CPU and
  // latency overhead is not offset by saving much space.
  if (ioptions.compaction_style == kCompactionStyleUniversal &&
      mutable_cf_options.compaction_options_universal
              .compression_size_percent >= 0) {
    return kNoCompression;
  }
  if (mutable_cf_options.compression_per_level.empty()) {
    return mutable_cf_options.compression;
  } else {
    // For leveled compress when min_level_to_compress != 0.
    return mutable_cf_options.compression_per_level[0];
  }
}

namespace {
void DumpSupportInfo(Logger* logger) {
  ROCKS_LOG_HEADER(logger, "Compression algorithms supported:");
  for (auto& compression : OptionsHelper::compression_type_string_map) {
    if (compression.second != kNoCompression &&
        compression.second != kDisableCompressionOption) {
      ROCKS_LOG_HEADER(logger, "\t%s supported: %d", compression.first.c_str(),
                       CompressionTypeSupported(compression.second));
    }
  }
  ROCKS_LOG_HEADER(logger, "Fast CRC32 supported: %s",
                   crc32c::IsFastCrc32Supported().c_str());

  ROCKS_LOG_HEADER(logger, "DMutex implementation: %s", DMutex::kName());

  bool jemalloc_supported = false;
#ifdef ROCKSDB_JEMALLOC
  jemalloc_supported = HasJemalloc();
#endif
  ROCKS_LOG_HEADER(logger, "Jemalloc supported: %d", jemalloc_supported);
}
}  // namespace

DBImpl::DBImpl(const DBOptions& options, const std::string& dbname,
               const bool seq_per_batch, const bool batch_per_txn,
               bool read_only)
    : dbname_(dbname),
      read_only_(read_only),
      own_info_log_(options.info_log == nullptr),
      initial_db_options_(SanitizeOptions(dbname, options, read_only,
                                          &init_logger_creation_s_)),
      env_(initial_db_options_.env),
      io_tracer_(std::make_shared<IOTracer>()),
      immutable_db_options_(initial_db_options_),
      fs_(immutable_db_options_.fs, io_tracer_),
      mutable_db_options_(initial_db_options_),
      stats_(immutable_db_options_.stats),
      mutex_hold_log_threshold_us_(
          initial_db_options_.mutex_hold_log_threshold_us),
#ifdef COERCE_CONTEXT_SWITCH
      mutex_(stats_, immutable_db_options_.clock, DB_MUTEX_WAIT_MICROS, &bg_cv_,
             immutable_db_options_.use_adaptive_mutex),
#else   // COERCE_CONTEXT_SWITCH
      mutex_(stats_, immutable_db_options_.clock, DB_MUTEX_WAIT_MICROS,
             immutable_db_options_.use_adaptive_mutex),
#endif  // COERCE_CONTEXT_SWITCH
      error_handler_(this, immutable_db_options_, &mutex_),
      event_logger_(immutable_db_options_.info_log.get()),
      max_total_in_memory_state_(0),
      file_options_(BuildDBOptions(immutable_db_options_, mutable_db_options_)),
      file_options_for_compaction_(fs_->OptimizeForCompactionTableWrite(
          file_options_, immutable_db_options_)),
      seq_per_batch_(seq_per_batch),
      batch_per_txn_(batch_per_txn),
      bg_cv_(&mutex_),
      wal_sync_cv_(&wal_write_mutex_),
      write_buffer_manager_(immutable_db_options_.write_buffer_manager.get()),
      write_thread_(immutable_db_options_),
      nonmem_write_thread_(immutable_db_options_),
      write_controller_(mutable_db_options_.delayed_write_rate),
      delete_obsolete_files_last_run_(immutable_db_options_.clock->NowMicros()),
      wal_manager_(immutable_db_options_, file_options_, io_tracer_,
                   seq_per_batch),
      two_write_queues_(options.two_write_queues),
      manual_wal_flush_(options.manual_wal_flush),
      // last_sequencee_ is always maintained by the main queue that also writes
      // to the memtable. When two_write_queues_ is disabled last seq in
      // memtable is the same as last seq published to the readers. When it is
      // enabled but seq_per_batch_ is disabled, last seq in memtable still
      // indicates last published seq since wal-only writes that go to the 2nd
      // queue do not consume a sequence number. Otherwise writes performed by
      // the 2nd queue could change what is visible to the readers. In this
      // cases, last_seq_same_as_publish_seq_==false, the 2nd queue maintains a
      // separate variable to indicate the last published sequence.
      last_seq_same_as_publish_seq_(
          !(seq_per_batch && options.two_write_queues)),
      // Since seq_per_batch_ is currently set only by WritePreparedTxn which
      // requires a custom gc for compaction, we use that to set use_custom_gc_
      // as well.
      use_custom_gc_(seq_per_batch),
      own_sfm_(options.sst_file_manager == nullptr),
      atomic_flush_install_cv_(&mutex_),
      blob_callback_(immutable_db_options_.sst_file_manager.get(), &mutex_,
                     &error_handler_, &event_logger_,
                     immutable_db_options_.listeners, dbname_) {
#if USE_COROUTINES
  immutable_db_options_.fs->SetReadIOExecutorThreads(
      immutable_db_options_.read_io_executor_threads);
#endif  // USE_COROUTINES

  // !batch_per_trx_ implies seq_per_batch_ because it is only unset for
  // WriteUnprepared, which should use seq_per_batch_.
  assert(batch_per_txn_ || seq_per_batch_);

  // Reserve ten files or so for other uses and give the rest to TableCache.
  // Give a large number for setting of "infinite" open files.
  const int table_cache_size = (mutable_db_options_.max_open_files == -1)
                                   ? TableCache::kInfiniteCapacity
                                   : mutable_db_options_.max_open_files - 10;
  LRUCacheOptions co;
  co.capacity = table_cache_size;
  co.num_shard_bits = immutable_db_options_.table_cache_numshardbits;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  // TODO: Consider a non-fixed seed once test fallout (prefetch_test) is
  // dealt with
  co.hash_seed = 0;
  table_cache_ = NewLRUCache(co);
  SetDbSessionId();
  assert(!db_session_id_.empty());

  if (stats_ != nullptr && immutable_db_options_.info_log != nullptr) {
    slow_mutex_logging_ = std::make_shared<SlowMutexLoggingContext>(
        stats_, immutable_db_options_.clock, env_,
        immutable_db_options_.info_log, GetMutexHoldLogThreshold());
    mutex_.ConfigureSlowHoldLogging(slow_mutex_logging_, "DBImpl::mutex_");
    wal_write_mutex_.ConfigureSlowHoldLogging(slow_mutex_logging_,
                                              "DBImpl::wal_write_mutex_");
    logs_with_prep_tracker_.ConfigureSlowHoldLogging(slow_mutex_logging_);
  }

  if (write_buffer_manager_ != nullptr && !read_only_) {
    wbm_flush_initiator_ = std::make_unique<WBMFlushInitiator>(
        this, immutable_db_options_.atomic_flush);
  }

  periodic_task_functions_.emplace(PeriodicTaskType::kDumpStats,
                                   [this]() { this->DumpStats(); });
  periodic_task_functions_.emplace(PeriodicTaskType::kPersistStats,
                                   [this]() { this->PersistStats(); });
  periodic_task_functions_.emplace(PeriodicTaskType::kFlushInfoLog,
                                   [this]() { this->FlushInfoLog(); });
  periodic_task_functions_.emplace(
      PeriodicTaskType::kRecordSeqnoTime,
      [this]() { this->RecordSeqnoToTimeMapping(); });
  periodic_task_functions_.emplace(
      PeriodicTaskType::kTriggerCompaction,
      [this]() { this->TriggerPeriodicCompaction(); });

  versions_.reset(new VersionSet(
      dbname_, &immutable_db_options_, mutable_db_options_, file_options_,
      table_cache_.get(), write_buffer_manager_, &write_controller_,
      &block_cache_tracer_, io_tracer_, db_id_, db_session_id_,
      options.daily_offpeak_time_utc, &error_handler_, read_only));
  versions_->GetColumnFamilySet()->SetFlushInitiator(
      wbm_flush_initiator_.get());
  column_family_memtables_.reset(
      new ColumnFamilyMemTablesImpl(versions_->GetColumnFamilySet()));

  DumpRocksDBBuildVersion(immutable_db_options_.info_log.get());
  DumpDBFileSummary(immutable_db_options_, dbname_, db_session_id_);
  immutable_db_options_.Dump(immutable_db_options_.info_log.get());
  mutable_db_options_.Dump(immutable_db_options_.info_log.get());
  DumpSupportInfo(immutable_db_options_.info_log.get());

  max_total_wal_size_.store(mutable_db_options_.max_total_wal_size,
                            std::memory_order_relaxed);
  if (write_buffer_manager_) {
    wbm_stall_.reset(new WBMStallInterface());
  }
}

Status DBImpl::Resume() {
  ROCKS_LOG_INFO(immutable_db_options_.info_log, "Resuming DB");

  InstrumentedMutexLock db_mutex(&mutex_);

  if (!error_handler_.IsDBStopped() && !error_handler_.IsBGWorkStopped()) {
    // Nothing to do
    return Status::OK();
  }

  if (read_only_) {
    return Status::NotSupported("Cannot resume a read-only DB");
  }

  if (error_handler_.IsRecoveryInProgress()) {
    // Don't allow a mix of manual and automatic recovery
    return Status::Busy("Recovery in progress");
  }

  mutex_.Unlock();
  Status s = error_handler_.RecoverFromBGError(true);
  mutex_.Lock();
  return s;
}

// This function implements both manual and automatic background-error
// recovery. It discards stale queued flush requests, waits for scheduled work
// to exit, rebuilds and runs the flush work required by the recovery context,
// purges obsolete files, and schedules any follow-up compactions.
//
// A file-scoped WAL write error needs a stronger protocol because the failed
// record might be absent, partial, or complete, and the failed file handle must
// never be used again:
// 1. Keep every WAL through failed_wal_number quarantined from concurrent sync.
// 2. Fence WAL iterators at the failed sequence range and persist its end so a
//    possibly complete record is neither exposed nor allowed to collide with a
//    later write after recovery.
// 3. Replace the failed WAL and atomically flush all live column families.
// 4. Verify every column family advanced beyond the failed WAL.
// 5. Detach the quarantined handles, then clear the cutoff only if a concurrent
//    failure did not extend it. Any failure before completion leaves the hard
//    background error and quarantine in place.
Status DBImpl::ResumeImpl(DBRecoverContext context,
                          Env::IOActivity io_activity) {
  mutex_.AssertHeld();

  const ReadOptions read_options(io_activity);
  const WriteOptions write_options(io_activity);

  if (lock_wal_owner_thread_id_counts_.count(env_->GetThreadID()) > 0) {
    return Status::Aborted(
        "Likely deadlock as the same thread called LockWAL()");
  }
  // Recovery replaces WALs, flushes memtables, and can reserve the sequence
  // numbers of an indeterminate WAL write. Defer it while any LockWAL() is held
  // so the frozen DB state (latest sequence number and current WAL) promised by
  // LockWAL() stays intact. UnlockWAL() and shutdown signal bg_cv_.
  while (!lock_wal_owner_thread_id_counts_.empty() && !shutdown_initiated_ &&
         !shutting_down_.load(std::memory_order_acquire)) {
    TEST_SYNC_POINT("DBImpl::ResumeImpl:WaitForUnlockWAL");
    bg_cv_.Wait();
  }

  assert(static_cast<size_t>(unscheduled_flushes_) <= flush_queue_.size());
  // Recovery rebuilds flush requests for every column family with pending
  // immutable data after scheduled workers exit, so every existing request is
  // redundant and safe to remove. A worker can exit on the background error
  // without popping a request, leaving it out of unscheduled_flushes_.
  while (!flush_queue_.empty()) {
    FlushRequest flush_req = PopFirstFromFlushQueue();
    for (const auto& item : flush_req.cfd_to_max_mem_id_to_persist) {
      ColumnFamilyData* cfd = item.first;
      assert(cfd);
      cfd->UnrefAndTryDelete();
    }
  }
  unscheduled_flushes_ = 0;

  TEST_SYNC_POINT("DBImpl::ResumeImpl:BeforeWaitForBackgroundWork");
  WaitForBackgroundWork();

  TEST_SYNC_POINT("DBImpl::ResumeImpl:Start");

  if (context.IsWALWriteErrorRecovery()) {
    ExtendWALRecoveryCutoff(context.failed_wal_number);
  }

  // With two_write_queues=true, sequence numbers are allocated via
  // FetchAddLastAllocatedSequence() before writes complete, but only
  // published via SetLastSequence() after success. If we're recovering from
  // an error, there may be allocated-but-not-published sequence numbers.
  // Start recovery from a published sequence number that covers writers which
  // were already in flight. Recovery flushes repeat this sync at the actual
  // memtable-switch fence, after FlushAllColumnFamilies() has dropped and
  // re-acquired the DB mutex.
  if (two_write_queues_) {
    WriteThread::Writer w;
    write_thread_.EnterUnbatched(&w, &mutex_);
    WriteThread::Writer nonmem_w;
    nonmem_write_thread_.EnterUnbatched(&nonmem_w, &mutex_);
    WaitForPendingWrites();

    versions_->SyncLastSequenceWithAllocated();

    nonmem_write_thread_.ExitUnbatched(&nonmem_w);
    write_thread_.ExitUnbatched(&w);
  }

  TEST_SYNC_POINT("DBImpl::ResumeImpl:AfterSyncSeq");

  Status s;
  if (shutdown_initiated_) {
    // Returning shutdown status to SFM during auto recovery will cause it
    // to abort the recovery and allow the shutdown to progress
    s = Status::ShutdownInProgress();
  }

  if (s.ok() && context.IsWALWriteErrorRecovery()) {
    const bool has_blob_direct_write =
        HasAnyBlobDirectWriteColumnFamilyWithLockHeld();
    if (immutable_db_options_.manual_wal_flush ||
        immutable_db_options_.allow_2pc || two_write_queues_ ||
        immutable_db_options_.enable_pipelined_write ||
        immutable_db_options_.unordered_write ||
        immutable_db_options_.recycle_log_file_num != 0 ||
        immutable_db_options_.WAL_ttl_seconds != 0 ||
        immutable_db_options_.WAL_size_limit_MB != 0 ||
        immutable_db_options_.track_and_verify_wals_in_manifest ||
        immutable_db_options_.track_and_verify_wals || has_blob_direct_write) {
      ROCKS_LOG_WARN(
          immutable_db_options_.info_log,
          "[WAL recovery] Unsupported configuration; DB remains write-stopped: "
          "manual_wal_flush=%d, allow_2pc=%d, two_write_queues=%d, "
          "pipelined_write=%d, unordered_write=%d, wal_recycling=%d, "
          "wal_ttl=%d, wal_size_limit=%d, track_wals_in_manifest=%d, "
          "track_wals=%d, blob_direct_write=%d",
          static_cast<int>(immutable_db_options_.manual_wal_flush),
          static_cast<int>(immutable_db_options_.allow_2pc),
          static_cast<int>(two_write_queues_),
          static_cast<int>(immutable_db_options_.enable_pipelined_write),
          static_cast<int>(immutable_db_options_.unordered_write),
          static_cast<int>(immutable_db_options_.recycle_log_file_num != 0),
          static_cast<int>(immutable_db_options_.WAL_ttl_seconds != 0),
          static_cast<int>(immutable_db_options_.WAL_size_limit_MB != 0),
          static_cast<int>(
              immutable_db_options_.track_and_verify_wals_in_manifest),
          static_cast<int>(immutable_db_options_.track_and_verify_wals),
          static_cast<int>(has_blob_direct_write));
      s = Status::NotSupported(
          "file-scoped WAL write error recovery is not supported with manual "
          "WAL flush, 2PC, two write queues, pipelined or unordered writes, "
          "WAL recycling, retention or tracking, or blob direct write");
    }
  }

  if (s.ok()) {
    Status bg_error = error_handler_.GetBGError();
    if (bg_error.severity() > Status::Severity::kHardError) {
      ROCKS_LOG_INFO(
          immutable_db_options_.info_log,
          "DB resume requested but failed due to Fatal/Unrecoverable error");
      s = bg_error;
    }
  }

  // Make sure the IO Status stored in version set is set to OK.
  if (s.ok()) {
    IOStatus io_s = versions_->io_status();
    if (io_s.IsIOError()) {
      // If resuming from IOError resulted from MANIFEST write, then assert
      // that we must have already set the MANIFEST writer to nullptr during
      // clean-up phase MANIFEST writing.
      assert(!versions_->descriptor_log_);
      // Since we are trying to recover from MANIFEST write error, we need to
      // switch to a new MANIFEST anyway. The old MANIFEST can be corrupted.
      // Therefore, force writing a dummy version edit because we do not know
      // whether there are flush jobs with non-empty data to flush, triggering
      // appends to MANIFEST.
      VersionEdit edit;
      auto cfh =
          static_cast_with_check<ColumnFamilyHandleImpl>(default_cf_handle_);
      assert(cfh);
      ColumnFamilyData* cfd = cfh->cfd();
      s = versions_->LogAndApply(cfd, read_options, write_options, &edit,
                                 &mutex_, directories_.GetDbDir());
      if (!s.ok()) {
        io_s = versions_->io_status();
        if (!io_s.ok()) {
          error_handler_.SetBGError(io_s,
                                    BackgroundErrorReason::kManifestWrite);
        }
      }
    }
  }

  if (s.ok() && context.IsWALWriteErrorRecovery() &&
      context.failed_wal_sequence > 0) {
    // A WAL error can be reported after the whole record reached storage but
    // before its sequence range was published. Persist the reservation before
    // admitting later writes so they cannot reuse an ambiguous sequence. This
    // guarantee belongs to file-scoped recovery, which also atomically advances
    // every column family past the failed WAL before retiring it. Unsupported
    // configurations fail the check above and remain write-stopped.
    assert(context.IsWALWriteErrorRecovery());
    assert(context.failed_wal_sequence_start != 0);
    assert(context.failed_wal_sequence_start <= context.failed_wal_sequence);
    wal_manager_.RecordIndeterminateWriteSequence(
        context.failed_wal_number, context.failed_wal_sequence_start,
        context.failed_wal_sequence);
    if (context.failed_wal_sequence > versions_->LastAllocatedSequence()) {
      versions_->SetLastAllocatedSequence(context.failed_wal_sequence);
    }
    if (context.failed_wal_sequence > versions_->LastPublishedSequence()) {
      versions_->SetLastPublishedSequence(context.failed_wal_sequence);
    }
    if (context.failed_wal_sequence > versions_->LastSequence()) {
      versions_->SetLastSequence(context.failed_wal_sequence);
    }
    VersionEdit edit;
    edit.SetLastSequence(context.failed_wal_sequence);
    auto cfh =
        static_cast_with_check<ColumnFamilyHandleImpl>(default_cf_handle_);
    assert(cfh);
    s = versions_->LogAndApply(cfh->cfd(), read_options, write_options, &edit,
                               &mutex_, directories_.GetDbDir());
    if (!s.ok() && versions_->io_status().IsIOError()) {
      error_handler_.SetBGError(versions_->io_status(),
                                BackgroundErrorReason::kManifestWrite);
    }
    if (s.ok()) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "[WAL recovery] Persisted sequence reservation through "
                     "#%" PRIu64,
                     context.failed_wal_sequence);
    } else {
      ROCKS_LOG_WARN(immutable_db_options_.info_log,
                     "[WAL recovery] Failed to persist sequence reservation "
                     "through #%" PRIu64 ": %s",
                     context.failed_wal_sequence, s.ToString().c_str());
    }
  }

  if (s.ok()) {
    recovering_from_wal_write_error_ = context.IsWALWriteErrorRecovery();
    recover_wal_through_number_ = context.failed_wal_number;
    if (context.flush_reason == FlushReason::kErrorRecoveryRetryFlush) {
      s = RetryFlushesForErrorRecovery(FlushReason::kErrorRecoveryRetryFlush,
                                       true /* wait */);
    } else {
      // We cannot guarantee consistency of the WAL. So force flush Memtables of
      // all the column families
      FlushOptions flush_opts;
      // We allow flush to stall write since we are trying to resume from error.
      flush_opts.allow_write_stall = true;
      // Keep the per-column-family WAL replay boundaries in one MANIFEST
      // commit. Otherwise a crash between sequential flushes can recover only
      // part of a write batch spanning multiple column families.
      flush_opts.force_atomic_flush = true;
      if (context.IsWALWriteErrorRecovery()) {
        ROCKS_LOG_INFO(immutable_db_options_.info_log,
                       "[WAL recovery] Starting atomic flush of all live "
                       "column families through WAL #%" PRIu64,
                       context.failed_wal_number);
      }
      s = FlushAllColumnFamilies(flush_opts, context.flush_reason);
      if (s.ok() && context.IsWALWriteErrorRecovery()) {
        ROCKS_LOG_INFO(immutable_db_options_.info_log,
                       "[WAL recovery] Atomic flush completed through WAL "
                       "#%" PRIu64,
                       context.failed_wal_number);
      }
    }
    recovering_from_wal_write_error_ = false;
    recover_wal_through_number_ = 0;
    if (!s.ok()) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "DB resume requested but failed due to Flush failure [%s]",
                     s.ToString().c_str());
    }
  }

  if (s.ok() && context.IsWALWriteErrorRecovery()) {
    for (auto* cfd : *versions_->GetColumnFamilySet()) {
      if (!cfd->IsDropped() &&
          cfd->GetLogNumber() <= context.failed_wal_number) {
        s = Status::Corruption(
            "WAL write error recovery did not advance column family " +
            cfd->GetName() + " past WAL " +
            std::to_string(context.failed_wal_number));
        break;
      }
    }
  }

  if (s.ok() && context.IsWALWriteErrorRecovery()) {
    // The flush and MANIFEST transition above make the abandoned WALs
    // unnecessary for recovery. Detach their writers even when physical file
    // deletion is disabled so no later SyncWAL() can touch a poisoned handle.
    mutex_.Unlock();
    {
      InstrumentedMutexLock wal_lock(&wal_write_mutex_);
      size_t retired_wal_count = 0;
      if (logs_.empty() || logs_.back().number <= context.failed_wal_number) {
        s = Status::Corruption(
            "WAL write error recovery did not install a replacement WAL "
            "after WAL " +
            std::to_string(context.failed_wal_number));
      } else {
        for (auto it = logs_.begin();
             it != logs_.end() && it->number <= context.failed_wal_number;) {
          if (it->IsSyncing()) {
            wal_sync_cv_.Wait();
            it = logs_.begin();
            continue;
          }
          wals_to_free_.push_back(it->ReleaseWriter());
          it = logs_.erase(it);
          ++retired_wal_count;
        }
        uint64_t expected_cutoff = context.failed_wal_number;
        // A failed compare-exchange means a concurrent WAL failure extended the
        // cutoff. That failure also sets recovery_error_, so the background
        // error remains set and a later recovery attempt owns the larger range.
        const bool cutoff_cleared =
            wal_recovery_cutoff_.compare_exchange_strong(
                expected_cutoff, 0, std::memory_order_acq_rel,
                std::memory_order_acquire);
        if (cutoff_cleared) {
          ROCKS_LOG_INFO(immutable_db_options_.info_log,
                         "[WAL recovery] Retired %" ROCKSDB_PRIszt
                         " WAL handle(s) through "
                         "#%" PRIu64 " and cleared the quarantine cutoff",
                         retired_wal_count, context.failed_wal_number);
        } else {
          ROCKS_LOG_WARN(
              immutable_db_options_.info_log,
              "[WAL recovery] Retired %" ROCKSDB_PRIszt
              " WAL handle(s) through "
              "#%" PRIu64 "; quarantine cutoff advanced to #%" PRIu64,
              retired_wal_count, context.failed_wal_number, expected_cutoff);
        }
      }
    }
    mutex_.Lock();
  }

  JobContext job_context(0);
  FindObsoleteFiles(&job_context, true);
  mutex_.Unlock();
  // If DB shutdown initiated here, it will wait for this ongoing recovery.
  job_context.manifest_file_number = 1;
  if (job_context.HaveSomethingToDelete()) {
    PurgeObsoleteFiles(job_context);
  }
  job_context.Clean();
  mutex_.Lock();

  if (s.ok()) {
    // Will notify and unblock threads waiting for error recovery to finish.
    s = error_handler_.ClearBGError();
    if (s.ok() && AsyncWALPrecreateEnabled()) {
      size_t max_write_buffer_size = 0;
      for (ColumnFamilyData* cfd : *versions_->GetColumnFamilySet()) {
        if (!cfd->IsDropped()) {
          max_write_buffer_size =
              std::max(max_write_buffer_size,
                       cfd->GetLatestMutableCFOptions().write_buffer_size);
        }
      }
      // WAL rotation during recovery cannot schedule its successor while the
      // hard background error is set. Refill the async slot only after
      // ClearBGError() has made normal background work eligible again.
      MaybeScheduleAsyncWALPrecreate(
          GetWalPreallocateBlockSize(max_write_buffer_size));
    }
  } else {
    // NOTE: this is needed to pass ASSERT_STATUS_CHECKED
    // in the DBSSTTest.DBWithMaxSpaceAllowedRandomized test.
    // See https://github.com/meta/rocksdb/pull/7715#issuecomment-754947952
    error_handler_.GetRecoveryError().PermitUncheckedError();
  }

  // Check for shutdown again before scheduling further compactions,
  // since we released and re-acquired the lock above
  if (shutdown_initiated_) {
    s = Status::ShutdownInProgress();
  }
  if (s.ok() && context.flush_after_recovery) {
    // Normal flush requests are discarded during recovery, and a new memtable
    // may fill while recovery releases the DB mutex. Schedule a catch-up flush.
    Status status = RetryFlushesForErrorRecovery(
        FlushReason::kCatchUpAfterErrorRecovery, false /* wait */);
    if (!status.ok()) {
      // FlushAllColumnFamilies internally should take care of setting
      // background error if needed.
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "The catch up flush after successful recovery failed [%s]",
                     s.ToString().c_str());
    }
    // FlushAllColumnFamilies releases and re-acquires mutex.
    if (shutdown_initiated_) {
      s = Status::ShutdownInProgress();
    }
  }

  if (s.ok()) {
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      EnqueuePendingCompaction(cfd);
    }
    MaybeScheduleFlushOrCompaction();
  }

  if (s.ok() && context.IsWALWriteErrorRecovery()) {
    ROCKS_LOG_INFO(
        immutable_db_options_.info_log,
        "[WAL recovery] Successfully resumed DB through WAL #%" PRIu64,
        context.failed_wal_number);
  } else if (s.ok()) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "Successfully resumed DB");
  } else if (context.IsWALWriteErrorRecovery()) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "[WAL recovery] Failed to resume DB through WAL #%" PRIu64
                   ": %s",
                   context.failed_wal_number, s.ToString().c_str());
  } else {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "Failed to resume DB [%s]",
                   s.ToString().c_str());
  }

  // Wake up any waiters - in this case, it could be the shutdown thread
  bg_cv_.SignalAll();

  // No need to check BGError again. If something happened, event listener would
  // be notified and the operation causing it would have failed
  return s;
}

void DBImpl::WaitForBackgroundWork() {
  // Wait for background work to finish
  while (bg_bottom_compaction_scheduled_ || bg_compaction_scheduled_ ||
         bg_flush_scheduled_ || bg_wbm_flush_scheduled_ ||
         bg_pressure_callback_in_progress_ ||
         bg_async_file_open_state_ == AsyncFileOpenState::kScheduled) {
    bg_cv_.Wait();
  }
}

void DBImpl::WaitForAsyncFileOpen() {
  if (!immutable_db_options_.open_files_async) {
    return;
  }
  InstrumentedMutexLock l(&mutex_);
  TEST_SYNC_POINT("DBImpl::WaitForAsyncFileOpen::BeforeWait");
  while (bg_async_file_open_state_ == AsyncFileOpenState::kScheduled &&
         !shutting_down_.load(std::memory_order_acquire)) {
    bg_cv_.Wait();
  }
}

void DBImpl::NotifyOnDBShutdownBegin() {
  mutex_.AssertHeld();
  if (shutdown_notification_sent_ || immutable_db_options_.listeners.empty()) {
    return;
  }
  shutdown_notification_sent_ = true;

  // release lock while notifying events
  mutex_.Unlock();
  for (const auto& listener : immutable_db_options_.listeners) {
    listener->OnDBShutdownBegin(this);
  }
  mutex_.Lock();
}

// Will lock the mutex_,  will wait for completion if wait is true
void DBImpl::CancelAllBackgroundWork(bool wait) {
  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "Shutdown: canceling all background work");
  Status s = CancelPeriodicTaskScheduler();
  s.PermitUncheckedError();

  InstrumentedMutexLock l(&mutex_);
  // Blob direct-write relies on clean shutdown flushes to register live blob
  // files in MANIFEST before close. WAL recovery of unregistered files is out
  // of scope in v1.
  const bool force_flush_for_blob_direct_write =
      HasAnyBlobDirectWriteColumnFamilyWithLockHeld();
  const bool flush_for_unpersisted_data =
      has_unpersisted_data_.load(std::memory_order_relaxed) &&
      !mutable_db_options_.avoid_flush_during_shutdown;
  const bool already_shutting_down =
      shutting_down_.load(std::memory_order_acquire);
  if (already_shutting_down && force_flush_for_blob_direct_write &&
      HasInFlightBlobDirectWriteFilesWithLockHeld()) {
    // Flush jobs treat `shutting_down_` as a rollback condition, so once the
    // shutdown marker is already published we cannot safely promise a final
    // BDW registration flush anymore. Make that loss of crash-safety explicit.
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "Shutdown already in progress before blob direct-write "
                   "close flush; active blob files may remain unregistered.");
  }
  if (!already_shutting_down &&
      (flush_for_unpersisted_data || force_flush_for_blob_direct_write)) {
    s = DBImpl::FlushAllColumnFamilies(FlushOptions(), FlushReason::kShutDown);
    if (!s.ok()) {
      ROCKS_LOG_WARN(
          immutable_db_options_.info_log, "Shutdown flush failed%s: %s",
          force_flush_for_blob_direct_write
              ? "; active blob direct-write files may remain unregistered"
              : "",
          s.ToString().c_str());
      s.PermitUncheckedError();
    }
  }

  // Cancel awaiting remote compactions
  if (immutable_db_options_.compaction_service) {
    immutable_db_options_.compaction_service->CancelAwaitingJobs();
  }

  if (!already_shutting_down) {
    NotifyOnDBShutdownBegin();
  }
  shutting_down_.store(true, std::memory_order_release);
  bg_cv_.SignalAll();
  env_->UnSchedule(&bg_async_file_open_state_, Env::Priority::HIGH);
  if (!wait) {
    return;
  }
  WaitForBackgroundWork();
}

Status DBImpl::MaybeReleaseTimestampedSnapshotsAndCheck() {
  size_t num_snapshots = 0;
  ReleaseTimestampedSnapshotsOlderThan(std::numeric_limits<uint64_t>::max(),
                                       &num_snapshots);

  // If there is unreleased snapshot, fail the close call
  if (num_snapshots > 0) {
    return Status::Aborted("Cannot close DB with unreleased snapshot.");
  }

  return Status::OK();
}

void DBImpl::UntrackDataFiles() {
  TrackOrUntrackFiles(/*existing_data_files=*/{},
                      /*track=*/false);
}

bool DBImpl::HasAnyBlobDirectWriteColumnFamily() {
  return blob_direct_write_cf_count_.load(std::memory_order_relaxed) > 0;
}

bool DBImpl::HasAnyBlobDirectWriteColumnFamilyWithLockHeld() {
  mutex_.AssertHeld();
  return blob_direct_write_cf_count_.load(std::memory_order_relaxed) > 0;
}

bool DBImpl::HasInFlightBlobDirectWriteFilesWithLockHeld() {
  mutex_.AssertHeld();
  if (!HasAnyBlobDirectWriteColumnFamilyWithLockHeld()) {
    return false;
  }

  UnorderedSet<uint64_t> active_blob_direct_write_files;
  for (auto* cfd : *versions_->GetColumnFamilySet()) {
    if (cfd->IsDropped()) {
      continue;
    }

    auto* mgr = cfd->blob_partition_manager();
    if (mgr == nullptr) {
      continue;
    }

    mgr->GetActiveBlobFileNumbers(&active_blob_direct_write_files);
    if (!active_blob_direct_write_files.empty()) {
      return true;
    }
  }

  return false;
}

void DBImpl::MaybeInitBlobDirectWriteColumnFamily(
    ColumnFamilyData* cfd, const ColumnFamilyOptions& cf_options,
    const std::string& column_family_name) {
  assert(cfd != nullptr);

  if (!cf_options.enable_blob_files || !cf_options.enable_blob_direct_write) {
    return;
  }
  if (cfd->blob_partition_manager() != nullptr) {
    // DB::Open(..., create_missing_column_families=true, ...) can create the
    // missing CF during open and then revisit the same handle in the post-open
    // initialization loop below. Treat BDW initialization as idempotent for
    // that path so we do not double-register the same CF.
    return;
  }

  auto mgr = std::make_shared<BlobFilePartitionManager>(
      cf_options.blob_direct_write_partitions,
      cf_options.blob_direct_write_partition_strategy,
      [vs = versions_.get()]() { return vs->NewFileNumber(); }, fs_.get(),
      immutable_db_options_.clock, stats_, file_options_, dbname_,
      column_family_name, cf_options.blob_file_size,
      immutable_db_options_.use_fsync,
      cf_options.blob_file_writable_file_max_buffer_size,
      cfd->blob_file_cache(), &blob_callback_, immutable_db_options_.listeners,
      immutable_db_options_.file_checksum_gen_factory.get(),
      immutable_db_options_.checksum_handoff_file_types, io_tracer_, db_id_,
      db_session_id_, immutable_db_options_.info_log.get());

  cfd->SetBlobPartitionManager(std::move(mgr));
  RegisterBlobDirectWriteColumnFamily();
}

void DBImpl::RegisterBlobDirectWriteColumnFamily() {
  blob_direct_write_cf_count_.fetch_add(1, std::memory_order_relaxed);
}

void DBImpl::UnregisterBlobDirectWriteColumnFamily() {
  uint32_t current =
      blob_direct_write_cf_count_.load(std::memory_order_relaxed);
  while (current != 0) {
    if (blob_direct_write_cf_count_.compare_exchange_weak(
            current, current - 1, std::memory_order_relaxed)) {
      return;
    }
  }
  assert(false);
}

Status DBImpl::MaybeWriteWalMarkersToManifestOnClose() {
  mutex_.AssertHeld();
  if (!mutable_db_options_.optimize_manifest_for_recovery ||
      !opened_successfully_ || read_only_ || versions_ == nullptr ||
      logs_.empty()) {
    return Status::OK();
  }

  TEST_SYNC_POINT("DBImpl::CloseHelper:WriteWalMarkersOnCloseEntered");
  const ReadOptions read_options(Env::IOActivity::kUnknown);
  const WriteOptions write_options(Env::IOActivity::kUnknown);
  uint64_t max_wal_number = logs_.back().number;
  TEST_SYNC_POINT_CALLBACK("DBImpl::CloseHelper:CapturedMaxWal",
                           &max_wal_number);
  const uint64_t new_log_num = max_wal_number + 1;

  // std::deque keeps VersionEdit pointers stable across emplace_back.
  std::deque<VersionEdit> edits;
  autovector<ColumnFamilyData*> cfds;
  autovector<autovector<VersionEdit*>> edit_lists;
  VersionEdit global_edit;
  bool persist_global_edit = false;
  bool reserved_log_number = false;

  bool all_cfs_empty = true;
  for (auto* cfd : *versions_->GetColumnFamilySet()) {
    if (cfd->IsDropped()) {
      continue;
    }
    // cfd->IsEmpty() checks both mem and imm. An immutable memtable queued for
    // flush still pins an older WAL; advancing past it would drop unflushed
    // records on the next open.
    if (!cfd->IsEmpty()) {
      all_cfs_empty = false;
      continue;
    }
    if (new_log_num > cfd->GetLogNumber()) {
      if (!reserved_log_number) {
        const uint64_t next_file_number = versions_->current_next_file_number();
        if (next_file_number <= new_log_num) {
          versions_->FetchAddFileNumber(new_log_num + 1 - next_file_number);
          global_edit.SetNextFile(versions_->current_next_file_number());
          persist_global_edit = true;
        }
        reserved_log_number = true;
      }
      edits.emplace_back();
      VersionEdit& edit = edits.back();
      edit.SetLogNumber(new_log_num);
      edit.SetColumnFamily(cfd->GetID());
      cfds.push_back(cfd);
      edit_lists.emplace_back();
      edit_lists.back().push_back(&edit);
    }
  }

  if (all_cfs_empty && !allow_2pc()) {
    if (new_log_num > versions_->min_log_number_to_keep()) {
      global_edit.SetMinLogNumberToKeep(new_log_num);
      persist_global_edit = true;
    }
    // Mirror the monotonicity guard in db_impl_files.cc:
    // PrecomputeWalDeletionEdit emits DeleteWalsBefore only when the
    // marker actually advances the WalSet's existing min. Skipping
    // this guard would let a stale-replay edit regress WalSet state.
    if (immutable_db_options_.track_and_verify_wals_in_manifest &&
        new_log_num > versions_->GetWalSet().GetMinWalNumberToKeep()) {
      global_edit.DeleteWalsBefore(new_log_num);
      persist_global_edit = true;
    }
  }

  if (persist_global_edit) {
    ColumnFamilyData* default_cfd =
        versions_->GetColumnFamilySet()->GetDefault();
    if (default_cfd && !default_cfd->IsDropped()) {
      bool attached_to_existing = false;
      for (size_t i = 0; i < cfds.size(); ++i) {
        if (cfds[i] == default_cfd) {
          edit_lists[i].push_back(&global_edit);
          attached_to_existing = true;
          break;
        }
      }
      if (!attached_to_existing) {
        cfds.push_back(default_cfd);
        edit_lists.emplace_back();
        edit_lists.back().push_back(&global_edit);
      }
    }
  }

  if (cfds.empty()) {
    return Status::OK();
  }
  return versions_->LogAndApply(cfds, read_options, write_options, edit_lists,
                                &mutex_, directories_.GetDbDir(),
                                /*new_descriptor_log=*/false);
}

void DBImpl::MaybeRegisterFlushInitiator() {
  // Register only fully opened read-write DBs. Do not hold mutex_: the lock
  // order is registry mutex before DB mutex.
  if (write_buffer_manager_ == nullptr || read_only_ || !opened_successfully_) {
    return;
  }
  if (wbm_flush_initiator_ == nullptr || wbm_flush_initiator_->IsRegistered()) {
    return;
  }
  write_buffer_manager_->RegisterFlushInitiator(wbm_flush_initiator_.get());
}

Status DBImpl::CloseHelper() {
  // Guarantee that there is no background error recovery in progress before
  // continuing with the shutdown
  mutex_.Lock();
  shutdown_initiated_ = true;
  // Wake a recovery waiting in ResumeImpl() for UnlockWAL().
  bg_cv_.SignalAll();
  error_handler_.CancelErrorRecoveryForShutDown();
  while (!error_handler_.ReadyForShutdown()) {
    bg_cv_.Wait();
  }
  mutex_.Unlock();

  // Deregister before teardown and without mutex_ to preserve registry-to-DB
  // lock ordering.
  if (write_buffer_manager_ && wbm_flush_initiator_ &&
      wbm_flush_initiator_->IsRegistered()) {
    write_buffer_manager_->DeregisterFlushInitiator(wbm_flush_initiator_.get());
  }

  // Below check is added as recovery_error_ is not checked and it causes crash
  // in DBSSTTest.DBWithMaxSpaceAllowedWithBlobFiles when space limit is
  // reached.
  error_handler_.GetRecoveryError().PermitUncheckedError();

  // CancelAllBackgroundWork called with false means we just set the shutdown
  // marker. After this we do a variant of the waiting and unschedule work
  // (to consider: moving all the waiting into CancelAllBackgroundWork(true))
  CancelAllBackgroundWork(false);

  // Cancel manual compaction if there's any
  if (HasPendingManualCompaction()) {
    DisableManualCompaction();
  }
  mutex_.Lock();
  // Unschedule all tasks for this DB
  for (uint8_t i = 0; i < static_cast<uint8_t>(TaskType::kCount); i++) {
    env_->UnSchedule(GetTaskTag(i), Env::Priority::BOTTOM);
    env_->UnSchedule(GetTaskTag(i), Env::Priority::LOW);
    env_->UnSchedule(GetTaskTag(i), Env::Priority::HIGH);
  }

  Status ret = Status::OK();

  // Wait for background work to finish
  while (bg_bottom_compaction_scheduled_ || bg_compaction_scheduled_ ||
         bg_flush_scheduled_ || bg_wbm_flush_scheduled_ ||
         bg_purge_scheduled_ || bg_pressure_callback_in_progress_ ||
         bg_async_file_open_state_ == AsyncFileOpenState::kScheduled ||
         async_wal_precreate_state_ == AsyncWALPrecreateState::kScheduled ||
         pending_purge_obsolete_files_ ||
         error_handler_.IsRecoveryInProgress()) {
    TEST_SYNC_POINT("DBImpl::~DBImpl:WaitJob");
    bg_cv_.Wait();
  }

  // Release any opened-but-unpublished WAL writer after the in-flight worker
  // has published its result. Clear the DB-owned async slot while holding
  // mutex_, but destroy the detached writer after dropping mutex_ because
  // log::Writer / WritableFileWriter destruction can flush and close the file.
  // The file itself can be left behind as an empty future WAL; recovery already
  // tolerates it and marks its file number used if observed.
  UnpublishedWAL unused_async_wal = std::move(async_wal_precreate_wal_);
  async_wal_precreate_state_ = AsyncWALPrecreateState::kNotScheduled;
  if (unused_async_wal.writer) {
    mutex_.Unlock();
    unused_async_wal.Reset();
    mutex_.Lock();
  }

  // Ensure subclasses don't forget to schedule async file opening
  assert(!immutable_db_options_.open_files_async || !opened_successfully_ ||
         bg_async_file_open_state_ != AsyncFileOpenState::kNotScheduled);

  // No FileIngestionHandle from PrepareFileIngestion() may still be
  // outstanding
  assert(num_outstanding_prepared_ingestions_.load() == 0);

  TEST_SYNC_POINT_CALLBACK("DBImpl::CloseHelper:PendingPurgeFinished",
                           &files_grabbed_for_purge_);
  EraseThreadStatusDbInfo();
  flush_scheduler_.Clear();
  trim_history_scheduler_.Clear();

  while (!flush_queue_.empty()) {
    const FlushRequest& flush_req = PopFirstFromFlushQueue();
    for (const auto& iter : flush_req.cfd_to_max_mem_id_to_persist) {
      iter.first->UnrefAndTryDelete();
    }
  }

  while (!compaction_queue_.empty()) {
    auto cfd = PopFirstFromCompactionQueue();
    cfd->UnrefAndTryDelete();
  }

  for (auto* cfd : parked_compaction_cfds_) {
    assert(cfd->queued_for_compaction());
    cfd->set_queued_for_compaction(false);
    cfd->UnrefAndTryDelete();
  }
  parked_compaction_cfds_.clear();

  if (default_cf_handle_ != nullptr || persist_stats_cf_handle_ != nullptr) {
    // we need to delete handle outside of lock because it does its own locking
    mutex_.Unlock();
    TEST_SYNC_POINT("DBImpl::CloseHelper:CFHandleCleanupUnlocked");
    TEST_SYNC_POINT("DBImpl::CloseHelper:CFHandleCleanupAllowed");
    if (default_cf_handle_) {
      delete default_cf_handle_;
      default_cf_handle_ = nullptr;
    }
    if (persist_stats_cf_handle_) {
      delete persist_stats_cf_handle_;
      persist_stats_cf_handle_ = nullptr;
    }
    mutex_.Lock();
  }

  // Clean up obsolete files due to SuperVersion release.
  // (1) Need to delete to obsolete files before closing because RepairDB()
  // scans all existing files in the file system and builds manifest file.
  // Keeping obsolete files confuses the repair process.
  // (2) Need to check if we Open()/Recover() the DB successfully before
  // deleting because if VersionSet recover fails (may be due to corrupted
  // manifest file), it is not able to identify live files correctly. As a
  // result, all "live" files can get deleted by accident. However, corrupted
  // manifest is recoverable by RepairDB().
  if (opened_successfully_ && !read_only_) {
    JobContext job_context(next_job_id_.fetch_add(1));
    FindObsoleteFiles(&job_context, true);

    mutex_.Unlock();
    // manifest number starting from 2
    job_context.manifest_file_number = 1;
    if (job_context.HaveSomethingToDelete()) {
      PurgeObsoleteFiles(job_context);
    }
    job_context.Clean();
    mutex_.Lock();
  }
  // Best-effort warm-reopen optimization: after obsolete-file cleanup and
  // before logs_.clear(), persist close-time WAL markers that can reduce
  // recovery work on the next open. This matters most when some CFs have no
  // unflushed writes and/or the newest WAL is empty, since recovery would
  // otherwise append marker-only MANIFEST edits on reopen.
  Status manifest_s = MaybeWriteWalMarkersToManifestOnClose();
  if (!manifest_s.ok()) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "optimize_manifest_for_recovery close-time write failed: %s",
                   manifest_s.ToString().c_str());
  }
  {
    InstrumentedMutexLock lock(&wal_write_mutex_);
    for (auto l : wals_to_free_) {
      delete l;
    }
    for (auto& log : logs_) {
      uint64_t log_number = log.writer->get_log_number();
      Status s = log.ClearWriter();
      if (!s.ok()) {
        ROCKS_LOG_WARN(
            immutable_db_options_.info_log,
            "Unable to clear writer for WAL %s with error -- %s",
            LogFileName(immutable_db_options_.GetWalDir(), log_number).c_str(),
            s.ToString().c_str());
        // Retain the first error
        if (ret.ok()) {
          ret = s;
        }
      }
    }
    logs_.clear();
  }

  for (auto& txn_entry : recovered_transactions_) {
    delete txn_entry.second;
  }

  // Return an unowned SstFileManager to a consistent state
  if (immutable_db_options_.sst_file_manager && !own_sfm_) {
    mutex_.Unlock();
    UntrackDataFiles();
    mutex_.Lock();
  }

  // versions need to be destroyed before table_cache since it can hold
  // references to table_cache.
  {
    Status s = versions_->Close(directories_.GetDbDir(), &mutex_);
    if (!s.ok()) {
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "Unable to close MANIFEST with error -- %s",
                      s.ToString().c_str());
      if (ret.ok()) {
        ret = s;
      }
    }
  }

  // Drain obsolete-file purge work started AFTER the early CloseHelper wait
  // near the top of this method. A late SuperVersion cleanup -- a dropped
  // column family handle, or an in-flight iterator/Get whose ReadOptions or
  // immutable_db_options_.avoid_unnecessary_blocking_io selected background
  // purge -- can run in one of the mutex-unlocked windows above and enter the
  // FindObsoleteFiles() -> PurgeObsoleteFiles(..., true) handoff. During that
  // handoff pending_purge_obsolete_files_ is already nonzero, but
  // bg_purge_scheduled_ may still be zero until SchedulePurge() runs at the end
  // of PurgeObsoleteFiles(). Wait for both states while mutex_/this are still
  // alive; bg_cv_.Wait() releases mutex_ so pending and scheduled purges can
  // finish and signal.
  TEST_SYNC_POINT("DBImpl::CloseHelper:BeforeFinalPurgeDrain");
  while (pending_purge_obsolete_files_ || bg_purge_scheduled_) {
    TEST_SYNC_POINT("DBImpl::CloseHelper:FinalPurgeDrainWait");
    bg_cv_.Wait();
  }

  // Table cache may have table handles holding blocks from the block cache.
  // We need to release them before the block cache is destroyed. The block
  // cache may be destroyed inside versions_.reset(), when column family data
  // list is destroyed, so leaving handles in table cache after
  // versions_.reset() may cause issues. Here we clean all unreferenced handles
  // in table cache, and (for certain builds/conditions) assert that no obsolete
  // files are hanging around unreferenced (leak) in the table/blob file cache.
  // Now we assume all user queries have finished, and close-time purge work has
  // settled, so only version set itself can possibly hold the blocks from block
  // cache. After releasing unreferenced handles here, only handles held by
  // version set left and inside versions_.reset(), we will release them. There,
  // we need to make sure every time a handle is released, we erase it from the
  // cache too. By doing that, we can guarantee that after versions_.reset(),
  // table cache is empty so the cache can be safely destroyed.
#ifndef NDEBUG
  TEST_VerifyNoObsoleteFilesCached(/*db_mutex_already_held=*/true);
#endif  // !NDEBUG
  table_cache_->EraseUnRefEntries();

  // The flush initiator was deregistered before background-work teardown.
  // Detach it before ColumnFamilySet destroys its column families one by one;
  // otherwise each removal rebuilds accounting by scanning all survivors.
  versions_->GetColumnFamilySet()->SetFlushInitiator(nullptr);
  versions_.reset();
  mutex_.Unlock();
  if (db_lock_ != nullptr) {
    // TODO: Check for unlock error
    env_->UnlockFile(db_lock_).PermitUncheckedError();
  }

  ROCKS_LOG_INFO(immutable_db_options_.info_log, "Shutdown complete");
  LogFlush(immutable_db_options_.info_log);

  // If the sst_file_manager was allocated by us during DB::Open(), ccall
  // Close() on it before closing the info_log. Otherwise, background thread
  // in SstFileManagerImpl might try to log something
  if (immutable_db_options_.sst_file_manager && own_sfm_) {
    auto sfm = static_cast<SstFileManagerImpl*>(
        immutable_db_options_.sst_file_manager.get());
    sfm->Close();
  }

  if (slow_mutex_logging_) {
    slow_mutex_logging_->stopped.store(true, std::memory_order_relaxed);
  }
  if (immutable_db_options_.info_log && own_info_log_) {
    Status s = immutable_db_options_.info_log->Close();
    if (!s.ok() && !s.IsNotSupported() && ret.ok()) {
      ret = s;
    }
  }

  if (write_buffer_manager_ && wbm_stall_) {
    write_buffer_manager_->RemoveDBFromQueue(wbm_stall_.get());
  }

  IOStatus io_s = directories_.Close(IOOptions(), nullptr /* dbg */);
  if (!io_s.ok()) {
    ret = io_s;
  }
  if (ret.IsAborted()) {
    // Reserve IsAborted() error for those where users didn't release
    // certain resource and they can release them and come back and
    // retry. In this case, we wrap this exception to something else.
    return Status::Incomplete(ret.ToString());
  }

  return ret;
}

Status DBImpl::CloseImpl() { return CloseHelper(); }

DBImpl::~DBImpl() {
  ThreadStatus::OperationType cur_op_type =
      ThreadStatusUtil::GetThreadOperation();
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OperationType::OP_UNKNOWN);

  // TODO: remove this.
  init_logger_creation_s_.PermitUncheckedError();

  SlowMutexLoggingPause pause_logging(slow_mutex_logging_);
  InstrumentedMutexLock closing_lock_guard(&closing_mutex_);
  if (!closed_) {
    closed_ = true;

    {
      const Status s = MaybeReleaseTimestampedSnapshotsAndCheck();
      s.PermitUncheckedError();
    }

    closing_status_ = DBImpl::CloseImpl();
    closing_status_.PermitUncheckedError();
  }
  ThreadStatusUtil::SetThreadOperation(cur_op_type);
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || immutable_db_options_.paranoid_checks) {
    // No change needed
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log, "Ignoring error %s",
                   s->ToString().c_str());
    *s = Status::OK();
  }
}

const Status DBImpl::CreateArchivalDirectory() {
  if (immutable_db_options_.WAL_ttl_seconds > 0 ||
      immutable_db_options_.WAL_size_limit_MB > 0) {
    std::string archivalPath =
        ArchivalDirectory(immutable_db_options_.GetWalDir());
    return env_->CreateDirIfMissing(archivalPath);
  }
  return Status::OK();
}

void DBImpl::PrintStatistics() {
  auto dbstats = immutable_db_options_.stats;
  if (dbstats) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "STATISTICS:\n %s",
                   dbstats->ToString().c_str());
  }
}

// Computes the minimum time-based compaction interval for a CF based on
// various options.
// Returns 0 if all time-based compaction options are disabled.
static uint64_t GetMinTimeBasedCompactionInterval(
    const ColumnFamilyOptions& cf_opts) {
  uint64_t min_interval = UINT64_MAX;
  if (cf_opts.periodic_compaction_seconds > 0) {
    min_interval = std::min(min_interval, cf_opts.periodic_compaction_seconds);
  }
  if (cf_opts.ttl > 0) {
    min_interval = std::min(min_interval, cf_opts.ttl);
  }
  const auto& fifo_thresholds =
      cf_opts.compaction_options_fifo.file_temperature_age_thresholds;
  if (!fifo_thresholds.empty() && fifo_thresholds[0].age > 0) {
    // Thresholds are in increasing order by age, so first is smallest
    min_interval = std::min(min_interval, fifo_thresholds[0].age);
  }
  if (cf_opts.bottommost_file_compaction_delay > 0) {
    // NOTE: 0 does not exactly mean "disabled" in this case but it does mean
    // there's no time component to the relevant compaction picking.
    min_interval = std::min(min_interval,
                            uint64_t{cf_opts.bottommost_file_compaction_delay});
  }
  // Note: Assume sentinel values like UINT64_MAX - 1 (used by ttl and
  // periodic_compaction_seconds) are like disabling, if they reach here
  // unsanitized.
  return min_interval > UINT64_MAX / 2 ? 0 : min_interval;
}

uint64_t DBImpl::ComputeTriggerCompactionPeriod() {
  // Start with the configured maximum, then reduce based on other options.
  uint64_t period_sec =
      mutable_db_options_.max_compaction_trigger_wakeup_seconds;

  // Consider DB-level options that have the DB waking up periodically anyway.
  // Waking up to check for compactions at the same interval should be no
  // problem, as it should be less overhead than these.
  if (mutable_db_options_.stats_dump_period_sec > 0) {
    period_sec = std::min(period_sec,
                          (uint64_t)mutable_db_options_.stats_dump_period_sec);
  }
  if (mutable_db_options_.stats_persist_period_sec > 0) {
    period_sec = std::min(
        period_sec, (uint64_t)mutable_db_options_.stats_persist_period_sec);
  }

  // Consider per-CF settings that can trigger compaction based on time.
  uint64_t compaction_trigger_sec = UINT64_MAX;
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    if (cfd->IsDropped()) {
      continue;
    }
    uint64_t cf_min =
        GetMinTimeBasedCompactionInterval(cfd->GetLatestCFOptions());
    if (cf_min > 0) {
      compaction_trigger_sec = std::min(compaction_trigger_sec, cf_min);
    }
  }
  // We might not align with those timings perfectly, so we tolerate only some
  // proportional delay, up to 1/kTriggerDivisor ~= 20%.
  constexpr uint64_t kTriggerDivisor = 5;
  period_sec = std::min(period_sec, compaction_trigger_sec / kTriggerDivisor);
  // But must be > 0
  period_sec = std::max(period_sec, uint64_t{1});

  return period_sec;
}

Status DBImpl::StartPeriodicTaskScheduler() {
  Status s;
#ifndef NDEBUG
  // It only used by test to disable scheduler
  bool disable_scheduler = false;
  TEST_SYNC_POINT_CALLBACK(
      "DBImpl::StartPeriodicTaskScheduler:DisableScheduler",
      &disable_scheduler);
  if (disable_scheduler) {
    return s;
  }

  {
    InstrumentedMutexLock l(&mutex_);
    TEST_SYNC_POINT_CALLBACK("DBImpl::StartPeriodicTaskScheduler:Init",
                             &periodic_task_scheduler_);
  }

#endif  // !NDEBUG
  if (mutable_db_options_.stats_dump_period_sec > 0) {
    s = periodic_task_scheduler_.Register(
        PeriodicTaskType::kDumpStats,
        periodic_task_functions_.at(PeriodicTaskType::kDumpStats),
        mutable_db_options_.stats_dump_period_sec,
        /*run_immediately=*/true);
    if (!s.ok()) {
      return s;
    }
  }
  if (mutable_db_options_.stats_persist_period_sec > 0) {
    s = periodic_task_scheduler_.Register(
        PeriodicTaskType::kPersistStats,
        periodic_task_functions_.at(PeriodicTaskType::kPersistStats),
        mutable_db_options_.stats_persist_period_sec,
        /*run_immediately=*/true);
    if (!s.ok()) {
      return s;
    }
  }

  s = periodic_task_scheduler_.Register(
      PeriodicTaskType::kFlushInfoLog,
      periodic_task_functions_.at(PeriodicTaskType::kFlushInfoLog),
      /*run_immediately=*/true);
  if (!s.ok()) {
    return s;
  }

  s = periodic_task_scheduler_.Register(
      PeriodicTaskType::kTriggerCompaction,
      periodic_task_functions_.at(PeriodicTaskType::kTriggerCompaction),
      ComputeTriggerCompactionPeriod(), /*run_immediately=*/false);

  return s;
}

Status DBImpl::RegisterRecordSeqnoTimeWorker() {
  options_mutex_.AssertHeld();

  // We assume InstallSuperVersionForConfigChange has already ensured suitable
  // mappings are present for each relevant CF. We just need to be sure the DB's
  // seqno_to_time_mapping_ and worker scheduler are appropriate for the
  // combination of CF settings.

  MinAndMaxPreserveSeconds preserve_info;
  uint64_t seqno_time_cadence;
  {
    InstrumentedMutexLock l(&mutex_);

    for (auto cfd : *versions_->GetColumnFamilySet()) {
      auto& mopts = cfd->GetLatestMutableCFOptions();
      if (!cfd->IsDropped()) {
        preserve_info.Combine(mopts);
      }
    }
    seqno_time_cadence = preserve_info.GetRecodingCadence();
    if (seqno_time_cadence == 0) {
      // To return as much as possible to the feature being disabled,
      // clear the existing mapping
      seqno_to_time_mapping_.SetCapacity(0);
      seqno_to_time_mapping_.SetMaxTimeSpan(UINT64_MAX);
      assert(seqno_to_time_mapping_.Empty());
    } else {
      uint64_t cap = std::min(kMaxSeqnoToTimeEntries,
                              preserve_info.max_preserve_seconds *
                                  kMaxSeqnoTimePairsPerCF /
                                  preserve_info.min_preserve_seconds);
      seqno_to_time_mapping_.SetCapacity(cap);
      seqno_to_time_mapping_.SetMaxTimeSpan(preserve_info.max_preserve_seconds);
    }
  }

  TEST_SYNC_POINT_CALLBACK(
      "DBImpl::RegisterRecordSeqnoTimeWorker:BeforePeriodicTaskType", nullptr);

  Status s;
  if (seqno_time_cadence == 0) {
    s = periodic_task_scheduler_.Unregister(PeriodicTaskType::kRecordSeqnoTime);
  } else {
    s = periodic_task_scheduler_.Register(
        PeriodicTaskType::kRecordSeqnoTime,
        periodic_task_functions_.at(PeriodicTaskType::kRecordSeqnoTime),
        seqno_time_cadence, /*run_immediately=*/true);
  }

  return s;
}

Status DBImpl::CancelPeriodicTaskScheduler() {
  Status s = Status::OK();
  for (uint8_t task_type = 0;
       task_type < static_cast<uint8_t>(PeriodicTaskType::kMax); task_type++) {
    s = periodic_task_scheduler_.Unregister(
        static_cast<PeriodicTaskType>(task_type));
    if (!s.ok()) {
      ROCKS_LOG_WARN(immutable_db_options_.info_log,
                     "Failed to unregister periodic task %d, status: %s",
                     task_type, s.ToString().c_str());
    }
  }
  return s;
}

// esitmate the total size of stats_history_
size_t DBImpl::EstimateInMemoryStatsHistorySize() const {
  stats_history_mutex_.AssertHeld();
  size_t size_total =
      sizeof(std::map<uint64_t, std::map<std::string, uint64_t>>);
  if (stats_history_.size() == 0) {
    return size_total;
  }
  size_t size_per_slice =
      sizeof(uint64_t) + sizeof(std::map<std::string, uint64_t>);
  // non-empty map, stats_history_.begin() guaranteed to exist
  for (const auto& pairs : stats_history_.begin()->second) {
    size_per_slice +=
        pairs.first.capacity() + sizeof(std::string) + sizeof(pairs.second);
  }
  size_total = size_per_slice * stats_history_.size();
  return size_total;
}

void DBImpl::PersistStats() {
  TEST_SYNC_POINT("DBImpl::PersistStats:Entry");
  if (shutdown_initiated_) {
    return;
  }
  TEST_SYNC_POINT("DBImpl::PersistStats:StartRunning");
  uint64_t now_seconds =
      immutable_db_options_.clock->NowMicros() / kMicrosInSecond;

  Statistics* statistics = immutable_db_options_.stats;
  if (!statistics) {
    return;
  }
  size_t stats_history_size_limit = 0;
  {
    InstrumentedMutexLock l(&mutex_);
    stats_history_size_limit = mutable_db_options_.stats_history_buffer_size;
  }

  std::map<std::string, uint64_t> stats_map;
  if (!statistics->getTickerMap(&stats_map)) {
    return;
  }
  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "------- PERSISTING STATS -------");

  if (immutable_db_options_.persist_stats_to_disk) {
    WriteBatch batch;
    Status s = Status::OK();
    if (stats_slice_initialized_) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "Reading %" ROCKSDB_PRIszt " stats from statistics\n",
                     stats_slice_.size());
      for (const auto& stat : stats_map) {
        if (s.ok()) {
          char key[100];
          int length =
              EncodePersistentStatsKey(now_seconds, stat.first, 100, key);
          // calculate the delta from last time
          if (stats_slice_.find(stat.first) != stats_slice_.end()) {
            uint64_t delta = stat.second - stats_slice_[stat.first];
            s = batch.Put(persist_stats_cf_handle_,
                          Slice(key, std::min(100, length)),
                          std::to_string(delta));
          }
        }
      }
    }
    stats_slice_initialized_ = true;
    std::swap(stats_slice_, stats_map);
    if (s.ok()) {
      // TODO: plumb Env::IOActivity, Env::IOPriority
      WriteOptions wo;
      wo.low_pri = true;
      wo.no_slowdown = true;
      wo.sync = false;
      s = Write(wo, &batch);
    }
    if (!s.ok()) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "Writing to persistent stats CF failed -- %s",
                     s.ToString().c_str());
    } else {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "Writing %" ROCKSDB_PRIszt " stats with timestamp %" PRIu64
                     " to persistent stats CF succeeded",
                     stats_slice_.size(), now_seconds);
    }
    // TODO(Zhongyi): add purging for persisted data
  } else {
    InstrumentedMutexLock l(&stats_history_mutex_);
    // calculate the delta from last time
    if (stats_slice_initialized_) {
      std::map<std::string, uint64_t> stats_delta;
      for (const auto& stat : stats_map) {
        if (stats_slice_.find(stat.first) != stats_slice_.end()) {
          stats_delta[stat.first] = stat.second - stats_slice_[stat.first];
        }
      }
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "Storing %" ROCKSDB_PRIszt " stats with timestamp %" PRIu64
                     " to in-memory stats history",
                     stats_slice_.size(), now_seconds);
      stats_history_[now_seconds] = std::move(stats_delta);
    }
    stats_slice_initialized_ = true;
    std::swap(stats_slice_, stats_map);
    TEST_SYNC_POINT("DBImpl::PersistStats:StatsCopied");

    // delete older stats snapshots to control memory consumption
    size_t stats_history_size = EstimateInMemoryStatsHistorySize();
    bool purge_needed = stats_history_size > stats_history_size_limit;
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[Pre-GC] In-memory stats history size: %" ROCKSDB_PRIszt
                   " bytes, slice count: %" ROCKSDB_PRIszt,
                   stats_history_size, stats_history_.size());
    while (purge_needed && !stats_history_.empty()) {
      stats_history_.erase(stats_history_.begin());
      purge_needed =
          EstimateInMemoryStatsHistorySize() > stats_history_size_limit;
    }
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[Post-GC] In-memory stats history size: %" ROCKSDB_PRIszt
                   " bytes, slice count: %" ROCKSDB_PRIszt,
                   stats_history_size, stats_history_.size());
  }
  TEST_SYNC_POINT("DBImpl::PersistStats:End");
}

bool DBImpl::FindStatsByTime(uint64_t start_time, uint64_t end_time,
                             uint64_t* new_time,
                             std::map<std::string, uint64_t>* stats_map) {
  assert(new_time);
  assert(stats_map);
  if (!new_time || !stats_map) {
    return false;
  }
  // lock when search for start_time
  {
    InstrumentedMutexLock l(&stats_history_mutex_);
    auto it = stats_history_.lower_bound(start_time);
    if (it != stats_history_.end() && it->first < end_time) {
      // make a copy for timestamp and stats_map
      *new_time = it->first;
      *stats_map = it->second;
      return true;
    } else {
      return false;
    }
  }
}

Status DBImpl::GetStatsHistory(
    uint64_t start_time, uint64_t end_time,
    std::unique_ptr<StatsHistoryIterator>* stats_iterator) {
  if (!stats_iterator) {
    return Status::InvalidArgument("stats_iterator not preallocated.");
  }
  if (immutable_db_options_.persist_stats_to_disk) {
    stats_iterator->reset(
        new PersistentStatsHistoryIterator(start_time, end_time, this));
  } else {
    stats_iterator->reset(
        new InMemoryStatsHistoryIterator(start_time, end_time, this));
  }
  return (*stats_iterator)->status();
}

void DBImpl::DumpStats() {
  TEST_SYNC_POINT("DBImpl::DumpStats:1");
  std::string stats;
  if (shutdown_initiated_) {
    return;
  }

  // Also probe block cache(s) for problems, dump to info log
  UnorderedSet<Cache*> probed_caches;
  TEST_SYNC_POINT("DBImpl::DumpStats:StartRunning");
  {
    InstrumentedMutexLock l(&mutex_);
    for (auto cfd : versions_->GetRefedColumnFamilySet()) {
      if (!cfd->initialized() || cfd->IsDropped()) {
        continue;
      }

      auto* table_factory =
          cfd->GetCurrentMutableCFOptions().table_factory.get();
      assert(table_factory != nullptr);
      // FIXME: need to a shared_ptr if/when block_cache is going to be mutable
      Cache* cache =
          table_factory->GetOptions<Cache>(TableFactory::kBlockCacheOpts());

      // Release DB mutex for gathering cache entry stats. Pass over all
      // column families for this first so that other stats are dumped
      // near-atomically.
      InstrumentedMutexUnlock u(&mutex_);
      cfd->internal_stats()->CollectCacheEntryStats(/*foreground=*/false);

      // Probe block cache for problems (if not already via another CF)
      if (immutable_db_options_.info_log) {
        if (cache && probed_caches.insert(cache).second) {
          cache->ReportProblems(immutable_db_options_.info_log);
        }
      }
    }

    const std::string* property = &DB::Properties::kDBStats;
    const DBPropertyInfo* property_info = GetPropertyInfo(*property);
    assert(property_info != nullptr);
    assert(!property_info->need_out_of_mutex);
    default_cf_internal_stats_->GetStringProperty(*property_info, *property,
                                                  &stats);

    property = &InternalStats::kPeriodicCFStats;
    property_info = GetPropertyInfo(*property);
    assert(property_info != nullptr);
    assert(!property_info->need_out_of_mutex);
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      if (cfd->initialized()) {
        cfd->internal_stats()->GetStringProperty(*property_info, *property,
                                                 &stats);
      }
    }
  }
  TEST_SYNC_POINT("DBImpl::DumpStats:2");
  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "------- DUMPING STATS -------");
  ROCKS_LOG_INFO(immutable_db_options_.info_log, "%s", stats.c_str());
  if (immutable_db_options_.dump_malloc_stats) {
    stats.clear();
    DumpMallocStats(&stats);
    if (!stats.empty()) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "------- Malloc STATS -------");
      ROCKS_LOG_INFO(immutable_db_options_.info_log, "%s", stats.c_str());
    }
  }

  PrintStatistics();
}

// Periodically flush info log out of application buffer at a low frequency.
// This improves debuggability in case of RocksDB hanging since it ensures the
// log messages leading up to the hang will eventually become visible in the
// log.
void DBImpl::FlushInfoLog() {
  if (shutdown_initiated_) {
    return;
  }
  TEST_SYNC_POINT("DBImpl::FlushInfoLog:StartRunning");
  LogFlush(immutable_db_options_.info_log);
}

Status DBImpl::TablesRangeTombstoneSummary(ColumnFamilyHandle* column_family,
                                           int max_entries_to_print,
                                           std::string* out_str) {
  auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  ColumnFamilyData* cfd = cfh->cfd();

  SuperVersion* super_version = cfd->GetReferencedSuperVersion(this);
  Version* version = super_version->current;

  Status s =
      version->TablesRangeTombstoneSummary(max_entries_to_print, out_str);

  CleanupSuperVersion(super_version);
  return s;
}

void DBImpl::ScheduleBgLogWriterClose(JobContext* job_context) {
  mutex_.AssertHeld();
  if (!job_context->wals_to_free.empty()) {
    for (auto l : job_context->wals_to_free) {
      AddToLogsToFreeQueue(l);
    }
    job_context->wals_to_free.clear();
  }
}

FSDirectory* DBImpl::GetDataDir(ColumnFamilyData* cfd, size_t path_id) const {
  assert(cfd);
  FSDirectory* ret_dir = cfd->GetDataDir(path_id);
  if (ret_dir == nullptr) {
    return directories_.GetDataDir(path_id);
  }
  return ret_dir;
}

Status DBImpl::SetOptions(
    const std::unordered_map<ColumnFamilyHandle*,
                             std::unordered_map<std::string, std::string>>&
        column_families_opts_map) {
  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;
  const WriteOptions write_options;

  if (column_families_opts_map.empty()) {
    return Status::OK();
  }

  for (const auto& cf_opts : column_families_opts_map) {
    if (cf_opts.second.empty()) {
      ROCKS_LOG_WARN(immutable_db_options_.info_log,
                     "SetOptions() on column family [%s], empty input",
                     cf_opts.first->GetName().c_str());
      return Status::InvalidArgument("empty input");
    }
  }

  autovector<std::pair<ColumnFamilyData*,
                       const std::unordered_map<std::string, std::string>*>>
      column_family_datas;
  for (const auto& cf_opts : column_families_opts_map) {
    column_family_datas.push_back(
        {static_cast_with_check<ColumnFamilyHandleImpl>(cf_opts.first)->cfd(),
         &cf_opts.second});
  }

  DeferredInstrumentedMutexLock ol(
      &options_mutex_,
      stats_ != nullptr && immutable_db_options_.info_log != nullptr);
  autovector<MutableCFOptions>
      new_options_copy;  // For logging outside of DB mutex
  Status s;
  Status persist_options_status;
  SuperVersionContext sv_context(/* create_superversion */ true);
  {
    auto db_options = GetDBOptions();
    InstrumentedMutexLock l(&mutex_);
    // Manifest writers + Version appenders like flush and compaction use
    // LogAndApply, which releases DB mutex to wait for other manifest writers
    // and for the manifest write. We need to append a Version for the options
    // to take full effect (e.g. compaction scores), but we don't want to
    // interleave with other callers of LogAndApply, which could at least
    // temporarily roll back option changes. Thus, we use a special call to
    // LogAndApply that allows us to
    //
    // (a) Apply the options update when we know we are the exclusive version
    // appender + (fake) manifest writer, and
    //
    // (b) Append a new Version without manifest write nor DB mutex release
    //
    // Thus aren't releasing the DB mutex from LogAndApply calling pre_cb,
    // through installing the new Version until the end of this block, after
    // installing the new SuperVersion.
    VersionEdit dummy_edit;
    dummy_edit.MarkNoManifestWriteDummy();
    TEST_SYNC_POINT_CALLBACK("DBImpl::SetOptions:dummy_edit", &dummy_edit);
    // If any CF is changing periodic_compaction_seconds, (re)anchor phasing to
    // now BEFORE the new Version is built below, so the new Version's scoring
    // spreads a turn-down's newly past-due cohort over the phase grid (within
    // ~N/4 of now) instead of firing it all at once (a herd). Anchoring is
    // DB-level and benign to CFs that are not changing their interval.
    bool changing_periodic_compaction_seconds = false;
    for (const auto& cfd_opts : column_family_datas) {
      if (cfd_opts.second->count("periodic_compaction_seconds") > 0) {
        changing_periodic_compaction_seconds = true;
        break;
      }
    }
    if (changing_periodic_compaction_seconds) {
      versions_->ReanchorCompactionPhase();
    }
    for (const auto& cfd_opts : column_family_datas) {
      auto* cfd = cfd_opts.first;
      const auto* options_map_ptr = cfd_opts.second;
      auto pre_cb = [&]() -> Status {
        Status cb_s = cfd->SetOptions(db_options, *options_map_ptr);
        if (cb_s.ok()) {
          new_options_copy.emplace_back(cfd->GetLatestMutableCFOptions());
        }
        return cb_s;
      };

      s = versions_->LogAndApply(
          cfd, read_options, write_options, &dummy_edit, &mutex_,
          directories_.GetDbDir(), false /*new_descriptor_log=*/,
          nullptr /*new_opts*/, {} /*manifest_wcb*/, pre_cb);
      if (!versions_->io_status().ok()) {
        assert(!s.ok());
        error_handler_.SetBGError(versions_->io_status(),
                                  BackgroundErrorReason::kManifestWrite);
      }
      if (!s.ok()) {
        break;
      }
    }

    if (s.ok()) {
      // Trigger possible flush/compactions. This has to be before we persist
      // options to file, otherwise there will be a deadlock with writer
      // thread.
      for (const auto& cfd_opts : column_family_datas) {
        auto* cfd = cfd_opts.first;
        InstallSuperVersionForConfigChange(cfd, &sv_context);
        if (auto* blob_partition_manager = cfd->blob_partition_manager()) {
          blob_partition_manager->SetBlobWriterMaxBufferSize(
              cfd->GetLatestMutableCFOptions()
                  .blob_file_writable_file_max_buffer_size);
        }
      }

      persist_options_status =
          WriteOptionsFile(write_options, true /*db_mutex_already_held*/);
      bg_cv_.SignalAll();

#ifndef NDEBUG
      for (size_t i = 0; i < column_family_datas.size(); ++i) {
        auto* cfd = column_family_datas[i].first;
        assert(new_options_copy[i] == cfd->GetLatestMutableCFOptions());
        assert(cfd->GetLatestMutableCFOptions() ==
               cfd->GetCurrentMutableCFOptions());
        assert(cfd->GetCurrentMutableCFOptions() ==
               cfd->current()->GetMutableCFOptions());
      }
#endif
    }
  }
  sv_context.Clean();

  if (s.ok()) {
    bool needs_seqno_worker = false;
    for (const auto& cf_opts : column_families_opts_map) {
      if (cf_opts.second.count("preserve_internal_time_seconds") > 0 ||
          cf_opts.second.count("preclude_last_level_data_seconds") > 0) {
        needs_seqno_worker = true;
        break;
      }
    }
    if (needs_seqno_worker) {
      s = RegisterRecordSeqnoTimeWorker();
    }
  }

  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "SetOptions() on [%zu] column families, inputs:",
                 column_family_datas.size());
  for (size_t i = 0; i < column_family_datas.size(); ++i) {
    const auto* cfd = column_family_datas[i].first;
    const auto* options_map_ptr = column_family_datas[i].second;
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Set options on column family [%s] (%zu/%zu), inputs:",
                   cfd->GetName().c_str(), i, column_family_datas.size());
    for (const auto& o : *options_map_ptr) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log, "%s: %s\n",
                     o.first.c_str(), o.second.c_str());
    }
  }
  if (s.ok()) {
    for (size_t i = 0; i < column_family_datas.size(); ++i) {
      const auto* cfd = column_family_datas[i].first;
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "Set options on column family [%s] (%zu/%zu) succeeded, "
                     "updated CF options:",
                     cfd->GetName().c_str(), i, column_family_datas.size());
      new_options_copy[i].Dump(immutable_db_options_.info_log.get());
    }
    if (!persist_options_status.ok()) {
      // NOTE: WriteOptionsFile already logs on failure
      s = persist_options_status;
    }
  } else {
    persist_options_status.PermitUncheckedError();  // less important
    ROCKS_LOG_WARN(immutable_db_options_.info_log, "SetOptions() failed: %s",
                   s.ToString().c_str());
  }
  LogFlush(immutable_db_options_.info_log);
  return s;
}

Status DBImpl::SetDBOptions(
    const std::unordered_map<std::string, std::string>& options_map) {
  if (options_map.empty()) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "SetDBOptions(), empty input.");
    return Status::InvalidArgument("empty input");
  }

  DeferredInstrumentedMutexLock ol(
      &options_mutex_,
      stats_ != nullptr && immutable_db_options_.info_log != nullptr);
  MutableDBOptions new_options;
  Status s;
  Status persist_options_status = Status::OK();
  bool wal_size_option_changed = false;
  bool wal_other_option_changed = false;
  WriteContext write_context;
  {
    InstrumentedMutexLock l(&mutex_);
    s = GetMutableDBOptionsFromStrings(mutable_db_options_, options_map,
                                       &new_options);

    if (new_options.bytes_per_sync == 0) {
      new_options.bytes_per_sync = 1024 * 1024;
    }

    if (MutableDBOptionsAreEqual(mutable_db_options_, new_options)) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "SetDBOptions(), input option value is not changed, "
                     "skipping updating.");
      persist_options_status.PermitUncheckedError();
      return s;
    }

    DBOptions new_db_options =
        BuildDBOptions(immutable_db_options_, new_options);
    if (s.ok()) {
      s = ValidateOptions(new_db_options);
    }
    if (s.ok()) {
      for (auto c : *versions_->GetColumnFamilySet()) {
        if (!c->IsDropped()) {
          auto cf_options = c->GetLatestCFOptions();
          s = ColumnFamilyData::ValidateOptions(new_db_options, cf_options);
          if (!s.ok()) {
            break;
          }
        }
      }
    }
    if (s.ok()) {
      const BGJobLimits current_bg_job_limits =
          GetBGJobLimits(mutable_db_options_.max_background_flushes,
                         mutable_db_options_.max_background_compactions,
                         mutable_db_options_.max_background_jobs,
                         /* parallelize_compactions */ true);
      const BGJobLimits new_bg_job_limits = GetBGJobLimits(
          new_options.max_background_flushes,
          new_options.max_background_compactions,
          new_options.max_background_jobs, /* parallelize_compactions */ true);

      const bool max_flushes_increased =
          new_bg_job_limits.max_flushes > current_bg_job_limits.max_flushes;
      const bool max_compactions_increased =
          new_bg_job_limits.max_compactions >
          current_bg_job_limits.max_compactions;
      const bool offpeak_time_changed =
          versions_->offpeak_time_option().daily_offpeak_time_utc !=
          new_db_options.daily_offpeak_time_utc;

      if (max_flushes_increased || max_compactions_increased ||
          offpeak_time_changed) {
        if (max_flushes_increased) {
          env_->IncBackgroundThreadsIfNeeded(new_bg_job_limits.max_flushes,
                                             Env::Priority::HIGH);
        }
        if (max_compactions_increased) {
          env_->IncBackgroundThreadsIfNeeded(new_bg_job_limits.max_compactions,
                                             Env::Priority::LOW);
        }
        if (offpeak_time_changed) {
          versions_->ChangeOffpeakTimeOption(
              new_db_options.daily_offpeak_time_utc);
        }

        MaybeScheduleFlushOrCompaction();
      }

      mutex_.Unlock();
      if (new_options.stats_dump_period_sec == 0) {
        s = periodic_task_scheduler_.Unregister(PeriodicTaskType::kDumpStats);
      } else {
        s = periodic_task_scheduler_.Register(
            PeriodicTaskType::kDumpStats,
            periodic_task_functions_.at(PeriodicTaskType::kDumpStats),
            new_options.stats_dump_period_sec, /*run_immediately=*/true);
      }
      if (new_options.max_total_wal_size !=
          mutable_db_options_.max_total_wal_size) {
        max_total_wal_size_.store(new_options.max_total_wal_size,
                                  std::memory_order_release);
      }
      if (s.ok()) {
        if (new_options.stats_persist_period_sec == 0) {
          s = periodic_task_scheduler_.Unregister(
              PeriodicTaskType::kPersistStats);
        } else {
          s = periodic_task_scheduler_.Register(
              PeriodicTaskType::kPersistStats,
              periodic_task_functions_.at(PeriodicTaskType::kPersistStats),
              new_options.stats_persist_period_sec, /*run_immediately=*/true);
        }
      }
      mutex_.Lock();
      if (!s.ok()) {
        return s;
      }

      write_controller_.set_max_delayed_write_rate(
          new_options.delayed_write_rate);
      table_cache_.get()->SetCapacity(new_options.max_open_files == -1
                                          ? TableCache::kInfiniteCapacity
                                          : new_options.max_open_files - 10);
      // Potential table cache capacity change requires updating if table
      // handles should get pinned.
      versions_->GetColumnFamilySet()->SetFastSstOpen(
          new_options.fast_sst_open);
      for (auto cfd : *versions_->GetColumnFamilySet()) {
        if (!cfd->IsDropped()) {
          cfd->table_cache()->UpdateShouldPinTableHandles();
          cfd->table_cache()->SetFastSstOpen(new_options.fast_sst_open);
        }
      }
      wal_other_option_changed = mutable_db_options_.wal_bytes_per_sync !=
                                 new_options.wal_bytes_per_sync;
      wal_size_option_changed = mutable_db_options_.max_total_wal_size !=
                                new_options.max_total_wal_size;
      mutable_db_options_ = new_options;
      mutex_hold_log_threshold_us_.store(
          new_options.mutex_hold_log_threshold_us, std::memory_order_relaxed);
      file_options_for_compaction_ = FileOptions(new_db_options);
      file_options_for_compaction_ = fs_->OptimizeForCompactionTableWrite(
          file_options_for_compaction_, immutable_db_options_);
      versions_->UpdatedMutableDbOptions(mutable_db_options_, &mutex_);
      // TODO(xiez): clarify why apply optimize for read to write options
      file_options_for_compaction_ = fs_->OptimizeForCompactionTableRead(
          file_options_for_compaction_, immutable_db_options_);
      TEST_SYNC_POINT_CALLBACK("DBImpl::SetDBOptions:FileOptionsForCompaction",
                               &file_options_for_compaction_);
      if (wal_other_option_changed || wal_size_option_changed) {
        WriteThread::Writer w;
        write_thread_.EnterUnbatched(&w, &mutex_);
        if (wal_other_option_changed ||
            wals_total_size_.LoadRelaxed() > GetMaxTotalWalSize()) {
          Status purge_wal_status = SwitchWAL(&write_context);
          if (!purge_wal_status.ok()) {
            ROCKS_LOG_WARN(immutable_db_options_.info_log,
                           "Unable to purge WAL files in SetDBOptions() -- %s",
                           purge_wal_status.ToString().c_str());
          }
        }
        write_thread_.ExitUnbatched(&w);
      }
      persist_options_status =
          WriteOptionsFile(WriteOptions(), true /*db_mutex_already_held*/);
    } else {
      // To get here, we must have had invalid options and will not attempt to
      // persist the options, which means the status is "OK/Uninitialized.
      persist_options_status.PermitUncheckedError();
    }
  }
  ROCKS_LOG_INFO(immutable_db_options_.info_log, "SetDBOptions(), inputs:");
  for (const auto& o : options_map) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "%s: %s\n", o.first.c_str(),
                   o.second.c_str());
  }
  if (s.ok()) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "SetDBOptions() succeeded");
    new_options.Dump(immutable_db_options_.info_log.get());
    if (!persist_options_status.ok()) {
      s = Status::IOError(
          "SetDBOptions() succeeded, but unable to persist options",
          persist_options_status.ToString());
    }
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log, "SetDBOptions failed");
  }
  LogFlush(immutable_db_options_.info_log);
  return s;
}

// return the same level if it cannot be moved
int DBImpl::FindMinimumEmptyLevelFitting(
    ColumnFamilyData* cfd, const MutableCFOptions& /*mutable_cf_options*/,
    int level) {
  mutex_.AssertHeld();
  const auto* vstorage = cfd->current()->storage_info();
  int minimum_level = level;
  for (int i = level - 1; i > 0; --i) {
    // stop if level i is not empty
    if (vstorage->NumLevelFiles(i) > 0) {
      break;
    }
    // stop if level i is too small (cannot fit the level files)
    if (vstorage->MaxBytesForLevel(i) < vstorage->NumLevelBytes(level)) {
      break;
    }

    minimum_level = i;
  }
  return minimum_level;
}

Status DBImpl::FlushWAL(const FlushWALOptions& options) {
  WriteOptions write_options;
  write_options.rate_limiter_priority = options.rate_limiter_priority;
  return FlushWAL(write_options, options.sync);
}

Status DBImpl::FlushWAL(const WriteOptions& write_options, bool sync) {
  if (manual_wal_flush_) {
    IOStatus io_s;
    {
      // We need to lock wal_write_mutex_ since logs_ might change concurrently
      InstrumentedMutexLock wl(&wal_write_mutex_);
      log::Writer* cur_log_writer = logs_.back().writer;
      io_s = cur_log_writer->WriteBuffer(write_options);
    }
    if (!io_s.ok()) {
      ROCKS_LOG_ERROR(immutable_db_options_.info_log, "WAL flush error %s",
                      io_s.ToString().c_str());
      // In case there is a fs error we should set it globally to prevent the
      // future writes
      WALIOStatusCheck(io_s);
      // whether sync or not, we should abort the rest of function upon error
      return static_cast<Status>(io_s);
    }
    if (!sync) {
      ROCKS_LOG_DEBUG(immutable_db_options_.info_log, "FlushWAL sync=false");
      return static_cast<Status>(io_s);
    }
  }
  if (!sync) {
    return Status::OK();
  }
  // sync = true
  ROCKS_LOG_DEBUG(immutable_db_options_.info_log, "FlushWAL sync=true");
  return SyncWAL();
}

bool DBImpl::WALBufferIsEmpty() {
  InstrumentedMutexLock l(&wal_write_mutex_);
  log::Writer* cur_log_writer = logs_.back().writer;
  auto res = cur_log_writer->BufferIsEmpty();
  return res;
}

Status DBImpl::GetOpenWalSizes(std::map<uint64_t, uint64_t>& number_to_size) {
  assert(number_to_size.empty());
  InstrumentedMutexLock l(&wal_write_mutex_);
  for (auto& log : logs_) {
    auto* open_file = log.writer->file();
    if (open_file) {
      number_to_size[log.number] = open_file->GetFlushedSize();
    }
  }
  return Status::OK();
}

Status DBImpl::SyncWAL() {
  TEST_SYNC_POINT("DBImpl::SyncWAL:Begin");
  WriteOptions write_options;
  VersionEdit synced_wals;
  Status s = SyncWalImpl(/*include_current_wal=*/true, write_options,
                         /*job_context=*/nullptr, &synced_wals,
                         /*error_recovery_in_prog=*/false);

  if (s.ok() && synced_wals.IsWalAddition()) {
    InstrumentedMutexLock l(&mutex_);
    // TODO: plumb Env::IOActivity, Env::IOPriority
    const ReadOptions read_options;
    s = ApplyWALToManifest(read_options, write_options, &synced_wals);
  }

  if (s.ok() && wal_recovery_cutoff_.load(std::memory_order_acquire) != 0) {
    InstrumentedMutexLock l(&mutex_);
    Status bg_error = error_handler_.GetBGError();
    if (!bg_error.ok()) {
      s = bg_error;
    }
  }

  TEST_SYNC_POINT("DBImpl::SyncWAL:BeforeMarkLogsSynced:2");
  return s;
}

IOStatus DBImpl::SyncWalImpl(bool include_current_wal,
                             const WriteOptions& write_options,
                             JobContext* job_context, VersionEdit* synced_wals,
                             bool error_recovery_in_prog) {
  autovector<log::Writer*, 1> wals_to_sync;
  bool need_wal_dir_sync;
  // Number of a WAL that was active at the start of call and maybe is by
  // the end of the call.
  uint64_t maybe_active_number;
  // Sync WALs up to this number
  uint64_t up_to_number;

  {
    InstrumentedMutexLock l(&wal_write_mutex_);
    assert(!logs_.empty());

    maybe_active_number = cur_wal_number_;
    up_to_number =
        include_current_wal ? maybe_active_number : maybe_active_number - 1;

    while (logs_.front().number <= up_to_number && logs_.front().IsSyncing()) {
      wal_sync_cv_.Wait();
    }
    // First check that logs are safe to sync in background.
    if (include_current_wal &&
        !logs_.back().writer->file()->writable_file()->IsSyncThreadSafe()) {
      return IOStatus::NotSupported(
          "SyncWAL() is not supported for this implementation of WAL file",
          immutable_db_options_.allow_mmap_writes
              ? "try setting Options::allow_mmap_writes to false"
              : Slice());
    }
    for (auto it = logs_.begin();
         it != logs_.end() && it->number <= up_to_number; ++it) {
      auto& log = *it;
      // Ensure the head of logs_ is marked as getting_synced if any is.
      log.PrepareForSync();
      if (log.number <= wal_recovery_cutoff_.load(std::memory_order_acquire)) {
        // The failed file handle is permanently unusable. Recovery will make
        // this WAL obsolete only after atomically persisting all accepted
        // memtable contents, so do not retry any operation on the handle.
        // PrepareForSync() pins the entry until MarkLogsSynced() or
        // MarkLogsNotSynced() calls FinishSync(), which prevents recovery from
        // clearing the cutoff while this call still relies on it.
        TEST_SYNC_POINT_CALLBACK("DBImpl::SyncWalImpl:SkipFailedWAL",
                                 &log.number);
        continue;
      }
      // If last sync failed on a later WAL, this could be a fully synced
      // and closed WAL that just needs to be recorded as synced in the
      // manifest.
      if (log.writer->file()) {
        wals_to_sync.push_back(log.writer);
      }
    }

    need_wal_dir_sync = !wal_dir_synced_;
  }

  if (include_current_wal) {
    TEST_SYNC_POINT("DBWALTest::SyncWALNotWaitWrite:1");
  }
  RecordTick(stats_, WAL_FILE_SYNCED);
  IOOptions opts;
  IOStatus io_s = WritableFileWriter::PrepareIOOptions(write_options, opts);
  std::list<log::Writer*> wals_internally_closed;
  uint64_t failed_wal_number = 0;
  bool failed_with_previous_error = false;
  if (io_s.ok()) {
    for (log::Writer* log : wals_to_sync) {
      if (log->get_log_number() <=
          wal_recovery_cutoff_.load(std::memory_order_acquire)) {
        continue;
      }
      TEST_SYNC_POINT_CALLBACK("DBImpl::SyncWalImpl:BeforeSyncWAL", log);
      if (job_context) {
        ROCKS_LOG_INFO(immutable_db_options_.info_log,
                       "[JOB %d] Syncing log #%" PRIu64, job_context->job_id,
                       log->get_log_number());
      }
      if (error_recovery_in_prog) {
        log->file()->reset_seen_error();
      }
      if (log->get_log_number() >= maybe_active_number) {
        assert(log->get_log_number() == maybe_active_number);
        io_s = log->file()->SyncWithoutFlush(
            opts, immutable_db_options_.use_fsync, &failed_with_previous_error);
      } else {
        io_s = log->file()->Sync(opts, immutable_db_options_.use_fsync,
                                 &failed_with_previous_error);
      }
      std::pair<log::Writer*, IOStatus*> wal_and_status(log, &io_s);
      TEST_SYNC_POINT_CALLBACK("DBImpl::SyncWalImpl:AfterSyncWAL",
                               &wal_and_status);
      if (!io_s.ok()) {
        failed_wal_number = log->get_log_number();
        break;
      }
      // WALs can be closed when purging obsolete files, but if recycling is
      // enabled, the log file is closed here so that it can be reused. And
      // immediate closure here upon final sync makes it easier to guarantee
      // that Checkpoint doesn't LinkFile on a WAL still open for write, which
      // might be unsupported for some FileSystem implementations. Close here
      // should be inexpensive because flush and sync are done, so the kill
      // switch background_close_inactive_wals is expected to be removed in
      // the future.
      if (log->get_log_number() < maybe_active_number &&
          (immutable_db_options_.recycle_log_file_num > 0 ||
           !immutable_db_options_.background_close_inactive_wals)) {
        if (error_recovery_in_prog) {
          log->file()->reset_seen_error();
        }
        io_s = log->file()->Close(opts);
        wals_internally_closed.push_back(log);
        if (!io_s.ok()) {
          failed_wal_number = log->get_log_number();
          break;
        }
      }
    }
  }
  if (!io_s.ok()) {
    ROCKS_LOG_ERROR(immutable_db_options_.info_log, "WAL Sync error %s",
                    io_s.ToString().c_str());
    // In case there is a fs error we should set it globally to prevent the
    // future writes
    // The live-WAL operation that first observed the I/O failure owns
    // classifying its original status, either through WALIOStatusCheck() or a
    // direct SetBGError() call. With paranoid checks enabled, a concurrent sync
    // must not classify the synthetic "previous error" status because it has
    // lost the original scope and could incorrectly make the error fatal. With
    // paranoid checks disabled, WALIOStatusCheck() instead preserves the
    // legacy behavior of resetting the writer so a later operation can retry.
    if (!failed_with_previous_error || !immutable_db_options_.paranoid_checks) {
      WALIOStatusCheck(io_s, failed_wal_number);
    }
  }
  if (io_s.ok() && need_wal_dir_sync) {
    io_s = directories_.GetWalDir()->FsyncWithDirOptions(
        IOOptions(), nullptr,
        DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
  }
  if (include_current_wal) {
    TEST_SYNC_POINT("DBWALTest::SyncWALNotWaitWrite:2");

    TEST_SYNC_POINT("DBImpl::SyncWAL:BeforeMarkLogsSynced:1");
  } else {
    TEST_SYNC_POINT_CALLBACK("DBImpl::SyncClosedWals:BeforeReLock",
                             /*arg=*/nullptr);
  }
  {
    InstrumentedMutexLock l(&wal_write_mutex_);
    for (auto* wal : wals_internally_closed) {
      // We can only modify the state of log::Writer under the mutex
      bool was_closed = wal->PublishIfClosed();
      assert(was_closed);
      (void)was_closed;
    }
    if (io_s.ok()) {
      MarkLogsSynced(up_to_number, need_wal_dir_sync, synced_wals);
    } else {
      MarkLogsNotSynced(up_to_number);
    }
  }
  return io_s;
}

Status DBImpl::ApplyWALToManifest(const ReadOptions& read_options,
                                  const WriteOptions& write_options,
                                  VersionEdit* synced_wals) {
  // not empty, write to MANIFEST.
  mutex_.AssertHeld();

  Status status = versions_->LogAndApplyToDefaultColumnFamily(
      read_options, write_options, synced_wals, &mutex_,
      directories_.GetDbDir());
  if (!status.ok() && versions_->io_status().IsIOError()) {
    error_handler_.SetBGError(versions_->io_status(),
                              BackgroundErrorReason::kManifestWrite);
  }
  return status;
}

Status DBImpl::LockWAL() {
  const uint64_t thread_id = env_->GetThreadID();
  {
    InstrumentedMutexLock lock(&mutex_);
    if (!lock_wal_owner_thread_id_counts_.empty()) {
      assert(lock_wal_write_token_);
      ++lock_wal_owner_thread_id_counts_[thread_id];
    } else {
      // NOTE: this will "unnecessarily" wait for other non-LockWAL() write
      // stalls to clear before LockWAL returns, however fixing that would
      // not be simple because if we notice the primary queue is already
      // stalled, that stall might clear while we release DB mutex in
      // EnterUnbatched() for the nonmem queue. And if we work around that in
      // the naive way, we could deadlock by locking the two queues in different
      // orders.

      WriteThread::Writer w;
      write_thread_.EnterUnbatched(&w, &mutex_);
      WriteThread::Writer nonmem_w;
      if (two_write_queues_) {
        nonmem_write_thread_.EnterUnbatched(&nonmem_w, &mutex_);
      }

      // NOTE: releasing mutex in EnterUnbatched might mean we are actually
      // now locked by another thread.
      if (lock_wal_owner_thread_id_counts_.empty()) {
        assert(!lock_wal_write_token_);
        lock_wal_write_token_ = write_controller_.GetStopToken();
      }
      assert(lock_wal_write_token_);
      ++lock_wal_owner_thread_id_counts_[thread_id];

      if (two_write_queues_) {
        nonmem_write_thread_.ExitUnbatched(&nonmem_w);
      }
      write_thread_.ExitUnbatched(&w);
    }
  }
  // NOTE: avoid I/O holding DB mutex
  Status s = FlushWAL(/*sync=*/false);
  if (!s.ok()) {
    // Undo this LockWAL().
    UnlockWAL().PermitUncheckedError();
  }
  return s;
}

Status DBImpl::UnlockWAL() {
  const uint64_t thread_id = env_->GetThreadID();
  bool signal = false;
  uint64_t maybe_stall_begun_count = 0;
  uint64_t nonmem_maybe_stall_begun_count = 0;
  {
    InstrumentedMutexLock lock(&mutex_);
    const auto owner = lock_wal_owner_thread_id_counts_.find(thread_id);
    if (owner == lock_wal_owner_thread_id_counts_.end()) {
      return Status::Aborted("No LockWAL() held by current thread");
    }
    if (--owner->second == 0) {
      lock_wal_owner_thread_id_counts_.erase(owner);
    }
    if (lock_wal_owner_thread_id_counts_.empty()) {
      lock_wal_write_token_.reset();
      signal = true;
      // For the last UnlockWAL, we don't want to return from UnlockWAL()
      // until the thread(s) that called BeginWriteStall() have had a chance to
      // call EndWriteStall(), so that no_slowdown writes after UnlockWAL() are
      // guaranteed to succeed if there's no other source of stall.
      maybe_stall_begun_count = write_thread_.GetBegunCountOfOutstandingStall();
      if (two_write_queues_) {
        nonmem_maybe_stall_begun_count =
            nonmem_write_thread_.GetBegunCountOfOutstandingStall();
      }
    }
  }
  if (signal) {
    // SignalAll outside of mutex for efficiency
    bg_cv_.SignalAll();
  }
  // Ensure stalls have cleared
  if (maybe_stall_begun_count) {
    write_thread_.WaitForStallEndedCount(maybe_stall_begun_count);
  }
  if (nonmem_maybe_stall_begun_count) {
    nonmem_write_thread_.WaitForStallEndedCount(nonmem_maybe_stall_begun_count);
  }
  return Status::OK();
}

void DBImpl::MarkLogsSynced(uint64_t up_to, bool synced_dir,
                            VersionEdit* synced_wals) {
  wal_write_mutex_.AssertHeld();
  if (synced_dir && cur_wal_number_ == up_to) {
    wal_dir_synced_ = true;
  }
  for (auto it = logs_.begin(); it != logs_.end() && it->number <= up_to;) {
    auto& wal = *it;
    assert(wal.IsSyncing());

    if (wal.number <= wal_recovery_cutoff_.load(std::memory_order_acquire)) {
      wal.FinishSync();
      ++it;
      continue;
    }

    if (wal.number < logs_.back().number) {
      // Inactive WAL
      if (immutable_db_options_.track_and_verify_wals_in_manifest &&
          wal.GetPreSyncSize() > 0) {
        synced_wals->AddWal(wal.number, WalMetadata(wal.GetPreSyncSize()));
      }
      // Reclaim closed WALs (wal.writer->file() == nullptr), and if we don't
      // need to close before that (background_close_inactive_wals) we can
      // opportunistically reclaim WALs that happen to be fully synced.
      // (Probably not worth extra code and mutex release to opportunistically
      // close WALs that became eligible since last holding the mutex.
      // FindObsoleteFiles can take care of it.)
      if (wal.writer->file() == nullptr ||
          (immutable_db_options_.background_close_inactive_wals &&
           wal.GetPreSyncSize() == wal.writer->file()->GetFlushedSize())) {
        // Fully synced
        wals_to_free_.push_back(wal.ReleaseWriter());
        it = logs_.erase(it);
      } else {
        wal.FinishSync();
        ++it;
      }
    } else {
      assert(wal.number == logs_.back().number);
      // Active WAL
      wal.FinishSync();
      ++it;
    }
  }
  wal_sync_cv_.SignalAll();
}

void DBImpl::MarkLogsNotSynced(uint64_t up_to) {
  wal_write_mutex_.AssertHeld();
  for (auto it = logs_.begin(); it != logs_.end() && it->number <= up_to;
       ++it) {
    auto& wal = *it;
    wal.FinishSync();
  }
  wal_sync_cv_.SignalAll();
}

SequenceNumber DBImpl::GetLatestSequenceNumber() const {
  return versions_->LastSequence();
}

void DBImpl::SetLastPublishedSequence(SequenceNumber seq) {
  versions_->SetLastPublishedSequence(seq);
}

Status DBImpl::GetFullHistoryTsLow(ColumnFamilyHandle* column_family,
                                   std::string* ts_low) {
  if (ts_low == nullptr) {
    return Status::InvalidArgument("ts_low is nullptr");
  }
  ColumnFamilyData* cfd = nullptr;
  if (column_family == nullptr) {
    cfd = default_cf_handle_->cfd();
  } else {
    auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
    assert(cfh != nullptr);
    cfd = cfh->cfd();
  }
  assert(cfd != nullptr && cfd->user_comparator() != nullptr);
  if (cfd->user_comparator()->timestamp_size() == 0) {
    return Status::InvalidArgument(
        "Timestamp is not enabled in this column family");
  }
  InstrumentedMutexLock l(&mutex_);
  *ts_low = cfd->GetFullHistoryTsLow();
  assert(ts_low->empty() ||
         cfd->user_comparator()->timestamp_size() == ts_low->size());
  return Status::OK();
}

Status DBImpl::GetNewestUserDefinedTimestamp(ColumnFamilyHandle* column_family,
                                             std::string* newest_timestamp) {
  if (newest_timestamp == nullptr) {
    return Status::InvalidArgument("newest_timestamp is nullptr");
  }
  ColumnFamilyData* cfd = nullptr;
  if (column_family == nullptr) {
    cfd = default_cf_handle_->cfd();
  } else {
    auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
    assert(cfh != nullptr);
    cfd = cfh->cfd();
  }
  assert(cfd != nullptr && cfd->user_comparator() != nullptr);
  if (cfd->user_comparator()->timestamp_size() == 0) {
    return Status::InvalidArgument(
        "Timestamp is not enabled in this column family");
  }
  if (cfd->ioptions().persist_user_defined_timestamps) {
    return Status::NotSupported(
        "GetNewestUserDefinedTimestamp doesn't support the case when user"
        "defined timestamps are persisted.");
  }

  Status status;
  // Acquire SuperVersion
  SuperVersion* sv = GetAndRefSuperVersion(cfd);
  {
    InstrumentedMutexLock l(&mutex_);
    bool enter_write_thread = sv->mem == cfd->mem();
    WriteThread::Writer w;
    // Enter write thread to read the mutable memtable to avoid racing access
    // with concurrent writes. No need to enter nonmem_write_thread_ since this
    // call only care about memtable writes, not WAL writes.
    if (enter_write_thread) {
      write_thread_.EnterUnbatched(&w, &mutex_);
      WaitForPendingWrites();
    }
    *newest_timestamp = sv->mem->GetNewestUDT().ToString();
    assert(!newest_timestamp->empty() || sv->mem->IsEmpty());
    if (enter_write_thread) {
      write_thread_.ExitUnbatched(&w);
    }
  }
  // Read from immutable memtables if nothing found in mutable memtable.
  if (newest_timestamp->empty()) {
    *newest_timestamp = sv->imm->GetNewestUDT().ToString();
  }
  // Read from SST files if no result can be found in memtables.
  if (newest_timestamp->empty() && sv->current->GetSstFilesSize() != 0) {
    // full_history_ts_low is used to track the exclusive upperbound of
    // flushed user defined timestamp. So we can use it to deduce the newest
    // timestamp in the SST files that the column family has seen.
    Slice full_history_ts_low = sv->full_history_ts_low;
    if (!full_history_ts_low.empty()) {
      GetU64CutoffTsFromFullHistoryTsLow(&full_history_ts_low,
                                         newest_timestamp);
    }
  }
  ReturnAndCleanupSuperVersion(cfd, sv);
  return status;
}

InternalIterator* DBImpl::NewInternalIterator(const ReadOptions& read_options,
                                              Arena* arena,
                                              SequenceNumber sequence,
                                              ColumnFamilyHandle* column_family,
                                              bool allow_unprepared_value) {
  ColumnFamilyData* cfd;
  if (column_family == nullptr) {
    cfd = default_cf_handle_->cfd();
  } else {
    auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
    cfd = cfh->cfd();
  }

  mutex_.Lock();
  SuperVersion* super_version = cfd->GetSuperVersion()->Ref();
  mutex_.Unlock();
  return NewInternalIterator(read_options, cfd, super_version, arena, sequence,
                             allow_unprepared_value);
}

void DBImpl::SchedulePurge() {
  mutex_.AssertHeld();
  assert(opened_successfully_);

  // Purge operations are put into High priority queue
  bg_purge_scheduled_++;
  env_->Schedule(&DBImpl::BGWorkPurge, this, Env::Priority::HIGH, nullptr);
}

void DBImpl::BackgroundCallPurge() {
  TEST_SYNC_POINT("DBImpl::BackgroundCallPurge:beforeMutexLock");
  mutex_.Lock();

  while (!wals_to_free_queue_.empty()) {
    assert(!wals_to_free_queue_.empty());
    log::Writer* log_writer = *(wals_to_free_queue_.begin());
    wals_to_free_queue_.pop_front();
    mutex_.Unlock();
    delete log_writer;
    mutex_.Lock();
  }
  while (!superversions_to_free_queue_.empty()) {
    assert(!superversions_to_free_queue_.empty());
    SuperVersion* sv = superversions_to_free_queue_.front();
    superversions_to_free_queue_.pop_front();
    mutex_.Unlock();
    delete sv;
    mutex_.Lock();
  }

  assert(bg_purge_scheduled_ > 0);

  // Can't use iterator to go over purge_files_ because inside the loop we're
  // unlocking the mutex that protects purge_files_.
  while (!purge_files_.empty()) {
    auto it = purge_files_.begin();
    // Need to make a copy of the PurgeFilesInfo before unlocking the mutex.
    PurgeFileInfo purge_file = it->second;

    const std::string& fname = purge_file.fname;
    const std::string& dir_to_sync = purge_file.dir_to_sync;
    FileType type = purge_file.type;
    uint64_t number = purge_file.number;
    int job_id = purge_file.job_id;

    purge_files_.erase(it);

    mutex_.Unlock();
    if (type == kBlobFile && ShouldKeepBlobFileDuringPurge(number, fname)) {
      mutex_.Lock();
      continue;
    }
    DeleteObsoleteFileImpl(job_id, fname, dir_to_sync, type, number);
    mutex_.Lock();
  }

  bg_purge_scheduled_--;

  bg_cv_.SignalAll();
  // IMPORTANT:there should be no code after calling SignalAll. This call may
  // signal the DB destructor that it's OK to proceed with destruction. In
  // that case, all DB variables will be dealloacated and referencing them
  // will cause trouble.
  mutex_.Unlock();
}

void DBImpl::CleanupSessionTmpDir() {
  if (!immutable_db_options_.use_session_tmp_dir_for_remote_compaction) {
    return;
  }

  // Deletion is not routed through SstFileManager: these files were produced by
  // a dead incarnation and were never in its accounting, and SFM's
  // slow-deletion path renames a file to `<name>.trash` in place, which would
  // leave the directory non-empty and therefore unremovable until a later open.
  const std::string output_dir = SessionTmpDir(dbname_);
  size_t num_removed = 0;

  std::vector<std::string> children;
  // Default IOOptions on purpose: setting IOOptions::do_not_recurse makes the
  // Posix implementation drop directory entries from the listing altogether,
  // and directory entries are precisely what this scan needs.
  Status s = env_->GetChildren(output_dir, &children);
  if (!s.ok()) {
    if (!s.IsNotFound()) {
      ROCKS_LOG_WARN(immutable_db_options_.info_log,
                     "Failed to list session temporary directory %s: %s",
                     output_dir.c_str(), s.ToString().c_str());
    }
    return;
  }

  for (const std::string& child : children) {
    if (child == "." || child == "..") {
      continue;
    }
    std::string child_path = output_dir;
    child_path.append("/").append(child);
    s = DestroyDir(env_, child_path);
    if (s.ok()) {
      ++num_removed;
    } else {
      ROCKS_LOG_WARN(immutable_db_options_.info_log,
                     "Failed to delete stale session temporary "
                     "directory %s/%s: %s",
                     output_dir.c_str(), child.c_str(), s.ToString().c_str());
    }
  }

  if (num_removed > 0) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Deleted %zu stale session temporary director%s",
                   num_removed, num_removed == 1 ? "y" : "ies");
  }
}

// A `SuperVersionHandle` holds a non-null `SuperVersion*` pointing at a
// `SuperVersion` referenced once for this object. It also contains the state
// needed to clean up the `SuperVersion` reference from outside of `DBImpl`
// using `CleanupSuperVersionHandle()`.
struct SuperVersionHandle {
  // `_super_version` must be non-nullptr and `Ref()`'d once as long as the
  // `SuperVersionHandle` may use it.
  SuperVersionHandle(DBImpl* _db, InstrumentedMutex* _mu,
                     SuperVersion* _super_version, bool _background_purge)
      : db(_db),
        mu(_mu),
        super_version(_super_version),
        background_purge(_background_purge) {}

  DBImpl* db;
  InstrumentedMutex* mu;
  SuperVersion* super_version;
  bool background_purge;
};

static void CleanupSuperVersionHandle(void* arg1, void* /*arg2*/) {
  SuperVersionHandle* sv_handle = static_cast<SuperVersionHandle*>(arg1);

  sv_handle->db->CleanupIteratorSuperVersion(sv_handle->super_version,
                                             sv_handle->background_purge);

  delete sv_handle;
}

void DBImpl::TransferSuperVersionPin(SuperVersion* super_version,
                                     Cleanable* pin) {
  assert(super_version != nullptr);
  assert(pin != nullptr);
  super_version->Ref();
  SuperVersionHandle* sv_handle = new SuperVersionHandle(
      this, &mutex_, super_version,
      immutable_db_options_.avoid_unnecessary_blocking_io);
  pin->RegisterCleanup(CleanupSuperVersionHandle, sv_handle, nullptr);
}

struct GetMergeOperandsState {
  GetMergeOperandsState(MergeContext _merge_context,
                        PinnedIteratorsManager _pinned_iters_mgr)
      : merge_context(std::move(_merge_context)),
        pinned_iters_mgr(std::move(_pinned_iters_mgr)),
        sv_handle(nullptr) {}

  MergeContext merge_context;
  PinnedIteratorsManager pinned_iters_mgr;
  SuperVersionHandle* sv_handle;
};

static void CleanupGetMergeOperandsState(void* arg1, void* /*arg2*/) {
  GetMergeOperandsState* state = static_cast<GetMergeOperandsState*>(arg1);
  CleanupSuperVersionHandle(state->sv_handle /* arg1 */, nullptr /* arg2 */);
  delete state;
}

void DBImpl::PrepareGetMergeOperands(const GetImplOptions& get_impl_options,
                                     MergeContext* merge_context) {
  assert(merge_context != nullptr);
  merge_context->get_merge_operands_options =
      get_impl_options.get_merge_operands_options;
}

Status DBImpl::FinalizeGetMergeOperands(
    const GetImplOptions& get_impl_options, SuperVersion* super_version,
    MergeContext* merge_context, PinnedIteratorsManager* pinned_iters_manager,
    size_t* bytes_read) {
  assert(!get_impl_options.get_value);
  assert(get_impl_options.merge_operands != nullptr);
  assert(get_impl_options.get_merge_operands_options != nullptr);
  assert(get_impl_options.number_of_operands != nullptr);
  assert(super_version != nullptr);
  assert(merge_context != nullptr);
  assert(pinned_iters_manager != nullptr);
  assert(bytes_read != nullptr);

  const int number_of_operands =
      static_cast<int>(merge_context->GetNumOperands());
  *get_impl_options.number_of_operands = number_of_operands;
  assert(number_of_operands > 0);
  if (number_of_operands > get_impl_options.get_merge_operands_options
                               ->expected_max_number_of_operands) {
    return Status::Incomplete(
        Status::SubCode::KMergeOperandsInsufficientCapacity);
  }

  // Only the slots being filled are reset, each just before it is written, so
  // the cost scales with the result rather than the caller's capacity. Reset
  // is still required there: a reused slot may hold an earlier pin, and
  // PinSlice/PinSelf assert the slot is unpinned.
  //
  // An operand can borrow storage from the SuperVersion, an SST iterator, or
  // MergeContext itself. Large results keep those resources together behind a
  // shared cleanup instead of copying every operand independently.
  if (ShouldReferenceSuperVersion(*merge_context)) {
    SharedCleanablePtr shared_cleanable;
    GetMergeOperandsState* state = new GetMergeOperandsState(
        std::move(*merge_context), std::move(*pinned_iters_manager));

    super_version->Ref();
    state->sv_handle = new SuperVersionHandle(
        this, &mutex_, super_version,
        immutable_db_options_.avoid_unnecessary_blocking_io);

    shared_cleanable.Allocate();
    shared_cleanable->RegisterCleanup(CleanupGetMergeOperandsState,
                                      state /* arg1 */, nullptr /* arg2 */);
    const std::vector<Slice>& operands = state->merge_context.GetOperands();
    for (size_t i = 0; i < operands.size(); ++i) {
      *bytes_read += operands[i].size();
      get_impl_options.merge_operands[i].Reset();
      get_impl_options.merge_operands[i].PinSlice(operands[i],
                                                  nullptr /* cleanable */);
      if (i + 1 == operands.size()) {
        shared_cleanable.MoveAsCleanupTo(&get_impl_options.merge_operands[i]);
      } else {
        shared_cleanable.RegisterCopyWith(&get_impl_options.merge_operands[i]);
      }
    }
  } else {
    size_t i = 0;
    for (const Slice& operand : merge_context->GetOperands()) {
      *bytes_read += operand.size();
      get_impl_options.merge_operands[i].Reset();
      get_impl_options.merge_operands[i++].PinSelf(operand);
    }
  }
  return Status::OK();
}

InternalIterator* DBImpl::NewInternalIterator(
    const ReadOptions& read_options, ColumnFamilyData* cfd,
    SuperVersion* super_version, Arena* arena, SequenceNumber sequence,
    bool allow_unprepared_value, ArenaWrappedDBIter* db_iter,
    const MultiScanArgs* scan_opts) {
  InternalIterator* internal_iter;
  assert(arena != nullptr);
  auto prefix_extractor =
      super_version->mutable_cf_options.prefix_extractor.get();
  // Need to create internal iterator from the arena.
  MergeIteratorBuilder merge_iter_builder(
      &cfd->internal_comparator(), arena,
      // FIXME? It's not clear what interpretation of prefix seek is needed
      // here, and no unit test cares about the value provided here.
      !read_options.total_order_seek && prefix_extractor != nullptr,
      read_options.iterate_upper_bound);
  Status s;
  const Comparator* user_comparator = cfd->user_comparator();
  const bool mem_intersects =
      !super_version->mem->IsEmpty() &&
      MultiScanIntersectsMemTable(super_version->mem, read_options,
                                  super_version->GetSeqnoToTimeMapping(),
                                  prefix_extractor, scan_opts, user_comparator);
  if (scan_opts == nullptr || mem_intersects) {
    // Collect iterator for mutable memtable
    auto mem_iter = super_version->mem->NewIterator(
        read_options, super_version->GetSeqnoToTimeMapping(), arena,
        super_version->mutable_cf_options.prefix_extractor.get(),
        /*for_flush=*/false);
    if (!read_options.ignore_range_deletions) {
      std::unique_ptr<TruncatedRangeDelIterator> mem_tombstone_iter;
      std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
          super_version->mem->NewRangeTombstoneIterator(
              read_options, sequence, false /* immutable_memtable */));
      if (range_del_iter != nullptr && !range_del_iter->empty()) {
        mem_tombstone_iter = std::make_unique<TruncatedRangeDelIterator>(
            std::move(range_del_iter), &cfd->ioptions().internal_comparator,
            nullptr /* smallest */, nullptr /* largest */);
      }
      merge_iter_builder.AddPointAndTombstoneIterator(
          mem_iter, std::move(mem_tombstone_iter));
    } else {
      merge_iter_builder.AddIterator(mem_iter);
    }
  } else if (scan_opts != nullptr) {
    merge_iter_builder.SetMemtablePruned(true);
  }

  if (s.ok() &&
      (scan_opts == nullptr || super_version->imm->GetTotalNumEntries() > 0)) {
    // Collect all needed child iterators for immutable memtables. When scan
    // ranges are provided, AddIterators prunes each immutable memtable
    // individually.
    super_version->imm->AddIterators(
        read_options, super_version->GetSeqnoToTimeMapping(),
        super_version->mutable_cf_options.prefix_extractor.get(),
        &merge_iter_builder, !read_options.ignore_range_deletions, sequence,
        scan_opts, user_comparator);
  }
  TEST_SYNC_POINT_CALLBACK("DBImpl::NewInternalIterator:StatusCallback", &s);
  if (s.ok()) {
    // Collect iterators for files in L0 - Ln
    if (read_options.read_tier != kMemtableTier) {
      super_version->current->AddIterators(
          read_options, file_options_, &merge_iter_builder,
          allow_unprepared_value, sequence, scan_opts);
    }
    internal_iter = merge_iter_builder.Finish(
        read_options.ignore_range_deletions ? nullptr : db_iter);
    if (internal_iter == nullptr) {
      internal_iter = NewEmptyInternalIterator<Slice>(arena);
    }
    SuperVersionHandle* cleanup = new SuperVersionHandle(
        this, &mutex_, super_version,
        read_options.background_purge_on_iterator_cleanup ||
            immutable_db_options_.avoid_unnecessary_blocking_io);
    internal_iter->RegisterCleanup(CleanupSuperVersionHandle, cleanup, nullptr);

    return internal_iter;
  } else {
    CleanupSuperVersion(super_version);
  }
  return NewErrorInternalIterator<Slice>(s, arena);
}

void DBImpl::CleanupIteratorSuperVersion(SuperVersion* super_version,
                                         bool background_purge) {
  background_purge =
      background_purge || immutable_db_options_.avoid_unnecessary_blocking_io;
  if (super_version->Unref()) {
    // Job id == 0 means that this is not our background process, but rather
    // user thread
    JobContext job_context(0);

    mutex_.Lock();
    super_version->Cleanup();
    FindObsoleteFiles(&job_context, false, true);
    if (background_purge) {
      ScheduleBgLogWriterClose(&job_context);
      AddSuperVersionsToFreeQueue(super_version);
      SchedulePurge();
    }
    mutex_.Unlock();

    if (!background_purge) {
      delete super_version;
    }
    if (job_context.HaveSomethingToDelete()) {
      PurgeObsoleteFiles(job_context, background_purge);
    }
    job_context.Clean();
  }
}

ColumnFamilyHandle* DBImpl::DefaultColumnFamily() const {
  return default_cf_handle_;
}

ColumnFamilyHandle* DBImpl::PersistentStatsColumnFamily() const {
  return persist_stats_cf_handle_;
}

namespace {

void CopyNewerVersionPresent(
    const autovector<KeyContext, MultiGetContext::MAX_BATCH_SIZE>& key_context,
    std::vector<uint8_t>* output) {
  if (output == nullptr) {
    return;
  }
  for (size_t i = 0; i < key_context.size(); ++i) {
    (*output)[i] = key_context[i].newer_version_present;
  }
}

}  // namespace

Status DB::GetWithMetadata(const ReadOptions& options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           PinnableSlice* value,
                           OutputMetadata* output_metadata) {
  std::string* timestamp = GetOutputTimestamp(output_metadata);
  bool* newer_version_present = GetOutputNewerVersionPresent(output_metadata);
  if (newer_version_present != nullptr) {
    *newer_version_present = false;
  }
  if (value == nullptr) {
    return Status::InvalidArgument(
        "Cannot call GetWithMetadata with a null value");
  }
  if (newer_version_present == nullptr || options.snapshot == nullptr) {
    return Get(options, column_family, key, value, timestamp);
  }
  return Status::NotSupported(
      "GetWithMetadata is not implemented by this DB subclass");
}

Status DBImpl::GetWithMetadata(const ReadOptions& _read_options,
                               ColumnFamilyHandle* column_family,
                               const Slice& key, PinnableSlice* value,
                               OutputMetadata* output_metadata) {
  std::string* timestamp = GetOutputTimestamp(output_metadata);
  bool* newer_version_present = GetOutputNewerVersionPresent(output_metadata);
  if (newer_version_present != nullptr) {
    *newer_version_present = false;
  }
  if (value == nullptr) {
    return Status::InvalidArgument(
        "Cannot call GetWithMetadata with a null value");
  }
  value->Reset();

  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kGet) {
    return Status::InvalidArgument(
        "Can only call Get with `ReadOptions::io_activity` is "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kGet`");
  }

  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kGet;
  }
  if (newer_version_present != nullptr && read_options.snapshot != nullptr &&
      read_options.read_tier == kPersistedTier) {
    return Status::NotSupported(
        "Newer-version metadata is not supported with kPersistedTier");
  }

  return GetImpl(
      read_options, column_family, key, value, timestamp,
      read_options.snapshot != nullptr ? newer_version_present : nullptr);
}

Status DB::GetEntityWithMetadata(const ReadOptions& options,
                                 ColumnFamilyHandle* column_family,
                                 const Slice& key, PinnableWideColumns* columns,
                                 OutputMetadata* output_metadata) {
  Status s = InitGetEntityOutputMetadata(output_metadata);
  if (!s.ok()) {
    return s;
  }
  if (column_family == nullptr) {
    return Status::InvalidArgument(
        "Cannot call GetEntityWithMetadata without a column family handle");
  }
  if (columns == nullptr) {
    return Status::InvalidArgument(
        "Cannot call GetEntityWithMetadata without a PinnableWideColumns "
        "object");
  }
  if (GetOutputNewerVersionPresent(output_metadata) == nullptr ||
      options.snapshot == nullptr) {
    return GetEntity(options, column_family, key, columns);
  }
  return Status::NotSupported(
      "GetEntityWithMetadata is not implemented by this DB subclass");
}

Status DBImpl::GetEntityWithMetadata(const ReadOptions& _read_options,
                                     ColumnFamilyHandle* column_family,
                                     const Slice& key,
                                     PinnableWideColumns* columns,
                                     OutputMetadata* output_metadata) {
  bool* newer_version_present = GetOutputNewerVersionPresent(output_metadata);
  Status s = InitGetEntityOutputMetadata(output_metadata);
  if (!s.ok()) {
    return s;
  }
  if (!column_family) {
    return Status::InvalidArgument(
        "Cannot call GetEntityWithMetadata without a column family handle");
  }
  if (!columns) {
    return Status::InvalidArgument(
        "Cannot call GetEntityWithMetadata without a PinnableWideColumns "
        "object");
  }
  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kGetEntity) {
    return Status::InvalidArgument(
        "Can only call GetEntityWithMetadata with `ReadOptions::io_activity` "
        "set to `Env::IOActivity::kUnknown` or "
        "`Env::IOActivity::kGetEntity`");
  }
  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kGetEntity;
  }
  if (newer_version_present != nullptr && read_options.snapshot != nullptr &&
      read_options.read_tier == kPersistedTier) {
    return Status::NotSupported(
        "Newer-version metadata is not supported with kPersistedTier");
  }
  columns->Reset();

  GetImplOptions get_impl_options;
  get_impl_options.column_family = column_family;
  get_impl_options.columns = columns;
  get_impl_options.newer_version_present =
      read_options.snapshot != nullptr ? newer_version_present : nullptr;
  return GetImpl(read_options, key, get_impl_options);
}

Status DBImpl::GetImpl(const ReadOptions& read_options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       PinnableSlice* value, std::string* timestamp,
                       bool* newer_version_present) {
  GetImplOptions get_impl_options;
  get_impl_options.column_family = column_family;
  get_impl_options.value = value;
  get_impl_options.timestamp = timestamp;
  get_impl_options.newer_version_present = newer_version_present;

  Status s = GetImpl(read_options, key, get_impl_options);
  return s;
}

Status DBImpl::GetEntityLazyImpl(const ReadOptions& read_options,
                                 ColumnFamilyHandle* column_family,
                                 const Slice& key, LazyWideColumns* result) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();

  // Point-lookup output: the entity buffer owned by the result. GetImpl fills
  // it with the (unresolved) entity and hands back the resolution context.
  PinnableWideColumns* entity_buffer =
      LazyWideColumnsHelper::EntityBuffer(result);

  const SameFileBlobReader* same_file_reader = nullptr;
  const Version* version = nullptr;
  Cleanable pin;

  GetImplOptions get_impl_options;
  get_impl_options.column_family = column_family;
  get_impl_options.columns = entity_buffer;
  get_impl_options.lazy_columns_pin = &pin;
  get_impl_options.lazy_columns_version = &version;
  get_impl_options.lazy_columns_same_file_reader = &same_file_reader;

  Status s = GetImpl(read_options, key, get_impl_options);
  if (!s.ok()) {
    result->Reset();
    return s;
  }

  s = LazyWideColumnsHelper::Finalize(
      result, key, version, read_options, cfd->blob_file_cache(),
      /*allow_write_path_fallback=*/cfd->blob_partition_manager() != nullptr,
      same_file_reader, std::move(pin));
  if (!s.ok()) {
    result->Reset();
  }
  return s;
}

Status DBImpl::GetEntityLazyForBatch(const ReadOptions& read_options,
                                     ColumnFamilyHandle* column_family,
                                     SuperVersion* super_version,
                                     SequenceNumber snapshot_seq,
                                     const Slice& key,
                                     LazyWideColumns* result) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();

  PinnableWideColumns* entity_buffer =
      LazyWideColumnsHelper::EntityBuffer(result);

  const SameFileBlobReader* same_file_reader = nullptr;
  const Version* version = nullptr;

  GetImplOptions get_impl_options;
  get_impl_options.column_family = column_family;
  get_impl_options.columns = entity_buffer;
  get_impl_options.lazy_columns_version = &version;
  get_impl_options.lazy_columns_same_file_reader = &same_file_reader;
  // Read against the batch's shared SuperVersion + consistent sequence number
  // (no per-key SuperVersion acquisition, no per-key pin).
  get_impl_options.lazy_columns_shared_sv = super_version;
  get_impl_options.lazy_columns_snapshot_seq = snapshot_seq;

  Status s = GetImpl(read_options, key, get_impl_options);
  if (!s.ok()) {
    result->Reset();
    return s;
  }

  s = LazyWideColumnsHelper::FinalizeInBatch(
      result, key, version, read_options, cfd->blob_file_cache(),
      /*allow_write_path_fallback=*/cfd->blob_partition_manager() != nullptr,
      same_file_reader);
  if (!s.ok()) {
    result->Reset();
  }
  return s;
}

Status DBImpl::GetEntityLazy(const ReadOptions& _read_options,
                             ColumnFamilyHandle* column_family,
                             const Slice& key, LazyWideColumns* result) {
  // Start from an empty result so a reused LazyWideColumns is left empty (not
  // stale) on every early-return / error path below.
  if (result != nullptr) {
    result->Reset();
  }
  if (!column_family) {
    return Status::InvalidArgument(
        "Cannot call GetEntityLazy without a column family handle");
  }
  if (!result) {
    return Status::InvalidArgument(
        "Cannot call GetEntityLazy without a LazyWideColumns object");
  }
  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kGetEntity) {
    return Status::InvalidArgument(
        "Can only call GetEntityLazy with `ReadOptions::io_activity` set to "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kGetEntity`");
  }
  // Lazy resolution keeps blob references (including same-file/embedded ones)
  // resolvable after the call returns; that relies on immortal, pinned table
  // readers, which the DB only guarantees when max_open_files == -1.
  if (mutable_db_options_.max_open_files != -1) {
    return Status::InvalidArgument(
        "GetEntityLazy requires the DB to be opened with max_open_files == -1");
  }

  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kGetEntity;
  }

  return GetEntityLazyImpl(read_options, column_family, key, result);
}

void DBImpl::MultiGetEntityLazy(const ReadOptions& _read_options,
                                ColumnFamilyHandle* column_family,
                                size_t num_keys, const Slice* keys,
                                LazyWideColumnsBatch* result, Status* statuses,
                                bool /* sorted_input */) {
  // Start from an empty batch so a reused LazyWideColumnsBatch is left empty
  // (not stale) on every early-return / error path below.
  if (result != nullptr) {
    result->Reset();
  }
  if (num_keys == 0) {
    return;
  }
  Status arg_status;
  if (!column_family) {
    arg_status = Status::InvalidArgument(
        "Cannot call MultiGetEntityLazy without a column family handle");
  } else if (!result) {
    arg_status = Status::InvalidArgument(
        "Cannot call MultiGetEntityLazy without a LazyWideColumnsBatch object");
  } else if (_read_options.io_activity != Env::IOActivity::kUnknown &&
             _read_options.io_activity != Env::IOActivity::kMultiGetEntity) {
    arg_status = Status::InvalidArgument(
        "Can only call MultiGetEntityLazy with `ReadOptions::io_activity` set "
        "to `Env::IOActivity::kUnknown` or `Env::IOActivity::kMultiGetEntity`");
  } else if (mutable_db_options_.max_open_files != -1) {
    arg_status = Status::InvalidArgument(
        "MultiGetEntityLazy requires the DB to be opened with max_open_files "
        "== "
        "-1");
  }
  if (!arg_status.ok()) {
    for (size_t i = 0; i < num_keys; ++i) {
      statuses[i] = arg_status;
    }
    return;
  }

  // Validate the read timestamp against the column family up front -- before
  // MultiCFSnapshot, whose collapsed-history check (FailIfReadCollapsedHistory)
  // assumes a correctly-sized timestamp and would otherwise assert in debug (or
  // read past a wrong-width timestamp buffer in release) when
  // full_history_ts_low is set. Mirrors NewIterator; the per-key GetImpl below
  // re-validates as defense in depth.
  {
    const Status ts_status =
        _read_options.timestamp
            ? FailIfTsMismatchCf(column_family, *(_read_options.timestamp))
            : FailIfCfHasTs(column_family);
    if (!ts_status.ok()) {
      for (size_t i = 0; i < num_keys; ++i) {
        statuses[i] = ts_status;
      }
      return;
    }
  }

  LazyWideColumnsHelper::InitBatch(result, num_keys);

  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kMultiGetEntity;
  }

  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();

  // Acquire ONE SuperVersion + one consistent (implicit) sequence number for
  // the whole batch (single column family), retaining the SuperVersion
  // reference (extra_sv_ref) so it can be transferred into the batch as a
  // single shared pin -- exactly as multi-CF iterators do (NewIterators). This
  // replaces the former per-key SuperVersion + explicit-snapshot loop:
  // MultiCFSnapshot fixes one read sequence number for every key (falling back
  // to the DB mutex only if a flush races) without adding a snapshot-list
  // entry, and every key reads from the same Version so cross-key blob
  // resolution coalesces maximally.
  std::array<ColumnFamilySuperVersionPair, 1> cf_sv_pairs{};
  cf_sv_pairs[0] = ColumnFamilySuperVersionPair(column_family, nullptr);
  SequenceNumber consistent_seqnum = kMaxSequenceNumber;
  bool sv_from_thread_local = false;
  Status s = MultiCFSnapshot<std::array<ColumnFamilySuperVersionPair, 1>>(
      read_options, /*callback=*/nullptr,
      [](std::array<ColumnFamilySuperVersionPair, 1>::iterator& cf_iter) {
        return &(*cf_iter);
      },
      &cf_sv_pairs,
      /*extra_sv_ref=*/true, &consistent_seqnum, &sv_from_thread_local);
  if (!s.ok()) {
    for (size_t i = 0; i < num_keys; ++i) {
      statuses[i] = s;
    }
    return;
  }
  (void)sv_from_thread_local;  // extra_sv_ref: always an independent reference

  SuperVersion* const sv = cf_sv_pairs[0].super_version;

  for (size_t i = 0; i < num_keys; ++i) {
    statuses[i] =
        GetEntityLazyForBatch(read_options, column_family, sv,
                              consistent_seqnum, keys[i], &(*result)[i]);
  }

  // Transfer one shared SuperVersion pin into the batch (keyed by column
  // family), then release the call-scoped reference acquired above. Because
  // extra_sv_ref requested an independent reference, it is released via
  // CleanupSuperVersion (not the thread-local return path).
  TransferSuperVersionPin(
      sv, LazyWideColumnsHelper::BatchCfPin(result, cfd->GetID()));
  CleanupSuperVersion(sv);

  // Link the populated entities back to the batch so batch reads can validate
  // column ownership.
  LazyWideColumnsHelper::FinalizeBatch(result);
}

bool DBImpl::ShouldReferenceSuperVersion(const MergeContext& merge_context) {
  // If both thresholds are reached, a function returning merge operands as
  // `PinnableSlice`s should reference the `SuperVersion` to avoid large and/or
  // numerous `memcpy()`s.
  //
  // The below constants enable the optimization conservatively. They are
  // verified to not regress `GetMergeOperands()` latency in the following
  // scenarios.
  //
  // - CPU: two socket Intel(R) Xeon(R) Gold 6138 CPU @ 2.00GHz
  // - `GetMergeOperands()` threads: 1 - 32
  // - Entry size: 32 bytes - 4KB
  // - Merges per key: 1 - 16K
  // - LSM component: memtable
  //
  // TODO(ajkr): expand measurement to SST files.
  static const size_t kNumBytesForSvRef = 32768;
  static const size_t kLog2AvgBytesForSvRef = 8;  // 256 bytes

  size_t num_bytes = 0;
  for (const Slice& sl : merge_context.GetOperands()) {
    num_bytes += sl.size();
  }
  return num_bytes >= kNumBytesForSvRef &&
         (num_bytes >> kLog2AvgBytesForSvRef) >=
             merge_context.GetOperands().size();
}

static Slice GetBlobLookupUserKey(const Slice& user_key,
                                  const std::string* timestamp,
                                  std::string* user_key_with_ts) {
  if (timestamp == nullptr || timestamp->empty()) {
    return user_key;
  }

  assert(user_key_with_ts != nullptr);
  user_key_with_ts->assign(user_key.data(), user_key.size());
  user_key_with_ts->append(timestamp->data(), timestamp->size());
  return Slice(*user_key_with_ts);
}

static Status DecodeDirectWriteBlobIndex(const Slice& blob_index_slice,
                                         BlobIndex* blob_idx) {
  assert(blob_idx != nullptr);

  Status status = blob_idx->DecodeFrom(blob_index_slice);
  if (status.ok() && blob_idx->HasTTL()) {
    status =
        Status::Corruption("Unexpected TTL blob index for blob direct write");
  }
  return status;
}

Status DBImpl::ResolveDirectWritePlainValue(const Slice& key,
                                            const BlobFetcher& blob_fetcher,
                                            PinnableSlice* value,
                                            PinnableWideColumns* columns) {
  Slice blob_index_slice;
  std::string blob_index_storage;
  if (value != nullptr) {
    if (value->size() > 0) {
      blob_index_slice = Slice(value->data(), value->size());
    } else {
      blob_index_slice = Slice(*(value->GetSelf()));
    }
  } else {
    assert(columns != nullptr);

    const WideColumns& plain_value_columns = columns->columns();
    // A plain value exposed through `PinnableWideColumns` is represented as a
    // single default column at index 0. Resolve that default-column payload as
    // the user's value.
    assert(plain_value_columns.size() == 1);
    assert(plain_value_columns.front().name() == kDefaultWideColumnName);
    blob_index_slice = plain_value_columns.front().value();
  }

  PinnableSlice resolved_value;
  PinnableSlice* target = value != nullptr ? value : &resolved_value;
  if (value != nullptr) {
    if (value->IsPinned()) {
      // We reuse `value` as the destination for the resolved blob payload.
      // If it is still pinned to a table block, Reset() will drop that cleanup
      // chain, so preserve the encoded blob-index bytes first.
      blob_index_storage.assign(blob_index_slice.data(),
                                blob_index_slice.size());
      blob_index_slice = Slice(blob_index_storage);
    }
    value->Reset();
  }

  BlobIndex blob_idx;
  Status status = DecodeDirectWriteBlobIndex(blob_index_slice, &blob_idx);
  if (status.ok()) {
    status =
        blob_fetcher.FetchBlob(key, blob_idx, nullptr /* prefetch_buffer */,
                               target, nullptr /* bytes_read */);
    if (status.ok() && columns != nullptr) {
      columns->SetPlainValue(std::move(*target));
    }
  }
  return status;
}

Status DBImpl::ResolveDirectWriteWideColumns(const Slice& key,
                                             const BlobFetcher& blob_fetcher,
                                             PinnableWideColumns* columns) {
  assert(columns != nullptr);

  const WideColumns& unresolved_columns = columns->columns();
  WideColumns resolved_columns;
  resolved_columns.reserve(unresolved_columns.size());

  // These sorted column positions still hold encoded blob indexes. Fetched blob
  // payloads are held in address-stable backing nodes so the resolved column
  // Slices can point directly at them -- no re-serialization of the entity is
  // needed.
  const std::vector<size_t>& unresolved_blob_column_indices =
      PinnableWideColumnsHelper::GetUnresolvedBlobColumnIndices(*columns);
  std::forward_list<PinnableSlice> extra_buffers;
  size_t unresolved_blob_idx = 0;

  for (size_t column_idx = 0; column_idx < unresolved_columns.size();
       ++column_idx) {
    const bool is_unresolved_blob =
        unresolved_blob_idx < unresolved_blob_column_indices.size() &&
        unresolved_blob_column_indices[unresolved_blob_idx] == column_idx;
    if (!is_unresolved_blob) {
      resolved_columns.emplace_back(unresolved_columns[column_idx].name(),
                                    unresolved_columns[column_idx].value());
      continue;
    }

    BlobIndex blob_idx;
    Status s = DecodeDirectWriteBlobIndex(
        unresolved_columns[column_idx].value(), &blob_idx);
    if (!s.ok()) {
      return s;
    }

    Slice resolved_value;
    if (blob_idx.IsInlined()) {
      // V2 entities can still encode inline blob indexes. In that case the
      // bytes are already present in the entity buffer, so no blob file read is
      // needed and the resolved value points straight at the entity.
      resolved_value = blob_idx.value();
    } else {
      extra_buffers.emplace_front();
      PinnableSlice& blob_value = extra_buffers.front();
      s = blob_fetcher.FetchBlob(key, blob_idx, nullptr /* prefetch_buffer */,
                                 &blob_value, nullptr /* bytes_read */);
      if (!s.ok()) {
        return s;
      }
      resolved_value = blob_value;
      // Test hook: exposes the freshly fetched blob buffer so tests can assert
      // the resolved column Slice points directly at it (zero-copy) and stays
      // stable across moves of the PinnableWideColumns.
      TEST_SYNC_POINT_CALLBACK(
          "DBImpl::ResolveDirectWriteWideColumns:BlobFetched", &blob_value);
    }

    resolved_columns.emplace_back(unresolved_columns[column_idx].name(),
                                  resolved_value);
    ++unresolved_blob_idx;
  }

  if (unresolved_blob_idx != unresolved_blob_column_indices.size()) {
    return Status::Corruption("Wide column blob metadata out of sync");
  }

  PinnableWideColumnsHelper::ResolveColumns(
      *columns, std::move(resolved_columns), std::move(extra_buffers));
  return Status::OK();
}

Status DBImpl::MaybeResolveMemtableBlobValue(
    const Slice& key, const BlobFetcher* blob_fetcher, PinnableSlice* value,
    PinnableWideColumns* columns, bool* did_resolve, bool* is_blob_index,
    bool* value_found) {
  assert(did_resolve != nullptr);
  *did_resolve = false;

  if (!value && !columns) {
    return Status::OK();
  }

  auto reset_outputs = [&]() {
    if (value != nullptr) {
      value->Reset();
    }
    if (columns != nullptr) {
      columns->Reset();
    }
  };
  auto clear_blob_state = [&]() {
    if (is_blob_index != nullptr) {
      *is_blob_index = false;
    }
  };

  const bool needs_plain_value_resolution =
      is_blob_index != nullptr && *is_blob_index;
  const bool needs_wide_column_resolution =
      columns != nullptr &&
      !PinnableWideColumnsHelper::GetUnresolvedBlobColumnIndices(*columns)
           .empty();
  if (!needs_plain_value_resolution && !needs_wide_column_resolution) {
    return Status::OK();
  }

  // There is a blob reference to resolve, so this function now owns finalizing
  // `value`/`columns`: populated on success, cleared on error.
  *did_resolve = true;

  if (needs_plain_value_resolution) {
    if (blob_fetcher == nullptr) {
      reset_outputs();
      clear_blob_state();
      // We treat the memtable much like persisted data: prefer detecting and
      // reporting something potentially caused by corruption rather than
      // asserting/crashing, though in practice this is most likely to happen
      // from a programming mistake. (The wide-column entity path below
      // delegates this same check to ResolveEntityBlobColumnsMultiBuffer; the
      // plain-value path open-codes the fetch, so it checks here.)
      return Status::Corruption(
          "Encountered blob-backed memtable value without blob fetcher.");
    }

    Slice blob_index_slice;
    std::string blob_index_storage;
    if (value != nullptr) {
      if (value->size() > 0) {
        blob_index_slice = Slice(value->data(), value->size());
      } else {
        blob_index_slice = Slice(*(value->GetSelf()));
      }
    } else {
      assert(columns != nullptr);

      const WideColumns& plain_value_columns = columns->columns();
      assert(plain_value_columns.size() == 1);
      assert(plain_value_columns.front().name() == kDefaultWideColumnName);
      blob_index_slice = plain_value_columns.front().value();
    }

    PinnableSlice resolved_value;
    PinnableSlice* target = value != nullptr ? value : &resolved_value;
    if (value != nullptr) {
      // BlobIndex::DecodeFrom can retain Slices into the encoded bytes for
      // inlined blob indices, so take an owned copy before resetting `value`
      // and reusing the same PinnableSlice as the output target.
      blob_index_storage.assign(blob_index_slice.data(),
                                blob_index_slice.size());
      blob_index_slice = Slice(blob_index_storage);
      value->Reset();
    }

    Status s = blob_fetcher->FetchBlob(key, blob_index_slice,
                                       nullptr /* prefetch_buffer */, target,
                                       nullptr /* bytes_read */);
    if (s.ok() && columns != nullptr) {
      columns->SetPlainValue(std::move(*target));
    } else if (!s.ok()) {
      reset_outputs();
      if (s.IsIncomplete() && value_found != nullptr) {
        *value_found = false;
      }
    }

    clear_blob_state();
    return s;
  }

  assert(columns != nullptr);

  // Resolve blob columns into address-stable backing buffers and splice them
  // into `columns` without re-serializing the entity. Inline columns keep
  // zero-copy Slices into the original entity buffer (still held in `columns`).
  WideColumns resolved_columns;
  std::forward_list<PinnableSlice> extra_buffers;
  bool entity_resolved = false;
  Status s = WideColumnSerialization::ResolveEntityBlobColumnsMultiBuffer(
      PinnableWideColumnsHelper::GetSerializedEntity(*columns), key,
      blob_fetcher, nullptr /* prefetch_buffers */, resolved_columns,
      extra_buffers, entity_resolved, nullptr /* total_bytes_read */,
      nullptr /* num_blobs_resolved */);
  if (s.ok()) {
    assert(entity_resolved);
    if (entity_resolved) {
      PinnableWideColumnsHelper::ResolveColumns(
          *columns, std::move(resolved_columns), std::move(extra_buffers));
    }
  } else {
    reset_outputs();
    if (s.IsIncomplete() && value_found != nullptr) {
      *value_found = false;
    }
  }

  clear_blob_state();
  return s;
}

Status DBImpl::PostprocessMemtableValueRead(
    const Slice& key, const std::string* timestamp,
    bool resolve_blob_backed_memtable_value,
    const BlobFetcher* memtable_blob_fetcher, PinnableSlice* value,
    PinnableWideColumns* columns, Status memtable_read_status,
    bool* is_blob_index, bool* value_found) {
  Status status = std::move(memtable_read_status);

  bool outputs_finalized = false;
  if (status.ok() && resolve_blob_backed_memtable_value) {
    std::string blob_lookup_key_storage;
    bool did_resolve = false;
    status = MaybeResolveMemtableBlobValue(
        GetBlobLookupUserKey(key, timestamp, &blob_lookup_key_storage),
        memtable_blob_fetcher, value, columns, &did_resolve, is_blob_index,
        value_found);
    // When there was a blob reference, MaybeResolveMemtableBlobValue has
    // already finalized the outputs (populated on success, cleared on error).
    outputs_finalized = did_resolve;
  }

  if (!outputs_finalized) {
    if (status.ok()) {
      if (value != nullptr) {
        value->PinSelf();
      }
    } else {
      // Never leave a half-populated result behind a non-OK status.
      if (value != nullptr) {
        value->Reset();
      }
      if (columns != nullptr) {
        columns->Reset();
      }
    }
  }

  return status;
}

bool DBImpl::MaybeResolveDirectWriteValue(
    const ReadOptions& read_options, const Slice& key,
    bool resolve_direct_write_value, const BlobFetcher& blob_fetcher,
    PinnableSlice* value, PinnableWideColumns* columns, Status* s,
    bool* is_blob_index, bool* value_found) {
  if (!s->ok() || !resolve_direct_write_value || (!value && !columns)) {
    return false;
  }

  const bool needs_plain_value_resolution =
      is_blob_index != nullptr && *is_blob_index;
  const bool needs_wide_column_resolution =
      columns != nullptr &&
      !PinnableWideColumnsHelper::GetUnresolvedBlobColumnIndices(*columns)
           .empty();
  if (!needs_plain_value_resolution && !needs_wide_column_resolution) {
    return false;
  }

  // This helper only handles the temporary read-time gap for blob direct write
  // references that can appear before flush makes the blob file visible through
  // normal Version metadata. There are two shapes to patch up here:
  //   1. a plain value encoded as a blob index
  //   2. a wide-column entity with some unresolved blob-valued columns
  // Once a blob file is registered in Version metadata, the normal integrated
  // blob read path is already agnostic to whether the file came from direct
  // write or flush/compaction. This helper only covers the pre-flush gap where
  // a memtable can already reference a blob file that is not yet
  // manifest-visible.
  if (read_options.read_tier == kBlockCacheTier) {
    // Resolving a pre-flush direct-write reference can require blob file I/O.
    // `kBlockCacheTier` forbids that, so surface the result as incomplete.
    if (value != nullptr) {
      value->Reset();
    } else {
      assert(columns != nullptr);
      columns->Reset();
    }
    if (value_found != nullptr) {
      *value_found = false;
    }
    *s = Status::Incomplete();
    if (is_blob_index != nullptr) {
      *is_blob_index = false;
    }
    return true;
  }

  if (needs_plain_value_resolution) {
    *s = ResolveDirectWritePlainValue(key, blob_fetcher, value, columns);
    assert(is_blob_index != nullptr);
    *is_blob_index = false;
    return true;
  }

  assert(columns != nullptr);
  *s = ResolveDirectWriteWideColumns(key, blob_fetcher, columns);

  if (is_blob_index != nullptr) {
    *is_blob_index = false;
  }

  return true;
}

void DBImpl::PostprocessDirectWriteValueRead(
    const ReadOptions& read_options, const Slice& key,
    const std::string* timestamp, bool resolve_direct_write_value,
    const BlobFetcher* blob_fetcher, PinnableSlice* value,
    PinnableWideColumns* columns, Status* s, bool* is_blob_index,
    bool* value_found) {
  if (resolve_direct_write_value) {
    assert(blob_fetcher != nullptr);
    std::string blob_lookup_key_storage;
    const bool value_resolved = MaybeResolveDirectWriteValue(
        read_options,
        GetBlobLookupUserKey(key, timestamp, &blob_lookup_key_storage),
        resolve_direct_write_value, *blob_fetcher, value, columns, s,
        is_blob_index, value_found);
    if (!value_resolved && value != nullptr) {
      value->PinSelf();
    }
    return;
  }

  if (value != nullptr) {
    value->PinSelf();
  }
}

template <class T, typename IterDerefFuncType>
Status DBImpl::MultiCFSnapshot(const ReadOptions& read_options,
                               ReadCallback* callback,
                               IterDerefFuncType iter_deref_func, T* cf_list,
                               bool extra_sv_ref, SequenceNumber* snapshot,
                               bool* sv_from_thread_local) {
  PERF_TIMER_GUARD(get_snapshot_time);

  assert(sv_from_thread_local);
  *sv_from_thread_local = true;
  Status s = Status::OK();
  const bool check_read_ts =
      read_options.timestamp && read_options.timestamp->size() > 0;
  // sv_from_thread_local set to false means the SuperVersion to be cleaned up
  // is acquired directly via ColumnFamilyData instead of thread local.
  const auto sv_cleanup_func = [&]() -> void {
    for (auto cf_iter = cf_list->begin(); cf_iter != cf_list->end();
         ++cf_iter) {
      auto node = iter_deref_func(cf_iter);
      SuperVersion* super_version = node->super_version;
      ColumnFamilyData* cfd = node->cfd;
      if (super_version != nullptr) {
        if (*sv_from_thread_local && !extra_sv_ref) {
          ReturnAndCleanupSuperVersion(cfd, super_version);
        } else {
          CleanupSuperVersion(super_version);
        }
      }
      node->super_version = nullptr;
    }
  };

  bool acquire_mutex = false;
  if (cf_list->size() == 1) {
    // Fast path for a single column family. We can simply get the thread local
    // super version
    auto cf_iter = cf_list->begin();
    auto node = iter_deref_func(cf_iter);
    if (extra_sv_ref) {
      node->super_version = node->cfd->GetReferencedSuperVersion(this);
    } else {
      node->super_version = GetAndRefSuperVersion(node->cfd);
    }
    if (check_read_ts) {
      s = FailIfReadCollapsedHistory(node->cfd, node->super_version,
                                     *(read_options.timestamp));
    }
    if (s.ok() && read_options.snapshot != nullptr) {
      // Note: In WritePrepared txns this is not necessary but not harmful
      // either.  Because prep_seq > snapshot => commit_seq > snapshot so if
      // a snapshot is specified we should be fine with skipping seq numbers
      // that are greater than that.
      //
      // In WriteUnprepared, we cannot set snapshot in the lookup key because we
      // may skip uncommitted data that should be visible to the transaction for
      // reading own writes.
      *snapshot =
          static_cast<const SnapshotImpl*>(read_options.snapshot)->number_;
      if (callback) {
        *snapshot = std::max(*snapshot, callback->max_visible_seq());
      }
    } else if (s.ok()) {
      // Since we get and reference the super version before getting
      // the snapshot number, without a mutex protection, it is possible
      // that a memtable switch happened in the middle and not all the
      // data for this snapshot is available. But it will contain all
      // the data available in the super version we have, which is also
      // a valid snapshot to read from.
      // We shouldn't get snapshot before finding and referencing the super
      // version because a flush happening in between may compact away data for
      // the snapshot, but the snapshot is earlier than the data overwriting it,
      // so users may see wrong results.
      *snapshot = GetLastPublishedSequence();
    }
  } else {
    // If we end up with the same issue of memtable getting sealed during 2
    // consecutive retries, it means the write rate is very high. In that case
    // it's probably ok to take the mutex on the 3rd try so we can succeed for
    // sure.
    constexpr int num_retries = 3;
    for (int i = 0; i < num_retries; ++i) {
      // When reading from kPersistedTier, we want a consistent view into CFs.
      // So we take mutex to prevent any SV change in any CF.
      acquire_mutex = ((i == num_retries - 1) && !read_options.snapshot) ||
                      read_options.read_tier == kPersistedTier;
      bool retry = false;

      if (i > 0) {
        sv_cleanup_func();
      }
      if (read_options.snapshot == nullptr) {
        *snapshot = GetLastPublishedSequence();
      } else {
        *snapshot =
            static_cast_with_check<const SnapshotImpl>(read_options.snapshot)
                ->number_;
      }
      if (acquire_mutex) {
        TEST_SYNC_POINT("DBImpl::MultiCFSnapshot::LastTry");
        // We're close to max number of retries. For the last retry,
        // acquire the lock so we're sure to succeed
        mutex_.Lock();
      }
      for (auto cf_iter = cf_list->begin(); cf_iter != cf_list->end();
           ++cf_iter) {
        auto node = iter_deref_func(cf_iter);
        if (!acquire_mutex) {
          if (extra_sv_ref) {
            node->super_version = node->cfd->GetReferencedSuperVersion(this);
          } else {
            node->super_version = GetAndRefSuperVersion(node->cfd);
          }
        } else {
          node->super_version = node->cfd->GetSuperVersion()->Ref();
        }
        TEST_SYNC_POINT("DBImpl::MultiCFSnapshot::AfterRefSV");
        if (check_read_ts) {
          s = FailIfReadCollapsedHistory(node->cfd, node->super_version,
                                         *(read_options.timestamp));
          if (!s.ok()) {
            // If read timestamp check failed, a.k.a ReadOptions.timestamp <
            // super_version.full_history_ts_low. There is no need to continue
            // because this check will keep failing for the same and newer
            // SuperVersions, instead we fail fast and ask user to provide
            // a higher read timestamp.
            retry = false;
            break;
          }
        }
        TEST_SYNC_POINT("DBImpl::MultiCFSnapshot::BeforeCheckingSnapshot");
        if (read_options.snapshot != nullptr || acquire_mutex) {
          // If user passed a snapshot, then we don't care if a memtable is
          // sealed or compaction happens because the snapshot would ensure
          // that older key versions are kept around. If this is the last
          // retry, then we have the lock so nothing bad can happen
          continue;
        }
        // We could get the earliest sequence number for the whole list of
        // memtables, which will include immutable memtables as well, but that
        // might be tricky to maintain in case we decide, in future, to do
        // memtable compaction.
        if (!acquire_mutex) {
          SequenceNumber seq =
              node->super_version->mem->GetEarliestSequenceNumber();
          if (seq > *snapshot) {
            retry = true;
            break;
          }
        }
      }
      if (!retry) {
        if (acquire_mutex) {
          mutex_.Unlock();
          TEST_SYNC_POINT("DBImpl::MultiCFSnapshot::AfterLastTryRefSV");
        }
        break;
      }
      assert(!acquire_mutex);
    }
  }

  TEST_SYNC_POINT("DBImpl::MultiCFSnapshot:AfterGetSeqNum1");
  TEST_SYNC_POINT("DBImpl::MultiCFSnapshot:AfterGetSeqNum2");
  PERF_TIMER_STOP(get_snapshot_time);
  *sv_from_thread_local = !acquire_mutex;
  if (!s.ok()) {
    sv_cleanup_func();
  }
  return s;
}

void DBImpl::MultiGetWithMetadata(const ReadOptions& _read_options,
                                  const size_t num_keys,
                                  ColumnFamilyHandle* const* column_families,
                                  const Slice* keys, PinnableSlice* values,
                                  Status* statuses,
                                  MultiGetOutputMetadata* output_metadata,
                                  const bool sorted_input) {
  std::vector<std::string>* timestamps = GetOutputTimestamps(output_metadata);
  if (timestamps != nullptr) {
    timestamps->resize(num_keys);
  }
  std::vector<uint8_t>* newer_version_present =
      GetOutputNewerVersionPresent(output_metadata);
  if (newer_version_present != nullptr) {
    newer_version_present->assign(num_keys, false);
  }
  std::string* timestamp_data =
      timestamps != nullptr ? timestamps->data() : nullptr;
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
    return;
  }

  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kMultiGet;
  }
  const bool newer_version_present_requested =
      newer_version_present != nullptr && read_options.snapshot != nullptr;
  if (newer_version_present_requested &&
      read_options.read_tier == kPersistedTier) {
    Status s = Status::NotSupported(
        "Newer-version metadata is not supported with kPersistedTier");
    for (size_t i = 0; i < num_keys; ++i) {
      if (statuses[i].ok()) {
        statuses[i] = s;
      }
    }
    return;
  }
  autovector<ColumnFamilyHandle*, MultiGetContext::MAX_BATCH_SIZE>
      stack_column_families;
  std::vector<ColumnFamilyHandle*> heap_column_families;
  ColumnFamilyHandle** mutable_column_families = MakeMutableCfHandles(
      column_families, num_keys, &stack_column_families, &heap_column_families);
  MultiGetCommon(
      read_options, num_keys, mutable_column_families, keys, values,
      /* columns */ nullptr, timestamp_data, statuses,
      newer_version_present_requested ? newer_version_present : nullptr,
      sorted_input);
}

namespace {
// Order keys by CF ID, followed by key contents
struct CompareKeyContext {
  inline bool operator()(const KeyContext* lhs, const KeyContext* rhs) {
    ColumnFamilyHandleImpl* cfh =
        static_cast<ColumnFamilyHandleImpl*>(lhs->column_family);
    uint32_t cfd_id1 = cfh->cfd()->GetID();
    const Comparator* comparator = cfh->cfd()->user_comparator();
    cfh = static_cast<ColumnFamilyHandleImpl*>(rhs->column_family);
    uint32_t cfd_id2 = cfh->cfd()->GetID();

    if (cfd_id1 < cfd_id2) {
      return true;
    } else if (cfd_id1 > cfd_id2) {
      return false;
    }

    // Both keys are from the same column family
    int cmp = comparator->CompareWithoutTimestamp(
        *(lhs->key), /*a_has_ts=*/false, *(rhs->key), /*b_has_ts=*/false);
    if (cmp < 0) {
      return true;
    }
    return false;
  }
};

}  // anonymous namespace

void DBImpl::PrepareMultiGetKeys(
    size_t num_keys, bool sorted_input,
    autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE>* sorted_keys) {
  if (sorted_input) {
#ifndef NDEBUG
    assert(std::is_sorted(sorted_keys->begin(), sorted_keys->end(),
                          CompareKeyContext()));
#endif
    return;
  }

  std::sort(sorted_keys->begin(), sorted_keys->begin() + num_keys,
            CompareKeyContext());
}

void DB::MultiGetWithMetadata(const ReadOptions& options, const size_t num_keys,
                              ColumnFamilyHandle* const* column_families,
                              const Slice* keys, PinnableSlice* values,
                              Status* statuses,
                              MultiGetOutputMetadata* output_metadata,
                              const bool sorted_input) {
  std::vector<std::string>* timestamps = GetOutputTimestamps(output_metadata);
  if (timestamps != nullptr) {
    timestamps->resize(num_keys);
  }
  std::vector<uint8_t>* newer_version_present =
      GetOutputNewerVersionPresent(output_metadata);
  if (newer_version_present != nullptr) {
    newer_version_present->assign(num_keys, false);
  }
  std::string* timestamp_data =
      timestamps != nullptr ? timestamps->data() : nullptr;
  if (newer_version_present == nullptr || options.snapshot == nullptr) {
    autovector<ColumnFamilyHandle*, MultiGetContext::MAX_BATCH_SIZE>
        stack_column_families;
    std::vector<ColumnFamilyHandle*> heap_column_families;
    ColumnFamilyHandle** mutable_column_families =
        MakeMutableCfHandles(column_families, num_keys, &stack_column_families,
                             &heap_column_families);
    MultiGet(options, num_keys, mutable_column_families, keys, values,
             timestamp_data, statuses, sorted_input);
    return;
  }
  const Status s = Status::NotSupported(
      "MultiGetWithMetadata is not implemented by this DB subclass");
  for (size_t i = 0; i < num_keys; ++i) {
    if (statuses[i].ok()) {
      statuses[i] = s;
    }
  }
}

void DB::MultiGetWithMetadata(const ReadOptions& options,
                              ColumnFamilyHandle* column_family,
                              const size_t num_keys, const Slice* keys,
                              PinnableSlice* values, Status* statuses,
                              MultiGetOutputMetadata* output_metadata,
                              const bool sorted_input) {
  // Use std::array, if possible, to avoid memory allocation overhead
  if (num_keys > MultiGetContext::MAX_BATCH_SIZE) {
    std::vector<ColumnFamilyHandle*> column_families(num_keys, column_family);
    MultiGetWithMetadata(options, num_keys, column_families.data(), keys,
                         values, statuses, output_metadata, sorted_input);
  } else {
    std::array<ColumnFamilyHandle*, MultiGetContext::MAX_BATCH_SIZE>
        column_families{};
    std::fill(column_families.begin(), column_families.begin() + num_keys,
              column_family);
    MultiGetWithMetadata(options, num_keys, column_families.data(), keys,
                         values, statuses, output_metadata, sorted_input);
  }
}

void DB::MultiGetEntityWithMetadata(const ReadOptions& options, size_t num_keys,
                                    ColumnFamilyHandle* const* column_families,
                                    const Slice* keys,
                                    PinnableWideColumns* results,
                                    Status* statuses,
                                    MultiGetOutputMetadata* output_metadata,
                                    bool sorted_input) {
  const Status s = InitMultiGetEntityOutputMetadata(num_keys, output_metadata);
  if (!s.ok()) {
    std::fill_n(statuses, num_keys, s);
    return;
  }
  if (GetOutputNewerVersionPresent(output_metadata) != nullptr &&
      options.snapshot != nullptr) {
    std::fill_n(statuses, num_keys,
                Status::NotSupported("MultiGetEntityWithMetadata is not "
                                     "implemented by this DB subclass"));
    return;
  }
  MultiGetEntityWithoutNewerVersionTracking(this, options, num_keys,
                                            column_families, keys, results,
                                            statuses, sorted_input);
}

void DB::MultiGetEntityWithMetadata(const ReadOptions& options,
                                    ColumnFamilyHandle* column_family,
                                    size_t num_keys, const Slice* keys,
                                    PinnableWideColumns* results,
                                    Status* statuses,
                                    MultiGetOutputMetadata* output_metadata,
                                    bool sorted_input) {
  // Use std::array, if possible, to avoid memory allocation overhead
  if (num_keys > MultiGetContext::MAX_BATCH_SIZE) {
    std::vector<ColumnFamilyHandle*> column_families(num_keys, column_family);
    MultiGetEntityWithMetadata(options, num_keys, column_families.data(), keys,
                               results, statuses, output_metadata,
                               sorted_input);
  } else {
    std::array<ColumnFamilyHandle*, MultiGetContext::MAX_BATCH_SIZE>
        column_families{};
    std::fill(column_families.begin(), column_families.begin() + num_keys,
              column_family);
    MultiGetEntityWithMetadata(options, num_keys, column_families.data(), keys,
                               results, statuses, output_metadata,
                               sorted_input);
  }
}

void DB::MultiGetEntityWithMetadata(const ReadOptions& options, size_t num_keys,
                                    const Slice* keys,
                                    PinnableAttributeGroups* results,
                                    MultiGetOutputMetadata* output_metadata) {
  const size_t total_count = CountAttributeGroups(num_keys, results);
  const Status s =
      InitMultiGetEntityOutputMetadata(total_count, output_metadata);
  if (!results) {
    s.PermitUncheckedError();
    return;
  }
  if (!s.ok()) {
    SetAttributeGroupsStatus(num_keys, results, s);
    return;
  }
  if (GetOutputNewerVersionPresent(output_metadata) != nullptr &&
      options.snapshot != nullptr) {
    SetAttributeGroupsStatus(
        num_keys, results,
        Status::NotSupported("MultiGetEntityWithMetadata is not implemented "
                             "by this DB subclass"));
    return;
  }
  MultiGetEntity(options, num_keys, keys, results);
}

void DBImpl::MultiGetCommon(const ReadOptions& read_options,
                            ColumnFamilyHandle* column_family,
                            const size_t num_keys, const Slice* keys,
                            PinnableSlice* values, PinnableWideColumns* columns,
                            std::string* timestamps, Status* statuses,
                            bool sorted_input) {
  if (tracer_) {
    // TODO: This mutex should be removed later, to improve performance when
    // tracing is enabled.
    InstrumentedMutexLock lock(&trace_mutex_);
    if (tracer_) {
      // TODO: maybe handle the tracing status?
      tracer_->MultiGet(num_keys, column_family, keys, read_options)
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

    key_context.emplace_back(column_family, keys[i], val, col,
                             timestamps ? &timestamps[i] : nullptr,
                             &statuses[i]);
  }
  for (size_t i = 0; i < num_keys; ++i) {
    sorted_keys[i] = &key_context[i];
  }
  PrepareMultiGetKeys(num_keys, sorted_input, &sorted_keys);
  MultiGetWithCallbackImpl(read_options, column_family, nullptr, &sorted_keys);
}

void DBImpl::MultiGetWithCallback(
    const ReadOptions& _read_options, ColumnFamilyHandle* column_family,
    ReadCallback* callback,
    autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE>* sorted_keys) {
  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kMultiGet) {
    assert(false);
    return;
  }

  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kMultiGet;
  }
  MultiGetWithCallbackImpl(read_options, column_family, callback, sorted_keys);
}

void DBImpl::MultiGetWithCallbackImpl(
    const ReadOptions& read_options, ColumnFamilyHandle* column_family,
    ReadCallback* callback,
    autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE>* sorted_keys) {
  assert(sorted_keys != nullptr);
  std::array<ColumnFamilySuperVersionPair, 1> cf_sv_pairs;
  cf_sv_pairs[0] = ColumnFamilySuperVersionPair(column_family, nullptr);
  size_t num_keys = sorted_keys->size();
  if (num_keys == 0) {
    return;
  }
  SequenceNumber consistent_seqnum = kMaxSequenceNumber;
  bool sv_from_thread_local = false;
  Status s = MultiCFSnapshot<std::array<ColumnFamilySuperVersionPair, 1>>(
      read_options, callback,
      [](std::array<ColumnFamilySuperVersionPair, 1>::iterator& cf_iter) {
        return &(*cf_iter);
      },
      &cf_sv_pairs,
      /* extra_sv_ref */ false, &consistent_seqnum, &sv_from_thread_local);
  if (!s.ok()) {
    return;
  }
#ifndef NDEBUG
  assert(sv_from_thread_local);
#else
  // Silence unused variable warning
  (void)sv_from_thread_local;
#endif  // NDEBUG

  if (callback && read_options.snapshot == nullptr) {
    // The unprep_seqs are not published for write unprepared, so it could be
    // that max_visible_seq is larger. Seek to the std::max of the two.
    // However, we still want our callback to contain the actual snapshot so
    // that it can do the correct visibility filtering.
    callback->Refresh(consistent_seqnum);

    // Internally, WriteUnpreparedTxnReadCallback::Refresh would set
    // max_visible_seq = max(max_visible_seq, snapshot)
    //
    // Currently, the commented out assert is broken by
    // InvalidSnapshotReadCallback, but if write unprepared recovery followed
    // the regular transaction flow, then this special read callback would not
    // be needed.
    //
    // assert(callback->max_visible_seq() >= snapshot);
    consistent_seqnum = callback->max_visible_seq();
  }

  GetWithTimestampReadCallback timestamp_read_callback(0);
  ReadCallback* read_callback = callback;
  if (read_options.timestamp && read_options.timestamp->size() > 0) {
    assert(!read_callback);  // timestamp with callback is not supported
    timestamp_read_callback.Refresh(consistent_seqnum);
    read_callback = &timestamp_read_callback;
  }

  s = MultiGetImpl(read_options, 0, num_keys, sorted_keys,
                   cf_sv_pairs[0].super_version, consistent_seqnum,
                   read_callback);
  assert(s.ok() || s.IsTimedOut() || s.IsAborted());
  ReturnAndCleanupSuperVersion(cf_sv_pairs[0].cfd,
                               cf_sv_pairs[0].super_version);
}

void DBImpl::MultiGetEntity(const ReadOptions& _read_options, size_t num_keys,
                            ColumnFamilyHandle** column_families,
                            const Slice* keys, PinnableWideColumns* results,
                            Status* statuses, bool sorted_input) {
  assert(statuses);

  if (!column_families) {
    const Status s = Status::InvalidArgument(
        "Cannot call MultiGetEntity without column families");
    for (size_t i = 0; i < num_keys; ++i) {
      statuses[i] = s;
    }

    return;
  }

  if (!keys) {
    const Status s =
        Status::InvalidArgument("Cannot call MultiGetEntity without keys");
    for (size_t i = 0; i < num_keys; ++i) {
      statuses[i] = s;
    }

    return;
  }

  if (!results) {
    const Status s = Status::InvalidArgument(
        "Cannot call MultiGetEntity without PinnableWideColumns objects");
    for (size_t i = 0; i < num_keys; ++i) {
      statuses[i] = s;
    }

    return;
  }

  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kMultiGetEntity) {
    const Status s = Status::InvalidArgument(
        "Can only call MultiGetEntity with `ReadOptions::io_activity` set to "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kMultiGetEntity`");
    for (size_t i = 0; i < num_keys; ++i) {
      statuses[i] = s;
    }

    return;
  }

  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kMultiGetEntity;
  }

  MultiGetCommon(read_options, num_keys, column_families, keys,
                 /* values */ nullptr, results, /* timestamps */ nullptr,
                 statuses, /* newer_version_present */ nullptr, sorted_input);
}

void DBImpl::MultiGetEntityWithMetadata(
    const ReadOptions& _read_options, size_t num_keys,
    ColumnFamilyHandle* const* column_families, const Slice* keys,
    PinnableWideColumns* results, Status* statuses,
    MultiGetOutputMetadata* output_metadata, bool sorted_input) {
  assert(statuses);

  std::vector<uint8_t>* newer_version_present =
      GetOutputNewerVersionPresent(output_metadata);
  const bool newer_version_present_requested =
      newer_version_present != nullptr && _read_options.snapshot != nullptr;
  Status s = InitMultiGetEntityOutputMetadata(num_keys, output_metadata);
  if (s.ok()) {
    if (!column_families) {
      s = Status::InvalidArgument(
          "Cannot call MultiGetEntityWithMetadata without column families");
    } else if (!keys) {
      s = Status::InvalidArgument(
          "Cannot call MultiGetEntityWithMetadata without keys");
    } else if (!results) {
      s = Status::InvalidArgument(
          "Cannot call MultiGetEntityWithMetadata without "
          "PinnableWideColumns objects");
    } else if (_read_options.io_activity != Env::IOActivity::kUnknown &&
               _read_options.io_activity != Env::IOActivity::kMultiGetEntity) {
      s = Status::InvalidArgument(
          "Can only call MultiGetEntityWithMetadata with "
          "`ReadOptions::io_activity` set to `Env::IOActivity::kUnknown` or "
          "`Env::IOActivity::kMultiGetEntity`");
    } else if (newer_version_present_requested &&
               _read_options.read_tier == kPersistedTier) {
      s = Status::NotSupported(
          "Newer-version metadata is not supported with kPersistedTier");
    }
  }
  if (!s.ok()) {
    std::fill_n(statuses, num_keys, s);
    return;
  }

  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kMultiGetEntity;
  }
  autovector<ColumnFamilyHandle*, MultiGetContext::MAX_BATCH_SIZE>
      stack_column_families;
  std::vector<ColumnFamilyHandle*> heap_column_families;
  ColumnFamilyHandle** mutable_column_families = MakeMutableCfHandles(
      column_families, num_keys, &stack_column_families, &heap_column_families);
  MultiGetCommon(
      read_options, num_keys, mutable_column_families, keys,
      /* values */ nullptr, results, /* timestamps */ nullptr, statuses,
      newer_version_present_requested ? newer_version_present : nullptr,
      sorted_input);
}

void DBImpl::MultiGetEntity(const ReadOptions& _read_options,
                            ColumnFamilyHandle* column_family, size_t num_keys,
                            const Slice* keys, PinnableWideColumns* results,
                            Status* statuses, bool sorted_input) {
  assert(statuses);

  if (!column_family) {
    const Status s = Status::InvalidArgument(
        "Cannot call MultiGetEntity without a column family handle");
    for (size_t i = 0; i < num_keys; ++i) {
      statuses[i] = s;
    }

    return;
  }

  if (!keys) {
    const Status s =
        Status::InvalidArgument("Cannot call MultiGetEntity without keys");
    for (size_t i = 0; i < num_keys; ++i) {
      statuses[i] = s;
    }

    return;
  }

  if (!results) {
    const Status s = Status::InvalidArgument(
        "Cannot call MultiGetEntity without PinnableWideColumns objects");
    for (size_t i = 0; i < num_keys; ++i) {
      statuses[i] = s;
    }

    return;
  }

  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kMultiGetEntity) {
    const Status s = Status::InvalidArgument(
        "Can only call MultiGetEntity with `ReadOptions::io_activity` set to "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kMultiGetEntity`");
    for (size_t i = 0; i < num_keys; ++i) {
      statuses[i] = s;
    }
    return;
  }

  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kMultiGetEntity;
  }

  MultiGetCommon(read_options, column_family, num_keys, keys,
                 /* values */ nullptr, results, /* timestamps */ nullptr,
                 statuses, sorted_input);
}

void DBImpl::MultiGetEntity(const ReadOptions& _read_options, size_t num_keys,
                            const Slice* keys,
                            PinnableAttributeGroups* results) {
  assert(results);

  if (!keys) {
    const Status s =
        Status::InvalidArgument("Cannot call MultiGetEntity without keys");
    for (size_t i = 0; i < num_keys; ++i) {
      for (size_t j = 0; j < results[i].size(); ++j) {
        results[i][j].SetStatus(s);
      }
    }

    return;
  }

  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kMultiGetEntity) {
    const Status s = Status::InvalidArgument(
        "Can only call MultiGetEntity with `ReadOptions::io_activity` set to "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kMultiGetEntity`");
    for (size_t i = 0; i < num_keys; ++i) {
      for (size_t j = 0; j < results[i].size(); ++j) {
        results[i][j].SetStatus(s);
      }
    }

    return;
  }

  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kMultiGetEntity;
  }

  std::vector<ColumnFamilyHandle*> column_families;
  std::vector<Slice> all_keys;
  size_t total_count = 0;

  for (size_t i = 0; i < num_keys; ++i) {
    for (size_t j = 0; j < results[i].size(); ++j) {
      // Adding the same key slice for different CFs
      all_keys.emplace_back(keys[i]);
      column_families.emplace_back(results[i][j].column_family());
      ++total_count;
    }
  }

  std::vector<Status> statuses(total_count);
  std::vector<PinnableWideColumns> columns(total_count);
  MultiGetCommon(read_options, total_count, column_families.data(),
                 all_keys.data(),
                 /* values */ nullptr, columns.data(),
                 /* timestamps */ nullptr, statuses.data(),
                 /* newer_version_present */ nullptr,
                 /* sorted_input */ false);

  // Set results
  size_t index = 0;
  for (size_t i = 0; i < num_keys; ++i) {
    for (size_t j = 0; j < results[i].size(); ++j) {
      results[i][j].Reset();
      results[i][j].SetStatus(std::move(statuses[index]));
      results[i][j].SetColumns(std::move(columns[index]));
      ++index;
    }
  }
}

void DBImpl::MultiGetEntityWithMetadata(
    const ReadOptions& _read_options, size_t num_keys, const Slice* keys,
    PinnableAttributeGroups* results, MultiGetOutputMetadata* output_metadata) {
  const size_t total_count = CountAttributeGroups(num_keys, results);
  std::vector<uint8_t>* newer_version_present =
      GetOutputNewerVersionPresent(output_metadata);
  const bool newer_version_present_requested =
      newer_version_present != nullptr && _read_options.snapshot != nullptr;
  Status s = InitMultiGetEntityOutputMetadata(total_count, output_metadata);
  if (!results) {
    s.PermitUncheckedError();
    return;
  }
  if (s.ok()) {
    if (!keys) {
      s = Status::InvalidArgument(
          "Cannot call MultiGetEntityWithMetadata without keys");
    } else if (_read_options.io_activity != Env::IOActivity::kUnknown &&
               _read_options.io_activity != Env::IOActivity::kMultiGetEntity) {
      s = Status::InvalidArgument(
          "Can only call MultiGetEntityWithMetadata with "
          "`ReadOptions::io_activity` set to `Env::IOActivity::kUnknown` or "
          "`Env::IOActivity::kMultiGetEntity`");
    } else if (newer_version_present_requested &&
               _read_options.read_tier == kPersistedTier) {
      s = Status::NotSupported(
          "Newer-version metadata is not supported with kPersistedTier");
    }
  }
  if (!s.ok()) {
    SetAttributeGroupsStatus(num_keys, results, s);
    return;
  }
  if (total_count == 0) {
    return;
  }

  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kMultiGetEntity;
  }

  std::vector<ColumnFamilyHandle*> column_families;
  std::vector<Slice> all_keys;
  column_families.reserve(total_count);
  all_keys.reserve(total_count);
  for (size_t i = 0; i < num_keys; ++i) {
    for (size_t j = 0; j < results[i].size(); ++j) {
      all_keys.emplace_back(keys[i]);
      column_families.emplace_back(results[i][j].column_family());
    }
  }

  std::vector<Status> statuses(total_count);
  std::vector<PinnableWideColumns> columns(total_count);
  MultiGetCommon(
      read_options, total_count, column_families.data(), all_keys.data(),
      /* values */ nullptr, columns.data(), /* timestamps */ nullptr,
      statuses.data(),
      newer_version_present_requested ? newer_version_present : nullptr,
      /* sorted_input */ false);

  size_t index = 0;
  for (size_t i = 0; i < num_keys; ++i) {
    for (size_t j = 0; j < results[i].size(); ++j) {
      results[i][j].Reset();
      results[i][j].SetStatus(statuses[index]);
      results[i][j].SetColumns(std::move(columns[index]));
      ++index;
    }
  }
}

void DBImpl::MultiGetEntityWithCallback(
    const ReadOptions& read_options, ColumnFamilyHandle* column_family,
    ReadCallback* callback,
    autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE>* sorted_keys) {
  assert(read_options.io_activity == Env::IOActivity::kMultiGetEntity);

  MultiGetWithCallbackImpl(read_options, column_family, callback, sorted_keys);
}

Status DBImpl::WrapUpCreateColumnFamilies(
    const WriteOptions& write_options,
    const std::vector<const ColumnFamilyOptions*>& cf_options) {
  options_mutex_.AssertHeld();

  // NOTE: this function is skipped for create_missing_column_families and
  // DB::Open, so new functionality here might need to go into Open also.
  bool register_worker = false;
  for (auto* opts_ptr : cf_options) {
    if (opts_ptr->preserve_internal_time_seconds > 0 ||
        opts_ptr->preclude_last_level_data_seconds > 0) {
      register_worker = true;
      break;
    }
  }
  // Tracked creates already published their OPTIONS snapshot in the same
  // MANIFEST edit. Legacy creates retain the historical post-create write.
  Status s;
  if (immutable_db_options_.track_options_file_number_in_manifest) {
    MaybeDeleteObsoleteOptionsFiles();
  } else {
    s = WriteOptionsFile(write_options, false /*db_mutex_already_held*/);
  }
  if (register_worker) {
    s.UpdateIfOk(RegisterRecordSeqnoTimeWorker());
  }
  return s;
}

Status DBImpl::CreateColumnFamily(const ReadOptions& read_options,
                                  const WriteOptions& write_options,
                                  const ColumnFamilyOptions& cf_options,
                                  const std::string& column_family,
                                  ColumnFamilyHandle** handle) {
  assert(handle != nullptr);
  DeferredInstrumentedMutexLock ol(
      &options_mutex_,
      stats_ != nullptr && immutable_db_options_.info_log != nullptr);
  Status s = CreateColumnFamilyImpl(read_options, write_options, cf_options,
                                    column_family, handle);
  if (s.ok()) {
    s.UpdateIfOk(WrapUpCreateColumnFamilies(write_options, {&cf_options}));
  }
  return s;
}

Status DBImpl::CreateColumnFamilies(
    const ReadOptions& read_options, const WriteOptions& write_options,
    const ColumnFamilyOptions& cf_options,
    const std::vector<std::string>& column_family_names,
    std::vector<ColumnFamilyHandle*>* handles) {
  assert(handles != nullptr);
  DeferredInstrumentedMutexLock ol(
      &options_mutex_,
      stats_ != nullptr && immutable_db_options_.info_log != nullptr);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.reserve(column_family_names.size());
  for (const auto& name : column_family_names) {
    column_families.emplace_back(name, cf_options);
  }
  Status s = CreateColumnFamiliesImpl(read_options, write_options,
                                      column_families, handles);
  if (!handles->empty()) {
    s.UpdateIfOk(WrapUpCreateColumnFamilies(write_options, {&cf_options}));
  }
  return s;
}

Status DBImpl::CreateColumnFamilies(
    const ReadOptions& read_options, const WriteOptions& write_options,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles) {
  assert(handles != nullptr);
  DeferredInstrumentedMutexLock ol(
      &options_mutex_,
      stats_ != nullptr && immutable_db_options_.info_log != nullptr);
  Status s = CreateColumnFamiliesImpl(read_options, write_options,
                                      column_families, handles);
  std::vector<const ColumnFamilyOptions*> cf_opts;
  cf_opts.reserve(handles->size());
  for (size_t i = 0; i < handles->size(); ++i) {
    cf_opts.push_back(&column_families[i].options);
  }
  if (!handles->empty()) {
    s.UpdateIfOk(WrapUpCreateColumnFamilies(write_options, cf_opts));
  }
  return s;
}

Status DBImpl::CreateColumnFamilyImpl(const ReadOptions& read_options,
                                      const WriteOptions& write_options,
                                      const ColumnFamilyOptions& cf_options,
                                      const std::string& column_family_name,
                                      ColumnFamilyHandle** handle) {
  std::vector<ColumnFamilyHandle*> handles;
  Status s = CreateColumnFamiliesImpl(
      read_options, write_options,
      {ColumnFamilyDescriptor(column_family_name, cf_options)}, &handles);
  *handle = handles.empty() ? nullptr : handles.front();
  return s;
}

Status DBImpl::CreateColumnFamiliesImpl(
    const ReadOptions& read_options, const WriteOptions& write_options,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles) {
  options_mutex_.AssertHeld();
  assert(handles != nullptr);
  handles->clear();

  std::unordered_set<std::string> unavailable_names;
  {
    InstrumentedMutexLock l(&mutex_);
    ColumnFamilySet* column_family_set = versions_->GetColumnFamilySet();
    assert(column_family_set != nullptr);
    for (auto* cfd : *column_family_set) {
      if (!cfd->IsDropped()) {
        unavailable_names.insert(cfd->GetName());
      }
    }
  }

  const DBOptions db_options =
      BuildDBOptions(immutable_db_options_, mutable_db_options_);
  std::vector<ColumnFamilyDescriptor> prepared_column_families;
  prepared_column_families.reserve(column_families.size());
  Status preparation_status;
  for (const auto& column_family : column_families) {
    const std::string& column_family_name = column_family.name;

    // The empty string is reserved as a "no/unknown column family name"
    // sentinel in APIs and serialization formats.
    if (column_family_name.empty()) {
      preparation_status = Status::InvalidArgument(
          "Column family name cannot be the empty string");
      break;
    }
    preparation_status = ValidateCompactionFilterOptions(column_family.options);
    if (preparation_status.ok()) {
      preparation_status =
          ColumnFamilyData::ValidateOptions(db_options, column_family.options);
    }
    if (preparation_status.ok()) {
      for (const auto& cf_path : column_family.options.cf_paths) {
        preparation_status = env_->CreateDirIfMissing(cf_path.path);
        if (!preparation_status.ok()) {
          break;
        }
      }
    }
    if (!preparation_status.ok()) {
      break;
    }
    if (unavailable_names.count(column_family_name) != 0) {
      preparation_status =
          Status::InvalidArgument("Column family already exists");
      break;
    }
    unavailable_names.insert(column_family_name);
    prepared_column_families.emplace_back(
        column_family_name,
        SanitizeCfOptions(immutable_db_options_, /*read_only=*/false,
                          column_family.options));
  }

  if (prepared_column_families.empty()) {
    return preparation_status;
  }

  uint64_t options_file_number = 0;
  uint64_t options_file_size = 0;
  Status s = PersistOptionsFileForColumnFamilyManipulation(
      write_options, prepared_column_families, {}, &options_file_number,
      &options_file_size);
  if (!s.ok()) {
    return s;
  }
  // TEST_SYNC_POINT handles the disabled singleton case internally.
  // @lint-ignore NULLSAFECLANG nullable-dereference
  TEST_SYNC_POINT("DBImpl::CreateColumnFamilyImpl:AfterPersistOptions");

  for (size_t i = 0; i < prepared_column_families.size(); ++i) {
    const auto& column_family = prepared_column_families[i];
    ColumnFamilyHandle* handle = nullptr;
    s = CreateColumnFamilyWithPreparedOptions(
        read_options, write_options, column_family.options, column_family.name,
        i == 0 ? options_file_number : 0, i == 0 ? options_file_size : 0,
        &handle);
    if (!s.ok()) {
      break;
    }
    handles->push_back(handle);
  }
  if (s.ok() && !preparation_status.ok()) {
    s = preparation_status;
  }
  return s;
}

Status DBImpl::CreateColumnFamilyWithPreparedOptions(
    const ReadOptions& read_options, const WriteOptions& write_options,
    const ColumnFamilyOptions& sanitized_cf_options,
    const std::string& column_family_name, uint64_t options_file_number,
    uint64_t options_file_size, ColumnFamilyHandle** handle) {
  options_mutex_.AssertHeld();
  *handle = nullptr;

  {
    InstrumentedMutexLock l(&mutex_);
    ColumnFamilySet* column_family_set = versions_->GetColumnFamilySet();
    assert(column_family_set != nullptr);
    if (column_family_set->GetColumnFamily(column_family_name) != nullptr) {
      return Status::InvalidArgument("Column family already exists");
    }
  }

  Status s = ValidateCompactionFilterOptions(sanitized_cf_options);
  if (!s.ok()) {
    return s;
  }
  SuperVersionContext sv_context(/* create_superversion */ true);
  {
    InstrumentedMutexLock l(&mutex_);

    if (versions_->GetColumnFamilySet()->GetColumnFamily(column_family_name) !=
        nullptr) {
      return Status::InvalidArgument("Column family already exists");
    }
    VersionEdit edit;
    edit.AddColumnFamily(column_family_name);
    uint32_t new_id = versions_->GetColumnFamilySet()->GetNextColumnFamilyID();
    edit.SetColumnFamily(new_id);
    edit.SetLogNumber(cur_wal_number_);
    edit.SetComparatorName(sanitized_cf_options.comparator->Name());
    edit.SetPersistUserDefinedTimestamps(
        sanitized_cf_options.persist_user_defined_timestamps);
    if (options_file_number != 0) {
      edit.SetCommittedOptionsFileNumber(options_file_number);
    }

    // LogAndApply will both write the creation in MANIFEST and create
    // ColumnFamilyData object
    {  // write thread
      WriteThread::Writer w;
      write_thread_.EnterUnbatched(&w, &mutex_);
      s = versions_->LogAndApplyColumnFamilyAddWithSanitizedOptions(
          read_options, write_options, &edit, &mutex_, directories_.GetDbDir(),
          sanitized_cf_options);
      write_thread_.ExitUnbatched(&w);
    }
    if (s.ok() && options_file_number != 0) {
      versions_->options_file_size_ = options_file_size;
    }
    if (s.ok()) {
      auto* cfd =
          versions_->GetColumnFamilySet()->GetColumnFamily(column_family_name);
      assert(cfd != nullptr);
      std::map<std::string, std::shared_ptr<FSDirectory>> dummy_created_dirs;
      s = cfd->AddDirectories(&dummy_created_dirs);
    }
    if (s.ok()) {
      auto* cfd =
          versions_->GetColumnFamilySet()->GetColumnFamily(column_family_name);
      assert(cfd != nullptr);
      MaybeInitBlobDirectWriteColumnFamily(cfd, sanitized_cf_options,
                                           column_family_name);
      InstallSuperVersionForConfigChange(cfd, &sv_context);

      if (!cfd->mem()->IsSnapshotSupported()) {
        // A concurrent GetSnapshot() checks this state while holding one
        // snapshot shard. Lock every shard for the true-to-false transition so
        // the getter either finishes first or observes that snapshots are no
        // longer supported.
        snapshots_.RunWithAllShardsLocked([this]() {
          is_snapshot_supported_.store(false, std::memory_order_relaxed);
        });
      }

      cfd->set_initialized();

      *handle = new ColumnFamilyHandleImpl(cfd, this, &mutex_);
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "Created column family [%s] (ID %u)",
                     column_family_name.c_str(), (unsigned)cfd->GetID());
    } else {
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "Creating column family [%s] FAILED -- %s",
                      column_family_name.c_str(), s.ToString().c_str());
    }
  }  // InstrumentedMutexLock l(&mutex_)

  sv_context.Clean();
  if (s.ok()) {
    NewThreadStatusCfInfo(
        static_cast_with_check<ColumnFamilyHandleImpl>(*handle)->cfd());
  }
  return s;
}

Status DBImpl::DropColumnFamily(ColumnFamilyHandle* column_family) {
  assert(column_family != nullptr);
  DeferredInstrumentedMutexLock ol(
      &options_mutex_,
      stats_ != nullptr && immutable_db_options_.info_log != nullptr);
  Status s = DropColumnFamilyImpl(column_family);
  if (s.ok()) {
    if (immutable_db_options_.track_options_file_number_in_manifest) {
      MaybeDeleteObsoleteOptionsFiles();
    } else {
      // TODO: plumb Env::IOActivity, Env::IOPriority
      s = WriteOptionsFile(WriteOptions(), false /*db_mutex_already_held*/);
    }
  }
  return s;
}

Status DBImpl::DropColumnFamilies(
    const std::vector<ColumnFamilyHandle*>& column_families) {
  DeferredInstrumentedMutexLock ol(
      &options_mutex_,
      stats_ != nullptr && immutable_db_options_.info_log != nullptr);
  Status s;
  bool success_once = false;
  std::vector<ColumnFamilyHandle*> validated_column_families;
  std::vector<uint32_t> excluded_cf_ids;
  std::unordered_set<uint32_t> seen_cf_ids;
  for (auto* handle : column_families) {
    auto* cfd = static_cast_with_check<ColumnFamilyHandleImpl>(handle)->cfd();
    assert(cfd != nullptr);
    if (cfd->GetID() == 0) {
      s = Status::InvalidArgument("Can't drop default column family");
      break;
    }
    {
      InstrumentedMutexLock l(&mutex_);
      if (cfd->IsDropped() || !seen_cf_ids.insert(cfd->GetID()).second) {
        s = Status::InvalidArgument("Column family already dropped!\n");
        break;
      }
    }
    validated_column_families.push_back(handle);
    excluded_cf_ids.push_back(cfd->GetID());
  }

  uint64_t options_file_number = 0;
  uint64_t options_file_size = 0;
  if (s.ok() && !validated_column_families.empty()) {
    s = PersistOptionsFileForColumnFamilyManipulation(
        WriteOptions(), {}, excluded_cf_ids, &options_file_number,
        &options_file_size);
    if (!s.ok()) {
      return s;
    }
  }
  const Status validation_status = s;
  s = Status::OK();

  ColumnFamilySet* const column_family_set = versions_->GetColumnFamilySet();
  const bool batch_flushable_mem_accounting =
      write_buffer_manager_ != nullptr &&
      write_buffer_manager_->ShouldTrackFlushInitiator();
  if (batch_flushable_mem_accounting) {
    InstrumentedMutexLock lock(&mutex_);
    column_family_set->BeginBulkFlushableMemAccountingUpdate();
  }
  {
    Defer finish_bulk_accounting(
        [this, column_family_set, batch_flushable_mem_accounting] {
          if (batch_flushable_mem_accounting) {
            InstrumentedMutexLock lock(&mutex_);
            column_family_set->EndBulkFlushableMemAccountingUpdate();
          }
        });
    for (size_t i = 0; i < validated_column_families.size(); ++i) {
      s = DropColumnFamilyWithOptionsFile(
          validated_column_families[i],
          validation_status.ok() && i + 1 == validated_column_families.size()
              ? options_file_number
              : 0,
          validation_status.ok() && i + 1 == validated_column_families.size()
              ? options_file_size
              : 0);
      if (!s.ok()) {
        break;
      }
      success_once = true;
    }
  }
  if (s.ok() && !validation_status.ok()) {
    s = validation_status;
  }
  if (success_once) {
    if (immutable_db_options_.track_options_file_number_in_manifest) {
      MaybeDeleteObsoleteOptionsFiles();
    } else {
      // TODO: plumb Env::IOActivity, Env::IOPriority
      s.UpdateIfOk(
          WriteOptionsFile(WriteOptions(), false /*db_mutex_already_held*/));
    }
  }
  return s;
}

Status DBImpl::DropColumnFamilyImpl(ColumnFamilyHandle* column_family) {
  options_mutex_.AssertHeld();

  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();
  assert(cfd != nullptr);
  if (cfd->GetID() == 0) {
    return Status::InvalidArgument("Can't drop default column family");
  }

  {
    InstrumentedMutexLock l(&mutex_);
    if (cfd->IsDropped()) {
      return Status::InvalidArgument("Column family already dropped!\n");
    }
  }

  uint64_t options_file_number = 0;
  uint64_t options_file_size = 0;
  Status s = PersistOptionsFileForColumnFamilyManipulation(
      WriteOptions(), {}, {cfd->GetID()}, &options_file_number,
      &options_file_size);
  if (!s.ok()) {
    return s;
  }
  return DropColumnFamilyWithOptionsFile(column_family, options_file_number,
                                         options_file_size);
}

Status DBImpl::DropColumnFamilyWithOptionsFile(
    ColumnFamilyHandle* column_family, uint64_t options_file_number,
    uint64_t options_file_size) {
  options_mutex_.AssertHeld();

  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;
  const WriteOptions write_options;

  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();
  assert(cfd != nullptr);
  if (cfd->GetID() == 0) {
    return Status::InvalidArgument("Can't drop default column family");
  }

  bool cf_support_snapshot = cfd->mem()->IsSnapshotSupported();
  Status s;
  VersionEdit edit;
  edit.DropColumnFamily();
  edit.SetColumnFamily(cfd->GetID());
  if (options_file_number != 0) {
    edit.SetCommittedOptionsFileNumber(options_file_number);
  }

  // Save re-aquiring lock for RegisterRecordSeqnoTimeWorker when not
  // applicable
  MinAndMaxPreserveSeconds preserve_info;
  {
    InstrumentedMutexLock l(&mutex_);
    if (cfd->IsDropped()) {
      s = Status::InvalidArgument("Column family already dropped!\n");
    }
    if (s.ok()) {
      // we drop column family from a single write thread
      WriteThread::Writer w;
      write_thread_.EnterUnbatched(&w, &mutex_);
      WaitForPendingWrites();
      s = versions_->LogAndApply(cfd, read_options, write_options, &edit,
                                 &mutex_, directories_.GetDbDir());
      write_thread_.ExitUnbatched(&w);
      if (s.ok() && options_file_number != 0) {
        versions_->options_file_size_ = options_file_size;
      }
      if (s.ok() && cfd->blob_partition_manager() != nullptr) {
        UnregisterBlobDirectWriteColumnFamily();
      }
      if (s.ok()) {
        RestoreParkedCompaction(cfd);
      }
    }
    if (s.ok()) {
      auto& moptions = cfd->GetLatestMutableCFOptions();
      max_total_in_memory_state_ -=
          moptions.write_buffer_size * moptions.max_write_buffer_number;
      preserve_info.Combine(moptions);
    }

    if (!cf_support_snapshot) {
      // Dropped Column Family doesn't support snapshot. Need to recalculate
      // is_snapshot_supported_.
      bool new_is_snapshot_supported = true;
      for (auto c : *versions_->GetColumnFamilySet()) {
        if (!c->IsDropped() && !c->mem()->IsSnapshotSupported()) {
          new_is_snapshot_supported = false;
          break;
        }
      }
      is_snapshot_supported_ = new_is_snapshot_supported;
    }
    bg_cv_.SignalAll();
  }

  if (preserve_info.IsEnabled()) {
    s = RegisterRecordSeqnoTimeWorker();
  }

  if (s.ok()) {
    // Note that here we erase the associated cf_info of the to-be-dropped
    // cfd before its ref-count goes to zero to avoid having to erase cf_info
    // later inside db_mutex.
    EraseThreadStatusCfInfo(cfd);
    assert(cfd->IsDropped());
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Dropped column family with id %u\n", cfd->GetID());
  } else {
    ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                    "Dropping column family with id %u FAILED -- %s\n",
                    cfd->GetID(), s.ToString().c_str());
  }

  return s;
}

bool DBImpl::KeyMayExist(const ReadOptions& read_options,
                         ColumnFamilyHandle* column_family, const Slice& key,
                         std::string* value, std::string* timestamp,
                         bool* value_found) {
  assert(read_options.io_activity == Env::IOActivity::kUnknown);

  if (value_found != nullptr) {
    // falsify later if key-may-exist but can't fetch value
    *value_found = true;
  }
  // TODO: plumb Env::IOActivity, Env::IOPriority
  ReadOptions roptions = read_options;
  roptions.read_tier = kBlockCacheTier;  // read from block cache only
  PinnableSlice pinnable_val;
  GetImplOptions get_impl_options;
  get_impl_options.column_family = column_family;
  get_impl_options.value = &pinnable_val;
  get_impl_options.value_found = value_found;
  get_impl_options.timestamp = timestamp;
  auto s = GetImpl(roptions, key, get_impl_options);
  if (value_found && *value_found && value) {
    value->assign(pinnable_val.data(), pinnable_val.size());
  }

  // If block_cache is enabled and the index block of the table didn't
  // not present in block_cache, the return value will be Status::Incomplete.
  // In this case, key may still exist in the table.
  return s.ok() || s.IsIncomplete();
}

std::unique_ptr<MultiScan> DBImpl::NewMultiScan(
    const ReadOptions& _read_options, ColumnFamilyHandle* column_family,
    const MultiScanArgs& scan_opts) {
  std::unique_ptr<MultiScan> ms_iter = std::make_unique<MultiScan>(
      _read_options, scan_opts, this, column_family);
  return ms_iter;
}

Iterator* DBImpl::NewIterator(const ReadOptions& _read_options,
                              ColumnFamilyHandle* column_family) {
  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kDBIterator) {
    return NewErrorIterator(Status::InvalidArgument(
        "Can only call NewIterator with `ReadOptions::io_activity` is "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kDBIterator`"));
  }
  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kDBIterator;
  }

  Iterator* result = nullptr;
  if (read_options.read_tier == kPersistedTier) {
    return NewErrorIterator(Status::NotSupported(
        "ReadTier::kPersistedData is not yet supported in iterators."));
  }
  assert(column_family);

  if (read_options.timestamp) {
    const Status s =
        FailIfTsMismatchCf(column_family, *(read_options.timestamp));
    if (!s.ok()) {
      return NewErrorIterator(s);
    }
  } else {
    const Status s = FailIfCfHasTs(column_family);
    if (!s.ok()) {
      return NewErrorIterator(s);
    }
  }

  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  assert(cfh != nullptr);
  ColumnFamilyData* cfd = cfh->cfd();
  assert(cfd != nullptr);
  SuperVersion* sv = cfd->GetReferencedSuperVersion(this);
  {
    const Status s = FailIfTableFilterWithRangeConversion(
        read_options, sv->mutable_cf_options);
    if (!s.ok()) {
      CleanupSuperVersion(sv);
      return NewErrorIterator(s);
    }
  }
  if (read_options.timestamp && read_options.timestamp->size() > 0) {
    const Status s =
        FailIfReadCollapsedHistory(cfd, sv, *(read_options.timestamp));
    if (!s.ok()) {
      CleanupSuperVersion(sv);
      return NewErrorIterator(s);
    }
  }
  if (read_options.tailing) {
    read_options.total_order_seek |=
        immutable_db_options_.prefix_seek_opt_in_only;

    auto iter = new ForwardIterator(this, read_options, cfd, sv,
                                    /* allow_unprepared_value */ true);
    // TODO(cbi): Add support for `memtable_op_scan_flush_trigger` for tailing
    // iterator. This requires refreshing DBIter's pointer to active_mem when
    // tailing iterator refreshes to new memtable internally.
    result = DBIter::NewIter(env_, read_options, cfd->ioptions(),
                             sv->mutable_cf_options, cfd->user_comparator(),
                             iter, sv->current, kMaxSequenceNumber,
                             /*read_callback=*/nullptr, /*active_mem=*/nullptr,
                             cfh, /*expose_blob_index=*/false);
  } else {
    // Note: no need to consider the special case of
    // last_seq_same_as_publish_seq_==false since NewIterator is overridden in
    // WritePreparedTxnDB
    result = NewIteratorImpl(read_options, cfh, sv,
                             (read_options.snapshot != nullptr)
                                 ? read_options.snapshot->GetSequenceNumber()
                                 : kMaxSequenceNumber,
                             nullptr /* read_callback */);
  }
  return result;
}

ArenaWrappedDBIter* DBImpl::NewIteratorImpl(
    const ReadOptions& read_options, ColumnFamilyHandleImpl* cfh,
    SuperVersion* sv, SequenceNumber snapshot, ReadCallback* read_callback,
    bool expose_blob_index, bool allow_refresh) {
  TEST_SYNC_POINT("DBImpl::NewIterator:1");
  TEST_SYNC_POINT("DBImpl::NewIterator:2");

  if (snapshot == kMaxSequenceNumber) {
    // Note that the snapshot is assigned AFTER referencing the super
    // version because otherwise a flush happening in between may compact away
    // data for the snapshot, so the reader would see neither data that was be
    // visible to the snapshot before compaction nor the newer data inserted
    // afterwards.
    // Note that the super version might not contain all the data available
    // to this snapshot, but in that case it can see all the data in the
    // super version, which is a valid consistent state after the user
    // calls NewIterator().
    snapshot = versions_->LastSequence();
    TEST_SYNC_POINT("DBImpl::NewIterator:3");
    TEST_SYNC_POINT("DBImpl::NewIterator:4");
  }

  // Try to generate a DB iterator tree in continuous memory area to be
  // cache friendly. Here is an example of result:
  // +-------------------------------+
  // |                               |
  // | ArenaWrappedDBIter            |
  // |  +                            |
  // |  +---> Inner Iterator   ------------+
  // |  |                            |     |
  // |  |    +-- -- -- -- -- -- -- --+     |
  // |  +--- | Arena                 |     |
  // |       |                       |     |
  // |          Allocated Memory:    |     |
  // |       |   +-------------------+     |
  // |       |   | DBIter            | <---+
  // |           |  +                |
  // |       |   |  +-> iter_  ------------+
  // |       |   |                   |     |
  // |       |   +-------------------+     |
  // |       |   | MergingIterator   | <---+
  // |           |  +                |
  // |       |   |  +->child iter1  ------------+
  // |       |   |  |                |          |
  // |           |  +->child iter2  ----------+ |
  // |       |   |  |                |        | |
  // |       |   |  +->child iter3  --------+ | |
  // |           |                   |      | | |
  // |       |   +-------------------+      | | |
  // |       |   | Iterator1         | <--------+
  // |       |   +-------------------+      | |
  // |       |   | Iterator2         | <------+
  // |       |   +-------------------+      |
  // |       |   | Iterator3         | <----+
  // |       |   +-------------------+
  // |       |                       |
  // +-------+-----------------------+
  //
  // ArenaWrappedDBIter inlines an arena area where all the iterators in
  // the iterator tree are allocated in the order of being accessed when
  // querying.
  // Laying out the iterators in the order of being accessed makes it more
  // likely that any iterator pointer is close to the iterator it points to so
  // that they are likely to be in the same cache line and/or page.
  return NewArenaWrappedDbIterator(env_, read_options, cfh, sv, snapshot,
                                   read_callback, this, expose_blob_index,
                                   allow_refresh,
                                   /*allow_mark_memtable_for_flush=*/true);
}

std::unique_ptr<Iterator> DBImpl::NewCoalescingIterator(
    const ReadOptions& _read_options,
    const std::vector<ColumnFamilyHandle*>& column_families) {
  if (column_families.size() == 1) {
    // Preserve direct iterator semantics, including lazy value preparation,
    // when callers route a single CF through the multi-CF API.
    return std::unique_ptr<Iterator>(
        NewIterator(_read_options, column_families.front()));
  }
  return NewMultiCfIterator<Iterator, CoalescingIterator>(
      _read_options, column_families, [](const Status& s) {
        return std::unique_ptr<Iterator>(NewErrorIterator(s));
      });
}

std::unique_ptr<AttributeGroupIterator> DBImpl::NewAttributeGroupIterator(
    const ReadOptions& _read_options,
    const std::vector<ColumnFamilyHandle*>& column_families) {
  return NewMultiCfIterator<AttributeGroupIterator, AttributeGroupIteratorImpl>(
      _read_options, column_families,
      [](const Status& s) { return NewAttributeGroupErrorIterator(s); });
}

template <typename IterType, typename ImplType, typename ErrorIteratorFuncType>
std::unique_ptr<IterType> DBImpl::NewMultiCfIterator(
    const ReadOptions& _read_options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    ErrorIteratorFuncType error_iterator_func) {
  if (column_families.size() == 0) {
    return error_iterator_func(
        Status::InvalidArgument("No Column Family was provided"));
  }
  const Comparator* first_comparator = column_families[0]->GetComparator();
  for (size_t i = 1; i < column_families.size(); ++i) {
    const Comparator* cf_comparator = column_families[i]->GetComparator();
    if (first_comparator != cf_comparator &&
        first_comparator->GetId().compare(cf_comparator->GetId()) != 0) {
      return error_iterator_func(Status::InvalidArgument(
          "Different comparators are being used across CFs"));
    }
  }

  std::vector<Iterator*> child_iterators;
  Status s = NewIterators(_read_options, column_families, &child_iterators);
  if (!s.ok()) {
    return error_iterator_func(s);
  }

  assert(column_families.size() == child_iterators.size());

  std::vector<std::pair<ColumnFamilyHandle*, std::unique_ptr<Iterator>>>
      cfh_iter_pairs;
  cfh_iter_pairs.reserve(column_families.size());
  for (size_t i = 0; i < column_families.size(); ++i) {
    cfh_iter_pairs.emplace_back(column_families[i], child_iterators[i]);
  }

  return std::make_unique<ImplType>(_read_options,
                                    column_families[0]->GetComparator(),
                                    std::move(cfh_iter_pairs));
}

Status DBImpl::NewIterators(
    const ReadOptions& _read_options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
  if (column_families.empty()) {
    if (_read_options.io_activity != Env::IOActivity::kUnknown &&
        _read_options.io_activity != Env::IOActivity::kDBIterator) {
      return Status::InvalidArgument(
          "Can only call NewIterators with `ReadOptions::io_activity` is "
          "`Env::IOActivity::kUnknown` or `Env::IOActivity::kDBIterator`");
    }
    if (_read_options.read_tier == kPersistedTier) {
      return Status::NotSupported(
          "ReadTier::kPersistedData is not yet supported in iterators.");
    }
  }
  return NewIterators(
      std::vector<ReadOptions>(column_families.size(), _read_options),
      column_families, iterators);
}

Status DBImpl::NewIterators(
    const std::vector<ReadOptions>& read_options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
  if (read_options.size() != column_families.size()) {
    return Status::InvalidArgument(
        "read_options and column_families must have the same size");
  }
  if (iterators == nullptr) {
    return Status::InvalidArgument("iterators not allowed to be nullptr");
  }

  if (column_families.empty()) {
    iterators->clear();
    return Status::OK();
  }

  const Snapshot* const snapshot = read_options.front().snapshot;
  const bool tailing = read_options.front().tailing;
  std::vector<ReadOptions> normalized_read_options;
  normalized_read_options.reserve(read_options.size());
  autovector<ColumnFamilySuperVersionPair, MultiGetContext::MAX_BATCH_SIZE>
      cf_sv_pairs;

  for (size_t i = 0; i < read_options.size(); ++i) {
    const ReadOptions& options = read_options[i];
    if (options.io_activity != Env::IOActivity::kUnknown &&
        options.io_activity != Env::IOActivity::kDBIterator) {
      return Status::InvalidArgument(
          "Can only call NewIterators with `ReadOptions::io_activity` is "
          "`Env::IOActivity::kUnknown` or `Env::IOActivity::kDBIterator`");
    }
    if (options.read_tier == kPersistedTier) {
      return Status::NotSupported(
          "ReadTier::kPersistedData is not yet supported in iterators.");
    }
    if (options.snapshot != snapshot) {
      return Status::InvalidArgument(
          "All ReadOptions must use the same snapshot");
    }
    if (options.tailing != tailing) {
      return Status::InvalidArgument(
          "All ReadOptions must use the same tailing setting");
    }

    auto* cf = column_families[i];
    assert(cf);
    Status s;
    if (options.timestamp) {
      s = FailIfTsMismatchCf(cf, *(options.timestamp));
    } else {
      s = FailIfCfHasTs(cf);
    }
    if (!s.ok()) {
      return s;
    }

    normalized_read_options.emplace_back(options);
    if (normalized_read_options.back().io_activity ==
        Env::IOActivity::kUnknown) {
      normalized_read_options.back().io_activity = Env::IOActivity::kDBIterator;
    }
    cf_sv_pairs.emplace_back(cf, nullptr);
  }

  iterators->clear();
  iterators->reserve(column_families.size());

  ReadOptions snapshot_options(normalized_read_options.front());
  snapshot_options.timestamp = nullptr;
  SequenceNumber consistent_seqnum = kMaxSequenceNumber;
  bool sv_from_thread_local = false;
  Status s = MultiCFSnapshot<autovector<ColumnFamilySuperVersionPair,
                                        MultiGetContext::MAX_BATCH_SIZE>>(
      snapshot_options, nullptr /* read_callback */,
      [](autovector<ColumnFamilySuperVersionPair,
                    MultiGetContext::MAX_BATCH_SIZE>::iterator& cf_iter) {
        return &(*cf_iter);
      },
      &cf_sv_pairs, /* extra_sv_ref */ true, &consistent_seqnum,
      &sv_from_thread_local);
  if (!s.ok()) {
    return s;
  }

  const auto cleanup_super_versions = [&]() {
    for (const auto& cf_sv_pair : cf_sv_pairs) {
      CleanupSuperVersion(cf_sv_pair.super_version);
    }
  };
  assert(cf_sv_pairs.size() == normalized_read_options.size());
  for (size_t i = 0; i < cf_sv_pairs.size(); ++i) {
    const auto& options = normalized_read_options[i];
    const auto& cf_sv_pair = cf_sv_pairs[i];
    if (options.timestamp && !options.timestamp->empty()) {
      s = FailIfReadCollapsedHistory(cf_sv_pair.cfd, cf_sv_pair.super_version,
                                     *(options.timestamp));
      if (!s.ok()) {
        cleanup_super_versions();
        return s;
      }
    }
    s = FailIfTableFilterWithRangeConversion(
        options, cf_sv_pair.super_version->mutable_cf_options);
    if (!s.ok()) {
      cleanup_super_versions();
      return s;
    }
  }

  if (tailing) {
    for (size_t i = 0; i < cf_sv_pairs.size(); ++i) {
      ReadOptions options(normalized_read_options[i]);
      options.total_order_seek |= immutable_db_options_.prefix_seek_opt_in_only;
      const auto& cf_sv_pair = cf_sv_pairs[i];
      auto* iter = new ForwardIterator(this, options, cf_sv_pair.cfd,
                                       cf_sv_pair.super_version,
                                       /* allow_unprepared_value */ true);
      iterators->push_back(DBIter::NewIter(
          env_, options, cf_sv_pair.cfd->ioptions(),
          cf_sv_pair.super_version->mutable_cf_options,
          cf_sv_pair.cfd->user_comparator(), iter,
          cf_sv_pair.super_version->current, kMaxSequenceNumber,
          nullptr /* read_callback */, /* active_mem */ nullptr, cf_sv_pair.cfh,
          /* expose_blob_index */ false, /* arena */ nullptr));
    }
  } else {
    for (size_t i = 0; i < cf_sv_pairs.size(); ++i) {
      const auto& cf_sv_pair = cf_sv_pairs[i];
      iterators->push_back(NewIteratorImpl(
          normalized_read_options[i], cf_sv_pair.cfh, cf_sv_pair.super_version,
          consistent_seqnum, nullptr /* read_callback */));
    }
  }
  return Status::OK();
}

const Snapshot* DBImpl::GetSnapshot() {
  return GetSnapshotImpl(false, SnapshotLockMode::kTryWithoutDBMutex);
}

const Snapshot* DBImpl::GetSnapshotForWriteConflictBoundary() {
  return GetSnapshotImpl(true, SnapshotLockMode::kTryWithoutDBMutex);
}

std::pair<Status, std::shared_ptr<const Snapshot>>
DBImpl::CreateTimestampedSnapshot(SequenceNumber snapshot_seq, uint64_t ts) {
  assert(ts != std::numeric_limits<uint64_t>::max());

  auto ret = CreateTimestampedSnapshotImpl(snapshot_seq, ts, /*lock=*/true);
  return ret;
}

std::shared_ptr<const SnapshotImpl> DBImpl::GetTimestampedSnapshot(
    uint64_t ts) const {
  InstrumentedMutexLock lock_guard(&mutex_);
  return timestamped_snapshots_.GetSnapshot(ts);
}

void DBImpl::ReleaseTimestampedSnapshotsOlderThan(uint64_t ts,
                                                  size_t* remaining_total_ss) {
  autovector<std::shared_ptr<const SnapshotImpl>> snapshots_to_release;
  {
    InstrumentedMutexLock lock_guard(&mutex_);
    timestamped_snapshots_.ReleaseSnapshotsOlderThan(ts, snapshots_to_release);
  }
  snapshots_to_release.clear();

  if (remaining_total_ss) {
    *remaining_total_ss = static_cast<size_t>(snapshots_.count());
  }
}

Status DBImpl::GetTimestampedSnapshots(
    uint64_t ts_lb, uint64_t ts_ub,
    std::vector<std::shared_ptr<const Snapshot>>& timestamped_snapshots) const {
  if (ts_lb >= ts_ub) {
    return Status::InvalidArgument(
        "timestamp lower bound must be smaller than upper bound");
  }
  timestamped_snapshots.clear();
  InstrumentedMutexLock lock_guard(&mutex_);
  timestamped_snapshots_.GetSnapshots(ts_lb, ts_ub, timestamped_snapshots);
  return Status::OK();
}

SnapshotImpl* DBImpl::GetSnapshotImpl(bool is_write_conflict_boundary,
                                      SnapshotLockMode lock_mode) {
  int64_t unix_time = 0;
  immutable_db_options_.clock->GetCurrentTime(&unix_time)
      .PermitUncheckedError();  // Ignore error
  if (lock_mode == SnapshotLockMode::kDBMutexHeld) {
    mutex_.AssertHeld();
  }
  SnapshotImpl* s = new SnapshotImpl;
  auto try_new = [this, s, unix_time,
                  is_write_conflict_boundary](bool db_mutex_held) {
    return snapshots_.TryNew(
        s, [this]() { return GetLastPublishedSequence(); },
        [this]() {
          return is_snapshot_supported_.load(std::memory_order_relaxed);
        },
        [](SequenceNumber /*seq*/) { return true; },
        [](SequenceNumber /*seq*/) {}, unix_time, is_write_conflict_boundary,
        std::numeric_limits<uint64_t>::max(), db_mutex_held);
  };

  ShardedSnapshotList::NewResult result;
  if (lock_mode == SnapshotLockMode::kAcquireDBMutex) {
    mutex_.Lock();
    result = try_new(/*db_mutex_held=*/true);
    mutex_.Unlock();
  } else {
    result = try_new(/*db_mutex_held=*/
                     lock_mode == SnapshotLockMode::kDBMutexHeld);
  }
  if (result == ShardedSnapshotList::NewResult::kRetryWithDBMutex) {
    assert(lock_mode == SnapshotLockMode::kTryWithoutDBMutex);
    mutex_.Lock();
    result = try_new(/*db_mutex_held=*/true);
    mutex_.Unlock();
  }
  if (result != ShardedSnapshotList::NewResult::kSuccess) {
    delete s;
    return nullptr;
  }
  return s;
}

std::pair<Status, std::shared_ptr<const SnapshotImpl>>
DBImpl::CreateTimestampedSnapshotImpl(SequenceNumber snapshot_seq, uint64_t ts,
                                      bool lock) {
  int64_t unix_time = 0;
  immutable_db_options_.clock->GetCurrentTime(&unix_time)
      .PermitUncheckedError();  // Ignore error
  TEST_SYNC_POINT("DBImpl::CreateTimestampedSnapshot:AfterClock");
  SnapshotImpl* s = new SnapshotImpl;

  const bool need_update_seq = (snapshot_seq != kMaxSequenceNumber);

  if (lock) {
    mutex_.Lock();
  } else {
    mutex_.AssertHeld();
  }
  Status status;
  std::shared_ptr<const SnapshotImpl> ret;
  // Select and lock the shard before reading the sequence number. Regular
  // snapshots therefore cannot insert a newer sequence into this shard before
  // the timestamped snapshot, so insertion remains constant time while the DB
  // mutex is held.
  auto result = snapshots_.TryNew(
      s,
      [this, snapshot_seq, need_update_seq]() {
        // A caller outside the write thread does not provide a valid sequence.
        return need_update_seq ? snapshot_seq : GetLastPublishedSequence();
      },
      [this]() {
        return is_snapshot_supported_.load(std::memory_order_relaxed);
      },
      [this, &snapshot_seq, &status, &ret, ts](SequenceNumber selected_seq) {
        snapshot_seq = selected_seq;
        std::shared_ptr<const SnapshotImpl> latest =
            timestamped_snapshots_.GetSnapshot(
                std::numeric_limits<uint64_t>::max());
        if (!latest) {
          return true;
        }

        const uint64_t latest_snap_ts = latest->GetTimestamp();
        const SequenceNumber latest_snap_seq = latest->GetSequenceNumber();
        assert(latest_snap_seq <= snapshot_seq);
        if (latest_snap_ts > ts) {
          // A snapshot created later cannot have smaller timestamp than a
          // previous timestamped snapshot.
          std::ostringstream oss;
          oss << "snapshot exists with larger timestamp " << latest_snap_ts
              << " > " << ts;
          status = Status::InvalidArgument(oss.str());
          return false;
        }
        if (latest_snap_ts == ts) {
          if (latest_snap_seq == snapshot_seq) {
            // Reuse the snapshot for the same sequence and timestamp.
            ret = latest;
            return false;
          }
          // Writes occurred since the latest timestamped snapshot, but the
          // caller requested the same timestamp.
          assert(latest_snap_seq < snapshot_seq);
          std::ostringstream oss;
          oss << "Allocated seq is " << snapshot_seq
              << ", while snapshot exists with smaller seq " << latest_snap_seq
              << " but same timestamp " << ts;
          status = Status::InvalidArgument(oss.str());
          return false;
        }
        return true;
      },
      [this, need_update_seq](SequenceNumber selected_seq) {
        if (!need_update_seq) {
          return;
        }
        // Publish only after the snapshot is linked, and before releasing its
        // shard. A regular snapshot in this shard cannot append the old
        // published sequence behind `selected_seq`.
        TEST_SYNC_POINT(
            "DBImpl::CreateTimestampedSnapshot:BeforeSequencePublish");
        if (last_seq_same_as_publish_seq_) {
          versions_->SetLastSequence(selected_seq);
        } else {
          // TODO: support write-prepared/write-unprepared transactions with
          // two write queues.
          assert(false);
        }
      },
      unix_time, /*is_write_conflict_boundary=*/true, ts,
      /*db_mutex_held=*/true);

  if (result != ShardedSnapshotList::NewResult::kSuccess) {
    delete s;
    if (result == ShardedSnapshotList::NewResult::kNotSupported) {
      status = Status::NotSupported("Memtable does not support snapshot");
    } else {
      assert(result == ShardedSnapshotList::NewResult::kNotCreated);
    }
    if (lock) {
      mutex_.Unlock();
    }
    return std::make_pair(status, ret);
  }

  ret = std::shared_ptr<const SnapshotImpl>(
      s, std::bind(&DBImpl::ReleaseSnapshot, this, std::placeholders::_1));
  timestamped_snapshots_.AddSnapshot(ret);

  if (lock) {
    mutex_.Unlock();
  }
  return std::make_pair(status, ret);
}

bool DBImpl::ReconcileOldestSnapshotForColumnFamily(
    ColumnFamilyData* cfd, SequenceNumber oldest_snapshot) {
  mutex_.AssertHeld();
  if (cfd->IsDropped()) {
    return false;
  }

  VersionStorageInfo* vstorage = cfd->current()->storage_info();
  if (oldest_snapshot <= vstorage->oldest_snapshot_seqnum()) {
    return false;
  }

  const SequenceNumber standalone_range_deletion_threshold =
      vstorage->standalone_range_tombstone_files_mark_threshold();
  vstorage->UpdateOldestSnapshot(oldest_snapshot, cfd->AllowIngestBehind(),
                                 cfd->ioptions().user_comparator,
                                 cfd->GetFullHistoryTsLow());
  const bool bottommost_file_ready =
      !vstorage->BottommostFilesMarkedForCompaction().empty();
  const bool standalone_range_deletion_ready =
      oldest_snapshot >= standalone_range_deletion_threshold;
  if (!bottommost_file_ready && !standalone_range_deletion_ready) {
    return false;
  }

  EnqueuePendingCompaction(cfd);
  return true;
}

void DBImpl::UpdateSnapshotCompactionThresholds() {
  mutex_.AssertHeld();
  bottommost_files_mark_threshold_ = kMaxSequenceNumber;
  standalone_range_deletion_files_mark_threshold_ = kMaxSequenceNumber;
  for (ColumnFamilyData* cfd : *versions_->GetColumnFamilySet()) {
    if (cfd->IsDropped()) {
      continue;
    }
    VersionStorageInfo* vstorage = cfd->current()->storage_info();
    if (!cfd->AllowIngestBehind()) {
      bottommost_files_mark_threshold_ =
          std::min(bottommost_files_mark_threshold_,
                   vstorage->bottommost_files_mark_threshold());
    }
    standalone_range_deletion_files_mark_threshold_ =
        std::min(standalone_range_deletion_files_mark_threshold_,
                 vstorage->standalone_range_tombstone_files_mark_threshold());
  }
}

void DBImpl::UpdateOldestSnapshotAndScheduleWork(
    SequenceNumber oldest_snapshot, uint64_t snapshot_release_count,
    ColumnFamilyData* installed_cfd) {
  mutex_.AssertHeld();
  if (read_only_) {
    return;
  }

  bool compaction_enqueued = false;
  bool thresholds_need_refresh = false;
  const bool snapshot_released =
      snapshot_release_count != last_processed_snapshot_release_count_;
  last_processed_snapshot_release_count_ = snapshot_release_count;
  if (snapshot_released && oldest_snapshot > last_processed_oldest_snapshot_) {
    last_processed_oldest_snapshot_ = oldest_snapshot;
    if (oldest_snapshot > bottommost_files_mark_threshold_ ||
        oldest_snapshot >= standalone_range_deletion_files_mark_threshold_) {
      for (ColumnFamilyData* cfd : *versions_->GetColumnFamilySet()) {
        compaction_enqueued =
            ReconcileOldestSnapshotForColumnFamily(cfd, oldest_snapshot) ||
            compaction_enqueued;
      }
      thresholds_need_refresh = true;
    }
  }

  if (installed_cfd != nullptr) {
    compaction_enqueued = ReconcileOldestSnapshotForColumnFamily(
                              installed_cfd, last_processed_oldest_snapshot_) ||
                          compaction_enqueued;
    thresholds_need_refresh = true;
  }
  if (thresholds_need_refresh) {
    UpdateSnapshotCompactionThresholds();
  }
  if (compaction_enqueued) {
    MaybeScheduleFlushOrCompaction();
  }
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  if (s == nullptr) {
    // DBImpl::GetSnapshot() can return nullptr when snapshot
    // not supported by specifying the condition:
    // inplace_update_support enabled.
    return;
  }
  const SnapshotImpl* casted_s = static_cast<const SnapshotImpl*>(s);
  snapshots_.Remove(casted_s);
  delete casted_s;
}

Status DBImpl::GetPropertiesOfAllTables(ColumnFamilyHandle* column_family,
                                        TablePropertiesCollection* props) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();

  // Increment the ref count
  mutex_.Lock();
  auto version = cfd->current();
  version->Ref();
  mutex_.Unlock();

  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;
  auto s = version->GetPropertiesOfAllTables(read_options, props);

  // Decrement the ref count
  mutex_.Lock();
  version->Unref();
  mutex_.Unlock();

  return s;
}

Status DBImpl::GetPropertiesOfTablesInRange(ColumnFamilyHandle* column_family,
                                            const Range* range, std::size_t n,
                                            TablePropertiesCollection* props) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();

  // Increment the ref count
  mutex_.Lock();
  auto version = cfd->current();
  version->Ref();
  mutex_.Unlock();

  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;
  const Comparator* const ucmp = cfd->user_comparator();
  assert(ucmp);
  size_t ts_sz = ucmp->timestamp_size();

  autovector<UserKeyRange> ukey_ranges;
  std::vector<std::string> keys;
  ukey_ranges.reserve(n);
  keys.reserve(2 * n);
  // Add timestamp if needed
  for (size_t i = 0; i < n; i++) {
    auto [start, limit] = MaybeAddTimestampsToRange(
        range[i].start, range[i].limit, ts_sz, &keys.emplace_back(),
        &keys.emplace_back(), /*exclusive_end=*/false);
    assert(start.has_value());
    assert(limit.has_value());
    ukey_ranges.emplace_back(start.value(), limit.value());
  }
  auto s =
      version->GetPropertiesOfTablesInRange(read_options, ukey_ranges, props);

  // Decrement the ref count
  mutex_.Lock();
  version->Unref();
  mutex_.Unlock();

  return s;
}

Status DBImpl::GetPropertiesOfTablesByLevel(
    ColumnFamilyHandle* column_family,
    std::vector<std::unique_ptr<TablePropertiesCollection>>* props_by_level) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();

  // Increment the ref count
  mutex_.Lock();
  auto version = cfd->current();
  version->Ref();
  mutex_.Unlock();

  const ReadOptions read_options;
  auto s = version->GetPropertiesOfTablesByLevel(read_options, props_by_level);

  // Decrement the ref count
  mutex_.Lock();
  version->Unref();
  mutex_.Unlock();

  return s;
}

const std::string& DBImpl::GetName() const { return dbname_; }

Env* DBImpl::GetEnv() const { return env_; }

FileSystem* DB::GetFileSystem() const {
  const auto& fs = GetEnv()->GetFileSystem();
  return fs.get();
}

void DB::MultiGet(const ReadOptions& options, ColumnFamilyHandle* column_family,
                  const size_t num_keys, const Slice* keys,
                  PinnableSlice* values, std::string* timestamps,
                  Status* statuses, const bool sorted_input) {
  // Use std::array, if possible, to avoid memory allocation overhead
  if (num_keys > MultiGetContext::MAX_BATCH_SIZE) {
    std::vector<ColumnFamilyHandle*> column_families(num_keys, column_family);
    MultiGet(options, num_keys, column_families.data(), keys, values,
             timestamps, statuses, sorted_input);
  } else {
    std::array<ColumnFamilyHandle*, MultiGetContext::MAX_BATCH_SIZE>
        column_families;
    std::fill(column_families.begin(), column_families.begin() + num_keys,
              column_family);
    MultiGet(options, num_keys, column_families.data(), keys, values,
             timestamps, statuses, sorted_input);
  }
}

FileSystem* DBImpl::GetFileSystem() const {
  return immutable_db_options_.fs.get();
}

SystemClock* DBImpl::GetSystemClock() const {
  return immutable_db_options_.clock;
}

Status DBImpl::StartIOTrace(const TraceOptions& trace_options,
                            std::unique_ptr<TraceWriter>&& trace_writer) {
  assert(trace_writer != nullptr);
  return io_tracer_->StartIOTrace(GetSystemClock(), trace_options,
                                  std::move(trace_writer));
}

Status DBImpl::EndIOTrace() {
  io_tracer_->EndIOTrace();
  return Status::OK();
}

Options DBImpl::GetOptions(ColumnFamilyHandle* column_family) const {
  InstrumentedMutexLock l(&mutex_);
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  return Options(BuildDBOptions(immutable_db_options_, mutable_db_options_),
                 cfh->cfd()->GetLatestCFOptions());
}

DBOptions DBImpl::GetDBOptions() const {
  InstrumentedMutexLock l(&mutex_);
  return BuildDBOptions(immutable_db_options_, mutable_db_options_);
}

bool DBImpl::GetProperty(ColumnFamilyHandle* column_family,
                         const Slice& property, std::string* value) {
  const DBPropertyInfo* property_info = GetPropertyInfo(property);
  value->clear();
  auto cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(column_family)->cfd();
  if (property_info == nullptr) {
    return false;
  } else if (property_info->handle_int) {
    uint64_t int_value;
    bool ret_value =
        GetIntPropertyInternal(cfd, *property_info, false, &int_value);
    if (ret_value) {
      *value = std::to_string(int_value);
    }
    return ret_value;
  } else if (property_info->handle_string) {
    if (property_info->need_out_of_mutex) {
      return cfd->internal_stats()->GetStringProperty(*property_info, property,
                                                      value);
    } else {
      InstrumentedMutexLock l(&mutex_);
      return cfd->internal_stats()->GetStringProperty(*property_info, property,
                                                      value);
    }
  } else if (property_info->handle_string_dbimpl) {
    if (property_info->need_out_of_mutex) {
      return (this->*(property_info->handle_string_dbimpl))(value);
    } else {
      InstrumentedMutexLock l(&mutex_);
      return (this->*(property_info->handle_string_dbimpl))(value);
    }
  }
  // Shouldn't reach here since exactly one of handle_string and handle_int
  // should be non-nullptr.
  assert(false);
  return false;
}

bool DBImpl::GetMapProperty(ColumnFamilyHandle* column_family,
                            const Slice& property,
                            std::map<std::string, std::string>* value) {
  const DBPropertyInfo* property_info = GetPropertyInfo(property);
  value->clear();
  auto cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(column_family)->cfd();
  if (property_info == nullptr) {
    return false;
  } else if (property_info->handle_map) {
    if (property_info->need_out_of_mutex) {
      return cfd->internal_stats()->GetMapProperty(*property_info, property,
                                                   value);
    } else {
      InstrumentedMutexLock l(&mutex_);
      return cfd->internal_stats()->GetMapProperty(*property_info, property,
                                                   value);
    }
  }
  // If we reach this point it means that handle_map is not provided for the
  // requested property
  return false;
}

bool DBImpl::GetIntProperty(ColumnFamilyHandle* column_family,
                            const Slice& property, uint64_t* value) {
  const DBPropertyInfo* property_info = GetPropertyInfo(property);
  if (property_info == nullptr || property_info->handle_int == nullptr) {
    return false;
  }
  auto cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(column_family)->cfd();
  return GetIntPropertyInternal(cfd, *property_info, false, value);
}

bool DBImpl::GetIntPropertyInternal(ColumnFamilyData* cfd,
                                    const DBPropertyInfo& property_info,
                                    bool is_locked, uint64_t* value) {
  assert(property_info.handle_int != nullptr);
  if (!property_info.need_out_of_mutex) {
    if (is_locked) {
      mutex_.AssertHeld();
      return cfd->internal_stats()->GetIntProperty(property_info, value, this);
    } else {
      InstrumentedMutexLock l(&mutex_);
      return cfd->internal_stats()->GetIntProperty(property_info, value, this);
    }
  } else {
    SuperVersion* sv = nullptr;
    if (is_locked) {
      mutex_.Unlock();
    }
    sv = GetAndRefSuperVersion(cfd);

    bool ret = cfd->internal_stats()->GetIntPropertyOutOfMutex(
        property_info, sv->current, value);

    ReturnAndCleanupSuperVersion(cfd, sv);
    if (is_locked) {
      mutex_.Lock();
    }

    return ret;
  }
}

bool DBImpl::GetPropertyHandleOptionsStatistics(std::string* value) {
  assert(value != nullptr);
  Statistics* statistics = immutable_db_options_.stats;
  if (!statistics) {
    return false;
  }
  *value = statistics->ToString();
  return true;
}

Status DBImpl::ResetStats() {
  InstrumentedMutexLock l(&mutex_);
  for (auto* cfd : *versions_->GetColumnFamilySet()) {
    if (cfd->initialized()) {
      cfd->internal_stats()->Clear();
    }
  }
  return Status::OK();
}

bool DBImpl::GetAggregatedIntProperty(const Slice& property,
                                      uint64_t* aggregated_value) {
  const DBPropertyInfo* property_info = GetPropertyInfo(property);
  if (property_info == nullptr || property_info->handle_int == nullptr) {
    return false;
  }
  auto aggregator = CreateIntPropertyAggregator(property);
  if (aggregator == nullptr) {
    return false;
  }

  bool ret = true;
  {
    // Needs mutex to protect the list of column families.
    InstrumentedMutexLock l(&mutex_);
    uint64_t value;
    for (auto* cfd : versions_->GetRefedColumnFamilySet()) {
      if (!cfd->initialized()) {
        continue;
      }
      ret = GetIntPropertyInternal(cfd, *property_info, true, &value);
      // GetIntPropertyInternal may release db mutex and re-acquire it.
      mutex_.AssertHeld();
      if (ret) {
        aggregator->Add(cfd, value);
      } else {
        ret = false;
        break;
      }
    }
  }
  *aggregated_value = aggregator->Aggregate();
  return ret;
}

SuperVersion* DBImpl::GetAndRefSuperVersion(ColumnFamilyData* cfd) {
  // TODO(ljin): consider using GetReferencedSuperVersion() directly
  return cfd->GetThreadLocalSuperVersion(this);
}

// REQUIRED: this function should only be called on the write thread or if the
// mutex is held.
SuperVersion* DBImpl::GetAndRefSuperVersion(uint32_t column_family_id) {
  auto column_family_set = versions_->GetColumnFamilySet();
  auto cfd = column_family_set->GetColumnFamily(column_family_id);
  if (!cfd) {
    return nullptr;
  }

  return GetAndRefSuperVersion(cfd);
}

void DBImpl::CleanupSuperVersion(SuperVersion* sv) {
  // Release SuperVersion
  if (sv->Unref()) {
    bool defer_purge = immutable_db_options().avoid_unnecessary_blocking_io;
    {
      InstrumentedMutexLock l(&mutex_);
      sv->Cleanup();
      if (defer_purge) {
        AddSuperVersionsToFreeQueue(sv);
        SchedulePurge();
      }
    }
    if (!defer_purge) {
      delete sv;
    }
    RecordTick(stats_, NUMBER_SUPERVERSION_CLEANUPS);
  }
  RecordTick(stats_, NUMBER_SUPERVERSION_RELEASES);
}

void DBImpl::ReturnAndCleanupSuperVersion(ColumnFamilyData* cfd,
                                          SuperVersion* sv) {
  if (!cfd->ReturnThreadLocalSuperVersion(sv)) {
    CleanupSuperVersion(sv);
  }
}

// REQUIRED: this function should only be called on the write thread.
void DBImpl::ReturnAndCleanupSuperVersion(uint32_t column_family_id,
                                          SuperVersion* sv) {
  auto column_family_set = versions_->GetColumnFamilySet();
  auto cfd = column_family_set->GetColumnFamily(column_family_id);

  // If SuperVersion is held, and we successfully fetched a cfd using
  // GetAndRefSuperVersion(), it must still exist.
  assert(cfd != nullptr);
  ReturnAndCleanupSuperVersion(cfd, sv);
}

// REQUIRED: this function should only be called on the write thread or if the
// mutex is held.
ColumnFamilyHandle* DBImpl::GetColumnFamilyHandle(uint32_t column_family_id) {
  ColumnFamilyMemTables* cf_memtables = column_family_memtables_.get();

  if (!cf_memtables->Seek(column_family_id)) {
    return nullptr;
  }

  return cf_memtables->GetColumnFamilyHandle();
}

// REQUIRED: mutex is NOT held.
std::unique_ptr<ColumnFamilyHandle> DBImpl::GetColumnFamilyHandleUnlocked(
    uint32_t column_family_id) {
  InstrumentedMutexLock l(&mutex_);

  auto* cfd =
      versions_->GetColumnFamilySet()->GetColumnFamily(column_family_id);
  if (cfd == nullptr) {
    return nullptr;
  }

  return std::unique_ptr<ColumnFamilyHandleImpl>(
      new ColumnFamilyHandleImpl(cfd, this, &mutex_));
}

void DBImpl::GetApproximateMemTableStats(ColumnFamilyHandle* column_family,
                                         const Range& range,
                                         uint64_t* const count,
                                         uint64_t* const size) {
  ColumnFamilyHandleImpl* cfh =
      static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  ColumnFamilyData* cfd = cfh->cfd();
  SuperVersion* sv = GetAndRefSuperVersion(cfd);

  const Comparator* const ucmp = column_family->GetComparator();
  assert(ucmp);
  size_t ts_sz = ucmp->timestamp_size();

  // Add timestamp if needed
  std::string start_with_ts;
  std::string limit_with_ts;
  auto [start, limit] = MaybeAddTimestampsToRange(
      range.start, range.limit, ts_sz, &start_with_ts, &limit_with_ts);
  assert(start.has_value());
  assert(limit.has_value());
  // Convert user_key into a corresponding internal key.
  InternalKey k1(start.value(), kMaxSequenceNumber, kValueTypeForSeek);
  InternalKey k2(limit.value(), kMaxSequenceNumber, kValueTypeForSeek);
  ReadOnlyMemTable::MemTableStats memStats =
      sv->mem->ApproximateStats(k1.Encode(), k2.Encode());
  ReadOnlyMemTable::MemTableStats immStats =
      sv->imm->ApproximateStats(k1.Encode(), k2.Encode());
  *count = memStats.count + immStats.count;
  *size = memStats.size + immStats.size;

  ReturnAndCleanupSuperVersion(cfd, sv);
}

Status DBImpl::GetApproximateSizes(const SizeApproximationOptions& options,
                                   ColumnFamilyHandle* column_family,
                                   const Range* range, int n, uint64_t* sizes) {
  if (!options.include_memtables && !options.include_files &&
      !options.include_blob_files) {
    return Status::InvalidArgument("Invalid options");
  }

  const Comparator* const ucmp = column_family->GetComparator();
  assert(ucmp);
  size_t ts_sz = ucmp->timestamp_size();

  Version* v;
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();
  SuperVersion* sv = GetAndRefSuperVersion(cfd);
  v = sv->current;

  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;

  // Pre-compute blob-to-SST ratio once (invariant across ranges for the same
  // Version). This avoids iterating all levels and blob files per range.
  double blob_to_sst_ratio = 0.0;
  if (options.include_blob_files) {
    const auto* vstorage = v->storage_info();
    uint64_t total_sst_size = 0;
    for (int level = 0; level < vstorage->num_non_empty_levels(); ++level) {
      total_sst_size += vstorage->NumLevelBytes(level);
    }
    if (total_sst_size > 0) {
      uint64_t total_blob_size = 0;
      const auto& blob_files = vstorage->GetBlobFiles();
      for (const auto& blob_file_meta : blob_files) {
        assert(blob_file_meta);
        total_blob_size += blob_file_meta->GetBlobFileSize();
      }
      blob_to_sst_ratio = static_cast<double>(total_blob_size) /
                          static_cast<double>(total_sst_size);
    }
  }

  for (int i = 0; i < n; i++) {
    // Add timestamp if needed
    std::string start_with_ts;
    std::string limit_with_ts;
    auto [start, limit] = MaybeAddTimestampsToRange(
        range[i].start, range[i].limit, ts_sz, &start_with_ts, &limit_with_ts);
    assert(start.has_value());
    assert(limit.has_value());
    // Convert user_key into a corresponding internal key.
    InternalKey k1(start.value(), kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(limit.value(), kMaxSequenceNumber, kValueTypeForSeek);
    sizes[i] = 0;
    // Compute SST size in range (needed for both include_files and
    // include_blob_files, since blob size is prorated by SST ratio).
    uint64_t sst_size_in_range = 0;
    if (options.include_files || options.include_blob_files) {
      sst_size_in_range = versions_->ApproximateSize(
          options, read_options, v, k1.Encode(), k2.Encode(),
          /*start_level=*/0,
          /*end_level=*/-1, TableReaderCaller::kUserApproximateSize);
    }
    if (options.include_files) {
      sizes[i] += sst_size_in_range;
    }
    if (options.include_memtables) {
      sizes[i] += sv->mem->ApproximateStats(k1.Encode(), k2.Encode()).size;
      sizes[i] += sv->imm->ApproximateStats(k1.Encode(), k2.Encode()).size;
    }
    if (options.include_blob_files) {
      sizes[i] += static_cast<uint64_t>(static_cast<double>(sst_size_in_range) *
                                        blob_to_sst_ratio);
    }
  }

  ReturnAndCleanupSuperVersion(cfd, sv);
  return Status::OK();
}

std::list<uint64_t>::iterator
DBImpl::CaptureCurrentFileNumberInPendingOutputs() {
  // We need to remember the iterator of our insert, because after the
  // background job is done, we need to remove that element from
  // pending_outputs_.
  pending_outputs_.push_back(versions_->current_next_file_number());
  auto pending_outputs_inserted_elem = pending_outputs_.end();
  --pending_outputs_inserted_elem;
  return pending_outputs_inserted_elem;
}

void DBImpl::ReleaseFileNumberFromPendingOutputs(
    std::unique_ptr<std::list<uint64_t>::iterator>& v) {
  if (v.get() != nullptr) {
    pending_outputs_.erase(*v.get());
    v.reset();
  }
}

std::list<uint64_t>::iterator DBImpl::CaptureOptionsFileNumber() {
  // We need to remember the iterator of our insert, because after the
  // compaction is done, we need to remove that element from
  // min_options_file_numbers_.
  min_options_file_numbers_.push_back(versions_->options_file_number());
  auto min_options_file_numbers_inserted_elem = min_options_file_numbers_.end();
  --min_options_file_numbers_inserted_elem;
  return min_options_file_numbers_inserted_elem;
}

void DBImpl::ReleaseOptionsFileNumber(
    std::unique_ptr<std::list<uint64_t>::iterator>& v) {
  if (v.get() != nullptr) {
    min_options_file_numbers_.erase(*v.get());
    v.reset();
  }
}

Status DBImpl::GetUpdatesSince(SequenceNumber seq,
                               std::unique_ptr<WalIterator>* iter,
                               const WalIterator::ReadOptions& read_options) {
  RecordTick(stats_, GET_UPDATES_SINCE_CALLS);
  if (seq_per_batch_) {
    return Status::NotSupported(
        "This API is not yet compatible with write-prepared/write-unprepared "
        "transactions");
  }
  if (seq > versions_->LastSequence()) {
    return Status::NotFound("Requested sequence not yet written in the db");
  }
  return wal_manager_.GetUpdatesSince(seq, iter, read_options, versions_.get());
}

Status DBImpl::DeleteFilesInRanges(ColumnFamilyHandle* column_family,
                                   const RangeOpt* ranges, size_t n,
                                   bool include_end) {
  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;
  const WriteOptions write_options;

  Status status = Status::OK();
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  ColumnFamilyData* cfd = cfh->cfd();
  const Comparator* ucmp = cfd->user_comparator();
  assert(ucmp);
  const size_t ts_sz = ucmp->timestamp_size();
  autovector<UserKeyRangeOpt> ukey_ranges;
  std::vector<std::string> keys;
  std::vector<Slice> key_slices;
  ukey_ranges.reserve(n);
  keys.reserve(2 * n);
  key_slices.reserve(2 * n);
  for (size_t i = 0; i < n; i++) {
    auto [start, limit] = MaybeAddTimestampsToRange(
        ranges[i].start, ranges[i].limit, ts_sz, &keys.emplace_back(),
        &keys.emplace_back(), !include_end);
    assert(ranges[i].start.has_value() == start.has_value());
    assert(ranges[i].limit.has_value() == limit.has_value());
    ukey_ranges.emplace_back(start, limit);
  }

  VersionEdit edit;
  edit.MarkForegroundOperation();
  std::set<FileMetaData*> deleted_files;
  JobContext job_context(next_job_id_.fetch_add(1), true);
  {
    InstrumentedMutexLock l(&mutex_);
    Version* input_version = cfd->current();

    auto* vstorage = input_version->storage_info();
    for (const auto& range : ukey_ranges) {
      auto begin = range.start.has_value() ? &range.start.value() : nullptr;
      auto end = range.limit.has_value() ? &range.limit.value() : nullptr;
      for (int i = 1; i < cfd->NumberLevels(); i++) {
        if (vstorage->LevelFiles(i).empty() ||
            !vstorage->OverlapInLevel(i, begin, end)) {
          continue;
        }
        std::vector<FileMetaData*> level_files;
        InternalKey begin_storage;
        InternalKey end_storage;
        InternalKey* begin_key;
        InternalKey* end_key;
        if (begin == nullptr) {
          begin_key = nullptr;
        } else {
          begin_storage.SetMinPossibleForUserKey(*begin);
          begin_key = &begin_storage;
        }
        if (end == nullptr) {
          end_key = nullptr;
        } else {
          end_storage.SetMaxPossibleForUserKey(*end);
          end_key = &end_storage;
        }

        vstorage->GetCleanInputsWithinInterval(
            i, begin_key, end_key, &level_files, -1 /* hint_index */,
            nullptr /* file_index */);
        FileMetaData* level_file;
        for (uint32_t j = 0; j < level_files.size(); j++) {
          level_file = level_files[j];
          if (level_file->being_compacted) {
            continue;
          }
          if (deleted_files.find(level_file) != deleted_files.end()) {
            continue;
          }
          if (!include_end && end != nullptr &&
              (ucmp->CompareWithoutTimestamp(level_file->largest.user_key(),
                                             *end) == 0)) {
            continue;
          }
          edit.SetColumnFamily(cfd->GetID());
          edit.DeleteFile(i, level_file->fd.GetNumber());
          deleted_files.insert(level_file);
          level_file->being_compacted = true;
        }
      }
    }
    if (!deleted_files.empty()) {
      vstorage->ComputeCompactionScore(cfd->ioptions(),
                                       cfd->GetLatestMutableCFOptions(),
                                       cfd->GetFullHistoryTsLow());
    }
    if (edit.GetDeletedFiles().empty()) {
      job_context.Clean();
      return status;
    }
    input_version->Ref();
    status = versions_->LogAndApply(cfd, read_options, write_options, &edit,
                                    &mutex_, directories_.GetDbDir());
    if (status.ok()) {
      InstallSuperVersionAndScheduleWork(
          cfd, job_context.superversion_contexts.data(),
          /*defer_snapshot_refresh=*/false);
    }
    for (auto* deleted_file : deleted_files) {
      deleted_file->being_compacted = false;
    }
    input_version->Unref();
    FindObsoleteFiles(&job_context, false);
  }  // lock released here

  LogFlush(immutable_db_options_.info_log);
  // remove files outside the db-lock
  if (job_context.HaveSomethingToDelete()) {
    // Call PurgeObsoleteFiles() without holding mutex.
    PurgeObsoleteFiles(job_context);
  }
  job_context.Clean();
  return status;
}

void DBImpl::GetLiveFilesMetaData(std::vector<LiveFileMetaData>* metadata) {
  InstrumentedMutexLock l(&mutex_);
  versions_->GetLiveFilesMetaData(metadata);
}

Status DBImpl::GetLiveFilesChecksumInfo(FileChecksumList* checksum_list) {
  InstrumentedMutexLock l(&mutex_);
  return versions_->GetLiveFilesChecksumInfo(checksum_list);
}

Status DBImpl::GetPreparedFileInfoForExternalSstIngestion(
    const std::string& file_path,
    std::shared_ptr<const PreparedFileInfo>* file_info) {
  if (file_info == nullptr) {
    return Status::InvalidArgument("file_info must not be null");
  }
  file_info->reset();

  const size_t file_name_pos = file_path.find_last_of("/\\");
  const std::string file_name = file_name_pos == std::string::npos
                                    ? file_path
                                    : file_path.substr(file_name_pos + 1);
  uint64_t file_number = 0;
  FileType file_type;
  if (!ParseFileName(file_name, &file_number, &file_type) ||
      file_type != kTableFile || file_number == 0) {
    return Status::InvalidArgument("Invalid table file name: " + file_path);
  }

  int file_level = -1;
  FileMetaData* file_meta = nullptr;
  ColumnFamilyData* file_cfd = nullptr;
  Version* file_version = nullptr;
  std::shared_ptr<const TableProperties> table_properties;
  ReadOptions read_options;

  {
    InstrumentedMutexLock l(&mutex_);
    Status s = versions_->GetMetadataForFile(file_number, &file_level,
                                             &file_meta, &file_cfd);
    if (!s.ok()) {
      return s;
    }

    const std::string expected_file_path = TableFileName(
        file_cfd->ioptions().cf_paths, file_number, file_meta->fd.GetPathId());
    if (file_path != expected_file_path) {
      return Status::InvalidArgument("Path does not match live table file: " +
                                     file_path);
    }

    file_cfd->Ref();
    file_version = file_cfd->current();
    file_version->Ref();
  }
  const Defer cleanup_refs([&]() {
    InstrumentedMutexLock l(&mutex_);
    file_version->Unref();
    file_cfd->UnrefAndTryDelete();
  });

  Status s = file_cfd->table_cache()->GetTableProperties(
      file_options_, read_options, file_cfd->internal_comparator(), *file_meta,
      &table_properties, file_version->GetMutableCFOptions(),
      false /* no_io */);
  if (!s.ok()) {
    return s;
  }
  assert(table_properties != nullptr);

  auto prepared_file_info = std::make_shared<PreparedFileInfo>();
  prepared_file_info->file_size = file_meta->fd.GetFileSize();
  prepared_file_info->smallest = file_meta->smallest;
  prepared_file_info->largest = file_meta->largest;
  prepared_file_info->table_properties = *table_properties;
  prepared_file_info->table_properties.key_largest_seqno =
      file_meta->fd.largest_seqno;
  prepared_file_info->table_properties.key_smallest_seqno =
      file_meta->fd.smallest_seqno;
  *file_info = std::move(prepared_file_info);
  return s;
}

void DBImpl::GetColumnFamilyMetaData(ColumnFamilyHandle* column_family,
                                     ColumnFamilyMetaData* cf_meta) {
  assert(column_family);
  auto* cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(column_family)->cfd();
  {
    // Without mutex, Version::GetColumnFamilyMetaData will have data race
    // with Compaction::MarkFilesBeingCompacted. One solution is to use mutex,
    // but this may cause regression. An alternative is to make
    // FileMetaData::being_compacted atomic, but it will make FileMetaData
    // non-copy-able. Another option is to separate these variables from
    // original FileMetaData struct, and this requires re-organization of data
    // structures. For now, we take the easy approach. If
    // DB::GetColumnFamilyMetaData is not called frequently, the regression
    // should not be big. We still need to keep an eye on it.
    InstrumentedMutexLock l(&mutex_);
    cfd->current()->GetColumnFamilyMetaData(cf_meta);
  }
}

void DBImpl::GetColumnFamilyMetaData(
    ColumnFamilyHandle* column_family,
    const GetColumnFamilyMetaDataOptions& options,
    ColumnFamilyMetaData* metadata) {
  assert(column_family);
  auto* cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(column_family)->cfd();
  {
    InstrumentedMutexLock l(&mutex_);
    cfd->current()->GetColumnFamilyMetaData(options, metadata);
  }
}

void DBImpl::GetAllColumnFamilyMetaData(
    std::vector<ColumnFamilyMetaData>* metadata) {
  InstrumentedMutexLock l(&mutex_);
  for (auto cfd : *(versions_->GetColumnFamilySet())) {
    {
      metadata->emplace_back();
      cfd->current()->GetColumnFamilyMetaData(&metadata->back());
    }
  }
}

Status DBImpl::GetDbIdentity(std::string& identity) const {
  identity.assign(db_id_);
  return Status::OK();
}

Status DBImpl::GetDbIdentityFromIdentityFile(const IOOptions& opts,
                                             std::string* identity) const {
  std::string idfilename = IdentityFileName(dbname_);
  const FileOptions soptions;

  Status s = ReadFileToString(fs_.get(), idfilename, opts, identity);
  if (!s.ok()) {
    return s;
  }

  // If last character is '\n' remove it from identity. (Old implementations
  // of Env::GenerateUniqueId() would include a trailing '\n'.)
  if (identity->size() > 0 && identity->back() == '\n') {
    identity->pop_back();
  }
  return s;
}

Status DBImpl::GetDbSessionId(std::string& session_id) const {
  session_id.assign(db_session_id_);
  return Status::OK();
}

namespace {
SemiStructuredUniqueIdGen* DbSessionIdGen() {
  static SemiStructuredUniqueIdGen gen;
  return &gen;
}
}  // namespace

void DBImpl::TEST_ResetDbSessionIdGen() { DbSessionIdGen()->Reset(); }

std::string DBImpl::GenerateDbSessionId(Env*) {
  // See SemiStructuredUniqueIdGen for its desirable properties.
  auto gen = DbSessionIdGen();

  uint64_t lo;
  uint64_t hi;
  gen->GenerateNext(&hi, &lo);
  if (lo == 0) {
    // Avoid emitting session ID with lo==0, so that SST unique
    // IDs can be more easily ensured non-zero
    gen->GenerateNext(&hi, &lo);
    assert(lo != 0);
  }
  return EncodeSessionId(hi, lo);
}

void DBImpl::SetDbSessionId() {
  db_session_id_ = GenerateDbSessionId(env_);
  TEST_SYNC_POINT_CALLBACK("DBImpl::SetDbSessionId", &db_session_id_);
}

// Default implementation -- returns not supported status
Status DB::CreateColumnFamily(const ColumnFamilyOptions& /*cf_options*/,
                              const std::string& /*column_family_name*/,
                              ColumnFamilyHandle** /*handle*/) {
  return Status::NotSupported("");
}

Status DB::CreateColumnFamilies(
    const ColumnFamilyOptions& /*cf_options*/,
    const std::vector<std::string>& /*column_family_names*/,
    std::vector<ColumnFamilyHandle*>* /*handles*/) {
  return Status::NotSupported("");
}

Status DB::CreateColumnFamilies(
    const std::vector<ColumnFamilyDescriptor>& /*column_families*/,
    std::vector<ColumnFamilyHandle*>* /*handles*/) {
  return Status::NotSupported("");
}

Status DB::DropColumnFamily(ColumnFamilyHandle* /*column_family*/) {
  return Status::NotSupported("");
}

Status DB::DropColumnFamilies(
    const std::vector<ColumnFamilyHandle*>& /*column_families*/) {
  return Status::NotSupported("");
}

Status DB::DestroyColumnFamilyHandle(ColumnFamilyHandle* column_family) {
  if (DefaultColumnFamily() == column_family) {
    return Status::InvalidArgument(
        "Cannot destroy the handle returned by DefaultColumnFamily()");
  }
  delete column_family;
  return Status::OK();
}

DB::~DB() = default;

Status DBImpl::Close() {
  SlowMutexLoggingPause pause_logging(slow_mutex_logging_);
  InstrumentedMutexLock closing_lock_guard(&closing_mutex_);
  if (closed_) {
    return closing_status_;
  }

  {
    const Status s = MaybeReleaseTimestampedSnapshotsAndCheck();
    if (!s.ok()) {
      return s;
    }
  }

  closing_status_ = CloseImpl();
  closed_ = true;
  return closing_status_;
}

Status DB::ListColumnFamilies(const DBOptions& db_options,
                              const std::string& name,
                              std::vector<std::string>* column_families) {
  const std::shared_ptr<FileSystem>& fs = db_options.env->GetFileSystem();
  return VersionSet::ListColumnFamilies(column_families, name, fs.get());
}

Snapshot::~Snapshot() = default;

Status DestroyDB(const std::string& dbname, const Options& options,
                 const std::vector<ColumnFamilyDescriptor>& column_families) {
  ImmutableDBOptions soptions(SanitizeOptions(dbname, options));
  Env* env = soptions.env;
  std::vector<std::string> filenames;
  bool wal_in_db_path = soptions.IsWalDirSameAsDBPath();
  auto sfm = static_cast_with_check<SstFileManagerImpl>(
      options.sst_file_manager.get());
  // Allocate a separate trash bucket to be used by all the to be deleted
  // files, so we can later wait for this bucket to be empty before return.
  std::optional<int32_t> bucket;
  if (sfm) {
    bucket = sfm->NewTrashBucket();
  }

  // Reset the logger because it holds a handle to the
  // log file and prevents cleanup and directory removal
  soptions.info_log.reset();
  IOOptions io_opts;

  std::set<std::string> paths_to_delete;
  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    if (sfm) {
      sfm->WaitForEmptyTrashInDirectory(dbname);
    }
    // Ignore error in case directory does not exist
    soptions.fs
        ->GetChildren(dbname, io_opts, &filenames,
                      /*IODebugContext*=*/nullptr)
        .PermitUncheckedError();

    uint64_t number;
    FileType type;
    InfoLogPrefix info_log_prefix(!soptions.db_log_dir.empty(), dbname);
    for (const auto& fname : filenames) {
      if (DeleteScheduler::IsTrashFile(fname)) {
        std::string trash_file_path = dbname;
        trash_file_path.append("/").append(fname);
        Status del = DeleteExistingTrashDBFile(&soptions, trash_file_path,
                                               dbname, /*force_bg=*/false,
                                               /*force_fg=*/false, bucket);
        if (!del.ok() && result.ok()) {
          result = del;
        }
        continue;
      }
      if (ParseFileName(fname, &number, info_log_prefix.prefix, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del;
        std::string path_to_delete = dbname + "/" + fname;
        if (type == kMetaDatabase) {
          del = DestroyDB(path_to_delete, options);
        } else if (type == kTableFile || type == kWalFile ||
                   type == kBlobFile) {
          del = DeleteUnaccountedDBFile(&soptions, path_to_delete, dbname,
                                        /*force_bg=*/false,
                                        /*force_fg=*/false, bucket);
        } else {
          del = env->DeleteFile(path_to_delete);
        }
        if (!del.ok() && result.ok()) {
          result = del;
        }
      } else if (soptions.use_session_tmp_dir_for_remote_compaction &&
                 fname == kSessionTmpDirName) {
        // The session temporary directory is owned by the DB, but
        // ParseFileName rejects directories and the non-recursive
        // DeleteDir(dbname) below would otherwise fail to remove the DB.
        // Ignore failures: leftovers here must not fail DestroyDB.
        DestroyDir(env, SessionTmpDir(dbname)).PermitUncheckedError();
      }
    }
    paths_to_delete.insert(dbname);

    std::set<std::string> paths;
    for (const DbPath& db_path : options.db_paths) {
      paths.insert(db_path.path);
    }
    for (const ColumnFamilyDescriptor& cf : column_families) {
      for (const DbPath& cf_path : cf.options.cf_paths) {
        paths.insert(cf_path.path);
      }
    }

    for (const auto& path : paths) {
      if (sfm) {
        sfm->WaitForEmptyTrashInDirectory(path);
      }
      if (soptions.fs
              ->GetChildren(path, io_opts, &filenames,
                            /*IODebugContext*=*/nullptr)
              .ok()) {
        for (const auto& fname : filenames) {
          if (DeleteScheduler::IsTrashFile(fname)) {
            std::string trash_file_path = path;
            trash_file_path.append("/").append(fname);
            Status del = DeleteExistingTrashDBFile(&soptions, trash_file_path,
                                                   path, /*force_bg=*/false,
                                                   /*force_fg=*/false, bucket);
            if (!del.ok() && result.ok()) {
              result = del;
            }
          } else if (ParseFileName(fname, &number, &type) &&
                     (type == kTableFile ||
                      type == kBlobFile)) {  // Lock file will be deleted at end
            std::string file_path = path + "/" + fname;
            Status del = DeleteUnaccountedDBFile(&soptions, file_path, dbname,
                                                 /*force_bg=*/false,
                                                 /*force_fg=*/false, bucket);
            if (!del.ok() && result.ok()) {
              result = del;
            }
          }
        }
      }
    }

    paths_to_delete.merge(paths);

    std::vector<std::string> walDirFiles;
    std::string archivedir = ArchivalDirectory(dbname);
    bool wal_dir_exists = false;
    if (!soptions.IsWalDirSameAsDBPath(dbname)) {
      if (sfm) {
        sfm->WaitForEmptyTrashInDirectory(soptions.wal_dir);
      }
      wal_dir_exists =
          soptions.fs
              ->GetChildren(soptions.wal_dir, io_opts, &walDirFiles,
                            /*IODebugContext*=*/nullptr)
              .ok();
      archivedir = ArchivalDirectory(soptions.wal_dir);
    }

    // Archive dir may be inside wal dir or dbname and should be
    // processed and removed before those otherwise we have issues
    // removing them
    std::vector<std::string> archiveFiles;
    if (sfm) {
      sfm->WaitForEmptyTrashInDirectory(archivedir);
    }
    if (soptions.fs
            ->GetChildren(archivedir, io_opts, &archiveFiles,
                          /*IODebugContext*=*/nullptr)
            .ok()) {
      // Delete archival files.
      for (const auto& file : archiveFiles) {
        if (DeleteScheduler::IsTrashFile(file)) {
          std::string trash_file_path = archivedir;
          trash_file_path.append("/").append(file);
          Status del = DeleteExistingTrashDBFile(
              &soptions, trash_file_path, archivedir, /*force_bg=*/false,
              /*force_fg=*/!wal_in_db_path, bucket);
          if (!del.ok() && result.ok()) {
            result = del;
          }
        } else if (ParseFileName(file, &number, &type) && type == kWalFile) {
          Status del = DeleteUnaccountedDBFile(
              &soptions, archivedir + "/" + file, archivedir,
              /*force_bg=*/false, /*force_fg=*/!wal_in_db_path, bucket);
          if (!del.ok() && result.ok()) {
            result = del;
          }
        }
      }
      paths_to_delete.insert(archivedir);
    }

    // Delete log files in the WAL dir
    if (wal_dir_exists) {
      for (const auto& file : walDirFiles) {
        if (DeleteScheduler::IsTrashFile(file)) {
          std::string trash_file_path = soptions.wal_dir;
          trash_file_path.append("/").append(file);
          Status del = DeleteExistingTrashDBFile(
              &soptions, trash_file_path, soptions.wal_dir,
              /*force_bg=*/false, /*force_fg=*/!wal_in_db_path, bucket);
          if (!del.ok() && result.ok()) {
            result = del;
          }
        } else if (ParseFileName(file, &number, &type) && type == kWalFile) {
          Status del = DeleteUnaccountedDBFile(
              &soptions, LogFileName(soptions.wal_dir, number),
              soptions.wal_dir, /*force_bg=*/false,
              /*force_fg=*/!wal_in_db_path, bucket);
          if (!del.ok() && result.ok()) {
            result = del;
          }
        }
      }
      paths_to_delete.insert(soptions.wal_dir);
    }

    // Ignore error since state is already gone
    env->UnlockFile(lock).PermitUncheckedError();
    env->DeleteFile(lockname).PermitUncheckedError();

    // Make sure trash files are all cleared before return.
    if (sfm && bucket.has_value()) {
      sfm->WaitForEmptyTrashBucket(bucket.value());
    }
    // sst_file_manager holds a ref to the logger. Make sure the logger is
    // gone before trying to remove the directory.
    soptions.sst_file_manager.reset();

    // Ignore error in case dir contains other files
    for (const auto& path_to_delete : paths_to_delete) {
      env->DeleteDir(path_to_delete).PermitUncheckedError();
    }
  }
  return result;
}

Status DBImpl::WriteOptionsFile(const WriteOptions& write_options,
                                bool db_mutex_already_held) {
  options_mutex_.AssertHeld();

  if (db_mutex_already_held) {
    mutex_.AssertHeld();
  } else {
    mutex_.Lock();
  }

  std::vector<std::string> cf_names;
  std::vector<ColumnFamilyOptions> cf_opts;

  // This part requires mutex to protect the column family options
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    if (cfd->IsDropped()) {
      continue;
    }
    cf_names.push_back(cfd->GetName());
    cf_opts.push_back(cfd->GetLatestCFOptions());
  }

  DBOptions db_options =
      BuildDBOptions(immutable_db_options_, mutable_db_options_);
  const bool track_options_file_number =
      immutable_db_options_.track_options_file_number_in_manifest;

  // Unlock during expensive operations.
  mutex_.Unlock();

  uint64_t options_file_number = 0;
  uint64_t options_file_size = 0;
  Status s = PersistOptionsFile(write_options, db_options, cf_names, cf_opts,
                                &options_file_number, &options_file_size);
  if (s.ok()) {
    // TEST_SYNC_POINT handles the disabled singleton case internally.
    // @lint-ignore NULLSAFECLANG nullable-dereference
    TEST_SYNC_POINT("DBImpl::WriteOptionsFile:AfterPersistOptions");
  }

  if (s.ok()) {
    mutex_.Lock();
    if (track_options_file_number) {
      VersionEdit edit;
      edit.SetCommittedOptionsFileNumber(options_file_number);
      edit.MarkOptionsFileManipulation();
      s = versions_->LogAndApplyToDefaultColumnFamily(
          ReadOptions(), write_options, &edit, &mutex_,
          directories_.GetDbDir());
      if (!s.ok() && versions_->io_status().IsIOError()) {
        error_handler_.SetBGError(versions_->io_status(),
                                  BackgroundErrorReason::kManifestWrite);
      }
    } else {
      versions_->ApplyLegacyOptionsFileNumber(options_file_number);
    }
    if (s.ok()) {
      versions_->options_file_size_ = options_file_size;
    }
    mutex_.Unlock();
  }

  if (s.ok()) {
    MaybeDeleteObsoleteOptionsFiles();
  }

  // Restore lock if appropriate
  if (db_mutex_already_held) {
    mutex_.Lock();
  }
  return s;
}

Status DBImpl::PersistOptionsFile(
    const WriteOptions& write_options, const DBOptions& db_options,
    const std::vector<std::string>& cf_names,
    const std::vector<ColumnFamilyOptions>& cf_opts,
    uint64_t* options_file_number, uint64_t* options_file_size) {
  assert(options_file_number != nullptr);
  assert(options_file_size != nullptr);
  *options_file_number = 0;
  *options_file_size = 0;

  TEST_SYNC_POINT("DBImpl::WriteOptionsFile:1");
  TEST_SYNC_POINT("DBImpl::WriteOptionsFile:2");
  TEST_SYNC_POINT_CALLBACK("DBImpl::WriteOptionsFile:PersistOptions",
                           const_cast<DBOptions*>(&db_options));

  const std::string file_name =
      TempOptionsFileName(GetName(), versions_->NewFileNumber());
  *options_file_number = versions_->NewFileNumber();

  Status s = PersistRocksDBOptions(write_options, db_options, cf_names, cf_opts,
                                   file_name, fs_.get());
  if (s.ok()) {
    s = RenameTempFileToOptionsFile(file_name, *options_file_number,
                                    options_file_size);
  }

  if (!s.ok() && GetEnv()->FileExists(file_name).ok()) {
    if (!GetEnv()->DeleteFile(file_name).ok()) {
      ROCKS_LOG_WARN(immutable_db_options_.info_log,
                     "Unable to delete temp options file %s",
                     file_name.c_str());
    }
  }
  if (!s.ok()) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "Unable to persist options -- %s", s.ToString().c_str());
    s = Status::IOError("Unable to persist options.", s.ToString().c_str());
  }
  return s;
}

Status DBImpl::PersistOptionsFileForColumnFamilyManipulation(
    const WriteOptions& write_options,
    const std::vector<ColumnFamilyDescriptor>& added_column_families,
    const std::vector<uint32_t>& excluded_cf_ids, uint64_t* options_file_number,
    uint64_t* options_file_size) {
  options_mutex_.AssertHeld();
  assert(options_file_number != nullptr);
  assert(options_file_size != nullptr);
  *options_file_number = 0;
  *options_file_size = 0;

  bool bootstrap_tracking = false;
  {
    InstrumentedMutexLock l(&mutex_);
    bootstrap_tracking =
        immutable_db_options_.track_options_file_number_in_manifest &&
        !versions_->has_committed_options_file_number();
  }
  if (bootstrap_tracking) {
    // A DB without a committed number still uses highest-numbered OPTIONS
    // semantics. Commit the current topology before writing a proposed
    // topology snapshot, so a failed CF MANIFEST edit cannot make its orphan
    // candidate authoritative.
    Status bootstrap_s =
        WriteOptionsFile(write_options, /*db_mutex_already_held=*/false);
    if (!bootstrap_s.ok()) {
      return bootstrap_s;
    }
  }

  std::vector<std::string> cf_names;
  std::vector<ColumnFamilyOptions> cf_opts;
  DBOptions db_options;
  const std::unordered_set<uint32_t> excluded_cf_id_set(excluded_cf_ids.begin(),
                                                        excluded_cf_ids.end());
  {
    InstrumentedMutexLock l(&mutex_);
    assert(versions_ != nullptr);
    if (!immutable_db_options_.track_options_file_number_in_manifest) {
      return Status::OK();
    }
    auto* column_family_set = versions_->GetColumnFamilySet();
    assert(column_family_set != nullptr);
    for (auto* cfd : *column_family_set) {
      assert(cfd != nullptr);
      if (cfd->IsDropped() || excluded_cf_id_set.count(cfd->GetID()) != 0) {
        continue;
      }
      cf_names.push_back(cfd->GetName());
      cf_opts.push_back(cfd->GetLatestCFOptions());
    }
    for (const auto& column_family : added_column_families) {
      cf_names.push_back(column_family.name);
      cf_opts.push_back(column_family.options);
    }
    db_options = BuildDBOptions(immutable_db_options_, mutable_db_options_);
  }

  return PersistOptionsFile(write_options, db_options, cf_names, cf_opts,
                            options_file_number, options_file_size);
}

namespace {
void DeleteOptionsFilesHelper(const std::map<uint64_t, std::string>& filenames,
                              const size_t num_files_to_keep,
                              uint64_t protected_file_number,
                              const std::shared_ptr<Logger>& info_log,
                              Env* env) {
  if (filenames.size() <= num_files_to_keep) {
    return;
  }
  for (auto iter = std::next(filenames.begin(), num_files_to_keep);
       iter != filenames.end(); ++iter) {
    const uint64_t file_number =
        std::numeric_limits<uint64_t>::max() - iter->first;
    if (file_number == protected_file_number) {
      continue;
    }
    if (!env->DeleteFile(iter->second).ok()) {
      ROCKS_LOG_WARN(info_log, "Unable to delete options file %s",
                     iter->second.c_str());
    }
  }
}
}  // namespace

Status DBImpl::DeleteObsoleteOptionsFiles(bool schedule_only) {
  std::vector<std::string> filenames;
  // use ordered map to store keep the filenames sorted from the newest
  // to the oldest.
  std::map<uint64_t, std::string> options_filenames;
  Status s;
  IOOptions io_opts;
  io_opts.do_not_recurse = true;
  s = fs_->GetChildren(GetName(), io_opts, &filenames,
                       /*IODebugContext*=*/nullptr);
  if (!s.ok()) {
    return s;
  }
  for (auto& filename : filenames) {
    uint64_t file_number;
    FileType type;
    if (ParseFileName(filename, &file_number, &type) && type == kOptionsFile) {
      options_filenames.insert(
          {std::numeric_limits<uint64_t>::max() - file_number,
           GetName() + "/" + filename});
    }
  }

  // Keep the latest two snapshots for legacy fallback. The snapshot named by
  // the committed MANIFEST number is retained separately below and can
  // increase the total number of retained OPTIONS files beyond two.
  const size_t kNumOptionsFilesKept = 2;
  uint64_t protected_file_number = 0;
  {
    InstrumentedMutexLock l(&mutex_);
    if (versions_->has_committed_options_file_number()) {
      protected_file_number = versions_->options_file_number();
    }
  }
  if (options_filenames.size() > kNumOptionsFilesKept) {
    if (schedule_only) {
      InstrumentedMutexLock l(&mutex_);
      for (auto iter =
               std::next(options_filenames.begin(), kNumOptionsFilesKept);
           iter != options_filenames.end(); ++iter) {
        const uint64_t file_number =
            std::numeric_limits<uint64_t>::max() - iter->first;
        if (file_number == protected_file_number) {
          continue;
        }
        SchedulePendingPurge(iter->second, GetName(), kOptionsFile, file_number,
                             /*job_id=*/0);
      }
      SchedulePurge();
    } else {
      DeleteOptionsFilesHelper(options_filenames, kNumOptionsFilesKept,
                               protected_file_number,
                               immutable_db_options_.info_log, GetEnv());
    }
  }
  return Status::OK();
}

Status DBImpl::RenameTempFileToOptionsFile(const std::string& file_name,
                                           uint64_t options_file_number,
                                           uint64_t* options_file_size) {
  Status s;

  assert(options_file_size != nullptr);
  std::string options_file_name =
      OptionsFileName(GetName(), options_file_number);
  Env* env = GetEnv();
  assert(env != nullptr);
  s = env->GetFileSize(file_name, options_file_size);
  if (s.ok()) {
    // Retry if the file name happen to conflict with an existing one.
    s = env->RenameFile(file_name, options_file_name);
    std::unique_ptr<FSDirectory> dir_obj;
    if (s.ok()) {
      s = fs_->NewDirectory(GetName(), IOOptions(), &dir_obj, nullptr);
    }
    if (s.ok()) {
      s = dir_obj->FsyncWithDirOptions(IOOptions(), nullptr,
                                       DirFsyncOptions(options_file_name));
    }
    if (s.ok()) {
      Status temp_s = dir_obj->Close(IOOptions(), nullptr);
      // The default Close() could return "NotSupproted" and we bypass it
      // if it is not impelmented. Detailed explanations can be found in
      // db/db_impl/db_impl.h
      if (!temp_s.ok()) {
        if (temp_s.IsNotSupported()) {
          temp_s.PermitUncheckedError();
        } else {
          s = temp_s;
        }
      }
    }
  }

  return s;
}

void DBImpl::MaybeDeleteObsoleteOptionsFiles() {
  enum class Cleanup { kSkip, kDeleteNow, kSchedule };
  Cleanup cleanup = Cleanup::kSkip;
  {
    InstrumentedMutexLock l(&mutex_);
    if (!disable_delete_obsolete_files_ &&
        immutable_db_options_.compaction_service == nullptr) {
      if (immutable_db_options_.avoid_unnecessary_blocking_io &&
          !reject_new_background_jobs_) {
        cleanup = opened_successfully_ ? Cleanup::kSchedule : Cleanup::kSkip;
      } else {
        cleanup = Cleanup::kDeleteNow;
      }
    }
  }
  if (cleanup == Cleanup::kSkip) {
    return;
  }
  Status s = DeleteObsoleteOptionsFiles(cleanup == Cleanup::kSchedule);
  if (!s.ok()) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "Unable to delete obsolete OPTIONS files: %s",
                   s.ToString().c_str());
  }
}

#ifndef NROCKSDB_THREAD_STATUS

void DBImpl::NewThreadStatusCfInfo(ColumnFamilyData* cfd) const {
  if (immutable_db_options_.enable_thread_tracking) {
    ThreadStatusUtil::NewColumnFamilyInfo(this, cfd, cfd->GetName(),
                                          cfd->ioptions().env);
  }
}

void DBImpl::EraseThreadStatusCfInfo(ColumnFamilyData* cfd) const {
  if (immutable_db_options_.enable_thread_tracking) {
    ThreadStatusUtil::EraseColumnFamilyInfo(cfd);
  }
}

void DBImpl::EraseThreadStatusDbInfo() const {
  if (immutable_db_options_.enable_thread_tracking) {
    ThreadStatusUtil::EraseDatabaseInfo(this);
  }
}

#else
void DBImpl::NewThreadStatusCfInfo(ColumnFamilyData* /*cfd*/) const {}

void DBImpl::EraseThreadStatusCfInfo(ColumnFamilyData* /*cfd*/) const {}

void DBImpl::EraseThreadStatusDbInfo() const {}
#endif  // !NROCKSDB_THREAD_STATUS

//
// A global method that can dump out the build version
void DumpRocksDBBuildVersion(Logger* log) {
  ROCKS_LOG_HEADER(log, "RocksDB version: %s\n",
                   GetRocksVersionAsString().c_str());
  const auto& props = GetRocksBuildProperties();
  const auto& sha = props.find("rocksdb_build_git_sha");
  if (sha != props.end()) {
    ROCKS_LOG_HEADER(log, "Git sha %s", sha->second.c_str());
  }
  const auto date = props.find("rocksdb_build_date");
  if (date != props.end()) {
    ROCKS_LOG_HEADER(log, "Compile date %s", date->second.c_str());
  }
}

SequenceNumber DBImpl::GetEarliestMemTableSequenceNumber(SuperVersion* sv,
                                                         bool include_history) {
  // Find the earliest sequence number that we know we can rely on reading
  // from the memtable without needing to check sst files.
  SequenceNumber earliest_seq =
      sv->imm->GetEarliestSequenceNumber(include_history);
  if (earliest_seq == kMaxSequenceNumber) {
    earliest_seq = sv->mem->GetEarliestSequenceNumber();
  }
  assert(sv->mem->GetEarliestSequenceNumber() >= earliest_seq);

  return earliest_seq;
}

Status DBImpl::GetLatestSequenceForKey(
    SuperVersion* sv, const Slice& key, bool cache_only,
    SequenceNumber lower_bound_seq, SequenceNumber* seq, std::string* timestamp,
    bool* found_record_for_key, bool* is_blob_index) {
  Status s;
  MergeContext merge_context;
  SequenceNumber max_covering_tombstone_seq = 0;

  // TODO: plumb Env::IOActivity, Env::IOPriority
  ReadOptions read_options;
  SequenceNumber current_seq = versions_->LastSequence();

  ColumnFamilyData* cfd = sv->cfd;
  assert(cfd);
  const Comparator* const ucmp = cfd->user_comparator();
  assert(ucmp);
  size_t ts_sz = ucmp->timestamp_size();
  std::string ts_buf;
  if (ts_sz > 0) {
    assert(timestamp);
    ts_buf.assign(ts_sz, '\xff');
  } else {
    assert(!timestamp);
  }
  Slice ts(ts_buf);

  LookupKey lkey(key, current_seq, ts_sz == 0 ? nullptr : &ts);

  *seq = kMaxSequenceNumber;
  *found_record_for_key = false;
  std::optional<VersionBlobFetcher> memtable_blob_fetcher;
  if (cfd->blob_partition_manager() != nullptr) {
    memtable_blob_fetcher.emplace(sv->current, read_options,
                                  cfd->blob_file_cache(),
                                  /*allow_write_path_fallback=*/true);
  }
  const BlobFetcher* memtable_blob_fetcher_ptr =
      memtable_blob_fetcher ? &*memtable_blob_fetcher : nullptr;

  // Check if there is a record for this key in the latest memtable
  sv->mem->Get(lkey, /*value=*/nullptr, /*columns=*/nullptr, timestamp, &s,
               &merge_context, &max_covering_tombstone_seq, seq, read_options,
               false /* immutable_memtable */, nullptr /*read_callback*/,
               is_blob_index, /*do_merge=*/true, memtable_blob_fetcher_ptr);

  if (!(s.ok() || s.IsNotFound() || s.IsMergeInProgress())) {
    // unexpected error reading memtable.
    ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                    "Unexpected status returned from MemTable::Get: %s\n",
                    s.ToString().c_str());

    return s;
  }
  assert(!ts_sz ||
         (*seq != kMaxSequenceNumber &&
          *timestamp != std::string(ts_sz, '\xff')) ||
         (*seq == kMaxSequenceNumber && timestamp->empty()));

  TEST_SYNC_POINT_CALLBACK("DBImpl::GetLatestSequenceForKey:mem", timestamp);

  if (*seq != kMaxSequenceNumber) {
    // Found a sequence number, no need to check immutable memtables
    *found_record_for_key = true;
    return Status::OK();
  }

  SequenceNumber lower_bound_in_mem = sv->mem->GetEarliestSequenceNumber();
  if (lower_bound_in_mem != kMaxSequenceNumber &&
      lower_bound_in_mem < lower_bound_seq) {
    *found_record_for_key = false;
    return Status::OK();
  }

  // Check if there is a record for this key in the immutable memtables
  sv->imm->Get(lkey, /*value=*/nullptr, /*columns=*/nullptr, timestamp, &s,
               &merge_context, &max_covering_tombstone_seq, seq, read_options,
               nullptr /*read_callback*/, is_blob_index,
               memtable_blob_fetcher_ptr);

  if (!(s.ok() || s.IsNotFound() || s.IsMergeInProgress())) {
    // unexpected error reading memtable.
    ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                    "Unexpected status returned from MemTableList::Get: %s\n",
                    s.ToString().c_str());

    return s;
  }

  assert(!ts_sz ||
         (*seq != kMaxSequenceNumber &&
          *timestamp != std::string(ts_sz, '\xff')) ||
         (*seq == kMaxSequenceNumber && timestamp->empty()));

  if (*seq != kMaxSequenceNumber) {
    // Found a sequence number, no need to check memtable history
    *found_record_for_key = true;
    return Status::OK();
  }

  SequenceNumber lower_bound_in_imm = sv->imm->GetEarliestSequenceNumber();
  if (lower_bound_in_imm != kMaxSequenceNumber &&
      lower_bound_in_imm < lower_bound_seq) {
    *found_record_for_key = false;
    return Status::OK();
  }

  // Check if there is a record for this key in the immutable memtables
  sv->imm->GetFromHistory(lkey, /*value=*/nullptr, /*columns=*/nullptr,
                          timestamp, &s, &merge_context,
                          &max_covering_tombstone_seq, seq, read_options,
                          is_blob_index, memtable_blob_fetcher_ptr);

  if (!(s.ok() || s.IsNotFound() || s.IsMergeInProgress())) {
    // unexpected error reading memtable.
    ROCKS_LOG_ERROR(
        immutable_db_options_.info_log,
        "Unexpected status returned from MemTableList::GetFromHistory: %s\n",
        s.ToString().c_str());

    return s;
  }

  assert(!ts_sz ||
         (*seq != kMaxSequenceNumber &&
          *timestamp != std::string(ts_sz, '\xff')) ||
         (*seq == kMaxSequenceNumber && timestamp->empty()));

  if (*seq != kMaxSequenceNumber) {
    // Found a sequence number, no need to check SST files
    assert(0 == ts_sz || *timestamp != std::string(ts_sz, '\xff'));
    *found_record_for_key = true;
    return Status::OK();
  }

  // We could do a sv->imm->GetEarliestSequenceNumber(/*include_history*/ true)
  // check here to skip the history if possible. But currently the caller
  // already does that. Maybe we should move the logic here later.

  // TODO(agiardullo): possible optimization: consider checking cached
  // SST files if cache_only=true?
  if (!cache_only) {
    // Check tables
    PinnedIteratorsManager pinned_iters_mgr;
    sv->current->Get(read_options, lkey, /*value=*/nullptr, /*columns=*/nullptr,
                     timestamp, &s, &merge_context, &max_covering_tombstone_seq,
                     &pinned_iters_mgr, nullptr /* value_found */,
                     found_record_for_key, seq, nullptr /*read_callback*/,
                     is_blob_index);

    if (!(s.ok() || s.IsNotFound() || s.IsMergeInProgress())) {
      // unexpected error reading SST files
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "Unexpected status returned from Version::Get: %s\n",
                      s.ToString().c_str());
    }
  }

  return s;
}

Status DBImpl::IngestExternalFile(
    ColumnFamilyHandle* column_family,
    const std::vector<std::string>& external_files,
    const IngestExternalFileOptions& ingestion_options) {
  IngestExternalFileArg arg;
  arg.column_family = column_family;
  arg.external_files = external_files;
  arg.options = ingestion_options;
  return IngestExternalFiles({arg});
}

class FileIngestionHandleImpl : public FileIngestionHandle {
 public:
  explicit FileIngestionHandleImpl(DBImpl* db) : db_(db) {}
  ~FileIngestionHandleImpl() override;

  FileIngestionHandleImpl(const FileIngestionHandleImpl&) = delete;
  FileIngestionHandleImpl& operator=(const FileIngestionHandleImpl&) = delete;
  FileIngestionHandleImpl(FileIngestionHandleImpl&&) = delete;
  FileIngestionHandleImpl& operator=(FileIngestionHandleImpl&&) = delete;

  Status Abort() override;

  DBImpl* const db_;
  std::vector<std::unique_ptr<ExternalSstFileIngestionJob>> jobs_;
  std::unique_ptr<std::list<uint64_t>::iterator> pending_output_elem_;
  bool fill_cache_ = true;
  // Set true once committed or aborted, so the destructor does not roll back.
  bool consumed_ = false;
};

FileIngestionHandleImpl::~FileIngestionHandleImpl() {
  if (consumed_ || db_ == nullptr) {
    return;
  }
  // Dropped without commit or abort: roll back as a safety net.
  db_->RollbackPreparedFileIngestion(this);
  ROCKS_LOG_WARN(
      db_->immutable_db_options_.info_log,
      "[%zu CF(s)] File ingestion handle destroyed without commit or "
      "abort; prepared files were rolled back.",
      jobs_.size());
}

void DBImpl::RollbackPreparedFileIngestion(FileIngestionHandleImpl* const h) {
  // Delete the staged internal files and release the reserved file numbers /
  // pending-output protection, leaving the DB unchanged.
  const Status rollback_status = Status::Incomplete("file ingestion aborted");
  for (auto& job : h->jobs_) {
    job->Cleanup(rollback_status);
  }
  {
    InstrumentedMutexLock l(&mutex_);
    ReleaseFileNumberFromPendingOutputs(h->pending_output_elem_);
  }
  h->consumed_ = true;
  num_outstanding_prepared_ingestions_.fetch_sub(1);
}

Status FileIngestionHandleImpl::Abort() {
  if (consumed_) {
    return Status::InvalidArgument(
        "file ingestion handle has already been committed or aborted");
  }
  db_->RollbackPreparedFileIngestion(this);
  return Status::OK();
}

Status DBImpl::PrepareFileIngestion(
    const std::vector<IngestExternalFileArg>& args,
    std::unique_ptr<FileIngestionHandle>* handle) {
  if (handle == nullptr) {
    return Status::InvalidArgument("file ingestion handle output is null");
  }
  handle->reset();
  // Recorded as INGEST_EXTERNAL_FILE_PREPARE_TIME on success below.
  const bool record_ingest_micros =
      stats_ != nullptr &&
      stats_->get_stats_level() > StatsLevel::kExceptTimers;
  const uint64_t prepare_start_micros =
      record_ingest_micros ? immutable_db_options_.clock->NowMicros() : 0;

  if (args.empty()) {
    return Status::InvalidArgument("ingestion arg list is empty");
  }
  {
    std::unordered_set<ColumnFamilyHandle*> unique_cfhs;
    for (const auto& arg : args) {
      if (arg.column_family == nullptr) {
        return Status::InvalidArgument("column family handle is null");
      } else if (unique_cfhs.count(arg.column_family) > 0) {
        return Status::InvalidArgument(
            "ingestion args have duplicate column families");
      }
      unique_cfhs.insert(arg.column_family);
    }
  }
  // Ingest multiple external SST files atomically.
  const size_t num_cfs = args.size();
  for (size_t i = 0; i != num_cfs; ++i) {
    if (args[i].external_files.empty()) {
      std::string err_msg =
          "external_files[" + std::to_string(i) + "] is empty";
      return Status::InvalidArgument(err_msg);
    }
    if (!args[i].file_infos.empty()) {
      if (args[i].file_infos.size() != args[i].external_files.size()) {
        return Status::InvalidArgument("file_infos[" + std::to_string(i) +
                                       "] size must match external_files[" +
                                       std::to_string(i) + "] size");
      }
    }
    if (i && args[i].options.fill_cache != args[i - 1].options.fill_cache) {
      return Status::InvalidArgument(
          "fill_cache should be the same across ingestion options.");
    }
  }
  for (const auto& arg : args) {
    const IngestExternalFileOptions& ingest_opts = arg.options;
    if (ingest_opts.ingest_behind) {
      auto ucmp = arg.column_family->GetComparator();
      assert(ucmp);
      if (ucmp->timestamp_size() > 0) {
        return Status::NotSupported(
            "Column family with user-defined "
            "timestamps enabled doesn't support ingest behind.");
      }

      if (!static_cast<ColumnFamilyHandleImpl*>(arg.column_family)
               ->cfd()
               ->AllowIngestBehind()) {
        return Status::InvalidArgument(
            "Can't ingest_behind file in ColumnFamily %s with "
            "cf_allow_ingest_behind=false");
      }
    }
    if (arg.atomic_replace_range.has_value()) {
      if (ingest_opts.ingest_behind) {
        return Status::InvalidArgument(
            "Can't combine atomic_replace_range with ingest_behind.");
      }
      if (ingest_opts.snapshot_consistency) {
        // TODO: support generating and ingesting a big tombstone file, which
        // might depend on non-nullptr start and limit
        return Status::NotSupported(
            "atomic_replace_range not yet supported with "
            "snapshot_consistency.");
      } else {
        if (arg.atomic_replace_range->start.has_value() ^
            arg.atomic_replace_range->limit.has_value()) {
          return Status::NotSupported(
              "Only one of atomic_replace_range.{start,limit}.has_value() is "
              "not supported.");
        }
      }
    }

    if (ingest_opts.move_files && ingest_opts.link_files) {
      return Status::InvalidArgument(
          "`move_files` and `link_files` can not both be true.");
    }
  }

  std::vector<std::unique_ptr<ExternalSstFileIngestionJob>> ingestion_jobs;
  ingestion_jobs.reserve(num_cfs);
  for (const auto& arg : args) {
    auto* cfd = static_cast<ColumnFamilyHandleImpl*>(arg.column_family)->cfd();
    ingestion_jobs.push_back(std::make_unique<ExternalSstFileIngestionJob>(
        versions_.get(), cfd, immutable_db_options_, mutable_db_options_,
        file_options_, &snapshots_, arg.options, &directories_, &event_logger_,
        io_tracer_));
  }

  // TODO (yanqin) maybe handle the case in which column_families have
  // duplicates
  std::unique_ptr<std::list<uint64_t>::iterator> pending_output_elem;
  size_t total = 0;
  for (size_t i = 0; i != num_cfs; ++i) {
    total += ingestion_jobs[i]->NumFilesToPrepare(args[i].external_files.size(),
                                                  args[i].atomic_replace_range);
  }
  uint64_t next_file_number = 0;
  Status status = ReserveFileNumbersBeforeIngestion(
      static_cast<ColumnFamilyHandleImpl*>(args[0].column_family)->cfd(), total,
      pending_output_elem, &next_file_number);
  if (!status.ok()) {
    InstrumentedMutexLock l(&mutex_);
    ReleaseFileNumberFromPendingOutputs(pending_output_elem);
    return status;
  }

  // TODO(yanqin) maybe make jobs run in parallel
  uint64_t start_file_number = next_file_number;
  for (size_t i = 1; i != num_cfs; ++i) {
    start_file_number += ingestion_jobs[i - 1]->NumFilesToPrepare(
        args[i - 1].external_files.size(), args[i - 1].atomic_replace_range);
    SuperVersion* super_version =
        ingestion_jobs[i]->GetColumnFamilyData()->GetReferencedSuperVersion(
            this);
    Status es = ingestion_jobs[i]->Prepare(
        args[i].external_files, args[i].files_checksums,
        args[i].files_checksum_func_names, args[i].file_infos,
        args[i].atomic_replace_range, args[i].file_temperature,
        start_file_number, super_version);
    // capture first error only
    if (!es.ok() && status.ok()) {
      status = es;
    }
    CleanupSuperVersion(super_version);
  }
  TEST_SYNC_POINT("DBImpl::IngestExternalFiles:BeforeLastJobPrepare:0");
  TEST_SYNC_POINT("DBImpl::IngestExternalFiles:BeforeLastJobPrepare:1");
  {
    SuperVersion* super_version =
        ingestion_jobs[0]->GetColumnFamilyData()->GetReferencedSuperVersion(
            this);
    Status es = ingestion_jobs[0]->Prepare(
        args[0].external_files, args[0].files_checksums,
        args[0].files_checksum_func_names, args[0].file_infos,
        args[0].atomic_replace_range, args[0].file_temperature,
        next_file_number, super_version);
    if (!es.ok()) {
      status = es;
    }
    CleanupSuperVersion(super_version);
  }
  if (!status.ok()) {
    for (size_t i = 0; i != num_cfs; ++i) {
      ingestion_jobs[i]->Cleanup(status);
    }
    InstrumentedMutexLock l(&mutex_);
    ReleaseFileNumberFromPendingOutputs(pending_output_elem);
    return status;
  }

  auto handle_impl = std::make_unique<FileIngestionHandleImpl>(this);
  handle_impl->jobs_ = std::move(ingestion_jobs);
  handle_impl->pending_output_elem_ = std::move(pending_output_elem);
  handle_impl->fill_cache_ = args[0].options.fill_cache;
  if (record_ingest_micros) {
    RecordTimeToHistogram(
        stats_, INGEST_EXTERNAL_FILE_PREPARE_TIME,
        immutable_db_options_.clock->NowMicros() - prepare_start_micros);
  }
  num_outstanding_prepared_ingestions_.fetch_add(1);
  *handle = std::move(handle_impl);
  return Status::OK();
}

Status DBImpl::CommitFileIngestionHandles(
    std::vector<std::unique_ptr<FileIngestionHandle>> handles) {
  if (handles.empty()) {
    return Status::InvalidArgument("no file ingestion handles to commit");
  }
  // Validate the handles and group their jobs by column family, merging same-CF
  // jobs into one so each column family commits via a single atomic version
  // edit and one SuperVersion install.
  Status status;
  std::vector<FileIngestionHandleImpl*> hs;
  hs.reserve(handles.size());
  std::vector<ExternalSstFileIngestionJob*> ingestion_jobs;
  const bool fill_cache =
      static_cast<FileIngestionHandleImpl*>(handles[0].get())->fill_cache_;
  int max_file_opening_threads = 1;

  {
    UnorderedMap<ColumnFamilyData*, ExternalSstFileIngestionJob*>
        primary_job_for_cfd;
    for (const auto& handle : handles) {
      assert(handle);
      auto* h = static_cast<FileIngestionHandleImpl*>(handle.get());
      assert(h->db_ == this);
      if (h->consumed_) {
        status = Status::InvalidArgument(
            "file ingestion handle has already been committed or aborted");
      }
      if (h->fill_cache_ != fill_cache) {
        status = Status::InvalidArgument(
            "fill cache arg must be consistent across all handles");
      }
      if (!status.ok()) {
        return status;
      }

      hs.push_back(h);
      for (auto& job : h->jobs_) {
        max_file_opening_threads =
            std::max(max_file_opening_threads, job->file_opening_threads());
        auto [it, inserted] = primary_job_for_cfd.try_emplace(
            job->GetColumnFamilyData(), job.get());
        if (inserted) {
          ingestion_jobs.push_back(job.get());
        } else {
          status = it->second->MergeForSameColumnFamily(job.get());
          if (!status.ok()) {
            return status;
          }
        }
      }
    }
  }
  const size_t num_jobs = ingestion_jobs.size();

  // TODO: plumb Env::IOActivity, Env::IOPriority
  const WriteOptions write_options;
  // Decided here (not in Prepare) so the histograms reflect the stats level at
  // commit time; recorded only on success below.
  const bool record_ingest_micros =
      stats_ != nullptr &&
      stats_->get_stats_level() > StatsLevel::kExceptTimers;
  const uint64_t run_start_micros =
      record_ingest_micros ? immutable_db_options_.clock->NowMicros() : 0;

  std::vector<SuperVersionContext> sv_ctxs;
  for (size_t i = 0; i != num_jobs; ++i) {
    sv_ctxs.emplace_back(true /* create_superversion */);
  }
  TEST_SYNC_POINT("DBImpl::IngestExternalFiles:BeforeJobsRun:0");
  TEST_SYNC_POINT("DBImpl::IngestExternalFiles:BeforeJobsRun:1");
  TEST_SYNC_POINT("DBImpl::AddFile:Start");

  // Acquire per-CF ingest_sst_lock as ReadLocks (shared) BEFORE the DB
  // mutex so the lock-acquisition order is ingest_sst_lock -> DB mutex
  // throughout. Use readlock so we still allow concurrent ingestions.
  std::vector<std::unique_ptr<ReadLock>> ingest_read_locks;
  ingest_read_locks.reserve(num_jobs);
  for (auto* job : ingestion_jobs) {
    auto* cfd = job->GetColumnFamilyData();
    if (!cfd->IsDropped()) {
      ingest_read_locks.emplace_back(
          std::make_unique<ReadLock>(&cfd->GetIngestSstLock()));
    }
  }
  {
    InstrumentedMutexLock l(&mutex_);
    TEST_SYNC_POINT("DBImpl::AddFile:MutexLock");

    // Stop writes to the DB by entering both write threads
    WriteThread::Writer w;
    write_thread_.EnterUnbatched(&w, &mutex_);
    WriteThread::Writer nonmem_w;
    if (two_write_queues_) {
      nonmem_write_thread_.EnterUnbatched(&nonmem_w, &mutex_);
    }
    PERF_TIMER_GUARD(file_ingestion_blocking_live_writes_nanos);

    // When unordered_write is enabled, the keys are writing to memtable in an
    // unordered way. If the ingestion job checks memtable key range before the
    // key landing in memtable, the ingestion job may skip the necessary
    // memtable flush.
    // So wait here to ensure there is no pending write to memtable.
    WaitForPendingWrites();

    num_running_ingest_file_ += static_cast<int>(num_jobs);
    TEST_SYNC_POINT("DBImpl::IngestExternalFile:AfterIncIngestFileCounter");
    TEST_SYNC_POINT("DBImpl::IngestExternalFile:AfterIncIngestFileCounter:2");

    bool at_least_one_cf_need_flush = false;
    std::vector<bool> need_flush(num_jobs, false);
    for (size_t i = 0; i != num_jobs; ++i) {
      auto* cfd = ingestion_jobs[i]->GetColumnFamilyData();
      if (cfd->IsDropped()) {
        // TODO (yanqin) investigate whether we should abort ingestion or
        // proceed with other non-dropped column families.
        status = Status::InvalidArgument(
            "cannot ingest an external file into a dropped CF");
        break;
      }
      bool tmp = false;
      status = ingestion_jobs[i]->NeedsFlush(&tmp, cfd->GetSuperVersion());
      need_flush[i] = tmp;
      at_least_one_cf_need_flush = (at_least_one_cf_need_flush || tmp);
      if (!status.ok()) {
        break;
      }
    }
    TEST_SYNC_POINT_CALLBACK("DBImpl::IngestExternalFile:NeedFlush",
                             &at_least_one_cf_need_flush);

    if (status.ok() && at_least_one_cf_need_flush) {
      FlushOptions flush_opts;
      flush_opts.allow_write_stall = true;
      if (immutable_db_options_.atomic_flush) {
        mutex_.Unlock();
        status = AtomicFlushMemTables(
            flush_opts, FlushReason::kExternalFileIngestion,
            {} /* provided_candidate_cfds */, true /* entered_write_thread */);
        mutex_.Lock();
      } else {
        for (size_t i = 0; i != num_jobs; ++i) {
          if (need_flush[i]) {
            mutex_.Unlock();
            status =
                FlushMemTable(ingestion_jobs[i]->GetColumnFamilyData(),
                              flush_opts, FlushReason::kExternalFileIngestion,
                              true /* entered_write_thread */);
            mutex_.Lock();
            if (!status.ok()) {
              break;
            }
          }
        }
      }
      if (status.ok()) {
        for (size_t i = 0; i != num_jobs; ++i) {
          if (immutable_db_options_.atomic_flush || need_flush[i]) {
            ingestion_jobs[i]->SetFlushedBeforeRun();
          }
        }
      }
    }
    // Run() may decide how to assign sequence numbers based on whether any
    // snapshot exists, and the last sequence number is published before the
    // new SuperVersion is installed. Snapshot creation does not normally take
    // mutex_, so make it fall back to mutex_ until the SuperVersion is
    // installed. A snapshot can still be created while LogAndApply releases
    // mutex_, matching the old serialization.
    const bool snapshots_blocked = status.ok();
    if (snapshots_blocked) {
      snapshots_.BlockNew();
    }
    // Run ingestion jobs.
    if (status.ok()) {
      for (size_t i = 0; i != num_jobs; ++i) {
        mutex_.AssertHeld();
        status = ingestion_jobs[i]->Run();
        if (!status.ok()) {
          break;
        }
        ingestion_jobs[i]->RegisterRange();
      }
    }
    // Now that Run() has assigned the actual seqno for each ingested file,
    // bump each affected memtable's ingest_seqno_barrier_ to that exact
    // value. We still hold the per-CF ingest_sst_lock as a ReadLock; any
    // concurrent conversion's TryWriteLock on the same lock fails, so no
    // converter can be reading or about to read the barrier while we
    // update it. After we explicitly release the ReadLocks before exact
    // snapshot maintenance, the next conversion observes the new barrier and
    // refuses any insert with insert_seq < assigned.
    if (status.ok()) {
      for (auto* job : ingestion_jobs) {
        auto* cfd = job->GetColumnFamilyData();
        SequenceNumber assigned = job->MaxAssignedSequenceNumber();
        if (assigned > 0) {
          cfd->mem()->BumpIngestSeqnoBarrier(assigned);
        }
      }
    }
    if (status.ok()) {
      ReadOptions read_options;
      read_options.fill_cache = fill_cache;
      autovector<ColumnFamilyData*> cfds_to_commit;
      autovector<autovector<VersionEdit*>> edit_lists;
      uint32_t num_entries = 0;
      for (auto* job : ingestion_jobs) {
        auto* cfd = job->GetColumnFamilyData();
        assert(!cfd->IsDropped());
        cfds_to_commit.push_back(cfd);
        autovector<VersionEdit*> edit_list;
        auto* edit = job->edit();
        edit->MarkForegroundOperation();
        edit_list.push_back(edit);
        edit_lists.push_back(edit_list);
        ++num_entries;
      }
      // Mark the version edits as an atomic group if the number of version
      // edits exceeds 1.
      if (cfds_to_commit.size() > 1) {
        for (auto& edits : edit_lists) {
          assert(edits.size() == 1);
          edits[0]->MarkAtomicGroup(--num_entries);
        }
        assert(0 == num_entries);
      }
      assert(snapshots_blocked);
      TEST_SYNC_POINT("DBImpl::IngestExternalFiles:BeforeLogAndApply");
      // LogAndApply releases mutex_ while writing the MANIFEST. GetSnapshot()
      // can acquire it then even though snapshot creation remains blocked from
      // the lock-free path.
      status = versions_->LogAndApply(
          cfds_to_commit, read_options, write_options, edit_lists, &mutex_,
          directories_.GetDbDir(), false /* new_descriptor_log */,
          nullptr /* new_cf_options */, {} /* manifest_wcbs */, {} /* pre_cb */,
          max_file_opening_threads);
      // It is safe to update VersionSet last seqno here after LogAndApply since
      // LogAndApply persists last sequence number from VersionEdits,
      // which are from file's largest seqno and not from VersionSet.
      //
      // It is necessary to update last seqno here since LogAndApply releases
      // mutex when persisting MANIFEST file, and the snapshots taken during
      // that period will not be stable if VersionSet last seqno is updated
      // before LogAndApply.
      SequenceNumber max_assigned_seqno = 0;
      for (auto* job : ingestion_jobs) {
        max_assigned_seqno =
            std::max(max_assigned_seqno, job->MaxAssignedSequenceNumber());
      }
      if (max_assigned_seqno > 0) {
        const SequenceNumber last_seqno = versions_->LastSequence();
        if (max_assigned_seqno > last_seqno) {
          versions_->SetLastAllocatedSequence(max_assigned_seqno);
          versions_->SetLastPublishedSequence(max_assigned_seqno);
          versions_->SetLastSequence(max_assigned_seqno);
        }
      }
      // An iterator whose Version predates the ingestion cannot see the
      // ingested files, whatever sequence number it reads at: files can get
      // no new seqno (seqno zero, ingest_behind, DB-generated files), and an
      // iterator can pair an older SuperVersion with a newer sequence number.
      // Such iterators must not convert tombstones over the ingested files.
      // We still hold the per-CF ingest_sst_lock as a ReadLock, so no
      // conversion runs until the barrier is in place.
      if (status.ok()) {
        for (auto* job : ingestion_jobs) {
          auto* cfd = job->GetColumnFamilyData();
          cfd->mem()->BumpIngestVersionBarrier(
              cfd->current()->GetVersionNumber());
        }
      }
    }

    for (auto* job : ingestion_jobs) {
      job->UnregisterRange();
    }

    if (status.ok()) {
      for (size_t i = 0; i != num_jobs; ++i) {
        auto* cfd = ingestion_jobs[i]->GetColumnFamilyData();
        assert(!cfd->IsDropped());
        InstallSuperVersionAndScheduleWork(cfd, &sv_ctxs[i],
                                           /*defer_snapshot_refresh=*/true);
#ifndef NDEBUG
        if (0 == i && num_jobs > 1) {
          TEST_SYNC_POINT("DBImpl::IngestExternalFiles:InstallSVForFirstCF:0");
          TEST_SYNC_POINT("DBImpl::IngestExternalFiles:InstallSVForFirstCF:1");
        }
#endif  // !NDEBUG
      }
      UpdateSnapshotCompactionThresholds();
    }
    if (snapshots_blocked) {
      snapshots_.UnblockNew();
    }
    if (!status.ok() && versions_->io_status().IsIOError()) {
      // Error while writing to MANIFEST.
      // In fact, versions_->io_status() can also be the result of renaming
      // CURRENT file. With current code, it's just difficult to tell. So just
      // be pessimistic and try write to a new MANIFEST.
      // TODO: distinguish between MANIFEST write and CURRENT renaming
      const IOStatus& io_s = versions_->io_status();
      // Should handle return error?
      error_handler_.SetBGError(io_s, BackgroundErrorReason::kManifestWrite);
    }

    // Resume writes to the DB
    if (two_write_queues_) {
      nonmem_write_thread_.ExitUnbatched(&nonmem_w);
    }
    write_thread_.ExitUnbatched(&w);
    PERF_TIMER_STOP(file_ingestion_blocking_live_writes_nanos);

    if (status.ok()) {
      for (auto* job : ingestion_jobs) {
        job->UpdateStats();
      }
      for (auto* h : hs) {
        ReleaseFileNumberFromPendingOutputs(h->pending_output_elem_);
      }
    }
    num_running_ingest_file_ -= static_cast<int>(num_jobs);
    if (0 == num_running_ingest_file_) {
      bg_cv_.SignalAll();
    }
    TEST_SYNC_POINT("DBImpl::AddFile:MutexUnlock");
  }
  // mutex_ is unlocked here

  // Exact snapshot maintenance acquires every shard lock. Release the
  // arbitrary-size set of per-CF ingestion locks first so sanitizer lock
  // tracking and production lock hold times remain bounded.
  ingest_read_locks.clear();
  if (status.ok()) {
    InstrumentedMutexLock l(&mutex_);
    uint64_t snapshot_release_count = 0;
    const SequenceNumber oldest_snapshot =
        snapshots_.OldestOr([this]() { return GetLastPublishedSequence(); },
                            &snapshot_release_count);
    UpdateOldestSnapshotAndScheduleWork(oldest_snapshot, snapshot_release_count,
                                        /*installed_cfd=*/nullptr);
  }

  for (auto& sv_ctx : sv_ctxs) {
    sv_ctx.Clean();
  }
  if (!status.ok()) {
    // The atomic commit failed; nothing was made visible. The handles are NOT
    // consumed, so each one's destructor rolls it back
    return status;
  }

  for (auto* job : ingestion_jobs) {
    job->Cleanup(status);
    auto* cfd = job->GetColumnFamilyData();
    if (!cfd->IsDropped()) {
      NotifyOnExternalFileIngested(cfd, *job);
    }
  }
  for (auto* h : hs) {
    h->consumed_ = true;
    num_outstanding_prepared_ingestions_.fetch_sub(1);
  }
  // Record commit latency.
  if (record_ingest_micros) {
    RecordTimeToHistogram(
        stats_, INGEST_EXTERNAL_FILE_RUN_TIME,
        immutable_db_options_.clock->NowMicros() - run_start_micros);
  }
  return status;
}

Status DBImpl::IngestExternalFiles(
    const std::vector<IngestExternalFileArg>& args) {
  PERF_TIMER_GUARD(file_ingestion_nanos);
  std::unique_ptr<FileIngestionHandle> handle;
  Status status = PrepareFileIngestion(args, &handle);
  if (!status.ok()) {
    return status;
  }
  return CommitFileIngestionHandle(std::move(handle));
}

Status DBImpl::CreateColumnFamilyWithImport(
    const ColumnFamilyOptions& options, const std::string& column_family_name,
    const ImportColumnFamilyOptions& import_options,
    const std::vector<const ExportImportFilesMetaData*>& metadatas,
    ColumnFamilyHandle** handle) {
  assert(handle != nullptr);
  assert(*handle == nullptr);
  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;
  const WriteOptions write_options;

  std::string cf_comparator_name = options.comparator->Name();

  size_t total_file_num = 0;
  std::vector<std::vector<LiveFileMetaData*>> metadata_files(metadatas.size());
  for (size_t i = 0; i < metadatas.size(); i++) {
    if (cf_comparator_name != metadatas[i]->db_comparator_name) {
      return Status::InvalidArgument("Comparator name mismatch");
    }
    for (auto& file : metadatas[i]->files) {
      metadata_files[i].push_back((LiveFileMetaData*)&file);
    }
    total_file_num += metadatas[i]->files.size();
  }

  // Create column family.
  auto status = CreateColumnFamily(read_options, write_options, options,
                                   column_family_name, handle);
  if (!status.ok()) {
    return status;
  }

  // Import sst files from metadata.
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(*handle);
  auto cfd = cfh->cfd();
  ImportColumnFamilyJob import_job(versions_.get(), cfd, immutable_db_options_,
                                   file_options_, import_options,
                                   metadata_files, io_tracer_);

  SuperVersionContext dummy_sv_ctx(/* create_superversion */ true);
  VersionEdit dummy_edit;
  dummy_edit.MarkForegroundOperation();
  uint64_t next_file_number = 0;
  std::unique_ptr<std::list<uint64_t>::iterator> pending_output_elem;
  {
    // Lock db mutex
    InstrumentedMutexLock l(&mutex_);
    if (error_handler_.IsDBStopped()) {
      // Don't import files when there is a bg_error
      status = error_handler_.GetBGError();
    }

    // Make sure that bg cleanup wont delete the files that we are importing
    pending_output_elem.reset(new std::list<uint64_t>::iterator(
        CaptureCurrentFileNumberInPendingOutputs()));

    if (status.ok()) {
      // If crash happen after a hard link established, Recover function may
      // reuse the file number that has already assigned to the internal file,
      // and this will overwrite the external file. To protect the external
      // file, we have to make sure the file number will never being reused.
      next_file_number = versions_->FetchAddFileNumber(total_file_num);
      status =
          versions_->LogAndApply(cfd, read_options, write_options, &dummy_edit,
                                 &mutex_, directories_.GetDbDir());
      if (status.ok()) {
        InstallSuperVersionForConfigChange(cfd, &dummy_sv_ctx);
      }
    }
  }
  dummy_sv_ctx.Clean();

  if (status.ok()) {
    SuperVersion* sv = cfd->GetReferencedSuperVersion(this);
    status = import_job.Prepare(next_file_number, sv);
    CleanupSuperVersion(sv);
  }

  if (status.ok()) {
    SuperVersionContext sv_context(true /*create_superversion*/);
    {
      // Lock db mutex
      InstrumentedMutexLock l(&mutex_);

      // Stop writes to the DB by entering both write threads
      WriteThread::Writer w;
      write_thread_.EnterUnbatched(&w, &mutex_);
      WriteThread::Writer nonmem_w;
      if (two_write_queues_) {
        nonmem_write_thread_.EnterUnbatched(&nonmem_w, &mutex_);
      }

      num_running_ingest_file_++;
      assert(!cfd->IsDropped());
      mutex_.AssertHeld();
      status = import_job.Run();

      // Install job edit [Mutex will be unlocked here]
      if (status.ok()) {
        auto* edit = import_job.edit();
        edit->MarkForegroundOperation();
        status = versions_->LogAndApply(cfd, read_options, write_options, edit,
                                        &mutex_, directories_.GetDbDir());
        if (status.ok()) {
          InstallSuperVersionForConfigChange(cfd, &sv_context);
        }
      }

      // Resume writes to the DB
      if (two_write_queues_) {
        nonmem_write_thread_.ExitUnbatched(&nonmem_w);
      }
      write_thread_.ExitUnbatched(&w);

      num_running_ingest_file_--;
      if (num_running_ingest_file_ == 0) {
        bg_cv_.SignalAll();
      }
    }
    // mutex_ is unlocked here

    sv_context.Clean();
  }

  {
    InstrumentedMutexLock l(&mutex_);
    ReleaseFileNumberFromPendingOutputs(pending_output_elem);
  }

  import_job.Cleanup(status);
  if (!status.ok()) {
    Status temp_s = DropColumnFamily(*handle);
    if (!temp_s.ok()) {
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "DropColumnFamily failed with error %s",
                      temp_s.ToString().c_str());
    }
    // Always returns Status::OK()
    temp_s = DestroyColumnFamilyHandle(*handle);
    assert(temp_s.ok());
    *handle = nullptr;
  }
  return status;
}

Status DBImpl::ClipColumnFamily(ColumnFamilyHandle* column_family,
                                const Slice& begin_key, const Slice& end_key) {
  assert(column_family);
  Status status;
  // Flush memtable
  FlushOptions flush_opts;
  flush_opts.allow_write_stall = true;
  auto* cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(column_family)->cfd();
  if (immutable_db_options_.atomic_flush) {
    status = AtomicFlushMemTables(flush_opts, FlushReason::kDeleteFiles,
                                  {} /* provided_candidate_cfds */,
                                  false /* entered_write_thread */);
  } else {
    status = FlushMemTable(cfd, flush_opts, FlushReason::kDeleteFiles,
                           false /* entered_write_thread */);
  }

  if (status.ok()) {
    // DeleteFilesInRanges non-overlap files except L0
    std::vector<RangeOpt> ranges;
    ranges.emplace_back(OptSlice{}, begin_key);
    ranges.emplace_back(end_key, OptSlice{});
    // begin_key is kept, so a file whose largest key is begin_key must stay.
    status = DeleteFilesInRanges(column_family, ranges.data(), ranges.size(),
                                 false /* include_end */);
  }

  // DeleteRange the remaining overlapping keys
  bool empty_after_delete = false;
  if (status.ok()) {
    // GetSstFilesBoundaryKeys returns Slices into the current Version's
    // FileMetaData. Copy them out while holding the mutex: the deletes below
    // drop the mutex and each is a full write, during which a background
    // compaction can install a new Version and free that metadata.
    std::string smallest_buf;
    std::string largest_buf;
    {
      // Lock db mutex
      InstrumentedMutexLock l(&mutex_);
      empty_after_delete =
          cfd->current()->storage_info()->num_non_empty_levels() == 0;
      if (!empty_after_delete) {
        Slice smallest_slice;
        Slice largest_slice;
        cfd->current()->GetSstFilesBoundaryKeys(&smallest_slice,
                                                &largest_slice);
        smallest_buf.assign(smallest_slice.data(), smallest_slice.size());
        largest_buf.assign(largest_slice.data(), largest_slice.size());
      }
    }
    Slice smallest_user_key(smallest_buf);
    Slice largest_user_key(largest_buf);
    // Opens the window in which the current Version (and the FileMetaData the
    // boundary keys were read from) can be replaced and freed by a background
    // job before the deletes below run.
    TEST_SYNC_POINT("DBImpl::ClipColumnFamily:PostBoundaryKeys");
    if (!empty_after_delete) {
      const Comparator* const ucmp = column_family->GetComparator();
      // TODO: plumb Env::IOActivity, Env::IOPriority
      WriteOptions wo;
      // Delete [smallest_user_key, clip_begin_key)
      if (ucmp->Compare(smallest_user_key, begin_key) < 0) {
        status = DeleteRange(wo, column_family, smallest_user_key, begin_key);
      }

      if (status.ok()) {
        // Delete [clip_end_key, largest_use_key]
        if (ucmp->Compare(end_key, largest_user_key) <= 0) {
          status = DeleteRange(wo, column_family, end_key, largest_user_key);
          if (status.ok()) {
            status = Delete(wo, column_family, largest_user_key);
          }
        }
      }
    }
  }

  if (status.ok() && !empty_after_delete) {
    // CompactRange delete all the tombstones
    CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = true;
    compact_options.bottommost_level_compaction =
        BottommostLevelCompaction::kForceOptimized;
    // We could just compact the ranges [null, clip_begin_key] and
    // [clip_end_key, null]. But due to how manual compaction calculates the
    // last level to compact to and that range tombstones are not dropped
    // during non-bottommost compactions, calling CompactRange() on these two
    // ranges may not clear all range tombstones.
    status = CompactRange(compact_options, column_family, nullptr, nullptr);
  }
  return status;
}

Status DBImpl::VerifyFileChecksums(const ReadOptions& _read_options) {
  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kVerifyFileChecksums) {
    return Status::InvalidArgument(
        "Can only call VerifyFileChecksums with `ReadOptions::io_activity` is "
        "`Env::IOActivity::kUnknown` or "
        "`Env::IOActivity::kVerifyFileChecksums`");
  }
  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kVerifyFileChecksums;
  }
  return VerifyChecksumInternal(read_options,
                                /*use_file_checksum=*/true);
}

Status DBImpl::VerifyChecksum(const ReadOptions& _read_options) {
  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kVerifyDBChecksum) {
    return Status::InvalidArgument(
        "Can only call VerifyChecksum with `ReadOptions::io_activity` is "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kVerifyDBChecksum`");
  }
  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kVerifyDBChecksum;
  }
  return VerifyChecksumInternal(read_options,
                                /*use_file_checksum=*/false);
}

Status DBImpl::VerifyChecksumInternal(const ReadOptions& read_options,
                                      bool use_file_checksum) {
  // `bytes_read` stat is enabled based on compile-time support and cannot
  // be dynamically toggled. So we do not need to worry about `PerfLevel`
  // here, unlike many other `IOStatsContext` / `PerfContext` stats.
  uint64_t prev_bytes_read = IOSTATS(bytes_read);

  Status s;

  if (use_file_checksum) {
    FileChecksumGenFactory* const file_checksum_gen_factory =
        immutable_db_options_.file_checksum_gen_factory.get();
    if (!file_checksum_gen_factory) {
      s = Status::InvalidArgument(
          "Cannot verify file checksum if options.file_checksum_gen_factory is "
          "null");
      return s;
    }
  }
  // FIXME? What does it mean if read_options.verify_checksums == false?

  // TODO: simplify using GetRefedColumnFamilySet?
  std::vector<ColumnFamilyData*> cfd_list;
  {
    InstrumentedMutexLock l(&mutex_);
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      if (!cfd->IsDropped() && cfd->initialized()) {
        cfd->Ref();
        cfd_list.push_back(cfd);
      }
    }
  }
  std::vector<SuperVersion*> sv_list;
  for (auto cfd : cfd_list) {
    sv_list.push_back(cfd->GetReferencedSuperVersion(this));
  }

  for (auto& sv : sv_list) {
    VersionStorageInfo* vstorage = sv->current->storage_info();
    ColumnFamilyData* cfd = sv->current->cfd();
    Options opts;
    if (!use_file_checksum) {
      InstrumentedMutexLock l(&mutex_);
      opts = Options(BuildDBOptions(immutable_db_options_, mutable_db_options_),
                     cfd->GetLatestCFOptions());
    }
    for (int i = 0; i < vstorage->num_non_empty_levels() && s.ok(); i++) {
      for (size_t j = 0; j < vstorage->LevelFilesBrief(i).num_files && s.ok();
           j++) {
        const auto& fd_with_krange = vstorage->LevelFilesBrief(i).files[j];
        const auto& fd = fd_with_krange.fd;
        const FileMetaData* fmeta = fd_with_krange.file_metadata;
        assert(fmeta);
        std::string fname = TableFileName(cfd->ioptions().cf_paths,
                                          fd.GetNumber(), fd.GetPathId());
        if (use_file_checksum) {
          s = VerifyFullFileChecksum(fmeta->file_checksum,
                                     fmeta->file_checksum_func_name, fname,
                                     read_options);
        } else {
          FileOptions fopts = file_options_;
          fopts.file_checksum = fmeta->file_checksum;
          fopts.file_checksum_func_name = fmeta->file_checksum_func_name;
          s = ROCKSDB_NAMESPACE::VerifySstFileChecksumInternal(
              opts, fopts, read_options, fname, fd.largest_seqno);
        }
        RecordTick(stats_, VERIFY_CHECKSUM_READ_BYTES,
                   IOSTATS(bytes_read) - prev_bytes_read);
        prev_bytes_read = IOSTATS(bytes_read);
      }
    }

    if (s.ok() && use_file_checksum) {
      const auto& blob_files = vstorage->GetBlobFiles();
      for (const auto& meta : blob_files) {
        assert(meta);

        const uint64_t blob_file_number = meta->GetBlobFileNumber();

        const std::string blob_file_name = BlobFileName(
            cfd->ioptions().cf_paths.front().path, blob_file_number);
        s = VerifyFullFileChecksum(meta->GetChecksumValue(),
                                   meta->GetChecksumMethod(), blob_file_name,
                                   read_options);
        RecordTick(stats_, VERIFY_CHECKSUM_READ_BYTES,
                   IOSTATS(bytes_read) - prev_bytes_read);
        prev_bytes_read = IOSTATS(bytes_read);
        if (!s.ok()) {
          break;
        }
      }
    }
    if (!s.ok()) {
      break;
    }
  }

  bool defer_purge = immutable_db_options().avoid_unnecessary_blocking_io;
  {
    InstrumentedMutexLock l(&mutex_);
    for (auto sv : sv_list) {
      if (sv && sv->Unref()) {
        sv->Cleanup();
        if (defer_purge) {
          AddSuperVersionsToFreeQueue(sv);
        } else {
          delete sv;
        }
      }
    }
    if (defer_purge) {
      SchedulePurge();
    }
    for (auto cfd : cfd_list) {
      cfd->UnrefAndTryDelete();
    }
  }
  RecordTick(stats_, VERIFY_CHECKSUM_READ_BYTES,
             IOSTATS(bytes_read) - prev_bytes_read);
  return s;
}

Status DBImpl::VerifyFullFileChecksum(const std::string& file_checksum_expected,
                                      const std::string& func_name_expected,
                                      const std::string& fname,
                                      const ReadOptions& read_options) {
  Status s;
  if (file_checksum_expected == kUnknownFileChecksum) {
    return s;
  }
  std::string file_checksum;
  std::string func_name;
  FileOptions fopts;
  fopts.file_checksum = file_checksum_expected;
  fopts.file_checksum_func_name = func_name_expected;
  s = ROCKSDB_NAMESPACE::GenerateOneFileChecksum(
      fs_.get(), fname, immutable_db_options_.file_checksum_gen_factory.get(),
      func_name_expected, &file_checksum, &func_name,
      read_options.readahead_size, immutable_db_options_.allow_mmap_reads,
      io_tracer_, immutable_db_options_.rate_limiter.get(), read_options,
      immutable_db_options_.stats, immutable_db_options_.clock, fopts);
  if (s.ok()) {
    assert(func_name_expected == func_name);
    if (file_checksum != file_checksum_expected) {
      std::ostringstream oss;
      oss << fname << " file checksum mismatch, ";
      oss << "expecting "
          << Slice(file_checksum_expected).ToString(/*hex=*/true);
      oss << ", but actual " << Slice(file_checksum).ToString(/*hex=*/true);
      s = Status::Corruption(oss.str());
      TEST_SYNC_POINT_CALLBACK("DBImpl::VerifyFullFileChecksum:mismatch", &s);
    }
  }
  return s;
}

void DBImpl::NotifyOnExternalFileIngested(
    ColumnFamilyData* cfd, const ExternalSstFileIngestionJob& ingestion_job) {
  if (immutable_db_options_.listeners.empty()) {
    return;
  }

  for (const IngestedFileInfo& f : ingestion_job.files_to_ingest()) {
    ExternalFileIngestionInfo info;
    info.cf_name = cfd->GetName();
    info.external_file_path = f.external_file_path;
    info.internal_file_path = f.internal_file_path;
    info.global_seqno = f.assigned_seqno;
    info.table_properties = f.table_properties;
    for (const auto& listener : immutable_db_options_.listeners) {
      listener->OnExternalFileIngested(this, info);
    }
  }
}

Status DBImpl::StartTrace(const TraceOptions& trace_options,
                          std::unique_ptr<TraceWriter>&& trace_writer) {
  InstrumentedMutexLock lock(&trace_mutex_);
  tracer_.reset(new Tracer(immutable_db_options_.clock, trace_options,
                           std::move(trace_writer)));
  return Status::OK();
}

Status DBImpl::EndTrace() {
  InstrumentedMutexLock lock(&trace_mutex_);
  Status s;
  if (tracer_ != nullptr) {
    s = tracer_->Close();
    tracer_.reset();
  } else {
    s = Status::IOError("No trace file to close");
  }
  return s;
}

Status DBImpl::NewDefaultReplayer(
    const std::vector<ColumnFamilyHandle*>& handles,
    std::unique_ptr<TraceReader>&& reader,
    std::unique_ptr<Replayer>* replayer) {
  replayer->reset(new ReplayerImpl(this, handles, std::move(reader)));
  return Status::OK();
}

Status DBImpl::StartBlockCacheTrace(
    const TraceOptions& trace_options,
    std::unique_ptr<TraceWriter>&& trace_writer) {
  BlockCacheTraceOptions block_trace_opts;
  block_trace_opts.sampling_frequency = trace_options.sampling_frequency;

  BlockCacheTraceWriterOptions trace_writer_opt;
  trace_writer_opt.max_trace_file_size = trace_options.max_trace_file_size;

  std::unique_ptr<BlockCacheTraceWriter> block_cache_trace_writer =
      NewBlockCacheTraceWriter(env_->GetSystemClock().get(), trace_writer_opt,
                               std::move(trace_writer));

  return block_cache_tracer_.StartTrace(block_trace_opts,
                                        std::move(block_cache_trace_writer));
}

Status DBImpl::StartBlockCacheTrace(
    const BlockCacheTraceOptions& trace_options,
    std::unique_ptr<BlockCacheTraceWriter>&& trace_writer) {
  return block_cache_tracer_.StartTrace(trace_options, std::move(trace_writer));
}

Status DBImpl::EndBlockCacheTrace() {
  block_cache_tracer_.EndTrace();
  return Status::OK();
}

Status DBImpl::TraceIteratorSeek(const uint32_t& cf_id, const Slice& key,
                                 const Slice& lower_bound,
                                 const Slice upper_bound,
                                 const TracedReadOptions& read_options) {
  Status s;
  if (tracer_) {
    InstrumentedMutexLock lock(&trace_mutex_);
    if (tracer_) {
      s = tracer_->IteratorSeek(cf_id, key, lower_bound, upper_bound,
                                read_options);
    }
  }
  return s;
}

Status DBImpl::TraceIteratorSeekForPrev(const uint32_t& cf_id, const Slice& key,
                                        const Slice& lower_bound,
                                        const Slice upper_bound,
                                        const TracedReadOptions& read_options) {
  Status s;
  if (tracer_) {
    InstrumentedMutexLock lock(&trace_mutex_);
    if (tracer_) {
      s = tracer_->IteratorSeekForPrev(cf_id, key, lower_bound, upper_bound,
                                       read_options);
    }
  }
  return s;
}

Status DBImpl::ReserveFileNumbersBeforeIngestion(
    ColumnFamilyData* cfd, uint64_t num,
    std::unique_ptr<std::list<uint64_t>::iterator>& pending_output_elem,
    uint64_t* next_file_number) {
  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;
  const WriteOptions write_options;

  Status s;
  SuperVersionContext dummy_sv_ctx(true /* create_superversion */);
  assert(nullptr != next_file_number);
  InstrumentedMutexLock l(&mutex_);
  if (error_handler_.IsDBStopped()) {
    // Do not ingest files when there is a bg_error
    return error_handler_.GetBGError();
  }
  pending_output_elem.reset(new std::list<uint64_t>::iterator(
      CaptureCurrentFileNumberInPendingOutputs()));
  *next_file_number = versions_->FetchAddFileNumber(static_cast<uint64_t>(num));
  VersionEdit dummy_edit;
  // If crash happen after a hard link established, Recover function may
  // reuse the file number that has already assigned to the internal file,
  // and this will overwrite the external file. To protect the external
  // file, we have to make sure the file number will never being reused.
  dummy_edit.MarkForegroundOperation();
  s = versions_->LogAndApply(cfd, read_options, write_options, &dummy_edit,
                             &mutex_, directories_.GetDbDir());
  if (s.ok()) {
    InstallSuperVersionAndScheduleWork(cfd, &dummy_sv_ctx,
                                       /*defer_snapshot_refresh=*/false);
  }
  dummy_sv_ctx.Clean();
  return s;
}

Status DBImpl::GetCreationTimeOfOldestFile(uint64_t* creation_time) {
  if (mutable_db_options_.max_open_files == -1) {
    uint64_t oldest_time = std::numeric_limits<uint64_t>::max();
    std::once_flag waited_for_async_open;
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      if (!cfd->IsDropped()) {
        uint64_t ctime;
        {
          SuperVersion* sv = GetAndRefSuperVersion(cfd);
          Version* version = sv->current;
          version->GetCreationTimeOfOldestFile(&ctime);
          // For modern DBs, manifest carries file_creation_time and the
          // first call returns the real value. We only need to wait for
          // BGWorkAsyncFileOpen on legacy DBs whose manifest lacks
          // file_creation_time -- those rely on the pinned reader, which is
          // null until async file open completes.
          if (ctime == 0 && immutable_db_options_.open_files_async) {
            std::call_once(waited_for_async_open, [&]() {
              WaitForAsyncFileOpen();
              version->GetCreationTimeOfOldestFile(&ctime);
            });
          }
          ReturnAndCleanupSuperVersion(cfd, sv);
        }

        if (ctime < oldest_time) {
          oldest_time = ctime;
        }
        if (oldest_time == 0) {
          break;
        }
      }
    }
    *creation_time = oldest_time;
    return Status::OK();
  } else {
    return Status::NotSupported("This API only works if max_open_files = -1");
  }
}

std::pair<SequenceNumber, uint64_t> DBImpl::GetSeqnoToTimeSample() const {
  // TECHNICALITY: Sample last sequence number *before* time, as prescribed
  // for SeqnoToTimeMapping. We don't know how long it has been since the last
  // sequence number was written, so we at least have a one-sided bound by
  // sampling in this order.
  // ALSO, to avoid out-of-order mappings, we need to get the seqno and times
  // while holding the DB mutex. (This is really to make testing happy because
  // it's fine to throw out extra close-but-not-quite-consistent mappings in
  // production.)
  mutex_.AssertHeld();
  SequenceNumber seqno = GetLatestSequenceNumber();
  // HACK/TODO: seqno might be zero but we can't record a mapping for that.
  // Start with 1, which should be close enough.
  seqno = std::max(seqno, SequenceNumber{1});
  int64_t unix_time_signed = 0;
  immutable_db_options_.clock->GetCurrentTime(&unix_time_signed)
      .PermitUncheckedError();  // Ignore error
  return {seqno, static_cast<uint64_t>(unix_time_signed)};
}

void DBImpl::EnsureSeqnoToTimeMapping(
    const MinAndMaxPreserveSeconds& preserve_info) {
  mutex_.AssertHeld();
  assert(preserve_info.IsEnabled());

  // Atomically with CF creation or mutable option change (see
  // InstallSuperVersionForConfigChange()), we need to be sure any data written
  // after setting preserve/preclude options must have a reasonable time
  // estimate (so that we can accurately place the data), which means at least
  // one entry in seqno_to_time_mapping_. It's not critical that `preserve_info`
  // take into account all CFs, as that's mostly relevant to how we add
  // recurring entries and purge old ones.

  auto [seqno, unix_time_now] = GetSeqnoToTimeSample();
  // Ensure at least one sample that is sufficiently recent
  uint64_t unix_time_last_sample = 0;
  if (seqno_to_time_mapping_.Empty()) {
    // The exact best settings will be found and applied in
    // RegisterRecordSeqnoTimeWorker()
    seqno_to_time_mapping_.SetCapacity(kMaxSeqnoToTimeEntries);
  } else {
    unix_time_last_sample =
        seqno_to_time_mapping_.GetProximalTimeBeforeSeqno(kMaxSequenceNumber);
  }
  uint64_t cadence = preserve_info.GetRecodingCadence();
  // Extend cadence so as to avoid stepping on toes of recorder job, which
  // could lag a bit.
  cadence += 3 + cadence / 100;
  if (unix_time_now >= cadence &&
      unix_time_last_sample <= unix_time_now - cadence) {
    assert(seqno > 0);  // See GetSeqnoToTimeSample()
    // Always successful assuming seqno never go backwards
    seqno_to_time_mapping_.Append(seqno, unix_time_now);
  }
}

void DBImpl::PrepopulateSeqnoToTimeMapping(
    const MinAndMaxPreserveSeconds& preserve_info) {
  // Only for opening a new DB, with preserve/preclude options set
  if (!preserve_info.IsEnabled()) {
    assert(false);
    return;
  }
  if (GetLatestSequenceNumber() != 0) {
    assert(false);
    return;
  }

  // Here we fulfill the following promise:
  //
  // Any DB/CF created with preserve/preclude options set from the beginning
  // will get pre-allocated seqnos with pre-populated time mappings back to
  // the times we are interested in. (This will enable future import of data
  // while preserving rough write time. We can only do this reliably from
  // DB::Open, as otherwise there could be a race between CreateColumnFamily
  // and the first Write to the DB, and seqno-to-time mappings need to be
  // monotonic.
  //
  // FIXME: We don't currently guarantee that if the first column family with
  // that setting is added or configured after initial DB::Open but before
  // the first user Write. Fixing this causes complications with the crash
  // test because if DB starts without preserve/preclude option, does some
  // user writes but all those writes are lost in crash, then re-opens with
  // preserve/preclude option, it sees seqno==1 which looks like one of the
  // user writes was recovered, when actually it was not.

  // Pre-allocate seqnos and pre-populate historical mapping
  // We can simply modify these, before writes are allowed
  constexpr uint64_t kMax = kMaxSeqnoTimePairsPerSST;
  versions_->SetLastAllocatedSequence(kMax);
  versions_->SetLastPublishedSequence(kMax);
  versions_->SetLastSequence(kMax);

  // And record in manifest, to avoid going backwards in seqno on re-open
  // (potentially with different options). Concurrency is simple because we
  // are in DB::Open
  const WriteOptions write_options(Env::IOActivity::kDBOpen);
  const ReadOptions read_options(Env::IOActivity::kDBOpen);
  VersionEdit edit;
  edit.SetLastSequence(kMax);
  Status s = versions_->LogAndApplyToDefaultColumnFamily(
      read_options, write_options, &edit, &mutex_, directories_.GetDbDir());
  if (!s.ok() && versions_->io_status().IsIOError()) {
    error_handler_.SetBGError(versions_->io_status(),
                              BackgroundErrorReason::kManifestWrite);
  }

  auto [seqno, unix_time_now] = GetSeqnoToTimeSample();
  uint64_t populate_historical_seconds = preserve_info.max_preserve_seconds;
  if (seqno > 1 && unix_time_now > populate_historical_seconds) {
    // seqno=0 is reserved
    SequenceNumber from_seqno = 1;
    seqno_to_time_mapping_.PrePopulate(
        from_seqno, seqno, unix_time_now - populate_historical_seconds,
        unix_time_now);
  } else {
    // One of these will fail
    assert(seqno > 1);
    assert(unix_time_now > populate_historical_seconds);
  }
}

void DBImpl::InstallSuperVersionForConfigChange(
    ColumnFamilyData* cfd, SuperVersionContext* sv_context) {
  MinAndMaxPreserveSeconds preserve_info{cfd->GetLatestCFOptions()};
  std::shared_ptr<SeqnoToTimeMapping> new_seqno_to_time_mapping;
  if (preserve_info.IsEnabled()) {
    // TODO: detect & optimize if mapping hasn't changed from previous
    // SuperVersion
    EnsureSeqnoToTimeMapping(preserve_info);
    new_seqno_to_time_mapping = std::make_shared<SeqnoToTimeMapping>();
    new_seqno_to_time_mapping->CopyFrom(seqno_to_time_mapping_);
  }
  InstallSuperVersionAndScheduleWork(cfd, sv_context,
                                     /*defer_snapshot_refresh=*/false,
                                     std::move(new_seqno_to_time_mapping));
}

void DBImpl::RecordSeqnoToTimeMapping() {
  SuperVersionContext sv_context;
  {
    InstrumentedMutexLock l(&mutex_);
    // Record next sample
    seqno_to_time_mapping_.Append(GetSeqnoToTimeSample());
    // Create an immutable snapshot for sharing across CFs
    std::shared_ptr<SeqnoToTimeMapping> new_seqno_to_time_mapping =
        std::make_shared<SeqnoToTimeMapping>();
    new_seqno_to_time_mapping->CopyFrom(seqno_to_time_mapping_);

    // Update in SV of all applicable CFs
    bool enqueued_any = false;
    for (ColumnFamilyData* cfd : *versions_->GetColumnFamilySet()) {
      if (cfd->IsDropped()) {
        continue;
      }
      MinAndMaxPreserveSeconds preserve_info{cfd->GetLatestCFOptions()};
      if (preserve_info.IsEnabled()) {
        sv_context.NewSuperVersion();
        cfd->InstallSuperVersion(&sv_context, &mutex_,
                                 new_seqno_to_time_mapping);
        // Recording a new sample can move the preserve-window boundary, aging a
        // bottommost file out of it. When that happens, recompute bottommost
        // marking and enqueue now: on a quiet DB nothing else would create a
        // new Version to pick up the change, so the compaction would otherwise
        // be delayed until unrelated activity.
        if (MaybeUpdatePreserveTimeMinSeqno(cfd) && !cfd->AllowIngestBehind()) {
          VersionStorageInfo* vstorage = cfd->current()->storage_info();
          vstorage->ComputeBottommostFilesMarkedForCompaction(
              /*allow_ingest_behind=*/false, cfd->ioptions().user_comparator,
              cfd->GetFullHistoryTsLow());
          if (!vstorage->BottommostFilesMarkedForCompaction().empty()) {
            EnqueuePendingCompaction(cfd);
            enqueued_any = true;
          }
        }
      }
    }
    if (enqueued_any) {
      MaybeScheduleFlushOrCompaction();
    }
    bg_cv_.SignalAll();
  }

  // clean up & report outside db mutex
  sv_context.Clean();
}

bool DBImpl::MaybeUpdatePreserveTimeMinSeqno(ColumnFamilyData* cfd) {
  mutex_.AssertHeld();
  VersionStorageInfo* vstorage = cfd->current()->storage_info();
  const SequenceNumber prev = vstorage->GetPreserveTimeMinSeqno();
  const MutableCFOptions& mopts = cfd->GetLatestMutableCFOptions();
  MinAndMaxPreserveSeconds preserve_info{mopts};
  if (!preserve_info.IsEnabled()) {
    // Preserve/preclude disabled: no restriction on bottommost seqno zeroing.
    vstorage->SetPreserveTimeMinSeqno(kMaxSequenceNumber);
    return prev != kMaxSequenceNumber;
  }
  int64_t current_time = 0;
  if (!immutable_db_options_.clock->GetCurrentTime(&current_time).ok()) {
    // Leave the previous value in place; being stale is safe (a hot key's
    // largest seqno stays above any past boundary and remains unmarked).
    return false;
  }
  SequenceNumber preserve_time_min_seqno = kMaxSequenceNumber;
  seqno_to_time_mapping_.GetCurrentTieringCutoffSeqnos(
      static_cast<uint64_t>(current_time), mopts.preserve_internal_time_seconds,
      mopts.preclude_last_level_data_seconds, &preserve_time_min_seqno,
      /*preclude_last_level_min_seqno=*/nullptr);
  vstorage->SetPreserveTimeMinSeqno(preserve_time_min_seqno);
  return prev != preserve_time_min_seqno;
}

void DBImpl::TriggerPeriodicCompaction() {
  TEST_SYNC_POINT("DBImpl::TriggerPeriodicCompaction:StartRunning");
  {
    InstrumentedMutexLock l(&mutex_);
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Running the periodic task to trigger compactions.");

    uint64_t snapshot_release_count = 0;
    const SequenceNumber oldest_snapshot =
        snapshots_.OldestOr([this]() { return GetLastPublishedSequence(); },
                            &snapshot_release_count);
    UpdateOldestSnapshotAndScheduleWork(oldest_snapshot, snapshot_release_count,
                                        /*installed_cfd=*/nullptr);

    for (ColumnFamilyData* cfd : *versions_->GetColumnFamilySet()) {
      if (cfd->IsDropped()) {
        continue;
      }
      if (cfd->queued_for_compaction()) {
        continue;
      }
      // Check if this CF has any time-based or read-triggered compaction
      // configured. This includes periodic_compaction_seconds, ttl, FIFO
      // temperature thresholds, or read_triggered_compaction_threshold.
      if (GetMinTimeBasedCompactionInterval(cfd->GetLatestCFOptions()) > 0 ||
          cfd->GetLatestMutableCFOptions().read_triggered_compaction_threshold >
              0.0) {
        TEST_SYNC_POINT_CALLBACK(
            "DBImpl::TriggerPeriodicCompaction:BeforeComputeCompactionScore",
            cfd);
        cfd->current()->storage_info()->ComputeCompactionScore(
            cfd->ioptions(), cfd->GetLatestMutableCFOptions(),
            cfd->GetFullHistoryTsLow());
        EnqueuePendingCompaction(cfd);
        if (cfd->queued_for_compaction()) {
          ROCKS_LOG_INFO(immutable_db_options_.info_log,
                         "Periodic task to trigger compaction queued Column "
                         "family [%s] for compaction.",
                         cfd->GetName().c_str());
        }
      }
    }
    MaybeScheduleFlushOrCompaction();
    bg_cv_.SignalAll();
  }
}

namespace {

void ResetThreadLocalStatsForAsyncRead() {
#ifndef NPERF_CONTEXT
  get_perf_context()->Reset();
#endif
#ifndef NIOSTATS_CONTEXT
  get_iostats_context()->Reset();
#endif
}

bool ThreadLocalStatsEnabledForAsyncRead() {
#ifndef NPERF_CONTEXT
  if (GetPerfLevel() != PerfLevel::kDisable) {
    return true;
  }
#endif
#ifndef NIOSTATS_CONTEXT
  if (!get_iostats_context()->disable_iostats) {
    return true;
  }
#endif
  return false;
}

void DisableThreadLocalStatsForAsyncRead() {
#ifndef NIOSTATS_CONTEXT
  get_iostats_context()->disable_iostats = true;
#endif
  SetPerfLevel(PerfLevel::kDisable);
  SetAsyncReadCPUTimeEnabled(false);
}

class AsyncReadStatsScope {
 public:
  AsyncReadStatsScope() {
    if (ThreadLocalStatsEnabledForAsyncRead()) {
      ResetThreadLocalStatsForAsyncRead();
    }
  }

  ~AsyncReadStatsScope() { DisableThreadLocalStatsForAsyncRead(); }

  AsyncReadStatsScope(const AsyncReadStatsScope&) = delete;
  AsyncReadStatsScope& operator=(const AsyncReadStatsScope&) = delete;
  AsyncReadStatsScope(AsyncReadStatsScope&&) = delete;
  AsyncReadStatsScope& operator=(AsyncReadStatsScope&&) = delete;
};

}  // namespace

void DB::GetAsync(const ReadOptions& options, ColumnFamilyHandle* column_family,
                  const Slice& key, PinnableSlice* value,
                  std::string* timestamp, Status& status,
                  AsyncCallback& callback) {
#if USE_COROUTINES
  CoroDB* coro_db = GetCoroDB();
  if (coro_db != nullptr) {
    auto* read_executor = GetFileSystem()->GetReadExecutor();
    if (read_executor != nullptr) {
      auto* read_event_base = read_executor->getEventBase();
      assert(read_event_base != nullptr);
      auto stats_config = CaptureAndDisableCoroutineStatsConfig();
      auto task = [](CoroutineStatsConfig task_stats_config, CoroDB* task_db,
                     ReadOptions task_options,
                     ColumnFamilyHandle* task_column_family, Slice task_key,
                     PinnableSlice* task_value, std::string* task_timestamp,
                     Status& task_status, Env* task_env,
                     AsyncCallback& task_callback) -> folly::coro::Task<void> {
        {
          CoroutineStatsContextScope stats_scope(std::move(task_stats_config),
                                                 task_env);
          task_status = co_await folly::coro::co_nothrow(
              task_db->GetCoroutine(task_options, task_column_family, task_key,
                                    task_value, task_timestamp));
        }

        task_callback.OnComplete();
      }(std::move(stats_config), coro_db, options, column_family, key, value,
                                                   timestamp, status, GetEnv(),
                                                   callback);
      folly::coro::co_withExecutor(
          folly::Executor::getKeepAliveToken(read_event_base), std::move(task))
          .start();
      return;
    }
  }
#endif  // USE_COROUTINES

  {
    AsyncReadStatsScope stats_scope;
    status = Get(options, column_family, key, value, timestamp);
  }
  callback.OnComplete();
}

void DB::MultiGetAsync(const ReadOptions& options, const size_t num_keys,
                       ColumnFamilyHandle** column_families, const Slice* keys,
                       PinnableSlice* values, std::string* timestamps,
                       Status* statuses, const bool sorted_input,
                       AsyncCallback& callback) {
#if USE_COROUTINES
  CoroDB* coro_db = GetCoroDB();
  if (coro_db != nullptr) {
    auto* read_executor = GetFileSystem()->GetReadExecutor();
    if (read_executor != nullptr) {
      auto* read_event_base = read_executor->getEventBase();
      assert(read_event_base != nullptr);
      auto stats_config = CaptureAndDisableCoroutineStatsConfig();
      auto task =
          [](CoroutineStatsConfig task_stats_config, CoroDB* task_db,
             ReadOptions task_options, size_t task_num_keys,
             ColumnFamilyHandle** task_column_families, const Slice* task_keys,
             PinnableSlice* task_values, std::string* task_timestamps,
             Status* task_statuses, bool task_sorted_input, Env* task_env,
             AsyncCallback& task_callback) -> folly::coro::Task<void> {
        {
          CoroutineStatsContextScope stats_scope(std::move(task_stats_config),
                                                 task_env);
          co_await folly::coro::co_nothrow(task_db->MultiGetCoroutine(
              task_options, task_num_keys, task_column_families, task_keys,
              task_values, task_timestamps, task_statuses, task_sorted_input));
        }

        task_callback.OnComplete();
      }(std::move(stats_config), coro_db, options, num_keys, column_families,
                                           keys, values, timestamps, statuses,
                                           sorted_input, GetEnv(), callback);
      folly::coro::co_withExecutor(
          folly::Executor::getKeepAliveToken(read_event_base), std::move(task))
          .start();
      return;
    }
  }
#endif  // USE_COROUTINES

  {
    AsyncReadStatsScope stats_scope;
    MultiGet(options, num_keys, column_families, keys, values, timestamps,
             statuses, sorted_input);
  }
  callback.OnComplete();
}

void DBImpl::TrackOrUntrackFiles(
    const std::vector<std::string>& existing_data_files, bool track) {
  auto sfm = static_cast_with_check<SstFileManagerImpl>(
      immutable_db_options_.sst_file_manager.get());
  assert(sfm);
  std::vector<ColumnFamilyMetaData> metadata;
  GetAllColumnFamilyMetaData(&metadata);
  auto action = [&](const std::string& file_path,
                    std::optional<uint64_t> size) {
    if (track) {
      if (size) {
        sfm->OnAddFile(file_path, *size).PermitUncheckedError();
      } else {
        sfm->OnAddFile(file_path).PermitUncheckedError();
      }
    } else {
      sfm->OnUntrackFile(file_path).PermitUncheckedError();
    }
  };

  std::unordered_set<std::string> referenced_files;
  for (const auto& md : metadata) {
    for (const auto& lmd : md.levels) {
      for (const auto& fmd : lmd.files) {
        // We're assuming that each sst file name exists in at most one of
        // the paths.
        std::string file_path =
            fmd.directory + kFilePathSeparator + fmd.relative_filename;
        action(file_path, fmd.size);
        referenced_files.insert(file_path);
      }
    }
    for (const auto& bmd : md.blob_files) {
      std::string name = bmd.blob_file_name;
      // The BlobMetaData.blob_file_name may start with "/".
      if (!name.empty() && name[0] == kFilePathSeparator) {
        name = name.substr(1);
      }
      // We're assuming that each blob file name exists in at most one of
      // the paths.
      std::string file_path = bmd.blob_file_path + kFilePathSeparator + name;
      action(file_path, bmd.blob_file_size);
      referenced_files.insert(file_path);
    }
  }

  for (const auto& file_path : existing_data_files) {
    if (referenced_files.find(file_path) != referenced_files.end()) {
      continue;
    }
    // There shouldn't be any duplicated files. In case there is, SstFileManager
    // will take care of deduping it.
    action(file_path, /*size=*/std::nullopt);
  }
}

}  // namespace ROCKSDB_NAMESPACE

// Generate the regular and coroutine versions of the functions defined in
// db_impl_sync_and_async.h by including it twice. Included here, after the
// namespace and all definitions in this file, so the generated bodies see the
// file-local types and helpers they use. The macros expand differently based on
// whether WITH_COROUTINES or WITHOUT_COROUTINES is defined.
// clang-format off
#define WITHOUT_COROUTINES
#include "db/db_impl/db_impl_sync_and_async.h"
#undef WITHOUT_COROUTINES
#define WITH_COROUTINES
#include "db/db_impl/db_impl_sync_and_async.h"
#undef WITH_COROUTINES
// clang-format on
