//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/flush_job.h"

#include <algorithm>
#include <cinttypes>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/event_helpers.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/memtable_list.h"
#include "db/merge_context.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "logging/event_logger.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics_impl.h"
#include "monitoring/thread_status_util.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "table/merging_iterator.h"
#include "table/table_builder.h"
#include "table/two_level_iterator.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/mutexlock.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

const char* GetFlushReasonString(FlushReason flush_reason) {
  switch (flush_reason) {
    case FlushReason::kOthers:
      return "Other Reasons";
    case FlushReason::kGetLiveFiles:
      return "Get Live Files";
    case FlushReason::kShutDown:
      return "Shut down";
    case FlushReason::kExternalFileIngestion:
      return "External File Ingestion";
    case FlushReason::kManualCompaction:
      return "Manual Compaction";
    case FlushReason::kWriteBufferManager:
      return "Write Buffer Manager";
    case FlushReason::kWriteBufferFull:
      return "Write Buffer Full";
    case FlushReason::kTest:
      return "Test";
    case FlushReason::kDeleteFiles:
      return "Delete Files";
    case FlushReason::kAutoCompaction:
      return "Auto Compaction";
    case FlushReason::kManualFlush:
      return "Manual Flush";
    case FlushReason::kErrorRecovery:
      return "Error Recovery";
    case FlushReason::kErrorRecoveryRetryFlush:
      return "Error Recovery Retry Flush";
    case FlushReason::kWalFull:
      return "WAL Full";
    case FlushReason::kCatchUpAfterErrorRecovery:
      return "Catch Up After Error Recovery";
    case FlushReason::kMemtableMaxRangeDeletions:
      return "Memtable Max Range Deletions";
    default:
      return "Invalid";
  }
}

FlushJob::FlushJob(
    const std::string& dbname, ColumnFamilyData* cfd,
    const ImmutableDBOptions& db_options,
    const MutableCFOptions& mutable_cf_options, uint64_t max_memtable_id,
    const FileOptions& file_options, VersionSet* versions,
    InstrumentedMutex* db_mutex, std::atomic<bool>* shutting_down,
    JobContext* job_context, FlushReason flush_reason, LogBuffer* log_buffer,
    FSDirectory* db_directory, FSDirectory* output_file_directory,
    CompressionType output_compression, Statistics* stats,
    EventLogger* event_logger, bool measure_io_stats,
    const bool sync_output_directory, const bool write_manifest,
    Env::Priority thread_pri, const std::shared_ptr<IOTracer>& io_tracer,
    std::shared_ptr<const SeqnoToTimeMapping> seqno_to_time_mapping,
    const std::string& db_id, const std::string& db_session_id,
    std::string full_history_ts_low, BlobFileCompletionCallback* blob_callback,
    bool fast_sst_open)
    : dbname_(dbname),
      db_id_(db_id),
      db_session_id_(db_session_id),
      cfd_(cfd),
      db_options_(db_options),
      mutable_cf_options_(mutable_cf_options),
      max_memtable_id_(max_memtable_id),
      file_options_(file_options),
      versions_(versions),
      db_mutex_(db_mutex),
      shutting_down_(shutting_down),
      earliest_snapshot_(job_context->GetEarliestSnapshotSequence()),
      job_context_(job_context),
      flush_reason_(flush_reason),
      log_buffer_(log_buffer),
      db_directory_(db_directory),
      output_file_directory_(output_file_directory),
      output_compression_(output_compression),
      stats_(stats),
      event_logger_(event_logger),
      measure_io_stats_(measure_io_stats),
      sync_output_directory_(sync_output_directory),
      write_manifest_(write_manifest),
      edit_(nullptr),
      base_(nullptr),
      pick_memtable_called(false),
      thread_pri_(thread_pri),
      io_tracer_(io_tracer),
      clock_(db_options_.clock),
      full_history_ts_low_(std::move(full_history_ts_low)),
      blob_callback_(blob_callback),
      fast_sst_open_(fast_sst_open),
      seqno_to_time_mapping_(std::move(seqno_to_time_mapping)) {
  assert(job_context->snapshot_context_initialized);
  // Update the thread status to indicate flush.
  ReportStartedFlush();
  TEST_SYNC_POINT("FlushJob::FlushJob()");
}

FlushJob::~FlushJob() { ThreadStatusUtil::ResetThreadStatus(); }

void FlushJob::ReportStartedFlush() {
  ThreadStatusUtil::SetEnableTracking(db_options_.enable_thread_tracking);
  ThreadStatusUtil::SetColumnFamily(cfd_);
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_FLUSH);
  ThreadStatusUtil::SetThreadOperationProperty(ThreadStatus::COMPACTION_JOB_ID,
                                               job_context_->job_id);

  IOSTATS_RESET(bytes_written);
}

void FlushJob::ReportFlushInputSize(const autovector<ReadOnlyMemTable*>& mems) {
  uint64_t input_size = 0;
  for (auto* mem : mems) {
    input_size += mem->ApproximateMemoryUsage();
  }
  ThreadStatusUtil::IncreaseThreadOperationProperty(
      ThreadStatus::FLUSH_BYTES_MEMTABLES, input_size);
}

void FlushJob::RecordFlushIOStats() {
  RecordTick(stats_, FLUSH_WRITE_BYTES, IOSTATS(bytes_written));
  ThreadStatusUtil::IncreaseThreadOperationProperty(
      ThreadStatus::FLUSH_BYTES_WRITTEN, IOSTATS(bytes_written));
  IOSTATS_RESET(bytes_written);
}
void FlushJob::PickMemTable() {
  db_mutex_->AssertHeld();
  assert(!pick_memtable_called);
  pick_memtable_called = true;

  // Maximum "NextLogNumber" of the memtables to flush.
  // When mempurge feature is turned off, this variable is useless
  // because the memtables are implicitly sorted by increasing order of creation
  // time. Therefore mems_->back()->GetNextLogNumber() is already equal to
  // max_next_log_number. However when Mempurge is on, the memtables are no
  // longer sorted by increasing order of creation time. Therefore this variable
  // becomes necessary because mems_->back()->GetNextLogNumber() is no longer
  // necessarily equal to max_next_log_number.
  uint64_t max_next_log_number = 0;

  // Save the contents of the earliest memtable as a new Table
  cfd_->imm()->PickMemtablesToFlush(max_memtable_id_, &mems_,
                                    &max_next_log_number);
  if (mems_.empty()) {
    return;
  }

  // Track effective cutoff user-defined timestamp during flush if
  // user-defined timestamps can be stripped.
  GetEffectiveCutoffUDTForPickedMemTables();
  GetPrecludeLastLevelMinSeqno();

  ReportFlushInputSize(mems_);

  // entries mems are (implicitly) sorted in ascending order by their created
  // time. We will use the first memtable's `edit` to keep the meta info for
  // this flush.
  ReadOnlyMemTable* m = mems_[0];
  edit_ = m->GetEdits();
  edit_->SetPrevLogNumber(0);
  // SetLogNumber(log_num) indicates logs with number smaller than log_num
  // will no longer be picked up for recovery.
  edit_->SetLogNumber(max_next_log_number);
  edit_->SetColumnFamily(cfd_->GetID());

  // path 0 for level 0 file.
  meta_.fd = FileDescriptor(versions_->NewFileNumber(), 0, 0);
  meta_.epoch_number = cfd_->NewEpochNumber();

  base_ = cfd_->current();
  base_->Ref();  // it is likely that we do not need this reference
}

Status FlushJob::Run(LogsWithPrepTracker* prep_tracker, FileMetaData* file_meta,
                     bool* switched_to_mempurge, bool* skipped_since_bg_error,
                     ErrorHandler* error_handler) {
  TEST_SYNC_POINT("FlushJob::Start");
  db_mutex_->AssertHeld();
  assert(pick_memtable_called);
  // Mempurge threshold can be dynamically changed.
  // For sake of consistency, mempurge_threshold is
  // saved locally to maintain consistency in each
  // FlushJob::Run call.
  double mempurge_threshold =
      mutable_cf_options_.experimental_mempurge_threshold;

  AutoThreadOperationStageUpdater stage_run(ThreadStatus::STAGE_FLUSH_RUN);
  if (mems_.empty()) {
    ROCKS_LOG_BUFFER(log_buffer_, "[%s] No memtable to flush",
                     cfd_->GetName().c_str());
    return Status::OK();
  }

  // I/O measurement variables
  PerfLevel prev_perf_level = PerfLevel::kEnableTime;
  uint64_t prev_write_nanos = 0;
  uint64_t prev_fsync_nanos = 0;
  uint64_t prev_range_sync_nanos = 0;
  uint64_t prev_prepare_write_nanos = 0;
  uint64_t prev_cpu_write_nanos = 0;
  uint64_t prev_cpu_read_nanos = 0;
  if (measure_io_stats_) {
    prev_perf_level = GetPerfLevel();
    SetPerfLevel(PerfLevel::kEnableTime);
    prev_write_nanos = IOSTATS(write_nanos);
    prev_fsync_nanos = IOSTATS(fsync_nanos);
    prev_range_sync_nanos = IOSTATS(range_sync_nanos);
    prev_prepare_write_nanos = IOSTATS(prepare_write_nanos);
    prev_cpu_write_nanos = IOSTATS(cpu_write_nanos);
    prev_cpu_read_nanos = IOSTATS(cpu_read_nanos);
  }
  Status mempurge_s = Status::NotFound("No MemPurge.");
  if ((mempurge_threshold > 0.0) &&
      (flush_reason_ == FlushReason::kWriteBufferFull) && (!mems_.empty()) &&
      MemPurgeDecider(mempurge_threshold) && !(db_options_.atomic_flush)) {
    cfd_->SetMempurgeUsed();
    mempurge_s = MemPurge();
    if (!mempurge_s.ok()) {
      // Mempurge is typically aborted when the output
      // bytes cannot be contained onto a single output memtable.
      if (mempurge_s.IsAborted()) {
        ROCKS_LOG_INFO(db_options_.info_log, "Mempurge process aborted: %s\n",
                       mempurge_s.ToString().c_str());
      } else {
        // However the mempurge process can also fail for
        // other reasons (eg: new_mem->Add() fails).
        ROCKS_LOG_WARN(db_options_.info_log, "Mempurge process failed: %s\n",
                       mempurge_s.ToString().c_str());
      }
    } else {
      if (switched_to_mempurge) {
        *switched_to_mempurge = true;
      } else {
        // The mempurge process was successful, but no switch_to_mempurge
        // pointer provided so no way to propagate the state of flush job.
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Mempurge process succeeded"
                       "but no 'switched_to_mempurge' ptr provided.\n");
      }
    }
  }
  Status s;
  if (mempurge_s.ok()) {
    base_->Unref();
    s = Status::OK();
  } else {
    // This will release and re-acquire the mutex.
    s = WriteLevel0Table();
  }

  if (s.ok() && cfd_->IsDropped()) {
    s = Status::ColumnFamilyDropped("Column family dropped during compaction");
  }
  if ((s.ok() || s.IsColumnFamilyDropped()) &&
      shutting_down_->load(std::memory_order_acquire)) {
    s = Status::ShutdownInProgress("Database shutdown");
  }

  if (s.ok()) {
    s = MaybeIncreaseFullHistoryTsLowToAboveCutoffUDT();
  }

  TEST_SYNC_POINT_CALLBACK("FlushJob::Run:PostBuildTable", &s);

  if (!s.ok()) {
    cfd_->imm()->RollbackMemtableFlush(
        mems_, /*rollback_succeeding_memtables=*/!db_options_.atomic_flush);
  } else if (write_manifest_) {
    assert(!db_options_.atomic_flush);
    if (!db_options_.atomic_flush &&
        flush_reason_ != FlushReason::kErrorRecovery &&
        flush_reason_ != FlushReason::kErrorRecoveryRetryFlush &&
        error_handler && !error_handler->GetBGError().ok() &&
        error_handler->IsBGWorkStopped()) {
      cfd_->imm()->RollbackMemtableFlush(
          mems_, /*rollback_succeeding_memtables=*/!db_options_.atomic_flush);
      s = error_handler->GetBGError();
      if (skipped_since_bg_error) {
        *skipped_since_bg_error = true;
      }
    } else {
      TEST_SYNC_POINT("FlushJob::InstallResults");
      // Replace immutable memtable with the generated Table
      s = cfd_->imm()->TryInstallMemtableFlushResults(
              cfd_, mems_, prep_tracker, versions_, db_mutex_,
              meta_.fd.GetNumber(), &job_context_->memtables_to_free, db_directory_,
              log_buffer_, &committed_flush_jobs_info_,
              !(mempurge_s.ok()) /* write_edit : true if no mempurge happened (or if aborted),
                              but 'false' if mempurge successful: no new min log number
                              or new level 0 file path to write to manifest. */);
    }
  }

  if (!s.ok() && meta_.fd.GetFileSize() > 0) {
    // If BuildTable succeeded (file was cached in table cache for user reads)
    // but the flush failed to install (e.g., LogAndApply MANIFEST I/O error,
    // CF dropped, shutdown), evict the cached entry. Without this, the file
    // is cached but not in any Version, and the FindObsoleteFiles backstop
    // can fail under metadata read fault injection, causing
    // TEST_VerifyNoObsoleteFilesCached to fire during Close().
    TableCache::ReleaseObsolete(cfd_->table_cache()->get_cache().get(),
                                meta_.fd.GetNumber(), nullptr /*handle*/,
                                mutable_cf_options_.uncache_aggressiveness);
  }

  if (s.ok() && file_meta != nullptr) {
    *file_meta = meta_;
  }
  RecordFlushIOStats();

  // When measure_io_stats_ is true, the default 512 bytes is not enough.
  auto stream = event_logger_->LogToBuffer(log_buffer_, 1024);
  stream << "job" << job_context_->job_id << "event" << "flush_finished";
  stream << "output_compression"
         << CompressionTypeToString(output_compression_);
  stream << "lsm_state";
  stream.StartArray();
  auto vstorage = cfd_->current()->storage_info();
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    stream << vstorage->NumLevelFiles(level);
  }
  stream.EndArray();

  const auto& blob_files = vstorage->GetBlobFiles();
  if (!blob_files.empty()) {
    assert(blob_files.front());
    stream << "blob_file_head" << blob_files.front()->GetBlobFileNumber();

    assert(blob_files.back());
    stream << "blob_file_tail" << blob_files.back()->GetBlobFileNumber();
  }

  stream << "immutable_memtables" << cfd_->imm()->NumNotFlushed();

  if (measure_io_stats_) {
    if (prev_perf_level != PerfLevel::kEnableTime) {
      SetPerfLevel(prev_perf_level);
    }
    stream << "file_write_nanos" << (IOSTATS(write_nanos) - prev_write_nanos);
    stream << "file_range_sync_nanos"
           << (IOSTATS(range_sync_nanos) - prev_range_sync_nanos);
    stream << "file_fsync_nanos" << (IOSTATS(fsync_nanos) - prev_fsync_nanos);
    stream << "file_prepare_write_nanos"
           << (IOSTATS(prepare_write_nanos) - prev_prepare_write_nanos);
    stream << "file_cpu_write_nanos"
           << (IOSTATS(cpu_write_nanos) - prev_cpu_write_nanos);
    stream << "file_cpu_read_nanos"
           << (IOSTATS(cpu_read_nanos) - prev_cpu_read_nanos);
  }

  TEST_SYNC_POINT("FlushJob::End");
  return s;
}

void FlushJob::Cancel() {
  db_mutex_->AssertHeld();
  assert(base_ != nullptr);
  base_->Unref();
}

Status FlushJob::MemPurge() {
  Status s;
  db_mutex_->AssertHeld();
  db_mutex_->Unlock();
  assert(!mems_.empty());

  // Measure purging time.
  const uint64_t start_micros = clock_->NowMicros();
  const uint64_t start_cpu_micros = clock_->CPUMicros();

  MemTable* new_mem = nullptr;
  // For performance/log investigation purposes:
  // look at how much useful payload we harvest in the new_mem.
  // This value is then printed to the DB log.
  double new_mem_capacity = 0.0;

  // Create two iterators, one for the memtable data (contains
  // info from puts + deletes), and one for the memtable
  // Range Tombstones (from DeleteRanges).
  // TODO: plumb Env::IOActivity, Env::IOPriority
  ReadOptions ro;
  ro.total_order_seek = true;
  Arena arena;
  std::vector<InternalIterator*> memtables;
  std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>>
      range_del_iters;
  for (ReadOnlyMemTable* m : mems_) {
    memtables.push_back(m->NewIterator(ro, /*seqno_to_time_mapping=*/nullptr,
                                       &arena, /*prefix_extractor=*/nullptr,
                                       /*for_flush=*/true));
    auto* range_del_iter = m->NewRangeTombstoneIterator(
        ro, kMaxSequenceNumber, true /* immutable_memtable */);
    if (range_del_iter != nullptr) {
      range_del_iters.emplace_back(range_del_iter);
    }
  }

  assert(!memtables.empty());
  SequenceNumber first_seqno = kMaxSequenceNumber;
  SequenceNumber earliest_seqno = kMaxSequenceNumber;
  // Pick first and earliest seqno as min of all first_seqno
  // and earliest_seqno of the mempurged memtables.
  for (const auto& mem : mems_) {
    first_seqno = mem->GetFirstSequenceNumber() < first_seqno
                      ? mem->GetFirstSequenceNumber()
                      : first_seqno;
    earliest_seqno = mem->GetEarliestSequenceNumber() < earliest_seqno
                         ? mem->GetEarliestSequenceNumber()
                         : earliest_seqno;
  }

  ScopedArenaPtr<InternalIterator> iter(
      NewMergingIterator(&(cfd_->internal_comparator()), memtables.data(),
                         static_cast<int>(memtables.size()), &arena));

  const auto& ioptions = cfd_->ioptions();

  // Place iterator at the First (meaning most recent) key node.
  iter->SeekToFirst();

  const std::string* const full_history_ts_low = &(cfd_->GetFullHistoryTsLow());
  std::unique_ptr<CompactionRangeDelAggregator> range_del_agg(
      new CompactionRangeDelAggregator(&(cfd_->internal_comparator()),
                                       job_context_->snapshot_seqs,
                                       full_history_ts_low));
  for (auto& rd_iter : range_del_iters) {
    range_del_agg->AddTombstones(std::move(rd_iter));
  }

  // If there is valid data in the memtable,
  // or at least range tombstones, copy over the info
  // to the new memtable.
  if (iter->Valid() || !range_del_agg->IsEmpty()) {
    // MaxSize is the size of a memtable.
    size_t maxSize = mutable_cf_options_.write_buffer_size;
    std::unique_ptr<CompactionFilter> compaction_filter;
    if (ioptions.compaction_filter_factory != nullptr &&
        ioptions.compaction_filter_factory->ShouldFilterTableFileCreation(
            TableFileCreationReason::kFlush)) {
      CompactionFilter::Context ctx;
      ctx.is_full_compaction = false;
      ctx.is_manual_compaction = false;
      ctx.column_family_id = cfd_->GetID();
      ctx.reason = TableFileCreationReason::kFlush;
      compaction_filter =
          ioptions.compaction_filter_factory->CreateCompactionFilter(ctx);
      CompactionFilter::MergeOrder order;
      s = GetCompactionFilterMergeOrder(
          compaction_filter.get(), ioptions.compaction_filter_factory.get(),
          TableFileCreationReason::kFlush, &order);
      if (s.ok() && compaction_filter != nullptr &&
          !compaction_filter->IgnoreSnapshots()) {
        s = Status::NotSupported(
            "CompactionFilter::IgnoreSnapshots() = false is not supported "
            "anymore.");
      }
      if (!s.ok()) {
        // Destroy the application filter before restoring the caller's lock.
        compaction_filter.reset();
        db_mutex_->Lock();
        return s;
      }
    }

    // MemPurge output is installed directly into the immutable list, so it
    // must not advertise memory that can be reclaimed by switching a mutable
    // memtable.
    new_mem = new MemTable(cfd_->internal_comparator(), cfd_->ioptions(),
                           mutable_cf_options_, cfd_->write_buffer_mgr(),
                           earliest_seqno, cfd_->GetID(),
                           nullptr /* flush_initiator */);
    assert(new_mem != nullptr);

    Env* env = db_options_.env;
    assert(env);
    MergeHelper merge(env, (cfd_->internal_comparator()).user_comparator(),
                      (ioptions.merge_operator).get(), compaction_filter.get(),
                      CompactionFilter::MergeOrder::kFilterBeforeMerge,
                      ioptions.logger,
                      true /* internal key corruption is not ok */,
                      job_context_->GetLatestSnapshotSequence(),
                      job_context_->snapshot_checker);
    assert(job_context_);
    const std::atomic<bool> kManualCompactionCanceledFalse{false};
    CompactionIterator c_iter(
        iter.get(), (cfd_->internal_comparator()).user_comparator(), &merge,
        kMaxSequenceNumber, &job_context_->snapshot_seqs, earliest_snapshot_,
        job_context_->earliest_write_conflict_snapshot,
        job_context_->GetJobSnapshotSequence(), job_context_->snapshot_checker,
        env, ShouldReportDetailedTime(env, ioptions.stats), range_del_agg.get(),
        nullptr, ioptions.allow_data_in_errors,
        ioptions.enforce_single_del_contracts,
        /*manual_compaction_canceled=*/kManualCompactionCanceledFalse,
        false /* must_count_input_entries */,
        /*compaction=*/nullptr, compaction_filter.get(),
        /*shutting_down=*/nullptr, ioptions.info_log, full_history_ts_low);

    // Set earliest sequence number in the new memtable
    // to be equal to the earliest sequence number of the
    // memtable being flushed (See later if there is a need
    // to update this number!).
    new_mem->SetEarliestSequenceNumber(earliest_seqno);
    // Likewise for first seq number.
    new_mem->SetFirstSequenceNumber(first_seqno);
    SequenceNumber new_first_seqno = kMaxSequenceNumber;

    c_iter.SeekToFirst();

    // Key transfer
    for (; c_iter.Valid(); c_iter.Next()) {
      const ParsedInternalKey ikey = c_iter.ikey();
      const Slice value = c_iter.value();
      new_first_seqno =
          ikey.sequence < new_first_seqno ? ikey.sequence : new_first_seqno;

      // Should we update "OldestKeyTime" ???? -> timestamp appear
      // to still be an "experimental" feature.
      s = new_mem->Add(
          ikey.sequence, ikey.type, ikey.user_key, value,
          nullptr,   // KV protection info set as nullptr since it
                     // should only be useful for the first add to
                     // the original memtable.
          false,     // : allow concurrent_memtable_writes_
                     // Not seen as necessary for now.
          nullptr,   // get_post_process_info(m) must be nullptr
                     // when concurrent_memtable_writes is switched off.
          nullptr);  // hint, only used when concurrent_memtable_writes_
                     // is switched on.
      if (!s.ok()) {
        break;
      }

      // If new_mem has size greater than maxSize,
      // then rollback to regular flush operation,
      // and destroy new_mem.
      if (new_mem->ApproximateMemoryUsage() > maxSize) {
        s = Status::Aborted("Mempurge filled more than one memtable.");
        new_mem_capacity = 1.0;
        break;
      }
    }

    // Check status and propagate
    // potential error status from c_iter
    if (!s.ok()) {
      c_iter.status().PermitUncheckedError();
    } else if (!c_iter.status().ok()) {
      s = c_iter.status();
    }

    // Range tombstone transfer.
    if (s.ok()) {
      auto range_del_it = range_del_agg->NewIterator();
      for (range_del_it->SeekToFirst(); range_del_it->Valid();
           range_del_it->Next()) {
        auto tombstone = range_del_it->Tombstone();
        new_first_seqno =
            tombstone.seq_ < new_first_seqno ? tombstone.seq_ : new_first_seqno;
        MemTablePostProcessInfo post_process_info;
        s = new_mem->Add(
            tombstone.seq_,        // Sequence number
            kTypeRangeDeletion,    // KV type
            tombstone.start_key_,  // Key is start key.
            tombstone.end_key_,    // Value is end key.
            nullptr,               // KV protection info set as nullptr since it
                                   // should only be useful for the first add to
                                   // the original memtable.
            true,                  // allow_concurrent: required for range
                                   // deletions since range_del_table_ uses
                                   // concurrent-safe SkipList.
            &post_process_info,
            nullptr);  // hint, only used when concurrent_memtable_writes_
                       // is switched on.
        if (s.ok()) {
          new_mem->BatchPostProcess(post_process_info);
        }

        if (!s.ok()) {
          break;
        }

        // If new_mem has size greater than maxSize,
        // then rollback to regular flush operation,
        // and destroy new_mem.
        if (new_mem->ApproximateMemoryUsage() > maxSize) {
          s = Status::Aborted(Slice("Mempurge filled more than one memtable."));
          new_mem_capacity = 1.0;
          break;
        }
      }
    }

    // If everything happened smoothly and new_mem contains valid data,
    // decide if it is flushed to storage or kept in the imm()
    // memtable list (memory).
    if (s.ok() && (new_first_seqno != kMaxSequenceNumber)) {
      // Rectify the first sequence number, which (unlike the earliest seq
      // number) needs to be present in the new memtable.
      new_mem->SetFirstSequenceNumber(new_first_seqno);

      // The new_mem is added to the list of immutable memtables
      // only if it filled at less than 100% capacity and isn't flagged
      // as in need of being flushed.
      if (new_mem->ApproximateMemoryUsage() < maxSize &&
          !(new_mem->ShouldFlushNow())) {
        // Construct fragmented memtable range tombstones without mutex
        new_mem->ConstructFragmentedRangeTombstones();
        TEST_SYNC_POINT("FlushJob::MemPurge:BeforeReacquireMutex");
        TEST_SYNC_POINT("FlushJob::MemPurge:AfterWaitForTest");
        db_mutex_->Lock();
        // Take the newest id, so that memtables in MemtableList don't have
        // out-of-order memtable ids. While the db mutex was released during
        // MemPurge, new memtables may have been switched to the immutable
        // list with higher IDs, so we must use the maximum of the original
        // flush batch ID and the current latest immutable memtable ID.
        uint64_t new_mem_id = std::max(
            mems_.back()->GetID(),
            cfd_->imm()->GetLatestMemTableID(false /*for_atomic_flush*/));

        new_mem->SetID(new_mem_id);
        // Take the latest memtable's next log number.
        new_mem->SetNextLogNumber(mems_.back()->GetNextLogNumber());

        // This addition will not trigger another flush, because
        // we do not call EnqueuePendingFlush().
        cfd_->imm()->Add(new_mem, &job_context_->memtables_to_free);
        new_mem->Ref();
        // Piggyback FlushJobInfo on the first flushed memtable.
        db_mutex_->AssertHeld();
        meta_.fd.file_size = 0;
        mems_[0]->SetFlushJobInfo(GetFlushJobInfo());
        db_mutex_->Unlock();
      } else {
        s = Status::Aborted(Slice("Mempurge filled more than one memtable."));
        new_mem_capacity = 1.0;
        if (new_mem) {
          job_context_->memtables_to_free.push_back(new_mem);
        }
      }
    } else {
      // In this case, the newly allocated new_mem is empty.
      assert(new_mem != nullptr);
      job_context_->memtables_to_free.push_back(new_mem);
    }
  }

  // Reacquire the mutex for WriteLevel0 function.
  db_mutex_->Lock();

  // If mempurge successful, don't write input tables to level0,
  // but write any full output table to level0.
  if (s.ok()) {
    TEST_SYNC_POINT("DBImpl::FlushJob:MemPurgeSuccessful");
  } else {
    TEST_SYNC_POINT("DBImpl::FlushJob:MemPurgeUnsuccessful");
  }
  const uint64_t micros = clock_->NowMicros() - start_micros;
  const uint64_t cpu_micros = clock_->CPUMicros() - start_cpu_micros;
  ROCKS_LOG_INFO(db_options_.info_log,
                 "[%s] [JOB %d] Mempurge lasted %" PRIu64
                 " microseconds, and %" PRIu64
                 " cpu "
                 "microseconds. Status is %s ok. Perc capacity: %f\n",
                 cfd_->GetName().c_str(), job_context_->job_id, micros,
                 cpu_micros, s.ok() ? "" : "not", new_mem_capacity);

  return s;
}

bool FlushJob::MemPurgeDecider(double threshold) {
  // Never trigger mempurge if threshold is not a strictly positive value.
  if (!(threshold > 0.0)) {
    return false;
  }
  if (threshold > (1.0 * mems_.size())) {
    return true;
  }
  // Payload and useful_payload (in bytes).
  // The useful payload ratio of a given MemTable
  // is estimated to be useful_payload/payload.
  uint64_t payload = 0;
  uint64_t useful_payload = 0;
  uint64_t entry_size = 0;

  // Local variables used repetitively inside the for-loop
  // when iterating over the sampled entries.
  Slice key_slice;
  Slice value_slice;
  ParsedInternalKey res;
  SnapshotImpl min_snapshot;
  std::string vget;
  Status mget_s;
  Status parse_s;
  MergeContext merge_context;
  SequenceNumber max_covering_tombstone_seq = 0;
  SequenceNumber sqno = 0;
  SequenceNumber min_seqno_snapshot = 0;
  bool get_res;
  bool can_be_useful_payload;
  bool not_in_next_mems;

  // If estimated_useful_payload is > threshold,
  // then flush to storage, else MemPurge.
  double estimated_useful_payload = 0.0;
  // Cochran formula for determining sample size.
  // 95% confidence interval, 7% precision.
  //    n0 = (1.96*1.96)*0.25/(0.07*0.07) = 196.0
  double n0 = 196.0;
  // TODO: plumb Env::IOActivity, Env::IOPriority
  ReadOptions ro;
  ro.total_order_seek = true;

  // Iterate over each memtable of the set.
  for (auto mem_iter = std::begin(mems_); mem_iter != std::end(mems_);
       ++mem_iter) {
    ReadOnlyMemTable* mt = *mem_iter;

    // Else sample from the table.
    uint64_t nentries = mt->NumEntries();
    // Corrected Cochran formula for small populations
    // (converges to n0 for large populations).
    uint64_t target_sample_size =
        static_cast<uint64_t>(ceil(n0 / (1.0 + (n0 / nentries))));
    std::unordered_set<const char*> sentries = {};
    // Populate sample entries set.
    mt->UniqueRandomSample(target_sample_size, &sentries);

    // Estimate the garbage ratio by comparing if
    // each sample corresponds to a valid entry.
    for (const char* ss : sentries) {
      key_slice = GetLengthPrefixedSlice(ss);
      parse_s = ParseInternalKey(key_slice, &res, true /*log_err_key*/);
      if (!parse_s.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Memtable Decider: ParseInternalKey did not parse "
                       "key_slice %s successfully.",
                       key_slice.data());
      }

      // Size of the entry is "key size (+ value size if KV entry)"
      entry_size = key_slice.size();
      if (res.type == kTypeValue) {
        value_slice =
            GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
        entry_size += value_slice.size();
      }

      // Count entry bytes as payload.
      payload += entry_size;

      LookupKey lkey(res.user_key, kMaxSequenceNumber);

      // Paranoia: zero out these values just in case.
      max_covering_tombstone_seq = 0;
      sqno = 0;

      // Pick the oldest existing snapshot that is more recent
      // than the sequence number of the sampled entry.
      min_seqno_snapshot = kMaxSequenceNumber;
      for (SequenceNumber seq_num : job_context_->snapshot_seqs) {
        if (seq_num > res.sequence && seq_num < min_seqno_snapshot) {
          min_seqno_snapshot = seq_num;
        }
      }
      min_snapshot.number_ = min_seqno_snapshot;
      ro.snapshot =
          min_seqno_snapshot < kMaxSequenceNumber ? &min_snapshot : nullptr;

      // Estimate if the sample entry is valid or not.
      get_res = mt->Get(lkey, &vget, /*columns=*/nullptr, /*timestamp=*/nullptr,
                        &mget_s, &merge_context, &max_covering_tombstone_seq,
                        &sqno, ro, true /* immutable_memtable */);
      if (!get_res) {
        ROCKS_LOG_WARN(
            db_options_.info_log,
            "Memtable Get returned false when Get(sampled entry). "
            "Yet each sample entry should exist somewhere in the memtable, "
            "unrelated to whether it has been deleted or not.");
      }

      // TODO(bjlemaire): evaluate typeMerge.
      // This is where the sampled entry is estimated to be
      // garbage or not. Note that this is a garbage *estimation*
      // because we do not include certain items such as
      // CompactionFitlers triggered at flush, or if the same delete
      // has been inserted twice or more in the memtable.

      // Evaluate if the entry can be useful payload
      // Situation #1: entry is a KV entry, was found in the memtable mt
      //               and the sequence numbers match.
      can_be_useful_payload = (res.type == kTypeValue) && get_res &&
                              mget_s.ok() && (sqno == res.sequence);

      // Situation #2: entry is a delete entry, was found in the memtable mt
      //               (because gres==true) and no valid KV entry is found.
      //               (note: duplicate delete entries are also taken into
      //               account here, because the sequence number 'sqno'
      //               in memtable->Get(&sqno) operation is set to be equal
      //               to the most recent delete entry as well).
      can_be_useful_payload |=
          ((res.type == kTypeDeletion) || (res.type == kTypeSingleDeletion)) &&
          mget_s.IsNotFound() && get_res && (sqno == res.sequence);

      // If there is a chance that the entry is useful payload
      // Verify that the entry does not appear in the following memtables
      // (memtables with greater memtable ID/larger sequence numbers).
      if (can_be_useful_payload) {
        not_in_next_mems = true;
        for (auto next_mem_iter = mem_iter + 1;
             next_mem_iter != std::end(mems_); next_mem_iter++) {
          if ((*next_mem_iter)
                  ->Get(lkey, &vget, /*columns=*/nullptr, /*timestamp=*/nullptr,
                        &mget_s, &merge_context, &max_covering_tombstone_seq,
                        &sqno, ro, true /* immutable_memtable */)) {
            not_in_next_mems = false;
            break;
          }
        }
        if (not_in_next_mems) {
          useful_payload += entry_size;
        }
      }
    }
    if (payload > 0) {
      // We use the estimated useful payload ratio to
      // evaluate how many of the memtable bytes are useful bytes.
      estimated_useful_payload +=
          (mt->ApproximateMemoryUsage()) * (useful_payload * 1.0 / payload);

      ROCKS_LOG_INFO(db_options_.info_log,
                     "Mempurge sampling [CF %s] - found garbage ratio from "
                     "sampling: %f. Threshold is %f\n",
                     cfd_->GetName().c_str(),
                     (payload - useful_payload) * 1.0 / payload, threshold);
    } else {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "Mempurge sampling: null payload measured, and collected "
                     "sample size is %zu\n.",
                     sentries.size());
    }
  }
  // We convert the total number of useful payload bytes
  // into the proportion of memtable necessary to store all these bytes.
  // We compare this proportion with the threshold value.
  return ((estimated_useful_payload / mutable_cf_options_.write_buffer_size) <
          threshold);
}

Status FlushJob::WriteLevel0Table() {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_FLUSH_WRITE_L0);
  db_mutex_->AssertHeld();
  const uint64_t start_micros = clock_->NowMicros();
  const uint64_t start_cpu_micros = clock_->CPUMicros();
  Status s;

  meta_.temperature = mutable_cf_options_.default_write_temperature;
  file_options_.temperature = meta_.temperature;

  const auto* ucmp = cfd_->internal_comparator().user_comparator();
  assert(ucmp);
  const size_t ts_sz = ucmp->timestamp_size();
  const bool logical_strip_timestamp =
      ts_sz > 0 && !cfd_->ioptions().persist_user_defined_timestamps;

  std::vector<BlobFileAddition> blob_file_additions;
  std::vector<BlobFileGarbage> blob_file_garbages;
  // Only direct-write memtables can carry blob indexes that reference
  // pre-existing blob files. Plain flushes write new blob files from inline
  // values, so there is no pre-existing blob garbage to meter on the input
  // side.
  std::vector<BlobFileGarbage>* const blob_file_garbages_for_filtering =
      cfd_->blob_partition_manager() != nullptr ? &blob_file_garbages : nullptr;
  // Note that here we treat flush as level 0 compaction in internal stats
  InternalStats::CompactionStats flush_stats(CompactionReason::kFlush,
                                             1 /* count**/);
  {
    auto write_hint = base_->storage_info()->CalculateSSTWriteHint(
        /*level=*/0, db_options_.calculate_sst_write_lifetime_hint_set);
    Env::IOPriority io_priority = GetRateLimiterPriority();
    db_mutex_->Unlock();
    if (log_buffer_) {
      log_buffer_->FlushBufferToLog();
    }
    // memtables and range_del_iters store internal iterators over each data
    // memtable and its associated range deletion memtable, respectively, at
    // corresponding indexes.
    std::vector<InternalIterator*> memtables;
    std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>>
        range_del_iters;
    ReadOptions ro;
    ro.total_order_seek = true;
    ro.io_activity = Env::IOActivity::kFlush;
    Arena arena;
    uint64_t total_num_input_entries = 0;
    uint64_t total_num_deletes = 0;
    uint64_t total_data_size = 0;
    size_t total_memory_usage = 0;
    uint64_t total_num_range_deletes = 0;
    // Used for testing:
    uint64_t mems_size = mems_.size();
    (void)mems_size;  // avoids unused variable error when
                      // TEST_SYNC_POINT_CALLBACK not used.
    TEST_SYNC_POINT_CALLBACK("FlushJob::WriteLevel0Table:num_memtables",
                             &mems_size);
    assert(job_context_);
    for (ReadOnlyMemTable* m : mems_) {
      ROCKS_LOG_INFO(db_options_.info_log,
                     "[%s] [JOB %d] Flushing memtable id %" PRIu64
                     " with next log file: %" PRIu64 ", marked_for_flush: %d\n",
                     cfd_->GetName().c_str(), job_context_->job_id, m->GetID(),
                     m->GetNextLogNumber(), m->IsMarkedForFlush());
      if (logical_strip_timestamp) {
        memtables.push_back(m->NewTimestampStrippingIterator(
            ro, /*seqno_to_time_mapping=*/nullptr, &arena,
            /*prefix_extractor=*/nullptr, ts_sz));
      } else {
        memtables.push_back(
            m->NewIterator(ro, /*seqno_to_time_mapping=*/nullptr, &arena,
                           /*prefix_extractor=*/nullptr, /*for_flush=*/true));
      }
      auto* range_del_iter =
          logical_strip_timestamp
              ? m->NewTimestampStrippingRangeTombstoneIterator(
                    ro, kMaxSequenceNumber, ts_sz)
              : m->NewRangeTombstoneIterator(ro, kMaxSequenceNumber,
                                             true /* immutable_memtable */);
      if (range_del_iter != nullptr) {
        range_del_iters.emplace_back(range_del_iter);
      }
      total_num_input_entries += m->NumEntries();
      total_num_deletes += m->NumDeletion();
      total_data_size += m->GetDataSize();
      total_memory_usage += m->ApproximateMemoryUsage();
      total_num_range_deletes += m->NumRangeDeletion();
    }

    RecordInHistogram(stats_, FLUSH_MEMTABLE_MEMORY_BYTES, total_memory_usage);
    RecordInHistogram(stats_, FLUSH_MEMTABLE_TOTAL_DATA_SIZE, total_data_size);
    if (flush_reason_ == FlushReason::kWriteBufferFull) {
      RecordTick(stats_, FLUSH_REASON_WRITE_BUFFER_FULL);
      RecordInHistogram(stats_, FLUSH_WRITE_BUFFER_FULL_MEMTABLE_MEMORY_BYTES,
                        total_memory_usage);
    } else if (flush_reason_ == FlushReason::kWriteBufferManager) {
      RecordTick(stats_, FLUSH_REASON_WRITE_BUFFER_MANAGER);
      RecordInHistogram(stats_,
                        FLUSH_WRITE_BUFFER_MANAGER_MEMTABLE_MEMORY_BYTES,
                        total_memory_usage);
    } else if (flush_reason_ == FlushReason::kMemtableMaxRangeDeletions) {
      RecordTick(stats_, FLUSH_REASON_MEMTABLE_MAX_RANGE_DELETIONS);
    }

    event_logger_->Log() << "job" << job_context_->job_id << "event"
                         << "flush_started" << "num_memtables" << mems_.size()
                         << "total_num_input_entries" << total_num_input_entries
                         << "num_deletes" << total_num_deletes
                         << "total_data_size" << total_data_size
                         << "memory_usage" << total_memory_usage
                         << "num_range_deletes" << total_num_range_deletes
                         << "flush_reason"
                         << GetFlushReasonString(flush_reason_);

    {
      ScopedArenaPtr<InternalIterator> iter(
          NewMergingIterator(&cfd_->internal_comparator(), memtables.data(),
                             static_cast<int>(memtables.size()), &arena));
      ROCKS_LOG_INFO(db_options_.info_log,
                     "[%s] [JOB %d] Level-0 flush table #%" PRIu64 ": started",
                     cfd_->GetName().c_str(), job_context_->job_id,
                     meta_.fd.GetNumber());

      TEST_SYNC_POINT_CALLBACK("FlushJob::WriteLevel0Table:output_compression",
                               &output_compression_);
      int64_t _current_time = 0;
      auto status = clock_->GetCurrentTime(&_current_time);
      // Safe to proceed even if GetCurrentTime fails. So, log and proceed.
      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Failed to get current time to populate "
                       "oldest_ancestor_time property. "
                       "Status: %s",
                       status.ToString().c_str());
      }
      const uint64_t current_time = static_cast<uint64_t>(_current_time);

      uint64_t oldest_key_time = mems_.front()->ApproximateOldestKeyTime();

      // It's not clear whether oldest_key_time is always available. In case
      // it is not available, use current_time.
      uint64_t oldest_ancestor_time = std::min(current_time, oldest_key_time);

      TEST_SYNC_POINT_CALLBACK(
          "FlushJob::WriteLevel0Table:oldest_ancestor_time",
          &oldest_ancestor_time);
      meta_.oldest_ancestor_time = oldest_ancestor_time;
      meta_.file_creation_time = current_time;

      uint64_t memtable_payload_bytes = 0;
      uint64_t memtable_garbage_bytes = 0;
      IOStatus io_s;

      const std::string* const full_history_ts_low =
          (full_history_ts_low_.empty()) ? nullptr : &full_history_ts_low_;
      ReadOptions read_options(Env::IOActivity::kFlush);
      read_options.rate_limiter_priority = io_priority;
      const WriteOptions write_options(io_priority, Env::IOActivity::kFlush);
      TableBuilderOptions tboptions(
          cfd_->ioptions(), mutable_cf_options_, read_options, write_options,
          cfd_->internal_comparator(), cfd_->internal_tbl_prop_coll_factories(),
          output_compression_, mutable_cf_options_.compression_opts,
          cfd_->GetID(), cfd_->GetName(), 0 /* level */,
          current_time /* newest_key_time */, false /* is_bottommost */,
          TableFileCreationReason::kFlush, oldest_key_time, current_time,
          db_id_, db_session_id_, 0 /* target_file_size */,
          meta_.fd.GetNumber(),
          preclude_last_level_min_seqno_ == kMaxSequenceNumber
              ? preclude_last_level_min_seqno_
              : std::min(earliest_snapshot_, preclude_last_level_min_seqno_),
          dbname_ /*db_name*/);
      s = BuildTable(
          dbname_, versions_, db_options_, tboptions, file_options_,
          cfd_->table_cache(), iter.get(), std::move(range_del_iters), &meta_,
          &blob_file_additions, job_context_->snapshot_seqs, earliest_snapshot_,
          job_context_->earliest_write_conflict_snapshot,
          job_context_->GetJobSnapshotSequence(),
          job_context_->snapshot_checker,
          mutable_cf_options_.paranoid_file_checks, cfd_->internal_stats(),
          &io_s, io_tracer_, BlobFileCreationReason::kFlush,
          seqno_to_time_mapping_.get(), event_logger_, job_context_->job_id,
          &table_properties_, write_hint, full_history_ts_low, blob_callback_,
          base_, &memtable_payload_bytes, &memtable_garbage_bytes, &flush_stats,
          blob_file_garbages_for_filtering, fast_sst_open_);
      TEST_SYNC_POINT_CALLBACK("FlushJob::WriteLevel0Table:s", &s);
      // TODO: Cleanup io_status in BuildTable and table builders
      assert(!s.ok() || io_s.ok());
      io_s.PermitUncheckedError();
      if (s.ok() && total_num_input_entries != flush_stats.num_input_records) {
        std::string msg = "Expected " +
                          std::to_string(total_num_input_entries) +
                          " entries in memtables, but read " +
                          std::to_string(flush_stats.num_input_records);
        ROCKS_LOG_WARN(db_options_.info_log, "[%s] [JOB %d] Level-0 flush %s",
                       cfd_->GetName().c_str(), job_context_->job_id,
                       msg.c_str());
        if (db_options_.flush_verify_memtable_count) {
          s = Status::Corruption(msg);
        }
      }

      // Only verify on table with format collects table properties
      if (s.ok() &&
          (mutable_cf_options_.table_factory->IsInstanceOf(
               TableFactory::kBlockBasedTableName()) ||
           mutable_cf_options_.table_factory->IsInstanceOf(
               TableFactory::kPlainTableName())) &&
          flush_stats.num_output_records != table_properties_.num_entries) {
        std::string msg =
            "Number of keys in flush output SST files does not match "
            "number of keys added to the table. Expected " +
            std::to_string(flush_stats.num_output_records) + " but there are " +
            std::to_string(table_properties_.num_entries) +
            " in output SST files";
        ROCKS_LOG_WARN(db_options_.info_log, "[%s] [JOB %d] Level-0 flush %s",
                       cfd_->GetName().c_str(), job_context_->job_id,
                       msg.c_str());
        if (db_options_.flush_verify_memtable_count) {
          s = Status::Corruption(msg);
        }
      }
      if (tboptions.reason == TableFileCreationReason::kFlush) {
        TEST_SYNC_POINT("DBImpl::FlushJob:Flush");
        RecordTick(stats_, MEMTABLE_PAYLOAD_BYTES_AT_FLUSH,
                   memtable_payload_bytes);
        RecordTick(stats_, MEMTABLE_GARBAGE_BYTES_AT_FLUSH,
                   memtable_garbage_bytes);
      }
      LogFlush(db_options_.info_log);
    }
    ROCKS_LOG_BUFFER(log_buffer_,
                     "[%s] [JOB %d] Level-0 flush table #%" PRIu64 ": %" PRIu64
                     " bytes %s"
                     " %s"
                     " %s",
                     cfd_->GetName().c_str(), job_context_->job_id,
                     meta_.fd.GetNumber(), meta_.fd.GetFileSize(),
                     s.ToString().c_str(),
                     s.ok() && meta_.fd.GetFileSize() == 0
                         ? "It's an empty SST file from a successful flush so "
                           "won't be kept in the DB"
                         : "",
                     meta_.marked_for_compaction ? " (needs compaction)" : "");

    if (s.ok() && output_file_directory_ != nullptr && sync_output_directory_) {
      s = output_file_directory_->FsyncWithDirOptions(
          IOOptions(), nullptr,
          DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
    }
    TEST_SYNC_POINT_CALLBACK("FlushJob::WriteLevel0Table", &mems_);
    db_mutex_->Lock();
  }
  base_->Unref();

  // Note that if file_size is zero, the SST has been deleted and should not be
  // added to the manifest. Blob metadata updates may still need to be
  // committed for direct-write files or flush-time filtering.
  const bool has_output = meta_.fd.GetFileSize() > 0;

  if (s.ok()) {
    if (has_output) {
      TEST_SYNC_POINT("DBImpl::FlushJob:SSTFileCreated");
      // if we have more than 1 background thread, then we cannot
      // insert files directly into higher levels because some other
      // threads could be concurrently producing compacted files for
      // that key range.
      // Add file to L0
      TEST_SYNC_POINT_CALLBACK("FileMetaData::FileMetaData", &meta_);
      edit_->AddFile(0 /* level */, meta_);
    }

    edit_->SetBlobFileAdditions(std::move(blob_file_additions));
    for (auto& garbage : blob_file_garbages) {
      edit_->AddBlobFileGarbage(std::move(garbage));
    }
    for (auto& addition : external_blob_file_additions_) {
      edit_->AddBlobFile(std::move(addition));
    }
    for (auto& garbage : external_blob_file_garbages_) {
      edit_->AddBlobFileGarbage(std::move(garbage));
    }
    external_blob_file_additions_.clear();
    external_blob_file_garbages_.clear();
  }
  // Piggyback FlushJobInfo on the first first flushed memtable.
  mems_[0]->SetFlushJobInfo(GetFlushJobInfo());

  const uint64_t micros = clock_->NowMicros() - start_micros;
  const uint64_t cpu_micros = clock_->CPUMicros() - start_cpu_micros;
  flush_stats.micros = micros;
  flush_stats.cpu_micros += cpu_micros;

  ROCKS_LOG_INFO(db_options_.info_log,
                 "[%s] [JOB %d] Flush lasted %" PRIu64
                 " microseconds, and %" PRIu64 " cpu microseconds.\n",
                 cfd_->GetName().c_str(), job_context_->job_id, micros,
                 flush_stats.cpu_micros);

  if (has_output) {
    flush_stats.bytes_written = meta_.fd.GetFileSize();
    flush_stats.num_output_files = 1;
  }

  const auto& blobs = edit_->GetBlobFileAdditions();
  for (const auto& blob : blobs) {
    flush_stats.bytes_written_blob += blob.GetTotalBlobBytes();
  }

  flush_stats.num_output_files_blob = static_cast<int>(blobs.size());

  RecordTimeToHistogram(stats_, FLUSH_TIME, flush_stats.micros);
  cfd_->internal_stats()->AddCompactionStats(0 /* level */, thread_pri_,
                                             flush_stats);
  cfd_->internal_stats()->AddCFStats(
      InternalStats::BYTES_FLUSHED,
      flush_stats.bytes_written + flush_stats.bytes_written_blob);
  RecordFlushIOStats();

  return s;
}

Env::IOPriority FlushJob::GetRateLimiterPriority() {
  if (versions_ && versions_->GetColumnFamilySet() &&
      versions_->GetColumnFamilySet()->write_controller()) {
    WriteController* write_controller =
        versions_->GetColumnFamilySet()->write_controller();
    if (write_controller->IsStopped() || write_controller->NeedsDelay()) {
      return Env::IO_USER;
    }
  }

  return Env::IO_HIGH;
}

std::unique_ptr<FlushJobInfo> FlushJob::GetFlushJobInfo() const {
  db_mutex_->AssertHeld();
  std::unique_ptr<FlushJobInfo> info(new FlushJobInfo{});
  info->cf_id = cfd_->GetID();
  info->cf_name = cfd_->GetName();

  const uint64_t file_number = meta_.fd.GetNumber();
  info->file_path =
      MakeTableFileName(cfd_->ioptions().cf_paths[0].path, file_number);
  info->file_number = file_number;
  info->oldest_blob_file_number = meta_.oldest_blob_file_number;
  info->thread_id = db_options_.env->GetThreadID();
  info->job_id = job_context_->job_id;
  info->smallest_seqno = meta_.fd.smallest_seqno;
  info->largest_seqno = meta_.fd.largest_seqno;
  info->table_properties = table_properties_;
  info->flush_reason = flush_reason_;
  info->blob_compression_type = mutable_cf_options_.blob_compression_type;

  // Update BlobFilesInfo.
  for (const auto& blob_file : edit_->GetBlobFileAdditions()) {
    BlobFileAdditionInfo blob_file_addition_info(
        BlobFileName(cfd_->ioptions().cf_paths.front().path,
                     blob_file.GetBlobFileNumber()) /*blob_file_path*/,
        blob_file.GetBlobFileNumber(), blob_file.GetTotalBlobCount(),
        blob_file.GetTotalBlobBytes());
    info->blob_file_addition_infos.emplace_back(
        std::move(blob_file_addition_info));
  }
  return info;
}

void FlushJob::GetEffectiveCutoffUDTForPickedMemTables() {
  db_mutex_->AssertHeld();
  assert(pick_memtable_called);
  const auto* ucmp = cfd_->internal_comparator().user_comparator();
  assert(ucmp);
  const size_t ts_sz = ucmp->timestamp_size();
  if (db_options_.atomic_flush || ts_sz == 0 ||
      cfd_->ioptions().persist_user_defined_timestamps) {
    return;
  }
  // Find the newest user-defined timestamps from all the flushed memtables.
  for (const ReadOnlyMemTable* m : mems_) {
    Slice table_newest_udt = m->GetNewestUDT();
    // Empty memtables can be legitimately created and flushed, for example
    // by error recovery flush attempts.
    if (table_newest_udt.empty()) {
      continue;
    }
    if (cutoff_udt_.empty() ||
        ucmp->CompareTimestamp(table_newest_udt, cutoff_udt_) > 0) {
      if (!cutoff_udt_.empty()) {
        assert(table_newest_udt.size() == cutoff_udt_.size());
      }
      cutoff_udt_.assign(table_newest_udt.data(), table_newest_udt.size());
    }
  }
}

void FlushJob::GetPrecludeLastLevelMinSeqno() {
  if (mutable_cf_options_.preclude_last_level_data_seconds == 0) {
    return;
  }
  // SuperVersion should guarantee this
  assert(seqno_to_time_mapping_);
  assert(!seqno_to_time_mapping_->Empty());
  int64_t current_time = 0;
  Status s = db_options_.clock->GetCurrentTime(&current_time);
  if (!s.ok()) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Failed to get current time in Flush: Status: %s",
                   s.ToString().c_str());
  } else {
    SequenceNumber preserve_time_min_seqno;
    seqno_to_time_mapping_->GetCurrentTieringCutoffSeqnos(
        static_cast<uint64_t>(current_time),
        mutable_cf_options_.preserve_internal_time_seconds,
        mutable_cf_options_.preclude_last_level_data_seconds,
        &preserve_time_min_seqno, &preclude_last_level_min_seqno_);
  }
}

Status FlushJob::MaybeIncreaseFullHistoryTsLowToAboveCutoffUDT() {
  db_mutex_->AssertHeld();
  const auto* ucmp = cfd_->user_comparator();
  assert(ucmp);
  const std::string& full_history_ts_low = cfd_->GetFullHistoryTsLow();
  // Update full_history_ts_low to right above cutoff udt only if that would
  // increase it.
  if (cutoff_udt_.empty() ||
      (!full_history_ts_low.empty() &&
       ucmp->CompareTimestamp(cutoff_udt_, full_history_ts_low) < 0)) {
    return Status::OK();
  }
  std::string new_full_history_ts_low;
  Slice cutoff_udt_slice = cutoff_udt_;
  // TODO(yuzhangyu): Add a member to AdvancedColumnFamilyOptions for an
  //  operation to get the next immediately larger user-defined timestamp to
  //  expand this feature to other user-defined timestamp formats.
  GetFullHistoryTsLowFromU64CutoffTs(&cutoff_udt_slice,
                                     &new_full_history_ts_low);
  VersionEdit edit;
  edit.SetColumnFamily(cfd_->GetID());
  edit.SetFullHistoryTsLow(new_full_history_ts_low);
  return versions_->LogAndApply(cfd_, ReadOptions(Env::IOActivity::kFlush),
                                WriteOptions(Env::IOActivity::kFlush), &edit,
                                db_mutex_, output_file_directory_);
}

}  // namespace ROCKSDB_NAMESPACE
