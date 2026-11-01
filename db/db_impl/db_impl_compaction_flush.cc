//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <algorithm>
#include <cinttypes>
#include <deque>
#include <memory>
#include <unordered_map>

#include "db/blob/blob_file_partition_manager.h"
#include "db/builder.h"
#include "db/db_impl/db_impl.h"
#include "db/error_handler.h"
#include "db/event_helpers.h"
#include "file/file_util.h"
#include "file/sst_file_manager_impl.h"
#include "logging/logging.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/thread_status_updater.h"
#include "monitoring/thread_status_util.h"
#include "options/options_helper.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/concurrent_task_limiter_impl.h"
#include "util/defer.h"
#include "util/udt_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {

void RecordAtomicFlushRequestReason(Statistics* stats,
                                    FlushReason flush_reason) {
  // Keep this aligned with the existing rocksdb.flush.reason.* counters: they
  // cover automatic flush triggers used for write-stall debugging. Other
  // FlushReason values are counted by the catch-all other ticker.
  switch (flush_reason) {
    case FlushReason::kWriteBufferFull:
      RecordTick(stats, ATOMIC_FLUSH_REQUEST_REASON_WRITE_BUFFER_FULL);
      break;
    case FlushReason::kWriteBufferManager:
      RecordTick(stats, ATOMIC_FLUSH_REQUEST_REASON_WRITE_BUFFER_MANAGER);
      break;
    case FlushReason::kMemtableMaxRangeDeletions:
      RecordTick(stats,
                 ATOMIC_FLUSH_REQUEST_REASON_MEMTABLE_MAX_RANGE_DELETIONS);
      break;
    default:
      RecordTick(stats, ATOMIC_FLUSH_REQUEST_REASON_OTHER);
      break;
  }
}

std::vector<uint64_t> MemTableIds(const autovector<ReadOnlyMemTable*>& mems) {
  std::vector<uint64_t> ids;
  ids.reserve(mems.size());
  for (const ReadOnlyMemTable* mem : mems) {
    ids.push_back(mem->GetID());
  }
  return ids;
}

}  // namespace

bool DBImpl::EnoughRoomForCompaction(
    ColumnFamilyData* cfd, const std::vector<CompactionInputFiles>& inputs,
    bool* sfm_reserved_compact_space, LogBuffer* log_buffer) {
  // Check if we have enough room to do the compaction
  bool enough_room = true;
  auto sfm = static_cast<SstFileManagerImpl*>(
      immutable_db_options_.sst_file_manager.get());
  if (sfm) {
    // Pass the current bg_error_ to SFM so it can decide what checks to
    // perform. If this DB instance hasn't seen any error yet, the SFM can be
    // optimistic and not do disk space checks
    Status bg_error = error_handler_.GetBGError();
    enough_room = sfm->EnoughRoomForCompaction(cfd, inputs, bg_error);
    bg_error.PermitUncheckedError();  // bg_error is just a copy of the Status
                                      // from the error_handler_
    if (enough_room) {
      *sfm_reserved_compact_space = true;
    }
  }
  if (!enough_room) {
    // Just in case tests want to change the value of enough_room
    TEST_SYNC_POINT_CALLBACK(
        "DBImpl::BackgroundCompaction():CancelledCompaction", &enough_room);
    ROCKS_LOG_BUFFER(log_buffer,
                     "Cancelled compaction because not enough room");
    RecordTick(stats_, COMPACTION_CANCELLED, 1);
  }
  return enough_room;
}

bool DBImpl::RequestCompactionToken(ColumnFamilyData* cfd, bool force,
                                    std::unique_ptr<TaskLimiterToken>* token,
                                    LogBuffer* log_buffer) {
  assert(*token == nullptr);
  auto limiter = static_cast<ConcurrentTaskLimiterImpl*>(
      cfd->ioptions().compaction_thread_limiter.get());
  if (limiter == nullptr) {
    return true;
  }
  *token = limiter->GetToken(force);
  if (*token != nullptr) {
    ROCKS_LOG_BUFFER(log_buffer,
                     "Thread limiter [%s] increase [%s] compaction task, "
                     "force: %s, tasks after: %d",
                     limiter->GetName().c_str(), cfd->GetName().c_str(),
                     force ? "true" : "false", limiter->GetOutstandingTask());
    return true;
  }
  return false;
}

bool DBImpl::ShouldRescheduleFlushRequestToRetainUDT(
    const FlushRequest& flush_req) {
  mutex_.AssertHeld();
  assert(flush_req.cfd_to_max_mem_id_to_persist.size() == 1);
  ColumnFamilyData* cfd = flush_req.cfd_to_max_mem_id_to_persist.begin()->first;
  if (cfd->GetAndClearFlushSkipReschedule()) {
    return false;
  }
  uint64_t max_memtable_id =
      flush_req.cfd_to_max_mem_id_to_persist.begin()->second;
  if (cfd->IsDropped() ||
      !cfd->ShouldPostponeFlushToRetainUDT(max_memtable_id)) {
    return false;
  }
  // Check if holding on the flush will cause entering write stall mode.
  // Write stall entered because of the accumulation of write buffers can be
  // alleviated if we continue with the flush instead of postponing it.
  const auto& mutable_cf_options = cfd->GetLatestMutableCFOptions();

  // Use the same criteria as WaitUntilFlushWouldNotStallWrites does w.r.t
  // defining what a write stall is about to happen means. If this uses a
  // stricter criteria, for example, a write stall is about to happen if the
  // last memtable is 10% full, there is a possibility that manual flush could
  // be waiting in `WaitUntilFlushWouldNotStallWrites` with the incorrect
  // expectation that others will clear up the excessive memtables and
  // eventually let it proceed. The others in this case won't start clearing
  // until the last memtable is 10% full. To avoid that scenario, the criteria
  // this uses should be the same or less strict than
  // `WaitUntilFlushWouldNotStallWrites` does.
  WriteStallCondition write_stall =
      ColumnFamilyData::GetWriteStallConditionAndCause(
          cfd->GetUnflushedMemTableCountForWriteStallCheck(),
          /*num_l0_files=*/0,
          /*num_compaction_needed_bytes=*/0, mutable_cf_options,
          cfd->ioptions())
          .first;
  if (write_stall != WriteStallCondition::kNormal) {
    return false;
  }
  return true;
}

IOStatus DBImpl::SyncClosedWals(const WriteOptions& write_options,
                                JobContext* job_context,
                                VersionEdit* synced_wals,
                                bool error_recovery_in_prog) {
  TEST_SYNC_POINT("DBImpl::SyncClosedWals:Start");

  IOStatus io_s = SyncWalImpl(/*include_current_wal*/ false, write_options,
                              job_context, synced_wals, error_recovery_in_prog);
  if (!io_s.ok()) {
    TEST_SYNC_POINT("DBImpl::SyncClosedWals:Failed");
  } else {
    TEST_SYNC_POINT("DBImpl::SyncClosedWals:end");
  }
  return io_s;
}

Status DBImpl::FlushMemTableToOutputFile(
    ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    bool* made_progress, JobContext* job_context, FlushReason flush_reason,
    SuperVersionContext* superversion_context, LogBuffer* log_buffer,
    Env::Priority thread_pri) {
  mutex_.AssertHeld();
  assert(cfd);
  assert(cfd->imm());
  assert(cfd->imm()->NumNotFlushed() != 0);
  assert(cfd->imm()->IsFlushPending());
  assert(versions_);
  assert(versions_->GetColumnFamilySet());
  const ReadOptions read_options(Env::IOActivity::kFlush);
  const WriteOptions write_options(Env::IOActivity::kFlush);
  // If there are more than one column families, we need to make sure that
  // all the log files except the most recent one are synced. Otherwise if
  // the host crashes after flushing and before WAL is persistent, the
  // flushed SST may contain data from write batches whose updates to
  // other (unflushed) column families are missing.
  //
  // When 2PC is enabled, non-recent WAL(s) may be needed for crash-recovery,
  // even when there is only one CF in the DB, for prepared transactions that
  // had not been committed yet. Make sure we sync them to keep the persisted
  // WAL state at least as new as the persisted SST state.
  const bool needs_to_sync_closed_wals =
      cur_wal_number_ > 0 &&
      (versions_->GetColumnFamilySet()->NumberOfColumnFamilies() > 1 ||
       allow_2pc());

  // If needs_to_sync_closed_wals is true, we need to record the current
  // maximum memtable ID of this column family so that a later PickMemtables()
  // call will not pick memtables whose IDs are higher. This is due to the fact
  // that SyncClosedWals() may release the db mutex, and memtable switch can
  // happen for this column family in the meantime. The newly created memtables
  // have their data backed by unsynced WALs, thus they cannot be included in
  // this flush job.
  // Another reason why we must record the current maximum memtable ID of this
  // column family: SyncClosedWals() may release db mutex, thus it's possible
  // for application to continue to insert into memtables increasing db's
  // sequence number. The application may take a snapshot, but this snapshot is
  // not included in `snapshot_seqs` which will be passed to flush job because
  // `snapshot_seqs` has already been computed before this function starts.
  // Recording the max memtable ID ensures that the flush job does not flush
  // a memtable without knowing such snapshot(s).
  uint64_t max_memtable_id =
      needs_to_sync_closed_wals
          ? cfd->imm()->GetLatestMemTableID(false /* for_atomic_flush */)
          : std::numeric_limits<uint64_t>::max();

  // If needs_to_sync_closed_wals is false, the flush job picks all existing
  // immutable memtables before NotifyOnFlushBegin() releases the DB mutex.
  // Until that pick, the DB mutex freezes the eligible input boundary; after
  // the pick, later writes remain outside this flush. Transaction modes that
  // need an additional visibility boundary install a managed snapshot or a
  // published-sequence pin in InitSnapshotContext(). Together these boundaries
  // preserve snapshots created after the snapshot-list scan even though their
  // creation does not require the DB mutex.

  FlushJob flush_job(
      dbname_, cfd, immutable_db_options_, mutable_cf_options, max_memtable_id,
      file_options_for_compaction_, versions_.get(), &mutex_, &shutting_down_,
      job_context, flush_reason, log_buffer, directories_.GetDbDir(),
      GetDataDir(cfd, 0U),
      GetCompressionFlush(cfd->ioptions(), mutable_cf_options), stats_,
      &event_logger_, mutable_cf_options.report_bg_io_stats,
      true /* sync_output_directory */, true /* write_manifest */, thread_pri,
      io_tracer_, cfd->GetSuperVersion()->ShareSeqnoToTimeMapping(), db_id_,
      db_session_id_, cfd->GetFullHistoryTsLow(), &blob_callback_,
      mutable_db_options_.fast_sst_open);
  FileMetaData file_meta;

  Status s;
  bool need_cancel = false;
  IOStatus log_io_s = IOStatus::OK();
  if (needs_to_sync_closed_wals) {
    // SyncClosedWals() may unlock and re-lock the wal_write_mutex multiple
    // times.
    VersionEdit synced_wals;
    bool error_recovery_in_prog = error_handler_.IsRecoveryInProgress();
    mutex_.Unlock();
    log_io_s = SyncClosedWals(write_options, job_context, &synced_wals,
                              error_recovery_in_prog);
    mutex_.Lock();
    if (log_io_s.ok() && synced_wals.IsWalAddition()) {
      log_io_s = status_to_io_status(
          ApplyWALToManifest(read_options, write_options, &synced_wals));
      TEST_SYNC_POINT_CALLBACK("DBImpl::FlushMemTableToOutputFile:CommitWal:1",
                               nullptr);
    }

    if (!log_io_s.ok() && !log_io_s.IsShutdownInProgress() &&
        !log_io_s.IsColumnFamilyDropped()) {
      error_handler_.SetBGError(log_io_s, BackgroundErrorReason::kFlush);
    }
  } else {
    TEST_SYNC_POINT("DBImpl::SyncClosedWals:Skip");
  }
  s = log_io_s;

  // If the log sync failed, we do not need to pick memtable. Otherwise,
  // num_flush_not_started_ needs to be rollback.
  TEST_SYNC_POINT("DBImpl::FlushMemTableToOutputFile:BeforePickMemtables");
  // Exit a flush due to bg error should not set bg error again.
  bool skip_set_bg_error = false;
  if (s.ok() && !error_handler_.GetBGError().ok() &&
      error_handler_.IsBGWorkStopped() &&
      flush_reason != FlushReason::kErrorRecovery &&
      flush_reason != FlushReason::kErrorRecoveryRetryFlush) {
    // Error recovery in progress, should not pick memtable which excludes
    // them from being picked up by recovery flush.
    // This ensures that when bg error is set, no new flush can pick
    // memtables.
    skip_set_bg_error = true;
    s = error_handler_.GetBGError();
    assert(!s.ok());
    ROCKS_LOG_BUFFER(log_buffer,
                     "[JOB %d] Skip flush due to background error %s",
                     job_context->job_id, s.ToString().c_str());
  }

  if (s.ok()) {
    flush_job.PickMemTable();
    need_cancel = true;
  }
  TEST_SYNC_POINT_CALLBACK(
      "DBImpl::FlushMemTableToOutputFile:AfterPickMemtables", &flush_job);

  // may temporarily unlock and lock the mutex.
  NotifyOnFlushBegin(cfd, &file_meta, mutable_cf_options, job_context->job_id,
                     flush_reason);

  bool switched_to_mempurge = false;
  std::vector<uint64_t> prepared_blob_memtable_ids;
  // Within flush_job.Run, rocksdb may call event listener to notify
  // file creation and deletion.
  //
  // Note that flush_job.Run will unlock and lock the db_mutex,
  // and EventListener callback will be called when the db_mutex
  // is unlocked by the current thread.
  if (s.ok()) {
    auto mgr_handle = cfd->blob_partition_manager_handle();
    auto* mgr = mgr_handle.get();
    if (mgr != nullptr) {
      prepared_blob_memtable_ids = MemTableIds(flush_job.GetMemTables());
      std::vector<BlobFileAddition> write_path_additions;
      std::vector<BlobFileGarbage> write_path_garbages;
      std::vector<std::vector<uint64_t>> generation_blob_file_numbers;
      mutex_.Unlock();
      s = mgr->PrepareFlushAdditions(
          write_options, prepared_blob_memtable_ids, &write_path_additions,
          &write_path_garbages, &generation_blob_file_numbers);
      mutex_.Lock();
      if (!s.ok()) {
        prepared_blob_memtable_ids.clear();
      } else if (!write_path_additions.empty()) {
        const auto& memtables = flush_job.GetMemTables();
        assert(generation_blob_file_numbers.size() == memtables.size());
        for (size_t i = 0; i < generation_blob_file_numbers.size(); ++i) {
          // Old SuperVersions can keep these memtables alive after the flush
          // commits, so keep their sealed direct-write blob files protected
          // from obsolete-file purge until the memtable is finally released.
          memtables[i]->ProtectSealedBlobFiles(mgr_handle,
                                               generation_blob_file_numbers[i]);
        }
        flush_job.AddExternalBlobFileAdditions(std::move(write_path_additions));
        flush_job.AddExternalBlobFileGarbages(std::move(write_path_garbages));
      } else if (!write_path_garbages.empty()) {
        flush_job.AddExternalBlobFileGarbages(std::move(write_path_garbages));
      }
    }
  }
  if (s.ok()) {
    s = flush_job.Run(&logs_with_prep_tracker_, &file_meta,
                      &switched_to_mempurge, &skip_set_bg_error,
                      &error_handler_);
    need_cancel = false;
  }

  // Reserve a pending flush-completion notification before the DB mutex can be
  // released, so that a Flush(wait=true, listener_wait=true) caller cannot
  // observe this flush as finished until NotifyOnFlushCompleted() below has
  // run. flush_job.Run() removed the memtables from the immutable list (which
  // is what unblocks the waiter) and the DB mutex has been held continuously
  // since, so there is no window in which the waiter could see the flush
  // committed with a zero count. Reserved only when a notification will
  // actually fire (matches the NotifyOnFlushCompleted call below): flush
  // committed successfully, not a mempurge, listeners are registered, and this
  // job committed at least one flush result to report.
  const bool reserved_flush_notification =
      s.ok() && !switched_to_mempurge &&
      !immutable_db_options_.listeners.empty() &&
      !flush_job.GetCommittedFlushJobsInfo()->empty();
  if (reserved_flush_notification) {
    cfd->imm()->AddPendingFlushNotifications(1);
  }
  // Release the reservation on every exit path (RAII) so a future early return
  // cannot leak the count and hang listener_wait waiters. The DB mutex is held
  // throughout this function, including at scope exit, which
  // SubPendingFlushNotifications() requires. Runs after
  // NotifyOnFlushCompleted() below has returned.
  Defer release_flush_notification([&] {
    if (reserved_flush_notification) {
      cfd->imm()->SubPendingFlushNotifications(1);
    }
  });

  if (cfd->blob_partition_manager() != nullptr &&
      !prepared_blob_memtable_ids.empty()) {
    auto unconsumed_additions = flush_job.TakeExternalBlobFileAdditions();
    auto unconsumed_garbages = flush_job.TakeExternalBlobFileGarbages();
    if (!s.ok() || !unconsumed_additions.empty() ||
        !unconsumed_garbages.empty()) {
      // Skip committing this attempt, but keep the prepared generations queued
      // in pending_generations_. The same immutable memtables still reference
      // these sealed blob files, so a later flush retry must publish the exact
      // same additions rather than abandoning them here.
      prepared_blob_memtable_ids.clear();
    }
  }

  if (!s.ok() && need_cancel) {
    flush_job.Cancel();
  }

  if (s.ok() && !prepared_blob_memtable_ids.empty()) {
    cfd->blob_partition_manager()->CommitFlushedGenerations(
        cfd->imm()->GetEarliestMemTableID());
  }

  if (s.ok()) {
    InstallSuperVersionAndScheduleWork(cfd, superversion_context,
                                       /*defer_snapshot_refresh=*/false);
    if (made_progress) {
      *made_progress = true;
    }

    const std::string& column_family_name = cfd->GetName();

    Version* const current = cfd->current();
    assert(current);

    const VersionStorageInfo* const storage_info = current->storage_info();
    assert(storage_info);

    VersionStorageInfo::LevelSummaryStorage tmp;
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Level summary: %s\n",
                     column_family_name.c_str(),
                     storage_info->LevelSummary(&tmp));

    const auto& blob_files = storage_info->GetBlobFiles();
    if (!blob_files.empty()) {
      assert(blob_files.front());
      assert(blob_files.back());

      ROCKS_LOG_BUFFER(
          log_buffer,
          "[%s] Blob file summary: head=%" PRIu64 ", tail=%" PRIu64 "\n",
          column_family_name.c_str(), blob_files.front()->GetBlobFileNumber(),
          blob_files.back()->GetBlobFileNumber());
    }
  }

  if (!s.ok() && !s.IsShutdownInProgress() && !s.IsColumnFamilyDropped() &&
      !skip_set_bg_error) {
    if (log_io_s.ok()) {
      // Error while writing to MANIFEST.
      // In fact, versions_->io_status() can also be the result of renaming
      // CURRENT file. With current code, it's just difficult to tell. So just
      // be pessimistic and try write to a new MANIFEST.
      // TODO: distinguish between MANIFEST write and CURRENT renaming
      if (!versions_->io_status().ok()) {
        // If WAL sync is successful (either WAL size is 0 or there is no IO
        // error), all the Manifest write will be map to soft error.
        // TODO: kManifestWriteNoWAL and kFlushNoWAL are misleading. Refactor is
        // needed.
        error_handler_.SetBGError(s,
                                  BackgroundErrorReason::kManifestWriteNoWAL);
      } else {
        // If WAL sync is successful (either WAL size is 0 or there is no IO
        // error), all the other SST file write errors will be set as
        // kFlushNoWAL.
        error_handler_.SetBGError(s, BackgroundErrorReason::kFlushNoWAL);
      }
    } else {
      assert(s == log_io_s);
      Status new_bg_error = s;
      error_handler_.SetBGError(new_bg_error, BackgroundErrorReason::kFlush);
    }
  }
  // If flush ran smoothly and no mempurge happened
  // install new SST file path.
  if (s.ok() && (!switched_to_mempurge)) {
    // may temporarily unlock and lock the mutex.
    NotifyOnFlushCompleted(cfd, mutable_cf_options,
                           flush_job.GetCommittedFlushJobsInfo());
    auto sfm = static_cast<SstFileManagerImpl*>(
        immutable_db_options_.sst_file_manager.get());
    if (sfm) {
      // Notify sst_file_manager that a new file was added
      std::string file_path = MakeTableFileName(
          cfd->ioptions().cf_paths[0].path, file_meta.fd.GetNumber());
      // TODO (PR7798).  We should only add the file to the FileManager if it
      // exists. Otherwise, some tests may fail.  Ignore the error in the
      // interim.
      sfm->OnAddFile(file_path).PermitUncheckedError();
      if (sfm->IsMaxAllowedSpaceReached()) {
        Status new_bg_error =
            Status::SpaceLimit("Max allowed space was reached");
        TEST_SYNC_POINT_CALLBACK(
            "DBImpl::FlushMemTableToOutputFile:MaxAllowedSpaceReached",
            &new_bg_error);
        error_handler_.SetBGError(new_bg_error, BackgroundErrorReason::kFlush);
      }
    }
  }
  TEST_SYNC_POINT("DBImpl::FlushMemTableToOutputFile:Finish");
  return s;
}

Status DBImpl::FlushMemTablesToOutputFiles(
    const autovector<BGFlushArg>& bg_flush_args, bool* made_progress,
    JobContext* job_context, LogBuffer* log_buffer, Env::Priority thread_pri) {
#ifndef NDEBUG
  for (const auto& bg_flush_arg : bg_flush_args) {
    assert(bg_flush_arg.atomic_flush_ == bg_flush_args[0].atomic_flush_);
  }
#endif /* !NDEBUG */
  if (bg_flush_args[0].atomic_flush_) {
    return AtomicFlushMemTablesToOutputFiles(
        bg_flush_args, made_progress, job_context, log_buffer, thread_pri);
  }
  assert(bg_flush_args.size() == 1);
  InitSnapshotContext(job_context);

  const auto& bg_flush_arg = bg_flush_args[0];
  ColumnFamilyData* cfd = bg_flush_arg.cfd_;
  // intentional infrequent copy for each flush
  const MutableCFOptions mutable_cf_options_copy =
      cfd->GetLatestMutableCFOptions();
  SuperVersionContext* superversion_context =
      bg_flush_arg.superversion_context_;
  FlushReason flush_reason = bg_flush_arg.flush_reason_;
  Status s = FlushMemTableToOutputFile(
      cfd, mutable_cf_options_copy, made_progress, job_context, flush_reason,
      superversion_context, log_buffer, thread_pri);
  return s;
}

/*
 * Atomically flushes multiple column families.
 *
 * For each column family, all memtables with ID smaller than or equal to the
 * ID specified in bg_flush_args will be flushed. Only after all column
 * families finish flush will this function commit to MANIFEST. If any of the
 * column families are not flushed successfully, this function does not have
 * any side-effect on the state of the database.
 */
Status DBImpl::AtomicFlushMemTablesToOutputFiles(
    const autovector<BGFlushArg>& bg_flush_args, bool* made_progress,
    JobContext* job_context, LogBuffer* log_buffer, Env::Priority thread_pri) {
  mutex_.AssertHeld();
  const ReadOptions read_options(Env::IOActivity::kFlush);
  const WriteOptions write_options(Env::IOActivity::kFlush);

  autovector<ColumnFamilyData*> cfds;
  for (const auto& arg : bg_flush_args) {
    cfds.emplace_back(arg.cfd_);
  }

#ifndef NDEBUG
  for (const auto cfd : cfds) {
    assert(cfd->imm()->NumNotFlushed() != 0);
    assert(cfd->imm()->IsFlushPending());
  }
  for (const auto& bg_flush_arg : bg_flush_args) {
    assert(bg_flush_arg.flush_reason_ == bg_flush_args[0].flush_reason_);
  }
#endif /* !NDEBUG */

  InitSnapshotContext(job_context);
  autovector<FSDirectory*> distinct_output_dirs;
  autovector<std::string> distinct_output_dir_paths;
  std::vector<std::unique_ptr<FlushJob>> jobs;
  std::vector<MutableCFOptions> all_mutable_cf_options;
  int num_cfs = static_cast<int>(cfds.size());
  all_mutable_cf_options.reserve(num_cfs);
  for (int i = 0; i < num_cfs; ++i) {
    auto cfd = cfds[i];
    FSDirectory* data_dir = GetDataDir(cfd, 0U);
    const std::string& curr_path = cfd->ioptions().cf_paths[0].path;

    // Add to distinct output directories if eligible. Use linear search. Since
    // the number of elements in the vector is not large, performance should be
    // tolerable.
    bool found = false;
    for (const auto& path : distinct_output_dir_paths) {
      if (path == curr_path) {
        found = true;
        break;
      }
    }
    if (!found) {
      distinct_output_dir_paths.emplace_back(curr_path);
      distinct_output_dirs.emplace_back(data_dir);
    }

    all_mutable_cf_options.emplace_back(cfd->GetLatestMutableCFOptions());
    const MutableCFOptions& mutable_cf_options = all_mutable_cf_options.back();
    uint64_t max_memtable_id = bg_flush_args[i].max_memtable_id_;
    FlushReason flush_reason = bg_flush_args[i].flush_reason_;
    jobs.emplace_back(new FlushJob(
        dbname_, cfd, immutable_db_options_, mutable_cf_options,
        max_memtable_id, file_options_for_compaction_, versions_.get(), &mutex_,
        &shutting_down_, job_context, flush_reason, log_buffer,
        directories_.GetDbDir(), data_dir,
        GetCompressionFlush(cfd->ioptions(), mutable_cf_options), stats_,
        &event_logger_, mutable_cf_options.report_bg_io_stats,
        false /* sync_output_directory */, false /* write_manifest */,
        thread_pri, io_tracer_,
        cfd->GetSuperVersion()->ShareSeqnoToTimeMapping(), db_id_,
        db_session_id_, cfd->GetFullHistoryTsLow(), &blob_callback_,
        mutable_db_options_.fast_sst_open));
  }

  std::vector<FileMetaData> file_meta(num_cfs);
  // Use of deque<bool> because vector<bool>
  // is specific and doesn't allow &v[i].
  std::deque<bool> switched_to_mempurge(num_cfs, false);
  Status s;
  IOStatus log_io_s = IOStatus::OK();
  assert(num_cfs == static_cast<int>(jobs.size()));

  for (int i = 0; i != num_cfs; ++i) {
    const MutableCFOptions& mutable_cf_options = all_mutable_cf_options.at(i);
    // may temporarily unlock and lock the mutex.
    FlushReason flush_reason = bg_flush_args[i].flush_reason_;
    NotifyOnFlushBegin(cfds[i], &file_meta[i], mutable_cf_options,
                       job_context->job_id, flush_reason);
  }

  if (cur_wal_number_ > 0) {
    // TODO (yanqin) investigate whether we should sync the closed logs for
    // single column family case.
    VersionEdit synced_wals;
    bool error_recovery_in_prog = error_handler_.IsRecoveryInProgress();
    mutex_.Unlock();
    log_io_s = SyncClosedWals(write_options, job_context, &synced_wals,
                              error_recovery_in_prog);
    mutex_.Lock();
    if (log_io_s.ok() && synced_wals.IsWalAddition()) {
      log_io_s = status_to_io_status(
          ApplyWALToManifest(read_options, write_options, &synced_wals));
    }

    if (!log_io_s.ok() && !log_io_s.IsShutdownInProgress() &&
        !log_io_s.IsColumnFamilyDropped()) {
      if (wals_total_size_.LoadRelaxed() > 0) {
        error_handler_.SetBGError(log_io_s, BackgroundErrorReason::kFlush);
      } else {
        // If the WAL is empty, we use different error reason
        error_handler_.SetBGError(log_io_s, BackgroundErrorReason::kFlushNoWAL);
      }
    }
  }
  s = log_io_s;

  // exec_status stores the execution status of flush_jobs as
  // <bool /* executed */, Status /* status code */>
  autovector<std::pair<bool, Status>> exec_status;
  std::vector<bool> pick_status;
  std::vector<std::vector<uint64_t>> prepared_blob_memtable_ids(num_cfs);
  for (int i = 0; i != num_cfs; ++i) {
    // Initially all jobs are not executed, with status OK.
    exec_status.emplace_back(false, Status::OK());
    pick_status.push_back(false);
  }

  bool flush_for_recovery =
      bg_flush_args[0].flush_reason_ == FlushReason::kErrorRecovery ||
      bg_flush_args[0].flush_reason_ == FlushReason::kErrorRecoveryRetryFlush;
  bool skip_set_bg_error = false;

  if (s.ok() && !error_handler_.GetBGError().ok() &&
      error_handler_.IsBGWorkStopped() && !flush_for_recovery) {
    s = error_handler_.GetBGError();
    skip_set_bg_error = true;
    assert(!s.ok());
    ROCKS_LOG_BUFFER(log_buffer,
                     "[JOB %d] Skip flush due to background error %s",
                     job_context->job_id, s.ToString().c_str());
  }

  if (s.ok()) {
    for (int i = 0; i != num_cfs; ++i) {
      jobs[i]->PickMemTable();
      pick_status[i] = true;
    }
  }

  if (s.ok()) {
    for (int i = 0; i != num_cfs; ++i) {
      auto mgr_handle = cfds[i]->blob_partition_manager_handle();
      auto* mgr = mgr_handle.get();
      if (mgr == nullptr) {
        continue;
      }

      prepared_blob_memtable_ids[i] = MemTableIds(jobs[i]->GetMemTables());
      std::vector<BlobFileAddition> write_path_additions;
      std::vector<BlobFileGarbage> write_path_garbages;
      std::vector<std::vector<uint64_t>> generation_blob_file_numbers;
      mutex_.Unlock();
      s = mgr->PrepareFlushAdditions(
          write_options, prepared_blob_memtable_ids[i], &write_path_additions,
          &write_path_garbages, &generation_blob_file_numbers);
      mutex_.Lock();
      if (!s.ok()) {
        prepared_blob_memtable_ids[i].clear();
        break;
      }
      if (!write_path_additions.empty()) {
        const auto& memtables = jobs[i]->GetMemTables();
        assert(generation_blob_file_numbers.size() == memtables.size());
        for (size_t j = 0; j < generation_blob_file_numbers.size(); ++j) {
          memtables[j]->ProtectSealedBlobFiles(mgr_handle,
                                               generation_blob_file_numbers[j]);
        }
        jobs[i]->AddExternalBlobFileAdditions(std::move(write_path_additions));
      }
      if (!write_path_garbages.empty()) {
        jobs[i]->AddExternalBlobFileGarbages(std::move(write_path_garbages));
      }
    }
  }

  if (s.ok()) {
    assert(switched_to_mempurge.size() ==
           static_cast<long unsigned int>(num_cfs));
    // TODO (yanqin): parallelize jobs with threads.
    for (int i = 1; i != num_cfs; ++i) {
      exec_status[i].second =
          jobs[i]->Run(&logs_with_prep_tracker_, &file_meta[i],
                       &(switched_to_mempurge.at(i)));
      exec_status[i].first = true;
    }
    if (num_cfs > 1) {
      TEST_SYNC_POINT(
          "DBImpl::AtomicFlushMemTablesToOutputFiles:SomeFlushJobsComplete:1");
      TEST_SYNC_POINT(
          "DBImpl::AtomicFlushMemTablesToOutputFiles:SomeFlushJobsComplete:2");
    }
    assert(exec_status.size() > 0);
    assert(!file_meta.empty());
    exec_status[0].second = jobs[0]->Run(
        &logs_with_prep_tracker_, file_meta.data() /* &file_meta[0] */,
        switched_to_mempurge.empty() ? nullptr : &(switched_to_mempurge.at(0)));
    exec_status[0].first = true;

    Status error_status;
    for (const auto& e : exec_status) {
      if (!e.second.ok()) {
        s = e.second;
        if (!e.second.IsShutdownInProgress() &&
            !e.second.IsColumnFamilyDropped()) {
          // If a flush job did not return OK, and the CF is not dropped, and
          // the DB is not shutting down, then we have to return this result to
          // caller later.
          error_status = e.second;
        }
      }
    }

    s = error_status.ok() ? s : error_status;
  }

  if (s.IsColumnFamilyDropped()) {
    s = Status::OK();
  }

  if (s.ok() || s.IsShutdownInProgress()) {
    // Sync on all distinct output directories.
    for (auto dir : distinct_output_dirs) {
      if (dir != nullptr) {
        IOOptions io_options;
        Status error_status =
            WritableFileWriter::PrepareIOOptions(write_options, io_options);
        if (error_status.ok()) {
          error_status = dir->FsyncWithDirOptions(
              io_options, nullptr,
              DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
        }
        if (!error_status.ok()) {
          s = error_status;
          break;
        }
      }
    }
  } else if (!skip_set_bg_error) {
    // When `skip_set_bg_error` is true, no memtable is picked so
    // there is no need to call Cancel() or RollbackMemtableFlush().
    //
    // Need to undo atomic flush if something went wrong, i.e. s is not OK and
    // it is not because of CF drop.
    // Have to cancel the flush jobs that have NOT executed because we need to
    // unref the versions.
    for (int i = 0; i != num_cfs; ++i) {
      if (pick_status[i] && !exec_status[i].first) {
        jobs[i]->Cancel();
      }
    }
    for (int i = 0; i != num_cfs; ++i) {
      if (exec_status[i].second.ok() && exec_status[i].first) {
        auto& mems = jobs[i]->GetMemTables();
        cfds[i]->imm()->RollbackMemtableFlush(
            mems, /*rollback_succeeding_memtables=*/false);
      }
    }
  }

  if (s.ok()) {
    const auto wait_to_install_func =
        [&]() -> std::pair<Status, bool /*continue to wait*/> {
      if (!versions_->io_status().ok()) {
        // Something went wrong elsewhere, we cannot count on waiting for our
        // turn to write/sync to MANIFEST or CURRENT. Just return.
        return std::make_pair(versions_->io_status(), false);
      } else if (shutting_down_.load(std::memory_order_acquire)) {
        return std::make_pair(Status::ShutdownInProgress(), false);
      }
      bool ready = true;
      for (size_t i = 0; i != cfds.size(); ++i) {
        const auto& mems = jobs[i]->GetMemTables();
        if (cfds[i]->IsDropped()) {
          // If the column family is dropped, then do not wait.
          continue;
        } else if (!mems.empty() &&
                   cfds[i]->imm()->GetEarliestMemTableID() < mems[0]->GetID()) {
          // If a flush job needs to install the flush result for mems and
          // mems[0] is not the earliest memtable, it means another thread must
          // be installing flush results for the same column family, then the
          // current thread needs to wait.
          ready = false;
          break;
        } else if (mems.empty() && cfds[i]->imm()->GetEarliestMemTableID() <=
                                       bg_flush_args[i].max_memtable_id_) {
          // If a flush job does not need to install flush results, then it has
          // to wait until all memtables up to max_memtable_id_ (inclusive) are
          // installed.
          ready = false;
          break;
        }
      }
      return std::make_pair(Status::OK(), !ready);
    };

    bool resuming_from_bg_err =
        error_handler_.IsDBStopped() || flush_for_recovery;
    while ((!resuming_from_bg_err || error_handler_.GetRecoveryError().ok())) {
      std::pair<Status, bool> res = wait_to_install_func();

      TEST_SYNC_POINT_CALLBACK(
          "DBImpl::AtomicFlushMemTablesToOutputFiles:WaitToCommit", &res);

      if (!res.first.ok()) {
        s = res.first;
        break;
      } else if (!res.second) {
        // we are the oldest immutable memtable
        break;
      }
      // We are not the oldest immutable memtable
      TEST_SYNC_POINT_CALLBACK(
          "DBImpl::AtomicFlushMemTablesToOutputFiles:WaitCV", &res);
      //
      // If bg work is stopped, recovery thread first calls
      // WaitForBackgroundWork() before proceeding to flush for recovery. This
      // flush can block WaitForBackgroundWork() while waiting for recovery
      // flush to install result. To avoid this deadlock, we should abort here
      // if there is background error.
      if (!flush_for_recovery && error_handler_.IsBGWorkStopped() &&
          !error_handler_.GetBGError().ok()) {
        s = error_handler_.GetBGError();
        assert(!s.ok());
        break;
      }
      atomic_flush_install_cv_.Wait();

      resuming_from_bg_err = error_handler_.IsDBStopped() || flush_for_recovery;
    }

    if (!resuming_from_bg_err) {
      // If not resuming from bg err, then we determine future action based on
      // whether we hit background error.
      if (s.ok()) {
        s = error_handler_.GetBGError();
      }
    } else if (s.ok()) {
      // If resuming from bg err, we still rely on wait_to_install_func()'s
      // result to determine future action. If wait_to_install_func() returns
      // non-ok already, then we should not proceed to flush result
      // installation.
      s = error_handler_.GetRecoveryError();
    }
    // Since we are not installing these memtables, need to rollback
    // to allow future flush job to pick up these memtables.
    if (!s.ok()) {
      for (int i = 0; i != num_cfs; ++i) {
        assert(exec_status[i].first);
        assert(exec_status[i].second.ok());
        auto& mems = jobs[i]->GetMemTables();
        cfds[i]->imm()->RollbackMemtableFlush(
            mems, /*rollback_succeeding_memtables=*/false);
      }
    }
  }

  if (s.ok()) {
    autovector<ColumnFamilyData*> tmp_cfds;
    autovector<const autovector<ReadOnlyMemTable*>*> mems_list;
    autovector<FileMetaData*> tmp_file_meta;
    autovector<std::list<std::unique_ptr<FlushJobInfo>>*>
        committed_flush_jobs_info;
    for (int i = 0; i != num_cfs; ++i) {
      const auto& mems = jobs[i]->GetMemTables();
      if (!cfds[i]->IsDropped() && !mems.empty()) {
        tmp_cfds.emplace_back(cfds[i]);
        mems_list.emplace_back(&mems);
        tmp_file_meta.emplace_back(&file_meta[i]);
        committed_flush_jobs_info.emplace_back(
            jobs[i]->GetCommittedFlushJobsInfo());
      }
    }

    s = InstallMemtableAtomicFlushResults(
        nullptr /* imm_lists */, tmp_cfds, mems_list, versions_.get(),
        &logs_with_prep_tracker_, &mutex_, tmp_file_meta,
        committed_flush_jobs_info, &job_context->memtables_to_free,
        directories_.GetDbDir(), log_buffer);
  }

  // Reserve a pending flush-completion notification for every CF that the
  // NotifyOnFlushCompleted loop below will service, while the DB mutex is still
  // held continuously since InstallMemtableAtomicFlushResults removed the
  // memtables. This closes the window where a
  // Flush(wait=true, listener_wait=true) waiter could observe an atomic flush
  // as finished before a later CF's OnFlushCompleted has run, since the notify
  // loop releases and re-acquires the mutex between CFs. The reservations are
  // released by a matching pass after the notify loop.
  autovector<uint8_t> reserved_flush_notification;
  reserved_flush_notification.reserve(num_cfs);
  {
    const bool has_listeners = !immutable_db_options_.listeners.empty();
    for (int i = 0; i != num_cfs; ++i) {
      const bool reserve = s.ok() && has_listeners &&
                           !switched_to_mempurge[i] && !cfds[i]->IsDropped() &&
                           !jobs[i]->GetCommittedFlushJobsInfo()->empty();
      if (reserve) {
        cfds[i]->imm()->AddPendingFlushNotifications(1);
      }
      reserved_flush_notification.push_back(reserve ? 1 : 0);
    }
  }
  // Release the reservations on every exit path (RAII) so a future early return
  // cannot leak the count and hang listener_wait waiters. Done independently of
  // the notify loop's skips so the count stays balanced even if a CF was
  // dropped after it was reserved. The DB mutex is held throughout this
  // function, including at scope exit, which SubPendingFlushNotifications()
  // requires.
  Defer release_flush_notifications([&] {
    for (int i = 0; i != num_cfs; ++i) {
      if (reserved_flush_notification[i]) {
        cfds[i]->imm()->SubPendingFlushNotifications(1);
      }
    }
  });

  if (!s.ok()) {
    // If the atomic flush's combined MANIFEST write failed, the output files
    // are cached in the table cache (added by BuildTable during FlushJob::Run)
    // but never installed into any Version. Evict them now so that
    // TEST_VerifyNoObsoleteFilesCached does not fire during Close() when the
    // FindObsoleteFiles full-scan backstop is disabled by
    // metadata_read_fault_one_in.
    for (int i = 0; i != num_cfs; ++i) {
      if (exec_status[i].first && exec_status[i].second.ok() &&
          !switched_to_mempurge[i] && file_meta[i].fd.GetFileSize() > 0) {
        TableCache::ReleaseObsolete(
            cfds[i]->table_cache()->get_cache().get(),
            file_meta[i].fd.GetNumber(), nullptr /*handle*/,
            all_mutable_cf_options[i].uncache_aggressiveness);
      }
    }
  }

  for (int i = 0; i != num_cfs; ++i) {
    auto* mgr = cfds[i]->blob_partition_manager();
    if (mgr == nullptr || prepared_blob_memtable_ids[i].empty()) {
      continue;
    }

    auto unconsumed_additions = jobs[i]->TakeExternalBlobFileAdditions();
    auto unconsumed_garbages = jobs[i]->TakeExternalBlobFileGarbages();
    if (!s.ok() || !unconsumed_additions.empty() ||
        !unconsumed_garbages.empty()) {
      // Same retry semantics as the single-CF flush path above: do not
      // consume these prepared generations yet, because the rolled-back
      // memtables still need to reuse the same sealed blob files on retry.
      prepared_blob_memtable_ids[i].clear();
      continue;
    }

    mgr->CommitFlushedGenerations(cfds[i]->imm()->GetEarliestMemTableID());
  }

  if (s.ok()) {
    assert(num_cfs ==
           static_cast<int>(job_context->superversion_contexts.size()));
    for (int i = 0; i != num_cfs; ++i) {
      assert(cfds[i]);

      if (cfds[i]->IsDropped()) {
        continue;
      }
      InstallSuperVersionAndScheduleWork(cfds[i],
                                         &job_context->superversion_contexts[i],
                                         /*defer_snapshot_refresh=*/false);

      const std::string& column_family_name = cfds[i]->GetName();

      Version* const current = cfds[i]->current();
      assert(current);

      const VersionStorageInfo* const storage_info = current->storage_info();
      assert(storage_info);

      VersionStorageInfo::LevelSummaryStorage tmp;
      ROCKS_LOG_BUFFER(log_buffer, "[%s] Level summary: %s\n",
                       column_family_name.c_str(),
                       storage_info->LevelSummary(&tmp));

      const auto& blob_files = storage_info->GetBlobFiles();
      if (!blob_files.empty()) {
        assert(blob_files.front());
        assert(blob_files.back());

        ROCKS_LOG_BUFFER(
            log_buffer,
            "[%s] Blob file summary: head=%" PRIu64 ", tail=%" PRIu64 "\n",
            column_family_name.c_str(), blob_files.front()->GetBlobFileNumber(),
            blob_files.back()->GetBlobFileNumber());
      }
    }
    if (made_progress) {
      *made_progress = true;
    }
    auto sfm = static_cast<SstFileManagerImpl*>(
        immutable_db_options_.sst_file_manager.get());
    assert(all_mutable_cf_options.size() == static_cast<size_t>(num_cfs));
    for (int i = 0; s.ok() && i != num_cfs; ++i) {
      // If mempurge happened instead of Flush,
      // no NotifyOnFlushCompleted call (no SST file created).
      if (switched_to_mempurge[i]) {
        continue;
      }
      if (cfds[i]->IsDropped()) {
        continue;
      }
      NotifyOnFlushCompleted(cfds[i], all_mutable_cf_options[i],
                             jobs[i]->GetCommittedFlushJobsInfo());
      if (sfm) {
        std::string file_path = MakeTableFileName(
            cfds[i]->ioptions().cf_paths[0].path, file_meta[i].fd.GetNumber());
        // TODO (PR7798).  We should only add the file to the FileManager if it
        // exists. Otherwise, some tests may fail.  Ignore the error in the
        // interim.
        sfm->OnAddFile(file_path).PermitUncheckedError();
        if (sfm->IsMaxAllowedSpaceReached() &&
            error_handler_.GetBGError().ok()) {
          Status new_bg_error =
              Status::SpaceLimit("Max allowed space was reached");
          error_handler_.SetBGError(new_bg_error,
                                    BackgroundErrorReason::kFlush);
        }
      }
    }
  }

  // Need to undo atomic flush if something went wrong, i.e. s is not OK and
  // it is not because of CF drop.
  if (!s.ok() && !s.IsColumnFamilyDropped() && !skip_set_bg_error) {
    if (log_io_s.ok()) {
      // Error while writing to MANIFEST.
      // In fact, versions_->io_status() can also be the result of renaming
      // CURRENT file. With current code, it's just difficult to tell. So just
      // be pessimistic and try write to a new MANIFEST.
      // TODO: distinguish between MANIFEST write and CURRENT renaming
      if (!versions_->io_status().ok()) {
        // If WAL sync is successful (either WAL size is 0 or there is no IO
        // error), all the Manifest write will be map to soft error.
        // TODO: kManifestWriteNoWAL and kFlushNoWAL are misleading. Refactor
        // is needed.
        error_handler_.SetBGError(s,
                                  BackgroundErrorReason::kManifestWriteNoWAL);
      } else {
        // If WAL sync is successful (either WAL size is 0 or there is no IO
        // error), all the other SST file write errors will be set as
        // kFlushNoWAL.
        error_handler_.SetBGError(s, BackgroundErrorReason::kFlushNoWAL);
      }
    } else {
      assert(s == log_io_s);
      Status new_bg_error = s;
      error_handler_.SetBGError(new_bg_error, BackgroundErrorReason::kFlush);
    }
  }

  return s;
}

void DBImpl::NotifyOnFlushBegin(ColumnFamilyData* cfd, FileMetaData* file_meta,
                                const MutableCFOptions& mutable_cf_options,
                                int job_id, FlushReason flush_reason) {
  if (immutable_db_options_.listeners.size() == 0U) {
    return;
  }
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }
  bool triggered_writes_slowdown =
      (cfd->current()->storage_info()->NumLevelFiles(0) >=
       mutable_cf_options.level0_slowdown_writes_trigger);
  bool triggered_writes_stop =
      (cfd->current()->storage_info()->NumLevelFiles(0) >=
       mutable_cf_options.level0_stop_writes_trigger);
  // release lock while notifying events
  mutex_.Unlock();
  {
    FlushJobInfo info{};
    info.cf_id = cfd->GetID();
    info.cf_name = cfd->GetName();
    // TODO(yhchiang): make db_paths dynamic in case flush does not
    //                 go to L0 in the future.
    const uint64_t file_number = file_meta->fd.GetNumber();
    info.file_path =
        MakeTableFileName(cfd->ioptions().cf_paths[0].path, file_number);
    info.file_number = file_number;
    info.thread_id = env_->GetThreadID();
    info.job_id = job_id;
    info.triggered_writes_slowdown = triggered_writes_slowdown;
    info.triggered_writes_stop = triggered_writes_stop;
    info.smallest_seqno = file_meta->fd.smallest_seqno;
    info.largest_seqno = file_meta->fd.largest_seqno;
    info.flush_reason = flush_reason;
    for (const auto& listener : immutable_db_options_.listeners) {
      listener->OnFlushBegin(this, info);
    }
  }
  mutex_.Lock();
  // no need to signal bg_cv_ as it will be signaled at the end of the
  // flush process.
}

void DBImpl::NotifyOnFlushCompleted(
    ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    std::list<std::unique_ptr<FlushJobInfo>>* flush_jobs_info) {
  assert(flush_jobs_info != nullptr);
  if (immutable_db_options_.listeners.size() == 0U) {
    return;
  }
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }
  bool triggered_writes_slowdown =
      (cfd->current()->storage_info()->NumLevelFiles(0) >=
       mutable_cf_options.level0_slowdown_writes_trigger);
  bool triggered_writes_stop =
      (cfd->current()->storage_info()->NumLevelFiles(0) >=
       mutable_cf_options.level0_stop_writes_trigger);
  // release lock while notifying events
  mutex_.Unlock();
  {
    for (auto& info : *flush_jobs_info) {
      info->triggered_writes_slowdown = triggered_writes_slowdown;
      info->triggered_writes_stop = triggered_writes_stop;
      for (const auto& listener : immutable_db_options_.listeners) {
        listener->OnFlushCompleted(this, *info);
      }
      TEST_SYNC_POINT(
          "DBImpl::NotifyOnFlushCompleted::PostAllOnFlushCompleted");
    }
    flush_jobs_info->clear();
  }
  mutex_.Lock();
  // no need to signal bg_cv_ as it will be signaled at the end of the
  // flush process.
}

Status DBImpl::CompactRange(const CompactRangeOptions& options,
                            ColumnFamilyHandle* column_family,
                            const Slice* begin_without_ts,
                            const Slice* end_without_ts) {
  auto* cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(column_family)->cfd();
  assert(cfd);

  if (manual_compaction_paused_.load(std::memory_order_acquire) > 0) {
    return Status::Incomplete(Status::SubCode::kManualCompactionPaused);
  }

  if (IsCompactionAborted(cfd)) {
    return Status::Incomplete(Status::SubCode::kCompactionAborted);
  }

  if (options.canceled && options.canceled->load(std::memory_order_acquire)) {
    return Status::Incomplete(Status::SubCode::kManualCompactionPaused);
  }

  const Comparator* const ucmp = column_family->GetComparator();
  assert(ucmp);
  size_t ts_sz = ucmp->timestamp_size();
  if (ts_sz == 0) {
    return CompactRangeInternal(options, column_family, begin_without_ts,
                                end_without_ts, "" /*trim_ts*/);
  }

  std::string begin_str;
  std::string end_str;
  auto [begin, end] =
      MaybeAddTimestampsToRange(OptSlice::CopyFromPtr(begin_without_ts),
                                OptSlice::CopyFromPtr(end_without_ts), ts_sz,
                                &begin_str, &end_str, false /*exclusive_end*/);

  return CompactRangeInternal(
      options, column_family, begin.has_value() ? &begin.value() : nullptr,
      end.has_value() ? &end.value() : nullptr, "" /*trim_ts*/);
}

Status DBImpl::IncreaseFullHistoryTsLow(ColumnFamilyHandle* column_family,
                                        std::string ts_low) {
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
  if (cfd->user_comparator()->timestamp_size() != ts_low.size()) {
    return Status::InvalidArgument("ts_low size mismatch");
  }
  return IncreaseFullHistoryTsLowImpl(cfd, ts_low);
}

Status DBImpl::IncreaseFullHistoryTsLowImpl(ColumnFamilyData* cfd,
                                            std::string ts_low) {
  VersionEdit edit;
  edit.SetColumnFamily(cfd->GetID());
  edit.SetFullHistoryTsLow(ts_low);

  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;
  const WriteOptions write_options;

  TEST_SYNC_POINT_CALLBACK("DBImpl::IncreaseFullHistoryTsLowImpl:BeforeEdit",
                           &edit);

  InstrumentedMutexLock l(&mutex_);
  std::string current_ts_low = cfd->GetFullHistoryTsLow();
  const Comparator* ucmp = cfd->user_comparator();
  assert(ucmp->timestamp_size() == ts_low.size() && !ts_low.empty());
  if (!current_ts_low.empty() &&
      ucmp->CompareTimestamp(ts_low, current_ts_low) < 0) {
    std::stringstream oss;
    oss << "Current full_history_ts_low: "
        << ucmp->TimestampToString(current_ts_low)
        << " is higher than provided ts: " << ucmp->TimestampToString(ts_low)
        << std::endl;
    return Status::InvalidArgument(oss.str());
  }

  Status s = versions_->LogAndApply(cfd, read_options, write_options, &edit,
                                    &mutex_, directories_.GetDbDir());
  if (!s.ok()) {
    return s;
  }
  current_ts_low = cfd->GetFullHistoryTsLow();
  if (!current_ts_low.empty() &&
      ucmp->CompareTimestamp(current_ts_low, ts_low) > 0) {
    std::stringstream oss;
    oss << "full_history_ts_low: " << Slice(current_ts_low).ToString(true)
        << " is set to be higher than the requested "
           "timestamp: "
        << Slice(ts_low).ToString(true) << std::endl;
    return Status::TryAgain(oss.str());
  }
  return Status::OK();
}

Status DBImpl::CompactRangeInternal(const CompactRangeOptions& options,
                                    ColumnFamilyHandle* column_family,
                                    const Slice* begin, const Slice* end,
                                    const std::string& trim_ts) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();

  if (options.target_path_id >= cfd->ioptions().cf_paths.size()) {
    return Status::InvalidArgument("Invalid target path ID");
  }
  if (options.change_level &&
      cfd->ioptions().compaction_style == kCompactionStyleFIFO) {
    return Status::NotSupported(
        "FIFO compaction does not support change_level.");
  }

  bool flush_needed = true;

  // Update full_history_ts_low if it's set
  if (options.full_history_ts_low != nullptr &&
      !options.full_history_ts_low->empty()) {
    std::string ts_low = options.full_history_ts_low->ToString();
    if (begin != nullptr || end != nullptr) {
      return Status::InvalidArgument(
          "Cannot specify compaction range with full_history_ts_low");
    }
    Status s = IncreaseFullHistoryTsLowImpl(cfd, ts_low);
    if (!s.ok()) {
      LogFlush(immutable_db_options_.info_log);
      return s;
    }
  }

  Status s;
  if (begin != nullptr && end != nullptr) {
    // TODO(ajkr): We could also optimize away the flush in certain cases where
    // one/both sides of the interval are unbounded. But it requires more
    // changes to RangesOverlapWithMemtables.
    UserKeyRange range(*begin, *end);
    SuperVersion* super_version = cfd->GetReferencedSuperVersion(this);
    s = cfd->RangesOverlapWithMemtables(
        {range}, super_version, immutable_db_options_.allow_data_in_errors,
        &flush_needed, /*range_limit_exclusive=*/false);
    CleanupSuperVersion(super_version);
  }

  if (s.ok() && flush_needed) {
    FlushOptions fo;
    fo.allow_write_stall = options.allow_write_stall;
    if (immutable_db_options_.atomic_flush) {
      s = AtomicFlushMemTables(fo, FlushReason::kManualCompaction);
      if (s.ok() && cfd->IsDropped()) {
        s = Status::ColumnFamilyDropped();
      }
    } else {
      s = FlushMemTable(cfd, fo, FlushReason::kManualCompaction);
    }
    if (!s.ok()) {
      LogFlush(immutable_db_options_.info_log);
      return s;
    }
  }

  constexpr int kInvalidLevel = -1;
  int final_output_level = kInvalidLevel;
  bool exclusive = options.exclusive_manual_compaction;
  if (cfd->ioptions().compaction_style == kCompactionStyleUniversal &&
      cfd->NumberLevels() > 1) {
    // Always compact all files together.
    final_output_level = cfd->NumberLevels() - 1;
    if (cfd->AllowIngestBehind()) {
      final_output_level--;
    }
    s = RunManualCompaction(cfd, ColumnFamilyData::kCompactAllLevels,
                            final_output_level, options, begin, end, exclusive,
                            false /* disable_trivial_move */,
                            std::numeric_limits<uint64_t>::max(), trim_ts);
  } else if (cfd->ioptions().compaction_style == kCompactionStyleFIFO) {
    // FIFOCompactionPicker::CompactRange() will ignore the input key range
    // [begin, end] and just try to pick compaction based on the configured
    // option `compaction_options_fifo`. So we skip checking if [begin, end]
    // overlaps with the DB here.
    final_output_level = 0;
    s = RunManualCompaction(cfd, /*input_level=*/0, final_output_level, options,
                            begin, end, exclusive,
                            false /* disable_trivial_move */,
                            std::numeric_limits<uint64_t>::max(), trim_ts);
  } else {
    int first_overlapped_level = kInvalidLevel;
    {
      SuperVersion* super_version = cfd->GetReferencedSuperVersion(this);
      Version* current_version = super_version->current;

      // Might need to query the partitioner
      SstPartitionerFactory* partitioner_factory =
          current_version->cfd()->ioptions().sst_partitioner_factory.get();
      std::unique_ptr<SstPartitioner> partitioner;
      if (partitioner_factory && begin != nullptr && end != nullptr) {
        SstPartitioner::Context context;
        context.is_full_compaction = false;
        context.is_manual_compaction = true;
        context.output_level = /*unknown*/ -1;
        // Small lies about compaction range
        context.smallest_user_key = *begin;
        context.largest_user_key = *end;
        partitioner = partitioner_factory->CreatePartitioner(context);
      }

      ReadOptions ro;
      ro.total_order_seek = true;
      ro.io_activity = Env::IOActivity::kCompaction;
      bool overlap;
      for (int level = 0;
           level < current_version->storage_info()->num_non_empty_levels();
           level++) {
        overlap = true;

        // Whether to look at specific keys within files for overlap with
        // compaction range, other than largest and smallest keys of the file
        // known in Version metadata.
        bool check_overlap_within_file = false;
        if (begin != nullptr && end != nullptr) {
          // Typically checking overlap within files in this case
          check_overlap_within_file = true;
          // WART: Not known why we don't check within file in one-sided bound
          // cases
          if (partitioner) {
            // Especially if the partitioner is new, the manual compaction
            // might be used to enforce the partitioning. Checking overlap
            // within files might miss cases where compaction is needed to
            // partition the files, as in this example:
            // * File has two keys "001" and "111"
            // * Compaction range is ["011", "101")
            // * Partition boundary at "100"
            // In cases like this, file-level overlap with the compaction
            // range is sufficient to force any partitioning that is needed
            // within the compaction range.
            //
            // But if there's no partitioning boundary within the compaction
            // range, we can be sure there's no need to fix partitioning
            // within that range, thus safe to check overlap within file.
            //
            // Use a hypothetical trivial move query to check for partition
            // boundary in range. (NOTE: in defiance of all conventions,
            // `begin` and `end` here are both INCLUSIVE bounds, which makes
            // this analogy to CanDoTrivialMove() accurate even when `end` is
            // the first key in a partition.)
            if (!partitioner->CanDoTrivialMove(*begin, *end)) {
              check_overlap_within_file = false;
            }
          }
        }
        if (check_overlap_within_file) {
          Status status = current_version->OverlapWithLevelIterator(
              ro, file_options_, *begin, *end, level, &overlap);
          if (!status.ok()) {
            check_overlap_within_file = false;
          }
        }
        if (!check_overlap_within_file) {
          overlap = current_version->storage_info()->OverlapInLevel(level,
                                                                    begin, end);
        }
        if (overlap) {
          first_overlapped_level = level;
          break;
        }
      }
      CleanupSuperVersion(super_version);
    }
    if (s.ok() && first_overlapped_level != kInvalidLevel) {
      if (cfd->ioptions().compaction_style == kCompactionStyleUniversal) {
        assert(first_overlapped_level == 0);
        s = RunManualCompaction(
            cfd, first_overlapped_level, first_overlapped_level, options, begin,
            end, exclusive, true /* disallow_trivial_move */,
            std::numeric_limits<uint64_t>::max() /* max_file_num_to_ignore */,
            trim_ts);
        final_output_level = first_overlapped_level;
      } else {
        assert(cfd->ioptions().compaction_style == kCompactionStyleLevel);
        uint64_t next_file_number = versions_->current_next_file_number();
        // Start compaction from `first_overlapped_level`, one level down at a
        // time, until output level >= max_overlapped_level.
        // When max_overlapped_level == 0, we will still compact from L0 -> L1
        // (or LBase), and followed by a bottommost level intra-level compaction
        // at L1 (or LBase), if applicable.
        int level = first_overlapped_level;
        final_output_level = level;
        int output_level = 0;
        int base_level = 0;
        for (;;) {
          // Always allow L0 -> L1 compaction
          if (level > 0) {
            if (cfd->ioptions().level_compaction_dynamic_level_bytes) {
              assert(final_output_level < cfd->ioptions().num_levels);
              if (final_output_level + 1 == cfd->ioptions().num_levels) {
                break;
              }
            } else {
              // TODO(cbi): there is still a race condition here where
              //  if a background compaction compacts some file beyond
              //  current()->storage_info()->num_non_empty_levels() right after
              //  the check here.This should happen very infrequently and should
              //  not happen once a user populates the last level of the LSM.
              InstrumentedMutexLock l(&mutex_);
              // num_non_empty_levels may be lower after a compaction, so
              // we check for >= here.
              if (final_output_level + 1 >=
                  cfd->current()->storage_info()->num_non_empty_levels()) {
                break;
              }
            }
          }
          output_level = level + 1;
          if (cfd->ioptions().level_compaction_dynamic_level_bytes &&
              level == 0) {
            output_level = ColumnFamilyData::kCompactToBaseLevel;
          }
          // Use max value for `max_file_num_to_ignore` to always compact
          // files down.
          s = RunManualCompaction(
              cfd, level, output_level, options, begin, end, exclusive,
              !trim_ts.empty() /* disallow_trivial_move */,
              std::numeric_limits<uint64_t>::max() /* max_file_num_to_ignore */,
              trim_ts,
              output_level == ColumnFamilyData::kCompactToBaseLevel
                  ? &base_level
                  : nullptr);
          if (!s.ok()) {
            break;
          }
          if (output_level == ColumnFamilyData::kCompactToBaseLevel) {
            assert(base_level > 0);
            level = base_level;
          } else {
            ++level;
          }
          final_output_level = level;
          TEST_SYNC_POINT("DBImpl::RunManualCompaction()::1");
          TEST_SYNC_POINT("DBImpl::RunManualCompaction()::2");
        }
        if (s.ok()) {
          assert(final_output_level > 0);
          // bottommost level intra-level compaction
          if ((options.bottommost_level_compaction ==
                   BottommostLevelCompaction::kIfHaveCompactionFilter &&
               (cfd->ioptions().compaction_filter != nullptr ||
                cfd->ioptions().compaction_filter_factory != nullptr)) ||
              options.bottommost_level_compaction ==
                  BottommostLevelCompaction::kForceOptimized ||
              options.bottommost_level_compaction ==
                  BottommostLevelCompaction::kForce) {
            // Use `next_file_number` as `max_file_num_to_ignore` to avoid
            // rewriting newly compacted files when it is kForceOptimized
            // or kIfHaveCompactionFilter with compaction filter set.
            s = RunManualCompaction(
                cfd, final_output_level, final_output_level, options, begin,
                end, exclusive, true /* disallow_trivial_move */,
                next_file_number /* max_file_num_to_ignore */, trim_ts);
          }
        }
      }
    }
  }
  if (!s.ok() || final_output_level == kInvalidLevel) {
    LogFlush(immutable_db_options_.info_log);
    return s;
  }

  if (options.change_level) {
    TEST_SYNC_POINT("DBImpl::CompactRange:BeforeRefit:1");
    TEST_SYNC_POINT("DBImpl::CompactRange:BeforeRefit:2");

    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[RefitLevel] waiting for background threads to stop");
    // TODO(hx235): remove `Enable/DisableManualCompaction` and
    // `Continue/PauseBackgroundWork` once we ensure registering RefitLevel()'s
    // range is sufficient (if not, what else is needed) for avoiding range
    // conflicts with other activities (e.g, compaction, flush) that are
    // currently avoided by `Enable/DisableManualCompaction` and
    // `Continue/PauseBackgroundWork`.
    DisableManualCompaction();
    s = PauseBackgroundWork();
    if (s.ok()) {
      TEST_SYNC_POINT("DBImpl::CompactRange:PreRefitLevel");
      s = ReFitLevel(cfd, final_output_level, options.target_level);
      TEST_SYNC_POINT("DBImpl::CompactRange:PostRefitLevel");
      // ContinueBackgroundWork always return Status::OK().
      Status temp_s = ContinueBackgroundWork();
      assert(temp_s.ok());
    }
    EnableManualCompaction();
    TEST_SYNC_POINT(
        "DBImpl::CompactRange:PostRefitLevel:ManualCompactionEnabled");
  }
  LogFlush(immutable_db_options_.info_log);

  {
    InstrumentedMutexLock l(&mutex_);
    // an automatic compaction that has been scheduled might have been
    // preempted by the manual compactions. Need to schedule it back.
    MaybeScheduleFlushOrCompaction();
  }

  return s;
}

Status DBImpl::CompactFiles(const CompactionOptions& compact_options,
                            ColumnFamilyHandle* column_family,
                            const std::vector<std::string>& input_file_names,
                            const int output_level, const int output_path_id,
                            std::vector<std::string>* const output_file_names,
                            CompactionJobInfo* compaction_job_info) {
  if (column_family == nullptr) {
    return Status::InvalidArgument("ColumnFamilyHandle must be non-null.");
  }

  auto cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(column_family)->cfd();
  assert(cfd);

  {
    InstrumentedMutexLock l(&mutex_);
    BeginInFlightCompaction(cfd);
  }
  Defer end_in_flight_compaction([&]() {
    InstrumentedMutexLock l(&mutex_);
    // The caller holds a live handle to this CF, so this can never be the final
    // unref (the return can only be true when the last reference is dropped).
    [[maybe_unused]] const bool cfd_deleted = EndInFlightCompaction(cfd);
    assert(!cfd_deleted);
    if (per_cf_compaction_abort_waiters_ > 0) {
      bg_cv_.SignalAll();
    }
  });

  Status s;
  JobContext job_context(next_job_id_.fetch_add(1), true);
  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());

  if (compact_options.compression !=
      CompressionType::kDisableCompressionOption) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "[%s] [JOB %d] Found use of deprecated option "
                   "`CompactionOptions::compression`",
                   cfd->GetName().c_str(), job_context.job_id);
  }

  // Perform CompactFiles
  TEST_SYNC_POINT("TestCompactFiles::IngestExternalFile2");
  TEST_SYNC_POINT_CALLBACK("TestCompactFiles:PausingManualCompaction:3",
                           static_cast<void*>(const_cast<std::atomic<int>*>(
                               &manual_compaction_paused_)));
  TEST_SYNC_POINT_CALLBACK("TestCancelCompactFiles:SuccessfulCompaction",
                           static_cast<void*>(const_cast<std::atomic<int>*>(
                               &manual_compaction_paused_)));
  {
    InstrumentedMutexLock l(&mutex_);
    auto* current = cfd->current();
    current->Ref();

    s = CompactFilesImpl(compact_options, cfd, current, input_file_names,
                         output_file_names, output_level, output_path_id,
                         &job_context, &log_buffer, compaction_job_info);

    current->Unref();
  }

  // Find and delete obsolete files
  {
    InstrumentedMutexLock l(&mutex_);
    // If !s.ok(), this means that Compaction failed. In that case, we want
    // to delete all obsolete files we might have created and we force
    // FindObsoleteFiles(). This is because job_context does not
    // catch all created files if compaction failed.
    FindObsoleteFiles(&job_context, !s.ok());
  }  // release the mutex

  // delete unnecessary files if any, this is done outside the mutex
  if (job_context.HaveSomethingToClean() ||
      job_context.HaveSomethingToDelete() || !log_buffer.IsEmpty()) {
    // Have to flush the info logs before bg_compaction_scheduled_--
    // because if bg_flush_scheduled_ becomes 0 and the lock is
    // released, the deconstructor of DB can kick in and destroy all the
    // states of DB so info_log might not be available after that point.
    // It also applies to access other states that DB owns.
    log_buffer.FlushBufferToLog();
    if (job_context.HaveSomethingToDelete()) {
      // no mutex is locked here.  No need to Unlock() and Lock() here.
      PurgeObsoleteFiles(job_context);
    }
    job_context.Clean();
  }

  return s;
}

void DBImpl::PrepareTrivialMoveEdit(Compaction& c, LogBuffer* log_buffer,
                                    size_t& moved_files, size_t& moved_bytes) {
  mutex_.AssertHeld();

  ROCKS_LOG_BUFFER(log_buffer, "[%s] Moving %d files to level-%d\n",
                   c.column_family_data()->GetName().c_str(),
                   static_cast<int>(c.num_input_files(0)), c.output_level());

  // Move files to the output level by editing the manifest
  for (unsigned int l = 0; l < c.num_input_levels(); l++) {
    if (c.level(l) == c.output_level()) {
      continue;
    }
    for (size_t i = 0; i < c.num_input_files(l); i++) {
      FileMetaData* f = c.input(l, i);
      c.edit()->DeleteFile(c.level(l), f->fd.GetNumber());
      c.edit()->AddFile(
          c.output_level(), f->fd.GetNumber(), f->fd.GetPathId(),
          f->fd.GetFileSize(), f->smallest, f->largest, f->fd.smallest_seqno,
          f->fd.largest_seqno, f->marked_for_compaction, f->temperature,
          f->oldest_blob_file_number, f->oldest_ancestor_time,
          f->file_creation_time, f->epoch_number, f->file_checksum,
          f->file_checksum_func_name, f->unique_id,
          f->compensated_range_deletion_size, f->tail_size,
          f->user_defined_timestamps_persisted, f->min_timestamp,
          f->max_timestamp, f->file_open_metadata);
      moved_bytes += static_cast<size_t>(c.input(l, i)->fd.GetFileSize());
      ROCKS_LOG_BUFFER(
          log_buffer, "[%s] Moved #%" PRIu64 " to level-%d %" PRIu64 " bytes\n",
          c.column_family_data()->GetName().c_str(), f->fd.GetNumber(),
          c.output_level(), f->fd.GetFileSize());
    }
    moved_files += c.num_input_files(l);
  }
}

Status DBImpl::CommitTrivialMove(Compaction& c, bool& compaction_released) {
  mutex_.AssertHeld();

  // Install the new version
  const ReadOptions read_options(Env::IOActivity::kCompaction);
  const WriteOptions write_options(Env::IOActivity::kCompaction);
  Status status = versions_->LogAndApply(
      c.column_family_data(), read_options, write_options, c.edit(), &mutex_,
      directories_.GetDbDir(), /*new_descriptor_log=*/false,
      /*column_family_options=*/nullptr,
      [&c, &compaction_released](const Status& s) {
        c.ReleaseCompactionFiles(s);
        compaction_released = true;
      });

  return status;
}

Status DBImpl::CompactFilesImpl(
    const CompactionOptions& compact_options, ColumnFamilyData* cfd,
    Version* version, const std::vector<std::string>& input_file_names,
    std::vector<std::string>* const output_file_names, const int output_level,
    int output_path_id, JobContext* job_context, LogBuffer* log_buffer,
    CompactionJobInfo* compaction_job_info) {
  mutex_.AssertHeld();

  if (shutting_down_.load(std::memory_order_acquire)) {
    return Status::ShutdownInProgress();
  }

  if (IsCompactionAborted(cfd)) {
    return Status::Incomplete(Status::SubCode::kCompactionAborted);
  }

  // triggered by DisableManualCompactions or by user-set canceled flag in
  // CompactionOptions
  if (manual_compaction_paused_.load(std::memory_order_acquire) > 0 ||
      (compact_options.canceled &&
       compact_options.canceled->load(std::memory_order_acquire))) {
    return Status::Incomplete(Status::SubCode::kManualCompactionPaused);
  }

  std::unordered_set<uint64_t> input_set;
  for (const auto& file_name : input_file_names) {
    input_set.insert(TableFileNameToNumber(file_name));
  }

  if (output_path_id < 0) {
    if (cfd->ioptions().cf_paths.size() == 1U) {
      output_path_id = 0;
    } else {
      return Status::NotSupported(
          "Automatic output path selection is not "
          "yet supported in CompactFiles()");
    }
  }

  if (cfd->AllowIngestBehind() &&
      output_level >= cfd->ioptions().num_levels - 1) {
    return Status::InvalidArgument(
        "Exceed the maximum output level defined by "
        "the current compaction algorithm with ingest_behind --- " +
        std::to_string(cfd->ioptions().num_levels - 1));
  }

  std::vector<CompactionInputFiles> input_files;
  Status s = cfd->compaction_picker()->SanitizeAndConvertCompactionInputFiles(
      &input_set, output_level, version, &input_files);
  TEST_SYNC_POINT(
      "DBImpl::CompactFilesImpl::PostSanitizeAndConvertCompactionInputFiles");
  if (!s.ok()) {
    return s;
  }

  for (const auto& inputs : input_files) {
    if (cfd->compaction_picker()->AreFilesInCompaction(inputs.files)) {
      return Status::Aborted(
          "Some of the necessary compaction input "
          "files are already being compacted");
    }
  }
  bool sfm_reserved_compact_space = false;
  // First check if we have enough room to do the compaction
  bool enough_room = EnoughRoomForCompaction(
      cfd, input_files, &sfm_reserved_compact_space, log_buffer);

  if (!enough_room) {
    // m's vars will get set properly at the end of this function,
    // as long as status == CompactionTooLarge
    return Status::CompactionTooLarge();
  }

  // At this point, CompactFiles will be run.
  const bool separate_remote_pool = UseSeparateRemoteCompactionPool();
  int& scheduled_compactions = separate_remote_pool
                                   ? remote_coordinators_scheduled_
                                   : bg_compaction_scheduled_;
  ++scheduled_compactions;

  std::unique_ptr<Compaction> c;
  assert(cfd->compaction_picker());
  c.reset(cfd->compaction_picker()->PickCompactionForCompactFiles(
      compact_options, input_files, output_level, version->storage_info(),
      cfd->GetLatestMutableCFOptions(), mutable_db_options_, output_path_id));
  // we already sanitized the set of input files and checked for conflicts
  // without releasing the lock, so we're guaranteed a compaction can be formed.
  assert(c != nullptr);

  c->FinalizeInputInfo(version);

  // deletion compaction currently not allowed in CompactFiles.
  assert(!c->deletion_compaction());

  // Check if this can be a trivial move (metadata-only update)
  // Similar to the logic in DBImpl::BackgroundCompaction
  // Note: We disable trivial move when compaction_service is present because
  // the service expects all compactions to go through CompactionJob for
  // tracking
  bool is_trivial_move = compact_options.allow_trivial_move &&
                         c->IsTrivialMove() &&
                         immutable_db_options().compaction_service == nullptr;

  if (is_trivial_move) {
    // Perform trivial move: just update manifest without rewriting data
    TEST_SYNC_POINT("DBImpl::CompactFilesImpl:TrivialMove");

    bool compaction_released = false;
    size_t moved_files = 0;
    size_t moved_bytes = 0;
    PrepareTrivialMoveEdit(*c.get(), log_buffer, moved_files, moved_bytes);
    Status status = CommitTrivialMove(*c.get(), compaction_released);

    if (status.ok()) {
      InstallSuperVersionAndScheduleWork(
          c->column_family_data(), job_context->superversion_contexts.data(),
          /*defer_snapshot_refresh=*/false);

      // Populate output file names for trivial move
      if (output_file_names != nullptr) {
        for (const auto& newf : c->edit()->GetNewFiles()) {
          output_file_names->push_back(TableFileName(
              c->immutable_options().cf_paths, newf.second.fd.GetNumber(),
              newf.second.fd.GetPathId()));
        }
      }

      ROCKS_LOG_BUFFER(
          log_buffer,
          "[%s] Trivial move succeeded for %zu files, %zu bytes total\n",
          c->column_family_data()->GetName().c_str(), moved_files, moved_bytes);
    } else {
      if (!compaction_released) {
        c->ReleaseCompactionFiles(status);
      }
      ROCKS_LOG_BUFFER(log_buffer, "[%s] Trivial move failed: %s\n",
                       c->column_family_data()->GetName().c_str(),
                       status.ToString().c_str());
      error_handler_.SetBGError(status, BackgroundErrorReason::kCompaction);
    }

    c.reset();
    --scheduled_compactions;
    if (separate_remote_pool || bg_compaction_scheduled_ == 0 ||
        per_cf_compaction_abort_waiters_ > 0) {
      bg_cv_.SignalAll();
    }
    MaybeScheduleFlushOrCompaction();

    return status;
  }

  // Not a trivial move, proceed with full compaction
  InitSnapshotContext(job_context);

  std::unique_ptr<std::list<uint64_t>::iterator> pending_outputs_inserted_elem(
      new std::list<uint64_t>::iterator(
          CaptureCurrentFileNumberInPendingOutputs()));

  assert(is_snapshot_supported_ || snapshots_.empty());
  CompactionJobStats compaction_job_stats;
  CompactionJob compaction_job(
      job_context->job_id, c.get(), immutable_db_options_, mutable_db_options_,
      file_options_for_compaction_, versions_.get(), &shutting_down_,
      log_buffer, directories_.GetDbDir(),
      GetDataDir(c->column_family_data(), c->output_path_id()),
      GetDataDir(c->column_family_data(), 0), stats_, &mutex_, &error_handler_,
      job_context, table_cache_, &event_logger_,
      c->mutable_cf_options().paranoid_file_checks,
      c->mutable_cf_options().report_bg_io_stats, dbname_,
      &compaction_job_stats, Env::Priority::USER, io_tracer_,
      kManualCompactionCanceledFalse_, compaction_aborted_, db_id_,
      db_session_id_, c->column_family_data()->GetFullHistoryTsLow(),
      c->trim_ts(), &blob_callback_, &bg_compaction_scheduled_,
      &bg_bottom_compaction_scheduled_, &num_running_remote_compactions_);
  ConfigureCompactionExecutors(&compaction_job, c->column_family_data(),
                               kManualCompactionCanceledFalse_, false,
                               separate_remote_pool);
  c->SetUseSeparateRemoteCompactionPool(separate_remote_pool);

  // Creating a compaction influences the compaction score because the score
  // takes running compactions into account (by skipping files that are already
  // being compacted). Since we just changed compaction score, we recalculate it
  // here.
  version->storage_info()->ComputeCompactionScore(
      cfd->ioptions(), c->mutable_cf_options(), cfd->GetFullHistoryTsLow());

  compaction_job.Prepare(std::nullopt /*subcompact to be computed*/);

  std::unique_ptr<std::list<uint64_t>::iterator> min_options_file_number_elem;
  if (immutable_db_options().compaction_service != nullptr) {
    min_options_file_number_elem.reset(
        new std::list<uint64_t>::iterator(CaptureOptionsFileNumber()));
  }

  mutex_.Unlock();
  TEST_SYNC_POINT("CompactFilesImpl:0");
  TEST_SYNC_POINT("CompactFilesImpl:1");
  // Ignore the status here, as it will be checked in the Install down below...
  compaction_job.Run().PermitUncheckedError();
  TEST_SYNC_POINT("CompactFilesImpl:2");
  TEST_SYNC_POINT("CompactFilesImpl:3");
  mutex_.Lock();

  if (immutable_db_options().compaction_service != nullptr) {
    ReleaseOptionsFileNumber(min_options_file_number_elem);
  }

  bool compaction_released = false;
  Status status = compaction_job.Install(&compaction_released);
  if (!compaction_released) {
    c->ReleaseCompactionFiles(s);
  }
  if (status.ok()) {
    assert(compaction_job.io_status().ok());
    InstallSuperVersionAndScheduleWork(
        c->column_family_data(), job_context->superversion_contexts.data(),
        /*defer_snapshot_refresh=*/false);
  }
  // status above captures any error during compaction_job.Install, so its ok
  // not check compaction_job.io_status() explicitly if we're not calling
  // SetBGError
  compaction_job.io_status().PermitUncheckedError();
  // Need to make sure SstFileManager does its bookkeeping
  auto sfm = static_cast<SstFileManagerImpl*>(
      immutable_db_options_.sst_file_manager.get());
  if (sfm && sfm_reserved_compact_space) {
    sfm->OnCompactionCompletion(c.get());
  }

  ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem);

  mutex_.Unlock();
  if (compaction_job_info != nullptr) {
    BuildCompactionJobInfo(cfd, c.get(), s, compaction_job_stats,
                           job_context->job_id, compaction_job_info);
  }
  mutex_.Lock();

  if (status.ok()) {
    // Done
  } else if (status.IsColumnFamilyDropped() || status.IsShutdownInProgress()) {
    // Ignore compaction errors found during shutting down
  } else if (status.IsManualCompactionPaused()) {
    // Don't report stopping manual compaction as error
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[%s] [JOB %d] Stopping manual compaction",
                   c->column_family_data()->GetName().c_str(),
                   job_context->job_id);
  } else if (status.IsCompactionAborted()) {
    // Don't report aborted compaction as error
    ROCKS_LOG_INFO(
        immutable_db_options_.info_log, "[%s] [JOB %d] Compaction aborted",
        c->column_family_data()->GetName().c_str(), job_context->job_id);
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "[%s] [JOB %d] Compaction error: %s",
                   c->column_family_data()->GetName().c_str(),
                   job_context->job_id, status.ToString().c_str());
    IOStatus io_s = compaction_job.io_status();
    if (!io_s.ok()) {
      error_handler_.SetBGError(io_s, BackgroundErrorReason::kCompaction);
    } else {
      error_handler_.SetBGError(status, BackgroundErrorReason::kCompaction);
    }
  }

  if (output_file_names != nullptr) {
    for (const auto& newf : c->edit()->GetNewFiles()) {
      output_file_names->push_back(TableFileName(
          c->immutable_options().cf_paths, newf.second.fd.GetNumber(),
          newf.second.fd.GetPathId()));
    }

    for (const auto& blob_file : c->edit()->GetBlobFileAdditions()) {
      output_file_names->push_back(
          BlobFileName(c->immutable_options().cf_paths.front().path,
                       blob_file.GetBlobFileNumber()));
    }
  }

  c.reset();

  --scheduled_compactions;
  if (separate_remote_pool || bg_compaction_scheduled_ == 0 ||
      per_cf_compaction_abort_waiters_ > 0) {
    bg_cv_.SignalAll();
  }
  MaybeScheduleFlushOrCompaction();
  TEST_SYNC_POINT("CompactFilesImpl:End");

  return status;
}

Status DBImpl::PauseBackgroundWork() {
  InstrumentedMutexLock guard_lock(&mutex_);
  bg_compaction_paused_++;
  while (CompactionsScheduled() > 0 || remote_compaction_pool_jobs_ > 0 ||
         bg_flush_scheduled_ > 0 || bg_wbm_flush_scheduled_ > 0) {
    TEST_SYNC_POINT("DBImpl::PauseBackgroundWork:Wait");
    bg_cv_.Wait();
  }
  bg_work_paused_++;
  return Status::OK();
}

Status DBImpl::ContinueBackgroundWork() {
  InstrumentedMutexLock guard_lock(&mutex_);
  if (bg_work_paused_ == 0) {
    return Status::InvalidArgument("Background work already unpaused");
  }
  assert(bg_work_paused_ > 0);
  assert(bg_compaction_paused_ > 0);
  bg_compaction_paused_--;
  bg_work_paused_--;
  // It's sufficient to check just bg_work_paused_ here since
  // bg_work_paused_ is always no greater than bg_compaction_paused_
  if (bg_work_paused_ == 0) {
    MaybeScheduleFlushOrCompaction();
  }
  return Status::OK();
}

void DBImpl::NotifyOnCompactionBegin(ColumnFamilyData* cfd, Compaction* c,
                                     const Status& st,
                                     const CompactionJobStats& job_stats,
                                     int job_id) {
  if (immutable_db_options_.listeners.empty()) {
    return;
  }
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }
  if (c->is_manual_compaction() &&
      manual_compaction_paused_.load(std::memory_order_acquire) > 0) {
    return;
  }

  c->SetNotifyOnCompactionCompleted();
  int num_l0_files = c->input_version()->storage_info()->NumLevelFiles(0);
  // release lock while notifying events
  mutex_.Unlock();
  TEST_SYNC_POINT("DBImpl::NotifyOnCompactionBegin::UnlockMutex");
  {
    CompactionJobInfo info{};
    info.num_l0_files = num_l0_files;
    BuildCompactionJobInfo(cfd, c, st, job_stats, job_id, &info);
    for (const auto& listener : immutable_db_options_.listeners) {
      listener->OnCompactionBegin(this, info);
    }
    info.status.PermitUncheckedError();
  }
  mutex_.Lock();
}

void DBImpl::NotifyOnCompactionCompleted(
    ColumnFamilyData* cfd, Compaction* c, const Status& st,
    const CompactionJobStats& compaction_job_stats, const int job_id) {
  if (immutable_db_options_.listeners.size() == 0U) {
    return;
  }
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }

  if (c->ShouldNotifyOnCompactionCompleted() == false) {
    return;
  }

  int num_l0_files = cfd->current()->storage_info()->NumLevelFiles(0);
  // release lock while notifying events
  mutex_.Unlock();
  TEST_SYNC_POINT("DBImpl::NotifyOnCompactionCompleted::UnlockMutex");
  {
    CompactionJobInfo info{};
    info.num_l0_files = num_l0_files;
    BuildCompactionJobInfo(cfd, c, st, compaction_job_stats, job_id, &info);
    for (const auto& listener : immutable_db_options_.listeners) {
      listener->OnCompactionCompleted(this, info);
    }
  }
  mutex_.Lock();
  // no need to signal bg_cv_ as it will be signaled at the end of the
  // flush process.
}

void DBImpl::NotifyOnCompactionPreCommit(
    ColumnFamilyData* cfd, Compaction* c, const Status& st,
    const CompactionJobStats& compaction_job_stats, const int job_id) {
  if (immutable_db_options_.listeners.size() == 0U) {
    return;
  }
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }

  // Only fire if OnCompactionBegin has fired for this compaction.
  if (c->ShouldNotifyOnCompactionCompleted() == false) {
    return;
  }
  // Idempotency: ensure listeners observe at most one
  // OnCompactionPreCommit per compaction lifecycle, even though the
  // notify helper is invoked at multiple potential ReleaseCompactionFiles()
  // sites for safety.
  if (c->WasNotifyOnCompactionPreCommitCalled()) {
    return;
  }
  c->SetNotifyOnCompactionPreCommitCalled();

  int num_l0_files = cfd->current()->storage_info()->NumLevelFiles(0);
  // release lock while notifying events
  mutex_.Unlock();
  TEST_SYNC_POINT("DBImpl::NotifyOnCompactionPreCommit::UnlockMutex");
  {
    CompactionJobInfo info{};
    info.num_l0_files = num_l0_files;
    BuildCompactionJobInfo(cfd, c, st, compaction_job_stats, job_id, &info);
    for (const auto& listener : immutable_db_options_.listeners) {
      listener->OnCompactionPreCommit(this, info);
    }
    info.status.PermitUncheckedError();
  }
  mutex_.Lock();
}

// REQUIREMENT: block all background work by calling PauseBackgroundWork()
// before calling this function
// TODO (hx235): Replace Status::NotSupported() with Status::Aborted() for
// better semantics like CompactFiles()
Status DBImpl::ReFitLevel(ColumnFamilyData* cfd, int level, int target_level) {
  assert(level < cfd->NumberLevels());
  if (target_level >= cfd->NumberLevels()) {
    return Status::InvalidArgument("Target level exceeds number of levels");
  }

  const ReadOptions read_options(Env::IOActivity::kCompaction);
  const WriteOptions write_options(Env::IOActivity::kCompaction);

  SuperVersionContext sv_context(/* create_superversion */ true);

  InstrumentedMutexLock guard_lock(&mutex_);

  auto* vstorage = cfd->current()->storage_info();
  if (vstorage->LevelFiles(level).empty()) {
    return Status::OK();
  }
  // only allow one thread refitting
  if (refitting_level_) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[ReFitLevel] another thread is refitting");
    return Status::NotSupported("another thread is refitting");
  }
  refitting_level_ = true;

  const auto& mutable_cf_options = cfd->GetLatestMutableCFOptions();
  // move to a smaller level
  int to_level = target_level;
  if (target_level < 0) {
    to_level = FindMinimumEmptyLevelFitting(cfd, mutable_cf_options, level);
  }

  if (to_level != level) {
    std::vector<CompactionInputFiles> input(1);
    input[0].level = level;
    // TODO (hx235): Only refit the output files in the current manual
    // compaction instead of all the files in the output level
    for (auto& f : vstorage->LevelFiles(level)) {
      input[0].files.push_back(f);
    }
    InternalKey refit_level_smallest;
    InternalKey refit_level_largest;
    cfd->compaction_picker()->GetRange(input[0], &refit_level_smallest,
                                       &refit_level_largest);
    if (to_level > level) {
      if (level == 0) {
        refitting_level_ = false;
        return Status::NotSupported(
            "Cannot change from level 0 to other levels.");
      }
      // Check levels are empty for a trivial move
      for (int l = level + 1; l <= to_level; l++) {
        if (vstorage->NumLevelFiles(l) > 0) {
          refitting_level_ = false;
          return Status::NotSupported(
              "Levels between source and target are not empty for a move.");
        }
        if (cfd->RangeOverlapWithCompaction(refit_level_smallest.user_key(),
                                            refit_level_largest.user_key(), l,
                                            /*range_limit_exclusive=*/false)) {
          refitting_level_ = false;
          return Status::NotSupported(
              "Levels between source and target "
              "will have some ongoing compaction's output.");
        }
      }
    } else {
      // to_level < level
      if (to_level == 0 && input[0].files.size() > 1) {
        refitting_level_ = false;
        return Status::Aborted(
            "Moving more than 1 file from non-L0 to L0 is not allowed as it "
            "does not bring any benefit to read nor write throughput.");
      }
      // Check levels are empty for a trivial move
      for (int l = to_level; l < level; l++) {
        if (vstorage->NumLevelFiles(l) > 0) {
          refitting_level_ = false;
          return Status::NotSupported(
              "Levels between source and target are not empty for a move.");
        }
        if (cfd->RangeOverlapWithCompaction(refit_level_smallest.user_key(),
                                            refit_level_largest.user_key(), l,
                                            /*range_limit_exclusive=*/false)) {
          refitting_level_ = false;
          return Status::NotSupported(
              "Levels between source and target "
              "will have some ongoing compaction's output.");
        }
      }
    }
    ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                    "[%s] Before refitting:\n%s", cfd->GetName().c_str(),
                    cfd->current()->DebugString().data());

    std::unique_ptr<Compaction> c(new Compaction(
        vstorage, cfd->ioptions(), mutable_cf_options, mutable_db_options_,
        {input}, to_level,
        MaxFileSizeForLevel(
            mutable_cf_options, to_level,
            cfd->ioptions()
                .compaction_style) /* output file size limit, not applicable */
        ,
        LLONG_MAX /* max compaction bytes, not applicable */,
        0 /* output path ID, not applicable */, mutable_cf_options.compression,
        mutable_cf_options.compression_opts, Temperature::kUnknown,
        0 /* max_subcompactions, not applicable */,
        {} /* grandparents, not applicable */,
        std::nullopt /* earliest_snapshot */, nullptr /* snapshot_checker */,
        CompactionReason::kRefitLevel, "" /* trim_ts */,
        -1 /* score, not applicable */,
        false /* l0_files_might_overlap, not applicable */));
    cfd->compaction_picker()->RegisterCompaction(c.get());
    TEST_SYNC_POINT("DBImpl::ReFitLevel:PostRegisterCompaction");
    VersionEdit edit;
    edit.SetColumnFamily(cfd->GetID());

    for (const auto& f : vstorage->LevelFiles(level)) {
      edit.DeleteFile(level, f->fd.GetNumber());
      edit.AddFile(
          to_level, f->fd.GetNumber(), f->fd.GetPathId(), f->fd.GetFileSize(),
          f->smallest, f->largest, f->fd.smallest_seqno, f->fd.largest_seqno,
          f->marked_for_compaction, f->temperature, f->oldest_blob_file_number,
          f->oldest_ancestor_time, f->file_creation_time, f->epoch_number,
          f->file_checksum, f->file_checksum_func_name, f->unique_id,
          f->compensated_range_deletion_size, f->tail_size,
          f->user_defined_timestamps_persisted, f->min_timestamp,
          f->max_timestamp, f->file_open_metadata);
    }
    ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                    "[%s] Apply version edit:\n%s", cfd->GetName().c_str(),
                    edit.DebugString().data());

    Status status =
        versions_->LogAndApply(cfd, read_options, write_options, &edit, &mutex_,
                               directories_.GetDbDir());
    c->MarkFilesBeingCompacted(false);
    cfd->compaction_picker()->UnregisterCompaction(c.get());
    c.reset();

    InstallSuperVersionAndScheduleWork(cfd, &sv_context,
                                       /*defer_snapshot_refresh=*/false);

    ROCKS_LOG_DEBUG(immutable_db_options_.info_log, "[%s] LogAndApply: %s\n",
                    cfd->GetName().c_str(), status.ToString().data());

    if (status.ok()) {
      ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                      "[%s] After refitting:\n%s", cfd->GetName().c_str(),
                      cfd->current()->DebugString().data());
    }
    sv_context.Clean();
    refitting_level_ = false;

    return status;
  }

  refitting_level_ = false;
  return Status::OK();
}

int DBImpl::NumberLevels(ColumnFamilyHandle* column_family) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  return cfh->cfd()->NumberLevels();
}

int DBImpl::Level0StopWriteTrigger(ColumnFamilyHandle* column_family) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  InstrumentedMutexLock l(&mutex_);
  return cfh->cfd()
      ->GetSuperVersion()
      ->mutable_cf_options.level0_stop_writes_trigger;
}

Status DBImpl::FlushAllColumnFamilies(const FlushOptions& flush_options,
                                      FlushReason flush_reason) {
  mutex_.AssertHeld();
  Status status;
  if (immutable_db_options_.atomic_flush || flush_options.force_atomic_flush) {
    mutex_.Unlock();
    TEST_SYNC_POINT(
        "DBImpl::FlushAllColumnFamilies:BeforeAtomicFlushMemTables");
    status = AtomicFlushMemTables(flush_options, flush_reason);
    mutex_.Lock();
  } else {
    for (auto cfd : versions_->GetRefedColumnFamilySet()) {
      if (cfd->IsDropped()) {
        continue;
      }
      mutex_.Unlock();
      TEST_SYNC_POINT("DBImpl::FlushAllColumnFamilies:BeforeFlushMemTable");
      status = FlushMemTable(cfd, flush_options, flush_reason);
      TEST_SYNC_POINT("DBImpl::FlushAllColumnFamilies:1");
      TEST_SYNC_POINT("DBImpl::FlushAllColumnFamilies:2");
      mutex_.Lock();
      if (!status.ok() && !status.IsColumnFamilyDropped()) {
        break;
      } else if (status.IsColumnFamilyDropped()) {
        status = Status::OK();
      }
    }
  }
  return status;
}

Status DBImpl::Flush(const FlushOptions& flush_options,
                     ColumnFamilyHandle* column_family) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  ROCKS_LOG_INFO(immutable_db_options_.info_log, "[%s] Manual flush start.",
                 cfh->GetName().c_str());
  Status s;
  if (immutable_db_options_.atomic_flush || flush_options.force_atomic_flush) {
    s = AtomicFlushMemTables(flush_options, FlushReason::kManualFlush,
                             {cfh->cfd()});
  } else {
    s = FlushMemTable(cfh->cfd(), flush_options, FlushReason::kManualFlush);
  }

  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "[%s] Manual flush finished, status: %s\n",
                 cfh->GetName().c_str(), s.ToString().c_str());
  return s;
}

Status DBImpl::Flush(const FlushOptions& flush_options,
                     const std::vector<ColumnFamilyHandle*>& column_families) {
  Status s;
  if (!immutable_db_options_.atomic_flush &&
      !flush_options.force_atomic_flush) {
    for (auto cfh : column_families) {
      s = Flush(flush_options, cfh);
      if (!s.ok()) {
        break;
      }
    }
  } else {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Manual atomic flush start.\n"
                   "=====Column families:=====");
    for (auto cfh : column_families) {
      auto cfhi = static_cast<ColumnFamilyHandleImpl*>(cfh);
      ROCKS_LOG_INFO(immutable_db_options_.info_log, "%s",
                     cfhi->GetName().c_str());
    }
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "=====End of column families list=====");
    autovector<ColumnFamilyData*> cfds;
    std::for_each(column_families.begin(), column_families.end(),
                  [&cfds](ColumnFamilyHandle* elem) {
                    auto cfh = static_cast<ColumnFamilyHandleImpl*>(elem);
                    cfds.emplace_back(cfh->cfd());
                  });
    s = AtomicFlushMemTables(flush_options, FlushReason::kManualFlush, cfds);
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Manual atomic flush finished, status: %s\n"
                   "=====Column families:=====",
                   s.ToString().c_str());
    for (auto cfh : column_families) {
      auto cfhi = static_cast<ColumnFamilyHandleImpl*>(cfh);
      ROCKS_LOG_INFO(immutable_db_options_.info_log, "%s",
                     cfhi->GetName().c_str());
    }
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "=====End of column families list=====");
  }
  return s;
}

Status DBImpl::RunManualCompaction(
    ColumnFamilyData* cfd, int input_level, int output_level,
    const CompactRangeOptions& compact_range_options, const Slice* begin,
    const Slice* end, bool exclusive, bool disallow_trivial_move,
    uint64_t max_file_num_to_ignore, const std::string& trim_ts,
    int* final_output_level) {
  assert(input_level == ColumnFamilyData::kCompactAllLevels ||
         input_level >= 0);

  InternalKey begin_storage;
  InternalKey end_storage;
  CompactionArg* ca = nullptr;

  bool scheduled = false;
  bool unscheduled = false;
  Env::Priority thread_pool_priority = Env::Priority::TOTAL;
  bool manual_conflict = false;

  ManualCompactionState manual(
      cfd, input_level, output_level, compact_range_options.target_path_id,
      exclusive, disallow_trivial_move, compact_range_options.canceled);
  // For universal compaction, we enforce every manual compaction to compact
  // all files.
  if (begin == nullptr ||
      cfd->ioptions().compaction_style == kCompactionStyleUniversal ||
      cfd->ioptions().compaction_style == kCompactionStyleFIFO) {
    manual.begin = nullptr;
  } else {
    begin_storage.SetMinPossibleForUserKey(*begin);
    manual.begin = &begin_storage;
  }
  if (end == nullptr ||
      cfd->ioptions().compaction_style == kCompactionStyleUniversal ||
      cfd->ioptions().compaction_style == kCompactionStyleFIFO) {
    manual.end = nullptr;
  } else {
    end_storage.SetMaxPossibleForUserKey(*end);
    manual.end = &end_storage;
  }

  TEST_SYNC_POINT("DBImpl::RunManualCompaction:0");
  TEST_SYNC_POINT("DBImpl::RunManualCompaction:1");
  InstrumentedMutexLock l(&mutex_);

  if (manual_compaction_paused_ > 0) {
    // Does not make sense to `AddManualCompaction()` in this scenario since
    // `DisableManualCompaction()` just waited for the manual compaction queue
    // to drain. So return immediately.
    TEST_SYNC_POINT("DBImpl::RunManualCompaction:PausedAtStart");
    manual.status =
        Status::Incomplete(Status::SubCode::kManualCompactionPaused);
    manual.done = true;
    return manual.status;
  }

  if (IsCompactionAborted(cfd)) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[%s] RunManualCompaction: aborting compaction",
                   cfd->GetName().c_str());
    manual.status = Status::Incomplete(Status::SubCode::kCompactionAborted);
    manual.done = true;
    return manual.status;
  }

  // When a manual compaction arrives, temporarily disable scheduling of
  // non-manual compactions and wait until the number of scheduled compaction
  // jobs drops to zero. This used to be needed to ensure that this manual
  // compaction can compact any range of keys/files. Now it is optional
  // (see `CompactRangeOptions::exclusive_manual_compaction`). The use case for
  // `exclusive_manual_compaction=true` is unclear beyond not trusting the code.
  //
  // HasPendingManualCompaction() is true when at least one thread is inside
  // RunManualCompaction(), i.e. during that time no other compaction will
  // get scheduled (see MaybeScheduleFlushOrCompaction).
  //
  // Note that the following loop doesn't stop more that one thread calling
  // RunManualCompaction() from getting to the second while loop below.
  // However, only one of them will actually schedule compaction, while
  // others will wait on a condition variable until it completes.

  AddManualCompaction(&manual);
  TEST_SYNC_POINT_CALLBACK("DBImpl::RunManualCompaction:NotScheduled", &mutex_);
  if (exclusive) {
    // Limitation: there's no way to wake up the below loop when user sets
    // `*manual.canceled`. So `CompactRangeOptions::exclusive_manual_compaction`
    // and `CompactRangeOptions::canceled` might not work well together.
    while (CompactionsScheduled() > 0 || remote_compaction_pool_jobs_ > 0) {
      if (IsCompactionAborted(cfd)) {
        // Pretend the error came from compaction so the below cleanup/error
        // handling code can process it.
        manual.done = true;
        manual.status = Status::Incomplete(Status::SubCode::kCompactionAborted);
        break;
      }
      if (manual_compaction_paused_ > 0 || manual.canceled == true) {
        // Pretend the error came from compaction so the below cleanup/error
        // handling code can process it.
        manual.done = true;
        manual.status =
            Status::Incomplete(Status::SubCode::kManualCompactionPaused);
        break;
      }
      TEST_SYNC_POINT("DBImpl::RunManualCompaction:WaitScheduled");
      ROCKS_LOG_INFO(
          immutable_db_options_.info_log,
          "[%s] Manual compaction waiting for all other scheduled background "
          "compactions to finish",
          cfd->GetName().c_str());
      bg_cv_.Wait();
    }
  }

  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());

  ROCKS_LOG_BUFFER(&log_buffer, "[%s] Manual compaction starting",
                   cfd->GetName().c_str());

  // We don't check bg_error_ here, because if we get the error in compaction,
  // the compaction will set manual.status to bg_error_ and set manual.done to
  // true.
  while (!manual.done) {
    assert(HasPendingManualCompaction());
    if (IsCompactionAborted(cfd) && !scheduled && !manual.in_progress) {
      manual.done = true;
      manual.status = Status::Incomplete(Status::SubCode::kCompactionAborted);
      break;
    }
    manual_conflict = false;
    Compaction* compaction = nullptr;
    if (ShouldntRunManualCompaction(&manual) || (manual.in_progress == true) ||
        scheduled ||
        (((manual.manual_end = &manual.tmp_storage1) != nullptr) &&
         ((compaction = manual.cfd->CompactRange(
               manual.cfd->GetLatestMutableCFOptions(), mutable_db_options_,
               manual.input_level, manual.output_level, compact_range_options,
               manual.begin, manual.end, &manual.manual_end, &manual_conflict,
               max_file_num_to_ignore, trim_ts)) == nullptr &&
          manual_conflict))) {
      if (!manual.done) {
        bg_cv_.Wait();
      }
      if (manual_compaction_paused_ > 0 && scheduled && !unscheduled) {
        assert(thread_pool_priority != Env::Priority::TOTAL);
        // unschedule all manual compactions
        auto unscheduled_task_num = env_->UnSchedule(
            GetTaskTag(TaskType::kManualCompaction), thread_pool_priority);
        if (unscheduled_task_num > 0) {
          ROCKS_LOG_INFO(
              immutable_db_options_.info_log,
              "[%s] Unscheduled %d number of manual compactions from the "
              "thread-pool",
              cfd->GetName().c_str(), unscheduled_task_num);
          // it may unschedule other manual compactions, notify others.
          bg_cv_.SignalAll();
        }
        unscheduled = true;
        TEST_SYNC_POINT("DBImpl::RunManualCompaction:Unscheduled");
      }
      if (scheduled && manual.incomplete == true) {
        assert(!manual.in_progress);
        scheduled = false;
        manual.incomplete = false;
      }
    } else if (!scheduled) {
      if (compaction == nullptr) {
        manual.done = true;
        if (final_output_level) {
          // No compaction needed or there is a conflicting compaction.
          // Still set `final_output_level` to the level where we would
          // have compacted to.
          *final_output_level = output_level;
          if (output_level == ColumnFamilyData::kCompactToBaseLevel) {
            *final_output_level = cfd->current()->storage_info()->base_level();
          }
        }
        bg_cv_.SignalAll();
        continue;
      }
      ca = new CompactionArg;
      ca->db = this;
      ca->prepicked_compaction = new PrepickedCompaction;
      ca->prepicked_compaction->manual_compaction_state = &manual;
      ca->prepicked_compaction->compaction = compaction;
      if (!RequestCompactionToken(
              cfd, true, &ca->prepicked_compaction->task_token, &log_buffer)) {
        // Don't throttle manual compaction, only count outstanding tasks.
        assert(false);
      }
      ca->prepicked_compaction->need_repick = false;
      manual.incomplete = false;
      if (compaction->bottommost_level() &&
          env_->GetBackgroundThreads(Env::Priority::BOTTOM) > 0) {
        ca->compaction_pri_ = Env::Priority::BOTTOM;
        thread_pool_priority = Env::Priority::BOTTOM;
      } else {
        ca->compaction_pri_ = Env::Priority::LOW;
        thread_pool_priority = Env::Priority::LOW;
      }
      Status schedule_status =
          ScheduleCompaction(ca, GetTaskTag(TaskType::kManualCompaction));
      if (!schedule_status.ok()) {
        manual.status = std::move(schedule_status);
        manual.done = true;
        bg_cv_.SignalAll();
        continue;
      }
      scheduled = true;
      TEST_SYNC_POINT("DBImpl::RunManualCompaction:Scheduled");
      if (final_output_level) {
        *final_output_level = compaction->output_level();
      }
    }
    if (!scheduled) {
      // There is nothing scheduled to wait on, so any cancellation can end the
      // manual now.
      if (IsCompactionAborted(cfd)) {
        // Stop waiting since it was canceled. Pretend the error came from
        // compaction so the below cleanup/error handling code can process it.
        manual.done = true;
        manual.status = Status::Incomplete(Status::SubCode::kCompactionAborted);
      } else if (manual_compaction_paused_ > 0 || manual.canceled == true) {
        // Stop waiting since it was canceled. Pretend the error came from
        // compaction so the below cleanup/error handling code can process it.
        manual.done = true;
        manual.status =
            Status::Incomplete(Status::SubCode::kManualCompactionPaused);
      }
    }
  }

  log_buffer.FlushBufferToLog();
  assert(!manual.in_progress);
  assert(HasPendingManualCompaction());
  RemoveManualCompaction(&manual);
  // if the manual job is unscheduled, try schedule other jobs in case there's
  // any unscheduled compaction job which was blocked by exclusive manual
  // compaction.
  if (manual.status.IsIncomplete() &&
      manual.status.subcode() == Status::SubCode::kManualCompactionPaused) {
    MaybeScheduleFlushOrCompaction();
  }
  bg_cv_.SignalAll();
  return manual.status;
}

void DBImpl::GenerateFlushRequest(const autovector<ColumnFamilyData*>& cfds,
                                  FlushReason flush_reason, FlushRequest* req) {
  assert(req != nullptr);
  req->flush_reason = flush_reason;
  req->cfd_to_max_mem_id_to_persist.reserve(cfds.size());
  for (const auto cfd : cfds) {
    if (nullptr == cfd) {
      // cfd may be null, see DBImpl::ScheduleFlushes
      continue;
    }
    uint64_t max_memtable_id = cfd->imm()->GetLatestMemTableID(
        req->atomic_flush /* for_atomic_flush */);
    req->cfd_to_max_mem_id_to_persist.emplace(cfd, max_memtable_id);
  }
}

void DBImpl::NotifyOnManualFlushScheduled(autovector<ColumnFamilyData*> cfds,
                                          FlushReason flush_reason) {
  if (immutable_db_options_.listeners.size() == 0U) {
    return;
  }
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }
  std::vector<ManualFlushInfo> info;
  for (ColumnFamilyData* cfd : cfds) {
    info.push_back({cfd->GetID(), cfd->GetName(), flush_reason});
  }
  for (const auto& listener : immutable_db_options_.listeners) {
    listener->OnManualFlushScheduled(this, info);
  }
}

void DBImpl::MaybeSyncLastSequenceWithAllocatedForRecovery(
    FlushReason flush_reason) {
  mutex_.AssertHeld();
  if (two_write_queues_ && IsRecoveryFlush(flush_reason)) {
    versions_->SyncLastSequenceWithAllocated();
  }
}

Status DBImpl::EnterWriteThreadForNonBlockingFlush(
    WriteThread::Writer* w, WriteThread::Writer* nonmem_w) {
  mutex_.AssertHeld();
  if (!write_thread_.EnterUnbatchedNonBlocking(w, &mutex_)) {
    return Status::Incomplete(
        "Write stall in progress, unable to join the write thread to switch "
        "memtables");
  }
  if (two_write_queues_ &&
      !nonmem_write_thread_.EnterUnbatchedNonBlocking(nonmem_w, &mutex_)) {
    write_thread_.ExitUnbatched(w);
    return Status::Incomplete(
        "Write stall in progress, unable to join the write thread to switch "
        "memtables");
  }
  if (!error_handler_.IsBGWorkStopped() && !write_controller_.IsStopped() &&
      !WouldBlockJoiningWriteThread()) {
    return Status::OK();
  }
  write_thread_.ExitUnbatched(w);
  if (two_write_queues_) {
    nonmem_write_thread_.ExitUnbatched(nonmem_w);
  }
  return Status::Incomplete(
      "Write stop started while joining the write thread to switch memtables");
}

Status DBImpl::FlushMemTable(ColumnFamilyData* cfd,
                             const FlushOptions& flush_options,
                             FlushReason flush_reason,
                             bool entered_write_thread) {
  return FlushMemTableImpl(cfd, flush_options, flush_reason,
                           entered_write_thread, WriteThreadJoinMode::kBlocking,
                           nullptr /* made_progress */);
}

Status DBImpl::FlushMemTableImpl(ColumnFamilyData* cfd,
                                 const FlushOptions& flush_options,
                                 FlushReason flush_reason,
                                 bool entered_write_thread,
                                 WriteThreadJoinMode join_mode,
                                 bool* made_progress) {
  if (made_progress != nullptr) {
    *made_progress = false;
  }
  // This method should not be called if atomic_flush is true.
  assert(!immutable_db_options_.atomic_flush);
  assert(!IsRecoveryFlush(flush_reason));
  if (!flush_options.wait && write_controller_.IsStopped()) {
    std::ostringstream oss;
    oss << "Writes have been stopped, thus unable to perform manual flush. "
           "Please try again later after writes are resumed";
    return Status::TryAgain(oss.str());
  }
  if (flush_options.wait) {
    InstrumentedMutexLock lock(&mutex_);
    if (lock_wal_owner_thread_id_counts_.count(env_->GetThreadID()) > 0) {
      return Status::Aborted(
          "Likely deadlock as the same thread called LockWAL()");
    }
  }
  Status s;
  if (!flush_options.allow_write_stall) {
    bool flush_needed = true;
    s = WaitUntilFlushWouldNotStallWrites(cfd, &flush_needed);
    TEST_SYNC_POINT("DBImpl::FlushMemTable:StallWaitDone");
    if (!s.ok() || !flush_needed) {
      return s;
    }
  }

  const bool needs_to_join_write_thread = !entered_write_thread;
  autovector<FlushRequest> flush_reqs;
  autovector<uint64_t> memtable_ids_to_wait;
  {
    WriteContext context;
    InstrumentedMutexLock guard_lock(&mutex_);

    WriteThread::Writer w;
    WriteThread::Writer nonmem_w;
    if (needs_to_join_write_thread) {
      if (join_mode == WriteThreadJoinMode::kNonBlocking) {
        s = EnterWriteThreadForNonBlockingFlush(&w, &nonmem_w);
        if (!s.ok()) {
          return s;
        }
      } else {
        write_thread_.EnterUnbatched(&w, &mutex_);
        if (two_write_queues_) {
          nonmem_write_thread_.EnterUnbatched(&nonmem_w, &mutex_);
        }
      }
    }
    if (join_mode == WriteThreadJoinMode::kNonBlocking && cfd->IsDropped()) {
      TEST_SYNC_POINT("DBImpl::FlushMemTableImpl:Dropped");
      if (needs_to_join_write_thread) {
        write_thread_.ExitUnbatched(&w);
        if (two_write_queues_) {
          nonmem_write_thread_.ExitUnbatched(&nonmem_w);
        }
      }
      s.PermitUncheckedOk();
      return Status::ColumnFamilyDropped();
    }
    WaitForPendingWrites();

    if (join_mode == WriteThreadJoinMode::kNonBlocking &&
        flush_reason == FlushReason::kWriteBufferManager &&
        cfd->imm()->IsFlushPendingOrRunning()) {
      TEST_SYNC_POINT("DBImpl::FlushMemTableImpl:AlreadyFlushing");
      if (needs_to_join_write_thread) {
        write_thread_.ExitUnbatched(&w);
        if (two_write_queues_) {
          nonmem_write_thread_.ExitUnbatched(&nonmem_w);
        }
      }
      return Status::Incomplete(
          "Column family started flushing before the WBM handoff completed");
    }

    // Recovery may have released `mutex_` after the earlier `ResumeImpl()`
    // sync. Refresh sequence state at the actual memtable-switch fence, after
    // both write queues are drained and before `SwitchMemtable()` consumes
    // `LastSequence()`.
    MaybeSyncLastSequenceWithAllocatedForRecovery(flush_reason);

    if (!cfd->mem()->IsEmpty() || !cached_recoverable_state_empty_.load() ||
        IsRecoveryFlush(flush_reason)) {
      s = SwitchMemtable(cfd, &context);
      if (s.ok() && made_progress != nullptr) {
        *made_progress = true;
      }
    }
    const uint64_t flush_memtable_id = std::numeric_limits<uint64_t>::max();
    if (s.ok()) {
      if (cfd->imm()->NumNotFlushed() != 0 || !cfd->mem()->IsEmpty() ||
          !cached_recoverable_state_empty_.load() ||
          IsRecoveryFlush(flush_reason)) {
        FlushRequest req{
            flush_reason, false /* atomic_flush */, {{cfd, flush_memtable_id}}};
        flush_reqs.emplace_back(std::move(req));
        memtable_ids_to_wait.emplace_back(
            cfd->imm()->GetLatestMemTableID(false /* for_atomic_flush */));
      }
      if (immutable_db_options_.persist_stats_to_disk) {
        ColumnFamilyData* cfd_stats =
            versions_->GetColumnFamilySet()->GetColumnFamily(
                kPersistentStatsColumnFamilyName);
        if (cfd_stats != nullptr && cfd_stats != cfd &&
            !cfd_stats->mem()->IsEmpty()) {
          // only force flush stats CF when it will be the only CF lagging
          // behind after the current flush
          bool stats_cf_flush_needed = true;
          for (auto* loop_cfd : *versions_->GetColumnFamilySet()) {
            if (loop_cfd == cfd_stats || loop_cfd == cfd) {
              continue;
            }
            if (loop_cfd->GetLogNumber() <= cfd_stats->GetLogNumber()) {
              stats_cf_flush_needed = false;
            }
          }
          if (stats_cf_flush_needed) {
            ROCKS_LOG_INFO(immutable_db_options_.info_log,
                           "Force flushing stats CF with manual flush of %s "
                           "to avoid holding old logs",
                           cfd->GetName().c_str());
            s = SwitchMemtable(cfd_stats, &context);
            FlushRequest req{flush_reason,
                             false /* atomic_flush */,
                             {{cfd_stats, flush_memtable_id}}};
            flush_reqs.emplace_back(std::move(req));
            memtable_ids_to_wait.emplace_back(
                cfd_stats->imm()->GetLatestMemTableID(
                    false /* for_atomic_flush */));
          }
        }
      }
    }

    if (s.ok() && !flush_reqs.empty()) {
      for (const auto& req : flush_reqs) {
        assert(req.cfd_to_max_mem_id_to_persist.size() == 1);
        ColumnFamilyData* loop_cfd =
            req.cfd_to_max_mem_id_to_persist.begin()->first;
        loop_cfd->imm()->FlushRequested();
      }
      // If the caller wants to wait for this flush to complete, it indicates
      // that the caller expects the ColumnFamilyData not to be free'ed by
      // other threads which may drop the column family concurrently.
      // Therefore, we increase the cfd's ref count.
      if (flush_options.wait) {
        for (const auto& req : flush_reqs) {
          assert(req.cfd_to_max_mem_id_to_persist.size() == 1);
          ColumnFamilyData* loop_cfd =
              req.cfd_to_max_mem_id_to_persist.begin()->first;
          loop_cfd->Ref();
        }
      }
      for (const auto& req : flush_reqs) {
        assert(req.cfd_to_max_mem_id_to_persist.size() == 1);
        ColumnFamilyData* loop_cfd =
            req.cfd_to_max_mem_id_to_persist.begin()->first;
        bool already_queued_for_flush = loop_cfd->queued_for_flush();
        bool flush_req_enqueued = EnqueuePendingFlush(req);
        if (flush_req_enqueued && made_progress != nullptr) {
          *made_progress = true;
        }
        if (flush_reason != FlushReason::kWriteBufferManager &&
            (already_queued_for_flush || flush_req_enqueued)) {
          loop_cfd->SetFlushSkipReschedule();
        }
      }
      MaybeScheduleFlushOrCompaction();
    }

    if (needs_to_join_write_thread) {
      write_thread_.ExitUnbatched(&w);
      if (two_write_queues_) {
        nonmem_write_thread_.ExitUnbatched(&nonmem_w);
      }
    }
  }

  if (flush_reason != FlushReason::kWriteBufferManager) {
    NotifyOnManualFlushScheduled({cfd}, flush_reason);
  }
  TEST_SYNC_POINT("DBImpl::FlushMemTable:AfterScheduleFlush");
  TEST_SYNC_POINT("DBImpl::FlushMemTable:BeforeWaitForBgFlush");
  if (s.ok() && flush_options.wait) {
    autovector<ColumnFamilyData*> cfds;
    autovector<const uint64_t*> flush_memtable_ids;
    assert(flush_reqs.size() == memtable_ids_to_wait.size());
    for (size_t i = 0; i < flush_reqs.size(); ++i) {
      assert(flush_reqs[i].cfd_to_max_mem_id_to_persist.size() == 1);
      cfds.push_back(flush_reqs[i].cfd_to_max_mem_id_to_persist.begin()->first);
      flush_memtable_ids.push_back(&(memtable_ids_to_wait[i]));
    }
    s = WaitForFlushMemTables(
        cfds, flush_memtable_ids,
        flush_reason == FlushReason::kErrorRecovery /* resuming_from_bg_err */,
        flush_reason, flush_options.listener_wait);
    InstrumentedMutexLock lock_guard(&mutex_);
    for (auto* tmp_cfd : cfds) {
      tmp_cfd->UnrefAndTryDelete();
    }
  }
  TEST_SYNC_POINT("DBImpl::FlushMemTable:FlushMemTableFinished");
  return s;
}

Status DBImpl::AtomicFlushMemTables(
    const FlushOptions& flush_options, FlushReason flush_reason,
    const autovector<ColumnFamilyData*>& provided_candidate_cfds,
    bool entered_write_thread) {
  return AtomicFlushMemTablesImpl(flush_options, flush_reason,
                                  provided_candidate_cfds, entered_write_thread,
                                  WriteThreadJoinMode::kBlocking,
                                  nullptr /* made_progress */);
}

Status DBImpl::AtomicFlushMemTablesImpl(
    const FlushOptions& flush_options, FlushReason flush_reason,
    const autovector<ColumnFamilyData*>& provided_candidate_cfds,
    bool entered_write_thread, WriteThreadJoinMode join_mode,
    bool* made_progress) {
  if (made_progress != nullptr) {
    *made_progress = false;
  }
  if (!flush_options.wait && write_controller_.IsStopped()) {
    std::ostringstream oss;
    oss << "Writes have been stopped, thus unable to perform manual flush. "
           "Please try again later after writes are resumed";
    return Status::TryAgain(oss.str());
  }
  if (flush_options.wait) {
    InstrumentedMutexLock lock(&mutex_);
    if (lock_wal_owner_thread_id_counts_.count(env_->GetThreadID()) > 0) {
      return Status::Aborted(
          "Likely deadlock as the same thread called LockWAL()");
    }
  }
  Status s;
  autovector<ColumnFamilyData*> candidate_cfds;
  if (provided_candidate_cfds.empty()) {
    // Generate candidate cfds if not provided
    {
      InstrumentedMutexLock l(&mutex_);
      for (ColumnFamilyData* cfd : *versions_->GetColumnFamilySet()) {
        if (!cfd->IsDropped() && cfd->initialized()) {
          cfd->Ref();
          candidate_cfds.push_back(cfd);
        }
      }
    }
  } else {
    candidate_cfds = provided_candidate_cfds;
  }

  const auto unref_generated_candidates = [&]() {
    if (!provided_candidate_cfds.empty()) {
      return;
    }
    for (auto candidate_cfd : candidate_cfds) {
      candidate_cfd->UnrefAndTryDelete();
    }
    candidate_cfds.clear();
  };

  if (!flush_options.allow_write_stall) {
    int num_cfs_to_flush = 0;
    for (auto cfd : candidate_cfds) {
      bool flush_needed = true;
      s = WaitUntilFlushWouldNotStallWrites(cfd, &flush_needed);
      if (s.IsColumnFamilyDropped() && provided_candidate_cfds.empty()) {
        // A generated candidate dropped while waiting has nothing to flush.
        s = Status::OK();
      } else if (!s.ok()) {
        unref_generated_candidates();
        return s;
      } else if (flush_needed) {
        ++num_cfs_to_flush;
      }
    }
    if (0 == num_cfs_to_flush) {
      unref_generated_candidates();
      return s;
    }
  }
  const bool needs_to_join_write_thread = !entered_write_thread;
  FlushRequest flush_req;
  flush_req.atomic_flush = true;
  autovector<ColumnFamilyData*> cfds;
  {
    WriteContext context;
    InstrumentedMutexLock guard_lock(&mutex_);

    WriteThread::Writer w;
    WriteThread::Writer nonmem_w;
    if (needs_to_join_write_thread) {
      if (join_mode == WriteThreadJoinMode::kNonBlocking) {
        s = EnterWriteThreadForNonBlockingFlush(&w, &nonmem_w);
        if (!s.ok()) {
          unref_generated_candidates();
          return s;
        }
      } else {
        write_thread_.EnterUnbatched(&w, &mutex_);
        if (two_write_queues_) {
          nonmem_write_thread_.EnterUnbatched(&nonmem_w, &mutex_);
        }
      }
    }
    WaitForPendingWrites();

    // Recovery may have released `mutex_` after the earlier `ResumeImpl()`
    // sync. Refresh sequence state at the atomic memtable-switch fence, after
    // both write queues are drained and before `SwitchMemtable()` consumes
    // `LastSequence()`.
    MaybeSyncLastSequenceWithAllocatedForRecovery(flush_reason);

    SelectColumnFamiliesForAtomicFlush(&cfds, candidate_cfds, flush_reason);

    unref_generated_candidates();

    const bool defer_wbm_accounting_refresh = cfds.size() > 1;
    bool switch_attempted = false;
    for (auto cfd : cfds) {
      if (cfd->mem()->IsEmpty() && cached_recoverable_state_empty_.load() &&
          !IsRecoveryFlush(flush_reason)) {
        continue;
      }
      cfd->Ref();
      switch_attempted = true;
      s = defer_wbm_accounting_refresh
              ? SwitchMemtableImpl(cfd, &context, nullptr, 0,
                                   false /* refresh_wbm_accounting */)
              : SwitchMemtable(cfd, &context);
      cfd->UnrefAndTryDelete();
      if (!s.ok()) {
        break;
      }
      if (made_progress != nullptr) {
        *made_progress = true;
      }
    }
    if (defer_wbm_accounting_refresh && switch_attempted) {
      UpdateWBMAccountingAfterMemtableSwitch(true /* refresh_now */);
    }
    if (s.ok()) {
      AssignAtomicFlushSeq(cfds);
      for (auto cfd : cfds) {
        cfd->imm()->FlushRequested();
      }
      // If the caller wants to wait for this flush to complete, it indicates
      // that the caller expects the ColumnFamilyData not to be free'ed by
      // other threads which may drop the column family concurrently.
      // Therefore, we increase the cfd's ref count.
      if (flush_options.wait) {
        for (auto cfd : cfds) {
          cfd->Ref();
        }
      }
      GenerateFlushRequest(cfds, flush_reason, &flush_req);
      if (EnqueuePendingFlush(flush_req) && made_progress != nullptr) {
        *made_progress = true;
      }
      MaybeScheduleFlushOrCompaction();
    }

    if (needs_to_join_write_thread) {
      write_thread_.ExitUnbatched(&w);
      if (two_write_queues_) {
        nonmem_write_thread_.ExitUnbatched(&nonmem_w);
      }
    }
  }
  if (flush_reason != FlushReason::kWriteBufferManager) {
    NotifyOnManualFlushScheduled(cfds, flush_reason);
  }
  TEST_SYNC_POINT("DBImpl::AtomicFlushMemTables:AfterScheduleFlush");
  TEST_SYNC_POINT("DBImpl::AtomicFlushMemTables:BeforeWaitForBgFlush");
  if (s.ok() && flush_options.wait) {
    // Walk cfds and look each one up, so the ids line up with cfds by
    // construction. Iterating the map instead pairs ids with the wrong CF
    // (its order is unrelated to cfds), and a CF that dropped out of the map
    // (an idle CF de-selected during the switch loop, or a duplicate handle
    // collapsed by the map) would make the two vectors different lengths.
    autovector<const uint64_t*> flush_memtable_ids;
    flush_memtable_ids.reserve(cfds.size());
    for (auto* cfd : cfds) {
      auto iter = flush_req.cfd_to_max_mem_id_to_persist.find(cfd);
      flush_memtable_ids.push_back(
          iter == flush_req.cfd_to_max_mem_id_to_persist.end()
              ? nullptr
              : &(iter->second));
    }
#ifndef NDEBUG
    // WaitForFlushMemTables pairs flush_memtable_ids[i] with cfds[i], so each
    // entry must point at this cfd's own map id (or be null when absent). Fires
    // if the ids are ever rebuilt in the request map's order.
    assert(flush_memtable_ids.size() == cfds.size());
    for (size_t i = 0; i < cfds.size(); ++i) {
      auto iter = flush_req.cfd_to_max_mem_id_to_persist.find(cfds[i]);
      assert(flush_memtable_ids[i] ==
             (iter == flush_req.cfd_to_max_mem_id_to_persist.end()
                  ? nullptr
                  : &(iter->second)));
    }
#endif  // NDEBUG
    s = WaitForFlushMemTables(
        cfds, flush_memtable_ids,
        flush_reason == FlushReason::kErrorRecovery /* resuming_from_bg_err */,
        flush_reason, flush_options.listener_wait);
    if (s.IsColumnFamilyDropped() && provided_candidate_cfds.empty()) {
      // Every selected CF was dropped. Generated candidates that were not
      // selected had nothing to flush.
      s = Status::OK();
    }
    InstrumentedMutexLock lock_guard(&mutex_);
    for (auto* cfd : cfds) {
      cfd->UnrefAndTryDelete();
    }
  }
  return s;
}

Status DBImpl::RetryFlushesForErrorRecovery(FlushReason flush_reason,
                                            bool wait) {
  mutex_.AssertHeld();
  assert(flush_reason == FlushReason::kErrorRecoveryRetryFlush ||
         flush_reason == FlushReason::kCatchUpAfterErrorRecovery);

  // Collect referenced CFDs.
  autovector<ColumnFamilyData*> cfds;
  for (ColumnFamilyData* cfd : *versions_->GetColumnFamilySet()) {
    if (!cfd->IsDropped() && cfd->initialized() &&
        cfd->imm()->NumNotFlushed() != 0) {
      cfd->Ref();
      cfd->imm()->FlushRequested();
      cfds.push_back(cfd);
    }
  }

  // Submit flush requests for all immutable memtables needing flush.
  // `flush_memtable_ids` will be populated such that all immutable
  // memtables eligible for flush are waited on before this function
  // returns.
  autovector<uint64_t> flush_memtable_ids;
  if (immutable_db_options_.atomic_flush) {
    FlushRequest flush_req;
    flush_req.atomic_flush = true;
    GenerateFlushRequest(cfds, flush_reason, &flush_req);
    EnqueuePendingFlush(flush_req);
    // Keep flush_memtable_ids aligned with cfds by walking cfds, since the
    // request map's order is unrelated to cfds. A CF not in the map waits on
    // its whole imm list, matching the max-bound the non-atomic branch uses.
    for (auto* cfd : cfds) {
      auto iter = flush_req.cfd_to_max_mem_id_to_persist.find(cfd);
      flush_memtable_ids.push_back(
          iter == flush_req.cfd_to_max_mem_id_to_persist.end()
              ? std::numeric_limits<uint64_t>::max()
              : iter->second);
    }
#ifndef NDEBUG
    // WaitForFlushMemTables pairs flush_memtable_ids[i] with cfds[i], so each
    // entry must be this cfd's own max id. Fires if the ids are ever rebuilt in
    // the request map's order, which is unrelated to cfds order.
    assert(flush_memtable_ids.size() == cfds.size());
    for (size_t i = 0; i < cfds.size(); ++i) {
      auto iter = flush_req.cfd_to_max_mem_id_to_persist.find(cfds[i]);
      assert(flush_memtable_ids[i] ==
             (iter == flush_req.cfd_to_max_mem_id_to_persist.end()
                  ? std::numeric_limits<uint64_t>::max()
                  : iter->second));
    }
#endif  // NDEBUG
  } else {
    for (auto cfd : cfds) {
      flush_memtable_ids.push_back(
          cfd->imm()->GetLatestMemTableID(false /* for_atomic_flush */));
      // Impose no bound on the highest memtable ID flushed. There is no
      // reason to do so outside of atomic flush.
      FlushRequest flush_req{
          flush_reason,
          false /* atomic_flush */,
          {{cfd,
            std::numeric_limits<uint64_t>::max() /* max_mem_id_to_persist */}}};
      if (EnqueuePendingFlush(flush_req)) {
        cfd->SetFlushSkipReschedule();
      };
    }
  }
  MaybeScheduleFlushOrCompaction();

  Status s;
  if (wait) {
    mutex_.Unlock();
    autovector<const uint64_t*> flush_memtable_id_ptrs;
    for (auto& flush_memtable_id : flush_memtable_ids) {
      flush_memtable_id_ptrs.push_back(&flush_memtable_id);
    }
    s = WaitForFlushMemTables(cfds, flush_memtable_id_ptrs,
                              true /* resuming_from_bg_err */, flush_reason);
    mutex_.Lock();
  }

  for (auto* cfd : cfds) {
    cfd->UnrefAndTryDelete();
  }
  return s;
}

// Calling FlushMemTable(), whether from DB::Flush() or from Backup Engine, can
// cause write stall, for example if one memtable is being flushed already.
// This method tries to avoid write stall (similar to CompactRange() behavior)
// it emulates how the SuperVersion / LSM would change if flush happens, checks
// it against various constrains and delays flush if it'd cause write stall.
// Caller should check status and flush_needed to see if flush already happened.
Status DBImpl::WaitUntilFlushWouldNotStallWrites(ColumnFamilyData* cfd,
                                                 bool* flush_needed) {
  {
    *flush_needed = true;
    InstrumentedMutexLock l(&mutex_);
    uint64_t orig_active_memtable_id = cfd->mem()->GetID();
    WriteStallCondition write_stall_condition = WriteStallCondition::kNormal;
    do {
      if (write_stall_condition != WriteStallCondition::kNormal) {
        // Same error handling as user writes: Don't wait if there's a
        // background error, even if it's a soft error. We might wait here
        // indefinitely as the pending flushes/compactions may never finish
        // successfully, resulting in the stall condition lasting indefinitely
        if (error_handler_.IsBGWorkStopped()) {
          return error_handler_.GetBGError();
        }

        TEST_SYNC_POINT("DBImpl::WaitUntilFlushWouldNotStallWrites:StallWait");
        ROCKS_LOG_INFO(immutable_db_options_.info_log,
                       "[%s] WaitUntilFlushWouldNotStallWrites"
                       " waiting on stall conditions to clear",
                       cfd->GetName().c_str());
        bg_cv_.Wait();
      }
      if (cfd->IsDropped()) {
        return Status::ColumnFamilyDropped();
      }
      if (shutting_down_.load(std::memory_order_acquire)) {
        return Status::ShutdownInProgress();
      }

      uint64_t earliest_memtable_id =
          std::min(cfd->mem()->GetID(), cfd->imm()->GetEarliestMemTableID());
      if (earliest_memtable_id > orig_active_memtable_id) {
        // We waited so long that the memtable we were originally waiting on was
        // flushed.
        *flush_needed = false;
        return Status::OK();
      }

      const auto& mutable_cf_options = cfd->GetLatestMutableCFOptions();
      const auto* vstorage = cfd->current()->storage_info();

      // Skip stalling check if we're below auto-flush and auto-compaction
      // triggers. If it stalled in these conditions, that'd mean the stall
      // triggers are so low that stalling is needed for any background work. In
      // that case we shouldn't wait since background work won't be scheduled.
      if (cfd->imm()->NumNotFlushed() <
              cfd->ioptions().min_write_buffer_number_to_merge &&
          vstorage->l0_delay_trigger_count() <
              mutable_cf_options.level0_file_num_compaction_trigger) {
        break;
      }

      // check whether one extra immutable memtable or an extra L0 file would
      // cause write stalling mode to be entered. It could still enter stall
      // mode due to pending compaction bytes, but that's less common
      // No extra immutable Memtable will be created if the current Memtable is
      // empty.
      write_stall_condition =
          ColumnFamilyData::GetWriteStallConditionAndCause(
              cfd->GetUnflushedMemTableCountForWriteStallCheck(),
              vstorage->l0_delay_trigger_count() + 1,
              vstorage->estimated_compaction_needed_bytes(), mutable_cf_options,
              cfd->ioptions())
              .first;
    } while (write_stall_condition != WriteStallCondition::kNormal);
  }
  return Status::OK();
}

// Wait for memtables to be flushed for multiple column families.
// let N = cfds.size()
// for i in [0, N),
//  1) if flush_memtable_ids[i] is not null, then the memtables with lower IDs
//     have to be flushed for THIS column family;
//  2) if flush_memtable_ids[i] is null, then all memtables in THIS column
//     family have to be flushed.
// Finish waiting when ALL column families finish flushing memtables.
// resuming_from_bg_err indicates whether the caller is trying to resume from
// background error or in normal processing.
// Note that the wait finishes when the flush result is installed to column
// families' Versions and persisted in MANIFEST. It doesn't wait until
// SuperVersion to reflect the flush result, except for the case when
// flush_reason is `kExternalFileIngestion`.
Status DBImpl::WaitForFlushMemTables(
    const autovector<ColumnFamilyData*>& cfds,
    const autovector<const uint64_t*>& flush_memtable_ids,
    bool resuming_from_bg_err, std::optional<FlushReason> flush_reason,
    bool wait_for_listener_notifications) {
  int num = static_cast<int>(cfds.size());
  // Wait until the compaction completes
  InstrumentedMutexLock l(&mutex_);
  Status s;
  // If the caller is trying to resume from bg error, then
  // error_handler_.IsDBStopped() is true.
  while (resuming_from_bg_err || !error_handler_.IsDBStopped()) {
    if (shutting_down_.load(std::memory_order_acquire)) {
      s = Status::ShutdownInProgress();
      return s;
    }
    // If an error has occurred during resumption, then no need to wait.
    // But flush operation may fail because of this error, so need to
    // return the status.
    if (!error_handler_.GetRecoveryError().ok()) {
      s = error_handler_.GetRecoveryError();
      break;
    }
    // If BGWorkStopped, which indicate that there is a BG error and
    // 1) soft error but requires no BG work, 2) no in auto_recovery_
    if (!resuming_from_bg_err && error_handler_.IsBGWorkStopped() &&
        error_handler_.GetBGError().severity() < Status::Severity::kHardError) {
      s = error_handler_.GetBGError();
      return s;
    }

    // Number of column families that have been dropped.
    int num_dropped = 0;
    // Number of column families that have finished flush.
    int num_finished = 0;
    for (int i = 0; i < num; ++i) {
      if (cfds[i]->IsDropped()) {
        ++num_dropped;
      } else if (cfds[i]->imm()->NumNotFlushed() == 0 ||
                 (flush_memtable_ids[i] != nullptr &&
                  cfds[i]->imm()->GetEarliestMemTableID() >
                      *flush_memtable_ids[i])) {
        // Make file ingestion's flush wait until SuperVersion is also updated
        // since after flush, it does range overlapping check and file level
        // assignment with the current SuperVersion.
        if (!flush_reason.has_value() ||
            flush_reason.value() != FlushReason::kExternalFileIngestion ||
            cfds[i]->GetSuperVersion()->imm->GetID() ==
                cfds[i]->imm()->current()->GetID()) {
          // When the caller requested it, also wait for the flush-completion
          // listener callbacks for this CF to finish before counting it as
          // finished (see FlushOptions::listener_wait).
          if (!wait_for_listener_notifications ||
              cfds[i]->imm()->NumPendingFlushNotifications() == 0) {
            ++num_finished;
          }
        }
      }
    }
    if (1 == num_dropped && 1 == num) {
      s = Status::ColumnFamilyDropped();
      return s;
    }
    // Column families involved in this flush request have either been dropped
    // or finished flush. Then it's time to finish waiting.
    if (num_dropped + num_finished == num) {
      break;
    }
    bg_cv_.Wait();
  }
  // If not resuming from bg error, and an error has caused the DB to stop,
  // then report the bg error to caller.
  if (!resuming_from_bg_err && error_handler_.IsDBStopped()) {
    s = error_handler_.GetBGError();
  }
  return s;
}

Status DBImpl::EnableAutoCompaction(
    const std::vector<ColumnFamilyHandle*>& column_family_handles) {
  return SetOptions(column_family_handles,
                    {{"disable_auto_compactions", "false"}});
}

// NOTE: Calling DisableManualCompaction() may overwrite the
// user-provided canceled variable in CompactRangeOptions
void DBImpl::DisableManualCompaction() {
  InstrumentedMutexLock l(&mutex_);
  manual_compaction_paused_.fetch_add(1, std::memory_order_release);

  // Mark the canceled as true when the cancellation is triggered by
  // manual_compaction_paused (may overwrite user-provided `canceled`)
  for (const auto& manual_compaction : manual_compaction_dequeue_) {
    manual_compaction->canceled = true;
  }

  // Wake up manual compactions waiting to start.
  bg_cv_.SignalAll();

  // Wait for any pending manual compactions to finish (typically through
  // failing with `Status::Incomplete`) prior to returning. This way we are
  // guaranteed no pending manual compaction will commit while manual
  // compactions are "disabled".
  while (HasPendingManualCompaction()) {
    bg_cv_.Wait();
  }
}

// NOTE: In contrast to DisableManualCompaction(), calling
// EnableManualCompaction() does NOT overwrite the user-provided *canceled
// variable to be false since there is NO CHANCE a canceled compaction
// is uncanceled. In other words, a canceled compaction must have been
// dropped out of the manual compaction queue, when we disable it.
void DBImpl::EnableManualCompaction() {
  InstrumentedMutexLock l(&mutex_);
  assert(manual_compaction_paused_ > 0);
  manual_compaction_paused_.fetch_sub(1, std::memory_order_release);
}

bool DBImpl::IsCompactionAborted(ColumnFamilyData* cfd) const {
  return compaction_aborted_.load(std::memory_order_acquire) > 0 ||
         cfd->compaction_aborted() > 0;
}

bool DBImpl::MustWaitForCompaction(ColumnFamilyData* cfd) {
  mutex_.AssertHeld();
  // num_in_flight_compactions_ and the picker's compactions_in_progress_ set
  // cover DIFFERENT windows and neither subsumes the other:
  //  - the counter covers a picked Compaction from pick through the end of
  //    BackgroundCompaction (including NotifyOnCompactionCompleted), which the
  //    picker set does not -- it is cleared during Install's manifest write.
  //  - the picker set covers the bottom-priority handoff: when a LOW thread
  //    forwards a bottom compaction, its EndInFlightCompaction() dips the
  //    counter to 0 while the compaction is still registered and queued for the
  //    BOTTOM pool, and the picker set keeps AbortCompactions() waiting across
  //    that gap. (The re-registered "intended" bottom compaction inherits the
  //    original compaction_reason(), so it is not filtered out below.)
  if (cfd->num_in_flight_compactions(&mutex_) > 0) {
    return true;
  }
  for (const auto* compaction :
       *cfd->compaction_picker()->compactions_in_progress()) {
    const auto reason = compaction->compaction_reason();
    // These are picker reservations for conflict tracking, not CompactionJobs,
    // so they never observe the abort atomic in ProcessKeyValue().
    if (reason != CompactionReason::kExternalSstIngestion &&
        reason != CompactionReason::kRefitLevel) {
      return true;
    }
  }
  return false;
}

void DBImpl::BeginInFlightCompaction(ColumnFamilyData* cfd) {
  mutex_.AssertHeld();
  cfd->Ref();
  cfd->inc_num_in_flight_compactions(&mutex_);
}

bool DBImpl::EndInFlightCompaction(ColumnFamilyData* cfd) {
  mutex_.AssertHeld();
  cfd->dec_num_in_flight_compactions(&mutex_);
  return cfd->UnrefAndTryDelete();
}

void DBImpl::AbortAllCompactions() {
  InstrumentedMutexLock l(&mutex_);

  // Increment the abort counter to signal all compactions to abort
  compaction_aborted_.fetch_add(1, std::memory_order_release);

  TEST_SYNC_POINT("DBImpl::AbortAllCompactions:FlagSet");

  // Mark all manual compactions as canceled
  for (const auto& manual_compaction : manual_compaction_dequeue_) {
    manual_compaction->canceled = true;
  }

  // Wake up any waiting compaction threads to check the abort signal
  bg_cv_.SignalAll();

  // Wait for all running compactions (both manual and automatic) to finish
  // or abort before returning.
  // Note: bg_cv_.Wait() releases the mutex while waiting, so other threads
  // can make progress and signal when compactions complete.
  while (CompactionsScheduled() > 0 || remote_compaction_pool_jobs_ > 0 ||
         HasPendingManualCompaction()) {
    TEST_SYNC_POINT("DBImpl::AbortAllCompactions:Wait");
    bg_cv_.Wait();
  }
}

void DBImpl::AbortCompactions(ColumnFamilyHandle* column_family) {
  InstrumentedMutexLock l(&mutex_);
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }

  assert(column_family != nullptr);
  auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  assert(cfh->db() == this);
  auto* cfd = cfh->cfd();
  assert(cfd != nullptr);

  cfd->inc_compaction_aborted(&mutex_);
  TEST_SYNC_POINT("DBImpl::AbortCompactions:FlagSet");

  for (const auto* manual_compaction : manual_compaction_dequeue_) {
    if (manual_compaction->cfd == cfd) {
      manual_compaction->canceled.store(true, std::memory_order_release);
    }
  }
  bg_cv_.SignalAll();

  // Wait for this CF's running compactions to finish or abort. If the DB
  // is shutting down, return early -- the counter stays incremented but the
  // CFD and DB will be torn down anyway. Callers that use SCOPE_EXIT for
  // the matching ResumeCompactions() are safe: the resume is a no-op on a
  // DB that is already closed.
  ++per_cf_compaction_abort_waiters_;
  while (!shutting_down_.load(std::memory_order_acquire) &&
         (MustWaitForCompaction(cfd) || HasPendingManualCompaction(cfd))) {
    TEST_SYNC_POINT("DBImpl::AbortCompactions:Waiting");
    bg_cv_.Wait();
  }
  --per_cf_compaction_abort_waiters_;
  TEST_SYNC_POINT("DBImpl::AbortCompactions:Complete");
}

void DBImpl::ResumeCompactions(ColumnFamilyHandle* column_family) {
  InstrumentedMutexLock l(&mutex_);
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }

  assert(column_family != nullptr);
  auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  assert(cfh->db() == this);
  auto* cfd = cfh->cfd();
  assert(cfd != nullptr);

  const int before = cfd->compaction_aborted();
  if (before <= 0) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "ResumeCompactions called without prior AbortCompactions "
                   "for column family %s (counter=%d)",
                   cfd->GetName().c_str(), before);
    return;
  }

  const int current = cfd->dec_compaction_aborted(&mutex_);
  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "[%s] ResumeCompactions: counter %d -> %d",
                 cfd->GetName().c_str(), before, current);
  if (current == 0) {
    // TODO: Aborting an automatic compaction after PickCompaction()
    // releases its inputs without recomputing the compaction score, so this
    // EnqueuePendingCompaction() can observe a stale sub-trigger score and
    // queue nothing, leaving the CF idle until the next flush. Pre-existing
    // (ResumeAllCompactions() has the same gap); fix separately for both APIs
    // by recomputing the score on the abort path.
    if (!RestoreParkedCompaction(cfd) && !cfd->IsDropped()) {
      EnqueuePendingCompaction(cfd);
    }
    MaybeScheduleFlushOrCompaction();
    bg_cv_.SignalAll();
  }
}

void DBImpl::ResumeAllCompactions() {
  InstrumentedMutexLock l(&mutex_);
  int before = compaction_aborted_.load(std::memory_order_acquire);

  // Guard against calling Resume without prior Abort
  if (before <= 0) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "ResumeAllCompactions called without prior "
                   "AbortAllCompactions (counter=%d)",
                   before);
    return;
  }

  // Decrement the abort counter
  compaction_aborted_.fetch_sub(1, std::memory_order_release);

  // As the operation is executed under db mutex, we could just use before value
  // to calculate the current value.
  int current = before - 1;
  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "ResumeAllCompactions: counter %d -> %d", before, current);

  // If this is the last resume call (abort counter back to 0), schedule
  // compactions that may have been waiting
  if (current == 0) {
    // The stale-score TODO in ResumeCompactions() also applies here.
    MaybeScheduleFlushOrCompaction();
  }
}

void DBImpl::MaybeScheduleFlushOrCompaction() {
  mutex_.AssertHeld();
  TEST_SYNC_POINT("DBImpl::MaybeScheduleFlushOrCompaction:Start");
  if (!opened_successfully_ || read_only_) {
    // Compaction may introduce data race to DB open
    return;
  }
  if (bg_work_paused_ > 0) {
    // we paused the background work
    return;
  } else if (error_handler_.IsBGWorkStopped() &&
             !error_handler_.IsRecoveryInProgress()) {
    // Recovery must be able to schedule its flushes while background work is
    // stopped. New non-recovery requests are rejected by EnqueuePendingFlush,
    // and BackgroundFlush drops any request queued before the error.
    return;
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
    return;
  }
  auto bg_job_limits = GetBGJobLimits();
  bool is_flush_pool_empty =
      env_->GetBackgroundThreads(Env::Priority::HIGH) == 0;
  while (!is_flush_pool_empty && unscheduled_flushes_ > 0 &&
         bg_flush_scheduled_ < bg_job_limits.max_flushes) {
    TEST_SYNC_POINT_CALLBACK(
        "DBImpl::MaybeScheduleFlushOrCompaction:BeforeSchedule",
        &unscheduled_flushes_);
    bg_flush_scheduled_++;
    FlushThreadArg* fta = new FlushThreadArg;
    fta->db_ = this;
    fta->thread_pri_ = Env::Priority::HIGH;
    env_->Schedule(&DBImpl::BGWorkFlush, fta, Env::Priority::HIGH, this,
                   &DBImpl::UnscheduleFlushCallback);
    --unscheduled_flushes_;
    TEST_SYNC_POINT_CALLBACK(
        "DBImpl::MaybeScheduleFlushOrCompaction:AfterSchedule:0",
        &unscheduled_flushes_);
  }

  // special case -- if high-pri (flush) thread pool is empty, then schedule
  // flushes in low-pri (compaction) thread pool.
  if (is_flush_pool_empty) {
    while (unscheduled_flushes_ > 0 &&
           bg_flush_scheduled_ + bg_compaction_scheduled_ <
               bg_job_limits.max_flushes) {
      bg_flush_scheduled_++;
      FlushThreadArg* fta = new FlushThreadArg;
      fta->db_ = this;
      fta->thread_pri_ = Env::Priority::LOW;
      env_->Schedule(&DBImpl::BGWorkFlush, fta, Env::Priority::LOW, this,
                     &DBImpl::UnscheduleFlushCallback);
      --unscheduled_flushes_;
    }
  }

  if (bg_compaction_paused_ > 0) {
    // we paused the background compaction
    return;
  } else if (compaction_aborted_.load(std::memory_order_acquire) > 0) {
    // we are aborting all compactions
    return;
  } else if (error_handler_.IsBGWorkStopped()) {
    // Compaction is not part of the recovery sequence from a hard error. We
    // might get here because recovery might do a flush and install a new
    // super version, which will try to schedule pending compactions. Bail
    // out here and let the higher level recovery handle compactions
    return;
  }

  if (HasExclusiveManualCompaction()) {
    // only manual compactions are allowed to run. don't schedule automatic
    // compactions
    TEST_SYNC_POINT("DBImpl::MaybeScheduleFlushOrCompaction:Conflict");
    return;
  }

  while ((UseSeparateRemoteCompactionPool() ||
          LocalCompactionsScheduled() < bg_job_limits.max_compactions) &&
         unscheduled_compactions_ > 0) {
    CompactionArg* ca = new CompactionArg;
    ca->db = this;
    ca->compaction_pri_ = Env::Priority::LOW;
    ca->prepicked_compaction = nullptr;
    Status schedule_status = ScheduleCompaction(ca, this);
    if (!schedule_status.ok()) {
      error_handler_.SetBGError(schedule_status,
                                BackgroundErrorReason::kCompaction);
      bg_cv_.SignalAll();
      break;
    }
    unscheduled_compactions_--;
  }
}

class DBImpl::RemoteCompactionCompletionGuard {
 public:
  explicit RemoteCompactionCompletionGuard(DBImpl& db) : db_(db) {}

  ~RemoteCompactionCompletionGuard() {
    db_.remote_compaction_pool_->CompleteJob();
    InstrumentedMutexLock completed(&db_.mutex_);
    --db_.remote_compaction_pool_jobs_;
    db_.bg_cv_.SignalAll();
  }

  RemoteCompactionCompletionGuard(const RemoteCompactionCompletionGuard&) =
      delete;
  RemoteCompactionCompletionGuard& operator=(
      const RemoteCompactionCompletionGuard&) = delete;
  RemoteCompactionCompletionGuard(RemoteCompactionCompletionGuard&&) = delete;
  RemoteCompactionCompletionGuard& operator=(
      RemoteCompactionCompletionGuard&&) = delete;

 private:
  DBImpl& db_;
};

void DBImpl::ScheduleRemoteCompaction(std::function<void()> work) {
  InstrumentedMutexLock lock(&mutex_);
  ScheduleRemoteCompactionLocked(std::move(work));
}

void DBImpl::ScheduleRemoteCompactionLocked(std::function<void()> work) {
  mutex_.AssertHeld();
  if (!remote_compaction_pool_) {
    remote_compaction_pool_ = RemoteCompactionThreadPool::GetShared();
  }
#ifndef NDEBUG
  std::function<void()> before_submit;
  TEST_SYNC_POINT_CALLBACK("DBImpl::ScheduleRemoteCompaction:BeforeSubmit",
                           &before_submit);
  if (before_submit) {
    before_submit();
  }
#endif
  remote_compaction_pool_->SubmitJob(
      [this, work = std::move(work)] {
        RemoteCompactionCompletionGuard completed(*this);
        work();
      },
      this, RemoteCompactionPoolBaseline());
  ++remote_compaction_pool_jobs_;
}

int DBImpl::RemoteCompactionPoolBaseline() const {
  mutex_.AssertHeld();
  return std::max(1, env_->GetBackgroundThreads(Env::Priority::LOW) +
                         env_->GetBackgroundThreads(Env::Priority::BOTTOM));
}

void DBImpl::ShrinkRemoteCompactionPool() {
  InstrumentedMutexLock lock(&mutex_);
  if (remote_compaction_pool_) {
    remote_compaction_pool_->Shrink(this, UseSeparateRemoteCompactionPool()
                                              ? RemoteCompactionPoolBaseline()
                                              : 0);
  }
}

void DBImpl::ReleaseRemoteCompactionPool() {
  if (remote_compaction_pool_) {
    remote_compaction_pool_->RemoveClient(this);
    remote_compaction_pool_.reset();
  }
}

Status DBImpl::ScheduleCompaction(CompactionArg* arg, void* tag) {
  mutex_.AssertHeld();
  const Env::Priority priority = arg->compaction_pri_;
  const bool separate_remote_pool = UseSeparateRemoteCompactionPool();
  void (*worker)(void*) = priority == Env::Priority::BOTTOM
                              ? &DBImpl::BGWorkBottomCompaction
                              : &DBImpl::BGWorkCompaction;
  arg->separate_remote_pool = separate_remote_pool;
  try {
    if (separate_remote_pool) {
      ScheduleRemoteCompactionLocked([worker, arg] { worker(arg); });
    } else {
      env_->Schedule(worker, arg, priority, tag,
                     &DBImpl::UnscheduleCompactionCallback);
    }
  } catch (...) {
    Status status = Status::Aborted();
    std::unique_ptr<CompactionArg> rejected(arg);
    std::unique_ptr<PrepickedCompaction> prepicked(arg->prepicked_compaction);
    if (prepicked && prepicked->compaction) {
      std::unique_ptr<Compaction> compaction(prepicked->compaction);
      compaction->ReleaseCompactionFiles(status);
    }
    return status;
  }
  if (separate_remote_pool) {
    ++remote_coordinators_scheduled_;
  } else if (priority == Env::Priority::BOTTOM) {
    ++bg_bottom_compaction_scheduled_;
  } else {
    ++bg_compaction_scheduled_;
  }
  return Status::OK();
}

void DBImpl::ConfigureCompactionExecutors(CompactionJob* job,
                                          ColumnFamilyData* cfd,
                                          const std::atomic<bool>& canceled,
                                          bool is_manual,
                                          bool separate_remote_pool) {
  if (!separate_remote_pool) {
    return;
  }
  job->SetCompactionExecutors(
      [this](std::function<void()> work) {
        ScheduleRemoteCompaction(std::move(work));
      },
      [this, cfd, &canceled, is_manual](std::function<void()> work,
                                        Env::Priority priority) {
        return RunLocalCompaction(std::move(work), priority, cfd, canceled,
                                  is_manual);
      });
}

Status DBImpl::LocalCompactionStatus(ColumnFamilyData* cfd,
                                     const std::atomic<bool>& canceled) {
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return Status::ShutdownInProgress();
  }
  if (IsCompactionAborted(cfd)) {
    return Status::Incomplete(Status::SubCode::kCompactionAborted);
  }
  if (canceled.load(std::memory_order_acquire)) {
    return Status::Incomplete(Status::SubCode::kManualCompactionPaused);
  }
  if (cfd->IsDropped()) {
    return Status::ColumnFamilyDropped();
  }
  if (error_handler_.IsBGWorkStopped()) {
    try {
#ifndef NDEBUG
      std::function<void()> before_copy;
      TEST_SYNC_POINT_CALLBACK("DBImpl::LocalCompactionStatus:BeforeGetBGError",
                               &before_copy);
      if (before_copy) {
        before_copy();
      }
#endif
      return error_handler_.GetBGError();
    } catch (...) {
      return Status::Aborted();
    }
  }
  return Status::OK();
}

Status DBImpl::RunLocalCompaction(std::function<void()> work,
                                  Env::Priority priority, ColumnFamilyData* cfd,
                                  const std::atomic<bool>& canceled,
                                  bool is_manual) {
  if (priority == Env::Priority::USER) {
    work();
    return Status::OK();
  }
  InstrumentedMutexLock lock(&mutex_);
  constexpr uint64_t kCancellationPollMicros = 100000;
  Status status;
  while ((status = LocalCompactionStatus(cfd, canceled)).ok() &&
         LocalCompactionsScheduled() >= GetBGJobLimits().max_compactions) {
    TEST_SYNC_POINT("DBImpl::RunLocalCompaction:WaitForCredit");
    if (is_manual) {
      bg_cv_.TimedWait(immutable_db_options_.clock->NowMicros() +
                       kCancellationPollMicros);
    } else {
      bg_cv_.Wait();
    }
  }
  if (!status.ok()) {
    return status;
  }
  assert(priority == Env::Priority::LOW || priority == Env::Priority::BOTTOM);
  LocalCompactionTask task{this,     cfd,         &canceled, std::move(work),
                           priority, Status::OK()};
  bool task_submitted = false;
  Defer drain_accepted_task([this, &task, &task_submitted] {
    while (task_submitted && !task.done) {
      bg_cv_.Wait();
    }
  });
  if (priority == Env::Priority::BOTTOM) {
    ++bg_bottom_compaction_scheduled_;
  } else {
    ++bg_compaction_scheduled_;
  }
  try {
    env_->Schedule(&DBImpl::BGWorkLocalCompaction, &task, priority, &task,
                   &DBImpl::UnscheduleLocalCompaction);
  } catch (...) {
    if (priority == Env::Priority::BOTTOM) {
      --bg_bottom_compaction_scheduled_;
    } else {
      --bg_compaction_scheduled_;
    }
    bg_cv_.SignalAll();
    return Status::Aborted();
  }
  task_submitted = true;
  TEST_SYNC_POINT("DBImpl::RunLocalCompaction:Scheduled");
  bool cancellation_requested = false;
  while (!task.done) {
    if (!cancellation_requested && !LocalCompactionStatus(cfd, canceled).ok()) {
      try {
        env_->UnSchedule(&task, priority);
        cancellation_requested = true;
      } catch (...) {
        if (!task.done) {
          bg_cv_.TimedWait(immutable_db_options_.clock->NowMicros() +
                           kCancellationPollMicros);
        }
        continue;
      }
      if (task.done) {
        break;
      }
    }
    if (is_manual) {
      bg_cv_.TimedWait(immutable_db_options_.clock->NowMicros() +
                       kCancellationPollMicros);
    } else {
      bg_cv_.Wait();
    }
  }
  return task.status;
}

IOStatus DBImpl::CopyTrivialCompactionFiles(
    Compaction* c, LogBuffer* log_buffer, std::vector<FileMetaData>* out_files,
    int job_id, bool db_mutex_held) {
  IOStatus io_s;
  ColumnFamilyData* const cfd = c->column_family_data();
  // Output whose creation was announced to listeners but has not finished.
  std::string unfinished_out_fname;
  for (const auto& in_file : *c->inputs(0)) {
    const uint64_t out_file_number = versions_->NewFileNumber();
    const std::string in_fname =
        TableFileName(c->immutable_options().cf_paths, in_file->fd.GetNumber(),
                      in_file->fd.GetPathId());
    const std::string out_fname = TableFileName(
        c->immutable_options().cf_paths, out_file_number, c->output_path_id());

    // The output is a byte-for-byte copy of the input, so its table
    // properties are exactly the input's, including the SST unique ID.
    // Loading them can read the input file, and listeners are called without
    // the DB mutex.
    std::shared_ptr<const TableProperties> out_table_properties;
    if (db_mutex_held) {
      mutex_.Unlock();
    }
    const TablePropertiesCollection& input_table_properties =
        c->GetOrInitInputTableProperties();
    auto input_table_properties_it = input_table_properties.find(in_fname);
    if (input_table_properties_it != input_table_properties.end()) {
      out_table_properties = input_table_properties_it->second;
    }
    EventHelpers::NotifyTableFileCreationStarted(
        cfd->ioptions().listeners, dbname_, cfd->GetName(), out_fname, job_id,
        TableFileCreationReason::kCompaction);
    if (db_mutex_held) {
      mutex_.Lock();
    }
    unfinished_out_fname = out_fname;

    int64_t tmp_current_time = 0;
    auto get_time_status =
        immutable_db_options_.clock->GetCurrentTime(&tmp_current_time);
    if (!get_time_status.ok()) {
      ROCKS_LOG_BUFFER(log_buffer,
                       "[%s] WARNING: Failed to get current time %s "
                       "status=%s",
                       c->column_family_data()->GetName().c_str(),
                       get_time_status.ToString().c_str());
    }
    uint64_t out_file_creation_time = static_cast<uint64_t>(tmp_current_time);

    FileOptions copied_file_options = file_options_;
    copied_file_options.temperature = c->GetOutputTemperature();
    std::unique_ptr<WritableFileWriter> dest_writer;
    {
      std::unique_ptr<FSWritableFile> dest_file;
      IOStatus writable_file_io_status =
          immutable_db_options_.fs.get()->NewWritableFile(
              out_fname, copied_file_options, &dest_file, nullptr /* dbg */);
      TEST_SYNC_POINT_CALLBACK(
          "NewWritableFile::FileOptions.temperature",
          const_cast<Temperature*>(&copied_file_options.temperature));
      if (!writable_file_io_status.ok()) {
        io_s = writable_file_io_status;
        ROCKS_LOG_BUFFER(
            log_buffer,
            "[%s] Error: Abort trivial copy compaction, failed to open "
            "NewWritableFile %s\n"
            " out_fname=%s, temperature=%s, io_status=%s",
            c->column_family_data()->GetName().c_str(), out_fname.c_str(),
            out_fname.c_str(),
            temperature_to_string[c->GetOutputTemperature()].c_str(),
            io_s.ToString().c_str());
        break;
      }

      FileTypeSet tmp_set = immutable_db_options_.checksum_handoff_file_types;
      dest_writer.reset(new WritableFileWriter(
          std::move(dest_file), out_fname, copied_file_options,
          immutable_db_options_.clock, io_tracer_, immutable_db_options_.stats,
          Histograms::SST_WRITE_MICROS, c->immutable_options().listeners,
          immutable_db_options_.file_checksum_gen_factory.get(),
          tmp_set.Contains(FileType::kTableFile),
          tmp_set.Contains(FileType::kTableFile)));
    }

    ROCKS_LOG_BUFFER(
        log_buffer,
        "[%s] Started copying from: %s\n"
        " temperature=%s, to: %s, temperature=%s, buffer_size=%" PRIu64,
        c->column_family_data()->GetName().c_str(), in_fname.c_str(),
        temperature_to_string[in_file->temperature].c_str(), out_fname.c_str(),
        temperature_to_string[c->GetOutputTemperature()].c_str(),
        c->mutable_cf_options()
            .compaction_options_fifo.trivial_copy_buffer_size);
    // Add IO_LOW HINT for compaction
    IOOptions copy_files_compaction_io_options;
    copy_files_compaction_io_options.rate_limiter_priority =
        Env::IOPriority::IO_LOW;
    copy_files_compaction_io_options.type = IOType::kData;
    copy_files_compaction_io_options.io_activity = Env::IOActivity::kCompaction;

    IOStatus copy_file_io_status = CopyFile(
        immutable_db_options_.fs.get() /* fileSystem */, in_fname /* source */,
        in_file->temperature /* src_temp_hint */, dest_writer /* dest_writer */,
        0 /* size */, true /* use_fsync */, io_tracer_ /* io_tracer*/,
        c->mutable_cf_options()
            .compaction_options_fifo
            .trivial_copy_buffer_size /* max_read_buffer_size
                                       */
        ,
        copy_files_compaction_io_options /* readIOOptions */,
        copy_files_compaction_io_options /* writeIOOptions */);
    if (dest_writer) {
      IOOptions close_files_compaction_io_options;
      close_files_compaction_io_options.rate_limiter_priority =
          Env::IOPriority::IO_LOW;
      close_files_compaction_io_options.type = IOType::kData;
      close_files_compaction_io_options.io_activity =
          Env::IOActivity::kCompaction;
      // Close the dest_write
      io_s = dest_writer->Close(close_files_compaction_io_options);
      if (!io_s.ok()) {
        ROCKS_LOG_BUFFER(
            log_buffer,
            "[%s] Failed to close the writer. Failed to copy from: %s\n"
            " temperature=%s, to=%s, temperature=%s, io_status=%s",
            c->column_family_data()->GetName().c_str(), in_fname.c_str(),
            temperature_to_string[in_file->temperature].c_str(),
            out_fname.c_str(),
            temperature_to_string[c->GetOutputTemperature()].c_str(),
            io_s.ToString().c_str());
        break;
      }
    }

    io_s = copy_file_io_status;

    if (!io_s.ok()) {
      ROCKS_LOG_BUFFER(log_buffer,
                       "[%s] Failed to copy from: %s\n"
                       " temperature=%s, to=%s, temperature=%s, io_status=%s",
                       c->column_family_data()->GetName().c_str(),
                       in_fname.c_str(),
                       temperature_to_string[in_file->temperature].c_str(),
                       out_fname.c_str(),
                       temperature_to_string[c->GetOutputTemperature()].c_str(),
                       io_s.ToString().c_str());
      break;
    }
    ROCKS_LOG_BUFFER(
        log_buffer,
        "[%s] Successfully copying from: %s\n"
        " temperature=%s, to=%s, temperature=%s, io_status=%s",
        c->column_family_data()->GetName().c_str(), in_fname.c_str(),
        temperature_to_string[in_file->temperature].c_str(), out_fname.c_str(),
        temperature_to_string[c->GetOutputTemperature()].c_str(),
        io_s.ToString().c_str());

    FileMetaData out_file_metadata{out_file_number,
                                   c->output_path_id(),
                                   in_file->fd.GetFileSize(),
                                   in_file->smallest,
                                   in_file->largest,
                                   in_file->fd.smallest_seqno,
                                   in_file->fd.largest_seqno,
                                   false /* marked_for_compact */,
                                   c->GetOutputTemperature() /* temperature */,
                                   in_file->oldest_blob_file_number,
                                   in_file->oldest_ancestor_time,
                                   out_file_creation_time,
                                   c->MinInputFileEpochNumber(),
                                   dest_writer->GetFileChecksum(),
                                   dest_writer->GetFileChecksumFuncName(),
                                   in_file->unique_id,
                                   in_file->compensated_range_deletion_size,
                                   in_file->tail_size,
                                   in_file->user_defined_timestamps_persisted,
                                   in_file->min_timestamp,
                                   in_file->max_timestamp};

    if (out_table_properties) {
      c->SetOutputTableProperties(out_fname, out_table_properties);
    }
    if (db_mutex_held) {
      mutex_.Unlock();
    }
    EventHelpers::LogAndNotifyTableFileCreationFinished(
        &event_logger_, cfd->ioptions().listeners, dbname_, cfd->GetName(),
        out_fname, job_id, out_file_metadata.fd,
        out_file_metadata.oldest_blob_file_number,
        out_table_properties ? *out_table_properties : TableProperties(),
        TableFileCreationReason::kCompaction, io_s,
        out_file_metadata.file_checksum,
        out_file_metadata.file_checksum_func_name);
    if (db_mutex_held) {
      mutex_.Lock();
    }
    unfinished_out_fname.clear();

    out_files->push_back(std::move(out_file_metadata));
  }

  if (!unfinished_out_fname.empty()) {
    assert(!io_s.ok());
    if (db_mutex_held) {
      mutex_.Unlock();
    }
    EventHelpers::LogAndNotifyTableFileCreationFinished(
        &event_logger_, cfd->ioptions().listeners, dbname_, cfd->GetName(),
        unfinished_out_fname, job_id, FileDescriptor(), kInvalidBlobFileNumber,
        TableProperties(), TableFileCreationReason::kCompaction, io_s,
        kUnknownFileChecksum, kUnknownFileChecksumFuncName);
    if (db_mutex_held) {
      mutex_.Lock();
    }
  }
  return io_s;
}

void DBImpl::BGWorkLocalCompaction(void* arg) {
  auto* task = static_cast<LocalCompactionTask*>(arg);
  DBImpl* db = task->db;
  const int priority_index = task->priority == Env::Priority::BOTTOM ? 0 : 1;
  IOSTATS_SET_THREAD_POOL_ID(task->priority);
  db->mutex_.Lock();
  ++db->local_compactions_running_by_priority_[priority_index];
  TEST_SYNC_POINT("DBImpl::BGWorkLocalCompaction:BeforeStatus");
  task->status = db->LocalCompactionStatus(task->cfd, *task->canceled);
  db->mutex_.Unlock();
  if (task->status.ok()) {
    TEST_SYNC_POINT("DBImpl::BGWorkLocalCompaction:Start");
    try {
      task->work();
    } catch (...) {
      task->status = Status::Aborted();
    }
    TEST_SYNC_POINT("DBImpl::BGWorkLocalCompaction:Finished");
  }
  InstrumentedMutexLock lock(&db->mutex_);
  if (task->priority == Env::Priority::BOTTOM) {
    --db->bg_bottom_compaction_scheduled_;
  } else {
    --db->bg_compaction_scheduled_;
  }
  --db->local_compactions_running_by_priority_[priority_index];
  task->done = true;
  db->MaybeScheduleFlushOrCompaction();
  db->bg_cv_.SignalAll();
}

void DBImpl::UnscheduleLocalCompaction(void* arg) {
  auto* task = static_cast<LocalCompactionTask*>(arg);
  task->db->mutex_.AssertHeld();
  task->status = task->db->LocalCompactionStatus(task->cfd, *task->canceled);
  if (task->status.ok()) {
    task->status = Status::ShutdownInProgress();
  }
  if (task->priority == Env::Priority::BOTTOM) {
    --task->db->bg_bottom_compaction_scheduled_;
  } else {
    --task->db->bg_compaction_scheduled_;
  }
  task->done = true;
  task->db->MaybeScheduleFlushOrCompaction();
  task->db->bg_cv_.SignalAll();
}

DBImpl::BGJobLimits DBImpl::GetBGJobLimits() const {
  mutex_.AssertHeld();
  return GetBGJobLimits(mutable_db_options_.max_background_flushes,
                        mutable_db_options_.max_background_compactions,
                        mutable_db_options_.max_background_jobs,
                        write_controller_.NeedSpeedupCompaction());
}

DBImpl::BGJobLimits DBImpl::GetBGJobLimits(int max_background_flushes,
                                           int max_background_compactions,
                                           int max_background_jobs,
                                           bool parallelize_compactions) {
  BGJobLimits res;
  if (max_background_flushes == -1 && max_background_compactions == -1) {
    // for our first stab implementing max_background_jobs, simply allocate a
    // quarter of the threads to flushes.
    res.max_flushes = std::max(1, max_background_jobs / 4);
    res.max_compactions = std::max(1, max_background_jobs - res.max_flushes);
  } else {
    // compatibility code in case users haven't migrated to max_background_jobs,
    // which automatically computes flush/compaction limits
    res.max_flushes = std::max(1, max_background_flushes);
    res.max_compactions = std::max(1, max_background_compactions);
  }
  if (!parallelize_compactions) {
    // throttle background compactions until we deem necessary
    res.max_compactions = 1;
  }
  return res;
}

BackgroundJobPressure DBImpl::CaptureBackgroundJobPressure() const {
  mutex_.AssertHeld();

  BackgroundJobPressure snapshot;

  snapshot.compaction_scheduled = CompactionsScheduled();
  snapshot.compaction_running = num_running_compactions_;

  // Per-priority breakdown for pool-specific expansion
  assert(num_running_compactions_ >= num_running_bottom_compactions_);
  snapshot.compaction_low_scheduled = bg_compaction_scheduled_;
  snapshot.compaction_low_running = local_compactions_running_by_priority_[1];
  snapshot.compaction_bottom_scheduled = bg_bottom_compaction_scheduled_;
  snapshot.compaction_bottom_running =
      local_compactions_running_by_priority_[0];

  // Flush
  snapshot.flush_scheduled = bg_flush_scheduled_;
  snapshot.flush_running = num_running_flushes_;

  snapshot.compaction_speedup_active =
      write_controller_.NeedSpeedupCompaction();

  // Compute write_stall_proximity_pct: max across all CFs.
  // Uses the same inputs as RecalculateWriteStallConditions() in
  // column_family.cc:
  //   - l0_delay_trigger_count(): sorted run count used for write stall
  //     decisions. Equals NumLevelFiles(0) for level compaction; for universal
  //     compaction, also counts non-empty levels from L1+ as sorted runs.
  //   - estimated_compaction_needed_bytes(): pending compaction bytes.
  // Prefers slowdown/soft triggers (gradual); falls back to stop/hard triggers.
  // level0_slowdown_writes_trigger is sanitized to always be > 0;
  // soft_pending_compaction_bytes_limit can be 0 (disabled).
  int max_proximity = 0;
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    if (cfd->IsDropped() || !cfd->initialized()) {
      continue;
    }
    const auto* vstorage = cfd->current()->storage_info();
    const auto& mutable_cf_options = cfd->GetCurrentMutableCFOptions();

    int l0_proximity = 0;
    int l0_trigger = mutable_cf_options.level0_slowdown_writes_trigger > 0
                         ? mutable_cf_options.level0_slowdown_writes_trigger
                         : mutable_cf_options.level0_stop_writes_trigger;
    if (l0_trigger > 0) {
      l0_proximity = static_cast<int>(
          100.0 * vstorage->l0_delay_trigger_count() / l0_trigger);
    }

    int bytes_proximity = 0;
    uint64_t bytes_limit =
        mutable_cf_options.soft_pending_compaction_bytes_limit > 0
            ? mutable_cf_options.soft_pending_compaction_bytes_limit
            : mutable_cf_options.hard_pending_compaction_bytes_limit;
    if (bytes_limit > 0) {
      bytes_proximity = static_cast<int>(
          100.0 *
          static_cast<double>(vstorage->estimated_compaction_needed_bytes()) /
          static_cast<double>(bytes_limit));
    }

    max_proximity =
        std::max(max_proximity, std::max(l0_proximity, bytes_proximity));
  }
  snapshot.write_stall_proximity_pct = max_proximity;

  return snapshot;
}

void DBImpl::NotifyOnBackgroundJobPressureChanged() {
  mutex_.AssertHeld();
  if (immutable_db_options_.listeners.empty()) {
    return;
  }
  BackgroundJobPressure pressure = CaptureBackgroundJobPressure();
  bg_pressure_callback_in_progress_++;
  mutex_.Unlock();
  for (const auto& listener : immutable_db_options_.listeners) {
    listener->OnBackgroundJobPressureChanged(this, pressure);
  }
  mutex_.Lock();
  bg_pressure_callback_in_progress_--;
}

void DBImpl::AddToCompactionQueue(ColumnFamilyData* cfd) {
  assert(!cfd->queued_for_compaction());
  cfd->Ref();
  compaction_queue_.push_back(cfd);
  cfd->set_queued_for_compaction(true);
  ++unscheduled_compactions_;
}

bool DBImpl::RestoreParkedCompaction(ColumnFamilyData* cfd) {
  mutex_.AssertHeld();
  auto it = parked_compaction_cfds_.find(cfd);
  if (it == parked_compaction_cfds_.end()) {
    return false;
  }

  parked_compaction_cfds_.erase(it);
  assert(cfd->queued_for_compaction());
  if (!cfd->IsDropped() && cfd->NeedsCompaction()) {
    TEST_SYNC_POINT_CALLBACK("DBImpl::RestoreParkedCompaction:cfd",
                             static_cast<void*>(cfd));
    compaction_queue_.push_back(cfd);
    ++unscheduled_compactions_;
  } else {
    cfd->set_queued_for_compaction(false);
    cfd->UnrefAndTryDelete();
  }
  return true;
}

ColumnFamilyData* DBImpl::PopFirstFromCompactionQueue() {
  assert(!compaction_queue_.empty());
  auto cfd = *compaction_queue_.begin();
  compaction_queue_.pop_front();
  assert(cfd->queued_for_compaction());
  cfd->set_queued_for_compaction(false);
  return cfd;
}

DBImpl::FlushRequest DBImpl::PopFirstFromFlushQueue() {
  assert(!flush_queue_.empty());
  FlushRequest flush_req = std::move(flush_queue_.front());
  flush_queue_.pop_front();
  if (!flush_req.atomic_flush) {
    assert(flush_req.cfd_to_max_mem_id_to_persist.size() == 1);
  }
  for (const auto& elem : flush_req.cfd_to_max_mem_id_to_persist) {
    if (!flush_req.atomic_flush) {
      ColumnFamilyData* cfd = elem.first;
      assert(cfd);
      assert(cfd->queued_for_flush());
      cfd->set_queued_for_flush(false);
    }
  }
  return flush_req;
}

DBImpl::CompactionQueuePickResult DBImpl::PickCompactionFromQueue(
    std::unique_ptr<TaskLimiterToken>* token, LogBuffer* log_buffer) {
  assert(!compaction_queue_.empty());
  assert(*token == nullptr);
  autovector<ColumnFamilyData*> throttled_candidates;
  CompactionQueuePickResult result = CompactionQueueThrottled{};
  while (!compaction_queue_.empty()) {
    auto first_cfd = *compaction_queue_.begin();
    compaction_queue_.pop_front();
    assert(first_cfd->queued_for_compaction());
    if (first_cfd->IsDropped()) {
      first_cfd->set_queued_for_compaction(false);
      result = CompactionQueuePicked{first_cfd};
      break;
    }
    if (first_cfd->compaction_aborted() > 0) {
      // Parking consumes this worker's credit. Remaining queue entries retain
      // their own credits and are scheduled when this worker returns.
      [[maybe_unused]] const bool inserted =
          parked_compaction_cfds_.insert(first_cfd).second;
      assert(inserted);
      TEST_SYNC_POINT("DBImpl::PickCompactionFromQueue:Parked");
      result = CompactionQueueParked{};
      break;
    }
    if (!RequestCompactionToken(first_cfd, false, token, log_buffer)) {
      throttled_candidates.push_back(first_cfd);
      continue;
    }
    first_cfd->set_queued_for_compaction(false);
    result = CompactionQueuePicked{first_cfd};
    break;
  }
  // Add throttled compaction candidates back to queue in the original order.
  for (auto iter = throttled_candidates.rbegin();
       iter != throttled_candidates.rend(); ++iter) {
    compaction_queue_.push_front(*iter);
  }
  return result;
}

bool DBImpl::EnqueuePendingFlush(const FlushRequest& flush_req) {
  mutex_.AssertHeld();
  bool enqueued = false;
  if (reject_new_background_jobs_) {
    return enqueued;
  }
  if (error_handler_.IsBGWorkStopped() &&
      !IsRecoveryFlush(flush_req.flush_reason)) {
    return enqueued;
  }
  if (flush_req.cfd_to_max_mem_id_to_persist.empty()) {
    return enqueued;
  }
  if (!flush_req.atomic_flush) {
    // For the non-atomic flush case, we never schedule multiple column
    // families in the same flush request.
    assert(flush_req.cfd_to_max_mem_id_to_persist.size() == 1);
    ColumnFamilyData* cfd =
        flush_req.cfd_to_max_mem_id_to_persist.begin()->first;
    assert(cfd);

    if (!cfd->queued_for_flush() && cfd->imm()->IsFlushPending()) {
      cfd->Ref();
      cfd->set_queued_for_flush(true);
      ++unscheduled_flushes_;
      flush_queue_.push_back(flush_req);
      enqueued = true;
    }
  } else {
    if (flush_req.flush_reason == FlushReason::kWriteBufferManager &&
        write_buffer_manager_->ShouldTrackFlushInitiator()) {
      for (const FlushRequest& queued : flush_queue_) {
        if (!queued.atomic_flush) {
          continue;
        }
        bool covered = true;
        for (const auto& requested : flush_req.cfd_to_max_mem_id_to_persist) {
          const auto queued_entry =
              queued.cfd_to_max_mem_id_to_persist.find(requested.first);
          if (queued_entry == queued.cfd_to_max_mem_id_to_persist.end() ||
              queued_entry->second < requested.second) {
            covered = false;
            break;
          }
        }
        if (covered) {
          TEST_SYNC_POINT(
              "DBImpl::EnqueuePendingFlush:AtomicWBMRequestDeduplicated");
          return false;
        }
      }
    }
    // Atomic flush requests normally bypass the per-CF queued_for_flush()
    // guard. Cross-DB WBM requests are deduplicated above because its
    // autonomous retry loop must not repeatedly enqueue the same atomic cut
    // while HIGH workers are unavailable. Other atomic requests retain their
    // existing behavior.
    for (auto& iter : flush_req.cfd_to_max_mem_id_to_persist) {
      ColumnFamilyData* cfd = iter.first;
      cfd->Ref();
    }
    ++unscheduled_flushes_;
    flush_queue_.push_back(flush_req);
    RecordAtomicFlushRequestReason(stats_, flush_req.flush_reason);
    enqueued = true;
  }
  return enqueued;
}

void DBImpl::EnqueuePendingCompaction(ColumnFamilyData* cfd) {
  mutex_.AssertHeld();
  if (reject_new_background_jobs_ || cfd->IsDropped()) {
    return;
  }
  if (!cfd->queued_for_compaction() && cfd->NeedsCompaction()) {
    TEST_SYNC_POINT_CALLBACK("EnqueuePendingCompaction::cfd",
                             static_cast<void*>(cfd));
    if (cfd->compaction_aborted() > 0) {
      cfd->Ref();
      cfd->set_queued_for_compaction(true);
      [[maybe_unused]] const bool inserted =
          parked_compaction_cfds_.insert(cfd).second;
      assert(inserted);
    } else {
      AddToCompactionQueue(cfd);
    }
  }
}

void DBImpl::SchedulePendingPurge(std::string fname, std::string dir_to_sync,
                                  FileType type, uint64_t number, int job_id) {
  mutex_.AssertHeld();
  if (reject_new_background_jobs_) {
    return;
  }
  PurgeFileInfo file_info(fname, dir_to_sync, type, number, job_id);
  purge_files_.insert({{number, std::move(file_info)}});
}

void DBImpl::BGWorkFlush(void* arg) {
  FlushThreadArg fta = *(static_cast<FlushThreadArg*>(arg));
  delete static_cast<FlushThreadArg*>(arg);

  IOSTATS_SET_THREAD_POOL_ID(fta.thread_pri_);
  TEST_SYNC_POINT("DBImpl::BGWorkFlush");
  static_cast_with_check<DBImpl>(fta.db_)->BackgroundCallFlush(fta.thread_pri_);
  TEST_SYNC_POINT("DBImpl::BGWorkFlush:done");
}

void DBImpl::BGWorkCompaction(void* arg) {
  CompactionArg ca = *(static_cast<CompactionArg*>(arg));
  delete static_cast<CompactionArg*>(arg);
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::LOW);
  TEST_SYNC_POINT("DBImpl::BGWorkCompaction");
  auto prepicked_compaction =
      static_cast<PrepickedCompaction*>(ca.prepicked_compaction);
  static_cast_with_check<DBImpl>(ca.db)->BackgroundCallCompaction(
      prepicked_compaction, Env::Priority::LOW, ca.separate_remote_pool);
  delete prepicked_compaction;
}

void DBImpl::BGWorkBottomCompaction(void* arg) {
  CompactionArg ca = *(static_cast<CompactionArg*>(arg));
  delete static_cast<CompactionArg*>(arg);
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::BOTTOM);
  TEST_SYNC_POINT("DBImpl::BGWorkBottomCompaction");
  auto* prepicked_compaction = ca.prepicked_compaction;
  assert(prepicked_compaction && prepicked_compaction->compaction);
  ca.db->BackgroundCallCompaction(prepicked_compaction, Env::Priority::BOTTOM,
                                  ca.separate_remote_pool);
  delete prepicked_compaction;
}

void DBImpl::BGWorkPurge(void* db) {
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::HIGH);
  TEST_SYNC_POINT("DBImpl::BGWorkPurge:start");
  static_cast<DBImpl*>(db)->BackgroundCallPurge();
  TEST_SYNC_POINT("DBImpl::BGWorkPurge:end");
}

void DBImpl::UnscheduleCompactionCallback(void* arg) {
  CompactionArg* ca_ptr = static_cast<CompactionArg*>(arg);
  Env::Priority compaction_pri = ca_ptr->compaction_pri_;
  if (Env::Priority::BOTTOM == compaction_pri) {
    // Decrement bg_bottom_compaction_scheduled_ if priority is BOTTOM
    ca_ptr->db->bg_bottom_compaction_scheduled_--;
  } else if (Env::Priority::LOW == compaction_pri) {
    // Decrement bg_compaction_scheduled_ if priority is LOW
    ca_ptr->db->bg_compaction_scheduled_--;
  }
  CompactionArg ca = *(ca_ptr);
  delete static_cast<CompactionArg*>(arg);
  if (ca.prepicked_compaction != nullptr) {
    // if it's a manual compaction, set status to ManualCompactionPaused
    if (ca.prepicked_compaction->manual_compaction_state) {
      ca.prepicked_compaction->manual_compaction_state->done = true;
      ca.prepicked_compaction->manual_compaction_state->status =
          Status::Incomplete(Status::SubCode::kManualCompactionPaused);
    }
    if (ca.prepicked_compaction->compaction != nullptr) {
      ca.prepicked_compaction->compaction->ReleaseCompactionFiles(
          Status::Incomplete(Status::SubCode::kManualCompactionPaused));
      delete ca.prepicked_compaction->compaction;
    }
    delete ca.prepicked_compaction;
  }
  TEST_SYNC_POINT("DBImpl::UnscheduleCompactionCallback");
}

void DBImpl::UnscheduleFlushCallback(void* arg) {
  // Decrement bg_flush_scheduled_ in flush callback
  static_cast<FlushThreadArg*>(arg)->db_->bg_flush_scheduled_--;
  Env::Priority flush_pri = static_cast<FlushThreadArg*>(arg)->thread_pri_;
  if (Env::Priority::LOW == flush_pri) {
    TEST_SYNC_POINT("DBImpl::UnscheduleLowFlushCallback");
  } else if (Env::Priority::HIGH == flush_pri) {
    TEST_SYNC_POINT("DBImpl::UnscheduleHighFlushCallback");
  }
  delete static_cast<FlushThreadArg*>(arg);
  TEST_SYNC_POINT("DBImpl::UnscheduleFlushCallback");
}

void DBImpl::RefreshFlushableMemAccounting() {
  mutex_.AssertHeld();
  versions_->GetColumnFamilySet()->RefreshFlushableMemAccounting();
}

bool DBImpl::TryRefreshFlushableMemAccounting() {
  if (!mutex_.TryLock()) {
    return false;
  }
  Defer unlock_mutex([this] { mutex_.Unlock(); });
  RefreshFlushableMemAccounting();
  return wbm_flush_initiator_ != nullptr &&
         wbm_flush_initiator_->HasAccurateFlushableMemUsage();
}

DBImpl::FlushableCFs DBImpl::CollectFlushableCFs(
    bool include_waiting_immutable) {
  mutex_.AssertHeld();
  FlushableCFs result;
  SequenceNumber oldest_seq = kMaxSequenceNumber;
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    if (cfd->IsDropped() || !cfd->initialized()) {
      continue;
    }
    const size_t mutable_mem =
        cfd->mem()->IsEmpty() ? 0 : cfd->mem()->WBMTrackedMemoryUsage();
    size_t waiting_immutable_mem = 0;
    if (include_waiting_immutable) {
      if (immutable_db_options_.atomic_flush) {
        waiting_immutable_mem =
            cfd->imm()->WBMTrackedUnstartedMemTablesMemoryUsage();
      } else if (!cfd->imm()->IsFlushPendingOrRunning()) {
        waiting_immutable_mem =
            cfd->imm()->WBMTrackedUnflushedMemTablesMemoryUsage();
      }
    }
    result.total_mem += mutable_mem + waiting_immutable_mem;
    // Non-atomic flushes skip CFs already flushing.
    if (cfd->imm()->IsFlushPendingOrRunning()) {
      continue;
    }
    size_t mem = mutable_mem;
    if (include_waiting_immutable) {
      mem += waiting_immutable_mem;
    }
    if (mem == 0) {
      continue;
    }
    if (result.largest == nullptr || mem > result.largest_mem) {
      result.largest = cfd;
      result.largest_mem = mem;
    }
    const SequenceNumber seq = cfd->mem()->GetCreationSeq();
    if (result.oldest == nullptr || seq < oldest_seq) {
      result.oldest = cfd;
      oldest_seq = seq;
    }
  }
  return result;
}

bool DBImpl::ScheduleWriteBufferManagerFlush() {
  {
    if (!mutex_.TryLock()) {
      return false;
    }
    Defer unlock_mutex([this] { mutex_.Unlock(); });
    // Recheck bid eligibility because DB state may have changed.
    if (!write_buffer_manager_->ShouldTrackFlushInitiator() ||
        shutdown_initiated_.load(std::memory_order_acquire) ||
        shutting_down_.load(std::memory_order_acquire) ||
        reject_new_background_jobs_ || !opened_successfully_ || read_only_ ||
        error_handler_.IsBGWorkStopped() || write_controller_.IsStopped() ||
        // Reject both the pause drain and fully paused states.
        bg_work_paused_ > 0 || bg_compaction_paused_ > 0 ||
        // Never queue work that would wait behind this DB's stall.
        WouldBlockJoiningWriteThread()) {
      return false;
    }
    // A queued or running job does not count as new progress. Returning false
    // lets the coordinator try another DB instead of queuing more work behind
    // a busy LOW pool.
    if (bg_wbm_flush_scheduled_ > 0) {
      return false;
    }
    const FlushableCFs flushable =
        CollectFlushableCFs(true /* include_waiting_immutable */);
    const bool has_flushable_cf = immutable_db_options_.atomic_flush
                                      ? flushable.total_mem > 0
                                      : flushable.largest != nullptr;
    if (!has_flushable_cf) {
      wbm_flush_initiator_->SetHasFlushableCF(false);
      write_buffer_manager_->NotifyFlushInitiatorChanged();
      return false;
    }
    wbm_flush_initiator_->SetHasFlushableCF(false);
    write_buffer_manager_->NotifyFlushInitiatorChanged();
    ++bg_wbm_flush_scheduled_;
  }

  // The WBM-owned executor provides bounded capacity independent of Env::LOW.
  // Run outside mutex_ so DB-local work cannot block other DB mutex users.
  BackgroundCallWBMFlush();
  return true;
}

void DBImpl::BackgroundCallWBMFlush() {
  const bool atomic_flush = immutable_db_options_.atomic_flush;
  ColumnFamilyData* cfd_to_flush = nullptr;
  bool has_flushable_cf = false;
  {
    InstrumentedMutexLock l(&mutex_);
    if (!shutdown_initiated_.load(std::memory_order_acquire) &&
        !shutting_down_.load(std::memory_order_acquire) &&
        !reject_new_background_jobs_ && opened_successfully_ && !read_only_ &&
        !error_handler_.IsBGWorkStopped() && !write_controller_.IsStopped() &&
        bg_work_paused_ == 0 && bg_compaction_paused_ == 0 &&
        !WouldBlockJoiningWriteThread()) {
      const FlushableCFs flushable =
          CollectFlushableCFs(true /* include_waiting_immutable */);
      has_flushable_cf =
          atomic_flush ? flushable.total_mem > 0 : flushable.largest != nullptr;
      if (!atomic_flush) {
        cfd_to_flush = flushable.largest;
        if (cfd_to_flush != nullptr) {
          cfd_to_flush->Ref();
        }
      }
    }
  }
  TEST_SYNC_POINT("DBImpl::BackgroundCallWBMFlush:AfterPick");

  FlushOptions flush_options;
  flush_options.wait = false;
  flush_options.allow_write_stall = true;
  bool made_progress = false;
  if (has_flushable_cf) {
    Status s;
    if (atomic_flush) {
      s = AtomicFlushMemTablesImpl(
          flush_options, FlushReason::kWriteBufferManager,
          {} /* provided_candidate_cfds */, false /* entered_write_thread */,
          WriteThreadJoinMode::kNonBlocking, &made_progress);
    } else {
      s = FlushMemTableImpl(cfd_to_flush, flush_options,
                            FlushReason::kWriteBufferManager,
                            false /* entered_write_thread */,
                            WriteThreadJoinMode::kNonBlocking, &made_progress);
    }
    if (!s.ok()) {
      ROCKS_LOG_WARN(
          immutable_db_options_.info_log,
          "Flush on behalf of a shared WriteBufferManager failed: %s",
          s.ToString().c_str());
    }
    s.PermitUncheckedError();
  }
  TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCallWBMFlush:MadeProgress",
                           &made_progress);

  InstrumentedMutexLock l(&mutex_);
  if (cfd_to_flush != nullptr) {
    cfd_to_flush->UnrefAndTryDelete();
  }
  RefreshFlushableMemAccounting();
  --bg_wbm_flush_scheduled_;
  write_buffer_manager_->NotifyFlushInitiatorFlushCompleted(
      wbm_flush_initiator_.get(), made_progress);
  TEST_SYNC_POINT_CALLBACK("DBImpl::BGWorkWBMFlush:done", this);
  bg_cv_.SignalAll();
}

Status DBImpl::BackgroundFlush(bool* made_progress, JobContext* job_context,
                               LogBuffer* log_buffer, FlushReason* reason,
                               bool* flush_rescheduled_to_retain_udt,
                               Env::Priority thread_pri) {
  mutex_.AssertHeld();

  Status status;
  *reason = FlushReason::kOthers;
  // During recovery, allow flush workers to inspect the queue. Non-recovery
  // requests are dropped below.
  if (!error_handler_.IsBGWorkStopped()) {
    if (shutting_down_.load(std::memory_order_acquire)) {
      status = Status::ShutdownInProgress();
    }
  } else if (!error_handler_.IsRecoveryInProgress()) {
    status = error_handler_.GetBGError();
  }

  if (!status.ok()) {
    return status;
  }

  autovector<BGFlushArg> bg_flush_args;
  std::vector<SuperVersionContext>& superversion_contexts =
      job_context->superversion_contexts;
  autovector<ColumnFamilyData*> column_families_not_to_flush;
  while (!flush_queue_.empty()) {
    // This cfd is already referenced
    FlushRequest flush_req = PopFirstFromFlushQueue();
    FlushReason flush_reason = flush_req.flush_reason;
    if (!error_handler_.GetBGError().ok() && error_handler_.IsBGWorkStopped() &&
        !IsRecoveryFlush(flush_reason)) {
      // A request queued before the error can reach a worker before recovery
      // clears the queue. Drop it; recovery rebuilds flush work for all CFs.
      status = error_handler_.GetBGError();
      assert(!status.ok());
      ROCKS_LOG_BUFFER(log_buffer,
                       "[JOB %d] Abort flush due to background error %s",
                       job_context->job_id, status.ToString().c_str());
      *reason = flush_reason;
      for (auto item : flush_req.cfd_to_max_mem_id_to_persist) {
        item.first->UnrefAndTryDelete();
      }
      return status;
    }
    if (!flush_req.atomic_flush &&
        ShouldRescheduleFlushRequestToRetainUDT(flush_req)) {
      assert(flush_req.cfd_to_max_mem_id_to_persist.size() == 1);
      ColumnFamilyData* cfd =
          flush_req.cfd_to_max_mem_id_to_persist.begin()->first;
      if (cfd->UnrefAndTryDelete()) {
        return Status::OK();
      }
      ROCKS_LOG_BUFFER(log_buffer,
                       "FlushRequest for column family %s is re-scheduled to "
                       "retain user-defined timestamps.",
                       cfd->GetName().c_str());
      // Reschedule the `FlushRequest` as is without checking dropped column
      // family etc. The follow-up job will do the check anyways, so save the
      // duplication. Column family is deduplicated by `SchdulePendingFlush` and
      // `PopFirstFromFlushQueue` contains at flush request enqueueing and
      // dequeueing time.
      // This flush request is rescheduled right after it's popped from the
      // queue while the db mutex is held, so there should be no other
      // FlushRequest for the same column family with higher `max_memtable_id`
      // in the queue to block the reschedule from succeeding.
#ifndef NDEBUG
      flush_req.reschedule_count += 1;
#endif /* !NDEBUG */
      EnqueuePendingFlush(flush_req);
      *reason = flush_reason;
      *flush_rescheduled_to_retain_udt = true;
      return Status::TryAgain();
    }
    superversion_contexts.clear();
    superversion_contexts.reserve(
        flush_req.cfd_to_max_mem_id_to_persist.size());

    for (const auto& [cfd, max_memtable_id] :
         flush_req.cfd_to_max_mem_id_to_persist) {
      if (cfd->GetMempurgeUsed()) {
        // If imm() contains silent memtables (e.g.: because
        // MemPurge was activated), requesting a flush will
        // mark the imm_needed as true.
        cfd->imm()->FlushRequested();
      }

      if (cfd->IsDropped() || !cfd->imm()->IsFlushPending()) {
        // can't flush this CF, try next one
        column_families_not_to_flush.push_back(cfd);
        continue;
      }
      superversion_contexts.emplace_back(true);
      bg_flush_args.emplace_back(cfd, max_memtable_id,
                                 &(superversion_contexts.back()), flush_reason,
                                 flush_req.atomic_flush);
    }
    // `MaybeScheduleFlushOrCompaction` schedules as many `BackgroundCallFlush`
    // jobs as the number of `FlushRequest` in the `flush_queue_`, a.k.a
    // `unscheduled_flushes_`. So it's sufficient to make each `BackgroundFlush`
    // handle one `FlushRequest` and each have a Status returned.
    if (!bg_flush_args.empty() || !column_families_not_to_flush.empty()) {
      TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundFlush:CheckFlushRequest:cb",
                               const_cast<int*>(&flush_req.reschedule_count));
      break;
    }
  }

  if (!bg_flush_args.empty()) {
    auto bg_job_limits = GetBGJobLimits();
    for (const auto& arg : bg_flush_args) {
      ColumnFamilyData* cfd = arg.cfd_;
      ROCKS_LOG_BUFFER(
          log_buffer,
          "Calling FlushMemTableToOutputFile with column "
          "family [%s], flush slots available %d, compaction slots available "
          "%d, "
          "flush slots scheduled %d, compaction slots scheduled %d",
          cfd->GetName().c_str(), bg_job_limits.max_flushes,
          bg_job_limits.max_compactions, bg_flush_scheduled_,
          bg_compaction_scheduled_);
    }
    status = FlushMemTablesToOutputFiles(bg_flush_args, made_progress,
                                         job_context, log_buffer, thread_pri);
    TEST_SYNC_POINT("DBImpl::BackgroundFlush:BeforeFlush");
// All the CFD/bg_flush_arg in the FlushReq must have the same flush reason, so
// just grab the first one
#ifndef NDEBUG
    for (const auto& bg_flush_arg : bg_flush_args) {
      assert(bg_flush_arg.flush_reason_ == bg_flush_args[0].flush_reason_);
    }
#endif /* !NDEBUG */
    *reason = bg_flush_args[0].flush_reason_;
    for (auto& arg : bg_flush_args) {
      ColumnFamilyData* cfd = arg.cfd_;
      if (cfd->UnrefAndTryDelete()) {
        arg.cfd_ = nullptr;
      }
    }
  }
  for (auto cfd : column_families_not_to_flush) {
    cfd->UnrefAndTryDelete();
  }
  return status;
}

void DBImpl::BackgroundCallFlush(Env::Priority thread_pri) {
  bool made_progress = false;
  JobContext job_context(next_job_id_.fetch_add(1), true);

  TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCallFlush:start", nullptr);

  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());
  TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:Start:1");
  TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:Start:2");
  {
    InstrumentedMutexLock l(&mutex_);
    assert(bg_flush_scheduled_);
    num_running_flushes_++;

    std::unique_ptr<std::list<uint64_t>::iterator>
        pending_outputs_inserted_elem(new std::list<uint64_t>::iterator(
            CaptureCurrentFileNumberInPendingOutputs()));
    FlushReason reason;
    bool flush_rescheduled_to_retain_udt = false;
    Status s =
        BackgroundFlush(&made_progress, &job_context, &log_buffer, &reason,
                        &flush_rescheduled_to_retain_udt, thread_pri);
    if (s.IsTryAgain() && flush_rescheduled_to_retain_udt) {
      bg_cv_.SignalAll();  // In case a waiter can proceed despite the error
      mutex_.Unlock();
      TEST_SYNC_POINT_CALLBACK("DBImpl::AfterRetainUDTReschedule:cb", nullptr);
      immutable_db_options_.clock->SleepForMicroseconds(
          100000);  // prevent hot loop
      mutex_.Lock();
    } else if (!s.ok() && !s.IsShutdownInProgress() &&
               !s.IsColumnFamilyDropped() &&
               reason != FlushReason::kErrorRecovery) {
      // Wait a little bit before retrying background flush in
      // case this is an environmental problem and we do not want to
      // chew up resources for failed flushes for the duration of
      // the problem.
      uint64_t error_cnt =
          default_cf_internal_stats_->BumpAndGetBackgroundErrorCount();
      bg_cv_.SignalAll();  // In case a waiter can proceed despite the error
      mutex_.Unlock();
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "[JOB %d] Waiting after background flush error: %s, "
                      "Accumulated background error counts: %" PRIu64,
                      job_context.job_id, s.ToString().c_str(), error_cnt);
      log_buffer.FlushBufferToLog();
      LogFlush(immutable_db_options_.info_log);
      immutable_db_options_.clock->SleepForMicroseconds(1000000);
      mutex_.Lock();
    }

    TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:FlushFinish:0");
    ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem);
    // There is no need to find obsolete files if the flush job is rescheduled
    // to retain user-defined timestamps because the job doesn't get to the
    // stage of actually flushing the MemTables.
    if (!flush_rescheduled_to_retain_udt) {
      // If flush failed, we want to delete all temporary files that we might
      // have created. Thus, we force full scan in FindObsoleteFiles()
      FindObsoleteFiles(&job_context, !s.ok() && !s.IsShutdownInProgress() &&
                                          !s.IsColumnFamilyDropped());
    }
    // delete unnecessary files if any, this is done outside the mutex
    if (job_context.HaveSomethingToClean() ||
        job_context.HaveSomethingToDelete() || !log_buffer.IsEmpty()) {
      mutex_.Unlock();
      TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:FilesFound");
      // Have to flush the info logs before bg_flush_scheduled_--
      // because if bg_flush_scheduled_ becomes 0 and the lock is
      // released, the deconstructor of DB can kick in and destroy all the
      // states of DB so info_log might not be available after that point.
      // It also applies to access other states that DB owns.
      log_buffer.FlushBufferToLog();
      if (job_context.HaveSomethingToDelete()) {
        PurgeObsoleteFiles(job_context);
      }
      job_context.Clean();
      mutex_.Lock();
    }
    TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:ContextCleanedUp");

    assert(num_running_flushes_ > 0);
    num_running_flushes_--;
    bg_flush_scheduled_--;
    if (write_buffer_manager_->ShouldTrackFlushInitiator()) {
      RefreshFlushableMemAccounting();
    }
    // See if there's more work to be done
    MaybeScheduleFlushOrCompaction();

    NotifyOnBackgroundJobPressureChanged();

    atomic_flush_install_cv_.SignalAll();
    bg_cv_.SignalAll();
    // IMPORTANT: there should be no code after calling SignalAll. This call may
    // signal the DB destructor that it's OK to proceed with destruction. In
    // that case, all DB variables will be dealloacated and referencing them
    // will cause trouble.
  }
}

void DBImpl::BackgroundCallCompaction(PrepickedCompaction* prepicked_compaction,
                                      Env::Priority bg_thread_pri,
                                      bool separate_remote_pool) {
  bool made_progress = false;
  int deferred_compactions = 0;
  JobContext job_context(next_job_id_.fetch_add(1), true);
  TEST_SYNC_POINT("BackgroundCallCompaction:0");
  if (bg_thread_pri == Env::Priority::BOTTOM) {
    TEST_SYNC_POINT("BackgroundCallCompaction:0:BottomPri");
  }

  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());
  {
    InstrumentedMutexLock l(&mutex_);

    num_running_compactions_++;
    const int priority_index = bg_thread_pri == Env::Priority::BOTTOM ? 0 : 1;
    if (!separate_remote_pool) {
      ++local_compactions_running_by_priority_[priority_index];
    }
    if (bg_thread_pri == Env::Priority::BOTTOM) {
      num_running_bottom_compactions_++;
    }

    std::unique_ptr<std::list<uint64_t>::iterator>
        pending_outputs_inserted_elem(new std::list<uint64_t>::iterator(
            CaptureCurrentFileNumberInPendingOutputs()));

    assert(bg_thread_pri == Env::Priority::BOTTOM ||
           bg_thread_pri == Env::Priority::LOW);
    assert(separate_remote_pool ? remote_coordinators_scheduled_ > 0
                                : (bg_thread_pri == Env::Priority::BOTTOM
                                       ? bg_bottom_compaction_scheduled_ > 0
                                       : bg_compaction_scheduled_ > 0));
    Status s = BackgroundCompaction(
        &made_progress, &job_context, &log_buffer, prepicked_compaction,
        bg_thread_pri, separate_remote_pool, &deferred_compactions);
    TEST_SYNC_POINT("BackgroundCallCompaction:1");
    if (s.IsBusy()) {
      bg_cv_.SignalAll();  // In case a waiter can proceed despite the error
      mutex_.Unlock();
      TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCallCompaction:BeforeBackoff",
                               &s);
      immutable_db_options_.clock->SleepForMicroseconds(
          10000);  // prevent hot loop
      mutex_.Lock();
    } else if (!s.ok() && !s.IsShutdownInProgress() &&
               !s.IsManualCompactionPaused() && !s.IsColumnFamilyDropped() &&
               !s.IsCompactionAborted()) {
      // Wait a little bit before retrying background compaction in
      // case this is an environmental problem and we do not want to
      // chew up resources for failed compactions for the duration of
      // the problem.
      uint64_t error_cnt =
          default_cf_internal_stats_->BumpAndGetBackgroundErrorCount();
      bg_cv_.SignalAll();  // In case a waiter can proceed despite the error
      mutex_.Unlock();
      TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCallCompaction:BeforeBackoff",
                               &s);
      log_buffer.FlushBufferToLog();
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "Waiting after background compaction error: %s, "
                      "Accumulated background error counts: %" PRIu64,
                      s.ToString().c_str(), error_cnt);
      LogFlush(immutable_db_options_.info_log);
      immutable_db_options_.clock->SleepForMicroseconds(1000000);
      mutex_.Lock();
    } else if (s.IsManualCompactionPaused()) {
      assert(prepicked_compaction);
      ManualCompactionState* m = prepicked_compaction->manual_compaction_state;
      assert(m);
      ROCKS_LOG_BUFFER(&log_buffer, "[%s] [JOB %d] Manual compaction paused",
                       m->cfd->GetName().c_str(), job_context.job_id);
    }

    ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem);

    // If compaction failed, we want to delete all temporary files that we
    // might have created (they might not be all recorded in job_context in
    // case of a failure). Thus, we force full scan in FindObsoleteFiles()
    FindObsoleteFiles(&job_context, !s.ok() && !s.IsShutdownInProgress() &&
                                        !s.IsManualCompactionPaused() &&
                                        !s.IsCompactionAborted() &&
                                        !s.IsColumnFamilyDropped() &&
                                        !s.IsBusy());
    TEST_SYNC_POINT("DBImpl::BackgroundCallCompaction:FoundObsoleteFiles");

    // delete unnecessary files if any, this is done outside the mutex
    if (job_context.HaveSomethingToClean() ||
        job_context.HaveSomethingToDelete() || !log_buffer.IsEmpty()) {
      mutex_.Unlock();
      // Have to flush the info logs before bg_compaction_scheduled_--
      // because if bg_flush_scheduled_ becomes 0 and the lock is
      // released, the deconstructor of DB can kick in and destroy all the
      // states of DB so info_log might not be available after that point.
      // It also applies to access other states that DB owns.
      log_buffer.FlushBufferToLog();
      if (job_context.HaveSomethingToDelete()) {
        PurgeObsoleteFiles(job_context);
        TEST_SYNC_POINT("DBImpl::BackgroundCallCompaction:PurgedObsoleteFiles");
      }
      job_context.Clean();
      mutex_.Lock();
    }

    assert(num_running_compactions_ > 0);
    num_running_compactions_--;
    if (separate_remote_pool) {
      --remote_coordinators_scheduled_;
    } else {
      --local_compactions_running_by_priority_[priority_index];
      if (bg_thread_pri == Env::Priority::BOTTOM) {
        --bg_bottom_compaction_scheduled_;
      } else {
        --bg_compaction_scheduled_;
      }
    }

    if (bg_thread_pri == Env::Priority::BOTTOM) {
      assert(num_running_bottom_compactions_ > 0);
      num_running_bottom_compactions_--;
    }

    unscheduled_compactions_ += deferred_compactions;
    // See if there's more work to be done
    MaybeScheduleFlushOrCompaction();

    NotifyOnBackgroundJobPressureChanged();

    if (prepicked_compaction != nullptr &&
        prepicked_compaction->task_token != nullptr) {
      // Releasing task tokens affects (and asserts on) the DB state, so
      // must be done before we potentially signal the DB close process to
      // proceed below.
      prepicked_compaction->task_token.reset();
    }

    if (separate_remote_pool || made_progress ||
        per_cf_compaction_abort_waiters_ > 0 || CompactionsScheduled() == 0 ||
        HasPendingManualCompaction() || unscheduled_compactions_ == 0) {
      // signal if
      // * made_progress -- need to wakeup DelayWrite
      // * bg_{bottom,}_compaction_scheduled_ == 0 -- need to wakeup ~DBImpl
      // * HasPendingManualCompaction -- need to wakeup RunManualCompaction
      // If none of this is true, there is no need to signal since nobody is
      // waiting for it
      bg_cv_.SignalAll();
    }
    // IMPORTANT: there should be no code after calling SignalAll. This call may
    // signal the DB destructor that it's OK to proceed with destruction. In
    // that case, all DB variables will be dealloacated and referencing them
    // will cause trouble.
  }
}

// Precondition: mutex_ must be held when calling this function.
Status DBImpl::BackgroundCompaction(
    bool* made_progress, JobContext* job_context, LogBuffer* log_buffer,
    PrepickedCompaction* prepicked_compaction, Env::Priority thread_pri,
    bool separate_remote_pool, int* deferred_compactions) {
  ManualCompactionState* manual_compaction =
      prepicked_compaction == nullptr
          ? nullptr
          : prepicked_compaction->manual_compaction_state;
  *made_progress = false;
  mutex_.AssertHeld();
  const auto restore_compaction_credit = [&] {
    if (separate_remote_pool) {
      ++*deferred_compactions;
    } else {
      ++unscheduled_compactions_;
    }
  };
  TEST_SYNC_POINT("DBImpl::BackgroundCompaction:Start");

  const ReadOptions read_options(Env::IOActivity::kCompaction);
  const WriteOptions write_options(Env::IOActivity::kCompaction);

  bool is_manual = (manual_compaction != nullptr);
  std::unique_ptr<Compaction> c;
  if (prepicked_compaction != nullptr &&
      prepicked_compaction->compaction != nullptr) {
    c.reset(prepicked_compaction->compaction);
  }
  // Track this CF as having an in-flight compaction for the duration of this
  // BackgroundCompaction (which covers NotifyOnCompactionCompleted). The Defer
  // ends tracking on every return path. BeginInFlightCompaction()'s Ref keeps a
  // dropped CF alive across any c.reset() until then; if this ends the CF's
  // last reference, the FindObsoleteFiles() pass in BackgroundCallCompaction
  // cleans up its files.
  ColumnFamilyData* in_flight_cfd = nullptr;
  Defer end_in_flight_compaction([&]() {
    if (in_flight_cfd != nullptr) {
      EndInFlightCompaction(in_flight_cfd);
    }
  });
  if (c != nullptr) {
    in_flight_cfd = c->column_family_data();
    BeginInFlightCompaction(in_flight_cfd);
  }
  bool is_prepicked = is_manual || c;
  ColumnFamilyData* prepicked_cfd = nullptr;
  if (c) {
    prepicked_cfd = c->column_family_data();
  } else if (manual_compaction != nullptr) {
    prepicked_cfd = manual_compaction->cfd;
  }

  // (manual_compaction->in_progress == false);
  bool trivial_move_disallowed =
      is_manual && manual_compaction->disallow_trivial_move;

  CompactionJobStats compaction_job_stats;
  // Set is_remote_compaction to true on CompactionBegin Event if
  // compaction_service is set except for trivial moves. We do not know whether
  // remote compaction will actually be successfully scheduled, or fall back to
  // local at this time. CompactionCompleted event will tell the truth where
  // the compaction actually happened.
  compaction_job_stats.is_remote_compaction =
      immutable_db_options().compaction_service != nullptr;

  Status status;
  if (!error_handler_.IsBGWorkStopped()) {
    if (shutting_down_.load(std::memory_order_acquire)) {
      status = Status::ShutdownInProgress();
    } else if (compaction_aborted_.load(std::memory_order_acquire) > 0) {
      status = Status::Incomplete(Status::SubCode::kCompactionAborted);
      if (!is_prepicked) {
        // This automatic compaction was scheduled from compaction_queue_, but
        // abort happened before PickCompactionFromQueue() could remove the
        // queued CF. Restore the unscheduled count so ResumeAllCompactions()
        // can schedule the still-queued work.
        restore_compaction_credit();
      }
    } else if (prepicked_cfd != nullptr &&
               prepicked_cfd->compaction_aborted() > 0) {
      status = Status::Incomplete(Status::SubCode::kCompactionAborted);
    } else if (is_manual &&
               manual_compaction->canceled.load(std::memory_order_acquire)) {
      status = Status::Incomplete(Status::SubCode::kManualCompactionPaused);
    }
  } else {
    status = error_handler_.GetBGError();
    // If we get here, it means a hard error happened after this compaction
    // was scheduled by MaybeScheduleFlushOrCompaction(), but before it got a
    // chance to execute. Since non-prepicked work consumed an unscheduled
    // compaction credit without popping a cfd from the compaction queue,
    // restore the credit here. Prepicked work was scheduled directly and did
    // not consume unscheduled_compactions_.
    if (!is_prepicked) {
      restore_compaction_credit();
    }
  }

  if (!status.ok()) {
    if (is_manual) {
      manual_compaction->status = status;
      manual_compaction->done = true;
      manual_compaction->in_progress = false;
      manual_compaction = nullptr;
    }
    if (c) {
      c->ReleaseCompactionFiles(status);
      c.reset();
    }
    return status;
  }

  if (is_manual) {
    // another thread cannot pick up the same work
    manual_compaction->in_progress = true;
  }

  TEST_SYNC_POINT("DBImpl::BackgroundCompaction:InProgress");

  std::unique_ptr<TaskLimiterToken> task_token;

  bool sfm_reserved_compact_space = false;
  if (is_manual) {
    ManualCompactionState* m = manual_compaction;
    assert(m->in_progress);
    if (!c) {
      m->done = true;
      m->manual_end = nullptr;
      ROCKS_LOG_BUFFER(
          log_buffer,
          "[%s] Manual compaction from level-%d from %s .. "
          "%s; nothing to do\n",
          m->cfd->GetName().c_str(), m->input_level,
          (m->begin ? m->begin->DebugString(true).c_str() : "(begin)"),
          (m->end ? m->end->DebugString(true).c_str() : "(end)"));
    } else {
      // First check if we have enough room to do the compaction
      bool enough_room = EnoughRoomForCompaction(
          m->cfd, *(c->inputs()), &sfm_reserved_compact_space, log_buffer);

      if (!enough_room) {
        // Then don't do the compaction
        c->ReleaseCompactionFiles(status);
        c.reset();
        // m's vars will get set properly at the end of this function,
        // as long as status == CompactionTooLarge
        status = Status::CompactionTooLarge();
      } else {
        ROCKS_LOG_BUFFER(
            log_buffer,
            "[%s] Manual compaction from level-%d to level-%d from %s .. "
            "%s; will stop at %s\n",
            m->cfd->GetName().c_str(), m->input_level, c->output_level(),
            (m->begin ? m->begin->DebugString(true).c_str() : "(begin)"),
            (m->end ? m->end->DebugString(true).c_str() : "(end)"),
            ((m->done || m->manual_end == nullptr)
                 ? "(end)"
                 : m->manual_end->DebugString(true).c_str()));
      }
    }
  } else if (ShouldPickCompaction(is_prepicked, prepicked_compaction)) {
    bool need_repick = is_prepicked && prepicked_compaction->need_repick;
    if (HasExclusiveManualCompaction()) {
      TEST_SYNC_POINT("DBImpl::BackgroundCompaction()::Conflict");

      // TODO(hx235): Resolve conflict between intended
      // bottom-priority compaction (requiring repick, i.e., need_repick = true)
      // and exclusive manual compaction by releasing the intended
      // bottom-priority compaction.
      if (!need_repick) {
        // Can't compact right now, but try again later
        //
        // Increase `unscheduled_compactions_` directly so we
        // don't need to
        // dequeue and enqueue the CFD again in the compaction queue and thus
        // keep the CFD's position in the queue
        restore_compaction_credit();

        return Status::OK();
      }
    }

    ColumnFamilyData* cfd = nullptr;

    if (!need_repick) {
      auto pick_result = PickCompactionFromQueue(&task_token, log_buffer);
      if (std::holds_alternative<CompactionQueueThrottled>(pick_result)) {
        // Can't find any executable task from the compaction queue.
        // All tasks have been throttled by compaction thread limiter.
        restore_compaction_credit();
        return Status::Busy();
      }
      if (std::holds_alternative<CompactionQueueParked>(pick_result)) {
        return Status::Incomplete(Status::SubCode::kCompactionAborted);
      }
      cfd = std::get<CompactionQueuePicked>(pick_result).cfd;
      assert(cfd != nullptr);

      // We unreference here because the following code will take a Ref() on
      // this cfd if it is going to use it (Compaction class holds a
      // reference).
      // This will all happen under a mutex so we don't have to be afraid of
      // somebody else deleting it.
      if (cfd->UnrefAndTryDelete()) {
        // This was the last reference of the column family, so no need to
        // compact.
        return Status::OK();
      }
    } else {
      cfd = c->column_family_data();
      assert(cfd);
      ResetBottomPriCompactionIntent(cfd, c);
      assert(c == nullptr);
    }

    // Pick up latest mutable CF Options and use it throughout the
    // compaction job
    // Compaction makes a copy of the latest MutableCFOptions. It should be used
    // throughout the compaction procedure to make sure consistency. It will
    // eventually be installed into SuperVersion
    const auto& mutable_cf_options = cfd->GetLatestMutableCFOptions();
    if (!mutable_cf_options.disable_auto_compactions && !cfd->IsDropped()) {
      // NOTE: try to avoid unnecessary copy of MutableCFOptions if
      // compaction is not necessary. Need to make sure mutex is held
      // until we make a copy in the following code
      TEST_SYNC_POINT("DBImpl::BackgroundCompaction():BeforePickCompaction");
      // This info is not useful for other scenarios, so save querying existing
      // snapshots for those cases.
      if (cfd->ioptions().compaction_style == kCompactionStyleUniversal &&
          cfd->user_comparator()->timestamp_size() == 0) {
        InitSnapshotContext(job_context);
        assert(is_snapshot_supported_ || snapshots_.empty());
      }
      c.reset(cfd->PickCompaction(
          mutable_cf_options, mutable_db_options_, job_context->snapshot_seqs,
          job_context->snapshot_checker, log_buffer,
          thread_pri == Env::Priority::BOTTOM /* require_max_output_level */));
      if (c != nullptr && in_flight_cfd == nullptr) {
        in_flight_cfd = cfd;
        BeginInFlightCompaction(in_flight_cfd);
      }
      if (thread_pri == Env::Priority::LOW) {
        TEST_SYNC_POINT("DBImpl::BackgroundCompaction():AfterPickCompaction");
      } else if (thread_pri == Env::Priority::BOTTOM) {
        TEST_SYNC_POINT_CALLBACK(
            "DBImpl::BackgroundCompaction():AfterPickCompactionBottomPri",
            c.get());
      }

      if (c != nullptr) {
        bool enough_room = EnoughRoomForCompaction(
            cfd, *(c->inputs()), &sfm_reserved_compact_space, log_buffer);

        if (!enough_room) {
          // Then don't do the compaction
          c->ReleaseCompactionFiles(status);
          c->column_family_data()
              ->current()
              ->storage_info()
              ->ComputeCompactionScore(c->immutable_options(),
                                       c->mutable_cf_options(),
                                       cfd->GetFullHistoryTsLow());
          EnqueuePendingCompaction(cfd);

          c.reset();
          // Don't need to sleep here, because BackgroundCallCompaction
          // will sleep if !s.ok()
          status = Status::CompactionTooLarge();
        } else {
          // update statistics
          size_t num_files = 0;
          for (auto& each_level : *c->inputs()) {
            num_files += each_level.files.size();
          }
          RecordInHistogram(stats_, NUM_FILES_IN_SINGLE_COMPACTION, num_files);

          // There are three things that can change compaction score:
          // 1) When flush or compaction finish. This case is covered by
          // InstallSuperVersionAndScheduleWork
          // 2) When MutableCFOptions changes. This case is also covered by
          // InstallSuperVersionAndScheduleWork, because this is when the new
          // options take effect.
          // 3) When we Pick a new compaction, we "remove" those files being
          // compacted from the calculation, which then influences compaction
          // score. Inside EnqueuePendingCompaction(),  we check if we need
          // the new compaction even without the files that are currently being
          // compacted. If we need another compaction, we might be able to
          // execute it in parallel, so we add it to the queue and schedule a
          // new thread.
          EnqueuePendingCompaction(cfd);
          MaybeScheduleFlushOrCompaction();
        }
      } else if (is_prepicked) {
        ROCKS_LOG_BUFFER(
            log_buffer,
            "[%s] Pre-picked compaction repicked files for compaction as "
            "required, "
            "but upon re-evaluation, no compaction was found necessary \n",
            cfd->GetName().c_str());
      }
    }
  }

  IOStatus io_s;
  bool compaction_released = false;
  if (c) {
    c->SetUseSeparateRemoteCompactionPool(separate_remote_pool);
  }
  if (!c) {
    // Nothing to do
    ROCKS_LOG_BUFFER(log_buffer, "Compaction nothing to do");
  } else if (c->deletion_compaction()) {
    // TODO(icanadi) Do we want to honor snapshots here? i.e. not delete old
    // file if there is alive snapshot pointing to it
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:BeforeCompaction",
                             c->column_family_data());
    assert(c->num_input_files(1) == 0);
    assert(c->column_family_data()->ioptions().compaction_style ==
           kCompactionStyleFIFO);

    compaction_job_stats.num_input_files = c->num_input_files(0);

    NotifyOnCompactionBegin(c->column_family_data(), c.get(), status,
                            compaction_job_stats, job_context->job_id);

    for (const auto& f : *c->inputs(0)) {
      c->edit()->DeleteFile(c->level(), f->fd.GetNumber());
    }
    NotifyOnCompactionPreCommit(c->column_family_data(), c.get(), status,
                                compaction_job_stats, job_context->job_id);
    status = versions_->LogAndApply(
        c->column_family_data(), read_options, write_options, c->edit(),
        &mutex_, directories_.GetDbDir(),
        /*new_descriptor_log=*/false, /*column_family_options=*/nullptr,
        [&c, &compaction_released](const Status& s) {
          c->ReleaseCompactionFiles(s);
          compaction_released = true;
        });
    io_s = versions_->io_status();
    InstallSuperVersionAndScheduleWork(
        c->column_family_data(), job_context->superversion_contexts.data(),
        /*defer_snapshot_refresh=*/false);
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Deleted %d files\n",
                     c->column_family_data()->GetName().c_str(),
                     c->num_input_files(0));
    if (status.ok() && io_s.ok()) {
      UpdateFIFOCompactionStatus(c);
    }
    *made_progress = true;
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:AfterCompaction",
                             c->column_family_data());
  } else if (c->is_trivial_copy_compaction()) {
    TEST_SYNC_POINT_CALLBACK(
        "DBImpl::BackgroundCompaction:TriviaCopyBeforeCompaction",
        c->column_family_data());
    assert(c->num_input_files(1) == 0);
    assert(c->column_family_data()->ioptions().compaction_style ==
           kCompactionStyleFIFO);
    assert(c->compaction_reason() == CompactionReason::kChangeTemperature);

    compaction_job_stats.num_input_files = c->num_input_files(0);

    NotifyOnCompactionBegin(c->column_family_data(), c.get(), status,
                            compaction_job_stats, job_context->job_id);

    std::vector<FileMetaData> out_files;
    if (separate_remote_pool) {
      mutex_.Unlock();
      try {
        status = RunLocalCompaction(
            [&] {
              io_s = CopyTrivialCompactionFiles(c.get(), log_buffer, &out_files,
                                                job_context->job_id, false);
            },
            thread_pri, c->column_family_data(),
            is_manual ? manual_compaction->canceled
                      : kManualCompactionCanceledFalse_,
            is_manual);
      } catch (...) {
        status = Status::Aborted();
      }
      mutex_.Lock();
    } else {
      io_s = CopyTrivialCompactionFiles(c.get(), log_buffer, &out_files,
                                        job_context->job_id, true);
    }

    // Update version set
    if (status.ok() && io_s.ok()) {
      // NOTE: ChangeTemperature should only copy one file at one file
      // hence *c->inputs(0) == out_files.size() == 1 if copy succeeded
      assert(c->inputs(0)->size() == 1);
      assert(out_files.size() == 1);

      auto out_file_metadata_it = out_files.begin();
      for (const auto& in_file : *c->inputs(0)) {
        if (out_file_metadata_it == out_files.end()) {
          break;
        }

        c->edit()->DeleteFile(c->level(), in_file->fd.GetNumber());
        c->edit()->AddFile(c->level(), *out_file_metadata_it);
        ++out_file_metadata_it;
      }

      NotifyOnCompactionPreCommit(c->column_family_data(), c.get(), status,
                                  compaction_job_stats, job_context->job_id);
      status = versions_->LogAndApply(
          c->column_family_data(), read_options, write_options, c->edit(),
          &mutex_, directories_.GetDbDir(),
          /*new_descriptor_log=*/false, /*column_family_options=*/nullptr,
          [&c, &compaction_released](const Status& s) {
            c->ReleaseCompactionFiles(s);
            compaction_released = true;
          });
    }

    if (io_s.ok()) {
      io_s = versions_->io_status();
    }

    InstallSuperVersionAndScheduleWork(
        c->column_family_data(), job_context->superversion_contexts.data(),
        /*defer_snapshot_refresh=*/false);
    if (status.ok() && io_s.ok()) {
      UpdateFIFOCompactionStatus(c);
    } else {
      for (const auto& in_file : *c->inputs(0)) {
        const std::string in_fname =
            TableFileName(c->immutable_options().cf_paths,
                          in_file->fd.GetNumber(), in_file->fd.GetPathId());
        ROCKS_LOG_BUFFER(
            log_buffer,
            "[%s] Failed to do trvial copy compaction: %s"
            " temperature=%s, to temperature=%s, status=%s, io_status=%s",
            c->column_family_data()->GetName().c_str(), in_fname.c_str(),
            temperature_to_string[in_file->temperature].c_str(),
            temperature_to_string[c->GetOutputTemperature()].c_str(),
            status.ToString().c_str(), io_s.ToString().c_str());
      }
    }
    *made_progress = true;
    TEST_SYNC_POINT_CALLBACK(
        "DBImpl::BackgroundCompaction:TriviaCopyAfterCompaction",
        c->column_family_data());
  } else if (!trivial_move_disallowed && c->IsTrivialMove()) {
    TEST_SYNC_POINT("DBImpl::BackgroundCompaction:TrivialMove");
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:BeforeCompaction",
                             c->column_family_data());
    // Instrument for event update
    // TODO(yhchiang): add op details for showing trivial-move.
    ThreadStatusUtil::SetColumnFamily(c->column_family_data());
    ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_COMPACTION);

    compaction_job_stats.num_input_files = c->num_input_files(0);
    // Trivial moves do not get compacted remotely
    compaction_job_stats.is_remote_compaction = false;
    compaction_job_stats.num_input_files_trivially_moved =
        compaction_job_stats.num_input_files;

    NotifyOnCompactionBegin(c->column_family_data(), c.get(), status,
                            compaction_job_stats, job_context->job_id);

    if (c->compaction_reason() == CompactionReason::kLevelMaxLevelSize &&
        c->immutable_options().compaction_pri == kRoundRobin) {
      int start_level = c->start_level();
      if (start_level > 0) {
        auto vstorage = c->input_version()->storage_info();
        c->edit()->AddCompactCursor(
            start_level,
            vstorage->GetNextCompactCursor(start_level, c->num_input_files(0)));
      }
    }

    // Perform the trivial move
    size_t moved_files = 0;
    size_t moved_bytes = 0;
    // Populate the edit first so that CompactionJobInfo has output file
    // info when OnCompactionPreCommit fires.
    PrepareTrivialMoveEdit(*c.get(), log_buffer, moved_files, moved_bytes);
    NotifyOnCompactionPreCommit(c->column_family_data(), c.get(), status,
                                compaction_job_stats, job_context->job_id);
    status = CommitTrivialMove(*c.get(), compaction_released);
    io_s = versions_->io_status();
    InstallSuperVersionAndScheduleWork(
        c->column_family_data(), job_context->superversion_contexts.data(),
        /*defer_snapshot_refresh=*/false);

    VersionStorageInfo::LevelSummaryStorage tmp;
    c->column_family_data()->internal_stats()->IncBytesMoved(c->output_level(),
                                                             moved_bytes);
    {
      event_logger_.LogToBuffer(log_buffer)
          << "job" << job_context->job_id << "event" << "trivial_move"
          << "destination_level" << c->output_level() << "files" << moved_files
          << "total_files_size" << moved_bytes;
    }
    ROCKS_LOG_BUFFER(
        log_buffer, "[%s] Moved #%d files to level-%zu %zu bytes %s: %s\n",
        c->column_family_data()->GetName().c_str(), moved_files,
        c->output_level(), moved_bytes, status.ToString().c_str(),
        c->column_family_data()->current()->storage_info()->LevelSummary(&tmp));
    *made_progress = true;

    // Clear Instrument
    ThreadStatusUtil::ResetThreadStatus();
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:AfterCompaction",
                             c->column_family_data());
  } else if (!is_prepicked &&
             Compaction::OutputToNonZeroMaxOutputLevel(
                 c->output_level(),
                 c->column_family_data()
                     ->current()
                     ->storage_info()
                     ->MaxOutputLevel(
                         c->immutable_options().cf_allow_ingest_behind ||
                         immutable_db_options_.allow_ingest_behind)) &&
             env_->GetBackgroundThreads(Env::Priority::BOTTOM) > 0) {
    assert(thread_pri == Env::Priority::LOW);
    // Forward compactions involving last level to the bottom pool if it exists,
    // such that compactions unlikely to contribute to write stalls can be
    // delayed or deprioritized.
    TEST_SYNC_POINT("DBImpl::BackgroundCompaction:ForwardToBottomPriPool");
    CompactionArg* ca = new CompactionArg;
    ca->db = this;
    ca->compaction_pri_ = Env::Priority::BOTTOM;
    ca->prepicked_compaction = new PrepickedCompaction;

    // If `universal_reduce_file_locking` is true, we only lock a limited set of
    // input files by creating an intended compaction to forward to bottom
    // priority pool and repicking files when bottom priority thread
    // gets to execute this intended compaction
    const bool need_repick =
        c->mutable_cf_options()
            .compaction_options_universal.reduce_file_locking;
    if (need_repick) {
      ca->prepicked_compaction->compaction =
          CreateIntendedCompactionForwardedToBottomPriorityPool(c.get());
      c.reset();
      ca->prepicked_compaction->need_repick = true;
    } else {
      ca->prepicked_compaction->compaction = c.release();
      ca->prepicked_compaction->need_repick = false;
    }
    ca->prepicked_compaction->manual_compaction_state = nullptr;
    // Transfer requested token, so it doesn't need to do it again.
    ca->prepicked_compaction->task_token = std::move(task_token);
    assert(c == nullptr);
    status = ScheduleCompaction(ca, this);
  } else {
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:BeforeCompaction",
                             c->column_family_data());
    int output_level __attribute__((__unused__));
    output_level = c->output_level();
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:NonTrivial",
                             &output_level);
    InitSnapshotContext(job_context);
    assert(is_snapshot_supported_ || snapshots_.empty());

    CompactionJob compaction_job(
        job_context->job_id, c.get(), immutable_db_options_,
        mutable_db_options_, file_options_for_compaction_, versions_.get(),
        &shutting_down_, log_buffer, directories_.GetDbDir(),
        GetDataDir(c->column_family_data(), c->output_path_id()),
        GetDataDir(c->column_family_data(), 0), stats_, &mutex_,
        &error_handler_, job_context, table_cache_, &event_logger_,
        c->mutable_cf_options().paranoid_file_checks,
        c->mutable_cf_options().report_bg_io_stats, dbname_,
        &compaction_job_stats, thread_pri, io_tracer_,
        is_manual ? manual_compaction->canceled
                  : kManualCompactionCanceledFalse_,
        compaction_aborted_, db_id_, db_session_id_,
        c->column_family_data()->GetFullHistoryTsLow(), c->trim_ts(),
        &blob_callback_, &bg_compaction_scheduled_,
        &bg_bottom_compaction_scheduled_, &num_running_remote_compactions_);
    ConfigureCompactionExecutors(&compaction_job, c->column_family_data(),
                                 is_manual ? manual_compaction->canceled
                                           : kManualCompactionCanceledFalse_,
                                 is_manual, separate_remote_pool);
    compaction_job.Prepare(std::nullopt /*subcompact to be computed*/);

    std::unique_ptr<std::list<uint64_t>::iterator> min_options_file_number_elem;
    if (immutable_db_options().compaction_service != nullptr) {
      min_options_file_number_elem.reset(
          new std::list<uint64_t>::iterator(CaptureOptionsFileNumber()));
    }

    NotifyOnCompactionBegin(c->column_family_data(), c.get(), status,
                            compaction_job_stats, job_context->job_id);
    mutex_.Unlock();
    if (thread_pri == Env::Priority::LOW) {
      TEST_SYNC_POINT_CALLBACK(
          "DBImpl::BackgroundCompaction:NonTrivial:BeforeRun", nullptr);
    } else {
      assert(thread_pri == Env::Priority::BOTTOM);
      TEST_SYNC_POINT(
          "DBImpl::BackgroundCompaction:NonTrivial:BeforeRunBottomPri");
    }

    // Should handle error?
    compaction_job.Run().PermitUncheckedError();
    TEST_SYNC_POINT("DBImpl::BackgroundCompaction:NonTrivial:AfterRun");
    mutex_.Lock();

    if (immutable_db_options().compaction_service != nullptr) {
      ReleaseOptionsFileNumber(min_options_file_number_elem);
    }

    // Fire OnCompactionPreCommit before compaction_job.Install runs
    // ReleaseCompactionFiles in its manifest write callback.
    NotifyOnCompactionPreCommit(c->column_family_data(), c.get(), status,
                                compaction_job_stats, job_context->job_id);
    status = compaction_job.Install(&compaction_released);
    io_s = compaction_job.io_status();
    if (status.ok()) {
      InstallSuperVersionAndScheduleWork(
          c->column_family_data(), job_context->superversion_contexts.data(),
          /*defer_snapshot_refresh=*/false);
    }
    *made_progress = true;
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:AfterCompaction",
                             c->column_family_data());
  }

  if (status.ok() && !io_s.ok()) {
    status = io_s;
  } else {
    io_s.PermitUncheckedError();
  }

  if (c != nullptr) {
    if (!compaction_released) {
      // Safety net: if any of the paths above did not fire the
      // OnCompactionPreCommit notification (e.g. status was not OK and
      // an early-out skipped the LogAndApply / Install), still fire the
      // notify here while files are about to be released. The notify is
      // idempotent and is a no-op if it has already fired (or if Begin never
      // fired).
      NotifyOnCompactionPreCommit(c->column_family_data(), c.get(), status,
                                  compaction_job_stats, job_context->job_id);
      c->ReleaseCompactionFiles(status);
    } else {
#ifndef NDEBUG
      // Sanity checking that compaction files are freed.
      for (size_t i = 0; i < c->num_input_levels(); i++) {
        for (size_t j = 0; j < c->inputs(i)->size(); j++) {
          // When status is not OK, compaction's result installation failed and
          // no new Version installed. The files could have been released and
          // picked up again by other compaction attempts.
          assert(!c->input(i, j)->being_compacted || !status.ok());
        }
      }
      std::unordered_set<Compaction*>* cip = c->column_family_data()
                                                 ->compaction_picker()
                                                 ->compactions_in_progress();
      assert(cip->find(c.get()) == cip->end());
#endif
    }

    *made_progress = true;

    // Need to make sure SstFileManager does its bookkeeping
    auto sfm = static_cast<SstFileManagerImpl*>(
        immutable_db_options_.sst_file_manager.get());
    if (sfm && sfm_reserved_compact_space) {
      sfm->OnCompactionCompletion(c.get());
    }

    NotifyOnCompactionCompleted(c->column_family_data(), c.get(), status,
                                compaction_job_stats, job_context->job_id);
  }

  if (status.ok() || status.IsCompactionTooLarge() ||
      status.IsManualCompactionPaused() || status.IsCompactionAborted()) {
    // Done
  } else if (status.IsColumnFamilyDropped() || status.IsShutdownInProgress()) {
    // Ignore compaction errors found during shutting down
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log, "Compaction error: %s",
                   status.ToString().c_str());
    if (!io_s.ok()) {
      // Error while writing to MANIFEST.
      // In fact, versions_->io_status() can also be the result of renaming
      // CURRENT file. With current code, it's just difficult to tell. So just
      // be pessimistic and try write to a new MANIFEST.
      // TODO: distinguish between MANIFEST write and CURRENT renaming
      auto err_reason = versions_->io_status().ok()
                            ? BackgroundErrorReason::kCompaction
                            : BackgroundErrorReason::kManifestWrite;
      error_handler_.SetBGError(io_s, err_reason);
    } else {
      error_handler_.SetBGError(status, BackgroundErrorReason::kCompaction);
    }
    if (c != nullptr && !is_manual && !error_handler_.IsBGWorkStopped()) {
      // Put this cfd back in the compaction queue so we can retry after some
      // time
      auto cfd = c->column_family_data();
      assert(cfd != nullptr);
      // Since this compaction failed, we need to recompute the score so it
      // takes the original input files into account
      c->column_family_data()
          ->current()
          ->storage_info()
          ->ComputeCompactionScore(c->immutable_options(),
                                   c->mutable_cf_options(),
                                   cfd->GetFullHistoryTsLow());
      const int unscheduled_before_retry = unscheduled_compactions_;
      EnqueuePendingCompaction(cfd);
      if (separate_remote_pool &&
          unscheduled_compactions_ > unscheduled_before_retry) {
        --unscheduled_compactions_;
        restore_compaction_credit();
      }
    }
  }
  // this will unref its input_version and column_family_data
  c.reset();

  if (is_manual) {
    ManualCompactionState* m = manual_compaction;
    if (!status.ok()) {
      m->status = status;
      m->done = true;
    }
    // For universal compaction:
    //   Because universal compaction always happens at level 0, so one
    //   compaction will pick up all overlapped files. No files will be
    //   filtered out due to size limit and left for a successive compaction.
    //   So we can safely conclude the current compaction.
    //
    //   Also note that, if we don't stop here, then the current compaction
    //   writes a new file back to level 0, which will be used in successive
    //   compaction. Hence the manual compaction will never finish.
    //
    // Stop the compaction if manual_end points to nullptr -- this means
    // that we compacted the whole range. manual_end should always point
    // to nullptr in case of universal compaction
    if (m->manual_end == nullptr) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      // Universal and FIFO compactions should always compact the whole range
      assert(m->cfd->ioptions().compaction_style != kCompactionStyleUniversal ||
             m->cfd->ioptions().num_levels > 1);
      assert(m->cfd->ioptions().compaction_style != kCompactionStyleFIFO);
      m->tmp_storage = *m->manual_end;
      m->begin = &m->tmp_storage;
      m->incomplete = true;
    }
    m->in_progress = false;  // not being processed anymore
  }
  TEST_SYNC_POINT("DBImpl::BackgroundCompaction:Finish");
  return status;
}

// Create an intended compaction to forward based on the original picked
// compaction. It serves two purposes while it is waiting
// for a bottom-priority thread becomes available to run:
// - Prevent the last input file (or sorted run if non-L0) from
// being included in compaction score calculations unnecessarily since the
// intended compaction is already scheduled to compact it
// - Allow other input files to be picked by low-priority compactions that can
// run right away
//
// Once a bottom-priority available to run this intended compaction, it will
// repick files to consider the LSM updates that occurred during the waiting
// period.
Compaction* DBImpl::CreateIntendedCompactionForwardedToBottomPriorityPool(
    Compaction* c) {
  auto* cfd = c->column_family_data();
  const auto& io = c->immutable_options();
  const auto& mo = c->mutable_cf_options();
  auto* vstorage = c->input_version()->storage_info();

  std::vector<CompactionInputFiles> inputs(1);

  const std::vector<FileMetaData*>* max_intput_level_files = nullptr;
  int max_intput_level = 0;

  for (size_t i = c->num_input_levels(); i >= 1; --i) {
    size_t level = i - 1;
    if (c->num_input_files(level) > 0) {
      max_intput_level = static_cast<int>(level);
      max_intput_level_files = c->inputs(level);
      break;
    }
  }

  assert(max_intput_level_files);
  assert(!max_intput_level_files->empty());
  inputs[0].level = max_intput_level;

  if (max_intput_level == 0) {
    // The last input file
    inputs[0].files.push_back(
        (*max_intput_level_files)[max_intput_level_files->size() - 1]);
  } else {
    // The last input sorted run
    for (FileMetaData* f : (*max_intput_level_files)) {
      inputs[0].files.push_back(f);
    }
  }

  c->ReleaseCompactionFiles(Status::OK());

  Compaction* intended_compaction =
      new Compaction(vstorage, io, mo, mutable_db_options_, std::move(inputs),
                     c->output_level(), c->target_output_file_size(),
                     c->max_compaction_bytes(), c->output_path_id(),
                     c->output_compression(), c->output_compression_opts(),
                     c->GetOutputTemperature(), c->max_subcompactions(),
                     c->grandparents(), std::nullopt /* earliest_snapshot */,
                     nullptr /* snapshot_checker */, c->compaction_reason());

  cfd->compaction_picker()->RegisterCompaction(intended_compaction);
  vstorage->ComputeCompactionScore(io, mo, cfd->GetFullHistoryTsLow());
  intended_compaction->FinalizeInputInfo(cfd->current());

  return intended_compaction;
}

bool DBImpl::HasPendingManualCompaction() {
  mutex_.AssertHeld();
  return (!manual_compaction_dequeue_.empty());
}

bool DBImpl::HasPendingManualCompaction(ColumnFamilyData* cfd) {
  mutex_.AssertHeld();
  return std::any_of(manual_compaction_dequeue_.begin(),
                     manual_compaction_dequeue_.end(),
                     [cfd](const ManualCompactionState* manual) {
                       return manual->cfd == cfd;
                     });
}

void DBImpl::AddManualCompaction(DBImpl::ManualCompactionState* m) {
  assert(manual_compaction_paused_ == 0);
  manual_compaction_dequeue_.push_back(m);
}

void DBImpl::RemoveManualCompaction(DBImpl::ManualCompactionState* m) {
  // Remove from queue
  std::deque<ManualCompactionState*>::iterator it =
      manual_compaction_dequeue_.begin();
  while (it != manual_compaction_dequeue_.end()) {
    if (m == (*it)) {
      it = manual_compaction_dequeue_.erase(it);
      return;
    }
    ++it;
  }
  assert(false);
}

bool DBImpl::ShouldntRunManualCompaction(ManualCompactionState* m) {
  if (m->exclusive) {
    return CompactionsScheduled() > 0 || remote_compaction_pool_jobs_ > 0;
  }
  std::deque<ManualCompactionState*>::iterator it =
      manual_compaction_dequeue_.begin();
  bool seen = false;
  while (it != manual_compaction_dequeue_.end()) {
    if (m == (*it)) {
      ++it;
      seen = true;
      continue;
    } else if (MCOverlap(m, (*it)) && (!seen && !(*it)->in_progress)) {
      // Consider the other manual compaction *it, conflicts if:
      // overlaps with m
      // and (*it) is ahead in the queue and is not yet in progress
      return true;
    }
    ++it;
  }
  return false;
}

bool DBImpl::HaveManualCompaction(ColumnFamilyData* cfd) {
  // Remove from priority queue
  std::deque<ManualCompactionState*>::iterator it =
      manual_compaction_dequeue_.begin();
  while (it != manual_compaction_dequeue_.end()) {
    if ((*it)->exclusive) {
      return true;
    }
    if ((cfd == (*it)->cfd) && (!((*it)->in_progress || (*it)->done))) {
      // Allow automatic compaction if manual compaction is
      // in progress
      return true;
    }
    ++it;
  }
  return false;
}

bool DBImpl::HasExclusiveManualCompaction() {
  // Remove from priority queue
  std::deque<ManualCompactionState*>::iterator it =
      manual_compaction_dequeue_.begin();
  while (it != manual_compaction_dequeue_.end()) {
    if ((*it)->exclusive) {
      return true;
    }
    ++it;
  }
  return false;
}

bool DBImpl::MCOverlap(ManualCompactionState* m, ManualCompactionState* m1) {
  if ((m->exclusive) || (m1->exclusive)) {
    return true;
  }
  if (m->cfd != m1->cfd) {
    return false;
  }
  return false;
}

void DBImpl::UpdateFIFOCompactionStatus(const std::unique_ptr<Compaction>& c) {
  if (c == nullptr) {
    return;
  }

  CompactionReason reason = c->compaction_reason();

  switch (reason) {
    case CompactionReason::kFIFOMaxSize:
      RecordTick(stats_, FIFO_MAX_SIZE_COMPACTIONS);
      break;
    case CompactionReason::kFIFOTtl:
      RecordTick(stats_, FIFO_TTL_COMPACTIONS);
      break;
    case CompactionReason::kChangeTemperature:
      RecordTick(stats_, FIFO_CHANGE_TEMPERATURE_COMPACTIONS);
      break;
    default:
      assert(false);
      break;
  }
}

void DBImpl::BuildCompactionJobInfo(
    const ColumnFamilyData* cfd, Compaction* c, const Status& st,
    const CompactionJobStats& compaction_job_stats, const int job_id,
    CompactionJobInfo* compaction_job_info) const {
  assert(compaction_job_info != nullptr);
  compaction_job_info->cf_id = cfd->GetID();
  compaction_job_info->cf_name = cfd->GetName();
  compaction_job_info->status = st;
  compaction_job_info->aborted = st.IsCompactionAborted();
  compaction_job_info->thread_id = env_->GetThreadID();
  compaction_job_info->job_id = job_id;
  compaction_job_info->use_separate_remote_compaction_pool =
      c->use_separate_remote_compaction_pool();
  compaction_job_info->base_input_level = c->start_level();
  compaction_job_info->output_level = c->output_level();
  compaction_job_info->stats = compaction_job_stats;
  const auto& input_table_properties = c->GetOrInitInputTableProperties();
  const auto& output_table_properties = c->GetOutputTableProperties();
  compaction_job_info->table_properties.insert(input_table_properties.begin(),
                                               input_table_properties.end());
  compaction_job_info->table_properties.insert(output_table_properties.begin(),
                                               output_table_properties.end());
  compaction_job_info->compaction_reason = c->compaction_reason();
  compaction_job_info->compression = c->output_compression();

  const ReadOptions read_options(Env::IOActivity::kCompaction);
  for (size_t i = 0; i < c->num_input_levels(); ++i) {
    for (const auto fmd : *c->inputs(i)) {
      const FileDescriptor& desc = fmd->fd;
      const uint64_t file_number = desc.GetNumber();
      auto fn = TableFileName(c->immutable_options().cf_paths, file_number,
                              desc.GetPathId());
      compaction_job_info->input_files.push_back(fn);
      compaction_job_info->input_file_infos.push_back(CompactionFileInfo{
          static_cast<int>(i), file_number, fmd->oldest_blob_file_number});
    }
  }

  for (const auto& newf : c->edit()->GetNewFiles()) {
    const FileMetaData& meta = newf.second;
    const FileDescriptor& desc = meta.fd;
    const uint64_t file_number = desc.GetNumber();
    compaction_job_info->output_files.push_back(TableFileName(
        c->immutable_options().cf_paths, file_number, desc.GetPathId()));
    compaction_job_info->output_file_infos.push_back(CompactionFileInfo{
        newf.first, file_number, meta.oldest_blob_file_number});
  }
  compaction_job_info->blob_compression_type =
      c->mutable_cf_options().blob_compression_type;

  // Update BlobFilesInfo.
  for (const auto& blob_file : c->edit()->GetBlobFileAdditions()) {
    BlobFileAdditionInfo blob_file_addition_info(
        BlobFileName(c->immutable_options().cf_paths.front().path,
                     blob_file.GetBlobFileNumber()) /*blob_file_path*/,
        blob_file.GetBlobFileNumber(), blob_file.GetTotalBlobCount(),
        blob_file.GetTotalBlobBytes());
    compaction_job_info->blob_file_addition_infos.emplace_back(
        std::move(blob_file_addition_info));
  }

  // Update BlobFilesGarbageInfo.
  for (const auto& blob_file : c->edit()->GetBlobFileGarbages()) {
    BlobFileGarbageInfo blob_file_garbage_info(
        BlobFileName(c->immutable_options().cf_paths.front().path,
                     blob_file.GetBlobFileNumber()) /*blob_file_path*/,
        blob_file.GetBlobFileNumber(), blob_file.GetGarbageBlobCount(),
        blob_file.GetGarbageBlobBytes());
    compaction_job_info->blob_file_garbage_infos.emplace_back(
        std::move(blob_file_garbage_info));
  }
}

// SuperVersionContext gets created and destructed outside of the lock --
// we use this conveniently to:
// * malloc one SuperVersion() outside of the lock -- new_superversion
// * delete SuperVersion()s outside of the lock -- superversions_to_free
//
// However, if InstallSuperVersionAndScheduleWork() gets called twice with the
// same sv_context, we can't reuse the SuperVersion() that got
// malloced because
// first call already used it. In that rare case, we take a hit and create a
// new SuperVersion() inside of the mutex. We do similar thing
// for superversion_to_free

void DBImpl::InstallSuperVersionAndScheduleWork(
    ColumnFamilyData* cfd, SuperVersionContext* sv_context,
    bool defer_snapshot_refresh,
    std::optional<std::shared_ptr<SeqnoToTimeMapping>>
        new_seqno_to_time_mapping) {
  mutex_.AssertHeld();

  // Update max_total_in_memory_state_
  size_t old_memtable_size = 0;
  auto* old_sv = cfd->GetSuperVersion();
  if (old_sv) {
    old_memtable_size = old_sv->mutable_cf_options.write_buffer_size *
                        old_sv->mutable_cf_options.max_write_buffer_number;
  }

  // this branch is unlikely to step in
  if (UNLIKELY(sv_context->new_superversion == nullptr)) {
    sv_context->NewSuperVersion();
  }
  cfd->InstallSuperVersion(sv_context, &mutex_,
                           std::move(new_seqno_to_time_mapping));

  // Refresh the seqno->time preserve-window bound used to gate bottommost file
  // marking, so a file whose largest seqno is still within the preserve window
  // is not marked for a compaction that cannot zero it out (infinite loop).
  MaybeUpdatePreserveTimeMinSeqno(cfd);

  if (defer_snapshot_refresh) {
    // Atomic multi-CF ingestion can already hold many per-CF locks. Reconcile
    // the installed Version against the last exact observation now and defer
    // the all-shard scan until those locks have been released.
    ReconcileOldestSnapshotForColumnFamily(cfd,
                                           last_processed_oldest_snapshot_);
  } else {
    TEST_SYNC_POINT(
        "DBImpl::InstallSuperVersionAndScheduleWork:BeforeSnapshotRefresh");
    uint64_t snapshot_release_count = 0;
    const SequenceNumber oldest_snapshot =
        snapshots_.OldestOr([this]() { return GetLastPublishedSequence(); },
                            &snapshot_release_count);
    UpdateOldestSnapshotAndScheduleWork(oldest_snapshot, snapshot_release_count,
                                        cfd);
  }

  // Whenever we install new SuperVersion, we might need to issue new flushes or
  // compactions.
  EnqueuePendingCompaction(cfd);
  MaybeScheduleFlushOrCompaction();

  // Update max_total_in_memory_state_
  max_total_in_memory_state_ =
      max_total_in_memory_state_ - old_memtable_size +
      cfd->GetLatestMutableCFOptions().write_buffer_size *
          cfd->GetLatestMutableCFOptions().max_write_buffer_number;
}

// ShouldPurge is called by FindObsoleteFiles when doing a full scan,
// and db mutex (mutex_) should already be held.
// Actually, the current implementation of FindObsoleteFiles with
// full_scan=true can issue I/O requests to obtain list of files in
// directories, e.g. env_->getChildren while holding db mutex.
bool DBImpl::ShouldPurge(uint64_t file_number) const {
  return files_grabbed_for_purge_.find(file_number) ==
             files_grabbed_for_purge_.end() &&
         purge_files_.find(file_number) == purge_files_.end();
}

// MarkAsGrabbedForPurge is called by FindObsoleteFiles, and db mutex
// (mutex_) should already be held.
void DBImpl::MarkAsGrabbedForPurge(uint64_t file_number) {
  files_grabbed_for_purge_.insert(file_number);
}

void DBImpl::EnableTrackPublishedSeqInSnapshotContext() {
  InstrumentedMutexLock l(&mutex_);
  track_published_seq_in_snapshot_context_ = true;
}

void DBImpl::SetSnapshotChecker(SnapshotChecker* snapshot_checker) {
  InstrumentedMutexLock l(&mutex_);
  // snapshot_checker_ should only set once. If we need to set it multiple
  // times, we need to make sure the old one is not deleted while it is still
  // using by a compaction job.
  assert(!snapshot_checker_);
  snapshot_checker_.reset(snapshot_checker);
}

void DBImpl::InitSnapshotContext(JobContext* job_context) {
  mutex_.AssertHeld();
  assert(job_context != nullptr);
  if (job_context->snapshot_context_initialized) {
    return;
  }
  SnapshotChecker* snapshot_checker = snapshot_checker_.get();
  if (use_custom_gc_ && !snapshot_checker) {
    snapshot_checker = DisableGCSnapshotChecker::Instance();
  }
  std::unique_ptr<ManagedSnapshot> managed_snapshot = nullptr;
  if (snapshot_checker) {
    // If snapshot_checker is used, that means the flush/compaction may
    // contain values not visible to snapshot taken after
    // flush/compaction job starts. Take a snapshot and it will appear
    // in snapshot_seqs and force compaction iterator to consider such
    // snapshots.
    const Snapshot* snapshot = GetSnapshotImpl(
        /*is_write_conflict_boundary=*/false, SnapshotLockMode::kDBMutexHeld);
    managed_snapshot.reset(new ManagedSnapshot(this, snapshot));
  }
  // The SnapshotChecker path above already adds a managed snapshot to
  // snapshot_seqs.
  const bool pin_published_seq =
      track_published_seq_in_snapshot_context_ && !snapshot_checker;
  // Read the published sequence number before loading the snapshots.
  // GetSnapshot() does not take mutex_, so a snapshot that the load below
  // misses reads its sequence number later and is no smaller than this pin.
  const SequenceNumber published_seq =
      pin_published_seq ? GetLastPublishedSequence() : 0;
  SequenceNumber earliest_write_conflict_snapshot = kMaxSequenceNumber;
  std::vector<SequenceNumber> snapshot_seqs;
  uint64_t snapshot_release_count = 0;
  const SequenceNumber oldest_snapshot =
      LoadSnapshots(&snapshot_seqs, &earliest_write_conflict_snapshot,
                    kMaxSequenceNumber, &snapshot_release_count);
  UpdateOldestSnapshotAndScheduleWork(oldest_snapshot, snapshot_release_count,
                                      /*installed_cfd=*/nullptr);

  if (pin_published_seq) {
    // Pin the published-sequence boundary into snapshot_seqs without taking
    // a real snapshot. This prevents flush/compaction from collapsing a version
    // visible at the published boundary into a newer unpublished seqno.
    auto it = std::lower_bound(snapshot_seqs.begin(), snapshot_seqs.end(),
                               published_seq);
    if (it == snapshot_seqs.end() || *it != published_seq) {
      snapshot_seqs.insert(it, published_seq);
    }
  }
  TEST_SYNC_POINT("DBImpl::InitSnapshotContext:BeforeInit");
  job_context->InitSnapshotContext(
      snapshot_checker, std::move(managed_snapshot),
      earliest_write_conflict_snapshot, std::move(snapshot_seqs));
}

Status DBImpl::WaitForCompact(
    const WaitForCompactOptions& wait_for_compact_options) {
  InstrumentedMutexLock l(&mutex_);
  // `close_db=true` needs the same direct-write cleanup guarantee as DB close:
  // register live blob files before relying on the DB being reopenable.
  const bool flush_for_blob_direct_write =
      HasAnyBlobDirectWriteColumnFamilyWithLockHeld();
  if (wait_for_compact_options.flush) {
    Status s = DBImpl::FlushAllColumnFamilies(FlushOptions(),
                                              FlushReason::kManualFlush);
    if (!s.ok()) {
      return s;
    }
  } else if (wait_for_compact_options.close_db &&
             (flush_for_blob_direct_write ||
              (has_unpersisted_data_.load(std::memory_order_relaxed) &&
               !mutable_db_options_.avoid_flush_during_shutdown))) {
    Status s =
        DBImpl::FlushAllColumnFamilies(FlushOptions(), FlushReason::kShutDown);
    if (!s.ok()) {
      return s;
    }
  }
  const auto deadline = immutable_db_options_.clock->NowMicros() +
                        wait_for_compact_options.timeout.count();
  TEST_SYNC_POINT("DBImpl::WaitForCompact:StartWaiting");
  for (;;) {
    // The coordinator reserves this DB before invoking its non-blocking
    // scheduling callback. Drain that DB-specific reservation without holding
    // mutex_; successful callbacks publish bg_wbm_flush_scheduled_ before
    // releasing the reservation.
    if (write_buffer_manager_ != nullptr &&
        write_buffer_manager_->ShouldTrackFlushInitiator() &&
        wbm_flush_initiator_ != nullptr) {
      // Fence all coordinator requests visible before this local quiescence
      // check. Candidate retrieval either observes this generation and skips
      // the DB, or atomically publishes a handoff reservation first.
      write_buffer_manager_->FenceFlushInitiator(wbm_flush_initiator_.get());
    }
    if (wbm_flush_initiator_ != nullptr &&
        wbm_flush_initiator_->HasPendingFlushHandoff()) {
      std::chrono::microseconds handoff_timeout{0};
      if (wait_for_compact_options.timeout.count() != 0) {
        const uint64_t now = immutable_db_options_.clock->NowMicros();
        if (now >= deadline) {
          return Status::TimedOut();
        }
        handoff_timeout = std::chrono::microseconds(deadline - now);
      }
      mutex_.Unlock();
      const bool handoff_finished =
          wbm_flush_initiator_->WaitForPendingFlushHandoff(handoff_timeout);
      mutex_.Lock();
      if (!handoff_finished) {
        return Status::TimedOut();
      }
      continue;
    }
    if (shutting_down_.load(std::memory_order_acquire)) {
      return Status::ShutdownInProgress();
    }
    if (bg_work_paused_ && wait_for_compact_options.abort_on_pause) {
      return Status::Aborted();
    }
    uint64_t snapshot_release_count = 0;
    const SequenceNumber oldest_snapshot =
        snapshots_.OldestOr([this]() { return GetLastPublishedSequence(); },
                            &snapshot_release_count);
    UpdateOldestSnapshotAndScheduleWork(oldest_snapshot, snapshot_release_count,
                                        /*installed_cfd=*/nullptr);
    // A WBM job can enqueue a regular flush, so wait for both to quiesce.
    if ((CompactionsScheduled() || remote_compaction_pool_jobs_ ||
         bg_flush_scheduled_ || bg_wbm_flush_scheduled_ ||
         unscheduled_compactions_ || !parked_compaction_cfds_.empty() ||
         (wait_for_compact_options.wait_for_purge && bg_purge_scheduled_) ||
         unscheduled_flushes_ || error_handler_.IsRecoveryInProgress()) &&
        (error_handler_.GetBGError().ok())) {
      if (wait_for_compact_options.timeout.count()) {
        if (bg_cv_.TimedWait(deadline)) {
          return Status::TimedOut();
        }
      } else {
        TEST_SYNC_POINT("DBImpl::WaitForCompact:InsideLoop");
        bg_cv_.Wait();
      }
    } else if (wait_for_compact_options.close_db) {
      reject_new_background_jobs_ = true;
      mutex_.Unlock();
      Status s = Close();
      mutex_.Lock();
      if (!s.ok()) {
        reject_new_background_jobs_ = false;
      }
      return s;
    } else {
      return error_handler_.GetBGError();
    }
  }
}

bool DBImpl::ShouldPickCompaction(
    bool is_prepicked, const PrepickedCompaction* prepicked_compaction) {
  return (!is_prepicked && !compaction_queue_.empty()) ||
         (is_prepicked && prepicked_compaction->need_repick);
}

void DBImpl::ResetBottomPriCompactionIntent(ColumnFamilyData* cfd,
                                            std::unique_ptr<Compaction>& c) {
  c->ReleaseCompactionFiles(Status::OK());
  cfd->current()->storage_info()->ComputeCompactionScore(
      c->immutable_options(), c->mutable_cf_options(),
      cfd->GetFullHistoryTsLow());
  c.reset();
}

}  // namespace ROCKSDB_NAMESPACE
