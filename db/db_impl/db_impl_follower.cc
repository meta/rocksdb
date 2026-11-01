//  Copyright (c) 2024-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_impl/db_impl_follower.h"

#include <algorithm>
#include <cinttypes>

#include "db/arena_wrapped_db_iter.h"
#include "db/merge_context.h"
#include "env/composite_env_wrapper.h"
#include "env/fs_on_demand.h"
#include "logging/auto_roll_logger.h"
#include "logging/logging.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/configurable.h"
#include "rocksdb/db.h"
#include "util/cast_util.h"
#include "util/write_batch_util.h"

namespace ROCKSDB_NAMESPACE {

DBImplFollower::DBImplFollower(const DBOptions& db_options,
                               std::unique_ptr<Env>&& env,
                               const std::string& dbname, std::string src_path)
    : DBImplSecondary(db_options, dbname, ""),
      env_guard_(std::move(env)),
      stop_requested_(false),
      src_path_(std::move(src_path)),
      cv_(&mu_) {
  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "Opening the db in follower mode");
  LogFlush(immutable_db_options_.info_log);
}

DBImplFollower::~DBImplFollower() {
  Status s = DBImplFollower::Close();
  if (!s.ok()) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "Error closing DB : %s",
                   s.ToString().c_str());
  }
}

// Recover a follower DB instance by reading the MANIFEST. The verification
// as part of the MANIFEST replay will ensure that local links to the
// leader's files are created, thus ensuring we can continue reading them
// even if the leader deletes those files due to compaction.
// TODO:
//  1. Devise a mechanism to prevent misconfiguration by, for example,
//     keeping a local copy of the IDENTITY file and cross checking
//  2. Make the recovery more robust by retrying if the first attempt
//     fails.
Status DBImplFollower::Recover(
    const std::vector<ColumnFamilyDescriptor>& column_families,
    bool /*readonly*/, bool /*error_if_wal_file_exists*/,
    bool /*error_if_data_exists_in_wals*/, bool /*is_retry*/, uint64_t*,
    RecoveryContext* /*recovery_ctx*/, bool* /*can_retry*/) {
  mutex_.AssertHeld();

  JobContext job_context(0);
  Status s;
  s = static_cast<ReactiveVersionSet*>(versions_.get())
          ->Recover(column_families, &manifest_reader_, &manifest_reporter_,
                    &manifest_reader_status_);
  if (!s.ok()) {
    if (manifest_reader_status_) {
      manifest_reader_status_->PermitUncheckedError();
    }
    return s;
  }
  if (s.ok()) {
    default_cf_handle_ = new ColumnFamilyHandleImpl(
        versions_->GetColumnFamilySet()->GetDefault(), this, &mutex_);
    default_cf_internal_stats_ = default_cf_handle_->cfd()->internal_stats();

    // Start the periodic catch-up thread
    // TODO: See if it makes sense to have a threadpool, rather than a thread
    // per follower DB instance
    catch_up_thread_.reset(
        new port::Thread(&DBImplFollower::PeriodicRefresh, this));
  }

  return s;
}

// Try to catch up by tailing the MANIFEST.
// TODO:
//   1. Cleanup obsolete files afterward
//   2. Add some error notifications and statistics
Status DBImplFollower::TryCatchUpWithLeader() {
  TEST_SYNC_POINT("DBImplFollower::TryCatchUpWithLeader:BeforeCatchUpLock");
  std::lock_guard<std::mutex> catch_up_lock(catch_up_mutex_);
  assert(versions_.get() != nullptr);
  assert(manifest_reader_.get() != nullptr);
  Status s;

  TEST_SYNC_POINT("DBImplFollower::TryCatchupWithLeader:Begin1");
  TEST_SYNC_POINT("DBImplFollower::TryCatchupWithLeader:Begin2");
  // read the manifest and apply new changes to the follower instance
  std::unordered_set<ColumnFamilyData*> cfds_changed;
  JobContext job_context(0, true /*create_superversion*/);
  {
    InstrumentedMutexLock lock_guard(&mutex_);
    std::vector<std::string> files_to_delete;
    s = static_cast_with_check<ReactiveVersionSet>(versions_.get())
            ->ReadAndApply(&mutex_, &manifest_reader_,
                           manifest_reader_status_.get(), &cfds_changed,
                           &files_to_delete);
    ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem_);
    pending_outputs_inserted_elem_.reset(new std::list<uint64_t>::iterator(
        CaptureCurrentFileNumberInPendingOutputs()));

    ROCKS_LOG_INFO(immutable_db_options_.info_log, "Last sequence is %" PRIu64,
                   static_cast<uint64_t>(versions_->LastSequence()));
    ROCKS_LOG_INFO(
        immutable_db_options_.info_log, "Next file number is %" PRIu64,
        static_cast<uint64_t>(versions_->current_next_file_number()));
    for (ColumnFamilyData* cfd : cfds_changed) {
      if (cfd->IsDropped()) {
        ROCKS_LOG_DEBUG(immutable_db_options_.info_log, "[%s] is dropped\n",
                        cfd->GetName().c_str());
        continue;
      }
      VersionStorageInfo::LevelSummaryStorage tmp;
      ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                      "[%s] Level summary: %s\n", cfd->GetName().c_str(),
                      cfd->current()->storage_info()->LevelSummary(&tmp));
    }

    if (s.ok()) {
      for (auto cfd : cfds_changed) {
        if (cfd->mem()->GetEarliestSequenceNumber() <
            versions_->LastSequence()) {
          // Seal the active memtable so that its replacement gets its earliest
          // sequence number set to the last sequence number in the VersionSet.
          // This matters when DBImpl::MultiCFSnapshot tries to get consistent
          // references to super versions in a lock free manner, it checks the
          // earliest sequence number to detect if there was a change in
          // version in the meantime.
          SealActiveMemtable(cfd, cfd->GetLogNumber(),
                             versions_->LastSequence(), &job_context);
        }

        // `cfd->GetLogNumber()` is a safe watermark here, unlike in
        // DBImplSecondary::TryCatchUpWithPrimary(): this catch-up path never
        // replays WALs, so the memtables it collects hold nothing that only a
        // not-yet-installed Version could make readable.
        //
        // This will check if the old memtable is still referenced
        cfd->imm()->RemoveOldMemTables(cfd->GetLogNumber(),
                                       &job_context.memtables_to_free);
        auto& sv_context = job_context.superversion_contexts.back();
        cfd->InstallSuperVersion(&sv_context, &mutex_);
        sv_context.NewSuperVersion();
      }
      PublishSecondaryReadView();
    }

    for (auto& file : files_to_delete) {
      IOStatus io_s = fs_->DeleteFile(file, IOOptions(), nullptr);
      if (!io_s.ok()) {
        ROCKS_LOG_INFO(immutable_db_options_.info_log,
                       "Cannot delete file %s: %s", file.c_str(),
                       io_s.ToString().c_str());
      }
    }
  }
  CleanupRetiredSecondaryReadViews();
  job_context.Clean();

  // Cleanup unused, obsolete files.
  JobContext purge_files_job_context(0);
  {
    InstrumentedMutexLock lock_guard(&mutex_);
    // Currently, follower instance does not create any database files, thus
    // is unnecessary for the follower to force full scan.
    FindObsoleteFiles(&purge_files_job_context, /*force=*/false);
  }
  if (purge_files_job_context.HaveSomethingToDelete()) {
    PurgeObsoleteFiles(purge_files_job_context);
  }
  purge_files_job_context.Clean();

  TEST_SYNC_POINT("DBImplFollower::TryCatchupWithLeader:End");

  return s;
}

void DBImplFollower::PeriodicRefresh() {
  while (!stop_requested_.load()) {
    MutexLock l(&mu_);
    int64_t wait_until =
        immutable_db_options_.clock->NowMicros() +
        immutable_db_options_.follower_refresh_catchup_period_ms * 1000;
    immutable_db_options_.clock->TimedWait(
        &cv_, std::chrono::microseconds(wait_until));
    if (stop_requested_.load()) {
      break;
    }
    Status s;
    for (uint64_t i = 0;
         i < immutable_db_options_.follower_catchup_retry_count &&
         !stop_requested_.load();
         ++i) {
      s = TryCatchUpWithLeader();

      if (s.ok()) {
        ROCKS_LOG_INFO(immutable_db_options_.info_log,
                       "Successful catch up on attempt %llu",
                       static_cast<unsigned long long>(i));
        break;
      }
      wait_until = immutable_db_options_.clock->NowMicros() +
                   immutable_db_options_.follower_catchup_retry_wait_ms * 1000;
      immutable_db_options_.clock->TimedWait(
          &cv_, std::chrono::microseconds(wait_until));
    }
    if (!s.ok()) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log, "Catch up unsuccessful");
    }
  }
}

Status DBImplFollower::Close() {
  if (catch_up_thread_) {
    stop_requested_.store(true);
    {
      MutexLock l(&mu_);
      cv_.SignalAll();
    }
    catch_up_thread_->join();
    catch_up_thread_.reset();
  }

  ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem_);

  return DBImpl::Close();
}

Status DB::OpenAsFollower(const Options& options, const std::string& dbname,
                          const std::string& leader_path,
                          std::unique_ptr<DB>* dbptr) {
  dbptr->reset();

  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(kDefaultColumnFamilyName, cf_options);
  std::vector<ColumnFamilyHandle*> handles;

  Status s = DB::OpenAsFollower(db_options, dbname, leader_path,
                                column_families, &handles, dbptr);
  if (s.ok()) {
    assert(handles.size() == 1);
    delete handles[0];
  }
  return s;
}

Status DB::OpenAsFollower(
    const DBOptions& db_options, const std::string& dbname,
    const std::string& src_path,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, std::unique_ptr<DB>* dbptr) {
  dbptr->reset();

  FileSystem* fs = db_options.env->GetFileSystem().get();
  {
    IOStatus io_s;
    if (db_options.create_if_missing) {
      io_s = fs->CreateDirIfMissing(dbname, IOOptions(), nullptr);
    } else {
      io_s = fs->FileExists(dbname, IOOptions(), nullptr);
    }
    if (!io_s.ok()) {
      return static_cast<Status>(io_s);
    }
  }
  std::unique_ptr<Env> new_env(new CompositeEnvWrapper(
      db_options.env, NewOnDemandFileSystem(db_options.env->GetFileSystem(),
                                            src_path, dbname)));

  DBOptions tmp_opts(db_options);
  Status s;
  tmp_opts.env = new_env.get();
  if (nullptr == tmp_opts.info_log) {
    s = CreateLoggerFromOptions(dbname, tmp_opts, &tmp_opts.info_log);
    if (!s.ok()) {
      tmp_opts.info_log = nullptr;
      return s;
    }
  }

  handles->clear();
  DBImplFollower* impl =
      new DBImplFollower(tmp_opts, std::move(new_env), dbname, src_path);
  impl->versions_.reset(new ReactiveVersionSet(
      dbname, &impl->immutable_db_options_, impl->mutable_db_options_,
      impl->file_options_, impl->table_cache_.get(),
      impl->write_buffer_manager_, &impl->write_controller_, impl->io_tracer_,
      impl->db_id_, impl->db_session_id_,
      /*defer_sst_file_opening=*/false));
  impl->column_family_memtables_.reset(
      new ColumnFamilyMemTablesImpl(impl->versions_->GetColumnFamilySet()));
  impl->wal_in_db_path_ = impl->immutable_db_options_.IsWalDirSameAsDBPath();

  impl->mutex_.Lock();
  s = impl->Recover(column_families, /*read_only=*/true,
                    /*error_if_wal_file_exists=*/false,
                    /*error_if_data_exists_in_wals=*/false);
  if (s.ok()) {
    for (const auto& cf : column_families) {
      auto cfd =
          impl->versions_->GetColumnFamilySet()->GetColumnFamily(cf.name);
      if (nullptr == cfd) {
        s = Status::InvalidArgument("Column family not found", cf.name);
        break;
      }
      handles->push_back(new ColumnFamilyHandleImpl(cfd, impl, &impl->mutex_));
    }
  }
  SuperVersionContext sv_context(false /* create_superversion */);
  if (s.ok()) {
    for (auto cfd : *impl->versions_->GetColumnFamilySet()) {
      sv_context.NewSuperVersion();
      cfd->InstallSuperVersion(&sv_context, &impl->mutex_);
    }
    impl->PublishSecondaryReadView();
    impl->MarkAsyncFileOpenNotNeeded();
  }
  impl->mutex_.Unlock();
  impl->CleanupRetiredSecondaryReadViews();
  sv_context.Clean();
  if (s.ok()) {
    dbptr->reset(impl);
    for (auto h : *handles) {
      impl->NewThreadStatusCfInfo(
          static_cast_with_check<ColumnFamilyHandleImpl>(h)->cfd());
    }
  } else {
    for (auto h : *handles) {
      delete h;
    }
    handles->clear();
    delete impl;
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
