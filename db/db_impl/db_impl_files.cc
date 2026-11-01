//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <array>
#include <cinttypes>
#include <set>
#include <unordered_set>

#include "db/blob/blob_file_partition_manager.h"
#include "db/blob/blob_log_format.h"
#include "db/db_impl/db_impl.h"
#include "db/event_helpers.h"
#include "db/memtable_list.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "file/sst_file_manager_impl.h"
#include "logging/logging.h"
#include "monitoring/thread_status_util.h"
#include "port/port.h"
#include "rocksdb/options.h"
#include "util/autovector.h"
#include "util/defer.h"

namespace ROCKSDB_NAMESPACE {

namespace {

Env::IOActivity GetCurrentThreadIOActivityForMetadataRead() {
  switch (ThreadStatusUtil::GetThreadOperation()) {
    case ThreadStatus::OperationType::OP_FLUSH:
      return Env::IOActivity::kFlush;
    case ThreadStatus::OperationType::OP_COMPACTION:
      return Env::IOActivity::kCompaction;
    case ThreadStatus::OperationType::OP_DBOPEN:
      return Env::IOActivity::kDBOpen;
    case ThreadStatus::OperationType::OP_GET:
      return Env::IOActivity::kGet;
    case ThreadStatus::OperationType::OP_MULTIGET:
      return Env::IOActivity::kMultiGet;
    case ThreadStatus::OperationType::OP_DBITERATOR:
      return Env::IOActivity::kDBIterator;
    case ThreadStatus::OperationType::OP_VERIFY_DB_CHECKSUM:
      return Env::IOActivity::kVerifyDBChecksum;
    case ThreadStatus::OperationType::OP_VERIFY_FILE_CHECKSUMS:
      return Env::IOActivity::kVerifyFileChecksums;
    case ThreadStatus::OperationType::OP_GETENTITY:
      return Env::IOActivity::kGetEntity;
    case ThreadStatus::OperationType::OP_MULTIGETENTITY:
      return Env::IOActivity::kMultiGetEntity;
    case ThreadStatus::OperationType::
        OP_GET_FILE_CHECKSUMS_FROM_CURRENT_MANIFEST:
      return Env::IOActivity::kGetFileChecksumsFromCurrentManifest;
    case ThreadStatus::OperationType::OP_LAZY_RESOLVE:
      return Env::IOActivity::kLazyResolve;
    default:
      return Env::IOActivity::kUnknown;
  }
}

// A full-scan obsolete-file purge can observe a newly created direct-write
// blob file before it is added to the manager's active-file set. Those
// in-flight files are still missing their footer, so conservatively keep any
// footer-less blob file here rather than risk deleting a live write-path file.
bool ShouldKeepFooterlessBlobFile(FileSystem* fs,
                                  const FileOptions& file_options,
                                  const std::string& blob_file_path) {
  assert(fs != nullptr);

  constexpr IODebugContext* dbg = nullptr;
  // This purge path can run from DB open, flush/compaction cleanup, or
  // iterator-triggered obsolete-file cleanup. Tag the probe with the current
  // thread activity so db_stress keeps validating the read against the active
  // operation instead of seeing an unexpected kUnknown metadata read.
  IOOptions io_options;
  io_options.io_activity = GetCurrentThreadIOActivityForMetadataRead();
  uint64_t file_size = 0;
  IOStatus io_s = fs->GetFileSize(blob_file_path, io_options, &file_size, dbg);
  if (!io_s.ok()) {
    return !io_s.IsPathNotFound();
  }

  if (file_size < BlobLogHeader::kSize + BlobLogFooter::kSize) {
    return true;
  }

  FileOptions read_file_options = file_options;
  read_file_options.use_direct_reads = false;

  std::unique_ptr<FSRandomAccessFile> file;
  io_s = fs->NewRandomAccessFile(blob_file_path, read_file_options, &file, dbg);
  if (!io_s.ok()) {
    return !io_s.IsPathNotFound();
  }

  std::array<char, BlobLogFooter::kSize> scratch{};
  Slice footer_slice;
  io_s = file->Read(file_size - BlobLogFooter::kSize, BlobLogFooter::kSize,
                    io_options, &footer_slice, scratch.data(), dbg);
  if (!io_s.ok()) {
    return !io_s.IsPathNotFound();
  }

  if (footer_slice.size() != BlobLogFooter::kSize) {
    return true;
  }

  BlobLogFooter footer;
  return !footer.DecodeFrom(footer_slice).ok();
}

}  // namespace

uint64_t DBImpl::MinLogNumberToKeep() {
  return versions_->min_log_number_to_keep();
}

uint64_t DBImpl::MinLogNumberToRecycle() { return min_wal_number_to_recycle_; }

bool DBImpl::IsWalInLogs(uint64_t wal_number) {
  InstrumentedMutexLock wal_lock(&wal_write_mutex_);
  for (const auto& wal : logs_) {
    if (wal.number == wal_number) {
      return true;
    }
  }
  return false;
}

uint64_t DBImpl::MinObsoleteSstNumberToKeep() {
  mutex_.AssertHeld();
  if (!pending_outputs_.empty()) {
    return *pending_outputs_.begin();
  }
  return std::numeric_limits<uint64_t>::max();
}

uint64_t DBImpl::GetObsoleteSstFilesSize() {
  mutex_.AssertHeld();
  return versions_->GetObsoleteSstFilesSize();
}

uint64_t DBImpl::MinOptionsFileNumberToKeep() {
  mutex_.AssertHeld();
  if (!min_options_file_numbers_.empty()) {
    return *min_options_file_numbers_.begin();
  }
  return std::numeric_limits<uint64_t>::max();
}

Status DBImpl::DisableFileDeletions() {
  Status s;
  int my_disable_delete_obsolete_files;
  {
    InstrumentedMutexLock l(&mutex_);
    s = DisableFileDeletionsWithLock();
    my_disable_delete_obsolete_files = disable_delete_obsolete_files_;
  }
  if (my_disable_delete_obsolete_files == 1) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "File Deletions Disabled");
  } else {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "File Deletions Disabled, but already disabled. Counter: %d",
                   my_disable_delete_obsolete_files);
  }
  return s;
}

// FIXME: can be inconsistent with DisableFileDeletions in cases like
// DBImplReadOnly
Status DBImpl::DisableFileDeletionsWithLock() {
  mutex_.AssertHeld();
  ++disable_delete_obsolete_files_;
  return Status::OK();
}

Status DBImpl::EnableFileDeletions() {
  // Job id == 0 means that this is not our background process, but rather
  // user thread
  JobContext job_context(0);
  int saved_counter;  // initialize on all paths
  {
    InstrumentedMutexLock l(&mutex_);
    if (disable_delete_obsolete_files_ > 0) {
      --disable_delete_obsolete_files_;
    }
    saved_counter = disable_delete_obsolete_files_;
    if (saved_counter == 0) {
      FindObsoleteFiles(&job_context, true);
      bg_cv_.SignalAll();
    }
  }
  if (saved_counter == 0) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "File Deletions Enabled");
    if (job_context.HaveSomethingToDelete()) {
      PurgeObsoleteFiles(job_context);
    }
  } else {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "File Deletions Enable, but not really enabled. Counter: %d",
                   saved_counter);
  }
  job_context.Clean();
  LogFlush(immutable_db_options_.info_log);
  return Status::OK();
}

bool DBImpl::IsFileDeletionsEnabled() const {
  return 0 == disable_delete_obsolete_files_;
}

// * Returns the list of live files in 'sst_live' and 'blob_live'.
// If it's doing full scan:
// * Returns the list of all files in the filesystem in
// 'full_scan_candidate_files'.
// Otherwise, gets obsolete files from VersionSet.
// no_full_scan = true -- never do the full scan using GetChildren()
// force = false -- don't force the full scan, except every
//  mutable_db_options_.delete_obsolete_files_period_micros
// force = true -- force the full scan
void DBImpl::FindObsoleteFiles(JobContext* job_context, bool force,
                               bool no_full_scan) {
  mutex_.AssertHeld();

  // if deletion is disabled, do nothing
  if (disable_delete_obsolete_files_ > 0) {
    return;
  }

  bool doing_the_full_scan = false;

  // logic for figuring out if we're doing the full scan
  if (no_full_scan) {
    doing_the_full_scan = false;
  } else if (force ||
             mutable_db_options_.delete_obsolete_files_period_micros == 0) {
    doing_the_full_scan = true;
  } else {
    const uint64_t now_micros = immutable_db_options_.clock->NowMicros();
    if ((delete_obsolete_files_last_run_ +
         mutable_db_options_.delete_obsolete_files_period_micros) <
        now_micros) {
      doing_the_full_scan = true;
      delete_obsolete_files_last_run_ = now_micros;
    }
  }

  // don't delete files that might be currently written to from compaction
  // threads
  // Since job_context->min_pending_output is set, until file scan finishes,
  // mutex_ cannot be released. Otherwise, we might see no min_pending_output
  // here but later find newer generated unfinalized files while scanning.
  assert(job_context != nullptr);
  job_context->min_pending_output = MinObsoleteSstNumberToKeep();
  job_context->files_to_quarantine = error_handler_.GetFilesToQuarantine();
  job_context->min_options_file_number = MinOptionsFileNumberToKeep();
  job_context->manifest_options_file_number =
      versions_->has_committed_options_file_number()
          ? versions_->options_file_number()
          : 0;
  job_context->min_blob_file_number_to_keep =
      versions_->current_next_file_number();
  for (auto* cfd : *versions_->GetColumnFamilySet()) {
    auto* mgr = cfd->blob_partition_manager();
    if (mgr != nullptr) {
      mgr->GetActiveBlobFileNumbers(
          &job_context->active_blob_direct_write_files);
      mgr->GetProtectedBlobFileNumbers(
          &job_context->active_blob_direct_write_files);
    }
  }

  // Get obsolete files.  This function will also update the list of
  // pending files in VersionSet().
  assert(versions_);
  versions_->GetObsoleteFiles(
      &job_context->sst_delete_files, &job_context->blob_delete_files,
      &job_context->manifest_delete_files, job_context->min_pending_output);

  // Mark the elements in job_context->sst_delete_files and
  // job_context->blob_delete_files as "grabbed for purge" so that other threads
  // calling FindObsoleteFiles with full_scan=true will not add these files to
  // candidate list for purge.
  for (const auto& sst_to_del : job_context->sst_delete_files) {
    MarkAsGrabbedForPurge(sst_to_del.metadata->fd.GetNumber());
  }

  for (const auto& blob_file : job_context->blob_delete_files) {
    MarkAsGrabbedForPurge(blob_file.GetBlobFileNumber());
  }

  // store the current filenum, lognum, etc
  job_context->manifest_file_number = versions_->manifest_file_number();
  job_context->pending_manifest_file_number =
      versions_->pending_manifest_file_number();
  job_context->log_number = MinLogNumberToKeep();
  job_context->prev_log_number = versions_->prev_log_number();

  if (doing_the_full_scan) {
    versions_->AddLiveFiles(&job_context->sst_live, &job_context->blob_live);
    // Preserve uncommitted ingestion files in this purge snapshot.
    for (const auto& [first_file_number, num_files] :
         pending_ingestion_outputs_) {
      for (uint64_t i = 0; i < num_files; ++i) {
        job_context->sst_live.push_back(first_file_number + i);
      }
    }
    InfoLogPrefix info_log_prefix(!immutable_db_options_.db_log_dir.empty(),
                                  dbname_);
    // PurgeObsoleteFiles will dedupe duplicate files.
    IOOptions io_opts;
    io_opts.do_not_recurse = true;
    for (auto& path : CollectAllDBPaths()) {
      // set of all files in the directory. We'll exclude files that are still
      // alive in the subsequent processings.
      std::vector<std::string> files;
      Status s = immutable_db_options_.fs->GetChildren(
          path, io_opts, &files, /*IODebugContext*=*/nullptr);
      s.PermitUncheckedError();  // TODO: What should we do on error?
      for (const std::string& file : files) {
        uint64_t number;
        FileType type;
        // 1. If we cannot parse the file name, we skip;
        // 2. If the file with file_number equals number has already been
        // grabbed for purge by another compaction job, or it has already been
        // schedule for purge, we also skip it if we
        // are doing full scan in order to avoid double deletion of the same
        // file under race conditions. See
        // https://github.com/meta/rocksdb/issues/3573
        if (!ParseFileName(file, &number, info_log_prefix.prefix, &type) ||
            !ShouldPurge(number)) {
          continue;
        }

        // TODO(icanadi) clean up this mess to avoid having one-off "/"
        // prefixes
        job_context->full_scan_candidate_files.emplace_back("/" + file, path);
      }
    }

    // Add log files in wal_dir
    if (!immutable_db_options_.IsWalDirSameAsDBPath(dbname_)) {
      std::vector<std::string> log_files;
      Status s = immutable_db_options_.fs->GetChildren(
          immutable_db_options_.wal_dir, io_opts, &log_files,
          /*IODebugContext*=*/nullptr);
      s.PermitUncheckedError();  // TODO: What should we do on error?
      for (const std::string& log_file : log_files) {
        job_context->full_scan_candidate_files.emplace_back(
            log_file, immutable_db_options_.wal_dir);
      }
    }

    // Add info log files in db_log_dir
    if (!immutable_db_options_.db_log_dir.empty() &&
        immutable_db_options_.db_log_dir != dbname_) {
      std::vector<std::string> info_log_files;
      Status s = immutable_db_options_.fs->GetChildren(
          immutable_db_options_.db_log_dir, io_opts, &info_log_files,
          /*IODebugContext*=*/nullptr);
      s.PermitUncheckedError();  // TODO: What should we do on error?
      for (std::string& log_file : info_log_files) {
        job_context->full_scan_candidate_files.emplace_back(
            log_file, immutable_db_options_.db_log_dir);
      }
    }
  } else {
    // Instead of filling ob_context->sst_live and job_context->blob_live,
    // directly remove files that show up in any Version. This is because
    // candidate files tend to be a small percentage of all files, so it is
    // usually cheaper to check them against every version, compared to
    // building a map for all files.
    versions_->RemoveLiveFiles(job_context->sst_delete_files,
                               job_context->blob_delete_files);
  }

  // Before potentially releasing mutex and waiting on condvar, increment
  // pending_purge_obsolete_files_ so that another thread executing
  // `GetSortedWals` will wait until this thread finishes execution since the
  // other thread will be waiting for `pending_purge_obsolete_files_`.
  // pending_purge_obsolete_files_ MUST be decremented if there is nothing to
  // delete.
  ++pending_purge_obsolete_files_;

  Defer cleanup([job_context, this]() {
    assert(job_context != nullptr);
    if (!job_context->HaveSomethingToDelete()) {
      mutex_.AssertHeld();
      --pending_purge_obsolete_files_;
      if (pending_purge_obsolete_files_ == 0) {
        bg_cv_.SignalAll();
      }
    }
  });

  // logs_ is empty when called during recovery, in which case there can't yet
  // be any tracked obsolete logs
  wal_write_mutex_.Lock();

  if (alive_wal_files_.empty() || logs_.empty()) {
    mutex_.AssertHeld();
    // We may reach here if the db is DBImplSecondary
    wal_write_mutex_.Unlock();
    return;
  }

  bool mutex_unlocked = false;
  if (!alive_wal_files_.empty() && !logs_.empty()) {
    uint64_t min_log_number = job_context->log_number;
    size_t num_alive_wal_files = alive_wal_files_.size();
    // find newly obsoleted log files
    while (alive_wal_files_.begin()->number < min_log_number) {
      auto& earliest = *alive_wal_files_.begin();
      if (immutable_db_options_.recycle_log_file_num >
              wal_recycle_files_.size() &&
          earliest.number >= MinLogNumberToRecycle()) {
        ROCKS_LOG_INFO(immutable_db_options_.info_log,
                       "adding log %" PRIu64 " to recycle list\n",
                       earliest.number);
        wal_recycle_files_.push_back(earliest.number);
      } else {
        job_context->log_delete_files.push_back(earliest.number);
      }
      if (job_context->size_log_to_delete == 0) {
        job_context->prev_wals_total_size = wals_total_size_.LoadRelaxed();
        job_context->num_alive_wal_files = num_alive_wal_files;
      }
      job_context->size_log_to_delete += earliest.size;
      wals_total_size_.FetchSubRelaxed(earliest.size);
      alive_wal_files_.pop_front();

      // Current log should always stay alive since it can't have
      // number < MinLogNumber().
      assert(alive_wal_files_.size());
    }
    wal_write_mutex_.Unlock();
    mutex_.Unlock();
    mutex_unlocked = true;
    TEST_SYNC_POINT_CALLBACK("FindObsoleteFiles::PostMutexUnlock", nullptr);
    wal_write_mutex_.Lock();
    while (!logs_.empty() && logs_.front().number < min_log_number) {
      auto& log = logs_.front();
      if (log.IsSyncing()) {
        wal_sync_cv_.Wait();
        // logs_ could have changed while we were waiting.
        continue;
      }
      // This WAL file is not live, so it's OK if we never sync the rest of it.
      // If it's already closed, then it's been fully synced. If
      // !background_close_inactive_wals then we need to Close it before
      // removing from logs_ but not blocking while holding wal_write_mutex_.
      // A WAL that may be recycled must also be closed before it leaves
      // logs_, because it can be reused as soon as it is gone from there.
      if ((!immutable_db_options_.background_close_inactive_wals ||
           immutable_db_options_.recycle_log_file_num > 0) &&
          log.writer->file()) {
        // We are taking ownership of and pinning the front entry, so we can
        // expect it to be the same after releasing and re-acquiring the lock
        log.PrepareForSync();
        wal_write_mutex_.Unlock();
        // TODO: maybe check the return value of Close.
        // TODO: plumb Env::IOActivity, Env::IOPriority
        auto s = log.writer->file()->Close({});
        s.PermitUncheckedError();
        wal_write_mutex_.Lock();
        log.writer->PublishIfClosed();
        assert(&log == &logs_.front());
        log.FinishSync();
        wal_sync_cv_.SignalAll();
      }
      wals_to_free_.push_back(log.ReleaseWriter());
      logs_.pop_front();
    }
    // Current log cannot be obsolete.
    assert(!logs_.empty());
  }

  // We're just cleaning up for DB::Write().
  assert(job_context->wals_to_free.empty());
  job_context->wals_to_free = wals_to_free_;

  wals_to_free_.clear();
  wal_write_mutex_.Unlock();
  if (mutex_unlocked) {
    mutex_.Lock();
  }
  job_context->log_recycle_files.assign(wal_recycle_files_.begin(),
                                        wal_recycle_files_.end());
}

// Delete obsolete files and log status and information of file deletion
void DBImpl::DeleteObsoleteFileImpl(int job_id, const std::string& fname,
                                    const std::string& path_to_sync,
                                    FileType type, uint64_t number) {
  TEST_SYNC_POINT_CALLBACK("DBImpl::DeleteObsoleteFileImpl::BeforeDeletion",
                           const_cast<std::string*>(&fname));
  IGNORE_STATUS_IF_ERROR(Status::IOError());

  Status file_deletion_status;
  if (type == kTableFile || type == kBlobFile || type == kWalFile) {
    // Rate limit WAL deletion only if its in the DB dir
    file_deletion_status = DeleteDBFile(
        &immutable_db_options_, fname, path_to_sync,
        /*force_bg=*/false,
        /*force_fg=*/(type == kWalFile) ? !wal_in_db_path_ : false);
  } else {
    file_deletion_status = env_->DeleteFile(fname);
  }
  TEST_SYNC_POINT_CALLBACK("DBImpl::DeleteObsoleteFileImpl:AfterDeletion",
                           &file_deletion_status);
  if (file_deletion_status.ok()) {
    ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                    "[JOB %d] [db_session_id=%s] Delete %s type=%d #%" PRIu64
                    " -- %s\n",
                    job_id, db_session_id_.c_str(), fname.c_str(), type, number,
                    file_deletion_status.ToString().c_str());
  } else if (env_->FileExists(fname).IsNotFound()) {
    ROCKS_LOG_INFO(
        immutable_db_options_.info_log,
        "[JOB %d] [db_session_id=%s] Tried to delete a non-existing file %s"
        " type=%d #%" PRIu64 " -- %s\n",
        job_id, db_session_id_.c_str(), fname.c_str(), type, number,
        file_deletion_status.ToString().c_str());
  } else {
    ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                    "[JOB %d] [db_session_id=%s] Failed to delete %s type=%d"
                    " #%" PRIu64 " -- %s\n",
                    job_id, db_session_id_.c_str(), fname.c_str(), type, number,
                    file_deletion_status.ToString().c_str());
  }
  if (type == kTableFile) {
    EventHelpers::LogAndNotifyTableFileDeletion(
        &event_logger_, job_id, number, fname, file_deletion_status, GetName(),
        db_session_id_, immutable_db_options_.listeners);
  }
  if (type == kBlobFile) {
    EventHelpers::LogAndNotifyBlobFileDeletion(
        &event_logger_, immutable_db_options_.listeners, job_id, number, fname,
        file_deletion_status, GetName(), db_session_id_);
  }
}

// Diffs the files listed in filenames and those that do not
// belong to live files are possibly removed. Also, removes all the
// files in sst_delete_files and log_delete_files.
// It is not necessary to hold the mutex when invoking this method.
void DBImpl::PurgeObsoleteFiles(JobContext& state, bool schedule_only) {
  TEST_SYNC_POINT("DBImpl::PurgeObsoleteFiles:Begin");
  // we'd better have sth to delete
  assert(state.HaveSomethingToDelete());

  // FindObsoleteFiles() should've populated this so nonzero
  assert(state.manifest_file_number != 0);

  IGNORE_STATUS_IF_ERROR(Status::IOError());

  // Now, convert lists to unordered sets, WITHOUT mutex held; set is slow.
  std::unordered_set<uint64_t> sst_live_set(state.sst_live.begin(),
                                            state.sst_live.end());
  std::unordered_set<uint64_t> blob_live_set(state.blob_live.begin(),
                                             state.blob_live.end());
  std::unordered_set<uint64_t> wal_recycle_files_set(
      state.log_recycle_files.begin(), state.log_recycle_files.end());
  std::unordered_set<uint64_t> quarantine_files_set(
      state.files_to_quarantine.begin(), state.files_to_quarantine.end());

  auto candidate_files = state.full_scan_candidate_files;
  candidate_files.reserve(
      candidate_files.size() + state.sst_delete_files.size() +
      state.blob_delete_files.size() + state.log_delete_files.size() +
      state.manifest_delete_files.size());
  // We may ignore the dbname when generating the file names.
  for (auto& file : state.sst_delete_files) {
    auto* handle = file.metadata->fd.pinned_reader.GetCacheHandle();
    if (file.only_delete_metadata) {
      if (handle) {
        // Simply release handle of file that is not being deleted
        table_cache_->Release(handle);
      }
    } else {
      // File is being deleted (actually obsolete)
      auto number = file.metadata->fd.GetNumber();
      candidate_files.emplace_back(MakeTableFileName(number), file.path);
      TableCache::ReleaseObsolete(table_cache_.get(), number, handle,
                                  file.uncache_aggressiveness);
    }
    file.DeleteMetadata();
  }

  for (const auto& blob_file : state.blob_delete_files) {
    candidate_files.emplace_back(BlobFileName(blob_file.GetBlobFileNumber()),
                                 blob_file.GetPath());
  }

  auto wal_dir = immutable_db_options_.GetWalDir();
  for (auto file_num : state.log_delete_files) {
    if (file_num > 0) {
      candidate_files.emplace_back(LogFileName(file_num), wal_dir);
    }
  }
  for (const auto& filename : state.manifest_delete_files) {
    candidate_files.emplace_back(filename, dbname_);
  }

  // dedup state.candidate_files so we don't try to delete the same
  // file twice
  std::sort(candidate_files.begin(), candidate_files.end(),
            [](const JobContext::CandidateFileInfo& lhs,
               const JobContext::CandidateFileInfo& rhs) {
              if (lhs.file_name < rhs.file_name) {
                return true;
              } else if (lhs.file_name > rhs.file_name) {
                return false;
              } else {
                return (lhs.file_path < rhs.file_path);
              }
            });
  candidate_files.erase(
      std::unique(candidate_files.begin(), candidate_files.end()),
      candidate_files.end());

  if (state.prev_wals_total_size > 0) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[JOB %d] Try to delete WAL files size %" PRIu64
                   ", prev total WAL file size %" PRIu64
                   ", number of live WAL files %" ROCKSDB_PRIszt ".\n",
                   state.job_id, state.size_log_to_delete,
                   state.prev_wals_total_size, state.num_alive_wal_files);
  }

  std::vector<std::string> old_info_log_files;
  InfoLogPrefix info_log_prefix(!immutable_db_options_.db_log_dir.empty(),
                                dbname_);

  // File numbers of most recent two OPTIONS file in candidate_files (found in
  // previous FindObsoleteFiles(full_scan=true))
  // At this point, there must not be any duplicate file numbers in
  // candidate_files.
  uint64_t optsfile_num1 = std::numeric_limits<uint64_t>::min();
  uint64_t optsfile_num2 = std::numeric_limits<uint64_t>::min();
  for (const auto& candidate_file : candidate_files) {
    const std::string& fname = candidate_file.file_name;
    uint64_t number;
    FileType type;
    if (!ParseFileName(fname, &number, info_log_prefix.prefix, &type) ||
        type != kOptionsFile) {
      continue;
    }
    if (number > optsfile_num1) {
      optsfile_num2 = optsfile_num1;
      optsfile_num1 = number;
    } else if (number > optsfile_num2) {
      optsfile_num2 = number;
    }
  }

  // For remote compactions, we need to keep OPTIONS file that may get
  // referenced by the remote worker

  optsfile_num2 = std::min(optsfile_num2, state.min_options_file_number);

  // Close WALs before trying to delete them.
  for (const auto w : state.wals_to_free) {
    // TODO: maybe check the return value of Close.
    // TODO: plumb Env::IOActivity, Env::IOPriority
    auto s = w->Close({});
    s.PermitUncheckedError();
  }

  bool own_files = OwnTablesAndLogs();
  std::unordered_set<uint64_t> files_to_del;
  for (const auto& candidate_file : candidate_files) {
    const std::string& to_delete = candidate_file.file_name;
    uint64_t number;
    FileType type;
    // Ignore file if we cannot recognize it.
    if (!ParseFileName(to_delete, &number, info_log_prefix.prefix, &type)) {
      continue;
    }

    if (quarantine_files_set.find(number) != quarantine_files_set.end()) {
      continue;
    }

    bool keep = true;
    switch (type) {
      case kWalFile:
        keep = ((number >= state.log_number) ||
                (number == state.prev_log_number) ||
                (wal_recycle_files_set.find(number) !=
                 wal_recycle_files_set.end()));
        break;
      case kDescriptorFile:
        // Keep my manifest file, and any newer incarnations'
        // (can happen during manifest roll)
        keep = (number >= state.manifest_file_number);
        break;
      case kTableFile:
        // If the second condition is not there, this makes
        // DontDeletePendingOutputs fail
        // FIXME: but should NOT keep if it came from sst_delete_files?
        keep = (sst_live_set.find(number) != sst_live_set.end()) ||
               number >= state.min_pending_output;
        if (!keep) {
          // NOTE: sometimes redundant (if came from sst_delete_files)
          // We don't know which column family is applicable here so we don't
          // know what uncache_aggressiveness would be used with
          // ReleaseObsolete(). Anyway, obsolete files ideally go into
          // sst_delete_files for better/quicker handling, and this is just a
          // backstop.
          TableCache::Evict(table_cache_.get(), number);
          files_to_del.insert(number);
        }
        break;
      case kBlobFile: {
        keep = number >= state.min_pending_output ||
               number >= state.min_blob_file_number_to_keep ||
               (blob_live_set.find(number) != blob_live_set.end()) ||
               (state.active_blob_direct_write_files.find(number) !=
                state.active_blob_direct_write_files.end());
        if (!keep) {
          const std::string blob_file_path =
              BlobFileName(candidate_file.file_path, number);
          if (ShouldKeepBlobFileDuringPurge(number, blob_file_path)) {
            keep = true;
            break;
          }
          TableCache::Evict(table_cache_.get(), number);
          files_to_del.insert(number);
        }
        break;
      }
      case kTempFile:
        // Any temp files that are currently being written to must
        // be recorded in pending_outputs_, which is inserted into "live".
        // Also, SetCurrentFile creates a temp file when writing out new
        // manifest, which is equal to state.pending_manifest_file_number. We
        // should not delete that file
        //
        // TODO(yhchiang): carefully modify the third condition to safely
        //                 remove the temp options files.
        keep = (sst_live_set.find(number) != sst_live_set.end()) ||
               (blob_live_set.find(number) != blob_live_set.end()) ||
               (number == state.pending_manifest_file_number) ||
               (to_delete.find(kOptionsFileNamePrefix) != std::string::npos);
        break;
      case kInfoLogFile:
        keep = true;
        if (number != 0) {
          old_info_log_files.push_back(to_delete);
        }
        break;
      case kOptionsFile:
        keep = (number >= optsfile_num2) ||
               (number == state.manifest_options_file_number);
        break;
      case kCompactionProgressFile:
        // Keep compaction progress files - they are managed
        // separately by DBImplSecondary for now
        keep = true;
        break;
      case kCurrentFile:
      case kDBLockFile:
      case kIdentityFile:
      case kMetaDatabase:
        keep = true;
        break;
    }

    if (keep) {
      continue;
    }

    std::string fname;
    std::string dir_to_sync;
    if (type == kTableFile) {
      fname = MakeTableFileName(candidate_file.file_path, number);
      dir_to_sync = candidate_file.file_path;
    } else if (type == kBlobFile) {
      fname = BlobFileName(candidate_file.file_path, number);
      dir_to_sync = candidate_file.file_path;
    } else {
      dir_to_sync = (type == kWalFile) ? wal_dir : dbname_;
      fname = dir_to_sync +
              ((!dir_to_sync.empty() && dir_to_sync.back() == '/') ||
                       (!to_delete.empty() && to_delete.front() == '/')
                   ? ""
                   : "/") +
              to_delete;
    }

    if (type == kWalFile && (immutable_db_options_.WAL_ttl_seconds > 0 ||
                             immutable_db_options_.WAL_size_limit_MB > 0)) {
      wal_manager_.ArchiveWALFile(fname, number);
      continue;
    }

    // If I do not own these files, e.g. secondary instance with max_open_files
    // = -1, then no need to delete or schedule delete these files since they
    // will be removed by their owner, e.g. the primary instance.
    if (!own_files) {
      continue;
    }
    if (schedule_only) {
      InstrumentedMutexLock guard_lock(&mutex_);
      SchedulePendingPurge(fname, dir_to_sync, type, number, state.job_id);
    } else {
      DeleteObsoleteFileImpl(state.job_id, fname, dir_to_sync, type, number);
    }
  }

  {
    // After purging obsolete files, remove them from files_grabbed_for_purge_.
    InstrumentedMutexLock guard_lock(&mutex_);
    autovector<uint64_t> to_be_removed;
    for (auto fn : files_grabbed_for_purge_) {
      if (files_to_del.count(fn) != 0) {
        to_be_removed.emplace_back(fn);
      }
    }
    for (auto fn : to_be_removed) {
      files_grabbed_for_purge_.erase(fn);
    }
  }

  // Delete old info log files.
  size_t old_info_log_file_count = old_info_log_files.size();
  if (old_info_log_file_count != 0 &&
      old_info_log_file_count >= immutable_db_options_.keep_log_file_num) {
    std::sort(old_info_log_files.begin(), old_info_log_files.end());
    size_t end =
        old_info_log_file_count - immutable_db_options_.keep_log_file_num;
    for (unsigned int i = 0; i <= end; i++) {
      std::string& to_delete = old_info_log_files.at(i);
      std::string full_path_to_delete =
          (immutable_db_options_.db_log_dir.empty()
               ? dbname_
               : immutable_db_options_.db_log_dir) +
          "/" + to_delete;
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "[JOB %d] Delete info log file %s\n", state.job_id,
                     full_path_to_delete.c_str());
      Status s = env_->DeleteFile(full_path_to_delete);
      if (!s.ok()) {
        if (env_->FileExists(full_path_to_delete).IsNotFound()) {
          ROCKS_LOG_INFO(
              immutable_db_options_.info_log,
              "[JOB %d] Tried to delete non-existing info log file %s FAILED "
              "-- %s\n",
              state.job_id, to_delete.c_str(), s.ToString().c_str());
        } else {
          ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                          "[JOB %d] Delete info log file %s FAILED -- %s\n",
                          state.job_id, to_delete.c_str(),
                          s.ToString().c_str());
        }
      }
    }
  }
  wal_manager_.PurgeObsoleteWALFiles();
  LogFlush(immutable_db_options_.info_log);
  TEST_SYNC_POINT("DBImpl::PurgeObsoleteFiles:BeforePendingPurgeFinished");
  InstrumentedMutexLock l(&mutex_);
  --pending_purge_obsolete_files_;
  assert(pending_purge_obsolete_files_ >= 0);
  if (schedule_only) {
    // Must change from pending_purge_obsolete_files_ to bg_purge_scheduled_
    // while holding mutex (for GetSortedWalFiles() etc.)
    SchedulePurge();
  }
  if (pending_purge_obsolete_files_ == 0) {
    bg_cv_.SignalAll();
  }
  TEST_SYNC_POINT("DBImpl::PurgeObsoleteFiles:End");
}

void DBImpl::DeleteObsoleteFiles() {
  mutex_.AssertHeld();
  JobContext job_context(next_job_id_.fetch_add(1));
  FindObsoleteFiles(&job_context, true);

  mutex_.Unlock();
  if (job_context.HaveSomethingToDelete()) {
    bool defer_purge = immutable_db_options_.avoid_unnecessary_blocking_io;
    PurgeObsoleteFiles(job_context, defer_purge);
  }
  job_context.Clean();
  mutex_.Lock();
}

bool DBImpl::ShouldKeepBlobFileDuringPurge(uint64_t number,
                                           const std::string& blob_file_path) {
  {
    InstrumentedMutexLock lock(&mutex_);
    for (auto* cfd : *versions_->GetColumnFamilySet()) {
      auto* mgr = cfd->blob_partition_manager();
      if (mgr != nullptr && mgr->IsTrackedBlobFileNumber(number)) {
        return true;
      }
    }
  }

  return ShouldKeepFooterlessBlobFile(fs_.get(), file_options_, blob_file_path);
}

VersionEdit GetDBRecoveryEditForObsoletingMemTables(
    VersionSet* vset, const ColumnFamilyData& cfd,
    const autovector<VersionEdit*>& edit_list,
    const autovector<ReadOnlyMemTable*>& memtables,
    LogsWithPrepTracker* prep_tracker) {
  VersionEdit wal_deletion_edit;
  uint64_t min_wal_number_to_keep = 0;
  assert(edit_list.size() > 0);
  if (vset->db_options()->allow_2pc) {
    // Note that if mempurge is successful, the edit_list will
    // not be applicable (contains info of new min_log number to keep,
    // and level 0 file path of SST file created during normal flush,
    // so both pieces of information are irrelevant after a successful
    // mempurge operation).
    min_wal_number_to_keep = PrecomputeMinLogNumberToKeep2PC(
        vset, cfd, edit_list, memtables, prep_tracker);

    // We piggyback the information of earliest log file to keep in the
    // manifest entry for the last file flushed.
  } else {
    min_wal_number_to_keep =
        PrecomputeMinLogNumberToKeepNon2PC(vset, cfd, edit_list);
  }

  wal_deletion_edit.SetMinLogNumberToKeep(min_wal_number_to_keep);
  if (vset->db_options()->track_and_verify_wals_in_manifest) {
    if (min_wal_number_to_keep > vset->GetWalSet().GetMinWalNumberToKeep()) {
      wal_deletion_edit.DeleteWalsBefore(min_wal_number_to_keep);
    }
  }
  return wal_deletion_edit;
}

uint64_t FindMinPrepLogReferencedByMemTable(
    VersionSet* vset, const autovector<ReadOnlyMemTable*>& memtables_to_flush) {
  uint64_t min_log = 0;

  // we must look through the memtables for two phase transactions
  // that have been committed but not yet flushed
  std::unordered_set<ReadOnlyMemTable*> memtables_to_flush_set(
      memtables_to_flush.begin(), memtables_to_flush.end());
  for (auto loop_cfd : *vset->GetColumnFamilySet()) {
    if (loop_cfd->IsDropped()) {
      continue;
    }

    auto log = loop_cfd->imm()->PrecomputeMinLogContainingPrepSection(
        &memtables_to_flush_set);

    if (log > 0 && (min_log == 0 || log < min_log)) {
      min_log = log;
    }

    log = loop_cfd->mem()->GetMinLogContainingPrepSection();

    if (log > 0 && (min_log == 0 || log < min_log)) {
      min_log = log;
    }
  }

  return min_log;
}

uint64_t FindMinPrepLogReferencedByMemTable(
    VersionSet* vset, const autovector<const autovector<ReadOnlyMemTable*>*>&
                          memtables_to_flush) {
  uint64_t min_log = 0;

  std::unordered_set<ReadOnlyMemTable*> memtables_to_flush_set;
  for (const autovector<ReadOnlyMemTable*>* memtables : memtables_to_flush) {
    memtables_to_flush_set.insert(memtables->begin(), memtables->end());
  }
  for (auto loop_cfd : *vset->GetColumnFamilySet()) {
    if (loop_cfd->IsDropped()) {
      continue;
    }

    auto log = loop_cfd->imm()->PrecomputeMinLogContainingPrepSection(
        &memtables_to_flush_set);
    if (log > 0 && (min_log == 0 || log < min_log)) {
      min_log = log;
    }

    log = loop_cfd->mem()->GetMinLogContainingPrepSection();
    if (log > 0 && (min_log == 0 || log < min_log)) {
      min_log = log;
    }
  }

  return min_log;
}

uint64_t PrecomputeMinLogNumberToKeepNon2PC(
    VersionSet* vset, const ColumnFamilyData& cfd_to_flush,
    const autovector<VersionEdit*>& edit_list) {
  assert(vset != nullptr);

  // Precompute the min log number containing unflushed data for the column
  // family being flushed (`cfd_to_flush`).
  uint64_t cf_min_log_number_to_keep = 0;
  for (auto& e : edit_list) {
    if (e->HasLogNumber()) {
      cf_min_log_number_to_keep =
          std::max(cf_min_log_number_to_keep, e->GetLogNumber());
    }
  }
  if (cf_min_log_number_to_keep == 0) {
    // No version edit contains information on log number. The log number
    // for this column family should stay the same as it is.
    cf_min_log_number_to_keep = cfd_to_flush.GetLogNumber();
  }

  // Get min log number containing unflushed data for other column families.
  uint64_t min_log_number_to_keep =
      vset->PreComputeMinLogNumberWithUnflushedData(&cfd_to_flush);
  if (cf_min_log_number_to_keep != 0) {
    min_log_number_to_keep =
        std::min(cf_min_log_number_to_keep, min_log_number_to_keep);
  }
  return min_log_number_to_keep;
}

uint64_t PrecomputeMinLogNumberToKeepNon2PC(
    VersionSet* vset, const autovector<ColumnFamilyData*>& cfds_to_flush,
    const autovector<autovector<VersionEdit*>>& edit_lists) {
  assert(vset != nullptr);
  assert(!cfds_to_flush.empty());
  assert(cfds_to_flush.size() == edit_lists.size());

  uint64_t min_log_number_to_keep = std::numeric_limits<uint64_t>::max();
  for (const auto& edit_list : edit_lists) {
    uint64_t log = 0;
    for (const auto& e : edit_list) {
      if (e->HasLogNumber()) {
        log = std::max(log, e->GetLogNumber());
      }
    }
    if (log != 0) {
      min_log_number_to_keep = std::min(min_log_number_to_keep, log);
    }
  }
  if (min_log_number_to_keep == std::numeric_limits<uint64_t>::max()) {
    min_log_number_to_keep = cfds_to_flush[0]->GetLogNumber();
    for (size_t i = 1; i < cfds_to_flush.size(); i++) {
      min_log_number_to_keep =
          std::min(min_log_number_to_keep, cfds_to_flush[i]->GetLogNumber());
    }
  }

  std::unordered_set<const ColumnFamilyData*> flushed_cfds(
      cfds_to_flush.begin(), cfds_to_flush.end());
  min_log_number_to_keep =
      std::min(min_log_number_to_keep,
               vset->PreComputeMinLogNumberWithUnflushedData(flushed_cfds));

  return min_log_number_to_keep;
}

uint64_t PrecomputeMinLogNumberToKeep2PC(
    VersionSet* vset, const ColumnFamilyData& cfd_to_flush,
    const autovector<VersionEdit*>& edit_list,
    const autovector<ReadOnlyMemTable*>& memtables_to_flush,
    LogsWithPrepTracker* prep_tracker) {
  assert(vset != nullptr);
  assert(prep_tracker != nullptr);
  // Calculate updated min_log_number_to_keep
  // Since the function should only be called in 2pc mode, log number in
  // the version edit should be sufficient.

  uint64_t min_log_number_to_keep =
      PrecomputeMinLogNumberToKeepNon2PC(vset, cfd_to_flush, edit_list);

  // if are 2pc we must consider logs containing prepared
  // sections of outstanding transactions.
  //
  // We must check min logs with outstanding prep before we check
  // logs references by memtables because a log referenced by the
  // first data structure could transition to the second under us.
  //
  // TODO: iterating over all column families under db mutex.
  // should find more optimal solution
  auto min_log_in_prep_heap =
      prep_tracker->FindMinLogContainingOutstandingPrep();

  if (min_log_in_prep_heap != 0 &&
      min_log_in_prep_heap < min_log_number_to_keep) {
    min_log_number_to_keep = min_log_in_prep_heap;
  }

  uint64_t min_log_refed_by_mem =
      FindMinPrepLogReferencedByMemTable(vset, memtables_to_flush);

  if (min_log_refed_by_mem != 0 &&
      min_log_refed_by_mem < min_log_number_to_keep) {
    min_log_number_to_keep = min_log_refed_by_mem;
  }
  return min_log_number_to_keep;
}

uint64_t PrecomputeMinLogNumberToKeep2PC(
    VersionSet* vset, const autovector<ColumnFamilyData*>& cfds_to_flush,
    const autovector<autovector<VersionEdit*>>& edit_lists,
    const autovector<const autovector<ReadOnlyMemTable*>*>& memtables_to_flush,
    LogsWithPrepTracker* prep_tracker) {
  assert(vset != nullptr);
  assert(prep_tracker != nullptr);
  assert(cfds_to_flush.size() == edit_lists.size());
  assert(cfds_to_flush.size() == memtables_to_flush.size());

  uint64_t min_log_number_to_keep =
      PrecomputeMinLogNumberToKeepNon2PC(vset, cfds_to_flush, edit_lists);

  uint64_t min_log_in_prep_heap =
      prep_tracker->FindMinLogContainingOutstandingPrep();

  if (min_log_in_prep_heap != 0 &&
      min_log_in_prep_heap < min_log_number_to_keep) {
    min_log_number_to_keep = min_log_in_prep_heap;
  }

  uint64_t min_log_refed_by_mem =
      FindMinPrepLogReferencedByMemTable(vset, memtables_to_flush);

  if (min_log_refed_by_mem != 0 &&
      min_log_refed_by_mem < min_log_number_to_keep) {
    min_log_number_to_keep = min_log_refed_by_mem;
  }

  return min_log_number_to_keep;
}

void DBImpl::SetDBId(std::string&& id, bool read_only,
                     VersionEdit* version_edit) {
  assert(db_id_.empty());
  assert(!id.empty());
  db_id_ = std::move(id);
  if (!read_only && version_edit) {
    assert(version_edit != nullptr);
    assert(versions_->GetColumnFamilySet() != nullptr);
    version_edit->SetDBId(db_id_);
    versions_->db_id_ = db_id_;
  }
}

Status DBImpl::SetupDBId(const WriteOptions& write_options, bool read_only,
                         bool is_new_db, bool is_retry,
                         VersionEdit* version_edit) {
  Status s;
  if (!is_new_db) {
    // Check for the IDENTITY file and create it if not there or
    // broken or not matching manifest
    std::string db_id_in_file;
    s = fs_->FileExists(IdentityFileName(dbname_), IOOptions(), nullptr);
    if (s.ok()) {
      IOOptions opts;
      if (is_retry) {
        opts.verify_and_reconstruct_read = true;
      }
      s = GetDbIdentityFromIdentityFile(opts, &db_id_in_file);
      if (s.ok() && !db_id_in_file.empty()) {
        if (db_id_.empty()) {
          // Loaded from file and wasn't already known from manifest
          SetDBId(std::move(db_id_in_file), read_only, version_edit);
          return s;
        } else if (db_id_ == db_id_in_file) {
          // Loaded from file and matches manifest
          return s;
        }
      }
    }
    if (s.IsNotFound()) {
      s = Status::OK();
    }
    if (!s.ok()) {
      assert(s.IsIOError());
      return s;
    }
  }
  // Otherwise IDENTITY file is missing or no good.
  // Generate new id if needed
  if (db_id_.empty()) {
    SetDBId(env_->GenerateUniqueId(), read_only, version_edit);
  }
  // Persist it to IDENTITY file if allowed
  if (!read_only && immutable_db_options_.write_identity_file) {
    s = SetIdentityFile(write_options, env_, dbname_,
                        immutable_db_options_.metadata_write_temperature,
                        db_id_);
  }
  // NOTE: an obsolete IDENTITY file with write_identity_file=false is handled
  // elsewhere, so that it's only deleted after successful recovery
  return s;
}

std::set<std::string> DBImpl::CollectAllDBPaths() {
  std::set<std::string> all_db_paths;
  all_db_paths.insert(NormalizePath(dbname_));
  for (const auto& db_path : immutable_db_options_.db_paths) {
    all_db_paths.insert(NormalizePath(db_path.path));
  }
  for (const auto* cfd : *versions_->GetColumnFamilySet()) {
    for (const auto& cf_path : cfd->ioptions().cf_paths) {
      all_db_paths.insert(NormalizePath(cf_path.path));
    }
  }
  return all_db_paths;
}

Status DBImpl::MaybeUpdateNextFileNumber(RecoveryContext* recovery_ctx) {
  mutex_.AssertHeld();
  const uint64_t prev_next_file_number = versions_->current_next_file_number();
  uint64_t largest_file_number = prev_next_file_number;
  bool on_disk_file_advanced_counter = false;
  bool manifest_relevant_file_advanced_counter = false;
  Status s;
  for (const auto& path : CollectAllDBPaths()) {
    std::vector<std::string> files;
    s = env_->GetChildren(path, &files);
    if (!s.ok()) {
      break;
    }
    if (path == NormalizePath(dbname_)) {
      recovery_ctx->existing_db_files_ = files;
    }
    for (const auto& fname : files) {
      uint64_t number = 0;
      FileType type;
      if (!ParseFileName(fname, &number, &type)) {
        continue;
      }
      std::string normalized_fpath = path;
      normalized_fpath += kFilePathSeparator;
      normalized_fpath.append(fname);
      // Use >= so a crashed-mid-allocation file at exactly
      // prev_next_file_number triggers the advance -- otherwise the next
      // NewFileNumber() would collide with it.
      if (number >= prev_next_file_number) {
        on_disk_file_advanced_counter = true;
        if (type != kOptionsFile && type != kTempFile) {
          manifest_relevant_file_advanced_counter = true;
        }
      }
      largest_file_number = std::max(largest_file_number, number);
      if ((type == kTableFile || type == kBlobFile)) {
        recovery_ctx->existing_data_files_.push_back(normalized_fpath);
      }
    }
  }
  if (!s.ok()) {
    return s;
  }

  // Legacy bug: initializing largest_file_number from prev_next_file_number
  // made the post-loop check trivially true and emitted a noop SetNextFile
  // on every recovery.
  // Preserved verbatim by default (some tests pin file numbers); skipped
  // only when the option is on AND we're not in salvage mode (which
  // requires the MANIFEST rewrite this emit triggers).
  const bool optimize_manifest_for_recovery =
      mutable_db_options_.optimize_manifest_for_recovery &&
      !immutable_db_options_.best_efforts_recovery;

  if (!optimize_manifest_for_recovery ||
      manifest_relevant_file_advanced_counter) {
    if (largest_file_number >= prev_next_file_number) {
      versions_->next_file_number_.store(largest_file_number + 1);
    }
    TEST_SYNC_POINT("DBImpl::MaybeUpdateNextFileNumber:EmitEdit");
    VersionEdit edit;
    edit.SetNextFile(versions_->next_file_number_.load());
    assert(versions_->GetColumnFamilySet());
    ColumnFamilyData* default_cfd =
        versions_->GetColumnFamilySet()->GetDefault();
    assert(default_cfd);
    recovery_ctx->UpdateVersionEdits(default_cfd, edit);
  } else {
    if (on_disk_file_advanced_counter) {
      versions_->next_file_number_.store(largest_file_number + 1);
    }
    TEST_SYNC_POINT("DBImpl::Recovery:SkippedNoopEdit:NextFileNumber");
  }
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
