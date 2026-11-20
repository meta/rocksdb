//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "db/job_context.h"
#include "db/version_set.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "logging/logging.h"
#include "options/options_parser.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/metadata.h"
#include "rocksdb/types.h"
#include "rocksdb/wal_iterator.h"
#include "test_util/sync_point.h"
#include "util/file_checksum_helper.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

Status DBImpl::FlushForGetLiveFiles(bool force_atomic_flush) {
  FlushOptions flush_opts;
  flush_opts.force_atomic_flush = force_atomic_flush;
  return DBImpl::FlushAllColumnFamilies(flush_opts, FlushReason::kGetLiveFiles);
}

Status DBImpl::GetLiveFiles(std::vector<std::string>& ret,
                            uint64_t* manifest_file_size, bool flush_memtable) {
  *manifest_file_size = 0;

  mutex_.Lock();

  if (flush_memtable) {
    Status status = FlushForGetLiveFiles();
    if (!status.ok()) {
      mutex_.Unlock();
      ROCKS_LOG_ERROR(immutable_db_options_.info_log, "Cannot Flush data %s\n",
                      status.ToString().c_str());
      return status;
    }
  }

  // Make a set of all of the live table and blob files
  std::vector<uint64_t> live_table_files;
  std::vector<uint64_t> live_blob_files;
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    if (cfd->IsDropped()) {
      continue;
    }
    cfd->current()->AddLiveFiles(&live_table_files, &live_blob_files);
  }

  ret.clear();
  ret.reserve(live_table_files.size() + live_blob_files.size() +
              3);  // for CURRENT + MANIFEST + OPTIONS

  // create names of the live files. The names are not absolute
  // paths, instead they are relative to dbname_.
  for (const auto& table_file_number : live_table_files) {
    ret.emplace_back(MakeTableFileName("", table_file_number));
  }

  for (const auto& blob_file_number : live_blob_files) {
    ret.emplace_back(BlobFileName("", blob_file_number));
  }

  ret.emplace_back(CurrentFileName(""));
  ret.emplace_back(DescriptorFileName("", versions_->manifest_file_number()));
  // In read-only mode the OPTIONS file number is zero when no OPTIONS file
  // exist at all. In this cases we do not record any OPTIONS file in the live
  // file list.
  if (versions_->options_file_number() != 0) {
    ret.emplace_back(OptionsFileName("", versions_->options_file_number()));
  }

  // find length of manifest file while holding the mutex lock
  *manifest_file_size = versions_->manifest_file_size();

  mutex_.Unlock();
  return Status::OK();
}

Status DBImpl::GetSortedWalFiles(VectorWalPtr& files) {
  return GetSortedWalFilesImpl(files,
                               /*need_seqnos*/ true);
}

Status DBImpl::GetSortedWalFilesImpl(VectorWalPtr& files, bool need_seqnos) {
  // Record tracked WALs as a (minimum) cross-check for directory scan
  std::vector<uint64_t> required_by_manifest;

  // If caller disabled deletions, this function should return files that are
  // guaranteed not to be deleted until deletions are re-enabled. We need to
  // wait for pending purges to finish since WalManager doesn't know which
  // files are going to be purged. Additional purges won't be scheduled as
  // long as deletions are disabled (so the below loop must terminate).
  // Also note that we disable deletions anyway to avoid the case where a
  // file is deleted in the middle of the scan, causing IO error.
  Status deletions_disabled = DisableFileDeletions();
  {
    InstrumentedMutexLock l(&mutex_);
    while (pending_purge_obsolete_files_ > 0 || bg_purge_scheduled_ > 0) {
      TEST_SYNC_POINT("DBImpl::GetSortedWalFilesImpl:WaitPurge");
      bg_cv_.Wait();
    }

    // Record tracked WALs as a (minimum) cross-check for directory scan
    const auto& manifest_wals = versions_->GetWalSet().GetWals();
    required_by_manifest.reserve(manifest_wals.size());
    for (const auto& wal : manifest_wals) {
      required_by_manifest.push_back(wal.first);
    }
  }

  // NOTE: need to include archived WALs because needed WALs might have been
  // archived since getting required_by_manifest set
  Status s = wal_manager_.GetSortedWalFiles(files, need_seqnos,
                                            /*include_archived*/ true);

  // DisableFileDeletions / EnableFileDeletions not supported in read-only DB
  if (deletions_disabled.ok()) {
    Status s2 = EnableFileDeletions();
    assert(s2.ok());
    s2.PermitUncheckedError();
  } else {
    assert(deletions_disabled.IsNotSupported());
  }

  if (s.ok()) {
    // Verify includes those required by manifest (one sorted list is superset
    // of the other)
    auto required = required_by_manifest.begin();
    auto included = files.begin();

    while (required != required_by_manifest.end()) {
      if (included == files.end() || *required < (*included)->LogNumber()) {
        // FAIL - did not find
        return Status::Corruption(
            "WAL file " + std::to_string(*required) +
            " required by manifest but not in directory list");
      }
      if (*required == (*included)->LogNumber()) {
        ++required;
        ++included;
      } else {
        assert(*required > (*included)->LogNumber());
        ++included;
      }
    }
  }

  if (s.ok()) {
    size_t wal_count = files.size();
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Number of WAL files %" ROCKSDB_PRIszt " (%" ROCKSDB_PRIszt
                   " required by manifest)",
                   wal_count, required_by_manifest.size());
#ifndef NDEBUG
    std::ostringstream wal_names;
    for (const auto& wal : files) {
      wal_names << wal->PathName() << " ";
    }

    std::ostringstream wal_required_by_manifest_names;
    for (const auto& wal : required_by_manifest) {
      wal_required_by_manifest_names << wal << ".log ";
    }

    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Log files : %s .Log files required by manifest: %s.",
                   wal_names.str().c_str(),
                   wal_required_by_manifest_names.str().c_str());
#endif  // NDEBUG
  }
  return s;
}

Status DBImpl::GetCurrentWalFile(std::unique_ptr<WalFile>* current_wal_file) {
  uint64_t current_logfile_number;
  {
    InstrumentedMutexLock l(&mutex_);
    current_logfile_number = cur_wal_number_;
  }

  return wal_manager_.GetLiveWalFile(current_logfile_number, current_wal_file);
}

Status DBImpl::AppendColumnFamilyDropsToManifest(
    const std::string& manifest_path, uint64_t manifest_size,
    const std::vector<uint32_t>& cf_ids,
    uint64_t committed_options_file_number) {
  const WriteOptions write_options;
  // Snapshot manifest_preallocation_size_ under the DB mutex per VersionSet's
  // contract (the field is mutated by SetDBOptions under this mutex).
  uint64_t preallocation_size = 0;
  {
    InstrumentedMutexLock l(&mutex_);
    preallocation_size = versions_->manifest_preallocation_size_;
  }
  return versions_->AppendColumnFamilyDropsToManifest(
      manifest_path, manifest_size, cf_ids, write_options, preallocation_size,
      committed_options_file_number);
}

Status DBImpl::CreateOptionsFileForSubsetCheckpoint(
    const std::string& checkpoint_dir,
    const SubsetCheckpointOptionsSnapshot& options_snapshot) {
  if (options_snapshot.options_file_number == 0) {
    return Status::OK();
  }

  // The snapshot's CF options are shallow copies that share live pointer
  // sub-options (e.g. RibbonFilterPolicy, whose bloom_before_level is mutated
  // in place by SetOptions). Hold options_mutex_, as WriteOptionsFile does, so
  // SetOptions cannot change them between writing the file and verifying it.
  InstrumentedMutexLock ol(&options_mutex_);

  const std::string options_path =
      OptionsFileName(checkpoint_dir, options_snapshot.options_file_number);
  return PersistRocksDBOptions(
      WriteOptions(), options_snapshot.db_options, options_snapshot.cf_names,
      options_snapshot.cf_options, options_path, fs_.get());
}

Status DBImpl::GetLiveFilesStorageInfo(
    const LiveFilesStorageInfoOptions& opts,
    std::vector<LiveFileStorageInfo>* files) {
  return GetLiveFilesStorageInfoImpl(opts, /*include_cf_ids=*/{}, files,
                                     /*excluded_cf_ids=*/nullptr,
                                     /*options_snapshot=*/nullptr);
}

Status DBImpl::GetLiveFilesStorageInfoForSubsetCheckpoint(
    const LiveFilesStorageInfoOptions& opts,
    const std::vector<uint32_t>& include_cf_ids,
    std::vector<LiveFileStorageInfo>* files,
    std::vector<uint32_t>* excluded_cf_ids,
    SubsetCheckpointOptionsSnapshot* options_snapshot) {
  // Internal contract: caller must coalesce ids and always include the default
  // CF (it cannot be dropped, so a subset checkpoint without it would produce
  // an unopenable DB). CheckpointImpl enforces this before calling here.
  assert(!include_cf_ids.empty());
  assert(std::find(include_cf_ids.begin(), include_cf_ids.end(),
                   default_cf_handle_->GetID()) != include_cf_ids.end());
  assert(excluded_cf_ids != nullptr);
  assert(options_snapshot != nullptr);
  return GetLiveFilesStorageInfoImpl(opts, include_cf_ids, files,
                                     excluded_cf_ids, options_snapshot);
}

Status DBImpl::GetLiveFilesStorageInfoImpl(
    const LiveFilesStorageInfoOptions& opts,
    const std::vector<uint32_t>& include_cf_ids,
    std::vector<LiveFileStorageInfo>* files,
    std::vector<uint32_t>* excluded_cf_ids,
    SubsetCheckpointOptionsSnapshot* options_snapshot) {
  // To avoid returning partial results, only move results to files on success.
  assert(files);
  files->clear();
  std::vector<LiveFileStorageInfo> results;

  const bool cf_subset = !include_cf_ids.empty();
  assert(!cf_subset || excluded_cf_ids != nullptr);
  assert(!cf_subset || options_snapshot != nullptr);
  const std::unordered_set<uint32_t> include_cf_id_set(include_cf_ids.begin(),
                                                       include_cf_ids.end());
  // Tracks which requested ids remain to be observed as live under the DB
  // mutex; any id still present after the enumeration was either dropped
  // before or during the checkpoint (race) and must not silently disappear
  // from the checkpoint.
  std::unordered_set<uint32_t> unseen_include_cf_ids = include_cf_id_set;

  // NOTE: This implementation was largely migrated from Checkpoint.

  VectorWalPtr live_wal_files;
  bool flush_memtable = true;
  if (!immutable_db_options_.allow_2pc) {
    if (opts.wal_size_for_flush == std::numeric_limits<uint64_t>::max()) {
      flush_memtable = false;
    } else if (opts.wal_size_for_flush > 0) {
      // FIXME: avoid querying the filesystem for current WAL state
      // If the outstanding WAL files are small, we skip the flush.
      // Don't take archived log size into account when calculating wal
      // size for flush, and don't need to verify consistency with manifest
      // here & now.
      Status wal_s = wal_manager_.GetSortedWalFiles(live_wal_files,
                                                    /* need_seqnos */ false,
                                                    /*include_archived*/ false);

      if (!wal_s.ok()) {
        return wal_s;
      }

      // Don't flush column families if total log size is smaller than
      // log_size_for_flush. We copy the log files instead.
      // We may be able to cover 2PC case too.
      uint64_t total_wal_size = 0;
      for (auto& wal : live_wal_files) {
        assert(wal->Type() == kAliveLogFile);
        total_wal_size += wal->SizeFileBytes();
      }
      if (total_wal_size < opts.wal_size_for_flush) {
        flush_memtable = false;
      }
      live_wal_files.clear();
    }
  }

  // This is a modified version of GetLiveFiles, to get access to more
  // metadata.
  mutex_.Lock();
  bool wal_locked = false;
  const bool needs_blob_direct_write_flush =
      HasInFlightBlobDirectWriteFilesWithLockHeld();
  if (needs_blob_direct_write_flush && !flush_memtable) {
    mutex_.Unlock();
    return Status::NotSupported(
        "Blob direct write requires flushing active blob files before "
        "capturing live files. Retry with flush enabled.");
  }
  if (flush_memtable) {
    wal_locked = !lock_wal_owner_thread_id_counts_.empty();
    if (wal_locked) {
      if (needs_blob_direct_write_flush) {
        mutex_.Unlock();
        return Status::NotSupported(
            "Blob direct write requires flushing active blob files before "
            "capturing live files. Retry with WAL unlocked.");
      }
    }
    if (wal_locked && !has_unpersisted_data_.load(std::memory_order_relaxed)) {
      ROCKS_LOG_INFO(
          immutable_db_options_.info_log,
          "Skipping FlushForGetLiveFiles while WAL is locked and all data is "
          "already persisted");
    } else {
      Status status = FlushForGetLiveFiles(opts.atomic_flush);
      if (!status.ok()) {
        mutex_.Unlock();
        ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                        "Cannot Flush data %s\n", status.ToString().c_str());
        return status;
      }
    }
  }

  // Make a set of all of the live table and blob files
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    if (cfd->IsDropped()) {
      continue;
    }
    if (cf_subset &&
        include_cf_id_set.find(cfd->GetID()) == include_cf_id_set.end()) {
      excluded_cf_ids->push_back(cfd->GetID());
      continue;
    }
    if (cf_subset) {
      unseen_include_cf_ids.erase(cfd->GetID());
    }
    VersionStorageInfo& vsi = *cfd->current()->storage_info();
    auto& cf_paths = cfd->ioptions().cf_paths;

    auto GetDir = [&](size_t path_id) {
      // Matching TableFileName() behavior
      if (path_id >= cf_paths.size()) {
        assert(false);
        return cf_paths.back().path;
      } else {
        return cf_paths[path_id].path;
      }
    };

    for (int level = 0; level < vsi.num_levels(); ++level) {
      const auto& level_files = vsi.LevelFiles(level);
      for (const auto& meta : level_files) {
        assert(meta);

        results.emplace_back();
        LiveFileStorageInfo& info = results.back();

        info.relative_filename = MakeTableFileName(meta->fd.GetNumber());
        info.directory = GetDir(meta->fd.GetPathId());
        info.file_number = meta->fd.GetNumber();
        info.file_type = kTableFile;
        info.size = meta->fd.GetFileSize();
        if (opts.include_checksum_info) {
          info.file_checksum_func_name = meta->file_checksum_func_name;
          info.file_checksum = meta->file_checksum;
          if (info.file_checksum_func_name.empty()) {
            info.file_checksum_func_name = kUnknownFileChecksumFuncName;
            info.file_checksum = kUnknownFileChecksum;
          }
        }
        info.temperature = meta->temperature;
      }
    }
    const auto& blob_files = vsi.GetBlobFiles();
    for (const auto& meta : blob_files) {
      assert(meta);

      results.emplace_back();
      LiveFileStorageInfo& info = results.back();

      info.relative_filename = BlobFileName(meta->GetBlobFileNumber());
      info.directory = GetDir(/* path_id */ 0);
      info.file_number = meta->GetBlobFileNumber();
      info.file_type = kBlobFile;
      info.size = meta->GetBlobFileSize();
      if (opts.include_checksum_info) {
        info.file_checksum_func_name = meta->GetChecksumMethod();
        info.file_checksum = meta->GetChecksumValue();
        if (info.file_checksum_func_name.empty()) {
          info.file_checksum_func_name = kUnknownFileChecksumFuncName;
          info.file_checksum = kUnknownFileChecksum;
        }
      }
      // TODO?: info.temperature
    }
  }

  // Release-build validation: every id the subset-checkpoint caller asked for
  // must have been observed as live under the mutex. If any is missing, it was
  // dropped before we ran (stale handle) or lost a race with a concurrent
  // drop; either way the checkpoint would silently omit it, violating the
  // caller's contract that the checkpoint contains the requested CFs.
  if (cf_subset && !unseen_include_cf_ids.empty()) {
    mutex_.Unlock();
    return Status::InvalidArgument(
        "Requested column family id was not live at checkpoint time");
  }

  if (cf_subset) {
    assert(options_snapshot != nullptr);
    assert(excluded_cf_ids != nullptr);
    *options_snapshot = SubsetCheckpointOptionsSnapshot();
    if (!excluded_cf_ids->empty()) {
      options_snapshot->options_file_number = versions_->NewFileNumber();
      options_snapshot->commit_options_file_number =
          versions_->has_committed_options_file_number();
      options_snapshot->db_options =
          BuildDBOptions(immutable_db_options_, mutable_db_options_);
      ColumnFamilySet* column_family_set = versions_->GetColumnFamilySet();
      assert(column_family_set != nullptr);
      for (auto* cfd : *column_family_set) {
        assert(cfd != nullptr);
        if (!cfd->IsDropped() &&
            include_cf_id_set.find(cfd->GetID()) != include_cf_id_set.end()) {
          options_snapshot->cf_names.push_back(cfd->GetName());
          options_snapshot->cf_options.push_back(cfd->GetLatestCFOptions());
        }
      }
    }
  }

  // Capture some final info before releasing mutex
  const uint64_t manifest_number = versions_->manifest_file_number();
  uint64_t manifest_size = versions_->manifest_file_size();
  if (cf_subset && !excluded_cf_ids->empty()) {
    Status manifest_s = versions_->GetManifestAppendBoundary(&manifest_size);
    if (!manifest_s.ok()) {
      mutex_.Unlock();
      return manifest_s;
    }
  }
  const uint64_t options_number = versions_->options_file_number();
  const uint64_t min_log_num = MinLogNumberToKeep();
  // Ensure consistency with manifest for track_and_verify_wals_in_manifest
  const uint64_t max_log_num = cur_wal_number_;

  if (options_number != 0) {
    // A concurrent SetOptions() may otherwise delete this OPTIONS file before
    // its size is read below, even when the caller has not disabled deletions.
    DisableFileDeletionsWithLock().PermitUncheckedError();
  }
  mutex_.Unlock();

  uint64_t options_size = 0;
  if (options_number != 0) {
    // File deletions must be disabled by callers while they copy this snapshot.
    // The number belongs to the MANIFEST/CF snapshot captured above, while the
    // immutable file's size can be read without holding the DB mutex.
    TEST_SYNC_POINT("DBImpl::GetLiveFilesStorageInfo:BeforeOptionsFileSize");
    Status options_s = GetEnv()->GetFileSize(
        OptionsFileName(GetName(), options_number), &options_size);
    {
      // Not EnableFileDeletions(): read-only DB implementations override it to
      // return NotSupported without undoing DisableFileDeletionsWithLock().
      InstrumentedMutexLock l(&mutex_);
      assert(disable_delete_obsolete_files_ > 0);
      --disable_delete_obsolete_files_;
    }
    if (!options_s.ok()) {
      return options_s;
    }
  }

  std::string manifest_fname = DescriptorFileName(manifest_number);
  {  // MANIFEST
    results.emplace_back();
    LiveFileStorageInfo& info = results.back();

    info.relative_filename = manifest_fname;
    info.directory = GetName();
    info.file_number = manifest_number;
    info.file_type = kDescriptorFile;
    info.size = manifest_size;
    info.trim_to_size = true;
    if (opts.include_checksum_info) {
      info.file_checksum_func_name = kUnknownFileChecksumFuncName;
      info.file_checksum = kUnknownFileChecksum;
    }
  }

  {  // CURRENT
    results.emplace_back();
    LiveFileStorageInfo& info = results.back();

    info.relative_filename = kCurrentFileName;
    info.directory = GetName();
    info.file_type = kCurrentFile;
    // CURRENT could be replaced so we have to record the contents as needed.
    info.replacement_contents = manifest_fname + "\n";
    info.size = manifest_fname.size() + 1;
    if (opts.include_checksum_info) {
      info.file_checksum_func_name = kUnknownFileChecksumFuncName;
      info.file_checksum = kUnknownFileChecksum;
    }
  }

  // In read-only mode the OPTIONS file number is zero when no OPTIONS file
  // exist at all. In this cases we do not record any OPTIONS file in the live
  // file list.
  if (options_number != 0) {
    results.emplace_back();
    LiveFileStorageInfo& info = results.back();

    info.relative_filename = OptionsFileName(options_number);
    info.directory = GetName();
    info.file_number = options_number;
    info.file_type = kOptionsFile;
    info.size = options_size;
    if (opts.include_checksum_info) {
      info.file_checksum_func_name = kUnknownFileChecksumFuncName;
      info.file_checksum = kUnknownFileChecksum;
    }
  }

  // Some legacy testing stuff  TODO: carefully clean up obsolete parts
  TEST_SYNC_POINT("CheckpointImpl::CreateCheckpoint:FlushDone");

  TEST_SYNC_POINT("CheckpointImpl::CreateCheckpoint:SavedLiveFiles1");
  TEST_SYNC_POINT("CheckpointImpl::CreateCheckpoint:SavedLiveFiles2");

  // FlushWAL is required to ensure we can physically copy everything
  // logically written to the WAL. (Sync not strictly required for
  // active WAL to be copied rather than hard linked, even when
  // Checkpoint guarantees that the copied-to file is sync-ed. Plus we can't
  // help track_and_verify_wals_in_manifest after manifest_size is
  // already determined.)
  Status s = FlushWAL(/*sync=*/false);
  if (s.IsNotSupported()) {  // read-only DB or similar
    s = Status::OK();
  }

  TEST_SYNC_POINT("CheckpointImpl::CreateCustomCheckpoint:AfterGetLive1");
  TEST_SYNC_POINT("CheckpointImpl::CreateCustomCheckpoint:AfterGetLive2");

  // Even after WAL flush, there could be multiple WALs that are not
  // fully synced. Although the output DB of a Checkpoint or Backup needs
  // to be fully synced on return, we don't strictly need to sync this
  // DB (the input DB). If we allow Checkpoint to hard link an inactive
  // WAL that isn't fully synced, that could result in an insufficiently
  // sync-ed Checkpoint. Here we get the set of WALs that are potentially
  // unsynced or still being written to, to prevent them from being hard
  // linked. Enforcing max_log_num from above ensures any new WALs after
  // GetOpenWalSizes() and before GetSortedWalFiles() are not included in
  // the results.
  // NOTE: we might still hard link a file that is open for writing, even
  // if we don't do any more writes to it.
  //
  // In a step toward reducing unnecessary file metadata queries, we also
  // get and use our known flushed sizes for those WALs.
  // FIXME: eventually we should not be using filesystem queries at all for
  // the required set of WAL files.
  //
  // However for recycled log files, we just copy the whole file,
  // for better or worse.
  //
  std::map<uint64_t, uint64_t> open_wal_number_to_size;
  bool recycling_log_files = immutable_db_options_.recycle_log_file_num > 0;
  if (s.ok() && !recycling_log_files) {
    s = GetOpenWalSizes(open_wal_number_to_size);
  }

  // [old comment] If we have more than one column family, we also need to get
  // WAL files.
  if (s.ok()) {
    // FIXME: avoid querying the filesystem for current WAL state
    s = GetSortedWalFilesImpl(live_wal_files,
                              /* need_seqnos */ false);
  }
  if (!s.ok()) {
    return s;
  }

  size_t wal_count = live_wal_files.size();
  // Link WAL files. Copy exact size of last one because it is the only one
  // that has changes after the last flush.
  auto wal_dir = immutable_db_options_.GetWalDir();
  for (size_t i = 0; s.ok() && i < wal_count; ++i) {
    if ((live_wal_files[i]->Type() == kAliveLogFile) &&
        (!flush_memtable || live_wal_files[i]->LogNumber() >= min_log_num) &&
        live_wal_files[i]->LogNumber() <= max_log_num) {
      results.emplace_back();
      LiveFileStorageInfo& info = results.back();
      auto f = live_wal_files[i]->PathName();
      assert(!f.empty() && f[0] == '/');
      info.relative_filename = f.substr(1);
      info.directory = wal_dir;
      info.file_number = live_wal_files[i]->LogNumber();
      info.file_type = kWalFile;
      if (recycling_log_files) {
        info.size = live_wal_files[i]->SizeFileBytes();
        // Recyclable WAL files must be copied instead of hard linked
        info.trim_to_size = true;
      } else {
        auto it = open_wal_number_to_size.find(info.file_number);
        if (it == open_wal_number_to_size.end()) {
          // Known fully synced and no future writes (in part from
          // max_log_num check). Ok to hard link
          info.size = live_wal_files[i]->SizeFileBytes();
          assert(!info.trim_to_size);
        } else {
          // Marked as (possibly) still open -> use our known flushed size
          // and force file copy instead of hard link
          info.size = it->second;
          info.trim_to_size = true;
          // FIXME: this is needed as long as db_stress uses
          // SetReadUnsyncedData(false), because it will only be able to
          // copy the synced portion of the WAL, which under
          // SetReadUnsyncedData(false) is given by the reported file size.
          info.size = std::min(info.size, live_wal_files[i]->SizeFileBytes());
        }
      }
      if (opts.include_checksum_info) {
        info.file_checksum_func_name = kUnknownFileChecksumFuncName;
        info.file_checksum = kUnknownFileChecksum;
      }
    }
  }

  if (s.ok()) {
    // Only move results to output on success.
    *files = std::move(results);
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
