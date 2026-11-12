//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 Facebook.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "utilities/checkpoint/checkpoint_impl.h"

#include <algorithm>
#include <cinttypes>
#include <future>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_set>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "db/wal_manager.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "logging/logging.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/metadata.h"
#include "rocksdb/options.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/wal_iterator.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/file_checksum_helper.h"
#include "utilities/copy_engine/copy_engine.h"

namespace ROCKSDB_NAMESPACE {

Status Checkpoint::Create(DB* db, Checkpoint** checkpoint_ptr) {
  *checkpoint_ptr = new CheckpointImpl(db);
  return Status::OK();
}

namespace {
// CheckpointEngine backed by the shared CopyEngine pool; reuses
// CheckpointImpl's enumeration via CreateCheckpointImpl.
class CheckpointEngineImpl : public CheckpointEngine {
 public:
  explicit CheckpointEngineImpl(const CheckpointEngineOptions& options)
      : options_(options) {}

  // Spawn the pool once at Open() so it is reused across checkpoints.
  void Initialize() {
    // Build a rate limiter from backup_rate_limit if the caller passed a number
    // instead of a limiter (mirrors BackupEngineImpl).
    if (options_.backup_rate_limiter == nullptr &&
        options_.backup_rate_limit > 0) {
      options_.backup_rate_limiter.reset(
          NewGenericRateLimiter(options_.backup_rate_limit));
    }
    CopyEngineOptions copy_options;
    copy_options.max_background_operations =
        std::max(1, options_.max_background_operations);
    copy_options.io_buffer_size = options_.io_buffer_size;
    copy_options.read_bytes_ticker = CHECKPOINT_READ_BYTES;
    copy_options.write_bytes_ticker = CHECKPOINT_WRITE_BYTES;
    copy_options.info_log = options_.info_log;
    copy_options.thread_name = "rocksdb:ckpt";
    copy_engine_ = std::make_unique<CopyEngine>(std::move(copy_options));
  }

  IOStatus CreateCheckpoint(
      DB* source_db, const std::string& destination_dir,
      uint64_t* sequence_number_ptr,
      const CreateCheckpointOptions& create_options) override {
    assert(copy_engine_ != nullptr);
    if (create_options.decrease_background_thread_cpu_priority) {
      copy_engine_->MaybeDecreaseCpuPriority(
          create_options.background_thread_cpu_priority);
    }

    std::vector<uint32_t> include_cf_ids;
    Status resolve_s = CheckpointImpl::ResolveIncludeColumnFamilyIds(
        source_db, create_options.column_families, &include_cf_ids);
    if (!resolve_s.ok()) {
      return status_to_io_status(std::move(resolve_s));
    }

    const bool use_link = options_.use_link_file_when_available;
    CheckpointImpl impl(source_db);
    return status_to_io_status(impl.CreateCheckpointImpl(
        destination_dir, create_options.log_size_for_flush, sequence_number_ptr,
        copy_engine_.get(), use_link, options_.backup_rate_limiter.get(),
        include_cf_ids));
  }

  IOStatus ExportColumnFamily(
      DB* source_db, ColumnFamilyHandle* handle,
      const std::string& destination_dir,
      ExportImportFilesMetaData** out_metadata,
      const CreateCheckpointOptions& /*options*/) override {
    // Export runs serially for now; parallelizing it is future work.
    CheckpointImpl impl(source_db);
    return status_to_io_status(
        impl.ExportColumnFamily(handle, destination_dir, out_metadata));
  }

 private:
  CheckpointEngineOptions options_;
  std::unique_ptr<CopyEngine> copy_engine_;
};

// Stages checkpoint files into the temp dir, hiding the serial-vs-parallel
// split from CreateCheckpointImpl. Finish() awaits any deferred work and
// returns the first error.
class CheckpointFileMover {
 public:
  virtual ~CheckpointFileMover() = default;

  // Returning NotSupported makes CreateCustomCheckpoint retry the file via
  // Copy. temperature is the file's temperature, preserved if a failed link
  // falls back to a copy.
  virtual Status Link(const std::string& src, const std::string& dst,
                      Temperature temperature) = 0;

  virtual Status Copy(const std::string& src, const std::string& dst,
                      uint64_t size_limit, Temperature temperature) = 0;

  virtual Status Finish() = 0;
};

// Links/copies inline (legacy Checkpoint behavior); a failed Link() lets
// CreateCustomCheckpoint fall back to copy. Finish() is a no-op.
class SerialFileMover : public CheckpointFileMover {
 public:
  SerialFileMover(FileSystem* fs, bool use_fsync, Logger* info_log)
      : fs_(fs), use_fsync_(use_fsync), info_log_(info_log) {}

  Status Link(const std::string& src, const std::string& dst,
              Temperature /*temperature*/) override {
    // A failed link falls back to copy_file_cb, which preserves temperature.
    ROCKS_LOG_INFO(info_log_, "Hard Linking %s", dst.c_str());
    return fs_->LinkFile(src, dst, IOOptions(), nullptr);
  }

  Status Copy(const std::string& src, const std::string& dst,
              uint64_t size_limit, Temperature temperature) override {
    ROCKS_LOG_INFO(info_log_, "Copying %s", dst.c_str());
    return CopyFile(fs_, src, temperature, dst, temperature, size_limit,
                    use_fsync_, /*io_tracer=*/nullptr);
  }

  Status Finish() override { return Status::OK(); }

 private:
  FileSystem* fs_;
  bool use_fsync_;
  Logger* info_log_;
};

// Enqueues link/copy work on the CopyEngine pool; Finish() awaits it and does
// the link->copy fallback for FileSystems that cannot hard-link (which costs an
// extra round-trip per file; the intended remote file systems support linking).
class ParallelFileMover : public CheckpointFileMover {
 public:
  ParallelFileMover(CopyEngine* engine, Env* env, bool use_link, bool use_fsync,
                    RateLimiter* copy_rate_limiter, Statistics* stats,
                    Logger* info_log)
      : engine_(engine),
        env_(env),
        use_link_(use_link),
        use_fsync_(use_fsync),
        copy_rate_limiter_(copy_rate_limiter),
        stats_(stats),
        info_log_(info_log) {}

  Status Link(const std::string& src, const std::string& dst,
              Temperature temperature) override {
    if (!use_link_) {
      // Force CreateCustomCheckpoint down its copy path.
      return Status::NotSupported("Linking disabled");
    }
    ROCKS_LOG_INFO(info_log_, "Hard Linking %s", dst.c_str());
    // Linking moves no data, so the WorkItem carries no temperature; it is kept
    // in PendingLink for the copy fallback in Finish().
    WorkItem w(src, dst, Temperature::kUnknown, Temperature::kUnknown,
               /*contents=*/"", env_, env_, EnvOptions(), /*sync=*/true,
               /*use_fsync=*/use_fsync_, /*rate_limiter=*/nullptr,
               /*size_limit=*/0, /*stats=*/nullptr);
    w.type = WorkItemType::Link;
    link_pendings_.push_back({w.result.get_future(), src, dst, temperature});
    engine_->Submit(std::move(w));
    return Status::OK();
  }

  Status Copy(const std::string& src, const std::string& dst,
              uint64_t size_limit, Temperature temperature) override {
    ROCKS_LOG_INFO(info_log_, "Copying %s", dst.c_str());
    WorkItem w(src, dst, temperature, temperature, /*contents=*/"", env_, env_,
               EnvOptions(), /*sync=*/true, /*use_fsync=*/use_fsync_,
               copy_rate_limiter_, size_limit, stats_);
    copy_futures_.push_back(w.result.get_future());
    engine_->Submit(std::move(w));
    return Status::OK();
  }

  Status Finish() override {
    Status result;
    auto fold = [&result](const IOStatus& io_s) {
      if (!io_s.ok() && result.ok()) {
        result = io_s;
      }
    };
    for (auto& f : copy_futures_) {
      fold(f.get().io_status);
    }
    // Await all links even after an error: every submitted WorkItem must finish
    // before we return, or pool threads could touch the staging dir during
    // cleanup. NotSupported ones fall back to a copy (still on the pool).
    std::vector<std::future<WorkItemResult>> fallback_copies;
    for (auto& p : link_pendings_) {
      IOStatus link_s = p.future.get().io_status;
      if (link_s.ok()) {
        continue;
      }
      if (link_s.IsNotSupported()) {
        ROCKS_LOG_INFO(info_log_, "Copying (link fallback) %s", p.dst.c_str());
        // size_limit 0 copies the whole file; only immutable (non-trim_to_size)
        // files are linked, so this matches the serial path's info.size bound.
        WorkItem w(p.src, p.dst, p.temperature, p.temperature,
                   /*contents=*/"", env_, env_, EnvOptions(), /*sync=*/true,
                   /*use_fsync=*/use_fsync_, copy_rate_limiter_,
                   /*size_limit=*/0, stats_);
        fallback_copies.push_back(w.result.get_future());
        engine_->Submit(std::move(w));
      } else {
        fold(link_s);
      }
    }
    for (auto& f : fallback_copies) {
      fold(f.get().io_status);
    }
    return result;
  }

 private:
  struct PendingLink {
    std::future<WorkItemResult> future;
    std::string src;
    std::string dst;
    Temperature temperature = Temperature::kUnknown;
  };

  CopyEngine* engine_;
  Env* env_;
  bool use_link_;
  bool use_fsync_;
  RateLimiter* copy_rate_limiter_;
  Statistics* stats_;
  Logger* info_log_;
  std::vector<std::future<WorkItemResult>> copy_futures_;
  std::vector<PendingLink> link_pendings_;
};
}  // namespace

Status CheckpointEngine::Open(
    const CheckpointEngineOptions& options,
    std::unique_ptr<CheckpointEngine>* out_checkpoint_engine) {
  if (out_checkpoint_engine == nullptr) {
    return Status::InvalidArgument("out_checkpoint_engine must not be null");
  }
  auto engine = std::make_unique<CheckpointEngineImpl>(options);
  engine->Initialize();
  *out_checkpoint_engine = std::move(engine);
  return Status::OK();
}

Status Checkpoint::CreateCheckpoint(const std::string& /*checkpoint_dir*/,
                                    uint64_t /*log_size_for_flush*/,
                                    uint64_t* /*sequence_number_ptr*/) {
  return Status::NotSupported("");
}

Status Checkpoint::CreateCheckpoint(
    const std::string& /*checkpoint_dir*/,
    const std::vector<ColumnFamilyHandle*>& /*column_families*/,
    uint64_t /*log_size_for_flush*/, uint64_t* /*sequence_number_ptr*/) {
  return Status::NotSupported("");
}

Status CheckpointImpl::CleanStagingDirectory(
    const std::string& full_private_path, Logger* info_log) {
  std::vector<std::string> subchildren;
  Status s = db_->GetEnv()->FileExists(full_private_path);
  if (s.IsNotFound()) {
    // Nothing to clean
    return Status::OK();
  } else if (!s.ok()) {
    return s;
  }
  assert(s.ok());
  ROCKS_LOG_INFO(info_log, "File exists %s -- %s", full_private_path.c_str(),
                 s.ToString().c_str());

  s = db_->GetEnv()->GetChildren(full_private_path, &subchildren);
  if (s.ok()) {
    for (auto& subchild : subchildren) {
      Status del_s;
      std::string subchild_path = full_private_path;
      subchild_path.append("/").append(subchild);
      del_s = db_->GetEnv()->DeleteFile(subchild_path);
      ROCKS_LOG_INFO(info_log, "Delete file %s -- %s", subchild_path.c_str(),
                     del_s.ToString().c_str());
      if (!del_s.ok() && s.ok()) {
        s = del_s;
      }
    }
  }

  // Then delete the private dir
  if (s.ok()) {
    s = db_->GetEnv()->DeleteDir(full_private_path);
    ROCKS_LOG_INFO(info_log, "Delete dir %s -- %s", full_private_path.c_str(),
                   s.ToString().c_str());
  }
  return s;
}

Status Checkpoint::ExportColumnFamily(
    ColumnFamilyHandle* /*handle*/, const std::string& /*export_dir*/,
    ExportImportFilesMetaData** /*metadata*/) {
  return Status::NotSupported("");
}

// Builds an openable snapshot of RocksDB
Status CheckpointImpl::CreateCheckpoint(const std::string& checkpoint_dir,
                                        uint64_t log_size_for_flush,
                                        uint64_t* sequence_number_ptr) {
  return CreateCheckpointImpl(checkpoint_dir, log_size_for_flush,
                              sequence_number_ptr, /*engine=*/nullptr,
                              /*use_link=*/true, /*copy_rate_limiter=*/nullptr);
}

Status CheckpointImpl::ResolveIncludeColumnFamilyIds(
    DB* db, const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<uint32_t>* include_cf_ids) {
  assert(include_cf_ids != nullptr);
  include_cf_ids->clear();
  // Empty list means "all column families" -- identical to the whole-DB API.
  if (column_families.empty()) {
    return Status::OK();
  }

  // Reject handles that do not belong to this DB. handle->GetID() alone would
  // silently coerce a foreign CF id into whatever CF in this DB happens to
  // share that id, producing a wrong-data subset checkpoint.
  DBImpl* this_db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());
  std::unordered_set<uint32_t> id_set;
  for (auto* handle : column_families) {
    if (handle == nullptr) {
      return Status::InvalidArgument("Null column family handle");
    }
    auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(handle);
    if (cfh->db() != this_db_impl) {
      return Status::InvalidArgument(
          "Column family handle does not belong to this DB");
    }
    if (cfh->cfd() != nullptr && cfh->cfd()->IsDropped()) {
      return Status::InvalidArgument(
          "Column family handle refers to a dropped column family");
    }
    id_set.insert(handle->GetID());
  }
  // The default column family cannot be dropped and must exist in every
  // openable DB, so it is always included and never eligible for exclusion.
  id_set.insert(db->DefaultColumnFamily()->GetID());

  include_cf_ids->assign(id_set.begin(), id_set.end());
  return Status::OK();
}

Status CheckpointImpl::CreateCheckpoint(
    const std::string& checkpoint_dir,
    const std::vector<ColumnFamilyHandle*>& column_families,
    uint64_t log_size_for_flush, uint64_t* sequence_number_ptr) {
  std::vector<uint32_t> include_cf_ids;
  Status s =
      ResolveIncludeColumnFamilyIds(db_, column_families, &include_cf_ids);
  if (!s.ok()) {
    return s;
  }
  return CreateCheckpointImpl(checkpoint_dir, log_size_for_flush,
                              sequence_number_ptr, /*engine=*/nullptr,
                              /*use_link=*/true, /*copy_rate_limiter=*/nullptr,
                              include_cf_ids);
}

Status CheckpointImpl::CreateCheckpointImpl(
    const std::string& checkpoint_dir, uint64_t log_size_for_flush,
    uint64_t* sequence_number_ptr, CopyEngine* engine, bool use_link,
    RateLimiter* copy_rate_limiter,
    const std::vector<uint32_t>& include_cf_ids) {
  DBOptions db_options = db_->GetDBOptions();
  Env* env = db_->GetEnv();
  const auto& fs = db_->GetFileSystem();

  Status file_exists_s = env->FileExists(checkpoint_dir);
  if (file_exists_s.ok()) {
    return Status::InvalidArgument("Directory exists");
  } else if (!file_exists_s.IsNotFound()) {
    assert(file_exists_s.IsIOError());
    return file_exists_s;
  } else {
    assert(file_exists_s.IsNotFound());
  };

  Status s;
  ROCKS_LOG_INFO(
      db_options.info_log,
      "Started the snapshot process -- creating snapshot in directory %s",
      checkpoint_dir.c_str());

  size_t final_nonslash_idx = checkpoint_dir.find_last_not_of('/');
  if (final_nonslash_idx == std::string::npos) {
    // npos means it's only slashes or empty. Non-empty means it's the root
    // directory, but it shouldn't be because we verified above the directory
    // doesn't exist.
    assert(checkpoint_dir.empty());
    s.PermitUncheckedError();
    return Status::InvalidArgument("invalid checkpoint directory name");
  }

  std::string full_private_path =
      checkpoint_dir.substr(0, final_nonslash_idx + 1) + ".tmp";
  ROCKS_LOG_INFO(db_options.info_log,
                 "Snapshot process -- using temporary directory %s",
                 full_private_path.c_str());

  s = CleanStagingDirectory(full_private_path, db_options.info_log.get());
  if (!s.ok()) {
    return Status::Aborted(
        "Failed to clean the temporary directory " + full_private_path +
        " needed before checkpoint creation : " + s.ToString());
  }

  std::unique_ptr<CheckpointFileMover> mover;
  if (engine == nullptr) {
    mover = std::make_unique<SerialFileMover>(fs, db_options.use_fsync,
                                              db_options.info_log.get());
  } else {
    mover = std::make_unique<ParallelFileMover>(
        engine, env, use_link, db_options.use_fsync, copy_rate_limiter,
        db_options.statistics.get(), db_options.info_log.get());
  }

  // create snapshot directory
  s = env->CreateDir(full_private_path);
  uint64_t sequence_number = 0;
  // Populated by CreateCustomCheckpoint when include_cf_ids restricts the
  // checkpoint to a subset of column families.
  std::vector<uint32_t> excluded_cf_ids;
  SubsetCheckpointOptionsSnapshot options_snapshot;
  std::string manifest_relative_filename;
  uint64_t manifest_size = 0;
  if (s.ok()) {
    // enable file deletions
    s = db_->DisableFileDeletions();
    const bool disabled_file_deletions = s.ok();

    if (s.ok() || s.IsNotSupported()) {
      s = CreateCustomCheckpoint(
          [&](const std::string& src_dirname, const std::string& fname,
              FileType, const Temperature temperature) -> Status {
            return mover->Link(src_dirname + "/" + fname,
                               full_private_path + "/" + fname, temperature);
          } /* link_file_cb */,
          [&](const std::string& src_dirname, const std::string& fname,
              uint64_t size_limit_bytes, FileType,
              const std::string& /* checksum_func_name */,
              const std::string& /* checksum_val */,
              const Temperature temperature) -> Status {
            return mover->Copy(src_dirname + "/" + fname,
                               full_private_path + "/" + fname,
                               size_limit_bytes, temperature);
          } /* copy_file_cb */,
          [&](const std::string& fname, const std::string& contents, FileType) {
            ROCKS_LOG_INFO(db_options.info_log, "Creating %s", fname.c_str());
            return CreateFile(fs, full_private_path + "/" + fname, contents,
                              db_options.use_fsync);
          } /* create_file_cb */,
          &sequence_number, log_size_for_flush, &options_snapshot,
          /*get_live_table_checksum=*/false, /*atomic_flush=*/false,
          include_cf_ids, &excluded_cf_ids, &manifest_relative_filename,
          &manifest_size);

      // Await any deferred work and fold in the first error before committing.
      Status finish_s = mover->Finish();
      if (s.ok()) {
        s = finish_s;
      } else {
        finish_s.PermitUncheckedError();
      }

      // All files (including the copied MANIFEST) are now materialized in the
      // staging dir. If a subset of column families was requested, record the
      // excluded CFs as dropped in the copied MANIFEST so it is consistent with
      // the reduced set of SST/blob files and opens under paranoid_checks.
      if (s.ok() && !excluded_cf_ids.empty()) {
        if (manifest_relative_filename.empty()) {
          s = Status::Corruption(
              "Subset checkpoint did not record a MANIFEST descriptor entry");
        } else {
          DBImpl* db_impl = static_cast_with_check<DBImpl>(db_->GetRootDB());
          s = db_impl->CreateOptionsFileForSubsetCheckpoint(full_private_path,
                                                            options_snapshot);
          if (s.ok()) {
            s = db_impl->AppendColumnFamilyDropsToManifest(
                full_private_path + "/" + manifest_relative_filename,
                manifest_size, excluded_cf_ids,
                options_snapshot.commit_options_file_number
                    ? options_snapshot.options_file_number
                    : 0);
          }
        }
      }

      // we copied all the files, enable file deletions
      if (disabled_file_deletions) {
        Status ss = db_->EnableFileDeletions();
        assert(ss.ok());
        ss.PermitUncheckedError();
      }
    }
  }

  if (s.ok()) {
    // move tmp private backup to real snapshot directory
    Status rename_s = env->RenameFile(full_private_path, checkpoint_dir);
    if (!rename_s.ok()) {
      s = rename_s;
    }
  }
  if (s.ok()) {
    std::unique_ptr<FSDirectory> checkpoint_directory;
    IOStatus new_dir_io_s = fs->NewDirectory(checkpoint_dir, IOOptions(),
                                             &checkpoint_directory, nullptr);
    if (new_dir_io_s.ok() && checkpoint_directory != nullptr) {
      IOStatus fsync_io_s = checkpoint_directory->FsyncWithDirOptions(
          IOOptions(), nullptr,
          DirFsyncOptions(DirFsyncOptions::FsyncReason::kDirRenamed));
      if (!fsync_io_s.ok()) {
        s = fsync_io_s;
      }
    } else if (!new_dir_io_s.ok()) {
      s = new_dir_io_s;
    }
  }

  if (s.ok()) {
    if (sequence_number_ptr != nullptr) {
      *sequence_number_ptr = sequence_number;
    }
    // here we know that we succeeded and installed the new snapshot
    ROCKS_LOG_INFO(db_options.info_log, "Snapshot DONE. All is good");
    ROCKS_LOG_INFO(db_options.info_log, "Snapshot sequence number: %" PRIu64,
                   sequence_number);
  } else {
    ROCKS_LOG_INFO(db_options.info_log, "Snapshot failed -- %s",
                   s.ToString().c_str());
    // clean all the files and directory we might have created
    Status del_s =
        CleanStagingDirectory(full_private_path, db_options.info_log.get());
    ROCKS_LOG_INFO(db_options.info_log,
                   "Clean files or directory we might have created %s: %s",
                   full_private_path.c_str(), del_s.ToString().c_str());
    del_s.PermitUncheckedError();
  }
  return s;
}

Status CheckpointImpl::CreateCustomCheckpoint(
    const std::function<Status(const std::string& src_dirname,
                               const std::string& src_fname, FileType type,
                               const Temperature temperature)>& link_file_cb,
    const std::function<Status(
        const std::string& src_dirname, const std::string& src_fname,
        uint64_t size_limit_bytes, FileType type,
        const std::string& checksum_func_name, const std::string& checksum_val,
        const Temperature temperature)>& copy_file_cb,
    const std::function<Status(const std::string& fname,
                               const std::string& contents, FileType type)>&
        create_file_cb,
    uint64_t* sequence_number, uint64_t log_size_for_flush,
    bool get_live_table_checksum, bool atomic_flush,
    const std::vector<uint32_t>& include_cf_ids,
    std::vector<uint32_t>* excluded_cf_ids,
    std::string* manifest_relative_filename, uint64_t* manifest_size) {
  return CreateCustomCheckpoint(link_file_cb, copy_file_cb, create_file_cb,
                                sequence_number, log_size_for_flush,
                                /*options_snapshot=*/nullptr,
                                get_live_table_checksum, atomic_flush,
                                include_cf_ids, excluded_cf_ids,
                                manifest_relative_filename, manifest_size);
}

Status CheckpointImpl::CreateCustomCheckpoint(
    const std::function<Status(const std::string& src_dirname,
                               const std::string& src_fname, FileType type,
                               const Temperature temperature)>& link_file_cb,
    const std::function<Status(
        const std::string& src_dirname, const std::string& src_fname,
        uint64_t size_limit_bytes, FileType type,
        const std::string& checksum_func_name, const std::string& checksum_val,
        const Temperature temperature)>& copy_file_cb,
    const std::function<Status(const std::string& fname,
                               const std::string& contents, FileType type)>&
        create_file_cb,
    uint64_t* sequence_number, uint64_t log_size_for_flush,
    SubsetCheckpointOptionsSnapshot* options_snapshot,
    bool get_live_table_checksum, bool atomic_flush,
    const std::vector<uint32_t>& include_cf_ids,
    std::vector<uint32_t>* excluded_cf_ids,
    std::string* manifest_relative_filename, uint64_t* manifest_size) {
  *sequence_number = db_->GetLatestSequenceNumber();

  LiveFilesStorageInfoOptions opts;
  opts.include_checksum_info = get_live_table_checksum;
  opts.wal_size_for_flush = log_size_for_flush;
  opts.atomic_flush = atomic_flush;

  std::vector<LiveFileStorageInfo> infos;
  {
    Status s;
    if (include_cf_ids.empty()) {
      s = db_->GetLiveFilesStorageInfo(opts, &infos);
    } else {
      assert(excluded_cf_ids != nullptr);
      assert(options_snapshot != nullptr);
      DBImpl* db_impl = static_cast_with_check<DBImpl>(db_->GetRootDB());
      s = db_impl->GetLiveFilesStorageInfoForSubsetCheckpoint(
          opts, include_cf_ids, &infos, excluded_cf_ids, options_snapshot);
    }
    if (!s.ok()) {
      return s;
    }
  }

  // Verify that everything except WAL files are in same directory
  // (db_paths / cf_paths not supported)
  std::unordered_set<std::string> dirs;
  for (auto& info : infos) {
    if (info.file_type != kWalFile) {
      dirs.insert(info.directory);
    }
  }
  if (dirs.size() > 1) {
    return Status::NotSupported(
        "db_paths / cf_paths not supported for Checkpoint nor BackupEngine");
  }

  bool same_fs = true;

  for (auto& info : infos) {
    Status s;
    // Record the MANIFEST location/size so the caller can post-process it
    // (append CF-drop records) after all files are materialized.
    if (info.file_type == kDescriptorFile) {
      if (manifest_relative_filename != nullptr) {
        assert(manifest_relative_filename->empty());
        *manifest_relative_filename = info.relative_filename;
      }
      if (manifest_size != nullptr) {
        *manifest_size = info.size;
      }
    }
    if (!info.replacement_contents.empty()) {
      // Currently should only be used for CURRENT file.
      assert(info.file_type == kCurrentFile);

      if (info.size != info.replacement_contents.size()) {
        s = Status::Corruption("Inconsistent size metadata for " +
                               info.relative_filename);
      } else {
        s = create_file_cb(info.relative_filename, info.replacement_contents,
                           info.file_type);
      }
    } else {
      if (same_fs && !info.trim_to_size) {
        s = link_file_cb(info.directory, info.relative_filename, info.file_type,
                         info.temperature);
        if (s.IsNotSupported()) {
          same_fs = false;
          s = Status::OK();
        }
        s.MustCheck();
      }
      if (!same_fs || info.trim_to_size) {
        assert(info.file_checksum_func_name.empty() ==
               !opts.include_checksum_info);
        // no assertion on file_checksum because empty is used for both "not
        // set" and "unknown"
        if (opts.include_checksum_info) {
          s = copy_file_cb(info.directory, info.relative_filename, info.size,
                           info.file_type, info.file_checksum_func_name,
                           info.file_checksum, info.temperature);
        } else {
          s = copy_file_cb(info.directory, info.relative_filename, info.size,
                           info.file_type, kUnknownFileChecksumFuncName,
                           kUnknownFileChecksum, info.temperature);
        }
      }
    }
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

// Exports all live SST files of a specified Column Family onto export_dir,
// returning SST files information in metadata.
Status CheckpointImpl::ExportColumnFamily(
    ColumnFamilyHandle* handle, const std::string& export_dir,
    ExportImportFilesMetaData** metadata) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(handle);
  const auto cf_name = cfh->GetName();
  const auto db_options = db_->GetDBOptions();

  assert(metadata != nullptr);
  assert(*metadata == nullptr);
  auto s = db_->GetEnv()->FileExists(export_dir);
  if (s.ok()) {
    return Status::InvalidArgument("Specified export_dir exists");
  } else if (!s.IsNotFound()) {
    assert(s.IsIOError());
    return s;
  }

  const auto final_nonslash_idx = export_dir.find_last_not_of('/');
  if (final_nonslash_idx == std::string::npos) {
    return Status::InvalidArgument("Specified export_dir invalid");
  }
  ROCKS_LOG_INFO(db_options.info_log,
                 "[%s] export column family onto export directory %s",
                 cf_name.c_str(), export_dir.c_str());

  // Create a temporary export directory.
  const auto tmp_export_dir =
      export_dir.substr(0, final_nonslash_idx + 1) + ".tmp";
  s = db_->GetEnv()->CreateDir(tmp_export_dir);

  if (s.ok()) {
    // FIXME: should respect atomic_flush and flush all CFs if needed.
    s = db_->Flush(ROCKSDB_NAMESPACE::FlushOptions(), handle);
  }

  ColumnFamilyMetaData db_metadata;
  if (s.ok()) {
    // Export live sst files with file deletions disabled.
    s = db_->DisableFileDeletions();
    if (s.ok()) {
      db_->GetColumnFamilyMetaData(handle, &db_metadata);

      s = ExportFilesInMetaData(
          db_options, db_metadata,
          [&](const std::string& src_dirname, const std::string& fname) {
            ROCKS_LOG_INFO(db_options.info_log, "[%s] HardLinking %s",
                           cf_name.c_str(), fname.c_str());
            return db_->GetEnv()->LinkFile(src_dirname + fname,
                                           tmp_export_dir + fname);
          } /*link_file_cb*/,
          [&](const std::string& src_dirname, const std::string& fname) {
            ROCKS_LOG_INFO(db_options.info_log, "[%s] Copying %s",
                           cf_name.c_str(), fname.c_str());
            // FIXME: temperature handling
            return CopyFile(db_->GetFileSystem(), src_dirname + fname,
                            Temperature::kUnknown, tmp_export_dir + fname,
                            Temperature::kUnknown, 0, db_options.use_fsync,
                            nullptr);
          } /*copy_file_cb*/);

      const auto enable_status = db_->EnableFileDeletions();
      if (s.ok()) {
        s = enable_status;
      }
    }
  }

  auto moved_to_user_specified_dir = false;
  if (s.ok()) {
    // Move temporary export directory to the actual export directory.
    s = db_->GetEnv()->RenameFile(tmp_export_dir, export_dir);
  }

  if (s.ok()) {
    // Fsync export directory.
    moved_to_user_specified_dir = true;
    std::unique_ptr<FSDirectory> dir_ptr;
    s = db_->GetFileSystem()->NewDirectory(export_dir, IOOptions(), &dir_ptr,
                                           nullptr);
    if (s.ok()) {
      assert(dir_ptr != nullptr);
      s = dir_ptr->FsyncWithDirOptions(
          IOOptions(), nullptr,
          DirFsyncOptions(DirFsyncOptions::FsyncReason::kDirRenamed));
    }
  }

  if (s.ok()) {
    // Export of files succeeded. Fill in the metadata information.
    auto result_metadata = new ExportImportFilesMetaData();
    result_metadata->db_comparator_name = handle->GetComparator()->Name();
    for (const auto& level_metadata : db_metadata.levels) {
      for (const auto& file_metadata : level_metadata.files) {
        LiveFileMetaData live_file_metadata;
        live_file_metadata.size = file_metadata.size;
        live_file_metadata.name = file_metadata.name;
        live_file_metadata.file_number = file_metadata.file_number;
        live_file_metadata.db_path = export_dir;
        live_file_metadata.smallest_seqno = file_metadata.smallest_seqno;
        live_file_metadata.largest_seqno = file_metadata.largest_seqno;
        live_file_metadata.smallestkey = file_metadata.smallestkey;
        live_file_metadata.largestkey = file_metadata.largestkey;
        live_file_metadata.oldest_blob_file_number =
            file_metadata.oldest_blob_file_number;
        live_file_metadata.oldest_ancestor_time =
            file_metadata.oldest_ancestor_time;
        live_file_metadata.file_creation_time =
            file_metadata.file_creation_time;
        live_file_metadata.db_ownership_start_time =
            file_metadata.db_ownership_start_time;
        live_file_metadata.data_file_provenance =
            file_metadata.data_file_provenance;
        live_file_metadata.epoch_number = file_metadata.epoch_number;
        live_file_metadata.level = level_metadata.level;
        live_file_metadata.smallest = file_metadata.smallest;
        live_file_metadata.largest = file_metadata.largest;
        result_metadata->files.push_back(live_file_metadata);
      }
    }
    *metadata = result_metadata;
    ROCKS_LOG_INFO(db_options.info_log, "[%s] Export succeeded.",
                   cf_name.c_str());
  } else {
    // Failure: Clean up all the files/directories created.
    ROCKS_LOG_INFO(db_options.info_log, "[%s] Export failed. %s",
                   cf_name.c_str(), s.ToString().c_str());
    std::vector<std::string> subchildren;
    const auto cleanup_dir =
        moved_to_user_specified_dir ? export_dir : tmp_export_dir;
    const auto children_status =
        db_->GetEnv()->GetChildren(cleanup_dir, &subchildren);
    if (!children_status.ok()) {
      ROCKS_LOG_WARN(db_options.info_log, "Failed to list dir %s: %s",
                     cleanup_dir.c_str(), children_status.ToString().c_str());
    }
    for (const auto& subchild : subchildren) {
      std::string subchild_path = cleanup_dir;
      subchild_path.append("/").append(subchild);
      const auto status = db_->GetEnv()->DeleteFile(subchild_path);
      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options.info_log, "Failed to cleanup file %s: %s",
                       subchild_path.c_str(), status.ToString().c_str());
      }
    }
    const auto status = db_->GetEnv()->DeleteDir(cleanup_dir);
    if (!status.ok()) {
      ROCKS_LOG_WARN(db_options.info_log, "Failed to cleanup dir %s: %s",
                     cleanup_dir.c_str(), status.ToString().c_str());
    }
  }
  return s;
}

Status CheckpointImpl::ExportFilesInMetaData(
    const DBOptions& db_options, const ColumnFamilyMetaData& metadata,
    const std::function<Status(const std::string& src_dirname,
                               const std::string& src_fname)>& link_file_cb,
    const std::function<Status(const std::string& src_dirname,
                               const std::string& src_fname)>& copy_file_cb) {
  Status s;
  auto hardlink_file = true;

  // Copy/hard link files in metadata.
  size_t num_files = 0;
  for (const auto& level_metadata : metadata.levels) {
    for (const auto& file_metadata : level_metadata.files) {
      uint64_t number;
      FileType type;
      const auto ok = ParseFileName(file_metadata.name, &number, &type);
      if (!ok) {
        s = Status::Corruption("Could not parse file name");
        break;
      }

      // We should only get sst files here.
      assert(type == kTableFile);
      assert(file_metadata.size > 0 && file_metadata.name[0] == '/');
      const auto src_fname = file_metadata.name;
      ++num_files;

      if (hardlink_file) {
        s = link_file_cb(db_->GetName(), src_fname);
        if (num_files == 1 && s.IsNotSupported()) {
          // Fallback to copy if link failed due to cross-device directories.
          hardlink_file = false;
          s = Status::OK();
        }
      }
      if (!hardlink_file) {
        s = copy_file_cb(db_->GetName(), src_fname);
      }
      if (!s.ok()) {
        break;
      }
    }
  }
  ROCKS_LOG_INFO(db_options.info_log, "Number of table files %" ROCKSDB_PRIszt,
                 num_files);

  return s;
}
}  // namespace ROCKSDB_NAMESPACE
