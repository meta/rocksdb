//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <atomic>
#include <cinttypes>
#include <cstdlib>
#include <exception>
#include <functional>
#include <future>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "env/composite_env_wrapper.h"
#include "env/fs_readonly.h"
#include "env/fs_remap.h"
#include "file/filename.h"
#include "file/line_file_reader.h"
#include "file/sequence_file_reader.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "monitoring/iostats_context_imp.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/env.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/statistics.h"
#include "rocksdb/wal_iterator.h"
#include "table/sst_file_dumper.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/channel.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/file_checksum_helper.h"
#include "util/math.h"
#include "util/rate_limiter_impl.h"
#include "util/string_util.h"
#include "utilities/backup/backup_engine_impl.h"
#include "utilities/checkpoint/checkpoint_impl.h"
#include "utilities/copy_engine/copy_engine.h"

namespace ROCKSDB_NAMESPACE {

namespace {
using ShareFilesNaming = BackupEngineOptions::ShareFilesNaming;

constexpr BackupID kLatestBackupIDMarker = static_cast<BackupID>(-2);

inline uint32_t ChecksumHexToInt32(const std::string& checksum_hex) {
  std::string checksum_str;
  Slice(checksum_hex).DecodeHex(&checksum_str);
  return EndianSwapValue(DecodeFixed32(checksum_str.c_str()));
}
inline std::string ChecksumStrToHex(const std::string& checksum_str) {
  return Slice(checksum_str).ToString(true);
}

const std::string kPrivateDirName = "private";
const std::string kMetaDirName = "meta";
const std::string kSharedDirName = "shared";
const std::string kSharedChecksumDirName = "shared_checksum";
const std::string kPrivateDirSlash = kPrivateDirName + "/";
const std::string kMetaDirSlash = kMetaDirName + "/";
const std::string kSharedDirSlash = kSharedDirName + "/";
const std::string kSharedChecksumDirSlash = kSharedChecksumDirName + "/";

}  // namespace

void BackupStatistics::IncrementNumberSuccessBackup() {
  number_success_backup++;
}
void BackupStatistics::IncrementNumberFailBackup() { number_fail_backup++; }

uint32_t BackupStatistics::GetNumberSuccessBackup() const {
  return number_success_backup;
}
uint32_t BackupStatistics::GetNumberFailBackup() const {
  return number_fail_backup;
}

std::string BackupStatistics::ToString() const {
  char result[50];
  snprintf(result, sizeof(result), "# success backup: %u, # fail backup: %u",
           GetNumberSuccessBackup(), GetNumberFailBackup());
  return result;
}

void BackupEngineOptions::Dump(Logger* logger) const {
  ROCKS_LOG_INFO(logger, "               Options.backup_dir: %s",
                 backup_dir.c_str());
  ROCKS_LOG_INFO(logger, "               Options.backup_env: %p", backup_env);
  ROCKS_LOG_INFO(logger, "        Options.share_table_files: %d",
                 static_cast<int>(share_table_files));
  ROCKS_LOG_INFO(logger, "                 Options.info_log: %p", info_log);
  ROCKS_LOG_INFO(logger, "                     Options.sync: %d",
                 static_cast<int>(sync));
  ROCKS_LOG_INFO(logger, "         Options.destroy_old_data: %d",
                 static_cast<int>(destroy_old_data));
  ROCKS_LOG_INFO(logger, "         Options.backup_log_files: %d",
                 static_cast<int>(backup_log_files));
  ROCKS_LOG_INFO(logger, "        Options.backup_rate_limit: %" PRIu64,
                 backup_rate_limit);
  ROCKS_LOG_INFO(logger, "       Options.restore_rate_limit: %" PRIu64,
                 restore_rate_limit);
  ROCKS_LOG_INFO(logger, "Options.max_background_operations: %d",
                 max_background_operations);
}

namespace {
// -------- BackupEngineImpl class ---------
class BackupEngineImpl {
 public:
  BackupEngineImpl(const BackupEngineOptions& options, Env* db_env,
                   bool read_only = false);
  ~BackupEngineImpl();

  // No copying allowed
  BackupEngineImpl(const BackupEngineImpl&) = delete;
  BackupEngineImpl& operator=(const BackupEngineImpl&) = delete;
  BackupEngineImpl(BackupEngineImpl&&) = delete;
  BackupEngineImpl& operator=(BackupEngineImpl&&) = delete;

  IOStatus CreateNewBackupWithMetadata(const CreateBackupOptions& options,
                                       DB* db, const std::string& app_metadata,
                                       BackupID* new_backup_id_ptr);

  IOStatus PurgeOldBackups(uint32_t num_backups_to_keep);

  IOStatus DeleteBackup(BackupID backup_id);

  void StopBackup() { stop_backup_.store(true, std::memory_order_release); }

  IOStatus GarbageCollect();

  // The returned BackupInfos are in chronological order, which means the
  // latest backup comes last.
  void GetBackupInfo(std::vector<BackupInfo>* backup_info,
                     bool include_file_details) const;

  Status GetBackupInfo(BackupID backup_id, BackupInfo* backup_info,
                       bool include_file_details = false) const;

  void GetCorruptedBackups(std::vector<BackupID>* corrupt_backup_ids) const;

  IOStatus RestoreDBFromBackup(
      const RestoreOptions& options, BackupID backup_id,
      const std::string& db_dir, const std::string& wal_dir,
      const std::list<const BackupEngineImpl*>& locked_restore_from_dirs) const;

  IOStatus VerifyBackup(BackupID backup_id,
                        bool verify_with_checksum = false) const;

  IOStatus Initialize();

  ShareFilesNaming GetNamingNoFlags() const {
    return options_.share_files_with_checksum_naming &
           BackupEngineOptions::kMaskNoNamingFlags;
  }
  ShareFilesNaming GetNamingFlags() const {
    return options_.share_files_with_checksum_naming &
           BackupEngineOptions::kMaskNamingFlags;
  }

  void TEST_SetDefaultRateLimitersClock(
      const std::shared_ptr<SystemClock>& backup_rate_limiter_clock,
      const std::shared_ptr<SystemClock>& restore_rate_limiter_clock) {
    if (backup_rate_limiter_clock) {
      static_cast<GenericRateLimiter*>(options_.backup_rate_limiter.get())
          ->TEST_SetClock(backup_rate_limiter_clock);
    }

    if (restore_rate_limiter_clock) {
      static_cast<GenericRateLimiter*>(options_.restore_rate_limiter.get())
          ->TEST_SetClock(restore_rate_limiter_clock);
    }
  }

 private:
  void DeleteChildren(const std::string& dir,
                      const std::unordered_set<uint64_t>& files_to_keep,
                      uint32_t file_type_filter = 0) const;
  IOStatus DeleteBackupNoGC(BackupID backup_id);

  // Extends the "result" map with pathname->size mappings for the contents of
  // "dir" in "env". Pathnames are prefixed with "dir".
  IOStatus ReadChildFileCurrentSizes(
      const std::string& dir, const std::shared_ptr<FileSystem>&,
      std::unordered_map<std::string, uint64_t>* result) const;

  struct FileInfo {
    FileInfo(const std::string& fname, uint64_t sz, const std::string& checksum,
             const std::string& id, const std::string& sid, Temperature _temp)
        : refs(0),
          filename(fname),
          size(sz),
          checksum_hex(checksum),
          db_id(id),
          db_session_id(sid),
          temp(_temp) {}

    FileInfo(const FileInfo&) = delete;
    FileInfo& operator=(const FileInfo&) = delete;
    FileInfo(FileInfo&&) = delete;
    FileInfo& operator=(FileInfo&&) = delete;

    ~FileInfo() = default;

    int refs;
    // Relative path from backup dir
    const std::string filename;
    const uint64_t size;
    // crc32c checksum as hex. empty == unknown / unavailable
    std::string checksum_hex;
    // DB identities
    // db_id is obtained for potential usage in the future but not used
    // currently
    const std::string db_id;
    // db_session_id appears in the backup SST filename if the table naming
    // option is kUseDbSessionId
    const std::string db_session_id;
    Temperature temp;

    std::string GetDbFileName() const {
      std::string rv;
      // extract the filename part
      size_t slash = filename.find_last_of('/');
      // file will either be shared/<file>, shared_checksum/<file_crc32c_size>,
      // shared_checksum/<file_session>, shared_checksum/<file_crc32c_session>,
      // or private/<number>/<file>
      assert(slash != std::string::npos);
      rv = filename.substr(slash + 1);

      // if the file was in shared_checksum, extract the real file name
      // in this case the file is <number>_<checksum>_<size>.<type>,
      // <number>_<session>.<type>, or <number>_<checksum>_<session>.<type>
      if (filename.substr(0, slash) == kSharedChecksumDirName) {
        rv = GetFileFromChecksumFile(rv);
      }
      return rv;
    }
  };

  static inline std::string WithoutTrailingSlash(const std::string& path) {
    if (path.empty() || path.back() != '/') {
      return path;
    } else {
      return path.substr(path.size() - 1);
    }
  }

  static inline std::string WithTrailingSlash(const std::string& path) {
    if (path.empty() || path.back() != '/') {
      return path + '/';
    } else {
      return path;
    }
  }

  // A filesystem wrapper that makes shared backup files appear to be in the
  // private backup directory (dst_dir), so that the private backup dir can
  // be opened as a read-only DB.
  class RemapSharedFileSystem : public RemapFileSystem {
   public:
    RemapSharedFileSystem(const std::shared_ptr<FileSystem>& base,
                          const std::string& dst_dir,
                          const std::string& src_base_dir,
                          const std::vector<std::shared_ptr<FileInfo>>& files)
        : RemapFileSystem(base),
          dst_dir_(WithoutTrailingSlash(dst_dir)),
          dst_dir_slash_(WithTrailingSlash(dst_dir)),
          src_base_dir_(WithTrailingSlash(src_base_dir)) {
      for (auto& info : files) {
        if (!StartsWith(info->filename, kPrivateDirSlash)) {
          assert(StartsWith(info->filename, kSharedDirSlash) ||
                 StartsWith(info->filename, kSharedChecksumDirSlash));
          remaps_[info->GetDbFileName()] = info;
        }
      }
    }

    const char* Name() const override {
      return "BackupEngineImpl::RemapSharedFileSystem";
    }

    // Sometimes a directory listing is required in opening a DB
    IOStatus GetChildren(const std::string& dir, const IOOptions& options,
                         std::vector<std::string>* result,
                         IODebugContext* dbg) override {
      IOStatus s = RemapFileSystem::GetChildren(dir, options, result, dbg);
      if (s.ok() && (dir == dst_dir_ || dir == dst_dir_slash_)) {
        // Assume remapped files exist
        for (auto& r : remaps_) {
          result->push_back(r.first);
        }
      }
      return s;
    }

    // Sometimes a directory listing is required in opening a DB
    IOStatus GetChildrenFileAttributes(const std::string& dir,
                                       const IOOptions& options,
                                       std::vector<FileAttributes>* result,
                                       IODebugContext* dbg) override {
      IOStatus s =
          RemapFileSystem::GetChildrenFileAttributes(dir, options, result, dbg);
      if (s.ok() && (dir == dst_dir_ || dir == dst_dir_slash_)) {
        // Assume remapped files exist with recorded size
        for (auto& r : remaps_) {
          result->emplace_back();  // clean up with C++20
          FileAttributes& attr = result->back();
          attr.name = r.first;
          attr.size_bytes = r.second->size;
        }
      }
      return s;
    }

   protected:
    // When a file in dst_dir is requested, see if we need to remap to shared
    // file path.
    std::pair<IOStatus, std::string> EncodePath(
        const std::string& path) override {
      if (path.empty() || path[0] != '/') {
        return {IOStatus::InvalidArgument(path, "Not an absolute path"), ""};
      }
      std::pair<IOStatus, std::string> rv{IOStatus(), path};
      if (StartsWith(path, dst_dir_slash_)) {
        std::string relative = path.substr(dst_dir_slash_.size());
        auto it = remaps_.find(relative);
        if (it != remaps_.end()) {
          rv.second = src_base_dir_ + it->second->filename;
        }
      }
      return rv;
    }

   private:
    // Absolute path to a directory that some extra files will be mapped into.
    const std::string dst_dir_;
    // Includes a trailing slash.
    const std::string dst_dir_slash_;
    // Absolute path to a directory containing some files to be mapped into
    // dst_dir_. Includes a trailing slash.
    const std::string src_base_dir_;
    // If remaps_[x] exists, attempt to read dst_dir_ / x should instead read
    // src_base_dir_ / remaps_[x]->filename. FileInfo is used to maximize
    // sharing with other backup data in memory.
    std::unordered_map<std::string, std::shared_ptr<FileInfo>> remaps_;
  };

  class BackupMeta {
   public:
    BackupMeta(
        const std::string& meta_filename, const std::string& meta_tmp_filename,
        std::unordered_map<std::string, std::shared_ptr<FileInfo>>* file_infos,
        Env* env, const std::shared_ptr<FileSystem>& fs)
        : timestamp_(0),
          sequence_number_(0),
          size_(0),
          meta_filename_(meta_filename),
          meta_tmp_filename_(meta_tmp_filename),
          file_infos_(file_infos),
          env_(env),
          fs_(fs) {}

    BackupMeta(const BackupMeta&) = delete;
    BackupMeta& operator=(const BackupMeta&) = delete;
    BackupMeta(BackupMeta&&) = delete;
    BackupMeta& operator=(BackupMeta&&) = delete;

    ~BackupMeta() = default;

    void RecordTimestamp() {
      // Best effort
      Status s = env_->GetCurrentTime(&timestamp_);
      if (!s.ok()) {
        timestamp_ = /* something clearly fabricated */ 1;
      }
    }
    int64_t GetTimestamp() const { return timestamp_; }
    uint64_t GetSize() const { return size_; }
    uint32_t GetNumberFiles() const {
      return static_cast<uint32_t>(files_.size());
    }
    void SetSequenceNumber(uint64_t sequence_number) {
      sequence_number_ = sequence_number;
    }

    const std::string& GetAppMetadata() const { return app_metadata_; }

    void SetAppMetadata(const std::string& app_metadata) {
      app_metadata_ = app_metadata;
    }

    IOStatus AddFile(std::shared_ptr<FileInfo> file_info);

    void AddExcludedFile(const std::string& relative_file) {
      excluded_files_.emplace_back(relative_file);
    }

    IOStatus Delete(bool delete_meta = true);

    bool Empty() const { return files_.empty(); }

    const std::vector<std::shared_ptr<FileInfo>>& GetFiles() const {
      return files_;
    }

    const std::vector<BackupExcludedFileInfo>& GetExcludedFiles() const {
      return excluded_files_;
    }

    // @param abs_path_to_size Pre-fetched file sizes (bytes).
    IOStatus LoadFromFile(
        const std::string& backup_dir,
        const std::unordered_map<std::string, uint64_t>& abs_path_to_size,
        RateLimiter* rate_limiter, Logger* info_log,
        std::unordered_set<std::string>* reported_ignored_fields);
    IOStatus StoreToFile(
        bool sync, int schema_version,
        const TEST_BackupMetaSchemaOptions* schema_test_options);

    std::string GetInfoString() {
      std::ostringstream ss;
      ss << "Timestamp: " << timestamp_ << std::endl;
      char human_size[16];
      AppendHumanBytes(size_, human_size, sizeof(human_size));
      ss << "Size: " << human_size << std::endl;
      ss << "Files:" << std::endl;
      for (const auto& file : files_) {
        AppendHumanBytes(file->size, human_size, sizeof(human_size));
        ss << file->filename << ", size " << human_size << ", refs "
           << file->refs << std::endl;
      }
      return ss.str();
    }

    const std::shared_ptr<Env>& GetEnvForOpen() const {
      if (!env_for_open_) {
        // Lazy initialize
        // Find directories
        std::string dst_dir = meta_filename_;
        auto i = dst_dir.rfind(kMetaDirSlash);
        assert(i != std::string::npos);
        std::string src_base_dir = dst_dir.substr(0, i);
        dst_dir.replace(i, kMetaDirSlash.size(), kPrivateDirSlash);
        // Make the RemapSharedFileSystem
        std::shared_ptr<FileSystem> remap_fs =
            std::make_shared<RemapSharedFileSystem>(fs_, dst_dir, src_base_dir,
                                                    files_);
        // Make it read-only for safety
        remap_fs = std::make_shared<ReadOnlyFileSystem>(remap_fs);
        // Make an Env wrapper
        env_for_open_ = std::make_shared<CompositeEnvWrapper>(env_, remap_fs);
      }
      return env_for_open_;
    }

   private:
    int64_t timestamp_;
    // sequence number is only approximate, should not be used
    // by clients
    uint64_t sequence_number_;
    uint64_t size_;
    std::string app_metadata_;
    std::string const meta_filename_;
    std::string const meta_tmp_filename_;
    // files with relative paths (without "/" prefix!!)
    std::vector<std::shared_ptr<FileInfo>> files_;
    std::vector<BackupExcludedFileInfo> excluded_files_;
    std::unordered_map<std::string, std::shared_ptr<FileInfo>>* file_infos_;
    Env* env_;
    mutable std::shared_ptr<Env> env_for_open_;
    std::shared_ptr<FileSystem> fs_;
    IOOptions iooptions_ = IOOptions();
  };  // BackupMeta

  void SetBackupInfoFromBackupMeta(BackupID id, const BackupMeta& meta,
                                   BackupInfo* backup_info,
                                   bool include_file_details) const;

  // Infers set of existing destination files that could be retained
  // during the incremental restore procedure in accordance with user
  // selected strategy (RestoreMode). File can be retained only if it's
  // deemed to exist in the referenced backup set.
  void InferDBFilesToRetainInRestore(
      const std::vector<std::pair<const BackupEngineImpl*, const FileInfo*>>&
          restore_file_infos,
      std::unordered_set<std::string>& unowned_backups,
      const std::string& db_dir, RestoreOptions::Mode mode,
      std::unordered_set<uint64_t>& files_to_keep) const;

  inline std::string GetAbsolutePath(
      const std::string& relative_path = "") const {
    assert(relative_path.size() == 0 || relative_path[0] != '/');
    return options_.backup_dir + "/" + relative_path;
  }
  inline std::string GetPrivateFileRel(BackupID backup_id, bool tmp = false,
                                       const std::string& file = "") const {
    assert(file.size() == 0 || file[0] != '/');
    return kPrivateDirSlash + std::to_string(backup_id) + (tmp ? ".tmp" : "") +
           "/" + file;
  }
  inline std::string GetSharedFileRel(const std::string& file = "",
                                      bool tmp = false) const {
    assert(file.size() == 0 || file[0] != '/');
    return kSharedDirSlash + std::string(tmp ? "." : "") + file +
           (tmp ? ".tmp" : "");
  }
  inline std::string GetSharedFileWithChecksumRel(const std::string& file = "",
                                                  bool tmp = false) const {
    assert(file.size() == 0 || file[0] != '/');
    return kSharedChecksumDirSlash + std::string(tmp ? "." : "") + file +
           (tmp ? ".tmp" : "");
  }
  inline bool UseLegacyNaming(const std::string& sid) const {
    return GetNamingNoFlags() ==
               BackupEngineOptions::kLegacyCrc32cAndFileSize ||
           sid.empty();
  }
  inline std::string GetSharedFileWithChecksum(
      const std::string& file, const std::string& checksum_hex,
      const uint64_t file_size, const std::string& db_session_id) const {
    assert(file.size() == 0 || file[0] != '/');
    std::string file_copy = file;
    if (UseLegacyNaming(db_session_id)) {
      assert(!checksum_hex.empty());
      file_copy.insert(file_copy.find_last_of('.'),
                       "_" + std::to_string(ChecksumHexToInt32(checksum_hex)) +
                           "_" + std::to_string(file_size));
    } else {
      file_copy.insert(file_copy.find_last_of('.'), "_s" + db_session_id);
      if (GetNamingFlags() & BackupEngineOptions::kFlagIncludeFileSize) {
        file_copy.insert(file_copy.find_last_of('.'),
                         "_" + std::to_string(file_size));
      }
    }
    return file_copy;
  }

  inline std::string GenerateSharedFileWithDbSessionIdAndSize(
      const std::string& file, const uint64_t file_size,
      const std::string& db_session_id) const {
    assert(file.size() == 0 || file[0] != '/');
    std::string file_copy = file;
    file_copy.insert(file_copy.find_last_of('.'), "_s" + db_session_id);
    file_copy.insert(file_copy.find_last_of('.'),
                     "_" + std::to_string(file_size));
    return kSharedChecksumDirSlash + file_copy;
  }

  static inline std::string GetFileFromChecksumFile(const std::string& file) {
    assert(file.size() == 0 || file[0] != '/');
    std::string file_copy = file;
    size_t first_underscore = file_copy.find_first_of('_');
    return file_copy.erase(first_underscore,
                           file_copy.find_last_of('.') - first_underscore);
  }

  inline std::string GetBackupMetaFile(BackupID backup_id, bool tmp) const {
    return GetAbsolutePath(kMetaDirName) + "/" + (tmp ? "." : "") +
           std::to_string(backup_id) + (tmp ? ".tmp" : "");
  }

  // Helper method to check if backup should be stopped. Can be overridden
  // via sync points for testing.
  bool ShouldStopBackup() const;

  // Obtain db_id and db_session_id from the table properties of file_path
  Status GetFileDbIdentities(Env* src_env, const EnvOptions& src_env_options,
                             const std::string& file_path,
                             Temperature file_temp, RateLimiter* rate_limiter,
                             std::string* db_id,
                             std::string* db_session_id) const;

  struct BackupAfterCopyOrCreateWorkItem {
    std::future<WorkItemResult> result;
    bool shared;
    bool needed_to_copy;
    Env* backup_env;
    std::string dst_path_tmp;
    std::string dst_path;
    std::string dst_relative;

    BackupAfterCopyOrCreateWorkItem(
        BackupAfterCopyOrCreateWorkItem&& o) noexcept {
      *this = std::move(o);
    }

    BackupAfterCopyOrCreateWorkItem& operator=(
        BackupAfterCopyOrCreateWorkItem&& o) noexcept {
      result = std::move(o.result);
      shared = o.shared;
      needed_to_copy = o.needed_to_copy;
      backup_env = o.backup_env;
      dst_path_tmp = std::move(o.dst_path_tmp);
      dst_path = std::move(o.dst_path);
      dst_relative = std::move(o.dst_relative);
      return *this;
    }

    ~BackupAfterCopyOrCreateWorkItem() = default;

    BackupAfterCopyOrCreateWorkItem(std::future<WorkItemResult>&& _result,
                                    bool _shared, bool _needed_to_copy,
                                    Env* _backup_env, std::string _dst_path_tmp,
                                    std::string _dst_path,
                                    std::string _dst_relative)
        : result(std::move(_result)),
          shared(_shared),
          needed_to_copy(_needed_to_copy),
          backup_env(_backup_env),
          dst_path_tmp(std::move(_dst_path_tmp)),
          dst_path(std::move(_dst_path)),
          dst_relative(std::move(_dst_relative)) {}
  };

  using BackupWorkItemPair =
      std::pair<WorkItem, BackupAfterCopyOrCreateWorkItem>;

  struct ComputeChecksumWorkItem {
    std::future<WorkItemResult> result;
    std::string file_path;
    uint64_t file_number;

    ComputeChecksumWorkItem(std::future<WorkItemResult>&& _result,
                            const std::string& _file_path,
                            uint64_t _file_number)
        : result(std::move(_result)),
          file_path(_file_path),
          file_number(_file_number) {}

    ComputeChecksumWorkItem(const ComputeChecksumWorkItem&) = delete;
    ComputeChecksumWorkItem& operator=(const ComputeChecksumWorkItem&) = delete;

    ComputeChecksumWorkItem(ComputeChecksumWorkItem&& o) noexcept {
      *this = std::move(o);
    }

    ComputeChecksumWorkItem& operator=(ComputeChecksumWorkItem&& o) noexcept {
      result = std::move(o.result);
      file_path = std::move(o.file_path);
      file_number = o.file_number;
      return *this;
    }

    ~ComputeChecksumWorkItem() = default;
  };

  struct RestoreAfterCopyOrCreateWorkItem {
    std::future<WorkItemResult> result;
    std::string from_file;
    std::string to_file;
    std::string checksum_hex;
    RestoreAfterCopyOrCreateWorkItem(std::future<WorkItemResult>&& _result,
                                     const std::string& _from_file,
                                     const std::string& _to_file,
                                     const std::string& _checksum_hex)
        : result(std::move(_result)),
          from_file(_from_file),
          to_file(_to_file),
          checksum_hex(_checksum_hex) {}
  };

  bool initialized_;
  std::unique_ptr<CopyEngine> copy_engine_;

  // Certain operations like PurgeOldBackups and DeleteBackup will trigger
  // automatic GarbageCollect (true) unless we've already done one in this
  // session and have not failed to delete backup files since then (false).
  bool might_need_garbage_collect_ = true;

  // Adds a file to the backup work queue to be copied or created if it doesn't
  // already exist.
  //
  // Exactly one of src_dir and contents must be non-empty.
  //
  // @param src_dir If non-empty, the file in this directory named fname will be
  //    copied.
  // @param fname Name of destination file and, in case of copy, source file.
  // @param contents If non-empty, the file will be created with these contents.
  IOStatus AddBackupFileWorkItem(
      std::unordered_set<std::string>& live_dst_paths,
      std::deque<BackupAfterCopyOrCreateWorkItem>& backup_items_to_finish,
      std::deque<BackupWorkItemPair>* excludable_items, BackupID backup_id,
      bool shared, const std::string& src_dir,
      const std::string& fname,  // starts with "/"
      const EnvOptions& src_env_options, RateLimiter* rate_limiter,
      FileType file_type, uint64_t size_bytes, Statistics* stats,
      uint64_t size_limit = 0, bool shared_checksum = false,
      std::function<void()> progress_callback = {},
      const std::string& contents = std::string(),
      const std::string& src_checksum_func_name = kUnknownFileChecksumFuncName,
      const std::string& src_checksum_str = kUnknownFileChecksum,
      const Temperature src_temperature = Temperature::kUnknown);

  // backup state data
  BackupID latest_backup_id_;
  BackupID latest_valid_backup_id_;
  std::map<BackupID, std::unique_ptr<BackupMeta>> backups_;
  std::map<BackupID, std::pair<IOStatus, std::unique_ptr<BackupMeta>>>
      corrupt_backups_;
  std::unordered_map<std::string, std::shared_ptr<FileInfo>>
      backuped_file_infos_;
  std::atomic<bool> stop_backup_;

  // options data
  BackupEngineOptions options_;
  Env* db_env_;
  Env* backup_env_;

  // directories
  std::unique_ptr<FSDirectory> backup_directory_;
  std::unique_ptr<FSDirectory> shared_directory_;
  std::unique_ptr<FSDirectory> meta_directory_;
  std::unique_ptr<FSDirectory> private_directory_;

  bool read_only_;
  BackupStatistics backup_statistics_;
  std::unordered_set<std::string> reported_ignored_fields_;
  static const size_t kMaxAppMetaSize = 1024 * 1024;  // 1MB
  std::shared_ptr<FileSystem> db_fs_;
  std::shared_ptr<FileSystem> backup_fs_;
  IOOptions io_options_ = IOOptions();

 public:
  std::unique_ptr<TEST_BackupMetaSchemaOptions> schema_test_options_;
};

// -------- BackupEngineImplThreadSafe class ---------
// This locking layer for thread safety in the public API is layered on
// top to prevent accidental recursive locking with RWMutex, which is UB.
// Note: BackupEngineReadOnlyBase inherited twice, but has no fields
class BackupEngineImplThreadSafe : public BackupEngine,
                                   public BackupEngineReadOnly {
 public:
  BackupEngineImplThreadSafe(const BackupEngineOptions& options, Env* db_env,
                             bool read_only = false)
      : impl_(options, db_env, read_only) {}
  ~BackupEngineImplThreadSafe() override = default;

  using BackupEngine::CreateNewBackupWithMetadata;
  IOStatus CreateNewBackupWithMetadata(const CreateBackupOptions& options,
                                       DB* db, const std::string& app_metadata,
                                       BackupID* new_backup_id) override {
    WriteLock lock(&mutex_);
    return impl_.CreateNewBackupWithMetadata(options, db, app_metadata,
                                             new_backup_id);
  }

  IOStatus PurgeOldBackups(uint32_t num_backups_to_keep) override {
    WriteLock lock(&mutex_);
    return impl_.PurgeOldBackups(num_backups_to_keep);
  }

  IOStatus DeleteBackup(BackupID backup_id) override {
    WriteLock lock(&mutex_);
    return impl_.DeleteBackup(backup_id);
  }

  void StopBackup() override {
    // No locking needed
    impl_.StopBackup();
  }

  IOStatus GarbageCollect() override {
    WriteLock lock(&mutex_);
    return impl_.GarbageCollect();
  }

  Status GetLatestBackupInfo(BackupInfo* backup_info,
                             bool include_file_details = false) const override {
    ReadLock lock(&mutex_);
    return impl_.GetBackupInfo(kLatestBackupIDMarker, backup_info,
                               include_file_details);
  }

  Status GetBackupInfo(BackupID backup_id, BackupInfo* backup_info,
                       bool include_file_details = false) const override {
    ReadLock lock(&mutex_);
    return impl_.GetBackupInfo(backup_id, backup_info, include_file_details);
  }

  void GetBackupInfo(std::vector<BackupInfo>* backup_info,
                     bool include_file_details) const override {
    ReadLock lock(&mutex_);
    impl_.GetBackupInfo(backup_info, include_file_details);
  }

  void GetCorruptedBackups(
      std::vector<BackupID>* corrupt_backup_ids) const override {
    ReadLock lock(&mutex_);
    impl_.GetCorruptedBackups(corrupt_backup_ids);
  }

  using BackupEngine::RestoreDBFromBackup;
  IOStatus RestoreDBFromBackup(const RestoreOptions& options,
                               BackupID backup_id, const std::string& db_dir,
                               const std::string& wal_dir) const override {
    // TSAN reports a lock inversion (potential deadlock) if we acquire read
    // locks in different orders. Assuming the implementation of RWMutex
    // allows simultaneous read locks, there should be no deadlock, because
    // there is no write lock involved here. Nevertheless, to appease TSAN and
    // in case of degraded RWMutex implementation, we lock the BackupEngines
    // including this one and those in options.alternate_dirs in a consistent
    // order.
    // However, locked_restore_from_dirs is kept in "search" order.
    std::list<const BackupEngineImpl*> locked_restore_from_dirs;
    std::vector<port::RWMutex*> mutexes;

    // Add `this`
    locked_restore_from_dirs.emplace_back(&impl_);
    mutexes.push_back(&mutex_);

    // Add alternates
    for (BackupEngineReadOnlyBase* be : options.alternate_dirs) {
      BackupEngineImplThreadSafe* bets =
          static_cast_with_check<BackupEngineImplThreadSafe>(
              be->AsBackupEngine());
      locked_restore_from_dirs.emplace_back(&bets->impl_);
      mutexes.push_back(&bets->mutex_);
    }

    // Acquire read locks in pointer order
    std::sort(mutexes.begin(), mutexes.end());
    std::vector<ReadLock> locks(mutexes.begin(), mutexes.end());

    // Impl
    return impl_.RestoreDBFromBackup(options, backup_id, db_dir, wal_dir,
                                     locked_restore_from_dirs);
  }

  using BackupEngine::RestoreDBFromLatestBackup;
  IOStatus RestoreDBFromLatestBackup(
      const RestoreOptions& options, const std::string& db_dir,
      const std::string& wal_dir) const override {
    // Defer to above function, which locks
    return RestoreDBFromBackup(options, kLatestBackupIDMarker, db_dir, wal_dir);
  }

  IOStatus VerifyBackup(BackupID backup_id,
                        bool verify_with_checksum = false) const override {
    ReadLock lock(&mutex_);
    return impl_.VerifyBackup(backup_id, verify_with_checksum);
  }

  BackupEngine* AsBackupEngine() override { return this; }

  // Not public API but needed
  IOStatus Initialize() {
    // No locking needed
    return impl_.Initialize();
  }

  // Not public API but used in testing
  void TEST_SetBackupMetaSchemaOptions(
      const TEST_BackupMetaSchemaOptions& options) {
    impl_.schema_test_options_.reset(new TEST_BackupMetaSchemaOptions(options));
  }

  // Not public API but used in testing
  void TEST_SetDefaultRateLimitersClock(
      const std::shared_ptr<SystemClock>& backup_rate_limiter_clock = nullptr,
      const std::shared_ptr<SystemClock>& restore_rate_limiter_clock =
          nullptr) {
    impl_.TEST_SetDefaultRateLimitersClock(backup_rate_limiter_clock,
                                           restore_rate_limiter_clock);
  }

 private:
  mutable port::RWMutex mutex_;
  BackupEngineImpl impl_;
};

}  // namespace

IOStatus BackupEngine::Open(const BackupEngineOptions& options, Env* env,
                            BackupEngine** backup_engine_ptr) {
  std::unique_ptr<BackupEngineImplThreadSafe> backup_engine(
      new BackupEngineImplThreadSafe(options, env));
  auto s = backup_engine->Initialize();
  if (!s.ok()) {
    *backup_engine_ptr = nullptr;
    return s;
  }
  *backup_engine_ptr = backup_engine.release();
  return IOStatus::OK();
}

namespace {
BackupEngineImpl::BackupEngineImpl(const BackupEngineOptions& options,
                                   Env* db_env, bool read_only)
    : initialized_(false),
      latest_backup_id_(0),
      latest_valid_backup_id_(0),
      stop_backup_(false),
      options_(options),
      db_env_(db_env),
      backup_env_(options.backup_env != nullptr ? options.backup_env : db_env_),
      read_only_(read_only) {
  if (options_.backup_rate_limiter == nullptr &&
      options_.backup_rate_limit > 0) {
    options_.backup_rate_limiter.reset(
        NewGenericRateLimiter(options_.backup_rate_limit));
  }
  if (options_.restore_rate_limiter == nullptr &&
      options_.restore_rate_limit > 0) {
    options_.restore_rate_limiter.reset(
        NewGenericRateLimiter(options_.restore_rate_limit));
  }
  db_fs_ = db_env_->GetFileSystem();
  backup_fs_ = backup_env_->GetFileSystem();
}

BackupEngineImpl::~BackupEngineImpl() {
  // copy_engine_'s destructor drains and joins its threads.
  copy_engine_.reset();
  LogFlush(options_.info_log);
  for (const auto& it : corrupt_backups_) {
    it.second.first.PermitUncheckedError();
  }
}

IOStatus BackupEngineImpl::Initialize() {
  assert(!initialized_);
  initialized_ = true;
  if (read_only_) {
    ROCKS_LOG_INFO(options_.info_log, "Starting read_only backup engine");
  }
  options_.Dump(options_.info_log);

  auto meta_path = GetAbsolutePath(kMetaDirName);

  if (!read_only_) {
    // we might need to clean up from previous crash or I/O errors
    might_need_garbage_collect_ = true;

    if (options_.max_valid_backups_to_open !=
        std::numeric_limits<int32_t>::max()) {
      options_.max_valid_backups_to_open = std::numeric_limits<int32_t>::max();
      ROCKS_LOG_WARN(
          options_.info_log,
          "`max_valid_backups_to_open` is not set to the default value. "
          "Ignoring its value since BackupEngine is not read-only.");
    }

    // gather the list of directories that we need to create
    std::vector<std::pair<std::string, std::unique_ptr<FSDirectory>*>>
        directories;
    directories.emplace_back(GetAbsolutePath(), &backup_directory_);
    if (options_.share_table_files) {
      if (options_.share_files_with_checksum) {
        directories.emplace_back(
            GetAbsolutePath(GetSharedFileWithChecksumRel()),
            &shared_directory_);
      } else {
        directories.emplace_back(GetAbsolutePath(GetSharedFileRel()),
                                 &shared_directory_);
      }
    }
    directories.emplace_back(GetAbsolutePath(kPrivateDirName),
                             &private_directory_);
    directories.emplace_back(meta_path, &meta_directory_);
    // create all the dirs we need
    for (const auto& d : directories) {
      IOStatus io_s =
          backup_fs_->CreateDirIfMissing(d.first, io_options_, nullptr);
      if (io_s.ok()) {
        io_s =
            backup_fs_->NewDirectory(d.first, io_options_, d.second, nullptr);
      }
      if (!io_s.ok()) {
        return io_s;
      }
    }
  }

  std::vector<std::string> backup_meta_files;
  {
    IOStatus io_s = backup_fs_->GetChildren(meta_path, io_options_,
                                            &backup_meta_files, nullptr);
    if (io_s.IsNotFound()) {
      return IOStatus::NotFound(meta_path + " is missing");
    } else if (!io_s.ok()) {
      return io_s;
    }
  }
  // create backups_ structure
  for (auto& file : backup_meta_files) {
    ROCKS_LOG_INFO(options_.info_log, "Detected backup %s", file.c_str());
    BackupID backup_id = 0;
    sscanf(file.c_str(), "%u", &backup_id);
    if (backup_id == 0 || file != std::to_string(backup_id)) {
      // Invalid file name, will be deleted with auto-GC when user
      // initiates an append or write operation. (Behave as read-only until
      // then.)
      ROCKS_LOG_INFO(options_.info_log, "Skipping unrecognized meta file %s",
                     file.c_str());
      continue;
    }
    assert(backups_.find(backup_id) == backups_.end());
    // Insert all the (backup_id, BackupMeta) that will be loaded later
    // The loading performed later will check whether there are corrupt backups
    // and move the corrupt backups to corrupt_backups_
    backups_.insert(std::make_pair(
        backup_id, std::unique_ptr<BackupMeta>(new BackupMeta(
                       GetBackupMetaFile(backup_id, false /* tmp */),
                       GetBackupMetaFile(backup_id, true /* tmp */),
                       &backuped_file_infos_, backup_env_, backup_fs_))));
  }

  latest_backup_id_ = 0;
  latest_valid_backup_id_ = 0;
  if (options_.destroy_old_data) {  // Destroy old data
    assert(!read_only_);
    ROCKS_LOG_INFO(
        options_.info_log,
        "Backup Engine started with destroy_old_data == true, deleting all "
        "backups");
    IOStatus io_s = PurgeOldBackups(0);
    if (io_s.ok()) {
      io_s = GarbageCollect();
    }
    if (!io_s.ok()) {
      return io_s;
    }
  } else {  // Load data from storage
    // abs_path_to_size: maps absolute paths of files in backup directory to
    // their corresponding sizes
    std::unordered_map<std::string, uint64_t> abs_path_to_size;
    // Insert files and their sizes in backup sub-directories (shared and
    // shared_checksum) to abs_path_to_size
    for (const auto& rel_dir :
         {GetSharedFileRel(), GetSharedFileWithChecksumRel()}) {
      const auto abs_dir = GetAbsolutePath(rel_dir);
      IOStatus io_s =
          ReadChildFileCurrentSizes(abs_dir, backup_fs_, &abs_path_to_size);
      if (!io_s.ok()) {
        // I/O error likely impacting all backups
        return io_s;
      }
    }
    // load the backups if any, until valid_backups_to_open of the latest
    // non-corrupted backups have been successfully opened.
    int valid_backups_to_open = options_.max_valid_backups_to_open;
    for (auto backup_iter = backups_.rbegin(); backup_iter != backups_.rend();
         ++backup_iter) {
      assert(latest_backup_id_ == 0 || latest_backup_id_ > backup_iter->first);
      if (latest_backup_id_ == 0) {
        latest_backup_id_ = backup_iter->first;
      }
      if (valid_backups_to_open == 0) {
        break;
      }

      // Insert files and their sizes in backup sub-directories
      // (private/backup_id) to abs_path_to_size
      IOStatus io_s = ReadChildFileCurrentSizes(
          GetAbsolutePath(GetPrivateFileRel(backup_iter->first)), backup_fs_,
          &abs_path_to_size);
      if (io_s.ok()) {
        io_s = backup_iter->second->LoadFromFile(
            options_.backup_dir, abs_path_to_size,
            options_.backup_rate_limiter.get(), options_.info_log,
            &reported_ignored_fields_);
      }
      if (io_s.IsCorruption() || io_s.IsNotSupported()) {
        ROCKS_LOG_INFO(options_.info_log, "Backup %u corrupted -- %s",
                       backup_iter->first, io_s.ToString().c_str());
        corrupt_backups_.insert(std::make_pair(
            backup_iter->first,
            std::make_pair(io_s, std::move(backup_iter->second))));
      } else if (!io_s.ok()) {
        // Distinguish corruption errors from errors in the backup Env.
        // Errors in the backup Env (i.e., this code path) will cause Open() to
        // fail, whereas corruption errors would not cause Open() failures.
        return io_s;
      } else {
        ROCKS_LOG_INFO(options_.info_log, "Loading backup %" PRIu32 " OK:\n%s",
                       backup_iter->first,
                       backup_iter->second->GetInfoString().c_str());
        assert(latest_valid_backup_id_ == 0 ||
               latest_valid_backup_id_ > backup_iter->first);
        if (latest_valid_backup_id_ == 0) {
          latest_valid_backup_id_ = backup_iter->first;
        }
        --valid_backups_to_open;
      }
    }

    for (const auto& corrupt : corrupt_backups_) {
      backups_.erase(backups_.find(corrupt.first));
    }
    // erase the backups before max_valid_backups_to_open
    int num_unopened_backups;
    if (options_.max_valid_backups_to_open == 0) {
      num_unopened_backups = 0;
    } else {
      num_unopened_backups =
          std::max(0, static_cast<int>(backups_.size()) -
                          options_.max_valid_backups_to_open);
    }
    for (int i = 0; i < num_unopened_backups; ++i) {
      assert(backups_.begin()->second->Empty());
      backups_.erase(backups_.begin());
    }
  }

  ROCKS_LOG_INFO(options_.info_log, "Latest backup is %u", latest_backup_id_);
  ROCKS_LOG_INFO(options_.info_log, "Latest valid backup is %u",
                 latest_valid_backup_id_);

  // Create the shared CopyEngine pool now that options_ (incl. rate limiters)
  // is finalized.
  CopyEngineOptions copy_options;
  copy_options.max_background_operations = options_.max_background_operations;
  copy_options.io_buffer_size = options_.io_buffer_size;
  copy_options.callback_trigger_interval_size =
      options_.callback_trigger_interval_size;
  copy_options.checksum_rate_limiter = options_.backup_rate_limiter.get();
  copy_options.info_log = options_.info_log;
  copy_options.thread_name = "backup_engine";
  copy_options.should_abort = [this]() { return ShouldStopBackup(); };
  copy_options.abort_status_message = "Backup stopped";
  copy_engine_ = std::make_unique<CopyEngine>(std::move(copy_options));

  ROCKS_LOG_INFO(options_.info_log, "Initialized BackupEngine");
  return IOStatus::OK();
}

IOStatus BackupEngineImpl::CreateNewBackupWithMetadata(
    const CreateBackupOptions& options, DB* db, const std::string& app_metadata,
    BackupID* new_backup_id_ptr) {
  assert(initialized_);
  assert(!read_only_);
  if (ShouldStopBackup()) {
    return status_to_io_status(Status::Incomplete("Backup stopped"));
  }
  if (app_metadata.size() > kMaxAppMetaSize) {
    return IOStatus::InvalidArgument("App metadata too large");
  }

  bool maybe_exclude_items = bool{options.exclude_files_callback};
  if (maybe_exclude_items && options_.schema_version < 2) {
    return IOStatus::InvalidArgument(
        "exclude_files_callback requires schema_version >= 2");
  }

  if (options.decrease_background_thread_cpu_priority) {
    copy_engine_->MaybeDecreaseCpuPriority(
        options.background_thread_cpu_priority);
  }

  BackupID new_backup_id = latest_backup_id_ + 1;

  // `bytes_read` and `bytes_written` stats are enabled based on compile-time
  // support and cannot be dynamically toggled. So we do not need to worry about
  // `PerfLevel` here, unlike many other `IOStatsContext` / `PerfContext` stats.
  uint64_t prev_bytes_read = IOSTATS(bytes_read);
  uint64_t prev_bytes_written = IOSTATS(bytes_written);

  assert(backups_.find(new_backup_id) == backups_.end());

  auto private_dir = GetAbsolutePath(GetPrivateFileRel(new_backup_id));
  IOStatus io_s = backup_fs_->FileExists(private_dir, io_options_, nullptr);
  if (io_s.ok()) {
    // maybe last backup failed and left partial state behind, clean it up.
    // need to do this before updating backups_ such that a private dir
    // named after new_backup_id will be cleaned up.
    // (If an incomplete new backup is followed by an incomplete delete
    // of the latest full backup, then there could be more than one next
    // id with a private dir, the last thing to be deleted in delete
    // backup, but all will be cleaned up with a GarbageCollect.)
    io_s = GarbageCollect();
  } else if (io_s.IsNotFound()) {
    // normal case, the new backup's private dir doesn't exist yet
    io_s = IOStatus::OK();
  }

  auto ret = backups_.insert(std::make_pair(
      new_backup_id, std::unique_ptr<BackupMeta>(new BackupMeta(
                         GetBackupMetaFile(new_backup_id, false /* tmp */),
                         GetBackupMetaFile(new_backup_id, true /* tmp */),
                         &backuped_file_infos_, backup_env_, backup_fs_))));
  assert(ret.second == true);
  auto& new_backup = ret.first->second;
  new_backup->RecordTimestamp();
  new_backup->SetAppMetadata(app_metadata);

  auto start_backup = backup_env_->NowMicros();

  ROCKS_LOG_INFO(options_.info_log,
                 "Started the backup process -- creating backup %u",
                 new_backup_id);

  if (options_.share_table_files && !options_.share_files_with_checksum) {
    ROCKS_LOG_WARN(options_.info_log,
                   "BackupEngineOptions::share_files_with_checksum=false is "
                   "DEPRECATED and could lead to data loss.");
  }

  if (io_s.ok()) {
    io_s = backup_fs_->CreateDir(private_dir, io_options_, nullptr);
  }

  // A set into which we will insert the dst_paths that are calculated for live
  // files and live WAL files.
  // This is used to check whether a live files shares a dst_path with another
  // live file.
  std::unordered_set<std::string> live_dst_paths;

  std::deque<BackupWorkItemPair> excludable_items;
  std::deque<BackupAfterCopyOrCreateWorkItem> backup_items_to_finish;
  // Add a WorkItem to the channel for each live file
  Status disabled = db->DisableFileDeletions();
  DBOptions db_options = db->GetDBOptions();
  Statistics* stats = db_options.statistics.get();
  if (io_s.ok()) {
    CheckpointImpl checkpoint(db);
    uint64_t sequence_number = 0;
    FileChecksumGenFactory* db_checksum_factory =
        db_options.file_checksum_gen_factory.get();
    const std::string kFileChecksumGenFactoryName =
        "FileChecksumGenCrc32cFactory";
    bool compare_checksum =
        db_checksum_factory != nullptr &&
                db_checksum_factory->Name() == kFileChecksumGenFactoryName
            ? true
            : false;
    EnvOptions src_raw_env_options(db_options);
    RateLimiter* rate_limiter = options_.backup_rate_limiter.get();
    io_s = status_to_io_status(checkpoint.CreateCustomCheckpoint(
        [&](const std::string& /*src_dirname*/, const std::string& /*fname*/,
            FileType, const Temperature /*temperature*/) {
          // custom checkpoint will switch to calling copy_file_cb after it sees
          // NotSupported returned from link_file_cb.
          return IOStatus::NotSupported();
        } /* link_file_cb */,
        [&](const std::string& src_dirname, const std::string& fname,
            uint64_t size_limit_bytes, FileType type,
            const std::string& checksum_func_name,
            const std::string& checksum_val,
            const Temperature src_temperature) {
          if (type == kWalFile && !options_.backup_log_files) {
            return IOStatus::OK();
          }
          Log(options_.info_log, "add file for backup %s", fname.c_str());
          uint64_t size_bytes = 0;
          IOStatus io_st;
          if (type == kTableFile || type == kBlobFile) {
            io_st = db_fs_->GetFileSize(src_dirname + "/" + fname, io_options_,
                                        &size_bytes, nullptr);
            if (!io_st.ok()) {
              Log(options_.info_log, "GetFileSize is failed: %s",
                  io_st.ToString().c_str());
              return io_st;
            }
          }
          EnvOptions src_env_options;
          switch (type) {
            case kWalFile:
              src_env_options =
                  db_env_->OptimizeForLogRead(src_raw_env_options);
              break;
            case kTableFile:
              src_env_options = db_env_->OptimizeForCompactionTableRead(
                  src_raw_env_options, ImmutableDBOptions(db_options));
              break;
            case kDescriptorFile:
              src_env_options =
                  db_env_->OptimizeForManifestRead(src_raw_env_options);
              break;
            case kBlobFile:
              src_env_options = db_env_->OptimizeForBlobFileRead(
                  src_raw_env_options, ImmutableDBOptions(db_options));
              break;
            default:
              // Other backed up files (like options file) are not read by live
              // DB, so don't need to worry about avoiding mixing buffered and
              // direct I/O. Just use plain defaults.
              src_env_options = src_raw_env_options;
              break;
          }
          io_st = AddBackupFileWorkItem(
              live_dst_paths, backup_items_to_finish,
              maybe_exclude_items ? &excludable_items : nullptr, new_backup_id,
              options_.share_table_files &&
                  (type == kTableFile || type == kBlobFile),
              src_dirname, fname, src_env_options, rate_limiter, type,
              size_bytes, db_options.statistics.get(), size_limit_bytes,
              options_.share_files_with_checksum &&
                  (type == kTableFile || type == kBlobFile),
              options.progress_callback, "" /* contents */, checksum_func_name,
              checksum_val, src_temperature);
          return io_st;
        } /* copy_file_cb */,
        [&](const std::string& fname, const std::string& contents,
            FileType type) {
          Log(options_.info_log, "add file for backup %s", fname.c_str());
          return AddBackupFileWorkItem(
              live_dst_paths, backup_items_to_finish,
              maybe_exclude_items ? &excludable_items : nullptr, new_backup_id,
              false /* shared */, "" /* src_dir */, fname,
              EnvOptions() /* src_env_options */, rate_limiter, type,
              contents.size(), db_options.statistics.get(), 0 /* size_limit */,
              false /* shared_checksum */, options.progress_callback, contents);
        } /* create_file_cb */,
        &sequence_number,
        options.flush_before_backup ? 0 : std::numeric_limits<uint64_t>::max(),
        compare_checksum, options.atomic_flush));
    if (io_s.ok()) {
      new_backup->SetSequenceNumber(sequence_number);
    }
  }
  ROCKS_LOG_INFO(options_.info_log, "add files for backup done.");
  if (io_s.ok() && maybe_exclude_items) {
    assert(options.exclude_files_callback);
    size_t count = excludable_items.size();
    std::vector<MaybeExcludeBackupFile> maybe_exclude_files;
    maybe_exclude_files.reserve(count);
    for (auto& e : excludable_items) {
      maybe_exclude_files.emplace_back(
          BackupExcludedFileInfo(e.second.dst_relative));
    }
    if (count > 0) {
      try {
        options.exclude_files_callback(
            &maybe_exclude_files.front(),
            /*end pointer*/ &maybe_exclude_files.back() + 1);
      } catch (const std::exception& exn) {
        io_s = IOStatus::Aborted("Exception in exclude_files_callback: " +
                                 std::string(exn.what()));
      } catch (...) {
        io_s = IOStatus::Aborted("Unknown exception in exclude_files_callback");
      }
    }
    if (io_s.ok()) {
      for (size_t i = 0; i < count; ++i) {
        auto& e = excludable_items[i];
        if (maybe_exclude_files[i].exclude_decision) {
          new_backup.get()->AddExcludedFile(e.second.dst_relative);
        } else {
          copy_engine_->Submit(std::move(e.first));
          backup_items_to_finish.push_back(std::move(e.second));
        }
      }
    }
    excludable_items.clear();
  } else {
    assert(!options.exclude_files_callback);
    assert(excludable_items.empty());
  }
  ROCKS_LOG_INFO(options_.info_log,
                 "dispatch files for backup done, wait for finish.");
  for (auto& item : backup_items_to_finish) {
    item.result.wait();
    auto result = item.result.get();
    IOStatus item_io_status = result.io_status;
    Temperature temp = result.expected_src_temperature;
    if (result.current_src_temperature != Temperature::kUnknown &&
        (temp == Temperature::kUnknown ||
         options_.current_temperatures_override_manifest)) {
      temp = result.current_src_temperature;
    }
    if (item_io_status.ok() && item.shared && item.needed_to_copy) {
      item_io_status = item.backup_env->GetFileSystem()->RenameFile(
          item.dst_path_tmp, item.dst_path, io_options_, nullptr);
    }
    if (item_io_status.ok()) {
      item_io_status = new_backup.get()->AddFile(std::make_shared<FileInfo>(
          item.dst_relative, result.size, result.checksum_hex, result.db_id,
          result.db_session_id, temp));
    }
    if (!item_io_status.ok()) {
      io_s = std::move(item_io_status);
      io_s.MustCheck();
    }
  }

  // we copied all the files, enable file deletions
  if (disabled.ok()) {  // If we successfully disabled file deletions
    db->EnableFileDeletions().PermitUncheckedError();
  }
  auto backup_time = backup_env_->NowMicros() - start_backup;

  if (io_s.ok()) {
    // persist the backup metadata on the disk
    io_s = new_backup->StoreToFile(options_.sync, options_.schema_version,
                                   schema_test_options_.get());
  }
  if (io_s.ok() && options_.sync) {
    std::unique_ptr<FSDirectory> backup_private_directory;
    backup_fs_
        ->NewDirectory(GetAbsolutePath(GetPrivateFileRel(new_backup_id, false)),
                       io_options_, &backup_private_directory, nullptr)
        .PermitUncheckedError();
    if (backup_private_directory != nullptr) {
      io_s = backup_private_directory->FsyncWithDirOptions(io_options_, nullptr,
                                                           DirFsyncOptions());
    }
    if (io_s.ok() && private_directory_ != nullptr) {
      io_s = private_directory_->FsyncWithDirOptions(io_options_, nullptr,
                                                     DirFsyncOptions());
    }
    if (io_s.ok() && meta_directory_ != nullptr) {
      io_s = meta_directory_->FsyncWithDirOptions(io_options_, nullptr,
                                                  DirFsyncOptions());
    }
    if (io_s.ok() && shared_directory_ != nullptr) {
      io_s = shared_directory_->FsyncWithDirOptions(io_options_, nullptr,
                                                    DirFsyncOptions());
    }
    if (io_s.ok() && backup_directory_ != nullptr) {
      io_s = backup_directory_->FsyncWithDirOptions(io_options_, nullptr,
                                                    DirFsyncOptions());
    }
  }

  if (io_s.ok()) {
    backup_statistics_.IncrementNumberSuccessBackup();
    // here we know that we succeeded and installed the new backup
    latest_backup_id_ = new_backup_id;
    latest_valid_backup_id_ = new_backup_id;
    if (new_backup_id_ptr) {
      *new_backup_id_ptr = new_backup_id;
    }
    ROCKS_LOG_INFO(options_.info_log, "Backup DONE. All is good");

    // backup_speed is in byte/second
    double backup_speed = new_backup->GetSize() / (1.048576 * backup_time);
    ROCKS_LOG_INFO(options_.info_log, "Backup number of files: %u",
                   new_backup->GetNumberFiles());
    char human_size[16];
    AppendHumanBytes(new_backup->GetSize(), human_size, sizeof(human_size));
    ROCKS_LOG_INFO(options_.info_log, "Backup size: %s", human_size);
    ROCKS_LOG_INFO(options_.info_log, "Backup time: %" PRIu64 " microseconds",
                   backup_time);
    ROCKS_LOG_INFO(options_.info_log, "Backup speed: %.3f MB/s", backup_speed);
    ROCKS_LOG_INFO(options_.info_log, "Backup Statistics %s",
                   backup_statistics_.ToString().c_str());
  } else {
    backup_statistics_.IncrementNumberFailBackup();
    // clean all the files we might have created
    ROCKS_LOG_INFO(options_.info_log, "Backup failed -- %s",
                   io_s.ToString().c_str());
    ROCKS_LOG_INFO(options_.info_log, "Backup Statistics %s\n",
                   backup_statistics_.ToString().c_str());
    // delete files that we might have already written
    might_need_garbage_collect_ = true;
    DeleteBackup(new_backup_id).PermitUncheckedError();
  }

  RecordTick(stats, BACKUP_READ_BYTES, IOSTATS(bytes_read) - prev_bytes_read);
  RecordTick(stats, BACKUP_WRITE_BYTES,
             IOSTATS(bytes_written) - prev_bytes_written);
  return io_s;
}

IOStatus BackupEngineImpl::PurgeOldBackups(uint32_t num_backups_to_keep) {
  assert(initialized_);
  assert(!read_only_);

  // Best effort deletion even with errors
  IOStatus overall_status = IOStatus::OK();

  ROCKS_LOG_INFO(options_.info_log, "Purging old backups, keeping %u",
                 num_backups_to_keep);
  std::vector<BackupID> to_delete;
  auto itr = backups_.begin();
  while ((backups_.size() - to_delete.size()) > num_backups_to_keep) {
    to_delete.push_back(itr->first);
    itr++;
  }
  for (auto backup_id : to_delete) {
    // Do not GC until end
    IOStatus io_s = DeleteBackupNoGC(backup_id);
    if (!io_s.ok()) {
      overall_status = io_s;
    }
  }
  // Clean up after any incomplete backup deletion, potentially from
  // earlier session.
  if (might_need_garbage_collect_) {
    IOStatus io_s = GarbageCollect();
    if (!io_s.ok() && overall_status.ok()) {
      overall_status = io_s;
    }
  }
  return overall_status;
}

IOStatus BackupEngineImpl::DeleteBackup(BackupID backup_id) {
  IOStatus s1 = DeleteBackupNoGC(backup_id);
  IOStatus s2 = IOStatus::OK();

  // Clean up after any incomplete backup deletion, potentially from
  // earlier session.
  if (might_need_garbage_collect_) {
    s2 = GarbageCollect();
  }

  if (!s1.ok()) {
    // Any failure in the primary objective trumps any failure in the
    // secondary objective.
    s2.PermitUncheckedError();
    return s1;
  } else {
    return s2;
  }
}

// Does not auto-GarbageCollect nor lock
IOStatus BackupEngineImpl::DeleteBackupNoGC(BackupID backup_id) {
  assert(initialized_);
  assert(!read_only_);

  ROCKS_LOG_INFO(options_.info_log, "Deleting backup %u", backup_id);
  auto backup = backups_.find(backup_id);
  if (backup != backups_.end()) {
    IOStatus io_s = backup->second->Delete();
    if (!io_s.ok()) {
      return io_s;
    }
    backups_.erase(backup);
    if (backups_.empty()) {
      latest_valid_backup_id_ = 0;
    } else {
      latest_valid_backup_id_ = backups_.rbegin()->first;
    }
  } else {
    auto corrupt = corrupt_backups_.find(backup_id);
    if (corrupt == corrupt_backups_.end()) {
      return IOStatus::NotFound("Backup not found");
    }
    IOStatus io_s = corrupt->second.second->Delete();
    if (!io_s.ok()) {
      return io_s;
    }
    corrupt->second.first.PermitUncheckedError();
    corrupt_backups_.erase(corrupt);
  }

  // After removing meta file, best effort deletion even with errors.
  // (Don't delete other files if we can't delete the meta file right
  // now.)
  std::vector<std::string> to_delete;
  for (auto& itr : backuped_file_infos_) {
    if (itr.second->refs == 0) {
      IOStatus io_s = backup_fs_->DeleteFile(GetAbsolutePath(itr.first),
                                             io_options_, nullptr);
      ROCKS_LOG_INFO(options_.info_log, "Deleting %s -- %s", itr.first.c_str(),
                     io_s.ToString().c_str());
      to_delete.push_back(itr.first);
      if (!io_s.ok()) {
        // Trying again later might work
        might_need_garbage_collect_ = true;
      }
    }
  }
  for (auto& td : to_delete) {
    backuped_file_infos_.erase(td);
  }

  // take care of private dirs -- GarbageCollect() will take care of them
  // if they are not empty
  std::string private_dir = GetPrivateFileRel(backup_id);
  IOStatus io_s =
      backup_fs_->DeleteDir(GetAbsolutePath(private_dir), io_options_, nullptr);
  ROCKS_LOG_INFO(options_.info_log, "Deleting private dir %s -- %s",
                 private_dir.c_str(), io_s.ToString().c_str());
  if (!io_s.ok()) {
    // Full gc or trying again later might work
    might_need_garbage_collect_ = true;
  }
  return IOStatus::OK();
}

void BackupEngineImpl::SetBackupInfoFromBackupMeta(
    BackupID id, const BackupMeta& meta, BackupInfo* backup_info,
    bool include_file_details) const {
  *backup_info = BackupInfo(id, meta.GetTimestamp(), meta.GetSize(),
                            meta.GetNumberFiles(), meta.GetAppMetadata());
  std::string dir =
      options_.backup_dir + "/" + kPrivateDirSlash + std::to_string(id);
  if (include_file_details) {
    auto& file_details = backup_info->file_details;
    file_details.reserve(meta.GetFiles().size());
    for (auto& file_ptr : meta.GetFiles()) {
      BackupFileInfo& finfo = file_details.emplace_back();
      finfo.relative_filename = file_ptr->filename;
      finfo.size = file_ptr->size;
      finfo.directory = dir;
      uint64_t number;
      FileType type;
      // file_ptr->filename may contain directory components (e.g.
      // "private/1/000008.log"). ParseFileName expects a bare filename,
      // so use GetDbFileName() to extract it.
      bool ok = ParseFileName(file_ptr->GetDbFileName(), &number, &type);
      if (ok) {
        finfo.file_number = number;
        finfo.file_type = type;
      }
      // TODO: temperature, file_checksum, file_checksum_func_name
      // finfo.temperature = file_ptr->temp;
    }
    backup_info->excluded_files = meta.GetExcludedFiles();

    backup_info->name_for_open = GetAbsolutePath(GetPrivateFileRel(id));
    backup_info->name_for_open.pop_back();  // remove trailing '/'
    backup_info->env_for_open = meta.GetEnvForOpen();
  }
}

Status BackupEngineImpl::GetBackupInfo(BackupID backup_id,
                                       BackupInfo* backup_info,
                                       bool include_file_details) const {
  assert(initialized_);
  if (backup_id == kLatestBackupIDMarker) {
    // Note: Read latest_valid_backup_id_ inside of lock
    backup_id = latest_valid_backup_id_;
  }
  auto corrupt_itr = corrupt_backups_.find(backup_id);
  if (corrupt_itr != corrupt_backups_.end()) {
    return Status::Corruption(corrupt_itr->second.first.ToString());
  }
  auto backup_itr = backups_.find(backup_id);
  if (backup_itr == backups_.end()) {
    return Status::NotFound("Backup not found");
  }
  auto& backup = backup_itr->second;
  if (backup->Empty()) {
    return Status::NotFound("Backup not found");
  }

  SetBackupInfoFromBackupMeta(backup_id, *backup, backup_info,
                              include_file_details);
  return Status::OK();
}

void BackupEngineImpl::GetBackupInfo(std::vector<BackupInfo>* backup_info,
                                     bool include_file_details) const {
  assert(initialized_);
  backup_info->resize(backups_.size());
  size_t i = 0;
  for (auto& backup : backups_) {
    const BackupMeta& meta = *backup.second;
    if (!meta.Empty()) {
      SetBackupInfoFromBackupMeta(backup.first, meta, &backup_info->at(i++),
                                  include_file_details);
    }
  }
}

void BackupEngineImpl::GetCorruptedBackups(
    std::vector<BackupID>* corrupt_backup_ids) const {
  assert(initialized_);
  corrupt_backup_ids->reserve(corrupt_backups_.size());
  for (auto& backup : corrupt_backups_) {
    corrupt_backup_ids->push_back(backup.first);
  }
}

IOStatus BackupEngineImpl::RestoreDBFromBackup(
    const RestoreOptions& options, BackupID backup_id,
    const std::string& db_dir, const std::string& wal_dir,
    const std::list<const BackupEngineImpl*>& locked_restore_from_dirs) const {
  assert(initialized_);
  if (backup_id == kLatestBackupIDMarker) {
    // Note: Read latest_valid_backup_id_ inside of lock
    backup_id = latest_valid_backup_id_;
  }
  auto corrupt_itr = corrupt_backups_.find(backup_id);
  if (corrupt_itr != corrupt_backups_.end()) {
    return corrupt_itr->second.first;
  }
  auto backup_itr = backups_.find(backup_id);
  if (backup_itr == backups_.end()) {
    return IOStatus::NotFound("Backup not found");
  }
  auto& backup = backup_itr->second;
  if (backup->Empty()) {
    return IOStatus::NotFound("Backup not found");
  }

  ROCKS_LOG_INFO(options_.info_log, "Restoring backup id %u\n", backup_id);
  ROCKS_LOG_INFO(options_.info_log, "keep_log_files: %d\n",
                 static_cast<int>(options.keep_log_files));

  // just in case. Ignore errors
  db_fs_->CreateDirIfMissing(db_dir, io_options_, nullptr)
      .PermitUncheckedError();
  db_fs_->CreateDirIfMissing(wal_dir, io_options_, nullptr)
      .PermitUncheckedError();

  // Files to restore, and from where (taking into account excluded files)
  std::vector<std::pair<const BackupEngineImpl*, const FileInfo*>>
      restore_file_infos;
  restore_file_infos.reserve(backup->GetFiles().size() +
                             backup->GetExcludedFiles().size());
  std::unordered_set<std::string> unowned_backups;
  for (const auto& ef : backup->GetExcludedFiles()) {
    const std::string& file = ef.relative_file;

    bool found = false;
    for (auto be : locked_restore_from_dirs) {
      auto it = be->backuped_file_infos_.find(file);
      if (it != be->backuped_file_infos_.end()) {
        restore_file_infos.emplace_back(be, &*it->second);
        found = true;
        break;
      }
    }
    if (!found) {
      // In `kKeepLatestDbSessionIdFiles` restore mode, it's not strictly
      // required for the corresponding backup file to be present for as long
      // as existing, on-disk db file metadata matches this unowned backup file
      // db_session_id and size.
      if (options.mode == RestoreOptions::Mode::kKeepLatestDbSessionIdFiles) {
        unowned_backups.insert(ef.relative_file);
        continue;
      }

      return IOStatus::InvalidArgument(
          "Excluded file " + file + " not found in any of %d" +
          std::to_string(locked_restore_from_dirs.size() - 1) +
          "backup directories!");
    }
  }

  // Non-excluded files
  for (const auto& file_info_shared : backup->GetFiles()) {
    restore_file_infos.emplace_back(this, &*file_info_shared);
  }

  std::unordered_set<uint64_t> files_to_keep;
  InferDBFilesToRetainInRestore(restore_file_infos, unowned_backups, db_dir,
                                options.mode, files_to_keep);

  if (!unowned_backups.empty()) {
    return IOStatus::InvalidArgument(
        "Excluded file " + *unowned_backups.begin() + " (one amongst " +
        std::to_string(unowned_backups.size()) + ") not found in any of" +
        std::to_string(locked_restore_from_dirs.size() - 1) +
        "backup directories!");
  }

  if (options.keep_log_files) {
    // delete non-matching files in db_dir, but keep all the log files
    DeleteChildren(db_dir, files_to_keep, 1 << kWalFile);
    // move all the files from archive dir to wal_dir
    std::string archive_dir = ArchivalDirectory(wal_dir);
    std::vector<std::string> archive_files;
    db_fs_->GetChildren(archive_dir, io_options_, &archive_files, nullptr)
        .PermitUncheckedError();  // ignore errors
    for (const auto& f : archive_files) {
      uint64_t number;
      FileType type;
      bool ok = ParseFileName(f, &number, &type);
      if (ok && type == kWalFile) {
        ROCKS_LOG_INFO(options_.info_log,
                       "Moving log file from archive/ to wal_dir: %s",
                       f.c_str());
        IOStatus io_s = db_fs_->RenameFile(
            archive_dir + "/" + f, wal_dir + "/" + f, io_options_, nullptr);
        if (!io_s.ok()) {
          // if we can't move log file from archive_dir to wal_dir,
          // we should fail, since it might mean data loss
          return io_s;
        }
      }
    }
  } else {
    DeleteChildren(wal_dir, files_to_keep);
    DeleteChildren(ArchivalDirectory(wal_dir), files_to_keep);
    DeleteChildren(db_dir, files_to_keep);
  }

  IOStatus io_s;
  std::vector<RestoreAfterCopyOrCreateWorkItem> restore_items_to_finish;
  std::string temporary_current_file;
  std::string final_current_file;
  std::unique_ptr<FSDirectory> db_dir_for_fsync;
  std::unique_ptr<FSDirectory> wal_dir_for_fsync;

  for (const auto& engine_and_file_info : restore_file_infos) {
    const FileInfo* file_info = engine_and_file_info.second;
    const std::string& file = file_info->filename;
    std::string absolute_file =
        engine_and_file_info.first->GetAbsolutePath(file);
    Env* src_env = engine_and_file_info.first->backup_env_;

    // 1. get DB filename
    std::string dst = file_info->GetDbFileName();

    // 2. find the filetype
    uint64_t number;
    FileType type;
    bool ok = ParseFileName(dst, &number, &type);
    if (!ok) {
      return IOStatus::Corruption("Backup corrupted: Fail to parse filename " +
                                  dst);
    }

    // `files_to_keep` identifies existing database files with contents
    // 'identical' to their respective backup files (standard or excluded)
    // as per user-selected RestoreOptions::Mode.
    if (files_to_keep.find(number) != files_to_keep.end()) {
      // This file is already in the destination directory. Skip restore.
      continue;
    }

    // 3. Construct the final path
    // kWalFile lives in wal_dir and all the rest live in db_dir
    if (type == kWalFile) {
      dst = wal_dir + "/" + dst;
      if (options_.sync && !wal_dir_for_fsync) {
        io_s = db_fs_->NewDirectory(wal_dir, io_options_, &wal_dir_for_fsync,
                                    nullptr);
        if (!io_s.ok()) {
          return io_s;
        }
      }
    } else {
      dst = db_dir + "/" + dst;
      if (options_.sync && !db_dir_for_fsync) {
        io_s = db_fs_->NewDirectory(db_dir, io_options_, &db_dir_for_fsync,
                                    nullptr);
        if (!io_s.ok()) {
          return io_s;
        }
      }
    }
    // For atomicity, initially restore CURRENT file to a temporary name.
    // This is useful even without options_.sync e.g. in case the restore
    // process is interrupted.
    if (type == kCurrentFile) {
      final_current_file = dst;
      dst = temporary_current_file = dst + ".tmp";
    }

    ROCKS_LOG_INFO(options_.info_log, "Restoring %s to %s\n", file.c_str(),
                   dst.c_str());

    // When file is being copied over, it means that it was either non-existent,
    // purged or its' original on-disk representation didn't meet incremental
    // restore tiering criteria. As such, we need to unconditionally recompute
    // the checksum on the newly restored files - even if checksum was already
    // computed on its' seed backup file in early assessment phase. Protection
    // is put in place to ensure that there are no bugs in the actual restore /
    // file copy logic and we're not producing garbage db files.
    WorkItem copy_or_create_work_item(
        absolute_file, dst, Temperature::kUnknown /* src_temp */,
        file_info->temp, "" /* contents */, src_env, db_env_,
        EnvOptions() /* src_env_options */, options_.sync,
        false /* use_fsync */, options_.restore_rate_limiter.get(),
        file_info->size, nullptr /* stats */);
    RestoreAfterCopyOrCreateWorkItem after_copy_or_create_work_item(
        copy_or_create_work_item.result.get_future(), file, dst,
        file_info->checksum_hex);
    copy_engine_->Submit(std::move(copy_or_create_work_item));
    restore_items_to_finish.push_back(
        std::move(after_copy_or_create_work_item));
  }
  IOStatus item_io_status;
  for (auto& item : restore_items_to_finish) {
    item.result.wait();
    auto result = item.result.get();
    item_io_status = result.io_status;
    // Note: It is possible that both of the following bad-status cases occur
    // during copying. But, we only return one status.
    if (!item_io_status.ok()) {
      io_s = item_io_status;
      break;
    } else if (!item.checksum_hex.empty() &&
               item.checksum_hex != result.checksum_hex) {
      io_s = IOStatus::Corruption(
          "While restoring " + item.from_file + " -> " + item.to_file +
          ": expected checksum is " + item.checksum_hex +
          " while computed checksum is " + result.checksum_hex);
      break;
    }
  }

  // When enabled, the first FsyncWithDirOptions is to ensure all files are
  // fully persisted before renaming CURRENT.tmp
  if (io_s.ok() && db_dir_for_fsync) {
    ROCKS_LOG_INFO(options_.info_log, "Restore: fsync\n");
    io_s = db_dir_for_fsync->FsyncWithDirOptions(io_options_, nullptr,
                                                 DirFsyncOptions());
  }

  if (io_s.ok() && wal_dir_for_fsync) {
    io_s = wal_dir_for_fsync->FsyncWithDirOptions(io_options_, nullptr,
                                                  DirFsyncOptions());
  }

  if (io_s.ok() && !temporary_current_file.empty()) {
    ROCKS_LOG_INFO(options_.info_log, "Restore: atomic rename CURRENT.tmp\n");
    assert(!final_current_file.empty());
    io_s = db_fs_->RenameFile(temporary_current_file, final_current_file,
                              io_options_, nullptr);
  }

  if (io_s.ok() && db_dir_for_fsync && !temporary_current_file.empty()) {
    // Second FsyncWithDirOptions is to ensure the final atomic rename of DB
    // restore is fully persisted even if power goes out right after restore
    // operation returns success
    assert(db_dir_for_fsync);
    io_s = db_dir_for_fsync->FsyncWithDirOptions(
        io_options_, nullptr, DirFsyncOptions(final_current_file));
  }

  ROCKS_LOG_INFO(options_.info_log, "Restoring done -- %s\n",
                 io_s.ToString().c_str());
  return io_s;
}

IOStatus BackupEngineImpl::VerifyBackup(BackupID backup_id,
                                        bool verify_with_checksum) const {
  assert(initialized_);
  // Check if backup_id is corrupted, or valid and registered
  auto corrupt_itr = corrupt_backups_.find(backup_id);
  if (corrupt_itr != corrupt_backups_.end()) {
    return corrupt_itr->second.first;
  }

  auto backup_itr = backups_.find(backup_id);
  if (backup_itr == backups_.end()) {
    return IOStatus::NotFound();
  }

  auto& backup = backup_itr->second;
  if (backup->Empty()) {
    return IOStatus::NotFound();
  }

  ROCKS_LOG_INFO(options_.info_log, "Verifying backup id %u\n", backup_id);

  // Find all existing backup files belong to backup_id
  std::unordered_map<std::string, uint64_t> curr_abs_path_to_size;
  for (const auto& rel_dir : {GetPrivateFileRel(backup_id), GetSharedFileRel(),
                              GetSharedFileWithChecksumRel()}) {
    const auto abs_dir = GetAbsolutePath(rel_dir);
    // Shared directories allowed to be missing in some cases. Expected but
    // missing files will be reported a few lines down.
    ReadChildFileCurrentSizes(abs_dir, backup_fs_, &curr_abs_path_to_size)
        .PermitUncheckedError();
  }

  // For all files registered in backup
  std::vector<ComputeChecksumWorkItem> backup_verification_checksum_work_items;
  std::unordered_map<std::string, std::string>
      file_abs_path_to_checksum_hex_map;
  for (const auto& file_info : backup->GetFiles()) {
    const auto abs_path = GetAbsolutePath(file_info->filename);
    // check existence of the file
    if (curr_abs_path_to_size.find(abs_path) == curr_abs_path_to_size.end()) {
      return IOStatus::NotFound("File missing: " + abs_path);
    }
    // verify file size
    if (file_info->size != curr_abs_path_to_size[abs_path]) {
      std::string size_info("Expected file size is " +
                            std::to_string(file_info->size) +
                            " while found file size is " +
                            std::to_string(curr_abs_path_to_size[abs_path]));
      return IOStatus::Corruption("File corrupted: File size mismatch for " +
                                  abs_path + ": " + size_info);
    }
    if (verify_with_checksum && !file_info->checksum_hex.empty()) {
      const std::string filename = file_info->GetDbFileName();
      uint64_t number;
      FileType type;
      if (!ParseFileName(filename, &number, &type)) {
        // In case of checksum verification, file parsing and its' number
        // retrieval are not strictly required. Rather, it's just a best effort
        // to preserve all the file related metadata within the task scope.
        number = 0;
      }

      file_abs_path_to_checksum_hex_map[abs_path] = file_info->checksum_hex;
      WorkItem backup_file_work_item(
          abs_path, "" /* dst_path */, Temperature::kUnknown,
          Temperature::kUnknown /* dst_temperature */, "" /* contents */,
          backup_env_, nullptr /* dst_env */, EnvOptions(), false /* sync */,
          false /* use_fsync */, options_.backup_rate_limiter.get(),
          0 /* size_limit */, nullptr /* stats */, {} /* progress_callback */,
          kUnknownFileChecksumFuncName /* src_checksum_func_name */,
          "" /* src_checksum_hex */, "" /* db_id */, "" /* db_session_id*/,
          WorkItemType::ComputeChecksum);
      ComputeChecksumWorkItem backup_file_checksum_work_item(
          backup_file_work_item.result.get_future(), abs_path, number);

      ROCKS_LOG_INFO(options_.info_log,
                     "Scheduling checksum evaluation for %s...\n",
                     abs_path.c_str());
      copy_engine_->Submit(std::move(backup_file_work_item));
      backup_verification_checksum_work_items.push_back(
          std::move(backup_file_checksum_work_item));
    }
  }

  IOStatus io_s = IOStatus::OK();
  if (verify_with_checksum) {
    for (auto& item : backup_verification_checksum_work_items) {
      // Given the limitations of the existing simple thread pooling model
      // we deliberately decided to wait on each file checksum computation.
      // Please refer to the comment below for more.
      item.result.wait();
      auto result = item.result.get();
      if (result.io_status.ok()) {
        auto find_it = file_abs_path_to_checksum_hex_map.find(item.file_path);
        assert(find_it != file_abs_path_to_checksum_hex_map.end());
        if (result.checksum_hex == find_it->second) {
          ROCKS_LOG_INFO(options_.info_log,
                         "Checksum successfully validated for %s\n",
                         item.file_path.c_str());
          continue;
        }
      }

      std::string err_msg;
      if (!result.io_status.ok()) {
        err_msg =
            "Failed to compute checksum for " + item.file_path +
            ", IOStatus(code: ," + std::to_string(result.io_status.code()) +
            ", subcode: " + std::to_string(result.io_status.subcode()) + ")";
      } else {  // checksum mismatch
        err_msg =
            "File corruption! Checksum mismatch for " + item.file_path + ". " +
            "Expected: " + file_abs_path_to_checksum_hex_map[item.file_path] +
            ", got: " + result.checksum_hex;
      }

      ROCKS_LOG_WARN(options_.info_log, "%s", err_msg.c_str());
      if (io_s.ok()) {
        // Memoize only the first corruption for reporting purpose.
        io_s = IOStatus::Corruption(err_msg);
      } else {
        // Ideally, we want to bail out as early as possible upon encountering
        // the very first mismatch, which would not only reduce the observed
        // user latency, but also limit (potentially remote) read IO to the
        // absolute minimum and allow the thread pool to reclaim the resources
        // earlier. Even better, if we could cancel all in-progress threads!
        //
        // Unfortunately, with our current simple thread pool implementation
        // we do not have by-tag control / handle over running threads.
        // Having the choice of 1) returning to the caller earlier and having
        // dangling threads occupied in evaluating checksums in the background
        // and 2) waiting for all threads to finish, we choose 2) for cleaner
        // and more intuitive semantics.
        //
        // TODO: Reevaluate after onboarding backup engine to a more
        //       sophisticated thread pool abstraction.
      }
    }
  }

  return io_s;
}

// fname will always start with "/"
IOStatus BackupEngineImpl::AddBackupFileWorkItem(
    std::unordered_set<std::string>& live_dst_paths,
    std::deque<BackupAfterCopyOrCreateWorkItem>& backup_items_to_finish,
    std::deque<BackupWorkItemPair>* excludable_items, BackupID backup_id,
    bool shared, const std::string& src_dir, const std::string& fname,
    const EnvOptions& src_env_options, RateLimiter* rate_limiter,
    FileType file_type, uint64_t size_bytes, Statistics* stats,
    uint64_t size_limit, bool shared_checksum,
    std::function<void()> progress_callback, const std::string& contents,
    const std::string& src_checksum_func_name,
    const std::string& src_checksum_str, const Temperature src_temperature) {
  assert(contents.empty() != src_dir.empty());

  std::string src_path = src_dir + "/" + fname;
  std::string dst_relative;
  std::string dst_relative_tmp;
  std::string db_id;
  std::string db_session_id;
  // crc32c checksum in hex. empty == unavailable / unknown
  std::string checksum_hex;

  // Whenever a default checksum function name is passed in, we will compares
  // the corresponding checksum values after copying. Note that only table and
  // blob files may have a known checksum function name passed in.
  //
  // If no default checksum function name is passed in and db session id is not
  // available, we will calculate the checksum *before* copying in two cases
  // (we always calcuate checksums when copying or creating for any file types):
  // a) share_files_with_checksum is true and file type is table;
  // b) share_table_files is true and the file exists already.
  //
  // Step 0: Check if default checksum function name is passed in
  if (kDbFileChecksumFuncName == src_checksum_func_name) {
    if (src_checksum_str == kUnknownFileChecksum) {
      return status_to_io_status(
          Status::Aborted("Unknown checksum value for " + fname));
    }
    checksum_hex = ChecksumStrToHex(src_checksum_str);
  }

  // Step 1: Prepare the relative path to destination
  if (shared && shared_checksum) {
    if (GetNamingNoFlags() != BackupEngineOptions::kLegacyCrc32cAndFileSize &&
        file_type != kBlobFile) {
      // Prepare db_session_id to add to the file name
      Status s = GetFileDbIdentities(db_env_, src_env_options, src_path,
                                     src_temperature, rate_limiter, &db_id,
                                     &db_session_id);
      if (s.IsPathNotFound()) {
        // Retry with any temperature
        s = GetFileDbIdentities(db_env_, src_env_options, src_path,
                                Temperature::kUnknown, rate_limiter, &db_id,
                                &db_session_id);
      }
      if (s.IsNotFound()) {
        // db_id and db_session_id will be empty, which is OK for old files
      } else if (!s.ok()) {
        return status_to_io_status(std::move(s));
      }
    }
    // Calculate checksum if checksum and db session id are not available.
    // If db session id is available, we will not calculate the checksum
    // since the session id should suffice to avoid file name collision in
    // the shared_checksum directory.
    if (checksum_hex.empty() && db_session_id.empty()) {
      IOStatus io_s = copy_engine_->ReadFileAndComputeChecksum(
          src_path, db_fs_, src_env_options, size_limit, &checksum_hex,
          src_temperature);
      if (!io_s.ok()) {
        return io_s;
      }
    }
    if (size_bytes == std::numeric_limits<uint64_t>::max()) {
      return IOStatus::NotFound("File missing: " + src_path);
    }
    // dst_relative depends on the following conditions:
    // 1) the naming scheme is kUseDbSessionId,
    // 2) db_session_id is not empty,
    // 3) checksum is available in the DB manifest.
    // If 1,2,3) are satisfied, then dst_relative will be of the form:
    // shared_checksum/<file_number>_<checksum>_<db_session_id>.sst
    // If 1,2) are satisfied, then dst_relative will be of the form:
    // shared_checksum/<file_number>_<db_session_id>.sst
    // Otherwise, dst_relative is of the form
    // shared_checksum/<file_number>_<checksum>_<size>.sst
    //
    // For blob files, db_session_id is not supported with the blob file format.
    // It uses original/legacy naming scheme.
    // dst_relative will be of the form:
    // shared_checksum/<file_number>_<checksum>_<size>.blob
    dst_relative = GetSharedFileWithChecksum(fname, checksum_hex, size_bytes,
                                             db_session_id);
    dst_relative_tmp = GetSharedFileWithChecksumRel(dst_relative, true);
    dst_relative = GetSharedFileWithChecksumRel(dst_relative, false);
  } else if (shared) {
    dst_relative_tmp = GetSharedFileRel(fname, true);
    dst_relative = GetSharedFileRel(fname, false);
  } else {
    dst_relative = GetPrivateFileRel(backup_id, false, fname);
  }

  // We copy into `temp_dest_path` and, once finished, rename it to
  // `final_dest_path`. This allows files to atomically appear at
  // `final_dest_path`. We can copy directly to the final path when atomicity
  // is unnecessary, like for files in private backup directories.
  const std::string* copy_dest_path;
  std::string temp_dest_path;
  std::string final_dest_path = GetAbsolutePath(dst_relative);
  if (!dst_relative_tmp.empty()) {
    temp_dest_path = GetAbsolutePath(dst_relative_tmp);
    copy_dest_path = &temp_dest_path;
  } else {
    copy_dest_path = &final_dest_path;
  }

  // Step 2: Determine whether to copy or not
  // if it's shared, we also need to check if it exists -- if it does, no need
  // to copy it again.
  bool need_to_copy = true;
  // true if final_dest_path is the same path as another live file
  const bool same_path =
      live_dst_paths.find(final_dest_path) != live_dst_paths.end();

  bool file_exists = false;
  if (shared && !same_path) {
    // Should be in shared directory but not a live path, check existence in
    // shared directory
    IOStatus exist =
        backup_fs_->FileExists(final_dest_path, io_options_, nullptr);
    if (exist.ok()) {
      file_exists = true;
    } else if (exist.IsNotFound()) {
      file_exists = false;
    } else {
      return exist;
    }
  }

  if (!contents.empty()) {
    need_to_copy = false;
  } else if (shared && (same_path || file_exists)) {
    need_to_copy = false;
    auto find_result = backuped_file_infos_.find(dst_relative);
    if (find_result == backuped_file_infos_.end() && !same_path) {
      // file exists but not referenced
      ROCKS_LOG_INFO(
          options_.info_log,
          "%s already present, but not referenced by any backup. We will "
          "overwrite the file.",
          fname.c_str());
      need_to_copy = true;
      // Defer any failure reporting to when we try to write the file
      backup_fs_->DeleteFile(final_dest_path, io_options_, nullptr)
          .PermitUncheckedError();
    } else {
      // file exists and referenced
      if (checksum_hex.empty()) {
        // same_path should not happen for a standard DB, so OK to
        // read file contents to check for checksum mismatch between
        // two files from same DB getting same name.
        // For compatibility with future meta file that might not have
        // crc32c checksum available, consider it might be empty, but
        // we don't currently generate meta file without crc32c checksum.
        // Therefore we have to read & compute it if we don't have it.
        if (!same_path && !find_result->second->checksum_hex.empty()) {
          assert(find_result != backuped_file_infos_.end());
          // Note: to save I/O on incremental backups, we copy prior known
          // checksum of the file instead of reading entire file contents
          // to recompute it.
          checksum_hex = find_result->second->checksum_hex;
          // Regarding corruption detection, consider:
          // (a) the DB file is corrupt (since previous backup) and the backup
          // file is OK: we failed to detect, but the backup is safe. DB can
          // be repaired/restored once its corruption is detected.
          // (b) the backup file is corrupt (since previous backup) and the
          // db file is OK: we failed to detect, but the backup is corrupt.
          // CreateNewBackup should support fast incremental backups and
          // there's no way to support that without reading all the files.
          // We might add an option for extra checks on incremental backup,
          // but until then, use VerifyBackups to check existing backup data.
          // (c) file name collision with legitimately different content.
          // This is almost inconceivable with a well-generated DB session
          // ID, but even in that case, we double check the file sizes in
          // BackupMeta::AddFile.
        } else {
          IOStatus io_s = copy_engine_->ReadFileAndComputeChecksum(
              src_path, db_fs_, src_env_options, size_limit, &checksum_hex,
              src_temperature);
          if (!io_s.ok()) {
            return io_s;
          }
        }
      }
      if (!db_session_id.empty()) {
        ROCKS_LOG_INFO(options_.info_log,
                       "%s already present, with checksum %s, size %" PRIu64
                       " and DB session identity %s",
                       fname.c_str(), checksum_hex.c_str(), size_bytes,
                       db_session_id.c_str());
      } else {
        ROCKS_LOG_INFO(options_.info_log,
                       "%s already present, with checksum %s and size %" PRIu64,
                       fname.c_str(), checksum_hex.c_str(), size_bytes);
      }
    }
  }
  live_dst_paths.insert(final_dest_path);

  // Step 3: Add work item
  if (!contents.empty() || need_to_copy) {
    WorkItem copy_or_create_work_item(
        src_dir.empty() ? "" : src_path, *copy_dest_path, src_temperature,
        Temperature::kUnknown /*dst_temp*/, contents, db_env_, backup_env_,
        src_env_options, options_.sync, false /* use_fsync */, rate_limiter,
        size_limit, stats, progress_callback, src_checksum_func_name,
        checksum_hex, db_id, db_session_id);
    BackupAfterCopyOrCreateWorkItem after_copy_or_create_work_item(
        copy_or_create_work_item.result.get_future(), shared, need_to_copy,
        backup_env_, temp_dest_path, final_dest_path, dst_relative);
    if (excludable_items != nullptr && shared && shared_checksum &&
        need_to_copy) {
      ROCKS_LOG_INFO(options_.info_log, "Copying (if not excluded) %s to %s",
                     fname.c_str(), copy_dest_path->c_str());
      excludable_items->emplace_back(std::move(copy_or_create_work_item),
                                     std::move(after_copy_or_create_work_item));
    } else {
      // For files known not excluded, can start copying even before finishing
      // the checkpoint
      ROCKS_LOG_INFO(options_.info_log, "Copying %s to %s", fname.c_str(),
                     copy_dest_path->c_str());
      copy_engine_->Submit(std::move(copy_or_create_work_item));
      backup_items_to_finish.push_back(
          std::move(after_copy_or_create_work_item));
    }
  } else {
    std::promise<WorkItemResult> promise_result;
    BackupAfterCopyOrCreateWorkItem after_copy_or_create_work_item(
        promise_result.get_future(), shared, need_to_copy, backup_env_,
        temp_dest_path, final_dest_path, dst_relative);
    backup_items_to_finish.push_back(std::move(after_copy_or_create_work_item));
    WorkItemResult result;
    result.io_status = IOStatus::OK();
    result.size = size_bytes;
    result.checksum_hex = std::move(checksum_hex);
    result.db_id = std::move(db_id);
    result.db_session_id = std::move(db_session_id);
    promise_result.set_value(std::move(result));
  }
  return IOStatus::OK();
}

bool BackupEngineImpl::ShouldStopBackup() const {
  bool should_stop = stop_backup_.load(std::memory_order_acquire);
  TEST_SYNC_POINT_CALLBACK("BackupEngineImpl::ShouldStopBackup", &should_stop);
  return should_stop;
}

Status BackupEngineImpl::GetFileDbIdentities(Env* src_env,
                                             const EnvOptions& src_env_options,
                                             const std::string& file_path,
                                             Temperature file_temp,
                                             RateLimiter* rate_limiter,
                                             std::string* db_id,
                                             std::string* db_session_id) const {
  assert(db_id != nullptr || db_session_id != nullptr);

  Options options;
  options.env = src_env;
  SstFileDumper sst_reader(options, file_path, file_temp,
                           2 * 1024 * 1024
                           /* readahead_size */,
                           true /* verify_checksum */, false /* output_hex */,
                           false /* decode_blob_index */, src_env_options,
                           true /* silent */);

  const TableProperties* table_properties = nullptr;
  std::shared_ptr<const TableProperties> tp;
  Status s = sst_reader.getStatus();

  if (s.ok()) {
    // Try to get table properties from the table reader of sst_reader
    if (!sst_reader.ReadTableProperties(&tp).ok()) {
      // FIXME (peterd): this logic is untested and seems obsolete.
      // Try to use table properties from the initialization of sst_reader
      table_properties = sst_reader.GetInitTableProperties();
    } else {
      table_properties = tp.get();
      if (table_properties != nullptr && rate_limiter != nullptr) {
        // sizeof(*table_properties) is a sufficent but far-from-exact
        // approximation of read bytes due to metaindex block, std::string
        // properties and varint compression
        LoopRateLimitRequestHelper(sizeof(*table_properties), rate_limiter,
                                   Env::IO_LOW, nullptr /* stats */,
                                   RateLimiter::OpType::kRead);
      }
    }
  } else {
    ROCKS_LOG_INFO(options_.info_log, "Failed to read %s: %s",
                   file_path.c_str(), s.ToString().c_str());
    return s;
  }

  if (table_properties != nullptr) {
    if (db_id != nullptr) {
      db_id->assign(table_properties->db_id);
    }
    if (db_session_id != nullptr) {
      db_session_id->assign(table_properties->db_session_id);
      if (db_session_id->empty()) {
        s = Status::NotFound("DB session identity not found in " + file_path);
        ROCKS_LOG_INFO(options_.info_log, "%s", s.ToString().c_str());
        return s;
      }
    }
    return Status::OK();
  } else {
    s = Status::Corruption("Table properties missing in " + file_path);
    ROCKS_LOG_INFO(options_.info_log, "%s", s.ToString().c_str());
    return s;
  }
}

void BackupEngineImpl::InferDBFilesToRetainInRestore(
    const std::vector<std::pair<const BackupEngineImpl*, const FileInfo*>>&
        restore_file_infos,
    std::unordered_set<std::string>& unowned_backups, const std::string& db_dir,
    RestoreOptions::Mode mode,
    std::unordered_set<uint64_t>& files_to_keep) const {
  if (mode == RestoreOptions::Mode::kPurgeAllFiles) {
    return;
  }

  ROCKS_LOG_INFO(options_.info_log,
                 "Starting incremental restore evaluation in %" PRIu32 " mode",
                 mode);

  ROCKS_LOG_INFO(options_.info_log, "Constructing backup files mapping...");
  std::unordered_map<uint64_t,
                     std::pair<const BackupEngineImpl*, const FileInfo*>>
      file_num_to_engine_infos;
  for (const auto& engine_and_file_info : restore_file_infos) {
    uint64_t number;
    FileType type;

    std::string filename = engine_and_file_info.second->GetDbFileName();
    if (!ParseFileName(filename, &number, &type)) {
      continue;
    }

    // We only care to optimize restore for large files - like SSTs and blobs.
    // Blobs are only supported in kVerifyChecksum.
    if (type == kTableFile ||
        (type == kBlobFile && mode == RestoreOptions::Mode::kVerifyChecksum)) {
      file_num_to_engine_infos[number] = engine_and_file_info;
    }
  }

  ROCKS_LOG_INFO(
      options_.info_log,
      "Evaluating existing .sst%s files restore retention eligibility...",
      mode == RestoreOptions::Mode::kVerifyChecksum ? " and .blob files" : "");

  std::vector<std::string> children;
  db_fs_->GetChildren(db_dir, io_options_, &children, nullptr)
      .PermitUncheckedError();  // ignore errors
  std::vector<ComputeChecksumWorkItem> backup_files_compute_checksum_work_items;
  std::vector<ComputeChecksumWorkItem> db_files_compute_checksum_work_items;
  std::unordered_map<uint64_t, std::string> backup_file_num_to_checksum;
  for (const auto& f : children) {
    uint64_t number;
    FileType type;
    bool ok = ParseFileName(f, &number, &type);
    if (!ok) {
      // Couldn't parse existing file name. We deliberately choose to sliently
      // skip here to avoid noisy & excessive logging in user controlled envs.
      continue;
    }

    if (type != kTableFile && type != kBlobFile) {
      // We only care to optimize restore for large files - like SSTs / blobs.
      continue;
    }

    if (type == kBlobFile && mode != RestoreOptions::Mode::kVerifyChecksum) {
      // Blob files are only supported in kVerifyChecksum mode.
      continue;
    }

    uint64_t size_bytes = 0;
    std::string db_file_path = db_dir + "/" + f;
    IOStatus io_st = db_fs_->GetFileSize(db_file_path, io_options_, &size_bytes,
                                         nullptr /* dbg */);
    if (!io_st.ok()) {
      Log(options_.info_log,
          "Failed to get the file size for existing file: '%s'. IO status: %s",
          f.c_str(), io_st.ToString().c_str());
      continue;
    }

    RateLimiter* rate_limiter = options_.restore_rate_limiter.get();
    if (mode == RestoreOptions::Mode::kKeepLatestDbSessionIdFiles) {
      // On-disk existing db file names require direct file footer query
      // as they don't follow same naming convention as backups.
      std::string db_id;
      std::string db_session_id;
      Status s = GetFileDbIdentities(
          db_env_, EnvOptions() /* src_env_options */,
          db_file_path /* file_path */, Temperature::kUnknown /* src_temp */,
          rate_limiter, &db_id, &db_session_id);
      if (!s.ok()) {
        Log(options_.info_log,
            "Encountered IO error while obtaining db session id metadata for "
            "existing file '%s'.",
            db_file_path.c_str());
        continue;
      }

      const std::string checksum_hex;
      std::string shared_file_name = GenerateSharedFileWithDbSessionIdAndSize(
          f, size_bytes, db_session_id);
      bool found = false;
      const auto& f_ei = file_num_to_engine_infos.find(number);
      if (f_ei != file_num_to_engine_infos.end()) {
        found = f_ei->second.second->filename == shared_file_name;
      }

      if (!found) {
        const auto& uo_sst_bfn = unowned_backups.find(shared_file_name);
        if (uo_sst_bfn != unowned_backups.end()) {
          // Db file has been successfully associated with the excluded backup.
          unowned_backups.erase(shared_file_name);
          found = true;
        }
      }

      if (found) {
        files_to_keep.insert(number);

        ROCKS_LOG_INFO(options_.info_log,
                       "Existing db file '%s' is retained for restore.",
                       f.c_str());
      }
    } else if (mode == RestoreOptions::Mode::kVerifyChecksum) {
      const auto& f_ei = file_num_to_engine_infos.find(number);
      if (f_ei == file_num_to_engine_infos.end() ||
          f_ei->second.second->GetDbFileName() != f) {
        Log(options_.info_log,
            "Existing file '%s' is not present in the backup!", f.c_str());
        continue;
      }

      auto backup_engine_impl = f_ei->second.first;
      auto backup_file_info = f_ei->second.second;
      DBOptions db_options;
      std::string backup_file_path =
          backup_engine_impl->GetAbsolutePath(backup_file_info->filename);
      std::string backup_file_checksum = backup_file_info->checksum_hex;
      if (!backup_file_checksum.empty()) {
        backup_file_num_to_checksum[number] = backup_file_checksum;
      } else {
        // Backup file checksum is missing in the backup metadata.
        // Given explicit requirement, compute it asynchronously.
        EnvOptions backup_env_options;
        if (type == kBlobFile) {
          backup_engine_impl->backup_env_->OptimizeForBlobFileRead(
              backup_env_options, ImmutableDBOptions(db_options));
        } else if (type == kTableFile) {
          backup_engine_impl->backup_env_->OptimizeForCompactionTableRead(
              backup_env_options, ImmutableDBOptions(db_options));
        }

        WorkItem backup_file_work_item(
            backup_file_path, "" /* dst_path */, backup_file_info->temp,
            Temperature::kUnknown /* dst_temperature */, "" /* contents */,
            backup_engine_impl->backup_env_, nullptr /* dst_env */,
            backup_env_options, false /* sync */, false /* use_fsync */,
            options_.restore_rate_limiter.get(), 0 /* size_limit */,
            nullptr /* stats */, {} /* progress_callback */,
            kUnknownFileChecksumFuncName /* src_checksum_func_name */,
            "" /* src_checksum_hex */, "" /* db_id */, "" /* db_session_id*/,
            WorkItemType::ComputeChecksum);

        ComputeChecksumWorkItem backup_file_checksum_work_item(
            backup_file_work_item.result.get_future(),
            backup_file_info->filename, number);

        copy_engine_->Submit(std::move(backup_file_work_item));
        backup_files_compute_checksum_work_items.push_back(
            std::move(backup_file_checksum_work_item));

        Log(options_.info_log,
            "Checksum is missing in '%s' backup file metadata."
            "Scheduled async computation...",
            backup_file_info->filename.c_str());
      }

      // Unconditionally compute checksum for existing file.
      EnvOptions db_env_options;
      if (type == kBlobFile) {
        db_env_->OptimizeForBlobFileRead(db_env_options,
                                         ImmutableDBOptions(db_options));
      } else if (type == kTableFile) {
        db_env_->OptimizeForCompactionTableRead(db_env_options,
                                                ImmutableDBOptions(db_options));
      }

      WorkItem db_file_work_item(
          db_file_path, "" /* dst_path */, backup_file_info->temp,
          Temperature::kUnknown /* dst_temperature */, "" /* contents */,
          db_env_, nullptr /* dst_env */, db_env_options, false /* sync */,
          false /* use_fsync */, options_.restore_rate_limiter.get(),
          0 /* size_limit */, nullptr /* stats */, {} /* progress_callback */,
          kUnknownFileChecksumFuncName /* src_checksum_func_name */,
          "" /* src_checksum_hex */, "" /* db_id */, "" /* db_session_id*/,
          WorkItemType::ComputeChecksum);

      ComputeChecksumWorkItem db_file_checksum_work_item(
          db_file_work_item.result.get_future(), db_file_path, number);
      copy_engine_->Submit(std::move(db_file_work_item));
      db_files_compute_checksum_work_items.push_back(
          std::move(db_file_checksum_work_item));

      Log(options_.info_log,
          "Schedule async checksum computation for file '%s'", f.c_str());
    }
  }

  if (mode == RestoreOptions::Mode::kVerifyChecksum) {
    // First loop through checksum computation results for backup files.
    for (auto& item : backup_files_compute_checksum_work_items) {
      item.result.wait();
      auto result = item.result.get();
      IOStatus item_io_status = result.io_status;
      if (!item_io_status.ok()) {
        // Failed computation for backup file will result in purging
        // the existing file and restoring the backup file.
        Log(options_.info_log,
            "Encountered IO error while computing checksum for "
            "backup file '%s'.",
            item.file_path.c_str());
        continue;
      }

      backup_file_num_to_checksum[item.file_number] = result.checksum_hex;
    }

    // Loop through db files checksum computation results.
    for (auto& item : db_files_compute_checksum_work_items) {
      item.result.wait();
      auto result = item.result.get();
      IOStatus item_io_status = result.io_status;
      if (!item_io_status.ok()) {
        // Failed computation for existing file will result in purging
        // and restoring it from the corresponding backup file.
        Log(options_.info_log,
            "Encountered IO error while computing checksum for "
            "existing file '%s'.",
            item.file_path.c_str());
        continue;
      }

      auto it = backup_file_num_to_checksum.find(item.file_number);
      if (it == backup_file_num_to_checksum.end()) {
        Log(options_.info_log,
            "Failed to find backup file checksum for existing file '%s'.",
            item.file_path.c_str());
        continue;
      }

      if (it->second != result.checksum_hex) {
        Log(options_.info_log,
            "Checksum mismatch between backup file and existing file '%s'.",
            item.file_path.c_str());
        continue;
      }

      files_to_keep.insert(item.file_number);

      Log(options_.info_log, "Existing file '%s' is retained for restore.",
          item.file_path.c_str());
    }
  }

  ROCKS_LOG_INFO(options_.info_log,
                 "Done with incremental restore evaluation. "
                 "Retained %zu files.",
                 files_to_keep.size());
}

void BackupEngineImpl::DeleteChildren(
    const std::string& dir, const std::unordered_set<uint64_t>& files_to_keep,
    uint32_t file_type_filter) const {
  std::vector<std::string> children;
  db_fs_->GetChildren(dir, io_options_, &children, nullptr)
      .PermitUncheckedError();  // ignore errors

  for (const auto& f : children) {
    uint64_t number;
    FileType type;
    bool ok = ParseFileName(f, &number, &type);
    if (ok && (files_to_keep.find(number) != files_to_keep.end())) {
      // don't delete file with this number.
      continue;
    }
    if (ok && (file_type_filter & (1 << type))) {
      // don't delete this file type.
      continue;
    }
    db_fs_->DeleteFile(dir + "/" + f, io_options_, nullptr)
        .PermitUncheckedError();  // ignore errors
  }
}

IOStatus BackupEngineImpl::ReadChildFileCurrentSizes(
    const std::string& dir, const std::shared_ptr<FileSystem>& fs,
    std::unordered_map<std::string, uint64_t>* result) const {
  assert(result != nullptr);
  std::vector<Env::FileAttributes> files_attrs;
  IOStatus io_status = fs->FileExists(dir, io_options_, nullptr);
  if (io_status.ok()) {
    io_status =
        fs->GetChildrenFileAttributes(dir, io_options_, &files_attrs, nullptr);
  } else if (io_status.IsNotFound()) {
    // Insert no entries can be considered success
    io_status = IOStatus::OK();
  }
  const bool slash_needed = dir.empty() || dir.back() != '/';
  for (const auto& file_attrs : files_attrs) {
    result->emplace(dir + (slash_needed ? "/" : "") + file_attrs.name,
                    file_attrs.size_bytes);
  }
  return io_status;
}

IOStatus BackupEngineImpl::GarbageCollect() {
  assert(!read_only_);

  // We will make a best effort to remove all garbage even in the presence
  // of inconsistencies or I/O failures that inhibit finding garbage.
  IOStatus overall_status = IOStatus::OK();
  // If all goes well, we don't need another auto-GC this session
  might_need_garbage_collect_ = false;

  ROCKS_LOG_INFO(options_.info_log, "Starting garbage collection");

  // delete obsolete shared files
  for (bool with_checksum : {false, true}) {
    std::vector<std::string> shared_children;
    {
      std::string shared_path;
      if (with_checksum) {
        shared_path = GetAbsolutePath(GetSharedFileWithChecksumRel());
      } else {
        shared_path = GetAbsolutePath(GetSharedFileRel());
      }
      IOStatus io_s = backup_fs_->FileExists(shared_path, io_options_, nullptr);
      if (io_s.ok()) {
        io_s = backup_fs_->GetChildren(shared_path, io_options_,
                                       &shared_children, nullptr);
      } else if (io_s.IsNotFound()) {
        io_s = IOStatus::OK();
      }
      if (!io_s.ok()) {
        overall_status = io_s;
        // Trying again later might work
        might_need_garbage_collect_ = true;
      }
    }
    for (auto& child : shared_children) {
      std::string rel_fname;
      if (with_checksum) {
        rel_fname = GetSharedFileWithChecksumRel(child);
      } else {
        rel_fname = GetSharedFileRel(child);
      }
      auto child_itr = backuped_file_infos_.find(rel_fname);
      // if it's not refcounted, delete it
      if (child_itr == backuped_file_infos_.end() ||
          child_itr->second->refs == 0) {
        // this might be a directory, but DeleteFile will just fail in that
        // case, so we're good
        IOStatus io_s = backup_fs_->DeleteFile(GetAbsolutePath(rel_fname),
                                               io_options_, nullptr);
        ROCKS_LOG_INFO(options_.info_log, "Deleting %s -- %s",
                       rel_fname.c_str(), io_s.ToString().c_str());
        backuped_file_infos_.erase(rel_fname);
        if (!io_s.ok()) {
          // Trying again later might work
          might_need_garbage_collect_ = true;
        }
      }
    }
  }

  // delete obsolete private files
  std::vector<std::string> private_children;
  {
    IOStatus io_s =
        backup_fs_->GetChildren(GetAbsolutePath(kPrivateDirName), io_options_,
                                &private_children, nullptr);
    if (!io_s.ok()) {
      overall_status = io_s;
      // Trying again later might work
      might_need_garbage_collect_ = true;
    }
  }
  for (auto& child : private_children) {
    BackupID backup_id = 0;
    bool tmp_dir = child.find(".tmp") != std::string::npos;
    sscanf(child.c_str(), "%u", &backup_id);
    if (!tmp_dir &&  // if it's tmp_dir, delete it
        (backup_id == 0 || backups_.find(backup_id) != backups_.end())) {
      // it's either not a number or it's still alive. continue
      continue;
    }
    // here we have to delete the dir and all its children
    std::string full_private_path =
        GetAbsolutePath(GetPrivateFileRel(backup_id));
    std::vector<std::string> subchildren;
    if (backup_fs_
            ->GetChildren(full_private_path, io_options_, &subchildren, nullptr)
            .ok()) {
      for (auto& subchild : subchildren) {
        IOStatus io_s = backup_fs_->DeleteFile(full_private_path + subchild,
                                               io_options_, nullptr);
        ROCKS_LOG_INFO(options_.info_log, "Deleting %s -- %s",
                       (full_private_path + subchild).c_str(),
                       io_s.ToString().c_str());
        if (!io_s.ok()) {
          // Trying again later might work
          might_need_garbage_collect_ = true;
        }
      }
    }
    // finally delete the private dir
    IOStatus io_s =
        backup_fs_->DeleteDir(full_private_path, io_options_, nullptr);
    ROCKS_LOG_INFO(options_.info_log, "Deleting dir %s -- %s",
                   full_private_path.c_str(), io_s.ToString().c_str());
    if (!io_s.ok()) {
      // Trying again later might work
      might_need_garbage_collect_ = true;
    }
  }

  assert(overall_status.ok() || might_need_garbage_collect_);
  return overall_status;
}

// ------- BackupMeta class --------

IOStatus BackupEngineImpl::BackupMeta::AddFile(
    std::shared_ptr<FileInfo> file_info) {
  auto itr = file_infos_->find(file_info->filename);
  if (itr == file_infos_->end()) {
    auto ret = file_infos_->insert({file_info->filename, file_info});
    if (ret.second) {
      itr = ret.first;
      itr->second->refs = 1;
    } else {
      // if this happens, something is seriously wrong
      return IOStatus::Corruption("In memory metadata insertion error");
    }
  } else {
    // Compare sizes, because we scanned that off the filesystem on both
    // ends. This is like a check in VerifyBackup.
    if (itr->second->size != file_info->size) {
      std::string msg = "Size mismatch for existing backup file: ";
      msg.append(file_info->filename);
      msg.append(" Size in backup is " + std::to_string(itr->second->size) +
                 " while size in DB is " + std::to_string(file_info->size));
      msg.append(
          " If this DB file checks as not corrupt, try deleting old"
          " backups or backing up to a different backup directory.");
      return IOStatus::Corruption(msg);
    }
    if (file_info->checksum_hex.empty()) {
      // No checksum available to check
    } else if (itr->second->checksum_hex.empty()) {
      // Remember checksum if newly acquired
      itr->second->checksum_hex = file_info->checksum_hex;
    } else if (itr->second->checksum_hex != file_info->checksum_hex) {
      // Note: to save I/O, these will be equal trivially on already backed
      // up files that don't have the checksum in their name. And it should
      // never fail for files that do have checksum in their name.

      // Should never reach here, but produce an appropriate corruption
      // message in case we do in a release build.
      assert(false);
      std::string msg = "Checksum mismatch for existing backup file: ";
      msg.append(file_info->filename);
      msg.append(" Expected checksum is " + itr->second->checksum_hex +
                 " while computed checksum is " + file_info->checksum_hex);
      msg.append(
          " If this DB file checks as not corrupt, try deleting old"
          " backups or backing up to a different backup directory.");
      return IOStatus::Corruption(msg);
    }
    ++itr->second->refs;  // increase refcount if already present
  }

  size_ += file_info->size;
  files_.push_back(itr->second);

  return IOStatus::OK();
}

IOStatus BackupEngineImpl::BackupMeta::Delete(bool delete_meta) {
  IOStatus io_s;
  for (const auto& file : files_) {
    --file->refs;  // decrease refcount
  }
  files_.clear();
  // delete meta file
  if (delete_meta) {
    io_s = fs_->FileExists(meta_filename_, iooptions_, nullptr);
    if (io_s.ok()) {
      io_s = fs_->DeleteFile(meta_filename_, iooptions_, nullptr);
    } else if (io_s.IsNotFound()) {
      io_s = IOStatus::OK();  // nothing to delete
    }
  }
  timestamp_ = 0;
  return io_s;
}

// Constants for backup meta file schema (see LoadFromFile)
const std::string kSchemaVersionPrefix{"schema_version "};
const std::string kFooterMarker{"// FOOTER"};

const std::string kAppMetaDataFieldName{"metadata"};

// WART: The checksums are crc32c but named "crc32"
const std::string kFileCrc32cFieldName{"crc32"};
const std::string kFileSizeFieldName{"size"};
const std::string kTemperatureFieldName{"temp"};
const std::string kExcludedFieldName{"ni::excluded"};

// Marks a (future) field that should cause failure if not recognized.
// Other fields are assumed to be ignorable. For example, in the future
// we might add
//  ni::file_name_escape uri_percent
// to indicate all file names have had spaces and special characters
// escaped using a URI percent encoding.
const std::string kNonIgnorableFieldPrefix{"ni::"};

// Each backup meta file is of the format (schema version 1):
//----------------------------------------------------------
// <timestamp>
// <seq number>
// metadata <metadata> (optional)
// <number of files>
// <file1> crc32 <crc32c_as_unsigned_decimal>
// <file2> crc32 <crc32c_as_unsigned_decimal>
// ...
//----------------------------------------------------------
//
// For schema version 2.x:
//----------------------------------------------------------
// schema_version <ver>
// <timestamp>
// <seq number>
// [<field name> <field data>]
// ...
// <number of files>
// <file1>( <field name> <field data no spaces>)*
// <file2>( <field name> <field data no spaces>)*
// ...
// [// FOOTER]
// [<field name> <field data>]
// ...
//----------------------------------------------------------
// where
// <ver> ::= [0-9]+([.][0-9]+)
// <field name> ::= [A-Za-z_][A-Za-z_0-9.]+
// <field data> is anything but newline
// <field data no spaces> is anything but space and newline
// Although "// FOOTER" wouldn't strictly be required as a delimiter
// given the number of files is included, it is there for parsing
// sanity in case of corruption. It is only required if followed
// by footer fields, such as a checksum of the meta file (so far).
// Unrecognized fields are ignored, to support schema evolution on
// non-critical features with forward compatibility. Update schema
// major version for breaking changes. Schema minor versions are indicated
// only for diagnostic/debugging purposes.
//
// Fields in schema version 2.0:
// * Top-level meta fields:
//   * Only "metadata" as in schema version 1
// * File meta fields:
//   * "crc32" - a crc32c checksum as in schema version 1
//   * "size" - the size of the file (new)
// * Footer meta fields:
//   * None yet (future use for meta file checksum anticipated)
//
IOStatus BackupEngineImpl::BackupMeta::LoadFromFile(
    const std::string& backup_dir,
    const std::unordered_map<std::string, uint64_t>& abs_path_to_size,
    RateLimiter* rate_limiter, Logger* info_log,
    std::unordered_set<std::string>* reported_ignored_fields) {
  assert(reported_ignored_fields);
  assert(Empty());

  std::unique_ptr<LineFileReader> backup_meta_reader;
  {
    IOStatus io_s = LineFileReader::Create(fs_, meta_filename_, FileOptions(),
                                           &backup_meta_reader,
                                           nullptr /* dbg */, rate_limiter);
    if (!io_s.ok()) {
      return io_s;
    }
  }

  // If we don't read an explicit schema_version, that implies version 1,
  // which is what we call the original backup meta schema.
  int schema_major_version = 1;

  // Failures handled at the end
  std::string line;
  if (backup_meta_reader->ReadLine(&line,
                                   Env::IO_LOW /* rate_limiter_priority */)) {
    if (StartsWith(line, kSchemaVersionPrefix)) {
      std::string ver = line.substr(kSchemaVersionPrefix.size());
      if (ver == "2" || StartsWith(ver, "2.")) {
        schema_major_version = 2;
      } else {
        return IOStatus::NotSupported(
            "Unsupported/unrecognized schema version: " + ver);
      }
      line.clear();
    } else if (line.empty()) {
      return IOStatus::Corruption("Unexpected empty line");
    }
  }
  if (!line.empty()) {
    timestamp_ = std::strtoull(line.c_str(), nullptr, /*base*/ 10);
  } else if (backup_meta_reader->ReadLine(
                 &line, Env::IO_LOW /* rate_limiter_priority */)) {
    timestamp_ = std::strtoull(line.c_str(), nullptr, /*base*/ 10);
  }
  if (backup_meta_reader->ReadLine(&line,
                                   Env::IO_LOW /* rate_limiter_priority */)) {
    sequence_number_ = std::strtoull(line.c_str(), nullptr, /*base*/ 10);
  }
  uint32_t num_files = UINT32_MAX;
  while (backup_meta_reader->ReadLine(
      &line, Env::IO_LOW /* rate_limiter_priority */)) {
    if (line.empty()) {
      return IOStatus::Corruption("Unexpected empty line");
    }
    // Number -> number of files -> exit loop reading optional meta fields
    if (line[0] >= '0' && line[0] <= '9') {
      num_files = static_cast<uint32_t>(strtoul(line.c_str(), nullptr, 10));
      break;
    }
    // else, must be a meta field assignment
    auto space_pos = line.find_first_of(' ');
    if (space_pos == std::string::npos) {
      return IOStatus::Corruption("Expected number of files or meta field");
    }
    std::string field_name = line.substr(0, space_pos);
    std::string field_data = line.substr(space_pos + 1);
    if (field_name == kAppMetaDataFieldName) {
      // app metadata present
      bool decode_success = Slice(field_data).DecodeHex(&app_metadata_);
      if (!decode_success) {
        return IOStatus::Corruption(
            "Failed to decode stored hex encoded app metadata");
      }
    } else if (schema_major_version < 2) {
      return IOStatus::Corruption("Expected number of files or \"" +
                                  kAppMetaDataFieldName + "\" field");
    } else if (StartsWith(field_name, kNonIgnorableFieldPrefix)) {
      return IOStatus::NotSupported("Unrecognized non-ignorable meta field " +
                                    field_name + " (from future version?)");
    } else {
      // Warn the first time we see any particular unrecognized meta field
      if (reported_ignored_fields->insert("meta:" + field_name).second) {
        ROCKS_LOG_WARN(info_log, "Ignoring unrecognized backup meta field %s",
                       field_name.c_str());
      }
    }
  }
  std::vector<std::shared_ptr<FileInfo>> files;
  bool footer_present = false;
  while (backup_meta_reader->ReadLine(
      &line, Env::IO_LOW /* rate_limiter_priority */)) {
    std::vector<std::string> components = StringSplit(line, ' ');

    if (components.size() < 1) {
      return IOStatus::Corruption("Empty line instead of file entry.");
    }
    if (schema_major_version >= 2 && components.size() == 2 &&
        line == kFooterMarker) {
      footer_present = true;
      break;
    }

    const std::string& filename = components[0];

    if (schema_major_version >= 2) {
      if (components.size() % 2 != 1) {
        return IOStatus::Corruption(
            "Bad number of line components for file entry.");
      }
    } else {
      // Check restricted original schema
      if (components.size() < 3) {
        return IOStatus::Corruption("File checksum is missing for " + filename +
                                    " in " + meta_filename_);
      }
      if (components[1] != kFileCrc32cFieldName) {
        return IOStatus::Corruption("Unknown checksum type for " + filename +
                                    " in " + meta_filename_);
      }
      if (components.size() > 3) {
        return IOStatus::Corruption("Extra data for entry " + filename +
                                    " in " + meta_filename_);
      }
    }

    std::optional<uint64_t> expected_size{};
    std::string checksum_hex;
    Temperature temp = Temperature::kUnknown;
    bool excluded = false;
    for (unsigned i = 1; i < components.size(); i += 2) {
      const std::string& field_name = components[i];
      const std::string& field_data = components[i + 1];

      if (field_name == kFileCrc32cFieldName) {
        uint32_t checksum_value =
            static_cast<uint32_t>(strtoul(field_data.c_str(), nullptr, 10));
        if (field_data != std::to_string(checksum_value)) {
          return IOStatus::Corruption("Invalid checksum value for " + filename +
                                      " in " + meta_filename_);
        }
        checksum_hex = ChecksumInt32ToHex(checksum_value);
      } else if (field_name == kFileSizeFieldName) {
        expected_size = std::strtoull(field_data.c_str(), nullptr, /*base*/ 10);
      } else if (field_name == kTemperatureFieldName) {
        auto iter = temperature_string_map.find(field_data);
        if (iter != temperature_string_map.end()) {
          temp = iter->second;
        } else {
          // Could report corruption, but in case of new temperatures added
          // in future, letting those map to kUnknown which should generally
          // be safe.
          temp = Temperature::kUnknown;
        }
      } else if (field_name == kExcludedFieldName) {
        if (field_data == "true") {
          excluded = true;
        } else if (field_data == "false") {
          excluded = false;
        } else {
          return IOStatus::NotSupported("Unrecognized value \"" + field_data +
                                        "\" for field " + field_name);
        }
      } else if (StartsWith(field_name, kNonIgnorableFieldPrefix)) {
        return IOStatus::NotSupported("Unrecognized non-ignorable file field " +
                                      field_name + " (from future version?)");
      } else {
        // Warn the first time we see any particular unrecognized file field
        if (reported_ignored_fields->insert("file:" + field_name).second) {
          ROCKS_LOG_WARN(info_log, "Ignoring unrecognized backup file field %s",
                         field_name.c_str());
        }
      }
    }

    if (excluded) {
      excluded_files_.emplace_back(filename);
    } else {
      // Verify file exists, with expected size
      std::string abs_path = backup_dir + "/" + filename;
      auto e = abs_path_to_size.find(abs_path);
      if (e == abs_path_to_size.end()) {
        return IOStatus::Corruption(
            "Pathname in meta file not found on disk: " + abs_path);
      }
      uint64_t actual_size = e->second;
      if (expected_size.has_value() && *expected_size != actual_size) {
        return IOStatus::Corruption("For file " + filename + " expected size " +
                                    std::to_string(*expected_size) +
                                    " but found size" +
                                    std::to_string(actual_size));
      }

      // NOTE: FileInfo will be coalesced for sharing later (AddFile below)
      files.emplace_back(
          std::make_shared<FileInfo>(filename, actual_size, checksum_hex,
                                     /*id*/ "", /*sid*/ "", temp));
    }
  }

  if (footer_present) {
    assert(schema_major_version >= 2);
    while (backup_meta_reader->ReadLine(
        &line, Env::IO_LOW /* rate_limiter_priority */)) {
      if (line.empty()) {
        return IOStatus::Corruption("Unexpected empty line");
      }
      auto space_pos = line.find_first_of(' ');
      if (space_pos == std::string::npos) {
        return IOStatus::Corruption("Expected footer field");
      }
      std::string field_name = line.substr(0, space_pos);
      std::string field_data = line.substr(space_pos + 1);
      if (StartsWith(field_name, kNonIgnorableFieldPrefix)) {
        return IOStatus::NotSupported("Unrecognized non-ignorable field " +
                                      field_name + " (from future version?)");
      } else if (reported_ignored_fields->insert("footer:" + field_name)
                     .second) {
        // Warn the first time we see any particular unrecognized footer field
        ROCKS_LOG_WARN(info_log,
                       "Ignoring unrecognized backup meta footer field %s",
                       field_name.c_str());
      }
    }
  }

  {
    IOStatus io_s = backup_meta_reader->GetStatus();
    if (!io_s.ok()) {
      return io_s;
    }
  }

  if (num_files != files.size()) {
    return IOStatus::Corruption(
        "Inconsistent number of files or missing/incomplete header in " +
        meta_filename_);
  }

  files_.reserve(files.size());
  for (const auto& file_info : files) {
    IOStatus io_s = AddFile(file_info);
    if (!io_s.ok()) {
      return io_s;
    }
  }

  return IOStatus::OK();
}

const std::vector<std::string> minor_version_strings{
    "",  // invalid major version 0
    "",  // implicit major version 1
    "2.1",
};

IOStatus BackupEngineImpl::BackupMeta::StoreToFile(
    bool sync, int schema_version,
    const TEST_BackupMetaSchemaOptions* schema_test_options) {
  if (schema_version < 1) {
    return IOStatus::InvalidArgument(
        "BackupEngineOptions::schema_version must be >= 1");
  }
  if (schema_version > static_cast<int>(minor_version_strings.size() - 1)) {
    return IOStatus::NotSupported(
        "Only BackupEngineOptions::schema_version <= " +
        std::to_string(minor_version_strings.size() - 1) + " is supported");
  }
  std::string ver = minor_version_strings[schema_version];

  // Need schema_version >= 2 for TEST_BackupMetaSchemaOptions
  assert(schema_version >= 2 || schema_test_options == nullptr);

  IOStatus io_s;
  std::unique_ptr<FSWritableFile> backup_meta_file;
  FileOptions file_options;
  file_options.use_mmap_writes = false;
  file_options.use_direct_writes = false;
  io_s = fs_->NewWritableFile(meta_tmp_filename_, file_options,
                              &backup_meta_file, nullptr);
  if (!io_s.ok()) {
    return io_s;
  }

  std::ostringstream buf;
  if (schema_test_options) {
    // override for testing
    ver = schema_test_options->version;
  }
  if (!ver.empty()) {
    assert(schema_version >= 2);
    buf << kSchemaVersionPrefix << ver << "\n";
  }
  buf << static_cast<unsigned long long>(timestamp_) << "\n";
  buf << sequence_number_ << "\n";

  if (!app_metadata_.empty()) {
    std::string hex_encoded_metadata =
        Slice(app_metadata_).ToString(/* hex */ true);
    buf << kAppMetaDataFieldName << " " << hex_encoded_metadata << "\n";
  }
  if (schema_test_options) {
    for (auto& e : schema_test_options->meta_fields) {
      buf << e.first << " " << e.second << "\n";
    }
  }
  buf << files_.size() << "\n";

  for (const auto& file : files_) {
    buf << file->filename;
    if (schema_test_options == nullptr ||
        schema_test_options->crc32c_checksums) {
      // use crc32c for now, switch to something else if needed
      buf << " " << kFileCrc32cFieldName << " "
          << ChecksumHexToInt32(file->checksum_hex);
    }
    if (schema_version >= 2 && file->temp != Temperature::kUnknown) {
      buf << " " << kTemperatureFieldName << " "
          << temperature_to_string[file->temp];
    }
    if (schema_test_options && schema_test_options->file_sizes) {
      buf << " " << kFileSizeFieldName << " " << std::to_string(file->size);
    }
    if (schema_test_options) {
      for (auto& e : schema_test_options->file_fields) {
        buf << " " << e.first << " " << e.second;
      }
    }
    buf << "\n";
  }

  for (const auto& file : excluded_files_) {
    assert(schema_version >= 2);
    buf << file.relative_file << " " << kExcludedFieldName << " true\n";
  }

  if (schema_test_options && !schema_test_options->footer_fields.empty()) {
    buf << kFooterMarker << "\n";
    for (auto& e : schema_test_options->footer_fields) {
      buf << e.first << " " << e.second << "\n";
    }
  }

  io_s = backup_meta_file->Append(Slice(buf.str()), iooptions_, nullptr);
  IOSTATS_ADD(bytes_written, buf.str().size());
  if (io_s.ok() && sync) {
    io_s = backup_meta_file->Sync(iooptions_, nullptr);
  }
  if (io_s.ok()) {
    io_s = backup_meta_file->Close(iooptions_, nullptr);
  }
  if (io_s.ok()) {
    io_s = fs_->RenameFile(meta_tmp_filename_, meta_filename_, iooptions_,
                           nullptr);
  }
  return io_s;
}
}  // namespace

IOStatus BackupEngineReadOnly::Open(const BackupEngineOptions& options,
                                    Env* env,
                                    BackupEngineReadOnly** backup_engine_ptr) {
  if (options.destroy_old_data) {
    return IOStatus::InvalidArgument(
        "Can't destroy old data with ReadOnly BackupEngine");
  }
  std::unique_ptr<BackupEngineImplThreadSafe> backup_engine(
      new BackupEngineImplThreadSafe(options, env, true /*read_only*/));
  auto s = backup_engine->Initialize();
  if (!s.ok()) {
    *backup_engine_ptr = nullptr;
    return s;
  }
  *backup_engine_ptr = backup_engine.release();
  return IOStatus::OK();
}

void TEST_SetBackupMetaSchemaOptions(
    BackupEngine* engine, const TEST_BackupMetaSchemaOptions& options) {
  BackupEngineImplThreadSafe* impl =
      static_cast_with_check<BackupEngineImplThreadSafe>(engine);
  impl->TEST_SetBackupMetaSchemaOptions(options);
}

void TEST_SetDefaultRateLimitersClock(
    BackupEngine* engine,
    const std::shared_ptr<SystemClock>& backup_rate_limiter_clock,
    const std::shared_ptr<SystemClock>& restore_rate_limiter_clock) {
  BackupEngineImplThreadSafe* impl =
      static_cast_with_check<BackupEngineImplThreadSafe>(engine);
  impl->TEST_SetDefaultRateLimitersClock(backup_rate_limiter_clock,
                                         restore_rate_limiter_clock);
}
}  // namespace ROCKSDB_NAMESPACE
