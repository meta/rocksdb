//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <climits>
#include <cstdint>
#include <forward_list>
#include <functional>
#include <map>
#include <string>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "rocksdb/metadata.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/checkpoint.h"

namespace ROCKSDB_NAMESPACE {
class BackupEngineReadOnlyBase;
class BackupEngine;

// The default DB file checksum function name.
constexpr char kDbFileChecksumFuncName[] = "FileChecksumCrc32c";
// The default BackupEngine file checksum function name.
constexpr char kBackupFileChecksumFuncName[] = "crc32c";

struct BackupEngineOptions : public CheckpointOrBackupEngineOptions {
  // Where to keep the backup files. Has to be different than dbname_
  // Best to set this to dbname_ + "/backups"
  // Required
  std::string backup_dir;

  // Backup Env object. It will be used for backup file I/O. If it's
  // nullptr, backups will be written out using DBs Env. If it's
  // non-nullptr, backup's I/O will be performed using this object.
  // Default: nullptr
  Env* backup_env;

  // share_table_files supports table and blob files.
  //
  // If share_table_files == true, the backup directory will share table and
  // blob files among backups, to save space among backups of the same DB and to
  // enable incremental backups by only copying new files.
  // If share_table_files == false, each backup will be on its own and will not
  // share any data with other backups.
  //
  // default: true
  bool share_table_files;

  // If sync == true, we can guarantee you'll get consistent backup and
  // restore even on a machine crash/reboot. Backup and restore processes are
  // slower with sync enabled. If sync == false, we can only guarantee that
  // other previously synced backups and restores are not modified while
  // creating a new one.
  // Default: true
  bool sync;

  // If true, it will delete whatever backups there are already
  // Default: false
  bool destroy_old_data;

  // If false, we won't backup log files. This option can be useful for backing
  // up in-memory databases where log file are persisted, but table files are in
  // memory.
  // Default: true
  bool backup_log_files;

  // Max bytes that can be transferred in a second during restore.
  // If 0, go as fast as you can
  // This limit only applies to writes. To also limit reads,
  // a rate limiter able to also limit reads (e.g, its mode = kAllIo)
  // have to be passed in through the option "restore_rate_limiter"
  // Default: 0
  uint64_t restore_rate_limit;

  // Restore rate limiter. Used to control transfer speed during restore. If
  // this is not null, restore_rate_limit is ignored.
  // Default: nullptr
  std::shared_ptr<RateLimiter> restore_rate_limiter{nullptr};

  // share_files_with_checksum supports table and blob files.
  //
  // Only used if share_table_files is set to true. Setting to false is
  // DEPRECATED and potentially dangerous because in that case BackupEngine
  // can lose data if backing up databases with distinct or divergent
  // history, for example if restoring from a backup other than the latest,
  // writing to the DB, and creating another backup. Setting to true (default)
  // prevents these issues by ensuring that different table files (SSTs) and
  // blob files with the same number are treated as distinct. See
  // share_files_with_checksum_naming and ShareFilesNaming.
  //
  // Default: true
  bool share_files_with_checksum;

  // During backup user can get callback every time next
  // callback_trigger_interval_size bytes being copied.
  // Default: 4194304
  uint64_t callback_trigger_interval_size;

  // For BackupEngineReadOnly, Open() will open at most this many of the
  // latest non-corrupted backups.
  //
  // Note: this setting is ignored (behaves like INT_MAX) for any kind of
  // writable BackupEngine because it would inhibit accounting for shared
  // files for proper backup deletion, including purging any incompletely
  // created backups on creation of a new backup.
  //
  // Default: INT_MAX
  int max_valid_backups_to_open;

  // ShareFilesNaming describes possible naming schemes for backup
  // table and blob file names when they are stored in the
  // shared_checksum directory (i.e., both share_table_files and
  // share_files_with_checksum are true).
  enum ShareFilesNaming : uint32_t {
    // Backup blob filenames are <file_number>_<crc32c>_<file_size>.blob and
    // backup SST filenames are <file_number>_<crc32c>_<file_size>.sst
    // where <crc32c> is an unsigned decimal integer. This is the
    // original/legacy naming scheme for share_files_with_checksum,
    // with two problems:
    // * At massive scale, collisions on this triple with different file
    //   contents is plausible.
    // * Determining the name to use requires computing the checksum,
    //   so generally requires reading the whole file even if the file
    //   is already backed up.
    //
    // ** ONLY RECOMMENDED FOR PRESERVING OLD BEHAVIOR **
    kLegacyCrc32cAndFileSize = 1U,

    // Backup SST filenames are <file_number>_s<db_session_id>.sst. This
    // pair of values should be very strongly unique for a given SST file
    // and easily determined before computing a checksum. The 's' indicates
    // the value is a DB session id, not a checksum.
    //
    // Exceptions:
    // * For blob files, kLegacyCrc32cAndFileSize is used as currently
    //   db_session_id is not supported by the blob file format.
    // * For old SST files without a DB session id, kLegacyCrc32cAndFileSize
    //   will be used instead, matching the names assigned by RocksDB versions
    //   not supporting the newer naming scheme.
    // * See also flags below.
    kUseDbSessionId = 2U,

    kMaskNoNamingFlags = 0xffffU,

    // If not already part of the naming scheme, insert
    //   _<file_size>
    // before .sst and .blob in the name. In case of user code actually parsing
    // the last _<whatever> before the .sst  and .blob as the file size, this
    // preserves that feature of kLegacyCrc32cAndFileSize. In other words, this
    // option makes official that unofficial feature of the backup metadata.
    //
    // We do not consider SST and blob file sizes to have sufficient entropy to
    // contribute significantly to naming uniqueness.
    kFlagIncludeFileSize = 1U << 31,

    kMaskNamingFlags = ~kMaskNoNamingFlags,
  };

  // Naming option for share_files_with_checksum table and blob files. See
  // ShareFilesNaming for details.
  //
  // Modifying this option cannot introduce a downgrade compatibility issue
  // because RocksDB can read, restore, and delete backups using different file
  // names, and it's OK for a backup directory to use a mixture of table and
  // blob files naming schemes.
  //
  // However, modifying this option and saving more backups to the same
  // directory can lead to the same file getting saved again to that
  // directory, under the new shared name in addition to the old shared
  // name.
  //
  // Default: kUseDbSessionId | kFlagIncludeFileSize
  //
  // Note: This option comes into effect only if both share_files_with_checksum
  // and share_table_files are true.
  ShareFilesNaming share_files_with_checksum_naming;

  // Major schema version to use when writing backup meta files
  // 1 (default) - compatible with very old versions of RocksDB.
  // 2 - can be read by RocksDB versions >= 6.19.0. Minimum schema version for
  //   * (Experimental) saving and restoring file temperature metadata
  int schema_version = 1;

  // (Experimental - subject to change or removal) When taking a backup and
  // saving file temperature info (minimum schema_version is 2), there are
  // two potential sources of truth for the placement of files into temperature
  // tiers: (a) the current file temperature reported by the FileSystem or
  // (b) the expected file temperature recorded in DB manifest. When this
  // option is false (default), (b) overrides (a) if both are not UNKNOWN.
  // When true, (a) overrides (b) if both are not UNKNOWN. Regardless of this
  // setting, a known temperature overrides UNKNOWN.
  bool current_temperatures_override_manifest = false;

  void Dump(Logger* logger) const;

  explicit BackupEngineOptions(
      const std::string& _backup_dir, Env* _backup_env = nullptr,
      bool _share_table_files = true, Logger* _info_log = nullptr,
      bool _sync = true, bool _destroy_old_data = false,
      bool _backup_log_files = true, uint64_t _io_buffer_size = 0,
      uint64_t _backup_rate_limit = 0, uint64_t _restore_rate_limit = 0,
      int _max_background_operations = 1,
      uint64_t _callback_trigger_interval_size = 4 * 1024 * 1024,
      int _max_valid_backups_to_open = INT_MAX,
      ShareFilesNaming _share_files_with_checksum_naming =
          static_cast<ShareFilesNaming>(kUseDbSessionId | kFlagIncludeFileSize))
      : CheckpointOrBackupEngineOptions{_info_log, _io_buffer_size,
                                        _backup_rate_limit,
                                        /*backup_rate_limiter=*/nullptr,
                                        _max_background_operations},
        backup_dir(_backup_dir),
        backup_env(_backup_env),
        share_table_files(_share_table_files),
        sync(_sync),
        destroy_old_data(_destroy_old_data),
        backup_log_files(_backup_log_files),
        restore_rate_limit(_restore_rate_limit),
        share_files_with_checksum(true),
        callback_trigger_interval_size(_callback_trigger_interval_size),
        max_valid_backups_to_open(_max_valid_backups_to_open),
        share_files_with_checksum_naming(_share_files_with_checksum_naming) {
    assert(share_table_files || !share_files_with_checksum);
    assert((share_files_with_checksum_naming & kMaskNoNamingFlags) != 0);
  }
};

inline BackupEngineOptions::ShareFilesNaming operator&(
    BackupEngineOptions::ShareFilesNaming lhs,
    BackupEngineOptions::ShareFilesNaming rhs) {
  uint32_t l = static_cast<uint32_t>(lhs);
  uint32_t r = static_cast<uint32_t>(rhs);
  assert(r == BackupEngineOptions::kMaskNoNamingFlags ||
         (r & BackupEngineOptions::kMaskNoNamingFlags) == 0);
  return static_cast<BackupEngineOptions::ShareFilesNaming>(l & r);
}

inline BackupEngineOptions::ShareFilesNaming operator|(
    BackupEngineOptions::ShareFilesNaming lhs,
    BackupEngineOptions::ShareFilesNaming rhs) {
  uint32_t l = static_cast<uint32_t>(lhs);
  uint32_t r = static_cast<uint32_t>(rhs);
  assert((r & BackupEngineOptions::kMaskNoNamingFlags) == 0);
  return static_cast<BackupEngineOptions::ShareFilesNaming>(l | r);
}

// Identifying information about a backup shared file that is (or might be)
// excluded from a backup using exclude_files_callback.
struct BackupExcludedFileInfo {
  explicit BackupExcludedFileInfo(const std::string& _relative_file)
      : relative_file(_relative_file) {}

  // File name and path relative to the backup dir.
  std::string relative_file;
};

// An auxiliary structure for exclude_files_callback
struct MaybeExcludeBackupFile {
  explicit MaybeExcludeBackupFile(BackupExcludedFileInfo&& _info)
      : info(std::move(_info)) {}

  // Identifying information about a backup shared file that could be excluded
  const BackupExcludedFileInfo info;

  // API user sets to true if the file should be excluded from this backup
  bool exclude_decision = false;
};

struct CreateBackupOptions : public CreateCheckpointOrBackupOptions {
  // Flush will always trigger if 2PC is enabled.
  // If write-ahead logs are disabled, set flush_before_backup=true to
  // avoid losing unflushed key/value pairs from the memtable.
  //
  // NOTE: If LockWAL() is active and WAL-disabled writes left unpersisted data,
  // backup may wait for UnlockWAL() before it can flush.
  bool flush_before_backup = false;

  // Callback for reporting progress, based on callback_trigger_interval_size.
  //
  // An exception thrown from the callback will result in Status::Aborted from
  // the operation.
  std::function<void()> progress_callback = {};

  // A callback that allows the API user to select files for exclusion, such
  // as if the files are known to exist in an alternate backup directory.
  // Only "shared" files can be excluded from backups. This is an advanced
  // feature because the BackupEngine user is trusted to keep track of files
  // such that the DB can be restored.
  //
  // Input to the callback is a [begin,end) range of sharable files live in
  // the DB being backed up, and the callback implementation sets
  // exclude_decision=true for files to exclude. A callback offers maximum
  // flexibility, e.g. if remote files are unavailable at backup time but
  // whose existence has been recorded somewhere. In case of an empty or
  // no-op callback, all files are included in the backup .
  //
  // To restore the DB, RestoreOptions::alternate_dirs must be used to provide
  // the excluded files.
  //
  // An exception thrown from the callback will result in Status::Aborted from
  // the operation.
  std::function<void(MaybeExcludeBackupFile* files_begin,
                     MaybeExcludeBackupFile* files_end)>
      exclude_files_callback = {};

  // If true, use atomic flush to flush all column families atomically
  // before creating the backup. This ensures cross-CF consistency without
  // needing WAL files. When combined with
  // BackupEngineOptions::backup_log_files=false, this allows skipping WAL
  // backup safely for multi-CF databases.
  // Only takes effect when flush_before_backup is also true.
  // Default: false
  bool atomic_flush = false;
};

struct RestoreOptions {
  // Enum reflecting tiered approach to restores.
  //
  // Options `kKeepLatestDbSessionIdFiles`, `kVerifyChecksum` introduce
  // incremental restore capability and are intended to be used separately.
  enum Mode : uint32_t {
    // Most efficient way to restore a healthy / non-corrupted DB from
    // the backup(s). This mode can almost always successfully recover from
    // incomplete / missing files, as in an incomplete copy of a DB.
    // This mode is also integrated with `exclude_files_callback` feature
    // and will opportunistically try to find excluded files in existing db
    // filesystem if missing in all supplied backup directories.
    //
    // Effective on data files following modern share files naming schemes.
    kKeepLatestDbSessionIdFiles = 1U,

    // Recommended when db is suspected to be unhealthy, ex. we want to retain
    // most of the files (therefore saving on write I/O) with an exception of
    // a few corrupted ones.
    //
    // When opted-in, restore engine will scan the db file, compute the
    // checksum and compare it against the checksum hardened in the backup file
    // metadata. If checksums match, existing file will be retained as-is.
    // Otherwise, it will be deleted and replaced it with its' restored backup
    // counterpart. If backup file doesn't have a checksum hardened in the
    // metadata, we'll schedule an async task to compute it.
    kVerifyChecksum = 2U,

    // Zero trust. Least efficient.
    //
    // Purge all the destination files and restores all files from the backup.
    kPurgeAllFiles = 0xffffU,
  };

  // If true, restore won't overwrite the existing log files in wal_dir. It will
  // also move all log files from archive directory to wal_dir. Use this option
  // in combination with BackupEngineOptions::backup_log_files = false for
  // persisting in-memory databases.
  // Default: false
  bool keep_log_files;

  // For backups that were created using exclude_files_callback, this
  // option enables restoring those backups by providing BackupEngines on
  // directories known to contain the required files.
  std::forward_list<BackupEngineReadOnlyBase*> alternate_dirs;

  // Specifies the level of incremental restore. 'kPurgeAllFiles' by default.
  Mode mode;

  // FIXME(https://github.com/meta/rocksdb/issues/13293)
  explicit RestoreOptions(bool _keep_log_files = false,
                          Mode _mode = Mode::kPurgeAllFiles)
      : keep_log_files(_keep_log_files), mode(_mode) {}
};

using BackupID = uint32_t;

using BackupFileInfo = FileStorageInfo;

struct BackupInfo {
  BackupID backup_id = 0U;
  // Creation time, according to GetCurrentTime
  int64_t timestamp = 0;

  // Total size in bytes (based on file payloads, not including filesystem
  // overheads or backup meta file)
  uint64_t size = 0U;

  // Number of backed up files, some of which might be shared with other
  // backups. Does not include backup meta file.
  uint32_t number_files = 0U;

  // Backup API user metadata
  std::string app_metadata;

  // Backup file details, if requested with include_file_details=true.
  // Does not include excluded_files.
  std::vector<BackupFileInfo> file_details;

  // Identifying information about shared files that were excluded from the
  // created backup. See exclude_files_callback and alternate_dirs.
  // This information is only provided if include_file_details=true.
  std::vector<BackupExcludedFileInfo> excluded_files;

  // DB "name" (a directory in the backup_env) for opening this backup as a
  // read-only DB. This should also be used as the DBOptions::wal_dir, such
  // as by default setting wal_dir="". See also env_for_open.
  // This field is only set if include_file_details=true
  std::string name_for_open;

  // An Env(+FileSystem) for opening this backup as a read-only DB, with
  // DB::OpenForReadOnly or similar. This field is only set if
  // include_file_details=true. (The FileSystem in this Env takes care
  // of making shared backup files openable from the `name_for_open` DB
  // directory.) See also name_for_open.
  //
  // This Env might or might not be shared with other backups. To work
  // around DBOptions::env being a raw pointer, this is a shared_ptr so
  // that keeping either this BackupInfo, the BackupEngine, or a copy of
  // this shared_ptr alive is sufficient to keep the Env alive for use by
  // a read-only DB.
  std::shared_ptr<Env> env_for_open;

  BackupInfo() {}

  explicit BackupInfo(BackupID _backup_id, int64_t _timestamp, uint64_t _size,
                      uint32_t _number_files, const std::string& _app_metadata)
      : backup_id(_backup_id),
        timestamp(_timestamp),
        size(_size),
        number_files(_number_files),
        app_metadata(_app_metadata) {}
};

class BackupStatistics {
 public:
  BackupStatistics() {
    number_success_backup = 0;
    number_fail_backup = 0;
  }

  explicit BackupStatistics(uint32_t _number_success_backup,
                            uint32_t _number_fail_backup)
      : number_success_backup(_number_success_backup),
        number_fail_backup(_number_fail_backup) {}

  ~BackupStatistics() = default;

  void IncrementNumberSuccessBackup();
  void IncrementNumberFailBackup();

  uint32_t GetNumberSuccessBackup() const;
  uint32_t GetNumberFailBackup() const;

  std::string ToString() const;

 private:
  uint32_t number_success_backup;
  uint32_t number_fail_backup;
};

// Read-only functions of a BackupEngine. (Restore writes to another directory
// not the backup directory.) See BackupEngine comments for details on
// safe concurrent operations.
class BackupEngineReadOnlyBase {
 public:
  virtual ~BackupEngineReadOnlyBase() = default;

  // Returns info about the latest good backup in backup_info, or NotFound
  // no good backup exists.
  // Setting include_file_details=true provides information about each
  // backed-up file in BackupInfo::file_details and more.
  virtual Status GetLatestBackupInfo(
      BackupInfo* backup_info, bool include_file_details = false) const = 0;

  // Returns info about a specific backup in backup_info, or NotFound
  // or Corruption status if the requested backup id does not exist or is
  // known corrupt.
  // Setting include_file_details=true provides information about each
  // backed-up file in BackupInfo::file_details and more.
  virtual Status GetBackupInfo(BackupID backup_id, BackupInfo* backup_info,
                               bool include_file_details = false) const = 0;

  // Returns info about non-corrupt backups in backup_infos.
  // Setting include_file_details=true provides information about each
  // backed-up file in BackupInfo::file_details and more.
  virtual void GetBackupInfo(std::vector<BackupInfo>* backup_infos,
                             bool include_file_details = false) const = 0;

  // Returns info about corrupt backups in corrupt_backups.
  // WARNING: Any write to the BackupEngine could trigger automatic
  // GarbageCollect(), which could delete files that would be needed to
  // manually recover a corrupt backup or to preserve an unrecognized (e.g.
  // incompatible future version) backup.
  virtual void GetCorruptedBackups(
      std::vector<BackupID>* corrupt_backup_ids) const = 0;

  // Restore to specified db_dir and wal_dir from backup_id.
  virtual IOStatus RestoreDBFromBackup(const RestoreOptions& options,
                                       BackupID backup_id,
                                       const std::string& db_dir,
                                       const std::string& wal_dir) const = 0;

  // keep for backward compatibility.
  virtual IOStatus RestoreDBFromBackup(
      BackupID backup_id, const std::string& db_dir, const std::string& wal_dir,
      const RestoreOptions& options = RestoreOptions()) const {
    return RestoreDBFromBackup(options, backup_id, db_dir, wal_dir);
  }

  // Like RestoreDBFromBackup but restores from latest non-corrupt backup_id
  virtual IOStatus RestoreDBFromLatestBackup(
      const RestoreOptions& options, const std::string& db_dir,
      const std::string& wal_dir) const = 0;

  // keep for backward compatibility.
  virtual IOStatus RestoreDBFromLatestBackup(
      const std::string& db_dir, const std::string& wal_dir,
      const RestoreOptions& options = RestoreOptions()) const {
    return RestoreDBFromLatestBackup(options, db_dir, wal_dir);
  }

  // If verify_with_checksum is true, this function
  // inspects the current checksums and file sizes of backup files to see if
  // they match our expectation.
  //
  // If verify_with_checksum is false, this function
  // checks that each file exists and that the size of the file matches our
  // expectation. It does not check file checksum.
  //
  // If this BackupEngine created the backup, it compares the files' current
  // sizes (and current checksum) against the number of bytes written to
  // them (and the checksum calculated) during creation.
  // Otherwise, it compares the files' current sizes (and checksums) against
  // their sizes (and checksums) when the BackupEngine was opened.
  //
  // Returns Status::OK() if all checks are good
  virtual IOStatus VerifyBackup(BackupID backup_id,
                                bool verify_with_checksum = false) const = 0;

  // Internal use only
  virtual BackupEngine* AsBackupEngine() = 0;
};

// Append-only functions of a BackupEngine. See BackupEngine comment for
// details on distinction between Append and Write operations and safe
// concurrent operations.
class BackupEngineAppendOnlyBase {
 public:
  virtual ~BackupEngineAppendOnlyBase() = default;

  // same as CreateNewBackup, but stores extra application metadata.
  virtual IOStatus CreateNewBackupWithMetadata(
      const CreateBackupOptions& options, DB* db,
      const std::string& app_metadata, BackupID* new_backup_id = nullptr) = 0;

  // keep here for backward compatibility.
  virtual IOStatus CreateNewBackupWithMetadata(
      DB* db, const std::string& app_metadata, bool flush_before_backup = false,
      std::function<void()> progress_callback = []() {}) {
    CreateBackupOptions options;
    options.flush_before_backup = flush_before_backup;
    options.progress_callback = progress_callback;
    return CreateNewBackupWithMetadata(options, db, app_metadata);
  }

  // Captures the state of the database by creating a new (latest) backup.
  // On success (OK status), the BackupID of the new backup is saved to
  // *new_backup_id when not nullptr.
  // NOTE: db_paths and cf_paths are not supported for creating backups,
  // and NotSupported will be returned when the DB (without WALs) uses more
  // than one directory.
  virtual IOStatus CreateNewBackup(const CreateBackupOptions& options, DB* db,
                                   BackupID* new_backup_id = nullptr) {
    return CreateNewBackupWithMetadata(options, db, "", new_backup_id);
  }

  // keep here for backward compatibility.
  virtual IOStatus CreateNewBackup(
      DB* db, bool flush_before_backup = false,
      std::function<void()> progress_callback = []() {}) {
    CreateBackupOptions options;
    options.flush_before_backup = flush_before_backup;
    options.progress_callback = progress_callback;
    return CreateNewBackup(options, db);
  }

  // Call this from another thread if you want to stop the backup
  // that is currently happening. It will return immediately, will
  // not wait for the backup to stop.
  // The backup will stop ASAP and the call to CreateNewBackup will
  // return Status::Incomplete(). It will not clean up after itself, but
  // the state will remain consistent. The state will be cleaned up the
  // next time you call CreateNewBackup or GarbageCollect for the same backup
  // directory on a new BackupEngine object.
  //
  // NOTE: This is a one-way operation. Once StopBackup() is called on a
  // BackupEngine instance, all subsequent backup requests (CreateNewBackup,
  // CreateNewBackupWithMetadata) will fail with Status::Incomplete().
  // To create new backups after calling StopBackup(), you must open a new
  // BackupEngine instance.
  virtual void StopBackup() = 0;

  // Will delete any files left over from incomplete creation or deletion of
  // a backup. This is not normally needed as those operations also clean up
  // after prior incomplete calls to the same kind of operation (create or
  // delete). This does not delete corrupt backups but can delete files that
  // would be needed to manually recover a corrupt backup or to preserve an
  // unrecognized (e.g. incompatible future version) backup.
  // NOTE: This is not designed to delete arbitrary files added to the backup
  // directory outside of BackupEngine, and clean-up is always subject to
  // permissions on and availability of the underlying filesystem.
  // NOTE2: For concurrency and interference purposes (see BackupEngine
  // comment), GarbageCollect (GC) is like other Append operations, even
  // though it seems different. Although GC can delete physical data, it does
  // not delete any logical data read by Read operations. GC can interfere
  // with Append or Write operations in another BackupEngine on the same
  // backup_dir, because temporary files will be treated as obsolete and
  // deleted.
  virtual IOStatus GarbageCollect() = 0;
};

// A backup engine for organizing and managing backups.
// This class is not user-extensible.
//
// This class declaration adds "Write" operations in addition to the
// operations from BackupEngineAppendOnlyBase and BackupEngineReadOnlyBase.
//
// # Concurrency between threads on the same BackupEngine* object
//
// As of version 6.20, BackupEngine* operations are generally thread-safe,
// using a read-write lock, though single-thread operation is still
// recommended to avoid TOCTOU bugs. Specifically, particular kinds of
// concurrent operations behave like this:
//
// op1\op2| Read  | Append | Write
// -------|-------|--------|--------
//   Read | conc  | block  | block
// Append | block | block  | block
//  Write | block | block  | block
//
// conc = operations safely proceed concurrently
// block = one of the operations safely blocks until the other completes.
//   There is generally no guarantee as to which completes first.
//
// StopBackup is the only operation that affects an ongoing operation.
//
// # Interleaving operations between BackupEngine* objects open on the
// same backup_dir
//
// It is recommended only to have one BackupEngine* object open for a given
// backup_dir, but it is possible to mix / interleave some operations
// (regardless of whether they are concurrent) with these caveats:
//
// op1\op2|  Open  |  Read  | Append | Write
// -------|--------|--------|--------|--------
//   Open | conc   | conc   | atomic | unspec
//   Read | conc   | conc   | old    | unspec
// Append | atomic | old    | unspec | unspec
//  Write | unspec | unspec | unspec | unspec
//
// Special case: Open with destroy_old_data=true is really a Write
//
// conc = operations safely proceed, concurrently when applicable
// atomic = operations are effectively atomic; if a concurrent Append
//   operation has not completed at some key point during Open, the
//   opened BackupEngine* will never see the result of the Append op.
// old = Read operations do not include any state changes from other
//   BackupEngine* objects; they return the state at their Open time.
// unspec = Behavior is unspecified, including possibly trashing the
//   backup_dir, but is "memory safe" (no C++ undefined behavior)
//
class BackupEngine : public BackupEngineReadOnlyBase,
                     public BackupEngineAppendOnlyBase {
 public:
  ~BackupEngine() override = default;

  // BackupEngineOptions have to be the same as the ones used in previous
  // BackupEngines for the same backup directory.
  static IOStatus Open(const BackupEngineOptions& options, Env* db_env,
                       BackupEngine** backup_engine_ptr);

  // keep for backward compatibility.
  static IOStatus Open(Env* db_env, const BackupEngineOptions& options,
                       BackupEngine** backup_engine_ptr) {
    return BackupEngine::Open(options, db_env, backup_engine_ptr);
  }

  // Deletes old backups, keeping latest num_backups_to_keep alive.
  // See also DeleteBackup.
  virtual IOStatus PurgeOldBackups(uint32_t num_backups_to_keep) = 0;

  // Deletes a specific backup. If this operation (or PurgeOldBackups)
  // is not completed due to crash, power failure, etc. the state
  // will be cleaned up the next time you call DeleteBackup,
  // PurgeOldBackups, or GarbageCollect.
  virtual IOStatus DeleteBackup(BackupID backup_id) = 0;
};

// A variant of BackupEngine that only allows "Read" operations. See
// BackupEngine comment for details. This class is not user-extensible.
class BackupEngineReadOnly : public BackupEngineReadOnlyBase {
 public:
  ~BackupEngineReadOnly() override = default;

  static IOStatus Open(const BackupEngineOptions& options, Env* db_env,
                       BackupEngineReadOnly** backup_engine_ptr);
  // keep for backward compatibility.
  static IOStatus Open(Env* db_env, const BackupEngineOptions& options,
                       BackupEngineReadOnly** backup_engine_ptr) {
    return BackupEngineReadOnly::Open(options, db_env, backup_engine_ptr);
  }
};

}  // namespace ROCKSDB_NAMESPACE
