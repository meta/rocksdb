//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/utilities/ldb_cmd.h"

namespace ROCKSDB_NAMESPACE {

class CompactorCommand : public LDBCommand {
 public:
  static std::string Name() { return "compact"; }

  CompactorCommand(const std::vector<std::string>& params,
                   const std::map<std::string, std::string>& options,
                   const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  void DoCommand() override;

 private:
  bool null_from_;
  std::string from_;
  bool null_to_;
  std::string to_;
};

class DBFileDumperCommand : public LDBCommand {
 public:
  static std::string Name() { return "dump_live_files"; }

  DBFileDumperCommand(const std::vector<std::string>& params,
                      const std::map<std::string, std::string>& options,
                      const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  void DoCommand() override;

 private:
  bool decode_blob_index_;
  bool dump_uncompressed_blobs_;
};

class DBLiveFilesMetadataDumperCommand : public LDBCommand {
 public:
  static std::string Name() { return "list_live_files_metadata"; }

  DBLiveFilesMetadataDumperCommand(
      const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  void DoCommand() override;

 private:
  bool sort_by_filename_;

  static const std::string ARG_SORT_BY_FILENAME;
};

class DBDumperCommand : public LDBCommand {
 public:
  static std::string Name() { return "dump"; }

  DBDumperCommand(const std::vector<std::string>& params,
                  const std::map<std::string, std::string>& options,
                  const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  void DoCommand() override;

 private:
  /**
   * Extract file name from the full path. We handle both the forward slash (/)
   * and backslash (\) to make sure that different OS-s are supported.
   */
  static std::string GetFileNameFromPath(const std::string& s) {
    std::size_t n = s.find_last_of("/\\");

    if (std::string::npos == n) {
      return s;
    } else {
      return s.substr(n + 1);
    }
  }

  void DoDumpCommand();

  bool null_from_;
  std::string from_;
  bool null_to_;
  std::string to_;
  int max_keys_;
  std::string delim_;
  bool count_only_;
  bool count_delim_;
  bool print_stats_;
  std::string path_;
  bool decode_blob_index_;
  bool dump_uncompressed_blobs_;

  static const std::string ARG_COUNT_ONLY;
  static const std::string ARG_COUNT_DELIM;
  static const std::string ARG_STATS;
  static const std::string ARG_TTL_BUCKET;
};

class InternalDumpCommand : public LDBCommand {
 public:
  static std::string Name() { return "idump"; }

  InternalDumpCommand(const std::vector<std::string>& params,
                      const std::map<std::string, std::string>& options,
                      const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  void DoCommand() override;

 private:
  bool has_from_;
  std::string from_;
  bool has_to_;
  std::string to_;
  int max_keys_;
  std::string delim_;
  bool count_only_;
  bool count_delim_;
  bool print_stats_;
  bool is_input_key_hex_;
  bool decode_blob_index_;

  static const std::string ARG_DELIM;
  static const std::string ARG_COUNT_ONLY;
  static const std::string ARG_COUNT_DELIM;
  static const std::string ARG_STATS;
  static const std::string ARG_INPUT_KEY_HEX;
};

class DBLoaderCommand : public LDBCommand {
 public:
  static std::string Name() { return "load"; }

  DBLoaderCommand(std::string& db_name, std::vector<std::string>& args);

  DBLoaderCommand(const std::vector<std::string>& params,
                  const std::map<std::string, std::string>& options,
                  const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  void DoCommand() override;

  void OverrideBaseOptions() override;

 private:
  bool disable_wal_;
  bool bulk_load_;
  bool compact_;

  static const std::string ARG_DISABLE_WAL;
  static const std::string ARG_BULK_LOAD;
  static const std::string ARG_COMPACT;
};

class ManifestDumpCommand : public LDBCommand {
 public:
  static std::string Name() { return "manifest_dump"; }

  ManifestDumpCommand(const std::vector<std::string>& params,
                      const std::map<std::string, std::string>& options,
                      const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  void DoCommand() override;

  bool NoDBOpen() override { return true; }

 private:
  bool verbose_;
  bool json_;
  std::string path_;

  static const std::string ARG_VERBOSE;
  static const std::string ARG_JSON;
  static const std::string ARG_PATH;
};

class UpdateManifestCommand : public LDBCommand {
 public:
  static std::string Name() { return "update_manifest"; }

  UpdateManifestCommand(const std::vector<std::string>& params,
                        const std::map<std::string, std::string>& options,
                        const std::vector<std::string>& flags);

  static void Help(std::string& ret);
  void DoCommand() override;

  bool NoDBOpen() override { return true; }

 private:
  bool verbose_;
  bool update_temperatures_;
  bool rebind_options_file_;
  std::string options_file_;
  std::vector<std::string> file_number_paths_;
  // TODO future: checksum_func for populating checksums

  static const std::string ARG_VERBOSE;
  static const std::string ARG_UPDATE_TEMPERATURES;
  static const std::string ARG_REBIND_OPTIONS_FILE;
  static const std::string ARG_OPTIONS_FILE;
  static const std::string ARG_FILE_NUMBER_PATHS;
};

class FileChecksumDumpCommand : public LDBCommand {
 public:
  static std::string Name() { return "file_checksum_dump"; }

  FileChecksumDumpCommand(const std::vector<std::string>& params,
                          const std::map<std::string, std::string>& options,
                          const std::vector<std::string>& flags);

  static void Help(std::string& ret);
  void DoCommand() override;

  bool NoDBOpen() override { return true; }

 private:
  std::string path_;
  bool is_checksum_hex_;

  static const std::string ARG_PATH;
};

class GetPropertyCommand : public LDBCommand {
 public:
  static std::string Name() { return "get_property"; }

  GetPropertyCommand(const std::vector<std::string>& params,
                     const std::map<std::string, std::string>& options,
                     const std::vector<std::string>& flags);

  static void Help(std::string& ret);
  void DoCommand() override;

 private:
  std::string property_;
};

class ListColumnFamiliesCommand : public LDBCommand {
 public:
  static std::string Name() { return "list_column_families"; }

  ListColumnFamiliesCommand(const std::vector<std::string>& params,
                            const std::map<std::string, std::string>& options,
                            const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  void DoCommand() override;

  bool NoDBOpen() override { return true; }
};

class CreateColumnFamilyCommand : public LDBCommand {
 public:
  static std::string Name() { return "create_column_family"; }

  CreateColumnFamilyCommand(const std::vector<std::string>& params,
                            const std::map<std::string, std::string>& options,
                            const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  void DoCommand() override;

  bool NoDBOpen() override { return false; }

 private:
  std::string new_cf_name_;
};

class DropColumnFamilyCommand : public LDBCommand {
 public:
  static std::string Name() { return "drop_column_family"; }

  DropColumnFamilyCommand(const std::vector<std::string>& params,
                          const std::map<std::string, std::string>& options,
                          const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  void DoCommand() override;

  bool NoDBOpen() override { return false; }

 private:
  std::string cf_name_to_drop_;
};

class ReduceDBLevelsCommand : public LDBCommand {
 public:
  static std::string Name() { return "reduce_levels"; }

  ReduceDBLevelsCommand(const std::vector<std::string>& params,
                        const std::map<std::string, std::string>& options,
                        const std::vector<std::string>& flags);

  void OverrideBaseCFOptions(ColumnFamilyOptions* cf_opts) override;

  void DoCommand() override;

  bool NoDBOpen() override { return true; }

  static void Help(std::string& msg);

  static std::vector<std::string> PrepareArgs(const std::string& db_path,
                                              int new_levels,
                                              bool print_old_level = false);

 private:
  int old_levels_;
  int new_levels_;
  bool print_old_levels_;

  static const std::string ARG_NEW_LEVELS;
  static const std::string ARG_PRINT_OLD_LEVELS;

  Status GetOldNumOfLevels(Options& opt, int* levels);
};

class ChangeCompactionStyleCommand : public LDBCommand {
 public:
  static std::string Name() { return "change_compaction_style"; }

  ChangeCompactionStyleCommand(
      const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  void OverrideBaseCFOptions(ColumnFamilyOptions* cf_opts) override;

  void DoCommand() override;

  static void Help(std::string& msg);

 private:
  int old_compaction_style_;
  int new_compaction_style_;

  static const std::string ARG_OLD_COMPACTION_STYLE;
  static const std::string ARG_NEW_COMPACTION_STYLE;
};

class WALDumperCommand : public LDBCommand {
 public:
  static std::string Name() { return "dump_wal"; }

  WALDumperCommand(const std::vector<std::string>& params,
                   const std::map<std::string, std::string>& options,
                   const std::vector<std::string>& flags);

  bool NoDBOpen() override { return no_db_open_; }

  static void Help(std::string& ret);

  void DoCommand() override;

 private:
  bool print_header_;
  std::string wal_file_;
  bool print_values_;
  bool only_print_seqno_gaps_;
  bool is_write_committed_;  // default will be set to true
  bool no_db_open_ = true;

  static const std::string ARG_WAL_FILE;
  static const std::string ARG_WRITE_COMMITTED;
  static const std::string ARG_PRINT_HEADER;
  static const std::string ARG_PRINT_VALUE;
  static const std::string ARG_ONLY_PRINT_SEQNO_GAPS;
};

class GetCommand : public LDBCommand {
 public:
  static std::string Name() { return "get"; }

  GetCommand(const std::vector<std::string>& params,
             const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags);

  void DoCommand() override;

  static void Help(std::string& ret);

 private:
  std::string key_;
};

class MultiGetCommand : public LDBCommand {
 public:
  static std::string Name() { return "multi_get"; }

  MultiGetCommand(const std::vector<std::string>& params,
                  const std::map<std::string, std::string>& options,
                  const std::vector<std::string>& flags);

  void DoCommand() override;

  static void Help(std::string& ret);

 private:
  std::vector<std::string> keys_;
};

class GetEntityCommand : public LDBCommand {
 public:
  static std::string Name() { return "get_entity"; }

  GetEntityCommand(const std::vector<std::string>& params,
                   const std::map<std::string, std::string>& options,
                   const std::vector<std::string>& flags);

  void DoCommand() override;

  static void Help(std::string& ret);

 private:
  std::string key_;
};

class MultiGetEntityCommand : public LDBCommand {
 public:
  static std::string Name() { return "multi_get_entity"; }

  MultiGetEntityCommand(const std::vector<std::string>& params,
                        const std::map<std::string, std::string>& options,
                        const std::vector<std::string>& flags);

  void DoCommand() override;

  static void Help(std::string& ret);

 private:
  std::vector<std::string> keys_;
};

class ApproxSizeCommand : public LDBCommand {
 public:
  static std::string Name() { return "approxsize"; }

  ApproxSizeCommand(const std::vector<std::string>& params,
                    const std::map<std::string, std::string>& options,
                    const std::vector<std::string>& flags);

  void DoCommand() override;

  static void Help(std::string& ret);

 private:
  std::string start_key_;
  std::string end_key_;
};

class BatchPutCommand : public LDBCommand {
 public:
  static std::string Name() { return "batchput"; }

  BatchPutCommand(const std::vector<std::string>& params,
                  const std::map<std::string, std::string>& options,
                  const std::vector<std::string>& flags);

  void DoCommand() override;

  static void Help(std::string& ret);

  void OverrideBaseOptions() override;

 private:
  /**
   * The key-values to be inserted.
   */
  std::vector<std::pair<std::string, std::string>> key_values_;
};

class ScanCommand : public LDBCommand {
 public:
  static std::string Name() { return "scan"; }

  ScanCommand(const std::vector<std::string>& params,
              const std::map<std::string, std::string>& options,
              const std::vector<std::string>& flags);

  void DoCommand() override;

  static void Help(std::string& ret);

 private:
  std::string start_key_;
  std::string end_key_;
  bool start_key_specified_;
  bool end_key_specified_;
  int max_keys_scanned_;
  bool no_value_;
  bool get_write_unix_time_;
};

class DeleteCommand : public LDBCommand {
 public:
  static std::string Name() { return "delete"; }

  DeleteCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  void DoCommand() override;

  static void Help(std::string& ret);

 private:
  std::string key_;
};

class SingleDeleteCommand : public LDBCommand {
 public:
  static std::string Name() { return "singledelete"; }

  SingleDeleteCommand(const std::vector<std::string>& params,
                      const std::map<std::string, std::string>& options,
                      const std::vector<std::string>& flags);

  void DoCommand() override;

  static void Help(std::string& ret);

 private:
  std::string key_;
};

class DeleteRangeCommand : public LDBCommand {
 public:
  static std::string Name() { return "deleterange"; }

  DeleteRangeCommand(const std::vector<std::string>& params,
                     const std::map<std::string, std::string>& options,
                     const std::vector<std::string>& flags);

  void DoCommand() override;

  static void Help(std::string& ret);

 private:
  std::string begin_key_;
  std::string end_key_;
};

class PutCommand : public LDBCommand {
 public:
  static std::string Name() { return "put"; }

  PutCommand(const std::vector<std::string>& params,
             const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags);

  void DoCommand() override;

  static void Help(std::string& ret);

  void OverrideBaseOptions() override;

 private:
  std::string key_;
  std::string value_;
};

class PutEntityCommand : public LDBCommand {
 public:
  static std::string Name() { return "put_entity"; }

  PutEntityCommand(const std::vector<std::string>& params,
                   const std::map<std::string, std::string>& options,
                   const std::vector<std::string>& flags);

  void DoCommand() override;

  static void Help(std::string& ret);

  void OverrideBaseOptions() override;

 private:
  std::string key_;
  std::vector<std::string> column_names_;
  std::vector<std::string> column_values_;
};

/**
 * Command that starts up a REPL shell that allows
 * get/put/delete.
 */
class DBQuerierCommand : public LDBCommand {
 public:
  static std::string Name() { return "query"; }

  DBQuerierCommand(const std::vector<std::string>& params,
                   const std::map<std::string, std::string>& options,
                   const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  void DoCommand() override;

 private:
  static const char* HELP_CMD;
  static const char* GET_CMD;
  static const char* PUT_CMD;
  static const char* DELETE_CMD;
  static const char* COUNT_CMD;
};

class CheckConsistencyCommand : public LDBCommand {
 public:
  static std::string Name() { return "checkconsistency"; }

  CheckConsistencyCommand(const std::vector<std::string>& params,
                          const std::map<std::string, std::string>& options,
                          const std::vector<std::string>& flags);

  void DoCommand() override;

  bool NoDBOpen() override { return true; }

  static void Help(std::string& ret);
};

class CheckPointCommand : public LDBCommand {
 public:
  static std::string Name() { return "checkpoint"; }

  CheckPointCommand(const std::vector<std::string>& params,
                    const std::map<std::string, std::string>& options,
                    const std::vector<std::string>& flags);

  void DoCommand() override;

  static void Help(std::string& ret);

  std::string checkpoint_dir_;

 private:
  static const std::string ARG_CHECKPOINT_DIR;
};

class RepairCommand : public LDBCommand {
 public:
  static std::string Name() { return "repair"; }

  RepairCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  void DoCommand() override;

  bool NoDBOpen() override { return true; }

  void OverrideBaseOptions() override;

  static void Help(std::string& ret);

 protected:
  bool verbose_;

 private:
  static const std::string ARG_VERBOSE;
};

class BackupEngineCommand : public LDBCommand {
 public:
  BackupEngineCommand(const std::vector<std::string>& params,
                      const std::map<std::string, std::string>& options,
                      const std::vector<std::string>& flags);

 protected:
  static void Help(const std::string& name, std::string& ret);
  std::string backup_env_uri_;
  std::string backup_fs_uri_;
  std::string backup_dir_;
  int num_threads_;
  std::unique_ptr<Logger> logger_;
  std::shared_ptr<Env> backup_env_guard_;

 private:
  static const std::string ARG_BACKUP_DIR;
  static const std::string ARG_BACKUP_ENV_URI;
  static const std::string ARG_BACKUP_FS_URI;
  static const std::string ARG_NUM_THREADS;
  static const std::string ARG_STDERR_LOG_LEVEL;
};

class BackupCommand : public BackupEngineCommand {
 public:
  static std::string Name() { return "backup"; }
  BackupCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);
  void DoCommand() override;
  static void Help(std::string& ret);
};

class RestoreCommand : public BackupEngineCommand {
 public:
  static std::string Name() { return "restore"; }
  RestoreCommand(const std::vector<std::string>& params,
                 const std::map<std::string, std::string>& options,
                 const std::vector<std::string>& flags);
  void DoCommand() override;
  bool NoDBOpen() override { return true; }
  static void Help(std::string& ret);
};

class WriteExternalSstFilesCommand : public LDBCommand {
 public:
  static std::string Name() { return "write_extern_sst"; }
  WriteExternalSstFilesCommand(
      const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  void DoCommand() override;

  bool NoDBOpen() override { return false; }

  void OverrideBaseOptions() override;

  static void Help(std::string& ret);

 private:
  std::string output_sst_path_;
};

class IngestExternalSstFilesCommand : public LDBCommand {
 public:
  static std::string Name() { return "ingest_extern_sst"; }
  IngestExternalSstFilesCommand(
      const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  void DoCommand() override;

  bool NoDBOpen() override { return false; }

  void OverrideBaseOptions() override;

  static void Help(std::string& ret);

 private:
  std::string input_sst_path_;
  bool move_files_;
  bool snapshot_consistency_;
  bool allow_global_seqno_;
  bool allow_blocking_flush_;
  bool ingest_behind_;

  static const std::string ARG_MOVE_FILES;
  static const std::string ARG_SNAPSHOT_CONSISTENCY;
  static const std::string ARG_ALLOW_GLOBAL_SEQNO;
  static const std::string ARG_ALLOW_BLOCKING_FLUSH;
  static const std::string ARG_INGEST_BEHIND;
};

// Command that prints out range delete tombstones in SST files.
class ListFileRangeDeletesCommand : public LDBCommand {
 public:
  static std::string Name() { return "list_file_range_deletes"; }

  ListFileRangeDeletesCommand(const std::map<std::string, std::string>& options,
                              const std::vector<std::string>& flags);

  void DoCommand() override;

  static void Help(std::string& ret);

 private:
  int max_keys_ = 1000;
};

// Command that removes the SST file forcibly from the manifest.
class UnsafeRemoveSstFileCommand : public LDBCommand {
 public:
  static std::string Name() { return "unsafe_remove_sst_file"; }

  UnsafeRemoveSstFileCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  void DoCommand() override;

  bool NoDBOpen() override { return true; }

 private:
  uint64_t sst_file_number_;
};

class CompactionProgressDumpCommand : public LDBCommand {
 public:
  static std::string Name() { return "compaction_progress_dump"; }

  CompactionProgressDumpCommand(
      const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  void DoCommand() override;

  bool NoDBOpen() override { return true; }

 private:
  std::string path_;

  static const std::string ARG_PATH;
};

class RemoteCompactionPrimaryCommand : public LDBCommand {
 public:
  static std::string Name() { return "remote_compaction_primary"; }
  RemoteCompactionPrimaryCommand(
      const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);
  void DoCommand() override;
  bool NoDBOpen() override { return true; }
  static void Help(std::string& ret);

 private:
  std::string job_dir_;
  static const std::string ARG_JOB_DIR;
};

class RemoteCompactionWorkerCommand : public LDBCommand {
 public:
  static std::string Name() { return "remote_compaction_worker"; }
  RemoteCompactionWorkerCommand(
      const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);
  void DoCommand() override;
  bool NoDBOpen() override { return true; }
  static void Help(std::string& ret);

 private:
  std::string job_dir_;
  static const std::string ARG_JOB_DIR;
};

}  // namespace ROCKSDB_NAMESPACE
