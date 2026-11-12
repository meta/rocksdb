//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "rocksdb/ldb_tool.h"

#include "rocksdb/utilities/ldb_cmd.h"
#include "tools/ldb_cmd_impl.h"

namespace ROCKSDB_NAMESPACE {

LDBOptions::LDBOptions() = default;

void LDBCommandRunner::PrintHelp(const LDBOptions& ldb_options,
                                 const char* /*exec_name*/, bool to_stderr) {
  std::string ret;

  ret.append(ldb_options.print_help_header);
  ret.append("\n\n");
  ret.append("commands MUST specify --" + LDBCommand::ARG_DB +
             "=<full_path_to_db_directory> when necessary\n");
  ret.append("\n");
  ret.append("commands can optionally specify\n");
  ret.append("  --" + LDBCommand::ARG_ENV_URI + "=<uri_of_environment> or --" +
             LDBCommand::ARG_FS_URI + "=<uri_of_filesystem> if necessary");
  ret.append("\n");
  ret.append("  --" + LDBCommand::ARG_SECONDARY_PATH +
             "=<secondary_path> to open DB as secondary instance. Operations "
             "not supported in secondary instance will fail.\n\n");
  ret.append("  --" + LDBCommand::ARG_LEADER_PATH +
             "=<leader_path> to open DB as a follower instance. Operations "
             "not supported in follower instance will fail.\n\n");
  ret.append(
      "The following optional parameters control if keys/values are "
      "input/output as hex or as plain strings:\n");
  ret.append("  --" + LDBCommand::ARG_KEY_HEX +
             " : Keys are input/output as hex\n");
  ret.append("  --" + LDBCommand::ARG_VALUE_HEX +
             " : Values are input/output as hex\n");
  ret.append("  --" + LDBCommand::ARG_HEX +
             " : Both keys and values are input/output as hex\n");
  ret.append("\n");

  ret.append(
      "The following optional parameters control the database "
      "internals:\n");
  ret.append(
      "  --" + LDBCommand::ARG_CF_NAME +
      "=<string> : name of the column family to operate on. default: default "
      "column family\n");
  ret.append("  --" + LDBCommand::ARG_TTL +
             " with 'put','get','scan','dump','query','batchput'"
             " : DB supports ttl and value is internally timestamp-suffixed\n");
  ret.append("  --" + LDBCommand::ARG_USE_TXN +
             " : Open database as TransactionDB. Required for databases "
             "created with WritePrepared or WriteUnprepared transactions.\n");
  ret.append("  --" + LDBCommand::ARG_TXN_WRITE_POLICY +
             "=<0|1|2> : Transaction write policy. "
             "0=WRITE_COMMITTED (default), 1=WRITE_PREPARED, "
             "2=WRITE_UNPREPARED\n");
  ret.append("  --" + LDBCommand::ARG_TRY_LOAD_OPTIONS +
             " : Try to load option file from DB. Default to true if " +
             LDBCommand::ARG_DB +
             " is specified and not creating a new DB and not open as TTL DB. "
             "Can be set to false explicitly.\n");
  ret.append("  --" + LDBCommand::ARG_DISABLE_CONSISTENCY_CHECKS +
             " : Set options.force_consistency_checks = false.\n");
  ret.append("  --" + LDBCommand::ARG_IGNORE_UNKNOWN_OPTIONS +
             " : Ignore unknown options when loading option file.\n");
  ret.append("  --" + LDBCommand::ARG_BLOOM_BITS + "=<int,e.g.:14>\n");
  ret.append("  --" + LDBCommand::ARG_FIX_PREFIX_LEN + "=<int,e.g.:14>\n");
  ret.append("  --" + LDBCommand::ARG_COMPRESSION_TYPE +
             "=<no|snappy|zlib|bzip2|lz4|lz4hc|xpress|zstd>\n");
  ret.append("  --" + LDBCommand::ARG_COMPRESSION_MAX_DICT_BYTES +
             "=<int,e.g.:16384>\n");
  ret.append("  --" + LDBCommand::ARG_BLOCK_SIZE + "=<block_size_in_bytes>\n");
  ret.append("  --" + LDBCommand::ARG_UNIFORM_CV_THRESHOLD +
             "=<double,e.g.:0.2>\n");
  ret.append("  --" + LDBCommand::ARG_AUTO_COMPACTION + "=<true|false>\n");
  ret.append("  --" + LDBCommand::ARG_DB_WRITE_BUFFER_SIZE +
             "=<int,e.g.:16777216>\n");
  ret.append("  --" + LDBCommand::ARG_WRITE_BUFFER_SIZE +
             "=<int,e.g.:4194304>\n");
  ret.append("  --" + LDBCommand::ARG_FILE_SIZE + "=<int,e.g.:2097152>\n");
  ret.append("  --" + LDBCommand::ARG_ENABLE_BLOB_FILES +
             " : Enable key-value separation using BlobDB\n");
  ret.append("  --" + LDBCommand::ARG_MIN_BLOB_SIZE + "=<int,e.g.:2097152>\n");
  ret.append("  --" + LDBCommand::ARG_BLOB_FILE_SIZE + "=<int,e.g.:2097152>\n");
  ret.append("  --" + LDBCommand::ARG_BLOB_COMPRESSION_TYPE +
             "=<no|snappy|zlib|bzip2|lz4|lz4hc|xpress|zstd>\n");
  ret.append("  --" + LDBCommand::ARG_ENABLE_BLOB_GARBAGE_COLLECTION +
             " : Enable blob garbage collection\n");
  ret.append("  --" + LDBCommand::ARG_BLOB_GARBAGE_COLLECTION_AGE_CUTOFF +
             "=<double,e.g.:0.25>\n");
  ret.append("  --" + LDBCommand::ARG_BLOB_GARBAGE_COLLECTION_FORCE_THRESHOLD +
             "=<double,e.g.:0.25>\n");
  ret.append("  --" + LDBCommand::ARG_BLOB_COMPACTION_READAHEAD_SIZE +
             "=<int,e.g.:2097152>\n");
  ret.append("  --" + LDBCommand::ARG_READ_TIMESTAMP +
             "=<uint64_ts, e.g.:323> : read timestamp, required if column "
             "family enables timestamp, otherwise invalid if provided.");

  ret.append("\n\n");
  ret.append("Data Access Commands:\n");
  PutCommand::Help(ret);
  PutEntityCommand::Help(ret);
  GetCommand::Help(ret);
  GetEntityCommand::Help(ret);
  MultiGetCommand::Help(ret);
  MultiGetEntityCommand::Help(ret);
  BatchPutCommand::Help(ret);
  ScanCommand::Help(ret);
  DeleteCommand::Help(ret);
  SingleDeleteCommand::Help(ret);
  DeleteRangeCommand::Help(ret);
  DBQuerierCommand::Help(ret);
  ApproxSizeCommand::Help(ret);
  CheckConsistencyCommand::Help(ret);
  ListFileRangeDeletesCommand::Help(ret);

  ret.append("\n\n");
  ret.append("Admin Commands:\n");
  WALDumperCommand::Help(ret);
  CompactorCommand::Help(ret);
  ReduceDBLevelsCommand::Help(ret);
  ChangeCompactionStyleCommand::Help(ret);
  DBDumperCommand::Help(ret);
  DBLoaderCommand::Help(ret);
  ManifestDumpCommand::Help(ret);
  CompactionProgressDumpCommand::Help(ret);
  UpdateManifestCommand::Help(ret);
  FileChecksumDumpCommand::Help(ret);
  GetPropertyCommand::Help(ret);
  ListColumnFamiliesCommand::Help(ret);
  CreateColumnFamilyCommand::Help(ret);
  DropColumnFamilyCommand::Help(ret);
  DBFileDumperCommand::Help(ret);
  InternalDumpCommand::Help(ret);
  DBLiveFilesMetadataDumperCommand::Help(ret);
  RepairCommand::Help(ret);
  BackupCommand::Help(ret);
  RestoreCommand::Help(ret);
  CheckPointCommand::Help(ret);
  WriteExternalSstFilesCommand::Help(ret);
  IngestExternalSstFilesCommand::Help(ret);
  UnsafeRemoveSstFileCommand::Help(ret);
  RemoteCompactionPrimaryCommand::Help(ret);
  RemoteCompactionWorkerCommand::Help(ret);

  fprintf(to_stderr ? stderr : stdout, "%s\n", ret.c_str());
}

int LDBCommandRunner::RunCommand(
    int argc, char const* const* argv, const Options& options,
    const LDBOptions& ldb_options,
    const std::vector<ColumnFamilyDescriptor>* column_families) {
  if (argc <= 2) {
    if (argc <= 1) {
      PrintHelp(ldb_options, argv[0], /*to_stderr*/ true);
      return 1;
    } else if (std::string(argv[1]) == "--version") {
      printf("ldb from RocksDB %d.%d.%d\n", ROCKSDB_MAJOR, ROCKSDB_MINOR,
             ROCKSDB_PATCH);
      return 0;
    } else if (std::string(argv[1]) == "--help") {
      PrintHelp(ldb_options, argv[0], /*to_stderr*/ false);
      return 0;
    } else {
      PrintHelp(ldb_options, argv[0], /*to_stderr*/ true);
      return 1;
    }
  }

  LDBCommand* cmdObj = LDBCommand::InitFromCmdLineArgs(
      argc, argv, options, ldb_options, column_families);
  if (cmdObj == nullptr) {
    fprintf(stderr, "Unknown command\n");
    PrintHelp(ldb_options, argv[0], /*to_stderr*/ true);
    return 1;
  }

  if (!cmdObj->ValidateCmdLineOptions()) {
    return 1;
  }

  cmdObj->Run();
  LDBCommandExecuteResult ret = cmdObj->GetExecuteState();
  if (!ret.ToString().empty()) {
    fprintf(stderr, "%s\n", ret.ToString().c_str());
  }
  delete cmdObj;

  return ret.IsFailed() ? 1 : 0;
}

void LDBTool::Run(int argc, char** argv, const Options& options,
                  const LDBOptions& ldb_options,
                  const std::vector<ColumnFamilyDescriptor>* column_families) {
  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  exit(RunAndReturn(argc, argv, options, ldb_options, column_families));
}

int LDBTool::RunAndReturn(
    int argc, char** argv, const Options& options,
    const LDBOptions& ldb_options,
    const std::vector<ColumnFamilyDescriptor>* column_families) {
  return LDBCommandRunner::RunCommand(argc, argv, options, ldb_options,
                                      column_families);
}
}  // namespace ROCKSDB_NAMESPACE
