
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "rocksdb/utilities/ldb_cmd.h"

#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>

#include "db/blob/blob_index.h"
#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "db/log_reader.h"
#include "db/manifest_ops.h"
#include "db/version_set.h"
#include "db/version_util.h"
#include "db/wal_reader.h"
#include "db/wide/wide_column_serialization.h"
#include "db/wide/wide_columns_helper.h"
#include "db/write_batch_internal.h"
#include "db_stress_tool/db_stress_compression_manager.h"
#include "file/filename.h"
#include "options/options_parser.h"
#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/experimental.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/utilities/backup_engine.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/debug.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/sst_file_dumper.h"
#include "tools/ldb_cmd_impl.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/defer.h"
#include "util/file_checksum_helper.h"
#include "util/simple_mixed_compressor.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"
#include "util/write_batch_util.h"
#include "utilities/blob_db/blob_dump_tool.h"
#include "utilities/merge_operators.h"
#include "utilities/ttl/db_ttl_impl.h"

namespace ROCKSDB_NAMESPACE {

class FileChecksumGenCrc32c;
class FileChecksumGenCrc32cFactory;

const std::string LDBCommand::ARG_ENV_URI = "env_uri";
const std::string LDBCommand::ARG_FS_URI = "fs_uri";
const std::string LDBCommand::ARG_DB = "db";
const std::string LDBCommand::ARG_PATH = "path";
const std::string LDBCommand::ARG_SECONDARY_PATH = "secondary_path";
const std::string LDBCommand::ARG_LEADER_PATH = "leader_path";
const std::string LDBCommand::ARG_HEX = "hex";
const std::string LDBCommand::ARG_KEY_HEX = "key_hex";
const std::string LDBCommand::ARG_VALUE_HEX = "value_hex";
const std::string LDBCommand::ARG_CF_NAME = "column_family";
const std::string LDBCommand::ARG_TTL = "ttl";
const std::string LDBCommand::ARG_TTL_START = "start_time";
const std::string LDBCommand::ARG_TTL_END = "end_time";
const std::string LDBCommand::ARG_USE_TXN = "use_txn";
const std::string LDBCommand::ARG_TXN_WRITE_POLICY = "txn_write_policy";
const std::string LDBCommand::ARG_TIMESTAMP = "timestamp";
const std::string LDBCommand::ARG_TRY_LOAD_OPTIONS = "try_load_options";
const std::string LDBCommand::ARG_DISABLE_CONSISTENCY_CHECKS =
    "disable_consistency_checks";
const std::string LDBCommand::ARG_IGNORE_UNKNOWN_OPTIONS =
    "ignore_unknown_options";
const std::string LDBCommand::ARG_FROM = "from";
const std::string LDBCommand::ARG_TO = "to";
const std::string LDBCommand::ARG_MAX_KEYS = "max_keys";
const std::string LDBCommand::ARG_BLOOM_BITS = "bloom_bits";
const std::string LDBCommand::ARG_FIX_PREFIX_LEN = "fix_prefix_len";
const std::string LDBCommand::ARG_COMPRESSION_TYPE = "compression_type";
const std::string LDBCommand::ARG_COMPRESSION_MAX_DICT_BYTES =
    "compression_max_dict_bytes";
const std::string LDBCommand::ARG_BLOCK_SIZE = "block_size";
const std::string LDBCommand::ARG_UNIFORM_CV_THRESHOLD = "uniform_cv_threshold";
const std::string LDBCommand::ARG_AUTO_COMPACTION = "auto_compaction";
const std::string LDBCommand::ARG_DB_WRITE_BUFFER_SIZE = "db_write_buffer_size";
const std::string LDBCommand::ARG_WRITE_BUFFER_SIZE = "write_buffer_size";
const std::string LDBCommand::ARG_FILE_SIZE = "file_size";
const std::string LDBCommand::ARG_CREATE_IF_MISSING = "create_if_missing";
const std::string LDBCommand::ARG_NO_VALUE = "no_value";
const std::string LDBCommand::ARG_ENABLE_BLOB_FILES = "enable_blob_files";
const std::string LDBCommand::ARG_MIN_BLOB_SIZE = "min_blob_size";
const std::string LDBCommand::ARG_BLOB_FILE_SIZE = "blob_file_size";
const std::string LDBCommand::ARG_BLOB_COMPRESSION_TYPE =
    "blob_compression_type";
const std::string LDBCommand::ARG_ENABLE_BLOB_GARBAGE_COLLECTION =
    "enable_blob_garbage_collection";
const std::string LDBCommand::ARG_BLOB_GARBAGE_COLLECTION_AGE_CUTOFF =
    "blob_garbage_collection_age_cutoff";
const std::string LDBCommand::ARG_BLOB_GARBAGE_COLLECTION_FORCE_THRESHOLD =
    "blob_garbage_collection_force_threshold";
const std::string LDBCommand::ARG_BLOB_COMPACTION_READAHEAD_SIZE =
    "blob_compaction_readahead_size";
const std::string LDBCommand::ARG_BLOB_FILE_STARTING_LEVEL =
    "blob_file_starting_level";
const std::string LDBCommand::ARG_PREPOPULATE_BLOB_CACHE =
    "prepopulate_blob_cache";
const std::string LDBCommand::ARG_DECODE_BLOB_INDEX = "decode_blob_index";
const std::string LDBCommand::ARG_DUMP_UNCOMPRESSED_BLOBS =
    "dump_uncompressed_blobs";
const std::string LDBCommand::ARG_READ_TIMESTAMP = "read_timestamp";
const std::string LDBCommand::ARG_GET_WRITE_UNIX_TIME = "get_write_unix_time";

const char* LDBCommand::DELIM = " ==> ";

namespace {
// Helper class to iterate WAL logs in a directory in chronological order.
class WALFileIterator {
 public:
  explicit WALFileIterator(const std::string& parent_dir,
                           const std::vector<std::string>& filenames);
  // REQUIRES Valid() == true
  std::string GetNextWAL();
  bool Valid() const { return wal_file_iter_ != log_files_.end(); }

 private:
  // WAL log file names(s)
  std::string parent_dir_;
  std::vector<std::string> log_files_;
  std::vector<std::string>::const_iterator wal_file_iter_;
};

WALFileIterator::WALFileIterator(const std::string& parent_dir,
                                 const std::vector<std::string>& filenames)
    : parent_dir_(parent_dir) {
  // populate wal logs
  assert(!filenames.empty());
  for (const auto& fname : filenames) {
    uint64_t file_num = 0;
    FileType file_type;
    bool parse_ok = ParseFileName(fname, &file_num, &file_type);
    if (parse_ok && file_type == kWalFile) {
      log_files_.push_back(fname);
    }
  }

  std::sort(log_files_.begin(), log_files_.end(),
            [](const std::string& lhs, const std::string& rhs) {
              uint64_t num1 = 0;
              uint64_t num2 = 0;
              FileType type1;
              FileType type2;
              bool parse_ok1 = ParseFileName(lhs, &num1, &type1);
              bool parse_ok2 = ParseFileName(rhs, &num2, &type2);
#ifndef NDEBUG
              assert(parse_ok1);
              assert(parse_ok2);
#else
        (void)parse_ok1;
        (void)parse_ok2;
#endif
              return num1 < num2;
            });
  wal_file_iter_ = log_files_.begin();
}

std::string WALFileIterator::GetNextWAL() {
  assert(Valid());
  std::string ret;
  if (wal_file_iter_ != log_files_.end()) {
    ret.assign(parent_dir_);
    if (ret.back() != kFilePathSeparator) {
      ret.push_back(kFilePathSeparator);
    }
    ret.append(*wal_file_iter_);
    ++wal_file_iter_;
  }
  return ret;
}

void DumpWalFiles(Options options, const std::string& dir_or_file,
                  bool print_header, bool print_values,
                  bool only_print_seqno_gaps, bool is_write_committed,
                  const std::map<uint32_t, const Comparator*>& ucmps,
                  LDBCommandExecuteResult* exec_state);

void DumpWalFile(Options options, const std::string& wal_file,
                 bool print_header, bool print_values,
                 bool only_print_seqno_gaps, bool is_write_committed,
                 const std::map<uint32_t, const Comparator*>& ucmps,
                 LDBCommandExecuteResult* exec_state,
                 std::optional<SequenceNumber>* prev_batch_seqno,
                 std::optional<uint32_t>* prev_batch_count);

void DumpSstFile(Options options, std::string filename, bool output_hex,
                 bool show_properties, bool decode_blob_index,
                 std::string from_key = "", std::string to_key = "");

void DumpBlobFile(const std::string& filename, bool is_key_hex,
                  bool is_value_hex, bool dump_uncompressed_blobs);

Status EncodeUserProvidedTimestamp(const std::string& user_timestamp,
                                   std::string* ts_buf);
}  // namespace

LDBCommand* LDBCommand::InitFromCmdLineArgs(
    int argc, char const* const* argv, const Options& options,
    const LDBOptions& ldb_options,
    const std::vector<ColumnFamilyDescriptor>* column_families) {
  std::vector<std::string> args;
  for (int i = 1; i < argc; i++) {
    args.emplace_back(argv[i]);
  }
  return InitFromCmdLineArgs(args, options, ldb_options, column_families,
                             SelectCommand);
}

void LDBCommand::ParseSingleParam(const std::string& param,
                                  ParsedParams& parsed_params,
                                  std::vector<std::string>& cmd_tokens) {
  const std::string OPTION_PREFIX = "--";

  if (param[0] == '-' && param[1] == '-') {
    std::vector<std::string> splits = StringSplit(param, '=');
    // --option_name=option_value
    if (splits.size() == 2) {
      std::string optionKey = splits[0].substr(OPTION_PREFIX.size());
      parsed_params.option_map[optionKey] = splits[1];
    } else if (splits.size() == 1) {
      // --flag_name
      std::string optionKey = splits[0].substr(OPTION_PREFIX.size());
      parsed_params.flags.push_back(optionKey);
    } else {
      // --option_name=option_value, option_value contains '='
      std::string optionKey = splits[0].substr(OPTION_PREFIX.size());
      parsed_params.option_map[optionKey] =
          param.substr(splits[0].length() + 1);
    }
  } else {
    cmd_tokens.push_back(param);
  }
}

/**
 * Parse the command-line arguments and create the appropriate LDBCommand2
 * instance.
 * The command line arguments must be in the following format:
 * ./ldb --db=PATH_TO_DB [--commonOpt1=commonOpt1Val] ..
 *        COMMAND <PARAM1> <PARAM2> ... [-cmdSpecificOpt1=cmdSpecificOpt1Val] ..
 * This is similar to the command line format used by HBaseClientTool.
 * Command name is not included in args.
 * Returns nullptr if the command-line cannot be parsed.
 */
LDBCommand* LDBCommand::InitFromCmdLineArgs(
    const std::vector<std::string>& args, const Options& options,
    const LDBOptions& ldb_options,
    const std::vector<ColumnFamilyDescriptor>* column_families,
    const std::function<LDBCommand*(const ParsedParams&)>& selector) {
  // --x=y command line arguments are added as x->y map entries in
  // parsed_params.option_map.
  //
  // Command-line arguments of the form --hex end up in this array as hex to
  // parsed_params.flags
  ParsedParams parsed_params;

  // Everything other than option_map and flags. Represents commands
  // and their parameters.  For eg: put key1 value1 go into this vector.
  std::vector<std::string> cmdTokens;

  for (const auto& arg : args) {
    ParseSingleParam(arg, parsed_params, cmdTokens);
  }

  if (cmdTokens.size() < 1) {
    fprintf(stderr, "Command not specified!");
    return nullptr;
  }

  parsed_params.cmd = cmdTokens[0];
  parsed_params.cmd_params.assign(cmdTokens.begin() + 1, cmdTokens.end());

  LDBCommand* command = selector(parsed_params);

  if (command) {
    command->SetDBOptions(options);
    command->SetLDBOptions(ldb_options);
    command->SetColumnFamilies(column_families);
  }
  return command;
}

LDBCommand* LDBCommand::SelectCommand(const ParsedParams& parsed_params) {
  if (parsed_params.cmd == GetCommand::Name()) {
    return new GetCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  } else if (parsed_params.cmd == MultiGetCommand::Name()) {
    return new MultiGetCommand(parsed_params.cmd_params,
                               parsed_params.option_map, parsed_params.flags);
  } else if (parsed_params.cmd == GetEntityCommand::Name()) {
    return new GetEntityCommand(parsed_params.cmd_params,
                                parsed_params.option_map, parsed_params.flags);
  } else if (parsed_params.cmd == MultiGetEntityCommand::Name()) {
    return new MultiGetEntityCommand(parsed_params.cmd_params,
                                     parsed_params.option_map,
                                     parsed_params.flags);
  } else if (parsed_params.cmd == PutCommand::Name()) {
    return new PutCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  } else if (parsed_params.cmd == PutEntityCommand::Name()) {
    return new PutEntityCommand(parsed_params.cmd_params,
                                parsed_params.option_map, parsed_params.flags);
  } else if (parsed_params.cmd == BatchPutCommand::Name()) {
    return new BatchPutCommand(parsed_params.cmd_params,
                               parsed_params.option_map, parsed_params.flags);
  } else if (parsed_params.cmd == ScanCommand::Name()) {
    return new ScanCommand(parsed_params.cmd_params, parsed_params.option_map,
                           parsed_params.flags);
  } else if (parsed_params.cmd == DeleteCommand::Name()) {
    return new DeleteCommand(parsed_params.cmd_params, parsed_params.option_map,
                             parsed_params.flags);
  } else if (parsed_params.cmd == SingleDeleteCommand::Name()) {
    return new SingleDeleteCommand(parsed_params.cmd_params,
                                   parsed_params.option_map,
                                   parsed_params.flags);
  } else if (parsed_params.cmd == DeleteRangeCommand::Name()) {
    return new DeleteRangeCommand(parsed_params.cmd_params,
                                  parsed_params.option_map,
                                  parsed_params.flags);
  } else if (parsed_params.cmd == ApproxSizeCommand::Name()) {
    return new ApproxSizeCommand(parsed_params.cmd_params,
                                 parsed_params.option_map, parsed_params.flags);
  } else if (parsed_params.cmd == DBQuerierCommand::Name()) {
    return new DBQuerierCommand(parsed_params.cmd_params,
                                parsed_params.option_map, parsed_params.flags);
  } else if (parsed_params.cmd == CompactorCommand::Name()) {
    return new CompactorCommand(parsed_params.cmd_params,
                                parsed_params.option_map, parsed_params.flags);
  } else if (parsed_params.cmd == WALDumperCommand::Name()) {
    return new WALDumperCommand(parsed_params.cmd_params,
                                parsed_params.option_map, parsed_params.flags);
  } else if (parsed_params.cmd == ReduceDBLevelsCommand::Name()) {
    return new ReduceDBLevelsCommand(parsed_params.cmd_params,
                                     parsed_params.option_map,
                                     parsed_params.flags);
  } else if (parsed_params.cmd == ChangeCompactionStyleCommand::Name()) {
    return new ChangeCompactionStyleCommand(parsed_params.cmd_params,
                                            parsed_params.option_map,
                                            parsed_params.flags);
  } else if (parsed_params.cmd == DBDumperCommand::Name()) {
    return new DBDumperCommand(parsed_params.cmd_params,
                               parsed_params.option_map, parsed_params.flags);
  } else if (parsed_params.cmd == DBLoaderCommand::Name()) {
    return new DBLoaderCommand(parsed_params.cmd_params,
                               parsed_params.option_map, parsed_params.flags);
  } else if (parsed_params.cmd == ManifestDumpCommand::Name()) {
    return new ManifestDumpCommand(parsed_params.cmd_params,
                                   parsed_params.option_map,
                                   parsed_params.flags);
  } else if (parsed_params.cmd == FileChecksumDumpCommand::Name()) {
    return new FileChecksumDumpCommand(parsed_params.cmd_params,
                                       parsed_params.option_map,
                                       parsed_params.flags);
  } else if (parsed_params.cmd == GetPropertyCommand::Name()) {
    return new GetPropertyCommand(parsed_params.cmd_params,
                                  parsed_params.option_map,
                                  parsed_params.flags);
  } else if (parsed_params.cmd == ListColumnFamiliesCommand::Name()) {
    return new ListColumnFamiliesCommand(parsed_params.cmd_params,
                                         parsed_params.option_map,
                                         parsed_params.flags);
  } else if (parsed_params.cmd == CreateColumnFamilyCommand::Name()) {
    return new CreateColumnFamilyCommand(parsed_params.cmd_params,
                                         parsed_params.option_map,
                                         parsed_params.flags);
  } else if (parsed_params.cmd == DropColumnFamilyCommand::Name()) {
    return new DropColumnFamilyCommand(parsed_params.cmd_params,
                                       parsed_params.option_map,
                                       parsed_params.flags);
  } else if (parsed_params.cmd == DBFileDumperCommand::Name()) {
    return new DBFileDumperCommand(parsed_params.cmd_params,
                                   parsed_params.option_map,
                                   parsed_params.flags);
  } else if (parsed_params.cmd == DBLiveFilesMetadataDumperCommand::Name()) {
    return new DBLiveFilesMetadataDumperCommand(parsed_params.cmd_params,
                                                parsed_params.option_map,
                                                parsed_params.flags);
  } else if (parsed_params.cmd == InternalDumpCommand::Name()) {
    return new InternalDumpCommand(parsed_params.cmd_params,
                                   parsed_params.option_map,
                                   parsed_params.flags);
  } else if (parsed_params.cmd == CheckConsistencyCommand::Name()) {
    return new CheckConsistencyCommand(parsed_params.cmd_params,
                                       parsed_params.option_map,
                                       parsed_params.flags);
  } else if (parsed_params.cmd == CheckPointCommand::Name()) {
    return new CheckPointCommand(parsed_params.cmd_params,
                                 parsed_params.option_map, parsed_params.flags);
  } else if (parsed_params.cmd == RepairCommand::Name()) {
    return new RepairCommand(parsed_params.cmd_params, parsed_params.option_map,
                             parsed_params.flags);
  } else if (parsed_params.cmd == BackupCommand::Name()) {
    return new BackupCommand(parsed_params.cmd_params, parsed_params.option_map,
                             parsed_params.flags);
  } else if (parsed_params.cmd == RestoreCommand::Name()) {
    return new RestoreCommand(parsed_params.cmd_params,
                              parsed_params.option_map, parsed_params.flags);
  } else if (parsed_params.cmd == WriteExternalSstFilesCommand::Name()) {
    return new WriteExternalSstFilesCommand(parsed_params.cmd_params,
                                            parsed_params.option_map,
                                            parsed_params.flags);
  } else if (parsed_params.cmd == IngestExternalSstFilesCommand::Name()) {
    return new IngestExternalSstFilesCommand(parsed_params.cmd_params,
                                             parsed_params.option_map,
                                             parsed_params.flags);
  } else if (parsed_params.cmd == ListFileRangeDeletesCommand::Name()) {
    return new ListFileRangeDeletesCommand(parsed_params.option_map,
                                           parsed_params.flags);
  } else if (parsed_params.cmd == UnsafeRemoveSstFileCommand::Name()) {
    return new UnsafeRemoveSstFileCommand(parsed_params.cmd_params,
                                          parsed_params.option_map,
                                          parsed_params.flags);
  } else if (parsed_params.cmd == UpdateManifestCommand::Name()) {
    return new UpdateManifestCommand(parsed_params.cmd_params,
                                     parsed_params.option_map,
                                     parsed_params.flags);
  } else if (parsed_params.cmd == CompactionProgressDumpCommand::Name()) {
    return new CompactionProgressDumpCommand(parsed_params.cmd_params,
                                             parsed_params.option_map,
                                             parsed_params.flags);
  } else if (parsed_params.cmd == RemoteCompactionPrimaryCommand::Name()) {
    return new RemoteCompactionPrimaryCommand(parsed_params.cmd_params,
                                              parsed_params.option_map,
                                              parsed_params.flags);
  } else if (parsed_params.cmd == RemoteCompactionWorkerCommand::Name()) {
    return new RemoteCompactionWorkerCommand(parsed_params.cmd_params,
                                             parsed_params.option_map,
                                             parsed_params.flags);
  }
  return nullptr;
}

/* Run the command, and return the execute result. */
void LDBCommand::Run() {
  if (!exec_state_.IsNotStarted()) {
    return;
  }

  if (!options_.env || options_.env == Env::Default()) {
    Env* env = Env::Default();
    Status s = Env::CreateFromUri(config_options_, env_uri_, fs_uri_, &env,
                                  &env_guard_);
    if (!s.ok()) {
      fprintf(stderr, "%s\n", s.ToString().c_str());
      exec_state_ = LDBCommandExecuteResult::Failed(s.ToString());
      return;
    }
    options_.env = env;
  }

  if (db_ == nullptr && !NoDBOpen()) {
    OpenDB();
    if (exec_state_.IsFailed() && try_load_options_) {
      // We don't always return if there is a failure because a WAL file or
      // manifest file can be given to "dump" command so we should continue.
      // --try_load_options is not valid in those cases.
      return;
    }
  }

  // We'll intentionally proceed even if the DB can't be opened because users
  // can also specify a filename, not just a directory.
  DoCommand();

  if (exec_state_.IsNotStarted()) {
    exec_state_ = LDBCommandExecuteResult::Succeed("");
  }

  if (db_ != nullptr) {
    CloseDB();
  }
}

LDBCommand::LDBCommand(const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags, bool is_read_only,
                       const std::vector<std::string>& valid_cmd_line_options)
    : db_(nullptr),
      db_ttl_(nullptr),
      db_txn_(nullptr),
      is_read_only_(is_read_only),
      is_key_hex_(false),
      is_value_hex_(false),
      is_db_ttl_(false),
      is_db_txn_(false),
      txn_write_policy_(0),
      timestamp_(false),
      try_load_options_(false),
      create_if_missing_(false),
      option_map_(options),
      flags_(flags),
      valid_cmd_line_options_(valid_cmd_line_options) {
  auto itr = options.find(ARG_DB);
  if (itr != options.end()) {
    db_path_ = itr->second;
  }

  itr = options.find(ARG_ENV_URI);
  if (itr != options.end()) {
    env_uri_ = itr->second;
  }

  itr = options.find(ARG_FS_URI);
  if (itr != options.end()) {
    fs_uri_ = itr->second;
  }

  itr = options.find(ARG_CF_NAME);
  if (itr != options.end()) {
    column_family_name_ = itr->second;
  } else {
    column_family_name_ = kDefaultColumnFamilyName;
  }

  itr = options.find(ARG_SECONDARY_PATH);
  secondary_path_ = "";
  if (itr != options.end()) {
    secondary_path_ = itr->second;
  }

  itr = options.find(ARG_LEADER_PATH);
  leader_path_ = "";
  if (itr != options.end()) {
    leader_path_ = itr->second;
  }

  is_key_hex_ = IsKeyHex(options, flags);
  is_value_hex_ = IsValueHex(options, flags);
  is_db_ttl_ = IsFlagPresent(flags, ARG_TTL);
  is_db_txn_ = IsFlagPresent(flags, ARG_USE_TXN);
  itr = options.find(ARG_TXN_WRITE_POLICY);
  if (itr != options.end()) {
    try {
      txn_write_policy_ = std::stoi(itr->second);
      if (txn_write_policy_ < 0 || txn_write_policy_ > 2) {
        fprintf(stderr, "Invalid txn_write_policy: %d. Must be 0, 1, or 2.\n",
                txn_write_policy_);
        txn_write_policy_ = 0;
      }
    } catch (const std::exception&) {
      fprintf(stderr, "Invalid txn_write_policy value: %s\n",
              itr->second.c_str());
    }
  }
  timestamp_ = IsFlagPresent(flags, ARG_TIMESTAMP);
  try_load_options_ = IsTryLoadOptions(options, flags);
  force_consistency_checks_ =
      !IsFlagPresent(flags, ARG_DISABLE_CONSISTENCY_CHECKS);
  enable_blob_files_ = IsFlagPresent(flags, ARG_ENABLE_BLOB_FILES);
  enable_blob_garbage_collection_ =
      IsFlagPresent(flags, ARG_ENABLE_BLOB_GARBAGE_COLLECTION);
  config_options_.ignore_unknown_options =
      IsFlagPresent(flags, ARG_IGNORE_UNKNOWN_OPTIONS);
}

void LDBCommand::OpenDB() {
  PrepareOptions();
  if (!exec_state_.IsNotStarted()) {
    return;
  }
  if (column_families_.empty() && !options_.merge_operator) {
    // No harm to add a general merge operator if it is not specified.
    options_.merge_operator = MergeOperators::CreateStringAppendOperator(':');
  }
  // Open the DB.
  Status st;
  std::vector<ColumnFamilyHandle*> handles_opened;
  if (is_db_txn_) {
    // TransactionDB mode
    if (is_db_ttl_) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          "Cannot use both --ttl and --use_txn flags together");
      return;
    }
    if (!secondary_path_.empty() || !leader_path_.empty()) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          "TransactionDB does not support secondary or follower mode");
      return;
    }
    if (is_read_only_) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          "TransactionDB does not support read-only mode");
      return;
    }
    TransactionDBOptions txn_db_options;
    txn_db_options.write_policy =
        static_cast<TxnDBWritePolicy>(txn_write_policy_);
    if (column_families_.empty()) {
      st = TransactionDB::Open(options_, txn_db_options, db_path_, &db_txn_);
    } else {
      st = TransactionDB::Open(options_, txn_db_options, db_path_,
                               column_families_, &handles_opened, &db_txn_);
    }
    db_.reset(db_txn_);
  } else if (is_db_ttl_) {
    // ldb doesn't yet support TTL DB with multiple column families
    if (!column_family_name_.empty() || !column_families_.empty()) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          "ldb doesn't support TTL DB with multiple column families");
    }
    if (!secondary_path_.empty() || !leader_path_.empty()) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          "Open as secondary or follower is not supported for TTL DB yet.");
    }
    if (is_read_only_) {
      st = DBWithTTL::Open(options_, db_path_, &db_ttl_, 0, true);
    } else {
      st = DBWithTTL::Open(options_, db_path_, &db_ttl_);
    }
    db_.reset(db_ttl_);
  } else {
    if (!secondary_path_.empty() && !leader_path_.empty()) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          "Cannot provide both secondary and leader paths");
    }
    if (is_read_only_ && secondary_path_.empty() && leader_path_.empty()) {
      if (column_families_.empty()) {
        st = DB::OpenForReadOnly(options_, db_path_, &db_);
      } else {
        st = DB::OpenForReadOnly(options_, db_path_, column_families_,
                                 &handles_opened, &db_);
      }
    } else {
      if (column_families_.empty()) {
        if (secondary_path_.empty() && leader_path_.empty()) {
          st = DB::Open(options_, db_path_, &db_);
        } else if (!secondary_path_.empty()) {
          st = DB::OpenAsSecondary(options_, db_path_, secondary_path_, &db_);
        } else {
          st = DB::OpenAsFollower(options_, db_path_, leader_path_, &db_);
        }
      } else {
        if (secondary_path_.empty() && leader_path_.empty()) {
          st = DB::Open(options_, db_path_, column_families_, &handles_opened,
                        &db_);
        } else if (!secondary_path_.empty()) {
          st = DB::OpenAsSecondary(options_, db_path_, secondary_path_,
                                   column_families_, &handles_opened, &db_);
        } else {
          st = DB::OpenAsFollower(options_, db_path_, leader_path_,
                                  column_families_, &handles_opened, &db_);
        }
      }
    }
  }
  if (!st.ok()) {
    std::string msg = st.ToString();
    exec_state_ = LDBCommandExecuteResult::Failed(msg);
  } else if (!handles_opened.empty()) {
    assert(handles_opened.size() == column_families_.size());
    bool found_cf_name = false;
    for (size_t i = 0; i < handles_opened.size(); i++) {
      cf_handles_[column_families_[i].name] = handles_opened[i];
      ucmps_[handles_opened[i]->GetID()] = handles_opened[i]->GetComparator();
      if (column_family_name_ == column_families_[i].name) {
        found_cf_name = true;
      }
    }
    if (!found_cf_name) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          "Non-existing column family " + column_family_name_);
      CloseDB();
    }
    ColumnFamilyHandle* default_cf = db_->DefaultColumnFamily();
    ucmps_[default_cf->GetID()] = default_cf->GetComparator();
  } else {
    // We successfully opened DB in single column family mode.
    assert(column_families_.empty());
    if (column_family_name_ != kDefaultColumnFamilyName) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          "Non-existing column family " + column_family_name_);
      CloseDB();
    }
    ColumnFamilyHandle* default_cf = db_->DefaultColumnFamily();
    ucmps_[default_cf->GetID()] = default_cf->GetComparator();
  }
}

void LDBCommand::CloseDB() {
  if (db_ != nullptr) {
    for (auto& pair : cf_handles_) {
      delete pair.second;
    }
    Status s = db_->Close();
    s.PermitUncheckedError();
    db_.reset();
    db_ttl_ = nullptr;
    db_txn_ = nullptr;
  }
}

ColumnFamilyHandle* LDBCommand::GetCfHandle() {
  if (!cf_handles_.empty()) {
    auto it = cf_handles_.find(column_family_name_);
    if (it == cf_handles_.end()) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          "Cannot find column family " + column_family_name_);
    } else {
      return it->second;
    }
  }
  return db_->DefaultColumnFamily();
}

std::vector<std::string> LDBCommand::BuildCmdLineOptions(
    std::vector<std::string> options) {
  std::vector<std::string> ret = {ARG_ENV_URI,
                                  ARG_FS_URI,
                                  ARG_DB,
                                  ARG_SECONDARY_PATH,
                                  ARG_LEADER_PATH,
                                  ARG_BLOOM_BITS,
                                  ARG_BLOCK_SIZE,
                                  ARG_UNIFORM_CV_THRESHOLD,
                                  ARG_AUTO_COMPACTION,
                                  ARG_COMPRESSION_TYPE,
                                  ARG_COMPRESSION_MAX_DICT_BYTES,
                                  ARG_WRITE_BUFFER_SIZE,
                                  ARG_FILE_SIZE,
                                  ARG_FIX_PREFIX_LEN,
                                  ARG_TRY_LOAD_OPTIONS,
                                  ARG_DISABLE_CONSISTENCY_CHECKS,
                                  ARG_ENABLE_BLOB_FILES,
                                  ARG_MIN_BLOB_SIZE,
                                  ARG_BLOB_FILE_SIZE,
                                  ARG_BLOB_COMPRESSION_TYPE,
                                  ARG_ENABLE_BLOB_GARBAGE_COLLECTION,
                                  ARG_BLOB_GARBAGE_COLLECTION_AGE_CUTOFF,
                                  ARG_BLOB_GARBAGE_COLLECTION_FORCE_THRESHOLD,
                                  ARG_BLOB_COMPACTION_READAHEAD_SIZE,
                                  ARG_BLOB_FILE_STARTING_LEVEL,
                                  ARG_PREPOPULATE_BLOB_CACHE,
                                  ARG_IGNORE_UNKNOWN_OPTIONS,
                                  ARG_CF_NAME,
                                  ARG_USE_TXN,
                                  ARG_TXN_WRITE_POLICY};
  ret.insert(ret.end(), options.begin(), options.end());
  return ret;
}

/**
 * Parses the specific double option and fills in the value.
 * Returns true if the option is found.
 * Returns false if the option is not found or if there is an error parsing the
 * value.  If there is an error, the specified exec_state is also
 * updated.
 */
bool LDBCommand::ParseDoubleOption(
    const std::map<std::string, std::string>& /*options*/,
    const std::string& option, double& value,
    LDBCommandExecuteResult& exec_state) {
  auto itr = option_map_.find(option);
  if (itr != option_map_.end()) {
#if defined(CYGWIN)
    char* str_end = nullptr;
    value = std::strtod(itr->second.c_str(), &str_end);
    if (str_end == itr->second.c_str()) {
      exec_state =
          LDBCommandExecuteResult::Failed(option + " has an invalid value.");
    } else if (errno == ERANGE) {
      exec_state = LDBCommandExecuteResult::Failed(
          option + " has a value out-of-range.");
    } else {
      return true;
    }
#else
    try {
      value = std::stod(itr->second);
      return true;
    } catch (const std::invalid_argument&) {
      exec_state =
          LDBCommandExecuteResult::Failed(option + " has an invalid value.");
    } catch (const std::out_of_range&) {
      exec_state = LDBCommandExecuteResult::Failed(
          option + " has a value out-of-range.");
    }
#endif
  }
  return false;
}

/**
 * Parses the specific integer option and fills in the value.
 * Returns true if the option is found.
 * Returns false if the option is not found or if there is an error parsing the
 * value.  If there is an error, the specified exec_state is also
 * updated.
 */
bool LDBCommand::ParseIntOption(
    const std::map<std::string, std::string>& /*options*/,
    const std::string& option, int& value,
    LDBCommandExecuteResult& exec_state) {
  auto itr = option_map_.find(option);
  if (itr != option_map_.end()) {
#if defined(CYGWIN)
    char* str_end = nullptr;
    value = strtol(itr->second.c_str(), &str_end, 10);
    if (str_end == itr->second.c_str()) {
      exec_state =
          LDBCommandExecuteResult::Failed(option + " has an invalid value.");
    } else if (errno == ERANGE) {
      exec_state = LDBCommandExecuteResult::Failed(
          option + " has a value out-of-range.");
    } else {
      return true;
    }
#else
    try {
      value = std::stoi(itr->second);
      return true;
    } catch (const std::invalid_argument&) {
      exec_state =
          LDBCommandExecuteResult::Failed(option + " has an invalid value.");
    } catch (const std::out_of_range&) {
      exec_state = LDBCommandExecuteResult::Failed(
          option + " has a value out-of-range.");
    }
#endif
  }
  return false;
}

/**
 * Parses the specified option and fills in the value.
 * Returns true if the option is found.
 * Returns false otherwise.
 */
bool LDBCommand::ParseStringOption(
    const std::map<std::string, std::string>& /*options*/,
    const std::string& option, std::string* value) {
  auto itr = option_map_.find(option);
  if (itr != option_map_.end()) {
    *value = itr->second;
    return true;
  }
  return false;
}

/**
 * Parses the specified compression type and fills in the value.
 * Returns true if the compression type is found.
 * Returns false otherwise.
 */
bool LDBCommand::ParseCompressionTypeOption(
    const std::map<std::string, std::string>& /*options*/,
    const std::string& option, CompressionType& value,
    LDBCommandExecuteResult& exec_state) {
  auto itr = option_map_.find(option);
  if (itr != option_map_.end()) {
    const std::string& comp = itr->second;
    if (comp == "no") {
      value = kNoCompression;
      return true;
    } else if (comp == "snappy") {
      if (!Snappy_Supported()) {
        exec_state = LDBCommandExecuteResult::Failed(
            "Snappy compression is not supported in this build.");
      } else {
        value = kSnappyCompression;
        return true;
      }
    } else if (comp == "zlib") {
      if (!Zlib_Supported()) {
        exec_state = LDBCommandExecuteResult::Failed(
            "zlib compression is not supported in this build.");
      } else {
        value = kZlibCompression;
        return true;
      }
    } else if (comp == "bzip2") {
      if (!BZip2_Supported()) {
        exec_state = LDBCommandExecuteResult::Failed(
            "bzip2 compression is not supported in this build.");
      } else {
        value = kBZip2Compression;
        return true;
      }
    } else if (comp == "lz4") {
      if (!LZ4_Supported()) {
        exec_state = LDBCommandExecuteResult::Failed(
            "lz4 compression is not supported in this build.");
      } else {
        value = kLZ4Compression;
        return true;
      }
    } else if (comp == "lz4hc") {
      if (!LZ4_Supported()) {
        exec_state = LDBCommandExecuteResult::Failed(
            "lz4hc compression is not supported in this build.");
      } else {
        value = kLZ4HCCompression;
        return true;
      }
    } else if (comp == "xpress") {
      if (!XPRESS_Supported()) {
        exec_state = LDBCommandExecuteResult::Failed(
            "xpress compression is not supported in this build.");
      } else {
        value = kXpressCompression;
        return true;
      }
    } else if (comp == "zstd") {
      if (!ZSTD_Supported()) {
        exec_state = LDBCommandExecuteResult::Failed(
            "zstd compression is not supported in this build.");
      } else {
        value = kZSTD;
        return true;
      }
#ifndef NDEBUG
    } else if (comp == "mixed" && option == ARG_COMPRESSION_TYPE) {
      if (GetSupportedCompressions().empty()) {
        exec_state = LDBCommandExecuteResult::Failed(
            "No compressions are supported in this build for \"mixed\".");
        return false;
      }
      options_.compression = kZSTD;
      options_.bottommost_compression = kZSTD;
      auto mgr =
          std::make_shared<RoundRobinManager>(GetBuiltinV2CompressionManager());
      options_.compression_manager = mgr;

      // Need to list zstd in the compression_name table property if it's
      // potentially used by being in the mix (i.e., potentially at least one
      // data block in the table is compressed by zstd). This ensures proper
      // context and dictionary handling, and prevents crashes in older RocksDB
      // versions.
      //
      // To achieve this, set `value` (the compression_type in Options which
      // will be used to set compression_name table property) to kZSTD, even if
      // multiple compression types are used within a single table.
      value = ZSTD_Supported() ? kZSTD : GetSupportedCompressions()[0];
      return true;
#endif  // !NDEBUG
    } else {
      // Unknown compression.
      exec_state = LDBCommandExecuteResult::Failed(
          "Unknown compression algorithm: " + comp);
    }
  }
  return false;
}

Status LDBCommand::MaybePopulateReadTimestamp(ColumnFamilyHandle* cfh,
                                              ReadOptions& ropts,
                                              Slice* read_timestamp) {
  const size_t ts_sz = cfh->GetComparator()->timestamp_size();

  auto iter = option_map_.find(ARG_READ_TIMESTAMP);
  if (iter == option_map_.end()) {
    if (ts_sz == 0) {
      return Status::OK();
    }
    return Status::InvalidArgument(
        "column family enables user-defined timestamp while --read_timestamp "
        "is not provided.");
  }
  if (iter->second.empty()) {
    if (ts_sz == 0) {
      return Status::OK();
    }
    return Status::InvalidArgument(
        "column family enables user-defined timestamp while --read_timestamp "
        "is empty.");
  }
  if (ts_sz == 0) {
    return Status::InvalidArgument(
        "column family does not enable user-defined timestamps while "
        "--read_timestamp is provided.");
  }
  Status s = EncodeUserProvidedTimestamp(iter->second, &read_timestamp_);
  if (!s.ok()) {
    return s;
  }
  *read_timestamp = read_timestamp_;
  ropts.timestamp = read_timestamp;
  return Status::OK();
}

void LDBCommand::OverrideBaseOptions() {
  options_.create_if_missing = false;

  int db_write_buffer_size;
  if (ParseIntOption(option_map_, ARG_DB_WRITE_BUFFER_SIZE,
                     db_write_buffer_size, exec_state_)) {
    if (db_write_buffer_size >= 0) {
      options_.db_write_buffer_size = db_write_buffer_size;
    } else {
      exec_state_ = LDBCommandExecuteResult::Failed(ARG_DB_WRITE_BUFFER_SIZE +
                                                    " must be >= 0.");
    }
  }

  if (options_.db_paths.size() == 0) {
    options_.db_paths.emplace_back(db_path_,
                                   std::numeric_limits<uint64_t>::max());
  }

  OverrideBaseCFOptions(static_cast<ColumnFamilyOptions*>(&options_));
}

void LDBCommand::OverrideBaseCFOptions(ColumnFamilyOptions* cf_opts) {
  BlockBasedTableOptions table_options;
  bool use_table_options = false;
  int bits;
  if (ParseIntOption(option_map_, ARG_BLOOM_BITS, bits, exec_state_)) {
    if (bits > 0) {
      use_table_options = true;
      table_options.filter_policy.reset(NewBloomFilterPolicy(bits));
    } else {
      exec_state_ =
          LDBCommandExecuteResult::Failed(ARG_BLOOM_BITS + " must be > 0.");
    }
  }

  int block_size;
  if (ParseIntOption(option_map_, ARG_BLOCK_SIZE, block_size, exec_state_)) {
    if (block_size > 0) {
      use_table_options = true;
      table_options.block_size = block_size;
    } else {
      exec_state_ =
          LDBCommandExecuteResult::Failed(ARG_BLOCK_SIZE + " must be > 0.");
    }
  }

  double uniform_cv_threshold;
  if (ParseDoubleOption(option_map_, ARG_UNIFORM_CV_THRESHOLD,
                        uniform_cv_threshold, exec_state_)) {
    use_table_options = true;
    table_options.uniform_cv_threshold = uniform_cv_threshold;
  }

  // Default comparator is BytewiseComparator, so only when it's not, it
  // means user has a command line override.
  if (options_.comparator != nullptr &&
      options_.comparator != BytewiseComparator()) {
    cf_opts->comparator = options_.comparator;
  }

  cf_opts->force_consistency_checks = force_consistency_checks_;
  if (use_table_options) {
    cf_opts->table_factory.reset(NewBlockBasedTableFactory(table_options));
  }

  cf_opts->enable_blob_files = enable_blob_files_;

  int min_blob_size;
  if (ParseIntOption(option_map_, ARG_MIN_BLOB_SIZE, min_blob_size,
                     exec_state_)) {
    if (min_blob_size >= 0) {
      cf_opts->min_blob_size = min_blob_size;
    } else {
      exec_state_ =
          LDBCommandExecuteResult::Failed(ARG_MIN_BLOB_SIZE + " must be >= 0.");
    }
  }

  int blob_file_size;
  if (ParseIntOption(option_map_, ARG_BLOB_FILE_SIZE, blob_file_size,
                     exec_state_)) {
    if (blob_file_size > 0) {
      cf_opts->blob_file_size = blob_file_size;
    } else {
      exec_state_ =
          LDBCommandExecuteResult::Failed(ARG_BLOB_FILE_SIZE + " must be > 0.");
    }
  }

  cf_opts->enable_blob_garbage_collection = enable_blob_garbage_collection_;

  double blob_garbage_collection_age_cutoff;
  if (ParseDoubleOption(option_map_, ARG_BLOB_GARBAGE_COLLECTION_AGE_CUTOFF,
                        blob_garbage_collection_age_cutoff, exec_state_)) {
    if (blob_garbage_collection_age_cutoff >= 0 &&
        blob_garbage_collection_age_cutoff <= 1) {
      cf_opts->blob_garbage_collection_age_cutoff =
          blob_garbage_collection_age_cutoff;
    } else {
      exec_state_ = LDBCommandExecuteResult::Failed(
          ARG_BLOB_GARBAGE_COLLECTION_AGE_CUTOFF + " must be >= 0 and <= 1.");
    }
  }

  double blob_garbage_collection_force_threshold;
  if (ParseDoubleOption(option_map_,
                        ARG_BLOB_GARBAGE_COLLECTION_FORCE_THRESHOLD,
                        blob_garbage_collection_force_threshold, exec_state_)) {
    if (blob_garbage_collection_force_threshold >= 0 &&
        blob_garbage_collection_force_threshold <= 1) {
      cf_opts->blob_garbage_collection_force_threshold =
          blob_garbage_collection_force_threshold;
    } else {
      exec_state_ = LDBCommandExecuteResult::Failed(
          ARG_BLOB_GARBAGE_COLLECTION_FORCE_THRESHOLD +
          " must be >= 0 and <= 1.");
    }
  }

  int blob_compaction_readahead_size;
  if (ParseIntOption(option_map_, ARG_BLOB_COMPACTION_READAHEAD_SIZE,
                     blob_compaction_readahead_size, exec_state_)) {
    if (blob_compaction_readahead_size > 0) {
      cf_opts->blob_compaction_readahead_size = blob_compaction_readahead_size;
    } else {
      exec_state_ = LDBCommandExecuteResult::Failed(
          ARG_BLOB_COMPACTION_READAHEAD_SIZE + " must be > 0.");
    }
  }

  int blob_file_starting_level;
  if (ParseIntOption(option_map_, ARG_BLOB_FILE_STARTING_LEVEL,
                     blob_file_starting_level, exec_state_)) {
    if (blob_file_starting_level >= 0) {
      cf_opts->blob_file_starting_level = blob_file_starting_level;
    } else {
      exec_state_ = LDBCommandExecuteResult::Failed(
          ARG_BLOB_FILE_STARTING_LEVEL + " must be >= 0.");
    }
  }

  int prepopulate_blob_cache;
  if (ParseIntOption(option_map_, ARG_PREPOPULATE_BLOB_CACHE,
                     prepopulate_blob_cache, exec_state_)) {
    switch (prepopulate_blob_cache) {
      case 0:
        cf_opts->prepopulate_blob_cache = PrepopulateBlobCache::kDisable;
        break;
      case 1:
        cf_opts->prepopulate_blob_cache = PrepopulateBlobCache::kFlushOnly;
        break;
      default:
        exec_state_ = LDBCommandExecuteResult::Failed(
            ARG_PREPOPULATE_BLOB_CACHE +
            " must be 0 (disable) or 1 (flush only).");
    }
  }

  auto itr = option_map_.find(ARG_AUTO_COMPACTION);
  if (itr != option_map_.end()) {
    cf_opts->disable_auto_compactions = !StringToBool(itr->second);
  }

  CompressionType compression_type;
  if (ParseCompressionTypeOption(option_map_, ARG_COMPRESSION_TYPE,
                                 compression_type, exec_state_)) {
    cf_opts->compression = compression_type;
  }

  CompressionType blob_compression_type;
  if (ParseCompressionTypeOption(option_map_, ARG_BLOB_COMPRESSION_TYPE,
                                 blob_compression_type, exec_state_)) {
    cf_opts->blob_compression_type = blob_compression_type;
  }

  int compression_max_dict_bytes;
  if (ParseIntOption(option_map_, ARG_COMPRESSION_MAX_DICT_BYTES,
                     compression_max_dict_bytes, exec_state_)) {
    if (compression_max_dict_bytes >= 0) {
      cf_opts->compression_opts.max_dict_bytes = compression_max_dict_bytes;
    } else {
      exec_state_ = LDBCommandExecuteResult::Failed(
          ARG_COMPRESSION_MAX_DICT_BYTES + " must be >= 0.");
    }
  }

  int write_buffer_size;
  if (ParseIntOption(option_map_, ARG_WRITE_BUFFER_SIZE, write_buffer_size,
                     exec_state_)) {
    if (write_buffer_size > 0) {
      cf_opts->write_buffer_size = write_buffer_size;
    } else {
      exec_state_ = LDBCommandExecuteResult::Failed(ARG_WRITE_BUFFER_SIZE +
                                                    " must be > 0.");
    }
  }

  int file_size;
  if (ParseIntOption(option_map_, ARG_FILE_SIZE, file_size, exec_state_)) {
    if (file_size > 0) {
      cf_opts->target_file_size_base = file_size;
    } else {
      exec_state_ =
          LDBCommandExecuteResult::Failed(ARG_FILE_SIZE + " must be > 0.");
    }
  }

  int fix_prefix_len;
  if (ParseIntOption(option_map_, ARG_FIX_PREFIX_LEN, fix_prefix_len,
                     exec_state_)) {
    if (fix_prefix_len > 0) {
      cf_opts->prefix_extractor.reset(
          NewFixedPrefixTransform(static_cast<size_t>(fix_prefix_len)));
    } else {
      exec_state_ =
          LDBCommandExecuteResult::Failed(ARG_FIX_PREFIX_LEN + " must be > 0.");
    }
  }
}

// First, initializes the options state using the OPTIONS file when enabled.
// Second, overrides the options according to the CLI arguments and the
// specific subcommand being run.
void LDBCommand::PrepareOptions() {
  DbStressCustomCompressionManager::Register();
  std::vector<ColumnFamilyDescriptor> column_families_from_options;

  if (!create_if_missing_ && try_load_options_) {
    config_options_.env = options_.env;
    Status s = LoadLatestOptions(config_options_, db_path_, &options_,
                                 &column_families_from_options);
    if (!s.ok() && !s.IsNotFound()) {
      // Option file exists but load option file error.
      std::string current_version = std::to_string(ROCKSDB_MAJOR) + "." +
                                    std::to_string(ROCKSDB_MINOR) + "." +
                                    std::to_string(ROCKSDB_PATCH);
      std::string msg =
          s.ToString() + "\nThis tool was built with version " +
          current_version +
          ". If your db is in a different version, please try again "
          "with option --" +
          LDBCommand::ARG_IGNORE_UNKNOWN_OPTIONS + ".";
      exec_state_ = LDBCommandExecuteResult::Failed(msg);
      db_ = nullptr;
      return;
    }
    if (!options_.wal_dir.empty()) {
      if (options_.env->FileExists(options_.wal_dir).IsNotFound()) {
        options_.wal_dir = db_path_;
        fprintf(
            stderr,
            "wal_dir loaded from the option file doesn't exist. Ignore it.\n");
      }
    }

    // If merge operator is not set, set a string append operator.
    for (auto& cf_entry : column_families_from_options) {
      if (!cf_entry.options.merge_operator) {
        cf_entry.options.merge_operator =
            MergeOperators::CreateStringAppendOperator(':');
      }
    }
  }

  if (options_.env == Env::Default()) {
    options_.env = config_options_.env;
  }

  OverrideBaseOptions();
  if (exec_state_.IsFailed()) {
    return;
  }

  if (column_families_.empty()) {
    // column_families not set. Either set it from MANIFEST or OPTIONS file.
    if (column_families_from_options.empty()) {
      // Reads the MANIFEST to figure out what column families exist. In this
      // case, the option overrides from the CLI argument/specific subcommand
      // apply to all column families.
      std::vector<std::string> cf_list;
      Status st = DB::ListColumnFamilies(options_, db_path_, &cf_list);
      // It is possible the DB doesn't exist yet, for "create if not
      // existing" case. The failure is ignored here. We rely on DB::Open()
      // to give us the correct error message for problem with opening
      // existing DB.
      if (st.ok() && cf_list.size() > 1) {
        // Ignore single column family DB.
        for (const auto& cf_name : cf_list) {
          column_families_.emplace_back(cf_name, options_);
        }
      }
    } else {
      SetColumnFamilies(&column_families_from_options);
    }
  }

  if (!column_families_from_options.empty()) {
    // We got column families from the OPTIONS file. In this case, the option
    // overrides from the CLI argument/specific subcommand only apply to the
    // column family specified by `--column_family_name`.
    auto column_families_iter =
        std::find_if(column_families_.begin(), column_families_.end(),
                     [this](const ColumnFamilyDescriptor& cf_desc) {
                       return cf_desc.name == column_family_name_;
                     });
    if (column_families_iter == column_families_.end()) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          "Non-existing column family " + column_family_name_);
      return;
    }
    OverrideBaseCFOptions(&column_families_iter->options);
  }
}

bool LDBCommand::ParseKeyValue(const std::string& line, std::string* key,
                               std::string* value, bool is_key_hex,
                               bool is_value_hex) {
  size_t pos = line.find(DELIM);
  if (pos != std::string::npos) {
    *key = line.substr(0, pos);
    *value = line.substr(pos + strlen(DELIM));
    if (is_key_hex) {
      *key = HexToString(*key);
    }
    if (is_value_hex) {
      *value = HexToString(*value);
    }
    return true;
  } else {
    return false;
  }
}

/**
 * Make sure that ONLY the command-line options and flags expected by this
 * command are specified on the command-line.  Extraneous options are usually
 * the result of user error.
 * Returns true if all checks pass.  Else returns false, and prints an
 * appropriate error msg to stderr.
 */
bool LDBCommand::ValidateCmdLineOptions() {
  for (auto itr = option_map_.begin(); itr != option_map_.end(); ++itr) {
    if (std::find(valid_cmd_line_options_.begin(),
                  valid_cmd_line_options_.end(),
                  itr->first) == valid_cmd_line_options_.end()) {
      fprintf(stderr, "Invalid command-line option %s\n", itr->first.c_str());
      return false;
    }
  }

  for (std::vector<std::string>::const_iterator itr = flags_.begin();
       itr != flags_.end(); ++itr) {
    if (std::find(valid_cmd_line_options_.begin(),
                  valid_cmd_line_options_.end(),
                  *itr) == valid_cmd_line_options_.end()) {
      fprintf(stderr, "Invalid command-line flag %s\n", itr->c_str());
      return false;
    }
  }

  if (!NoDBOpen() && option_map_.find(ARG_DB) == option_map_.end() &&
      option_map_.find(ARG_PATH) == option_map_.end()) {
    fprintf(stderr, "Either %s or %s must be specified.\n", ARG_DB.c_str(),
            ARG_PATH.c_str());
    return false;
  }

  return true;
}

std::string LDBCommand::HexToString(const std::string& str) {
  std::string result;
  std::string::size_type len = str.length();
  if (len < 2 || str[0] != '0' || str[1] != 'x') {
    fprintf(stderr, "Invalid hex input %s.  Must start with 0x\n", str.c_str());
    throw "Invalid hex input";
  }
  if (!Slice(str.data() + 2, len - 2).DecodeHex(&result)) {
    throw "Invalid hex input";
  }
  return result;
}

std::string LDBCommand::StringToHex(const std::string& str) {
  std::string result("0x");
  result.append(Slice(str).ToString(true));
  return result;
}

std::string LDBCommand::PrintKeyValue(const std::string& key,
                                      const std::string& timestamp,
                                      const std::string& value, bool is_key_hex,
                                      bool is_value_hex,
                                      const Comparator* ucmp) {
  std::string result;
  result.append(is_key_hex ? StringToHex(key) : key);
  if (!timestamp.empty()) {
    result.append("|timestamp:");
    result.append(ucmp->TimestampToString(timestamp));
  }
  result.append(DELIM);
  result.append(is_value_hex ? StringToHex(value) : value);
  return result;
}

std::string LDBCommand::PrintKeyValue(const std::string& key,
                                      const std::string& timestamp,
                                      const std::string& value, bool is_hex,
                                      const Comparator* ucmp) {
  return PrintKeyValue(key, timestamp, value, is_hex, is_hex, ucmp);
}

std::string LDBCommand::PrintKeyValueOrWideColumns(
    const Slice& key, const Slice& timestamp, const Slice& value,
    const WideColumns& wide_columns, bool is_key_hex, bool is_value_hex,
    const Comparator* ucmp) {
  if (wide_columns.empty() ||
      WideColumnsHelper::HasDefaultColumnOnly(wide_columns)) {
    return PrintKeyValue(key.ToString(), timestamp.ToString(), value.ToString(),
                         is_key_hex, is_value_hex, ucmp);
  }
  /*
  // Sample plaintext output (first column is kDefaultWideColumnName)
  key_1 ==> :foo attr_name1:bar attr_name2:baz

  // Sample hex output (first column is kDefaultWideColumnName)
  0x6669727374 ==> :0x68656C6C6F 0x617474725F6E616D6531:0x666F6F
  */
  std::ostringstream oss;
  WideColumnsHelper::DumpWideColumns(wide_columns, oss, is_value_hex);
  return PrintKeyValue(key.ToString(), timestamp.ToString(), oss.str().c_str(),
                       is_key_hex, false,
                       ucmp);  // is_value_hex_ is already honored in oss.
                               // avoid double-hexing it.
}

std::string LDBCommand::HelpRangeCmdArgs() {
  std::ostringstream str_stream;
  str_stream << " ";
  str_stream << "[--" << ARG_FROM << "] ";
  str_stream << "[--" << ARG_TO << "] ";
  return str_stream.str();
}

bool LDBCommand::IsKeyHex(const std::map<std::string, std::string>& options,
                          const std::vector<std::string>& flags) {
  return (IsFlagPresent(flags, ARG_HEX) || IsFlagPresent(flags, ARG_KEY_HEX) ||
          ParseBooleanOption(options, ARG_HEX, false) ||
          ParseBooleanOption(options, ARG_KEY_HEX, false));
}

bool LDBCommand::IsValueHex(const std::map<std::string, std::string>& options,
                            const std::vector<std::string>& flags) {
  return (IsFlagPresent(flags, ARG_HEX) ||
          IsFlagPresent(flags, ARG_VALUE_HEX) ||
          ParseBooleanOption(options, ARG_HEX, false) ||
          ParseBooleanOption(options, ARG_VALUE_HEX, false));
}

bool LDBCommand::IsTryLoadOptions(
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags) {
  if (IsFlagPresent(flags, ARG_TRY_LOAD_OPTIONS)) {
    return true;
  }
  // if `DB` is specified and not explicitly to create a new db, default
  // `try_load_options` to true. The user could still disable that by set
  // `try_load_options=false`.
  // Note: Opening as TTL DB doesn't support `try_load_options`, so it's default
  // to false. TODO: TTL_DB may need to fix that, otherwise it's unable to open
  // DB which has incompatible setting with default options.
  bool default_val = (options.find(ARG_DB) != options.end()) &&
                     !IsFlagPresent(flags, ARG_CREATE_IF_MISSING) &&
                     !IsFlagPresent(flags, ARG_TTL);
  return ParseBooleanOption(options, ARG_TRY_LOAD_OPTIONS, default_val);
}

bool LDBCommand::ParseBooleanOption(
    const std::map<std::string, std::string>& options,
    const std::string& option, bool default_val) {
  auto itr = options.find(option);
  if (itr != options.end()) {
    std::string option_val = itr->second;
    return StringToBool(itr->second);
  }
  return default_val;
}

bool LDBCommand::StringToBool(std::string val) {
  std::transform(val.begin(), val.end(), val.begin(),
                 [](char ch) -> char { return (char)::tolower(ch); });

  if (val == "true") {
    return true;
  } else if (val == "false") {
    return false;
  } else {
    throw "Invalid value for boolean argument";
  }
}

CompactorCommand::CompactorCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, false /* is_read_only */,
                 BuildCmdLineOptions({ARG_FROM, ARG_TO, ARG_HEX, ARG_KEY_HEX,
                                      ARG_VALUE_HEX, ARG_TTL})),
      null_from_(true),
      null_to_(true) {
  auto itr = options.find(ARG_FROM);
  if (itr != options.end()) {
    null_from_ = false;
    from_ = itr->second;
  }

  itr = options.find(ARG_TO);
  if (itr != options.end()) {
    null_to_ = false;
    to_ = itr->second;
  }

  if (is_key_hex_) {
    if (!null_from_) {
      from_ = HexToString(from_);
    }
    if (!null_to_) {
      to_ = HexToString(to_);
    }
  }
}

void CompactorCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(CompactorCommand::Name());
  ret.append(HelpRangeCmdArgs());
  ret.append("\n");
}

void CompactorCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }

  Slice* begin = nullptr;
  Slice* end = nullptr;
  if (!null_from_) {
    begin = new Slice(from_);
  }
  if (!null_to_) {
    end = new Slice(to_);
  }

  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForceOptimized;

  Status s = db_->CompactRange(cro, GetCfHandle(), begin, end);
  if (!s.ok()) {
    std::stringstream oss;
    oss << "Compaction failed: " << s.ToString();
    exec_state_ = LDBCommandExecuteResult::Failed(oss.str());
  } else {
    exec_state_ = LDBCommandExecuteResult::Succeed("");
  }

  delete begin;
  delete end;
}

// ---------------------------------------------------------------------------
const std::string DBLoaderCommand::ARG_DISABLE_WAL = "disable_wal";
const std::string DBLoaderCommand::ARG_BULK_LOAD = "bulk_load";
const std::string DBLoaderCommand::ARG_COMPACT = "compact";

DBLoaderCommand::DBLoaderCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(
          options, flags, false /* is_read_only */,
          BuildCmdLineOptions({ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX, ARG_FROM,
                               ARG_TO, ARG_CREATE_IF_MISSING, ARG_DISABLE_WAL,
                               ARG_BULK_LOAD, ARG_COMPACT})),
      disable_wal_(false),
      bulk_load_(false),
      compact_(false) {
  create_if_missing_ = IsFlagPresent(flags, ARG_CREATE_IF_MISSING);
  disable_wal_ = IsFlagPresent(flags, ARG_DISABLE_WAL);
  bulk_load_ = IsFlagPresent(flags, ARG_BULK_LOAD);
  compact_ = IsFlagPresent(flags, ARG_COMPACT);
}

void DBLoaderCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(DBLoaderCommand::Name());
  ret.append(" [--" + ARG_CREATE_IF_MISSING + "]");
  ret.append(" [--" + ARG_DISABLE_WAL + "]");
  ret.append(" [--" + ARG_BULK_LOAD + "]");
  ret.append(" [--" + ARG_COMPACT + "]");
  ret.append("\n");
}

void DBLoaderCommand::OverrideBaseOptions() {
  LDBCommand::OverrideBaseOptions();
  options_.create_if_missing = create_if_missing_;
  if (bulk_load_) {
    options_.PrepareForBulkLoad();
  }
}

void DBLoaderCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }

  WriteOptions write_options;
  if (disable_wal_) {
    write_options.disableWAL = true;
  }

  int bad_lines = 0;
  std::string line;
  // prefer ifstream getline performance vs that from std::cin istream
  std::ifstream ifs_stdin("/dev/stdin");
  std::istream* istream_p = ifs_stdin.is_open() ? &ifs_stdin : &std::cin;
  Status s;
  while (s.ok() && getline(*istream_p, line, '\n')) {
    std::string key;
    std::string value;
    if (ParseKeyValue(line, &key, &value, is_key_hex_, is_value_hex_)) {
      s = db_->Put(write_options, GetCfHandle(), Slice(key), Slice(value));
    } else if (0 == line.find("Keys in range:")) {
      // ignore this line
    } else if (0 == line.find("Created bg thread 0x")) {
      // ignore this line
    } else {
      bad_lines++;
    }
  }

  if (bad_lines > 0) {
    std::cout << "Warning: " << bad_lines << " bad lines ignored." << std::endl;
  }
  if (!s.ok()) {
    std::stringstream oss;
    oss << "Load failed: " << s.ToString();
    exec_state_ = LDBCommandExecuteResult::Failed(oss.str());
  }
  if (compact_ && s.ok()) {
    s = db_->CompactRange(CompactRangeOptions(), GetCfHandle(), nullptr,
                          nullptr);
  }
  if (!s.ok()) {
    std::stringstream oss;
    oss << "Compaction failed: " << s.ToString();
    exec_state_ = LDBCommandExecuteResult::Failed(oss.str());
  }
}

// ----------------------------------------------------------------------------

namespace {

void DumpManifestFile(Options options, std::string file, bool verbose, bool hex,
                      bool json,
                      const std::vector<ColumnFamilyDescriptor>& cf_descs) {
  EnvOptions sopt;
  std::string dbname("dummy");
  std::shared_ptr<Cache> tc(NewLRUCache(options.max_open_files - 10,
                                        options.table_cache_numshardbits));
  // Notice we are using the default options not through SanitizeOptions(),
  // if VersionSet::DumpManifest() depends on any option done by
  // SanitizeOptions(), we need to initialize it manually.
  options.db_paths.emplace_back("dummy", 0);
  options.num_levels = 64;
  WriteController wc(options.delayed_write_rate);
  WriteBufferManager wb(options.db_write_buffer_size);
  ImmutableDBOptions immutable_db_options(options);
  VersionSet versions(dbname, &immutable_db_options, MutableDBOptions{}, sopt,
                      tc.get(), &wb, &wc,
                      /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
                      /*db_id=*/"", /*db_session_id=*/"",
                      options.daily_offpeak_time_utc,
                      /*error_handler=*/nullptr, /*unchanging=*/true);
  Status s = versions.DumpManifest(options, file, verbose, hex, json, cf_descs);
  if (!s.ok()) {
    fprintf(stderr, "Error in processing file %s %s\n", file.c_str(),
            s.ToString().c_str());
  }
}

void DumpCompactionProgressFile(const std::string& file_path) {
  Status s;
  std::unique_ptr<SequentialFileReader> file_reader;

  std::unique_ptr<FSSequentialFile> file;
  const std::shared_ptr<FileSystem>& fs = Env::Default()->GetFileSystem();
  s = fs->NewSequentialFile(file_path, FileOptions(), &file, nullptr);
  if (!s.ok()) {
    fprintf(stderr, "Failed to open compaction progress file %s: %s\n",
            file_path.c_str(), s.ToString().c_str());
    return;
  }

  file_reader = std::make_unique<SequentialFileReader>(std::move(file),
                                                       file_path, 0, nullptr);

  log::Reader reader(nullptr, std::move(file_reader), nullptr,
                     true /* checksum */, 0);

  Slice record;
  std::string scratch;
  int count = 0;

  fprintf(stdout, "Compaction Progress File: %s\n", file_path.c_str());
  fprintf(stdout, "============================================\n");

  while (reader.ReadRecord(&record, &scratch)) {
    VersionEdit edit;
    s = edit.DecodeFrom(record);
    if (!s.ok()) {
      fprintf(stderr, "Failed to decode VersionEdit: %s\n",
              s.ToString().c_str());
      continue;
    }

    if (edit.HasSubcompactionProgress()) {
      fprintf(stdout, "Progress Record %d:\n", count);
      const auto& progress = edit.GetSubcompactionProgress();
      fprintf(stdout, "%s\n", progress.ToString().c_str());
      ++count;
    }
  }

  if (count == 0) {
    fprintf(stdout,
            "No valid records found in the compaction progress file.\n");
  } else {
    fprintf(stdout, "\nTotal records: %d\n", count);
  }
}

}  // namespace

const std::string ManifestDumpCommand::ARG_VERBOSE = "verbose";
const std::string ManifestDumpCommand::ARG_JSON = "json";
const std::string ManifestDumpCommand::ARG_PATH = "path";

void ManifestDumpCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(ManifestDumpCommand::Name());
  ret.append(" [--" + ARG_VERBOSE + "]");
  ret.append(" [--" + ARG_JSON + "]");
  ret.append(" [--" + ARG_PATH + "=<path_to_manifest_file>]");
  ret.append("\n");
}

ManifestDumpCommand::ManifestDumpCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(
          options, flags, true /* is_read_only */,
          BuildCmdLineOptions({ARG_VERBOSE, ARG_PATH, ARG_HEX, ARG_JSON})),
      verbose_(false),
      json_(false) {
  verbose_ = IsFlagPresent(flags, ARG_VERBOSE);
  json_ = IsFlagPresent(flags, ARG_JSON);

  auto itr = options.find(ARG_PATH);
  if (itr != options.end()) {
    path_ = itr->second;
    if (path_.empty()) {
      exec_state_ = LDBCommandExecuteResult::Failed("--path: missing pathname");
    }
  }
}

void ManifestDumpCommand::DoCommand() {
  PrepareOptions();
  std::string manifestfile;

  if (!path_.empty()) {
    manifestfile = path_;
  } else {
    // We need to find the manifest file by searching the directory
    // containing the db for files of the form MANIFEST_[0-9]+

    std::vector<std::string> files;
    Status s = options_.env->GetChildren(db_path_, &files);
    if (!s.ok()) {
      std::string err_msg = s.ToString();
      err_msg.append(": Failed to list the content of ");
      err_msg.append(db_path_);
      exec_state_ = LDBCommandExecuteResult::Failed(err_msg);
      return;
    }
    const std::string kManifestNamePrefix = "MANIFEST-";
    std::string matched_file;
#ifdef OS_WIN
    const char kPathDelim = '\\';
#else
    const char kPathDelim = '/';
#endif
    for (const auto& file_path : files) {
      // Some Env::GetChildren() return absolute paths. Some directories' path
      // end with path delim, e.g. '/' or '\\'.
      size_t pos = file_path.find_last_of(kPathDelim);
      if (pos == file_path.size() - 1) {
        continue;
      }
      std::string fname;
      if (pos != std::string::npos) {
        // Absolute path.
        fname.assign(file_path, pos + 1, file_path.size() - pos - 1);
      } else {
        fname = file_path;
      }
      uint64_t file_num = 0;
      FileType file_type = kWalFile;  // Just for initialization
      if (ParseFileName(fname, &file_num, &file_type) &&
          file_type == kDescriptorFile) {
        if (!matched_file.empty()) {
          exec_state_ = LDBCommandExecuteResult::Failed(
              "Multiple MANIFEST files found; use --path to select one");
          return;
        } else {
          matched_file.swap(fname);
        }
      }
    }
    if (matched_file.empty()) {
      std::string err_msg("No MANIFEST found in ");
      err_msg.append(db_path_);
      exec_state_ = LDBCommandExecuteResult::Failed(err_msg);
      return;
    }
    if (db_path_.back() != '/') {
      db_path_.append("/");
    }
    manifestfile = db_path_ + matched_file;
  }

  if (verbose_) {
    fprintf(stdout, "Processing Manifest file %s\n", manifestfile.c_str());
  }

  DumpManifestFile(options_, manifestfile, verbose_, is_key_hex_, json_,
                   column_families_);

  if (verbose_) {
    fprintf(stdout, "Processing Manifest file %s done\n", manifestfile.c_str());
  }
}

// ----------------------------------------------------------------------------
namespace {

Status GetLiveFilesChecksumInfoFromVersionSet(Options options,
                                              const std::string& db_path,
                                              FileChecksumList* checksum_list) {
  EnvOptions sopt;
  Status s;
  std::string dbname(db_path);
  std::shared_ptr<Cache> tc(NewLRUCache(options.max_open_files - 10,
                                        options.table_cache_numshardbits));
  // Notice we are using the default options not through SanitizeOptions(),
  // if VersionSet::GetLiveFilesChecksumInfo depends on any option done by
  // SanitizeOptions(), we need to initialize it manually.
  options.db_paths.emplace_back(db_path, 0);
  options.num_levels = 64;
  WriteController wc(options.delayed_write_rate);
  WriteBufferManager wb(options.db_write_buffer_size);
  ImmutableDBOptions immutable_db_options(options);
  VersionSet versions(dbname, &immutable_db_options, MutableDBOptions{options},
                      sopt, tc.get(), &wb, &wc,
                      /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
                      /*db_id=*/"", /*db_session_id=*/"",
                      options.daily_offpeak_time_utc,
                      /*error_handler=*/nullptr, /*unchanging=*/true);
  std::vector<std::string> cf_name_list;
  s = versions.ListColumnFamilies(&cf_name_list, db_path,
                                  immutable_db_options.fs.get());
  if (s.ok()) {
    std::vector<ColumnFamilyDescriptor> cf_list;
    for (const auto& name : cf_name_list) {
      cf_list.emplace_back(name, ColumnFamilyOptions(options));
    }
    s = versions.Recover(cf_list, true);
  }
  if (s.ok()) {
    s = versions.GetLiveFilesChecksumInfo(checksum_list);
  }
  return s;
}

}  // namespace

const std::string FileChecksumDumpCommand::ARG_PATH = "path";

void FileChecksumDumpCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(FileChecksumDumpCommand::Name());
  ret.append(" [--" + ARG_PATH + "=<path_to_manifest_file>]");
  ret.append("\n");
}

FileChecksumDumpCommand::FileChecksumDumpCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, true /* is_read_only */,
                 BuildCmdLineOptions({ARG_PATH, ARG_HEX})) {
  auto itr = options.find(ARG_PATH);
  if (itr != options.end()) {
    path_ = itr->second;
    if (path_.empty()) {
      exec_state_ = LDBCommandExecuteResult::Failed("--path: missing pathname");
    }
  }
  is_checksum_hex_ = IsFlagPresent(flags, ARG_HEX);
}

void FileChecksumDumpCommand::DoCommand() {
  PrepareOptions();
  // print out the checksum information in the following format:
  //  sst file number, checksum function name, checksum value
  //  sst file number, checksum function name, checksum value
  //  ......

  std::unique_ptr<FileChecksumList> checksum_list(NewFileChecksumList());
  Status s = GetLiveFilesChecksumInfoFromVersionSet(options_, db_path_,
                                                    checksum_list.get());
  if (s.ok() && checksum_list != nullptr) {
    std::vector<uint64_t> file_numbers;
    std::vector<std::string> checksums;
    std::vector<std::string> checksum_func_names;
    s = checksum_list->GetAllFileChecksums(&file_numbers, &checksums,
                                           &checksum_func_names);
    if (s.ok()) {
      for (size_t i = 0; i < file_numbers.size(); i++) {
        assert(i < file_numbers.size());
        assert(i < checksums.size());
        assert(i < checksum_func_names.size());
        std::string checksum;
        if (is_checksum_hex_) {
          checksum = StringToHex(checksums[i]);
        } else {
          checksum = std::move(checksums[i]);
        }
        fprintf(stdout, "%" PRId64 ", %s, %s\n", file_numbers[i],
                checksum_func_names[i].c_str(), checksum.c_str());
      }
      fprintf(stdout, "Print SST file checksum information finished \n");
    }
  }

  if (!s.ok()) {
    exec_state_ = LDBCommandExecuteResult::Failed(s.ToString());
  }
}

// ----------------------------------------------------------------------------

void GetPropertyCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(GetPropertyCommand::Name());
  ret.append(" <property_name>");
  ret.append("\n");
}

GetPropertyCommand::GetPropertyCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, true /* is_read_only */,
                 BuildCmdLineOptions({})) {
  if (params.size() != 1) {
    exec_state_ =
        LDBCommandExecuteResult::Failed("property name must be specified");
  } else {
    property_ = params[0];
  }
}

void GetPropertyCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }

  std::map<std::string, std::string> value_map;
  std::string value;

  // Rather than having different ldb command for map properties vs. string
  // properties, we simply try Map property first. (This order only chosen
  // because I prefer the map-style output for
  // "rocksdb.aggregated-table-properties".)
  if (db_->GetMapProperty(GetCfHandle(), property_, &value_map)) {
    if (value_map.empty()) {
      fprintf(stdout, "%s: <empty map>\n", property_.c_str());
    } else {
      for (auto& e : value_map) {
        fprintf(stdout, "%s.%s: %s\n", property_.c_str(), e.first.c_str(),
                e.second.c_str());
      }
    }
  } else if (db_->GetProperty(GetCfHandle(), property_, &value)) {
    fprintf(stdout, "%s: %s\n", property_.c_str(), value.c_str());
  } else {
    exec_state_ =
        LDBCommandExecuteResult::Failed("failed to get property: " + property_);
  }
}

// ----------------------------------------------------------------------------

void ListColumnFamiliesCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(ListColumnFamiliesCommand::Name());
  ret.append("\n");
}

ListColumnFamiliesCommand::ListColumnFamiliesCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, true /* is_read_only */,
                 BuildCmdLineOptions({})) {}

void ListColumnFamiliesCommand::DoCommand() {
  PrepareOptions();
  std::vector<std::string> column_families;
  Status s = DB::ListColumnFamilies(options_, db_path_, &column_families);
  if (!s.ok()) {
    fprintf(stderr, "Error in processing db %s %s\n", db_path_.c_str(),
            s.ToString().c_str());
  } else {
    fprintf(stdout, "Column families in %s: \n{", db_path_.c_str());
    bool first = true;
    for (const auto& cf : column_families) {
      if (!first) {
        fprintf(stdout, ", ");
      }
      first = false;
      fprintf(stdout, "%s", cf.c_str());
    }
    fprintf(stdout, "}\n");
  }
}

void CreateColumnFamilyCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(CreateColumnFamilyCommand::Name());
  ret.append(" --db=<db_path> <new_column_family_name>");
  ret.append("\n");
}

CreateColumnFamilyCommand::CreateColumnFamilyCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, false /* is_read_only */, {ARG_DB}) {
  if (params.size() != 1) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "new column family name must be specified");
  } else {
    new_cf_name_ = params[0];
  }
}

void CreateColumnFamilyCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  ColumnFamilyHandle* new_cf_handle = nullptr;
  Status st = db_->CreateColumnFamily(options_, new_cf_name_, &new_cf_handle);
  if (st.ok()) {
    fprintf(stdout, "OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "Fail to create new column family: " + st.ToString());
  }
  delete new_cf_handle;
  CloseDB();
}

void DropColumnFamilyCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(DropColumnFamilyCommand::Name());
  ret.append(" --db=<db_path> <column_family_name_to_drop>");
  ret.append("\n");
}

DropColumnFamilyCommand::DropColumnFamilyCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, false /* is_read_only */, {ARG_DB}) {
  if (params.size() != 1) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "The name of column family to drop must be specified");
  } else {
    cf_name_to_drop_ = params[0];
  }
}

void DropColumnFamilyCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  auto iter = cf_handles_.find(cf_name_to_drop_);
  if (iter == cf_handles_.end()) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "Column family: " + cf_name_to_drop_ + " doesn't exist in db.");
    return;
  }
  ColumnFamilyHandle* cf_handle_to_drop = iter->second;
  Status st = db_->DropColumnFamily(cf_handle_to_drop);
  if (st.ok()) {
    fprintf(stdout, "OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "Fail to drop column family: " + st.ToString());
  }
  CloseDB();
}

// ----------------------------------------------------------------------------
namespace {

// This function only called when it's the sane case of >1 buckets in time-range
// Also called only when timekv falls between ttl_start and ttl_end provided
void IncBucketCounts(std::vector<uint64_t>& bucket_counts, int ttl_start,
                     int time_range, int bucket_size, int timekv,
                     int num_buckets) {
#ifdef NDEBUG
  (void)time_range;
  (void)num_buckets;
#endif
  assert(time_range > 0 && timekv >= ttl_start && bucket_size > 0 &&
         timekv < (ttl_start + time_range) && num_buckets > 1);
  int bucket = (timekv - ttl_start) / bucket_size;
  bucket_counts[bucket]++;
}

void PrintBucketCounts(const std::vector<uint64_t>& bucket_counts,
                       int ttl_start, int ttl_end, int bucket_size,
                       int num_buckets) {
  int time_point = ttl_start;
  for (int i = 0; i < num_buckets - 1; i++, time_point += bucket_size) {
    fprintf(stdout, "Keys in range %s to %s : %lu\n",
            TimeToHumanString(time_point).c_str(),
            TimeToHumanString(time_point + bucket_size).c_str(),
            (unsigned long)bucket_counts[i]);
  }
  fprintf(stdout, "Keys in range %s to %s : %lu\n",
          TimeToHumanString(time_point).c_str(),
          TimeToHumanString(ttl_end).c_str(),
          (unsigned long)bucket_counts[num_buckets - 1]);
}

}  // namespace

const std::string InternalDumpCommand::ARG_COUNT_ONLY = "count_only";
const std::string InternalDumpCommand::ARG_COUNT_DELIM = "count_delim";
const std::string InternalDumpCommand::ARG_STATS = "stats";
const std::string InternalDumpCommand::ARG_INPUT_KEY_HEX = "input_key_hex";

InternalDumpCommand::InternalDumpCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, true /* is_read_only */,
                 BuildCmdLineOptions(
                     {ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX, ARG_FROM, ARG_TO,
                      ARG_MAX_KEYS, ARG_COUNT_ONLY, ARG_COUNT_DELIM, ARG_STATS,
                      ARG_INPUT_KEY_HEX, ARG_DECODE_BLOB_INDEX})),
      has_from_(false),
      has_to_(false),
      max_keys_(-1),
      delim_("."),
      count_only_(false),
      count_delim_(false),
      print_stats_(false),
      is_input_key_hex_(false),
      decode_blob_index_(false) {
  has_from_ = ParseStringOption(options, ARG_FROM, &from_);
  has_to_ = ParseStringOption(options, ARG_TO, &to_);

  ParseIntOption(options, ARG_MAX_KEYS, max_keys_, exec_state_);
  auto itr = options.find(ARG_COUNT_DELIM);
  if (itr != options.end()) {
    delim_ = itr->second;
    count_delim_ = true;
    // fprintf(stdout,"delim = %c\n",delim_[0]);
  } else {
    count_delim_ = IsFlagPresent(flags, ARG_COUNT_DELIM);
    delim_ = ".";
  }

  print_stats_ = IsFlagPresent(flags, ARG_STATS);
  count_only_ = IsFlagPresent(flags, ARG_COUNT_ONLY);
  is_input_key_hex_ = IsFlagPresent(flags, ARG_INPUT_KEY_HEX);
  decode_blob_index_ = IsFlagPresent(flags, ARG_DECODE_BLOB_INDEX);

  if (is_input_key_hex_) {
    if (has_from_) {
      from_ = HexToString(from_);
    }
    if (has_to_) {
      to_ = HexToString(to_);
    }
  }
}

void InternalDumpCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(InternalDumpCommand::Name());
  ret.append(HelpRangeCmdArgs());
  ret.append(" [--" + ARG_INPUT_KEY_HEX + "]");
  ret.append(" [--" + ARG_MAX_KEYS + "=<N>]");
  ret.append(" [--" + ARG_COUNT_ONLY + "]");
  ret.append(" [--" + ARG_COUNT_DELIM + "=<char>]");
  ret.append(" [--" + ARG_STATS + "]");
  ret.append(" [--" + ARG_DECODE_BLOB_INDEX + "]");
  ret.append("\n");
}

void InternalDumpCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  ColumnFamilyHandle* cfh = GetCfHandle();
  const Comparator* ucmp = cfh->GetComparator();
  size_t ts_sz = ucmp->timestamp_size();
  if (print_stats_) {
    std::string stats;
    if (db_->GetProperty(cfh, "rocksdb.stats", &stats)) {
      fprintf(stdout, "%s\n", stats.c_str());
    }
  }

  // Cast as DBImpl to get internal iterator
  std::vector<KeyVersion> key_versions;
  Status st = GetAllKeyVersions(
      db_.get(), GetCfHandle(), has_from_ ? from_ : OptSlice{},
      has_to_ ? to_ : OptSlice{}, max_keys_, &key_versions);
  if (!st.ok()) {
    exec_state_ = LDBCommandExecuteResult::Failed(st.ToString());
    return;
  }
  std::string rtype1;
  std::string rtype2;
  std::string row;
  std::string val;
  rtype2 = "";
  uint64_t c = 0;
  uint64_t s1 = 0;
  uint64_t s2 = 0;

  long long count = 0;
  for (auto& key_version : key_versions) {
    ValueType value_type = static_cast<ValueType>(key_version.type);
    InternalKey ikey(key_version.user_key, key_version.sequence, value_type);
    Slice user_key_without_ts = ikey.user_key();
    if (ts_sz > 0) {
      user_key_without_ts.remove_suffix(ts_sz);
    }
    if (has_to_ && ucmp->Compare(user_key_without_ts, to_) == 0) {
      // GetAllKeyVersions() includes keys with user key `to_`, but idump has
      // traditionally excluded such keys.
      break;
    }
    ++count;
    int k;
    if (count_delim_) {
      rtype1 = "";
      s1 = 0;
      row = ikey.Encode().ToString();
      val = key_version.value;
      for (k = 0; row[k] != '\x01' && row[k] != '\0'; k++) {
        s1++;
      }
      for (k = 0; val[k] != '\x01' && val[k] != '\0'; k++) {
        s1++;
      }
      for (int j = 0; row[j] != delim_[0] && row[j] != '\0' && row[j] != '\x01';
           j++) {
        rtype1 += row[j];
      }
      if (rtype2.compare("") && rtype2.compare(rtype1) != 0) {
        fprintf(stdout, "%s => count:%" PRIu64 "\tsize:%" PRIu64 "\n",
                rtype2.c_str(), c, s2);
        c = 1;
        s2 = s1;
        rtype2 = rtype1;
      } else {
        c++;
        s2 += s1;
        rtype2 = rtype1;
      }
    }

    if (!count_only_ && !count_delim_) {
      std::string key = ikey.DebugString(is_key_hex_, ucmp);
      Slice value(key_version.value);
      if (!decode_blob_index_ || value_type != kTypeBlobIndex) {
        if (value_type == kTypeWideColumnEntity) {
          std::ostringstream oss;
          const Status s = WideColumnsHelper::DumpSliceAsWideColumns(
              value, oss, is_value_hex_);
          if (!s.ok()) {
            fprintf(stderr, "%s => error deserializing wide columns\n",
                    key.c_str());
          } else {
            fprintf(stdout, "%s => %s\n", key.c_str(), oss.str().c_str());
          }
        } else {
          fprintf(stdout, "%s => %s\n", key.c_str(),
                  value.ToString(is_value_hex_).c_str());
        }
      } else {
        BlobIndex blob_index;

        const Status s = blob_index.DecodeFrom(value);
        if (!s.ok()) {
          fprintf(stderr, "%s => error decoding blob index =>\n", key.c_str());
        } else {
          fprintf(stdout, "%s => %s\n", key.c_str(),
                  blob_index.DebugString(is_value_hex_).c_str());
        }
      }
    }

    // Terminate if maximum number of keys have been dumped
    if (max_keys_ > 0 && count >= max_keys_) {
      break;
    }
  }
  if (count_delim_) {
    fprintf(stdout, "%s => count:%" PRIu64 "\tsize:%" PRIu64 "\n",
            rtype2.c_str(), c, s2);
  } else {
    fprintf(stdout, "Internal keys in range: %lld\n", count);
  }
}

const std::string DBDumperCommand::ARG_COUNT_ONLY = "count_only";
const std::string DBDumperCommand::ARG_COUNT_DELIM = "count_delim";
const std::string DBDumperCommand::ARG_STATS = "stats";
const std::string DBDumperCommand::ARG_TTL_BUCKET = "bucket";

DBDumperCommand::DBDumperCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(
          options, flags, true /* is_read_only */,
          BuildCmdLineOptions(
              {ARG_TTL, ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX, ARG_FROM, ARG_TO,
               ARG_MAX_KEYS, ARG_COUNT_ONLY, ARG_COUNT_DELIM, ARG_STATS,
               ARG_TTL_START, ARG_TTL_END, ARG_TTL_BUCKET, ARG_TIMESTAMP,
               ARG_PATH, ARG_DECODE_BLOB_INDEX, ARG_DUMP_UNCOMPRESSED_BLOBS})),
      null_from_(true),
      null_to_(true),
      max_keys_(-1),
      count_only_(false),
      count_delim_(false),
      print_stats_(false),
      decode_blob_index_(false),
      dump_uncompressed_blobs_(false) {
  auto itr = options.find(ARG_FROM);
  if (itr != options.end()) {
    null_from_ = false;
    from_ = itr->second;
  }

  itr = options.find(ARG_TO);
  if (itr != options.end()) {
    null_to_ = false;
    to_ = itr->second;
  }

  itr = options.find(ARG_MAX_KEYS);
  if (itr != options.end()) {
    try {
#if defined(CYGWIN)
      max_keys_ = strtol(itr->second.c_str(), 0, 10);
#else
      max_keys_ = std::stoi(itr->second);
#endif
    } catch (const std::invalid_argument&) {
      exec_state_ = LDBCommandExecuteResult::Failed(ARG_MAX_KEYS +
                                                    " has an invalid value");
    } catch (const std::out_of_range&) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          ARG_MAX_KEYS + " has a value out-of-range");
    }
  }
  itr = options.find(ARG_COUNT_DELIM);
  if (itr != options.end()) {
    delim_ = itr->second;
    count_delim_ = true;
  } else {
    count_delim_ = IsFlagPresent(flags, ARG_COUNT_DELIM);
    delim_ = ".";
  }

  print_stats_ = IsFlagPresent(flags, ARG_STATS);
  count_only_ = IsFlagPresent(flags, ARG_COUNT_ONLY);
  decode_blob_index_ = IsFlagPresent(flags, ARG_DECODE_BLOB_INDEX);
  dump_uncompressed_blobs_ = IsFlagPresent(flags, ARG_DUMP_UNCOMPRESSED_BLOBS);

  if (is_key_hex_) {
    if (!null_from_) {
      from_ = HexToString(from_);
    }
    if (!null_to_) {
      to_ = HexToString(to_);
    }
  }

  itr = options.find(ARG_PATH);
  if (itr != options.end()) {
    path_ = itr->second;
    if (db_path_.empty()) {
      db_path_ = path_;
    }
  }
}

void DBDumperCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(DBDumperCommand::Name());
  ret.append(HelpRangeCmdArgs());
  ret.append(" [--" + ARG_TTL + "]");
  ret.append(" [--" + ARG_MAX_KEYS + "=<N>]");
  ret.append(" [--" + ARG_TIMESTAMP + "]");
  ret.append(" [--" + ARG_COUNT_ONLY + "]");
  ret.append(" [--" + ARG_COUNT_DELIM + "=<char>]");
  ret.append(" [--" + ARG_STATS + "]");
  ret.append(" [--" + ARG_TTL_BUCKET + "=<N>]");
  ret.append(" [--" + ARG_TTL_START + "=<N>:- is inclusive]");
  ret.append(" [--" + ARG_TTL_END + "=<N>:- is exclusive]");
  ret.append(" [--" + ARG_PATH + "=<path_to_a_file>]");
  ret.append(" [--" + ARG_DECODE_BLOB_INDEX + "]");
  ret.append(" [--" + ARG_DUMP_UNCOMPRESSED_BLOBS + "]");
  ret.append("\n");
}

/**
 * Handles two separate cases:
 *
 * 1) --db is specified - just dump the database.
 *
 * 2) --path is specified - determine based on file extension what dumping
 *    function to call. Please note that we intentionally use the extension
 *    and avoid probing the file contents under the assumption that renaming
 *    the files is not a supported scenario.
 *
 */
void DBDumperCommand::DoCommand() {
  if (!db_) {
    assert(!path_.empty());
    std::string fileName = GetFileNameFromPath(path_);
    uint64_t number;
    FileType type;

    exec_state_ = LDBCommandExecuteResult::Succeed("");

    if (!ParseFileName(fileName, &number, &type)) {
      exec_state_ =
          LDBCommandExecuteResult::Failed("Can't parse file type: " + path_);
      return;
    }

    switch (type) {
      case kWalFile:
        // TODO(myabandeh): allow configuring is_write_commited
        DumpWalFiles(options_, path_, /* print_header_ */ true,
                     /* print_values_ */ true,
                     /* only_print_seqno_gaps */ false,
                     true /* is_write_commited */, ucmps_, &exec_state_);
        break;
      case kTableFile:
        DumpSstFile(options_, path_, is_key_hex_, /* show_properties */ true,
                    decode_blob_index_, from_, to_);
        break;
      case kDescriptorFile:
        DumpManifestFile(options_, path_, /* verbose_ */ false, is_key_hex_,
                         /*  json_ */ false, column_families_);
        break;
      case kBlobFile:
        DumpBlobFile(path_, is_key_hex_, is_value_hex_,
                     dump_uncompressed_blobs_);
        break;
      default:
        exec_state_ = LDBCommandExecuteResult::Failed(
            "File type not supported: " + path_);
        break;
    }

  } else {
    DoDumpCommand();
  }
}

void DBDumperCommand::DoDumpCommand() {
  assert(nullptr != db_);
  assert(path_.empty());

  // Parse command line args
  uint64_t count = 0;
  if (print_stats_) {
    std::string stats;
    if (db_->GetProperty("rocksdb.stats", &stats)) {
      fprintf(stdout, "%s\n", stats.c_str());
    }
  }

  // Setup key iterator
  ReadOptions scan_read_opts;
  Slice read_timestamp;
  ColumnFamilyHandle* cfh = GetCfHandle();
  const Comparator* ucmp = cfh->GetComparator();
  size_t ts_sz = ucmp->timestamp_size();
  if (ucmp->timestamp_size() > 0) {
    read_timestamp = ucmp->GetMaxTimestamp();
    scan_read_opts.timestamp = &read_timestamp;
  }
  scan_read_opts.total_order_seek = true;
  Iterator* iter = db_->NewIterator(scan_read_opts, cfh);
  Status st = iter->status();
  if (!st.ok()) {
    exec_state_ =
        LDBCommandExecuteResult::Failed("Iterator error." + st.ToString());
  }

  if (!null_from_) {
    iter->Seek(from_);
  } else {
    iter->SeekToFirst();
  }

  int max_keys = max_keys_;
  int ttl_start;
  if (!ParseIntOption(option_map_, ARG_TTL_START, ttl_start, exec_state_)) {
    ttl_start = DBWithTTLImpl::kMinTimestamp;  // TTL introduction time
  }
  int ttl_end;
  if (!ParseIntOption(option_map_, ARG_TTL_END, ttl_end, exec_state_)) {
    ttl_end = DBWithTTLImpl::kMaxTimestamp;  // Max time allowed by TTL feature
  }
  if (ttl_end < ttl_start) {
    fprintf(stderr, "Error: End time can't be less than start time\n");
    delete iter;
    return;
  }
  int time_range = ttl_end - ttl_start;
  int bucket_size;
  if (!ParseIntOption(option_map_, ARG_TTL_BUCKET, bucket_size, exec_state_) ||
      bucket_size <= 0) {
    bucket_size = time_range;  // Will have just 1 bucket by default
  }
  // cretaing variables for row count of each type
  std::string rtype1;
  std::string rtype2;
  std::string row;
  std::string val;
  rtype2 = "";
  uint64_t c = 0;
  uint64_t s1 = 0;
  uint64_t s2 = 0;

  // At this point, bucket_size=0 => time_range=0
  int num_buckets = (bucket_size >= time_range)
                        ? 1
                        : ((time_range + bucket_size - 1) / bucket_size);
  std::vector<uint64_t> bucket_counts(num_buckets, 0);
  if (is_db_ttl_ && !count_only_ && timestamp_ && !count_delim_) {
    fprintf(stdout, "Dumping key-values from %s to %s\n",
            TimeToHumanString(ttl_start).c_str(),
            TimeToHumanString(ttl_end).c_str());
  }

  HistogramImpl vsize_hist;

  for (; iter->Valid(); iter->Next()) {
    int rawtime = 0;
    // If end marker was specified, we stop before it
    if (!null_to_ && ucmp->Compare(iter->key(), to_) >= 0) {
      break;
    }
    // Terminate if maximum number of keys have been dumped
    if (max_keys == 0) {
      break;
    }
    if (is_db_ttl_) {
      TtlIterator* it_ttl = static_cast_with_check<TtlIterator>(iter);
      rawtime = it_ttl->ttl_timestamp();
      if (rawtime < ttl_start || rawtime >= ttl_end) {
        continue;
      }
    }
    if (max_keys > 0) {
      --max_keys;
    }
    if (is_db_ttl_ && num_buckets > 1) {
      IncBucketCounts(bucket_counts, ttl_start, time_range, bucket_size,
                      rawtime, num_buckets);
    }
    ++count;
    if (count_delim_) {
      rtype1 = "";
      row = iter->key().ToString();
      val = iter->value().ToString();
      s1 = row.size() + val.size();
      for (int j = 0; row[j] != delim_[0] && row[j] != '\0'; j++) {
        rtype1 += row[j];
      }
      if (rtype2.compare("") && rtype2.compare(rtype1) != 0) {
        fprintf(stdout, "%s => count:%" PRIu64 "\tsize:%" PRIu64 "\n",
                rtype2.c_str(), c, s2);
        c = 1;
        s2 = s1;
        rtype2 = rtype1;
      } else {
        c++;
        s2 += s1;
        rtype2 = rtype1;
      }
    }

    if (count_only_) {
      vsize_hist.Add(iter->value().size());
    }

    if (!count_only_ && !count_delim_) {
      if (is_db_ttl_ && timestamp_) {
        fprintf(stdout, "%s ", TimeToHumanString(rawtime).c_str());
      }
      // (TODO) TTL Iterator does not support wide columns yet.
      std::string str =
          is_db_ttl_
              ? PrintKeyValue(iter->key().ToString(),
                              ts_sz == 0 ? "" : iter->timestamp().ToString(),
                              iter->value().ToString(), is_key_hex_,
                              is_value_hex_, ucmp)
              : PrintKeyValueOrWideColumns(
                    iter->key(), ts_sz == 0 ? "" : iter->timestamp().ToString(),
                    iter->value(), iter->columns(), is_key_hex_, is_value_hex_,
                    ucmp);
      fprintf(stdout, "%s\n", str.c_str());
    }
  }

  // Check for iterator errors that may have occurred during iteration
  st = iter->status();
  if (!st.ok()) {
    exec_state_ =
        LDBCommandExecuteResult::Failed("Iterator error: " + st.ToString());
  }

  if (num_buckets > 1 && is_db_ttl_) {
    PrintBucketCounts(bucket_counts, ttl_start, ttl_end, bucket_size,
                      num_buckets);
  } else if (count_delim_) {
    fprintf(stdout, "%s => count:%" PRIu64 "\tsize:%" PRIu64 "\n",
            rtype2.c_str(), c, s2);
  } else {
    fprintf(stdout, "Keys in range: %" PRIu64 "\n", count);
  }

  if (count_only_) {
    fprintf(stdout, "Value size distribution: \n");
    fprintf(stdout, "%s\n", vsize_hist.ToString().c_str());
  }
  // Clean up
  delete iter;
}

const std::string ReduceDBLevelsCommand::ARG_NEW_LEVELS = "new_levels";
const std::string ReduceDBLevelsCommand::ARG_PRINT_OLD_LEVELS =
    "print_old_levels";

ReduceDBLevelsCommand::ReduceDBLevelsCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, false /* is_read_only */,
                 BuildCmdLineOptions({ARG_NEW_LEVELS, ARG_PRINT_OLD_LEVELS})),
      old_levels_(1 << 7),
      new_levels_(-1),
      print_old_levels_(false) {
  ParseIntOption(option_map_, ARG_NEW_LEVELS, new_levels_, exec_state_);
  print_old_levels_ = IsFlagPresent(flags, ARG_PRINT_OLD_LEVELS);

  if (new_levels_ <= 0) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        " Use --" + ARG_NEW_LEVELS + " to specify a new level number\n");
  }
}

std::vector<std::string> ReduceDBLevelsCommand::PrepareArgs(
    const std::string& db_path, int new_levels, bool print_old_level) {
  std::vector<std::string> ret;
  ret.emplace_back("reduce_levels");
  ret.push_back("--" + ARG_DB + "=" + db_path);
  ret.push_back("--" + ARG_NEW_LEVELS + "=" + std::to_string(new_levels));
  if (print_old_level) {
    ret.push_back("--" + ARG_PRINT_OLD_LEVELS);
  }
  return ret;
}

void ReduceDBLevelsCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(ReduceDBLevelsCommand::Name());
  ret.append(" --" + ARG_NEW_LEVELS + "=<New number of levels>");
  ret.append(" [--" + ARG_PRINT_OLD_LEVELS + "]");
  ret.append("\n");
}

void ReduceDBLevelsCommand::OverrideBaseCFOptions(
    ColumnFamilyOptions* cf_opts) {
  LDBCommand::OverrideBaseCFOptions(cf_opts);
  cf_opts->num_levels = old_levels_;
  cf_opts->max_bytes_for_level_multiplier_additional.resize(cf_opts->num_levels,
                                                            1);
  // Disable size compaction
  cf_opts->max_bytes_for_level_base = 1ULL << 50;
  cf_opts->max_bytes_for_level_multiplier = 1;
}

Status ReduceDBLevelsCommand::GetOldNumOfLevels(Options& opt, int* levels) {
  ImmutableDBOptions db_options(opt);
  EnvOptions soptions;
  std::shared_ptr<Cache> tc(
      NewLRUCache(opt.max_open_files - 10, opt.table_cache_numshardbits));
  const InternalKeyComparator cmp(opt.comparator);
  WriteController wc(opt.delayed_write_rate);
  WriteBufferManager wb(opt.db_write_buffer_size);
  VersionSet versions(db_path_, &db_options, MutableDBOptions{opt}, soptions,
                      tc.get(), &wb, &wc,
                      /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
                      /*db_id=*/"", /*db_session_id=*/"",
                      opt.daily_offpeak_time_utc,
                      /*error_handler=*/nullptr, /*unchanging=*/false);
  std::vector<ColumnFamilyDescriptor> dummy;
  ColumnFamilyDescriptor dummy_descriptor(kDefaultColumnFamilyName,
                                          ColumnFamilyOptions(opt));
  dummy.push_back(dummy_descriptor);
  // We rely the VersionSet::Recover to tell us the internal data structures
  // in the db. And the Recover() should never do any change
  // (like LogAndApply) to the manifest file.
  Status st = versions.Recover(dummy);
  if (!st.ok()) {
    return st;
  }
  int max = -1;
  auto default_cfd = versions.GetColumnFamilySet()->GetDefault();
  for (int i = 0; i < default_cfd->NumberLevels(); i++) {
    if (default_cfd->current()->storage_info()->NumLevelFiles(i)) {
      max = i;
    }
  }

  *levels = max + 1;
  return st;
}

void ReduceDBLevelsCommand::DoCommand() {
  if (new_levels_ <= 1) {
    exec_state_ =
        LDBCommandExecuteResult::Failed("Invalid number of levels.\n");
    return;
  }

  Status st;
  PrepareOptions();
  int old_level_num = -1;
  st = GetOldNumOfLevels(options_, &old_level_num);
  if (!st.ok()) {
    exec_state_ = LDBCommandExecuteResult::Failed(st.ToString());
    return;
  }

  if (print_old_levels_) {
    fprintf(stdout, "The old number of levels in use is %d\n", old_level_num);
  }

  if (old_level_num <= new_levels_) {
    return;
  }

  old_levels_ = old_level_num;

  OpenDB();
  if (exec_state_.IsFailed()) {
    return;
  }
  assert(db_ != nullptr);
  // Compact the whole DB to put all files to the highest level.
  fprintf(stdout, "Compacting the db...\n");
  st =
      db_->CompactRange(CompactRangeOptions(), GetCfHandle(), nullptr, nullptr);

  CloseDB();

  if (st.ok()) {
    EnvOptions soptions;
    st = VersionSet::ReduceNumberOfLevels(db_path_, &options_, soptions,
                                          new_levels_);
  }
  if (!st.ok()) {
    exec_state_ = LDBCommandExecuteResult::Failed(st.ToString());
    return;
  }
}

const std::string ChangeCompactionStyleCommand::ARG_OLD_COMPACTION_STYLE =
    "old_compaction_style";
const std::string ChangeCompactionStyleCommand::ARG_NEW_COMPACTION_STYLE =
    "new_compaction_style";

ChangeCompactionStyleCommand::ChangeCompactionStyleCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, false /* is_read_only */,
                 BuildCmdLineOptions(
                     {ARG_OLD_COMPACTION_STYLE, ARG_NEW_COMPACTION_STYLE})),
      old_compaction_style_(-1),
      new_compaction_style_(-1) {
  ParseIntOption(option_map_, ARG_OLD_COMPACTION_STYLE, old_compaction_style_,
                 exec_state_);
  if (old_compaction_style_ != kCompactionStyleLevel &&
      old_compaction_style_ != kCompactionStyleUniversal) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "Use --" + ARG_OLD_COMPACTION_STYLE + " to specify old compaction " +
        "style. Check ldb help for proper compaction style value.\n");
    return;
  }

  ParseIntOption(option_map_, ARG_NEW_COMPACTION_STYLE, new_compaction_style_,
                 exec_state_);
  if (new_compaction_style_ != kCompactionStyleLevel &&
      new_compaction_style_ != kCompactionStyleUniversal) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "Use --" + ARG_NEW_COMPACTION_STYLE + " to specify new compaction " +
        "style. Check ldb help for proper compaction style value.\n");
    return;
  }

  if (new_compaction_style_ == old_compaction_style_) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "Old compaction style is the same as new compaction style. "
        "Nothing to do.\n");
    return;
  }

  if (old_compaction_style_ == kCompactionStyleUniversal &&
      new_compaction_style_ == kCompactionStyleLevel) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "Convert from universal compaction to level compaction. "
        "Nothing to do.\n");
    return;
  }
}

void ChangeCompactionStyleCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(ChangeCompactionStyleCommand::Name());
  ret.append(" --" + ARG_OLD_COMPACTION_STYLE + "=<Old compaction style: 0 " +
             "for level compaction, 1 for universal compaction>");
  ret.append(" --" + ARG_NEW_COMPACTION_STYLE + "=<New compaction style: 0 " +
             "for level compaction, 1 for universal compaction>");
  ret.append("\n");
}

void ChangeCompactionStyleCommand::OverrideBaseCFOptions(
    ColumnFamilyOptions* cf_opts) {
  LDBCommand::OverrideBaseCFOptions(cf_opts);
  if (old_compaction_style_ == kCompactionStyleLevel &&
      new_compaction_style_ == kCompactionStyleUniversal) {
    // In order to convert from level compaction to universal compaction, we
    // need to compact all data into a single file and move it to level 0.
    cf_opts->disable_auto_compactions = true;
    cf_opts->target_file_size_base = INT_MAX;
    cf_opts->target_file_size_multiplier = 1;
    cf_opts->max_bytes_for_level_base = INT_MAX;
    cf_opts->max_bytes_for_level_multiplier = 1;
  }
}

void ChangeCompactionStyleCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  // print db stats before we have made any change
  std::string property;
  std::string files_per_level;
  for (int i = 0; i < db_->NumberLevels(GetCfHandle()); i++) {
    db_->GetProperty(GetCfHandle(),
                     "rocksdb.num-files-at-level" + std::to_string(i),
                     &property);

    // format print string
    char buf[100];
    snprintf(buf, sizeof(buf), "%s%s", (i ? "," : ""), property.c_str());
    files_per_level += buf;
  }
  fprintf(stdout, "files per level before compaction: %s\n",
          files_per_level.c_str());

  // manual compact into a single file and move the file to level 0
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 0;
  Status s =
      db_->CompactRange(compact_options, GetCfHandle(), nullptr, nullptr);
  if (!s.ok()) {
    std::stringstream oss;
    oss << "Compaction failed: " << s.ToString();
    exec_state_ = LDBCommandExecuteResult::Failed(oss.str());
    return;
  }

  // verify compaction result
  files_per_level = "";
  int num_files = 0;
  for (int i = 0; i < db_->NumberLevels(GetCfHandle()); i++) {
    db_->GetProperty(GetCfHandle(),
                     "rocksdb.num-files-at-level" + std::to_string(i),
                     &property);

    // format print string
    char buf[100];
    snprintf(buf, sizeof(buf), "%s%s", (i ? "," : ""), property.c_str());
    files_per_level += buf;

    num_files = atoi(property.c_str());

    // level 0 should have only 1 file
    if (i == 0 && num_files != 1) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          "Number of db files at "
          "level 0 after compaction is " +
          std::to_string(num_files) + ", not 1.\n");
      return;
    }
    // other levels should have no file
    if (i > 0 && num_files != 0) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          "Number of db files at "
          "level " +
          std::to_string(i) + " after compaction is " +
          std::to_string(num_files) + ", not 0.\n");
      return;
    }
  }

  fprintf(stdout, "files per level after compaction: %s\n",
          files_per_level.c_str());
}

// ----------------------------------------------------------------------------

namespace {

struct StdErrReporter : public log::Reader::Reporter {
  void Corruption(size_t /*bytes*/, const Status& s,
                  uint64_t /*log_number*/ = kMaxSequenceNumber) override {
    std::cerr << "Corruption detected in log file " << s.ToString() << "\n";
  }
};

class InMemoryHandler : public WriteBatch::Handler {
 public:
  InMemoryHandler(std::stringstream& row, bool print_values,
                  bool write_after_commit,
                  const std::map<uint32_t, const Comparator*>& ucmps)
      : Handler(),
        row_(row),
        print_values_(print_values),
        write_after_commit_(write_after_commit),
        ucmps_(ucmps) {}

  void commonPutMerge(uint32_t cf, const Slice& key, const Slice& value) {
    std::string k = PrintKey(cf, key);
    if (print_values_) {
      std::string v = LDBCommand::StringToHex(value.ToString());
      row_ << k << " : ";
      row_ << v << " ";
    } else {
      row_ << k << " ";
    }
  }

  Status PutCF(uint32_t cf, const Slice& key, const Slice& value) override {
    row_ << "PUT(" << cf << ") : ";
    commonPutMerge(cf, key, value);
    return Status::OK();
  }

  Status PutEntityCF(uint32_t cf, const Slice& key,
                     const Slice& value) override {
    row_ << "PUT_ENTITY(" << cf << ") : " << PrintKey(cf, key);
    if (print_values_) {
      row_ << " : ";
      const Status s =
          WideColumnsHelper::DumpSliceAsWideColumns(value, row_, true);
      if (!s.ok()) {
        return s;
      }
    }

    row_ << ' ';
    return Status::OK();
  }

  Status MergeCF(uint32_t cf, const Slice& key, const Slice& value) override {
    row_ << "MERGE(" << cf << ") : ";
    commonPutMerge(cf, key, value);
    return Status::OK();
  }

  Status MarkNoop(bool) override {
    row_ << "NOOP ";
    return Status::OK();
  }

  Status DeleteCF(uint32_t cf, const Slice& key) override {
    row_ << "DELETE(" << cf << ") : ";
    row_ << PrintKey(cf, key) << " ";
    return Status::OK();
  }

  Status SingleDeleteCF(uint32_t cf, const Slice& key) override {
    row_ << "SINGLE_DELETE(" << cf << ") : ";
    row_ << PrintKey(cf, key) << " ";
    return Status::OK();
  }

  Status DeleteRangeCF(uint32_t cf, const Slice& begin_key,
                       const Slice& end_key) override {
    row_ << "DELETE_RANGE(" << cf << ") : ";
    row_ << PrintKey(cf, begin_key) << " ";
    row_ << PrintKey(cf, end_key) << " ";
    return Status::OK();
  }

  Status MarkBeginPrepare(bool unprepare) override {
    row_ << "BEGIN_PREPARE(";
    row_ << (unprepare ? "true" : "false") << ") ";
    return Status::OK();
  }

  Status MarkEndPrepare(const Slice& xid) override {
    row_ << "END_PREPARE(";
    row_ << LDBCommand::StringToHex(xid.ToString()) << ") ";
    return Status::OK();
  }

  Status MarkRollback(const Slice& xid) override {
    row_ << "ROLLBACK(";
    row_ << LDBCommand::StringToHex(xid.ToString()) << ") ";
    return Status::OK();
  }

  Status MarkCommit(const Slice& xid) override {
    row_ << "COMMIT(";
    row_ << LDBCommand::StringToHex(xid.ToString()) << ") ";
    return Status::OK();
  }

  Status MarkCommitWithTimestamp(const Slice& xid,
                                 const Slice& commit_ts) override {
    row_ << "COMMIT_WITH_TIMESTAMP(";
    row_ << LDBCommand::StringToHex(xid.ToString()) << ", ";
    row_ << LDBCommand::StringToHex(commit_ts.ToString()) << ") ";
    return Status::OK();
  }

  ~InMemoryHandler() override = default;

 protected:
  Handler::OptionState WriteAfterCommit() const override {
    return write_after_commit_ ? Handler::OptionState::kEnabled
                               : Handler::OptionState::kDisabled;
  }

 private:
  std::string PrintKey(uint32_t cf, const Slice& key) {
    auto ucmp_iter = ucmps_.find(cf);
    if (ucmp_iter == ucmps_.end()) {
      // Fallback to default print slice as hex
      return LDBCommand::StringToHex(key.ToString());
    }
    size_t ts_sz = ucmp_iter->second->timestamp_size();
    if (ts_sz == 0) {
      return LDBCommand::StringToHex(key.ToString());
    } else {
      // This could happen if there is corruption or undetected comparator
      // change.
      if (key.size() < ts_sz) {
        return "CORRUPT KEY";
      }
      Slice user_key_without_ts = key;
      user_key_without_ts.remove_suffix(ts_sz);
      Slice ts = Slice(key.data() + key.size() - ts_sz, ts_sz);
      return LDBCommand::StringToHex(user_key_without_ts.ToString()) +
             "|timestamp:" + ucmp_iter->second->TimestampToString(ts);
    }
  }
  std::stringstream& row_;
  bool print_values_;
  bool write_after_commit_;
  const std::map<uint32_t, const Comparator*> ucmps_;
};

void DumpWalFiles(Options options, const std::string& dir_or_file,
                  bool print_header, bool print_values,
                  bool only_print_seqno_gaps, bool is_write_committed,
                  const std::map<uint32_t, const Comparator*>& ucmps,
                  LDBCommandExecuteResult* exec_state) {
  std::vector<std::string> filenames;
  ROCKSDB_NAMESPACE::Env* env = options.env;
  ROCKSDB_NAMESPACE::Status st = env->GetChildren(dir_or_file, &filenames);
  std::optional<SequenceNumber> prev_batch_seqno;
  std::optional<uint32_t> prev_batch_count;
  if (!st.ok() || filenames.empty()) {
    // dir_or_file does not exist or does not contain children
    // Check its existence first
    Status s = env->FileExists(dir_or_file);
    // dir_or_file does not exist
    if (!s.ok()) {
      if (exec_state) {
        *exec_state = LDBCommandExecuteResult::Failed(
            dir_or_file + ": No such file or directory");
      }
      return;
    }
    // If it exists and doesn't have children, it should be a log file.
    if (dir_or_file.length() <= 4 ||
        dir_or_file.rfind(".log") != dir_or_file.length() - 4) {
      if (exec_state) {
        *exec_state = LDBCommandExecuteResult::Failed(
            dir_or_file + ": Invalid log file name");
      }
      return;
    }
    DumpWalFile(options, dir_or_file, print_header, print_values,
                only_print_seqno_gaps, is_write_committed, ucmps, exec_state,
                &prev_batch_seqno, &prev_batch_count);
  } else {
    WALFileIterator wal_file_iter(dir_or_file, filenames);
    if (!wal_file_iter.Valid()) {
      if (exec_state) {
        *exec_state = LDBCommandExecuteResult::Failed(
            dir_or_file + ": No valid wal logs found");
      }
      return;
    }
    std::string wal_file = wal_file_iter.GetNextWAL();
    while (!wal_file.empty()) {
      std::cout << "Checking wal file: " << wal_file << std::endl;
      DumpWalFile(options, wal_file, print_header, print_values,
                  only_print_seqno_gaps, is_write_committed, ucmps, exec_state,
                  &prev_batch_seqno, &prev_batch_count);
      if (exec_state->IsFailed() || !wal_file_iter.Valid()) {
        return;
      }
      wal_file = wal_file_iter.GetNextWAL();
    }
  }
}

void DumpWalFile(Options options, const std::string& wal_file,
                 bool print_header, bool print_values,
                 bool only_print_seqno_gaps, bool is_write_committed,
                 const std::map<uint32_t, const Comparator*>& ucmps,
                 LDBCommandExecuteResult* exec_state,
                 std::optional<SequenceNumber>* prev_batch_seqno,
                 std::optional<uint32_t>* prev_batch_count) {
  const auto& fs = options.env->GetFileSystem();
  FileOptions soptions(options);
  std::unique_ptr<SequentialFileReader> wal_file_reader;
  Status status = SequentialFileReader::Create(
      fs, wal_file, soptions, &wal_file_reader, nullptr /* dbg */,
      nullptr /* rate_limiter */);
  if (!status.ok()) {
    if (exec_state) {
      *exec_state = LDBCommandExecuteResult::Failed("Failed to open WAL file " +
                                                    status.ToString());
    } else {
      std::cerr << "Error: Failed to open WAL file " << status.ToString()
                << std::endl;
    }
  } else {
    StdErrReporter reporter;
    uint64_t log_number;
    FileType type;

    // Comparators are available and will be used for formatting user key if DB
    // is opened for this dump wal operation.
    UnorderedMap<uint32_t, size_t> running_ts_sz;
    for (const auto& [cf_id, ucmp] : ucmps) {
      running_ts_sz.emplace(cf_id, ucmp->timestamp_size());
    }
    // we need the log number, but ParseFilename expects dbname/NNN.log.
    std::string sanitized = wal_file;
    size_t lastslash = sanitized.rfind('/');
    if (lastslash != std::string::npos) {
      sanitized = sanitized.substr(lastslash + 1);
    }
    if (!ParseFileName(sanitized, &log_number, &type)) {
      // bogus input, carry on as best we can
      log_number = 0;
    }
    std::unique_ptr<WalReader> reader = NewWalReader(
        options.info_log, std::move(wal_file_reader), &reporter,
        true /* checksum */, log_number, /*track_and_verify_wals=*/false,
        /*stop_replay_for_corruption=*/false,
        std::numeric_limits<uint64_t>::max(), PredecessorWALInfo(),
        /*buffered=*/false);
    std::unordered_set<uint32_t> encountered_cf_ids;
    std::string scratch;
    WriteBatch batch;
    Slice record;
    std::stringstream row;
    if (print_header) {
      std::cout << "Sequence,Count,ByteSize,Physical Offset,Key(s)";
      if (print_values) {
        std::cout << " : value ";
      }
      std::cout << "\n";
    }
    while (status.ok() && reader->ReadRecord(&record, &scratch)) {
      row.str("");
      if (record.size() < WriteBatchInternal::kHeader) {
        reporter.Corruption(record.size(),
                            Status::Corruption("log record too small"));
      } else {
        status = WriteBatchInternal::SetContents(&batch, record);
        if (!status.ok()) {
          std::stringstream oss;
          oss << "Parsing write batch failed: " << status.ToString();
          if (exec_state) {
            *exec_state = LDBCommandExecuteResult::Failed(oss.str());
          } else {
            std::cerr << oss.str() << std::endl;
          }
          break;
        }
        const UnorderedMap<uint32_t, size_t> recorded_ts_sz =
            reader->GetRecordedTimestampSize();
        if (!running_ts_sz.empty()) {
          status = HandleWriteBatchTimestampSizeDifference(
              &batch, running_ts_sz, recorded_ts_sz,
              TimestampSizeConsistencyMode::kVerifyConsistency,
              /* seq_per_batch */ false, /* batch_per_txn */ true,
              /*new_batch=*/nullptr);
          if (!status.ok()) {
            std::stringstream oss;
            oss << "Format for user keys in WAL file is inconsistent with the "
                   "comparator used to open the DB. Timestamp size recorded in "
                   "WAL vs specified by "
                   "comparator: {";
            bool first_cf = true;
            for (const auto& [cf_id, ts_sz] : running_ts_sz) {
              if (first_cf) {
                first_cf = false;
              } else {
                oss << ", ";
              }
              auto record_ts_iter = recorded_ts_sz.find(cf_id);
              size_t ts_sz_in_wal = (record_ts_iter == recorded_ts_sz.end())
                                        ? 0
                                        : record_ts_iter->second;
              oss << "(cf_id: " << cf_id << ", [recorded: " << ts_sz_in_wal
                  << ", comparator: " << ts_sz << "])";
            }
            oss << "}";
            if (exec_state) {
              *exec_state = LDBCommandExecuteResult::Failed(oss.str());
            } else {
              std::cerr << oss.str() << std::endl;
            }
            break;
          }
        }
        SequenceNumber sequence_number = WriteBatchInternal::Sequence(&batch);
        uint32_t batch_count = WriteBatchInternal::Count(&batch);
        assert(prev_batch_seqno);
        assert(prev_batch_count);
        assert(prev_batch_seqno->has_value() == prev_batch_count->has_value());
        // TODO(yuzhangyu): handle pessimistic transactions case.
        if (only_print_seqno_gaps) {
          if (!prev_batch_seqno->has_value() ||
              !prev_batch_count->has_value() ||
              prev_batch_seqno->value() + prev_batch_count->value() ==
                  sequence_number) {
            *prev_batch_seqno = sequence_number;
            *prev_batch_count = batch_count;
            continue;
          } else if (prev_batch_seqno->has_value() &&
                     prev_batch_count->has_value()) {
            row << "Prev batch sequence number: " << prev_batch_seqno->value()
                << ", prev batch count: " << prev_batch_count->value() << ", ";
            *prev_batch_seqno = sequence_number;
            *prev_batch_count = batch_count;
          }
        }
        row << sequence_number << ",";
        row << batch_count << ",";
        *prev_batch_seqno = sequence_number;
        *prev_batch_count = batch_count;
        row << WriteBatchInternal::ByteSize(&batch) << ",";
        row << reader->LastRecordOffset() << ",";
        ColumnFamilyCollector cf_collector;
        status = batch.Iterate(&cf_collector);
        auto cf_ids = cf_collector.column_families();
        encountered_cf_ids.insert(cf_ids.begin(), cf_ids.end());
        InMemoryHandler handler(row, print_values, is_write_committed, ucmps);
        status = batch.Iterate(&handler);
        if (!status.ok()) {
          if (exec_state) {
            std::stringstream oss;
            oss << "Print write batch error: " << status.ToString();
            *exec_state = LDBCommandExecuteResult::Failed(oss.str());
          }
          row << "error: " << status.ToString();
          break;
        }
        row << "\n";
      }
      std::cout << row.str();
    }

    std::stringstream cf_ids_oss;
    bool empty_cfs = true;
    for (uint32_t cf_id : encountered_cf_ids) {
      if (ucmps.find(cf_id) == ucmps.end()) {
        if (empty_cfs) {
          cf_ids_oss << "[";
          empty_cfs = false;
        } else {
          cf_ids_oss << ",";
        }
        cf_ids_oss << cf_id;
      }
    }
    if (!empty_cfs) {
      cf_ids_oss << "]";
      std::cout
          << "(Column family id: " << cf_ids_oss.str()
          << " contained in WAL are not opened in DB. Applied default "
             "hex formatting for user key. Specify --db=<db_path> to "
             "open DB for better user key formatting if it contains timestamp.)"
          << std::endl;
    }
  }
}

}  // namespace

const std::string WALDumperCommand::ARG_WAL_FILE = "walfile";
const std::string WALDumperCommand::ARG_WRITE_COMMITTED = "write_committed";
const std::string WALDumperCommand::ARG_PRINT_VALUE = "print_value";
const std::string WALDumperCommand::ARG_PRINT_HEADER = "header";
const std::string WALDumperCommand::ARG_ONLY_PRINT_SEQNO_GAPS =
    "only_print_seqno_gaps";

WALDumperCommand::WALDumperCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, true /* is_read_only */,
                 BuildCmdLineOptions({ARG_WAL_FILE, ARG_DB, ARG_WRITE_COMMITTED,
                                      ARG_PRINT_HEADER, ARG_PRINT_VALUE,
                                      ARG_ONLY_PRINT_SEQNO_GAPS})),
      print_header_(false),
      print_values_(false),
      only_print_seqno_gaps_(false),
      is_write_committed_(false) {
  wal_file_.clear();

  auto itr = options.find(ARG_WAL_FILE);
  if (itr != options.end()) {
    wal_file_ = itr->second;
  }

  print_header_ = IsFlagPresent(flags, ARG_PRINT_HEADER);
  print_values_ = IsFlagPresent(flags, ARG_PRINT_VALUE);
  only_print_seqno_gaps_ = IsFlagPresent(flags, ARG_ONLY_PRINT_SEQNO_GAPS);
  is_write_committed_ = ParseBooleanOption(options, ARG_WRITE_COMMITTED, true);

  if (wal_file_.empty()) {
    exec_state_ = LDBCommandExecuteResult::Failed("Argument " + ARG_WAL_FILE +
                                                  " must be specified.");
  }

  if (!db_path_.empty()) {
    no_db_open_ = false;
  }
}

void WALDumperCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(WALDumperCommand::Name());
  ret.append(" --" + ARG_WAL_FILE +
             "=<write_ahead_log_file_path_or_directory>");
  ret.append(" [--" + ARG_DB + "=<db_path>]");
  ret.append(" [--" + ARG_PRINT_HEADER + "] ");
  ret.append(" [--" + ARG_PRINT_VALUE + "] ");
  ret.append(" [--" + ARG_ONLY_PRINT_SEQNO_GAPS +
             "] (only correct if not using pessimistic transactions)");
  ret.append(" [--" + ARG_WRITE_COMMITTED + "=true|false] ");
  ret.append("\n");
}

void WALDumperCommand::DoCommand() {
  PrepareOptions();
  DumpWalFiles(options_, wal_file_, print_header_, print_values_,
               only_print_seqno_gaps_, is_write_committed_, ucmps_,
               &exec_state_);
}

// ----------------------------------------------------------------------------

GetCommand::GetCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags, true /* is_read_only */,
                 BuildCmdLineOptions({ARG_TTL, ARG_HEX, ARG_KEY_HEX,
                                      ARG_VALUE_HEX, ARG_READ_TIMESTAMP})) {
  if (params.size() != 1) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "<key> must be specified for the get command");
  } else {
    key_ = params.at(0);
  }

  if (is_key_hex_) {
    key_ = HexToString(key_);
  }
}

void GetCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(GetCommand::Name());
  ret.append(" <key>");
  ret.append(" [--" + ARG_READ_TIMESTAMP + "=<uint64_ts>] ");
  ret.append(" [--" + ARG_TTL + "]");
  ret.append("\n");
}

void GetCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  ReadOptions ropts;
  Slice read_timestamp;
  ColumnFamilyHandle* cfh = GetCfHandle();
  Status st = MaybePopulateReadTimestamp(cfh, ropts, &read_timestamp);
  if (!st.ok()) {
    std::stringstream oss;
    oss << "Get failed: " << st.ToString();
    exec_state_ = LDBCommandExecuteResult::Failed(oss.str());
    return;
  }
  std::string value;
  st = db_->Get(ropts, cfh, key_, &value);
  if (st.ok()) {
    fprintf(stdout, "%s\n",
            (is_value_hex_ ? StringToHex(value) : value).c_str());
  } else if (st.IsNotFound()) {
    fprintf(stdout, "Key not found\n");
  } else {
    std::stringstream oss;
    oss << "Get failed: " << st.ToString();
    exec_state_ = LDBCommandExecuteResult::Failed(oss.str());
  }
}

// ----------------------------------------------------------------------------

MultiGetCommand::MultiGetCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, true /* is_read_only */,
                 BuildCmdLineOptions({ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX,
                                      ARG_READ_TIMESTAMP})) {
  if (params.size() < 1) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "At least one <key> must be specified for multi_get.");
  } else {
    for (size_t i = 0; i < params.size(); ++i) {
      std::string key = params.at(i);
      keys_.emplace_back(is_key_hex_ ? HexToString(key) : key);
    }
  }
}

void MultiGetCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(MultiGetCommand::Name());
  ret.append(" <key_1> <key_2> <key_3> ...");
  ret.append(" [--" + ARG_READ_TIMESTAMP + "=<uint64_ts>] ");
  ret.append("\n");
}

void MultiGetCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  ReadOptions ropts;
  Slice read_timestamp;
  ColumnFamilyHandle* cfh = GetCfHandle();
  Status st = MaybePopulateReadTimestamp(cfh, ropts, &read_timestamp);
  if (!st.ok()) {
    std::stringstream oss;
    oss << "MultiGet failed: " << st.ToString();
    exec_state_ = LDBCommandExecuteResult::Failed(oss.str());
    return;
  }
  size_t num_keys = keys_.size();
  std::vector<Slice> key_slices;
  std::vector<PinnableSlice> values(num_keys);
  std::vector<Status> statuses(num_keys);
  for (const std::string& key : keys_) {
    key_slices.emplace_back(key);
  }
  db_->MultiGet(ropts, cfh, num_keys, key_slices.data(), values.data(),
                statuses.data());

  bool failed = false;
  for (size_t i = 0; i < num_keys; ++i) {
    if (statuses[i].ok()) {
      fprintf(stdout, is_value_hex_ ? "%s%s0x%s\n" : "%s%s%s\n",
              (is_key_hex_ ? StringToHex(keys_[i]) : keys_[i]).c_str(), DELIM,
              values[i].ToString(is_value_hex_).c_str());
    } else if (statuses[i].IsNotFound()) {
      fprintf(stdout, "Key not found: %s\n",
              (is_key_hex_ ? StringToHex(keys_[i]) : keys_[i]).c_str());
    } else {
      fprintf(stderr, "Status for key %s: %s\n",
              (is_key_hex_ ? StringToHex(keys_[i]) : keys_[i]).c_str(),
              statuses[i].ToString().c_str());
      failed = true;
    }
  }
  if (failed) {
    exec_state_ =
        LDBCommandExecuteResult::Failed("one or more keys had non-okay status");
  }
}

// ----------------------------------------------------------------------------

GetEntityCommand::GetEntityCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, true /* is_read_only */,
                 BuildCmdLineOptions({ARG_TTL, ARG_HEX, ARG_KEY_HEX,
                                      ARG_VALUE_HEX, ARG_READ_TIMESTAMP})) {
  if (params.size() != 1) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "<key> must be specified for the get_entity command");
  } else {
    key_ = params.at(0);
  }

  if (is_key_hex_) {
    key_ = HexToString(key_);
  }
}

void GetEntityCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(GetEntityCommand::Name());
  ret.append(" <key>");
  ret.append(" [--" + ARG_READ_TIMESTAMP + "=<uint64_ts>] ");
  ret.append(" [--" + ARG_TTL + "]");
  ret.append("\n");
}

void GetEntityCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  ReadOptions ropt;
  Slice read_timestamp;
  ColumnFamilyHandle* cfh = GetCfHandle();
  Status st = MaybePopulateReadTimestamp(cfh, ropt, &read_timestamp);
  if (!st.ok()) {
    std::stringstream oss;
    oss << "GetEntity failed: " << st.ToString();
    exec_state_ = LDBCommandExecuteResult::Failed(oss.str());
    return;
  }
  PinnableWideColumns pinnable_wide_columns;
  st = db_->GetEntity(ropt, cfh, key_, &pinnable_wide_columns);
  if (st.ok()) {
    std::ostringstream oss;
    WideColumnsHelper::DumpWideColumns(pinnable_wide_columns.columns(), oss,
                                       is_value_hex_);
    fprintf(stdout, "%s\n", oss.str().c_str());
  } else {
    std::stringstream oss;
    oss << "GetEntity failed: " << st.ToString();
    exec_state_ = LDBCommandExecuteResult::Failed(oss.str());
  }
}

// ----------------------------------------------------------------------------

MultiGetEntityCommand::MultiGetEntityCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, true /* is_read_only */,
                 BuildCmdLineOptions({ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX,
                                      ARG_READ_TIMESTAMP})) {
  if (params.size() < 1) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "At least one <key> must be specified for the multi_get_entity "
        "command");
  } else {
    for (size_t i = 0; i < params.size(); i++) {
      std::string key = params.at(i);
      keys_.emplace_back(is_key_hex_ ? HexToString(key) : key);
    }
  }
}

void MultiGetEntityCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(MultiGetEntityCommand::Name());
  ret.append(" <key_1> <key_2> <key_3> ...");
  ret.append(" [--" + ARG_READ_TIMESTAMP + "=<uint64_ts>] ");
  ret.append("\n");
}

void MultiGetEntityCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }

  ReadOptions ropt;
  Slice read_timestamp;
  ColumnFamilyHandle* cfh = GetCfHandle();
  Status st = MaybePopulateReadTimestamp(cfh, ropt, &read_timestamp);
  if (!st.ok()) {
    std::stringstream oss;
    oss << "MultiGetEntity failed: " << st.ToString();
    exec_state_ = LDBCommandExecuteResult::Failed(oss.str());
    return;
  }
  size_t num_keys = keys_.size();
  std::vector<Slice> key_slices;
  std::vector<PinnableWideColumns> results(num_keys);
  std::vector<Status> statuses(num_keys);
  for (const std::string& key : keys_) {
    key_slices.emplace_back(key);
  }

  db_->MultiGetEntity(ropt, cfh, num_keys, key_slices.data(), results.data(),
                      statuses.data());

  bool failed = false;
  for (size_t i = 0; i < num_keys; ++i) {
    std::string key = is_key_hex_ ? StringToHex(keys_[i]) : keys_[i];
    if (statuses[i].ok()) {
      std::ostringstream oss;
      oss << key << DELIM;
      WideColumnsHelper::DumpWideColumns(results[i].columns(), oss,
                                         is_value_hex_);
      fprintf(stdout, "%s\n", oss.str().c_str());
    } else if (statuses[i].IsNotFound()) {
      fprintf(stdout, "Key not found: %s\n", key.c_str());
    } else {
      fprintf(stderr, "Status for key %s: %s\n", key.c_str(),
              statuses[i].ToString().c_str());
      failed = true;
    }
  }
  if (failed) {
    exec_state_ =
        LDBCommandExecuteResult::Failed("one or more keys had non-okay status");
  }
}

// ----------------------------------------------------------------------------

ApproxSizeCommand::ApproxSizeCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, true /* is_read_only */,
                 BuildCmdLineOptions(
                     {ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX, ARG_FROM, ARG_TO})) {
  if (options.find(ARG_FROM) != options.end()) {
    start_key_ = options.find(ARG_FROM)->second;
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(
        ARG_FROM + " must be specified for approxsize command");
    return;
  }

  if (options.find(ARG_TO) != options.end()) {
    end_key_ = options.find(ARG_TO)->second;
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(
        ARG_TO + " must be specified for approxsize command");
    return;
  }

  if (is_key_hex_) {
    start_key_ = HexToString(start_key_);
    end_key_ = HexToString(end_key_);
  }
}

void ApproxSizeCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(ApproxSizeCommand::Name());
  ret.append(HelpRangeCmdArgs());
  ret.append("\n");
}

void ApproxSizeCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  Range ranges[1];
  ranges[0] = Range(start_key_, end_key_);
  uint64_t sizes[1];
  Status s = db_->GetApproximateSizes(GetCfHandle(), ranges, 1, sizes);
  if (!s.ok()) {
    std::stringstream oss;
    oss << "ApproximateSize failed: " << s.ToString();
    exec_state_ = LDBCommandExecuteResult::Failed(oss.str());
  } else {
    fprintf(stdout, "%lu\n", (unsigned long)sizes[0]);
  }
}

// ----------------------------------------------------------------------------

BatchPutCommand::BatchPutCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, false /* is_read_only */,
                 BuildCmdLineOptions({ARG_TTL, ARG_HEX, ARG_KEY_HEX,
                                      ARG_VALUE_HEX, ARG_CREATE_IF_MISSING})) {
  if (params.size() < 2) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "At least one <key> <value> pair must be specified batchput.");
  } else if (params.size() % 2 != 0) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "Equal number of <key>s and <value>s must be specified for batchput.");
  } else {
    for (size_t i = 0; i < params.size(); i += 2) {
      std::string key = params.at(i);
      std::string value = params.at(i + 1);
      key_values_.emplace_back(is_key_hex_ ? HexToString(key) : key,
                               is_value_hex_ ? HexToString(value) : value);
    }
  }
  create_if_missing_ = IsFlagPresent(flags_, ARG_CREATE_IF_MISSING);
}

void BatchPutCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(BatchPutCommand::Name());
  ret.append(" <key> <value> [<key> <value>] [..]");
  ret.append(" [--" + ARG_CREATE_IF_MISSING + "]");
  ret.append(" [--" + ARG_TTL + "]");
  ret.append("\n");
}

void BatchPutCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  WriteBatch batch;

  Status st;
  std::stringstream oss;
  for (std::vector<std::pair<std::string, std::string>>::const_iterator itr =
           key_values_.begin();
       itr != key_values_.end(); ++itr) {
    st = batch.Put(GetCfHandle(), itr->first, itr->second);
    if (!st.ok()) {
      oss << "Put to write batch failed: " << itr->first << "=>" << itr->second
          << " error: " << st.ToString();
      break;
    }
  }
  if (st.ok()) {
    st = db_->Write(WriteOptions(), &batch);
    if (!st.ok()) {
      oss << "Write failed: " << st.ToString();
    }
  }
  if (st.ok()) {
    fprintf(stdout, "OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(oss.str());
  }
}

void BatchPutCommand::OverrideBaseOptions() {
  LDBCommand::OverrideBaseOptions();
  options_.create_if_missing = create_if_missing_;
}

// ----------------------------------------------------------------------------

ScanCommand::ScanCommand(const std::vector<std::string>& /*params*/,
                         const std::map<std::string, std::string>& options,
                         const std::vector<std::string>& flags)
    : LDBCommand(
          options, flags, true /* is_read_only */,
          BuildCmdLineOptions({ARG_TTL, ARG_NO_VALUE, ARG_HEX, ARG_KEY_HEX,
                               ARG_TO, ARG_VALUE_HEX, ARG_FROM, ARG_TIMESTAMP,
                               ARG_MAX_KEYS, ARG_TTL_START, ARG_TTL_END,
                               ARG_READ_TIMESTAMP, ARG_GET_WRITE_UNIX_TIME})),
      start_key_specified_(false),
      end_key_specified_(false),
      max_keys_scanned_(-1),
      no_value_(false) {
  auto itr = options.find(ARG_FROM);
  if (itr != options.end()) {
    start_key_ = itr->second;
    if (is_key_hex_) {
      start_key_ = HexToString(start_key_);
    }
    start_key_specified_ = true;
  }
  itr = options.find(ARG_TO);
  if (itr != options.end()) {
    end_key_ = itr->second;
    if (is_key_hex_) {
      end_key_ = HexToString(end_key_);
    }
    end_key_specified_ = true;
  }

  std::vector<std::string>::const_iterator vitr =
      std::find(flags.begin(), flags.end(), ARG_NO_VALUE);
  if (vitr != flags.end()) {
    no_value_ = true;
  }

  itr = options.find(ARG_MAX_KEYS);
  if (itr != options.end()) {
    try {
#if defined(CYGWIN)
      max_keys_scanned_ = strtol(itr->second.c_str(), 0, 10);
#else
      max_keys_scanned_ = std::stoi(itr->second);
#endif
    } catch (const std::invalid_argument&) {
      exec_state_ = LDBCommandExecuteResult::Failed(ARG_MAX_KEYS +
                                                    " has an invalid value");
    } catch (const std::out_of_range&) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          ARG_MAX_KEYS + " has a value out-of-range");
    }
  }
  get_write_unix_time_ = IsFlagPresent(flags_, ARG_GET_WRITE_UNIX_TIME);
}

void ScanCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(ScanCommand::Name());
  ret.append(HelpRangeCmdArgs());
  ret.append(" [--" + ARG_TTL + "]");
  ret.append(" [--" + ARG_TIMESTAMP + "]");
  ret.append(" [--" + ARG_MAX_KEYS + "=<N>q] ");
  ret.append(" [--" + ARG_TTL_START + "=<N>:- is inclusive]");
  ret.append(" [--" + ARG_TTL_END + "=<N>:- is exclusive]");
  ret.append(" [--" + ARG_NO_VALUE + "]");
  ret.append(" [--" + ARG_READ_TIMESTAMP + "=<uint64_ts>] ");
  ret.append(" [--" + ARG_GET_WRITE_UNIX_TIME + "]");
  ret.append("\n");
}

void ScanCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }

  int num_keys_scanned = 0;
  ReadOptions scan_read_opts;
  ColumnFamilyHandle* cfh = GetCfHandle();
  const Comparator* ucmp = cfh->GetComparator();
  size_t ts_sz = ucmp->timestamp_size();
  Slice read_timestamp;
  Status st = MaybePopulateReadTimestamp(cfh, scan_read_opts, &read_timestamp);
  if (!st.ok()) {
    std::stringstream oss;
    oss << "Scan failed: " << st.ToString();
    exec_state_ = LDBCommandExecuteResult::Failed(oss.str());
    return;
  }
  scan_read_opts.total_order_seek = true;
  Iterator* it = db_->NewIterator(scan_read_opts, cfh);
  if (start_key_specified_) {
    it->Seek(start_key_);
  } else {
    it->SeekToFirst();
  }
  int ttl_start;
  if (!ParseIntOption(option_map_, ARG_TTL_START, ttl_start, exec_state_)) {
    ttl_start = DBWithTTLImpl::kMinTimestamp;  // TTL introduction time
  }
  int ttl_end;
  if (!ParseIntOption(option_map_, ARG_TTL_END, ttl_end, exec_state_)) {
    ttl_end = DBWithTTLImpl::kMaxTimestamp;  // Max time allowed by TTL feature
  }
  if (ttl_end < ttl_start) {
    fprintf(stderr, "Error: End time can't be less than start time\n");
    delete it;
    return;
  }
  if (is_db_ttl_ && timestamp_) {
    fprintf(stdout, "Scanning key-values from %s to %s\n",
            TimeToHumanString(ttl_start).c_str(),
            TimeToHumanString(ttl_end).c_str());
  }
  for (;
       it->Valid() && (!end_key_specified_ || it->key().ToString() < end_key_);
       it->Next()) {
    if (is_db_ttl_) {
      TtlIterator* it_ttl = static_cast_with_check<TtlIterator>(it);
      int rawtime = it_ttl->ttl_timestamp();
      if (rawtime < ttl_start || rawtime >= ttl_end) {
        continue;
      }
      if (timestamp_) {
        fprintf(stdout, "%s ", TimeToHumanString(rawtime).c_str());
      }
    }

    if (no_value_) {
      std::string key_str = it->key().ToString();
      if (is_key_hex_) {
        key_str = StringToHex(key_str);
      } else if (ldb_options_.key_formatter) {
        key_str = ldb_options_.key_formatter->Format(key_str);
      }
      fprintf(stdout, "%s\n", key_str.c_str());
    } else {
      std::string str =
          is_db_ttl_
              ? PrintKeyValue(it->key().ToString(),
                              ts_sz == 0 ? "" : it->timestamp().ToString(),
                              it->value().ToString(), is_key_hex_,
                              is_value_hex_, ucmp)
              : PrintKeyValueOrWideColumns(
                    it->key(), ts_sz == 0 ? "" : it->timestamp(), it->value(),
                    it->columns(), is_key_hex_, is_value_hex_, ucmp);
      fprintf(stdout, "%s\n", str.c_str());
    }

    if (get_write_unix_time_) {
      std::string write_unix_time;
      uint64_t write_time_int = std::numeric_limits<uint64_t>::max();
      Status s =
          it->GetProperty("rocksdb.iterator.write-time", &write_unix_time);
      if (s.ok()) {
        s = DecodeU64Ts(write_unix_time, &write_time_int);
      }
      if (!s.ok()) {
        fprintf(stdout, "  Failed to get write unix time: %s\n",
                s.ToString().c_str());
      } else {
        fprintf(stdout, "  write unix time: %s\n",
                std::to_string(write_time_int).c_str());
      }
    }
    num_keys_scanned++;
    if (max_keys_scanned_ >= 0 && num_keys_scanned >= max_keys_scanned_) {
      break;
    }
  }
  if (!it->status().ok()) {  // Check for any errors found during the scan
    exec_state_ = LDBCommandExecuteResult::Failed(it->status().ToString());
  }
  delete it;
}

// ----------------------------------------------------------------------------

DeleteCommand::DeleteCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags, false /* is_read_only */,
                 BuildCmdLineOptions({ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX})) {
  if (params.size() != 1) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "KEY must be specified for the delete command");
  } else {
    key_ = params.at(0);
    if (is_key_hex_) {
      key_ = HexToString(key_);
    }
  }
}

void DeleteCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(DeleteCommand::Name() + " <key>");
  ret.append("\n");
}

void DeleteCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  Status st = db_->Delete(WriteOptions(), GetCfHandle(), key_);
  if (st.ok()) {
    fprintf(stdout, "OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(st.ToString());
  }
}

SingleDeleteCommand::SingleDeleteCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, false /* is_read_only */,
                 BuildCmdLineOptions({ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX})) {
  if (params.size() != 1) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "KEY must be specified for the single delete command");
  } else {
    key_ = params.at(0);
    if (is_key_hex_) {
      key_ = HexToString(key_);
    }
  }
}

void SingleDeleteCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(SingleDeleteCommand::Name() + " <key>");
  ret.append("\n");
}

void SingleDeleteCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  Status st = db_->SingleDelete(WriteOptions(), GetCfHandle(), key_);
  if (st.ok()) {
    fprintf(stdout, "OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(st.ToString());
  }
}

DeleteRangeCommand::DeleteRangeCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, false /* is_read_only */,
                 BuildCmdLineOptions({ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX})) {
  if (params.size() != 2) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "begin and end keys must be specified for the delete command");
  } else {
    begin_key_ = params.at(0);
    end_key_ = params.at(1);
    if (is_key_hex_) {
      begin_key_ = HexToString(begin_key_);
      end_key_ = HexToString(end_key_);
    }
  }
}

void DeleteRangeCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(DeleteRangeCommand::Name() + " <begin key> <end key>");
  ret.append("\n");
}

void DeleteRangeCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  Status st =
      db_->DeleteRange(WriteOptions(), GetCfHandle(), begin_key_, end_key_);
  if (st.ok()) {
    fprintf(stdout, "OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(st.ToString());
  }
}

PutCommand::PutCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags, false /* is_read_only */,
                 BuildCmdLineOptions({ARG_TTL, ARG_HEX, ARG_KEY_HEX,
                                      ARG_VALUE_HEX, ARG_CREATE_IF_MISSING})) {
  if (params.size() != 2) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "<key> and <value> must be specified for the put command");
  } else {
    key_ = params.at(0);
    value_ = params.at(1);
  }

  if (is_key_hex_) {
    key_ = HexToString(key_);
  }

  if (is_value_hex_) {
    value_ = HexToString(value_);
  }
  create_if_missing_ = IsFlagPresent(flags_, ARG_CREATE_IF_MISSING);
}

void PutCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(PutCommand::Name());
  ret.append(" <key> <value>");
  ret.append(" [--" + ARG_CREATE_IF_MISSING + "]");
  ret.append(" [--" + ARG_TTL + "]");
  ret.append("\n");
}

void PutCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  Status st = db_->Put(WriteOptions(), GetCfHandle(), key_, value_);
  if (st.ok()) {
    fprintf(stdout, "OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(st.ToString());
  }
}

void PutCommand::OverrideBaseOptions() {
  LDBCommand::OverrideBaseOptions();
  options_.create_if_missing = create_if_missing_;
}

// ----------------------------------------------------------------------------

PutEntityCommand::PutEntityCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, false /* is_read_only */,
                 BuildCmdLineOptions({ARG_TTL, ARG_HEX, ARG_KEY_HEX,
                                      ARG_VALUE_HEX, ARG_CREATE_IF_MISSING})) {
  if (params.size() < 2) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "<key> and at least one column <column_name>:<column_value> must be "
        "specified for the put_entity command");
  } else {
    auto iter = params.begin();
    key_ = *iter;
    if (is_key_hex_) {
      key_ = HexToString(key_);
    }
    for (++iter; iter != params.end(); ++iter) {
      auto split = StringSplit(*iter, ':');
      if (split.size() != 2) {
        exec_state_ = LDBCommandExecuteResult::Failed(
            "wide column format needs to be <column_name>:<column_value> (did "
            "you mean put <key> <value>?)");
        return;
      }
      std::string name(split[0]);
      std::string value(split[1]);
      if (is_value_hex_) {
        name = HexToString(name);
        value = HexToString(value);
      }
      column_names_.push_back(name);
      column_values_.push_back(value);
    }
  }
  create_if_missing_ = IsFlagPresent(flags_, ARG_CREATE_IF_MISSING);
}

void PutEntityCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(PutEntityCommand::Name());
  ret.append(
      " <key> <column1_name>:<column1_value> <column2_name>:<column2_value> "
      "<...>");
  ret.append(" [--" + ARG_CREATE_IF_MISSING + "]");
  ret.append(" [--" + ARG_TTL + "]");
  ret.append("\n");
}

void PutEntityCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  assert(column_names_.size() == column_values_.size());
  WideColumns columns;
  for (size_t i = 0; i < column_names_.size(); i++) {
    WideColumn column(column_names_[i], column_values_[i]);
    columns.emplace_back(column);
  }
  Status st = db_->PutEntity(WriteOptions(), GetCfHandle(), key_, columns);
  if (st.ok()) {
    fprintf(stdout, "OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(st.ToString());
  }
}

void PutEntityCommand::OverrideBaseOptions() {
  LDBCommand::OverrideBaseOptions();
  options_.create_if_missing = create_if_missing_;
}

// ----------------------------------------------------------------------------

const char* DBQuerierCommand::HELP_CMD = "help";
const char* DBQuerierCommand::GET_CMD = "get";
const char* DBQuerierCommand::PUT_CMD = "put";
const char* DBQuerierCommand::DELETE_CMD = "delete";
const char* DBQuerierCommand::COUNT_CMD = "count";

DBQuerierCommand::DBQuerierCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(
          options, flags, false /* is_read_only */,
          BuildCmdLineOptions({ARG_TTL, ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX})) {

}

void DBQuerierCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(DBQuerierCommand::Name());
  ret.append(" [--" + ARG_TTL + "]");
  ret.append("\n");
  ret.append(
      "    Starts a REPL shell.  Type help for list of available "
      "commands.");
  ret.append("\n");
}

void DBQuerierCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }

  std::string line;
  Status s;
  ColumnFamilyHandle* cfh = GetCfHandle();
  const Comparator* ucmp = cfh->GetComparator();
  while ((s.ok() || s.IsNotFound() || s.IsInvalidArgument()) &&
         getline(std::cin, line, '\n')) {
    std::string key;
    std::string timestamp;
    std::string value;
    // Reset to OK status before parsing and executing next user command.
    s = Status::OK();
    std::stringstream oss;
    // Parse line into std::vector<std::string>
    std::vector<std::string> tokens;
    ParsedParams parsed_params;
    size_t pos = 0;
    while (true) {
      size_t pos2 = line.find(' ', pos);
      std::string token =
          line.substr(pos, (pos2 == std::string::npos) ? pos2 : (pos2 - pos));
      ParseSingleParam(token, parsed_params, tokens);
      if (pos2 == std::string::npos) {
        break;
      }
      pos = pos2 + 1;
    }

    if (tokens.empty() || !parsed_params.flags.empty()) {
      fprintf(stdout, "Bad command\n");
      continue;
    }

    const std::string& cmd = tokens[0];
    ReadOptions read_options;
    WriteOptions write_options;
    Slice read_timestamp;

    if (cmd == HELP_CMD) {
      fprintf(stdout,
              "get <key> [--read_timestamp=<uint64_ts>]\n"
              "put <key> [<write_timestamp>] <value>\n"
              "delete <key> [<write_timestamp>]\n"
              "count [--from=<start_key>] [--to=<end_key>] "
              "[--read_timestamp=<uint64_ts>]\n");
    } else if (cmd == DELETE_CMD && parsed_params.option_map.empty()) {
      key = (is_key_hex_ ? HexToString(tokens[1]) : tokens[1]);
      if (tokens.size() == 2) {
        s = db_->Delete(write_options, cfh, Slice(key));
      } else if (tokens.size() == 3) {
        Status encode_s = EncodeUserProvidedTimestamp(tokens[2], &timestamp);
        if (encode_s.ok()) {
          s = db_->Delete(write_options, cfh, Slice(key), Slice(timestamp));
        } else {
          fprintf(stdout, "delete gets invalid argument: %s\n",
                  encode_s.ToString().c_str());
          continue;
        }
      } else {
        fprintf(stdout, "delete gets invalid arguments\n");
        continue;
      }
      oss << "delete " << (is_key_hex_ ? StringToHex(key) : key);
      if (!timestamp.empty()) {
        oss << " write_ts: " << ucmp->TimestampToString(timestamp);
      }
      if (s.ok()) {
        oss << " succeeded";
      } else {
        oss << " failed: " << s.ToString();
      }
      fprintf(stdout, "%s\n", oss.str().c_str());
    } else if (cmd == PUT_CMD && parsed_params.option_map.empty()) {
      key = (is_key_hex_ ? HexToString(tokens[1]) : tokens[1]);
      if (tokens.size() == 3) {
        value = (is_value_hex_ ? HexToString(tokens[2]) : tokens[2]);
        s = db_->Put(write_options, cfh, Slice(key), Slice(value));
      } else if (tokens.size() == 4) {
        value = (is_value_hex_ ? HexToString(tokens[3]) : tokens[3]);
        Status encode_s = EncodeUserProvidedTimestamp(tokens[2], &timestamp);
        if (encode_s.ok()) {
          s = db_->Put(write_options, cfh, Slice(key), Slice(timestamp),
                       Slice(value));
        } else {
          fprintf(stdout, "put gets invalid argument: %s\n",
                  encode_s.ToString().c_str());
          continue;
        }
      } else {
        fprintf(stdout, "put gets invalid arguments\n");
        continue;
      }

      oss << "put " << (is_key_hex_ ? StringToHex(key) : key);
      if (!timestamp.empty()) {
        oss << " write_ts: " << ucmp->TimestampToString(timestamp);
      }
      oss << " => " << (is_value_hex_ ? StringToHex(value) : value);
      if (s.ok()) {
        oss << " succeeded";
      } else {
        oss << " failed: " << s.ToString();
      }
      fprintf(stdout, "%s\n", oss.str().c_str());
    } else if (cmd == GET_CMD && tokens.size() == 2) {
      key = (is_key_hex_ ? HexToString(tokens[1]) : tokens[1]);
      bool bad_option = false;
      for (auto& option : parsed_params.option_map) {
        if (option.first == "read_timestamp") {
          Status encode_s =
              EncodeUserProvidedTimestamp(option.second, &timestamp);
          if (!encode_s.ok()) {
            fprintf(stdout, "get gets invalid argument: %s\n",
                    encode_s.ToString().c_str());
            bad_option = true;
            break;
          }
          read_timestamp = timestamp;
          read_options.timestamp = &read_timestamp;
        } else {
          fprintf(stdout, "get gets invalid arguments\n");
          bad_option = true;
          break;
        }
      }
      if (bad_option) {
        continue;
      }
      s = db_->Get(read_options, cfh, Slice(key), &value);
      if (s.ok()) {
        fprintf(stdout, "%s\n",
                PrintKeyValue(key, timestamp, value, is_key_hex_, is_value_hex_,
                              ucmp)
                    .c_str());
      } else {
        oss << "get " << (is_key_hex_ ? StringToHex(key) : key);
        if (!timestamp.empty()) {
          oss << " read_timestamp: " << ucmp->TimestampToString(timestamp);
        }
        oss << " status: " << s.ToString();
        fprintf(stdout, "%s\n", oss.str().c_str());
      }
    } else if (cmd == COUNT_CMD) {
      std::string start_key;
      std::string end_key;
      bool bad_option = false;
      for (auto& option : parsed_params.option_map) {
        if (option.first == "from") {
          start_key =
              (is_key_hex_ ? HexToString(option.second) : option.second);
        } else if (option.first == "to") {
          end_key = (is_key_hex_ ? HexToString(option.second) : option.second);
        } else if (option.first == "read_timestamp") {
          Status encode_s =
              EncodeUserProvidedTimestamp(option.second, &timestamp);
          if (!encode_s.ok()) {
            bad_option = true;
            fprintf(stdout, "count gets invalid argument: %s\n",
                    encode_s.ToString().c_str());
            break;
          }
          read_timestamp = timestamp;
          read_options.timestamp = &read_timestamp;
        } else {
          fprintf(stdout, "count gets invalid arguments\n");
          bad_option = true;
          break;
        }
      }
      if (bad_option) {
        continue;
      }

      Slice end_key_slice(end_key);
      uint64_t count = 0;
      if (!end_key.empty()) {
        read_options.iterate_upper_bound = &end_key_slice;
      }
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options, cfh));
      if (start_key.empty()) {
        iter->SeekToFirst();
      } else {
        iter->Seek(start_key);
      }
      while (iter->status().ok() && iter->Valid()) {
        count++;
        iter->Next();
      }
      if (iter->status().ok()) {
        fprintf(stdout, "%" PRIu64 "\n", count);
      } else {
        oss << "scan from "
            << (is_key_hex_ ? StringToHex(start_key) : start_key);
        if (!timestamp.empty()) {
          oss << " read_timestamp: " << ucmp->TimestampToString(timestamp);
        }
        oss << " to " << (is_key_hex_ ? StringToHex(end_key) : end_key)
            << " failed: " << iter->status().ToString();
        fprintf(stdout, "%s\n", oss.str().c_str());
      }
    } else {
      fprintf(stdout, "Unknown command %s\n", line.c_str());
    }
  }
  if (!(s.ok() || s.IsNotFound() || s.IsInvalidArgument())) {
    exec_state_ = LDBCommandExecuteResult::Failed(s.ToString());
  }
}

// ----------------------------------------------------------------------------

CheckConsistencyCommand::CheckConsistencyCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, true /* is_read_only */,
                 BuildCmdLineOptions({})) {}

void CheckConsistencyCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(CheckConsistencyCommand::Name());
  ret.append("\n");
}

void CheckConsistencyCommand::DoCommand() {
  options_.paranoid_checks = true;
  options_.num_levels = 64;
  OpenDB();
  if (exec_state_.IsSucceed() || exec_state_.IsNotStarted()) {
    fprintf(stdout, "OK\n");
  }
  CloseDB();
}

// ----------------------------------------------------------------------------

const std::string CheckPointCommand::ARG_CHECKPOINT_DIR = "checkpoint_dir";

CheckPointCommand::CheckPointCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, false /* is_read_only */,
                 BuildCmdLineOptions({ARG_CHECKPOINT_DIR})) {
  auto itr = options.find(ARG_CHECKPOINT_DIR);
  if (itr != options.end()) {
    checkpoint_dir_ = itr->second;
  }
}

void CheckPointCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(CheckPointCommand::Name());
  ret.append(" [--" + ARG_CHECKPOINT_DIR + "] ");
  ret.append("\n");
}

void CheckPointCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  Checkpoint* checkpoint;
  Status status = Checkpoint::Create(db_.get(), &checkpoint);
  status = checkpoint->CreateCheckpoint(checkpoint_dir_);
  if (status.ok()) {
    fprintf(stdout, "OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(status.ToString());
  }
}

// ----------------------------------------------------------------------------

const std::string RepairCommand::ARG_VERBOSE = "verbose";

RepairCommand::RepairCommand(const std::vector<std::string>& /*params*/,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags, false /* is_read_only */,
                 BuildCmdLineOptions({ARG_VERBOSE})) {
  verbose_ = IsFlagPresent(flags, ARG_VERBOSE);
}

void RepairCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(RepairCommand::Name());
  ret.append(" [--" + ARG_VERBOSE + "]");
  ret.append("\n");
}

void RepairCommand::OverrideBaseOptions() {
  LDBCommand::OverrideBaseOptions();
  auto level = verbose_ ? InfoLogLevel::INFO_LEVEL : InfoLogLevel::WARN_LEVEL;
  options_.info_log.reset(new StderrLogger(level));
}

void RepairCommand::DoCommand() {
  PrepareOptions();
  Status status = RepairDB(db_path_, options_);
  if (status.ok()) {
    fprintf(stdout, "OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(status.ToString());
  }
}

// ----------------------------------------------------------------------------

const std::string BackupEngineCommand::ARG_NUM_THREADS = "num_threads";
const std::string BackupEngineCommand::ARG_BACKUP_ENV_URI = "backup_env_uri";
const std::string BackupEngineCommand::ARG_BACKUP_FS_URI = "backup_fs_uri";
const std::string BackupEngineCommand::ARG_BACKUP_DIR = "backup_dir";
const std::string BackupEngineCommand::ARG_STDERR_LOG_LEVEL =
    "stderr_log_level";

BackupEngineCommand::BackupEngineCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, false /* is_read_only */,
                 BuildCmdLineOptions({ARG_BACKUP_ENV_URI, ARG_BACKUP_FS_URI,
                                      ARG_BACKUP_DIR, ARG_NUM_THREADS,
                                      ARG_STDERR_LOG_LEVEL})),
      num_threads_(1) {
  auto itr = options.find(ARG_NUM_THREADS);
  if (itr != options.end()) {
    num_threads_ = std::stoi(itr->second);
  }
  itr = options.find(ARG_BACKUP_ENV_URI);
  if (itr != options.end()) {
    backup_env_uri_ = itr->second;
  }
  itr = options.find(ARG_BACKUP_FS_URI);
  if (itr != options.end()) {
    backup_fs_uri_ = itr->second;
  }
  if (!backup_env_uri_.empty() && !backup_fs_uri_.empty()) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "you may not specity both --" + ARG_BACKUP_ENV_URI + " and --" +
        ARG_BACKUP_FS_URI);
  }
  itr = options.find(ARG_BACKUP_DIR);
  if (itr == options.end()) {
    exec_state_ = LDBCommandExecuteResult::Failed("--" + ARG_BACKUP_DIR +
                                                  ": missing backup directory");
  } else {
    backup_dir_ = itr->second;
  }

  itr = options.find(ARG_STDERR_LOG_LEVEL);
  if (itr != options.end()) {
    int stderr_log_level = std::stoi(itr->second);
    if (stderr_log_level < 0 ||
        stderr_log_level >= InfoLogLevel::NUM_INFO_LOG_LEVELS) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          ARG_STDERR_LOG_LEVEL + " must be >= 0 and < " +
          std::to_string(InfoLogLevel::NUM_INFO_LOG_LEVELS) + ".");
    } else {
      logger_.reset(
          new StderrLogger(static_cast<InfoLogLevel>(stderr_log_level)));
    }
  }
}

void BackupEngineCommand::Help(const std::string& name, std::string& ret) {
  ret.append("  ");
  ret.append(name);
  ret.append(" [--" + ARG_BACKUP_ENV_URI + " | --" + ARG_BACKUP_FS_URI + "] ");
  ret.append(" [--" + ARG_BACKUP_DIR + "] ");
  ret.append(" [--" + ARG_NUM_THREADS + "] ");
  ret.append(" [--" + ARG_STDERR_LOG_LEVEL + "=<int (InfoLogLevel)>] ");
  ret.append("\n");
}

// ----------------------------------------------------------------------------

BackupCommand::BackupCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : BackupEngineCommand(params, options, flags) {}

void BackupCommand::Help(std::string& ret) {
  BackupEngineCommand::Help(Name(), ret);
}

void BackupCommand::DoCommand() {
  BackupEngine* backup_engine;
  Status status;
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  fprintf(stdout, "open db OK\n");

  Env* custom_env = backup_env_guard_.get();
  if (custom_env == nullptr) {
    Status s =
        Env::CreateFromUri(config_options_, backup_env_uri_, backup_fs_uri_,
                           &custom_env, &backup_env_guard_);
    if (!s.ok()) {
      exec_state_ = LDBCommandExecuteResult::Failed(s.ToString());
      return;
    }
  }
  assert(custom_env != nullptr);

  BackupEngineOptions backup_options =
      BackupEngineOptions(backup_dir_, custom_env);
  backup_options.info_log = logger_.get();
  backup_options.max_background_operations = num_threads_;
  status = BackupEngine::Open(options_.env, backup_options, &backup_engine);
  if (status.ok()) {
    fprintf(stdout, "open backup engine OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(status.ToString());
    return;
  }
  status = backup_engine->CreateNewBackup(db_.get());
  if (status.ok()) {
    fprintf(stdout, "create new backup OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(status.ToString());
    return;
  }
}

// ----------------------------------------------------------------------------

RestoreCommand::RestoreCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : BackupEngineCommand(params, options, flags) {}

void RestoreCommand::Help(std::string& ret) {
  BackupEngineCommand::Help(Name(), ret);
}

void RestoreCommand::DoCommand() {
  Env* custom_env = backup_env_guard_.get();
  if (custom_env == nullptr) {
    Status s =
        Env::CreateFromUri(config_options_, backup_env_uri_, backup_fs_uri_,
                           &custom_env, &backup_env_guard_);
    if (!s.ok()) {
      exec_state_ = LDBCommandExecuteResult::Failed(s.ToString());
      return;
    }
  }
  assert(custom_env != nullptr);

  std::unique_ptr<BackupEngineReadOnly> restore_engine;
  Status status;
  {
    BackupEngineOptions opts(backup_dir_, custom_env);
    opts.info_log = logger_.get();
    opts.max_background_operations = num_threads_;
    BackupEngineReadOnly* raw_restore_engine_ptr;
    status =
        BackupEngineReadOnly::Open(options_.env, opts, &raw_restore_engine_ptr);
    if (status.ok()) {
      restore_engine.reset(raw_restore_engine_ptr);
    }
  }
  if (status.ok()) {
    fprintf(stdout, "open restore engine OK\n");
    status = restore_engine->RestoreDBFromLatestBackup(db_path_, db_path_);
  }
  if (status.ok()) {
    fprintf(stdout, "restore from backup OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(status.ToString());
  }
}

// ----------------------------------------------------------------------------

namespace {

void DumpSstFile(Options options, std::string filename, bool output_hex,
                 bool show_properties, bool decode_blob_index,
                 std::string from_key, std::string to_key) {
  if (filename.length() <= 4 ||
      filename.rfind(".sst") != filename.length() - 4) {
    std::cout << "Invalid sst file name." << std::endl;
    return;
  }
  // no verification
  ROCKSDB_NAMESPACE::SstFileDumper dumper(
      options, filename, Temperature::kUnknown,
      2 * 1024 * 1024 /* readahead_size */,
      /* verify_checksum */ false, output_hex, decode_blob_index);
  Status st = dumper.ReadSequential(true, std::numeric_limits<uint64_t>::max(),
                                    !from_key.empty(), from_key,
                                    !to_key.empty(), to_key);
  if (!st.ok()) {
    std::cerr << "Error in reading SST file " << filename << st.ToString()
              << std::endl;
    return;
  }

  if (show_properties) {
    const ROCKSDB_NAMESPACE::TableProperties* table_properties;

    std::shared_ptr<const ROCKSDB_NAMESPACE::TableProperties>
        table_properties_from_reader;
    st = dumper.ReadTableProperties(&table_properties_from_reader);
    if (!st.ok()) {
      std::cerr << filename << ": " << st.ToString()
                << ". Try to use initial table properties" << std::endl;
      table_properties = dumper.GetInitTableProperties();
    } else {
      table_properties = table_properties_from_reader.get();
    }
    if (table_properties != nullptr) {
      std::cout << std::endl << "Table Properties:" << std::endl;
      std::cout << table_properties->ToString("\n") << std::endl;
    }
  }
}

void DumpBlobFile(const std::string& filename, bool is_key_hex,
                  bool is_value_hex, bool dump_uncompressed_blobs) {
  using ROCKSDB_NAMESPACE::blob_db::BlobDumpTool;
  BlobDumpTool tool;
  BlobDumpTool::DisplayType blob_type = is_value_hex
                                            ? BlobDumpTool::DisplayType::kHex
                                            : BlobDumpTool::DisplayType::kRaw;
  BlobDumpTool::DisplayType show_uncompressed_blob =
      dump_uncompressed_blobs ? blob_type : BlobDumpTool::DisplayType::kNone;
  BlobDumpTool::DisplayType show_blob =
      dump_uncompressed_blobs ? BlobDumpTool::DisplayType::kNone : blob_type;
  BlobDumpTool::DisplayType show_key = is_key_hex
                                           ? BlobDumpTool::DisplayType::kHex
                                           : BlobDumpTool::DisplayType::kRaw;
  Status s = tool.Run(filename, show_key, show_blob, show_uncompressed_blob,
                      /* show_summary */ true);
  if (!s.ok()) {
    fprintf(stderr, "Failed: %s\n", s.ToString().c_str());
  }
}

Status EncodeUserProvidedTimestamp(const std::string& user_timestamp,
                                   std::string* ts_buf) {
  uint64_t int_timestamp;
  std::istringstream iss(user_timestamp);
  if (!(iss >> int_timestamp)) {
    return Status::InvalidArgument(
        "user provided timestamp is not a valid uint64 value.");
  }
  EncodeU64Ts(int_timestamp, ts_buf);
  return Status::OK();
}
}  // namespace

DBFileDumperCommand::DBFileDumperCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, true /* is_read_only */,
                 BuildCmdLineOptions(
                     {ARG_DECODE_BLOB_INDEX, ARG_DUMP_UNCOMPRESSED_BLOBS})),
      decode_blob_index_(IsFlagPresent(flags, ARG_DECODE_BLOB_INDEX)),
      dump_uncompressed_blobs_(
          IsFlagPresent(flags, ARG_DUMP_UNCOMPRESSED_BLOBS)) {}

void DBFileDumperCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(DBFileDumperCommand::Name());
  ret.append(" [--" + ARG_DECODE_BLOB_INDEX + "]");
  ret.append(" [--" + ARG_DUMP_UNCOMPRESSED_BLOBS + "]");
  ret.append("\n");
}

void DBFileDumperCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  Status s;

  // TODO: Use --hex, --key_hex, --value_hex flags consistently for
  // dumping manifest file, sst files and blob files.
  std::cout << "Manifest File" << std::endl;
  std::cout << "==============================" << std::endl;
  std::string manifest_filename;
  s = ReadFileToString(db_->GetEnv(), CurrentFileName(db_->GetName()),
                       &manifest_filename);
  if (!s.ok() || manifest_filename.empty() ||
      manifest_filename.back() != '\n') {
    std::cerr << "Error when reading CURRENT file "
              << CurrentFileName(db_->GetName()) << std::endl;
  }
  // remove the trailing '\n'
  manifest_filename.resize(manifest_filename.size() - 1);
  std::string manifest_filepath = db_->GetName() + "/" + manifest_filename;
  // Correct concatenation of filepath and filename:
  // Check that there is no double slashes (or more!) when concatenation
  // happens.
  manifest_filepath = NormalizePath(manifest_filepath);

  std::cout << manifest_filepath << std::endl;
  DumpManifestFile(options_, manifest_filepath, false, false, false,
                   column_families_);
  std::cout << std::endl;

  std::vector<ColumnFamilyMetaData> column_families;
  db_->GetAllColumnFamilyMetaData(&column_families);
  for (const auto& column_family : column_families) {
    std::cout << "Column family name: " << column_family.name << std::endl;
    std::cout << "==============================" << std::endl;
    std::cout << std::endl;
    std::cout << "SST Files" << std::endl;
    std::cout << "==============================" << std::endl;
    for (const LevelMetaData& level : column_family.levels) {
      for (const SstFileMetaData& sst_file : level.files) {
        std::string filename = sst_file.db_path + "/" + sst_file.name;
        // Correct concatenation of filepath and filename:
        // Check that there is no double slashes (or more!) when concatenation
        // happens.
        filename = NormalizePath(filename);
        std::cout << filename << " level:" << level.level << std::endl;
        std::cout << "------------------------------" << std::endl;
        DumpSstFile(options_, filename, false, true, decode_blob_index_);
        std::cout << std::endl;
      }
    }
    std::cout << "Blob Files" << std::endl;
    std::cout << "==============================" << std::endl;
    for (const BlobMetaData& blob_file : column_family.blob_files) {
      std::string filename =
          blob_file.blob_file_path + "/" + blob_file.blob_file_name;
      // Correct concatenation of filepath and filename:
      // Check that there is no double slashes (or more!) when concatenation
      // happens.
      filename = NormalizePath(filename);
      std::cout << filename << std::endl;
      std::cout << "------------------------------" << std::endl;
      DumpBlobFile(filename, /* is_key_hex */ false, /* is_value_hex */ false,
                   dump_uncompressed_blobs_);
      std::cout << std::endl;
    }
  }
  std::cout << std::endl;

  std::cout << "Write Ahead Log Files" << std::endl;
  std::cout << "==============================" << std::endl;
  ROCKSDB_NAMESPACE::VectorWalPtr wal_files;
  s = db_->GetSortedWalFiles(wal_files);
  if (!s.ok()) {
    std::cerr << "Error when getting WAL files" << std::endl;
  } else {
    std::string wal_dir;
    if (options_.wal_dir.empty()) {
      wal_dir = db_->GetName();
    } else {
      wal_dir = NormalizePath(options_.wal_dir + "/");
    }
    std::optional<SequenceNumber> prev_batch_seqno;
    std::optional<uint32_t> prev_batch_count;
    for (auto& wal : wal_files) {
      // TODO(qyang): option.wal_dir should be passed into ldb command
      std::string filename = wal_dir + wal->PathName();
      std::cout << filename << std::endl;
      // TODO(myabandeh): allow configuring is_write_commited
      DumpWalFile(
          options_, filename, true /* print_header */, true /* print_values */,
          false /* only_print_seqno_gapstrue */, true /* is_write_commited */,
          ucmps_, &exec_state_, &prev_batch_seqno, &prev_batch_count);
    }
  }
}

const std::string DBLiveFilesMetadataDumperCommand::ARG_SORT_BY_FILENAME =
    "sort_by_filename";

DBLiveFilesMetadataDumperCommand::DBLiveFilesMetadataDumperCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, true /* is_read_only */,
                 BuildCmdLineOptions({ARG_SORT_BY_FILENAME})) {
  sort_by_filename_ = IsFlagPresent(flags, ARG_SORT_BY_FILENAME);
}

void DBLiveFilesMetadataDumperCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(DBLiveFilesMetadataDumperCommand::Name());
  ret.append(" [--" + ARG_SORT_BY_FILENAME + "] ");
  ret.append("\n");
}

void DBLiveFilesMetadataDumperCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  std::vector<ColumnFamilyMetaData> metadata;
  db_->GetAllColumnFamilyMetaData(&metadata);
  if (sort_by_filename_) {
    std::cout << "Live SST and Blob Files:" << std::endl;
    // tuple of <file path, level, column family name>
    std::vector<std::tuple<std::string, int, std::string>> all_files;

    for (const auto& column_metadata : metadata) {
      // Iterate Levels
      const auto& levels = column_metadata.levels;
      const std::string& cf = column_metadata.name;
      for (const auto& level_metadata : levels) {
        // Iterate SST files
        const auto& sst_files = level_metadata.files;
        int level = level_metadata.level;
        for (const auto& sst_metadata : sst_files) {
          // The SstFileMetaData.name always starts with "/",
          // however SstFileMetaData.db_path is the string provided by
          // the user as an input. Therefore we check if we can
          // concantenate the two strings directly or if we need to
          // drop a possible extra "/" at the end of SstFileMetaData.db_path.
          std::string filename =
              NormalizePath(sst_metadata.db_path + "/" + sst_metadata.name);
          all_files.emplace_back(filename, level, cf);
        }  // End of for-loop over sst files
      }  // End of for-loop over levels

      const auto& blob_files = column_metadata.blob_files;
      for (const auto& blob_metadata : blob_files) {
        // The BlobMetaData.blob_file_name always starts with "/",
        // however BlobMetaData.blob_file_path is the string provided by
        // the user as an input. Therefore we check if we can
        // concantenate the two strings directly or if we need to
        // drop a possible extra "/" at the end of BlobMetaData.blob_file_path.
        std::string filename = NormalizePath(
            blob_metadata.blob_file_path + "/" + blob_metadata.blob_file_name);
        // Level for blob files is encoded as -1
        all_files.emplace_back(filename, -1, cf);
      }  // End of for-loop over blob files
    }  // End of for-loop over column metadata

    // Sort by filename (i.e. first entry in tuple)
    std::sort(all_files.begin(), all_files.end());

    for (const auto& item : all_files) {
      const std::string& filename = std::get<0>(item);
      int level = std::get<1>(item);
      const std::string& cf = std::get<2>(item);
      if (level == -1) {  // Blob File
        std::cout << filename << ", column family '" << cf << "'" << std::endl;
      } else {  // SST file
        std::cout << filename << " : level " << level << ", column family '"
                  << cf << "'" << std::endl;
      }
    }
  } else {
    for (const auto& column_metadata : metadata) {
      std::cout << "===== Column Family: " << column_metadata.name
                << " =====" << std::endl;

      std::cout << "Live SST Files:" << std::endl;
      // Iterate levels
      const auto& levels = column_metadata.levels;
      for (const auto& level_metadata : levels) {
        std::cout << "---------- level " << level_metadata.level
                  << " ----------" << std::endl;
        // Iterate SST files
        const auto& sst_files = level_metadata.files;
        for (const auto& sst_metadata : sst_files) {
          // The SstFileMetaData.name always starts with "/",
          // however SstFileMetaData.db_path is the string provided by
          // the user as an input. Therefore we check if we can
          // concantenate the two strings directly or if we need to
          // drop a possible extra "/" at the end of SstFileMetaData.db_path.
          std::string filename =
              NormalizePath(sst_metadata.db_path + "/" + sst_metadata.name);
          std::cout << filename << std::endl;
        }  // End of for-loop over sst files
      }  // End of for-loop over levels

      std::cout << "Live Blob Files:" << std::endl;
      const auto& blob_files = column_metadata.blob_files;
      for (const auto& blob_metadata : blob_files) {
        // The BlobMetaData.blob_file_name always starts with "/",
        // however BlobMetaData.blob_file_path is the string provided by
        // the user as an input. Therefore we check if we can
        // concantenate the two strings directly or if we need to
        // drop a possible extra "/" at the end of BlobMetaData.blob_file_path.
        std::string filename = NormalizePath(
            blob_metadata.blob_file_path + "/" + blob_metadata.blob_file_name);
        std::cout << filename << std::endl;
      }  // End of for-loop over blob files
    }  // End of for-loop over column metadata
  }  // End of else ("not sort_by_filename")
  std::cout << "------------------------------" << std::endl;
}

void WriteExternalSstFilesCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(WriteExternalSstFilesCommand::Name());
  ret.append(" <output_sst_path>");
  ret.append("\n");
}

WriteExternalSstFilesCommand::WriteExternalSstFilesCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(
          options, flags, false /* is_read_only */,
          BuildCmdLineOptions({ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX, ARG_FROM,
                               ARG_TO, ARG_CREATE_IF_MISSING})) {
  create_if_missing_ =
      IsFlagPresent(flags, ARG_CREATE_IF_MISSING) ||
      ParseBooleanOption(options, ARG_CREATE_IF_MISSING, false);
  if (params.size() != 1) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "output SST file path must be specified");
  } else {
    output_sst_path_ = params.at(0);
  }
}

void WriteExternalSstFilesCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  ColumnFamilyHandle* cfh = GetCfHandle();
  SstFileWriter sst_file_writer(EnvOptions(), db_->GetOptions(), cfh);
  Status status = sst_file_writer.Open(output_sst_path_);
  if (!status.ok()) {
    exec_state_ = LDBCommandExecuteResult::Failed("failed to open SST file: " +
                                                  status.ToString());
    return;
  }

  int bad_lines = 0;
  std::string line;
  std::ifstream ifs_stdin("/dev/stdin");
  std::istream* istream_p = ifs_stdin.is_open() ? &ifs_stdin : &std::cin;
  while (getline(*istream_p, line, '\n')) {
    std::string key;
    std::string value;
    if (ParseKeyValue(line, &key, &value, is_key_hex_, is_value_hex_)) {
      status = sst_file_writer.Put(key, value);
      if (!status.ok()) {
        exec_state_ = LDBCommandExecuteResult::Failed(
            "failed to write record to file: " + status.ToString());
        return;
      }
    } else if (0 == line.find("Keys in range:")) {
      // ignore this line
    } else if (0 == line.find("Created bg thread 0x")) {
      // ignore this line
    } else {
      bad_lines++;
    }
  }

  status = sst_file_writer.Finish();
  if (!status.ok()) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "Failed to finish writing to file: " + status.ToString());
    return;
  }

  if (bad_lines > 0) {
    fprintf(stderr, "Warning: %d bad lines ignored.\n", bad_lines);
  }
  exec_state_ = LDBCommandExecuteResult::Succeed(
      "external SST file written to " + output_sst_path_);
}

void WriteExternalSstFilesCommand::OverrideBaseOptions() {
  LDBCommand::OverrideBaseOptions();
  options_.create_if_missing = create_if_missing_;
}

const std::string IngestExternalSstFilesCommand::ARG_MOVE_FILES = "move_files";
const std::string IngestExternalSstFilesCommand::ARG_SNAPSHOT_CONSISTENCY =
    "snapshot_consistency";
const std::string IngestExternalSstFilesCommand::ARG_ALLOW_GLOBAL_SEQNO =
    "allow_global_seqno";
const std::string IngestExternalSstFilesCommand::ARG_ALLOW_BLOCKING_FLUSH =
    "allow_blocking_flush";
const std::string IngestExternalSstFilesCommand::ARG_INGEST_BEHIND =
    "ingest_behind";

void IngestExternalSstFilesCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(IngestExternalSstFilesCommand::Name());
  ret.append(" <input_sst_path>");
  ret.append(" [--" + ARG_MOVE_FILES + "] ");
  ret.append(" [--" + ARG_SNAPSHOT_CONSISTENCY + "] ");
  ret.append(" [--" + ARG_ALLOW_GLOBAL_SEQNO + "] ");
  ret.append(" [--" + ARG_ALLOW_BLOCKING_FLUSH + "] ");
  ret.append(" [--" + ARG_INGEST_BEHIND + "] ");
  ret.append("\n");
}

IngestExternalSstFilesCommand::IngestExternalSstFilesCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(
          options, flags, false /* is_read_only */,
          BuildCmdLineOptions({ARG_MOVE_FILES, ARG_SNAPSHOT_CONSISTENCY,
                               ARG_ALLOW_GLOBAL_SEQNO, ARG_CREATE_IF_MISSING,
                               ARG_ALLOW_BLOCKING_FLUSH, ARG_INGEST_BEHIND})),
      move_files_(false),
      snapshot_consistency_(true),
      allow_global_seqno_(true),
      allow_blocking_flush_(true),
      ingest_behind_(false) {
  create_if_missing_ =
      IsFlagPresent(flags, ARG_CREATE_IF_MISSING) ||
      ParseBooleanOption(options, ARG_CREATE_IF_MISSING, false);
  move_files_ = IsFlagPresent(flags, ARG_MOVE_FILES) ||
                ParseBooleanOption(options, ARG_MOVE_FILES, false);
  snapshot_consistency_ =
      IsFlagPresent(flags, ARG_SNAPSHOT_CONSISTENCY) ||
      ParseBooleanOption(options, ARG_SNAPSHOT_CONSISTENCY, true);
  allow_global_seqno_ =
      IsFlagPresent(flags, ARG_ALLOW_GLOBAL_SEQNO) ||
      ParseBooleanOption(options, ARG_ALLOW_GLOBAL_SEQNO, true);
  allow_blocking_flush_ =
      IsFlagPresent(flags, ARG_ALLOW_BLOCKING_FLUSH) ||
      ParseBooleanOption(options, ARG_ALLOW_BLOCKING_FLUSH, true);
  ingest_behind_ = IsFlagPresent(flags, ARG_INGEST_BEHIND) ||
                   ParseBooleanOption(options, ARG_INGEST_BEHIND, false);

  if (params.size() != 1) {
    exec_state_ =
        LDBCommandExecuteResult::Failed("input SST path must be specified");
  } else {
    input_sst_path_ = params.at(0);
  }
}

void IngestExternalSstFilesCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  if (GetExecuteState().IsFailed()) {
    return;
  }
  ColumnFamilyHandle* cfh = GetCfHandle();
  IngestExternalFileOptions ifo;
  ifo.move_files = move_files_;
  ifo.snapshot_consistency = snapshot_consistency_;
  ifo.allow_global_seqno = allow_global_seqno_;
  ifo.allow_blocking_flush = allow_blocking_flush_;
  ifo.ingest_behind = ingest_behind_;
  Status status = db_->IngestExternalFile(cfh, {input_sst_path_}, ifo);
  if (!status.ok()) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "failed to ingest external SST: " + status.ToString());
  } else {
    exec_state_ =
        LDBCommandExecuteResult::Succeed("external SST files ingested");
  }
}

void IngestExternalSstFilesCommand::OverrideBaseOptions() {
  LDBCommand::OverrideBaseOptions();
  options_.create_if_missing = create_if_missing_;
}

ListFileRangeDeletesCommand::ListFileRangeDeletesCommand(
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, true /* is_read_only */,
                 BuildCmdLineOptions({ARG_MAX_KEYS})) {
  auto itr = options.find(ARG_MAX_KEYS);
  if (itr != options.end()) {
    try {
#if defined(CYGWIN)
      max_keys_ = strtol(itr->second.c_str(), 0, 10);
#else
      max_keys_ = std::stoi(itr->second);
#endif
    } catch (const std::invalid_argument&) {
      exec_state_ = LDBCommandExecuteResult::Failed(ARG_MAX_KEYS +
                                                    " has an invalid value");
    } catch (const std::out_of_range&) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          ARG_MAX_KEYS + " has a value out-of-range");
    }
  }
}

void ListFileRangeDeletesCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(ListFileRangeDeletesCommand::Name());
  ret.append(" [--" + ARG_MAX_KEYS + "=<N>]");
  ret.append(" : print tombstones in SST files.\n");
}

void ListFileRangeDeletesCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }

  DBImpl* db_impl = static_cast_with_check<DBImpl>(db_->GetRootDB());

  std::string out_str;

  Status st =
      db_impl->TablesRangeTombstoneSummary(GetCfHandle(), max_keys_, &out_str);
  if (st.ok()) {
    TEST_SYNC_POINT_CALLBACK(
        "ListFileRangeDeletesCommand::DoCommand:BeforePrint", &out_str);
    fprintf(stdout, "%s\n", out_str.c_str());
  }
}

void UnsafeRemoveSstFileCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(UnsafeRemoveSstFileCommand::Name());
  ret.append(" <SST file number>");
  ret.append("  ");
  ret.append("    MUST NOT be used on a live DB.");
  ret.append("\n");
}

UnsafeRemoveSstFileCommand::UnsafeRemoveSstFileCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, false /* is_read_only */,
                 BuildCmdLineOptions({})) {
  if (params.size() != 1) {
    exec_state_ =
        LDBCommandExecuteResult::Failed("SST file number must be specified");
  } else {
    char* endptr = nullptr;
    sst_file_number_ = strtoull(params.at(0).c_str(), &endptr, 10 /* base */);
    if (endptr == nullptr || *endptr != '\0') {
      exec_state_ = LDBCommandExecuteResult::Failed(
          "Failed to parse SST file number " + params.at(0));
    }
  }
}

void UnsafeRemoveSstFileCommand::DoCommand() {
  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;
  const WriteOptions write_options;

  PrepareOptions();

  OfflineManifestWriter w(options_, db_path_);
  if (column_families_.empty()) {
    column_families_.emplace_back(kDefaultColumnFamilyName, options_);
  }
  Status s = w.Recover(column_families_);

  ColumnFamilyData* cfd = nullptr;
  int level = -1;
  if (s.ok()) {
    FileMetaData* metadata = nullptr;
    s = w.Versions().GetMetadataForFile(sst_file_number_, &level, &metadata,
                                        &cfd);
  }

  if (s.ok()) {
    VersionEdit edit;
    edit.SetColumnFamily(cfd->GetID());
    edit.DeleteFile(level, sst_file_number_);
    std::unique_ptr<FSDirectory> db_dir;
    s = options_.env->GetFileSystem()->NewDirectory(db_path_, IOOptions(),
                                                    &db_dir, nullptr);
    if (s.ok()) {
      s = w.LogAndApply(read_options, write_options, cfd, &edit, db_dir.get());
    }
  }

  if (!s.ok()) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "failed to unsafely remove SST file: " + s.ToString());
  } else {
    exec_state_ = LDBCommandExecuteResult::Succeed("unsafely removed SST file");
  }
}

const std::string UpdateManifestCommand::ARG_VERBOSE = "verbose";
const std::string UpdateManifestCommand::ARG_UPDATE_TEMPERATURES =
    "update_temperatures";
const std::string UpdateManifestCommand::ARG_REBIND_OPTIONS_FILE =  // NOLINT
    "rebind_options_file";
const std::string UpdateManifestCommand::ARG_OPTIONS_FILE =  // NOLINT
    "options_file";
const std::string UpdateManifestCommand::ARG_FILE_NUMBER_PATHS =  // NOLINT
    "file_number_paths";

void UpdateManifestCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(UpdateManifestCommand::Name());
  ret.append(" [--update_temperatures]");
  ret.append(" [--rebind_options_file]");
  ret.append(" [--options_file=OPTIONS-<number>]");
  ret.append(" [--file_number_paths=<path1>,<path2>,...]");
  ret.append("  ");
  ret.append("    MUST NOT be used on a live DB.");
  ret.append("\n");
}

UpdateManifestCommand::UpdateManifestCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, false /* is_read_only */,
                 BuildCmdLineOptions({ARG_VERBOSE, ARG_UPDATE_TEMPERATURES,
                                      ARG_REBIND_OPTIONS_FILE, ARG_OPTIONS_FILE,
                                      ARG_FILE_NUMBER_PATHS})) {
  verbose_ = IsFlagPresent(flags, ARG_VERBOSE) ||
             ParseBooleanOption(options, ARG_VERBOSE, false);
  update_temperatures_ =
      IsFlagPresent(flags, ARG_UPDATE_TEMPERATURES) ||
      ParseBooleanOption(options, ARG_UPDATE_TEMPERATURES, false);
  rebind_options_file_ =
      IsFlagPresent(flags, ARG_REBIND_OPTIONS_FILE) ||
      ParseBooleanOption(options, ARG_REBIND_OPTIONS_FILE, false);
  auto options_file_iter = options.find(ARG_OPTIONS_FILE);
  if (options_file_iter != options.end()) {
    options_file_ = options_file_iter->second;
  }
  auto file_number_paths_iter = options.find(ARG_FILE_NUMBER_PATHS);
  if (file_number_paths_iter != options.end()) {
    file_number_paths_ = StringSplit(file_number_paths_iter->second, ',');
  }

  if (rebind_options_file_) {
    // The repair must be able to inspect an explicitly selected OPTIONS file
    // even when the currently bound file is missing or corrupt.
    try_load_options_ = false;
  }
  if (!update_temperatures_ && !rebind_options_file_) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "No action like --update_temperatures or --rebind_options_file "
        "specified for update_manifest");
  } else if (rebind_options_file_ && options_file_.empty()) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "--rebind_options_file requires an explicit trusted "
        "--options_file=OPTIONS-<number>");
  } else if (!rebind_options_file_ && !options_file_.empty()) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "--options_file requires --rebind_options_file");
  } else if (!rebind_options_file_ && !file_number_paths_.empty()) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "--file_number_paths requires --rebind_options_file");
  } else if (std::any_of(
                 file_number_paths_.begin(), file_number_paths_.end(),
                 [](const std::string& path) { return path.empty(); })) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "--file_number_paths cannot contain an empty path");
  } else if (rebind_options_file_ && update_temperatures_) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "--rebind_options_file and --update_temperatures cannot be combined");
  }
}

namespace {
Status RebindManifestToSelectedOptions(
    const Options& base_options, const std::string& db_path,
    const std::string& selected_options_name,
    const std::vector<std::string>& file_number_paths) {
  assert(base_options.env != nullptr);
  Env& env = *base_options.env;
  const auto& fs = env.GetFileSystem();
  FileLock* db_lock = nullptr;
  Status s = env.LockFile(LockFileName(db_path), &db_lock);
  if (!s.ok()) {
    return s;
  }
  assert(db_lock != nullptr);
  Defer unlock_db([&]() { env.UnlockFile(db_lock).PermitUncheckedError(); });

  OptionsFileManifestState recovered_manifest_state;
  uint64_t manifest_next_file_number = 0;
  uint64_t manifest_append_boundary = 0;
  s = VersionSet::GetOptionsFileManifestState(
      db_path, fs.get(), &recovered_manifest_state, &manifest_next_file_number,
      &manifest_append_boundary);
  if (!s.ok()) {
    return s;
  }
  if (manifest_append_boundary == 0) {
    return Status::Corruption("MANIFEST has no complete record to repair");
  }

  [[maybe_unused]] uint64_t selected_options_number = 0;
  FileType selected_type;
  if (selected_options_name.find_first_of("/\\") != std::string::npos ||
      !ParseFileName(selected_options_name, &selected_options_number,
                     &selected_type) ||
      selected_type != kOptionsFile) {
    return Status::InvalidArgument(
        "--options_file must be a basename in OPTIONS-<number> format");
  }

  ConfigOptions config_options;
  config_options.env = &env;
  config_options.ignore_unknown_options = true;
  DBOptions loaded_db_options;
  std::vector<ColumnFamilyDescriptor> loaded_cfs;
  s = LoadOptionsFromFile(config_options, db_path + "/" + selected_options_name,
                          &loaded_db_options, &loaded_cfs);
  if (!s.ok()) {
    return s;
  }
  if (loaded_cfs.empty()) {
    return Status::Corruption("OPTIONS file has no column families");
  }
  if (!loaded_db_options.track_options_file_number_in_manifest) {
    return Status::InvalidArgument(
        "Selected OPTIONS file does not enable MANIFEST tracking");
  }

  // OPTIONS does not persist DBOptions::db_paths or
  // ColumnFamilyOptions::cf_paths. Callers using either must supply those
  // roots through --file_number_paths. wal_dir is persisted and is included
  // automatically. Failing closed on an unavailable root prevents the repair
  // from allocating a number that is already in use outside the DB directory.
  const std::string wal_dir =
      loaded_db_options.wal_dir.empty() ? db_path : loaded_db_options.wal_dir;
  std::set<std::string> allocation_roots = {db_path, wal_dir};
  for (const DbPath& db_root : loaded_db_options.db_paths) {
    if (!db_root.path.empty()) {
      allocation_roots.insert(db_root.path);
    }
  }
  for (const ColumnFamilyDescriptor& cf : loaded_cfs) {
    for (const DbPath& cf_root : cf.options.cf_paths) {
      if (!cf_root.path.empty()) {
        allocation_roots.insert(cf_root.path);
      }
    }
  }
  allocation_roots.insert(file_number_paths.begin(), file_number_paths.end());
  uint64_t highest_file_number =
      recovered_manifest_state.has_committed_options_file_number
          ? recovered_manifest_state.committed_options_file_number
          : 0;
  auto scan_file_numbers = [&](const std::string& path,
                               bool allow_missing) -> Status {
    std::vector<std::string> children;
    Status scan_s = env.GetChildren(path, &children);
    if (allow_missing && (scan_s.IsNotFound() || scan_s.IsPathNotFound())) {
      return Status::OK();
    }
    if (!scan_s.ok()) {
      return scan_s;
    }
    for (const auto& child : children) {
      uint64_t number = 0;
      FileType type;
      if (ParseFileName(child, &number, &type)) {
        highest_file_number = std::max(highest_file_number, number);
      }
    }
    return Status::OK();
  };
  for (const std::string& path : allocation_roots) {
    s = scan_file_numbers(path, /*allow_missing=*/false);
    if (!s.ok()) {
      return s;
    }
  }
  s = scan_file_numbers(ArchivalDirectory(wal_dir), /*allow_missing=*/true);
  if (!s.ok()) {
    return s;
  }
  if (highest_file_number >= kFileNumberMask ||
      manifest_next_file_number > kFileNumberMask) {
    return Status::InvalidArgument("File number space exhausted");
  }

  std::vector<std::string> manifest_cfs;
  s = VersionSet::ListColumnFamilies(&manifest_cfs, db_path, fs.get());
  if (!s.ok()) {
    return s;
  }
  std::vector<std::string> options_cfs;
  options_cfs.reserve(loaded_cfs.size());
  for (const auto& cf : loaded_cfs) {
    options_cfs.push_back(cf.name);
  }
  std::sort(manifest_cfs.begin(), manifest_cfs.end());
  std::sort(options_cfs.begin(), options_cfs.end());
  if (!std::includes(options_cfs.begin(), options_cfs.end(),
                     manifest_cfs.begin(), manifest_cfs.end())) {
    return Status::InvalidArgument(
        "Selected OPTIONS file is missing a MANIFEST column family");
  }

  // Bound recovery-created data files from the WAL contents. Every recovery
  // L0 output contains at least one WriteBatch entry, and each blob-file
  // output contains at least one blob entry, so twice the total entry count is
  // a conservative upper bound for both kinds together. Count all live and
  // archived WALs; including obsolete records only makes the repair safer.
  struct WalCountReporter : public log::Reader::Reporter {
    Status status;
    void Corruption(size_t /*bytes*/, const Status& corruption,
                    uint64_t /*log_number*/ = kMaxSequenceNumber) override {
      status.UpdateIfOk(corruption);
    }
  };
  uint64_t wal_entry_count = 0;
  for (const std::string& dir : {wal_dir, ArchivalDirectory(wal_dir)}) {
    std::vector<std::string> wal_children;
    s = env.GetChildren(dir, &wal_children);
    if (s.IsNotFound() || s.IsPathNotFound()) {
      s = Status::OK();
      continue;
    }
    if (!s.ok()) {
      return s;
    }
    for (const auto& child : wal_children) {
      uint64_t wal_number = 0;
      FileType type;
      if (!ParseFileName(child, &wal_number, &type) || type != kWalFile) {
        continue;
      }
      std::string wal_path = dir;
      wal_path.push_back('/');
      wal_path.append(child);
      std::unique_ptr<SequentialFileReader> wal_file_reader;
      s = SequentialFileReader::Create(
          fs, wal_path, FileOptions(), &wal_file_reader,
          /*dbg=*/nullptr, /*rate_limiter=*/nullptr);
      if (!s.ok()) {
        return s;
      }
      WalCountReporter reporter;
      log::Reader reader(nullptr, std::move(wal_file_reader), &reporter,
                         /*checksum=*/true, wal_number);
      Slice record;
      std::string scratch;
      WriteBatch batch;
      while (reader.ReadRecord(&record, &scratch)) {
        if (record.size() < WriteBatchInternal::kHeader) {
          return Status::Corruption("WAL record too small to bound recovery");
        }
        s = WriteBatchInternal::SetContents(&batch, record);
        if (!s.ok()) {
          return s;
        }
        const uint64_t count = WriteBatchInternal::Count(&batch);
        if (count > kFileNumberMask - wal_entry_count) {
          return Status::InvalidArgument("File number space exhausted");
        }
        wal_entry_count += count;
      }
      if (!reporter.status.ok()) {
        return reporter.status;
      }
    }
  }

  const uint64_t new_options_number =
      std::max(highest_file_number + 1, manifest_next_file_number);
  // Repair cannot know the DBOptions or column-family descriptors that the
  // next writable opener will supply. In addition to the two recovery/scan
  // advances, reserve a WAL, fresh MANIFEST, persistent-stats recreation,
  // final OPTIONS publication, asynchronous WAL precreation, and an initial
  // flush. Also reserve a temporary/canonical OPTIONS pair plus a possible
  // MANIFEST rollover for every column family that one open could create.
  // Column-family IDs bound that count to uint32.
  constexpr uint64_t kMaxMissingColumnFamilies =
      std::numeric_limits<uint32_t>::max();
  constexpr uint64_t kFileNumbersPerMissingColumnFamily = 3;
  // Two recovery/scan advances, open WAL, recovery MANIFEST, up to six for
  // persistent-stats drop/recreate, three for final OPTIONS publication,
  // asynchronous WAL precreation, and table plus MANIFEST for an initial
  // flush.
  constexpr uint64_t kFixedWritableOpenFileNumberHeadroom = 16;
  constexpr uint64_t kWritableOpenFileNumberHeadroom =
      kFileNumbersPerMissingColumnFamily * kMaxMissingColumnFamilies +
      kFixedWritableOpenFileNumberHeadroom;
  static_assert(kWritableOpenFileNumberHeadroom < kFileNumberMask);
  if (new_options_number > kFileNumberMask - kWritableOpenFileNumberHeadroom ||
      wal_entry_count > (kFileNumberMask - new_options_number -
                         kWritableOpenFileNumberHeadroom) /
                            2) {
    return Status::InvalidArgument("File number space exhausted");
  }
  const std::string temp_options =
      TempOptionsFileName(db_path, new_options_number);
  const std::string final_options =
      OptionsFileName(db_path, new_options_number);
  env.DeleteFile(temp_options).PermitUncheckedError();

  std::string manifest_path;
  [[maybe_unused]] uint64_t manifest_number = 0;
  s = GetCurrentManifestPath(db_path, fs.get(), /*is_retry=*/false,
                             &manifest_path, &manifest_number);
  if (!s.ok()) {
    return s;
  }
  Options loaded_options(loaded_db_options, loaded_cfs.front().options);
  loaded_options.env = &env;
  if (loaded_options.db_paths.empty()) {
    loaded_options.db_paths.emplace_back(db_path, 0);
  }
  ImmutableDBOptions immutable_options(loaded_options);
  MutableDBOptions mutable_options(loaded_options);
  FileOptions file_options;
  std::shared_ptr<Cache> table_cache = NewLRUCache(8 * 1024 * 1024);
  WriteController write_controller(loaded_options.delayed_write_rate);
  WriteBufferManager write_buffer_manager(loaded_options.db_write_buffer_size);
  VersionSet versions(
      db_path, &immutable_options, mutable_options, file_options,
      table_cache.get(), &write_buffer_manager, &write_controller,
      /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr, /*db_id=*/"",
      /*db_session_id=*/"", loaded_options.daily_offpeak_time_utc,
      /*error_handler=*/nullptr, /*unchanging=*/false);
  // Parsing above verifies every live MANIFEST CF has options. Extra CFs are a
  // safe partial-batch superset and are filtered by persisted-options APIs.
  // Copy byte-for-byte so an older ldb does not discard option fields
  // introduced by a newer binary.
  s = CopyFile(fs.get(), db_path + "/" + selected_options_name,
               Temperature::kUnknown, temp_options, Temperature::kUnknown,
               /*size=*/0, loaded_db_options.use_fsync,
               /*io_tracer=*/nullptr);
  if (s.ok()) {
    s = env.RenameFile(temp_options, final_options);
  }
  if (!s.ok()) {
    env.DeleteFile(temp_options).PermitUncheckedError();
    return s;
  }
  std::unique_ptr<FSDirectory> db_dir;
  IOStatus io_s = fs->NewDirectory(db_path, IOOptions(), &db_dir, nullptr);
  if (io_s.ok()) {
    assert(db_dir != nullptr);
    io_s = db_dir->FsyncWithDirOptions(IOOptions(), nullptr,
                                       DirFsyncOptions(final_options));
  }
  if (!io_s.ok()) {
    return io_s;
  }

  return versions.AppendColumnFamilyDropsToManifest(
      manifest_path, manifest_append_boundary, /*cf_ids=*/{}, WriteOptions(),
      loaded_options.manifest_preallocation_size, new_options_number);
}
}  // namespace

void UpdateManifestCommand::DoCommand() {
  PrepareOptions();
  if (!exec_state_.IsNotStarted()) {
    return;
  }

  auto level = verbose_ ? InfoLogLevel::INFO_LEVEL : InfoLogLevel::WARN_LEVEL;
  options_.info_log = std::make_shared<StderrLogger>(level);

  Status s;
  if (rebind_options_file_) {
    s = RebindManifestToSelectedOptions(options_, db_path_, options_file_,
                                        file_number_paths_);
  }
  if (s.ok() && update_temperatures_) {
    if (column_families_.empty()) {
      column_families_.emplace_back(kDefaultColumnFamilyName, options_);
    }
    s = experimental::UpdateManifestForFilesState(options_, db_path_,
                                                  column_families_);
  }

  if (!s.ok()) {
    std::string error = "failed to update manifest: ";
    error.append(s.ToString());
    exec_state_ = LDBCommandExecuteResult::Failed(std::move(error));
  } else if (rebind_options_file_) {
    // Rebind is also exercised through ldb's stdout-oriented CLI test helper.
    fprintf(stdout, "Manifest updates successful\n");
    exec_state_ = LDBCommandExecuteResult::Succeed("");
  } else {
    // Preserve the existing update_temperatures result contract. The command
    // runner renders this nonempty result through its normal result channel,
    // and programmatic callers can inspect it through GetExecuteState().
    exec_state_ =
        LDBCommandExecuteResult::Succeed("Manifest updates successful");
  }
}

const std::string CompactionProgressDumpCommand::ARG_PATH = "path";

CompactionProgressDumpCommand::CompactionProgressDumpCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, false, BuildCmdLineOptions({ARG_PATH})) {
  auto itr = options.find(ARG_PATH);
  if (itr != options.end()) {
    path_ = itr->second;
  } else {
    path_ = "";
  }

  if (path_.empty()) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "The --path option is required for compaction_progress_dump command");
  }
}

void CompactionProgressDumpCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(CompactionProgressDumpCommand::Name());
  ret.append(" [--" + ARG_PATH + "=<path_to_compaction_progress_file>]");
  ret.append("\n");
}

void CompactionProgressDumpCommand::DoCommand() {
  DumpCompactionProgressFile(path_);
}

namespace {

const std::string kInputFile = "input.bin";
const std::string kResultFile = "result.bin";
constexpr int kPollIntervalMs = 100;
constexpr int kPollTimeoutMs = 120000;
constexpr int kPollMaxAttempts = kPollTimeoutMs / kPollIntervalMs;

// Atomic write via tmp+rename so the polling reader never sees partial data.
bool AtomicWriteStringToFile(const std::string& path, const std::string& data) {
  std::string tmp_path = path + ".tmp";
  std::ofstream ofs(tmp_path, std::ios::binary);
  if (!ofs) {
    return false;
  }
  ofs.write(data.data(), data.size());
  if (!ofs.good()) {
    return false;
  }
  ofs.close();
  return std::rename(tmp_path.c_str(), path.c_str()) == 0;
}

bool PollForFile(Env* env, const std::string& path, std::string* data) {
  for (int i = 0; i < kPollMaxAttempts; i++) {
    Status s = ROCKSDB_NAMESPACE::ReadFileToString(env, path, data);
    if (s.ok() && !data->empty()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(kPollIntervalMs));
  }
  return false;
}

class LocalFileCompactionService : public CompactionService {
 public:
  explicit LocalFileCompactionService(const std::string& job_dir)
      : job_dir_(job_dir) {}

  const char* Name() const override { return "LocalFileCompactionService"; }

  CompactionServiceScheduleResponse Schedule(
      const CompactionServiceJobInfo& /*info*/,
      const std::string& compaction_service_input) override {
    assert(!scheduled_);
    scheduled_ = true;
    if (!AtomicWriteStringToFile(job_dir_ + "/" + kInputFile,
                                 compaction_service_input)) {
      return CompactionServiceScheduleResponse(
          CompactionServiceJobStatus::kFailure);
    }
    return CompactionServiceScheduleResponse(
        "job_0001", CompactionServiceJobStatus::kSuccess);
  }

  CompactionServiceJobStatus Wait(const std::string& /*scheduled_job_id*/,
                                  std::string* result) override {
    if (!PollForFile(Env::Default(), job_dir_ + "/" + kResultFile, result)) {
      return CompactionServiceJobStatus::kFailure;
    }
    return CompactionServiceJobStatus::kSuccess;
  }

 private:
  std::string job_dir_;
  bool scheduled_ = false;
};

}  // namespace

const std::string RemoteCompactionPrimaryCommand::ARG_JOB_DIR =
    "job_dir";  // NOLINT(cert-err58-cpp)

RemoteCompactionPrimaryCommand::RemoteCompactionPrimaryCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, false, BuildCmdLineOptions({ARG_JOB_DIR})) {
  auto it = options.find(ARG_JOB_DIR);
  if (it != options.end()) {
    job_dir_ = it->second;
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed("--job_dir is required");
  }
}

void RemoteCompactionPrimaryCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(Name());
  ret.append(" --db=<db_path> --job_dir=<dir>");
  ret.append("\n    Open an existing DB and run CompactRange() through ");
  ret.append("LocalFileCompactionService.\n");
  ret.append("    Schedule() writes job_dir/input.bin (once). ");
  ret.append("Wait() polls for job_dir/result.bin.\n");
  ret.append("    Must run with remote_compaction_worker in background on ");
  ret.append("the same --db and --job_dir,\n");
  ret.append("    otherwise Wait() will hang until timeout.\n");
}

void RemoteCompactionPrimaryCommand::DoCommand() {
  auto cs = std::make_shared<LocalFileCompactionService>(job_dir_);

  Options options;
  options.compaction_service = cs;
  options.disable_auto_compactions = true;

  std::unique_ptr<DB> db;
  Status s = DB::Open(options, db_path_, &db);
  if (!s.ok()) {
    exec_state_ = LDBCommandExecuteResult::Failed("Open: " + s.ToString());
    return;
  }

  s = db->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  if (!s.ok()) {
    exec_state_ =
        LDBCommandExecuteResult::Failed("CompactRange: " + s.ToString());
    return;
  }

  fprintf(stdout, "remote_compaction_primary: OK\n");
}

const std::string RemoteCompactionWorkerCommand::ARG_JOB_DIR =
    "job_dir";  // NOLINT(cert-err58-cpp)

RemoteCompactionWorkerCommand::RemoteCompactionWorkerCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, false, BuildCmdLineOptions({ARG_JOB_DIR})) {
  auto it = options.find(ARG_JOB_DIR);
  if (it != options.end()) {
    job_dir_ = it->second;
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed("--job_dir is required");
  }
}

void RemoteCompactionWorkerCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(Name());
  ret.append(" --db=<db_path> --job_dir=<dir>");
  ret.append("\n    Read job_dir/input.bin, run DB::OpenAndCompact(), ");
  ret.append("write job_dir/result.bin.\n");
}

void RemoteCompactionWorkerCommand::DoCommand() {
  std::string input_path = job_dir_ + "/" + kInputFile;
  std::string input;
  if (!PollForFile(Env::Default(), input_path, &input)) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "Timed out (" + std::to_string(kPollTimeoutMs / 1000) +
        "s) waiting for " + input_path);
    return;
  }

  std::string result;
  CompactionServiceOptionsOverride override_options;
  override_options.table_factory.reset(NewBlockBasedTableFactory());
  Status s = DB::OpenAndCompact(OpenAndCompactOptions(), db_path_, "job_0001",
                                input, &result, override_options);
  if (!s.ok()) {
    exec_state_ =
        LDBCommandExecuteResult::Failed("OpenAndCompact: " + s.ToString());
    return;
  }

  std::string result_path = job_dir_ + "/" + kResultFile;
  if (!AtomicWriteStringToFile(result_path, result)) {
    exec_state_ =
        LDBCommandExecuteResult::Failed("Failed to write " + result_path);
    return;
  }

  fprintf(stdout, "remote_compaction_worker: OK (%zu bytes result)\n",
          result.size());
}

}  // namespace ROCKSDB_NAMESPACE
