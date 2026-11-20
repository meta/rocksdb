//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once

#include <stdio.h>
#include <stdlib.h>

#include <algorithm>
#include <functional>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/ldb_tool.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/db_ttl.h"
#include "rocksdb/utilities/ldb_cmd_execute_result.h"
#include "rocksdb/utilities/transaction_db.h"

namespace ROCKSDB_NAMESPACE {

class LDBCommand {
 public:
  // Command-line arguments
  static const std::string ARG_ENV_URI;
  static const std::string ARG_FS_URI;
  static const std::string ARG_DB;
  static const std::string ARG_PATH;
  static const std::string ARG_SECONDARY_PATH;
  static const std::string ARG_LEADER_PATH;
  static const std::string ARG_HEX;
  static const std::string ARG_KEY_HEX;
  static const std::string ARG_VALUE_HEX;
  static const std::string ARG_CF_NAME;
  static const std::string ARG_TTL;
  static const std::string ARG_TTL_START;
  static const std::string ARG_TTL_END;
  static const std::string ARG_USE_TXN;
  static const std::string ARG_TXN_WRITE_POLICY;
  static const std::string ARG_TIMESTAMP;
  static const std::string ARG_TRY_LOAD_OPTIONS;
  static const std::string ARG_IGNORE_UNKNOWN_OPTIONS;
  static const std::string ARG_FROM;
  static const std::string ARG_TO;
  static const std::string ARG_MAX_KEYS;
  static const std::string ARG_BLOOM_BITS;
  static const std::string ARG_FIX_PREFIX_LEN;
  static const std::string ARG_COMPRESSION_TYPE;
  static const std::string ARG_COMPRESSION_MAX_DICT_BYTES;
  static const std::string ARG_BLOCK_SIZE;
  static const std::string ARG_UNIFORM_CV_THRESHOLD;
  static const std::string ARG_AUTO_COMPACTION;
  static const std::string ARG_DB_WRITE_BUFFER_SIZE;
  static const std::string ARG_WRITE_BUFFER_SIZE;
  static const std::string ARG_FILE_SIZE;
  static const std::string ARG_CREATE_IF_MISSING;
  static const std::string ARG_NO_VALUE;
  static const std::string ARG_DISABLE_CONSISTENCY_CHECKS;
  static const std::string ARG_ENABLE_BLOB_FILES;
  static const std::string ARG_MIN_BLOB_SIZE;
  static const std::string ARG_BLOB_FILE_SIZE;
  static const std::string ARG_BLOB_COMPRESSION_TYPE;
  static const std::string ARG_ENABLE_BLOB_GARBAGE_COLLECTION;
  static const std::string ARG_BLOB_GARBAGE_COLLECTION_AGE_CUTOFF;
  static const std::string ARG_BLOB_GARBAGE_COLLECTION_FORCE_THRESHOLD;
  static const std::string ARG_BLOB_COMPACTION_READAHEAD_SIZE;
  static const std::string ARG_BLOB_FILE_STARTING_LEVEL;
  static const std::string ARG_PREPOPULATE_BLOB_CACHE;
  static const std::string ARG_DECODE_BLOB_INDEX;
  static const std::string ARG_DUMP_UNCOMPRESSED_BLOBS;
  static const std::string ARG_READ_TIMESTAMP;
  static const std::string ARG_GET_WRITE_UNIX_TIME;

  struct ParsedParams {
    std::string cmd;
    std::vector<std::string> cmd_params;
    std::map<std::string, std::string> option_map;
    std::vector<std::string> flags;
  };

  static LDBCommand* SelectCommand(const ParsedParams& parsed_parms);

  static void ParseSingleParam(const std::string& param,
                               ParsedParams& parsed_params,
                               std::vector<std::string>& cmd_tokens);

  static LDBCommand* InitFromCmdLineArgs(
      const std::vector<std::string>& args, const Options& options,
      const LDBOptions& ldb_options,
      const std::vector<ColumnFamilyDescriptor>* column_families,
      const std::function<LDBCommand*(const ParsedParams&)>& selector =
          SelectCommand);

  static LDBCommand* InitFromCmdLineArgs(
      int argc, char const* const* argv, const Options& options,
      const LDBOptions& ldb_options,
      const std::vector<ColumnFamilyDescriptor>* column_families);

  bool ValidateCmdLineOptions();

  virtual void PrepareOptions();

  virtual void OverrideBaseOptions();

  virtual void OverrideBaseCFOptions(ColumnFamilyOptions* cf_opts);

  virtual void SetDBOptions(Options options) { options_ = std::move(options); }

  virtual void SetColumnFamilies(
      const std::vector<ColumnFamilyDescriptor>* column_families) {
    if (column_families != nullptr) {
      column_families_ = *column_families;
    } else {
      column_families_.clear();
    }
  }

  void SetLDBOptions(const LDBOptions& ldb_options) {
    ldb_options_ = ldb_options;
  }

  const std::map<std::string, std::string>& TEST_GetOptionMap() {
    return option_map_;
  }

  const std::vector<std::string>& TEST_GetFlags() { return flags_; }

  virtual bool NoDBOpen() { return false; }

  // No copying allowed
  LDBCommand(const LDBCommand&) = delete;
  LDBCommand& operator=(const LDBCommand&) = delete;
  LDBCommand(LDBCommand&&) = delete;
  LDBCommand& operator=(LDBCommand&&) = delete;

  virtual ~LDBCommand() { CloseDB(); }

  /* Run the command, and return the execute result. */
  void Run();

  virtual void DoCommand() = 0;

  LDBCommandExecuteResult GetExecuteState() { return exec_state_; }

  void ClearPreviousRunState() { exec_state_.Reset(); }

  // Consider using Slice::DecodeHex directly instead if you don't need the
  // 0x prefix
  static std::string HexToString(const std::string& str);

  // Consider using Slice::ToString(true) directly instead if
  // you don't need the 0x prefix
  static std::string StringToHex(const std::string& str);

  static const char* DELIM;

 protected:
  LDBCommandExecuteResult exec_state_;
  std::string env_uri_;
  std::string fs_uri_;
  std::string db_path_;
  // If empty, open DB as primary. If non-empty, open the DB as secondary
  // with this secondary path. When running against a database opened by
  // another process, ldb wll leave the source directory completely intact.
  std::string secondary_path_;
  std::string leader_path_;
  std::string column_family_name_;
  std::unique_ptr<DB> db_;
  DBWithTTL* db_ttl_;
  TransactionDB* db_txn_;
  std::map<std::string, ColumnFamilyHandle*> cf_handles_;
  std::map<uint32_t, const Comparator*> ucmps_;

  /**
   * true implies that this command can work if the db is opened in read-only
   * mode.
   */
  bool is_read_only_;

  /** If true, the key is input/output as hex in get/put/scan/delete etc. */
  bool is_key_hex_;

  /** If true, the value is input/output as hex in get/put/scan/delete etc. */
  bool is_value_hex_;

  /** If true, the value is treated as timestamp suffixed */
  bool is_db_ttl_;

  /** If true, open the DB as TransactionDB */
  bool is_db_txn_;

  /** Transaction write policy (0=WRITE_COMMITTED, 1=WRITE_PREPARED,
   * 2=WRITE_UNPREPARED) */
  int txn_write_policy_;

  // If true, the kvs are output with their insert/modify timestamp in a ttl db
  bool timestamp_;

  // If true, try to construct options from DB's option files.
  bool try_load_options_;

  // The value passed to options.force_consistency_checks.
  bool force_consistency_checks_;

  bool enable_blob_files_;

  bool enable_blob_garbage_collection_;

  bool create_if_missing_;

  /** Encoded user provided uint64_t read timestamp. */
  std::string read_timestamp_;

  /**
   * Map of options passed on the command-line.
   */
  const std::map<std::string, std::string> option_map_;

  /**
   * Flags passed on the command-line.
   */
  const std::vector<std::string> flags_;

  /** List of command-line options valid for this command */
  const std::vector<std::string> valid_cmd_line_options_;

  /** Shared pointer to underlying environment if applicable **/
  std::shared_ptr<Env> env_guard_;

  bool ParseKeyValue(const std::string& line, std::string* key,
                     std::string* value, bool is_key_hex, bool is_value_hex);

  LDBCommand(const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags, bool is_read_only,
             const std::vector<std::string>& valid_cmd_line_options);

  void OpenDB();

  void CloseDB();

  ColumnFamilyHandle* GetCfHandle();

  static std::string PrintKeyValue(const std::string& key,
                                   const std::string& timestamp,
                                   const std::string& value, bool is_key_hex,
                                   bool is_value_hex, const Comparator* ucmp);

  static std::string PrintKeyValue(const std::string& key,
                                   const std::string& timestamp,
                                   const std::string& value, bool is_hex,
                                   const Comparator* ucmp);

  static std::string PrintKeyValueOrWideColumns(
      const Slice& key, const Slice& timestamp, const Slice& value,
      const WideColumns& wide_columns, bool is_key_hex, bool is_value_hex,
      const Comparator* ucmp);

  /**
   * Return true if the specified flag is present in the specified flags vector
   */
  static bool IsFlagPresent(const std::vector<std::string>& flags,
                            const std::string& flag) {
    return (std::find(flags.begin(), flags.end(), flag) != flags.end());
  }

  static std::string HelpRangeCmdArgs();

  /**
   * A helper function that returns a list of command line options
   * used by this command.  It includes the common options and the ones
   * passed in.
   */
  static std::vector<std::string> BuildCmdLineOptions(
      std::vector<std::string> options);

  bool ParseIntOption(const std::map<std::string, std::string>& options,
                      const std::string& option, int& value,
                      LDBCommandExecuteResult& exec_state);

  bool ParseDoubleOption(const std::map<std::string, std::string>& options,
                         const std::string& option, double& value,
                         LDBCommandExecuteResult& exec_state);

  bool ParseStringOption(const std::map<std::string, std::string>& options,
                         const std::string& option, std::string* value);

  bool ParseCompressionTypeOption(
      const std::map<std::string, std::string>& options,
      const std::string& option, CompressionType& value,
      LDBCommandExecuteResult& exec_state);

  /**
   * Returns the value of the specified option as a boolean.
   * default_val is used if the option is not found in options.
   * Throws an exception if the value of the option is not
   * "true" or "false" (case insensitive).
   */
  bool ParseBooleanOption(const std::map<std::string, std::string>& options,
                          const std::string& option, bool default_val);

  /* Populate `ropts.timestamp` from command line flag --read_timestamp */
  Status MaybePopulateReadTimestamp(ColumnFamilyHandle* cfh, ReadOptions& ropts,
                                    Slice* read_timestamp);

  Options options_;
  std::vector<ColumnFamilyDescriptor> column_families_;
  ConfigOptions config_options_;
  LDBOptions ldb_options_;

 private:
  /**
   * Interpret command line options and flags to determine if the key
   * should be input/output in hex.
   */
  bool IsKeyHex(const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  /**
   * Interpret command line options and flags to determine if the value
   * should be input/output in hex.
   */
  bool IsValueHex(const std::map<std::string, std::string>& options,
                  const std::vector<std::string>& flags);

  bool IsTryLoadOptions(const std::map<std::string, std::string>& options,
                        const std::vector<std::string>& flags);

  /**
   * Converts val to a boolean.
   * val must be either true or false (case insensitive).
   * Otherwise an exception is thrown.
   */
  bool StringToBool(std::string val);
};

class LDBCommandRunner {
 public:
  static void PrintHelp(const LDBOptions& ldb_options, const char* exec_name,
                        bool to_stderr = true);

  // Returns the status code to return. 0 is no error.
  static int RunCommand(
      int argc, char const* const* argv, const Options& options,
      const LDBOptions& ldb_options,
      const std::vector<ColumnFamilyDescriptor>* column_families);
};

}  // namespace ROCKSDB_NAMESPACE
