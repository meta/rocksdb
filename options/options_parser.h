// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

struct ConfigOptions;
class OptionTypeInfo;
class TableFactory;

#define ROCKSDB_OPTION_FILE_MAJOR 1
#define ROCKSDB_OPTION_FILE_MINOR 1

enum OptionSection : char {
  kOptionSectionVersion = 0,
  kOptionSectionDBOptions,
  kOptionSectionCFOptions,
  kOptionSectionTableOptions,
  kOptionSectionUnknown
};

static const std::string opt_section_titles[] = {
    "Version", "DBOptions", "CFOptions", "TableOptions/", "Unknown"};

Status PersistRocksDBOptions(const WriteOptions& write_options,
                             const DBOptions& db_opt,
                             const std::vector<std::string>& cf_names,
                             const std::vector<ColumnFamilyOptions>& cf_opts,
                             const std::string& file_name, FileSystem* fs);
Status PersistRocksDBOptions(const WriteOptions& write_options,
                             const ConfigOptions& config_options,
                             const DBOptions& db_opt,
                             const std::vector<std::string>& cf_names,
                             const std::vector<ColumnFamilyOptions>& cf_opts,
                             const std::string& file_name, FileSystem* fs);

class RocksDBOptionsParser {
 public:
  explicit RocksDBOptionsParser();
  ~RocksDBOptionsParser() = default;
  void Reset();

  // `file_readahead_size` is used for readahead for the option file.
  // If 0 is given, a default value will be used.
  Status Parse(const std::string& file_name, FileSystem* fs,
               bool ignore_unknown_options, size_t file_readahead_size);

  Status Parse(const ConfigOptions& config_options,
               const std::string& file_name, FileSystem* fs);

  static std::string TrimAndRemoveComment(const std::string& line,
                                          const bool trim_only = false);

  const DBOptions* db_opt() const { return &db_opt_; }
  const std::unordered_map<std::string, std::string>* db_opt_map() const {
    return &db_opt_map_;
  }
  const std::vector<ColumnFamilyOptions>* cf_opts() const { return &cf_opts_; }
  const std::vector<std::string>* cf_names() const { return &cf_names_; }
  const std::vector<std::unordered_map<std::string, std::string>>* cf_opt_maps()
      const {
    return &cf_opt_maps_;
  }

  const ColumnFamilyOptions* GetCFOptions(const std::string& name) {
    return GetCFOptionsImpl(name);
  }

  // The TableOptions section of a column family as written in the file, before
  // parsing drops options and components config_options says to ignore.
  struct TableOptionsSection {
    std::string table_factory_id;
    std::unordered_map<std::string, std::string> opt_map;
  };
  // Returns nullptr if the column family has no TableOptions section.
  const TableOptionsSection* GetCFTableOptionsSection(
      const std::string& cf_name) const {
    std::unordered_map<std::string, TableOptionsSection>::const_iterator it =
        cf_table_options_sections_.find(cf_name);
    return it == cf_table_options_sections_.end() ? nullptr : &it->second;
  }
  // Whether a newer RocksDB than this one wrote the file. Parse() honors
  // ConfigOptions::ignore_unknown_options only for such a file.
  bool WrittenByNewerVersion() const;
  size_t NumColumnFamilies() { return cf_opts_.size(); }
  static Status VerifyRocksDBOptionsFromFile(
      const ConfigOptions& config_options, const DBOptions& db_opt,
      const std::vector<std::string>& cf_names,
      const std::vector<ColumnFamilyOptions>& cf_opts,
      const std::string& file_name, FileSystem* fs);
  static Status VerifyRocksDBOptionsFromFile(
      const ConfigOptions& config_options, const DBOptions& db_opt,
      const std::vector<std::string>& cf_names,
      const std::vector<ColumnFamilyOptions>& cf_opts,
      const std::unordered_set<std::string>& live_cf_names,
      const std::string& file_name, FileSystem* fs);
  static Status VerifyDBOptions(
      const ConfigOptions& config_options, const DBOptions& base_opt,
      const DBOptions& new_opt,
      const std::unordered_map<std::string, std::string>* new_opt_map =
          nullptr);

  static Status VerifyCFOptions(
      const ConfigOptions& config_options, const ColumnFamilyOptions& base_opt,
      const ColumnFamilyOptions& new_opt,
      const std::unordered_map<std::string, std::string>* new_opt_map =
          nullptr);

  static Status VerifyTableFactory(const ConfigOptions& config_options,
                                   const TableFactory* base_tf,
                                   const TableFactory* file_tf);

  static Status ExtraParserCheck(const RocksDBOptionsParser& input_parser);

  static Status ParseStatement(std::string* name, std::string* value,
                               const std::string& line, const int line_num);

 protected:
  bool IsSection(const std::string& line);
  Status ParseSection(OptionSection* section, std::string* title,
                      std::string* argument, const std::string& line,
                      const int line_num);

  Status CheckSection(const OptionSection section,
                      const std::string& section_arg, const int line_num);

  Status EndSection(
      const ConfigOptions& config_options, const OptionSection section,
      const std::string& title, const std::string& section_arg,
      const std::unordered_map<std::string, std::string>& opt_map);

  Status ValidityCheck();

  static Status InvalidArgument(const int line_num, const std::string& message);

  Status ParseVersionNumber(const std::string& ver_name,
                            const std::string& ver_string, const int max_count,
                            int* version);

  ColumnFamilyOptions* GetCFOptionsImpl(const std::string& name) {
    assert(cf_names_.size() == cf_opts_.size());
    for (size_t i = 0; i < cf_names_.size(); ++i) {
      if (cf_names_[i] == name) {
        return &cf_opts_[i];
      }
    }
    return nullptr;
  }

 private:
  DBOptions db_opt_;
  std::unordered_map<std::string, std::string> db_opt_map_;
  std::vector<std::string> cf_names_;
  std::vector<ColumnFamilyOptions> cf_opts_;
  std::vector<std::unordered_map<std::string, std::string>> cf_opt_maps_;
  std::unordered_map<std::string, TableOptionsSection>
      cf_table_options_sections_;
  bool has_version_section_;
  bool has_db_options_;
  bool has_default_cf_options_;
  int db_version[3];
  int opt_file_version[3];
};

}  // namespace ROCKSDB_NAMESPACE
