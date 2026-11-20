// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/options_util.h"

#include <unordered_set>

#include "db/version_set.h"
#include "file/filename.h"
#include "options/options_parser.h"
#include "rocksdb/convenience.h"
#include "rocksdb/options.h"
#include "table/block_based/block_based_table_factory.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {
Status LoadOptionsFromFile(const ConfigOptions& config_options,
                           const std::string& file_name, DBOptions* db_options,
                           std::vector<ColumnFamilyDescriptor>* cf_descs,
                           std::shared_ptr<Cache>* cache) {
  RocksDBOptionsParser parser;
  const auto& fs = config_options.env->GetFileSystem();
  Status s = parser.Parse(config_options, file_name, fs.get());
  if (!s.ok()) {
    return s;
  }
  *db_options = *parser.db_opt();
  const std::vector<std::string>& cf_names = *parser.cf_names();
  const std::vector<ColumnFamilyOptions>& cf_opts = *parser.cf_opts();
  cf_descs->clear();
  for (size_t i = 0; i < cf_opts.size(); ++i) {
    cf_descs->emplace_back(cf_names[i], cf_opts[i]);
    if (cache != nullptr) {
      TableFactory* tf = cf_opts[i].table_factory.get();
      if (tf != nullptr) {
        auto* opts = tf->GetOptions<BlockBasedTableOptions>();
        if (opts != nullptr) {
          opts->block_cache = *cache;
        }
      }
    }
  }
  return Status::OK();
}

namespace {
struct LatestOptionsFileInfo {
  std::string file_name;
  OptionsFileManifestState manifest_state;
  OptionsFileSelection selection;
};

bool ShouldFilterColumnFamilies(const LatestOptionsFileInfo& info) {
  return !info.manifest_state.column_family_names.empty() &&
         (info.selection.selected_by_manifest || info.selection.used_fallback);
}

Status FilterColumnFamilies(const OptionsFileManifestState& manifest_state,
                            std::vector<ColumnFamilyDescriptor>* cf_descs) {
  std::unordered_set<std::string> live_cf_names;
  for (const auto& [id, name] : manifest_state.column_family_names) {
    (void)id;
    live_cf_names.insert(name);
  }

  std::unordered_set<std::string> found_cf_names;
  std::vector<ColumnFamilyDescriptor> filtered;
  filtered.reserve(live_cf_names.size());
  for (auto& descriptor : *cf_descs) {
    if (live_cf_names.count(descriptor.name) != 0) {
      found_cf_names.insert(descriptor.name);
      filtered.push_back(std::move(descriptor));
    }
  }
  if (found_cf_names.size() != live_cf_names.size()) {
    return Status::Corruption(
        "The committed OPTIONS file is missing a live column family");
  }
  *cf_descs = std::move(filtered);
  return Status::OK();
}

Status GetLatestOptionsFileInfo(const std::string& dbpath, Env* env,
                                LatestOptionsFileInfo* info) {
  assert(env != nullptr);
  assert(info != nullptr);
  *info = LatestOptionsFileInfo();

  std::vector<std::string> file_names;
  Status s = env->GetChildren(dbpath, &file_names);
  if (s.IsNotFound()) {
    return Status::NotFound(Status::kPathNotFound,
                            "No options files found in the DB directory.",
                            dbpath);
  }
  if (!s.ok()) {
    return s;
  }

  s = VersionSet::ResolveOptionsFileNumber(
      dbpath, env->GetFileSystem().get(), info->manifest_state, file_names,
      /*inspect_options_file_tracking=*/true, &info->selection);
  if (!s.ok()) {
    return s;
  }

  // A legacy highest-numbered OPTIONS file is authoritative without consulting
  // MANIFEST. A tracked file requires the committed number.
  if (info->selection.used_fallback) {
    TEST_SYNC_POINT("GetLatestOptionsFileName:BeforeManifestReplay");
    s = VersionSet::GetOptionsFileManifestState(
        dbpath, env->GetFileSystem().get(), &info->manifest_state);
    if (!s.ok()) {
      return s;
    }
    s = VersionSet::ResolveOptionsFileNumber(
        dbpath, env->GetFileSystem().get(), info->manifest_state, file_names,
        /*inspect_options_file_tracking=*/true, &info->selection);
    if (!s.ok()) {
      return s;
    }
  }

  if (info->selection.file_number == 0) {
    return Status::NotFound(Status::kPathNotFound,
                            "No options files found in the DB directory.",
                            dbpath);
  }

  // Preserve the directory entry's spelling for legacy files. Historically
  // ParseFileName accepted non-canonical zero padding such as OPTIONS-0001.
  for (const std::string& file_name : file_names) {
    uint64_t number = 0;
    FileType type;
    if (ParseFileName(file_name, &number, &type) && type == kOptionsFile &&
        number == info->selection.file_number) {
      info->file_name = file_name;
      return Status::OK();
    }
  }
  info->file_name = OptionsFileName(info->selection.file_number);
  return Status::OK();
}
}  // namespace

Status GetLatestOptionsFileName(const std::string& dbpath, Env* env,
                                std::string* options_file_name) {
  assert(options_file_name != nullptr);
  LatestOptionsFileInfo info;
  Status s = GetLatestOptionsFileInfo(dbpath, env, &info);
  if (s.ok()) {
    *options_file_name = std::move(info.file_name);
  }
  return s;
}

Status LoadLatestOptions(const ConfigOptions& config_options,
                         const std::string& dbpath, DBOptions* db_options,
                         std::vector<ColumnFamilyDescriptor>* cf_descs,
                         std::shared_ptr<Cache>* cache) {
  LatestOptionsFileInfo info;
  Status s = GetLatestOptionsFileInfo(dbpath, config_options.env, &info);
  if (!s.ok()) {
    return s;
  }
  s = LoadOptionsFromFile(config_options, dbpath + "/" + info.file_name,
                          db_options, cf_descs, cache);
  if (s.ok() && ShouldFilterColumnFamilies(info)) {
    s = FilterColumnFamilies(info.manifest_state, cf_descs);
  }
  return s;
}

Status CheckOptionsCompatibility(
    const ConfigOptions& config_options, const std::string& dbpath,
    const DBOptions& db_options,
    const std::vector<ColumnFamilyDescriptor>& cf_descs) {
  LatestOptionsFileInfo info;
  Status s = GetLatestOptionsFileInfo(dbpath, config_options.env, &info);
  if (!s.ok()) {
    return s;
  }

  std::vector<std::string> cf_names;
  std::vector<ColumnFamilyOptions> cf_opts;
  for (const auto& cf_desc : cf_descs) {
    cf_names.push_back(cf_desc.name);
    cf_opts.push_back(cf_desc.options);
  }

  const auto& fs = config_options.env->GetFileSystem();

  if (ShouldFilterColumnFamilies(info)) {
    std::unordered_set<std::string> live_cf_names;
    for (const auto& [id, name] : info.manifest_state.column_family_names) {
      (void)id;
      live_cf_names.insert(name);
    }
    return RocksDBOptionsParser::VerifyRocksDBOptionsFromFile(
        config_options, db_options, cf_names, cf_opts, live_cf_names,
        dbpath + "/" + info.file_name, fs.get());
  }
  return RocksDBOptionsParser::VerifyRocksDBOptionsFromFile(
      config_options, db_options, cf_names, cf_opts,
      dbpath + "/" + info.file_name, fs.get());
}

}  // namespace ROCKSDB_NAMESPACE
