//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/live_tune/live_tune_options_helper.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <memory>
#include <unordered_map>

#include "options/cf_options.h"
#include "options/db_options.h"
#include "options/options_helper.h"
#include "rocksdb/configurable.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/live_tune.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"

namespace ROCKSDB_NAMESPACE {
namespace live_tune {

namespace {

bool IsOptionName(const std::string& text) {
  return !text.empty() && std::all_of(text.begin(), text.end(), [](char c) {
    return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
  });
}

// A "{field=value;...}" value; not a list of them.
bool IsNestedValue(const std::string& value) {
  std::unordered_map<std::string, std::string> map;
  if (value.empty() || value.front() != '{' || !StringToMap(value, &map).ok() ||
      map.empty()) {
    return false;
  }
  return std::all_of(map.begin(), map.end(), [](const auto& entry) {
    return IsOptionName(entry.first);
  });
}

// Adds each option in `options_string` as `prefix` + its name, or a nested
// one by its fields, as `prefix` + name.field.
void AddOptionValues(const std::string& options_string,
                     const std::string& prefix,
                     std::map<std::string, std::string>* values) {
  std::unordered_map<std::string, std::string> map;
  if (!StringToMap(options_string, &map).ok()) {
    return;
  }
  for (const auto& entry : map) {
    const std::string name = prefix + entry.first;
    if (IsNestedValue(entry.second)) {
      AddOptionValues(entry.second, name + ".", values);
    } else {
      (*values)[name] = entry.second;
    }
  }
}

}  // namespace

bool ToDouble(const std::string& text, double* value) {
  // strtod, like RocksDB's own parsing of double options (ParseDouble()), so
  // both read option values the same way in any process locale.
  if (text.empty()) {
    return false;
  }
  char* end = nullptr;
  errno = 0;
  const double parsed = std::strtod(text.c_str(), &end);
  if (end != text.c_str() + text.size() || errno == ERANGE ||
      !std::isfinite(parsed)) {
    return false;
  }
  *value = parsed;
  return true;
}

std::pair<std::string, std::string> SplitNested(const std::string& name) {
  const size_t dot = name.find('.');
  if (dot == std::string::npos) {
    return {name, ""};
  }
  return {name.substr(0, dot), name.substr(dot + 1)};
}

namespace {

// The options of `cf` and of the DB, which read and parse single options by
// name, including a field of a struct or of the table factory named with a
// dot (e.g. compaction_options_universal.size_ratio or
// block_based_table_factory.block_size).
struct OptionsView {
  OptionsView(DB* db, ColumnFamilyHandle* cf)
      : cf_options(db->GetOptions(cf)),
        db_options(db->GetDBOptions()),
        cf_config(CFOptionsAsConfigurable(cf_options)),
        db_config(DBOptionsAsConfigurable(db_options)) {}

  // Whether `name` is a column family option (else a DB option, or unknown).
  bool IsColumnFamilyOption(const std::string& name) const {
    std::string ignored;
    return cf_config->GetOption(config, name, &ignored).ok();
  }

  // Empty for an option neither has.
  Status Get(const std::string& name, std::string* value) const {
    Status s = cf_config->GetOption(config, name, value);
    if (s.IsNotFound()) {
      s = db_config->GetOption(config, name, value);
    }
    if (s.IsNotFound()) {
      value->clear();
      return Status::OK();
    }
    return s;
  }

  // `value` of option `name` as RocksDB writes it (e.g. 64M -> 67108864).
  Status Normalize(const std::string& name, std::string* value) const {
    if (IsColumnFamilyOption(name)) {
      ColumnFamilyOptions parsed;
      Status s = GetColumnFamilyOptionsFromMap(config, UnsharedCFOptions(),
                                               {{name, *value}}, &parsed);
      return s.ok() ? CFOptionsAsConfigurable(parsed)->GetOption(config, name,
                                                                 value)
                    : s;
    }
    DBOptions parsed;
    Status s =
        GetDBOptionsFromMap(config, db_options, {{name, *value}}, &parsed);
    return s.ok()
               ? DBOptionsAsConfigurable(parsed)->GetOption(config, name, value)
               : s;
  }

  // cf_options with its own table factory and filter policy. Parsing a field
  // of the table factory can set a mutable field of an object under it in
  // place (e.g. filter_policy.bloom_before_level, see TableFactoryParseFn),
  // which in a plain copy is the DB's own object.
  ColumnFamilyOptions UnsharedCFOptions() const {
    ColumnFamilyOptions options = cf_options;
    if (options.table_factory == nullptr) {
      return options;
    }
    std::unique_ptr<TableFactory> table = options.table_factory->Clone();
    if (table == nullptr) {
      options.table_factory.reset();
      return options;
    }
    BlockBasedTableOptions* table_options =
        table->GetOptions<BlockBasedTableOptions>();
    if (table_options != nullptr && table_options->filter_policy != nullptr) {
      std::shared_ptr<const FilterPolicy> policy;
      // A policy that can't be copied is left out: its fields then can't be
      // parsed, rather than parsed into the DB's object.
      if (!FilterPolicy::CreateFromString(
               config, table_options->filter_policy->ToString(config), &policy)
               .ok()) {
        policy.reset();
      }
      table_options->filter_policy = std::move(policy);
    }
    options.table_factory = std::move(table);
    return options;
  }

  const ConfigOptions config;
  const ColumnFamilyOptions cf_options;
  const DBOptions db_options;
  const std::unique_ptr<Configurable> cf_config;
  const std::unique_ptr<Configurable> db_config;
};

}  // namespace

Status GetOptionValues(
    DB* db, ColumnFamilyHandle* cf,
    const std::map<std::string, std::pair<double, double>>& allowed_options,
    std::map<std::string, std::string>* values) {
  values->clear();
  const OptionsView view(db, cf);
  std::map<std::string, std::string> result;
  if (allowed_options.count(kLiveTuneAnyOption) > 0) {
    std::string mutable_cf;
    Status s = GetStringFromMutableCFOptions(
        view.config, MutableCFOptions(view.cf_options), &mutable_cf);
    if (!s.ok()) {
      return s;
    }
    std::string mutable_db;
    s = GetStringFromMutableDBOptions(
        view.config, MutableDBOptions(view.db_options), &mutable_db);
    if (!s.ok()) {
      return s;
    }
    AddOptionValues(mutable_cf, "", &result);
    AddOptionValues(mutable_db, "", &result);
    // Setting it replaces the table factory, with its block cache and filter.
    result.erase("table_factory");
    const std::shared_ptr<TableFactory>& table = view.cf_options.table_factory;
    if (table && table->IsInstanceOf(TableFactory::kBlockBasedTableName())) {
      std::string table_string;
      s = table->GetOptionString(view.config, &table_string);
      if (!s.ok()) {
        return s;
      }
      AddOptionValues(table_string, "block_based_table_factory.", &result);
    }
  }
  for (const auto& entry : allowed_options) {
    if (entry.first == kLiveTuneAnyOption) {
      continue;
    }
    Status s = view.Get(entry.first, &result[entry.first]);
    if (!s.ok()) {
      return s;
    }
  }
  *values = std::move(result);
  return Status::OK();
}

Status CheckOptionNames(
    DB* db, ColumnFamilyHandle* cf,
    const std::map<std::string, std::pair<double, double>>& allowed_options) {
  const OptionsView view(db, cf);
  for (const auto& entry : allowed_options) {
    if (entry.first == kLiveTuneAnyOption) {
      continue;
    }
    std::string ignored;
    if (!view.cf_config->GetOption(view.config, entry.first, &ignored).ok() &&
        !view.db_config->GetOption(view.config, entry.first, &ignored).ok()) {
      return Status::InvalidArgument("live_tune: unknown allowed option " +
                                     entry.first);
    }
  }
  return Status::OK();
}

Status NormalizeOptionValues(DB* db, ColumnFamilyHandle* cf,
                             std::map<std::string, std::string>* values) {
  const OptionsView view(db, cf);
  for (auto& entry : *values) {
    Status s = view.Normalize(entry.first, &entry.second);
    if (!s.ok()) {
      return Status::InvalidArgument(entry.first + "=" + entry.second + ": " +
                                     s.ToString());
    }
  }
  return Status::OK();
}

namespace {

// Whether `db` has every value in `values`, read back by name.
bool AllInEffect(DB* db, ColumnFamilyHandle* cf,
                 std::map<std::string, std::string> values) {
  if (!NormalizeOptionValues(db, cf, &values).ok()) {
    return false;
  }
  std::map<std::string, std::pair<double, double>> names;
  for (const auto& entry : values) {
    names[entry.first] = {0, 0};
  }
  std::map<std::string, std::string> now;
  return GetOptionValues(db, cf, names, &now).ok() && now == values;
}

}  // namespace

Status SetOptionValues(DB* db, ColumnFamilyHandle* cf,
                       const std::map<std::string, std::string>& values) {
  // Dotted names ("a.x") go to SetOptions() as they are; it sets just that
  // field of the struct or table factory.
  std::unordered_map<std::string, std::string> cf_changes;
  std::unordered_map<std::string, std::string> db_changes;
  std::map<std::string, std::string> cf_values;
  std::map<std::string, std::string> db_values;
  {
    const OptionsView view(db, cf);
    for (const auto& entry : values) {
      if (view.IsColumnFamilyOption(entry.first)) {
        cf_changes.insert(entry);
        cf_values.insert(entry);
      } else {
        db_changes.insert(entry);
        db_values.insert(entry);
      }
    }
    // Check the DB options before changing anything, so a bad one does not
    // leave the column family options half applied.
    if (!db_changes.empty()) {
      DBOptions ignored;
      Status s = GetDBOptionsFromMap(view.config, view.db_options, db_changes,
                                     &ignored);
      if (!s.ok()) {
        return s;
      }
    }
  }
  // The column family values before, to restore them if the DB options then
  // fail.
  std::map<std::string, std::pair<double, double>> cf_names;
  for (const auto& entry : cf_values) {
    cf_names[entry.first] = {0, 0};
  }
  std::map<std::string, std::string> cf_previous;
  Status s = GetOptionValues(db, cf, cf_names, &cf_previous);
  if (!s.ok()) {
    return s;
  }
  if (!cf_changes.empty()) {
    s = db->SetOptions(cf, cf_changes);
    if (!s.ok() && !AllInEffect(db, cf, cf_values)) {
      return s;
    }
  }
  if (!db_changes.empty()) {
    Status db_status = db->SetDBOptions(db_changes);
    if (!db_status.ok() && !AllInEffect(db, cf, db_values)) {
      if (!cf_changes.empty()) {
        // Put the column family options back: nothing stays half applied.
        const std::unordered_map<std::string, std::string> restore(
            cf_previous.begin(), cf_previous.end());
        const Status restored = db->SetOptions(cf, restore);
        if (!restored.ok() && !AllInEffect(db, cf, cf_previous)) {
          return Status::Incomplete(
              db_status.ToString() +
              "; restoring the column family options also failed: " +
              restored.ToString());
        }
      }
      return db_status;
    }
  }
  return Status::OK();
}

}  // namespace live_tune
}  // namespace ROCKSDB_NAMESPACE
