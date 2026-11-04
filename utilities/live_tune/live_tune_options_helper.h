//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <map>
#include <string>
#include <utility>

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyHandle;
class DB;

namespace live_tune {

// Parses all of `text` as a finite number, the way RocksDB parses double
// options.
bool ToDouble(const std::string& text, double* value);

// "a.b" -> {"a", "b"}; "a" -> {"a", ""}.
std::pair<std::string, std::string> SplitNested(const std::string& name);

// The current value of each of `allowed_options` (see
// LiveTuneIntent::allowed_options) in `cf` of `db`, or in the DB options,
// as RocksDB writes it (empty for an unknown option); with kLiveTuneAnyOption,
// of every mutable option it allows. Fails, leaving `values` empty, if any of
// the options cannot be serialized.
Status GetOptionValues(
    DB* db, ColumnFamilyHandle* cf,
    const std::map<std::string, std::pair<double, double>>& allowed_options,
    std::map<std::string, std::string>* values);

// InvalidArgument naming the first of `allowed_options` (other than
// kLiveTuneAnyOption) that neither `cf` of `db` nor the DB options have.
Status CheckOptionNames(
    DB* db, ColumnFamilyHandle* cf,
    const std::map<std::string, std::pair<double, double>>& allowed_options);

// Rewrites each value in `values` (option name -> value) as RocksDB writes it,
// e.g. 64M as 67108864, by parsing it as that option of `cf` or of the DB.
// InvalidArgument for a value the option doesn't accept.
Status NormalizeOptionValues(DB* db, ColumnFamilyHandle* cf,
                             std::map<std::string, std::string>* values);

// Sets options by name with DB::SetOptions() on `cf` and DB::SetDBOptions();
// a dotted name ("a.x") sets one field of a struct or of the table factory.
// The DB options are checked before anything is set. After
// a failure the values are read back, because SetOptions() and
// SetDBOptions() can fail with the options already in effect (e.g. when the
// OPTIONS file write fails): if the DB options turn out not to be set, the
// column family options set before them are restored (Incomplete if that
// fails too).
Status SetOptionValues(DB* db, ColumnFamilyHandle* cf,
                       const std::map<std::string, std::string>& values);

}  // namespace live_tune
}  // namespace ROCKSDB_NAMESPACE
