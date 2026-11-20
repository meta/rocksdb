//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/db_split_merge.h"

#include <cinttypes>
#include <cstdio>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/options_util.h"

namespace ROCKSDB_NAMESPACE {
namespace {

class OpenedDB {
 public:
  OpenedDB() = default;
  OpenedDB(const OpenedDB&) = delete;
  OpenedDB& operator=(const OpenedDB&) = delete;
  OpenedDB(OpenedDB&&) = delete;
  OpenedDB& operator=(OpenedDB&&) = delete;

  ~OpenedDB() {
    for (ColumnFamilyHandle* handle : handles_) {
      delete handle;
    }
  }

  Status Open(const std::string& path) {
    ConfigOptions config_options;
    config_options.env = Env::Default();
    // A comparator, merge operator, or compaction filter that cannot be
    // recreated here would otherwise be silently dropped.
    config_options.ignore_unsupported_options = false;
    DBOptions db_options;
    std::vector<ColumnFamilyDescriptor> descriptors;
    Status status = LoadLatestOptions(config_options, path, &db_options,
                                      &descriptors, nullptr);
    if (!status.ok()) {
      return status;
    }
    db_options.create_if_missing = false;
    db_options.create_missing_column_families = false;
    db_options.error_if_exists = false;
    status = DB::Open(db_options, path, descriptors, &handles_, &db_);
    if (!status.ok()) {
      return status;
    }
    for (ColumnFamilyHandle* handle : handles_) {
      if (handle == nullptr) {
        return Status::Corruption("DB::Open returned a null column family");
      }
      handles_by_name_.emplace(handle->GetName(), handle);
    }
    return Status::OK();
  }

  DB* db() const { return db_.get(); }

  ColumnFamilyHandle* FindColumnFamily(const std::string& name) const {
    const std::unordered_map<std::string, ColumnFamilyHandle*>::const_iterator
        it = handles_by_name_.find(name);
    return it == handles_by_name_.end() ? nullptr : it->second;
  }

 private:
  std::unique_ptr<DB> db_;
  std::vector<ColumnFamilyHandle*> handles_;
  std::unordered_map<std::string, ColumnFamilyHandle*> handles_by_name_;
};

void PrintUsage() {
  fprintf(stderr,
          "Usage:\n"
          "  db_split_merge split <source_db> <destination_db>\n"
          "      <source_cf> <begin_key_hex> <end_key_hex> [...]\n"
          "  db_split_merge delete-ranges <source_db>\n"
          "      <source_cf> <begin_key_hex> <end_key_hex> [...]\n"
          "  db_split_merge merge <source_db> <destination_db> "
          "<checkpoint_dir>\n"
          "      <source_cf> <destination_cf> <begin_key_hex> <end_key_hex> "
          "[...]\n"
          "Keys are even-length hexadecimal strings without a 0x prefix.\n");
}

Status DecodeKey(const std::string& encoded, std::string* decoded) {
  if (!Slice(encoded).DecodeHex(decoded)) {
    return Status::InvalidArgument("invalid hexadecimal key: " + encoded);
  }
  return Status::OK();
}

// Parses (<source_cf> <begin_key_hex> <end_key_hex>) triples starting at
// args[first].
Status ParseSplits(const OpenedDB& source, const std::vector<std::string>& args,
                   size_t first, std::vector<ColumnFamilySplit>* splits) {
  if (splits == nullptr) {
    return Status::InvalidArgument("splits is null");
  }
  if (args.size() <= first || (args.size() - first) % 3 != 0) {
    return Status::InvalidArgument("invalid split ranges");
  }
  splits->reserve((args.size() - first) / 3);
  for (size_t i = first; i < args.size(); i += 3) {
    ColumnFamilySplit split;
    split.source_cf = source.FindColumnFamily(args[i]);
    if (split.source_cf == nullptr) {
      return Status::InvalidArgument("column family does not exist: " +
                                     args[i]);
    }
    Status status = DecodeKey(args[i + 1], &split.begin_key);
    if (!status.ok()) {
      return status;
    }
    status = DecodeKey(args[i + 2], &split.end_key);
    if (!status.ok()) {
      return status;
    }
    splits->push_back(std::move(split));
  }
  return Status::OK();
}

Status RunSplit(const std::vector<std::string>& args,
                SplitMergeResult* result) {
  if (args.size() < 4) {
    return Status::InvalidArgument("invalid split arguments");
  }

  OpenedDB source;
  Status status = source.Open(args[2]);
  if (!status.ok()) {
    return status;
  }

  SplitDBOptions options;
  options.destination_db_path = args[3];
  status = ParseSplits(source, args, 4, &options.column_family_splits);
  if (!status.ok()) {
    return status;
  }
  status = SplitDB(source.db(), options, result);
  if (!status.ok()) {
    return status;
  }
  status = DeleteSplitRanges(source.db(), options);
  if (!status.ok()) {
    fprintf(stderr,
            "%s is complete, but deleting the ranges from %s failed. Move "
            "traffic to %s, then rerun the same ranges with delete-ranges.\n",
            args[3].c_str(), args[2].c_str(), args[3].c_str());
  }
  return status;
}

Status RunDeleteRanges(const std::vector<std::string>& args) {
  if (args.size() < 3) {
    return Status::InvalidArgument("invalid delete-ranges arguments");
  }

  OpenedDB source;
  Status status = source.Open(args[2]);
  if (!status.ok()) {
    return status;
  }

  SplitDBOptions options;
  status = ParseSplits(source, args, 3, &options.column_family_splits);
  if (!status.ok()) {
    return status;
  }
  return DeleteSplitRanges(source.db(), options);
}

Status RunMerge(const std::vector<std::string>& args,
                SplitMergeResult* result) {
  if (args.size() < 9 || (args.size() - 5) % 4 != 0) {
    return Status::InvalidArgument("invalid merge arguments");
  }

  OpenedDB source;
  Status status = source.Open(args[2]);
  if (!status.ok()) {
    return status;
  }
  OpenedDB destination;
  status = destination.Open(args[3]);
  if (!status.ok()) {
    return status;
  }

  const size_t range_count = (args.size() - 5) / 4;
  MergeDBOptions options;
  options.checkpoint_directory = args[4];
  options.column_family_merges.reserve(range_count);
  for (size_t i = 5; i < args.size(); i += 4) {
    ColumnFamilyMerge merge;
    merge.source_cf = source.FindColumnFamily(args[i]);
    if (merge.source_cf == nullptr) {
      return Status::InvalidArgument("column family does not exist: " +
                                     args[i]);
    }
    merge.destination_cf = destination.FindColumnFamily(args[i + 1]);
    if (merge.destination_cf == nullptr) {
      return Status::InvalidArgument("column family does not exist: " +
                                     args[i + 1]);
    }
    status = DecodeKey(args[i + 2], &merge.begin_key);
    if (!status.ok()) {
      return status;
    }
    status = DecodeKey(args[i + 3], &merge.end_key);
    if (!status.ok()) {
      return status;
    }
    options.column_family_merges.push_back(std::move(merge));
  }
  return MergeDB(source.db(), destination.db(), options, result);
}

}  // namespace

int RunDBSplitMergeTool(const std::vector<std::string>& args) {
  if (args.size() < 2) {
    PrintUsage();
    return 1;
  }

  SplitMergeResult result;
  Status status;
  const std::string& command = args[1];
  if (command == "split") {
    status = RunSplit(args, &result);
  } else if (command == "delete-ranges") {
    status = RunDeleteRanges(args);
  } else if (command == "merge") {
    status = RunMerge(args, &result);
  } else {
    PrintUsage();
    return 1;
  }

  if (!status.ok()) {
    fprintf(stderr, "%s\n", status.ToString().c_str());
    if (status.IsInvalidArgument()) {
      PrintUsage();
    }
    return 1;
  }
  fprintf(stdout,
          "{\"checkpoint_sequence\":%" PRIu64 ",\"transferred_files\":%" PRIu64
          ",\"transferred_bytes\":%" PRIu64 "}\n",
          result.checkpoint_sequence, result.transferred_files,
          result.transferred_bytes);
  return 0;
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  if (argc < 0 || argv == nullptr) {
    return 1;
  }
  std::vector<std::string> args;
  for (int i = 0; i < argc; ++i) {
    if (argv[i] == nullptr) {
      return 1;
    }
    args.emplace_back(argv[i]);
  }
  return ROCKSDB_NAMESPACE::RunDBSplitMergeTool(args);
}
