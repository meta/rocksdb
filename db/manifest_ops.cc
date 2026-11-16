//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/manifest_ops.h"

#include "file/filename.h"

namespace ROCKSDB_NAMESPACE {

Status GetCurrentManifestPath(const std::string& dbname, FileSystem* fs,
                              bool is_retry, std::string* manifest_path,
                              uint64_t* manifest_file_number) {
  return GetCurrentManifestPath(dbname, fs, is_retry, manifest_path,
                                manifest_file_number, Env::IOActivity::kUnknown);
}

Status GetCurrentManifestPath(const std::string& dbname, FileSystem* fs,
                              bool is_retry, std::string* manifest_path,
                              uint64_t* manifest_file_number,
                              Env::IOActivity io_activity) {
  assert(fs != nullptr);
  assert(manifest_path != nullptr);
  assert(manifest_file_number != nullptr);

  IOOptions opts;
  opts.io_activity = io_activity;
  std::string fname;
  if (is_retry) {
    opts.verify_and_reconstruct_read = true;
  }
  Status s = ReadFileToString(fs, CurrentFileName(dbname), opts, &fname);
  if (!s.ok()) {
    return s;
  }
  if (fname.empty() || fname.back() != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  // remove the trailing '\n'
  fname.resize(fname.size() - 1);
  FileType type;
  bool parse_ok = ParseFileName(fname, manifest_file_number, &type);
  if (!parse_ok || type != kDescriptorFile) {
    return Status::Corruption("CURRENT file corrupted");
  }
  *manifest_path = dbname;
  if (dbname.back() != '/') {
    manifest_path->push_back('/');
  }
  manifest_path->append(fname);
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
