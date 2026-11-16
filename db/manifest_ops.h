//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <cassert>

#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

// Parameter is_retry=true sets the verify_and_reconstruct_read flag.
// It comes handy when caller intends to re-read the data with much stronger
// data integrity checking - e.g. in case of a perceived file corruption.
Status GetCurrentManifestPath(const std::string& dbname, FileSystem* fs,
                              bool is_retry, std::string* manifest_path,
                              uint64_t* manifest_file_number);

// The caller supplies the activity for CURRENT reads. The overload above
// retains kUnknown for callers that are not opening a DB.
Status GetCurrentManifestPath(const std::string& dbname, FileSystem* fs,
                              bool is_retry, std::string* manifest_path,
                              uint64_t* manifest_file_number,
                              Env::IOActivity io_activity);
}  // namespace ROCKSDB_NAMESPACE
