//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <cassert>
#include <memory>
#include <string>

#include "db/log_reader.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

// Parameter is_retry=true sets the verify_and_reconstruct_read flag.
// It comes handy when caller intends to re-read the data with much stronger
// data integrity checking - e.g. in case of a perceived file corruption.
Status GetCurrentManifestPath(const std::string& dbname, FileSystem* fs,
                              bool is_retry, std::string* manifest_path,
                              uint64_t* manifest_file_number);

// Keeps the first error that a log::Reader of a MANIFEST file reports, for
// example a read error or a record that fails its checksum. Later reports do
// not change it. The caller sets status_ptr before the first read.
struct ManifestReporter : public log::Reader::Reporter {
  Status* status_ptr = nullptr;
  void Corruption(size_t /*bytes*/, const Status& st,
                  uint64_t /*log_number*/ = kMaxSequenceNumber) override {
    status_ptr->UpdateIfOk(st);
  }
};

// Opens the MANIFEST file at manifest_path for reading with a log::Reader that
// verifies record checksums. reporter must outlive *reader.
Status NewManifestReader(FileSystem* fs, const std::string& manifest_path,
                         ManifestReporter* reporter,
                         std::unique_ptr<log::Reader>* reader);
}  // namespace ROCKSDB_NAMESPACE
