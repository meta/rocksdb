//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

struct DumpOptions {
  // Database that will be dumped
  std::string db_path;
  // File location that will contain dump output
  std::string dump_location;
  // Don't include db information header in the dump
  bool anonymous = false;
};

class DbDumpTool {
 public:
  bool Run(const DumpOptions& dump_options,
           ROCKSDB_NAMESPACE::Options options = ROCKSDB_NAMESPACE::Options());
};

struct UndumpOptions {
  // Database that we will load the dumped file into
  std::string db_path;
  // File location of the dumped file that will be loaded
  std::string dump_location;
  // Compact the db after loading the dumped file
  bool compact_db = false;
};

class DbUndumpTool {
 public:
  bool Run(const UndumpOptions& undump_options,
           ROCKSDB_NAMESPACE::Options options = ROCKSDB_NAMESPACE::Options());
};
}  // namespace ROCKSDB_NAMESPACE
