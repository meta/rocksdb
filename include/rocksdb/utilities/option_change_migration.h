//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
// Prepares a database to be compatible with new_opts after using old_opts.
// Restructures the LSM tree but does NOT apply new_opts - you must call
// DB::Open(new_opts, dbname) afterward to actually use the new configuration.
// It is best-effort with no guarantee to succeed. A full compaction may be
// executed.
//
// Limitations: single column family only
//
// WARNING: using this to migrate from non-FIFO to FIFO compaction
// with `Options::compaction_options_fifo.max_table_files_size` > 0 can cause
// the whole DB to be dropped right after migration if the migrated data is
// larger than `max_table_files_size`
Status OptionChangeMigration(const std::string& dbname, const Options& old_opts,
                             const Options& new_opts);

// Multi-CF version: Prepares a database with multiple column families to be
// compatible with new options after using old options.
//
// REQUIREMENTS:
// - old_cf_descs and new_cf_descs MUST have the same number of CFs
// - old_cf_descs and new_cf_descs MUST have the same CF names IN THE SAME ORDER
// - Adding or dropping CFs is NOT supported - use CreateColumnFamily() or
//   DropColumnFamily() separately before/after migration
//
// The function will return InvalidArgument status if these requirements are
// violated.
//
// WARNING: using this to migrate from non-FIFO to FIFO compaction
// with `max_table_files_size` > 0 can cause the whole DB to be dropped right
// after migration if the migrated data is larger than `max_table_files_size`
Status OptionChangeMigration(
    const std::string& dbname, const DBOptions& old_db_opts,
    const std::vector<ColumnFamilyDescriptor>& old_cf_descs,
    const DBOptions& new_db_opts,
    const std::vector<ColumnFamilyDescriptor>& new_cf_descs);

}  // namespace ROCKSDB_NAMESPACE
