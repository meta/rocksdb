// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// This file contains utility functions for RocksDB Options.
#pragma once

#include <string>
#include <vector>

#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
struct ConfigOptions;
// Constructs the DBOptions and ColumnFamilyDescriptors by loading the
// latest RocksDB options file stored in the specified rocksdb database.
//
// Note that the all the pointer options (except table_factory, which will
// be described in more details below) will be initialized with the default
// values.  Developers can further initialize them after this function call.
// Below is an example list of pointer options which will be initialized
//
// * env
// * memtable_factory
// * compaction_filter_factory
// * prefix_extractor
// * comparator
// * merge_operator
// * compaction_filter
//
// User can also choose to load customized comparator, env, and/or
// merge_operator through object registry:
// * comparator needs to be registered through Registrar<const Comparator>
// * env needs to be registered through Registrar<Env>
// * merge operator needs to be registered through
//     Registrar<std::shared_ptr<MergeOperator>>.
//
// For table_factory, this function further supports deserializing
// BlockBasedTableFactory and its BlockBasedTableOptions except the
// pointer options of BlockBasedTableOptions (flush_block_policy_factory,
// block_cache, and block_cache_compressed), which will be initialized with
// default values.  Developers can further specify these three options by
// casting the return value of TableFactory::GetOptions() to
// BlockBasedTableOptions and making necessary changes.
//
// config_options contains a set of options that controls the processing
// of the options.
//
// config_options.ignore_unknown_options can be set to true if you want to
// ignore options that are from a newer version of the db, essentially for
// forward compatibility.
//
// examples/options_file_example.cc demonstrates how to use this function
// to open a RocksDB instance.
//
// When MANIFEST tracking selects the OPTIONS file, a partially completed
// multi-column-family operation can leave extra column families in the file.
// This function filters those entries against live MANIFEST membership. A
// selected file that omits a live column family is reported as corruption.
//
// @return the function returns an OK status when it went successfully.  If
//     the specified "dbpath" does not contain any option file, then a
//     Status::NotFound will be returned.  A return value other than
//     Status::OK or Status::NotFound indicates there is some error related
//     to the options file itself.
//
// @see LoadOptionsFromFile
Status LoadLatestOptions(const ConfigOptions& config_options,
                         const std::string& dbpath, DBOptions* db_options,
                         std::vector<ColumnFamilyDescriptor>* cf_descs,
                         std::shared_ptr<Cache>* cache = {});

// Similar to LoadLatestOptions, this function constructs the DBOptions
// and ColumnFamilyDescriptors based on the specified RocksDB Options file.
//
// @see LoadLatestOptions
Status LoadOptionsFromFile(const ConfigOptions& config_options,
                           const std::string& options_file_name,
                           DBOptions* db_options,
                           std::vector<ColumnFamilyDescriptor>* cf_descs,
                           std::shared_ptr<Cache>* cache = {});

// Returns the highest-numbered legacy OPTIONS file without reading MANIFEST.
// If the highest file enables MANIFEST tracking, returns the committed file,
// skipping uncommitted tracked files. RepairDB output with no usable commit
// falls back to the highest OPTIONS file. The returned file can contain a safe
// superset of live MANIFEST column families; LoadLatestOptions performs the
// membership filtering.
Status GetLatestOptionsFileName(const std::string& dbpath, Env* env,
                                std::string* options_file_name);

// Returns Status::OK if the input DBOptions and ColumnFamilyDescriptors
// are compatible with the latest options stored in the specified DB path.
//
// If the return status is non-ok, it means the specified RocksDB instance
// might not be correctly opened with the input set of options.  Currently,
// changing one of the following options will fail the compatibility check:
//
// * comparator
// * prefix_extractor
// * table_factory
// * merge_operator
// * persist_user_defined_timestamps
Status CheckOptionsCompatibility(
    const ConfigOptions& config_options, const std::string& dbpath,
    const DBOptions& db_options,
    const std::vector<ColumnFamilyDescriptor>& cf_descs);

}  // namespace ROCKSDB_NAMESPACE
