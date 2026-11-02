// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "rocksdb/advanced_options.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"

namespace ROCKSDB_NAMESPACE {
struct ColumnFamilyOptions;
struct ConfigOptions;
struct DBOptions;
struct ImmutableCFOptions;
struct ImmutableDBOptions;
struct MutableDBOptions;
struct MutableCFOptions;
struct Options;

const std::vector<CompressionType>& GetSupportedCompressions();

const std::vector<CompressionType>& GetSupportedDictCompressions();

const std::vector<ChecksumType>& GetSupportedChecksums();

inline bool IsSupportedChecksumType(ChecksumType type) {
  // Avoid annoying compiler warning-as-error (-Werror=type-limits)
  auto min = kNoChecksum;
  auto max = kXXH3;
  return type >= min && type <= max;
}

// Returns true if read_options has an active table filter, i.e. a non-null
// pointer to a non-empty std::function. A null pointer, or a pointer to an
// empty std::function, means no filtering.
inline bool HasTableFilter(const ReadOptions& read_options) {
  return read_options.table_filter != nullptr && *read_options.table_filter;
}

// Checks that the combination of DBOptions and ColumnFamilyOptions are valid
Status ValidateOptions(const DBOptions& db_opts,
                       const ColumnFamilyOptions& cf_opts);

// Configuration queries: callers must not hold the DB mutex.
Status GetCompactionFilterMergeOrder(const CompactionFilter* filter,
                                     const CompactionFilterFactory* factory,
                                     TableFileCreationReason reason,
                                     CompactionFilter::MergeOrder* order);
Status ValidateCompactionFilterOptions(const ColumnFamilyOptions& cf_opts);
// Whether the direct filter or the factory declares merge-before-filter.
bool DeclaresMergeBeforeFilter(const ColumnFamilyOptions& cf_opts);
Status MergeBeforeFilterTransactionError();

DBOptions BuildDBOptions(const ImmutableDBOptions& immutable_db_options,
                         const MutableDBOptions& mutable_db_options);
// Overwrites `options`
void BuildDBOptions(const ImmutableDBOptions& immutable_db_options,
                    const MutableDBOptions& mutable_db_options,
                    DBOptions& options);

ColumnFamilyOptions BuildColumnFamilyOptions(
    const ColumnFamilyOptions& ioptions,
    const MutableCFOptions& mutable_cf_options);

void UpdateColumnFamilyOptions(const ImmutableCFOptions& ioptions,
                               ColumnFamilyOptions* cf_opts);
void UpdateColumnFamilyOptions(const MutableCFOptions& moptions,
                               ColumnFamilyOptions* cf_opts);

std::unique_ptr<Configurable> DBOptionsAsConfigurable(
    const MutableDBOptions& opts);
std::unique_ptr<Configurable> DBOptionsAsConfigurable(
    const DBOptions& opts,
    const std::unordered_map<std::string, std::string>* opt_map = nullptr);
std::unique_ptr<Configurable> CFOptionsAsConfigurable(
    const MutableCFOptions& opts);
std::unique_ptr<Configurable> CFOptionsAsConfigurable(
    const ColumnFamilyOptions& opts,
    const std::unordered_map<std::string, std::string>* opt_map = nullptr);

Status StringToMap(const std::string& opts_str,
                   std::unordered_map<std::string, std::string>* opts_map);

Status GetStringFromCompressionType(std::string* compression_str,
                                    CompressionType compression_type);

struct OptionsHelper {
  static const std::string kCFOptionsName /*= "ColumnFamilyOptions"*/;
  static const std::string kDBOptionsName /*= "DBOptions" */;
  static std::map<CompactionStyle, std::string> compaction_style_to_string;
  static std::map<CompactionPri, std::string> compaction_pri_to_string;
  static std::map<CompactionStopStyle, std::string>
      compaction_stop_style_to_string;
  static std::map<Temperature, std::string> temperature_to_string;
  static std::unordered_map<std::string, ChecksumType> checksum_type_string_map;
  static std::unordered_map<std::string, CompressionType>
      compression_type_string_map;
  static std::unordered_map<std::string, PrepopulateBlobCache>
      prepopulate_blob_cache_string_map;
  static std::unordered_map<std::string, CompactionStopStyle>
      compaction_stop_style_string_map;
  static std::unordered_map<std::string, EncodingType> encoding_type_string_map;
  static std::unordered_map<std::string, CompactionStyle>
      compaction_style_string_map;
  static std::unordered_map<std::string, CompactionPri>
      compaction_pri_string_map;
  static std::unordered_map<std::string, Temperature> temperature_string_map;
};

// Some aliasing
static auto& compaction_style_to_string =
    OptionsHelper::compaction_style_to_string;
static auto& compaction_pri_to_string = OptionsHelper::compaction_pri_to_string;
static auto& compaction_stop_style_to_string =
    OptionsHelper::compaction_stop_style_to_string;
static auto& temperature_to_string = OptionsHelper::temperature_to_string;
static auto& checksum_type_string_map = OptionsHelper::checksum_type_string_map;
static auto& compaction_stop_style_string_map =
    OptionsHelper::compaction_stop_style_string_map;
static auto& compression_type_string_map =
    OptionsHelper::compression_type_string_map;
static auto& encoding_type_string_map = OptionsHelper::encoding_type_string_map;
static auto& compaction_style_string_map =
    OptionsHelper::compaction_style_string_map;
static auto& compaction_pri_string_map =
    OptionsHelper::compaction_pri_string_map;
static auto& temperature_string_map = OptionsHelper::temperature_string_map;
static auto& prepopulate_blob_cache_string_map =
    OptionsHelper::prepopulate_blob_cache_string_map;

}  // namespace ROCKSDB_NAMESPACE
