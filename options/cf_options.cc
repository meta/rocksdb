//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "options/cf_options.h"

#include <cassert>
#include <cinttypes>
#include <iostream>
#include <limits>
#include <string>

#include "logging/logging.h"
#include "options/configurable_helper.h"
#include "options/db_options.h"
#include "options/options_helper.h"
#include "options/options_parser.h"
#include "port/port.h"
#include "rocksdb/advanced_cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/concurrent_task_limiter.h"
#include "rocksdb/configurable.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "util/cast_util.h"
#include "util/string_util.h"

// NOTE: in this file, many option flags that were deprecated
// and removed from the rest of the code have to be kept here
// and marked as kDeprecated in order to be able to read old
// OPTIONS files.

namespace ROCKSDB_NAMESPACE {

static Status ParseCompressionOptions(const std::string& value,
                                      const std::string& name,
                                      CompressionOptions& compression_opts) {
  const char kDelimiter = ':';
  std::istringstream field_stream(value);
  std::string field;

  if (!std::getline(field_stream, field, kDelimiter)) {
    return Status::InvalidArgument("unable to parse the specified CF option " +
                                   name);
  }
  compression_opts.window_bits = ParseInt(field);

  if (!std::getline(field_stream, field, kDelimiter)) {
    return Status::InvalidArgument("unable to parse the specified CF option " +
                                   name);
  }
  compression_opts.level = ParseInt(field);

  if (!std::getline(field_stream, field, kDelimiter)) {
    return Status::InvalidArgument("unable to parse the specified CF option " +
                                   name);
  }
  compression_opts.strategy = ParseInt(field);

  // max_dict_bytes is optional for backwards compatibility
  if (!field_stream.eof()) {
    if (!std::getline(field_stream, field, kDelimiter)) {
      return Status::InvalidArgument(
          "unable to parse the specified CF option " + name);
    }
    compression_opts.max_dict_bytes = ParseInt(field);
  }

  // zstd_max_train_bytes is optional for backwards compatibility
  if (!field_stream.eof()) {
    if (!std::getline(field_stream, field, kDelimiter)) {
      return Status::InvalidArgument(
          "unable to parse the specified CF option " + name);
    }
    compression_opts.zstd_max_train_bytes = ParseInt(field);
  }

  // parallel_threads is optional for backwards compatibility
  if (!field_stream.eof()) {
    if (!std::getline(field_stream, field, kDelimiter)) {
      return Status::InvalidArgument(
          "unable to parse the specified CF option " + name);
    }
    // Since parallel_threads comes before enabled but was added optionally
    // later, we need to check if this is the final token (meaning it is the
    // enabled bit), or if there are more tokens (meaning this one is
    // parallel_threads).
    if (!field_stream.eof()) {
      compression_opts.parallel_threads = ParseInt(field);
    } else {
      // parallel_threads is not serialized with this format, but enabled is
      compression_opts.enabled = ParseBoolean("", field);
    }
  }

  // enabled is optional for backwards compatibility
  if (!field_stream.eof()) {
    if (!std::getline(field_stream, field, kDelimiter)) {
      return Status::InvalidArgument(
          "unable to parse the specified CF option " + name);
    }
    compression_opts.enabled = ParseBoolean("", field);
  }

  // max_dict_buffer_bytes is optional for backwards compatibility
  if (!field_stream.eof()) {
    if (!std::getline(field_stream, field, kDelimiter)) {
      return Status::InvalidArgument(
          "unable to parse the specified CF option " + name);
    }
    compression_opts.max_dict_buffer_bytes = ParseUint64(field);
  }

  // use_zstd_dict_trainer is optional for backwards compatibility
  if (!field_stream.eof()) {
    if (!std::getline(field_stream, field, kDelimiter)) {
      return Status::InvalidArgument(
          "unable to parse the specified CF option " + name);
    }
    compression_opts.use_zstd_dict_trainer = ParseBoolean("", field);
  }

  if (!field_stream.eof()) {
    return Status::InvalidArgument("unable to parse the specified CF option " +
                                   name);
  }
  return Status::OK();
}

static Status CloneTableFactoryForOptionsUpdate(
    TableFactory* table_factory,
    std::shared_ptr<TableFactory>* cloned_factory) {
  assert(table_factory != nullptr);
  assert(cloned_factory != nullptr);

  std::unique_ptr<TableFactory> clone = table_factory->Clone();
  if (clone == nullptr) {
    return Status::InvalidArgument(
        "Table factory does not support option updates: " +
        std::string(table_factory->Name()));
  }
  *cloned_factory = std::move(clone);
  return Status::OK();
}

static Status TableFactoryParseFn(const ConfigOptions& opts,
                                  const std::string& name,
                                  const std::string& value, void* addr) {
  assert(addr);
  auto table_factory = static_cast<std::shared_ptr<TableFactory>*>(addr);

  // The general approach to mutating a table factory is to clone it, then
  // mutate and save the clone. This avoids race conditions between SetOptions
  // and consumers of table_factory/table options by leveraging
  // MutableCFOptions infrastructure to track the table_factory pointer.

  // However, in the atypical case of setting an option that is safely mutable
  // under something pointed to by the table factory, we should avoid cloning.
  // The simple way to detect that case is to try with "mutable_options_only"
  // and see if it works. If it does, we are finished. If not, we proceed to
  // cloning etc.
  //
  // The canonical example of what is handled here is
  // table_factory.filter_policy.bloom_before_level for RibbonFilterPolicy.
  if (table_factory->get() != nullptr && !EndsWith(name, "table_factory")) {
    ConfigOptions opts_mutable_only{opts};
    opts_mutable_only.mutable_options_only = true;
    Status s =
        table_factory->get()->ConfigureOption(opts_mutable_only, name, value);
    if (s.ok()) {
      return s;
    }
    s.PermitUncheckedError();
  }

  std::shared_ptr<TableFactory> new_factory;
  Status s;
  if (name == "block_based_table_factory") {
    if (table_factory->get() != nullptr) {
      std::string factory_name = table_factory->get()->Name();
      if (factory_name == TableFactory::kBlockBasedTableName()) {
        s = CloneTableFactoryForOptionsUpdate(table_factory->get(),
                                              &new_factory);
        if (!s.ok()) {
          return s;
        }
      } else {
        s = Status::InvalidArgument("Cannot modify " + factory_name + " as " +
                                    name);
        return s;
      }
    } else {
      new_factory.reset(NewBlockBasedTableFactory());
    }
    // Passing an object string to configure/instantiate a table factory
    s = new_factory->ConfigureFromString(opts, value);
  } else if (name == "plain_table_factory") {
    if (table_factory->get() != nullptr) {
      std::string factory_name = table_factory->get()->Name();
      if (factory_name == TableFactory::kPlainTableName()) {
        s = CloneTableFactoryForOptionsUpdate(table_factory->get(),
                                              &new_factory);
        if (!s.ok()) {
          return s;
        }
      } else {
        s = Status::InvalidArgument("Cannot modify " + factory_name + " as " +
                                    name);
        return s;
      }
    } else {
      new_factory.reset(NewPlainTableFactory());
    }
    // Passing an object string to configure/instantiate a table factory
    s = new_factory->ConfigureFromString(opts, value);
  } else if (name == "table_factory" || name == OptionTypeInfo::kIdPropName()) {
    // Related to OptionTypeInfo::AsCustomSharedPtr
    if (value.empty()) {
      new_factory = nullptr;
    } else {
      s = TableFactory::CreateFromString(opts, value, &new_factory);
    }
  } else if (table_factory->get() != nullptr) {
    s = CloneTableFactoryForOptionsUpdate(table_factory->get(), &new_factory);
    if (!s.ok()) {
      return s;
    }
    // Presumably passing a value for a specific field of the table factory
    s = new_factory->ConfigureOption(opts, name, value);
  } else {
    s = Status::NotFound("Unable to instantiate a table factory from option: ",
                         name);
    return s;
  }

  // Only keep the modified clone if everything went OK
  if (s.ok()) {
    *table_factory = std::move(new_factory);
  }
  return s;
}

constexpr char kOptNameBMCompOpts[] = "bottommost_compression_opts";
constexpr char kOptNameCompOpts[] = "compression_opts";

// OptionTypeInfo map for CompressionOptions
static std::unordered_map<std::string, OptionTypeInfo>
    compression_options_type_info = {
        {"window_bits",
         {offsetof(struct CompressionOptions, window_bits), OptionType::kInt,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
        {"level",
         {offsetof(struct CompressionOptions, level), OptionType::kInt,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
        {"strategy",
         {offsetof(struct CompressionOptions, strategy), OptionType::kInt,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
        {"max_compressed_bytes_per_kb",
         {offsetof(struct CompressionOptions, max_compressed_bytes_per_kb),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"auto_skip",
         {offsetof(struct CompressionOptions, auto_skip), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
        {"auto_skip_min_sample_every",
         {offsetof(struct CompressionOptions, auto_skip_min_sample_every),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"max_dict_bytes",
         {offsetof(struct CompressionOptions, max_dict_bytes), OptionType::kInt,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
        {"zstd_max_train_bytes",
         {offsetof(struct CompressionOptions, zstd_max_train_bytes),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"parallel_threads",
         {offsetof(struct CompressionOptions, parallel_threads),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"enabled",
         {offsetof(struct CompressionOptions, enabled), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
        {"max_dict_buffer_bytes",
         {offsetof(struct CompressionOptions, max_dict_buffer_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"use_zstd_dict_trainer",
         {offsetof(struct CompressionOptions, use_zstd_dict_trainer),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"checksum",
         {offsetof(struct CompressionOptions, checksum), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
};

static std::unordered_map<std::string, OptionTypeInfo>
    file_temperature_age_type_info = {
        {"temperature",
         {offsetof(struct FileTemperatureAge, temperature),
          OptionType::kTemperature, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"age",
         {offsetof(struct FileTemperatureAge, age), OptionType::kUInt64T,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
};

static std::unordered_map<std::string, OptionTypeInfo>
    fifo_compaction_options_type_info = {
        {"max_table_files_size",
         {offsetof(struct CompactionOptionsFIFO, max_table_files_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"age_for_warm",
         {offsetof(struct CompactionOptionsFIFO, age_for_warm),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"ttl",
         {0, OptionType::kUInt64T, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"allow_compaction",
         {offsetof(struct CompactionOptionsFIFO, allow_compaction),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"file_temperature_age_thresholds",
         OptionTypeInfo::Vector<struct FileTemperatureAge>(
             offsetof(struct CompactionOptionsFIFO,
                      file_temperature_age_thresholds),
             OptionVerificationType::kNormal, OptionTypeFlags::kMutable,
             OptionTypeInfo::Struct("file_temperature_age_thresholds",
                                    &file_temperature_age_type_info, 0,
                                    OptionVerificationType::kNormal,
                                    OptionTypeFlags::kMutable))},
        {"allow_trivial_copy_when_change_temperature",
         {offsetof(struct CompactionOptionsFIFO,
                   allow_trivial_copy_when_change_temperature),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"trivial_copy_buffer_size",
         {offsetof(struct CompactionOptionsFIFO, trivial_copy_buffer_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"max_data_files_size",
         {offsetof(struct CompactionOptionsFIFO, max_data_files_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"use_kv_ratio_compaction",
         {offsetof(struct CompactionOptionsFIFO, use_kv_ratio_compaction),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}}};

static std::unordered_map<std::string, OptionTypeInfo>
    universal_compaction_options_type_info = {
        {"size_ratio",
         {offsetof(class CompactionOptionsUniversal, size_ratio),
          OptionType::kUInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"min_merge_width",
         {offsetof(class CompactionOptionsUniversal, min_merge_width),
          OptionType::kUInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"max_merge_width",
         {offsetof(class CompactionOptionsUniversal, max_merge_width),
          OptionType::kUInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"max_size_amplification_percent",
         {offsetof(class CompactionOptionsUniversal,
                   max_size_amplification_percent),
          OptionType::kUInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"compression_size_percent",
         {offsetof(class CompactionOptionsUniversal, compression_size_percent),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"max_read_amp",
         {offsetof(class CompactionOptionsUniversal, max_read_amp),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"stop_style",
         {offsetof(class CompactionOptionsUniversal, stop_style),
          OptionType::kCompactionStopStyle, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"incremental",
         {offsetof(class CompactionOptionsUniversal, incremental),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"allow_trivial_move",
         {offsetof(class CompactionOptionsUniversal, allow_trivial_move),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"reduce_file_locking",
         {offsetof(class CompactionOptionsUniversal, reduce_file_locking),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}}};

static std::unordered_map<std::string, OptionTypeInfo>
    cf_mutable_options_type_info = {
        {"report_bg_io_stats",
         {offsetof(struct MutableCFOptions, report_bg_io_stats),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"disable_auto_compactions",
         {offsetof(struct MutableCFOptions, disable_auto_compactions),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"table_factory",
         {offsetof(struct MutableCFOptions, table_factory),
          OptionType::kCustomizable, OptionVerificationType::kByName,
          OptionTypeFlags::kShared | OptionTypeFlags::kCompareLoose |
              OptionTypeFlags::kStringNameOnly | OptionTypeFlags::kDontPrepare |
              OptionTypeFlags::kMutable,
          TableFactoryParseFn}},
        {"block_based_table_factory",
         {offsetof(struct MutableCFOptions, table_factory),
          OptionType::kCustomizable, OptionVerificationType::kAlias,
          OptionTypeFlags::kShared | OptionTypeFlags::kCompareLoose |
              OptionTypeFlags::kMutable,
          TableFactoryParseFn}},
        {"plain_table_factory",
         {offsetof(struct MutableCFOptions, table_factory),
          OptionType::kCustomizable, OptionVerificationType::kAlias,
          OptionTypeFlags::kShared | OptionTypeFlags::kCompareLoose |
              OptionTypeFlags::kMutable,
          TableFactoryParseFn}},
        {"filter_deletes",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"check_flush_compaction_key_order",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"paranoid_file_checks",
         {offsetof(struct MutableCFOptions, paranoid_file_checks),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"verify_output_flags",
         {offsetof(struct MutableCFOptions, verify_output_flags),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"verify_checksums_in_compaction",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"soft_pending_compaction_bytes_limit",
         {offsetof(struct MutableCFOptions,
                   soft_pending_compaction_bytes_limit),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"hard_pending_compaction_bytes_limit",
         {offsetof(struct MutableCFOptions,
                   hard_pending_compaction_bytes_limit),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"hard_rate_limit",
         {0, OptionType::kDouble, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"soft_rate_limit",
         {0, OptionType::kDouble, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"max_compaction_bytes",
         {offsetof(struct MutableCFOptions, max_compaction_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"ignore_max_compaction_bytes_for_input",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"expanded_compaction_factor",
         {0, OptionType::kInt, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"level0_file_num_compaction_trigger",
         {offsetof(struct MutableCFOptions, level0_file_num_compaction_trigger),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"level0_slowdown_writes_trigger",
         {offsetof(struct MutableCFOptions, level0_slowdown_writes_trigger),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"level0_stop_writes_trigger",
         {offsetof(struct MutableCFOptions, level0_stop_writes_trigger),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"max_grandparent_overlap_factor",
         {0, OptionType::kInt, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"max_write_buffer_number",
         {offsetof(struct MutableCFOptions, max_write_buffer_number),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"source_compaction_factor",
         {0, OptionType::kInt, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"target_file_size_multiplier",
         {offsetof(struct MutableCFOptions, target_file_size_multiplier),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"target_file_size_is_upper_bound",
         {offsetof(struct MutableCFOptions, target_file_size_is_upper_bound),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"arena_block_size",
         {offsetof(struct MutableCFOptions, arena_block_size),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"inplace_update_num_locks",
         {offsetof(struct MutableCFOptions, inplace_update_num_locks),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"max_successive_merges",
         {offsetof(struct MutableCFOptions, max_successive_merges),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"strict_max_successive_merges",
         {offsetof(struct MutableCFOptions, strict_max_successive_merges),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"memtable_huge_page_size",
         {offsetof(struct MutableCFOptions, memtable_huge_page_size),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"memtable_prefix_bloom_huge_page_tlb_size",
         {0, OptionType::kSizeT, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"write_buffer_size",
         {offsetof(struct MutableCFOptions, write_buffer_size),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"memtable_prefix_bloom_bits",
         {0, OptionType::kUInt32T, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"memtable_prefix_bloom_size_ratio",
         {offsetof(struct MutableCFOptions, memtable_prefix_bloom_size_ratio),
          OptionType::kDouble, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"memtable_prefix_bloom_probes",
         {0, OptionType::kUInt32T, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"memtable_whole_key_filtering",
         {offsetof(struct MutableCFOptions, memtable_whole_key_filtering),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"min_partial_merge_operands",
         {0, OptionType::kUInt32T, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"max_bytes_for_level_base",
         {offsetof(struct MutableCFOptions, max_bytes_for_level_base),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"snap_refresh_nanos",
         {0, OptionType::kUInt64T, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"max_bytes_for_level_multiplier",
         {offsetof(struct MutableCFOptions, max_bytes_for_level_multiplier),
          OptionType::kDouble, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"max_bytes_for_level_multiplier_additional",
         OptionTypeInfo::Vector<int>(
             offsetof(struct MutableCFOptions,
                      max_bytes_for_level_multiplier_additional),
             OptionVerificationType::kNormal, OptionTypeFlags::kMutable,
             {0, OptionType::kInt})},
        {"max_sequential_skip_in_iterations",
         {offsetof(struct MutableCFOptions, max_sequential_skip_in_iterations),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"target_file_size_base",
         {offsetof(struct MutableCFOptions, target_file_size_base),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"compression",
         {offsetof(struct MutableCFOptions, compression),
          OptionType::kCompressionType, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"prefix_extractor",
         OptionTypeInfo::AsCustomSharedPtr<const SliceTransform>(
             offsetof(struct MutableCFOptions, prefix_extractor),
             OptionVerificationType::kByNameAllowNull,
             (OptionTypeFlags::kMutable | OptionTypeFlags::kAllowNull))},
        {"compaction_options_fifo",
         OptionTypeInfo::Struct(
             "compaction_options_fifo", &fifo_compaction_options_type_info,
             offsetof(struct MutableCFOptions, compaction_options_fifo),
             OptionVerificationType::kNormal, OptionTypeFlags::kMutable)
             .SetParseFunc([](const ConfigOptions& opts,
                              const std::string& name, const std::string& value,
                              void* addr) {
               // This is to handle backward compatibility, where
               // compaction_options_fifo could be assigned a single scalar
               // value, say, like "23", which would be assigned to
               // max_table_files_size.
               if (name == "compaction_options_fifo" &&
                   value.find('=') == std::string::npos) {
                 // Old format. Parse just a single uint64_t value.
                 auto options = static_cast<CompactionOptionsFIFO*>(addr);
                 options->max_table_files_size = ParseUint64(value);
                 return Status::OK();
               } else {
                 return OptionTypeInfo::ParseStruct(
                     opts, "compaction_options_fifo",
                     &fifo_compaction_options_type_info, name, value, addr);
               }
             })},
        {"compaction_options_universal",
         OptionTypeInfo::Struct(
             "compaction_options_universal",
             &universal_compaction_options_type_info,
             offsetof(struct MutableCFOptions, compaction_options_universal),
             OptionVerificationType::kNormal, OptionTypeFlags::kMutable)},
        {"ttl",
         {offsetof(struct MutableCFOptions, ttl), OptionType::kUInt64T,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
        {"periodic_compaction_seconds",
         {offsetof(struct MutableCFOptions, periodic_compaction_seconds),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"read_triggered_compaction_threshold",
         {offsetof(struct MutableCFOptions,
                   read_triggered_compaction_threshold),
          OptionType::kDouble, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"preclude_last_level_data_seconds",
         {offsetof(struct MutableCFOptions, preclude_last_level_data_seconds),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"preserve_internal_time_seconds",
         {offsetof(struct MutableCFOptions, preserve_internal_time_seconds),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"bottommost_temperature",
         {0, OptionType::kTemperature, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"last_level_temperature",
         {offsetof(struct MutableCFOptions, last_level_temperature),
          OptionType::kTemperature, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"default_write_temperature",
         {offsetof(struct MutableCFOptions, default_write_temperature),
          OptionType::kTemperature, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"enable_blob_files",
         {offsetof(struct MutableCFOptions, enable_blob_files),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"min_blob_size",
         {offsetof(struct MutableCFOptions, min_blob_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"blob_file_size",
         {offsetof(struct MutableCFOptions, blob_file_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"blob_file_writable_file_max_buffer_size",
         {offsetof(struct MutableCFOptions,
                   blob_file_writable_file_max_buffer_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"blob_compression_type",
         {offsetof(struct MutableCFOptions, blob_compression_type),
          OptionType::kCompressionType, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"blob_compression_opts",
         OptionTypeInfo::Struct(
             "blob_compression_opts", &compression_options_type_info,
             offsetof(struct MutableCFOptions, blob_compression_opts),
             OptionVerificationType::kNormal,
             (OptionTypeFlags::kMutable | OptionTypeFlags::kCompareNever),
             [](const ConfigOptions& opts, const std::string& name,
                const std::string& value, void* addr) {
               if (name == "blob_compression_opts" &&
                   value.find('=') == std::string::npos) {
                 auto* compression = static_cast<CompressionOptions*>(addr);
                 return ParseCompressionOptions(value, name, *compression);
               } else {
                 return OptionTypeInfo::ParseStruct(
                     opts, "blob_compression_opts",
                     &compression_options_type_info, name, value, addr);
               }
             })},
        {"enable_blob_garbage_collection",
         {offsetof(struct MutableCFOptions, enable_blob_garbage_collection),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"blob_garbage_collection_age_cutoff",
         {offsetof(struct MutableCFOptions, blob_garbage_collection_age_cutoff),
          OptionType::kDouble, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"blob_garbage_collection_force_threshold",
         {offsetof(struct MutableCFOptions,
                   blob_garbage_collection_force_threshold),
          OptionType::kDouble, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"blob_compaction_readahead_size",
         {offsetof(struct MutableCFOptions, blob_compaction_readahead_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"blob_file_starting_level",
         {offsetof(struct MutableCFOptions, blob_file_starting_level),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"prepopulate_blob_cache",
         OptionTypeInfo::Enum<PrepopulateBlobCache>(
             offsetof(struct MutableCFOptions, prepopulate_blob_cache),
             &prepopulate_blob_cache_string_map, OptionTypeFlags::kMutable)},
        {"sample_for_compression",
         {offsetof(struct MutableCFOptions, sample_for_compression),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"bottommost_compression",
         {offsetof(struct MutableCFOptions, bottommost_compression),
          OptionType::kCompressionType, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"compression_per_level",
         OptionTypeInfo::Vector<CompressionType>(
             offsetof(struct MutableCFOptions, compression_per_level),
             OptionVerificationType::kNormal, OptionTypeFlags::kMutable,
             {0, OptionType::kCompressionType})},
        {"experimental_mempurge_threshold",
         {offsetof(struct MutableCFOptions, experimental_mempurge_threshold),
          OptionType::kDouble, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"memtable_protection_bytes_per_key",
         {offsetof(struct MutableCFOptions, memtable_protection_bytes_per_key),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"bottommost_file_compaction_delay",
         {offsetof(struct MutableCFOptions, bottommost_file_compaction_delay),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"uncache_aggressiveness",
         {offsetof(struct MutableCFOptions, uncache_aggressiveness),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"block_protection_bytes_per_key",
         {offsetof(struct MutableCFOptions, block_protection_bytes_per_key),
          OptionType::kUInt8T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"paranoid_memory_checks",
         {offsetof(struct MutableCFOptions, paranoid_memory_checks),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        // Keep parsing the old misspelled name for existing OPTIONS files.
        {"memtable_veirfy_per_key_checksum_on_seek",
         {offsetof(struct MutableCFOptions,
                   memtable_verify_per_key_checksum_on_seek),
          OptionType::kBoolean, OptionVerificationType::kAlias,
          OptionTypeFlags::kMutable}},
        {"memtable_verify_per_key_checksum_on_seek",
         {offsetof(struct MutableCFOptions,
                   memtable_verify_per_key_checksum_on_seek),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {kOptNameCompOpts,
         OptionTypeInfo::Struct(
             kOptNameCompOpts, &compression_options_type_info,
             offsetof(struct MutableCFOptions, compression_opts),
             OptionVerificationType::kNormal,
             (OptionTypeFlags::kMutable | OptionTypeFlags::kCompareNever),
             [](const ConfigOptions& opts, const std::string& name,
                const std::string& value, void* addr) {
               // This is to handle backward compatibility, where
               // compression_options was a ":" separated list.
               if (name == kOptNameCompOpts &&
                   value.find('=') == std::string::npos) {
                 auto* compression = static_cast<CompressionOptions*>(addr);
                 return ParseCompressionOptions(value, name, *compression);
               } else {
                 return OptionTypeInfo::ParseStruct(
                     opts, kOptNameCompOpts, &compression_options_type_info,
                     name, value, addr);
               }
             })},
        {kOptNameBMCompOpts,
         OptionTypeInfo::Struct(
             kOptNameBMCompOpts, &compression_options_type_info,
             offsetof(struct MutableCFOptions, bottommost_compression_opts),
             OptionVerificationType::kNormal,
             (OptionTypeFlags::kMutable | OptionTypeFlags::kCompareNever),
             [](const ConfigOptions& opts, const std::string& name,
                const std::string& value, void* addr) {
               // This is to handle backward compatibility, where
               // compression_options was a ":" separated list.
               if (name == kOptNameBMCompOpts &&
                   value.find('=') == std::string::npos) {
                 auto* compression = static_cast<CompressionOptions*>(addr);
                 return ParseCompressionOptions(value, name, *compression);
               } else {
                 return OptionTypeInfo::ParseStruct(
                     opts, kOptNameBMCompOpts, &compression_options_type_info,
                     name, value, addr);
               }
             })},
        {"compression_manager",
         OptionTypeInfo::AsCustomSharedPtr<CompressionManager>(
             offsetof(struct MutableCFOptions, compression_manager),
             OptionVerificationType::kByNameAllowNull,
             (OptionTypeFlags::kMutable | OptionTypeFlags::kAllowNull))},
        // End special case properties
        {"memtable_max_range_deletions",
         {offsetof(struct MutableCFOptions, memtable_max_range_deletions),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"memtable_op_scan_flush_trigger",
         {offsetof(struct MutableCFOptions, memtable_op_scan_flush_trigger),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"memtable_avg_op_scan_flush_trigger",
         {offsetof(struct MutableCFOptions, memtable_avg_op_scan_flush_trigger),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"min_tombstones_for_range_conversion",
         {offsetof(struct MutableCFOptions,
                   min_tombstones_for_range_conversion),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
};

static std::unordered_map<std::string, OptionTypeInfo>
    cf_immutable_options_type_info = {
        /* not yet supported
        CompressionOptions compression_opts;
        TablePropertiesCollectorFactories table_properties_collector_factories;
        using TablePropertiesCollectorFactories =
            std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>;
        UpdateStatus (*inplace_callback)(char* existing_value,
                                         uint34_t* existing_value_size,
                                         Slice delta_value,
                                         std::string* merged_value);
        std::vector<DbPath> cf_paths;
         */
        {"compaction_measure_io_stats",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"purge_redundant_kvs_while_flush",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"inplace_update_support",
         {offsetof(struct ImmutableCFOptions, inplace_update_support),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"level_compaction_dynamic_level_bytes",
         {offsetof(struct ImmutableCFOptions,
                   level_compaction_dynamic_level_bytes),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"level_compaction_dynamic_file_size",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"optimize_filters_for_hits",
         {offsetof(struct ImmutableCFOptions, optimize_filters_for_hits),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"force_consistency_checks",
         {offsetof(struct ImmutableCFOptions, force_consistency_checks),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"disallow_memtable_writes",
         {offsetof(struct ImmutableCFOptions, disallow_memtable_writes),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"default_temperature",
         {offsetof(struct ImmutableCFOptions, default_temperature),
          OptionType::kTemperature, OptionVerificationType::kNormal,
          OptionTypeFlags::kCompareNever}},
        // Need to keep this around to be able to read old OPTIONS files.
        {"max_mem_compaction_level",
         {0, OptionType::kInt, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"max_write_buffer_number_to_maintain",
         {0, OptionType::kInt, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone, nullptr}},
        {"max_write_buffer_size_to_maintain",
         {offsetof(struct ImmutableCFOptions,
                   max_write_buffer_size_to_maintain),
          OptionType::kInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"min_write_buffer_number_to_merge",
         {offsetof(struct ImmutableCFOptions, min_write_buffer_number_to_merge),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, nullptr}},
        {"num_levels",
         {offsetof(struct ImmutableCFOptions, num_levels), OptionType::kInt,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"bloom_locality",
         {offsetof(struct ImmutableCFOptions, bloom_locality),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"rate_limit_delay_max_milliseconds",
         {0, OptionType::kUInt, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"comparator",
         OptionTypeInfo::AsCustomRawPtr<const Comparator>(
             offsetof(struct ImmutableCFOptions, user_comparator),
             OptionVerificationType::kByName, OptionTypeFlags::kCompareLoose)
             .SetSerializeFunc(
                 // Serializes a Comparator
                 [](const ConfigOptions& opts, const std::string&,
                    const void* addr, std::string* value) {
                   // it's a const pointer of const Comparator*
                   const auto* ptr =
                       static_cast<const Comparator* const*>(addr);
                   if (*ptr == nullptr) {
                     *value = kNullptrString;
                   } else if (opts.mutable_options_only) {
                     *value = "";
                   } else {
                     *value = (*ptr)->ToString(opts);
                   }
                   return Status::OK();
                 })},
        {"memtable_insert_with_hint_prefix_extractor",
         OptionTypeInfo::AsCustomSharedPtr<const SliceTransform>(
             offsetof(struct ImmutableCFOptions,
                      memtable_insert_with_hint_prefix_extractor),
             OptionVerificationType::kByNameAllowNull, OptionTypeFlags::kNone)},
        {"memtable_factory",
         {offsetof(struct ImmutableCFOptions, memtable_factory),
          OptionType::kCustomizable, OptionVerificationType::kByName,
          OptionTypeFlags::kShared,
          [](const ConfigOptions& opts, const std::string&,
             const std::string& value, void* addr) {
            std::unique_ptr<MemTableRepFactory> factory;
            auto* shared =
                static_cast<std::shared_ptr<MemTableRepFactory>*>(addr);
            Status s =
                MemTableRepFactory::CreateFromString(opts, value, shared);
            return s;
          }}},
        {"memtable",
         {offsetof(struct ImmutableCFOptions, memtable_factory),
          OptionType::kCustomizable, OptionVerificationType::kAlias,
          OptionTypeFlags::kShared,
          [](const ConfigOptions& opts, const std::string&,
             const std::string& value, void* addr) {
            std::unique_ptr<MemTableRepFactory> factory;
            auto* shared =
                static_cast<std::shared_ptr<MemTableRepFactory>*>(addr);
            Status s =
                MemTableRepFactory::CreateFromString(opts, value, shared);
            return s;
          }}},
        {"table_properties_collectors",
         OptionTypeInfo::Vector<
             std::shared_ptr<TablePropertiesCollectorFactory>>(
             offsetof(struct ImmutableCFOptions,
                      table_properties_collector_factories),
             OptionVerificationType::kByName, OptionTypeFlags::kNone,
             OptionTypeInfo::AsCustomSharedPtr<TablePropertiesCollectorFactory>(
                 0, OptionVerificationType::kByName, OptionTypeFlags::kNone))},
        {"compaction_filter",
         OptionTypeInfo::AsCustomRawPtr<const CompactionFilter>(
             offsetof(struct ImmutableCFOptions, compaction_filter),
             OptionVerificationType::kByName, OptionTypeFlags::kAllowNull)},
        {"compaction_filter_factory",
         OptionTypeInfo::AsCustomSharedPtr<CompactionFilterFactory>(
             offsetof(struct ImmutableCFOptions, compaction_filter_factory),
             OptionVerificationType::kByName, OptionTypeFlags::kAllowNull)},
        {"merge_operator",
         OptionTypeInfo::AsCustomSharedPtr<MergeOperator>(
             offsetof(struct ImmutableCFOptions, merge_operator),
             OptionVerificationType::kByNameAllowFromNull,
             OptionTypeFlags::kCompareLoose | OptionTypeFlags::kAllowNull)},
        {"compaction_style",
         {offsetof(struct ImmutableCFOptions, compaction_style),
          OptionType::kCompactionStyle, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"compaction_pri",
         {offsetof(struct ImmutableCFOptions, compaction_pri),
          OptionType::kCompactionPri, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"sst_partitioner_factory",
         OptionTypeInfo::AsCustomSharedPtr<SstPartitionerFactory>(
             offsetof(struct ImmutableCFOptions, sst_partitioner_factory),
             OptionVerificationType::kByName, OptionTypeFlags::kAllowNull)},
        {"blob_cache",
         {offsetof(struct ImmutableCFOptions, blob_cache), OptionType::kUnknown,
          OptionVerificationType::kNormal,
          (OptionTypeFlags::kCompareNever | OptionTypeFlags::kDontSerialize),
          // Parses the input value as a Cache
          [](const ConfigOptions& opts, const std::string&,
             const std::string& value, void* addr) {
            auto* cache = static_cast<std::shared_ptr<Cache>*>(addr);
            return Cache::CreateFromString(opts, value, cache);
          }}},
        {"enable_blob_direct_write",
         {offsetof(struct ImmutableCFOptions, enable_blob_direct_write),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"blob_direct_write_partitions",
         {offsetof(struct ImmutableCFOptions, blob_direct_write_partitions),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"blob_direct_write_partition_strategy",
         {offsetof(struct ImmutableCFOptions,
                   blob_direct_write_partition_strategy),
          OptionType::kUnknown, OptionVerificationType::kNormal,
          (OptionTypeFlags::kCompareNever | OptionTypeFlags::kDontSerialize)}},
        {"persist_user_defined_timestamps",
         {offsetof(struct ImmutableCFOptions, persist_user_defined_timestamps),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kCompareLoose}},
        {"cf_allow_ingest_behind",
         {offsetof(struct ImmutableCFOptions, cf_allow_ingest_behind),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"memtable_batch_lookup_optimization",
         {offsetof(struct ImmutableCFOptions,
                   memtable_batch_lookup_optimization),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
};

class ConfigurableMutableCFOptions : public Configurable {
 public:
  explicit ConfigurableMutableCFOptions(const MutableCFOptions& mcf) {
    mutable_ = mcf;
    RegisterOptions(&mutable_, &cf_mutable_options_type_info);
  }

 protected:
  MutableCFOptions mutable_;
};

class ConfigurableCFOptions : public ConfigurableMutableCFOptions {
 public:
  ConfigurableCFOptions(const ColumnFamilyOptions& opts,
                        const std::unordered_map<std::string, std::string>* map)
      : ConfigurableMutableCFOptions(MutableCFOptions(opts)),
        immutable_(opts),
        cf_options_(opts),
        opt_map_(map) {
    RegisterOptions(&immutable_, &cf_immutable_options_type_info);
  }

 protected:
  Status ConfigureOptions(
      const ConfigOptions& config_options,
      const std::unordered_map<std::string, std::string>& opts_map,
      std::unordered_map<std::string, std::string>* unused) override {
    Status s = Configurable::ConfigureOptions(config_options, opts_map, unused);
    if (s.ok()) {
      UpdateColumnFamilyOptions(mutable_, &cf_options_);
      UpdateColumnFamilyOptions(immutable_, &cf_options_);
      s = PrepareOptions(config_options);
    }
    return s;
  }

  const void* GetOptionsPtr(const std::string& name) const override {
    if (name == OptionsHelper::kCFOptionsName) {
      return &cf_options_;
    } else {
      return ConfigurableMutableCFOptions::GetOptionsPtr(name);
    }
  }

  bool OptionsAreEqual(const ConfigOptions& config_options,
                       const OptionTypeInfo& opt_info,
                       const std::string& opt_name, const void* const this_ptr,
                       const void* const that_ptr,
                       std::string* mismatch) const override {
    bool equals = opt_info.AreEqual(config_options, opt_name, this_ptr,
                                    that_ptr, mismatch);
    if (!equals && opt_info.IsByName()) {
      if (opt_map_ == nullptr) {
        equals = true;
      } else {
        const auto& iter = opt_map_->find(opt_name);
        if (iter == opt_map_->end()) {
          equals = true;
        } else {
          equals = opt_info.AreEqualByName(config_options, opt_name, this_ptr,
                                           iter->second);
        }
      }
      if (equals) {  // False alarm, clear mismatch
        *mismatch = "";
      }
    }
    if (equals && opt_info.IsConfigurable() && opt_map_ != nullptr) {
      const auto* this_config = opt_info.AsRawPointer<Configurable>(this_ptr);
      if (this_config == nullptr) {
        const auto& iter = opt_map_->find(opt_name);
        // If the name exists in the map and is not empty/null,
        // then the this_config should be set.
        if (iter != opt_map_->end() && !iter->second.empty() &&
            iter->second != kNullptrString) {
          *mismatch = opt_name;
          equals = false;
        }
      }
    }
    return equals;
  }

 private:
  ImmutableCFOptions immutable_;
  ColumnFamilyOptions cf_options_;
  const std::unordered_map<std::string, std::string>* opt_map_;
};

std::unique_ptr<Configurable> CFOptionsAsConfigurable(
    const MutableCFOptions& opts) {
  std::unique_ptr<Configurable> ptr(new ConfigurableMutableCFOptions(opts));
  return ptr;
}
std::unique_ptr<Configurable> CFOptionsAsConfigurable(
    const ColumnFamilyOptions& opts,
    const std::unordered_map<std::string, std::string>* opt_map) {
  std::unique_ptr<Configurable> ptr(new ConfigurableCFOptions(opts, opt_map));
  return ptr;
}

ImmutableCFOptions::ImmutableCFOptions() : ImmutableCFOptions(Options()) {}

ImmutableCFOptions::ImmutableCFOptions(const ColumnFamilyOptions& cf_options)
    : compaction_style(cf_options.compaction_style),
      compaction_pri(cf_options.compaction_pri),
      user_comparator(cf_options.comparator),
      internal_comparator(InternalKeyComparator(cf_options.comparator)),
      merge_operator(cf_options.merge_operator),
      compaction_filter(cf_options.compaction_filter),
      compaction_filter_factory(cf_options.compaction_filter_factory),
      min_write_buffer_number_to_merge(
          cf_options.min_write_buffer_number_to_merge),
      max_write_buffer_size_to_maintain(
          cf_options.max_write_buffer_size_to_maintain),
      inplace_update_support(cf_options.inplace_update_support),
      inplace_callback(cf_options.inplace_callback),
      memtable_factory(cf_options.memtable_factory),
      table_properties_collector_factories(
          cf_options.table_properties_collector_factories),
      bloom_locality(cf_options.bloom_locality),
      level_compaction_dynamic_level_bytes(
          cf_options.level_compaction_dynamic_level_bytes),
      num_levels(cf_options.num_levels),
      optimize_filters_for_hits(cf_options.optimize_filters_for_hits),
      force_consistency_checks(cf_options.force_consistency_checks),
      disallow_memtable_writes(cf_options.disallow_memtable_writes),
      default_temperature(cf_options.default_temperature),
      memtable_insert_with_hint_prefix_extractor(
          cf_options.memtable_insert_with_hint_prefix_extractor),
      cf_paths(cf_options.cf_paths),
      compaction_thread_limiter(cf_options.compaction_thread_limiter),
      sst_partitioner_factory(cf_options.sst_partitioner_factory),
      blob_cache(cf_options.blob_cache),
      enable_blob_direct_write(cf_options.enable_blob_direct_write),
      blob_direct_write_partitions(cf_options.blob_direct_write_partitions),
      blob_direct_write_partition_strategy(
          cf_options.blob_direct_write_partition_strategy),
      persist_user_defined_timestamps(
          cf_options.persist_user_defined_timestamps),
      cf_allow_ingest_behind(cf_options.cf_allow_ingest_behind),
      memtable_batch_lookup_optimization(
          cf_options.memtable_batch_lookup_optimization) {}

ImmutableOptions::ImmutableOptions() : ImmutableOptions(Options()) {}

ImmutableOptions::ImmutableOptions(const Options& options)
    : ImmutableOptions(options, options) {}

ImmutableOptions::ImmutableOptions(const DBOptions& db_options,
                                   const ColumnFamilyOptions& cf_options)
    : ImmutableDBOptions(db_options), ImmutableCFOptions(cf_options) {}

ImmutableOptions::ImmutableOptions(const DBOptions& db_options,
                                   const ImmutableCFOptions& cf_options)
    : ImmutableDBOptions(db_options), ImmutableCFOptions(cf_options) {}

ImmutableOptions::ImmutableOptions(const ImmutableDBOptions& db_options,
                                   const ColumnFamilyOptions& cf_options)
    : ImmutableDBOptions(db_options), ImmutableCFOptions(cf_options) {}

ImmutableOptions::ImmutableOptions(const ImmutableDBOptions& db_options,
                                   const ImmutableCFOptions& cf_options)
    : ImmutableDBOptions(db_options), ImmutableCFOptions(cf_options) {}

// Multiple two operands. If they overflow, return op1.
uint64_t MultiplyCheckOverflow(uint64_t op1, double op2) {
  if (op1 == 0 || op2 <= 0) {
    return 0;
  }

  if (op1 * op2 < static_cast<double>(std::numeric_limits<uint64_t>::max())) {
    return static_cast<uint64_t>(op1 * op2);
  }

  return op1;
}

// when level_compaction_dynamic_level_bytes is true and leveled compaction
// is used, the base level is not always L1, so precomupted max_file_size can
// no longer be used. Recompute file_size_for_level from base level.
uint64_t MaxFileSizeForLevel(const MutableCFOptions& cf_options, int level,
                             CompactionStyle compaction_style, int base_level,
                             bool level_compaction_dynamic_level_bytes) {
  if (!level_compaction_dynamic_level_bytes || level < base_level ||
      compaction_style != kCompactionStyleLevel) {
    assert(level >= 0);
    assert(level < (int)cf_options.max_file_size.size());
    return cf_options.max_file_size[level];
  } else {
    assert(level >= 0 && base_level >= 0);
    assert(level - base_level < (int)cf_options.max_file_size.size());
    return cf_options.max_file_size[level - base_level];
  }
}

size_t MaxFileSizeForL0MetaPin(const MutableCFOptions& cf_options) {
  // We do not want to pin meta-blocks that almost certainly came from intra-L0
  // or a former larger `write_buffer_size` value to avoid surprising users with
  // pinned memory usage. We use a factor of 1.5 to account for overhead
  // introduced during flush in most cases.
  if (std::numeric_limits<size_t>::max() / 3 <
      cf_options.write_buffer_size / 2) {
    return std::numeric_limits<size_t>::max();
  }
  return cf_options.write_buffer_size / 2 * 3;
}

void MutableCFOptions::RefreshDerivedOptions(int num_levels,
                                             CompactionStyle compaction_style) {
  max_file_size.resize(num_levels);
  for (int i = 0; i < num_levels; ++i) {
    if (i == 0 && compaction_style == kCompactionStyleUniversal) {
      max_file_size[i] = ULLONG_MAX;
    } else if (i > 1) {
      max_file_size[i] = MultiplyCheckOverflow(max_file_size[i - 1],
                                               target_file_size_multiplier);
    } else {
      max_file_size[i] = target_file_size_base;
    }
  }
}

void MutableCFOptions::Dump(Logger* log) const {
  // Memtable related options
  ROCKS_LOG_INFO(log,
                 "                        write_buffer_size: %" ROCKSDB_PRIszt,
                 write_buffer_size);
  ROCKS_LOG_INFO(log, "                  max_write_buffer_number: %d",
                 max_write_buffer_number);
  ROCKS_LOG_INFO(log,
                 "                         arena_block_size: %" ROCKSDB_PRIszt,
                 arena_block_size);
  ROCKS_LOG_INFO(log, "              memtable_prefix_bloom_ratio: %f",
                 memtable_prefix_bloom_size_ratio);
  ROCKS_LOG_INFO(log, "              memtable_whole_key_filtering: %d",
                 memtable_whole_key_filtering);
  ROCKS_LOG_INFO(log,
                 "                  memtable_huge_page_size: %" ROCKSDB_PRIszt,
                 memtable_huge_page_size);
  ROCKS_LOG_INFO(log,
                 "                    max_successive_merges: %" ROCKSDB_PRIszt,
                 max_successive_merges);
  ROCKS_LOG_INFO(log, "             strict_max_successive_merges: %d",
                 strict_max_successive_merges);
  ROCKS_LOG_INFO(log,
                 "                 inplace_update_num_locks: %" ROCKSDB_PRIszt,
                 inplace_update_num_locks);
  ROCKS_LOG_INFO(log, "                         prefix_extractor: %s",
                 prefix_extractor == nullptr
                     ? "nullptr"
                     : prefix_extractor->GetId().c_str());
  ROCKS_LOG_INFO(log, "                 disable_auto_compactions: %d",
                 disable_auto_compactions);
  ROCKS_LOG_INFO(log, "      soft_pending_compaction_bytes_limit: %" PRIu64,
                 soft_pending_compaction_bytes_limit);
  ROCKS_LOG_INFO(log, "      hard_pending_compaction_bytes_limit: %" PRIu64,
                 hard_pending_compaction_bytes_limit);
  ROCKS_LOG_INFO(log, "       level0_file_num_compaction_trigger: %d",
                 level0_file_num_compaction_trigger);
  ROCKS_LOG_INFO(log, "           level0_slowdown_writes_trigger: %d",
                 level0_slowdown_writes_trigger);
  ROCKS_LOG_INFO(log, "               level0_stop_writes_trigger: %d",
                 level0_stop_writes_trigger);
  ROCKS_LOG_INFO(log, "                     max_compaction_bytes: %" PRIu64,
                 max_compaction_bytes);
  ROCKS_LOG_INFO(log, "                    target_file_size_base: %" PRIu64,
                 target_file_size_base);
  ROCKS_LOG_INFO(log, "              target_file_size_multiplier: %d",
                 target_file_size_multiplier);
  ROCKS_LOG_INFO(log, "         target_file_size_is_upper_bound: %d",
                 target_file_size_is_upper_bound);
  ROCKS_LOG_INFO(log, "                 max_bytes_for_level_base: %" PRIu64,
                 max_bytes_for_level_base);
  ROCKS_LOG_INFO(log, "           max_bytes_for_level_multiplier: %f",
                 max_bytes_for_level_multiplier);
  ROCKS_LOG_INFO(log, "                                      ttl: %" PRIu64,
                 ttl);
  ROCKS_LOG_INFO(log, "              periodic_compaction_seconds: %" PRIu64,
                 periodic_compaction_seconds);
  ROCKS_LOG_INFO(log, "    read_triggered_compaction_threshold: %f",
                 read_triggered_compaction_threshold);
  ROCKS_LOG_INFO(log,
                 "              preclude_last_level_data_seconds: %" PRIu64,
                 preclude_last_level_data_seconds);
  ROCKS_LOG_INFO(log, "              preserve_internal_time_seconds: %" PRIu64,
                 preserve_internal_time_seconds);
  ROCKS_LOG_INFO(log, "                   paranoid_memory_checks: %d",
                 paranoid_memory_checks);
  ROCKS_LOG_INFO(log, "memtable_verify_per_key_checksum_on_seek: %d",
                 memtable_verify_per_key_checksum_on_seek);
  std::string result;
  char buf[10];
  for (const auto m : max_bytes_for_level_multiplier_additional) {
    snprintf(buf, sizeof(buf), "%d, ", m);
    result += buf;
  }
  if (result.size() >= 2) {
    result.resize(result.size() - 2);
  } else {
    result = "";
  }

  ROCKS_LOG_INFO(log, "max_bytes_for_level_multiplier_additional: %s",
                 result.c_str());
  ROCKS_LOG_INFO(log, "        max_sequential_skip_in_iterations: %" PRIu64,
                 max_sequential_skip_in_iterations);
  ROCKS_LOG_INFO(log, "                     paranoid_file_checks: %d",
                 paranoid_file_checks);
  ROCKS_LOG_INFO(log, "                       report_bg_io_stats: %d",
                 report_bg_io_stats);
  ROCKS_LOG_INFO(log, "                              compression: %d",
                 static_cast<int>(compression));
  ROCKS_LOG_INFO(log, "          experimental_mempurge_threshold: %f",
                 experimental_mempurge_threshold);
  ROCKS_LOG_INFO(log, "         bottommost_file_compaction_delay: %" PRIu32,
                 bottommost_file_compaction_delay);
  ROCKS_LOG_INFO(log, "                   uncache_aggressiveness: %" PRIu32,
                 uncache_aggressiveness);
  ROCKS_LOG_INFO(log, "             memtable_op_scan_flush_trigger: %" PRIu32,
                 memtable_op_scan_flush_trigger);
  ROCKS_LOG_INFO(log, "         memtable_avg_op_scan_flush_trigger: %" PRIu32,
                 memtable_avg_op_scan_flush_trigger);
  ROCKS_LOG_INFO(log, "         min_tombstones_for_range_conversion: %" PRIu32,
                 min_tombstones_for_range_conversion);
  // Universal Compaction Options
  ROCKS_LOG_INFO(log, "compaction_options_universal.size_ratio : %d",
                 compaction_options_universal.size_ratio);
  ROCKS_LOG_INFO(log, "compaction_options_universal.min_merge_width : %d",
                 compaction_options_universal.min_merge_width);
  ROCKS_LOG_INFO(log, "compaction_options_universal.max_merge_width : %d",
                 compaction_options_universal.max_merge_width);
  ROCKS_LOG_INFO(
      log, "compaction_options_universal.max_size_amplification_percent : %d",
      compaction_options_universal.max_size_amplification_percent);
  ROCKS_LOG_INFO(log,
                 "compaction_options_universal.compression_size_percent : %d",
                 compaction_options_universal.compression_size_percent);
  ROCKS_LOG_INFO(log, "compaction_options_universal.max_read_amp:  %d",
                 compaction_options_universal.max_read_amp);
  ROCKS_LOG_INFO(log, "compaction_options_universal.stop_style : %d",
                 compaction_options_universal.stop_style);
  ROCKS_LOG_INFO(
      log, "compaction_options_universal.allow_trivial_move : %d",
      static_cast<int>(compaction_options_universal.allow_trivial_move));
  ROCKS_LOG_INFO(log, "compaction_options_universal.incremental        : %d",
                 static_cast<int>(compaction_options_universal.incremental));
  ROCKS_LOG_INFO(log, "compaction_options_universal.reduce_file_locking : %d",
                 compaction_options_universal.reduce_file_locking);

  // FIFO Compaction Options
  ROCKS_LOG_INFO(log, "compaction_options_fifo.max_table_files_size : %" PRIu64,
                 compaction_options_fifo.max_table_files_size);
  ROCKS_LOG_INFO(log, "compaction_options_fifo.allow_compaction : %d",
                 compaction_options_fifo.allow_compaction);
  ROCKS_LOG_INFO(log, "compaction_options_fifo.max_data_files_size : %" PRIu64,
                 compaction_options_fifo.max_data_files_size);
  ROCKS_LOG_INFO(log, "compaction_options_fifo.use_kv_ratio_compaction : %d",
                 compaction_options_fifo.use_kv_ratio_compaction);

  // Blob file related options
  ROCKS_LOG_INFO(log, "                        enable_blob_files: %s",
                 enable_blob_files ? "true" : "false");
  ROCKS_LOG_INFO(log, "                            min_blob_size: %" PRIu64,
                 min_blob_size);
  ROCKS_LOG_INFO(log, "                           blob_file_size: %" PRIu64,
                 blob_file_size);
  ROCKS_LOG_INFO(log, "  blob_file_writable_file_max_buffer_size: %" PRIu64,
                 blob_file_writable_file_max_buffer_size);
  ROCKS_LOG_INFO(log, "                    blob_compression_type: %s",
                 CompressionTypeToString(blob_compression_type).c_str());
  ROCKS_LOG_INFO(log, "             blob_compression_opts.level: %d",
                 blob_compression_opts.level);
  ROCKS_LOG_INFO(log, "           enable_blob_garbage_collection: %s",
                 enable_blob_garbage_collection ? "true" : "false");
  ROCKS_LOG_INFO(log, "       blob_garbage_collection_age_cutoff: %f",
                 blob_garbage_collection_age_cutoff);
  ROCKS_LOG_INFO(log, "  blob_garbage_collection_force_threshold: %f",
                 blob_garbage_collection_force_threshold);
  ROCKS_LOG_INFO(log, "           blob_compaction_readahead_size: %" PRIu64,
                 blob_compaction_readahead_size);
  ROCKS_LOG_INFO(log, "                 blob_file_starting_level: %d",
                 blob_file_starting_level);
  ROCKS_LOG_INFO(log, "                   prepopulate_blob_cache: %s",
                 prepopulate_blob_cache == PrepopulateBlobCache::kFlushOnly
                     ? "flush only"
                     : "disable");
  ROCKS_LOG_INFO(log, "                   last_level_temperature: %d",
                 static_cast<int>(last_level_temperature));
}

MutableCFOptions::MutableCFOptions(const Options& options)
    : MutableCFOptions(ColumnFamilyOptions(options)) {}

Status GetMutableOptionsFromStrings(
    const MutableCFOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    Logger* /*info_log*/, MutableCFOptions* new_options) {
  assert(new_options);
  *new_options = base_options;
  ConfigOptions config_options;
  Status s = OptionTypeInfo::ParseType(
      config_options, options_map, cf_mutable_options_type_info, new_options);
  if (!s.ok()) {
    *new_options = base_options;
  }
  return s;
}

bool MutableCFOptionsUnchangedByMap(
    const MutableCFOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map) {
  for (const auto& opt : options_map) {
    std::string elem_name;
    const OptionTypeInfo* opt_info = OptionTypeInfo::Find(
        opt.first, cf_mutable_options_type_info, &elem_name);
    if (opt_info == nullptr || !opt_info->IsMutable() ||
        opt_info->IsConfigurable()) {
      return false;
    }
  }
  MutableCFOptions new_options;
  return GetMutableOptionsFromStrings(base_options, options_map,
                                      /*info_log=*/nullptr, &new_options)
             .ok() &&
         new_options == base_options;
}

Status GetStringFromMutableCFOptions(const ConfigOptions& config_options,
                                     const MutableCFOptions& mutable_opts,
                                     std::string* opt_string) {
  assert(opt_string);
  opt_string->clear();
  return OptionTypeInfo::SerializeType(
      config_options, cf_mutable_options_type_info, &mutable_opts, opt_string);
}

#ifndef NDEBUG
std::vector<std::string> TEST_GetImmutableInMutableCFOptions() {
  std::vector<std::string> result;
  for (const auto& opt : cf_mutable_options_type_info) {
    if (!opt.second.IsMutable()) {
      result.emplace_back(opt.first);
    }
  }
  if (result.size() > 0) {
    std::cerr << "Warning: " << result.size() << " immutable options in "
              << "MutableCFOptions" << std::endl;
  }
  return result;
}

bool TEST_allowSetOptionsImmutableInMutable = false;
#endif  // !NDEBUG
}  // namespace ROCKSDB_NAMESPACE
