//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/block_based/block_based_table_factory.h"

#include <cinttypes>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include "cache/cache_entry_roles.h"
#include "cache/cache_reservation_manager.h"
#include "logging/logging.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/convenience.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/index_factory.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/format.h"
#include "util/cast_util.h"
#include "util/mutexlock.h"
#include "util/string_util.h"
#include "utilities/trie_index/trie_index_factory.h"

namespace ROCKSDB_NAMESPACE {

void TailPrefetchStats::RecordEffectiveSize(size_t len) {
  MutexLock l(&mutex_);
  if (num_records_ < kNumTracked) {
    num_records_++;
  }
  records_[next_++] = len;
  if (next_ == kNumTracked) {
    next_ = 0;
  }
}

size_t TailPrefetchStats::GetSuggestedPrefetchSize() {
  std::vector<size_t> sorted;
  {
    MutexLock l(&mutex_);

    if (num_records_ == 0) {
      return 0;
    }
    sorted.assign(records_, records_ + num_records_);
  }

  // Of the historic size, we find the maximum one that satisifis the condtiion
  // that if prefetching all, less than 1/8 will be wasted.
  std::sort(sorted.begin(), sorted.end());

  // Assuming we have 5 data points, and after sorting it looks like this:
  //
  //                                     +---+
  //                             +---+   |   |
  //                             |   |   |   |
  //                             |   |   |   |
  //                             |   |   |   |
  //                             |   |   |   |
  //                    +---+    |   |   |   |
  //                    |   |    |   |   |   |
  //           +---+    |   |    |   |   |   |
  //           |   |    |   |    |   |   |   |
  //  +---+    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  +---+    +---+    +---+    +---+   +---+
  //
  // and we use every of the value as a candidate, and estimate how much we
  // wasted, compared to read. For example, when we use the 3rd record
  // as candidate. This area is what we read:
  //                                     +---+
  //                             +---+   |   |
  //                             |   |   |   |
  //                             |   |   |   |
  //                             |   |   |   |
  //                             |   |   |   |
  //  ***  ***  ***  ***+ ***  ***  *** *** **
  //  *                 |   |    |   |   |   |
  //           +---+    |   |    |   |   |   *
  //  *        |   |    |   |    |   |   |   |
  //  +---+    |   |    |   |    |   |   |   *
  //  *   |    |   |    | X |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   *
  //  *   |    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   *
  //  *   |    |   |    |   |    |   |   |   |
  //  *** *** ***-***  ***--*** ***--*** +****
  // which is (size of the record) X (number of records).
  //
  // While wasted is this area:
  //                                     +---+
  //                             +---+   |   |
  //                             |   |   |   |
  //                             |   |   |   |
  //                             |   |   |   |
  //                             |   |   |   |
  //  ***  ***  ***  ****---+    |   |   |   |
  //  *                 *   |    |   |   |   |
  //  *        *-***  ***   |    |   |   |   |
  //  *        *   |    |   |    |   |   |   |
  //  *--**  ***   |    |   |    |   |   |   |
  //  |   |    |   |    | X |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  +---+    +---+    +---+    +---+   +---+
  //
  // Which can be calculated iteratively.
  // The difference between wasted using 4th and 3rd record, will
  // be following area:
  //                                     +---+
  //  +--+  +-+   ++  +-+  +-+   +---+   |   |
  //  + xxxxxxxxxxxxxxxxxxxxxxxx |   |   |   |
  //    xxxxxxxxxxxxxxxxxxxxxxxx |   |   |   |
  //  + xxxxxxxxxxxxxxxxxxxxxxxx |   |   |   |
  //  | xxxxxxxxxxxxxxxxxxxxxxxx |   |   |   |
  //  +-+ +-+  +-+  ++  +---+ +--+   |   |   |
  //  |                 |   |    |   |   |   |
  //           +---+ ++ |   |    |   |   |   |
  //  |        |   |    |   |    | X |   |   |
  //  +---+ ++ |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  +---+    +---+    +---+    +---+   +---+
  //
  // which will be the size difference between 4th and 3rd record,
  // times 3, which is number of records before the 4th.
  // Here we assume that all data within the prefetch range will be useful. In
  // reality, it may not be the case when a partial block is inside the range,
  // or there are data in the middle that is not read. We ignore those cases
  // for simplicity.
  assert(!sorted.empty());
  size_t prev_size = sorted[0];
  size_t max_qualified_size = sorted[0];
  size_t wasted = 0;
  for (size_t i = 1; i < sorted.size(); i++) {
    size_t read = sorted[i] * sorted.size();
    wasted += (sorted[i] - prev_size) * i;
    if (wasted <= read / 8) {
      max_qualified_size = sorted[i];
    }
    prev_size = sorted[i];
  }
  const size_t kMaxPrefetchSize = 512 * 1024;  // Never exceed 512KB
  return std::min(kMaxPrefetchSize, max_qualified_size);
}

const std::string kOptNameMetadataCacheOpts = "metadata_cache_options";

static std::unordered_map<std::string, PinningTier>
    pinning_tier_type_string_map = {
        {"kFallback", PinningTier::kFallback},
        {"kNone", PinningTier::kNone},
        {"kFlushedAndSimilar", PinningTier::kFlushedAndSimilar},
        {"kAll", PinningTier::kAll}};

static std::unordered_map<std::string, BlockBasedTableOptions::IndexType>
    block_base_table_index_type_string_map = {
        {"kBinarySearch", BlockBasedTableOptions::IndexType::kBinarySearch},
        {"kHashSearch", BlockBasedTableOptions::IndexType::kHashSearch},
        {"kTwoLevelIndexSearch",
         BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch},
        {"kBinarySearchWithFirstKey",
         BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey}};

static std::unordered_map<std::string, BlockBasedTableOptions::BlockSearchType>
    block_base_table_index_search_type_string_map = {
        {"kBinary", BlockBasedTableOptions::BlockSearchType::kBinary},
        {"kInterpolation",
         BlockBasedTableOptions::BlockSearchType::kInterpolation},
        {"kAuto", BlockBasedTableOptions::BlockSearchType::kAuto}};

static std::unordered_map<std::string,
                          BlockBasedTableOptions::DataBlockIndexType>
    block_base_table_data_block_index_type_string_map = {
        {"kDataBlockBinarySearch",
         BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch},
        {"kDataBlockBinaryAndHash",
         BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinaryAndHash}};

static std::unordered_map<std::string,
                          BlockBasedTableOptions::OptimizeKeyCommonPrefix>
    block_base_table_optimize_key_common_prefix_string_map = {
        {"kDisabled",
         BlockBasedTableOptions::OptimizeKeyCommonPrefix::kDisabled},
        {"kIfFastSeek",
         BlockBasedTableOptions::OptimizeKeyCommonPrefix::kIfFastSeek},
        {"kEnabled",
         BlockBasedTableOptions::OptimizeKeyCommonPrefix::kEnabled}};

static std::unordered_map<std::string,
                          BlockBasedTableOptions::IndexShorteningMode>
    block_base_table_index_shortening_mode_string_map = {
        {"kNoShortening",
         BlockBasedTableOptions::IndexShorteningMode::kNoShortening},
        {"kShortenSeparators",
         BlockBasedTableOptions::IndexShorteningMode::kShortenSeparators},
        {"kShortenSeparatorsAndSuccessor",
         BlockBasedTableOptions::IndexShorteningMode::
             kShortenSeparatorsAndSuccessor}};

static const std::unordered_map<std::string, BlockBasedTableOptions::IndexMode>
    block_base_table_index_mode_string_map = {  // NOLINT(cert-err58-cpp)
        {"kStandardOnly", BlockBasedTableOptions::IndexMode::kStandardOnly},
        {"kStandardDefault",
         BlockBasedTableOptions::IndexMode::kStandardDefault},
        {"kCustomDefault", BlockBasedTableOptions::IndexMode::kCustomDefault},
        {"kCustomOnly", BlockBasedTableOptions::IndexMode::kCustomOnly},
        {"kStandardRequired",
         BlockBasedTableOptions::IndexMode::kStandardRequired}};

static std::unordered_map<std::string, OptionTypeInfo>
    metadata_cache_options_type_info = {
        {"top_level_index_pinning",
         OptionTypeInfo::Enum<PinningTier>(
             offsetof(struct MetadataCacheOptions, top_level_index_pinning),
             &pinning_tier_type_string_map)},
        {"partition_pinning",
         OptionTypeInfo::Enum<PinningTier>(
             offsetof(struct MetadataCacheOptions, partition_pinning),
             &pinning_tier_type_string_map)},
        {"unpartitioned_pinning",
         OptionTypeInfo::Enum<PinningTier>(
             offsetof(struct MetadataCacheOptions, unpartitioned_pinning),
             &pinning_tier_type_string_map)}};

static std::unordered_map<std::string,
                          BlockBasedTableOptions::PrepopulateBlockCache>
    block_base_table_prepopulate_block_cache_string_map = {
        {"kDisable", BlockBasedTableOptions::PrepopulateBlockCache::kDisable},
        {"kFlushOnly",
         BlockBasedTableOptions::PrepopulateBlockCache::kFlushOnly},
        {"kFlushAndCompaction",
         BlockBasedTableOptions::PrepopulateBlockCache::kFlushAndCompaction}};

namespace {

OptionTypeInfo LegacyIndexBoolean(
    bool BlockBasedTableOptions::* input,
    BlockBasedTableOptions::IndexMode serialized_mode) {
  return {
      0,
      OptionType::kBoolean,
      OptionVerificationType::kNormal,
      OptionTypeFlags::kCompareNever,
      [input](const ConfigOptions&, const std::string& name,
              const std::string& value, void* addr) {
        auto* options = static_cast<BlockBasedTableOptions*>(addr);
        options->*input = ParseBoolean(name, value);
        return Status::OK();
      },
      [serialized_mode](const ConfigOptions&, const std::string&,
                        const void* addr, std::string* value) {
        const auto* options = static_cast<const BlockBasedTableOptions*>(addr);
        // Older binaries route only by these flags. Project the effective
        // policy even when an explicit mode cleared the input aliases.
        *value = options->GetEffectiveIndexMode() == serialized_mode ? "true"
                                                                     : "false";
        return Status::OK();
      }};
}

}  // namespace

static struct BlockBasedTableTypeInfo {
  std::unordered_map<std::string, OptionTypeInfo> info;

  BlockBasedTableTypeInfo() {
    info = {
        // NOTE: Below the list, most of these options are marked as mutable.
        // In theory, there should be no danger in mutability, as table
        // builders and readers work from copies of BlockBasedTableOptions.
        // However, there is currently an unresolved read-write race that
        // affecting SetOptions on BBTO fields. This should be generally
        // acceptable for non-pointer options of 64 bits or less, but a fix
        // is needed to make it mutability general here. See
        // https://github.com/meta/rocksdb/issues/10079
        /* currently not supported:
          CacheUsageOptions cache_usage_options;
         */
        {"flush_block_policy_factory",
         OptionTypeInfo::AsCustomSharedPtr<FlushBlockPolicyFactory>(
             offsetof(struct BlockBasedTableOptions,
                      flush_block_policy_factory),
             OptionVerificationType::kByName, OptionTypeFlags::kCompareNever)},
        {"cache_index_and_filter_blocks",
         {offsetof(struct BlockBasedTableOptions,
                   cache_index_and_filter_blocks),
          OptionType::kBoolean, OptionVerificationType::kNormal}},
        {"cache_index_and_filter_blocks_with_high_priority",
         {offsetof(struct BlockBasedTableOptions,
                   cache_index_and_filter_blocks_with_high_priority),
          OptionType::kBoolean, OptionVerificationType::kNormal}},
        {"pin_l0_filter_and_index_blocks_in_cache",
         {offsetof(struct BlockBasedTableOptions,
                   pin_l0_filter_and_index_blocks_in_cache),
          OptionType::kBoolean, OptionVerificationType::kNormal}},
        {"index_type", OptionTypeInfo::Enum<BlockBasedTableOptions::IndexType>(
                           offsetof(struct BlockBasedTableOptions, index_type),
                           &block_base_table_index_type_string_map)},
        {"index_block_search_type",
         OptionTypeInfo::Enum<BlockBasedTableOptions::BlockSearchType>(
             offsetof(struct BlockBasedTableOptions, index_block_search_type),
             &block_base_table_index_search_type_string_map)},
        {"hash_index_allow_collision",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated}},
        {"data_block_index_type",
         OptionTypeInfo::Enum<BlockBasedTableOptions::DataBlockIndexType>(
             offsetof(struct BlockBasedTableOptions, data_block_index_type),
             &block_base_table_data_block_index_type_string_map)},
        {"optimize_key_common_prefix",
         OptionTypeInfo::Enum<BlockBasedTableOptions::OptimizeKeyCommonPrefix>(
             offsetof(struct BlockBasedTableOptions,
                      optimize_key_common_prefix),
             &block_base_table_optimize_key_common_prefix_string_map)},
        {"index_shortening",
         OptionTypeInfo::Enum<BlockBasedTableOptions::IndexShorteningMode>(
             offsetof(struct BlockBasedTableOptions, index_shortening),
             &block_base_table_index_shortening_mode_string_map)},
        {"index_mode",
         {0, OptionType::kEnum, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone,
          [](const ConfigOptions&, const std::string& name,
             const std::string& value, void* addr) {
            auto* options = static_cast<BlockBasedTableOptions*>(addr);
            if (ParseEnum(block_base_table_index_mode_string_map, value,
                          &options->index_mode)) {
              return Status::OK();
            }
            return Status::InvalidArgument("No mapping for enum ", name);
          },
          [](const ConfigOptions&, const std::string& name, const void* addr,
             std::string* value) {
            const auto* options =
                static_cast<const BlockBasedTableOptions*>(addr);
            if (SerializeEnum(block_base_table_index_mode_string_map,
                              options->GetEffectiveIndexMode(), value)) {
              return Status::OK();
            }
            return Status::InvalidArgument("No mapping for enum ", name);
          },
          [](const ConfigOptions&, const std::string&, const void* addr1,
             const void* addr2, std::string*) {
            const auto* options1 =
                static_cast<const BlockBasedTableOptions*>(addr1);
            const auto* options2 =
                static_cast<const BlockBasedTableOptions*>(addr2);
            return options1->GetEffectiveIndexMode() ==
                   options2->GetEffectiveIndexMode();
          }}},
        {"data_block_hash_table_util_ratio",
         {offsetof(struct BlockBasedTableOptions,
                   data_block_hash_table_util_ratio),
          OptionType::kDouble, OptionVerificationType::kNormal}},
        {"checksum",
         {offsetof(struct BlockBasedTableOptions, checksum),
          OptionType::kChecksumType, OptionVerificationType::kNormal}},
        {"no_block_cache",
         {offsetof(struct BlockBasedTableOptions, no_block_cache),
          OptionType::kBoolean, OptionVerificationType::kNormal}},
        {"block_size",
         {offsetof(struct BlockBasedTableOptions, block_size),
          OptionType::kSizeT, OptionVerificationType::kNormal}},
        {"block_size_deviation",
         {offsetof(struct BlockBasedTableOptions, block_size_deviation),
          OptionType::kInt, OptionVerificationType::kNormal}},
        {"block_restart_interval",
         {offsetof(struct BlockBasedTableOptions, block_restart_interval),
          OptionType::kInt, OptionVerificationType::kNormal}},
        {"index_block_restart_interval",
         {offsetof(struct BlockBasedTableOptions, index_block_restart_interval),
          OptionType::kInt, OptionVerificationType::kNormal}},
        {"index_per_partition",
         {0, OptionType::kUInt64T, OptionVerificationType::kDeprecated}},
        {"metadata_block_size",
         {offsetof(struct BlockBasedTableOptions, metadata_block_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal}},
        {"partition_filters",
         {offsetof(struct BlockBasedTableOptions, partition_filters),
          OptionType::kBoolean, OptionVerificationType::kNormal}},
        {"decouple_partitioned_filters",
         {offsetof(struct BlockBasedTableOptions, decouple_partitioned_filters),
          OptionType::kBoolean, OptionVerificationType::kNormal}},
        {"optimize_filters_for_memory",
         {offsetof(struct BlockBasedTableOptions, optimize_filters_for_memory),
          OptionType::kBoolean, OptionVerificationType::kNormal}},
        {"use_delta_encoding",
         {offsetof(struct BlockBasedTableOptions, use_delta_encoding),
          OptionType::kBoolean, OptionVerificationType::kNormal}},
        {"filter_policy",
         OptionTypeInfo::AsCustomSharedPtr<const FilterPolicy>(
             offsetof(struct BlockBasedTableOptions, filter_policy),
             OptionVerificationType::kByNameAllowFromNull)},
        {"user_defined_index_factory",
         OptionTypeInfo::AsCustomSharedPtr<IndexFactory>(
             offsetof(struct BlockBasedTableOptions,
                      user_defined_index_factory),
             OptionVerificationType::kByNameAllowFromNull)},
        {"whole_key_filtering",
         {offsetof(struct BlockBasedTableOptions, whole_key_filtering),
          OptionType::kBoolean, OptionVerificationType::kNormal}},
        {"detect_filter_construct_corruption",
         {offsetof(struct BlockBasedTableOptions,
                   detect_filter_construct_corruption),
          OptionType::kBoolean, OptionVerificationType::kNormal}},
        {"reserve_table_builder_memory",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated}},
        {"reserve_table_reader_memory",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated}},
        {"skip_table_builder_flush",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated}},
        {"format_version",
         {offsetof(struct BlockBasedTableOptions, format_version),
          OptionType::kUInt32T, OptionVerificationType::kNormal}},
        {"separate_key_value_in_data_block",
         {offsetof(struct BlockBasedTableOptions,
                   separate_key_value_in_data_block),
          OptionType::kBoolean, OptionVerificationType::kNormal}},
        {"uniform_cv_threshold",
         {offsetof(struct BlockBasedTableOptions, uniform_cv_threshold),
          OptionType::kDouble, OptionVerificationType::kNormal}},
        {"verify_compression",
         {offsetof(struct BlockBasedTableOptions, verify_compression),
          OptionType::kBoolean, OptionVerificationType::kNormal}},
        {"read_amp_bytes_per_bit",
         {offsetof(struct BlockBasedTableOptions, read_amp_bytes_per_bit),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone,
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const std::string& value, void* addr) {
            // A workaround to fix a bug in 6.10, 6.11, 6.12, 6.13
            // and 6.14. The bug will write out 8 bytes to OPTIONS file from the
            // starting address of BlockBasedTableOptions.read_amp_bytes_per_bit
            // which is actually a uint32. Consequently, the value of
            // read_amp_bytes_per_bit written in the OPTIONS file is wrong.
            // From 6.15, RocksDB will try to parse the read_amp_bytes_per_bit
            // from OPTIONS file as a uint32. To be able to load OPTIONS file
            // generated by affected releases before the fix, we need to
            // manually parse read_amp_bytes_per_bit with this special hack.
            uint64_t read_amp_bytes_per_bit = ParseUint64(value);
            *(static_cast<uint32_t*>(addr)) =
                static_cast<uint32_t>(read_amp_bytes_per_bit);
            return Status::OK();
          }}},
        {"enable_index_compression",
         {offsetof(struct BlockBasedTableOptions, enable_index_compression),
          OptionType::kBoolean, OptionVerificationType::kNormal}},
        {"block_align",
         {offsetof(struct BlockBasedTableOptions, block_align),
          OptionType::kBoolean, OptionVerificationType::kNormal}},
        {"super_block_alignment_size",
         {offsetof(struct BlockBasedTableOptions, super_block_alignment_size),
          OptionType::kSizeT, OptionVerificationType::kNormal}},
        {"super_block_alignment_space_overhead_ratio",
         {offsetof(struct BlockBasedTableOptions,
                   super_block_alignment_space_overhead_ratio),
          OptionType::kSizeT, OptionVerificationType::kNormal}},
        {"pin_top_level_index_and_filter",
         {offsetof(struct BlockBasedTableOptions,
                   pin_top_level_index_and_filter),
          OptionType::kBoolean, OptionVerificationType::kNormal}},
        {kOptNameMetadataCacheOpts,
         OptionTypeInfo::Struct(
             kOptNameMetadataCacheOpts, &metadata_cache_options_type_info,
             offsetof(struct BlockBasedTableOptions, metadata_cache_options),
             OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
        {"block_cache",
         {offsetof(struct BlockBasedTableOptions, block_cache),
          OptionType::kUnknown, OptionVerificationType::kNormal,
          (OptionTypeFlags::kCompareNever | OptionTypeFlags::kDontSerialize),
          // Parses the input value as a Cache
          [](const ConfigOptions& opts, const std::string&,
             const std::string& value, void* addr) {
            auto* cache = static_cast<std::shared_ptr<Cache>*>(addr);
            return Cache::CreateFromString(opts, value, cache);
          }}},
        {"block_cache_compressed",
         {0, OptionType::kUnknown, OptionVerificationType::kDeprecated}},
        {"max_auto_readahead_size",
         {offsetof(struct BlockBasedTableOptions, max_auto_readahead_size),
          OptionType::kSizeT, OptionVerificationType::kNormal}},
        {"prepopulate_block_cache",
         OptionTypeInfo::Enum<BlockBasedTableOptions::PrepopulateBlockCache>(
             offsetof(struct BlockBasedTableOptions, prepopulate_block_cache),
             &block_base_table_prepopulate_block_cache_string_map)},
        {"initial_auto_readahead_size",
         {offsetof(struct BlockBasedTableOptions, initial_auto_readahead_size),
          OptionType::kSizeT, OptionVerificationType::kNormal}},
        {"num_file_reads_for_auto_readahead",
         {offsetof(struct BlockBasedTableOptions,
                   num_file_reads_for_auto_readahead),
          OptionType::kUInt64T, OptionVerificationType::kNormal}},
        {"fail_if_no_udi_on_open",
         LegacyIndexBoolean(
             &BlockBasedTableOptions::fail_if_no_udi_on_open,
             BlockBasedTableOptions::IndexMode::kStandardRequired)},
        {"use_udi_as_primary_index",
         LegacyIndexBoolean(&BlockBasedTableOptions::use_udi_as_primary_index,
                            BlockBasedTableOptions::IndexMode::kCustomDefault)},
    };
  }
} block_based_table_type_info;

Status ValidateIndexMode(BlockBasedTableOptions::IndexMode index_mode) {
  switch (index_mode) {
    case BlockBasedTableOptions::IndexMode::kStandardOnly:
    case BlockBasedTableOptions::IndexMode::kStandardDefault:
    case BlockBasedTableOptions::IndexMode::kStandardRequired:
    case BlockBasedTableOptions::IndexMode::kCustomDefault:
    case BlockBasedTableOptions::IndexMode::kCustomOnly:
      return Status::OK();
    default:
      return Status::InvalidArgument(
          "Unrecognized index_mode: " +
          std::to_string(lossless_cast<int>(index_mode)));
  }
}

void BlockBasedTableFactory::UpdateIndexMode() {
  if (index_mode_explicit_) {
    // Effective options can be copied into a new factory. Clear ignored legacy
    // inputs so an explicit kStandardDefault survives that reconstruction.
    table_options_.use_udi_as_primary_index = false;
    table_options_.fail_if_no_udi_on_open = false;
    skip_standard_index_ = false;
    return;
  }
  using IndexMode = BlockBasedTableOptions::IndexMode;
  if (skip_standard_index_) {
    table_options_.index_mode = IndexMode::kCustomOnly;
  } else {
    // Keep the requested default instead of storing a derived mode that would
    // become explicit when callers copy these options into a new factory.
    table_options_.index_mode = IndexMode::kStandardDefault;
  }
}

// TODO(myabandeh): We should return an error instead of silently changing the
// options
BlockBasedTableFactory::BlockBasedTableFactory(
    const BlockBasedTableOptions& _table_options)
    : table_options_(_table_options),
      index_mode_explicit_(_table_options.index_mode !=
                           BlockBasedTableOptions::IndexMode::kStandardDefault),
      shared_state_(std::make_shared<SharedState>()) {
  UpdateIndexMode();
  InitializeOptions();
  RegisterOptions(&table_options_, &block_based_table_type_info.info);
  static const std::unordered_map<std::string, OptionTypeInfo>
      skip_standard_type_info = {
          {"skip_standard_index",
           {0, OptionType::kBoolean, OptionVerificationType::kNormal,
            OptionTypeFlags::kDontSerialize | OptionTypeFlags::kCompareNever}}};
  RegisterOptions("LegacyIndexOptions", &skip_standard_index_,
                  &skip_standard_type_info);
  // Old OPTIONS files serialized a default index_mode even when the legacy
  // flags controlled routing. Newly serialized modes are authoritative.
  static const std::unordered_map<std::string, OptionTypeInfo>
      index_mode_encoding_type_info = {
          {"index_mode_explicit",
           {0, OptionType::kBoolean, OptionVerificationType::kNormal,
            OptionTypeFlags::kCompareNever,
            [](const ConfigOptions&, const std::string&,
               const std::string& value, void*) {
              if (value == "true" || value == "1") {
                return Status::OK();
              }
              return Status::InvalidArgument(
                  "index_mode_explicit must be true");
            },
            [](const ConfigOptions&, const std::string&, const void*,
               std::string* value) {
              *value = "true";
              return Status::OK();
            }}}};
  RegisterOptions("IndexModeEncoding", &index_mode_explicit_,
                  &index_mode_encoding_type_info);

  const auto table_reader_charged =
      table_options_.cache_usage_options.options_overrides
          .at(CacheEntryRole::kBlockBasedTableReader)
          .charged;
  if (table_options_.block_cache &&
      table_reader_charged == CacheEntryRoleOptions::Decision::kEnabled) {
    shared_state_->table_reader_cache_res_mgr =
        std::make_shared<ConcurrentCacheReservationManager>(
            std::make_shared<CacheReservationManagerImpl<
                CacheEntryRole::kBlockBasedTableReader>>(
                table_options_.block_cache));
  }
}

void BlockBasedTableFactory::InitializeOptions() {
  if (table_options_.flush_block_policy_factory == nullptr) {
    table_options_.flush_block_policy_factory.reset(
        new FlushBlockBySizePolicyFactory());
  }
  if (table_options_.no_block_cache) {
    table_options_.block_cache.reset();
  } else if (table_options_.block_cache == nullptr) {
    // Now using AutoHCC by default, with existing default size of 32MB
    // which is just one cache shard in HCC
    HyperClockCacheOptions hcc_opts{size_t{32} << 20};
    table_options_.block_cache = hcc_opts.MakeSharedCache();
  }
  if (table_options_.block_size_deviation < 0 ||
      table_options_.block_size_deviation > 100) {
    table_options_.block_size_deviation = 0;
  }
  if (table_options_.block_restart_interval < 1) {
    table_options_.block_restart_interval = 1;
  }
  if (table_options_.index_block_restart_interval < 1) {
    table_options_.index_block_restart_interval = 1;
  }
  if (table_options_.index_type == BlockBasedTableOptions::kHashSearch &&
      table_options_.index_block_restart_interval != 1) {
    // Currently kHashSearch is incompatible with
    // index_block_restart_interval > 1
    table_options_.index_block_restart_interval = 1;
  }
  if (table_options_.partition_filters &&
      table_options_.index_type !=
          BlockBasedTableOptions::kTwoLevelIndexSearch) {
    // We do not support partitioned filters without partitioning indexes
    table_options_.partition_filters = false;
  }
  auto& options_overrides =
      table_options_.cache_usage_options.options_overrides;
  const auto options = table_options_.cache_usage_options.options;
  for (std::uint32_t i = 0; i < kNumCacheEntryRoles; ++i) {
    CacheEntryRole role = static_cast<CacheEntryRole>(i);
    auto options_overrides_iter = options_overrides.find(role);
    if (options_overrides_iter == options_overrides.end()) {
      options_overrides.insert({role, options});
    } else if (options_overrides_iter->second.charged ==
               CacheEntryRoleOptions::Decision::kFallback) {
      options_overrides_iter->second.charged = options.charged;
    }
  }

  if (table_options_.format_version < kMinSupportedBbtFormatVersionForWrite) {
    // In TEST mode, allow writing format versions that are at least supported
    // for reading (so that we have a way of testing the read side).
    if (TEST_AllowUnsupportedFormatVersion()) {
      if (table_options_.format_version <
          kMinSupportedBbtFormatVersionForRead) {
        table_options_.format_version = kMinSupportedBbtFormatVersionForWrite;
      }
    } else {
      table_options_.format_version = kMinSupportedBbtFormatVersionForWrite;
    }
  }
  // NOTE: do not sanitize too high format_version, so that it can be rejected
  // in validation
}

Status BlockBasedTableFactory::PrepareOptions(const ConfigOptions& opts) {
  InitializeOptions();
  return TableFactory::PrepareOptions(opts);
}

namespace {
// Different cache kinds use the same keys for physically different values, so
// they must not share an underlying key space with each other.
Status CheckCacheOptionCompatibility(const BlockBasedTableOptions& bbto) {
  int cache_count =
      (bbto.block_cache != nullptr) + (bbto.persistent_cache != nullptr);
  if (cache_count <= 1) {
    // Nothing to share / overlap
    return Status::OK();
  }

  // More complex test of shared key space, in case the instances are wrappers
  // for some shared underlying cache.
  static Cache::CacheItemHelper kHelper{CacheEntryRole::kMisc};
  CacheKey sentinel_key = CacheKey::CreateUniqueForProcessLifetime();
  struct SentinelValue {
    explicit SentinelValue(char _c) : c(_c) {}
    char c;
  };
  static SentinelValue kRegularBlockCacheMarker{'b'};
  static char kPersistentCacheMarker{'p'};
  if (bbto.block_cache) {
    bbto.block_cache
        ->Insert(sentinel_key.AsSlice(), &kRegularBlockCacheMarker, &kHelper, 1)
        .PermitUncheckedError();
  }
  if (bbto.persistent_cache) {
    // Note: persistent cache copies the data, not keeping the pointer
    bbto.persistent_cache
        ->Insert(sentinel_key.AsSlice(), &kPersistentCacheMarker, 1)
        .PermitUncheckedError();
  }
  // If we get something different from what we inserted, that indicates
  // dangerously overlapping key spaces.
  if (bbto.block_cache) {
    auto handle = bbto.block_cache->Lookup(sentinel_key.AsSlice());
    if (handle) {
      auto v = static_cast<SentinelValue*>(bbto.block_cache->Value(handle));
      char c = v->c;
      bbto.block_cache->Release(handle);
      if (c == kPersistentCacheMarker) {
        return Status::InvalidArgument(
            "block_cache and persistent_cache share the same key space, "
            "which is not supported");
      } else if (v != &kRegularBlockCacheMarker) {
        return Status::Corruption("Unexpected mutation to block_cache");
      }
    }
  }

  if (bbto.persistent_cache) {
    std::unique_ptr<char[]> data;
    size_t size = 0;
    bbto.persistent_cache->Lookup(sentinel_key.AsSlice(), &data, &size)
        .PermitUncheckedError();
    if (data && size > 0) {
      if (data[0] == kRegularBlockCacheMarker.c) {
        return Status::InvalidArgument(
            "persistent_cache and block_cache share the same key space, "
            "which is not supported");
      } else if (data[0] != kPersistentCacheMarker) {
        return Status::Corruption("Unexpected mutation to persistent_cache");
      }
    }
  }
  return Status::OK();
}

}  // namespace

Status BlockBasedTableFactory::NewTableReader(
    const ReadOptions& ro, const TableReaderOptions& table_reader_options,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    std::unique_ptr<TableReader>* table_reader,
    bool prefetch_index_and_filter_in_cache) const {
  return BlockBasedTable::Open(
      ro, table_reader_options.ioptions, table_reader_options.env_options,
      table_options_, table_reader_options.internal_comparator, std::move(file),
      file_size, table_reader_options.block_protection_bytes_per_key,
      table_reader, table_reader_options.tail_size,
      shared_state_->table_reader_cache_res_mgr,
      table_reader_options.prefix_extractor,
      table_reader_options.compression_manager,
      prefetch_index_and_filter_in_cache, table_reader_options.skip_filters,
      table_reader_options.level, table_reader_options.immortal,
      table_reader_options.largest_seqno,
      table_reader_options.force_direct_prefetch,
      &shared_state_->tail_prefetch_stats,
      table_reader_options.block_cache_tracer,
      table_reader_options.max_file_size_for_l0_meta_pin,
      table_reader_options.cur_db_session_id, table_reader_options.cur_file_num,
      table_reader_options.unique_id,
      table_reader_options.user_defined_timestamps_persisted,
      table_reader_options.avoid_shared_metadata_cache,
      table_reader_options.blob_source);
}

TableBuilder* BlockBasedTableFactory::NewTableBuilder(
    const TableBuilderOptions& table_builder_options,
    WritableFileWriter* file) const {
  // BlockBasedTableBuilder self-detects embedded-blob mode from
  // table_builder_options.embedded_blob_options.
  return new BlockBasedTableBuilder(table_options_, table_builder_options,
                                    file);
}

Status BlockBasedTableFactory::ValidateOptions(
    const DBOptions& db_opts, const ColumnFamilyOptions& cf_opts) const {
  Status index_mode_status =
      ValidateIndexMode(table_options_.GetEffectiveIndexMode());
  if (!index_mode_status.ok()) {
    return index_mode_status;
  }
  if (table_options_.index_type == BlockBasedTableOptions::kHashSearch &&
      cf_opts.prefix_extractor == nullptr) {
    return Status::InvalidArgument(
        "Hash index is specified for block-based "
        "table, but prefix_extractor is not given");
  }
  if (table_options_.index_block_search_type ==
      BlockBasedTableOptions::kInterpolation) {
    // Interpolation search requires BytewiseComparator
    if (cf_opts.comparator != BytewiseComparator()) {
      return Status::InvalidArgument(
          "Interpolation search requires BytewiseComparator");
    }
  }
  // See IndexFactoryOptions for why the two can't be combined. The table
  // builder repeats this check because SstFileWriter and SetOptions never reach
  // ValidateOptions; catching it here fails DB::Open instead of letting the DB
  // open and take writes until the first flush stops it with a fatal error.
  if (table_options_.GetEffectiveIndexMode() !=
          BlockBasedTableOptions::IndexMode::kStandardOnly &&
      table_options_.user_defined_index_factory != nullptr &&
      cf_opts.comparator != nullptr &&
      cf_opts.comparator->timestamp_size() > 0) {
    return Status::InvalidArgument(
        "user_defined_index_factory is incompatible with user-defined "
        "timestamps");
  }
  if (table_options_.cache_index_and_filter_blocks &&
      table_options_.no_block_cache) {
    return Status::InvalidArgument(
        "Enable cache_index_and_filter_blocks, "
        ", but block cache is disabled");
  }
  if (table_options_.pin_l0_filter_and_index_blocks_in_cache &&
      table_options_.no_block_cache) {
    return Status::InvalidArgument(
        "Enable pin_l0_filter_and_index_blocks_in_cache, "
        ", but block cache is disabled");
  }
  // In TEST mode, also allow writing
  // (a) old format_versions that for users are only supported for reads
  // (b) future "draft" format versions that are not yet published to users
  if (!(IsSupportedFormatVersionForWrite(kBlockBasedTableMagicNumber,
                                         table_options_.format_version) ||
        (TEST_AllowUnsupportedFormatVersion() &&
         table_options_.format_version >=
             kMinSupportedBbtFormatVersionForRead))) {
    return Status::InvalidArgument(
        "Unsupported BlockBasedTable format_version. Please check "
        "include/rocksdb/table.h for more info");
  }
  bool using_builtin_compatible_compression = true;
  if (cf_opts.compression_manager &&
      strcmp(cf_opts.compression_manager->CompatibilityName(),
             GetBuiltinV2CompressionManager()->CompatibilityName()) != 0) {
    if (FormatVersionUsesCompressionManagerName(
            table_options_.format_version)) {
      using_builtin_compatible_compression = false;
    } else {
      return Status::InvalidArgument(
          "Using a CompressionManager incompatible with built-in (custom "
          "CompatibilityName()) is not supported for format_version < 7");
    }
  }
  auto validate_compression_type_fn = [&](CompressionType ctype,
                                          const char* context) {
    if (ctype == kNoCompression) {
      return Status::OK();
    }
    if (ctype == kDisableCompressionOption) {
      if (strcmp(context, "compression") == 0) {
        return Status::InvalidArgument(
            "kDisableCompressionOption not permitted for option: "
            "compression");
      } else {
        return Status::OK();
      }
    }
    if (table_options_.block_align) {
      return Status::InvalidArgument("Enable block_align, but " +
                                     std::string(context) + " enabled");
    }
    if (ctype > kLastBuiltinCompression &&
        using_builtin_compatible_compression) {
      return Status::InvalidArgument(
          "Using a CompressionType other than built-in ...");  // TODO
    }
    // Otherwise
    return Status::OK();
  };
  {
    Status s = validate_compression_type_fn(cf_opts.compression, "compression");
    if (!s.ok()) {
      return s;
    }
    s = validate_compression_type_fn(cf_opts.bottommost_compression,
                                     "bottommost_compression");
    if (!s.ok()) {
      return s;
    }
    for (auto ctype : cf_opts.compression_per_level) {
      s = validate_compression_type_fn(ctype, "compression_per_level");
      if (!s.ok()) {
        return s;
      }
    }
  }
  if (table_options_.block_align &&
      (table_options_.block_size & (table_options_.block_size - 1))) {
    return Status::InvalidArgument(
        "Block alignment requested but block size is not a power of 2");
  }
  if (table_options_.block_size > std::numeric_limits<uint32_t>::max()) {
    return Status::InvalidArgument(
        "block size exceeds maximum number (4GiB) allowed");
  }
  if ((table_options_.super_block_alignment_size &
       (table_options_.super_block_alignment_size - 1))) {
    return Status::InvalidArgument(
        "Super Block alignment requested but super block alignment size is not "
        "a power of 2");
  }
  if (table_options_.super_block_alignment_size >
      std::numeric_limits<uint32_t>::max()) {
    return Status::InvalidArgument(
        "Super block alignment size exceeds maximum number (4GiB) allowed");
  }
  if (table_options_.super_block_alignment_space_overhead_ratio > 0 &&
      table_options_.super_block_alignment_space_overhead_ratio < 4) {
    return Status::InvalidArgument(
        "Super block alignment space overhead is too high");
  }
  if (table_options_.data_block_index_type ==
          BlockBasedTableOptions::kDataBlockBinaryAndHash &&
      table_options_.data_block_hash_table_util_ratio <= 0) {
    return Status::InvalidArgument(
        "data_block_hash_table_util_ratio should be greater than 0 when "
        "data_block_index_type is set to kDataBlockBinaryAndHash");
  }
  if (table_options_.GetEffectiveIndexMode() ==
          BlockBasedTableOptions::IndexMode::kCustomDefault ||
      table_options_.GetEffectiveIndexMode() ==
          BlockBasedTableOptions::IndexMode::kCustomOnly) {
    if (!table_options_.user_defined_index_factory) {
      return Status::InvalidArgument(
          "index_mode kCustomDefault/kCustomOnly requires "
          "user_defined_index_factory");
    }
    if (table_options_.GetEffectiveIndexMode() ==
            BlockBasedTableOptions::IndexMode::kCustomOnly &&
        table_options_.format_version < 6) {
      return Status::InvalidArgument(
          "index_mode kCustomOnly requires format_version >= 6; got " +
          std::to_string(table_options_.format_version));
    }
    // Whether parallel compression is usable with a particular UDI is a
    // per-implementation property
    // (IndexFactoryBuilder::SupportsParallelAddEntry). The table builder makes
    // the actual decision when builders are created (see
    // BlockBasedTableBuilder::Rep::Rep) and silently falls back to
    // single-threaded when any builder doesn't support the parallel
    // protocol. We don't reject the configuration here.
    if (table_options_.index_type ==
        BlockBasedTableOptions::kTwoLevelIndexSearch) {
      return Status::InvalidArgument(
          "index_mode kCustomDefault/kCustomOnly is incompatible with "
          "partitioned index (kTwoLevelIndexSearch).");
    }
    if (table_options_.partition_filters) {
      return Status::InvalidArgument(
          "index_mode kCustomDefault/kCustomOnly is incompatible with "
          "partitioned filters.");
    }
  }
  if (db_opts.unordered_write && cf_opts.max_successive_merges > 0) {
    // TODO(myabandeh): support it
    return Status::InvalidArgument(
        "max_successive_merges larger than 0 is currently inconsistent with "
        "unordered_write");
  }
  const auto& options_overrides =
      table_options_.cache_usage_options.options_overrides;
  for (auto options_overrides_iter = options_overrides.cbegin();
       options_overrides_iter != options_overrides.cend();
       ++options_overrides_iter) {
    const CacheEntryRole role = options_overrides_iter->first;
    const CacheEntryRoleOptions options = options_overrides_iter->second;
    static const std::set<CacheEntryRole> kMemoryChargingSupported = {
        CacheEntryRole::kCompressionDictionaryBuildingBuffer,
        CacheEntryRole::kFilterConstruction,
        CacheEntryRole::kBlockBasedTableReader, CacheEntryRole::kFileMetadata,
        CacheEntryRole::kBlobCache};
    if (options.charged != CacheEntryRoleOptions::Decision::kFallback &&
        kMemoryChargingSupported.count(role) == 0) {
      return Status::NotSupported(
          "Enable/Disable CacheEntryRoleOptions::charged"
          " for CacheEntryRole " +
          kCacheEntryRoleToCamelString[static_cast<uint32_t>(role)] +
          " is not supported");
    }
    if (table_options_.no_block_cache &&
        options.charged == CacheEntryRoleOptions::Decision::kEnabled) {
      return Status::InvalidArgument(
          "Enable CacheEntryRoleOptions::charged"
          " for CacheEntryRole " +
          kCacheEntryRoleToCamelString[static_cast<uint32_t>(role)] +
          " but block cache is disabled");
    }
    if (role == CacheEntryRole::kBlobCache &&
        options.charged == CacheEntryRoleOptions::Decision::kEnabled) {
      if (cf_opts.blob_cache == nullptr) {
        return Status::InvalidArgument(
            "Enable CacheEntryRoleOptions::charged"
            " for CacheEntryRole " +
            kCacheEntryRoleToCamelString[static_cast<uint32_t>(role)] +
            " but blob cache is not configured");
      }
      if (table_options_.no_block_cache) {
        return Status::InvalidArgument(
            "Enable CacheEntryRoleOptions::charged"
            " for CacheEntryRole " +
            kCacheEntryRoleToCamelString[static_cast<uint32_t>(role)] +
            " but block cache is disabled");
      }
      if (table_options_.block_cache == cf_opts.blob_cache) {
        return Status::InvalidArgument(
            "Enable CacheEntryRoleOptions::charged"
            " for CacheEntryRole " +
            kCacheEntryRoleToCamelString[static_cast<uint32_t>(role)] +
            " but blob cache is the same as block cache");
      }
      if (cf_opts.blob_cache->GetCapacity() >
          table_options_.block_cache->GetCapacity()) {
        return Status::InvalidArgument(
            "Enable CacheEntryRoleOptions::charged"
            " for CacheEntryRole " +
            kCacheEntryRoleToCamelString[static_cast<uint32_t>(role)] +
            " but blob cache capacity is larger than block cache capacity");
      }
    }
  }
  {
    Status s = CheckCacheOptionCompatibility(table_options_);
    if (!s.ok()) {
      return s;
    }
  }
  std::string garbage;
  if (!SerializeEnum<ChecksumType>(checksum_type_string_map,
                                   table_options_.checksum, &garbage)) {
    return Status::InvalidArgument(
        "Unrecognized ChecksumType for checksum: " +
        std::to_string(static_cast<uint32_t>(table_options_.checksum)));
  }
  return TableFactory::ValidateOptions(db_opts, cf_opts);
}

std::string BlockBasedTableFactory::GetPrintableOptions() const {
  std::string ret;
  ret.reserve(20000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];

  snprintf(buffer, kBufferSize, "  flush_block_policy_factory: %s (%p)\n",
           table_options_.flush_block_policy_factory->Name(),
           static_cast<void*>(table_options_.flush_block_policy_factory.get()));
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  cache_index_and_filter_blocks: %d\n",
           table_options_.cache_index_and_filter_blocks);
  ret.append(buffer);
  snprintf(buffer, kBufferSize,
           "  cache_index_and_filter_blocks_with_high_priority: %d\n",
           table_options_.cache_index_and_filter_blocks_with_high_priority);
  ret.append(buffer);
  snprintf(buffer, kBufferSize,
           "  pin_l0_filter_and_index_blocks_in_cache: %d\n",
           table_options_.pin_l0_filter_and_index_blocks_in_cache);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  pin_top_level_index_and_filter: %d\n",
           table_options_.pin_top_level_index_and_filter);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  index_type: %d\n",
           table_options_.index_type);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  data_block_index_type: %d\n",
           table_options_.data_block_index_type);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  optimize_key_common_prefix: %d\n",
           static_cast<int>(table_options_.optimize_key_common_prefix));
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  index_shortening: %d\n",
           static_cast<int>(table_options_.index_shortening));
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  data_block_hash_table_util_ratio: %lf\n",
           table_options_.data_block_hash_table_util_ratio);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  checksum: %d\n", table_options_.checksum);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  no_block_cache: %d\n",
           table_options_.no_block_cache);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  block_cache: %p\n",
           static_cast<void*>(table_options_.block_cache.get()));
  ret.append(buffer);
  if (table_options_.block_cache) {
    const char* block_cache_name = table_options_.block_cache->Name();
    if (block_cache_name != nullptr) {
      snprintf(buffer, kBufferSize, "  block_cache_name: %s\n",
               block_cache_name);
      ret.append(buffer);
    }
    ret.append("  block_cache_options:\n");
    ret.append(table_options_.block_cache->GetPrintableOptions());
  }
  snprintf(buffer, kBufferSize, "  persistent_cache: %p\n",
           static_cast<void*>(table_options_.persistent_cache.get()));
  ret.append(buffer);
  if (table_options_.persistent_cache) {
    snprintf(buffer, kBufferSize, "  persistent_cache_options:\n");
    ret.append(buffer);
    ret.append(table_options_.persistent_cache->GetPrintableOptions());
  }
  snprintf(buffer, kBufferSize, "  block_size: %" PRIu64 "\n",
           table_options_.block_size);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  block_size_deviation: %d\n",
           table_options_.block_size_deviation);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  block_restart_interval: %d\n",
           table_options_.block_restart_interval);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  index_block_restart_interval: %d\n",
           table_options_.index_block_restart_interval);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  metadata_block_size: %" PRIu64 "\n",
           table_options_.metadata_block_size);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  partition_filters: %d\n",
           table_options_.partition_filters);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  use_delta_encoding: %d\n",
           table_options_.use_delta_encoding);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  filter_policy: %s\n",
           table_options_.filter_policy == nullptr
               ? "nullptr"
               : table_options_.filter_policy->Name());
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  user_defined_index_factory: %s\n",
           table_options_.user_defined_index_factory == nullptr
               ? "nullptr"
               : table_options_.user_defined_index_factory->Name());
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  index_mode: %d\n",
           static_cast<int>(table_options_.GetEffectiveIndexMode()));
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  whole_key_filtering: %d\n",
           table_options_.whole_key_filtering);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  verify_compression: %d\n",
           table_options_.verify_compression);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  read_amp_bytes_per_bit: %d\n",
           table_options_.read_amp_bytes_per_bit);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  format_version: %d\n",
           table_options_.format_version);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  uniform_cv_threshold: %lf\n",
           table_options_.uniform_cv_threshold);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  enable_index_compression: %d\n",
           table_options_.enable_index_compression);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  block_align: %d\n",
           table_options_.block_align);
  ret.append(buffer);
  snprintf(buffer, kBufferSize,
           "  super_block_alignment_size: %" ROCKSDB_PRIszt "\n",
           table_options_.super_block_alignment_size);
  ret.append(buffer);
  snprintf(buffer, kBufferSize,
           "  super_block_alignment_space_overhead_ratio: %" ROCKSDB_PRIszt
           "\n",
           table_options_.super_block_alignment_space_overhead_ratio);
  ret.append(buffer);
  snprintf(buffer, kBufferSize,
           "  max_auto_readahead_size: %" ROCKSDB_PRIszt "\n",
           table_options_.max_auto_readahead_size);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  prepopulate_block_cache: %d\n",
           static_cast<int>(table_options_.prepopulate_block_cache));
  ret.append(buffer);
  snprintf(buffer, kBufferSize,
           "  initial_auto_readahead_size: %" ROCKSDB_PRIszt "\n",
           table_options_.initial_auto_readahead_size);
  ret.append(buffer);
  snprintf(buffer, kBufferSize,
           "  num_file_reads_for_auto_readahead: %" PRIu64 "\n",
           table_options_.num_file_reads_for_auto_readahead);
  ret.append(buffer);
  return ret;
}

const void* BlockBasedTableFactory::GetOptionsPtr(
    const std::string& name) const {
  if (name == kBlockCacheOpts()) {
    if (table_options_.no_block_cache) {
      return nullptr;
    } else {
      return table_options_.block_cache.get();
    }
  } else {
    return TableFactory::GetOptionsPtr(name);
  }
}

// Take a default BlockBasedTableOptions "table_options" in addition to a
// map "opts_map" of option name to option value to construct the new
// BlockBasedTableOptions "new_table_options".
//
// Below are the instructions of how to config some non-primitive-typed
// options in BlockBasedTableOptions:
//
// * filter_policy:
//   We currently only support the following FilterPolicy in the convenience
//   functions:
//   - BloomFilter: use "bloomfilter:[bits_per_key]:[use_block_based_builder]"
//     to specify BloomFilter.  The above string is equivalent to calling
//     NewBloomFilterPolicy(bits_per_key, use_block_based_builder).
//     [Example]:
//     - Pass {"filter_policy", "bloomfilter:4:true"} in
//       GetBlockBasedTableOptionsFromMap to use a BloomFilter with 4-bits
//       per key and use_block_based_builder enabled.
//
// * block_cache / block_cache_compressed:
//   We currently only support LRU cache in the GetOptions API.  The LRU
//   cache can be set by directly specifying its size.
//   [Example]:
//   - Passing {"block_cache", "1M"} in GetBlockBasedTableOptionsFromMap is
//     equivalent to setting block_cache using NewLRUCache(1024 * 1024).
//
// @param table_options the default options of the output "new_table_options".
// @param opts_map an option name to value map for specifying how
//     "new_table_options" should be set.
// @param new_table_options the resulting options based on "table_options"
//     with the change specified in "opts_map".
// @param input_strings_escaped when set to true, each escaped characters
//     prefixed by '\' in the values of the opts_map will be further converted
//     back to the raw string before assigning to the associated options.
// @param ignore_unknown_options when set to true, unknown options are ignored
//     instead of resulting in an unknown-option error.
// @return Status::OK() on success.  Otherwise, a non-ok status indicating
//     error will be returned, and "new_table_options" will be set to
//     "table_options".
Status BlockBasedTableFactory::ParseOption(const ConfigOptions& config_options,
                                           const OptionTypeInfo& opt_info,
                                           const std::string& opt_name,
                                           const std::string& opt_value,
                                           void* opt_ptr) {
  Status status = TableFactory::ParseOption(config_options, opt_info, opt_name,
                                            opt_value, opt_ptr);
  if (status.ok()) {
    if (opt_name == "index_mode") {
      index_mode_explicit_ = true;
      UpdateIndexMode();
    } else if (opt_name == "use_udi_as_primary_index" ||
               opt_name == "fail_if_no_udi_on_open" ||
               opt_name == "skip_standard_index") {
      UpdateIndexMode();
    }
  }
  if (config_options.input_strings_escaped && !status.ok()) {  // Got an error
    // !input_strings_escaped indicates the old API, where everything is
    // parsable.
    if (opt_info.IsByName()) {
      status = Status::OK();
    }
  }
  return status;
}

Status BlockBasedTableFactory::ConfigureOptions(
    const ConfigOptions& config_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    std::unordered_map<std::string, std::string>* unused) {
  const bool was_explicit = index_mode_explicit_;
  const bool skip_standard = skip_standard_index_;
  const bool use_as_primary = table_options_.use_udi_as_primary_index;
  const bool fail_if_missing = table_options_.fail_if_no_udi_on_open;
  const BlockBasedTableOptions::IndexMode mode = table_options_.index_mode;
  const std::unordered_map<std::string, std::string>::const_iterator mode_iter =
      opts_map.find("index_mode");
  const bool legacy_encoding =
      mode_iter != opts_map.end() && mode_iter->second == "kStandardDefault" &&
      opts_map.find("index_mode_explicit") == opts_map.end() &&
      (opts_map.find("use_udi_as_primary_index") != opts_map.end() ||
       opts_map.find("fail_if_no_udi_on_open") != opts_map.end() ||
       opts_map.find("skip_standard_index") != opts_map.end());
  Status s;
  if (legacy_encoding) {
    // Drop the old serialized default before parsing, so map iteration order
    // cannot clear or override the legacy flags.
    std::unordered_map<std::string, std::string> legacy_opts = opts_map;
    legacy_opts.erase("index_mode");
    index_mode_explicit_ = false;
    s = TableFactory::ConfigureOptions(config_options, legacy_opts, unused);
  } else {
    s = TableFactory::ConfigureOptions(config_options, opts_map, unused);
  }
  if (!s.ok()) {
    // The base rollback serializes the effective mode, not its legacy inputs.
    index_mode_explicit_ = was_explicit;
    skip_standard_index_ = skip_standard;
    table_options_.use_udi_as_primary_index = use_as_primary;
    table_options_.fail_if_no_udi_on_open = fail_if_missing;
    table_options_.index_mode = mode;
  }
  return s;
}

Status GetBlockBasedTableOptionsFromString(
    const ConfigOptions& config_options,
    const BlockBasedTableOptions& table_options, const std::string& opts_str,
    BlockBasedTableOptions* new_table_options) {
  std::unordered_map<std::string, std::string> opts_map;
  Status s = StringToMap(opts_str, &opts_map);
  if (!s.ok()) {
    return s;
  }
  s = GetBlockBasedTableOptionsFromMap(config_options, table_options, opts_map,
                                       new_table_options);
  // Translate any errors (NotFound, NotSupported, to InvalidArgument
  if (s.ok() || s.IsInvalidArgument()) {
    return s;
  } else {
    return Status::InvalidArgument(s.getState());
  }
}

Status GetBlockBasedTableOptionsFromMap(
    const ConfigOptions& config_options,
    const BlockBasedTableOptions& table_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    BlockBasedTableOptions* new_table_options) {
  assert(new_table_options);
  BlockBasedTableFactory bbtf(table_options);
  Status s = bbtf.ConfigureFromMap(config_options, opts_map);
  if (s.ok()) {
    *new_table_options = *(bbtf.GetOptions<BlockBasedTableOptions>());
  } else {
    *new_table_options = table_options;
  }
  return s;
}

TableFactory* NewBlockBasedTableFactory(
    const BlockBasedTableOptions& _table_options) {
  return new BlockBasedTableFactory(_table_options);
}

Status IndexFactory::CreateFromString(const ConfigOptions& config_options,
                                      const std::string& value,
                                      std::shared_ptr<IndexFactory>* factory) {
  static std::once_flag once;
  std::call_once(once, [&]() {
    trie_index::RegisterBuiltinTrieIndexFactory(
        *(ObjectLibrary::Default().get()), "");
  });
  return LoadSharedObject<IndexFactory>(config_options, value, factory);
}

const std::string BlockBasedTablePropertyNames::kIndexType =
    "rocksdb.block.based.table.index.type";
const std::string BlockBasedTablePropertyNames::kWholeKeyFiltering =
    "rocksdb.block.based.table.whole.key.filtering";
const std::string BlockBasedTablePropertyNames::kPrefixFiltering =
    "rocksdb.block.based.table.prefix.filtering";
const std::string BlockBasedTablePropertyNames::kDecoupledPartitionedFilters =
    "rocksdb.block.based.table.decoupled.partitioned.filters";
const std::string kHashIndexPrefixesBlock = "rocksdb.hashindex.prefixes";
const std::string kHashIndexPrefixesMetadataBlock =
    "rocksdb.hashindex.metadata";
const std::string kPropTrue = "1";
const std::string kPropFalse = "0";

}  // namespace ROCKSDB_NAMESPACE
