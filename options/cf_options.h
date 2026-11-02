// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <vector>

#include "db/dbformat.h"
#include "options/db_options.h"
#include "rocksdb/options.h"
#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {

// ImmutableCFOptions is a data struct used by RocksDB internal. It contains a
// subset of Options that should not be changed during the entire lifetime
// of DB. Raw pointers defined in this struct do not have ownership to the data
// they point to. Options contains std::shared_ptr to these data.
struct ImmutableCFOptions {
 public:
  static const char* kName() { return "ImmutableCFOptions"; }
  explicit ImmutableCFOptions();
  explicit ImmutableCFOptions(const ColumnFamilyOptions& cf_options);

  CompactionStyle compaction_style;

  CompactionPri compaction_pri;

  const Comparator* user_comparator;
  InternalKeyComparator internal_comparator;  // Only in Immutable

  std::shared_ptr<MergeOperator> merge_operator;

  const CompactionFilter* compaction_filter;

  std::shared_ptr<CompactionFilterFactory> compaction_filter_factory;

  int min_write_buffer_number_to_merge;

  int64_t max_write_buffer_size_to_maintain;

  bool inplace_update_support;

  UpdateStatus (*inplace_callback)(char* existing_value,
                                   uint32_t* existing_value_size,
                                   Slice delta_value,
                                   std::string* merged_value);

  std::shared_ptr<MemTableRepFactory> memtable_factory;

  Options::TablePropertiesCollectorFactories
      table_properties_collector_factories;

  // This options is required by PlainTableReader. May need to move it
  // to PlainTableOptions just like bloom_bits_per_key
  uint32_t bloom_locality;

  bool level_compaction_dynamic_level_bytes;

  int num_levels;

  bool optimize_filters_for_hits;

  bool force_consistency_checks;

  bool disallow_memtable_writes;

  Temperature default_temperature;

  std::shared_ptr<const SliceTransform>
      memtable_insert_with_hint_prefix_extractor;

  std::vector<DbPath> cf_paths;

  std::shared_ptr<ConcurrentTaskLimiter> compaction_thread_limiter;

  std::shared_ptr<SstPartitionerFactory> sst_partitioner_factory;

  std::shared_ptr<Cache> blob_cache;

  // Immutable snapshot of
  // AdvancedColumnFamilyOptions::enable_blob_direct_write.
  bool enable_blob_direct_write;

  // Immutable snapshot of
  // AdvancedColumnFamilyOptions::blob_direct_write_partitions.
  uint32_t blob_direct_write_partitions;

  // Immutable snapshot of
  // AdvancedColumnFamilyOptions::blob_direct_write_partition_strategy.
  std::shared_ptr<BlobFilePartitionStrategy>
      blob_direct_write_partition_strategy;

  bool persist_user_defined_timestamps;

  bool cf_allow_ingest_behind;

  bool memtable_batch_lookup_optimization;
};

struct ImmutableOptions : public ImmutableDBOptions, public ImmutableCFOptions {
  explicit ImmutableOptions();
  explicit ImmutableOptions(const Options& options);

  ImmutableOptions(const DBOptions& db_options,
                   const ColumnFamilyOptions& cf_options);

  ImmutableOptions(const ImmutableDBOptions& db_options,
                   const ImmutableCFOptions& cf_options);

  ImmutableOptions(const DBOptions& db_options,
                   const ImmutableCFOptions& cf_options);

  ImmutableOptions(const ImmutableDBOptions& db_options,
                   const ColumnFamilyOptions& cf_options);
};

struct MutableCFOptions {
  static const char* kName() { return "MutableCFOptions"; }
  explicit MutableCFOptions(const ColumnFamilyOptions& options)
      : write_buffer_size(options.write_buffer_size),
        max_write_buffer_number(options.max_write_buffer_number),
        arena_block_size(options.arena_block_size),
        memtable_prefix_bloom_size_ratio(
            options.memtable_prefix_bloom_size_ratio),
        memtable_whole_key_filtering(options.memtable_whole_key_filtering),
        memtable_huge_page_size(options.memtable_huge_page_size),
        max_successive_merges(options.max_successive_merges),
        strict_max_successive_merges(options.strict_max_successive_merges),
        inplace_update_num_locks(options.inplace_update_num_locks),
        prefix_extractor(options.prefix_extractor),
        experimental_mempurge_threshold(
            options.experimental_mempurge_threshold),
        disable_auto_compactions(options.disable_auto_compactions),
        table_factory(options.table_factory),
        soft_pending_compaction_bytes_limit(
            options.soft_pending_compaction_bytes_limit),
        hard_pending_compaction_bytes_limit(
            options.hard_pending_compaction_bytes_limit),
        level0_file_num_compaction_trigger(
            options.level0_file_num_compaction_trigger),
        level0_slowdown_writes_trigger(options.level0_slowdown_writes_trigger),
        level0_stop_writes_trigger(options.level0_stop_writes_trigger),
        max_compaction_bytes(options.max_compaction_bytes),
        target_file_size_base(options.target_file_size_base),
        target_file_size_multiplier(options.target_file_size_multiplier),
        target_file_size_is_upper_bound(
            options.target_file_size_is_upper_bound),
        max_bytes_for_level_base(options.max_bytes_for_level_base),
        max_bytes_for_level_multiplier(options.max_bytes_for_level_multiplier),
        ttl(options.ttl),
        periodic_compaction_seconds(options.periodic_compaction_seconds),
        read_triggered_compaction_threshold(
            options.read_triggered_compaction_threshold),
        max_bytes_for_level_multiplier_additional(
            options.max_bytes_for_level_multiplier_additional),
        compaction_options_fifo(options.compaction_options_fifo),
        compaction_options_universal(options.compaction_options_universal),
        preclude_last_level_data_seconds(
            options.preclude_last_level_data_seconds),
        preserve_internal_time_seconds(options.preserve_internal_time_seconds),
        verify_output_flags(options.verify_output_flags),
        enable_blob_files(options.enable_blob_files),
        min_blob_size(options.min_blob_size),
        blob_file_size(options.blob_file_size),
        blob_file_writable_file_max_buffer_size(
            options.blob_file_writable_file_max_buffer_size),
        blob_compression_type(options.blob_compression_type),
        blob_compression_opts(options.blob_compression_opts),
        enable_blob_garbage_collection(options.enable_blob_garbage_collection),
        blob_garbage_collection_age_cutoff(
            options.blob_garbage_collection_age_cutoff),
        blob_garbage_collection_force_threshold(
            options.blob_garbage_collection_force_threshold),
        blob_compaction_readahead_size(options.blob_compaction_readahead_size),
        blob_file_starting_level(options.blob_file_starting_level),
        prepopulate_blob_cache(options.prepopulate_blob_cache),
        max_sequential_skip_in_iterations(
            options.max_sequential_skip_in_iterations),
        paranoid_file_checks(options.paranoid_file_checks),
        report_bg_io_stats(options.report_bg_io_stats),
        compression(options.compression),
        bottommost_compression(options.bottommost_compression),
        compression_opts(options.compression_opts),
        bottommost_compression_opts(options.bottommost_compression_opts),
        compression_manager(options.compression_manager),
        last_level_temperature(options.last_level_temperature),
        default_write_temperature(options.default_write_temperature),
        memtable_protection_bytes_per_key(
            options.memtable_protection_bytes_per_key),
        block_protection_bytes_per_key(options.block_protection_bytes_per_key),
        paranoid_memory_checks(options.paranoid_memory_checks),
        memtable_verify_per_key_checksum_on_seek(
            options.memtable_verify_per_key_checksum_on_seek),
        sample_for_compression(
            options.sample_for_compression),  // TODO: is 0 fine here?
        compression_per_level(options.compression_per_level),
        memtable_max_range_deletions(options.memtable_max_range_deletions),
        bottommost_file_compaction_delay(
            options.bottommost_file_compaction_delay),
        uncache_aggressiveness(options.uncache_aggressiveness),
        memtable_op_scan_flush_trigger(options.memtable_op_scan_flush_trigger),
        memtable_avg_op_scan_flush_trigger(
            options.memtable_avg_op_scan_flush_trigger),
        min_tombstones_for_range_conversion(
            options.min_tombstones_for_range_conversion) {
    RefreshDerivedOptions(options.num_levels, options.compaction_style);
  }

  MutableCFOptions()
      : write_buffer_size(0),
        max_write_buffer_number(0),
        arena_block_size(0),
        memtable_prefix_bloom_size_ratio(0),
        memtable_whole_key_filtering(false),
        memtable_huge_page_size(0),
        max_successive_merges(0),
        strict_max_successive_merges(false),
        inplace_update_num_locks(0),
        prefix_extractor(nullptr),
        experimental_mempurge_threshold(0.0),
        disable_auto_compactions(false),
        soft_pending_compaction_bytes_limit(0),
        hard_pending_compaction_bytes_limit(0),
        level0_file_num_compaction_trigger(0),
        level0_slowdown_writes_trigger(0),
        level0_stop_writes_trigger(0),
        max_compaction_bytes(0),
        target_file_size_base(0),
        target_file_size_multiplier(0),
        target_file_size_is_upper_bound(false),
        max_bytes_for_level_base(0),
        max_bytes_for_level_multiplier(0),
        ttl(0),
        periodic_compaction_seconds(0),
        read_triggered_compaction_threshold(0.0),
        compaction_options_fifo(),
        preclude_last_level_data_seconds(0),
        preserve_internal_time_seconds(0),
        verify_output_flags(VerifyOutputFlags::kVerifyNone),
        enable_blob_files(false),
        min_blob_size(0),
        blob_file_size(0),
        blob_file_writable_file_max_buffer_size(0),
        blob_compression_type(kNoCompression),
        blob_compression_opts(),
        enable_blob_garbage_collection(false),
        blob_garbage_collection_age_cutoff(0.0),
        blob_garbage_collection_force_threshold(0.0),
        blob_compaction_readahead_size(0),
        blob_file_starting_level(0),
        prepopulate_blob_cache(PrepopulateBlobCache::kDisable),
        max_sequential_skip_in_iterations(0),
        paranoid_file_checks(false),
        report_bg_io_stats(false),
        compression(GetDefaultCompressionType()),
        bottommost_compression(kDisableCompressionOption),
        last_level_temperature(Temperature::kUnknown),
        default_write_temperature(Temperature::kUnknown),
        memtable_protection_bytes_per_key(0),
        block_protection_bytes_per_key(0),
        paranoid_memory_checks(false),
        memtable_verify_per_key_checksum_on_seek(false),
        sample_for_compression(0),
        memtable_max_range_deletions(0),
        bottommost_file_compaction_delay(0),
        uncache_aggressiveness(0),
        memtable_op_scan_flush_trigger(0),
        memtable_avg_op_scan_flush_trigger(0),
        min_tombstones_for_range_conversion(0) {}

  explicit MutableCFOptions(const Options& options);

  // Must be called after any change to MutableCFOptions
  void RefreshDerivedOptions(int num_levels, CompactionStyle compaction_style);

  void RefreshDerivedOptions(const ImmutableCFOptions& ioptions) {
    RefreshDerivedOptions(ioptions.num_levels, ioptions.compaction_style);
  }

  int MaxBytesMultiplerAdditional(int level) const {
    if (level >=
        static_cast<int>(max_bytes_for_level_multiplier_additional.size())) {
      return 1;
    }
    return max_bytes_for_level_multiplier_additional[level];
  }

  void Dump(Logger* log) const;

  bool operator==(const MutableCFOptions& rhs) const = default;

  // Memtable related options
  size_t write_buffer_size;
  int max_write_buffer_number;
  size_t arena_block_size;
  double memtable_prefix_bloom_size_ratio;
  bool memtable_whole_key_filtering;
  size_t memtable_huge_page_size;
  size_t max_successive_merges;
  bool strict_max_successive_merges;
  size_t inplace_update_num_locks;
  // NOTE: if too many shared_ptr make their way into MutableCFOptions, the
  // copy performance might suffer enough to warrant aggregating them in an
  // immutable+copy-on-write sub-object managed through a single shared_ptr.
  std::shared_ptr<const SliceTransform> prefix_extractor;
  // [experimental]
  // Used to activate or deactive the Mempurge feature (memtable garbage
  // collection). (deactivated by default). At every flush, the total useful
  // payload (total entries minus garbage entries) is estimated as a ratio
  // [useful payload bytes]/[size of a memtable (in bytes)]. This ratio is then
  // compared to this `threshold` value:
  //     - if ratio<threshold: the flush is replaced by a mempurge operation
  //     - else: a regular flush operation takes place.
  // Threshold values:
  //   0.0: mempurge deactivated (default).
  //   1.0: recommended threshold value.
  //   >1.0 : aggressive mempurge.
  //   0 < threshold < 1.0: mempurge triggered only for very low useful payload
  //   ratios.
  // [experimental]
  double experimental_mempurge_threshold;

  // Compaction related options
  bool disable_auto_compactions;
  std::shared_ptr<TableFactory> table_factory;
  uint64_t soft_pending_compaction_bytes_limit;
  uint64_t hard_pending_compaction_bytes_limit;
  int level0_file_num_compaction_trigger;
  int level0_slowdown_writes_trigger;
  int level0_stop_writes_trigger;
  uint64_t max_compaction_bytes;
  uint64_t target_file_size_base;
  int target_file_size_multiplier;
  bool target_file_size_is_upper_bound;
  uint64_t max_bytes_for_level_base;
  double max_bytes_for_level_multiplier;
  uint64_t ttl;
  uint64_t periodic_compaction_seconds;
  double read_triggered_compaction_threshold;
  std::vector<int> max_bytes_for_level_multiplier_additional;
  CompactionOptionsFIFO compaction_options_fifo;
  CompactionOptionsUniversal compaction_options_universal;
  uint64_t preclude_last_level_data_seconds;
  uint64_t preserve_internal_time_seconds;
  VerifyOutputFlags verify_output_flags;

  // Blob file related options
  bool enable_blob_files;
  uint64_t min_blob_size;
  uint64_t blob_file_size;
  uint64_t blob_file_writable_file_max_buffer_size;
  CompressionType blob_compression_type;
  CompressionOptions blob_compression_opts;
  bool enable_blob_garbage_collection;
  double blob_garbage_collection_age_cutoff;
  double blob_garbage_collection_force_threshold;
  uint64_t blob_compaction_readahead_size;
  int blob_file_starting_level;
  PrepopulateBlobCache prepopulate_blob_cache;

  // Misc options
  uint64_t max_sequential_skip_in_iterations;
  bool paranoid_file_checks;
  bool report_bg_io_stats;
  CompressionType compression;
  CompressionType bottommost_compression;
  CompressionOptions compression_opts;
  CompressionOptions bottommost_compression_opts;
  std::shared_ptr<CompressionManager> compression_manager;
  Temperature last_level_temperature;
  Temperature default_write_temperature;
  uint32_t memtable_protection_bytes_per_key;
  uint8_t block_protection_bytes_per_key;
  bool paranoid_memory_checks;
  bool memtable_verify_per_key_checksum_on_seek;

  uint64_t sample_for_compression;
  std::vector<CompressionType> compression_per_level;
  uint32_t memtable_max_range_deletions;
  uint32_t bottommost_file_compaction_delay;
  uint32_t uncache_aggressiveness;
  uint32_t memtable_op_scan_flush_trigger;
  uint32_t memtable_avg_op_scan_flush_trigger;
  uint32_t min_tombstones_for_range_conversion;

  // Derived options
  // Per-level target file size.
  std::vector<uint64_t> max_file_size;
};

uint64_t MultiplyCheckOverflow(uint64_t op1, double op2);

// Get the max file size in a given level.
uint64_t MaxFileSizeForLevel(const MutableCFOptions& cf_options, int level,
                             CompactionStyle compaction_style,
                             int base_level = 1,
                             bool level_compaction_dynamic_level_bytes = false);

// Get the max size of an L0 file for which we will pin its meta-blocks when
// `pin_l0_filter_and_index_blocks_in_cache` is set.
size_t MaxFileSizeForL0MetaPin(const MutableCFOptions& cf_options);

Status GetStringFromMutableCFOptions(const ConfigOptions& config_options,
                                     const MutableCFOptions& mutable_opts,
                                     std::string* opt_string);

Status GetMutableOptionsFromStrings(
    const MutableCFOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    Logger* info_log, MutableCFOptions* new_options);

// Returns true if applying options_map to base_options changes no option.
// Returns false if options_map does not parse, or if it names an option that
// MutableCFOptions does not store by value, such as table_factory or one of
// its fields. MutableCFOptions compares such objects by pointer, and setting
// one of their fields can change the shared object in place.
bool MutableCFOptionsUnchangedByMap(
    const MutableCFOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map);

#ifndef NDEBUG
std::vector<std::string> TEST_GetImmutableInMutableCFOptions();
extern bool TEST_allowSetOptionsImmutableInMutable;
#endif

}  // namespace ROCKSDB_NAMESPACE
