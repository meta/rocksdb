//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "options/options_helper.h"

#include <atomic>
#include <cassert>
#include <cctype>
#include <cstdlib>
#include <set>
#include <unordered_set>
#include <vector>

#include "options/cf_options.h"
#include "options/db_options.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/convenience.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
Status GetCompactionFilterMergeOrder(const CompactionFilter* filter,
                                     const CompactionFilterFactory* factory,
                                     TableFileCreationReason reason,
                                     CompactionFilter::MergeOrder* order) {
  using Order = CompactionFilter::MergeOrder;
  const Order filter_order =
      filter ? filter->GetMergeOrder() : Order::kFilterBeforeMerge;
  const Order factory_order =
      factory ? factory->GetMergeOrder() : Order::kFilterBeforeMerge;
  if ((filter_order != Order::kFilterBeforeMerge &&
       filter_order != Order::kMergeBeforeFilter) ||
      (factory_order != Order::kFilterBeforeMerge &&
       factory_order != Order::kMergeBeforeFilter)) {
    return Status::InvalidArgument("Invalid compaction filter merge order");
  }
  if (factory && filter && filter_order != factory_order) {
    return Status::InvalidArgument(
        "Compaction filter/factory merge order mismatch");
  }
  *order = factory ? factory_order : filter_order;
  if (reason != TableFileCreationReason::kCompaction &&
      *order == Order::kMergeBeforeFilter) {
    return Status::InvalidArgument(
        "Merge-before-filter is supported only during compaction");
  }
  return Status::OK();
}

Status ValidateCompactionFilterOptions(const ColumnFamilyOptions& cf_opts) {
  using Order = CompactionFilter::MergeOrder;
  Order order;
  Status s = GetCompactionFilterMergeOrder(cf_opts.compaction_filter, nullptr,
                                           TableFileCreationReason::kCompaction,
                                           &order);
  if (!s.ok()) {
    return s;
  }
  const CompactionFilterFactory* factory =
      cf_opts.compaction_filter_factory.get();
  s = GetCompactionFilterMergeOrder(
      nullptr, factory, TableFileCreationReason::kCompaction, &order);
  if (!s.ok()) {
    return s;
  }
  if (factory && order == Order::kMergeBeforeFilter) {
    for (TableFileCreationReason reason :
         {TableFileCreationReason::kFlush, TableFileCreationReason::kRecovery,
          TableFileCreationReason::kMisc,
          TableFileCreationReason::kSstFileWriter}) {
      if (factory->ShouldFilterTableFileCreation(reason)) {
        return Status::InvalidArgument(
            "Merge-before-filter factory selects non-compaction creation");
      }
    }
  }
  return Status::OK();
}

ConfigOptions::ConfigOptions() : registry(ObjectRegistry::NewInstance()) {
  env = Env::Default();
}

ConfigOptions::ConfigOptions(const DBOptions& db_opts) : env(db_opts.env) {
  registry = ObjectRegistry::NewInstance();
}

Status ValidateOptions(const DBOptions& db_opts,
                       const ColumnFamilyOptions& cf_opts) {
  Status s;
  auto db_cfg = DBOptionsAsConfigurable(db_opts);
  auto cf_cfg = CFOptionsAsConfigurable(cf_opts);
  s = db_cfg->ValidateOptions(db_opts, cf_opts);
  if (s.ok()) {
    s = cf_cfg->ValidateOptions(db_opts, cf_opts);
  }
  return s;
}

DBOptions BuildDBOptions(const ImmutableDBOptions& immutable_db_options,
                         const MutableDBOptions& mutable_db_options) {
  DBOptions options;
  BuildDBOptions(immutable_db_options, mutable_db_options, options);
  return options;
}

void BuildDBOptions(const ImmutableDBOptions& immutable_db_options,
                    const MutableDBOptions& mutable_db_options,
                    DBOptions& options) {
  options.create_if_missing = immutable_db_options.create_if_missing;
  options.create_missing_column_families =
      immutable_db_options.create_missing_column_families;
  options.error_if_exists = immutable_db_options.error_if_exists;
  options.paranoid_checks = immutable_db_options.paranoid_checks;
  options.open_files_async = immutable_db_options.open_files_async;
  options.flush_verify_memtable_count =
      immutable_db_options.flush_verify_memtable_count;
  options.compaction_verify_record_count =
      immutable_db_options.compaction_verify_record_count;
  options.track_and_verify_wals_in_manifest =
      immutable_db_options.track_and_verify_wals_in_manifest;
  options.track_and_verify_wals = immutable_db_options.track_and_verify_wals;
  options.verify_sst_unique_id_in_manifest =
      immutable_db_options.verify_sst_unique_id_in_manifest;
  options.fast_sst_open = mutable_db_options.fast_sst_open;
  options.env = immutable_db_options.env;
  options.rate_limiter = immutable_db_options.rate_limiter;
  options.sst_file_manager = immutable_db_options.sst_file_manager;
  options.info_log = immutable_db_options.info_log;
  options.info_log_level = immutable_db_options.info_log_level;
  options.max_open_files = mutable_db_options.max_open_files;
  options.max_file_opening_threads =
      immutable_db_options.max_file_opening_threads;
  options.read_io_executor_threads =
      immutable_db_options.read_io_executor_threads;
  options.max_total_wal_size = mutable_db_options.max_total_wal_size;
  options.statistics = immutable_db_options.statistics;
  options.use_fsync = immutable_db_options.use_fsync;
  options.db_paths = immutable_db_options.db_paths;
  options.db_log_dir = immutable_db_options.db_log_dir;
  options.wal_dir = immutable_db_options.wal_dir;
  options.delete_obsolete_files_period_micros =
      mutable_db_options.delete_obsolete_files_period_micros;
  options.max_background_jobs = mutable_db_options.max_background_jobs;
  options.max_background_compactions =
      mutable_db_options.max_background_compactions;
  options.max_subcompactions = mutable_db_options.max_subcompactions;
  options.max_background_flushes = mutable_db_options.max_background_flushes;
  options.max_log_file_size = immutable_db_options.max_log_file_size;
  options.log_file_time_to_roll = immutable_db_options.log_file_time_to_roll;
  options.keep_log_file_num = immutable_db_options.keep_log_file_num;
  options.recycle_log_file_num = immutable_db_options.recycle_log_file_num;
  options.async_wal_precreate = immutable_db_options.async_wal_precreate;
  options.max_manifest_file_size = mutable_db_options.max_manifest_file_size;
  options.max_manifest_space_amp_pct =
      mutable_db_options.max_manifest_space_amp_pct;
  options.table_cache_numshardbits =
      immutable_db_options.table_cache_numshardbits;
  options.WAL_ttl_seconds = immutable_db_options.WAL_ttl_seconds;
  options.WAL_size_limit_MB = immutable_db_options.WAL_size_limit_MB;
  options.manifest_preallocation_size =
      mutable_db_options.manifest_preallocation_size;
  options.allow_mmap_reads = immutable_db_options.allow_mmap_reads;
  options.allow_mmap_writes = immutable_db_options.allow_mmap_writes;
  options.use_direct_reads = immutable_db_options.use_direct_reads;
  options.use_direct_io_for_compaction_reads =
      immutable_db_options.use_direct_io_for_compaction_reads;
  options.use_direct_io_for_flush_and_compaction =
      immutable_db_options.use_direct_io_for_flush_and_compaction;
  options.allow_fallocate = immutable_db_options.allow_fallocate;
  options.is_fd_close_on_exec = immutable_db_options.is_fd_close_on_exec;
  options.stats_dump_period_sec = mutable_db_options.stats_dump_period_sec;
  options.mutex_hold_log_threshold_us =
      mutable_db_options.mutex_hold_log_threshold_us;
  options.stats_persist_period_sec =
      mutable_db_options.stats_persist_period_sec;
  options.persist_stats_to_disk = immutable_db_options.persist_stats_to_disk;
  options.stats_history_buffer_size =
      mutable_db_options.stats_history_buffer_size;
  options.advise_random_on_open = immutable_db_options.advise_random_on_open;
  options.db_write_buffer_size = immutable_db_options.db_write_buffer_size;
  options.write_buffer_manager = immutable_db_options.write_buffer_manager;
  options.compaction_readahead_size =
      mutable_db_options.compaction_readahead_size;
  options.writable_file_max_buffer_size =
      mutable_db_options.writable_file_max_buffer_size;
  options.use_adaptive_mutex = immutable_db_options.use_adaptive_mutex;
  options.bytes_per_sync = mutable_db_options.bytes_per_sync;
  options.wal_bytes_per_sync = mutable_db_options.wal_bytes_per_sync;
  options.strict_bytes_per_sync = mutable_db_options.strict_bytes_per_sync;
  options.listeners = immutable_db_options.listeners;
  options.enable_thread_tracking = immutable_db_options.enable_thread_tracking;
  options.delayed_write_rate = mutable_db_options.delayed_write_rate;
  options.enable_pipelined_write = immutable_db_options.enable_pipelined_write;
  options.unordered_write = immutable_db_options.unordered_write;
  options.allow_concurrent_memtable_write =
      immutable_db_options.allow_concurrent_memtable_write;
  options.enable_write_thread_adaptive_yield =
      immutable_db_options.enable_write_thread_adaptive_yield;
  options.max_write_batch_group_size_bytes =
      immutable_db_options.max_write_batch_group_size_bytes;
  options.write_thread_max_yield_usec =
      immutable_db_options.write_thread_max_yield_usec;
  options.write_thread_slow_yield_usec =
      immutable_db_options.write_thread_slow_yield_usec;
  options.skip_stats_update_on_db_open =
      immutable_db_options.skip_stats_update_on_db_open;
  options.wal_recovery_mode = immutable_db_options.wal_recovery_mode;
  options.allow_2pc = immutable_db_options.allow_2pc;
  options.row_cache = immutable_db_options.row_cache;
  options.wal_filter = immutable_db_options.wal_filter;
  options.dump_malloc_stats = immutable_db_options.dump_malloc_stats;
  options.avoid_flush_during_recovery =
      immutable_db_options.avoid_flush_during_recovery;
  options.enforce_write_buffer_manager_during_recovery =
      immutable_db_options.enforce_write_buffer_manager_during_recovery;
  options.avoid_flush_during_shutdown =
      mutable_db_options.avoid_flush_during_shutdown;
  options.allow_ingest_behind = immutable_db_options.allow_ingest_behind;
  options.two_write_queues = immutable_db_options.two_write_queues;
  options.manual_wal_flush = immutable_db_options.manual_wal_flush;
  options.wal_compression = immutable_db_options.wal_compression;
  options.background_close_inactive_wals =
      immutable_db_options.background_close_inactive_wals;
  options.atomic_flush = immutable_db_options.atomic_flush;
  options.avoid_unnecessary_blocking_io =
      immutable_db_options.avoid_unnecessary_blocking_io;
  options.write_dbid_to_manifest = immutable_db_options.write_dbid_to_manifest;
  options.track_options_file_number_in_manifest =
      immutable_db_options.track_options_file_number_in_manifest;
  options.write_identity_file = immutable_db_options.write_identity_file;
  options.reuse_manifest_on_open = immutable_db_options.reuse_manifest_on_open;
  options.prefix_seek_opt_in_only =
      immutable_db_options.prefix_seek_opt_in_only;
  options.log_readahead_size = immutable_db_options.log_readahead_size;
  options.file_checksum_gen_factory =
      immutable_db_options.file_checksum_gen_factory;
  options.best_efforts_recovery = immutable_db_options.best_efforts_recovery;
  options.max_bgerror_resume_count =
      immutable_db_options.max_bgerror_resume_count;
  options.bgerror_resume_retry_interval =
      immutable_db_options.bgerror_resume_retry_interval;
  options.db_host_id = immutable_db_options.db_host_id;
  options.allow_data_in_errors = immutable_db_options.allow_data_in_errors;
  options.checksum_handoff_file_types =
      immutable_db_options.checksum_handoff_file_types;
  options.lowest_used_cache_tier = immutable_db_options.lowest_used_cache_tier;
  options.enforce_single_del_contracts =
      immutable_db_options.enforce_single_del_contracts;
  options.verify_manifest_content_on_close =
      mutable_db_options.verify_manifest_content_on_close;
  options.optimize_manifest_for_recovery =
      mutable_db_options.optimize_manifest_for_recovery;
  options.daily_offpeak_time_utc = mutable_db_options.daily_offpeak_time_utc;
  options.max_compaction_trigger_wakeup_seconds =
      mutable_db_options.max_compaction_trigger_wakeup_seconds;
  options.remote_compaction_manifest_floor =
      mutable_db_options.remote_compaction_manifest_floor;
  options.periodic_compaction_phase_recovery_percent =
      mutable_db_options.periodic_compaction_phase_recovery_percent;
  options.follower_refresh_catchup_period_ms =
      immutable_db_options.follower_refresh_catchup_period_ms;
  options.follower_catchup_retry_count =
      immutable_db_options.follower_catchup_retry_count;
  options.follower_catchup_retry_wait_ms =
      immutable_db_options.follower_catchup_retry_wait_ms;
  options.metadata_write_temperature =
      immutable_db_options.metadata_write_temperature;
  options.wal_write_temperature = immutable_db_options.wal_write_temperature;
  options.compaction_service = immutable_db_options.compaction_service;
  options.use_session_tmp_dir_for_remote_compaction =
      immutable_db_options.use_session_tmp_dir_for_remote_compaction;
  options.calculate_sst_write_lifetime_hint_set =
      immutable_db_options.calculate_sst_write_lifetime_hint_set;
}

ColumnFamilyOptions BuildColumnFamilyOptions(
    const ColumnFamilyOptions& options,
    const MutableCFOptions& mutable_cf_options) {
  ColumnFamilyOptions cf_opts(options);
  UpdateColumnFamilyOptions(mutable_cf_options, &cf_opts);
  // TODO(yhchiang): find some way to handle the following derived options
  // * max_file_size
  return cf_opts;
}

void UpdateColumnFamilyOptions(const MutableCFOptions& moptions,
                               ColumnFamilyOptions* cf_opts) {
  // Memtable related options
  cf_opts->write_buffer_size = moptions.write_buffer_size;
  cf_opts->max_write_buffer_number = moptions.max_write_buffer_number;
  cf_opts->arena_block_size = moptions.arena_block_size;
  cf_opts->memtable_prefix_bloom_size_ratio =
      moptions.memtable_prefix_bloom_size_ratio;
  cf_opts->memtable_whole_key_filtering = moptions.memtable_whole_key_filtering;
  cf_opts->memtable_huge_page_size = moptions.memtable_huge_page_size;
  cf_opts->max_successive_merges = moptions.max_successive_merges;
  cf_opts->strict_max_successive_merges = moptions.strict_max_successive_merges;
  cf_opts->inplace_update_num_locks = moptions.inplace_update_num_locks;
  cf_opts->prefix_extractor = moptions.prefix_extractor;
  cf_opts->experimental_mempurge_threshold =
      moptions.experimental_mempurge_threshold;
  cf_opts->memtable_protection_bytes_per_key =
      moptions.memtable_protection_bytes_per_key;
  cf_opts->block_protection_bytes_per_key =
      moptions.block_protection_bytes_per_key;
  cf_opts->paranoid_memory_checks = moptions.paranoid_memory_checks;
  cf_opts->memtable_verify_per_key_checksum_on_seek =
      moptions.memtable_verify_per_key_checksum_on_seek;
  cf_opts->bottommost_file_compaction_delay =
      moptions.bottommost_file_compaction_delay;

  // Compaction related options
  cf_opts->disable_auto_compactions = moptions.disable_auto_compactions;
  cf_opts->table_factory = moptions.table_factory;
  cf_opts->soft_pending_compaction_bytes_limit =
      moptions.soft_pending_compaction_bytes_limit;
  cf_opts->hard_pending_compaction_bytes_limit =
      moptions.hard_pending_compaction_bytes_limit;
  cf_opts->level0_file_num_compaction_trigger =
      moptions.level0_file_num_compaction_trigger;
  cf_opts->level0_slowdown_writes_trigger =
      moptions.level0_slowdown_writes_trigger;
  cf_opts->level0_stop_writes_trigger = moptions.level0_stop_writes_trigger;
  cf_opts->max_compaction_bytes = moptions.max_compaction_bytes;
  cf_opts->target_file_size_base = moptions.target_file_size_base;
  cf_opts->target_file_size_multiplier = moptions.target_file_size_multiplier;
  cf_opts->target_file_size_is_upper_bound =
      moptions.target_file_size_is_upper_bound;
  cf_opts->max_bytes_for_level_base = moptions.max_bytes_for_level_base;
  cf_opts->max_bytes_for_level_multiplier =
      moptions.max_bytes_for_level_multiplier;
  cf_opts->ttl = moptions.ttl;
  cf_opts->periodic_compaction_seconds = moptions.periodic_compaction_seconds;
  cf_opts->read_triggered_compaction_threshold =
      moptions.read_triggered_compaction_threshold;
  cf_opts->preclude_last_level_data_seconds =
      moptions.preclude_last_level_data_seconds;
  cf_opts->preserve_internal_time_seconds =
      moptions.preserve_internal_time_seconds;

  cf_opts->max_bytes_for_level_multiplier_additional.clear();
  for (auto value : moptions.max_bytes_for_level_multiplier_additional) {
    cf_opts->max_bytes_for_level_multiplier_additional.emplace_back(value);
  }

  cf_opts->compaction_options_fifo = moptions.compaction_options_fifo;
  cf_opts->compaction_options_universal = moptions.compaction_options_universal;

  cf_opts->verify_output_flags = moptions.verify_output_flags;

  // Blob file related options
  cf_opts->enable_blob_files = moptions.enable_blob_files;
  cf_opts->min_blob_size = moptions.min_blob_size;
  cf_opts->blob_file_size = moptions.blob_file_size;
  cf_opts->blob_file_writable_file_max_buffer_size =
      moptions.blob_file_writable_file_max_buffer_size;
  cf_opts->blob_compression_type = moptions.blob_compression_type;
  cf_opts->blob_compression_opts = moptions.blob_compression_opts;
  cf_opts->enable_blob_garbage_collection =
      moptions.enable_blob_garbage_collection;
  cf_opts->blob_garbage_collection_age_cutoff =
      moptions.blob_garbage_collection_age_cutoff;
  cf_opts->blob_garbage_collection_force_threshold =
      moptions.blob_garbage_collection_force_threshold;
  cf_opts->blob_compaction_readahead_size =
      moptions.blob_compaction_readahead_size;
  cf_opts->blob_file_starting_level = moptions.blob_file_starting_level;
  cf_opts->prepopulate_blob_cache = moptions.prepopulate_blob_cache;

  // Misc options
  cf_opts->max_sequential_skip_in_iterations =
      moptions.max_sequential_skip_in_iterations;
  cf_opts->paranoid_file_checks = moptions.paranoid_file_checks;
  cf_opts->report_bg_io_stats = moptions.report_bg_io_stats;
  cf_opts->compression = moptions.compression;
  cf_opts->compression_opts = moptions.compression_opts;
  cf_opts->bottommost_compression = moptions.bottommost_compression;
  cf_opts->bottommost_compression_opts = moptions.bottommost_compression_opts;
  cf_opts->compression_manager = moptions.compression_manager;
  cf_opts->sample_for_compression = moptions.sample_for_compression;
  cf_opts->compression_per_level = moptions.compression_per_level;
  cf_opts->last_level_temperature = moptions.last_level_temperature;
  cf_opts->default_write_temperature = moptions.default_write_temperature;
  cf_opts->memtable_max_range_deletions = moptions.memtable_max_range_deletions;
  cf_opts->uncache_aggressiveness = moptions.uncache_aggressiveness;
  cf_opts->memtable_op_scan_flush_trigger =
      moptions.memtable_op_scan_flush_trigger;
  cf_opts->memtable_avg_op_scan_flush_trigger =
      moptions.memtable_avg_op_scan_flush_trigger;
  cf_opts->min_tombstones_for_range_conversion =
      moptions.min_tombstones_for_range_conversion;
}

void UpdateColumnFamilyOptions(const ImmutableCFOptions& ioptions,
                               ColumnFamilyOptions* cf_opts) {
  cf_opts->compaction_style = ioptions.compaction_style;
  cf_opts->compaction_pri = ioptions.compaction_pri;
  cf_opts->comparator = ioptions.user_comparator;
  cf_opts->merge_operator = ioptions.merge_operator;
  cf_opts->compaction_filter = ioptions.compaction_filter;
  cf_opts->compaction_filter_factory = ioptions.compaction_filter_factory;
  cf_opts->min_write_buffer_number_to_merge =
      ioptions.min_write_buffer_number_to_merge;
  cf_opts->max_write_buffer_size_to_maintain =
      ioptions.max_write_buffer_size_to_maintain;
  cf_opts->inplace_update_support = ioptions.inplace_update_support;
  cf_opts->inplace_callback = ioptions.inplace_callback;
  cf_opts->memtable_factory = ioptions.memtable_factory;
  cf_opts->table_properties_collector_factories =
      ioptions.table_properties_collector_factories;
  cf_opts->bloom_locality = ioptions.bloom_locality;
  cf_opts->level_compaction_dynamic_level_bytes =
      ioptions.level_compaction_dynamic_level_bytes;
  cf_opts->num_levels = ioptions.num_levels;
  cf_opts->optimize_filters_for_hits = ioptions.optimize_filters_for_hits;
  cf_opts->force_consistency_checks = ioptions.force_consistency_checks;
  cf_opts->disallow_memtable_writes = ioptions.disallow_memtable_writes;
  cf_opts->memtable_insert_with_hint_prefix_extractor =
      ioptions.memtable_insert_with_hint_prefix_extractor;
  cf_opts->cf_paths = ioptions.cf_paths;
  cf_opts->compaction_thread_limiter = ioptions.compaction_thread_limiter;
  cf_opts->sst_partitioner_factory = ioptions.sst_partitioner_factory;
  cf_opts->blob_cache = ioptions.blob_cache;
  cf_opts->enable_blob_direct_write = ioptions.enable_blob_direct_write;
  cf_opts->blob_direct_write_partitions = ioptions.blob_direct_write_partitions;
  cf_opts->blob_direct_write_partition_strategy =
      ioptions.blob_direct_write_partition_strategy;
  cf_opts->persist_user_defined_timestamps =
      ioptions.persist_user_defined_timestamps;
  cf_opts->default_temperature = ioptions.default_temperature;
  cf_opts->cf_allow_ingest_behind = ioptions.cf_allow_ingest_behind;
  cf_opts->memtable_batch_lookup_optimization =
      ioptions.memtable_batch_lookup_optimization;

  // TODO(yhchiang): find some way to handle the following derived options
  // * max_file_size
}

std::map<CompactionStyle, std::string>
    OptionsHelper::compaction_style_to_string = {
        {kCompactionStyleLevel, "kCompactionStyleLevel"},
        {kCompactionStyleUniversal, "kCompactionStyleUniversal"},
        {kCompactionStyleFIFO, "kCompactionStyleFIFO"},
        {kCompactionStyleNone, "kCompactionStyleNone"}};

std::map<CompactionPri, std::string> OptionsHelper::compaction_pri_to_string = {
    {kByCompensatedSize, "kByCompensatedSize"},
    {kOldestLargestSeqFirst, "kOldestLargestSeqFirst"},
    {kOldestSmallestSeqFirst, "kOldestSmallestSeqFirst"},
    {kMinOverlappingRatio, "kMinOverlappingRatio"},
    {kRoundRobin, "kRoundRobin"}};

std::map<CompactionStopStyle, std::string>
    OptionsHelper::compaction_stop_style_to_string = {
        {kCompactionStopStyleSimilarSize, "kCompactionStopStyleSimilarSize"},
        {kCompactionStopStyleTotalSize, "kCompactionStopStyleTotalSize"}};

std::map<Temperature, std::string> OptionsHelper::temperature_to_string = {
    {Temperature::kUnknown, "kUnknown"}, {Temperature::kHot, "kHot"},
    {Temperature::kWarm, "kWarm"},       {Temperature::kCool, "kCool"},
    {Temperature::kCold, "kCold"},       {Temperature::kIce, "kIce"}};

std::unordered_map<std::string, ChecksumType>
    OptionsHelper::checksum_type_string_map = {{"kNoChecksum", kNoChecksum},
                                               {"kCRC32c", kCRC32c},
                                               {"kxxHash", kxxHash},
                                               {"kxxHash64", kxxHash64},
                                               {"kXXH3", kXXH3}};

std::unordered_map<std::string, CompressionType>
    OptionsHelper::compression_type_string_map = {
        {"kNoCompression", kNoCompression},
        {"kSnappyCompression", kSnappyCompression},
        {"kZlibCompression", kZlibCompression},
        {"kBZip2Compression", kBZip2Compression},
        {"kLZ4Compression", kLZ4Compression},
        {"kLZ4HCCompression", kLZ4HCCompression},
        {"kXpressCompression", kXpressCompression},
        {"kZSTD", kZSTD},
        {"kCustomCompression80", kCustomCompression80},
        {"kCustomCompression81", kCustomCompression81},
        {"kCustomCompression82", kCustomCompression82},
        {"kCustomCompression83", kCustomCompression83},
        {"kCustomCompression84", kCustomCompression84},
        {"kCustomCompression85", kCustomCompression85},
        {"kCustomCompression86", kCustomCompression86},
        {"kCustomCompression87", kCustomCompression87},
        {"kCustomCompression88", kCustomCompression88},
        {"kCustomCompression89", kCustomCompression89},
        {"kCustomCompression8A", kCustomCompression8A},
        {"kCustomCompression8B", kCustomCompression8B},
        {"kCustomCompression8C", kCustomCompression8C},
        {"kCustomCompression8D", kCustomCompression8D},
        {"kCustomCompression8E", kCustomCompression8E},
        {"kCustomCompression8F", kCustomCompression8F},
        {"kCustomCompression90", kCustomCompression90},
        {"kCustomCompression91", kCustomCompression91},
        {"kCustomCompression92", kCustomCompression92},
        {"kCustomCompression93", kCustomCompression93},
        {"kCustomCompression94", kCustomCompression94},
        {"kCustomCompression95", kCustomCompression95},
        {"kCustomCompression96", kCustomCompression96},
        {"kCustomCompression97", kCustomCompression97},
        {"kCustomCompression98", kCustomCompression98},
        {"kCustomCompression99", kCustomCompression99},
        {"kCustomCompression9A", kCustomCompression9A},
        {"kCustomCompression9B", kCustomCompression9B},
        {"kCustomCompression9C", kCustomCompression9C},
        {"kCustomCompression9D", kCustomCompression9D},
        {"kCustomCompression9E", kCustomCompression9E},
        {"kCustomCompression9F", kCustomCompression9F},
        {"kCustomCompressionA0", kCustomCompressionA0},
        {"kCustomCompressionA1", kCustomCompressionA1},
        {"kCustomCompressionA2", kCustomCompressionA2},
        {"kCustomCompressionA3", kCustomCompressionA3},
        {"kCustomCompressionA4", kCustomCompressionA4},
        {"kCustomCompressionA5", kCustomCompressionA5},
        {"kCustomCompressionA6", kCustomCompressionA6},
        {"kCustomCompressionA7", kCustomCompressionA7},
        {"kCustomCompressionA8", kCustomCompressionA8},
        {"kCustomCompressionA9", kCustomCompressionA9},
        {"kCustomCompressionAA", kCustomCompressionAA},
        {"kCustomCompressionAB", kCustomCompressionAB},
        {"kCustomCompressionAC", kCustomCompressionAC},
        {"kCustomCompressionAD", kCustomCompressionAD},
        {"kCustomCompressionAE", kCustomCompressionAE},
        {"kCustomCompressionAF", kCustomCompressionAF},
        {"kCustomCompressionB0", kCustomCompressionB0},
        {"kCustomCompressionB1", kCustomCompressionB1},
        {"kCustomCompressionB2", kCustomCompressionB2},
        {"kCustomCompressionB3", kCustomCompressionB3},
        {"kCustomCompressionB4", kCustomCompressionB4},
        {"kCustomCompressionB5", kCustomCompressionB5},
        {"kCustomCompressionB6", kCustomCompressionB6},
        {"kCustomCompressionB7", kCustomCompressionB7},
        {"kCustomCompressionB8", kCustomCompressionB8},
        {"kCustomCompressionB9", kCustomCompressionB9},
        {"kCustomCompressionBA", kCustomCompressionBA},
        {"kCustomCompressionBB", kCustomCompressionBB},
        {"kCustomCompressionBC", kCustomCompressionBC},
        {"kCustomCompressionBD", kCustomCompressionBD},
        {"kCustomCompressionBE", kCustomCompressionBE},
        {"kCustomCompressionBF", kCustomCompressionBF},
        {"kCustomCompressionC0", kCustomCompressionC0},
        {"kCustomCompressionC1", kCustomCompressionC1},
        {"kCustomCompressionC2", kCustomCompressionC2},
        {"kCustomCompressionC3", kCustomCompressionC3},
        {"kCustomCompressionC4", kCustomCompressionC4},
        {"kCustomCompressionC5", kCustomCompressionC5},
        {"kCustomCompressionC6", kCustomCompressionC6},
        {"kCustomCompressionC7", kCustomCompressionC7},
        {"kCustomCompressionC8", kCustomCompressionC8},
        {"kCustomCompressionC9", kCustomCompressionC9},
        {"kCustomCompressionCA", kCustomCompressionCA},
        {"kCustomCompressionCB", kCustomCompressionCB},
        {"kCustomCompressionCC", kCustomCompressionCC},
        {"kCustomCompressionCD", kCustomCompressionCD},
        {"kCustomCompressionCE", kCustomCompressionCE},
        {"kCustomCompressionCF", kCustomCompressionCF},
        {"kCustomCompressionD0", kCustomCompressionD0},
        {"kCustomCompressionD1", kCustomCompressionD1},
        {"kCustomCompressionD2", kCustomCompressionD2},
        {"kCustomCompressionD3", kCustomCompressionD3},
        {"kCustomCompressionD4", kCustomCompressionD4},
        {"kCustomCompressionD5", kCustomCompressionD5},
        {"kCustomCompressionD6", kCustomCompressionD6},
        {"kCustomCompressionD7", kCustomCompressionD7},
        {"kCustomCompressionD8", kCustomCompressionD8},
        {"kCustomCompressionD9", kCustomCompressionD9},
        {"kCustomCompressionDA", kCustomCompressionDA},
        {"kCustomCompressionDB", kCustomCompressionDB},
        {"kCustomCompressionDC", kCustomCompressionDC},
        {"kCustomCompressionDD", kCustomCompressionDD},
        {"kCustomCompressionDE", kCustomCompressionDE},
        {"kCustomCompressionDF", kCustomCompressionDF},
        {"kCustomCompressionE0", kCustomCompressionE0},
        {"kCustomCompressionE1", kCustomCompressionE1},
        {"kCustomCompressionE2", kCustomCompressionE2},
        {"kCustomCompressionE3", kCustomCompressionE3},
        {"kCustomCompressionE4", kCustomCompressionE4},
        {"kCustomCompressionE5", kCustomCompressionE5},
        {"kCustomCompressionE6", kCustomCompressionE6},
        {"kCustomCompressionE7", kCustomCompressionE7},
        {"kCustomCompressionE8", kCustomCompressionE8},
        {"kCustomCompressionE9", kCustomCompressionE9},
        {"kCustomCompressionEA", kCustomCompressionEA},
        {"kCustomCompressionEB", kCustomCompressionEB},
        {"kCustomCompressionEC", kCustomCompressionEC},
        {"kCustomCompressionED", kCustomCompressionED},
        {"kCustomCompressionEE", kCustomCompressionEE},
        {"kCustomCompressionEF", kCustomCompressionEF},
        {"kCustomCompressionF0", kCustomCompressionF0},
        {"kCustomCompressionF1", kCustomCompressionF1},
        {"kCustomCompressionF2", kCustomCompressionF2},
        {"kCustomCompressionF3", kCustomCompressionF3},
        {"kCustomCompressionF4", kCustomCompressionF4},
        {"kCustomCompressionF5", kCustomCompressionF5},
        {"kCustomCompressionF6", kCustomCompressionF6},
        {"kCustomCompressionF7", kCustomCompressionF7},
        {"kCustomCompressionF8", kCustomCompressionF8},
        {"kCustomCompressionF9", kCustomCompressionF9},
        {"kCustomCompressionFA", kCustomCompressionFA},
        {"kCustomCompressionFB", kCustomCompressionFB},
        {"kCustomCompressionFC", kCustomCompressionFC},
        {"kCustomCompressionFD", kCustomCompressionFD},
        {"kCustomCompressionFE", kCustomCompressionFE},
        {"kDisableCompressionOption", kDisableCompressionOption}};

const std::vector<CompressionType>& GetSupportedCompressions() {
  static std::vector<CompressionType> supported_compressions = []() {
    // std::set internally to deduplicate potential name aliases
    std::set<CompressionType> comp_set;
    for (const auto& comp_to_name :
         OptionsHelper::compression_type_string_map) {
      CompressionType t = comp_to_name.second;
      if (t != kDisableCompressionOption && CompressionTypeSupported(t)) {
        comp_set.insert(t);
      }
    }
    return std::vector<CompressionType>(comp_set.begin(), comp_set.end());
  }();
  return supported_compressions;
}

const std::vector<CompressionType>& GetSupportedDictCompressions() {
  static std::vector<CompressionType> supported_dict_compressions = []() {
    std::set<CompressionType> comp_set;
    for (const auto& comp_to_name :
         OptionsHelper::compression_type_string_map) {
      CompressionType t = comp_to_name.second;
      if (t != kDisableCompressionOption && DictCompressionTypeSupported(t)) {
        comp_set.insert(t);
      }
    }
    return std::vector<CompressionType>(comp_set.begin(), comp_set.end());
  }();
  return supported_dict_compressions;
}

const std::vector<ChecksumType>& GetSupportedChecksums() {
  static std::vector<ChecksumType> supported_checksums = []() {
    std::set<ChecksumType> checksum_types;
    for (const auto& e : OptionsHelper::checksum_type_string_map) {
      checksum_types.insert(e.second);
    }
    return std::vector<ChecksumType>(checksum_types.begin(),
                                     checksum_types.end());
  }();
  return supported_checksums;
}

static bool ParseOptionHelper(void* opt_address, const OptionType& opt_type,
                              const std::string& value) {
  switch (opt_type) {
    case OptionType::kBoolean:
      *static_cast<bool*>(opt_address) = ParseBoolean("", value);
      break;
    case OptionType::kInt:
      *static_cast<int*>(opt_address) = ParseInt(value);
      break;
    case OptionType::kInt32T:
      *static_cast<int32_t*>(opt_address) = ParseInt32(value);
      break;
    case OptionType::kInt64T:
      PutUnaligned(static_cast<int64_t*>(opt_address), ParseInt64(value));
      break;
    case OptionType::kUInt:
      *static_cast<unsigned int*>(opt_address) = ParseUint32(value);
      break;
    case OptionType::kUInt8T:
      *static_cast<uint8_t*>(opt_address) = ParseUint8(value);
      break;
    case OptionType::kUInt32T:
      *static_cast<uint32_t*>(opt_address) = ParseUint32(value);
      break;
    case OptionType::kUInt64T:
      PutUnaligned(static_cast<uint64_t*>(opt_address), ParseUint64(value));
      break;
    case OptionType::kSizeT:
      PutUnaligned(static_cast<size_t*>(opt_address), ParseSizeT(value));
      break;
    case OptionType::kAtomicInt:
      static_cast<std::atomic<int>*>(opt_address)
          ->store(ParseInt(value), std::memory_order_release);
      break;
    case OptionType::kString:
      *static_cast<std::string*>(opt_address) = value;
      break;
    case OptionType::kDouble:
      *static_cast<double*>(opt_address) = ParseDouble(value);
      break;
    case OptionType::kCompactionStyle:
      return ParseEnum<CompactionStyle>(
          compaction_style_string_map, value,
          static_cast<CompactionStyle*>(opt_address));
    case OptionType::kCompactionPri:
      return ParseEnum<CompactionPri>(compaction_pri_string_map, value,
                                      static_cast<CompactionPri*>(opt_address));
    case OptionType::kCompressionType:
      return ParseEnum<CompressionType>(
          compression_type_string_map, value,
          static_cast<CompressionType*>(opt_address));
    case OptionType::kChecksumType:
      return ParseEnum<ChecksumType>(checksum_type_string_map, value,
                                     static_cast<ChecksumType*>(opt_address));
    case OptionType::kEncodingType:
      return ParseEnum<EncodingType>(encoding_type_string_map, value,
                                     static_cast<EncodingType*>(opt_address));
    case OptionType::kCompactionStopStyle:
      return ParseEnum<CompactionStopStyle>(
          compaction_stop_style_string_map, value,
          static_cast<CompactionStopStyle*>(opt_address));
    case OptionType::kEncodedString: {
      // DecodeHex() can write part of the result before it finds a bad digit,
      // so keep the option unchanged until the whole value decodes.
      std::string decoded;
      if (!Slice(value).DecodeHex(&decoded)) {
        return false;
      }
      *static_cast<std::string*>(opt_address) = std::move(decoded);
      break;
    }
    case OptionType::kTemperature: {
      return ParseEnum<Temperature>(temperature_string_map, value,
                                    static_cast<Temperature*>(opt_address));
    }
    default:
      return false;
  }
  return true;
}

bool SerializeSingleOptionHelper(const void* opt_address,
                                 const OptionType opt_type,
                                 std::string* value) {
  assert(value);
  switch (opt_type) {
    case OptionType::kBoolean:
      *value = *(static_cast<const bool*>(opt_address)) ? "true" : "false";
      break;
    case OptionType::kInt:
      *value = std::to_string(*(static_cast<const int*>(opt_address)));
      break;
    case OptionType::kInt32T:
      *value = std::to_string(*(static_cast<const int32_t*>(opt_address)));
      break;
    case OptionType::kInt64T: {
      int64_t v;
      GetUnaligned(static_cast<const int64_t*>(opt_address), &v);
      *value = std::to_string(v);
    } break;
    case OptionType::kUInt:
      *value = std::to_string(*(static_cast<const unsigned int*>(opt_address)));
      break;
    case OptionType::kUInt8T:
      *value = std::to_string(*(static_cast<const uint8_t*>(opt_address)));
      break;
    case OptionType::kUInt32T:
      *value = std::to_string(*(static_cast<const uint32_t*>(opt_address)));
      break;
    case OptionType::kUInt64T: {
      uint64_t v;
      GetUnaligned(static_cast<const uint64_t*>(opt_address), &v);
      *value = std::to_string(v);
    } break;
    case OptionType::kSizeT: {
      size_t v;
      GetUnaligned(static_cast<const size_t*>(opt_address), &v);
      *value = std::to_string(v);
    } break;
    case OptionType::kDouble:
      *value = std::to_string(*(static_cast<const double*>(opt_address)));
      break;
    case OptionType::kAtomicInt:
      *value = std::to_string(static_cast<const std::atomic<int>*>(opt_address)
                                  ->load(std::memory_order_acquire));
      break;
    case OptionType::kString:
      *value =
          EscapeOptionString(*(static_cast<const std::string*>(opt_address)));
      break;
    case OptionType::kCompactionStyle:
      return SerializeEnum<CompactionStyle>(
          compaction_style_string_map,
          *(static_cast<const CompactionStyle*>(opt_address)), value);
    case OptionType::kCompactionPri:
      return SerializeEnum<CompactionPri>(
          compaction_pri_string_map,
          *(static_cast<const CompactionPri*>(opt_address)), value);
    case OptionType::kCompressionType:
      return SerializeEnum<CompressionType>(
          compression_type_string_map,
          *(static_cast<const CompressionType*>(opt_address)), value);
    case OptionType::kChecksumType:
      return SerializeEnum<ChecksumType>(
          checksum_type_string_map,
          *static_cast<const ChecksumType*>(opt_address), value);
    case OptionType::kEncodingType:
      return SerializeEnum<EncodingType>(
          encoding_type_string_map,
          *static_cast<const EncodingType*>(opt_address), value);
    case OptionType::kCompactionStopStyle:
      return SerializeEnum<CompactionStopStyle>(
          compaction_stop_style_string_map,
          *static_cast<const CompactionStopStyle*>(opt_address), value);
    case OptionType::kEncodedString: {
      const auto* ptr = static_cast<const std::string*>(opt_address);
      *value = (Slice(*ptr)).ToString(true);
      break;
    }
    case OptionType::kTemperature: {
      return SerializeEnum<Temperature>(
          temperature_string_map, *static_cast<const Temperature*>(opt_address),
          value);
    }
    default:
      return false;
  }
  return true;
}

template <typename T>
Status ConfigureFromMap(
    const ConfigOptions& config_options,
    const std::unordered_map<std::string, std::string>& opt_map,
    const std::string& option_name, Configurable* config, T* new_opts) {
  Status s = config->ConfigureFromMap(config_options, opt_map);
  if (s.ok()) {
    *new_opts = *(config->GetOptions<T>(option_name));
  }
  return s;
}

Status StringToMap(const std::string& opts_str,
                   std::unordered_map<std::string, std::string>* opts_map) {
  assert(opts_map);
  // Example:
  //   opts_str = "write_buffer_size=1024;max_write_buffer_number=2;"
  //              "nested_opt={opt1=1;opt2=2};max_bytes_for_level_base=100"
  //
  // Each value in the resulting map can be substituted directly into a
  // `key=value;` context (e.g. SetOptions) and re-parsed without ambiguity:
  // values that were wrapped in `{...}` in the input keep their wrapping,
  // so embedded `;` characters stay escaped.
  size_t pos = 0;
  std::string opts = trim(opts_str);
  // If the input string starts and ends with "{...}", strip off the brackets
  while (opts.size() > 2 && opts[0] == '{' && opts[opts.size() - 1] == '}') {
    opts = trim(opts.substr(1, opts.size() - 2));
  }

  while (pos < opts.size()) {
    size_t eq_pos = opts.find_first_of("={};", pos);
    if (eq_pos == std::string::npos) {
      return Status::InvalidArgument("Mismatched key value pair, '=' expected");
    } else if (opts[eq_pos] != '=') {
      return Status::InvalidArgument("Unexpected char in key");
    }

    std::string key = trim(opts.substr(pos, eq_pos - pos));
    if (key.empty()) {
      return Status::InvalidArgument("Empty key found");
    }

    // Locate value start (after '=' and any whitespace).
    size_t value_start = eq_pos + 1;
    while (value_start < opts.size() && isspace(opts[value_start])) {
      ++value_start;
    }

    std::string value;
    if (value_start < opts.size() && opts[value_start] == '{') {
      // Braced value: extract the entire `{...}` substring INCLUDING braces
      // so that the value can be re-emitted as-is in a `key=value;` context.
      int count = 1;
      size_t brace_pos = value_start + 1;
      while (brace_pos < opts.size() && count > 0) {
        if (opts[brace_pos] == '{') {
          ++count;
        } else if (opts[brace_pos] == '}') {
          --count;
        }
        if (count > 0) {
          ++brace_pos;
        }
      }
      if (count != 0) {
        return Status::InvalidArgument(
            "Mismatched curly braces for nested options");
      }
      value = opts.substr(value_start, brace_pos - value_start + 1);
      pos = brace_pos + 1;
      // Allow whitespace, then either delimiter or end.
      while (pos < opts.size() && isspace(opts[pos])) {
        ++pos;
      }
      if (pos < opts.size() && opts[pos] != ';') {
        return Status::InvalidArgument("Unexpected chars after nested options");
      }
    } else {
      // Non-braced: scan to the next ';'. The value may contain '=' (only
      // the first '=' separates key from value).
      size_t end = opts.find(';', value_start);
      if (end == std::string::npos) {
        value = trim(opts.substr(value_start));
        pos = std::string::npos;
      } else {
        value = trim(opts.substr(value_start, end - value_start));
        pos = end;
      }
    }

    (*opts_map)[key] = value;
    if (pos == std::string::npos) {
      break;
    } else {
      pos++;
    }
  }

  return Status::OK();
}

Status MapToString(const std::unordered_map<std::string, std::string>& opts_map,
                   std::string* opts_str) {
  assert(opts_str);
  opts_str->clear();
  for (const auto& [key, value] : opts_map) {
    opts_str->append(key);
    opts_str->append("=");
    opts_str->append(value);
    opts_str->append(";");
  }
  return Status::OK();
}

Status GetStringFromDBOptions(std::string* opt_string,
                              const DBOptions& db_options,
                              const std::string& delimiter) {
  ConfigOptions config_options(db_options);
  config_options.delimiter = delimiter;
  return GetStringFromDBOptions(config_options, db_options, opt_string);
}

Status GetStringFromDBOptions(const ConfigOptions& config_options,
                              const DBOptions& db_options,
                              std::string* opt_string) {
  assert(opt_string);
  opt_string->clear();
  auto config = DBOptionsAsConfigurable(db_options);
  return config->GetOptionString(config_options, opt_string);
}

Status GetStringFromColumnFamilyOptions(std::string* opt_string,
                                        const ColumnFamilyOptions& cf_options,
                                        const std::string& delimiter) {
  ConfigOptions config_options;
  config_options.delimiter = delimiter;
  return GetStringFromColumnFamilyOptions(config_options, cf_options,
                                          opt_string);
}

Status GetStringFromColumnFamilyOptions(const ConfigOptions& config_options,
                                        const ColumnFamilyOptions& cf_options,
                                        std::string* opt_string) {
  const auto config = CFOptionsAsConfigurable(cf_options);
  return config->GetOptionString(config_options, opt_string);
}

Status GetStringFromCompressionType(std::string* compression_str,
                                    CompressionType compression_type) {
  bool ok = SerializeEnum<CompressionType>(compression_type_string_map,
                                           compression_type, compression_str);
  if (ok) {
    return Status::OK();
  } else {
    return Status::InvalidArgument("Invalid compression types");
  }
}

Status GetColumnFamilyOptionsFromMap(
    const ConfigOptions& config_options,
    const ColumnFamilyOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    ColumnFamilyOptions* new_options) {
  assert(new_options);

  *new_options = base_options;

  const auto config = CFOptionsAsConfigurable(base_options);
  Status s = ConfigureFromMap<ColumnFamilyOptions>(
      config_options, opts_map, OptionsHelper::kCFOptionsName, config.get(),
      new_options);
  // Translate any errors (NotFound, NotSupported, to InvalidArgument
  if (s.ok() || s.IsInvalidArgument()) {
    return s;
  } else {
    return Status::InvalidArgument(s.getState());
  }
}

Status GetColumnFamilyOptionsFromString(const ConfigOptions& config_options,
                                        const ColumnFamilyOptions& base_options,
                                        const std::string& opts_str,
                                        ColumnFamilyOptions* new_options) {
  std::unordered_map<std::string, std::string> opts_map;
  Status s = StringToMap(opts_str, &opts_map);
  if (!s.ok()) {
    *new_options = base_options;
    return s;
  }
  return GetColumnFamilyOptionsFromMap(config_options, base_options, opts_map,
                                       new_options);
}

Status GetDBOptionsFromMap(
    const ConfigOptions& config_options, const DBOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    DBOptions* new_options) {
  assert(new_options);
  *new_options = base_options;
  auto config = DBOptionsAsConfigurable(base_options);
  Status s = ConfigureFromMap<DBOptions>(config_options, opts_map,
                                         OptionsHelper::kDBOptionsName,
                                         config.get(), new_options);
  // Translate any errors (NotFound, NotSupported, to InvalidArgument
  if (s.ok() || s.IsInvalidArgument()) {
    return s;
  } else {
    return Status::InvalidArgument(s.getState());
  }
}

Status GetDBOptionsFromString(const ConfigOptions& config_options,
                              const DBOptions& base_options,
                              const std::string& opts_str,
                              DBOptions* new_options) {
  std::unordered_map<std::string, std::string> opts_map;
  Status s = StringToMap(opts_str, &opts_map);
  if (!s.ok()) {
    *new_options = base_options;
    return s;
  }
  return GetDBOptionsFromMap(config_options, base_options, opts_map,
                             new_options);
}

Status GetOptionsFromString(const Options& base_options,
                            const std::string& opts_str, Options* new_options) {
  ConfigOptions config_options(base_options);
  config_options.input_strings_escaped = false;
  config_options.ignore_unknown_options = false;

  return GetOptionsFromString(config_options, base_options, opts_str,
                              new_options);
}

Status GetOptionsFromString(const ConfigOptions& config_options,
                            const Options& base_options,
                            const std::string& opts_str, Options* new_options) {
  ColumnFamilyOptions new_cf_options;
  std::unordered_map<std::string, std::string> unused_opts;
  std::unordered_map<std::string, std::string> opts_map;

  assert(new_options);
  *new_options = base_options;
  Status s = StringToMap(opts_str, &opts_map);
  if (!s.ok()) {
    return s;
  }
  auto config = DBOptionsAsConfigurable(base_options);
  s = config->ConfigureFromMap(config_options, opts_map, &unused_opts);

  if (s.ok()) {
    DBOptions* new_db_options =
        config->GetOptions<DBOptions>(OptionsHelper::kDBOptionsName);
    if (!unused_opts.empty()) {
      s = GetColumnFamilyOptionsFromMap(config_options, base_options,
                                        unused_opts, &new_cf_options);
      if (s.ok()) {
        *new_options = Options(*new_db_options, new_cf_options);
      }
    } else {
      *new_options = Options(*new_db_options, base_options);
    }
  }
  // Translate any errors (NotFound, NotSupported, to InvalidArgument
  if (s.ok() || s.IsInvalidArgument()) {
    return s;
  } else {
    return Status::InvalidArgument(s.getState());
  }
}

std::unordered_map<std::string, EncodingType>
    OptionsHelper::encoding_type_string_map = {{"kPlain", kPlain},
                                               {"kPrefix", kPrefix}};

std::unordered_map<std::string, CompactionStyle>
    OptionsHelper::compaction_style_string_map = {
        {"kCompactionStyleLevel", kCompactionStyleLevel},
        {"kCompactionStyleUniversal", kCompactionStyleUniversal},
        {"kCompactionStyleFIFO", kCompactionStyleFIFO},
        {"kCompactionStyleNone", kCompactionStyleNone}};

std::unordered_map<std::string, CompactionPri>
    OptionsHelper::compaction_pri_string_map = {
        {"kByCompensatedSize", kByCompensatedSize},
        {"kOldestLargestSeqFirst", kOldestLargestSeqFirst},
        {"kOldestSmallestSeqFirst", kOldestSmallestSeqFirst},
        {"kMinOverlappingRatio", kMinOverlappingRatio},
        {"kRoundRobin", kRoundRobin}};

std::unordered_map<std::string, CompactionStopStyle>
    OptionsHelper::compaction_stop_style_string_map = {
        {"kCompactionStopStyleSimilarSize", kCompactionStopStyleSimilarSize},
        {"kCompactionStopStyleTotalSize", kCompactionStopStyleTotalSize}};

std::unordered_map<std::string, Temperature>
    OptionsHelper::temperature_string_map = {
        {"kUnknown", Temperature::kUnknown}, {"kHot", Temperature::kHot},
        {"kWarm", Temperature::kWarm},       {"kCool", Temperature::kCool},
        {"kCold", Temperature::kCold},       {"kIce", Temperature::kIce}};

std::unordered_map<std::string, PrepopulateBlobCache>
    OptionsHelper::prepopulate_blob_cache_string_map = {
        {"kDisable", PrepopulateBlobCache::kDisable},
        {"kFlushOnly", PrepopulateBlobCache::kFlushOnly}};

Status OptionTypeInfo::NextToken(const std::string& opts, char delimiter,
                                 size_t pos, size_t* end, std::string* token) {
  while (pos < opts.size() && isspace(opts[pos])) {
    ++pos;
  }
  // Empty value at the end
  if (pos >= opts.size()) {
    *token = "";
    *end = std::string::npos;
    return Status::OK();
  } else if (opts[pos] == '{') {
    int count = 1;
    size_t brace_pos = pos + 1;
    while (brace_pos < opts.size()) {
      if (opts[brace_pos] == '{') {
        ++count;
      } else if (opts[brace_pos] == '}') {
        --count;
        if (count == 0) {
          break;
        }
      }
      ++brace_pos;
    }
    // found the matching closing brace
    if (count == 0) {
      *token = trim(opts.substr(pos + 1, brace_pos - pos - 1));
      // skip all whitespace and move to the next delimiter
      // brace_pos points to the next position after the matching '}'
      pos = brace_pos + 1;
      while (pos < opts.size() && isspace(opts[pos])) {
        ++pos;
      }
      if (pos < opts.size() && opts[pos] != delimiter) {
        return Status::InvalidArgument("Unexpected chars after nested options");
      }
      *end = pos;
    } else {
      return Status::InvalidArgument(
          "Mismatched curly braces for nested options");
    }
  } else {
    *end = opts.find(delimiter, pos);
    if (*end == std::string::npos) {
      // It either ends with a trailing semi-colon or the last key-value pair
      *token = trim(opts.substr(pos));
    } else {
      *token = trim(opts.substr(pos, *end - pos));
    }
  }
  return Status::OK();
}

std::string OptionTypeInfo::StripOuterBraces(const std::string& value) {
  if (value.size() < 2 || value.front() != '{' || value.back() != '}') {
    return value;
  }
  // Verify the leading '{' actually pairs with the trailing '}'.
  // For `{a:b}` the leading brace closes only at the trailing `}` (one
  // matching pair, OK to strip). For `{a}:{b}` the leading brace closes
  // at the first inner `}`, so the leading and trailing braces are
  // independent -- leave the value alone. We strip at most one layer:
  // each layer of `{}` corresponds to one level of nesting that the
  // encoder added for that level, and the typed parser at this level
  // wants to peel exactly its own layer.
  int depth = 0;
  for (size_t i = 0; i + 1 < value.size(); ++i) {
    if (value[i] == '{') {
      ++depth;
    } else if (value[i] == '}') {
      --depth;
      if (depth == 0) {
        return value;  // outer braces are independent
      }
    }
  }
  return value.substr(1, value.size() - 2);
}

Status OptionTypeInfo::Parse(const ConfigOptions& config_options,
                             const std::string& opt_name,
                             const std::string& value, void* opt_ptr) const {
  if (IsDeprecated()) {
    return Status::OK();
  }
  try {
    const std::string& opt_value = config_options.input_strings_escaped
                                       ? UnescapeOptionString(value)
                                       : value;

    if (opt_ptr == nullptr) {
      return Status::NotFound("Nullptr option", opt_name);
    } else if (parse_func_ != nullptr) {
      ConfigOptions copy = config_options;
      copy.invoke_prepare_options = false;
      void* opt_addr = GetOffset(opt_ptr);
      return parse_func_(copy, opt_name, opt_value, opt_addr);
    } else if (ParseOptionHelper(GetOffset(opt_ptr), type_,
                                 StripOuterBraces(opt_value))) {
      // Scalar types (int, bool, enum, string, ...). StringToMap now
      // preserves outer braces in nested values, so a hand-crafted
      // `key={42}` (or `db_log_dir={/tmp}`) lands here with a wrap
      // that scalar parsers don't expect; permissively strip one
      // level so braced scalar input still parses as before.
      return Status::OK();
    } else if (IsConfigurable()) {
      // The option is <config>.<name>
      Configurable* config = AsRawPointer<Configurable>(opt_ptr);
      if (opt_value.empty()) {
        return Status::OK();
      } else if (config == nullptr) {
        return Status::NotFound("Could not find configurable: ", opt_name);
      } else {
        ConfigOptions copy = config_options;
        copy.ignore_unknown_options = false;
        copy.invoke_prepare_options = false;
        if (opt_value.find('=') != std::string::npos) {
          return config->ConfigureFromString(copy, opt_value);
        } else {
          return config->ConfigureOption(copy, opt_name, opt_value);
        }
      }
    } else if (IsByName()) {
      return Status::NotSupported("Deserializing the option " + opt_name +
                                  " is not supported");
    } else {
      return Status::InvalidArgument("Error parsing:", opt_name);
    }
  } catch (std::exception& e) {
    return Status::InvalidArgument("Error parsing " + opt_name + ":" +
                                   std::string(e.what()));
  }
}

Status OptionTypeInfo::ParseType(
    const ConfigOptions& config_options, const std::string& opts_str,
    const std::unordered_map<std::string, OptionTypeInfo>& type_map,
    void* opt_addr, std::unordered_map<std::string, std::string>* unused) {
  std::unordered_map<std::string, std::string> opts_map;
  Status status = StringToMap(opts_str, &opts_map);
  if (!status.ok()) {
    return status;
  } else {
    return ParseType(config_options, opts_map, type_map, opt_addr, unused);
  }
}

Status OptionTypeInfo::ParseType(
    const ConfigOptions& config_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    const std::unordered_map<std::string, OptionTypeInfo>& type_map,
    void* opt_addr, std::unordered_map<std::string, std::string>* unused) {
  for (const auto& opts_iter : opts_map) {
    std::string opt_name;
    const auto* opt_info = Find(opts_iter.first, type_map, &opt_name);
    if (opt_info != nullptr) {
      Status status =
          opt_info->Parse(config_options, opt_name, opts_iter.second, opt_addr);
      if (!status.ok()) {
        return status;
      }
    } else if (unused != nullptr) {
      (*unused)[opts_iter.first] = opts_iter.second;
    } else if (!config_options.ignore_unknown_options) {
      return Status::NotFound("Unrecognized option", opts_iter.first);
    }
  }
  return Status::OK();
}

Status OptionTypeInfo::ParseStruct(
    const ConfigOptions& config_options, const std::string& struct_name,
    const std::unordered_map<std::string, OptionTypeInfo>* struct_map,
    const std::string& opt_name, const std::string& opt_value, void* opt_addr) {
  assert(struct_map);
  Status status;
  if (opt_name == struct_name || EndsWith(opt_name, "." + struct_name)) {
    // This option represents the entire struct
    std::unordered_map<std::string, std::string> unused;
    status =
        ParseType(config_options, opt_value, *struct_map, opt_addr, &unused);
    if (status.ok() && !unused.empty() &&
        !config_options.ignore_unknown_options) {
      status = Status::InvalidArgument(
          "Unrecognized option", struct_name + "." + unused.begin()->first);
    }
  } else if (StartsWith(opt_name, struct_name + ".")) {
    // This option represents a nested field in the struct (e.g, struct.field)
    std::string elem_name;
    const auto opt_info =
        Find(opt_name.substr(struct_name.size() + 1), *struct_map, &elem_name);
    if (opt_info != nullptr) {
      status = opt_info->Parse(config_options, elem_name, opt_value, opt_addr);
    } else if (!config_options.ignore_unknown_options) {
      status = Status::InvalidArgument("Unrecognized option", opt_name);
    }
  } else {
    // This option represents a field in the struct (e.g. field)
    std::string elem_name;
    const auto opt_info = Find(opt_name, *struct_map, &elem_name);
    if (opt_info != nullptr) {
      status = opt_info->Parse(config_options, elem_name, opt_value, opt_addr);
    } else if (!config_options.ignore_unknown_options) {
      status = Status::InvalidArgument("Unrecognized option",
                                       struct_name + "." + opt_name);
    }
  }
  return status;
}

Status OptionTypeInfo::Serialize(const ConfigOptions& config_options,
                                 const std::string& opt_name,
                                 const void* const opt_ptr,
                                 std::string* opt_value) const {
  // If the option is no longer used in rocksdb and marked as deprecated,
  // we skip it in the serialization.
  if (opt_ptr == nullptr || IsDeprecated()) {
    return Status::OK();
  } else if (IsEnabled(OptionTypeFlags::kDontSerialize)) {
    return Status::NotSupported("Cannot serialize option: ", opt_name);
  } else if (serialize_func_ != nullptr) {
    const void* opt_addr = GetOffset(opt_ptr);
    return serialize_func_(config_options, opt_name, opt_addr, opt_value);
  } else if (IsCustomizable()) {
    const Customizable* custom = AsRawPointer<Customizable>(opt_ptr);
    opt_value->clear();
    if (custom == nullptr) {
      // We do not have a custom object to serialize.
      // If the option is not mutable and we are doing only mutable options,
      // we return an empty string (which will cause the option not to be
      // printed). Otherwise, we return the "nullptr" string, which will result
      // in "option=nullptr" being printed.
      if (IsMutable() || !config_options.mutable_options_only) {
        *opt_value = kNullptrString;
      } else {
        *opt_value = "";
      }
    } else if (IsEnabled(OptionTypeFlags::kStringNameOnly) &&
               !config_options.IsDetailed()) {
      if (!config_options.mutable_options_only || IsMutable()) {
        *opt_value = custom->GetId();
      }
    } else {
      ConfigOptions embedded = config_options;
      embedded.delimiter = ";";
      // If this option is mutable, everything inside it should be considered
      // mutable
      if (IsMutable()) {
        embedded.mutable_options_only = false;
      }
      std::string value = custom->ToString(embedded);
      if (!embedded.mutable_options_only ||
          value.find('=') != std::string::npos) {
        *opt_value = value;
      } else {
        *opt_value = "";
      }
    }
    return Status::OK();
  } else if (IsConfigurable()) {
    const Configurable* config = AsRawPointer<Configurable>(opt_ptr);
    if (config != nullptr) {
      ConfigOptions embedded = config_options;
      embedded.delimiter = ";";
      *opt_value = config->ToString(embedded);
    }
    return Status::OK();
  } else if (config_options.mutable_options_only && !IsMutable()) {
    return Status::OK();
  } else if (SerializeSingleOptionHelper(GetOffset(opt_ptr), type_,
                                         opt_value)) {
    return Status::OK();
  } else {
    return Status::InvalidArgument("Cannot serialize option: ", opt_name);
  }
}

Status OptionTypeInfo::SerializeType(
    const ConfigOptions& config_options,
    const std::unordered_map<std::string, OptionTypeInfo>& type_map,
    const void* opt_addr, std::string* result) {
  Status status;
  for (const auto& iter : type_map) {
    std::string single;
    const auto& opt_info = iter.second;
    if (opt_info.ShouldSerialize()) {
      status =
          opt_info.Serialize(config_options, iter.first, opt_addr, &single);
      if (!status.ok()) {
        return status;
      } else {
        result->append(iter.first + "=" + single + config_options.delimiter);
      }
    }
  }
  return status;
}

Status OptionTypeInfo::SerializeStruct(
    const ConfigOptions& config_options, const std::string& struct_name,
    const std::unordered_map<std::string, OptionTypeInfo>* struct_map,
    const std::string& opt_name, const void* opt_addr, std::string* value) {
  assert(struct_map);
  Status status;
  if (EndsWith(opt_name, struct_name)) {
    // We are going to write the struct as "{ prop1=value1; prop2=value2;}.
    // Set the delimiter to ";" so that the everything will be on one line.
    ConfigOptions embedded = config_options;
    embedded.delimiter = ";";

    // This option represents the entire struct
    std::string result;
    status = SerializeType(embedded, *struct_map, opt_addr, &result);
    if (!status.ok()) {
      return status;
    } else {
      *value = "{" + result + "}";
    }
  } else if (StartsWith(opt_name, struct_name + ".")) {
    // This option represents a nested field in the struct (e.g, struct.field)
    std::string elem_name;
    const auto opt_info =
        Find(opt_name.substr(struct_name.size() + 1), *struct_map, &elem_name);
    if (opt_info != nullptr) {
      status = opt_info->Serialize(config_options, elem_name, opt_addr, value);
    } else {
      status = Status::InvalidArgument("Unrecognized option", opt_name);
    }
  } else {
    // This option represents a field in the struct (e.g. field)
    std::string elem_name;
    const auto opt_info = Find(opt_name, *struct_map, &elem_name);
    if (opt_info == nullptr) {
      status = Status::InvalidArgument("Unrecognized option", opt_name);
    } else if (opt_info->ShouldSerialize()) {
      status = opt_info->Serialize(config_options, opt_name + "." + elem_name,
                                   opt_addr, value);
    }
  }
  return status;
}

template <typename T>
bool IsOptionEqual(const void* offset1, const void* offset2) {
  return (*static_cast<const T*>(offset1) == *static_cast<const T*>(offset2));
}

static bool AreEqualDoubles(const double a, const double b) {
  return (fabs(a - b) < 0.00001);
}

static bool AreOptionsEqual(OptionType type, const void* this_offset,
                            const void* that_offset) {
  switch (type) {
    case OptionType::kBoolean:
      return IsOptionEqual<bool>(this_offset, that_offset);
    case OptionType::kInt:
      return IsOptionEqual<int>(this_offset, that_offset);
    case OptionType::kUInt:
      return IsOptionEqual<unsigned int>(this_offset, that_offset);
    case OptionType::kInt32T:
      return IsOptionEqual<int32_t>(this_offset, that_offset);
    case OptionType::kInt64T: {
      int64_t v1;
      int64_t v2;
      GetUnaligned(static_cast<const int64_t*>(this_offset), &v1);
      GetUnaligned(static_cast<const int64_t*>(that_offset), &v2);
      return (v1 == v2);
    }
    case OptionType::kUInt8T:
      return IsOptionEqual<uint8_t>(this_offset, that_offset);
    case OptionType::kUInt32T:
      return IsOptionEqual<uint32_t>(this_offset, that_offset);
    case OptionType::kUInt64T: {
      uint64_t v1;
      uint64_t v2;
      GetUnaligned(static_cast<const uint64_t*>(this_offset), &v1);
      GetUnaligned(static_cast<const uint64_t*>(that_offset), &v2);
      return (v1 == v2);
    }
    case OptionType::kSizeT: {
      size_t v1;
      size_t v2;
      GetUnaligned(static_cast<const size_t*>(this_offset), &v1);
      GetUnaligned(static_cast<const size_t*>(that_offset), &v2);
      return (v1 == v2);
    }
    case OptionType::kAtomicInt:
      return IsOptionEqual<std::atomic<int>>(this_offset, that_offset);
    case OptionType::kString:
      return IsOptionEqual<std::string>(this_offset, that_offset);
    case OptionType::kDouble:
      return AreEqualDoubles(*static_cast<const double*>(this_offset),
                             *static_cast<const double*>(that_offset));
    case OptionType::kCompactionStyle:
      return IsOptionEqual<CompactionStyle>(this_offset, that_offset);
    case OptionType::kCompactionStopStyle:
      return IsOptionEqual<CompactionStopStyle>(this_offset, that_offset);
    case OptionType::kCompactionPri:
      return IsOptionEqual<CompactionPri>(this_offset, that_offset);
    case OptionType::kCompressionType:
      return IsOptionEqual<CompressionType>(this_offset, that_offset);
    case OptionType::kChecksumType:
      return IsOptionEqual<ChecksumType>(this_offset, that_offset);
    case OptionType::kEncodingType:
      return IsOptionEqual<EncodingType>(this_offset, that_offset);
    case OptionType::kEncodedString:
      return IsOptionEqual<std::string>(this_offset, that_offset);
    case OptionType::kTemperature:
      return IsOptionEqual<Temperature>(this_offset, that_offset);
    default:
      return false;
  }  // End switch
}

bool OptionTypeInfo::AreEqual(const ConfigOptions& config_options,
                              const std::string& opt_name,
                              const void* const this_ptr,
                              const void* const that_ptr,
                              std::string* mismatch) const {
  auto level = GetSanityLevel();
  if (!config_options.IsCheckEnabled(level)) {
    return true;  // If the sanity level is not being checked, skip it
  }
  if (this_ptr == nullptr || that_ptr == nullptr) {
    if (this_ptr == that_ptr) {
      return true;
    }
  } else if (equals_func_ != nullptr) {
    const void* this_addr = GetOffset(this_ptr);
    const void* that_addr = GetOffset(that_ptr);
    if (equals_func_(config_options, opt_name, this_addr, that_addr,
                     mismatch)) {
      return true;
    }
  } else {
    const void* this_addr = GetOffset(this_ptr);
    const void* that_addr = GetOffset(that_ptr);
    if (AreOptionsEqual(type_, this_addr, that_addr)) {
      return true;
    } else if (IsConfigurable()) {
      const auto* this_config = AsRawPointer<Configurable>(this_ptr);
      const auto* that_config = AsRawPointer<Configurable>(that_ptr);
      if (this_config == that_config) {
        return true;
      } else if (this_config != nullptr && that_config != nullptr) {
        std::string bad_name;
        bool matches;
        if (level < config_options.sanity_level) {
          ConfigOptions copy = config_options;
          copy.sanity_level = level;
          matches = this_config->AreEquivalent(copy, that_config, &bad_name);
        } else {
          matches = this_config->AreEquivalent(config_options, that_config,
                                               &bad_name);
        }
        if (!matches) {
          *mismatch = opt_name + "." + bad_name;
        }
        return matches;
      }
    }
  }
  if (mismatch->empty()) {
    *mismatch = opt_name;
  }
  return false;
}

bool OptionTypeInfo::TypesAreEqual(
    const ConfigOptions& config_options,
    const std::unordered_map<std::string, OptionTypeInfo>& type_map,
    const void* this_addr, const void* that_addr, std::string* mismatch) {
  for (const auto& iter : type_map) {
    const auto& opt_info = iter.second;
    if (!opt_info.AreEqual(config_options, iter.first, this_addr, that_addr,
                           mismatch)) {
      return false;
    }
  }
  return true;
}

bool OptionTypeInfo::StructsAreEqual(
    const ConfigOptions& config_options, const std::string& struct_name,
    const std::unordered_map<std::string, OptionTypeInfo>* struct_map,
    const std::string& opt_name, const void* this_addr, const void* that_addr,
    std::string* mismatch) {
  assert(struct_map);
  bool matches = true;
  std::string result;
  if (EndsWith(opt_name, struct_name)) {
    // This option represents the entire struct
    matches = TypesAreEqual(config_options, *struct_map, this_addr, that_addr,
                            &result);
    if (!matches) {
      *mismatch = struct_name + "." + result;
      return false;
    }
  } else if (StartsWith(opt_name, struct_name + ".")) {
    // This option represents a nested field in the struct (e.g, struct.field)
    std::string elem_name;
    const auto opt_info =
        Find(opt_name.substr(struct_name.size() + 1), *struct_map, &elem_name);
    assert(opt_info);
    if (opt_info == nullptr) {
      *mismatch = opt_name;
      matches = false;
    } else if (!opt_info->AreEqual(config_options, elem_name, this_addr,
                                   that_addr, &result)) {
      matches = false;
      *mismatch = struct_name + "." + result;
    }
  } else {
    // This option represents a field in the struct (e.g. field)
    std::string elem_name;
    const auto opt_info = Find(opt_name, *struct_map, &elem_name);
    assert(opt_info);
    if (opt_info == nullptr) {
      *mismatch = struct_name + "." + opt_name;
      matches = false;
    } else if (!opt_info->AreEqual(config_options, elem_name, this_addr,
                                   that_addr, &result)) {
      matches = false;
      *mismatch = struct_name + "." + result;
    }
  }
  return matches;
}

bool MatchesOptionsTypeFromMap(
    const ConfigOptions& config_options,
    const std::unordered_map<std::string, OptionTypeInfo>& type_map,
    const void* const this_ptr, const void* const that_ptr,
    std::string* mismatch) {
  for (auto& pair : type_map) {
    // We skip checking deprecated variables as they might
    // contain random values since they might not be initialized
    if (config_options.IsCheckEnabled(pair.second.GetSanityLevel())) {
      if (!pair.second.AreEqual(config_options, pair.first, this_ptr, that_ptr,
                                mismatch) &&
          !pair.second.AreEqualByName(config_options, pair.first, this_ptr,
                                      that_ptr)) {
        return false;
      }
    }
  }
  return true;
}

bool OptionTypeInfo::AreEqualByName(const ConfigOptions& config_options,
                                    const std::string& opt_name,
                                    const void* const this_ptr,
                                    const void* const that_ptr) const {
  if (IsByName()) {
    std::string that_value;
    if (Serialize(config_options, opt_name, that_ptr, &that_value).ok()) {
      return AreEqualByName(config_options, opt_name, this_ptr, that_value);
    }
  }
  return false;
}

bool OptionTypeInfo::AreEqualByName(const ConfigOptions& config_options,
                                    const std::string& opt_name,
                                    const void* const opt_ptr,
                                    const std::string& that_value) const {
  std::string this_value;
  if (!IsByName()) {
    return false;
  } else if (!Serialize(config_options, opt_name, opt_ptr, &this_value).ok()) {
    return false;
  } else if (IsEnabled(OptionVerificationType::kByNameAllowFromNull)) {
    if (that_value == kNullptrString) {
      return true;
    }
  } else if (IsEnabled(OptionVerificationType::kByNameAllowNull)) {
    if (that_value == kNullptrString) {
      return true;
    }
  }
  return (this_value == that_value);
}

Status OptionTypeInfo::Prepare(const ConfigOptions& config_options,
                               const std::string& name, void* opt_ptr) const {
  if (ShouldPrepare()) {
    if (prepare_func_ != nullptr) {
      void* opt_addr = GetOffset(opt_ptr);
      return prepare_func_(config_options, name, opt_addr);
    } else if (IsConfigurable()) {
      Configurable* config = AsRawPointer<Configurable>(opt_ptr);
      if (config != nullptr) {
        return config->PrepareOptions(config_options);
      } else if (!CanBeNull()) {
        return Status::NotFound("Missing configurable object", name);
      }
    }
  }
  return Status::OK();
}

Status OptionTypeInfo::Validate(const DBOptions& db_opts,
                                const ColumnFamilyOptions& cf_opts,
                                const std::string& name,
                                const void* opt_ptr) const {
  if (ShouldValidate()) {
    if (validate_func_ != nullptr) {
      const void* opt_addr = GetOffset(opt_ptr);
      return validate_func_(db_opts, cf_opts, name, opt_addr);
    } else if (IsConfigurable()) {
      const Configurable* config = AsRawPointer<Configurable>(opt_ptr);
      if (config != nullptr) {
        return config->ValidateOptions(db_opts, cf_opts);
      } else if (!CanBeNull()) {
        return Status::NotFound("Missing configurable object", name);
      }
    }
  }
  return Status::OK();
}

const OptionTypeInfo* OptionTypeInfo::Find(
    const std::string& opt_name,
    const std::unordered_map<std::string, OptionTypeInfo>& opt_map,
    std::string* elem_name) {
  const auto iter = opt_map.find(opt_name);  // Look up the value in the map
  if (iter != opt_map.end()) {               // Found the option in the map
    *elem_name = opt_name;                   // Return the name
    return &(iter->second);  // Return the contents of the iterator
  } else {
    auto idx = opt_name.find('.');              // Look for a separator
    if (idx > 0 && idx != std::string::npos) {  // We found a separator
      auto siter =
          opt_map.find(opt_name.substr(0, idx));  // Look for the short name
      if (siter != opt_map.end()) {               // We found the short name
        if (siter->second.IsStruct() ||           // If the object is a struct
            siter->second.IsConfigurable()) {     // or a Configurable
          *elem_name = opt_name.substr(idx + 1);  // Return the rest
          return &(siter->second);  // Return the contents of the iterator
        }
      }
    }
  }
  return nullptr;
}

}  // namespace ROCKSDB_NAMESPACE
