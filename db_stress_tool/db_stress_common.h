//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The test uses an array to compare against values written to the database.
// Keys written to the array are in 1:1 correspondence to the actual values in
// the database according to the formula in the function GenerateValue.

// Space is reserved in the array from 0 to FLAGS_max_key and values are
// randomly written/deleted/read from those positions. During verification we
// compare all the positions in the array. To shorten/elongate the running
// time, you could change the settings: FLAGS_max_key, FLAGS_ops_per_thread,
// (sometimes also FLAGS_threads).
//
// NOTE that if FLAGS_test_batches_snapshots is set, the test will have
// different behavior. See comment of the flag for details.

#ifdef GFLAGS
#pragma once
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cinttypes>
#include <exception>
#include <queue>
#include <thread>

#include "db/db_impl/db_impl.h"
#include "db/version_set.h"
#include "db/wide/wide_columns_helper.h"
#include "db_stress_tool/db_stress_env_wrapper.h"
#include "db_stress_tool/db_stress_listener.h"
#include "db_stress_tool/db_stress_shared_state.h"
#include "db_stress_tool/db_stress_status.h"
#include "db_stress_tool/db_stress_test_base.h"
#include "logging/logging.h"
#include "monitoring/histogram.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/cache.h"
#include "rocksdb/env.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/statistics.h"
#include "rocksdb/utilities/backup_engine.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/db_ttl.h"
#include "rocksdb/utilities/debug.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/write_buffer_manager.h"
#include "test_util/testutil.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/gflags_compat.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/string_util.h"
#include "utilities/blob_db/blob_db.h"
#include "utilities/fault_injection_fs.h"
#include "utilities/merge_operators.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;

DECLARE_uint64(seed);
DECLARE_bool(read_only);
DECLARE_int64(max_key);
DECLARE_double(hot_key_alpha);
DECLARE_int32(max_key_len);
DECLARE_string(key_len_percent_dist);
DECLARE_int32(key_window_scale_factor);
DECLARE_int32(column_families);
DECLARE_string(options_file);
DECLARE_int64(active_width);
DECLARE_bool(test_batches_snapshots);
DECLARE_bool(atomic_flush);
DECLARE_int32(lock_wal_one_in);
DECLARE_bool(test_cf_consistency);
DECLARE_bool(test_multi_ops_txns);
DECLARE_int32(threads);
DECLARE_int32(ttl);
DECLARE_int32(value_size_mult);
DECLARE_int32(compaction_readahead_size);
DECLARE_bool(enable_pipelined_write);
DECLARE_bool(verify_before_write);
DECLARE_bool(histogram);
DECLARE_bool(destroy_db_initially);
DECLARE_bool(destroy_db_and_exit);
DECLARE_string(delete_dir_and_exit);
DECLARE_bool(verbose);
DECLARE_bool(progress_reports);
DECLARE_string(stress_diagnostics_dir);
DECLARE_bool(stress_diagnostics_breadcrumbs);
DECLARE_uint64(stress_diagnostics_breadcrumb_entries);
DECLARE_bool(stress_diagnostics_scan_witness);
DECLARE_uint64(stress_diagnostics_scan_witness_entries);
DECLARE_uint64(db_write_buffer_size);
DECLARE_int32(write_buffer_size);
DECLARE_int32(max_write_buffer_number);
DECLARE_int32(min_write_buffer_number_to_merge);
DECLARE_int64(max_write_buffer_size_to_maintain);
DECLARE_bool(use_write_buffer_manager);
DECLARE_int32(wbm_flush_policy);
DECLARE_double(memtable_prefix_bloom_size_ratio);
DECLARE_bool(memtable_whole_key_filtering);
DECLARE_int32(open_files);
DECLARE_bool(open_files_async);
DECLARE_bool(async_wal_precreate);
DECLARE_uint64(compressed_secondary_cache_size);
DECLARE_int32(compressed_secondary_cache_numshardbits);
DECLARE_int32(secondary_cache_update_interval);
DECLARE_double(compressed_secondary_cache_ratio);
DECLARE_int32(compaction_style);
DECLARE_int32(compaction_pri);
DECLARE_int32(num_levels);
DECLARE_int32(level0_file_num_compaction_trigger);
DECLARE_int32(level0_slowdown_writes_trigger);
DECLARE_int32(level0_stop_writes_trigger);
DECLARE_int32(block_size);
DECLARE_int32(format_version);
DECLARE_bool(separate_key_value_in_data_block);
DECLARE_int32(index_block_restart_interval);
DECLARE_int32(max_background_compactions);
DECLARE_int32(num_bottom_pri_threads);
DECLARE_int32(compaction_thread_pool_adjust_interval);
DECLARE_int32(compaction_thread_pool_variations);
DECLARE_int32(max_background_flushes);
DECLARE_int32(universal_size_ratio);
DECLARE_int32(universal_min_merge_width);
DECLARE_int32(universal_max_merge_width);
DECLARE_int32(universal_max_size_amplification_percent);
DECLARE_int32(universal_max_read_amp);
DECLARE_int32(clear_column_family_one_in);
DECLARE_bool(test_cf_lifecycle);
DECLARE_int32(cf_lifecycle_fault_target);
DECLARE_int32(get_live_files_apis_one_in);
DECLARE_bool(checkpoint_atomic_flush);
DECLARE_int32(get_all_column_family_metadata_one_in);
DECLARE_int32(get_sorted_wal_files_one_in);
DECLARE_int32(get_current_wal_file_one_in);
DECLARE_int32(set_options_one_in);
DECLARE_int32(set_in_place_one_in);
DECLARE_int64(cache_size);
DECLARE_int32(cache_numshardbits);
DECLARE_bool(cache_index_and_filter_blocks);
DECLARE_bool(charge_compression_dictionary_building_buffer);
DECLARE_bool(charge_filter_construction);
DECLARE_bool(charge_table_reader);
DECLARE_bool(charge_file_metadata);
DECLARE_bool(charge_blob_cache);
DECLARE_bool(decouple_partitioned_filters);
DECLARE_int32(top_level_index_pinning);
DECLARE_int32(partition_pinning);
DECLARE_int32(unpartitioned_pinning);
DECLARE_string(cache_type);
DECLARE_uint64(subcompactions);
DECLARE_uint64(periodic_compaction_seconds);
DECLARE_string(daily_offpeak_time_utc);
DECLARE_uint64(compaction_ttl);
DECLARE_bool(fifo_allow_compaction);
DECLARE_uint64(fifo_compaction_max_data_files_size_mb);
DECLARE_uint64(fifo_compaction_max_table_files_size_mb);
DECLARE_bool(fifo_compaction_use_kv_ratio_compaction);
DECLARE_bool(allow_concurrent_memtable_write);
DECLARE_double(experimental_mempurge_threshold);
DECLARE_bool(enable_write_thread_adaptive_yield);
DECLARE_int32(reopen);
DECLARE_double(bloom_bits);
DECLARE_int32(bloom_before_level);
DECLARE_bool(partition_filters);
DECLARE_bool(optimize_filters_for_memory);
DECLARE_bool(detect_filter_construct_corruption);
DECLARE_string(sqfc_name);
DECLARE_uint32(sqfc_version);
DECLARE_bool(use_sqfc_for_range_queries);
DECLARE_int32(index_type);
DECLARE_int32(data_block_index_type);
DECLARE_int32(optimize_key_common_prefix);
DECLARE_int32(index_block_search_type);
DECLARE_double(uniform_cv_threshold);
DECLARE_bool(use_trie_index);
DECLARE_uint32(trie_index_format_version);
DECLARE_bool(use_full_external_table);
DECLARE_int32(index_mode);
DECLARE_int32(read_index);
DECLARE_bool(test_backward_scan);
DECLARE_string(db);
DECLARE_string(secondaries_base);
DECLARE_bool(test_secondary);
DECLARE_int32(open_read_only_one_in);
DECLARE_string(expected_values_dir);
DECLARE_int32(num_dbs);
DECLARE_bool(expected_state_trace_debug);
DECLARE_int64(expected_state_trace_debug_key);
DECLARE_int32(expected_state_trace_debug_max_logs);
DECLARE_bool(verify_checksum);
DECLARE_bool(mmap_read);
DECLARE_bool(mmap_write);
DECLARE_bool(use_direct_reads);
DECLARE_bool(use_direct_io_for_compaction_reads);
DECLARE_bool(use_direct_io_for_flush_and_compaction);
DECLARE_bool(mock_direct_io);
DECLARE_bool(statistics);
DECLARE_bool(sync);
DECLARE_bool(use_fsync);
DECLARE_uint64(stats_dump_period_sec);
DECLARE_uint64(mutex_hold_log_threshold_us);
DECLARE_uint64(max_compaction_trigger_wakeup_seconds);
DECLARE_int32(periodic_compaction_phase_recovery_percent);
DECLARE_uint64(bytes_per_sync);
DECLARE_uint64(wal_bytes_per_sync);
DECLARE_int32(kill_random_test);
DECLARE_string(kill_exclude_prefixes);
DECLARE_uint64(recycle_log_file_num);
DECLARE_int64(target_file_size_base);
DECLARE_int32(target_file_size_multiplier);
DECLARE_uint64(max_bytes_for_level_base);
DECLARE_double(max_bytes_for_level_multiplier);
DECLARE_uint64(rate_limiter_bytes_per_sec);
DECLARE_bool(rate_limit_bg_reads);
DECLARE_bool(rate_limit_user_ops);
DECLARE_bool(rate_limit_auto_wal_flush);
DECLARE_uint64(sst_file_manager_bytes_per_sec);
DECLARE_uint64(sst_file_manager_bytes_per_truncate);
DECLARE_int32(backup_one_in);
DECLARE_uint64(backup_max_size);
DECLARE_int32(checkpoint_one_in);
DECLARE_int32(checkpoint_engine_max_background_operations);
DECLARE_bool(checkpoint_engine_use_link_file_when_available);
DECLARE_int32(parallel_checkpoint_one_in);
DECLARE_int32(subset_cf_checkpoint_one_in);
DECLARE_int32(ingest_external_file_one_in);
DECLARE_int32(ingest_external_file_width);
DECLARE_int32(ingest_external_file_prepare_commit_one_in);
DECLARE_int32(ingest_external_file_use_file_info_one_in);
DECLARE_int32(ingest_external_file_atomic_replace_one_in);
DECLARE_bool(ingest_external_file_with_embedded_blobs);
DECLARE_int32(compact_files_one_in);
DECLARE_int32(compact_range_one_in);
DECLARE_int32(promote_l0_one_in);
DECLARE_int32(mark_for_compaction_one_file_in);
DECLARE_int32(flush_one_in);
DECLARE_int32(key_may_exist_one_in);
DECLARE_int32(reset_stats_one_in);
DECLARE_int32(pause_background_one_in);
DECLARE_int32(disable_file_deletions_one_in);
DECLARE_int32(disable_manual_compaction_one_in);
DECLARE_int32(abort_and_resume_compactions_one_in);
DECLARE_int32(abort_and_resume_cf_compactions_one_in);
DECLARE_int32(compact_range_width);
DECLARE_int32(acquire_snapshot_one_in);
DECLARE_bool(compare_full_db_state_snapshot);
DECLARE_uint64(snapshot_hold_ops);
DECLARE_bool(long_running_snapshots);
DECLARE_bool(use_multiget);
DECLARE_bool(use_async_db_api);
DECLARE_bool(use_get_entity);
DECLARE_bool(use_multi_get_entity);
DECLARE_int32(lazy_entity_read_one_in);
DECLARE_int32(readpercent);
DECLARE_int32(prefixpercent);
DECLARE_int32(writepercent);
DECLARE_int32(delpercent);
DECLARE_int32(delrangepercent);
DECLARE_int32(nooverwritepercent);
DECLARE_int32(iterpercent);
DECLARE_uint64(num_iterations);
DECLARE_int32(customopspercent);
DECLARE_string(compression_type);
DECLARE_string(bottommost_compression_type);
DECLARE_int32(compression_max_dict_bytes);
DECLARE_int32(compression_zstd_max_train_bytes);
DECLARE_int32(compression_parallel_threads);
DECLARE_uint64(compression_max_dict_buffer_bytes);
DECLARE_bool(compression_auto_skip);
DECLARE_int32(compression_auto_skip_min_sample_every);
DECLARE_bool(compression_use_zstd_dict_trainer);
DECLARE_bool(compression_checksum);
DECLARE_string(checksum_type);
DECLARE_string(env_uri);
DECLARE_string(fs_uri);
DECLARE_uint64(ops_per_thread);
DECLARE_uint64(log2_keys_per_lock);
DECLARE_uint64(max_manifest_file_size);
DECLARE_int32(max_manifest_space_amp_pct);
DECLARE_bool(verify_manifest_content_on_close);
DECLARE_bool(in_place_update);
DECLARE_string(memtablerep);
DECLARE_int32(prefix_size);
DECLARE_bool(use_merge);
DECLARE_uint32(use_put_entity_one_in);
DECLARE_bool(use_attribute_group);
DECLARE_bool(use_multi_cf_iterator);
DECLARE_bool(use_full_merge_v1);
DECLARE_int32(sync_wal_one_in);
DECLARE_bool(avoid_unnecessary_blocking_io);
DECLARE_bool(write_dbid_to_manifest);
DECLARE_bool(track_options_file_number_in_manifest);
DECLARE_bool(optimize_manifest_for_recovery);
DECLARE_bool(write_identity_file);
DECLARE_bool(reuse_manifest_on_open);
DECLARE_bool(avoid_flush_during_recovery);
DECLARE_bool(enforce_write_buffer_manager_during_recovery);
DECLARE_uint64(max_write_batch_group_size_bytes);
DECLARE_bool(level_compaction_dynamic_level_bytes);
DECLARE_int32(verify_checksum_one_in);
DECLARE_int32(verify_file_checksums_one_in);
DECLARE_int32(verify_db_one_in);
DECLARE_int32(continuous_verification_interval);
DECLARE_int32(get_property_one_in);
DECLARE_int32(get_properties_of_all_tables_one_in);
DECLARE_string(file_checksum_impl);
DECLARE_bool(verification_only);
DECLARE_string(last_level_temperature);
DECLARE_string(default_write_temperature);
DECLARE_string(default_temperature);
DECLARE_uint32(verify_output_flags);
DECLARE_bool(paranoid_memory_checks);
DECLARE_bool(memtable_verify_per_key_checksum_on_seek);
DECLARE_bool(memtable_batch_lookup_optimization);

// Options for transaction dbs.
// Use TransactionDB (a.k.a. Pessimistic Transaction DB)
// OR OptimisticTransactionDB
DECLARE_bool(use_txn);

// Options for TransactionDB (a.k.a. Pessimistic Transaction DB)
DECLARE_uint64(txn_write_policy);
DECLARE_bool(unordered_write);
DECLARE_bool(use_per_key_point_lock_mgr);

// Options for OptimisticTransactionDB
DECLARE_bool(use_optimistic_txn);
DECLARE_uint64(occ_validation_policy);
DECLARE_bool(share_occ_lock_buckets);
DECLARE_uint32(occ_lock_bucket_count);

// Options for StackableDB-based BlobDB
DECLARE_bool(use_blob_db);
DECLARE_uint64(blob_db_file_size);
DECLARE_bool(blob_db_enable_gc);

// Options for integrated BlobDB
DECLARE_bool(allow_setting_blob_options_dynamically);
DECLARE_bool(enable_blob_files);
DECLARE_bool(enable_blob_direct_write);
DECLARE_uint64(blob_direct_write_partitions);
DECLARE_uint64(min_blob_size);
DECLARE_uint64(blob_file_size);
DECLARE_uint64(blob_file_writable_file_max_buffer_size);
DECLARE_string(blob_compression_type);
DECLARE_bool(enable_blob_garbage_collection);
DECLARE_double(blob_garbage_collection_age_cutoff);
DECLARE_double(blob_garbage_collection_force_threshold);
DECLARE_uint64(blob_compaction_readahead_size);
DECLARE_int32(blob_file_starting_level);
DECLARE_bool(use_blob_cache);
DECLARE_bool(use_shared_block_and_blob_cache);
DECLARE_uint64(blob_cache_size);
DECLARE_int32(blob_cache_numshardbits);
DECLARE_int32(prepopulate_blob_cache);

DECLARE_int32(approximate_size_one_in);
DECLARE_bool(best_efforts_recovery);
DECLARE_bool(skip_verifydb);
DECLARE_string(verify_cpu_corruption_dir);
DECLARE_bool(paranoid_file_checks);
DECLARE_uint64(batch_protection_bytes_per_key);
DECLARE_uint32(memtable_protection_bytes_per_key);
DECLARE_uint32(block_protection_bytes_per_key);

DECLARE_uint64(user_timestamp_size);
DECLARE_bool(persist_user_defined_timestamps);
DECLARE_string(secondary_cache_uri);
DECLARE_int32(secondary_cache_fault_one_in);

DECLARE_int32(prepopulate_block_cache);

DECLARE_bool(two_write_queues);
DECLARE_bool(use_only_the_last_commit_time_batch_for_recovery);
DECLARE_uint64(wp_snapshot_cache_bits);
DECLARE_uint64(wp_commit_cache_bits);

DECLARE_bool(adaptive_readahead);
DECLARE_bool(async_io);
DECLARE_string(wal_compression);
DECLARE_bool(verify_sst_unique_id_in_manifest);
DECLARE_bool(fast_sst_open);

DECLARE_int32(create_timestamped_snapshot_one_in);

DECLARE_bool(allow_data_in_errors);

DECLARE_bool(enable_thread_tracking);

DECLARE_uint32(memtable_max_range_deletions);

DECLARE_uint32(bottommost_file_compaction_delay);

// Tiered storage
DECLARE_int64(preclude_last_level_data_seconds);
DECLARE_int64(preserve_internal_time_seconds);
DECLARE_uint32(use_timed_put_one_in);

DECLARE_int32(verify_iterator_with_expected_state_one_in);
DECLARE_bool(preserve_unverified_changes);

DECLARE_uint64(readahead_size);
DECLARE_uint64(initial_auto_readahead_size);
DECLARE_uint64(max_auto_readahead_size);
DECLARE_uint64(num_file_reads_for_auto_readahead);
DECLARE_bool(auto_readahead_size);
DECLARE_bool(allow_fallocate);
DECLARE_int32(table_cache_numshardbits);
DECLARE_bool(enable_write_thread_adaptive_yield);
DECLARE_uint64(log_readahead_size);
DECLARE_uint64(bgerror_resume_retry_interval);
DECLARE_uint64(delete_obsolete_files_period_micros);
DECLARE_uint64(max_log_file_size);
DECLARE_uint64(log_file_time_to_roll);
DECLARE_bool(use_adaptive_mutex);
DECLARE_bool(advise_random_on_open);
DECLARE_uint64(WAL_ttl_seconds);
DECLARE_uint64(WAL_size_limit_MB);
DECLARE_bool(strict_bytes_per_sync);
DECLARE_bool(avoid_flush_during_shutdown);
DECLARE_bool(fill_cache);
DECLARE_bool(optimize_multiget_for_io);
DECLARE_bool(memtable_insert_hint_per_batch);
DECLARE_bool(dump_malloc_stats);
DECLARE_uint64(stats_history_buffer_size);
DECLARE_bool(skip_stats_update_on_db_open);
DECLARE_bool(optimize_filters_for_hits);
DECLARE_uint64(sample_for_compression);
DECLARE_bool(report_bg_io_stats);
DECLARE_bool(cache_index_and_filter_blocks_with_high_priority);
DECLARE_bool(use_delta_encoding);
DECLARE_bool(verify_compression);
DECLARE_uint32(read_amp_bytes_per_bit);
DECLARE_bool(enable_index_compression);
DECLARE_uint32(index_shortening);
DECLARE_uint32(metadata_charge_policy);
DECLARE_bool(use_adaptive_mutex_lru);
DECLARE_uint64(manifest_preallocation_size);
DECLARE_bool(enable_checksum_handoff);
DECLARE_string(compression_manager);
DECLARE_uint64(max_total_wal_size);
DECLARE_double(high_pri_pool_ratio);
DECLARE_double(low_pri_pool_ratio);
DECLARE_uint64(soft_pending_compaction_bytes_limit);
DECLARE_uint64(hard_pending_compaction_bytes_limit);
DECLARE_uint64(max_sequential_skip_in_iterations);
DECLARE_bool(enable_sst_partitioner_factory);
DECLARE_bool(enable_do_not_compress_roles);
DECLARE_bool(block_align);
DECLARE_uint64(super_block_alignment_size);
DECLARE_uint64(super_block_alignment_space_overhead_ratio);
DECLARE_uint32(lowest_used_cache_tier);
DECLARE_bool(enable_custom_split_merge);
DECLARE_uint32(adm_policy);
DECLARE_bool(enable_memtable_insert_with_hint_prefix_extractor);
DECLARE_bool(check_multiget_consistency);
DECLARE_bool(check_multiget_entity_consistency);
DECLARE_bool(inplace_update_support);
DECLARE_uint32(uncache_aggressiveness);
DECLARE_int32(test_ingest_standalone_range_deletion_one_in);
DECLARE_bool(allow_unprepared_value);
DECLARE_string(file_temperature_age_thresholds);
DECLARE_bool(allow_trivial_copy_when_change_temperature);
DECLARE_uint32(commit_bypass_memtable_one_in);
DECLARE_bool(track_and_verify_wals);
DECLARE_int32(remote_compaction_worker_threads);
DECLARE_int32(remote_compaction_worker_interval);
DECLARE_bool(remote_compaction_failure_fall_back_to_local);
DECLARE_bool(use_separate_remote_compaction_pool);
DECLARE_uint32(openandcompact_max_secondary_open_retries);
DECLARE_int32(allow_resumption_one_in);
DECLARE_bool(auto_refresh_iterator_with_snapshot);
DECLARE_uint32(memtable_op_scan_flush_trigger);
DECLARE_uint32(memtable_avg_op_scan_flush_trigger);
DECLARE_uint32(min_tombstones_for_range_conversion);
DECLARE_uint32(ingest_wbwi_one_in);
DECLARE_bool(universal_reduce_file_locking);
DECLARE_bool(use_multiscan);
DECLARE_bool(multiscan_reverse);
DECLARE_bool(multiscan_use_async_io);
DECLARE_bool(read_scoped_block_buffer_provider);
DECLARE_uint64(multiscan_max_prefetch_memory_bytes);

// Compaction deletion trigger declarations for stress testing
DECLARE_bool(enable_compaction_on_deletion_trigger);
DECLARE_uint64(compaction_on_deletion_min_file_size);
DECLARE_int32(compaction_on_deletion_trigger_count);
DECLARE_int32(compaction_on_deletion_window_size);
DECLARE_double(compaction_on_deletion_ratio);
DECLARE_double(read_triggered_compaction_threshold);

DECLARE_string(listener_uri);

constexpr long KB = 1024;
constexpr int kRandomValueMaxFactor = 3;
constexpr int kValueMaxLen = 100;
constexpr uint32_t kLargePrimeForCommonFactorSkew = 1872439133;

// Base env from --env_uri/--fs_uri. No wrappers (no DbStressFSWrapper, no
// fault injection). Could be PosixEnv or remote. Used for infrastructure ops
// (threads, time, dirs, cleanup). Per-StressTest env adds fault injection +
// DbStressFSWrapper on top for DB I/O.
extern ROCKSDB_NAMESPACE::Env* raw_env;
extern std::shared_ptr<ROCKSDB_NAMESPACE::SecondaryCache>
    compressed_secondary_cache;
extern std::shared_ptr<ROCKSDB_NAMESPACE::Cache> block_cache;
extern std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager> wbm;
extern std::shared_ptr<ROCKSDB_NAMESPACE::RateLimiter> rate_limiter;

extern enum ROCKSDB_NAMESPACE::CompressionType compression_type_e;
extern enum ROCKSDB_NAMESPACE::CompressionType bottommost_compression_type_e;
extern enum ROCKSDB_NAMESPACE::ChecksumType checksum_type_e;

enum RepFactory { kSkipList, kHashSkipList, kVectorRep };

inline enum RepFactory StringToRepFactory(const char* ctype) {
  assert(ctype);

  if (!strcasecmp(ctype, "skip_list")) {
    return kSkipList;
  } else if (!strcasecmp(ctype, "prefix_hash")) {
    return kHashSkipList;
  } else if (!strcasecmp(ctype, "vector")) {
    return kVectorRep;
  }

  fprintf(stdout, "Cannot parse memreptable %s\n", ctype);
  return kSkipList;
}

extern enum RepFactory FLAGS_rep_factory;

namespace ROCKSDB_NAMESPACE {
void RegisterDbStressBdwFlagValidators();

inline enum ROCKSDB_NAMESPACE::CompressionType StringToCompressionType(
    const char* ctype) {
  assert(ctype);

  ROCKSDB_NAMESPACE::CompressionType ret_compression_type;

  if (!strcasecmp(ctype, "disable")) {
    ret_compression_type = ROCKSDB_NAMESPACE::kDisableCompressionOption;
  } else if (!strcasecmp(ctype, "none")) {
    ret_compression_type = ROCKSDB_NAMESPACE::kNoCompression;
  } else if (!strcasecmp(ctype, "snappy")) {
    ret_compression_type = ROCKSDB_NAMESPACE::kSnappyCompression;
  } else if (!strcasecmp(ctype, "zlib")) {
    ret_compression_type = ROCKSDB_NAMESPACE::kZlibCompression;
  } else if (!strcasecmp(ctype, "bzip2")) {
    ret_compression_type = ROCKSDB_NAMESPACE::kBZip2Compression;
  } else if (!strcasecmp(ctype, "lz4")) {
    ret_compression_type = ROCKSDB_NAMESPACE::kLZ4Compression;
  } else if (!strcasecmp(ctype, "lz4hc")) {
    ret_compression_type = ROCKSDB_NAMESPACE::kLZ4HCCompression;
  } else if (!strcasecmp(ctype, "xpress")) {
    ret_compression_type = ROCKSDB_NAMESPACE::kXpressCompression;
  } else if (!strcasecmp(ctype, "zstd")) {
    ret_compression_type = ROCKSDB_NAMESPACE::kZSTD;
  } else {
    fprintf(stderr, "Cannot parse compression type '%s'\n", ctype);
    ret_compression_type =
        ROCKSDB_NAMESPACE::kSnappyCompression;  // default value
  }
  if (ret_compression_type != ROCKSDB_NAMESPACE::kDisableCompressionOption &&
      !CompressionTypeSupported(ret_compression_type)) {
    // Use no compression will be more portable but considering this is
    // only a stress test and snappy is widely available. Use snappy here.
    ret_compression_type = ROCKSDB_NAMESPACE::kSnappyCompression;
  }
  return ret_compression_type;
}

inline enum ROCKSDB_NAMESPACE::ChecksumType StringToChecksumType(
    const char* ctype) {
  assert(ctype);
  auto iter = ROCKSDB_NAMESPACE::checksum_type_string_map.find(ctype);
  if (iter != ROCKSDB_NAMESPACE::checksum_type_string_map.end()) {
    return iter->second;
  }
  fprintf(stderr, "Cannot parse checksum type '%s'\n", ctype);
  return ROCKSDB_NAMESPACE::kCRC32c;
}

inline std::string ChecksumTypeToString(ROCKSDB_NAMESPACE::ChecksumType ctype) {
  auto iter = std::find_if(
      ROCKSDB_NAMESPACE::checksum_type_string_map.begin(),
      ROCKSDB_NAMESPACE::checksum_type_string_map.end(),
      [&](const std::pair<std::string, ROCKSDB_NAMESPACE::ChecksumType>&
              name_and_enum_val) { return name_and_enum_val.second == ctype; });
  assert(iter != ROCKSDB_NAMESPACE::checksum_type_string_map.end());
  return iter->first;
}

inline enum ROCKSDB_NAMESPACE::Temperature StringToTemperature(
    const char* ctype) {
  assert(ctype);
  auto iter = std::find_if(
      ROCKSDB_NAMESPACE::temperature_to_string.begin(),
      ROCKSDB_NAMESPACE::temperature_to_string.end(),
      [&](const std::pair<ROCKSDB_NAMESPACE::Temperature, std::string>&
              temp_and_string_val) {
        return ctype == temp_and_string_val.second;
      });
  assert(iter != ROCKSDB_NAMESPACE::temperature_to_string.end());
  return iter->first;
}

inline std::string TemperatureToString(
    ROCKSDB_NAMESPACE::Temperature temperature) {
  auto iter =
      ROCKSDB_NAMESPACE::OptionsHelper::temperature_to_string.find(temperature);
  assert(iter != ROCKSDB_NAMESPACE::OptionsHelper::temperature_to_string.end());
  return iter->second;
}

inline std::vector<std::string> SplitString(std::string src) {
  std::vector<std::string> ret;
  if (src.empty()) {
    return ret;
  }
  size_t pos = 0;
  size_t pos_comma;
  while ((pos_comma = src.find(',', pos)) != std::string::npos) {
    ret.push_back(src.substr(pos, pos_comma - pos));
    pos = pos_comma + 1;
  }
  ret.push_back(src.substr(pos, src.length()));
  return ret;
}

#ifdef _MSC_VER
#pragma warning(push)
// truncation of constant value on static_cast
#pragma warning(disable : 4309)
#endif
inline bool GetNextPrefix(const ROCKSDB_NAMESPACE::Slice& src, std::string* v) {
  std::string ret = src.ToString();
  for (int i = static_cast<int>(ret.size()) - 1; i >= 0; i--) {
    if (ret[i] != static_cast<char>(255)) {
      ret[i] = ret[i] + 1;
      break;
    } else if (i != 0) {
      ret[i] = 0;
    } else {
      // all FF. No next prefix
      return false;
    }
  }
  *v = ret;
  return true;
}
#ifdef _MSC_VER
#pragma warning(pop)
#endif

// Append `val` to `*key` in fixed-width big-endian format
inline void AppendIntToString(uint64_t val, std::string* key) {
  // PutFixed64 uses little endian
  PutFixed64(key, val);
  // Reverse to get big endian
  char* int_data = &((*key)[key->size() - sizeof(uint64_t)]);
  for (size_t i = 0; i < sizeof(uint64_t) / 2; ++i) {
    std::swap(int_data[i], int_data[sizeof(uint64_t) - 1 - i]);
  }
}

// A struct for maintaining the parameters for generating variable length keys
struct KeyGenContext {
  // Number of adjacent keys in one cycle of key lengths
  uint64_t window;
  // Number of keys of each possible length in a given window
  std::vector<uint64_t> weights;
};
extern KeyGenContext key_gen_ctx;

// Generate a variable length key string from the given int64 val. The
// order of the keys is preserved. The key could be anywhere from 8 to
// max_key_len * 8 bytes.
// The algorithm picks the length based on the
// offset of the val within a configured window and the distribution of the
// number of keys of various lengths in that window. For example, if x, y, x are
// the weights assigned to each possible key length, the keys generated would be
// - {0}...{x-1}
// {(x-1),0}..{(x-1),(y-1)},{(x-1),(y-1),0}..{(x-1),(y-1),(z-1)} and so on.
// Additionally, a trailer of 0-7 bytes could be appended.
inline std::string Key(int64_t val) {
  uint64_t window = key_gen_ctx.window;
  size_t levels = key_gen_ctx.weights.size();
  std::string key;
  // Over-reserve and for now do not bother `shrink_to_fit()` since the key
  // strings are transient.
  key.reserve(FLAGS_max_key_len * 8);

  uint64_t window_idx = static_cast<uint64_t>(val) / window;
  uint64_t offset = static_cast<uint64_t>(val) % window;
  for (size_t level = 0; level < levels; ++level) {
    uint64_t weight = key_gen_ctx.weights[level];
    uint64_t pfx;
    if (level == 0) {
      pfx = window_idx * weight;
    } else {
      pfx = 0;
    }
    pfx += offset >= weight ? weight - 1 : offset;
    AppendIntToString(pfx, &key);
    if (offset < weight) {
      // Use the bottom 3 bits of offset as the number of trailing 'x's in the
      // key. If the next key is going to be of the next level, then skip the
      // trailer as it would break ordering. If the key length is already at
      // max, skip the trailer.
      if (offset < weight - 1 && level < levels - 1) {
        size_t trailer_len = offset & 0x7;
        key.append(trailer_len, 'x');
      }
      break;
    }
    offset -= weight;
  }

  return key;
}

// Given a string key, map it to an index into the expected values buffer
inline bool GetIntVal(std::string big_endian_key, uint64_t* key_p) {
  size_t size_key = big_endian_key.size();
  std::vector<uint64_t> prefixes;

  assert(size_key <= key_gen_ctx.weights.size() * sizeof(uint64_t));

  std::string little_endian_key;
  little_endian_key.resize(size_key);
  for (size_t start = 0; start + sizeof(uint64_t) <= size_key;
       start += sizeof(uint64_t)) {
    size_t end = start + sizeof(uint64_t);
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
      little_endian_key[start + i] = big_endian_key[end - 1 - i];
    }
    Slice little_endian_slice =
        Slice(&little_endian_key[start], sizeof(uint64_t));
    uint64_t pfx;
    if (!GetFixed64(&little_endian_slice, &pfx)) {
      return false;
    }
    prefixes.emplace_back(pfx);
  }

  uint64_t key = 0;
  for (size_t i = 0; i < prefixes.size(); ++i) {
    uint64_t pfx = prefixes[i];
    key += (pfx / key_gen_ctx.weights[i]) * key_gen_ctx.window +
           pfx % key_gen_ctx.weights[i];
    if (i < prefixes.size() - 1) {
      // The encoding writes a `key_gen_ctx.weights[i] - 1` that counts for
      // `key_gen_ctx.weights[i]` when there are more prefixes to come. So we
      // need to add back the one here as we're at a non-last prefix.
      ++key;
    }
  }
  *key_p = key;
  return true;
}

// Given a string prefix, map it to the first corresponding index in the
// expected values buffer.
inline bool GetFirstIntValInPrefix(std::string big_endian_prefix,
                                   uint64_t* key_p) {
  size_t size_key = big_endian_prefix.size();
  // Pad with zeros to make it a multiple of 8. This function may be called
  // with a prefix, in which case we return the first index that falls
  // inside or outside that prefix, dependeing on whether the prefix is
  // the start of upper bound of a scan
  unsigned int pad = sizeof(uint64_t) - (size_key % sizeof(uint64_t));
  if (pad < sizeof(uint64_t)) {
    big_endian_prefix.append(pad, '\0');
  }
  return GetIntVal(std::move(big_endian_prefix), key_p);
}

inline uint64_t GetPrefixKeyCount(const std::string& prefix,
                                  const std::string& ub) {
  uint64_t start = 0;
  uint64_t end = 0;

  if (!GetFirstIntValInPrefix(prefix, &start) ||
      !GetFirstIntValInPrefix(ub, &end)) {
    return 0;
  }

  return end - start;
}

inline std::string StringToHex(const std::string& str) {
  std::string result = "0x";
  result.append(Slice(str).ToString(true));
  return result;
}

inline std::string WideColumnsToHex(const WideColumns& columns) {
  if (columns.empty()) {
    return std::string();
  }

  std::ostringstream oss;

  WideColumnsHelper::DumpWideColumns(columns, oss, true);

  return oss.str();
}

// Unified output format for double parameters
inline std::string FormatDoubleParam(double param) {
  return std::to_string(param);
}

// Make sure that double parameter is a value we can reproduce by
// re-inputting the value printed.
inline void SanitizeDoubleParam(double* param) {
  *param = std::atof(FormatDoubleParam(*param).c_str());
}

void PoolSizeChangeThread(void* v);

void DbVerificationThread(void* v);

void LivenessWatchdogThread(void* v);

void RemoteCompactionWorkerThread(void* v);

void CompressedCacheSetCapacityThread(void* v);

void TimestampedSnapshotsThread(void* v);

void PrintKeyValue(int cf, uint64_t key, const char* value, size_t sz);

int64_t GenerateOneKey(ThreadState* thread, uint64_t iteration);

std::vector<int64_t> GenerateNKeys(ThreadState* thread, int num_keys,
                                   uint64_t iteration);

size_t GenerateValue(uint32_t rand, char* v, size_t max_sz);
uint32_t GetValueBase(Slice s);

WideColumns GenerateWideColumns(uint32_t value_base, const Slice& slice);
WideColumns GenerateExpectedWideColumns(uint32_t value_base,
                                        const Slice& slice);
bool VerifyWideColumns(const Slice& value, const WideColumns& columns);
bool VerifyWideColumns(const WideColumns& columns);
bool VerifyIteratorAttributeGroups(
    const IteratorAttributeGroups& attribute_groups);

AttributeGroups GenerateAttributeGroups(
    const std::vector<ColumnFamilyHandle*>& cfhs, uint32_t value_base,
    const Slice& slice);

Status DbStressGet(DB* db, const ReadOptions& options,
                   ColumnFamilyHandle* column_family, const Slice& key,
                   PinnableSlice* value, std::string* timestamp = nullptr);
Status DbStressGet(DB* db, const ReadOptions& options,
                   ColumnFamilyHandle* column_family, const Slice& key,
                   std::string* value, std::string* timestamp = nullptr);
Status DbStressGet(DB* db, const ReadOptions& options, const Slice& key,
                   std::string* value);
void DbStressMultiGet(DB* db, const ReadOptions& options,
                      ColumnFamilyHandle* column_family, size_t num_keys,
                      const Slice* keys, PinnableSlice* values,
                      Status* statuses);

StressTest* CreateCfConsistencyStressTest(int db_index,
                                          const std::string& db_path,
                                          const std::string& ev_path,
                                          const std::string& sec_path);
StressTest* CreateBatchedOpsStressTest(int db_index, const std::string& db_path,
                                       const std::string& ev_path,
                                       const std::string& sec_path);
StressTest* CreateNonBatchedOpsStressTest(int db_index,
                                          const std::string& db_path,
                                          const std::string& ev_path,
                                          const std::string& sec_path);
StressTest* CreateMultiOpsTxnsStressTest(int db_index,
                                         const std::string& db_path,
                                         const std::string& ev_path,
                                         const std::string& sec_path);
void CheckAndSetOptionsForMultiOpsTxnStressTest();
void InitializeHotKeyGenerator(double alpha);
int64_t GetOneHotKeyID(double rand_seed, int64_t max_key);

std::string GetNowNanos();

std::string GetReadTimestamp();

uint64_t GetWriteUnixTime(ThreadState* thread);

std::shared_ptr<FileChecksumGenFactory> GetFileChecksumImpl(
    const std::string& name);

Status DeleteFilesInDirectory(const std::string& dirname);
Status SaveFilesInDirectory(const std::string& src_dirname,
                            const std::string& dst_dirname);
Status DestroyUnverifiedSubdir(const std::string& dirname);
Status InitUnverifiedSubdir(const std::string& dirname);

// Destroy the DB at the given path under the env configured for db_stress.
// Handles both regular DB and BlobDB, and cleans and removes the entire dir.
Status DbStressDestroyDb(const std::string& db_path);

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
