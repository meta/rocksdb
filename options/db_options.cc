// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "options/db_options.h"

#include <cinttypes>

#include "logging/logging.h"
#include "options/configurable_helper.h"
#include "options/options_helper.h"
#include "options/options_parser.h"
#include "port/port.h"
#include "rocksdb/advanced_cache.h"
#include "rocksdb/configurable.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/listener.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/statistics.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/utilities/options_type.h"
#include "rocksdb/wal_filter.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
static std::unordered_map<std::string, WALRecoveryMode>
    wal_recovery_mode_string_map = {
        {"kTolerateCorruptedTailRecords",
         WALRecoveryMode::kTolerateCorruptedTailRecords},
        {"kAbsoluteConsistency", WALRecoveryMode::kAbsoluteConsistency},
        {"kPointInTimeRecovery", WALRecoveryMode::kPointInTimeRecovery},
        {"kSkipAnyCorruptedRecords",
         WALRecoveryMode::kSkipAnyCorruptedRecords}};

static std::unordered_map<std::string, CacheTier> cache_tier_string_map = {
    {"kVolatileTier", CacheTier::kVolatileTier},
    {"kVolatileCompressedTier", CacheTier::kVolatileCompressedTier},
    {"kNonVolatileBlockTier", CacheTier::kNonVolatileBlockTier}};

static std::unordered_map<std::string, InfoLogLevel> info_log_level_string_map =
    {{"DEBUG_LEVEL", InfoLogLevel::DEBUG_LEVEL},
     {"INFO_LEVEL", InfoLogLevel::INFO_LEVEL},
     {"WARN_LEVEL", InfoLogLevel::WARN_LEVEL},
     {"ERROR_LEVEL", InfoLogLevel::ERROR_LEVEL},
     {"FATAL_LEVEL", InfoLogLevel::FATAL_LEVEL},
     {"HEADER_LEVEL", InfoLogLevel::HEADER_LEVEL}};

static std::unordered_map<std::string, OptionTypeInfo>
    db_mutable_options_type_info = {
        {"allow_os_buffer",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"base_background_compactions",
         {0, OptionType::kInt, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"max_background_jobs",
         {offsetof(struct MutableDBOptions, max_background_jobs),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"max_background_compactions",
         {offsetof(struct MutableDBOptions, max_background_compactions),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"max_subcompactions",
         {offsetof(struct MutableDBOptions, max_subcompactions),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"avoid_flush_during_shutdown",
         {offsetof(struct MutableDBOptions, avoid_flush_during_shutdown),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"writable_file_max_buffer_size",
         {offsetof(struct MutableDBOptions, writable_file_max_buffer_size),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"delayed_write_rate",
         {offsetof(struct MutableDBOptions, delayed_write_rate),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"max_total_wal_size",
         {offsetof(struct MutableDBOptions, max_total_wal_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"delete_obsolete_files_period_micros",
         {offsetof(struct MutableDBOptions,
                   delete_obsolete_files_period_micros),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"stats_dump_period_sec",
         {offsetof(struct MutableDBOptions, stats_dump_period_sec),
          OptionType::kUInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"mutex_hold_log_threshold_us",
         {offsetof(struct MutableDBOptions, mutex_hold_log_threshold_us),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"stats_persist_period_sec",
         {offsetof(struct MutableDBOptions, stats_persist_period_sec),
          OptionType::kUInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"stats_history_buffer_size",
         {offsetof(struct MutableDBOptions, stats_history_buffer_size),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"max_open_files",
         {offsetof(struct MutableDBOptions, max_open_files), OptionType::kInt,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
        {"bytes_per_sync",
         {offsetof(struct MutableDBOptions, bytes_per_sync),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"wal_bytes_per_sync",
         {offsetof(struct MutableDBOptions, wal_bytes_per_sync),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"strict_bytes_per_sync",
         {offsetof(struct MutableDBOptions, strict_bytes_per_sync),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"compaction_readahead_size",
         {offsetof(struct MutableDBOptions, compaction_readahead_size),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"max_background_flushes",
         {offsetof(struct MutableDBOptions, max_background_flushes),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"max_manifest_file_size",
         {offsetof(struct MutableDBOptions, max_manifest_file_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"max_manifest_space_amp_pct",
         {offsetof(struct MutableDBOptions, max_manifest_space_amp_pct),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"manifest_preallocation_size",
         {offsetof(struct MutableDBOptions, manifest_preallocation_size),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"verify_manifest_content_on_close",
         {offsetof(struct MutableDBOptions, verify_manifest_content_on_close),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"optimize_manifest_for_recovery",
         {offsetof(struct MutableDBOptions, optimize_manifest_for_recovery),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"daily_offpeak_time_utc",
         {offsetof(struct MutableDBOptions, daily_offpeak_time_utc),
          OptionType::kString, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"max_compaction_trigger_wakeup_seconds",
         {offsetof(struct MutableDBOptions,
                   max_compaction_trigger_wakeup_seconds),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"periodic_compaction_phase_recovery_percent",
         {offsetof(struct MutableDBOptions,
                   periodic_compaction_phase_recovery_percent),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"fast_sst_open",
         {offsetof(struct MutableDBOptions, fast_sst_open),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"remote_compaction_manifest_floor",
         {offsetof(struct MutableDBOptions, remote_compaction_manifest_floor),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"use_separate_remote_compaction_pool",
         {offsetof(struct MutableDBOptions,
                   use_separate_remote_compaction_pool),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable | OptionTypeFlags::kDontSerialize |
              OptionTypeFlags::kCompareNever}},
};

static std::unordered_map<std::string, OptionTypeInfo>
    db_immutable_options_type_info = {
        /*
         // not yet supported
          std::shared_ptr<Cache> row_cache;
          std::shared_ptr<DeleteScheduler> delete_scheduler;
          std::shared_ptr<Logger> info_log;
          std::shared_ptr<RateLimiter> rate_limiter;
          std::shared_ptr<Statistics> statistics;
          std::vector<DbPath> db_paths;
          FileTypeSet checksum_handoff_file_types;
          CompactionStyleSet calculate_sst_write_lifetime_hint_set;
         */
        {"advise_random_on_open",
         {offsetof(struct ImmutableDBOptions, advise_random_on_open),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"allow_mmap_reads",
         {offsetof(struct ImmutableDBOptions, allow_mmap_reads),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"allow_fallocate",
         {offsetof(struct ImmutableDBOptions, allow_fallocate),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"allow_mmap_writes",
         {offsetof(struct ImmutableDBOptions, allow_mmap_writes),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"use_direct_reads",
         {offsetof(struct ImmutableDBOptions, use_direct_reads),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"use_direct_io_for_compaction_reads",
         {offsetof(struct ImmutableDBOptions,
                   use_direct_io_for_compaction_reads),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"use_direct_writes",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"use_direct_io_for_flush_and_compaction",
         {offsetof(struct ImmutableDBOptions,
                   use_direct_io_for_flush_and_compaction),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"allow_2pc",
         {offsetof(struct ImmutableDBOptions, allow_2pc), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"wal_filter",
         OptionTypeInfo::AsCustomRawPtr<WalFilter>(
             offsetof(struct ImmutableDBOptions, wal_filter),
             OptionVerificationType::kByName,
             (OptionTypeFlags::kAllowNull | OptionTypeFlags::kCompareNever))},
        {"create_if_missing",
         {offsetof(struct ImmutableDBOptions, create_if_missing),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"create_missing_column_families",
         {offsetof(struct ImmutableDBOptions, create_missing_column_families),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"disableDataSync",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"disable_data_sync",  // for compatibility
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"enable_thread_tracking",
         {offsetof(struct ImmutableDBOptions, enable_thread_tracking),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"error_if_exists",
         {offsetof(struct ImmutableDBOptions, error_if_exists),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"experimental_allow_mempurge",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"experimental_mempurge_policy",
         {0, OptionType::kString, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"experimental_mempurge_threshold",
         {0, OptionType::kDouble, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"is_fd_close_on_exec",
         {offsetof(struct ImmutableDBOptions, is_fd_close_on_exec),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"paranoid_checks",
         {offsetof(struct ImmutableDBOptions, paranoid_checks),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"open_files_async",
         {offsetof(struct ImmutableDBOptions, open_files_async),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"flush_verify_memtable_count",
         {offsetof(struct ImmutableDBOptions, flush_verify_memtable_count),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"compaction_verify_record_count",
         {offsetof(struct ImmutableDBOptions, compaction_verify_record_count),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"track_and_verify_wals_in_manifest",
         {offsetof(struct ImmutableDBOptions,
                   track_and_verify_wals_in_manifest),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"track_and_verify_wals",
         {offsetof(struct ImmutableDBOptions, track_and_verify_wals),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"verify_sst_unique_id_in_manifest",
         {offsetof(struct ImmutableDBOptions, verify_sst_unique_id_in_manifest),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"skip_log_error_on_recovery",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"skip_stats_update_on_db_open",
         {offsetof(struct ImmutableDBOptions, skip_stats_update_on_db_open),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"skip_checking_sst_file_sizes_on_db_open",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"new_table_reader_for_compaction_inputs",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"random_access_max_buffer_size",
         {0, OptionType::kSizeT, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"use_adaptive_mutex",
         {offsetof(struct ImmutableDBOptions, use_adaptive_mutex),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"use_fsync",
         {offsetof(struct ImmutableDBOptions, use_fsync), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"max_file_opening_threads",
         {offsetof(struct ImmutableDBOptions, max_file_opening_threads),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"read_io_executor_threads",
         {offsetof(struct ImmutableDBOptions, read_io_executor_threads),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"table_cache_numshardbits",
         {offsetof(struct ImmutableDBOptions, table_cache_numshardbits),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"db_write_buffer_size",
         {offsetof(struct ImmutableDBOptions, db_write_buffer_size),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"keep_log_file_num",
         {offsetof(struct ImmutableDBOptions, keep_log_file_num),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"recycle_log_file_num",
         {offsetof(struct ImmutableDBOptions, recycle_log_file_num),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"async_wal_precreate",
         {offsetof(struct ImmutableDBOptions, async_wal_precreate),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"log_file_time_to_roll",
         {offsetof(struct ImmutableDBOptions, log_file_time_to_roll),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"max_log_file_size",
         {offsetof(struct ImmutableDBOptions, max_log_file_size),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"db_log_dir",
         {offsetof(struct ImmutableDBOptions, db_log_dir), OptionType::kString,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"wal_dir",
         {offsetof(struct ImmutableDBOptions, wal_dir), OptionType::kString,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"WAL_size_limit_MB",
         {offsetof(struct ImmutableDBOptions, WAL_size_limit_MB),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"WAL_ttl_seconds",
         {offsetof(struct ImmutableDBOptions, WAL_ttl_seconds),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"persist_stats_to_disk",
         {offsetof(struct ImmutableDBOptions, persist_stats_to_disk),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"fail_if_options_file_error",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"enable_pipelined_write",
         {offsetof(struct ImmutableDBOptions, enable_pipelined_write),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"unordered_write",
         {offsetof(struct ImmutableDBOptions, unordered_write),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"allow_concurrent_memtable_write",
         {offsetof(struct ImmutableDBOptions, allow_concurrent_memtable_write),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"wal_recovery_mode",
         OptionTypeInfo::Enum<WALRecoveryMode>(
             offsetof(struct ImmutableDBOptions, wal_recovery_mode),
             &wal_recovery_mode_string_map)},
        {"enable_write_thread_adaptive_yield",
         {offsetof(struct ImmutableDBOptions,
                   enable_write_thread_adaptive_yield),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"write_thread_slow_yield_usec",
         {offsetof(struct ImmutableDBOptions, write_thread_slow_yield_usec),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"max_write_batch_group_size_bytes",
         {offsetof(struct ImmutableDBOptions, max_write_batch_group_size_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"write_thread_max_yield_usec",
         {offsetof(struct ImmutableDBOptions, write_thread_max_yield_usec),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"access_hint_on_compaction_start",
         OptionTypeInfo::Enum<bool>(0, nullptr, OptionTypeFlags::kNone,
                                    OptionVerificationType::kDeprecated)},
        {"info_log_level",
         OptionTypeInfo::Enum<InfoLogLevel>(
             offsetof(struct ImmutableDBOptions, info_log_level),
             &info_log_level_string_map)},
        {"dump_malloc_stats",
         {offsetof(struct ImmutableDBOptions, dump_malloc_stats),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"avoid_flush_during_recovery",
         {offsetof(struct ImmutableDBOptions, avoid_flush_during_recovery),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"enforce_write_buffer_manager_during_recovery",
         {offsetof(struct ImmutableDBOptions,
                   enforce_write_buffer_manager_during_recovery),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"allow_ingest_behind",
         {offsetof(struct ImmutableDBOptions, allow_ingest_behind),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"preserve_deletes",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"concurrent_prepare",  // Deprecated by two_write_queues
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"two_write_queues",
         {offsetof(struct ImmutableDBOptions, two_write_queues),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"manual_wal_flush",
         {offsetof(struct ImmutableDBOptions, manual_wal_flush),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"wal_compression",
         {offsetof(struct ImmutableDBOptions, wal_compression),
          OptionType::kCompressionType, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"background_close_inactive_wals",
         {offsetof(struct ImmutableDBOptions, background_close_inactive_wals),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"seq_per_batch",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"atomic_flush",
         {offsetof(struct ImmutableDBOptions, atomic_flush),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"avoid_unnecessary_blocking_io",
         {offsetof(struct ImmutableDBOptions, avoid_unnecessary_blocking_io),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"prefix_seek_opt_in_only",
         {offsetof(struct ImmutableDBOptions, prefix_seek_opt_in_only),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"write_dbid_to_manifest",
         {offsetof(struct ImmutableDBOptions, write_dbid_to_manifest),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"track_options_file_number_in_manifest",
         {offsetof(struct ImmutableDBOptions,
                   track_options_file_number_in_manifest),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"reuse_manifest_on_open",
         {offsetof(struct ImmutableDBOptions, reuse_manifest_on_open),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"write_identity_file",
         {offsetof(struct ImmutableDBOptions, write_identity_file),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"log_readahead_size",
         {offsetof(struct ImmutableDBOptions, log_readahead_size),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"best_efforts_recovery",
         {offsetof(struct ImmutableDBOptions, best_efforts_recovery),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"max_bgerror_resume_count",
         {offsetof(struct ImmutableDBOptions, max_bgerror_resume_count),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"bgerror_resume_retry_interval",
         {offsetof(struct ImmutableDBOptions, bgerror_resume_retry_interval),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"db_host_id",
         {offsetof(struct ImmutableDBOptions, db_host_id), OptionType::kString,
          OptionVerificationType::kNormal, OptionTypeFlags::kCompareNever}},
        // Temporarily deprecated due to race conditions (examples in PR 10375).
        {"rate_limiter",
         {offsetof(struct ImmutableDBOptions, rate_limiter),
          OptionType::kUnknown, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kDontSerialize | OptionTypeFlags::kCompareNever}},
        // The following properties were handled as special cases in ParseOption
        // This means that the properties could be read from the options file
        // but never written to the file or compared to each other.
        {"rate_limiter_bytes_per_sec",
         {offsetof(struct ImmutableDBOptions, rate_limiter),
          OptionType::kUnknown, OptionVerificationType::kNormal,
          (OptionTypeFlags::kDontSerialize | OptionTypeFlags::kCompareNever),
          // Parse the input value as a RateLimiter
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const std::string& value, void* addr) {
            auto limiter = static_cast<std::shared_ptr<RateLimiter>*>(addr);
            limiter->reset(NewGenericRateLimiter(
                static_cast<int64_t>(ParseUint64(value))));
            return Status::OK();
          }}},
        {"env",  //**TODO: Should this be kCustomizable?
         OptionTypeInfo(
             offsetof(struct ImmutableDBOptions, env), OptionType::kUnknown,
             OptionVerificationType::kNormal,
             (OptionTypeFlags::kDontSerialize | OptionTypeFlags::kCompareNever))
             .SetParseFunc([](const ConfigOptions& opts,
                              const std::string& /*name*/,
                              const std::string& value, void* addr) {
               // Parse the input value as an Env
               auto old_env = static_cast<Env**>(addr);  // Get the old value
               Env* new_env = *old_env;                  // Set new to old
               Status s = Env::CreateFromString(opts, value,
                                                &new_env);  // Update new value
               if (s.ok()) {                                // It worked
                 *old_env = new_env;  // Update the old one
               }
               return s;
             })
             .SetPrepareFunc([](const ConfigOptions& opts,
                                const std::string& /*name*/, void* addr) {
               auto env = static_cast<Env**>(addr);
               return (*env)->PrepareOptions(opts);
             })
             .SetValidateFunc([](const DBOptions& db_opts,
                                 const ColumnFamilyOptions& cf_opts,
                                 const std::string& /*name*/,
                                 const void* addr) {
               const auto env = static_cast<const Env* const*>(addr);
               return (*env)->ValidateOptions(db_opts, cf_opts);
             })},
        {"allow_data_in_errors",
         {offsetof(struct ImmutableDBOptions, allow_data_in_errors),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_checksum_gen_factory",
         OptionTypeInfo::AsCustomSharedPtr<FileChecksumGenFactory>(
             offsetof(struct ImmutableDBOptions, file_checksum_gen_factory),
             OptionVerificationType::kByNameAllowFromNull,
             OptionTypeFlags::kAllowNull)},
        {"statistics",
         OptionTypeInfo::AsCustomSharedPtr<Statistics>(
             // Statistics should not be compared and can be null
             // Statistics are maked "don't serialize" until they can be shared
             // between DBs
             offsetof(struct ImmutableDBOptions, statistics),
             OptionVerificationType::kNormal,
             OptionTypeFlags::kCompareNever | OptionTypeFlags::kDontSerialize |
                 OptionTypeFlags::kAllowNull)},
        // Allow EventListeners that have a non-empty Name() to be read/written
        // as options Each listener will either be
        // - A simple name (e.g. "MyEventListener")
        // - A name with properties (e.g. "{id=MyListener1; timeout=60}"
        // Multiple listeners will be separated by a ":":
        //   - "MyListener0;{id=MyListener1; timeout=60}
        {"listeners",
         {offsetof(struct ImmutableDBOptions, listeners), OptionType::kVector,
          OptionVerificationType::kByNameAllowNull,
          OptionTypeFlags::kCompareNever,
          [](const ConfigOptions& opts, const std::string& /*name*/,
             const std::string& value, void* addr) {
            ConfigOptions embedded = opts;
            embedded.ignore_unsupported_options = true;
            std::vector<std::shared_ptr<EventListener>> listeners;
            Status s;
            // Permit `{item1:item2}` or bare `item1:item2`.
            std::string stripped = OptionTypeInfo::StripOuterBraces(value);
            for (size_t start = 0, end = 0;
                 s.ok() && start < stripped.size() && end != std::string::npos;
                 start = end + 1) {
              std::string token;
              s = OptionTypeInfo::NextToken(stripped, ':', start, &end, &token);
              if (s.ok() && !token.empty()) {
                std::shared_ptr<EventListener> listener;
                s = EventListener::CreateFromString(embedded, token, &listener);
                if (s.ok() && listener != nullptr) {
                  listeners.push_back(listener);
                }
              }
            }
            if (s.ok()) {  // It worked
              *(static_cast<std::vector<std::shared_ptr<EventListener>>*>(
                  addr)) = listeners;
            }
            return s;
          },
          [](const ConfigOptions& opts, const std::string& /*name*/,
             const void* addr, std::string* value) {
            const auto listeners =
                static_cast<const std::vector<std::shared_ptr<EventListener>>*>(
                    addr);
            ConfigOptions embedded = opts;
            embedded.delimiter = ";";
            int printed = 0;
            for (const auto& listener : *listeners) {
              auto id = listener->GetId();
              if (!id.empty()) {
                std::string elem_str = listener->ToString(embedded, "");
                if (printed++ == 0) {
                  value->append("{");
                } else {
                  value->append(":");
                }
                value->append(elem_str);
              }
            }
            if (printed > 0) {
              value->append("}");
            }
            return Status::OK();
          },
          nullptr}},
        {"lowest_used_cache_tier",
         OptionTypeInfo::Enum<CacheTier>(
             offsetof(struct ImmutableDBOptions, lowest_used_cache_tier),
             &cache_tier_string_map, OptionTypeFlags::kNone)},
        {"use_session_tmp_dir_for_remote_compaction",
         {offsetof(struct ImmutableDBOptions,
                   use_session_tmp_dir_for_remote_compaction),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"enforce_single_del_contracts",
         {offsetof(struct ImmutableDBOptions, enforce_single_del_contracts),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"follower_refresh_catchup_period_ms",
         {offsetof(struct ImmutableDBOptions,
                   follower_refresh_catchup_period_ms),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"follower_catchup_retry_count",
         {offsetof(struct ImmutableDBOptions, follower_catchup_retry_count),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"follower_catchup_retry_wait_ms",
         {offsetof(struct ImmutableDBOptions, follower_catchup_retry_wait_ms),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"metadata_write_temperature",
         {offsetof(struct ImmutableDBOptions, metadata_write_temperature),
          OptionType::kTemperature, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"wal_write_temperature",
         {offsetof(struct ImmutableDBOptions, wal_write_temperature),
          OptionType::kTemperature, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
};

const std::string OptionsHelper::kDBOptionsName = "DBOptions";

class MutableDBConfigurable : public Configurable {
 public:
  explicit MutableDBConfigurable(
      const MutableDBOptions& mdb,
      const std::unordered_map<std::string, std::string>* map = nullptr)
      : mutable_(mdb), opt_map_(map) {
    RegisterOptions(&mutable_, &db_mutable_options_type_info);
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

 protected:
  MutableDBOptions mutable_;
  const std::unordered_map<std::string, std::string>* opt_map_;
};

class DBOptionsConfigurable : public MutableDBConfigurable {
 public:
  explicit DBOptionsConfigurable(
      const DBOptions& opts,
      const std::unordered_map<std::string, std::string>* map = nullptr)
      : MutableDBConfigurable(MutableDBOptions{opts}, map), db_options_(opts) {
    // The ImmutableDBOptions currently requires the env to be non-null.  Make
    // sure it is
    if (opts.env != nullptr) {
      immutable_ = ImmutableDBOptions(opts);
    } else {
      DBOptions copy = opts;
      copy.env = Env::Default();
      immutable_ = ImmutableDBOptions(copy);
    }
    RegisterOptions(&immutable_, &db_immutable_options_type_info);
  }

 protected:
  Status ConfigureOptions(
      const ConfigOptions& config_options,
      const std::unordered_map<std::string, std::string>& opts_map,
      std::unordered_map<std::string, std::string>* unused) override {
    Status s = Configurable::ConfigureOptions(config_options, opts_map, unused);
    if (s.ok()) {
      db_options_ = BuildDBOptions(immutable_, mutable_);
      s = PrepareOptions(config_options);
    }
    return s;
  }

  const void* GetOptionsPtr(const std::string& name) const override {
    if (name == OptionsHelper::kDBOptionsName) {
      return &db_options_;
    } else {
      return MutableDBConfigurable::GetOptionsPtr(name);
    }
  }

 private:
  ImmutableDBOptions immutable_;
  DBOptions db_options_;
};

std::unique_ptr<Configurable> DBOptionsAsConfigurable(
    const MutableDBOptions& opts) {
  std::unique_ptr<Configurable> ptr(new MutableDBConfigurable(opts));
  return ptr;
}
std::unique_ptr<Configurable> DBOptionsAsConfigurable(
    const DBOptions& opts,
    const std::unordered_map<std::string, std::string>* opt_map) {
  std::unique_ptr<Configurable> ptr(new DBOptionsConfigurable(opts, opt_map));
  return ptr;
}

ImmutableDBOptions::ImmutableDBOptions() : ImmutableDBOptions(DBOptions{}) {}

ImmutableDBOptions::ImmutableDBOptions(const DBOptions& options)
    : create_if_missing(options.create_if_missing),
      create_missing_column_families(options.create_missing_column_families),
      error_if_exists(options.error_if_exists),
      paranoid_checks(options.paranoid_checks),
      open_files_async(options.open_files_async),
      flush_verify_memtable_count(options.flush_verify_memtable_count),
      compaction_verify_record_count(options.compaction_verify_record_count),
      track_and_verify_wals_in_manifest(
          options.track_and_verify_wals_in_manifest),
      track_and_verify_wals(options.track_and_verify_wals),
      verify_sst_unique_id_in_manifest(
          options.verify_sst_unique_id_in_manifest),
      env(options.env),
      rate_limiter(options.rate_limiter),
      sst_file_manager(options.sst_file_manager),
      info_log(options.info_log),
      info_log_level(options.info_log_level),
      max_file_opening_threads(options.max_file_opening_threads),
      read_io_executor_threads(options.read_io_executor_threads),
      statistics(options.statistics),
      use_fsync(options.use_fsync),
      db_paths(options.db_paths),
      db_log_dir(options.db_log_dir),
      wal_dir(options.wal_dir),
      max_log_file_size(options.max_log_file_size),
      log_file_time_to_roll(options.log_file_time_to_roll),
      keep_log_file_num(options.keep_log_file_num),
      recycle_log_file_num(options.recycle_log_file_num),
      async_wal_precreate(options.async_wal_precreate),
      table_cache_numshardbits(options.table_cache_numshardbits),
      WAL_ttl_seconds(options.WAL_ttl_seconds),
      WAL_size_limit_MB(options.WAL_size_limit_MB),
      max_write_batch_group_size_bytes(
          options.max_write_batch_group_size_bytes),
      allow_mmap_reads(options.allow_mmap_reads),
      allow_mmap_writes(options.allow_mmap_writes),
      use_direct_reads(options.use_direct_reads),
      use_direct_io_for_compaction_reads(
          options.use_direct_io_for_compaction_reads),
      use_direct_io_for_flush_and_compaction(
          options.use_direct_io_for_flush_and_compaction),
      allow_fallocate(options.allow_fallocate),
      is_fd_close_on_exec(options.is_fd_close_on_exec),
      advise_random_on_open(options.advise_random_on_open),
      db_write_buffer_size(options.db_write_buffer_size),
      write_buffer_manager(options.write_buffer_manager),
      use_adaptive_mutex(options.use_adaptive_mutex),
      listeners(options.listeners),
      enable_thread_tracking(options.enable_thread_tracking),
      enable_pipelined_write(options.enable_pipelined_write),
      unordered_write(options.unordered_write),
      allow_concurrent_memtable_write(options.allow_concurrent_memtable_write),
      enable_write_thread_adaptive_yield(
          options.enable_write_thread_adaptive_yield),
      write_thread_max_yield_usec(options.write_thread_max_yield_usec),
      write_thread_slow_yield_usec(options.write_thread_slow_yield_usec),
      skip_stats_update_on_db_open(options.skip_stats_update_on_db_open),
      wal_recovery_mode(options.wal_recovery_mode),
      allow_2pc(options.allow_2pc),
      row_cache(options.row_cache),
      wal_filter(options.wal_filter),
      dump_malloc_stats(options.dump_malloc_stats),
      avoid_flush_during_recovery(options.avoid_flush_during_recovery),
      enforce_write_buffer_manager_during_recovery(
          options.enforce_write_buffer_manager_during_recovery),
      allow_ingest_behind(options.allow_ingest_behind),
      two_write_queues(options.two_write_queues),
      manual_wal_flush(options.manual_wal_flush),
      wal_compression(options.wal_compression),
      background_close_inactive_wals(options.background_close_inactive_wals),
      atomic_flush(options.atomic_flush),
      avoid_unnecessary_blocking_io(options.avoid_unnecessary_blocking_io),
      prefix_seek_opt_in_only(options.prefix_seek_opt_in_only),
      persist_stats_to_disk(options.persist_stats_to_disk),
      write_dbid_to_manifest(options.write_dbid_to_manifest),
      track_options_file_number_in_manifest(
          options.track_options_file_number_in_manifest),
      reuse_manifest_on_open(options.reuse_manifest_on_open),
      write_identity_file(options.write_identity_file),
      log_readahead_size(options.log_readahead_size),
      file_checksum_gen_factory(options.file_checksum_gen_factory),
      best_efforts_recovery(options.best_efforts_recovery),
      max_bgerror_resume_count(options.max_bgerror_resume_count),
      bgerror_resume_retry_interval(options.bgerror_resume_retry_interval),
      allow_data_in_errors(options.allow_data_in_errors),
      db_host_id(options.db_host_id),
      checksum_handoff_file_types(options.checksum_handoff_file_types),
      lowest_used_cache_tier(options.lowest_used_cache_tier),
      compaction_service(options.compaction_service),
      use_session_tmp_dir_for_remote_compaction(
          options.use_session_tmp_dir_for_remote_compaction),
      enforce_single_del_contracts(options.enforce_single_del_contracts),
      follower_refresh_catchup_period_ms(
          options.follower_refresh_catchup_period_ms),
      follower_catchup_retry_count(options.follower_catchup_retry_count),
      follower_catchup_retry_wait_ms(options.follower_catchup_retry_wait_ms),
      metadata_write_temperature(options.metadata_write_temperature),
      wal_write_temperature(options.wal_write_temperature),
      calculate_sst_write_lifetime_hint_set(
          options.calculate_sst_write_lifetime_hint_set) {
  fs = env->GetFileSystem();
  clock = env->GetSystemClock().get();
  logger = info_log.get();
  stats = statistics.get();
}

void ImmutableDBOptions::Dump(Logger* log) const {
  ROCKS_LOG_HEADER(log, "                        Options.error_if_exists: %d",
                   error_if_exists);
  ROCKS_LOG_HEADER(log, "                      Options.create_if_missing: %d",
                   create_if_missing);
  ROCKS_LOG_HEADER(log, "                        Options.paranoid_checks: %d",
                   paranoid_checks);
  ROCKS_LOG_HEADER(log, "                       Options.open_files_async: %d",
                   open_files_async);
  ROCKS_LOG_HEADER(log, "            Options.flush_verify_memtable_count: %d",
                   flush_verify_memtable_count);
  ROCKS_LOG_HEADER(log, "         Options.compaction_verify_record_count: %d",
                   compaction_verify_record_count);
  ROCKS_LOG_HEADER(log,
                   "                              "
                   "Options.track_and_verify_wals_in_manifest: %d",
                   track_and_verify_wals_in_manifest);
  ROCKS_LOG_HEADER(log,
                   "                              "
                   "Options.track_and_verify_wals: %d",
                   track_and_verify_wals);
  ROCKS_LOG_HEADER(log, "       Options.verify_sst_unique_id_in_manifest: %d",
                   verify_sst_unique_id_in_manifest);
  ROCKS_LOG_HEADER(log, "                                    Options.env: %p",
                   env);
  ROCKS_LOG_HEADER(log, "                                     Options.fs: %s",
                   fs->Name());
  ROCKS_LOG_HEADER(log, "                               Options.info_log: %p",
                   info_log.get());
  ROCKS_LOG_HEADER(log, "               Options.max_file_opening_threads: %d",
                   max_file_opening_threads);
  ROCKS_LOG_HEADER(log, "               Options.read_io_executor_threads: %d",
                   read_io_executor_threads);
  ROCKS_LOG_HEADER(log, "                             Options.statistics: %p",
                   stats);
  if (stats) {
    ROCKS_LOG_HEADER(
        log, "                             Options.statistics stats level: %u",
        stats->get_stats_level());
  }
  ROCKS_LOG_HEADER(log, "                              Options.use_fsync: %d",
                   use_fsync);
  ROCKS_LOG_HEADER(
      log, "                      Options.max_log_file_size: %" ROCKSDB_PRIszt,
      max_log_file_size);
  ROCKS_LOG_HEADER(
      log, "                  Options.log_file_time_to_roll: %" ROCKSDB_PRIszt,
      log_file_time_to_roll);
  ROCKS_LOG_HEADER(
      log, "                      Options.keep_log_file_num: %" ROCKSDB_PRIszt,
      keep_log_file_num);
  ROCKS_LOG_HEADER(
      log, "                   Options.recycle_log_file_num: %" ROCKSDB_PRIszt,
      recycle_log_file_num);
  ROCKS_LOG_HEADER(log, "                    Options.async_wal_precreate: %d",
                   async_wal_precreate);
  ROCKS_LOG_HEADER(log, "                        Options.allow_fallocate: %d",
                   allow_fallocate);
  ROCKS_LOG_HEADER(log, "                       Options.allow_mmap_reads: %d",
                   allow_mmap_reads);
  ROCKS_LOG_HEADER(log, "                      Options.allow_mmap_writes: %d",
                   allow_mmap_writes);
  ROCKS_LOG_HEADER(log, "                       Options.use_direct_reads: %d",
                   use_direct_reads);
  ROCKS_LOG_HEADER(log,
                   "            Options.use_direct_io_for_compaction_reads: %d",
                   use_direct_io_for_compaction_reads);
  ROCKS_LOG_HEADER(log,
                   "        Options.use_direct_io_for_flush_and_compaction: %d",
                   use_direct_io_for_flush_and_compaction);
  ROCKS_LOG_HEADER(log, "         Options.create_missing_column_families: %d",
                   create_missing_column_families);
  ROCKS_LOG_HEADER(log, "                             Options.db_log_dir: %s",
                   db_log_dir.c_str());
  ROCKS_LOG_HEADER(log, "                                Options.wal_dir: %s",
                   wal_dir.c_str());
  ROCKS_LOG_HEADER(log, "               Options.table_cache_numshardbits: %d",
                   table_cache_numshardbits);
  ROCKS_LOG_HEADER(log,
                   "                        Options.WAL_ttl_seconds: %" PRIu64,
                   WAL_ttl_seconds);
  ROCKS_LOG_HEADER(log,
                   "                      Options.WAL_size_limit_MB: %" PRIu64,
                   WAL_size_limit_MB);
  ROCKS_LOG_HEADER(log,
                   "                       "
                   "Options.max_write_batch_group_size_bytes: %" PRIu64,
                   max_write_batch_group_size_bytes);
  ROCKS_LOG_HEADER(log, "                    Options.is_fd_close_on_exec: %d",
                   is_fd_close_on_exec);
  ROCKS_LOG_HEADER(log, "                  Options.advise_random_on_open: %d",
                   advise_random_on_open);
  ROCKS_LOG_HEADER(
      log, "                   Options.db_write_buffer_size: %" ROCKSDB_PRIszt,
      db_write_buffer_size);
  ROCKS_LOG_HEADER(log, "                   Options.write_buffer_manager: %p",
                   write_buffer_manager.get());
  ROCKS_LOG_HEADER(log, "                     Options.use_adaptive_mutex: %d",
                   use_adaptive_mutex);
  ROCKS_LOG_HEADER(log, "                           Options.rate_limiter: %p",
                   rate_limiter.get());
  Header(
      log, "    Options.sst_file_manager.rate_bytes_per_sec: %" PRIi64,
      sst_file_manager ? sst_file_manager->GetDeleteRateBytesPerSecond() : 0);
  ROCKS_LOG_HEADER(log, "                      Options.wal_recovery_mode: %d",
                   static_cast<int>(wal_recovery_mode));
  ROCKS_LOG_HEADER(log, "                 Options.enable_thread_tracking: %d",
                   enable_thread_tracking);
  ROCKS_LOG_HEADER(log, "                 Options.enable_pipelined_write: %d",
                   enable_pipelined_write);
  ROCKS_LOG_HEADER(log, "                 Options.unordered_write: %d",
                   unordered_write);
  ROCKS_LOG_HEADER(log, "        Options.allow_concurrent_memtable_write: %d",
                   allow_concurrent_memtable_write);
  ROCKS_LOG_HEADER(log, "     Options.enable_write_thread_adaptive_yield: %d",
                   enable_write_thread_adaptive_yield);
  ROCKS_LOG_HEADER(log,
                   "            Options.write_thread_max_yield_usec: %" PRIu64,
                   write_thread_max_yield_usec);
  ROCKS_LOG_HEADER(log,
                   "           Options.write_thread_slow_yield_usec: %" PRIu64,
                   write_thread_slow_yield_usec);
  if (row_cache) {
    ROCKS_LOG_HEADER(
        log,
        "                              Options.row_cache: %" ROCKSDB_PRIszt,
        row_cache->GetCapacity());
  } else {
    ROCKS_LOG_HEADER(log,
                     "                              Options.row_cache: None");
  }
  ROCKS_LOG_HEADER(log, "                             Options.wal_filter: %s",
                   wal_filter ? wal_filter->Name() : "None");

  ROCKS_LOG_HEADER(log, "            Options.avoid_flush_during_recovery: %d",
                   avoid_flush_during_recovery);
  ROCKS_LOG_HEADER(
      log, "        Options.enforce_write_buffer_manager_during_recovery: %d",
      enforce_write_buffer_manager_during_recovery);
  ROCKS_LOG_HEADER(log, "            Options.allow_ingest_behind: %d",
                   allow_ingest_behind);
  ROCKS_LOG_HEADER(log, "            Options.two_write_queues: %d",
                   two_write_queues);
  ROCKS_LOG_HEADER(log, "            Options.manual_wal_flush: %d",
                   manual_wal_flush);
  ROCKS_LOG_HEADER(log, "            Options.wal_compression: %d",
                   wal_compression);
  ROCKS_LOG_HEADER(log,
                   "            Options.background_close_inactive_wals: %d",
                   background_close_inactive_wals);
  ROCKS_LOG_HEADER(log, "            Options.atomic_flush: %d", atomic_flush);
  ROCKS_LOG_HEADER(log, "            Options.avoid_unnecessary_blocking_io: %d",
                   avoid_unnecessary_blocking_io);
  ROCKS_LOG_HEADER(log, "            Options.prefix_seek_opt_in_only: %d",
                   prefix_seek_opt_in_only);
  ROCKS_LOG_HEADER(log, "                Options.persist_stats_to_disk: %u",
                   persist_stats_to_disk);
  ROCKS_LOG_HEADER(log, "                Options.write_dbid_to_manifest: %d",
                   write_dbid_to_manifest);
  ROCKS_LOG_HEADER(log, " Options.track_options_file_number_in_manifest: %d",
                   track_options_file_number_in_manifest);
  ROCKS_LOG_HEADER(log, "                Options.reuse_manifest_on_open: %d",
                   reuse_manifest_on_open);
  ROCKS_LOG_HEADER(log, "                Options.write_identity_file: %d",
                   write_identity_file);
  ROCKS_LOG_HEADER(
      log, "                Options.log_readahead_size: %" ROCKSDB_PRIszt,
      log_readahead_size);
  ROCKS_LOG_HEADER(log, "                Options.file_checksum_gen_factory: %s",
                   file_checksum_gen_factory ? file_checksum_gen_factory->Name()
                                             : kUnknownFileChecksumFuncName);
  ROCKS_LOG_HEADER(log, "                Options.best_efforts_recovery: %d",
                   static_cast<int>(best_efforts_recovery));
  ROCKS_LOG_HEADER(log, "               Options.max_bgerror_resume_count: %d",
                   max_bgerror_resume_count);
  ROCKS_LOG_HEADER(log,
                   "           Options.bgerror_resume_retry_interval: %" PRIu64,
                   bgerror_resume_retry_interval);
  ROCKS_LOG_HEADER(log, "            Options.allow_data_in_errors: %d",
                   allow_data_in_errors);
  ROCKS_LOG_HEADER(log, "            Options.db_host_id: %s",
                   db_host_id.c_str());
  ROCKS_LOG_HEADER(
      log, "            Options.use_session_tmp_dir_for_remote_compaction: %s",
      use_session_tmp_dir_for_remote_compaction ? "true" : "false");
  ROCKS_LOG_HEADER(log, "            Options.enforce_single_del_contracts: %s",
                   enforce_single_del_contracts ? "true" : "false");
  ROCKS_LOG_HEADER(log, "            Options.metadata_write_temperature: %s",
                   temperature_to_string[metadata_write_temperature].c_str());
  ROCKS_LOG_HEADER(log, "            Options.wal_write_temperature: %s",
                   temperature_to_string[wal_write_temperature].c_str());
}

bool ImmutableDBOptions::IsWalDirSameAsDBPath() const {
  assert(!db_paths.empty());
  return IsWalDirSameAsDBPath(db_paths[0].path);
}

bool ImmutableDBOptions::IsWalDirSameAsDBPath(
    const std::string& db_path) const {
  bool same = wal_dir.empty();
  if (!same) {
    Status s = env->AreFilesSame(wal_dir, db_path, &same);
    if (s.IsNotSupported()) {
      same = wal_dir == db_path;
    }
  }
  return same;
}

const std::string& ImmutableDBOptions::GetWalDir() const {
  if (wal_dir.empty()) {
    assert(!db_paths.empty());
    return db_paths[0].path;
  } else {
    return wal_dir;
  }
}

const std::string& ImmutableDBOptions::GetWalDir(
    const std::string& path) const {
  if (wal_dir.empty()) {
    return path;
  } else {
    return wal_dir;
  }
}

MutableDBOptions::MutableDBOptions() : MutableDBOptions(DBOptions{}) {}

MutableDBOptions::MutableDBOptions(const DBOptions& options)
    : writable_file_max_buffer_size(options.writable_file_max_buffer_size),
      delayed_write_rate(options.delayed_write_rate),
      max_total_wal_size(options.max_total_wal_size),
      delete_obsolete_files_period_micros(
          options.delete_obsolete_files_period_micros),
      mutex_hold_log_threshold_us(options.mutex_hold_log_threshold_us),
      stats_history_buffer_size(options.stats_history_buffer_size),
      bytes_per_sync(options.bytes_per_sync),
      wal_bytes_per_sync(options.wal_bytes_per_sync),
      compaction_readahead_size(options.compaction_readahead_size),
      max_manifest_file_size(options.max_manifest_file_size),
      manifest_preallocation_size(options.manifest_preallocation_size),
      max_compaction_trigger_wakeup_seconds(
          options.max_compaction_trigger_wakeup_seconds),
      daily_offpeak_time_utc(options.daily_offpeak_time_utc),
      max_background_jobs(options.max_background_jobs),
      max_background_compactions(options.max_background_compactions),
      max_subcompactions(options.max_subcompactions),
      stats_dump_period_sec(options.stats_dump_period_sec),
      stats_persist_period_sec(options.stats_persist_period_sec),
      max_open_files(options.max_open_files),
      max_background_flushes(options.max_background_flushes),
      max_manifest_space_amp_pct(options.max_manifest_space_amp_pct),
      periodic_compaction_phase_recovery_percent(
          options.periodic_compaction_phase_recovery_percent),
      avoid_flush_during_shutdown(options.avoid_flush_during_shutdown),
      strict_bytes_per_sync(options.strict_bytes_per_sync),
      verify_manifest_content_on_close(
          options.verify_manifest_content_on_close),
      optimize_manifest_for_recovery(options.optimize_manifest_for_recovery),
      fast_sst_open(options.fast_sst_open),
      remote_compaction_manifest_floor(
          options.remote_compaction_manifest_floor),
      use_separate_remote_compaction_pool(
          options.use_separate_remote_compaction_pool) {}

void MutableDBOptions::Dump(Logger* log) const {
  ROCKS_LOG_HEADER(log, "            Options.max_background_jobs: %d",
                   max_background_jobs);
  ROCKS_LOG_HEADER(log, "            Options.max_background_compactions: %d",
                   max_background_compactions);
  ROCKS_LOG_HEADER(log, "            Options.max_subcompactions: %" PRIu32,
                   max_subcompactions);
  ROCKS_LOG_HEADER(log, "            Options.avoid_flush_during_shutdown: %d",
                   avoid_flush_during_shutdown);
  ROCKS_LOG_HEADER(
      log, "          Options.writable_file_max_buffer_size: %" ROCKSDB_PRIszt,
      writable_file_max_buffer_size);
  ROCKS_LOG_HEADER(log, "            Options.delayed_write_rate : %" PRIu64,
                   delayed_write_rate);
  ROCKS_LOG_HEADER(log, "            Options.max_total_wal_size: %" PRIu64,
                   max_total_wal_size);
  ROCKS_LOG_HEADER(
      log, "            Options.delete_obsolete_files_period_micros: %" PRIu64,
      delete_obsolete_files_period_micros);
  ROCKS_LOG_HEADER(log, "                  Options.stats_dump_period_sec: %u",
                   stats_dump_period_sec);
  ROCKS_LOG_HEADER(log, "Options.mutex_hold_log_threshold_us: %" PRIu64,
                   mutex_hold_log_threshold_us);
  ROCKS_LOG_HEADER(log, "                Options.stats_persist_period_sec: %d",
                   stats_persist_period_sec);
  ROCKS_LOG_HEADER(
      log,
      "                Options.stats_history_buffer_size: %" ROCKSDB_PRIszt,
      stats_history_buffer_size);
  ROCKS_LOG_HEADER(log, "                         Options.max_open_files: %d",
                   max_open_files);
  ROCKS_LOG_HEADER(log,
                   "                         Options.bytes_per_sync: %" PRIu64,
                   bytes_per_sync);
  ROCKS_LOG_HEADER(log,
                   "                     Options.wal_bytes_per_sync: %" PRIu64,
                   wal_bytes_per_sync);
  ROCKS_LOG_HEADER(log, "                  Options.strict_bytes_per_sync: %d",
                   strict_bytes_per_sync);
  ROCKS_LOG_HEADER(log,
                   "      Options.compaction_readahead_size: %" ROCKSDB_PRIszt,
                   compaction_readahead_size);
  ROCKS_LOG_HEADER(log, "                 Options.max_background_flushes: %d",
                   max_background_flushes);
  ROCKS_LOG_HEADER(log,
                   "                 Options.max_manifest_file_size: %" PRIu64,
                   max_manifest_file_size);
  ROCKS_LOG_HEADER(log,
                   "                 Options.max_manifest_space_amp_pct: %d",
                   max_manifest_space_amp_pct);
  ROCKS_LOG_HEADER(
      log, "            Options.manifest_preallocation_size: %" ROCKSDB_PRIszt,
      manifest_preallocation_size);
  ROCKS_LOG_HEADER(log, "      Options.verify_manifest_content_on_close: %d",
                   verify_manifest_content_on_close);
  ROCKS_LOG_HEADER(log, "         Options.optimize_manifest_for_recovery: %d",
                   optimize_manifest_for_recovery);
  ROCKS_LOG_HEADER(log, "Options.daily_offpeak_time_utc: %s",
                   daily_offpeak_time_utc.c_str());
  ROCKS_LOG_HEADER(log,
                   "Options.max_compaction_trigger_wakeup_seconds: %" PRIu64,
                   max_compaction_trigger_wakeup_seconds);
  ROCKS_LOG_HEADER(log,
                   "Options.periodic_compaction_phase_recovery_percent: %d",
                   periodic_compaction_phase_recovery_percent);
  ROCKS_LOG_HEADER(log, "                         Options.fast_sst_open: %d",
                   fast_sst_open);
  ROCKS_LOG_HEADER(log, "      Options.remote_compaction_manifest_floor: %d",
                   remote_compaction_manifest_floor);
  ROCKS_LOG_HEADER(log, "Options.use_separate_remote_compaction_pool: %s",
                   use_separate_remote_compaction_pool ? "true" : "false");
}

Status GetMutableDBOptionsFromStrings(
    const MutableDBOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    MutableDBOptions* new_options) {
  assert(new_options);
  *new_options = base_options;
  ConfigOptions config_options;
  Status s = OptionTypeInfo::ParseType(
      config_options, options_map, db_mutable_options_type_info, new_options);
  if (!s.ok()) {
    *new_options = base_options;
  }
  return s;
}

bool MutableDBOptionsAreEqual(const MutableDBOptions& this_options,
                              const MutableDBOptions& that_options) {
  if (this_options.use_separate_remote_compaction_pool !=
      that_options.use_separate_remote_compaction_pool) {
    return false;
  }
  ConfigOptions config_options;
  std::string mismatch;
  return OptionTypeInfo::StructsAreEqual(
      config_options, "MutableDBOptions", &db_mutable_options_type_info,
      "MutableDBOptions", &this_options, &that_options, &mismatch);
}

Status GetStringFromMutableDBOptions(const ConfigOptions& config_options,
                                     const MutableDBOptions& mutable_opts,
                                     std::string* opt_string) {
  return OptionTypeInfo::SerializeType(
      config_options, db_mutable_options_type_info, &mutable_opts, opt_string);
}
}  // namespace ROCKSDB_NAMESPACE
