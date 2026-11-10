//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cstddef>
#include <cstring>

#include "options/cf_options.h"
#include "options/db_options.h"
#include "options/options_helper.h"
#include "rocksdb/convenience.h"
#include "test_util/testharness.h"

#ifndef GFLAGS
bool FLAGS_enable_print = false;
#else
#include "util/gflags_compat.h"
using GFLAGS_NAMESPACE::ParseCommandLineFlags;
DEFINE_bool(enable_print, false, "Print options generated to console.");
#endif  // GFLAGS

namespace ROCKSDB_NAMESPACE {

constexpr size_t kReadOptionsAfterAutoRefresh =
    offsetof(ReadOptions, auto_refresh_iterator_with_snapshot) + sizeof(bool);
constexpr size_t kReadOptionsIndexFactoryPtrAlign =
    alignof(const IndexFactory*);
constexpr size_t kExpectedTableIndexFactoryOffset =
    ((kReadOptionsAfterAutoRefresh + kReadOptionsIndexFactoryPtrAlign - 1) /
     kReadOptionsIndexFactoryPtrAlign) *
    kReadOptionsIndexFactoryPtrAlign;
static_assert(
    offsetof(ReadOptions, table_index_factory) ==
        kExpectedTableIndexFactoryOffset,
    "Keep table_index_factory at the first aligned pointer slot after "
    "auto_refresh_iterator_with_snapshot");
static_assert(offsetof(ReadOptions, read_index) >=
                      kReadOptionsAfterAutoRefresh &&
                  offsetof(ReadOptions, read_index) +
                          sizeof(ReadOptions::ReadIndex) <=
                      offsetof(ReadOptions, table_index_factory),
              "Keep read_index in padding before table_index_factory so "
              "legacy ReadOptions field offsets stay unchanged");
static_assert(static_cast<int>(ReadOptions::ReadIndex::kDefault) == 0);
static_assert(static_cast<int>(ReadOptions::ReadIndex::kBuiltin) == 1);
static_assert(static_cast<int>(ReadOptions::ReadIndex::kPreferCustom) == 2);
static_assert(
    static_cast<int>(BlockBasedTableOptions::IndexMode::kStandardOnly) == 0);
static_assert(
    static_cast<int>(BlockBasedTableOptions::IndexMode::kStandardDefault) == 1);
static_assert(
    static_cast<int>(BlockBasedTableOptions::IndexMode::kCustomDefault) == 2);
static_assert(
    static_cast<int>(BlockBasedTableOptions::IndexMode::kCustomOnly) == 3);
static_assert(static_cast<int>(
                  BlockBasedTableOptions::IndexMode::kStandardRequired) == 4);

// Verify options are settable from options strings.
// We take the approach that depends on compiler behavior that copy constructor
// won't touch implicit padding bytes, so that the test is fragile.
// As a result, we only run the tests to verify new fields in options are
// settable through string on limited platforms as it depends on behavior of
// compilers.
#if defined OS_LINUX || defined OS_WIN
#ifndef __clang__
#ifndef ROCKSDB_UBSAN_RUN

class OptionsSettableTest : public testing::Test {
 public:
  OptionsSettableTest() {}
};

const char kSpecialChar = 'z';
using OffsetGap = std::vector<std::pair<size_t, size_t>>;

void FillWithSpecialChar(char* start_ptr, size_t total_size,
                         const OffsetGap& excluded,
                         char special_char = kSpecialChar) {
  size_t offset = 0;
  // The excluded vector contains pairs of bytes, (first, second).
  // The first bytes are all set to the special char (represented as 'c' below).
  // The second bytes are simply skipped (padding bytes).
  // ccccc[skipped]cccccccc[skiped]cccccccc[skipped]
  for (auto& pair : excluded) {
    std::memset(start_ptr + offset, special_char, pair.first - offset);
    offset = pair.first + pair.second;
  }
  // The rest of the structure is filled with the special characters.
  // ccccc[skipped]cccccccc[skiped]cccccccc[skipped]cccccccccccccccc
  std::memset(start_ptr + offset, special_char, total_size - offset);
}

int NumUnsetBytes(char* start_ptr, size_t total_size,
                  const OffsetGap& excluded) {
  int total_unset_bytes_base = 0;
  size_t offset = 0;
  for (auto& pair : excluded) {
    // The first part of the structure contains memory spaces that can be
    // set (pair.first), and memory spaces that cannot be set (pair.second).
    // Therefore total_unset_bytes_base only agregates bytes set to kSpecialChar
    // in the pair.first bytes, but skips the pair.second bytes (padding bytes).
    for (char* ptr = start_ptr + offset; ptr < start_ptr + pair.first; ptr++) {
      if (*ptr == kSpecialChar) {
        total_unset_bytes_base++;
      }
    }
    offset = pair.first + pair.second;
  }
  // Then total_unset_bytes_base aggregates the bytes
  // set to kSpecialChar in the rest of the structure
  for (char* ptr = start_ptr + offset; ptr < start_ptr + total_size; ptr++) {
    if (*ptr == kSpecialChar) {
      total_unset_bytes_base++;
    }
  }
  return total_unset_bytes_base;
}

// Return true iff two structs are the same except excluded fields.
bool CompareBytes(char* start_ptr1, char* start_ptr2, size_t total_size,
                  const OffsetGap& excluded) {
  size_t offset = 0;
  for (auto& pair : excluded) {
    for (; offset < pair.first; offset++) {
      if (*(start_ptr1 + offset) != *(start_ptr2 + offset)) {
        return false;
      }
    }
    offset = pair.first + pair.second;
  }
  for (; offset < total_size; offset++) {
    if (*(start_ptr1 + offset) != *(start_ptr2 + offset)) {
      return false;
    }
  }
  return true;
}

// If the test fails, likely a new option is added to BlockBasedTableOptions
// but it cannot be set through GetBlockBasedTableOptionsFromString(), or the
// test is not updated accordingly.
// After adding an option, we need to make sure it is settable by
// GetBlockBasedTableOptionsFromString() and add the option to the input string
// passed to the GetBlockBasedTableOptionsFromString() in this test.
// If it is a complicated type, you also need to add the field to
// kBbtoExcluded, and maybe add customized verification for it.
TEST_F(OptionsSettableTest, BlockBasedTableOptionsAllFieldsSettable) {
  // Items in the form of <offset, size>. Need to be in ascending order
  // and not overlapping. Need to update if new option to be excluded is added
  // (e.g, pointer-type)
  const OffsetGap kBbtoExcluded = {
      {offsetof(struct BlockBasedTableOptions, flush_block_policy_factory),
       sizeof(std::shared_ptr<FlushBlockPolicyFactory>)},
      {offsetof(struct BlockBasedTableOptions, block_cache),
       sizeof(std::shared_ptr<Cache>)},
      {offsetof(struct BlockBasedTableOptions, persistent_cache),
       sizeof(std::shared_ptr<PersistentCache>)},
      {offsetof(struct BlockBasedTableOptions, cache_usage_options),
       sizeof(CacheUsageOptions)},
      {offsetof(struct BlockBasedTableOptions, filter_policy),
       sizeof(std::shared_ptr<const FilterPolicy>)},
      {offsetof(struct BlockBasedTableOptions, user_defined_index_factory),
       sizeof(std::shared_ptr<IndexFactory>)},
      {offsetof(struct BlockBasedTableOptions, use_udi_as_primary_index),
       sizeof(bool)},
      {offsetof(struct BlockBasedTableOptions, fail_if_no_udi_on_open),
       sizeof(bool)},
  };

  // In this test, we catch a new option of BlockBasedTableOptions that is not
  // settable through GetBlockBasedTableOptionsFromString().
  // We count padding bytes of the option struct, and assert it to be the same
  // as unset bytes of an option struct initialized by
  // GetBlockBasedTableOptionsFromString().

  char* bbto_ptr = new char[sizeof(BlockBasedTableOptions)];

  // Count padding bytes by setting all bytes in the memory to a special char,
  // copy a well constructed struct to this memory and see how many special
  // bytes left.
  BlockBasedTableOptions* bbto = new (bbto_ptr) BlockBasedTableOptions();
  FillWithSpecialChar(bbto_ptr, sizeof(BlockBasedTableOptions), kBbtoExcluded);
  // It based on the behavior of compiler that padding bytes are not changed
  // when copying the struct. It's prone to failure when compiler behavior
  // changes. We verify there is unset bytes to detect the case.
  *bbto = BlockBasedTableOptions();
  int unset_bytes_base =
      NumUnsetBytes(bbto_ptr, sizeof(BlockBasedTableOptions), kBbtoExcluded);
  ASSERT_GT(unset_bytes_base, 0);
  bbto->~BlockBasedTableOptions();

  // Construct the base option passed into
  // GetBlockBasedTableOptionsFromString().
  bbto = new (bbto_ptr) BlockBasedTableOptions();
  FillWithSpecialChar(bbto_ptr, sizeof(BlockBasedTableOptions), kBbtoExcluded);

  char* new_bbto_ptr = new char[sizeof(BlockBasedTableOptions)];
  BlockBasedTableOptions* new_bbto =
      new (new_bbto_ptr) BlockBasedTableOptions();
  FillWithSpecialChar(new_bbto_ptr, sizeof(BlockBasedTableOptions),
                      kBbtoExcluded);

  // Need to update the option string if a new option is added.
  ConfigOptions config_options;
  config_options.input_strings_escaped = false;
  config_options.ignore_unknown_options = false;
  config_options.invoke_prepare_options = false;
  config_options.ignore_unsupported_options = false;
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, *bbto,
      "cache_index_and_filter_blocks=1;"
      "cache_index_and_filter_blocks_with_high_priority=true;"
      "metadata_cache_options={top_level_index_pinning=kFallback;"
      "partition_pinning=kAll;"
      "unpartitioned_pinning=kFlushedAndSimilar;};"
      "pin_l0_filter_and_index_blocks_in_cache=1;"
      "pin_top_level_index_and_filter=1;"
      "index_type=kHashSearch;"
      "index_block_search_type=kBinary;"
      "data_block_index_type=kDataBlockBinaryAndHash;"
      "optimize_key_common_prefix=kEnabled;"
      "index_shortening=kNoShortening;"
      "index_mode=kCustomDefault;"
      "data_block_hash_table_util_ratio=0.75;"
      "checksum=kxxHash;no_block_cache=1;"
      "block_cache=1M;block_cache_compressed=1k;block_size=1024;"
      "block_size_deviation=8;block_restart_interval=4; "
      "metadata_block_size=1024;"
      "partition_filters=false;"
      "decouple_partitioned_filters=true;"
      "optimize_filters_for_memory=true;"
      "use_delta_encoding=true;"
      "index_block_restart_interval=4;"
      "filter_policy=bloomfilter:4:true;whole_key_filtering=1;detect_filter_"
      "construct_corruption=false;"
      "format_version=1;"
      "verify_compression=true;read_amp_bytes_per_bit=0;"
      "enable_index_compression=false;"
      "block_align=true;"
      "super_block_alignment_size=65536;"
      "super_block_alignment_space_overhead_ratio=4096;"
      "max_auto_readahead_size=0;"
      "prepopulate_block_cache=kDisable;"
      "initial_auto_readahead_size=0;"
      "num_file_reads_for_auto_readahead=0;"
      "separate_key_value_in_data_block=true;"
      "uniform_cv_threshold=0.2",
      new_bbto));

  ASSERT_EQ(unset_bytes_base,
            NumUnsetBytes(new_bbto_ptr, sizeof(BlockBasedTableOptions),
                          kBbtoExcluded));

  ASSERT_TRUE(new_bbto->block_cache.get() != nullptr);
  ASSERT_TRUE(new_bbto->filter_policy.get() != nullptr);

  bbto->~BlockBasedTableOptions();
  new_bbto->~BlockBasedTableOptions();

  delete[] bbto_ptr;
  delete[] new_bbto_ptr;
}

TEST_F(OptionsSettableTest, TablePropertiesAllFieldsSettable) {
  const OffsetGap kTablePropertiesExcluded = {
      {offsetof(struct TableProperties, db_id), sizeof(std::string)},
      {offsetof(struct TableProperties, db_session_id), sizeof(std::string)},
      {offsetof(struct TableProperties, db_host_id), sizeof(std::string)},
      {offsetof(struct TableProperties, column_family_name),
       sizeof(std::string)},
      {offsetof(struct TableProperties, filter_policy_name),
       sizeof(std::string)},
      {offsetof(struct TableProperties, comparator_name), sizeof(std::string)},
      {offsetof(struct TableProperties, merge_operator_name),
       sizeof(std::string)},
      {offsetof(struct TableProperties, prefix_extractor_name),
       sizeof(std::string)},
      {offsetof(struct TableProperties, property_collectors_names),
       sizeof(std::string)},
      {offsetof(struct TableProperties, compression_name), sizeof(std::string)},
      {offsetof(struct TableProperties, compression_options),
       sizeof(std::string)},
      {offsetof(struct TableProperties, seqno_to_time_mapping),
       sizeof(std::string)},
      {offsetof(struct TableProperties, user_collected_properties),
       sizeof(UserCollectedProperties)},
      {offsetof(struct TableProperties, readable_properties),
       sizeof(UserCollectedProperties)},
  };

  char* tp_ptr = new char[sizeof(TableProperties)];

  TableProperties* tp = new (tp_ptr) TableProperties();
  FillWithSpecialChar(tp_ptr, sizeof(TableProperties),
                      kTablePropertiesExcluded);
  ASSERT_GT(
      NumUnsetBytes(tp_ptr, sizeof(TableProperties), kTablePropertiesExcluded),
      0);

  char* new_tp_ptr = new char[sizeof(TableProperties)];
  TableProperties* new_tp = new (new_tp_ptr) TableProperties();
  FillWithSpecialChar(new_tp_ptr, sizeof(TableProperties),
                      kTablePropertiesExcluded);

  // Need to update the option string if a new option is added.
  ConfigOptions config_options;
  config_options.input_strings_escaped = false;
  config_options.ignore_unknown_options = false;
  config_options.invoke_prepare_options = false;
  config_options.ignore_unsupported_options = false;
  ASSERT_OK(TableProperties::Parse(
      config_options,
      "readable_properties={7265616461626C655F6B6579="
      "7265616461626C655F76616C7565;};compression_options=;compression_name=;"
      "property_collectors_names=;prefix_extractor_name=;db_host_id="
      "64625F686F73745F6964;db_session_id=64625F73657373696F6E5F6964;creation_"
      "time=0;num_data_blocks=123;num_data_blocks_compression_rejected=42;"
      "num_data_blocks_compression_bypassed=7;"
      "index_value_is_delta_encoded=0;top_level_"
      "index_size=0;data_size=100;uncompressed_data_size=1234;"
      "merge_operator_name=;index_partitions=0;file_"
      "creation_time=0;raw_value_size=0;index_size=200;user_collected_"
      "properties={757365725F6B6579=757365725F76616C7565;};tail_start_offset=0;"
      "seqno_to_time_mapping=;raw_key_size=0;slow_compression_estimated_data_"
      "size=0;filter_size=0;orig_file_number=3;num_deletions=0;num_range_"
      "deletions=0;format_version=0;comparator_name="
      "636F6D70617261746F725F6E616D65;num_filter_entries=0;db_id="
      "64625F686F73745F6964;"
      "column_family_id=2147483647;fixed_key_len=0;fast_"
      "compression_estimated_data_size=0;filter_policy_name="
      "66696C7465725F706F6C6963795F6E616D65;oldest_key_time=0;newest_key_time="
      "0;column_family_"
      "name=64656661756C74;user_defined_timestamps_persisted=1;num_entries=100;"
      "num_merge_operands=0;index_key_is_user_key=0;key_largest_seqno="
      "18446744073709551615;key_smallest_seqno="
      "18;data_block_restart_interval=16;index_block_restart_interval=1;"
      "separate_key_value_in_data_block=0;num_uniform_blocks=0;"
      "lsm_info_at_creation=513;"
      "udi_is_primary_index=0;standard_index_is_stub=0;",
      new_tp));

  // All bytes are set from the parse
  ASSERT_EQ(NumUnsetBytes(new_tp_ptr, sizeof(TableProperties),
                          kTablePropertiesExcluded),
            0);

  ASSERT_EQ(new_tp->db_host_id, "db_host_id");
  ASSERT_EQ(new_tp->num_entries, 100);
  ASSERT_EQ(new_tp->num_data_blocks, 123);
  ASSERT_EQ(new_tp->user_collected_properties.size(), 1);
  ASSERT_EQ(new_tp->readable_properties.size(), 1);

  tp->~TableProperties();
  new_tp->~TableProperties();

  delete[] tp_ptr;
  delete[] new_tp_ptr;
}

// If the test fails, likely a new option is added to DBOptions
// but it cannot be set through GetDBOptionsFromString(), or the test is not
// updated accordingly.
// After adding an option, we need to make sure it is settable by
// GetDBOptionsFromString() and add the option to the input string passed to
// DBOptionsFromString()in this test.
// If it is a complicated type, you also need to add the field to
// kDBOptionsExcluded, and maybe add customized verification for it.
TEST_F(OptionsSettableTest, DBOptionsAllFieldsSettable) {
  const OffsetGap kDBOptionsExcluded = {
      {offsetof(struct DBOptions, env), sizeof(Env*)},
      {offsetof(struct DBOptions, rate_limiter),
       sizeof(std::shared_ptr<RateLimiter>)},
      {offsetof(struct DBOptions, sst_file_manager),
       sizeof(std::shared_ptr<SstFileManager>)},
      {offsetof(struct DBOptions, info_log), sizeof(std::shared_ptr<Logger>)},
      {offsetof(struct DBOptions, statistics),
       sizeof(std::shared_ptr<Statistics>)},
      {offsetof(struct DBOptions, db_paths), sizeof(std::vector<DbPath>)},
      {offsetof(struct DBOptions, db_log_dir), sizeof(std::string)},
      {offsetof(struct DBOptions, wal_dir), sizeof(std::string)},
      {offsetof(struct DBOptions, write_buffer_manager),
       sizeof(std::shared_ptr<WriteBufferManager>)},
      {offsetof(struct DBOptions, listeners),
       sizeof(std::vector<std::shared_ptr<EventListener>>)},
      {offsetof(struct DBOptions, row_cache), sizeof(std::shared_ptr<Cache>)},
      {offsetof(struct DBOptions, wal_filter), sizeof(const WalFilter*)},
      {offsetof(struct DBOptions, file_checksum_gen_factory),
       sizeof(std::shared_ptr<FileChecksumGenFactory>)},
      {offsetof(struct DBOptions, db_host_id), sizeof(std::string)},
      {offsetof(struct DBOptions, checksum_handoff_file_types),
       sizeof(FileTypeSet)},
      {offsetof(struct DBOptions, compaction_service),
       sizeof(std::shared_ptr<CompactionService>)},
      {offsetof(struct DBOptions, use_session_tmp_dir_for_remote_compaction),
       sizeof(bool)},
      {offsetof(struct DBOptions, daily_offpeak_time_utc), sizeof(std::string)},
      {offsetof(struct DBOptions, calculate_sst_write_lifetime_hint_set),
       sizeof(CompactionStyleSet)},
  };

  char* options_ptr = new char[sizeof(DBOptions)];

  // Count padding bytes by setting all bytes in the memory to a special char,
  // copy a well constructed struct to this memory and see how many special
  // bytes left.
  DBOptions* options = new (options_ptr) DBOptions();
  FillWithSpecialChar(options_ptr, sizeof(DBOptions), kDBOptionsExcluded);
  // It based on the behavior of compiler that padding bytes are not changed
  // when copying the struct. It's prone to failure when compiler behavior
  // changes. We verify there is unset bytes to detect the case.
  *options = DBOptions();
  int unset_bytes_base =
      NumUnsetBytes(options_ptr, sizeof(DBOptions), kDBOptionsExcluded);
  ASSERT_GT(unset_bytes_base, 0);
  options->~DBOptions();

  // Now also check that BuildDBOptions populates everything
  FillWithSpecialChar(options_ptr, sizeof(DBOptions), kDBOptionsExcluded);
  BuildDBOptions({}, {}, *options);
  ASSERT_EQ(unset_bytes_base,
            NumUnsetBytes(options_ptr, sizeof(DBOptions), kDBOptionsExcluded));

  options = new (options_ptr) DBOptions();
  FillWithSpecialChar(options_ptr, sizeof(DBOptions), kDBOptionsExcluded);

  char* new_options_ptr = new char[sizeof(DBOptions)];
  DBOptions* new_options = new (new_options_ptr) DBOptions();
  FillWithSpecialChar(new_options_ptr, sizeof(DBOptions), kDBOptionsExcluded);

  // Need to update the option string if a new option is added.
  ConfigOptions config_options(*options);
  config_options.input_strings_escaped = false;
  config_options.ignore_unknown_options = false;
  ASSERT_OK(GetDBOptionsFromString(
      config_options, *options,
      "wal_bytes_per_sync=4295048118;"
      "delete_obsolete_files_period_micros=4294967758;"
      "WAL_ttl_seconds=4295008036;"
      "WAL_size_limit_MB=4295036161;"
      "max_write_batch_group_size_bytes=1048576;"
      "wal_dir=path/to/wal_dir;"
      "db_write_buffer_size=2587;"
      "max_subcompactions=64330;"
      "table_cache_numshardbits=28;"
      "max_open_files=72;"
      "max_file_opening_threads=35;"
      "read_io_executor_threads=7;"
      "max_background_jobs=8;"
      "max_background_compactions=33;"
      "use_fsync=true;"
      "use_adaptive_mutex=false;"
      "max_total_wal_size=4295005604;"
      "compaction_readahead_size=0;"
      "keep_log_file_num=4890;"
      "skip_stats_update_on_db_open=false;"
      "max_manifest_file_size=4295009941;"
      "max_manifest_space_amp_pct=321;"
      "db_log_dir=path/to/db_log_dir;"
      "writable_file_max_buffer_size=1048576;"
      "paranoid_checks=true;"
      "open_files_async=true;"
      "flush_verify_memtable_count=true;"
      "compaction_verify_record_count=true;"
      "track_and_verify_wals_in_manifest=true;"
      "track_and_verify_wals=true;"
      "verify_sst_unique_id_in_manifest=true;"
      "fast_sst_open=true;"
      "is_fd_close_on_exec=false;"
      "bytes_per_sync=4295013613;"
      "strict_bytes_per_sync=true;"
      "enable_thread_tracking=false;"
      "recycle_log_file_num=0;"
      "async_wal_precreate=true;"
      "create_missing_column_families=true;"
      "log_file_time_to_roll=3097;"
      "max_background_flushes=35;"
      "create_if_missing=false;"
      "error_if_exists=true;"
      "delayed_write_rate=4294976214;"
      "manifest_preallocation_size=1222;"
      "allow_mmap_writes=false;"
      "stats_dump_period_sec=70127;"
      "mutex_hold_log_threshold_us=76543;"
      "stats_persist_period_sec=54321;"
      "persist_stats_to_disk=true;"
      "stats_history_buffer_size=14159;"
      "allow_fallocate=true;"
      "allow_mmap_reads=false;"
      "use_direct_reads=false;"
      "use_direct_io_for_compaction_reads=false;"
      "use_direct_io_for_flush_and_compaction=false;"
      "max_log_file_size=4607;"
      "advise_random_on_open=true;"
      "enable_pipelined_write=false;"
      "unordered_write=false;"
      "allow_concurrent_memtable_write=true;"
      "wal_recovery_mode=kPointInTimeRecovery;"
      "enable_write_thread_adaptive_yield=true;"
      "write_thread_slow_yield_usec=5;"
      "write_thread_max_yield_usec=1000;"
      "info_log_level=DEBUG_LEVEL;"
      "dump_malloc_stats=false;"
      "allow_2pc=false;"
      "avoid_flush_during_recovery=false;"
      "enforce_write_buffer_manager_during_recovery=true;"
      "avoid_flush_during_shutdown=false;"
      "allow_ingest_behind=false;"
      "concurrent_prepare=false;"
      "two_write_queues=false;"
      "manual_wal_flush=false;"
      "wal_compression=kZSTD;"
      "background_close_inactive_wals=true;"
      "wal_iterator_tail_rotations=true;"
      "seq_per_batch=false;"
      "atomic_flush=false;"
      "avoid_unnecessary_blocking_io=false;"
      "log_readahead_size=0;"
      "write_dbid_to_manifest=false;"
      "track_options_file_number_in_manifest=true;"
      "optimize_manifest_for_recovery=false;"
      "best_efforts_recovery=false;"
      "max_bgerror_resume_count=2;"
      "bgerror_resume_retry_interval=1000000;"
      "db_host_id=hostname;"
      "lowest_used_cache_tier=kNonVolatileBlockTier;"
      "allow_data_in_errors=false;"
      "enforce_single_del_contracts=false;"
      "daily_offpeak_time_utc=08:30-19:00;"
      "max_compaction_trigger_wakeup_seconds=43200;"
      "periodic_compaction_phase_recovery_percent=25;"
      "follower_refresh_catchup_period_ms=123;"
      "follower_catchup_retry_count=456;"
      "follower_catchup_retry_wait_ms=789;"
      "metadata_write_temperature=kCold;"
      "wal_write_temperature=kHot;"
      "background_close_inactive_wals=true;"
      "wal_iterator_tail_rotations=true;"
      "write_dbid_to_manifest=true;"
      "optimize_manifest_for_recovery=true;"
      "write_identity_file=true;"
      "verify_manifest_content_on_close=false;"
      "prefix_seek_opt_in_only=true;"
      "fast_sst_open=true;"
      "remote_compaction_manifest_floor=false;"
      "use_separate_remote_compaction_pool=true;"
      "use_session_tmp_dir_for_remote_compaction=true;"
      "reuse_manifest_on_open=true;",
      new_options));

  ASSERT_EQ(unset_bytes_base, NumUnsetBytes(new_options_ptr, sizeof(DBOptions),
                                            kDBOptionsExcluded));

  options->~DBOptions();
  new_options->~DBOptions();

  delete[] options_ptr;
  delete[] new_options_ptr;
}

// status check adds CXX flag -fno-elide-constructors which fails this test.
#ifndef ROCKSDB_ASSERT_STATUS_CHECKED
// If the test fails, likely a new option is added to ColumnFamilyOptions
// but it cannot be set through GetColumnFamilyOptionsFromString(), or the
// test is not updated accordingly.
// After adding an option, we need to make sure it is settable by
// GetColumnFamilyOptionsFromString() and add the option to the input
// string passed to GetColumnFamilyOptionsFromString() in this test.
// If it is a complicated type, you also need to add the field to
// kColumnFamilyOptionsExcluded, and maybe add customized verification
// for it.
TEST_F(OptionsSettableTest, ColumnFamilyOptionsAllFieldsSettable) {
  // options in the excluded set need to appear in the same order as in
  // ColumnFamilyOptions.
  const OffsetGap kColumnFamilyOptionsExcluded = {
      {offsetof(struct ColumnFamilyOptions, inplace_callback),
       sizeof(UpdateStatus (*)(char*, uint32_t*, Slice, std::string*))},
      {offsetof(struct ColumnFamilyOptions,
                memtable_insert_with_hint_prefix_extractor),
       sizeof(std::shared_ptr<const SliceTransform>)},
      {offsetof(struct ColumnFamilyOptions, compression_per_level),
       sizeof(std::vector<CompressionType>)},
      {offsetof(struct ColumnFamilyOptions,
                max_bytes_for_level_multiplier_additional),
       sizeof(std::vector<int>)},
      {offsetof(struct ColumnFamilyOptions, compaction_options_fifo),
       sizeof(struct CompactionOptionsFIFO)},
      {offsetof(struct ColumnFamilyOptions, memtable_factory),
       sizeof(std::shared_ptr<MemTableRepFactory>)},
      {offsetof(struct ColumnFamilyOptions,
                table_properties_collector_factories),
       sizeof(ColumnFamilyOptions::TablePropertiesCollectorFactories)},
      {offsetof(struct ColumnFamilyOptions, preclude_last_level_data_seconds),
       sizeof(uint64_t)},
      {offsetof(struct ColumnFamilyOptions, preserve_internal_time_seconds),
       sizeof(uint64_t)},
      {offsetof(struct ColumnFamilyOptions, blob_compression_opts),
       sizeof(CompressionOptions)},
      {offsetof(struct ColumnFamilyOptions, blob_cache),
       sizeof(std::shared_ptr<Cache>)},
      {offsetof(struct ColumnFamilyOptions,
                blob_direct_write_partition_strategy),
       sizeof(std::shared_ptr<BlobFilePartitionStrategy>)},
      {offsetof(struct ColumnFamilyOptions, comparator), sizeof(Comparator*)},
      {offsetof(struct ColumnFamilyOptions, merge_operator),
       sizeof(std::shared_ptr<MergeOperator>)},
      {offsetof(struct ColumnFamilyOptions, compaction_filter),
       sizeof(const CompactionFilter*)},
      {offsetof(struct ColumnFamilyOptions, compaction_filter_factory),
       sizeof(std::shared_ptr<CompactionFilterFactory>)},
      {offsetof(struct ColumnFamilyOptions, compression_manager),
       sizeof(std::shared_ptr<CompressionManager>)},
      {offsetof(struct ColumnFamilyOptions, prefix_extractor),
       sizeof(std::shared_ptr<const SliceTransform>)},
      {offsetof(struct ColumnFamilyOptions, table_factory),
       sizeof(std::shared_ptr<TableFactory>)},
      {offsetof(struct ColumnFamilyOptions, cf_paths),
       sizeof(std::vector<DbPath>)},
      {offsetof(struct ColumnFamilyOptions, compaction_thread_limiter),
       sizeof(std::shared_ptr<ConcurrentTaskLimiter>)},
      {offsetof(struct ColumnFamilyOptions, sst_partitioner_factory),
       sizeof(std::shared_ptr<SstPartitionerFactory>)},
  };

  char* options_ptr = new char[sizeof(ColumnFamilyOptions)];

  // Count padding bytes by setting all bytes in the memory to a special char,
  // copy a well constructed struct to this memory and see how many special
  // bytes left.
  FillWithSpecialChar(options_ptr, sizeof(ColumnFamilyOptions),
                      kColumnFamilyOptionsExcluded);

  // Invoke a user-defined constructor in the hope that it does not overwrite
  // padding bytes. Note that previously we relied on the implicitly-defined
  // copy-assignment operator (i.e., `*options = ColumnFamilyOptions();`) here,
  // which did in fact modify padding bytes.
  ColumnFamilyOptions* options = new (options_ptr) ColumnFamilyOptions();

  int unset_bytes_base = NumUnsetBytes(options_ptr, sizeof(ColumnFamilyOptions),
                                       kColumnFamilyOptionsExcluded);
  ASSERT_GT(unset_bytes_base, 0);
  options->~ColumnFamilyOptions();

  options = new (options_ptr) ColumnFamilyOptions();
  FillWithSpecialChar(options_ptr, sizeof(ColumnFamilyOptions),
                      kColumnFamilyOptionsExcluded);

  // Following options are not settable through
  // GetColumnFamilyOptionsFromString():
  options->compaction_options_universal = CompactionOptionsUniversal();
  options->num_levels = 42;  // Initialize options for MutableCF
  options->compaction_filter = nullptr;
  options->sst_partitioner_factory = nullptr;

  char* new_options_ptr = new char[sizeof(ColumnFamilyOptions)];
  ColumnFamilyOptions* new_options =
      new (new_options_ptr) ColumnFamilyOptions();
  FillWithSpecialChar(new_options_ptr, sizeof(ColumnFamilyOptions),
                      kColumnFamilyOptionsExcluded);

  // Need to update the option string if a new option is added.
  ConfigOptions config_options;
  config_options.input_strings_escaped = false;
  config_options.ignore_unknown_options = false;
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, *options,
      "compaction_filter_factory=mpudlojcujCompactionFilterFactory;"
      "table_factory=PlainTable;"
      "prefix_extractor=rocksdb.CappedPrefix.13;"
      "comparator=leveldb.BytewiseComparator;"
      "compression_per_level=kBZip2Compression:kBZip2Compression:"
      "kBZip2Compression:kNoCompression:kZlibCompression:kBZip2Compression:"
      "kSnappyCompression;"
      "max_bytes_for_level_base=986;"
      "bloom_locality=8016;"
      "target_file_size_base=4294976376;"
      "memtable_huge_page_size=2557;"
      "max_successive_merges=5497;"
      "strict_max_successive_merges=true;"
      "max_sequential_skip_in_iterations=4294971408;"
      "arena_block_size=1893;"
      "target_file_size_multiplier=35;"
      "target_file_size_is_upper_bound=false;"
      "min_write_buffer_number_to_merge=9;"
      "max_write_buffer_number=84;"
      "write_buffer_size=1653;"
      "max_compaction_bytes=64;"
      "ignore_max_compaction_bytes_for_input=true;"
      "max_bytes_for_level_multiplier=60;"
      "memtable_factory=SkipListFactory;"
      "compression=kNoCompression;"
      "compression_opts={max_dict_buffer_bytes=5;use_zstd_dict_trainer=true;"
      "enabled=false;parallel_threads=6;zstd_max_train_bytes=7;strategy=8;max_"
      "dict_bytes=9;level=10;window_bits=11;max_compressed_bytes_per_kb=987;"
      "auto_skip=true;auto_skip_min_sample_every=17;"
      "checksum=true};"
      "bottommost_compression_opts={max_dict_buffer_bytes=4;use_zstd_dict_"
      "trainer=true;enabled=true;parallel_threads=5;zstd_max_train_bytes=6;"
      "strategy=7;max_dict_bytes=8;level=9;window_bits=10;max_compressed_bytes_"
      "per_kb=876;auto_skip=true;auto_skip_min_sample_"
      "every=23;checksum=true};"
      "bottommost_compression=kDisableCompressionOption;"
      "compression_manager=BuiltinV2;"
      "level0_stop_writes_trigger=33;"
      "num_levels=99;"
      "level0_slowdown_writes_trigger=22;"
      "level0_file_num_compaction_trigger=14;"
      "compaction_filter=urxcqstuwnCompactionFilter;"
      "soft_pending_compaction_bytes_limit=0;"
      "max_write_buffer_size_to_maintain=2147483648;"
      "merge_operator=aabcxehazrMergeOperator;"
      "memtable_prefix_bloom_size_ratio=0.4642;"
      "memtable_whole_key_filtering=true;"
      "memtable_insert_with_hint_prefix_extractor=rocksdb.CappedPrefix.13;"
      "check_flush_compaction_key_order=false;"
      "paranoid_file_checks=true;"
      "force_consistency_checks=true;"
      "inplace_update_num_locks=7429;"
      "experimental_mempurge_threshold=0.0001;"
      "optimize_filters_for_hits=false;"
      "level_compaction_dynamic_level_bytes=false;"
      "level_compaction_dynamic_file_size=true;"
      "inplace_update_support=false;"
      "compaction_style=kCompactionStyleFIFO;"
      "compaction_pri=kMinOverlappingRatio;"
      "hard_pending_compaction_bytes_limit=0;"
      "disable_auto_compactions=false;"
      "report_bg_io_stats=true;"
      "disallow_memtable_writes=true;"
      "ttl=60;"
      "periodic_compaction_seconds=3600;"
      "read_triggered_compaction_threshold=0.5;"
      "sample_for_compression=0;"
      "enable_blob_files=true;"
      "enable_blob_direct_write=true;"
      "min_blob_size=256;"
      "blob_file_size=1000000;"
      "blob_file_writable_file_max_buffer_size=131072;"
      "blob_compression_type=kBZip2Compression;"
      "blob_compression_opts={window_bits=-14;level=1;strategy=0;max_dict_"
      "bytes=0;"
      "zstd_max_train_bytes=0;enabled=true;parallel_threads=1;"
      "max_dict_buffer_bytes=0;use_zstd_dict_trainer=true;"
      "max_compressed_bytes_per_kb=896;auto_skip=false;"
      "auto_skip_min_sample_every=0;checksum=false};"
      "enable_blob_garbage_collection=true;"
      "blob_garbage_collection_age_cutoff=0.5;"
      "blob_garbage_collection_force_threshold=0.75;"
      "blob_compaction_readahead_size=262144;"
      "blob_file_starting_level=1;"
      "blob_direct_write_partitions=3;"
      "prepopulate_blob_cache=kDisable;"
      "bottommost_temperature=kWarm;"
      "last_level_temperature=kWarm;"
      "default_write_temperature=kCold;"
      "default_temperature=kHot;"
      "preclude_last_level_data_seconds=86400;"
      "preserve_internal_time_seconds=86400;"
      "compaction_options_fifo={max_table_files_size=3;allow_"
      "compaction=true;age_for_warm=0;file_temperature_age_thresholds={{"
      "temperature=kCold;age=12345}};max_data_files_size=1073741824;"
      "use_kv_ratio_compaction=false;};"
      "blob_cache=1M;"
      "memtable_protection_bytes_per_key=2;"
      "persist_user_defined_timestamps=true;"
      "block_protection_bytes_per_key=1;"
      "memtable_max_range_deletions=999999;"
      "bottommost_file_compaction_delay=7200;"
      "uncache_aggressiveness=1234;"
      "paranoid_memory_checks=1;"
      "memtable_verify_per_key_checksum_on_seek=1;"
      "memtable_op_scan_flush_trigger=123;"
      "memtable_avg_op_scan_flush_trigger=12;"
      "min_tombstones_for_range_conversion=8;"
      "cf_allow_ingest_behind=1;"
      "memtable_batch_lookup_optimization=1;"
      "verify_output_flags=2053;",
      new_options));

  ASSERT_NE(new_options->blob_cache.get(), nullptr);

  ASSERT_EQ(unset_bytes_base,
            NumUnsetBytes(new_options_ptr, sizeof(ColumnFamilyOptions),
                          kColumnFamilyOptionsExcluded));

  // Custom verification since compaction_options_fifo was in
  // kColumnFamilyOptionsExcluded
  ASSERT_TRUE(new_options->enable_blob_direct_write);
  ASSERT_EQ(new_options->blob_direct_write_partitions, 3U);
  ASSERT_EQ(new_options->blob_file_writable_file_max_buffer_size, 131072U);
  ASSERT_EQ(new_options->compaction_options_fifo.max_table_files_size, 3);
  ASSERT_EQ(new_options->compaction_options_fifo.allow_compaction, true);
  ASSERT_EQ(new_options->compaction_options_fifo.file_temperature_age_thresholds
                .size(),
            1);
  ASSERT_EQ(
      new_options->compaction_options_fifo.file_temperature_age_thresholds[0]
          .temperature,
      Temperature::kCold);
  ASSERT_EQ(
      new_options->compaction_options_fifo.file_temperature_age_thresholds[0]
          .age,
      12345);
  // TODO: try to enhance ObjectLibrary to support singletons
  // ASSERT_EQ(new_options->compression_manager,
  //           GetBuiltinV2CompressionManager());
  ASSERT_STREQ(new_options->compression_manager->Name(),
               GetBuiltinV2CompressionManager()->Name());

  ColumnFamilyOptions rnd_filled_options = *new_options;

  options->~ColumnFamilyOptions();
  new_options->~ColumnFamilyOptions();

  delete[] options_ptr;
  delete[] new_options_ptr;

  // Test copying to mutabable and immutable options and copy back the mutable
  // part.
  const OffsetGap kMutableCFOptionsExcluded = {
      {offsetof(struct MutableCFOptions, prefix_extractor),
       sizeof(std::shared_ptr<const SliceTransform>)},
      {offsetof(struct MutableCFOptions,
                max_bytes_for_level_multiplier_additional),
       sizeof(std::vector<int>)},
      {offsetof(struct MutableCFOptions, compaction_options_fifo),
       sizeof(struct CompactionOptionsFIFO)},
      {offsetof(struct MutableCFOptions, compression_manager),
       sizeof(std::shared_ptr<CompressionManager>)},
      {offsetof(struct MutableCFOptions, compression_per_level),
       sizeof(std::vector<CompressionType>)},
      {offsetof(struct MutableCFOptions, max_file_size),
       sizeof(std::vector<uint64_t>)},
  };

  // For all memory used for options, pre-fill every char. Otherwise, the
  // padding bytes might be different so that byte-wise comparison doesn't
  // general equal results even if objects are equal.
  const char kMySpecialChar = 'x';
  char* mcfo1_ptr = new char[sizeof(MutableCFOptions)];
  FillWithSpecialChar(mcfo1_ptr, sizeof(MutableCFOptions),
                      kMutableCFOptionsExcluded, kMySpecialChar);
  char* mcfo2_ptr = new char[sizeof(MutableCFOptions)];
  FillWithSpecialChar(mcfo2_ptr, sizeof(MutableCFOptions),
                      kMutableCFOptionsExcluded, kMySpecialChar);

  // A clean column family options is constructed after filling the same special
  // char as the initial one. So that the padding bytes are the same.
  char* cfo_clean_ptr = new char[sizeof(ColumnFamilyOptions)];
  FillWithSpecialChar(cfo_clean_ptr, sizeof(ColumnFamilyOptions),
                      kColumnFamilyOptionsExcluded);
  rnd_filled_options.num_levels = 66;
  ColumnFamilyOptions* cfo_clean = new (cfo_clean_ptr) ColumnFamilyOptions();

  MutableCFOptions* mcfo1 =
      new (mcfo1_ptr) MutableCFOptions(rnd_filled_options);
  ColumnFamilyOptions cfo_back = BuildColumnFamilyOptions(*cfo_clean, *mcfo1);
  MutableCFOptions* mcfo2 = new (mcfo2_ptr) MutableCFOptions(cfo_back);

  ASSERT_TRUE(CompareBytes(mcfo1_ptr, mcfo2_ptr, sizeof(MutableCFOptions),
                           kMutableCFOptionsExcluded));

  cfo_clean->~ColumnFamilyOptions();
  mcfo1->~MutableCFOptions();
  mcfo2->~MutableCFOptions();
  delete[] mcfo1_ptr;
  delete[] mcfo2_ptr;
  delete[] cfo_clean_ptr;
}
#endif  // !ROCKSDB_ASSERT_STATUS_CHECKED
#endif  // !ROCKSDB_UBSAN_RUN
#endif  // !__clang__
#endif  // OS_LINUX || OS_WIN

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
#ifdef GFLAGS
  ParseCommandLineFlags(&argc, &argv, true);
#endif  // GFLAGS
  return RUN_ALL_TESTS();
}
