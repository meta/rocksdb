//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cctype>
#include <cinttypes>
#include <cstring>
#include <map>
#include <type_traits>
#include <unordered_map>

#include "cache/lru_cache.h"
#include "cache/sharded_cache.h"
#include "options/options_helper.h"
#include "options/options_parser.h"
#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/utilities/leveldb_options.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "table/block_based/filter_policy_internal.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"
#include "utilities/merge_operators/bytesxor.h"
#include "utilities/merge_operators/sortlist.h"
#include "utilities/merge_operators/string_append/stringappend.h"
#include "utilities/merge_operators/string_append/stringappend2.h"

#ifndef GFLAGS
bool FLAGS_enable_print = false;
#else
#include "util/gflags_compat.h"
using GFLAGS_NAMESPACE::ParseCommandLineFlags;
DEFINE_bool(enable_print, false, "Print options generated to console.");
#endif  // GFLAGS

namespace ROCKSDB_NAMESPACE {

// ReadOptions is copied frequently on read paths, so keep it cheap to copy:
// all members must stay trivially copyable (use pointers, not std::function /
// std::string / std::shared_ptr members).
static_assert(std::is_trivially_copyable_v<ReadOptions>);

class OptionsTest : public testing::Test {};

class UnregisteredTableFactory : public TableFactory {
 public:
  UnregisteredTableFactory() = default;
  const char* Name() const override { return "Unregistered"; }
  using TableFactory::NewTableReader;
  Status NewTableReader(const ReadOptions&, const TableReaderOptions&,
                        std::unique_ptr<RandomAccessFileReader>&&, uint64_t,
                        std::unique_ptr<TableReader>*, bool) const override {
    return Status::NotSupported();
  }
  TableBuilder* NewTableBuilder(const TableBuilderOptions&,
                                WritableFileWriter*) const override {
    return nullptr;
  }
  std::unique_ptr<TableFactory> Clone() const override {
    return std::make_unique<UnregisteredTableFactory>();
  }
};

TEST_F(OptionsTest, GetOptionsFromMapTest) {
  std::unordered_map<std::string, std::string> cf_options_map = {
      {"write_buffer_size", "1"},
      {"max_write_buffer_number", "2"},
      {"min_write_buffer_number_to_merge", "3"},
      {"max_write_buffer_number_to_maintain", "99"},
      {"max_write_buffer_size_to_maintain", "-99999"},
      {"compression", "kSnappyCompression"},
      {"compression_per_level",
       "kNoCompression:"
       "kSnappyCompression:"
       "kZlibCompression:"
       "kBZip2Compression:"
       "kLZ4Compression:"
       "kLZ4HCCompression:"
       "kXpressCompression:"
       "kZSTD"},
      {"bottommost_compression", "kLZ4Compression"},
      {"bottommost_compression_opts", "5:6:7:8:10:true"},
      {"compression_opts", "4:5:6:7:8:2:true:100:false"},
      {"num_levels", "8"},
      {"level0_file_num_compaction_trigger", "8"},
      {"level0_slowdown_writes_trigger", "9"},
      {"level0_stop_writes_trigger", "10"},
      {"target_file_size_base", "12"},
      {"target_file_size_multiplier", "13"},
      {"max_bytes_for_level_base", "14"},
      {"level_compaction_dynamic_level_bytes", "true"},
      {"max_bytes_for_level_multiplier", "15.0"},
      {"max_bytes_for_level_multiplier_additional", "16:17:18"},
      {"max_compaction_bytes", "21"},
      {"hard_pending_compaction_bytes_limit", "211"},
      {"arena_block_size", "22"},
      {"disable_auto_compactions", "true"},
      {"compaction_style", "kCompactionStyleLevel"},
      {"compaction_pri", "kOldestSmallestSeqFirst"},
      {"verify_checksums_in_compaction", "false"},
      {"compaction_options_fifo",
       "{allow_compaction=true;max_table_files_size=11002244;"
       "file_temperature_age_thresholds={{temperature=kCold;age=12345}}}"},
      {"max_sequential_skip_in_iterations", "24"},
      {"inplace_update_support", "true"},
      {"report_bg_io_stats", "true"},
      {"compaction_measure_io_stats", "false"},
      {"purge_redundant_kvs_while_flush", "false"},
      {"inplace_update_num_locks", "25"},
      {"memtable_prefix_bloom_size_ratio", "0.26"},
      {"memtable_whole_key_filtering", "true"},
      {"memtable_huge_page_size", "28"},
      {"bloom_locality", "29"},
      {"max_successive_merges", "30"},
      {"strict_max_successive_merges", "true"},
      {"min_partial_merge_operands", "31"},
      {"prefix_extractor", "fixed:31"},
      {"experimental_mempurge_threshold", "0.003"},
      {"optimize_filters_for_hits", "true"},
      {"enable_blob_files", "true"},
      {"min_blob_size", "1K"},
      {"blob_file_size", "1G"},
      {"blob_file_writable_file_max_buffer_size", "128K"},
      {"blob_compression_type", "kZSTD"},
      {"blob_compression_opts", "-14:1:0:0:0:true"},
      {"enable_blob_garbage_collection", "true"},
      {"blob_garbage_collection_age_cutoff", "0.5"},
      {"blob_garbage_collection_force_threshold", "0.75"},
      {"blob_compaction_readahead_size", "256K"},
      {"blob_file_starting_level", "1"},
      {"prepopulate_blob_cache", "kDisable"},
      {"last_level_temperature", "kWarm"},
      {"default_write_temperature", "kCold"},
      {"default_temperature", "kHot"},
      {"persist_user_defined_timestamps", "true"},
      {"memtable_max_range_deletions", "0"},
  };

  std::unordered_map<std::string, std::string> db_options_map = {
      {"create_if_missing", "false"},
      {"create_missing_column_families", "true"},
      {"error_if_exists", "false"},
      {"paranoid_checks", "true"},
      {"track_and_verify_wals_in_manifest", "true"},
      {"track_and_verify_wals", "true"},
      {"verify_sst_unique_id_in_manifest", "true"},
      {"async_wal_precreate", "true"},
      {"fast_sst_open", "true"},
      {"max_open_files", "32"},
      {"max_total_wal_size", "33"},
      {"use_fsync", "true"},
      {"db_log_dir", "/db_log_dir"},
      {"wal_dir", "/wal_dir"},
      {"delete_obsolete_files_period_micros", "34"},
      {"max_background_compactions", "35"},
      {"max_background_flushes", "36"},
      {"max_log_file_size", "37"},
      {"log_file_time_to_roll", "38"},
      {"keep_log_file_num", "39"},
      {"recycle_log_file_num", "5"},
      {"max_manifest_file_size", "40"},
      {"max_manifest_space_amp_pct", "42"},
      {"table_cache_numshardbits", "41"},
      {"WAL_ttl_seconds", "43"},
      {"WAL_size_limit_MB", "44"},
      {"manifest_preallocation_size", "45"},
      {"allow_mmap_reads", "true"},
      {"allow_mmap_writes", "false"},
      {"use_direct_reads", "false"},
      {"use_direct_io_for_compaction_reads", "false"},
      {"use_direct_io_for_flush_and_compaction", "false"},
      {"is_fd_close_on_exec", "true"},
      {"skip_log_error_on_recovery", "false"},
      {"stats_dump_period_sec", "46"},
      {"stats_persist_period_sec", "57"},
      {"persist_stats_to_disk", "false"},
      {"stats_history_buffer_size", "69"},
      {"advise_random_on_open", "true"},
      {"use_adaptive_mutex", "false"},
      {"compaction_readahead_size", "100"},
      {"writable_file_max_buffer_size", "314159"},
      {"bytes_per_sync", "47"},
      {"wal_bytes_per_sync", "48"},
      {"strict_bytes_per_sync", "true"},
      {"preserve_deletes", "false"},
      {"daily_offpeak_time_utc", ""},
  };

  ColumnFamilyOptions base_cf_opt;
  ColumnFamilyOptions new_cf_opt;
  ConfigOptions exact;
  ConfigOptions loose;
  exact.input_strings_escaped = false;
  exact.ignore_unknown_options = false;
  exact.sanity_level = ConfigOptions::kSanityLevelExactMatch;
  loose.sanity_level = ConfigOptions::kSanityLevelLooselyCompatible;

  loose.input_strings_escaped = false;
  loose.ignore_unknown_options = true;
  ASSERT_OK(GetColumnFamilyOptionsFromMap(exact, base_cf_opt, cf_options_map,
                                          &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 1U);
  ASSERT_EQ(new_cf_opt.max_write_buffer_number, 2);
  ASSERT_EQ(new_cf_opt.min_write_buffer_number_to_merge, 3);
  ASSERT_EQ(new_cf_opt.max_write_buffer_size_to_maintain, -99999);
  ASSERT_EQ(new_cf_opt.compression, kSnappyCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level.size(), 8U);
  ASSERT_EQ(new_cf_opt.compression_per_level[0], kNoCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level[1], kSnappyCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level[2], kZlibCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level[3], kBZip2Compression);
  ASSERT_EQ(new_cf_opt.compression_per_level[4], kLZ4Compression);
  ASSERT_EQ(new_cf_opt.compression_per_level[5], kLZ4HCCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level[6], kXpressCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level[7], kZSTD);
  ASSERT_EQ(new_cf_opt.compression_opts.window_bits, 4);
  ASSERT_EQ(new_cf_opt.compression_opts.level, 5);
  ASSERT_EQ(new_cf_opt.compression_opts.strategy, 6);
  ASSERT_EQ(new_cf_opt.compression_opts.max_dict_bytes, 7u);
  ASSERT_EQ(new_cf_opt.compression_opts.zstd_max_train_bytes, 8u);
  ASSERT_EQ(new_cf_opt.compression_opts.parallel_threads, 2u);
  ASSERT_EQ(new_cf_opt.compression_opts.enabled, true);
  ASSERT_EQ(new_cf_opt.compression_opts.max_dict_buffer_bytes, 100u);
  ASSERT_EQ(new_cf_opt.compression_opts.use_zstd_dict_trainer, false);
  ASSERT_EQ(new_cf_opt.bottommost_compression, kLZ4Compression);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.window_bits, 5);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.level, 6);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.strategy, 7);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.max_dict_bytes, 8u);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.zstd_max_train_bytes, 10u);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.parallel_threads,
            CompressionOptions().parallel_threads);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.enabled, true);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.use_zstd_dict_trainer,
            CompressionOptions().use_zstd_dict_trainer);
  ASSERT_EQ(new_cf_opt.num_levels, 8);
  ASSERT_EQ(new_cf_opt.level0_file_num_compaction_trigger, 8);
  ASSERT_EQ(new_cf_opt.level0_slowdown_writes_trigger, 9);
  ASSERT_EQ(new_cf_opt.level0_stop_writes_trigger, 10);
  ASSERT_EQ(new_cf_opt.target_file_size_base, static_cast<uint64_t>(12));
  ASSERT_EQ(new_cf_opt.target_file_size_multiplier, 13);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_base, 14U);
  ASSERT_EQ(new_cf_opt.level_compaction_dynamic_level_bytes, true);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier, 15.0);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier_additional.size(), 3U);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier_additional[0], 16);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier_additional[1], 17);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier_additional[2], 18);
  ASSERT_EQ(new_cf_opt.max_compaction_bytes, 21);
  ASSERT_EQ(new_cf_opt.hard_pending_compaction_bytes_limit, 211);
  ASSERT_EQ(new_cf_opt.arena_block_size, 22U);
  ASSERT_EQ(new_cf_opt.disable_auto_compactions, true);
  ASSERT_EQ(new_cf_opt.compaction_style, kCompactionStyleLevel);
  ASSERT_EQ(new_cf_opt.compaction_pri, kOldestSmallestSeqFirst);
  ASSERT_EQ(new_cf_opt.compaction_options_fifo.max_table_files_size,
            static_cast<uint64_t>(11002244));
  ASSERT_EQ(new_cf_opt.compaction_options_fifo.allow_compaction, true);
  ASSERT_EQ(
      new_cf_opt.compaction_options_fifo.file_temperature_age_thresholds.size(),
      1);
  ASSERT_EQ(
      new_cf_opt.compaction_options_fifo.file_temperature_age_thresholds[0]
          .temperature,
      Temperature::kCold);
  ASSERT_EQ(
      new_cf_opt.compaction_options_fifo.file_temperature_age_thresholds[0].age,
      12345);
  ASSERT_EQ(new_cf_opt.max_sequential_skip_in_iterations,
            static_cast<uint64_t>(24));
  ASSERT_EQ(new_cf_opt.inplace_update_support, true);
  ASSERT_EQ(new_cf_opt.inplace_update_num_locks, 25U);
  ASSERT_EQ(new_cf_opt.memtable_prefix_bloom_size_ratio, 0.26);
  ASSERT_EQ(new_cf_opt.memtable_whole_key_filtering, true);
  ASSERT_EQ(new_cf_opt.memtable_huge_page_size, 28U);
  ASSERT_EQ(new_cf_opt.bloom_locality, 29U);
  ASSERT_EQ(new_cf_opt.max_successive_merges, 30U);
  ASSERT_EQ(new_cf_opt.strict_max_successive_merges, true);
  ASSERT_TRUE(new_cf_opt.prefix_extractor != nullptr);
  ASSERT_EQ(new_cf_opt.optimize_filters_for_hits, true);
  ASSERT_EQ(new_cf_opt.prefix_extractor->AsString(), "rocksdb.FixedPrefix.31");
  ASSERT_EQ(new_cf_opt.experimental_mempurge_threshold, 0.003);
  ASSERT_EQ(new_cf_opt.enable_blob_files, true);
  ASSERT_EQ(new_cf_opt.min_blob_size, 1ULL << 10);
  ASSERT_EQ(new_cf_opt.blob_file_size, 1ULL << 30);
  ASSERT_EQ(new_cf_opt.blob_file_writable_file_max_buffer_size, 128ULL << 10);
  ASSERT_EQ(new_cf_opt.blob_compression_type, kZSTD);
  ASSERT_EQ(new_cf_opt.blob_compression_opts.level, 1);
  ASSERT_EQ(new_cf_opt.enable_blob_garbage_collection, true);
  ASSERT_EQ(new_cf_opt.blob_garbage_collection_age_cutoff, 0.5);
  ASSERT_EQ(new_cf_opt.blob_garbage_collection_force_threshold, 0.75);
  ASSERT_EQ(new_cf_opt.blob_compaction_readahead_size, 262144);
  ASSERT_EQ(new_cf_opt.blob_file_starting_level, 1);
  ASSERT_EQ(new_cf_opt.prepopulate_blob_cache, PrepopulateBlobCache::kDisable);
  ASSERT_EQ(new_cf_opt.last_level_temperature, Temperature::kWarm);
  ASSERT_EQ(new_cf_opt.default_write_temperature, Temperature::kCold);
  ASSERT_EQ(new_cf_opt.default_temperature, Temperature::kHot);
  ASSERT_EQ(new_cf_opt.persist_user_defined_timestamps, true);
  ASSERT_EQ(new_cf_opt.memtable_max_range_deletions, 0);

  cf_options_map["write_buffer_size"] = "hello";
  ASSERT_NOK(GetColumnFamilyOptionsFromMap(exact, base_cf_opt, cf_options_map,
                                           &new_cf_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  cf_options_map["write_buffer_size"] = "1";
  ASSERT_OK(GetColumnFamilyOptionsFromMap(exact, base_cf_opt, cf_options_map,
                                          &new_cf_opt));

  cf_options_map["unknown_option"] = "1";
  ASSERT_NOK(GetColumnFamilyOptionsFromMap(exact, base_cf_opt, cf_options_map,
                                           &new_cf_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  // ignore_unknown_options=true;input_strings_escaped=false
  ASSERT_OK(GetColumnFamilyOptionsFromMap(loose, base_cf_opt, cf_options_map,
                                          &new_cf_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyCFOptions(loose, base_cf_opt, new_cf_opt));
  ASSERT_NOK(
      RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  DBOptions base_db_opt;
  DBOptions new_db_opt;
  ASSERT_OK(
      GetDBOptionsFromMap(exact, base_db_opt, db_options_map, &new_db_opt));
  ASSERT_EQ(new_db_opt.create_if_missing, false);
  ASSERT_EQ(new_db_opt.create_missing_column_families, true);
  ASSERT_EQ(new_db_opt.error_if_exists, false);
  ASSERT_EQ(new_db_opt.paranoid_checks, true);
  ASSERT_EQ(new_db_opt.track_and_verify_wals_in_manifest, true);
  ASSERT_EQ(new_db_opt.track_and_verify_wals, true);
  ASSERT_EQ(new_db_opt.verify_sst_unique_id_in_manifest, true);
  ASSERT_EQ(new_db_opt.async_wal_precreate, true);
  ASSERT_EQ(new_db_opt.fast_sst_open, true);
  ASSERT_EQ(new_db_opt.max_open_files, 32);
  ASSERT_EQ(new_db_opt.max_total_wal_size, static_cast<uint64_t>(33));
  ASSERT_EQ(new_db_opt.use_fsync, true);
  ASSERT_EQ(new_db_opt.db_log_dir, "/db_log_dir");
  ASSERT_EQ(new_db_opt.wal_dir, "/wal_dir");
  ASSERT_EQ(new_db_opt.delete_obsolete_files_period_micros,
            static_cast<uint64_t>(34));
  ASSERT_EQ(new_db_opt.max_background_compactions, 35);
  ASSERT_EQ(new_db_opt.max_background_flushes, 36);
  ASSERT_EQ(new_db_opt.max_log_file_size, 37U);
  ASSERT_EQ(new_db_opt.log_file_time_to_roll, 38U);
  ASSERT_EQ(new_db_opt.keep_log_file_num, 39U);
  ASSERT_EQ(new_db_opt.recycle_log_file_num, 5U);
  ASSERT_EQ(new_db_opt.max_manifest_file_size, uint64_t{40});
  ASSERT_EQ(new_db_opt.max_manifest_space_amp_pct, 42);
  ASSERT_EQ(new_db_opt.table_cache_numshardbits, 41);
  ASSERT_EQ(new_db_opt.WAL_ttl_seconds, static_cast<uint64_t>(43));
  ASSERT_EQ(new_db_opt.WAL_size_limit_MB, static_cast<uint64_t>(44));
  ASSERT_EQ(new_db_opt.manifest_preallocation_size, 45U);
  ASSERT_EQ(new_db_opt.allow_mmap_reads, true);
  ASSERT_EQ(new_db_opt.allow_mmap_writes, false);
  ASSERT_EQ(new_db_opt.use_direct_reads, false);
  ASSERT_EQ(new_db_opt.use_direct_io_for_compaction_reads, false);
  ASSERT_EQ(new_db_opt.use_direct_io_for_flush_and_compaction, false);
  ASSERT_EQ(new_db_opt.is_fd_close_on_exec, true);
  ASSERT_EQ(new_db_opt.stats_dump_period_sec, 46U);
  ASSERT_EQ(new_db_opt.stats_persist_period_sec, 57U);
  ASSERT_EQ(new_db_opt.persist_stats_to_disk, false);
  ASSERT_EQ(new_db_opt.stats_history_buffer_size, 69U);
  ASSERT_EQ(new_db_opt.advise_random_on_open, true);
  ASSERT_EQ(new_db_opt.use_adaptive_mutex, false);
  ASSERT_EQ(new_db_opt.compaction_readahead_size, 100);
  ASSERT_EQ(new_db_opt.writable_file_max_buffer_size, 314159);
  ASSERT_EQ(new_db_opt.bytes_per_sync, static_cast<uint64_t>(47));
  ASSERT_EQ(new_db_opt.wal_bytes_per_sync, static_cast<uint64_t>(48));
  ASSERT_EQ(new_db_opt.strict_bytes_per_sync, true);
  ASSERT_EQ(new_db_opt.daily_offpeak_time_utc, "");

  db_options_map["max_open_files"] = "hello";
  Status s =
      GetDBOptionsFromMap(exact, base_db_opt, db_options_map, &new_db_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());

  ASSERT_OK(
      RocksDBOptionsParser::VerifyDBOptions(exact, base_db_opt, new_db_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyDBOptions(loose, base_db_opt, new_db_opt));

  // unknow options should fail parsing without ignore_unknown_options = true
  db_options_map["unknown_db_option"] = "1";
  s = GetDBOptionsFromMap(exact, base_db_opt, db_options_map, &new_db_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_OK(
      RocksDBOptionsParser::VerifyDBOptions(exact, base_db_opt, new_db_opt));

  ASSERT_OK(
      GetDBOptionsFromMap(loose, base_db_opt, db_options_map, &new_db_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyDBOptions(loose, base_db_opt, new_db_opt));
  ASSERT_NOK(
      RocksDBOptionsParser::VerifyDBOptions(exact, base_db_opt, new_db_opt));
}

TEST_F(OptionsTest, GetColumnFamilyOptionsFromStringTest) {
  ColumnFamilyOptions base_cf_opt;
  ColumnFamilyOptions new_cf_opt;
  ConfigOptions config_options;
  config_options.input_strings_escaped = false;
  config_options.ignore_unknown_options = false;

  base_cf_opt.table_factory.reset();
  ASSERT_OK(GetColumnFamilyOptionsFromString(config_options, base_cf_opt, "",
                                             &new_cf_opt));
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt, "write_buffer_size=5", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 5U);
  ASSERT_TRUE(new_cf_opt.table_factory == nullptr);
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt, "write_buffer_size=6;", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 6U);
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "memtable_verify_per_key_checksum_on_seek=true", &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.memtable_verify_per_key_checksum_on_seek);
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt, "  write_buffer_size =  7  ", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 7U);
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt, "  write_buffer_size =  8 ; ", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 8U);
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=9;max_write_buffer_number=10", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 9U);
  ASSERT_EQ(new_cf_opt.max_write_buffer_number, 10);
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=11; max_write_buffer_number  =  12 ;", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 11U);
  ASSERT_EQ(new_cf_opt.max_write_buffer_number, 12);
  // Wrong name "max_write_buffer_number_"
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=13;max_write_buffer_number_=14;", &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_cf_opt,
                                                  new_cf_opt));

  // Comparator from object registry
  std::string kCompName = "reverse_comp";
  ObjectLibrary::Default()->AddFactory<const Comparator>(
      kCompName,
      [](const std::string& /*name*/,
         std::unique_ptr<const Comparator>* /*guard*/,
         std::string* /* errmsg */) { return ReverseBytewiseComparator(); });

  ASSERT_OK(GetColumnFamilyOptionsFromString(config_options, base_cf_opt,
                                             "comparator=" + kCompName + ";",
                                             &new_cf_opt));
  ASSERT_EQ(new_cf_opt.comparator, ReverseBytewiseComparator());

  // MergeOperator from object registry
  std::unique_ptr<BytesXOROperator> bxo(new BytesXOROperator());
  std::string kMoName = bxo->Name();

  ASSERT_OK(GetColumnFamilyOptionsFromString(config_options, base_cf_opt,
                                             "merge_operator=" + kMoName + ";",
                                             &new_cf_opt));
  ASSERT_EQ(kMoName, std::string(new_cf_opt.merge_operator->Name()));

  // Wrong key/value pair
  Status s = GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=13;max_write_buffer_number;", &new_cf_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());

  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_cf_opt,
                                                  new_cf_opt));

  // Error Parsing value
  s = GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=13;max_write_buffer_number=;", &new_cf_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_cf_opt,
                                                  new_cf_opt));

  // Missing option name
  s = GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt, "write_buffer_size=13; =100;", &new_cf_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_cf_opt,
                                                  new_cf_opt));

  const uint64_t kilo = 1024UL;
  const uint64_t mega = 1024 * kilo;
  const uint64_t giga = 1024 * mega;
  const uint64_t tera = 1024 * giga;

  // Units (k)
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt, "max_write_buffer_number=15K", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.max_write_buffer_number, 15 * kilo);
  // Units (m)
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "max_write_buffer_number=16m;inplace_update_num_locks=17M", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.max_write_buffer_number, 16 * mega);
  ASSERT_EQ(new_cf_opt.inplace_update_num_locks, 17u * mega);
  // Units (g)
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=18g;prefix_extractor=capped:8;"
      "arena_block_size=19G",
      &new_cf_opt));

  ASSERT_EQ(new_cf_opt.write_buffer_size, 18 * giga);
  ASSERT_EQ(new_cf_opt.arena_block_size, 19 * giga);
  ASSERT_TRUE(new_cf_opt.prefix_extractor.get() != nullptr);
  ASSERT_EQ(new_cf_opt.prefix_extractor->AsString(), "rocksdb.CappedPrefix.8");

  // Units (t)
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt, "write_buffer_size=20t;arena_block_size=21T",
      &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 20 * tera);
  ASSERT_EQ(new_cf_opt.arena_block_size, 21 * tera);

  // Nested block based table options
  // Empty
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={};arena_block_size=1024",
      &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.table_factory != nullptr);
  // Non-empty
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={block_cache=1M;block_size=4;};"
      "arena_block_size=1024",
      &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.table_factory != nullptr);
  // Last one
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={block_cache=1M;block_size=4;}",
      &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.table_factory != nullptr);
  // Mismatch curly braces
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={{{block_size=4;};"
      "arena_block_size=1024",
      &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_cf_opt,
                                                  new_cf_opt));

  // Unexpected chars after closing curly brace
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={block_size=4;}};"
      "arena_block_size=1024",
      &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_cf_opt,
                                                  new_cf_opt));

  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={block_size=4;}xdfa;"
      "arena_block_size=1024",
      &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_cf_opt,
                                                  new_cf_opt));

  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={block_size=4;}xdfa",
      &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_cf_opt,
                                                  new_cf_opt));

  // Invalid block based table option
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={xx_block_size=4;}",
      &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_cf_opt,
                                                  new_cf_opt));

  ASSERT_OK(GetColumnFamilyOptionsFromString(config_options, base_cf_opt,
                                             "optimize_filters_for_hits=true",
                                             &new_cf_opt));
  ASSERT_OK(GetColumnFamilyOptionsFromString(config_options, base_cf_opt,
                                             "optimize_filters_for_hits=false",
                                             &new_cf_opt));

  ASSERT_NOK(GetColumnFamilyOptionsFromString(config_options, base_cf_opt,
                                              "optimize_filters_for_hits=junk",
                                              &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_cf_opt,
                                                  new_cf_opt));

  // Nested plain table options
  // Empty
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "plain_table_factory={};arena_block_size=1024",
      &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.table_factory != nullptr);
  ASSERT_EQ(std::string(new_cf_opt.table_factory->Name()), "PlainTable");
  // Non-empty
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "plain_table_factory={user_key_len=66;bloom_bits_per_key=20;};"
      "arena_block_size=1024",
      &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.table_factory != nullptr);
  ASSERT_EQ(std::string(new_cf_opt.table_factory->Name()), "PlainTable");

  // memtable factory
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "memtable=skip_list:10;arena_block_size=1024",
      &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.memtable_factory != nullptr);
  ASSERT_EQ(std::string(new_cf_opt.memtable_factory->Name()),
            "SkipListFactory");
  ASSERT_TRUE(new_cf_opt.memtable_factory->IsInstanceOf("SkipListFactory"));

  // blob cache
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "blob_cache={capacity=1M;num_shard_bits=4;"
      "strict_capacity_limit=true;high_pri_pool_ratio=0.5;};",
      &new_cf_opt));
  ASSERT_NE(new_cf_opt.blob_cache, nullptr);
  ASSERT_EQ(new_cf_opt.blob_cache->GetCapacity(), 1024UL * 1024UL);
  ASSERT_EQ(static_cast<ShardedCacheBase*>(new_cf_opt.blob_cache.get())
                ->GetNumShardBits(),
            4);
  ASSERT_EQ(new_cf_opt.blob_cache->HasStrictCapacityLimit(), true);
  ASSERT_EQ(static_cast<LRUCache*>(new_cf_opt.blob_cache.get())
                ->GetHighPriPoolRatio(),
            0.5);
}

TEST_F(OptionsTest, CompressionOptionsFromString) {
  ColumnFamilyOptions base_cf_opt;
  ColumnFamilyOptions new_cf_opt;
  ConfigOptions config_options;
  std::string opts_str;
  config_options.ignore_unknown_options = false;
  CompressionOptions dflt;
  // Test with some optional values removed....
  ASSERT_OK(
      GetColumnFamilyOptionsFromString(config_options, ColumnFamilyOptions(),
                                       "compression_opts=3:4:5; "
                                       "bottommost_compression_opts=4:5:6:7",
                                       &base_cf_opt));
  ASSERT_EQ(base_cf_opt.compression_opts.window_bits, 3);
  ASSERT_EQ(base_cf_opt.compression_opts.level, 4);
  ASSERT_EQ(base_cf_opt.compression_opts.strategy, 5);
  ASSERT_EQ(base_cf_opt.compression_opts.max_dict_bytes, dflt.max_dict_bytes);
  ASSERT_EQ(base_cf_opt.compression_opts.zstd_max_train_bytes,
            dflt.zstd_max_train_bytes);
  ASSERT_EQ(base_cf_opt.compression_opts.parallel_threads,
            dflt.parallel_threads);
  ASSERT_EQ(base_cf_opt.compression_opts.enabled, dflt.enabled);
  ASSERT_EQ(base_cf_opt.compression_opts.use_zstd_dict_trainer,
            dflt.use_zstd_dict_trainer);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.window_bits, 4);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.level, 5);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.strategy, 6);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.max_dict_bytes, 7u);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.zstd_max_train_bytes,
            dflt.zstd_max_train_bytes);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.parallel_threads,
            dflt.parallel_threads);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.enabled, dflt.enabled);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.use_zstd_dict_trainer,
            dflt.use_zstd_dict_trainer);

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, ColumnFamilyOptions(),
      "compression_opts=4:5:6:7:8:9:true:10:false; "
      "bottommost_compression_opts=5:6:7:8:9:false",
      &base_cf_opt));
  ASSERT_EQ(base_cf_opt.compression_opts.window_bits, 4);
  ASSERT_EQ(base_cf_opt.compression_opts.level, 5);
  ASSERT_EQ(base_cf_opt.compression_opts.strategy, 6);
  ASSERT_EQ(base_cf_opt.compression_opts.max_dict_bytes, 7u);
  ASSERT_EQ(base_cf_opt.compression_opts.zstd_max_train_bytes, 8u);
  ASSERT_EQ(base_cf_opt.compression_opts.parallel_threads, 9u);
  ASSERT_EQ(base_cf_opt.compression_opts.enabled, true);
  ASSERT_EQ(base_cf_opt.compression_opts.max_dict_buffer_bytes, 10u);
  ASSERT_EQ(base_cf_opt.compression_opts.use_zstd_dict_trainer, false);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.window_bits, 5);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.level, 6);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.strategy, 7);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.max_dict_bytes, 8u);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.zstd_max_train_bytes, 9u);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.parallel_threads,
            dflt.parallel_threads);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.enabled, false);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.use_zstd_dict_trainer,
            dflt.use_zstd_dict_trainer);

  ASSERT_OK(
      GetStringFromColumnFamilyOptions(config_options, base_cf_opt, &opts_str));
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, ColumnFamilyOptions(), opts_str, &new_cf_opt));
  ASSERT_EQ(new_cf_opt.compression_opts.window_bits, 4);
  ASSERT_EQ(new_cf_opt.compression_opts.level, 5);
  ASSERT_EQ(new_cf_opt.compression_opts.strategy, 6);
  ASSERT_EQ(new_cf_opt.compression_opts.max_dict_bytes, 7u);
  ASSERT_EQ(new_cf_opt.compression_opts.zstd_max_train_bytes, 8u);
  ASSERT_EQ(new_cf_opt.compression_opts.parallel_threads, 9u);
  ASSERT_EQ(new_cf_opt.compression_opts.enabled, true);
  ASSERT_EQ(base_cf_opt.compression_opts.max_dict_buffer_bytes, 10u);
  ASSERT_EQ(base_cf_opt.compression_opts.use_zstd_dict_trainer, false);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.window_bits, 5);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.level, 6);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.strategy, 7);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.max_dict_bytes, 8u);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.zstd_max_train_bytes, 9u);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.parallel_threads,
            dflt.parallel_threads);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.enabled, false);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.use_zstd_dict_trainer,
            dflt.use_zstd_dict_trainer);

  // Test as struct values
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, ColumnFamilyOptions(),
      "compression_opts={window_bits=5; level=6; strategy=7; max_dict_bytes=8;"
      "zstd_max_train_bytes=9;parallel_threads=10;enabled=true;use_zstd_dict_"
      "trainer=false}; "
      "bottommost_compression_opts={window_bits=4; level=5; strategy=6;"
      " max_dict_bytes=7;zstd_max_train_bytes=8;parallel_threads=9;"
      "enabled=false;use_zstd_dict_trainer=true}; ",
      &new_cf_opt));
  ASSERT_EQ(new_cf_opt.compression_opts.window_bits, 5);
  ASSERT_EQ(new_cf_opt.compression_opts.level, 6);
  ASSERT_EQ(new_cf_opt.compression_opts.strategy, 7);
  ASSERT_EQ(new_cf_opt.compression_opts.max_dict_bytes, 8u);
  ASSERT_EQ(new_cf_opt.compression_opts.zstd_max_train_bytes, 9u);
  ASSERT_EQ(new_cf_opt.compression_opts.parallel_threads, 10u);
  ASSERT_EQ(new_cf_opt.compression_opts.enabled, true);
  ASSERT_EQ(new_cf_opt.compression_opts.use_zstd_dict_trainer, false);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.window_bits, 4);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.level, 5);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.strategy, 6);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.max_dict_bytes, 7u);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.zstd_max_train_bytes, 8u);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.parallel_threads, 9u);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.enabled, false);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.use_zstd_dict_trainer, true);

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "compression_opts={window_bits=4; strategy=5;};"
      "bottommost_compression_opts={level=6; strategy=7;}",
      &new_cf_opt));
  ASSERT_EQ(new_cf_opt.compression_opts.window_bits, 4);
  ASSERT_EQ(new_cf_opt.compression_opts.strategy, 5);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.level, 6);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.strategy, 7);

  ASSERT_EQ(new_cf_opt.compression_opts.level,
            base_cf_opt.compression_opts.level);
  ASSERT_EQ(new_cf_opt.compression_opts.max_dict_bytes,
            base_cf_opt.compression_opts.max_dict_bytes);
  ASSERT_EQ(new_cf_opt.compression_opts.zstd_max_train_bytes,
            base_cf_opt.compression_opts.zstd_max_train_bytes);
  ASSERT_EQ(new_cf_opt.compression_opts.parallel_threads,
            base_cf_opt.compression_opts.parallel_threads);
  ASSERT_EQ(new_cf_opt.compression_opts.enabled,
            base_cf_opt.compression_opts.enabled);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.window_bits,
            base_cf_opt.bottommost_compression_opts.window_bits);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.max_dict_bytes,
            base_cf_opt.bottommost_compression_opts.max_dict_bytes);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.zstd_max_train_bytes,
            base_cf_opt.bottommost_compression_opts.zstd_max_train_bytes);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.parallel_threads,
            base_cf_opt.bottommost_compression_opts.parallel_threads);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.enabled,
            base_cf_opt.bottommost_compression_opts.enabled);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.use_zstd_dict_trainer,
            base_cf_opt.bottommost_compression_opts.use_zstd_dict_trainer);

  // Test a few individual struct values
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "compression_opts.enabled=false; "
      "bottommost_compression_opts.enabled=true; ",
      &new_cf_opt));
  ASSERT_EQ(new_cf_opt.compression_opts.enabled, false);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.enabled, true);

  // Now test some illegal values
  ConfigOptions ignore;
  ignore.ignore_unknown_options = true;
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, ColumnFamilyOptions(),
      "compression_opts=5:6:7:8:9:x:false", &base_cf_opt));
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      ignore, ColumnFamilyOptions(), "compression_opts=5:6:7:8:9:x:false",
      &base_cf_opt));
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, ColumnFamilyOptions(),
      "compression_opts=1:2:3:4:5:6:true:8", &base_cf_opt));
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      ignore, ColumnFamilyOptions(), "compression_opts=1:2:3:4:5:6:true:8",
      &base_cf_opt));
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, ColumnFamilyOptions(),
      "compression_opts=1:2:3:4:5:6:true:8:9", &base_cf_opt));
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      ignore, ColumnFamilyOptions(), "compression_opts=1:2:3:4:5:6:true:8:9",
      &base_cf_opt));
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, ColumnFamilyOptions(), "compression_opts={unknown=bad;}",
      &base_cf_opt));
  ASSERT_OK(GetColumnFamilyOptionsFromString(ignore, ColumnFamilyOptions(),
                                             "compression_opts={unknown=bad;}",
                                             &base_cf_opt));
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, ColumnFamilyOptions(), "compression_opts.unknown=bad",
      &base_cf_opt));
  ASSERT_OK(GetColumnFamilyOptionsFromString(ignore, ColumnFamilyOptions(),
                                             "compression_opts.unknown=bad",
                                             &base_cf_opt));

  // Test with some additional values added
  config_options.ignore_unknown_options = false;
  ASSERT_NOK(
      GetColumnFamilyOptionsFromString(config_options, ColumnFamilyOptions(),
                                       "new_unknown_field=whatever; "
                                       "bottommost_compression_opts=4:5:6:7",
                                       &base_cf_opt));
  // Test Ignoring Unknown Options
  config_options.ignore_unknown_options = true;
  ASSERT_OK(
      GetColumnFamilyOptionsFromString(config_options, ColumnFamilyOptions(),
                                       "new_unknown_field=whatever; "
                                       "bottommost_compression_opts=4:5:6:7",
                                       &base_cf_opt));
}

TEST_F(OptionsTest, OldInterfaceTest) {
  ColumnFamilyOptions base_cf_opt;
  ColumnFamilyOptions new_cf_opt;
  ConfigOptions exact;
  ConfigOptions cf_config_options;
  cf_config_options.input_strings_escaped = false;
  cf_config_options.ignore_unknown_options = false;
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      cf_config_options, base_cf_opt,
      "write_buffer_size=18;prefix_extractor=capped:8;"
      "arena_block_size=19",
      &new_cf_opt));

  ASSERT_EQ(new_cf_opt.write_buffer_size, 18);
  ASSERT_EQ(new_cf_opt.arena_block_size, 19);
  ASSERT_TRUE(new_cf_opt.prefix_extractor.get() != nullptr);

  // And with a bad option
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      cf_config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={xx_block_size=4;}",
      &new_cf_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  std::unordered_map<std::string, std::string> cf_options_map = {
      {"write_buffer_size", "1"},
      {"max_write_buffer_number", "2"},
      {"min_write_buffer_number_to_merge", "3"},
  };
  ASSERT_OK(GetColumnFamilyOptionsFromMap(cf_config_options, base_cf_opt,
                                          cf_options_map, &new_cf_opt));
  cf_options_map["unknown_option"] = "1";
  ASSERT_NOK(GetColumnFamilyOptionsFromMap(cf_config_options, base_cf_opt,
                                           cf_options_map, &new_cf_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));
  cf_config_options.input_strings_escaped = true;
  cf_config_options.ignore_unknown_options = true;
  ASSERT_OK(GetColumnFamilyOptionsFromMap(cf_config_options, base_cf_opt,
                                          cf_options_map, &new_cf_opt));

  DBOptions base_db_opt;
  DBOptions new_db_opt;
  std::unordered_map<std::string, std::string> db_options_map = {
      {"create_if_missing", "false"},
      {"create_missing_column_families", "true"},
      {"error_if_exists", "false"},
      {"paranoid_checks", "true"},
      {"track_and_verify_wals_in_manifest", "true"},
      {"track_and_verify_wals", "true"},
      {"verify_sst_unique_id_in_manifest", "true"},
      {"async_wal_precreate", "true"},
      {"fast_sst_open", "true"},
      {"max_open_files", "32"},
      {"daily_offpeak_time_utc", "06:30-23:30"},
  };

  ConfigOptions db_config_options(base_db_opt);
  db_config_options.input_strings_escaped = false;
  db_config_options.ignore_unknown_options = false;
  ASSERT_OK(GetDBOptionsFromMap(db_config_options, base_db_opt, db_options_map,
                                &new_db_opt));
  ASSERT_EQ(new_db_opt.create_if_missing, false);
  ASSERT_EQ(new_db_opt.create_missing_column_families, true);
  ASSERT_EQ(new_db_opt.error_if_exists, false);
  ASSERT_EQ(new_db_opt.paranoid_checks, true);
  ASSERT_EQ(new_db_opt.track_and_verify_wals_in_manifest, true);
  ASSERT_EQ(new_db_opt.track_and_verify_wals, true);
  ASSERT_EQ(new_db_opt.verify_sst_unique_id_in_manifest, true);
  ASSERT_EQ(new_db_opt.async_wal_precreate, true);
  ASSERT_EQ(new_db_opt.fast_sst_open, true);
  ASSERT_EQ(new_db_opt.max_open_files, 32);
  db_options_map["unknown_option"] = "1";
  Status s = GetDBOptionsFromMap(db_config_options, base_db_opt, db_options_map,
                                 &new_db_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());

  ASSERT_OK(
      RocksDBOptionsParser::VerifyDBOptions(exact, base_db_opt, new_db_opt));
  db_config_options.input_strings_escaped = true;
  db_config_options.ignore_unknown_options = true;
  ASSERT_OK(GetDBOptionsFromMap(db_config_options, base_db_opt, db_options_map,
                                &new_db_opt));
  db_config_options.input_strings_escaped = false;
  db_config_options.ignore_unknown_options = false;
  ASSERT_OK(GetDBOptionsFromString(
      db_config_options, base_db_opt,
      "create_if_missing=false;error_if_exists=false;max_open_files=42;"
      "daily_offpeak_time_utc=08:30-19:00;",
      &new_db_opt));
  ASSERT_EQ(new_db_opt.create_if_missing, false);
  ASSERT_EQ(new_db_opt.error_if_exists, false);
  ASSERT_EQ(new_db_opt.max_open_files, 42);
  ASSERT_EQ(new_db_opt.daily_offpeak_time_utc, "08:30-19:00");
  s = GetDBOptionsFromString(
      db_config_options, base_db_opt,
      "create_if_missing=false;error_if_exists=false;max_open_files=42;"
      "unknown_option=1;",
      &new_db_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_OK(
      RocksDBOptionsParser::VerifyDBOptions(exact, base_db_opt, new_db_opt));
}

TEST_F(OptionsTest, GetBlockBasedTableOptionsFromString) {
  BlockBasedTableOptions table_opt;
  BlockBasedTableOptions new_opt;
  ConfigOptions config_options;
  config_options.input_strings_escaped = false;
  config_options.ignore_unknown_options = false;
  config_options.ignore_unsupported_options = false;

  // make sure default values are overwritten by something else
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "cache_index_and_filter_blocks=1;index_type=kHashSearch;"
      "checksum=kxxHash;"
      "block_cache=1M;block_cache_compressed=1k;block_size=1024;"
      "block_size_deviation=8;block_restart_interval=4;"
      "format_version=5;whole_key_filtering=1;"
      "filter_policy=bloomfilter:4.567:false;detect_filter_construct_"
      "corruption=true;"
      // A bug caused read_amp_bytes_per_bit to be a large integer in OPTIONS
      // file generated by 6.10 to 6.14. Though bug is fixed in these releases,
      // we need to handle the case of loading OPTIONS file generated before the
      // fix.
      "read_amp_bytes_per_bit=17179869185;",
      &new_opt));
  ASSERT_TRUE(new_opt.cache_index_and_filter_blocks);
  ASSERT_EQ(new_opt.index_type, BlockBasedTableOptions::kHashSearch);
  ASSERT_EQ(new_opt.checksum, ChecksumType::kxxHash);
  ASSERT_TRUE(new_opt.block_cache != nullptr);
  ASSERT_EQ(new_opt.block_cache->GetCapacity(), 1024UL * 1024UL);
  ASSERT_EQ(new_opt.block_size, 1024UL);
  ASSERT_EQ(new_opt.block_size_deviation, 8);
  ASSERT_EQ(new_opt.block_restart_interval, 4);
  ASSERT_EQ(new_opt.format_version, 5U);
  ASSERT_EQ(new_opt.whole_key_filtering, true);
  ASSERT_EQ(new_opt.detect_filter_construct_corruption, true);
  ASSERT_TRUE(new_opt.filter_policy != nullptr);
  auto bfp = new_opt.filter_policy->CheckedCast<BloomFilterPolicy>();
  ASSERT_NE(bfp, nullptr);
  EXPECT_EQ(bfp->GetMillibitsPerKey(), 4567);
  EXPECT_EQ(bfp->GetWholeBitsPerKey(), 5);
  // Verify that only the lower 32bits are stored in
  // new_opt.read_amp_bytes_per_bit.
  EXPECT_EQ(1U, new_opt.read_amp_bytes_per_bit);

  // unknown option
  Status s = GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "cache_index_and_filter_blocks=1;index_type=kBinarySearch;"
      "bad_option=1",
      &new_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_EQ(static_cast<bool>(table_opt.cache_index_and_filter_blocks),
            new_opt.cache_index_and_filter_blocks);
  ASSERT_EQ(table_opt.index_type, new_opt.index_type);

  // unrecognized index type
  s = GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "cache_index_and_filter_blocks=1;index_type=kBinarySearchXX", &new_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_EQ(table_opt.cache_index_and_filter_blocks,
            new_opt.cache_index_and_filter_blocks);
  ASSERT_EQ(table_opt.index_type, new_opt.index_type);

  // unrecognized checksum type
  ASSERT_NOK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "cache_index_and_filter_blocks=1;checksum=kxxHashXX", &new_opt));
  ASSERT_EQ(table_opt.cache_index_and_filter_blocks,
            new_opt.cache_index_and_filter_blocks);
  ASSERT_EQ(table_opt.index_type, new_opt.index_type);

  // unrecognized filter policy name
  s = GetBlockBasedTableOptionsFromString(config_options, table_opt,
                                          "filter_policy=bloomfilterxx:4:true",
                                          &new_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());

  // missing bits per key
  s = GetBlockBasedTableOptionsFromString(
      config_options, table_opt, "filter_policy=bloomfilter", &new_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());

  // Used to be rejected, now accepted
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt, "filter_policy=bloomfilter:4", &new_opt));
  bfp = dynamic_cast<const BloomFilterPolicy*>(new_opt.filter_policy.get());
  EXPECT_EQ(bfp->GetMillibitsPerKey(), 4000);
  EXPECT_EQ(bfp->GetWholeBitsPerKey(), 4);

  // use_block_based_builder=true now ignored in public API (same as false)
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt, "filter_policy=bloomfilter:4:true", &new_opt));
  bfp = dynamic_cast<const BloomFilterPolicy*>(new_opt.filter_policy.get());
  EXPECT_EQ(bfp->GetMillibitsPerKey(), 4000);
  EXPECT_EQ(bfp->GetWholeBitsPerKey(), 4);

  // Test configuring using other internal names
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "filter_policy=rocksdb.internal.LegacyBloomFilter:3", &new_opt));
  auto builtin =
      dynamic_cast<const BuiltinFilterPolicy*>(new_opt.filter_policy.get());
  EXPECT_EQ(builtin->GetId(), "rocksdb.internal.LegacyBloomFilter:3");

  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "filter_policy=rocksdb.internal.FastLocalBloomFilter:1.234", &new_opt));
  builtin =
      dynamic_cast<const BuiltinFilterPolicy*>(new_opt.filter_policy.get());
  EXPECT_EQ(builtin->GetId(), "rocksdb.internal.FastLocalBloomFilter:1.234");

  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "filter_policy=rocksdb.internal.Standard128RibbonFilter:1.234",
      &new_opt));
  builtin =
      dynamic_cast<const BuiltinFilterPolicy*>(new_opt.filter_policy.get());
  EXPECT_EQ(builtin->GetId(), "rocksdb.internal.Standard128RibbonFilter:1.234");

  // Ribbon filter policy (no Bloom hybrid)
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt, "filter_policy=ribbonfilter:5.678:-1;",
      &new_opt));
  ASSERT_TRUE(new_opt.filter_policy != nullptr);
  auto rfp =
      dynamic_cast<const RibbonFilterPolicy*>(new_opt.filter_policy.get());
  EXPECT_EQ(rfp->GetMillibitsPerKey(), 5678);
  EXPECT_EQ(rfp->GetBloomBeforeLevel(), -1);

  // Ribbon filter policy (default Bloom hybrid)
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt, "filter_policy=ribbonfilter:6.789;",
      &new_opt));
  ASSERT_TRUE(new_opt.filter_policy != nullptr);
  rfp = dynamic_cast<const RibbonFilterPolicy*>(new_opt.filter_policy.get());
  EXPECT_EQ(rfp->GetMillibitsPerKey(), 6789);
  EXPECT_EQ(rfp->GetBloomBeforeLevel(), 0);

  // Ribbon filter policy (custom Bloom hybrid)
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt, "filter_policy=ribbonfilter:6.789:5;",
      &new_opt));
  ASSERT_TRUE(new_opt.filter_policy != nullptr);
  rfp = dynamic_cast<const RibbonFilterPolicy*>(new_opt.filter_policy.get());
  EXPECT_EQ(rfp->GetMillibitsPerKey(), 6789);
  EXPECT_EQ(rfp->GetBloomBeforeLevel(), 5);

  // Check block cache options are overwritten when specified
  // in new format as a struct.
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "block_cache={capacity=1M;num_shard_bits=4;"
      "strict_capacity_limit=true;high_pri_pool_ratio=0.5;};"
      "block_cache_compressed={capacity=1M;num_shard_bits=4;"
      "strict_capacity_limit=true;high_pri_pool_ratio=0.5;}",
      &new_opt));
  ASSERT_TRUE(new_opt.block_cache != nullptr);
  ASSERT_EQ(new_opt.block_cache->GetCapacity(), 1024UL * 1024UL);
  ASSERT_EQ(std::dynamic_pointer_cast<ShardedCacheBase>(new_opt.block_cache)
                ->GetNumShardBits(),
            4);
  ASSERT_EQ(new_opt.block_cache->HasStrictCapacityLimit(), true);
  ASSERT_EQ(std::dynamic_pointer_cast<LRUCache>(new_opt.block_cache)
                ->GetHighPriPoolRatio(),
            0.5);

  // Set only block cache capacity. Check other values are
  // reset to default values.
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "block_cache={capacity=2M};"
      "block_cache_compressed={capacity=2M}",
      &new_opt));
  ASSERT_TRUE(new_opt.block_cache != nullptr);
  ASSERT_EQ(new_opt.block_cache->GetCapacity(), 2 * 1024UL * 1024UL);
  // Default values
  ASSERT_EQ(std::dynamic_pointer_cast<ShardedCacheBase>(new_opt.block_cache)
                ->GetNumShardBits(),
            GetDefaultCacheShardBits(new_opt.block_cache->GetCapacity()));
  ASSERT_EQ(new_opt.block_cache->HasStrictCapacityLimit(), false);
  ASSERT_EQ(std::dynamic_pointer_cast<LRUCache>(new_opt.block_cache)
                ->GetHighPriPoolRatio(),
            0.5);

  // Set couple of block cache options.
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "block_cache={num_shard_bits=5;high_pri_pool_ratio=0.5;};"
      "block_cache_compressed={num_shard_bits=5;"
      "high_pri_pool_ratio=0.0;}",
      &new_opt));
  ASSERT_EQ(new_opt.block_cache->GetCapacity(), 0);
  ASSERT_EQ(std::dynamic_pointer_cast<ShardedCacheBase>(new_opt.block_cache)
                ->GetNumShardBits(),
            5);
  ASSERT_EQ(new_opt.block_cache->HasStrictCapacityLimit(), false);
  ASSERT_EQ(std::dynamic_pointer_cast<LRUCache>(new_opt.block_cache)
                ->GetHighPriPoolRatio(),
            0.5);

  // Set couple of block cache options.
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "block_cache={capacity=1M;num_shard_bits=4;"
      "strict_capacity_limit=true;};"
      "block_cache_compressed={capacity=1M;num_shard_bits=4;"
      "strict_capacity_limit=true;}",
      &new_opt));
  ASSERT_TRUE(new_opt.block_cache != nullptr);
  ASSERT_EQ(new_opt.block_cache->GetCapacity(), 1024UL * 1024UL);
  ASSERT_EQ(std::dynamic_pointer_cast<ShardedCacheBase>(new_opt.block_cache)
                ->GetNumShardBits(),
            4);
  ASSERT_EQ(new_opt.block_cache->HasStrictCapacityLimit(), true);
  ASSERT_EQ(std::dynamic_pointer_cast<LRUCache>(new_opt.block_cache)
                ->GetHighPriPoolRatio(),
            0.5);

  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt, "filter_policy=rocksdb.BloomFilter:1.234",
      &new_opt));
  ASSERT_TRUE(new_opt.filter_policy != nullptr);
  ASSERT_TRUE(
      new_opt.filter_policy->IsInstanceOf(BloomFilterPolicy::kClassName()));
  ASSERT_TRUE(
      new_opt.filter_policy->IsInstanceOf(BloomFilterPolicy::kNickName()));

  // Ribbon filter policy alternative name
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt, "filter_policy=rocksdb.RibbonFilter:6.789:5;",
      &new_opt));
  ASSERT_TRUE(new_opt.filter_policy != nullptr);
  ASSERT_TRUE(
      new_opt.filter_policy->IsInstanceOf(RibbonFilterPolicy::kClassName()));
  ASSERT_TRUE(
      new_opt.filter_policy->IsInstanceOf(RibbonFilterPolicy::kNickName()));
}

TEST_F(OptionsTest, GetPlainTableOptionsFromString) {
  PlainTableOptions table_opt;
  PlainTableOptions new_opt;
  ConfigOptions config_options;
  config_options.input_strings_escaped = false;
  config_options.ignore_unknown_options = false;
  // make sure default values are overwritten by something else
  ASSERT_OK(GetPlainTableOptionsFromString(
      config_options, table_opt,
      "user_key_len=66;bloom_bits_per_key=20;hash_table_ratio=0.5;"
      "index_sparseness=8;huge_page_tlb_size=4;encoding_type=kPrefix;"
      "full_scan_mode=true;store_index_in_file=true",
      &new_opt));
  ASSERT_EQ(new_opt.user_key_len, 66u);
  ASSERT_EQ(new_opt.bloom_bits_per_key, 20);
  ASSERT_EQ(new_opt.hash_table_ratio, 0.5);
  ASSERT_EQ(new_opt.index_sparseness, 8);
  ASSERT_EQ(new_opt.huge_page_tlb_size, 4);
  ASSERT_EQ(new_opt.encoding_type, EncodingType::kPrefix);
  ASSERT_TRUE(new_opt.full_scan_mode);
  ASSERT_TRUE(new_opt.store_index_in_file);

  // unknown option
  Status s = GetPlainTableOptionsFromString(
      config_options, table_opt,
      "user_key_len=66;bloom_bits_per_key=20;hash_table_ratio=0.5;"
      "bad_option=1",
      &new_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());

  // unrecognized EncodingType
  s = GetPlainTableOptionsFromString(
      config_options, table_opt,
      "user_key_len=66;bloom_bits_per_key=20;hash_table_ratio=0.5;"
      "encoding_type=kPrefixXX",
      &new_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
}

TEST_F(OptionsTest, GetMemTableRepFactoryFromString) {
  std::unique_ptr<MemTableRepFactory> new_mem_factory = nullptr;

  ASSERT_OK(GetMemTableRepFactoryFromString("skip_list", &new_mem_factory));
  ASSERT_OK(GetMemTableRepFactoryFromString("skip_list:16", &new_mem_factory));
  ASSERT_STREQ(new_mem_factory->Name(), "SkipListFactory");
  ASSERT_NOK(GetMemTableRepFactoryFromString("skip_list:16:invalid_opt",
                                             &new_mem_factory));

  ASSERT_OK(GetMemTableRepFactoryFromString("prefix_hash", &new_mem_factory));
  ASSERT_OK(
      GetMemTableRepFactoryFromString("prefix_hash:1000", &new_mem_factory));
  ASSERT_STREQ(new_mem_factory->Name(), "HashSkipListRepFactory");
  ASSERT_NOK(GetMemTableRepFactoryFromString("prefix_hash:1000:invalid_opt",
                                             &new_mem_factory));

  ASSERT_OK(
      GetMemTableRepFactoryFromString("hash_linkedlist", &new_mem_factory));
  ASSERT_OK(GetMemTableRepFactoryFromString("hash_linkedlist:1000",
                                            &new_mem_factory));
  ASSERT_EQ(std::string(new_mem_factory->Name()), "HashLinkListRepFactory");
  ASSERT_NOK(GetMemTableRepFactoryFromString("hash_linkedlist:1000:invalid_opt",
                                             &new_mem_factory));

  ASSERT_OK(GetMemTableRepFactoryFromString("vector", &new_mem_factory));
  ASSERT_OK(GetMemTableRepFactoryFromString("vector:1024", &new_mem_factory));
  ASSERT_EQ(std::string(new_mem_factory->Name()), "VectorRepFactory");
  ASSERT_NOK(GetMemTableRepFactoryFromString("vector:1024:invalid_opt",
                                             &new_mem_factory));

  ASSERT_NOK(GetMemTableRepFactoryFromString("cuckoo", &new_mem_factory));
  // CuckooHash memtable is already removed.
  ASSERT_NOK(GetMemTableRepFactoryFromString("cuckoo:1024", &new_mem_factory));

  ASSERT_NOK(GetMemTableRepFactoryFromString("bad_factory", &new_mem_factory));
}

TEST_F(OptionsTest, MemTableRepFactoryCreateFromString) {
  std::unique_ptr<MemTableRepFactory> new_mem_factory = nullptr;
  ConfigOptions config_options;
  config_options.ignore_unsupported_options = false;
  config_options.ignore_unknown_options = false;

  ASSERT_OK(MemTableRepFactory::CreateFromString(config_options, "skip_list",
                                                 &new_mem_factory));
  ASSERT_OK(MemTableRepFactory::CreateFromString(config_options, "skip_list:16",
                                                 &new_mem_factory));
  ASSERT_STREQ(new_mem_factory->Name(), "SkipListFactory");
  ASSERT_TRUE(new_mem_factory->IsInstanceOf("skip_list"));
  ASSERT_TRUE(new_mem_factory->IsInstanceOf("SkipListFactory"));
  ASSERT_NOK(MemTableRepFactory::CreateFromString(
      config_options, "skip_list:16:invalid_opt", &new_mem_factory));

  ASSERT_NOK(MemTableRepFactory::CreateFromString(
      config_options, "invalid_opt=10", &new_mem_factory));

  // Test a reset
  ASSERT_OK(MemTableRepFactory::CreateFromString(config_options, "",
                                                 &new_mem_factory));
  ASSERT_EQ(new_mem_factory, nullptr);
  ASSERT_NOK(MemTableRepFactory::CreateFromString(
      config_options, "invalid_opt=10", &new_mem_factory));

  ASSERT_OK(MemTableRepFactory::CreateFromString(
      config_options, "id=skip_list; lookahead=32", &new_mem_factory));
  ASSERT_OK(MemTableRepFactory::CreateFromString(config_options, "prefix_hash",
                                                 &new_mem_factory));
  ASSERT_OK(MemTableRepFactory::CreateFromString(
      config_options, "prefix_hash:1000", &new_mem_factory));
  ASSERT_STREQ(new_mem_factory->Name(), "HashSkipListRepFactory");
  ASSERT_TRUE(new_mem_factory->IsInstanceOf("prefix_hash"));
  ASSERT_TRUE(new_mem_factory->IsInstanceOf("HashSkipListRepFactory"));
  ASSERT_NOK(MemTableRepFactory::CreateFromString(
      config_options, "prefix_hash:1000:invalid_opt", &new_mem_factory));
  ASSERT_OK(MemTableRepFactory::CreateFromString(
      config_options,
      "id=prefix_hash; bucket_count=32; skiplist_height=64; "
      "branching_factor=16",
      &new_mem_factory));
  ASSERT_NOK(MemTableRepFactory::CreateFromString(
      config_options,
      "id=prefix_hash; bucket_count=32; skiplist_height=64; "
      "branching_factor=16; invalid=unknown",
      &new_mem_factory));

  ASSERT_OK(MemTableRepFactory::CreateFromString(
      config_options, "hash_linkedlist", &new_mem_factory));
  ASSERT_OK(MemTableRepFactory::CreateFromString(
      config_options, "hash_linkedlist:1000", &new_mem_factory));
  ASSERT_STREQ(new_mem_factory->Name(), "HashLinkListRepFactory");
  ASSERT_TRUE(new_mem_factory->IsInstanceOf("hash_linkedlist"));
  ASSERT_TRUE(new_mem_factory->IsInstanceOf("HashLinkListRepFactory"));
  ASSERT_NOK(MemTableRepFactory::CreateFromString(
      config_options, "hash_linkedlist:1000:invalid_opt", &new_mem_factory));
  ASSERT_OK(MemTableRepFactory::CreateFromString(
      config_options,
      "id=hash_linkedlist; bucket_count=32; threshold=64; huge_page_size=16; "
      "logging_threshold=12; log_when_flash=true",
      &new_mem_factory));
  ASSERT_NOK(MemTableRepFactory::CreateFromString(
      config_options,
      "id=hash_linkedlist; bucket_count=32; threshold=64; huge_page_size=16; "
      "logging_threshold=12; log_when_flash=true; invalid=unknown",
      &new_mem_factory));

  ASSERT_OK(MemTableRepFactory::CreateFromString(config_options, "vector",
                                                 &new_mem_factory));
  ASSERT_OK(MemTableRepFactory::CreateFromString(config_options, "vector:1024",
                                                 &new_mem_factory));
  ASSERT_STREQ(new_mem_factory->Name(), "VectorRepFactory");
  ASSERT_TRUE(new_mem_factory->IsInstanceOf("vector"));
  ASSERT_TRUE(new_mem_factory->IsInstanceOf("VectorRepFactory"));
  ASSERT_NOK(MemTableRepFactory::CreateFromString(
      config_options, "vector:1024:invalid_opt", &new_mem_factory));
  ASSERT_OK(MemTableRepFactory::CreateFromString(
      config_options, "id=vector; count=42", &new_mem_factory));
  ASSERT_NOK(MemTableRepFactory::CreateFromString(
      config_options, "id=vector; invalid=unknown", &new_mem_factory));
  ASSERT_NOK(MemTableRepFactory::CreateFromString(config_options, "cuckoo",
                                                  &new_mem_factory));
  // CuckooHash memtable is already removed.
  ASSERT_NOK(MemTableRepFactory::CreateFromString(config_options, "cuckoo:1024",
                                                  &new_mem_factory));

  ASSERT_NOK(MemTableRepFactory::CreateFromString(config_options, "bad_factory",
                                                  &new_mem_factory));
}

class CustomEnv : public EnvWrapper {
 public:
  explicit CustomEnv(Env* _target) : EnvWrapper(_target) {}
  static const char* kClassName() { return "CustomEnv"; }
  const char* Name() const override { return kClassName(); }
};

TEST_F(OptionsTest, GetOptionsFromStringTest) {
  Options base_options;
  Options new_options;
  ConfigOptions config_options;
  config_options.input_strings_escaped = false;
  config_options.ignore_unknown_options = false;

  base_options.write_buffer_size = 20;
  base_options.min_write_buffer_number_to_merge = 15;
  BlockBasedTableOptions block_based_table_options;
  block_based_table_options.cache_index_and_filter_blocks = true;
  base_options.table_factory.reset(
      NewBlockBasedTableFactory(block_based_table_options));

  // Register an Env with object registry.
  ObjectLibrary::Default()->AddFactory<Env>(
      CustomEnv::kClassName(),
      [](const std::string& /*name*/, std::unique_ptr<Env>* /*env_guard*/,
         std::string* /* errmsg */) {
        static CustomEnv env(Env::Default());
        return &env;
      });

  ASSERT_OK(GetOptionsFromString(
      config_options, base_options,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={block_cache=1M;block_size=4;};"
      "compression_opts=4:5:6;create_if_missing=true;max_open_files=1;"
      "bottommost_compression_opts=5:6:7;create_if_missing=true;max_open_files="
      "1;"
      "rate_limiter_bytes_per_sec=1024;env=CustomEnv",
      &new_options));

  ASSERT_EQ(new_options.compression_opts.window_bits, 4);
  ASSERT_EQ(new_options.compression_opts.level, 5);
  ASSERT_EQ(new_options.compression_opts.strategy, 6);
  ASSERT_EQ(new_options.compression_opts.max_dict_bytes, 0u);
  ASSERT_EQ(new_options.compression_opts.zstd_max_train_bytes, 0u);
  ASSERT_EQ(new_options.compression_opts.parallel_threads, 1u);
  ASSERT_EQ(new_options.compression_opts.enabled, false);
  ASSERT_EQ(new_options.compression_opts.use_zstd_dict_trainer, true);
  ASSERT_EQ(new_options.bottommost_compression, kDisableCompressionOption);
  ASSERT_EQ(new_options.bottommost_compression_opts.window_bits, 5);
  ASSERT_EQ(new_options.bottommost_compression_opts.level, 6);
  ASSERT_EQ(new_options.bottommost_compression_opts.strategy, 7);
  ASSERT_EQ(new_options.bottommost_compression_opts.max_dict_bytes, 0u);
  ASSERT_EQ(new_options.bottommost_compression_opts.zstd_max_train_bytes, 0u);
  ASSERT_EQ(new_options.bottommost_compression_opts.parallel_threads, 1u);
  ASSERT_EQ(new_options.bottommost_compression_opts.enabled, false);
  ASSERT_EQ(new_options.bottommost_compression_opts.use_zstd_dict_trainer,
            true);
  ASSERT_EQ(new_options.write_buffer_size, 10U);
  ASSERT_EQ(new_options.max_write_buffer_number, 16);
  const auto new_bbto =
      new_options.table_factory->GetOptions<BlockBasedTableOptions>();
  ASSERT_NE(new_bbto, nullptr);
  ASSERT_EQ(new_bbto->block_cache->GetCapacity(), 1U << 20);
  ASSERT_EQ(new_bbto->block_size, 4U);
  // don't overwrite block based table options
  ASSERT_TRUE(new_bbto->cache_index_and_filter_blocks);

  ASSERT_EQ(new_options.create_if_missing, true);
  ASSERT_EQ(new_options.max_open_files, 1);
  ASSERT_TRUE(new_options.rate_limiter.get() != nullptr);
  Env* newEnv = new_options.env;
  ASSERT_OK(Env::CreateFromString({}, CustomEnv::kClassName(), &newEnv));
  ASSERT_EQ(newEnv, new_options.env);

  config_options.ignore_unknown_options = false;
  // Test a bad value for a DBOption returns a failure
  base_options.dump_malloc_stats = false;
  base_options.write_buffer_size = 1024;
  Options bad_options = new_options;
  Status s = GetOptionsFromString(config_options, base_options,
                                  "create_if_missing=XX;dump_malloc_stats=true",
                                  &bad_options);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_EQ(bad_options.dump_malloc_stats, false);

  bad_options = new_options;
  s = GetOptionsFromString(config_options, base_options,
                           "write_buffer_size=XX;dump_malloc_stats=true",
                           &bad_options);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());

  ASSERT_EQ(bad_options.dump_malloc_stats, false);

  // Test a bad value for a TableFactory Option returns a failure
  bad_options = new_options;
  s = GetOptionsFromString(config_options, base_options,
                           "write_buffer_size=16;dump_malloc_stats=true"
                           "block_based_table_factory={block_size=XX;};",
                           &bad_options);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_EQ(bad_options.dump_malloc_stats, false);
  ASSERT_EQ(bad_options.write_buffer_size, 1024);

  config_options.ignore_unknown_options = true;
  ASSERT_OK(GetOptionsFromString(config_options, base_options,
                                 "create_if_missing=XX;dump_malloc_stats=true;"
                                 "write_buffer_size=XX;"
                                 "block_based_table_factory={block_size=XX;};",
                                 &bad_options));
  ASSERT_EQ(bad_options.create_if_missing, base_options.create_if_missing);
  ASSERT_EQ(bad_options.dump_malloc_stats, true);
  ASSERT_EQ(bad_options.write_buffer_size, base_options.write_buffer_size);

  // Test the old interface
  ASSERT_OK(GetOptionsFromString(
      base_options,
      "write_buffer_size=22;max_write_buffer_number=33;max_open_files=44;",
      &new_options));
  ASSERT_EQ(new_options.write_buffer_size, 22U);
  ASSERT_EQ(new_options.max_write_buffer_number, 33);
  ASSERT_EQ(new_options.max_open_files, 44);
}

TEST_F(OptionsTest, DBOptionsSerialization) {
  Options base_options;
  Options new_options;
  Random rnd(301);
  ConfigOptions config_options;
  config_options.input_strings_escaped = false;
  config_options.ignore_unknown_options = false;

  // Phase 1: Make big change in base_options
  test::RandomInitDBOptions(&base_options, &rnd);

  // Phase 2: obtain a string from base_option
  std::string base_options_file_content;
  ASSERT_OK(GetStringFromDBOptions(config_options, base_options,
                                   &base_options_file_content));

  // Phase 3: Set new_options from the derived string and expect
  //          new_options == base_options
  ASSERT_OK(GetDBOptionsFromString(config_options, DBOptions(),
                                   base_options_file_content, &new_options));
  ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(config_options, base_options,
                                                  new_options));
}

TEST_F(OptionsTest, RemoteCompactionPoolNotSerialized) {
  ConfigOptions config_options;
  config_options.input_strings_escaped = false;
  DBOptions options;
  ASSERT_OK(GetDBOptionsFromString(config_options, DBOptions(),
                                   "use_separate_remote_compaction_pool=true",
                                   &options));
  ASSERT_TRUE(options.use_separate_remote_compaction_pool);
  std::string serialized;
  ASSERT_OK(GetStringFromDBOptions(config_options, options, &serialized));
  ASSERT_EQ(serialized.find("use_separate_remote_compaction_pool"),
            std::string::npos);
  DBOptions restored;
  ASSERT_OK(GetDBOptionsFromString(config_options, DBOptions(), serialized,
                                   &restored));
  ASSERT_FALSE(restored.use_separate_remote_compaction_pool);
  config_options.sanity_level = ConfigOptions::kSanityLevelExactMatch;
  ASSERT_OK(
      RocksDBOptionsParser::VerifyDBOptions(config_options, options, restored));
  ASSERT_OK(GetStringFromMutableDBOptions(
      config_options, MutableDBOptions(options), &serialized));
  ASSERT_EQ(serialized.find("use_separate_remote_compaction_pool"),
            std::string::npos);
}

TEST_F(OptionsTest, OptionsComposeDecompose) {
  // build an Options from DBOptions + CFOptions, then decompose it to verify
  // we get same constituent options.
  DBOptions base_db_opts;
  ColumnFamilyOptions base_cf_opts;
  ConfigOptions
      config_options;  // Use default for ignore(false) and check (exact)
  config_options.input_strings_escaped = false;

  Random rnd(301);
  test::RandomInitDBOptions(&base_db_opts, &rnd);
  test::RandomInitCFOptions(&base_cf_opts, base_db_opts, &rnd);

  Options base_opts(base_db_opts, base_cf_opts);
  DBOptions new_db_opts(base_opts);
  ColumnFamilyOptions new_cf_opts(base_opts);

  ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(config_options, base_db_opts,
                                                  new_db_opts));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_cf_opts,
                                                  new_cf_opts));
  delete new_cf_opts.compaction_filter;
}

TEST_F(OptionsTest, DBOptionsComposeImmutable) {
  // Build a DBOptions from an Immutable/Mutable one and verify that
  // we get same constituent options.
  ConfigOptions config_options;
  Random rnd(301);
  DBOptions base_opts;
  DBOptions new_opts;
  test::RandomInitDBOptions(&base_opts, &rnd);
  MutableDBOptions m_opts(base_opts);
  ImmutableDBOptions i_opts(base_opts);
  new_opts = BuildDBOptions(i_opts, m_opts);
  ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(config_options, base_opts,
                                                  new_opts));
}

TEST_F(OptionsTest, GetMutableDBOptions) {
  Random rnd(228);
  DBOptions base_opts;
  std::string opts_str;
  std::unordered_map<std::string, std::string> opts_map;
  ConfigOptions config_options;

  test::RandomInitDBOptions(&base_opts, &rnd);
  ImmutableDBOptions i_opts(base_opts);
  MutableDBOptions m_opts(base_opts);
  MutableDBOptions new_opts;
  ASSERT_OK(GetStringFromMutableDBOptions(config_options, m_opts, &opts_str));
  ASSERT_OK(StringToMap(opts_str, &opts_map));
  ASSERT_OK(GetMutableDBOptionsFromStrings(m_opts, opts_map, &new_opts));
  ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(
      config_options, base_opts, BuildDBOptions(i_opts, new_opts)));
}

TEST_F(OptionsTest, CFOptionsComposeImmutable) {
  // Build a DBOptions from an Immutable/Mutable one and verify that
  // we get same constituent options.
  ConfigOptions config_options;
  Random rnd(301);
  ColumnFamilyOptions base_opts;
  ColumnFamilyOptions new_opts;
  DBOptions dummy;  // Needed to create ImmutableCFOptions
  test::RandomInitCFOptions(&base_opts, dummy, &rnd);
  MutableCFOptions m_opts(base_opts);
  ImmutableCFOptions i_opts(base_opts);
  UpdateColumnFamilyOptions(i_opts, &new_opts);
  UpdateColumnFamilyOptions(m_opts, &new_opts);
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_opts,
                                                  new_opts));
  delete new_opts.compaction_filter;
}

TEST_F(OptionsTest, GetMutableCFOptions) {
  Random rnd(228);
  ColumnFamilyOptions base;
  ColumnFamilyOptions copy;
  std::string opts_str;
  std::unordered_map<std::string, std::string> opts_map;
  ConfigOptions config_options;
  DBOptions dummy;  // Needed to create ImmutableCFOptions

  test::RandomInitCFOptions(&base, dummy, &rnd);
  ColumnFamilyOptions result;
  MutableCFOptions m_opts(base);
  MutableCFOptions new_opts;

  ASSERT_OK(GetStringFromMutableCFOptions(config_options, m_opts, &opts_str));
  ASSERT_OK(StringToMap(opts_str, &opts_map));
  ASSERT_OK(GetMutableOptionsFromStrings(m_opts, opts_map, nullptr, &new_opts));
  UpdateColumnFamilyOptions(ImmutableCFOptions(base), &copy);
  UpdateColumnFamilyOptions(new_opts, &copy);

  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base, copy));
  delete copy.compaction_filter;
}

TEST_F(OptionsTest, ColumnFamilyOptionsSerialization) {
  Options options;
  ColumnFamilyOptions base_opt;
  ColumnFamilyOptions new_opt;
  base_opt.comparator = test::BytewiseComparatorWithU64TsWrapper();
  Random rnd(302);
  ConfigOptions config_options;
  config_options.input_strings_escaped = false;

  // Phase 1: randomly assign base_opt
  // custom type options
  test::RandomInitCFOptions(&base_opt, options, &rnd);

  // Phase 2: obtain a string from base_opt
  std::string base_options_file_content;
  ASSERT_OK(GetStringFromColumnFamilyOptions(config_options, base_opt,
                                             &base_options_file_content));

  // Phase 3: Set new_opt from the derived string and expect
  //          new_opt == base_opt
  ASSERT_OK(
      GetColumnFamilyOptionsFromString(config_options, ColumnFamilyOptions(),
                                       base_options_file_content, &new_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyCFOptions(config_options, base_opt, new_opt));
  ASSERT_EQ(base_opt.comparator, new_opt.comparator);
  if (base_opt.compaction_filter) {
    delete base_opt.compaction_filter;
  }
}

TEST_F(OptionsTest, CheckBlockBasedTableOptions) {
  ColumnFamilyOptions cf_opts;
  DBOptions db_opts;
  ConfigOptions config_opts;

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_opts, cf_opts, "prefix_extractor=capped:8", &cf_opts));
  ASSERT_OK(TableFactory::CreateFromString(config_opts, "BlockBasedTable",
                                           &cf_opts.table_factory));
  ASSERT_NE(cf_opts.table_factory.get(), nullptr);
  ASSERT_TRUE(cf_opts.table_factory->IsInstanceOf(
      TableFactory::kBlockBasedTableName()));
  auto bbto = cf_opts.table_factory->GetOptions<BlockBasedTableOptions>();
  ASSERT_OK(cf_opts.table_factory->ConfigureFromString(
      config_opts,
      "block_cache={capacity=1M;num_shard_bits=4;};"
      "block_size_deviation=101;"
      "block_restart_interval=0;"
      "index_block_restart_interval=5;"
      "partition_filters=true;"
      "index_type=kHashSearch;"
      "no_block_cache=1;"));
  ASSERT_NE(bbto, nullptr);
  ASSERT_EQ(bbto->block_cache.get(), nullptr);
  ASSERT_EQ(bbto->block_size_deviation, 0);
  ASSERT_EQ(bbto->block_restart_interval, 1);
  ASSERT_EQ(bbto->index_block_restart_interval, 1);
  ASSERT_FALSE(bbto->partition_filters);
  ASSERT_OK(TableFactory::CreateFromString(config_opts, "BlockBasedTable",
                                           &cf_opts.table_factory));
  bbto = cf_opts.table_factory->GetOptions<BlockBasedTableOptions>();

  ASSERT_OK(cf_opts.table_factory->ConfigureFromString(config_opts,
                                                       "no_block_cache=0;"));
  ASSERT_NE(bbto->block_cache.get(), nullptr);
  ASSERT_OK(cf_opts.table_factory->ValidateOptions(db_opts, cf_opts));
}

TEST_F(OptionsTest, MutableTableOptions) {
  ConfigOptions config_options;
  std::shared_ptr<TableFactory> bbtf;
  bbtf.reset(NewBlockBasedTableFactory());
  auto bbto = bbtf->GetOptions<BlockBasedTableOptions>();
  ASSERT_NE(bbto, nullptr);
  ASSERT_OK(bbtf->ConfigureOption(config_options, "no_block_cache", "true"));
  ASSERT_OK(bbtf->ConfigureOption(config_options, "block_size", "1024"));
  ASSERT_EQ(bbto->no_block_cache, true);
  ASSERT_EQ(bbto->block_size, 1024);
  ASSERT_OK(bbtf->PrepareOptions(config_options));
  config_options.mutable_options_only = true;
  // Options on BlockBasedTableOptions/Factory are no longer directly mutable
  // but have to be mutated on a live DB with SetOptions replacing the
  // table_factory with a copy using the new options.
  ASSERT_NOK(bbtf->ConfigureOption(config_options, "no_block_cache", "false"));
  ASSERT_NOK(bbtf->ConfigureOption(config_options, "block_size", "2048"));
  ASSERT_EQ(bbto->no_block_cache, true);
  ASSERT_EQ(bbto->block_size, 1024);

  ColumnFamilyOptions cf_opts;
  cf_opts.table_factory = bbtf;
  // FIXME: find a way to make this fail again
  /*
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, cf_opts, "block_based_table_factory.no_block_cache=false",
      &cf_opts));
  */
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, cf_opts, "block_based_table_factory.block_size=8192",
      &cf_opts));
  const auto new_bbto =
      cf_opts.table_factory->GetOptions<BlockBasedTableOptions>();
  ASSERT_NE(new_bbto, nullptr);
  ASSERT_NE(new_bbto, bbto);
  ASSERT_EQ(new_bbto->no_block_cache, true);
  ASSERT_EQ(new_bbto->block_size, 8192);
  ASSERT_EQ(bbto->block_size, 1024);
}

TEST_F(OptionsTest, MutableCFOptions) {
  ConfigOptions config_options;
  ColumnFamilyOptions cf_opts;

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, cf_opts,
      "paranoid_file_checks=true; "
      "verify_output_flags=2049; "
      "block_based_table_factory.block_align=false; "
      "block_based_table_factory.super_block_alignment_size=65536; "
      "block_based_table_factory.super_block_alignment_space_overhead_ratio="
      "4096; "
      "block_based_table_factory.block_size=8192;",
      &cf_opts));
  ASSERT_TRUE(cf_opts.paranoid_file_checks);
  ASSERT_NE(
      (cf_opts.verify_output_flags & VerifyOutputFlags::kVerifyBlockChecksum),
      VerifyOutputFlags::kVerifyNone);
  ASSERT_NE((cf_opts.verify_output_flags &
             VerifyOutputFlags::kEnableForRemoteCompaction),
            VerifyOutputFlags::kVerifyNone);
  ASSERT_EQ((cf_opts.verify_output_flags &
             VerifyOutputFlags::kEnableForLocalCompaction),
            VerifyOutputFlags::kVerifyNone);
  ASSERT_NE(cf_opts.table_factory.get(), nullptr);
  auto* bbto = cf_opts.table_factory->GetOptions<BlockBasedTableOptions>();
  ASSERT_NE(bbto, nullptr);
  ASSERT_EQ(bbto->block_size, 8192);
  ASSERT_EQ(bbto->block_align, false);
  ASSERT_EQ(bbto->super_block_alignment_size, 65536);
  ASSERT_EQ(bbto->super_block_alignment_space_overhead_ratio, 4096);
  std::unordered_map<std::string, std::string> unused_opts;
  ASSERT_OK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts, {{"paranoid_file_checks", "false"}}, &cf_opts));
  ASSERT_EQ(cf_opts.paranoid_file_checks, false);

  // Test verify_output_flags with kVerifyFileChecksum (bit 2)
  // 2052 = 4 (kVerifyFileChecksum) | 2048 (kEnableForRemoteCompaction)
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, cf_opts, "verify_output_flags=2052;", &cf_opts));
  ASSERT_NE(
      (cf_opts.verify_output_flags & VerifyOutputFlags::kVerifyFileChecksum),
      VerifyOutputFlags::kVerifyNone);
  ASSERT_NE((cf_opts.verify_output_flags &
             VerifyOutputFlags::kEnableForRemoteCompaction),
            VerifyOutputFlags::kVerifyNone);
  ASSERT_EQ(
      (cf_opts.verify_output_flags & VerifyOutputFlags::kVerifyBlockChecksum),
      VerifyOutputFlags::kVerifyNone);
  // Should replace the factory with the new setting
  ASSERT_OK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts,
      {{"block_based_table_factory.block_size", "16384"}}, &cf_opts));
  bbto = cf_opts.table_factory->GetOptions<BlockBasedTableOptions>();
  ASSERT_EQ(bbto->block_size, 16384);

  config_options.mutable_options_only = true;
  // Force consistency checks is not mutable
  ASSERT_NOK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts, {{"force_consistency_checks", "true"}},
      &cf_opts));

  // Attempt to change the table factory kind. This was previously disallowed
  // and is a dubious operation but is tricky to disallow without breaking
  // other things (FIXME?)
  ASSERT_OK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts, {{"table_factory", "PlainTable"}}, &cf_opts));
  ASSERT_STREQ(cf_opts.table_factory->Name(), TableFactory::kPlainTableName());
  ASSERT_OK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts, {{"table_factory", "BlockBasedTable"}},
      &cf_opts));
  ASSERT_STREQ(cf_opts.table_factory->Name(),
               TableFactory::kBlockBasedTableName());
  ASSERT_OK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts, {{"table_factory.id", "PlainTable"}}, &cf_opts));
  ASSERT_STREQ(cf_opts.table_factory->Name(), TableFactory::kPlainTableName());
  ASSERT_OK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts, {{"table_factory.id", "BlockBasedTable"}},
      &cf_opts));
  ASSERT_STREQ(cf_opts.table_factory->Name(),
               TableFactory::kBlockBasedTableName());
  ASSERT_OK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts,
      {{"table_factory", "{id=PlainTable;bloom_bits_per_key=42}"}}, &cf_opts));
  ASSERT_STREQ(cf_opts.table_factory->Name(), TableFactory::kPlainTableName());

  // Should at least be allowed to instantiate in place of nullptr, for
  // initialization purposes.
  cf_opts.table_factory = nullptr;
  ASSERT_OK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts,
      {{"table_factory", "{id=BlockBasedTable;block_size=12345}"}}, &cf_opts));
  ASSERT_STREQ(cf_opts.table_factory->Name(),
               TableFactory::kBlockBasedTableName());
  bbto = cf_opts.table_factory->GetOptions<BlockBasedTableOptions>();
  ASSERT_EQ(bbto->block_size, 12345);

  // Accessing through the wrong factory alias fails gracefully
  ASSERT_NOK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts,
      {{"plain_table_factory", "{bloom_bits_per_key=42}"}}, &cf_opts));
  ASSERT_NOK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts,
      {{"plain_table_factory.bloom_bits_per_key", "42"}}, &cf_opts));
  ASSERT_STREQ(cf_opts.table_factory->Name(),
               TableFactory::kBlockBasedTableName());

  // Change the block size.
  ASSERT_OK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts,
      {{"block_based_table_factory.block_size", "8192"}}, &cf_opts));
  bbto = cf_opts.table_factory->GetOptions<BlockBasedTableOptions>();
  ASSERT_EQ(bbto->block_size, 8192);

  // Attempt to turn off block cache fails, as this option is not mutable
  // FIXME: find a way to make this fail again
  /*
  ASSERT_NOK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts,
      {{"block_based_table_factory.no_block_cache", "true"}}, &cf_opts));
  */

  // Attempt to change the block size via a config string/map.
  ASSERT_OK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts,
      {{"block_based_table_factory", "{block_size=32768}"}}, &cf_opts));
  bbto = cf_opts.table_factory->GetOptions<BlockBasedTableOptions>();
  ASSERT_EQ(bbto->block_size, 32768);

  // Attempt to change the block size and no cache through the map.  Should
  // fail, leaving the old values intact
  // FIXME: find a way to make this fail again
  /*
  ASSERT_NOK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts,
      {{"block_based_table_factory",
        "{block_size=16384; no_block_cache=true}"}},
      &cf_opts));
  */
  ASSERT_EQ(bbto->block_size, 32768);

  // Switch to plain table for some tests
  cf_opts.table_factory = nullptr;
  ASSERT_OK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts,
      {{"table_factory", "{id=PlainTable;bloom_bits_per_key=42}"}}, &cf_opts));
  ASSERT_STREQ(cf_opts.table_factory->Name(), TableFactory::kPlainTableName());
  auto* pto = cf_opts.table_factory->GetOptions<PlainTableOptions>();
  ASSERT_EQ(pto->bloom_bits_per_key, 42);

  // Accessing through the wrong factory alias fails gracefully
  ASSERT_NOK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts,
      {{"block_based_table_factory.block_size", "8192"}}, &cf_opts));
  ASSERT_NOK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts,
      {{"block_based_table_factory", "{block_size=32768}"}}, &cf_opts));
  ASSERT_STREQ(cf_opts.table_factory->Name(), TableFactory::kPlainTableName());
  ASSERT_EQ(pto, cf_opts.table_factory->GetOptions<PlainTableOptions>());
}

Status StringToMap(const std::string& opts_str,
                   std::unordered_map<std::string, std::string>* opts_map);

TEST_F(OptionsTest, StringToMapTest) {
  std::unordered_map<std::string, std::string> opts_map;
  // Regular options
  ASSERT_OK(StringToMap("k1=v1;k2=v2;k3=v3", &opts_map));
  ASSERT_EQ(opts_map["k1"], "v1");
  ASSERT_EQ(opts_map["k2"], "v2");
  ASSERT_EQ(opts_map["k3"], "v3");
  // Value with '='
  opts_map.clear();
  ASSERT_OK(StringToMap("k1==v1;k2=v2=;", &opts_map));
  ASSERT_EQ(opts_map["k1"], "=v1");
  ASSERT_EQ(opts_map["k2"], "v2=");
  // Overwrriten option
  opts_map.clear();
  ASSERT_OK(StringToMap("k1=v1;k1=v2;k3=v3", &opts_map));
  ASSERT_EQ(opts_map["k1"], "v2");
  ASSERT_EQ(opts_map["k3"], "v3");
  // Empty value
  opts_map.clear();
  ASSERT_OK(StringToMap("k1=v1;k2=;k3=v3;k4=", &opts_map));
  ASSERT_EQ(opts_map["k1"], "v1");
  ASSERT_TRUE(opts_map.find("k2") != opts_map.end());
  ASSERT_EQ(opts_map["k2"], "");
  ASSERT_EQ(opts_map["k3"], "v3");
  ASSERT_TRUE(opts_map.find("k4") != opts_map.end());
  ASSERT_EQ(opts_map["k4"], "");
  opts_map.clear();
  ASSERT_OK(StringToMap("k1=v1;k2=;k3=v3;k4=   ", &opts_map));
  ASSERT_EQ(opts_map["k1"], "v1");
  ASSERT_TRUE(opts_map.find("k2") != opts_map.end());
  ASSERT_EQ(opts_map["k2"], "");
  ASSERT_EQ(opts_map["k3"], "v3");
  ASSERT_TRUE(opts_map.find("k4") != opts_map.end());
  ASSERT_EQ(opts_map["k4"], "");
  opts_map.clear();
  ASSERT_OK(StringToMap("k1=v1;k2=;k3=", &opts_map));
  ASSERT_EQ(opts_map["k1"], "v1");
  ASSERT_TRUE(opts_map.find("k2") != opts_map.end());
  ASSERT_EQ(opts_map["k2"], "");
  ASSERT_TRUE(opts_map.find("k3") != opts_map.end());
  ASSERT_EQ(opts_map["k3"], "");
  opts_map.clear();
  ASSERT_OK(StringToMap("k1=v1;k2=;k3=;", &opts_map));
  ASSERT_EQ(opts_map["k1"], "v1");
  ASSERT_TRUE(opts_map.find("k2") != opts_map.end());
  ASSERT_EQ(opts_map["k2"], "");
  ASSERT_TRUE(opts_map.find("k3") != opts_map.end());
  ASSERT_EQ(opts_map["k3"], "");
  // Regular nested options. Braces are preserved on values that came in
  // braced, so the value is self-contained for direct embedding via
  // `key=value;` and round-trips through StringToMap/MapToString.
  opts_map.clear();
  ASSERT_OK(StringToMap("k1=v1;k2={nk1=nv1;nk2=nv2};k3=v3", &opts_map));
  ASSERT_EQ(opts_map["k1"], "v1");
  ASSERT_EQ(opts_map["k2"], "{nk1=nv1;nk2=nv2}");
  ASSERT_EQ(opts_map["k3"], "v3");
  // Multi-level nested options
  opts_map.clear();
  ASSERT_OK(
      StringToMap("k1=v1;k2={nk1=nv1;nk2={nnk1=nnk2}};"
                  "k3={nk1={nnk1={nnnk1=nnnv1;nnnk2;nnnv2}}};k4=v4",
                  &opts_map));
  ASSERT_EQ(opts_map["k1"], "v1");
  ASSERT_EQ(opts_map["k2"], "{nk1=nv1;nk2={nnk1=nnk2}}");
  ASSERT_EQ(opts_map["k3"], "{nk1={nnk1={nnnk1=nnnv1;nnnk2;nnnv2}}}");
  ASSERT_EQ(opts_map["k4"], "v4");
  // Garbage inside curly braces
  opts_map.clear();
  ASSERT_OK(StringToMap("k1=v1;k2={dfad=};k3={=};k4=v4", &opts_map));
  ASSERT_EQ(opts_map["k1"], "v1");
  ASSERT_EQ(opts_map["k2"], "{dfad=}");
  ASSERT_EQ(opts_map["k3"], "{=}");
  ASSERT_EQ(opts_map["k4"], "v4");
  // Empty nested options
  opts_map.clear();
  ASSERT_OK(StringToMap("k1=v1;k2={};", &opts_map));
  ASSERT_EQ(opts_map["k1"], "v1");
  ASSERT_EQ(opts_map["k2"], "{}");
  opts_map.clear();
  ASSERT_OK(StringToMap("k1=v1;k2={{{{}}}{}{}};", &opts_map));
  ASSERT_EQ(opts_map["k1"], "v1");
  ASSERT_EQ(opts_map["k2"], "{{{{}}}{}{}}");
  // With random spaces. Internal whitespace is preserved verbatim now
  // (we keep the original substring of the value rather than trimming).
  opts_map.clear();
  ASSERT_OK(
      StringToMap("  k1 =  v1 ; k2= {nk1=nv1; nk2={nnk1=nnk2}}  ; "
                  "k3={  {   } }; k4= v4  ",
                  &opts_map));
  ASSERT_EQ(opts_map["k1"], "v1");
  ASSERT_EQ(opts_map["k2"], "{nk1=nv1; nk2={nnk1=nnk2}}");
  ASSERT_EQ(opts_map["k3"], "{  {   } }");
  ASSERT_EQ(opts_map["k4"], "v4");

  // Empty key
  ASSERT_NOK(StringToMap("k1=v1;k2=v2;=", &opts_map));
  ASSERT_NOK(StringToMap("=v1;k2=v2", &opts_map));
  ASSERT_NOK(StringToMap("k1=v1;k2v2;", &opts_map));
  ASSERT_NOK(StringToMap("k1=v1;k2=v2;fadfa", &opts_map));
  ASSERT_NOK(StringToMap("k1=v1;k2=v2;;", &opts_map));
  // Mismatch curly braces
  ASSERT_NOK(StringToMap("k1=v1;k2={;k3=v3", &opts_map));
  ASSERT_NOK(StringToMap("k1=v1;k2={{};k3=v3", &opts_map));
  ASSERT_NOK(StringToMap("k1=v1;k2={}};k3=v3", &opts_map));
  ASSERT_NOK(StringToMap("k1=v1;k2={{}{}}};k3=v3", &opts_map));
  // However this is valid!
  opts_map.clear();
  ASSERT_OK(StringToMap("k1=v1;k2=};k3=v3", &opts_map));
  ASSERT_EQ(opts_map["k1"], "v1");
  ASSERT_EQ(opts_map["k2"], "}");
  ASSERT_EQ(opts_map["k3"], "v3");

  // Invalid chars after closing curly brace
  ASSERT_NOK(StringToMap("k1=v1;k2={{}}{};k3=v3", &opts_map));
  ASSERT_NOK(StringToMap("k1=v1;k2={{}}cfda;k3=v3", &opts_map));
  ASSERT_NOK(StringToMap("k1=v1;k2={{}}  cfda;k3=v3", &opts_map));
  ASSERT_NOK(StringToMap("k1=v1;k2={{}}  cfda", &opts_map));
  ASSERT_NOK(StringToMap("k1=v1;k2={{}}{}", &opts_map));
  ASSERT_NOK(StringToMap("k1=v1;k2={{dfdl}adfa}{}", &opts_map));
}

TEST_F(OptionsTest, EmptyBracedVectorAndBracedScalarTest) {
  // An empty braced list value (`key={}`) must parse as an empty
  // collection, not as a vector with one empty element. StringToMap
  // preserves the braces, so the typed vector parser sees `{}`; its
  // outer-brace strip needs to handle the size-2 case.
  ColumnFamilyOptions cf_opts;
  ConfigOptions cfg;
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      cfg, ColumnFamilyOptions(), "compression_per_level={};", &cf_opts));
  EXPECT_TRUE(cf_opts.compression_per_level.empty());

  // A scalar value wrapped in `{}` (e.g. hand-crafted `key={42}`) must
  // still parse. StringToMap preserves the braces, so the scalar
  // dispatch in OptionTypeInfo::Parse permissively strips one outer
  // layer.
  DBOptions db_opts;
  ASSERT_OK(GetDBOptionsFromString(cfg, DBOptions(), "max_open_files={42};",
                                   &db_opts));
  EXPECT_EQ(db_opts.max_open_files, 42);

  ASSERT_OK(GetDBOptionsFromString(cfg, DBOptions(),
                                   "create_if_missing={true};", &db_opts));
  EXPECT_TRUE(db_opts.create_if_missing);
}

TEST_F(OptionsTest, MapToStringTest) {
  using Map = std::unordered_map<std::string, std::string>;
  std::string s;

  // Simple values.
  Map simple{{"a", "1"}, {"b", "hello"}};
  ASSERT_OK(MapToString(simple, &s));
  Map reparsed;
  ASSERT_OK(StringToMap(s, &reparsed));
  ASSERT_EQ(reparsed, simple);

  // A value already in self-contained braced form (as returned by
  // StringToMap) passes through unchanged. No extra wrapping is needed
  // for round-trip.
  Map already_braced{{"k", "{a=b;c=d}"}};
  ASSERT_OK(MapToString(already_braced, &s));
  ASSERT_EQ(s, "k={a=b;c=d};");
  Map reparsed_braced;
  ASSERT_OK(StringToMap(s, &reparsed_braced));
  ASSERT_EQ(reparsed_braced, already_braced);
}

TEST_F(OptionsTest, StringToMapMapToStringRoundTripTest) {
  using Map = std::unordered_map<std::string, std::string>;
  // Pull a single entry out of a StringToMap-ed map and re-embed it
  // directly into a `key=value;` SetOptions-style string. The pulled
  // value must round-trip without any extra escaping by the caller --
  // this is the property that lets users compose option strings from
  // map entries safely.
  const std::string original =
      "filter_policy={id=ribbonfilter:10:-1;bloom_before_level=-1;};"
      "block_size=4096;cache_index_and_filter_blocks=true";

  Map parsed;
  ASSERT_OK(StringToMap(original, &parsed));
  // The filter_policy value retains its braces -- it's self-contained.
  EXPECT_EQ(parsed["filter_policy"],
            "{id=ribbonfilter:10:-1;bloom_before_level=-1;}");
  EXPECT_EQ(parsed["block_size"], "4096");
  EXPECT_EQ(parsed["cache_index_and_filter_blocks"], "true");

  // Pull a single entry out and embed it in a fresh `key=value;`
  // string. No extra wrapping required by the caller.
  const std::string embedded =
      "filter_policy=" + parsed["filter_policy"] + ";other_opt=42";
  Map reparsed;
  ASSERT_OK(StringToMap(embedded, &reparsed));
  EXPECT_EQ(reparsed["filter_policy"], parsed["filter_policy"]);
  EXPECT_EQ(reparsed["other_opt"], "42");

  // Whole-map round-trip via MapToString.
  std::string regenerated;
  ASSERT_OK(MapToString(parsed, &regenerated));
  Map reparsed2;
  ASSERT_OK(StringToMap(regenerated, &reparsed2));
  EXPECT_EQ(reparsed2, parsed);
}

TEST_F(OptionsTest, FullOptionsStringToMapRoundTripTest) {
  // Round-trip a populated DBOptions and CFOptions through the
  // StringToMap -> MapToString path. This catches any custom option
  // serializer that emits a value containing the StringToMap delimiter
  // (';') without enclosing it in '{}' -- such a value would survive
  // StringToMap as a self-contained entry but corrupt subsequent
  // re-serialization. Existing typed tests
  // (ColumnFamilyOptionsSerialization, DBOptionsSerialization) cover the
  // typed Configurable round-trip; this test specifically exercises the
  // public StringToMap + MapToString path that callers use to compose,
  // mutate, and re-emit option strings.
  Random rnd(607);
  ConfigOptions config_options;
  config_options.input_strings_escaped = false;

  // ---- ColumnFamilyOptions ----
  Options options;
  ColumnFamilyOptions base_cf_opt;
  base_cf_opt.comparator = test::BytewiseComparatorWithU64TsWrapper();
  test::RandomInitCFOptions(&base_cf_opt, options, &rnd);
  std::string cf_str;
  ASSERT_OK(
      GetStringFromColumnFamilyOptions(config_options, base_cf_opt, &cf_str));

  std::unordered_map<std::string, std::string> cf_map;
  ASSERT_OK(StringToMap(cf_str, &cf_map));
  // Each entry must be embeddable as-is in a `key=value;` context.
  for (const auto& [k, v] : cf_map) {
    std::string solo;
    solo.append(k).append("=").append(v).append(";");
    std::unordered_map<std::string, std::string> solo_map;
    ASSERT_OK(StringToMap(solo, &solo_map))
        << "single-entry round-trip failed for " << k << ": " << v;
    ASSERT_EQ(solo_map.size(), 1u) << k << "=" << v;
    ASSERT_EQ(solo_map[k], v) << "single-entry value mismatch for " << k;
  }
  // Whole-map MapToString must reproduce a string parseable into a CF
  // options object equivalent to the original.
  std::string cf_round;
  ASSERT_OK(MapToString(cf_map, &cf_round));
  ColumnFamilyOptions cf_back;
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, ColumnFamilyOptions(), cf_round, &cf_back));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_cf_opt,
                                                  cf_back));

  // ---- DBOptions ----
  DBOptions base_db_opt;
  test::RandomInitDBOptions(&base_db_opt, &rnd);
  std::string db_str;
  ASSERT_OK(GetStringFromDBOptions(config_options, base_db_opt, &db_str));

  std::unordered_map<std::string, std::string> db_map;
  ASSERT_OK(StringToMap(db_str, &db_map));
  for (const auto& [k, v] : db_map) {
    std::string solo;
    solo.append(k).append("=").append(v).append(";");
    std::unordered_map<std::string, std::string> solo_map;
    ASSERT_OK(StringToMap(solo, &solo_map))
        << "single-entry round-trip failed for " << k << ": " << v;
    ASSERT_EQ(solo_map.size(), 1u) << k << "=" << v;
    ASSERT_EQ(solo_map[k], v) << "single-entry value mismatch for " << k;
  }
  std::string db_round;
  ASSERT_OK(MapToString(db_map, &db_round));
  DBOptions db_back;
  ASSERT_OK(
      GetDBOptionsFromString(config_options, DBOptions(), db_round, &db_back));
  ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(config_options, base_db_opt,
                                                  db_back));

  // RandomInitCFOptions allocates a raw CompactionFilter*; the
  // ColumnFamilyOptions destructor doesn't own it.
  delete base_cf_opt.compaction_filter;
}

TEST_F(OptionsTest, StringToMapRandomTest) {
  std::unordered_map<std::string, std::string> opts_map;
  // Make sure segfault is not hit by semi-random strings

  std::vector<std::string> bases = {
      "a={aa={};tt={xxx={}}};c=defff",
      "a={aa={};tt={xxx={}}};c=defff;d={{}yxx{}3{xx}}",
      "abc={{}{}{}{{{}}}{{}{}{}{}{}{}{}"};

  for (const std::string& base : bases) {
    for (int rand_seed = 301; rand_seed < 401; rand_seed++) {
      Random rnd(rand_seed);
      for (int attempt = 0; attempt < 10; attempt++) {
        std::string str = base;
        // Replace random position to space
        size_t pos =
            static_cast<size_t>(rnd.Uniform(static_cast<int>(base.size())));
        str[pos] = ' ';
        Status s = StringToMap(str, &opts_map);
        ASSERT_TRUE(s.ok() || s.IsInvalidArgument());
        if (s.ok()) {
          // Round-trip: StringToMap entries are self-contained, so
          // MapToString of them must parse back to the same map.
          std::string regen;
          ASSERT_OK(MapToString(opts_map, &regen));
          std::unordered_map<std::string, std::string> reparsed;
          ASSERT_OK(StringToMap(regen, &reparsed));
          ASSERT_EQ(reparsed, opts_map) << "regen=" << regen;
        }
        opts_map.clear();
      }
    }
  }

  // Random Construct a string
  std::vector<char> chars = {'{', '}', ' ', '=', ';', 'c'};
  for (int rand_seed = 301; rand_seed < 1301; rand_seed++) {
    Random rnd(rand_seed);
    int len = rnd.Uniform(30);
    std::string str;
    for (int attempt = 0; attempt < len; attempt++) {
      // Add a random character
      size_t pos =
          static_cast<size_t>(rnd.Uniform(static_cast<int>(chars.size())));
      str.append(1, chars[pos]);
    }
    Status s = StringToMap(str, &opts_map);
    ASSERT_TRUE(s.ok() || s.IsInvalidArgument());
    if (s.ok()) {
      std::string regen;
      ASSERT_OK(MapToString(opts_map, &regen));
      std::unordered_map<std::string, std::string> reparsed;
      ASSERT_OK(StringToMap(regen, &reparsed));
      ASSERT_EQ(reparsed, opts_map) << "regen=" << regen;
    }
    s = StringToMap("name=" + str, &opts_map);
    ASSERT_TRUE(s.ok() || s.IsInvalidArgument());
    if (s.ok()) {
      std::string regen;
      ASSERT_OK(MapToString(opts_map, &regen));
      std::unordered_map<std::string, std::string> reparsed;
      ASSERT_OK(StringToMap(regen, &reparsed));
      ASSERT_EQ(reparsed, opts_map) << "regen=" << regen;
    }
    opts_map.clear();
  }
}

TEST_F(OptionsTest, GetStringFromCompressionType) {
  std::string res;

  ASSERT_OK(GetStringFromCompressionType(&res, kNoCompression));
  ASSERT_EQ(res, "kNoCompression");

  ASSERT_OK(GetStringFromCompressionType(&res, kSnappyCompression));
  ASSERT_EQ(res, "kSnappyCompression");

  ASSERT_OK(GetStringFromCompressionType(&res, kDisableCompressionOption));
  ASSERT_EQ(res, "kDisableCompressionOption");

  ASSERT_OK(GetStringFromCompressionType(&res, kLZ4Compression));
  ASSERT_EQ(res, "kLZ4Compression");

  ASSERT_OK(GetStringFromCompressionType(&res, kZlibCompression));
  ASSERT_EQ(res, "kZlibCompression");

  ASSERT_NOK(
      GetStringFromCompressionType(&res, static_cast<CompressionType>(0x7F)));
}

TEST_F(OptionsTest, OnlyMutableDBOptions) {
  std::string opt_str;
  Random rnd(302);
  ConfigOptions cfg_opts;
  DBOptions db_opts;
  DBOptions mdb_opts;
  std::unordered_set<std::string> m_names;
  std::unordered_set<std::string> a_names;

  test::RandomInitDBOptions(&db_opts, &rnd);
  auto db_config = DBOptionsAsConfigurable(db_opts);

  // Get all of the DB Option names (mutable or not)
  ASSERT_OK(db_config->GetOptionNames(cfg_opts, &a_names));

  // Get only the mutable options from db_opts and set those in mdb_opts
  cfg_opts.mutable_options_only = true;

  // Get only the Mutable DB Option names
  ASSERT_OK(db_config->GetOptionNames(cfg_opts, &m_names));
  ASSERT_OK(GetStringFromDBOptions(cfg_opts, db_opts, &opt_str));
  ASSERT_OK(GetDBOptionsFromString(cfg_opts, mdb_opts, opt_str, &mdb_opts));
  std::string mismatch;
  // Comparing only the mutable options, the two are equivalent
  auto mdb_config = DBOptionsAsConfigurable(mdb_opts);
  ASSERT_TRUE(mdb_config->AreEquivalent(cfg_opts, db_config.get(), &mismatch));
  ASSERT_TRUE(db_config->AreEquivalent(cfg_opts, mdb_config.get(), &mismatch));

  ASSERT_GT(a_names.size(), m_names.size());
  for (const auto& n : m_names) {
    std::string m;
    std::string d;
    if (n == "use_separate_remote_compaction_pool") {
      ASSERT_TRUE(mdb_config->GetOption(cfg_opts, n, &m).IsNotSupported());
      ASSERT_TRUE(db_config->GetOption(cfg_opts, n, &d).IsNotSupported());
      continue;
    }
    ASSERT_OK(mdb_config->GetOption(cfg_opts, n, &m));
    ASSERT_OK(db_config->GetOption(cfg_opts, n, &d));
    ASSERT_EQ(m, d);
  }

  cfg_opts.mutable_options_only = false;
  // Comparing all of the options, the two are not equivalent
  ASSERT_FALSE(mdb_config->AreEquivalent(cfg_opts, db_config.get(), &mismatch));
  ASSERT_FALSE(db_config->AreEquivalent(cfg_opts, mdb_config.get(), &mismatch));

  // Make sure there are only mutable options being configured
  ASSERT_OK(GetDBOptionsFromString(cfg_opts, DBOptions(), opt_str, &db_opts));
}

TEST_F(OptionsTest, OnlyMutableCFOptions) {
  std::string opt_str;
  Random rnd(302);
  ConfigOptions cfg_opts;
  DBOptions db_opts;
  ColumnFamilyOptions mcf_opts;
  ColumnFamilyOptions cf_opts;
  std::unordered_set<std::string> m_names;
  std::unordered_set<std::string> a_names;

  test::RandomInitCFOptions(&cf_opts, db_opts, &rnd);
  cf_opts.comparator = ReverseBytewiseComparator();
  auto cf_config = CFOptionsAsConfigurable(cf_opts);

  // Get all of the CF Option names (mutable or not)
  ASSERT_OK(cf_config->GetOptionNames(cfg_opts, &a_names));

  // Get only the mutable options from cf_opts and set those in mcf_opts
  cfg_opts.mutable_options_only = true;
  // Get only the Mutable CF Option names
  ASSERT_OK(cf_config->GetOptionNames(cfg_opts, &m_names));
  ASSERT_OK(GetStringFromColumnFamilyOptions(cfg_opts, cf_opts, &opt_str));
  ASSERT_OK(
      GetColumnFamilyOptionsFromString(cfg_opts, mcf_opts, opt_str, &mcf_opts));
  std::string mismatch;

  auto mcf_config = CFOptionsAsConfigurable(mcf_opts);
  // Comparing only the mutable options, the two are equivalent
  ASSERT_TRUE(mcf_config->AreEquivalent(cfg_opts, cf_config.get(), &mismatch));
  ASSERT_TRUE(cf_config->AreEquivalent(cfg_opts, mcf_config.get(), &mismatch));

  ASSERT_GT(a_names.size(), m_names.size());
  for (const auto& n : m_names) {
    std::string m;
    std::string d;
    ASSERT_OK(mcf_config->GetOption(cfg_opts, n, &m));
    ASSERT_OK(cf_config->GetOption(cfg_opts, n, &d));
    ASSERT_EQ(m, d);
  }

  cfg_opts.mutable_options_only = false;
  // Comparing all of the options, the two are not equivalent
  ASSERT_FALSE(mcf_config->AreEquivalent(cfg_opts, cf_config.get(), &mismatch));
  ASSERT_FALSE(cf_config->AreEquivalent(cfg_opts, mcf_config.get(), &mismatch));
  delete cf_opts.compaction_filter;

  // Make sure the options string contains only mutable options
  ASSERT_OK(GetColumnFamilyOptionsFromString(cfg_opts, ColumnFamilyOptions(),
                                             opt_str, &cf_opts));
  delete cf_opts.compaction_filter;
}

TEST_F(OptionsTest, SstPartitionerTest) {
  ConfigOptions cfg_opts;
  ColumnFamilyOptions cf_opts;
  ColumnFamilyOptions new_opt;
  std::string opts_str;
  std::string mismatch;

  ASSERT_OK(SstPartitionerFactory::CreateFromString(
      cfg_opts, SstPartitionerFixedPrefixFactory::kClassName(),
      &cf_opts.sst_partitioner_factory));
  ASSERT_NE(cf_opts.sst_partitioner_factory, nullptr);
  ASSERT_STREQ(cf_opts.sst_partitioner_factory->Name(),
               SstPartitionerFixedPrefixFactory::kClassName());
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      cfg_opts, ColumnFamilyOptions(),
      std::string("sst_partitioner_factory={id=") +
          SstPartitionerFixedPrefixFactory::kClassName() + "; unknown=10;}",
      &cf_opts));
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      cfg_opts, ColumnFamilyOptions(),
      std::string("sst_partitioner_factory={id=") +
          SstPartitionerFixedPrefixFactory::kClassName() + "; length=10;}",
      &cf_opts));
  ASSERT_NE(cf_opts.sst_partitioner_factory, nullptr);
  ASSERT_STREQ(cf_opts.sst_partitioner_factory->Name(),
               SstPartitionerFixedPrefixFactory::kClassName());
  ASSERT_OK(GetStringFromColumnFamilyOptions(cfg_opts, cf_opts, &opts_str));
  ASSERT_OK(
      GetColumnFamilyOptionsFromString(cfg_opts, cf_opts, opts_str, &new_opt));
  ASSERT_NE(new_opt.sst_partitioner_factory, nullptr);
  ASSERT_STREQ(new_opt.sst_partitioner_factory->Name(),
               SstPartitionerFixedPrefixFactory::kClassName());
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(cfg_opts, cf_opts, new_opt));
  ASSERT_TRUE(cf_opts.sst_partitioner_factory->AreEquivalent(
      cfg_opts, new_opt.sst_partitioner_factory.get(), &mismatch));
}

TEST_F(OptionsTest, FileChecksumGenFactoryTest) {
  ConfigOptions cfg_opts;
  DBOptions db_opts;
  DBOptions new_opt;
  std::string opts_str;
  std::string mismatch;
  auto factory = GetFileChecksumGenCrc32cFactory();

  cfg_opts.ignore_unsupported_options = false;

  ASSERT_OK(GetStringFromDBOptions(cfg_opts, db_opts, &opts_str));
  ASSERT_OK(GetDBOptionsFromString(cfg_opts, db_opts, opts_str, &new_opt));

  ASSERT_NE(factory, nullptr);
  ASSERT_OK(FileChecksumGenFactory::CreateFromString(
      cfg_opts, factory->Name(), &db_opts.file_checksum_gen_factory));
  ASSERT_NE(db_opts.file_checksum_gen_factory, nullptr);
  ASSERT_STREQ(db_opts.file_checksum_gen_factory->Name(), factory->Name());
  ASSERT_NOK(GetDBOptionsFromString(
      cfg_opts, DBOptions(), "file_checksum_gen_factory=unknown", &db_opts));
  ASSERT_OK(GetDBOptionsFromString(
      cfg_opts, DBOptions(),
      std::string("file_checksum_gen_factory=") + factory->Name(), &db_opts));
  ASSERT_NE(db_opts.file_checksum_gen_factory, nullptr);
  ASSERT_STREQ(db_opts.file_checksum_gen_factory->Name(), factory->Name());

  ASSERT_OK(GetStringFromDBOptions(cfg_opts, db_opts, &opts_str));
  ASSERT_OK(GetDBOptionsFromString(cfg_opts, db_opts, opts_str, &new_opt));
  ASSERT_NE(new_opt.file_checksum_gen_factory, nullptr);
  ASSERT_STREQ(new_opt.file_checksum_gen_factory->Name(), factory->Name());
  ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(cfg_opts, db_opts, new_opt));
  ASSERT_TRUE(factory->AreEquivalent(
      cfg_opts, new_opt.file_checksum_gen_factory.get(), &mismatch));
  ASSERT_TRUE(db_opts.file_checksum_gen_factory->AreEquivalent(
      cfg_opts, new_opt.file_checksum_gen_factory.get(), &mismatch));
}

class TestTablePropertiesCollectorFactory
    : public TablePropertiesCollectorFactory {
 private:
  std::string id_;

 public:
  explicit TestTablePropertiesCollectorFactory(const std::string& id)
      : id_(id) {}
  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context /*context*/) override {
    return nullptr;
  }
  static const char* kClassName() { return "TestCollector"; }
  const char* Name() const override { return kClassName(); }
  std::string GetId() const override {
    return std::string(kClassName()) + ":" + id_;
  }
};

TEST_F(OptionsTest, OptionTablePropertiesTest) {
  ConfigOptions cfg_opts;
  ColumnFamilyOptions orig;
  ColumnFamilyOptions copy;
  orig.table_properties_collector_factories.push_back(
      std::make_shared<TestTablePropertiesCollectorFactory>("1"));
  orig.table_properties_collector_factories.push_back(
      std::make_shared<TestTablePropertiesCollectorFactory>("2"));

  // Push two TablePropertiesCollectorFactories then create a new
  // ColumnFamilyOptions based on those settings.  The copy should
  // have no properties but still match the original
  std::string opts_str;
  ASSERT_OK(GetStringFromColumnFamilyOptions(cfg_opts, orig, &opts_str));
  ASSERT_OK(GetColumnFamilyOptionsFromString(cfg_opts, orig, opts_str, &copy));
  ASSERT_EQ(copy.table_properties_collector_factories.size(), 0);
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(cfg_opts, orig, copy));

  // Now register a TablePropertiesCollectorFactory
  // Repeat the experiment.  The copy should have the same
  // properties as the original
  cfg_opts.registry->AddLibrary("collector")
      ->AddFactory<TablePropertiesCollectorFactory>(
          ObjectLibrary::PatternEntry(
              TestTablePropertiesCollectorFactory::kClassName(), false)
              .AddSeparator(":"),
          [](const std::string& name,
             std::unique_ptr<TablePropertiesCollectorFactory>* guard,
             std::string* /* errmsg */) {
            std::string id = name.substr(
                strlen(TestTablePropertiesCollectorFactory::kClassName()) + 1);
            guard->reset(new TestTablePropertiesCollectorFactory(id));
            return guard->get();
          });

  ASSERT_OK(GetColumnFamilyOptionsFromString(cfg_opts, orig, opts_str, &copy));
  ASSERT_EQ(copy.table_properties_collector_factories.size(), 2);
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(cfg_opts, orig, copy));
}

TEST_F(OptionsTest, ConvertOptionsTest) {
  LevelDBOptions leveldb_opt;
  Options converted_opt = ConvertOptions(leveldb_opt);

  ASSERT_EQ(converted_opt.create_if_missing, leveldb_opt.create_if_missing);
  ASSERT_EQ(converted_opt.error_if_exists, leveldb_opt.error_if_exists);
  ASSERT_EQ(converted_opt.paranoid_checks, leveldb_opt.paranoid_checks);
  ASSERT_EQ(converted_opt.env, leveldb_opt.env);
  ASSERT_EQ(converted_opt.info_log.get(), leveldb_opt.info_log);
  ASSERT_EQ(converted_opt.write_buffer_size, leveldb_opt.write_buffer_size);
  ASSERT_EQ(converted_opt.max_open_files, leveldb_opt.max_open_files);
  ASSERT_EQ(converted_opt.compression, leveldb_opt.compression);

  std::shared_ptr<TableFactory> table_factory = converted_opt.table_factory;
  const auto table_opt = table_factory->GetOptions<BlockBasedTableOptions>();
  ASSERT_NE(table_opt, nullptr);

  ASSERT_EQ(table_opt->block_cache->GetCapacity(), 32UL << 20);
  ASSERT_EQ(table_opt->block_size, leveldb_opt.block_size);
  ASSERT_EQ(table_opt->block_restart_interval,
            leveldb_opt.block_restart_interval);
  ASSERT_EQ(table_opt->filter_policy.get(), leveldb_opt.filter_policy);
}
class TestEventListener : public EventListener {
 private:
  std::string id_;

 public:
  explicit TestEventListener(const std::string& id) : id_("Test" + id) {}
  const char* Name() const override { return id_.c_str(); }
};

static std::unordered_map<std::string, OptionTypeInfo>
    test_listener_option_info = {
        {"s",
         {0, OptionType::kString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},

};

class TestConfigEventListener : public TestEventListener {
 private:
  std::string s_;

 public:
  explicit TestConfigEventListener(const std::string& id)
      : TestEventListener("Config" + id) {
    s_ = id;
    RegisterOptions("Test", &s_, &test_listener_option_info);
  }
};

static int RegisterTestEventListener(ObjectLibrary& library,
                                     const std::string& arg) {
  library.AddFactory<EventListener>(
      "Test" + arg,
      [](const std::string& name, std::unique_ptr<EventListener>* guard,
         std::string* /* errmsg */) {
        guard->reset(new TestEventListener(name.substr(4)));
        return guard->get();
      });
  library.AddFactory<EventListener>(
      "TestConfig" + arg,
      [](const std::string& name, std::unique_ptr<EventListener>* guard,
         std::string* /* errmsg */) {
        guard->reset(new TestConfigEventListener(name.substr(10)));
        return guard->get();
      });
  return 1;
}
TEST_F(OptionsTest, OptionsListenerTest) {
  DBOptions orig;
  DBOptions copy;
  orig.listeners.push_back(std::make_shared<TestEventListener>("1"));
  orig.listeners.push_back(std::make_shared<TestEventListener>("2"));
  orig.listeners.push_back(std::make_shared<TestEventListener>(""));
  orig.listeners.push_back(std::make_shared<TestConfigEventListener>("1"));
  orig.listeners.push_back(std::make_shared<TestConfigEventListener>("2"));
  orig.listeners.push_back(std::make_shared<TestConfigEventListener>(""));
  ConfigOptions config_opts(orig);
  config_opts.registry->AddLibrary("listener", RegisterTestEventListener, "1");
  std::string opts_str;
  ASSERT_OK(GetStringFromDBOptions(config_opts, orig, &opts_str));
  ASSERT_OK(GetDBOptionsFromString(config_opts, orig, opts_str, &copy));
  ASSERT_OK(GetStringFromDBOptions(config_opts, copy, &opts_str));
  ASSERT_EQ(
      copy.listeners.size(),
      2);  // The Test{Config}1 Listeners could be loaded but not the others
  ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(config_opts, orig, copy));
}

const static std::string kCustomEnvName = "Custom";
const static std::string kCustomEnvProp = "env=" + kCustomEnvName;

static int RegisterCustomEnv(ObjectLibrary& library, const std::string& arg) {
  library.AddFactory<Env>(
      arg, [](const std::string& /*name*/, std::unique_ptr<Env>* /*env_guard*/,
              std::string* /* errmsg */) {
        static CustomEnv env(Env::Default());
        return &env;
      });
  return 1;
}

// This test suite tests the old APIs into the Configure options methods.
// Once those APIs are officially deprecated, this test suite can be deleted.
class OptionsOldApiTest : public testing::Test {};

TEST_F(OptionsOldApiTest, GetOptionsFromMapTest) {
  std::unordered_map<std::string, std::string> cf_options_map = {
      {"write_buffer_size", "1"},
      {"max_write_buffer_number", "2"},
      {"min_write_buffer_number_to_merge", "3"},
      {"max_write_buffer_number_to_maintain", "99"},
      {"max_write_buffer_size_to_maintain", "-99999"},
      {"compression", "kSnappyCompression"},
      {"compression_per_level",
       "kNoCompression:"
       "kSnappyCompression:"
       "kZlibCompression:"
       "kBZip2Compression:"
       "kLZ4Compression:"
       "kLZ4HCCompression:"
       "kXpressCompression:"
       "kZSTD"},
      {"bottommost_compression", "kLZ4Compression"},
      {"bottommost_compression_opts", "5:6:7:8:9:true"},
      {"compression_opts", "4:5:6:7:8:9:true:10:false"},
      {"num_levels", "8"},
      {"level0_file_num_compaction_trigger", "8"},
      {"level0_slowdown_writes_trigger", "9"},
      {"level0_stop_writes_trigger", "10"},
      {"target_file_size_base", "12"},
      {"target_file_size_multiplier", "13"},
      {"max_bytes_for_level_base", "14"},
      {"level_compaction_dynamic_level_bytes", "true"},
      {"level_compaction_dynamic_file_size", "true"},
      {"max_bytes_for_level_multiplier", "15.0"},
      {"max_bytes_for_level_multiplier_additional", "16:17:18"},
      {"max_compaction_bytes", "21"},
      {"soft_rate_limit", "1.1"},
      {"hard_rate_limit", "2.1"},
      {"snap_refresh_nanos", "1000000"},
      {"rate_limit_delay_max_milliseconds", "100"},
      {"hard_pending_compaction_bytes_limit", "211"},
      {"arena_block_size", "22"},
      {"disable_auto_compactions", "true"},
      {"compaction_style", "kCompactionStyleLevel"},
      {"compaction_pri", "kOldestSmallestSeqFirst"},
      {"verify_checksums_in_compaction", "false"},
      {"compaction_options_fifo",
       "{allow_compaction=true;max_table_files_size=11002244;"
       "file_temperature_age_thresholds={{temperature=kCold;age=12345}}}"},
      {"max_sequential_skip_in_iterations", "24"},
      {"inplace_update_support", "true"},
      {"report_bg_io_stats", "true"},
      {"compaction_measure_io_stats", "false"},
      {"purge_redundant_kvs_while_flush", "false"},
      {"inplace_update_num_locks", "25"},
      {"memtable_prefix_bloom_size_ratio", "0.26"},
      {"memtable_whole_key_filtering", "true"},
      {"memtable_huge_page_size", "28"},
      {"bloom_locality", "29"},
      {"max_successive_merges", "30"},
      {"strict_max_successive_merges", "true"},
      {"min_partial_merge_operands", "31"},
      {"prefix_extractor", "fixed:31"},
      {"experimental_mempurge_threshold", "0.003"},
      {"optimize_filters_for_hits", "true"},
      {"enable_blob_files", "true"},
      {"min_blob_size", "1K"},
      {"blob_file_size", "1G"},
      {"blob_file_writable_file_max_buffer_size", "128K"},
      {"blob_compression_type", "kZSTD"},
      {"blob_compression_opts", "-14:1:0:0:0:true"},
      {"enable_blob_garbage_collection", "true"},
      {"blob_garbage_collection_age_cutoff", "0.5"},
      {"blob_garbage_collection_force_threshold", "0.75"},
      {"blob_compaction_readahead_size", "256K"},
      {"blob_file_starting_level", "1"},
      {"prepopulate_blob_cache", "kDisable"},
      {"last_level_temperature", "kWarm"},
      {"default_write_temperature", "kCold"},
      {"default_temperature", "kHot"},
      {"persist_user_defined_timestamps", "true"},
      {"memtable_max_range_deletions", "0"},
      {"memtable_veirfy_per_key_checksum_on_seek", "true"},
  };

  std::unordered_map<std::string, std::string> db_options_map = {
      {"create_if_missing", "false"},
      {"create_missing_column_families", "true"},
      {"error_if_exists", "false"},
      {"paranoid_checks", "true"},
      {"track_and_verify_wals_in_manifest", "true"},
      {"track_and_verify_wals", "true"},
      {"verify_sst_unique_id_in_manifest", "true"},
      {"async_wal_precreate", "true"},
      {"fast_sst_open", "true"},
      {"max_open_files", "32"},
      {"max_total_wal_size", "33"},
      {"use_fsync", "true"},
      {"db_log_dir", "/db_log_dir"},
      {"wal_dir", "/wal_dir"},
      {"delete_obsolete_files_period_micros", "34"},
      {"max_background_compactions", "35"},
      {"max_background_flushes", "36"},
      {"max_log_file_size", "37"},
      {"log_file_time_to_roll", "38"},
      {"keep_log_file_num", "39"},
      {"recycle_log_file_num", "5"},
      {"max_manifest_file_size", "40"},
      {"max_manifest_space_amp_pct", "42"},
      {"table_cache_numshardbits", "41"},
      {"WAL_ttl_seconds", "43"},
      {"WAL_size_limit_MB", "44"},
      {"manifest_preallocation_size", "45"},
      {"allow_mmap_reads", "true"},
      {"allow_mmap_writes", "false"},
      {"use_direct_reads", "false"},
      {"use_direct_io_for_compaction_reads", "false"},
      {"use_direct_io_for_flush_and_compaction", "false"},
      {"is_fd_close_on_exec", "true"},
      {"skip_log_error_on_recovery", "false"},
      {"stats_dump_period_sec", "46"},
      {"stats_persist_period_sec", "57"},
      {"persist_stats_to_disk", "false"},
      {"stats_history_buffer_size", "69"},
      {"advise_random_on_open", "true"},
      {"use_adaptive_mutex", "false"},
      {"compaction_readahead_size", "100"},
      {"writable_file_max_buffer_size", "314159"},
      {"bytes_per_sync", "47"},
      {"wal_bytes_per_sync", "48"},
      {"strict_bytes_per_sync", "true"},
      {"preserve_deletes", "false"},
  };

  ColumnFamilyOptions base_cf_opt;
  ColumnFamilyOptions new_cf_opt;
  ConfigOptions cf_config_options;
  cf_config_options.ignore_unknown_options = false;
  cf_config_options.input_strings_escaped = false;
  ASSERT_OK(GetColumnFamilyOptionsFromMap(cf_config_options, base_cf_opt,
                                          cf_options_map, &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 1U);
  ASSERT_EQ(new_cf_opt.max_write_buffer_number, 2);
  ASSERT_EQ(new_cf_opt.min_write_buffer_number_to_merge, 3);
  ASSERT_EQ(new_cf_opt.max_write_buffer_size_to_maintain, -99999);
  ASSERT_EQ(new_cf_opt.compression, kSnappyCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level.size(), 8U);
  ASSERT_EQ(new_cf_opt.compression_per_level[0], kNoCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level[1], kSnappyCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level[2], kZlibCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level[3], kBZip2Compression);
  ASSERT_EQ(new_cf_opt.compression_per_level[4], kLZ4Compression);
  ASSERT_EQ(new_cf_opt.compression_per_level[5], kLZ4HCCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level[6], kXpressCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level[7], kZSTD);
  ASSERT_EQ(new_cf_opt.compression_opts.window_bits, 4);
  ASSERT_EQ(new_cf_opt.compression_opts.level, 5);
  ASSERT_EQ(new_cf_opt.compression_opts.strategy, 6);
  ASSERT_EQ(new_cf_opt.compression_opts.max_dict_bytes, 7u);
  ASSERT_EQ(new_cf_opt.compression_opts.zstd_max_train_bytes, 8u);
  ASSERT_EQ(new_cf_opt.compression_opts.parallel_threads, 9u);
  ASSERT_EQ(new_cf_opt.compression_opts.enabled, true);
  ASSERT_EQ(new_cf_opt.compression_opts.max_dict_buffer_bytes, 10u);
  ASSERT_EQ(new_cf_opt.compression_opts.use_zstd_dict_trainer, false);
  ASSERT_EQ(new_cf_opt.bottommost_compression, kLZ4Compression);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.window_bits, 5);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.level, 6);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.strategy, 7);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.max_dict_bytes, 8u);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.zstd_max_train_bytes, 9u);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.parallel_threads,
            CompressionOptions().parallel_threads);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.enabled, true);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.max_dict_buffer_bytes,
            CompressionOptions().max_dict_buffer_bytes);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.use_zstd_dict_trainer,
            CompressionOptions().use_zstd_dict_trainer);
  ASSERT_EQ(new_cf_opt.num_levels, 8);
  ASSERT_EQ(new_cf_opt.level0_file_num_compaction_trigger, 8);
  ASSERT_EQ(new_cf_opt.level0_slowdown_writes_trigger, 9);
  ASSERT_EQ(new_cf_opt.level0_stop_writes_trigger, 10);
  ASSERT_EQ(new_cf_opt.target_file_size_base, static_cast<uint64_t>(12));
  ASSERT_EQ(new_cf_opt.target_file_size_multiplier, 13);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_base, 14U);
  ASSERT_EQ(new_cf_opt.level_compaction_dynamic_level_bytes, true);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier, 15.0);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier_additional.size(), 3U);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier_additional[0], 16);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier_additional[1], 17);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier_additional[2], 18);
  ASSERT_EQ(new_cf_opt.max_compaction_bytes, 21);
  ASSERT_EQ(new_cf_opt.hard_pending_compaction_bytes_limit, 211);
  ASSERT_EQ(new_cf_opt.arena_block_size, 22U);
  ASSERT_EQ(new_cf_opt.disable_auto_compactions, true);
  ASSERT_EQ(new_cf_opt.compaction_style, kCompactionStyleLevel);
  ASSERT_EQ(new_cf_opt.compaction_pri, kOldestSmallestSeqFirst);
  ASSERT_EQ(new_cf_opt.compaction_options_fifo.max_table_files_size,
            static_cast<uint64_t>(11002244));
  ASSERT_EQ(new_cf_opt.compaction_options_fifo.allow_compaction, true);
  ASSERT_EQ(
      new_cf_opt.compaction_options_fifo.file_temperature_age_thresholds.size(),
      1);
  ASSERT_EQ(
      new_cf_opt.compaction_options_fifo.file_temperature_age_thresholds[0]
          .temperature,
      Temperature::kCold);
  ASSERT_EQ(
      new_cf_opt.compaction_options_fifo.file_temperature_age_thresholds[0].age,
      12345);
  ASSERT_EQ(new_cf_opt.max_sequential_skip_in_iterations,
            static_cast<uint64_t>(24));
  ASSERT_EQ(new_cf_opt.inplace_update_support, true);
  ASSERT_EQ(new_cf_opt.inplace_update_num_locks, 25U);
  ASSERT_EQ(new_cf_opt.memtable_prefix_bloom_size_ratio, 0.26);
  ASSERT_EQ(new_cf_opt.memtable_whole_key_filtering, true);
  ASSERT_EQ(new_cf_opt.memtable_huge_page_size, 28U);
  ASSERT_EQ(new_cf_opt.bloom_locality, 29U);
  ASSERT_EQ(new_cf_opt.max_successive_merges, 30U);
  ASSERT_EQ(new_cf_opt.strict_max_successive_merges, true);
  ASSERT_TRUE(new_cf_opt.prefix_extractor != nullptr);
  ASSERT_EQ(new_cf_opt.optimize_filters_for_hits, true);
  ASSERT_EQ(new_cf_opt.prefix_extractor->AsString(), "rocksdb.FixedPrefix.31");
  ASSERT_EQ(new_cf_opt.experimental_mempurge_threshold, 0.003);
  ASSERT_EQ(new_cf_opt.verify_output_flags, VerifyOutputFlags::kVerifyNone);
  ASSERT_EQ(new_cf_opt.enable_blob_files, true);
  ASSERT_EQ(new_cf_opt.min_blob_size, 1ULL << 10);
  ASSERT_EQ(new_cf_opt.blob_file_size, 1ULL << 30);
  ASSERT_EQ(new_cf_opt.blob_file_writable_file_max_buffer_size, 128ULL << 10);
  ASSERT_EQ(new_cf_opt.blob_compression_type, kZSTD);
  ASSERT_EQ(new_cf_opt.blob_compression_opts.level, 1);
  ASSERT_EQ(new_cf_opt.enable_blob_garbage_collection, true);
  ASSERT_EQ(new_cf_opt.blob_garbage_collection_age_cutoff, 0.5);
  ASSERT_EQ(new_cf_opt.blob_garbage_collection_force_threshold, 0.75);
  ASSERT_EQ(new_cf_opt.blob_compaction_readahead_size, 262144);
  ASSERT_EQ(new_cf_opt.blob_file_starting_level, 1);
  ASSERT_EQ(new_cf_opt.prepopulate_blob_cache, PrepopulateBlobCache::kDisable);
  ASSERT_EQ(new_cf_opt.last_level_temperature, Temperature::kWarm);
  ASSERT_EQ(new_cf_opt.default_write_temperature, Temperature::kCold);
  ASSERT_EQ(new_cf_opt.default_temperature, Temperature::kHot);
  ASSERT_EQ(new_cf_opt.persist_user_defined_timestamps, true);
  ASSERT_TRUE(new_cf_opt.memtable_verify_per_key_checksum_on_seek);
  ASSERT_EQ(new_cf_opt.memtable_max_range_deletions, 0);

  cf_options_map["write_buffer_size"] = "hello";
  ASSERT_NOK(GetColumnFamilyOptionsFromMap(cf_config_options, base_cf_opt,
                                           cf_options_map, &new_cf_opt));
  ConfigOptions exact;
  ConfigOptions loose;
  exact.sanity_level = ConfigOptions::kSanityLevelExactMatch;
  loose.sanity_level = ConfigOptions::kSanityLevelLooselyCompatible;

  ASSERT_OK(
      RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  cf_options_map["write_buffer_size"] = "1";
  ASSERT_OK(GetColumnFamilyOptionsFromMap(cf_config_options, base_cf_opt,
                                          cf_options_map, &new_cf_opt));

  cf_options_map["unknown_option"] = "1";
  ASSERT_NOK(GetColumnFamilyOptionsFromMap(cf_config_options, base_cf_opt,
                                           cf_options_map, &new_cf_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  cf_config_options.input_strings_escaped = false;
  cf_config_options.ignore_unknown_options = true;
  ASSERT_OK(GetColumnFamilyOptionsFromMap(cf_config_options, base_cf_opt,
                                          cf_options_map, &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(
      loose, base_cf_opt, new_cf_opt, nullptr /* new_opt_map */));
  ASSERT_NOK(RocksDBOptionsParser::VerifyCFOptions(
      exact /* default for VerifyCFOptions */, base_cf_opt, new_cf_opt,
      nullptr));

  DBOptions base_db_opt;
  DBOptions new_db_opt;
  ConfigOptions db_config_options(base_db_opt);
  db_config_options.input_strings_escaped = false;
  db_config_options.ignore_unknown_options = false;
  ASSERT_OK(GetDBOptionsFromMap(db_config_options, base_db_opt, db_options_map,
                                &new_db_opt));
  ASSERT_EQ(new_db_opt.create_if_missing, false);
  ASSERT_EQ(new_db_opt.create_missing_column_families, true);
  ASSERT_EQ(new_db_opt.error_if_exists, false);
  ASSERT_EQ(new_db_opt.paranoid_checks, true);
  ASSERT_EQ(new_db_opt.track_and_verify_wals_in_manifest, true);
  ASSERT_EQ(new_db_opt.track_and_verify_wals, true);
  ASSERT_EQ(new_db_opt.async_wal_precreate, true);
  ASSERT_EQ(new_db_opt.max_open_files, 32);
  ASSERT_EQ(new_db_opt.max_total_wal_size, static_cast<uint64_t>(33));
  ASSERT_EQ(new_db_opt.use_fsync, true);
  ASSERT_EQ(new_db_opt.db_log_dir, "/db_log_dir");
  ASSERT_EQ(new_db_opt.wal_dir, "/wal_dir");
  ASSERT_EQ(new_db_opt.delete_obsolete_files_period_micros,
            static_cast<uint64_t>(34));
  ASSERT_EQ(new_db_opt.max_background_compactions, 35);
  ASSERT_EQ(new_db_opt.max_background_flushes, 36);
  ASSERT_EQ(new_db_opt.max_log_file_size, 37U);
  ASSERT_EQ(new_db_opt.log_file_time_to_roll, 38U);
  ASSERT_EQ(new_db_opt.keep_log_file_num, 39U);
  ASSERT_EQ(new_db_opt.recycle_log_file_num, 5U);
  ASSERT_EQ(new_db_opt.max_manifest_file_size, uint64_t{40});
  ASSERT_EQ(new_db_opt.max_manifest_space_amp_pct, 42);
  ASSERT_EQ(new_db_opt.table_cache_numshardbits, 41);
  ASSERT_EQ(new_db_opt.WAL_ttl_seconds, static_cast<uint64_t>(43));
  ASSERT_EQ(new_db_opt.WAL_size_limit_MB, static_cast<uint64_t>(44));
  ASSERT_EQ(new_db_opt.manifest_preallocation_size, 45U);
  ASSERT_EQ(new_db_opt.allow_mmap_reads, true);
  ASSERT_EQ(new_db_opt.allow_mmap_writes, false);
  ASSERT_EQ(new_db_opt.use_direct_reads, false);
  ASSERT_EQ(new_db_opt.use_direct_io_for_compaction_reads, false);
  ASSERT_EQ(new_db_opt.use_direct_io_for_flush_and_compaction, false);
  ASSERT_EQ(new_db_opt.is_fd_close_on_exec, true);
  ASSERT_EQ(new_db_opt.stats_dump_period_sec, 46U);
  ASSERT_EQ(new_db_opt.stats_persist_period_sec, 57U);
  ASSERT_EQ(new_db_opt.persist_stats_to_disk, false);
  ASSERT_EQ(new_db_opt.stats_history_buffer_size, 69U);
  ASSERT_EQ(new_db_opt.advise_random_on_open, true);
  ASSERT_EQ(new_db_opt.use_adaptive_mutex, false);
  ASSERT_EQ(new_db_opt.compaction_readahead_size, 100);
  ASSERT_EQ(new_db_opt.writable_file_max_buffer_size, 314159);
  ASSERT_EQ(new_db_opt.bytes_per_sync, static_cast<uint64_t>(47));
  ASSERT_EQ(new_db_opt.wal_bytes_per_sync, static_cast<uint64_t>(48));
  ASSERT_EQ(new_db_opt.strict_bytes_per_sync, true);

  db_options_map["max_open_files"] = "hello";
  ASSERT_NOK(GetDBOptionsFromMap(db_config_options, base_db_opt, db_options_map,
                                 &new_db_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyDBOptions(exact, base_db_opt, new_db_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyDBOptions(loose, base_db_opt, new_db_opt));

  // unknow options should fail parsing without ignore_unknown_options = true
  db_options_map["unknown_db_option"] = "1";
  ASSERT_NOK(GetDBOptionsFromMap(db_config_options, base_db_opt, db_options_map,
                                 &new_db_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyDBOptions(exact, base_db_opt, new_db_opt));

  db_config_options.input_strings_escaped = false;
  db_config_options.ignore_unknown_options = true;
  ASSERT_OK(GetDBOptionsFromMap(db_config_options, base_db_opt, db_options_map,
                                &new_db_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyDBOptions(loose, base_db_opt, new_db_opt));
  ASSERT_NOK(
      RocksDBOptionsParser::VerifyDBOptions(exact, base_db_opt, new_db_opt));
}

TEST_F(OptionsOldApiTest, GetColumnFamilyOptionsFromStringTest) {
  ColumnFamilyOptions base_cf_opt;
  ColumnFamilyOptions new_cf_opt;
  base_cf_opt.table_factory.reset();
  ConfigOptions config_options;
  config_options.input_strings_escaped = false;
  config_options.ignore_unknown_options = false;
  ASSERT_OK(GetColumnFamilyOptionsFromString(config_options, base_cf_opt, "",
                                             &new_cf_opt));
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt, "write_buffer_size=5", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 5U);
  ASSERT_TRUE(new_cf_opt.table_factory == nullptr);
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt, "write_buffer_size=6;", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 6U);
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt, "  write_buffer_size =  7  ", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 7U);
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt, "  write_buffer_size =  8 ; ", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 8U);
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=9;max_write_buffer_number=10", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 9U);
  ASSERT_EQ(new_cf_opt.max_write_buffer_number, 10);
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=11; max_write_buffer_number  =  12 ;", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 11U);
  ASSERT_EQ(new_cf_opt.max_write_buffer_number, 12);
  // Wrong name "max_write_buffer_number_"
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=13;max_write_buffer_number_=14;", &new_cf_opt));
  ConfigOptions exact;
  exact.sanity_level = ConfigOptions::kSanityLevelExactMatch;
  ASSERT_OK(
      RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  // Comparator from object registry
  std::string kCompName = "reverse_comp";
  ObjectLibrary::Default()->AddFactory<const Comparator>(
      kCompName,
      [](const std::string& /*name*/,
         std::unique_ptr<const Comparator>* /*guard*/,
         std::string* /* errmsg */) { return ReverseBytewiseComparator(); });

  ASSERT_OK(GetColumnFamilyOptionsFromString(config_options, base_cf_opt,
                                             "comparator=" + kCompName + ";",
                                             &new_cf_opt));
  ASSERT_EQ(new_cf_opt.comparator, ReverseBytewiseComparator());

  // MergeOperator from object registry
  std::unique_ptr<BytesXOROperator> bxo(new BytesXOROperator());
  std::string kMoName = bxo->Name();
  ASSERT_OK(GetColumnFamilyOptionsFromString(config_options, base_cf_opt,
                                             "merge_operator=" + kMoName + ";",
                                             &new_cf_opt));
  ASSERT_EQ(kMoName, std::string(new_cf_opt.merge_operator->Name()));

  // Wrong key/value pair
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=13;max_write_buffer_number;", &new_cf_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  // Error Paring value
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=13;max_write_buffer_number=;", &new_cf_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  // Missing option name
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt, "write_buffer_size=13; =100;", &new_cf_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  const uint64_t kilo = 1024UL;
  const uint64_t mega = 1024 * kilo;
  const uint64_t giga = 1024 * mega;
  const uint64_t tera = 1024 * giga;

  // Units (k)
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt, "max_write_buffer_number=15K", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.max_write_buffer_number, 15 * kilo);
  // Units (m)
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "max_write_buffer_number=16m;inplace_update_num_locks=17M", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.max_write_buffer_number, 16 * mega);
  ASSERT_EQ(new_cf_opt.inplace_update_num_locks, 17u * mega);
  // Units (g)
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=18g;prefix_extractor=capped:8;"
      "arena_block_size=19G",
      &new_cf_opt));

  ASSERT_EQ(new_cf_opt.write_buffer_size, 18 * giga);
  ASSERT_EQ(new_cf_opt.arena_block_size, 19 * giga);
  ASSERT_TRUE(new_cf_opt.prefix_extractor.get() != nullptr);
  ASSERT_EQ(new_cf_opt.prefix_extractor->AsString(), "rocksdb.CappedPrefix.8");

  // Units (t)
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt, "write_buffer_size=20t;arena_block_size=21T",
      &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 20 * tera);
  ASSERT_EQ(new_cf_opt.arena_block_size, 21 * tera);

  // Nested block based table options
  // Empty
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={};arena_block_size=1024",
      &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.table_factory != nullptr);
  // Non-empty
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={block_cache=1M;block_size=4;};"
      "arena_block_size=1024",
      &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.table_factory != nullptr);
  // Last one
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={block_cache=1M;block_size=4;}",
      &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.table_factory != nullptr);
  // Mismatch curly braces
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={{{block_size=4;};"
      "arena_block_size=1024",
      &new_cf_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  // Unexpected chars after closing curly brace
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={block_size=4;}};"
      "arena_block_size=1024",
      &new_cf_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={block_size=4;}xdfa;"
      "arena_block_size=1024",
      &new_cf_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={block_size=4;}xdfa",
      &new_cf_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  // Invalid block based table option
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={xx_block_size=4;}",
      &new_cf_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  ASSERT_OK(GetColumnFamilyOptionsFromString(config_options, base_cf_opt,
                                             "optimize_filters_for_hits=true",
                                             &new_cf_opt));
  ASSERT_OK(GetColumnFamilyOptionsFromString(config_options, base_cf_opt,
                                             "optimize_filters_for_hits=false",
                                             &new_cf_opt));

  ASSERT_NOK(GetColumnFamilyOptionsFromString(config_options, base_cf_opt,
                                              "optimize_filters_for_hits=junk",
                                              &new_cf_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  // Nested plain table options
  // Empty
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "plain_table_factory={};arena_block_size=1024",
      &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.table_factory != nullptr);
  ASSERT_EQ(std::string(new_cf_opt.table_factory->Name()), "PlainTable");
  // Non-empty
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "plain_table_factory={user_key_len=66;bloom_bits_per_key=20;};"
      "arena_block_size=1024",
      &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.table_factory != nullptr);
  ASSERT_EQ(std::string(new_cf_opt.table_factory->Name()), "PlainTable");

  // memtable factory
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "memtable=skip_list:10;arena_block_size=1024",
      &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.memtable_factory != nullptr);
  ASSERT_TRUE(new_cf_opt.memtable_factory->IsInstanceOf("SkipListFactory"));

  // blob cache
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "blob_cache={capacity=1M;num_shard_bits=4;"
      "strict_capacity_limit=true;high_pri_pool_ratio=0.5;};",
      &new_cf_opt));
  ASSERT_NE(new_cf_opt.blob_cache, nullptr);
  ASSERT_EQ(new_cf_opt.blob_cache->GetCapacity(), 1024UL * 1024UL);
  ASSERT_EQ(static_cast<ShardedCacheBase*>(new_cf_opt.blob_cache.get())
                ->GetNumShardBits(),
            4);
  ASSERT_EQ(new_cf_opt.blob_cache->HasStrictCapacityLimit(), true);
  ASSERT_EQ(static_cast<LRUCache*>(new_cf_opt.blob_cache.get())
                ->GetHighPriPoolRatio(),
            0.5);
}

TEST_F(OptionsTest, SliceTransformCreateFromString) {
  std::shared_ptr<const SliceTransform> transform = nullptr;
  ConfigOptions config_options;
  config_options.ignore_unsupported_options = false;
  config_options.ignore_unknown_options = false;

  ASSERT_OK(
      SliceTransform::CreateFromString(config_options, "fixed:31", &transform));
  ASSERT_NE(transform, nullptr);
  ASSERT_FALSE(transform->IsInstanceOf("capped"));
  ASSERT_TRUE(transform->IsInstanceOf("fixed"));
  ASSERT_TRUE(transform->IsInstanceOf("rocksdb.FixedPrefix"));
  ASSERT_EQ(transform->GetId(), "rocksdb.FixedPrefix.31");
  ASSERT_OK(SliceTransform::CreateFromString(
      config_options, "rocksdb.FixedPrefix.42", &transform));
  ASSERT_NE(transform, nullptr);
  ASSERT_EQ(transform->GetId(), "rocksdb.FixedPrefix.42");

  ASSERT_OK(SliceTransform::CreateFromString(config_options, "capped:16",
                                             &transform));
  ASSERT_NE(transform, nullptr);
  ASSERT_FALSE(transform->IsInstanceOf("fixed"));
  ASSERT_TRUE(transform->IsInstanceOf("capped"));
  ASSERT_TRUE(transform->IsInstanceOf("rocksdb.CappedPrefix"));
  ASSERT_EQ(transform->GetId(), "rocksdb.CappedPrefix.16");
  ASSERT_OK(SliceTransform::CreateFromString(
      config_options, "rocksdb.CappedPrefix.42", &transform));
  ASSERT_NE(transform, nullptr);
  ASSERT_EQ(transform->GetId(), "rocksdb.CappedPrefix.42");

  ASSERT_OK(SliceTransform::CreateFromString(config_options, "rocksdb.Noop",
                                             &transform));
  ASSERT_NE(transform, nullptr);

  ASSERT_NOK(SliceTransform::CreateFromString(config_options,
                                              "fixed:21:invalid", &transform));
  ASSERT_NOK(SliceTransform::CreateFromString(config_options,
                                              "capped:21:invalid", &transform));
  ASSERT_NOK(
      SliceTransform::CreateFromString(config_options, "fixed", &transform));
  ASSERT_NOK(
      SliceTransform::CreateFromString(config_options, "capped", &transform));
  ASSERT_NOK(
      SliceTransform::CreateFromString(config_options, "fixed:", &transform));
  ASSERT_NOK(
      SliceTransform::CreateFromString(config_options, "capped:", &transform));
  ASSERT_NOK(SliceTransform::CreateFromString(
      config_options, "rocksdb.FixedPrefix:42", &transform));
  ASSERT_NOK(SliceTransform::CreateFromString(
      config_options, "rocksdb.CappedPrefix:42", &transform));
  ASSERT_NOK(SliceTransform::CreateFromString(
      config_options, "rocksdb.FixedPrefix", &transform));
  ASSERT_NOK(SliceTransform::CreateFromString(
      config_options, "rocksdb.CappedPrefix", &transform));
  ASSERT_NOK(SliceTransform::CreateFromString(
      config_options, "rocksdb.FixedPrefix.", &transform));
  ASSERT_NOK(SliceTransform::CreateFromString(
      config_options, "rocksdb.CappedPrefix.", &transform));
  ASSERT_NOK(
      SliceTransform::CreateFromString(config_options, "invalid", &transform));

  ASSERT_OK(SliceTransform::CreateFromString(
      config_options, "rocksdb.CappedPrefix.11", &transform));
  ASSERT_NE(transform, nullptr);
  ASSERT_EQ(transform->GetId(), "rocksdb.CappedPrefix.11");
  ASSERT_TRUE(transform->IsInstanceOf("capped"));
  ASSERT_TRUE(transform->IsInstanceOf("capped:11"));
  ASSERT_TRUE(transform->IsInstanceOf("rocksdb.CappedPrefix"));
  ASSERT_TRUE(transform->IsInstanceOf("rocksdb.CappedPrefix.11"));
  ASSERT_FALSE(transform->IsInstanceOf("fixed"));
  ASSERT_FALSE(transform->IsInstanceOf("fixed:11"));
  ASSERT_FALSE(transform->IsInstanceOf("rocksdb.FixedPrefix"));
  ASSERT_FALSE(transform->IsInstanceOf("rocksdb.FixedPrefix.11"));

  ASSERT_OK(SliceTransform::CreateFromString(
      config_options, "rocksdb.FixedPrefix.11", &transform));
  ASSERT_TRUE(transform->IsInstanceOf("fixed"));
  ASSERT_TRUE(transform->IsInstanceOf("fixed:11"));
  ASSERT_TRUE(transform->IsInstanceOf("rocksdb.FixedPrefix"));
  ASSERT_TRUE(transform->IsInstanceOf("rocksdb.FixedPrefix.11"));
  ASSERT_FALSE(transform->IsInstanceOf("capped"));
  ASSERT_FALSE(transform->IsInstanceOf("capped:11"));
  ASSERT_FALSE(transform->IsInstanceOf("rocksdb.CappedPrefix"));
  ASSERT_FALSE(transform->IsInstanceOf("rocksdb.CappedPrefix.11"));
}

TEST_F(OptionsOldApiTest, GetBlockBasedTableOptionsFromString) {
  BlockBasedTableOptions table_opt;
  BlockBasedTableOptions new_opt;
  ConfigOptions config_options;
  config_options.input_strings_escaped = false;
  config_options.ignore_unknown_options = false;
  config_options.invoke_prepare_options = false;
  config_options.ignore_unsupported_options = false;

  // make sure default values are overwritten by something else
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "cache_index_and_filter_blocks=1;index_type=kHashSearch;"
      "checksum=kxxHash;no_block_cache=1;"
      "block_cache=1M;block_cache_compressed=1k;block_size=1024;"
      "block_size_deviation=8;block_restart_interval=4;"
      "format_version=5;whole_key_filtering=1;"
      "filter_policy=bloomfilter:4.567:false;",
      &new_opt));
  ASSERT_TRUE(new_opt.cache_index_and_filter_blocks);
  ASSERT_EQ(new_opt.index_type, BlockBasedTableOptions::kHashSearch);
  ASSERT_EQ(new_opt.checksum, ChecksumType::kxxHash);
  ASSERT_TRUE(new_opt.no_block_cache);
  ASSERT_TRUE(new_opt.block_cache != nullptr);
  ASSERT_EQ(new_opt.block_cache->GetCapacity(), 1024UL * 1024UL);
  ASSERT_EQ(new_opt.block_size, 1024UL);
  ASSERT_EQ(new_opt.block_size_deviation, 8);
  ASSERT_EQ(new_opt.block_restart_interval, 4);
  ASSERT_EQ(new_opt.format_version, 5U);
  ASSERT_EQ(new_opt.whole_key_filtering, true);
  ASSERT_TRUE(new_opt.filter_policy != nullptr);
  const BloomFilterPolicy* bfp =
      dynamic_cast<const BloomFilterPolicy*>(new_opt.filter_policy.get());
  EXPECT_EQ(bfp->GetMillibitsPerKey(), 4567);
  EXPECT_EQ(bfp->GetWholeBitsPerKey(), 5);

  // unknown option
  ASSERT_NOK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "cache_index_and_filter_blocks=1;index_type=kBinarySearch;"
      "bad_option=1",
      &new_opt));
  ASSERT_EQ(static_cast<bool>(table_opt.cache_index_and_filter_blocks),
            new_opt.cache_index_and_filter_blocks);
  ASSERT_EQ(table_opt.index_type, new_opt.index_type);

  // unrecognized index type
  ASSERT_NOK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "cache_index_and_filter_blocks=1;index_type=kBinarySearchXX", &new_opt));
  ASSERT_EQ(table_opt.cache_index_and_filter_blocks,
            new_opt.cache_index_and_filter_blocks);
  ASSERT_EQ(table_opt.index_type, new_opt.index_type);

  // unrecognized checksum type
  ASSERT_NOK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "cache_index_and_filter_blocks=1;checksum=kxxHashXX", &new_opt));
  ASSERT_EQ(table_opt.cache_index_and_filter_blocks,
            new_opt.cache_index_and_filter_blocks);
  ASSERT_EQ(table_opt.index_type, new_opt.index_type);

  // unrecognized filter policy name
  ASSERT_NOK(
      GetBlockBasedTableOptionsFromString(config_options, table_opt,
                                          "cache_index_and_filter_blocks=1;"
                                          "filter_policy=bloomfilterxx:4:true",
                                          &new_opt));
  ASSERT_EQ(table_opt.cache_index_and_filter_blocks,
            new_opt.cache_index_and_filter_blocks);
  ASSERT_EQ(table_opt.filter_policy, new_opt.filter_policy);

  // Used to be rejected, now accepted
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt, "filter_policy=bloomfilter:4", &new_opt));
  bfp = dynamic_cast<const BloomFilterPolicy*>(new_opt.filter_policy.get());
  EXPECT_EQ(bfp->GetMillibitsPerKey(), 4000);
  EXPECT_EQ(bfp->GetWholeBitsPerKey(), 4);

  // Check block cache options are overwritten when specified
  // in new format as a struct.
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "block_cache={capacity=1M;num_shard_bits=4;"
      "strict_capacity_limit=true;high_pri_pool_ratio=0.5;};"
      "block_cache_compressed={capacity=1M;num_shard_bits=4;"
      "strict_capacity_limit=true;high_pri_pool_ratio=0.5;}",
      &new_opt));
  ASSERT_TRUE(new_opt.block_cache != nullptr);
  ASSERT_EQ(new_opt.block_cache->GetCapacity(), 1024UL * 1024UL);
  ASSERT_EQ(std::dynamic_pointer_cast<ShardedCacheBase>(new_opt.block_cache)
                ->GetNumShardBits(),
            4);
  ASSERT_EQ(new_opt.block_cache->HasStrictCapacityLimit(), true);
  ASSERT_EQ(std::dynamic_pointer_cast<LRUCache>(new_opt.block_cache)
                ->GetHighPriPoolRatio(),
            0.5);

  // Set only block cache capacity. Check other values are
  // reset to default values.
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "block_cache={capacity=2M};"
      "block_cache_compressed={capacity=2M}",
      &new_opt));
  ASSERT_TRUE(new_opt.block_cache != nullptr);
  ASSERT_EQ(new_opt.block_cache->GetCapacity(), 2 * 1024UL * 1024UL);
  // Default values
  ASSERT_EQ(std::dynamic_pointer_cast<ShardedCacheBase>(new_opt.block_cache)
                ->GetNumShardBits(),
            GetDefaultCacheShardBits(new_opt.block_cache->GetCapacity()));
  ASSERT_EQ(new_opt.block_cache->HasStrictCapacityLimit(), false);
  ASSERT_EQ(std::dynamic_pointer_cast<LRUCache>(new_opt.block_cache)
                ->GetHighPriPoolRatio(),
            0.5);

  // Set couple of block cache options.
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "block_cache={num_shard_bits=5;high_pri_pool_ratio=0.5;};"
      "block_cache_compressed={num_shard_bits=5;"
      "high_pri_pool_ratio=0.0;}",
      &new_opt));
  ASSERT_EQ(new_opt.block_cache->GetCapacity(), 0);
  ASSERT_EQ(std::dynamic_pointer_cast<ShardedCacheBase>(new_opt.block_cache)
                ->GetNumShardBits(),
            5);
  ASSERT_EQ(new_opt.block_cache->HasStrictCapacityLimit(), false);
  ASSERT_EQ(std::dynamic_pointer_cast<LRUCache>(new_opt.block_cache)
                ->GetHighPriPoolRatio(),
            0.5);

  // Set couple of block cache options.
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "block_cache={capacity=1M;num_shard_bits=4;"
      "strict_capacity_limit=true;};"
      "block_cache_compressed={capacity=1M;num_shard_bits=4;"
      "strict_capacity_limit=true;}",
      &new_opt));
  ASSERT_TRUE(new_opt.block_cache != nullptr);
  ASSERT_EQ(new_opt.block_cache->GetCapacity(), 1024UL * 1024UL);
  ASSERT_EQ(std::dynamic_pointer_cast<ShardedCacheBase>(new_opt.block_cache)
                ->GetNumShardBits(),
            4);
  ASSERT_EQ(new_opt.block_cache->HasStrictCapacityLimit(), true);
  ASSERT_EQ(std::dynamic_pointer_cast<LRUCache>(new_opt.block_cache)
                ->GetHighPriPoolRatio(),
            0.5);
}

TEST_F(OptionsOldApiTest, GetPlainTableOptionsFromString) {
  PlainTableOptions table_opt;
  PlainTableOptions new_opt;
  // make sure default values are overwritten by something else
  ConfigOptions config_options_from_string;
  config_options_from_string.input_strings_escaped = false;
  config_options_from_string.ignore_unknown_options = false;
  config_options_from_string.invoke_prepare_options = false;
  ASSERT_OK(GetPlainTableOptionsFromString(
      config_options_from_string, table_opt,
      "user_key_len=66;bloom_bits_per_key=20;hash_table_ratio=0.5;"
      "index_sparseness=8;huge_page_tlb_size=4;encoding_type=kPrefix;"
      "full_scan_mode=true;store_index_in_file=true",
      &new_opt));
  ASSERT_EQ(new_opt.user_key_len, 66u);
  ASSERT_EQ(new_opt.bloom_bits_per_key, 20);
  ASSERT_EQ(new_opt.hash_table_ratio, 0.5);
  ASSERT_EQ(new_opt.index_sparseness, 8);
  ASSERT_EQ(new_opt.huge_page_tlb_size, 4);
  ASSERT_EQ(new_opt.encoding_type, EncodingType::kPrefix);
  ASSERT_TRUE(new_opt.full_scan_mode);
  ASSERT_TRUE(new_opt.store_index_in_file);

  std::unordered_map<std::string, std::string> opt_map;
  ASSERT_OK(StringToMap(
      "user_key_len=55;bloom_bits_per_key=10;huge_page_tlb_size=8;", &opt_map));
  ConfigOptions config_options_from_map;
  config_options_from_map.input_strings_escaped = false;
  config_options_from_map.ignore_unknown_options = false;
  ASSERT_OK(GetPlainTableOptionsFromMap(config_options_from_map, table_opt,
                                        opt_map, &new_opt));
  ASSERT_EQ(new_opt.user_key_len, 55u);
  ASSERT_EQ(new_opt.bloom_bits_per_key, 10);
  ASSERT_EQ(new_opt.huge_page_tlb_size, 8);

  // unknown option
  ASSERT_NOK(GetPlainTableOptionsFromString(
      config_options_from_string, table_opt,
      "user_key_len=66;bloom_bits_per_key=20;hash_table_ratio=0.5;"
      "bad_option=1",
      &new_opt));

  // unrecognized EncodingType
  ASSERT_NOK(GetPlainTableOptionsFromString(
      config_options_from_string, table_opt,
      "user_key_len=66;bloom_bits_per_key=20;hash_table_ratio=0.5;"
      "encoding_type=kPrefixXX",
      &new_opt));
}

TEST_F(OptionsOldApiTest, GetOptionsFromStringTest) {
  Options base_options;
  Options new_options;
  base_options.write_buffer_size = 20;
  base_options.min_write_buffer_number_to_merge = 15;
  BlockBasedTableOptions block_based_table_options;
  block_based_table_options.cache_index_and_filter_blocks = true;
  base_options.table_factory.reset(
      NewBlockBasedTableFactory(block_based_table_options));

  // Register an Env with object registry.
  ObjectLibrary::Default()->AddFactory<Env>(
      "CustomEnvDefault",
      [](const std::string& /*name*/, std::unique_ptr<Env>* /*env_guard*/,
         std::string* /* errmsg */) {
        static CustomEnv env(Env::Default());
        return &env;
      });

  ASSERT_OK(GetOptionsFromString(
      base_options,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={block_cache=1M;block_size=4;};"
      "compression_opts=4:5:6;create_if_missing=true;max_open_files=1;"
      "bottommost_compression_opts=5:6:7;create_if_missing=true;max_open_files="
      "1;"
      "rate_limiter_bytes_per_sec=1024;env=CustomEnvDefault",
      &new_options));

  ASSERT_EQ(new_options.compression_opts.window_bits, 4);
  ASSERT_EQ(new_options.compression_opts.level, 5);
  ASSERT_EQ(new_options.compression_opts.strategy, 6);
  ASSERT_EQ(new_options.compression_opts.max_dict_bytes, 0u);
  ASSERT_EQ(new_options.compression_opts.zstd_max_train_bytes, 0u);
  ASSERT_EQ(new_options.compression_opts.parallel_threads, 1u);
  ASSERT_EQ(new_options.compression_opts.enabled, false);
  ASSERT_EQ(new_options.compression_opts.use_zstd_dict_trainer, true);
  ASSERT_EQ(new_options.bottommost_compression, kDisableCompressionOption);
  ASSERT_EQ(new_options.bottommost_compression_opts.window_bits, 5);
  ASSERT_EQ(new_options.bottommost_compression_opts.level, 6);
  ASSERT_EQ(new_options.bottommost_compression_opts.strategy, 7);
  ASSERT_EQ(new_options.bottommost_compression_opts.max_dict_bytes, 0u);
  ASSERT_EQ(new_options.bottommost_compression_opts.zstd_max_train_bytes, 0u);
  ASSERT_EQ(new_options.bottommost_compression_opts.parallel_threads, 1u);
  ASSERT_EQ(new_options.bottommost_compression_opts.enabled, false);
  ASSERT_EQ(new_options.bottommost_compression_opts.use_zstd_dict_trainer,
            true);
  ASSERT_EQ(new_options.write_buffer_size, 10U);
  ASSERT_EQ(new_options.max_write_buffer_number, 16);

  auto new_block_based_table_options =
      new_options.table_factory->GetOptions<BlockBasedTableOptions>();
  ASSERT_NE(new_block_based_table_options, nullptr);
  ASSERT_EQ(new_block_based_table_options->block_cache->GetCapacity(),
            1U << 20);
  ASSERT_EQ(new_block_based_table_options->block_size, 4U);
  // don't overwrite block based table options
  ASSERT_TRUE(new_block_based_table_options->cache_index_and_filter_blocks);

  ASSERT_EQ(new_options.create_if_missing, true);
  ASSERT_EQ(new_options.max_open_files, 1);
  ASSERT_TRUE(new_options.rate_limiter.get() != nullptr);
  Env* newEnv = new_options.env;
  ASSERT_OK(Env::CreateFromString({}, "CustomEnvDefault", &newEnv));
  ASSERT_EQ(newEnv, new_options.env);
}

TEST_F(OptionsOldApiTest, DBOptionsSerialization) {
  Options base_options;
  Options new_options;
  Random rnd(301);

  // Phase 1: Make big change in base_options
  test::RandomInitDBOptions(&base_options, &rnd);

  // Phase 2: obtain a string from base_option
  std::string base_options_file_content;
  ASSERT_OK(GetStringFromDBOptions(&base_options_file_content, base_options));

  // Phase 3: Set new_options from the derived string and expect
  //          new_options == base_options
  const DBOptions base_db_options;
  ConfigOptions db_config_options(base_db_options);
  db_config_options.input_strings_escaped = false;
  db_config_options.ignore_unknown_options = false;
  ASSERT_OK(GetDBOptionsFromString(db_config_options, base_db_options,
                                   base_options_file_content, &new_options));
  ConfigOptions verify_db_config_options;
  ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(verify_db_config_options,
                                                  base_options, new_options));
}

TEST_F(OptionsOldApiTest, ColumnFamilyOptionsSerialization) {
  Options options;
  ColumnFamilyOptions base_opt;
  ColumnFamilyOptions new_opt;
  Random rnd(302);
  // Phase 1: randomly assign base_opt
  // custom type options
  test::RandomInitCFOptions(&base_opt, options, &rnd);
  base_opt.memtable_verify_per_key_checksum_on_seek = true;

  // Phase 2: obtain a string from base_opt
  std::string base_options_file_content;
  ASSERT_OK(
      GetStringFromColumnFamilyOptions(&base_options_file_content, base_opt));
  ASSERT_EQ(base_options_file_content.find(
                "memtable_veirfy_per_key_checksum_on_seek="),
            std::string::npos);
  ASSERT_NE(base_options_file_content.find(
                "memtable_verify_per_key_checksum_on_seek="),
            std::string::npos);

  // Phase 3: Set new_opt from the derived string and expect
  //          new_opt == base_opt
  ConfigOptions cf_config_options;
  cf_config_options.input_strings_escaped = false;
  cf_config_options.ignore_unknown_options = false;
  ASSERT_OK(
      GetColumnFamilyOptionsFromString(cf_config_options, ColumnFamilyOptions(),
                                       base_options_file_content, &new_opt));
  ConfigOptions verify_cf_config_options;
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(verify_cf_config_options,
                                                  base_opt, new_opt));
  if (base_opt.compaction_filter) {
    delete base_opt.compaction_filter;
  }
}

class OptionsParserTest : public testing::Test {
 public:
  OptionsParserTest() { fs_.reset(new test::StringFS(FileSystem::Default())); }

 protected:
  std::shared_ptr<test::StringFS> fs_;
};

TEST_F(OptionsParserTest, Comment) {
  DBOptions db_opt;
  db_opt.max_open_files = 12345;
  db_opt.max_background_flushes = 301;
  db_opt.max_total_wal_size = 1024;
  ColumnFamilyOptions cf_opt;

  std::string options_file_content =
      "# This is a testing option string.\n"
      "# Currently we only support \"#\" styled comment.\n"
      "\n"
      "[Version]\n"
      "  rocksdb_version=3.14.0\n"
      "  options_file_version=1\n"
      "[ DBOptions ]\n"
      "  # note that we don't support space around \"=\"\n"
      "  max_open_files=12345;\n"
      "  max_background_flushes=301  # comment after a statement is fine\n"
      "  # max_background_flushes=1000  # this line would be ignored\n"
      "  # max_background_compactions=2000 # so does this one\n"
      "  max_total_wal_size=1024  # keep_log_file_num=1000\n"
      "[CFOptions   \"default\"]  # column family must be specified\n"
      "                     # in the correct order\n"
      "  # if a section is blank, we will use the default\n";

  const std::string kTestFileName = "test-rocksdb-options.ini";
  ASSERT_OK(fs_->WriteToNewFile(kTestFileName, options_file_content));
  RocksDBOptionsParser parser;
  ASSERT_OK(
      parser.Parse(kTestFileName, fs_.get(), false, 4096 /* readahead_size */));

  ConfigOptions exact;
  exact.input_strings_escaped = false;
  exact.sanity_level = ConfigOptions::kSanityLevelExactMatch;
  ASSERT_OK(
      RocksDBOptionsParser::VerifyDBOptions(exact, *parser.db_opt(), db_opt));
  ASSERT_EQ(parser.NumColumnFamilies(), 1U);
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(
      exact, *parser.GetCFOptions("default"), cf_opt));
}

TEST_F(OptionsParserTest, ExtraSpace) {
  std::string options_file_content =
      "# This is a testing option string.\n"
      "# Currently we only support \"#\" styled comment.\n"
      "\n"
      "[      Version   ]\n"
      "  rocksdb_version     = 3.14.0      \n"
      "  options_file_version=1   # some comment\n"
      "[DBOptions  ]  # some comment\n"
      "max_open_files=12345   \n"
      "    max_background_flushes   =    301   \n"
      " max_total_wal_size     =   1024  # keep_log_file_num=1000\n"
      "        [CFOptions      \"default\"     ]\n"
      "  # if a section is blank, we will use the default\n";

  const std::string kTestFileName = "test-rocksdb-options.ini";
  ASSERT_OK(fs_->WriteToNewFile(kTestFileName, options_file_content));
  RocksDBOptionsParser parser;
  ASSERT_OK(
      parser.Parse(kTestFileName, fs_.get(), false, 4096 /* readahead_size */));
}

TEST_F(OptionsParserTest, MissingDBOptions) {
  std::string options_file_content =
      "# This is a testing option string.\n"
      "# Currently we only support \"#\" styled comment.\n"
      "\n"
      "[Version]\n"
      "  rocksdb_version=3.14.0\n"
      "  options_file_version=1\n"
      "[CFOptions \"default\"]\n"
      "  # if a section is blank, we will use the default\n";

  const std::string kTestFileName = "test-rocksdb-options.ini";
  ASSERT_OK(fs_->WriteToNewFile(kTestFileName, options_file_content));
  RocksDBOptionsParser parser;
  ASSERT_NOK(
      parser.Parse(kTestFileName, fs_.get(), false, 4096 /* readahead_size */));
  ;
}

TEST_F(OptionsParserTest, DoubleDBOptions) {
  DBOptions db_opt;
  db_opt.max_open_files = 12345;
  db_opt.max_background_flushes = 301;
  db_opt.max_total_wal_size = 1024;
  ColumnFamilyOptions cf_opt;

  std::string options_file_content =
      "# This is a testing option string.\n"
      "# Currently we only support \"#\" styled comment.\n"
      "\n"
      "[Version]\n"
      "  rocksdb_version=3.14.0\n"
      "  options_file_version=1\n"
      "[DBOptions]\n"
      "  max_open_files=12345\n"
      "  max_background_flushes=301\n"
      "  max_total_wal_size=1024  # keep_log_file_num=1000\n"
      "[DBOptions]\n"
      "[CFOptions \"default\"]\n"
      "  # if a section is blank, we will use the default\n";

  const std::string kTestFileName = "test-rocksdb-options.ini";
  ASSERT_OK(fs_->WriteToNewFile(kTestFileName, options_file_content));
  RocksDBOptionsParser parser;
  ASSERT_NOK(
      parser.Parse(kTestFileName, fs_.get(), false, 4096 /* readahead_size */));
}

TEST_F(OptionsParserTest, NoDefaultCFOptions) {
  DBOptions db_opt;
  db_opt.max_open_files = 12345;
  db_opt.max_background_flushes = 301;
  db_opt.max_total_wal_size = 1024;
  ColumnFamilyOptions cf_opt;

  std::string options_file_content =
      "# This is a testing option string.\n"
      "# Currently we only support \"#\" styled comment.\n"
      "\n"
      "[Version]\n"
      "  rocksdb_version=3.14.0\n"
      "  options_file_version=1\n"
      "[DBOptions]\n"
      "  max_open_files=12345\n"
      "  max_background_flushes=301\n"
      "  max_total_wal_size=1024  # keep_log_file_num=1000\n"
      "[CFOptions \"something_else\"]\n"
      "  # if a section is blank, we will use the default\n";

  const std::string kTestFileName = "test-rocksdb-options.ini";
  ASSERT_OK(fs_->WriteToNewFile(kTestFileName, options_file_content));
  RocksDBOptionsParser parser;
  ASSERT_NOK(
      parser.Parse(kTestFileName, fs_.get(), false, 4096 /* readahead_size */));
}

TEST_F(OptionsParserTest, DefaultCFOptionsMustBeTheFirst) {
  DBOptions db_opt;
  db_opt.max_open_files = 12345;
  db_opt.max_background_flushes = 301;
  db_opt.max_total_wal_size = 1024;
  ColumnFamilyOptions cf_opt;

  std::string options_file_content =
      "# This is a testing option string.\n"
      "# Currently we only support \"#\" styled comment.\n"
      "\n"
      "[Version]\n"
      "  rocksdb_version=3.14.0\n"
      "  options_file_version=1\n"
      "[DBOptions]\n"
      "  max_open_files=12345\n"
      "  max_background_flushes=301\n"
      "  max_total_wal_size=1024  # keep_log_file_num=1000\n"
      "[CFOptions \"something_else\"]\n"
      "  # if a section is blank, we will use the default\n"
      "[CFOptions \"default\"]\n"
      "  # if a section is blank, we will use the default\n";

  const std::string kTestFileName = "test-rocksdb-options.ini";
  ASSERT_OK(fs_->WriteToNewFile(kTestFileName, options_file_content));
  RocksDBOptionsParser parser;
  ASSERT_NOK(
      parser.Parse(kTestFileName, fs_.get(), false, 4096 /* readahead_size */));
}

TEST_F(OptionsParserTest, DuplicateCFOptions) {
  DBOptions db_opt;
  db_opt.max_open_files = 12345;
  db_opt.max_background_flushes = 301;
  db_opt.max_total_wal_size = 1024;
  ColumnFamilyOptions cf_opt;

  std::string options_file_content =
      "# This is a testing option string.\n"
      "# Currently we only support \"#\" styled comment.\n"
      "\n"
      "[Version]\n"
      "  rocksdb_version=3.14.0\n"
      "  options_file_version=1\n"
      "[DBOptions]\n"
      "  max_open_files=12345\n"
      "  max_background_flushes=301\n"
      "  max_total_wal_size=1024  # keep_log_file_num=1000\n"
      "[CFOptions \"default\"]\n"
      "[CFOptions \"something_else\"]\n"
      "[CFOptions \"something_else\"]\n";

  const std::string kTestFileName = "test-rocksdb-options.ini";
  ASSERT_OK(fs_->WriteToNewFile(kTestFileName, options_file_content));
  RocksDBOptionsParser parser;
  ASSERT_NOK(
      parser.Parse(kTestFileName, fs_.get(), false, 4096 /* readahead_size */));
}

TEST_F(OptionsParserTest, IgnoreUnknownOptions) {
  auto testCase = [&](bool should_ignore, const std::string& version_string) {
    SCOPED_TRACE(std::to_string(should_ignore) + ", " + version_string);

    std::string options_file_content =
        "# This is a testing option string.\n"
        "# Currently we only support \"#\" styled comment.\n"
        "\n"
        "[Version]\n"
        "  rocksdb_version=" +
        version_string +
        "\n"
        "  options_file_version=1\n"
        "[DBOptions]\n"
        "  max_open_files=12345\n"
        "  max_background_flushes=301\n"
        "  max_total_wal_size=1024  # keep_log_file_num=1000\n"
        "  unknown_db_option1=321\n"
        "  unknown_db_option2=false\n"
        "[CFOptions \"default\"]\n"
        "  unknown_cf_option1=hello\n"
        "[CFOptions \"something_else\"]\n"
        "  unknown_cf_option2=world\n"
        "  # if a section is blank, we will use the default\n";

    const std::string kTestFileName = "test-rocksdb-options.ini";
    auto s = fs_->FileExists(kTestFileName, IOOptions(), nullptr);
    ASSERT_TRUE(s.ok() || s.IsNotFound());
    if (s.ok()) {
      ASSERT_OK(fs_->DeleteFile(kTestFileName, IOOptions(), nullptr));
    }
    ASSERT_OK(fs_->WriteToNewFile(kTestFileName, options_file_content));
    RocksDBOptionsParser parser;
    ASSERT_NOK(parser.Parse(kTestFileName, fs_.get(), false,
                            4096 /* readahead_size */));
    Status parse_status = parser.Parse(kTestFileName, fs_.get(),
                                       true /* ignore_unknown_options */,
                                       4096 /* readahead_size */);
    if (should_ignore) {
      ASSERT_OK(parse_status);
    } else {
      ASSERT_NOK(parse_status);
    }
  };

  // Same version
  testCase(false, GetRocksVersionAsString());
  // Same except .0 patch
  testCase(false, std::to_string(ROCKSDB_MAJOR) + "." +
                      std::to_string(ROCKSDB_MINOR) + ".0");
  // Higher major version
  testCase(true, std::to_string(ROCKSDB_MAJOR + 1) + "." +
                     std::to_string(ROCKSDB_MINOR) + ".0");
  // Higher minor version
  testCase(true, std::to_string(ROCKSDB_MAJOR) + "." +
                     std::to_string(ROCKSDB_MINOR + 1) + ".0");
  // Higher patch version
  testCase(true, std::to_string(ROCKSDB_MAJOR) + "." +
                     std::to_string(ROCKSDB_MINOR) + "." +
                     std::to_string(ROCKSDB_PATCH + 1));
  // Lower major version
  testCase(false, std::to_string(ROCKSDB_MAJOR - 1) + "." +
                      std::to_string(ROCKSDB_MINOR) + ".0");
#if ROCKSDB_MINOR > 0
  // Lower minor version
  testCase(false, std::to_string(ROCKSDB_MAJOR) + "." +
                      std::to_string(ROCKSDB_MINOR - 1) + ".0");
#endif
#if ROCKSDB_PATCH > 0
  // Lower patch version
  testCase(false, std::to_string(ROCKSDB_MAJOR) + "." +
                      std::to_string(ROCKSDB_MINOR - 1) + "." +
                      std::to_string(ROCKSDB_PATCH - 1));
#endif
}

TEST_F(OptionsParserTest, ParseVersion) {
  DBOptions db_opt;
  db_opt.max_open_files = 12345;
  db_opt.max_background_flushes = 301;
  db_opt.max_total_wal_size = 1024;
  ColumnFamilyOptions cf_opt;

  std::string file_template =
      "# This is a testing option string.\n"
      "# Currently we only support \"#\" styled comment.\n"
      "\n"
      "[Version]\n"
      "  rocksdb_version=3.13.1\n"
      "  options_file_version=%s\n"
      "[DBOptions]\n"
      "[CFOptions \"default\"]\n";
  const int kLength = 1000;
  char buffer[kLength];
  RocksDBOptionsParser parser;

  const std::vector<std::string> invalid_versions = {
      "a.b.c",
      "3.2.2b",
      "3.-12",
      "3. 1",  // only digits and dots are allowed
      "1.2.3.4",
      "1.2.3"  // can only contains at most one dot.
      "0",     // options_file_version must be at least one
      "3..2",
      ".",
      ".1.2",  // must have at least one digit before each dot
      "1.2.",
      "1.",
      "2.34."};  // must have at least one digit after each dot
  for (const auto& iv : invalid_versions) {
    snprintf(buffer, kLength - 1, file_template.c_str(), iv.c_str());

    parser.Reset();
    ASSERT_OK(fs_->WriteToNewFile(iv, buffer));
    ASSERT_NOK(parser.Parse(iv, fs_.get(), false, 0 /* readahead_size */));
  }

  const std::vector<std::string> valid_versions = {
      "1.232", "100", "3.12", "1", "12.3  ", "  1.25  "};
  for (const auto& vv : valid_versions) {
    snprintf(buffer, kLength - 1, file_template.c_str(), vv.c_str());
    parser.Reset();
    ASSERT_OK(fs_->WriteToNewFile(vv, buffer));
    ASSERT_OK(parser.Parse(vv, fs_.get(), false, 0 /* readahead_size */));
  }
}

void VerifyCFPointerTypedOptions(
    ColumnFamilyOptions* base_cf_opt, const ColumnFamilyOptions* new_cf_opt,
    const std::unordered_map<std::string, std::string>* new_cf_opt_map) {
  std::string name_buffer;
  ConfigOptions config_options;
  config_options.input_strings_escaped = false;
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, *base_cf_opt,
                                                  *new_cf_opt, new_cf_opt_map));

  // change the name of merge operator back-and-forth
  {
    auto* merge_operator = base_cf_opt->merge_operator
                               ->CheckedCast<test::ChanglingMergeOperator>();
    if (merge_operator != nullptr) {
      name_buffer = merge_operator->Name();
      // change the name  and expect non-ok status
      merge_operator->SetName("some-other-name");
      ASSERT_NOK(RocksDBOptionsParser::VerifyCFOptions(
          config_options, *base_cf_opt, *new_cf_opt, new_cf_opt_map));
      // change the name back and expect ok status
      merge_operator->SetName(name_buffer);
      ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(
          config_options, *base_cf_opt, *new_cf_opt, new_cf_opt_map));
    }
  }

  // change the name of the compaction filter factory back-and-forth
  {
    auto* compaction_filter_factory =
        base_cf_opt->compaction_filter_factory
            ->CheckedCast<test::ChanglingCompactionFilterFactory>();
    if (compaction_filter_factory != nullptr) {
      name_buffer = compaction_filter_factory->Name();
      // change the name and expect non-ok status
      compaction_filter_factory->SetName("some-other-name");
      ASSERT_NOK(RocksDBOptionsParser::VerifyCFOptions(
          config_options, *base_cf_opt, *new_cf_opt, new_cf_opt_map));
      // change the name back and expect ok status
      compaction_filter_factory->SetName(name_buffer);
      ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(
          config_options, *base_cf_opt, *new_cf_opt, new_cf_opt_map));
    }
  }

  // test by setting compaction_filter to nullptr
  {
    auto* tmp_compaction_filter = base_cf_opt->compaction_filter;
    if (tmp_compaction_filter != nullptr) {
      base_cf_opt->compaction_filter = nullptr;
      // set compaction_filter to nullptr and expect non-ok status
      ASSERT_NOK(RocksDBOptionsParser::VerifyCFOptions(
          config_options, *base_cf_opt, *new_cf_opt, new_cf_opt_map));
      // set the value back and expect ok status
      base_cf_opt->compaction_filter = tmp_compaction_filter;
      ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(
          config_options, *base_cf_opt, *new_cf_opt, new_cf_opt_map));
    }
  }

  // test by setting table_factory to nullptr
  {
    auto tmp_table_factory = base_cf_opt->table_factory;
    if (tmp_table_factory != nullptr) {
      base_cf_opt->table_factory.reset();
      // set table_factory to nullptr and expect non-ok status
      ASSERT_NOK(RocksDBOptionsParser::VerifyCFOptions(
          config_options, *base_cf_opt, *new_cf_opt, new_cf_opt_map));
      // set the value back and expect ok status
      base_cf_opt->table_factory = tmp_table_factory;
      ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(
          config_options, *base_cf_opt, *new_cf_opt, new_cf_opt_map));
    }
  }

  // test by setting memtable_factory to nullptr
  {
    auto tmp_memtable_factory = base_cf_opt->memtable_factory;
    if (tmp_memtable_factory != nullptr) {
      base_cf_opt->memtable_factory.reset();
      // set memtable_factory to nullptr and expect non-ok status
      ASSERT_NOK(RocksDBOptionsParser::VerifyCFOptions(
          config_options, *base_cf_opt, *new_cf_opt, new_cf_opt_map));
      // set the value back and expect ok status
      base_cf_opt->memtable_factory = tmp_memtable_factory;
      ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(
          config_options, *base_cf_opt, *new_cf_opt, new_cf_opt_map));
    }
  }
}

TEST_F(OptionsParserTest, RemoteCompactionPoolNotPersisted) {
  DBOptions options;
  options.use_separate_remote_compaction_pool = true;
  const std::string file_name = "remote-compaction-pool-options.ini";
  ASSERT_OK(PersistRocksDBOptions(WriteOptions(), options, {"default"},
                                  {ColumnFamilyOptions()}, file_name,
                                  fs_.get()));
  RocksDBOptionsParser parser;
  ConfigOptions config_options;
  ASSERT_OK(parser.Parse(config_options, file_name, fs_.get()));
  ASSERT_EQ(parser.db_opt_map()->count("use_separate_remote_compaction_pool"),
            0U);
  ASSERT_FALSE(parser.db_opt()->use_separate_remote_compaction_pool);
}

TEST_F(OptionsParserTest, Readahead) {
  DBOptions base_db_opt;
  std::vector<ColumnFamilyOptions> base_cf_opts;
  base_cf_opts.emplace_back();
  base_cf_opts.emplace_back();

  std::string one_mb_string = std::string(1024 * 1024, 'x');
  std::vector<std::string> cf_names = {"default", one_mb_string};
  const std::string kOptionsFileName = "test-persisted-options.ini";

  ASSERT_OK(PersistRocksDBOptions(WriteOptions(), base_db_opt, cf_names,
                                  base_cf_opts, kOptionsFileName, fs_.get()));

  uint64_t file_size = 0;
  ASSERT_OK(
      fs_->GetFileSize(kOptionsFileName, IOOptions(), &file_size, nullptr));
  assert(file_size > 0);

  RocksDBOptionsParser parser;

  fs_->num_seq_file_read_ = 0;
  size_t readahead_size = 128 * 1024;

  ASSERT_OK(parser.Parse(kOptionsFileName, fs_.get(), false, readahead_size));
  ASSERT_EQ(fs_->num_seq_file_read_.load(),
            (file_size - 1) / readahead_size + 1);

  fs_->num_seq_file_read_.store(0);
  readahead_size = 1024 * 1024;
  ASSERT_OK(parser.Parse(kOptionsFileName, fs_.get(), false, readahead_size));
  ASSERT_EQ(fs_->num_seq_file_read_.load(),
            (file_size - 1) / readahead_size + 1);

  // Tiny readahead. 8 KB is read each time.
  fs_->num_seq_file_read_.store(0);
  ASSERT_OK(
      parser.Parse(kOptionsFileName, fs_.get(), false, 1 /* readahead_size */));
  ASSERT_GE(fs_->num_seq_file_read_.load(), file_size / (8 * 1024));
  ASSERT_LT(fs_->num_seq_file_read_.load(), file_size / (8 * 1024) * 2);

  // Disable readahead means 512KB readahead.
  fs_->num_seq_file_read_.store(0);
  ASSERT_OK(
      parser.Parse(kOptionsFileName, fs_.get(), false, 0 /* readahead_size */));
  ASSERT_GE(fs_->num_seq_file_read_.load(), (file_size - 1) / (512 * 1024) + 1);
}

TEST_F(OptionsParserTest, DumpAndParse) {
  DBOptions base_db_opt;
  std::vector<ColumnFamilyOptions> base_cf_opts;
  std::vector<std::string> cf_names = {"default",
                                       "cf1",
                                       "cf2",
                                       "cf3",
                                       "c:f:4:4:4"
                                       "p\\i\\k\\a\\chu\\\\\\",
                                       "###rocksdb#1-testcf#2###"};
  const int num_cf = static_cast<int>(cf_names.size());
  Random rnd(302);
  test::RandomInitDBOptions(&base_db_opt, &rnd);
  base_db_opt.db_log_dir += "/#odd #but #could #happen #path #/\\\\#OMG";

  BlockBasedTableOptions special_bbto;
  special_bbto.cache_index_and_filter_blocks = true;
  special_bbto.block_size = 999999;

  for (int c = 0; c < num_cf; ++c) {
    ColumnFamilyOptions cf_opt;
    Random cf_rnd(0xFB + c);
    test::RandomInitCFOptions(&cf_opt, base_db_opt, &cf_rnd);
    if (c < 4) {
      cf_opt.prefix_extractor.reset(test::RandomSliceTransform(&rnd, c));
    }
    if (c < 3) {
      cf_opt.table_factory.reset(test::RandomTableFactory(&rnd, c));
    } else if (c == 4) {
      cf_opt.table_factory.reset(NewBlockBasedTableFactory(special_bbto));
    } else if (c == 5) {
      // A table factory that doesn't support deserialization should be
      // supported.
      cf_opt.table_factory.reset(new UnregisteredTableFactory());
    }
    base_cf_opts.emplace_back(cf_opt);
  }

  const std::string kOptionsFileName = "test-persisted-options.ini";
  // Use default for escaped(true), unknown(false) and check (exact)
  ConfigOptions config_options;
  ASSERT_OK(PersistRocksDBOptions(WriteOptions(), base_db_opt, cf_names,
                                  base_cf_opts, kOptionsFileName, fs_.get()));

  RocksDBOptionsParser parser;
  ASSERT_OK(parser.Parse(config_options, kOptionsFileName, fs_.get()));

  // Make sure block-based table factory options was deserialized correctly
  std::shared_ptr<TableFactory> ttf = (*parser.cf_opts())[4].table_factory;
  ASSERT_EQ(TableFactory::kBlockBasedTableName(), std::string(ttf->Name()));
  const auto parsed_bbto = ttf->GetOptions<BlockBasedTableOptions>();
  ASSERT_NE(parsed_bbto, nullptr);
  ASSERT_EQ(special_bbto.block_size, parsed_bbto->block_size);
  ASSERT_EQ(special_bbto.cache_index_and_filter_blocks,
            parsed_bbto->cache_index_and_filter_blocks);

  ASSERT_OK(RocksDBOptionsParser::VerifyRocksDBOptionsFromFile(
      config_options, base_db_opt, cf_names, base_cf_opts, kOptionsFileName,
      fs_.get()));

  ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(
      config_options, *parser.db_opt(), base_db_opt));
  for (int c = 0; c < num_cf; ++c) {
    const auto* cf_opt = parser.GetCFOptions(cf_names[c]);
    ASSERT_NE(cf_opt, nullptr);
    ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(
        config_options, base_cf_opts[c], *cf_opt,
        &(parser.cf_opt_maps()->at(c))));
  }

  // Further verify pointer-typed options
  for (int c = 0; c < num_cf; ++c) {
    const auto* cf_opt = parser.GetCFOptions(cf_names[c]);
    ASSERT_NE(cf_opt, nullptr);
    VerifyCFPointerTypedOptions(&base_cf_opts[c], cf_opt,
                                &(parser.cf_opt_maps()->at(c)));
  }

  ASSERT_EQ(parser.GetCFOptions("does not exist"), nullptr);

  base_db_opt.max_open_files++;
  ASSERT_NOK(RocksDBOptionsParser::VerifyRocksDBOptionsFromFile(
      config_options, base_db_opt, cf_names, base_cf_opts, kOptionsFileName,
      fs_.get()));

  for (int c = 0; c < num_cf; ++c) {
    if (base_cf_opts[c].compaction_filter) {
      delete base_cf_opts[c].compaction_filter;
    }
  }
}

TEST_F(OptionsParserTest, DifferentDefault) {
  const std::string kOptionsFileName = "test-persisted-options.ini";

  ColumnFamilyOptions cf_level_opts;
  ASSERT_EQ(CompactionPri::kMinOverlappingRatio, cf_level_opts.compaction_pri);
  cf_level_opts.OptimizeLevelStyleCompaction();

  ColumnFamilyOptions cf_univ_opts;
  cf_univ_opts.OptimizeUniversalStyleCompaction();

  ASSERT_OK(PersistRocksDBOptions(
      WriteOptions(), DBOptions(), {"default", "universal"},
      {cf_level_opts, cf_univ_opts}, kOptionsFileName, fs_.get()));

  RocksDBOptionsParser parser;
  ASSERT_OK(parser.Parse(kOptionsFileName, fs_.get(), false,
                         4096 /* readahead_size */));

  {
    Options old_default_opts;
    old_default_opts.OldDefaults();
    ASSERT_EQ(10 * 1048576, old_default_opts.max_bytes_for_level_base);
    ASSERT_EQ(5000, old_default_opts.max_open_files);
    ASSERT_EQ(2 * 1024U * 1024U, old_default_opts.delayed_write_rate);
    ASSERT_EQ(WALRecoveryMode::kTolerateCorruptedTailRecords,
              old_default_opts.wal_recovery_mode);
  }
  {
    Options old_default_opts;
    old_default_opts.OldDefaults(4, 6);
    ASSERT_EQ(10 * 1048576, old_default_opts.max_bytes_for_level_base);
    ASSERT_EQ(5000, old_default_opts.max_open_files);
  }
  {
    Options old_default_opts;
    old_default_opts.OldDefaults(4, 7);
    ASSERT_NE(10 * 1048576, old_default_opts.max_bytes_for_level_base);
    ASSERT_NE(4, old_default_opts.table_cache_numshardbits);
    ASSERT_EQ(5000, old_default_opts.max_open_files);
    ASSERT_EQ(2 * 1024U * 1024U, old_default_opts.delayed_write_rate);
  }
  {
    ColumnFamilyOptions old_default_cf_opts;
    old_default_cf_opts.OldDefaults();
    ASSERT_EQ(2 * 1048576, old_default_cf_opts.target_file_size_base);
    ASSERT_EQ(4 << 20, old_default_cf_opts.write_buffer_size);
    ASSERT_EQ(2 * 1048576, old_default_cf_opts.target_file_size_base);
    ASSERT_EQ(0, old_default_cf_opts.soft_pending_compaction_bytes_limit);
    ASSERT_EQ(0, old_default_cf_opts.hard_pending_compaction_bytes_limit);
    ASSERT_EQ(CompactionPri::kByCompensatedSize,
              old_default_cf_opts.compaction_pri);
  }
  {
    ColumnFamilyOptions old_default_cf_opts;
    old_default_cf_opts.OldDefaults(4, 6);
    ASSERT_EQ(2 * 1048576, old_default_cf_opts.target_file_size_base);
    ASSERT_EQ(CompactionPri::kByCompensatedSize,
              old_default_cf_opts.compaction_pri);
  }
  {
    ColumnFamilyOptions old_default_cf_opts;
    old_default_cf_opts.OldDefaults(4, 7);
    ASSERT_NE(2 * 1048576, old_default_cf_opts.target_file_size_base);
    ASSERT_EQ(CompactionPri::kByCompensatedSize,
              old_default_cf_opts.compaction_pri);
  }
  {
    Options old_default_opts;
    old_default_opts.OldDefaults(5, 1);
    ASSERT_EQ(2 * 1024U * 1024U, old_default_opts.delayed_write_rate);
  }
  {
    Options old_default_opts;
    old_default_opts.OldDefaults(5, 2);
    ASSERT_EQ(16 * 1024U * 1024U, old_default_opts.delayed_write_rate);
    ASSERT_TRUE(old_default_opts.compaction_pri ==
                CompactionPri::kByCompensatedSize);
  }
  {
    Options old_default_opts;
    old_default_opts.OldDefaults(5, 18);
    ASSERT_TRUE(old_default_opts.compaction_pri ==
                CompactionPri::kByCompensatedSize);
  }

  Options small_opts;
  small_opts.OptimizeForSmallDb();
  ASSERT_EQ(2 << 20, small_opts.write_buffer_size);
  ASSERT_EQ(5000, small_opts.max_open_files);
}

class OptionsSanityCheckTest : public OptionsParserTest,
                               public ::testing::WithParamInterface<bool> {
 protected:
  ConfigOptions config_options_;

 public:
  OptionsSanityCheckTest() {
    config_options_.ignore_unknown_options = false;
    config_options_.ignore_unsupported_options = GetParam();
    config_options_.input_strings_escaped = true;
  }

 protected:
  Status SanityCheckOptions(const DBOptions& db_opts,
                            const ColumnFamilyOptions& cf_opts,
                            ConfigOptions::SanityLevel level) {
    config_options_.sanity_level = level;
    return RocksDBOptionsParser::VerifyRocksDBOptionsFromFile(
        config_options_, db_opts, {"default"}, {cf_opts}, kOptionsFileName,
        fs_.get());
  }

  Status SanityCheckCFOptions(const ColumnFamilyOptions& cf_opts,
                              ConfigOptions::SanityLevel level) {
    return SanityCheckOptions(DBOptions(), cf_opts, level);
  }

  void SanityCheckCFOptions(const ColumnFamilyOptions& opts, bool exact) {
    ASSERT_OK(SanityCheckCFOptions(
        opts, ConfigOptions::kSanityLevelLooselyCompatible));
    ASSERT_OK(SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelNone));
    if (exact) {
      ASSERT_OK(
          SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));
    } else {
      ASSERT_NOK(
          SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));
    }
  }

  Status SanityCheckDBOptions(const DBOptions& db_opts,
                              ConfigOptions::SanityLevel level) {
    return SanityCheckOptions(db_opts, ColumnFamilyOptions(), level);
  }

  void SanityCheckDBOptions(const DBOptions& opts, bool exact) {
    ASSERT_OK(SanityCheckDBOptions(
        opts, ConfigOptions::kSanityLevelLooselyCompatible));
    ASSERT_OK(SanityCheckDBOptions(opts, ConfigOptions::kSanityLevelNone));
    if (exact) {
      ASSERT_OK(
          SanityCheckDBOptions(opts, ConfigOptions::kSanityLevelExactMatch));
    } else {
      ASSERT_NOK(
          SanityCheckDBOptions(opts, ConfigOptions::kSanityLevelExactMatch));
    }
  }

  Status PersistOptions(const DBOptions& db_opts,
                        const ColumnFamilyOptions& cf_opts) {
    Status s = fs_->DeleteFile(kOptionsFileName, IOOptions(), nullptr);
    if (!s.ok()) {
      return s;
    }
    return PersistRocksDBOptions(WriteOptions(), db_opts, {"default"},
                                 {cf_opts}, kOptionsFileName, fs_.get());
  }

  Status PersistCFOptions(const ColumnFamilyOptions& cf_opts) {
    return PersistOptions(DBOptions(), cf_opts);
  }

  Status PersistDBOptions(const DBOptions& db_opts) {
    return PersistOptions(db_opts, ColumnFamilyOptions());
  }

  const std::string kOptionsFileName = "OPTIONS";
};

TEST_P(OptionsSanityCheckTest, MergeOperatorErrorMessage) {
  ColumnFamilyOptions opts;
  Random rnd(301);
  opts.merge_operator.reset(test::RandomMergeOperator(&rnd));
  std::string merge_op_name = opts.merge_operator->Name();
  ASSERT_OK(PersistCFOptions(opts));

  // Test when going from merge operator -> nullptr
  opts.merge_operator = nullptr;
  Status s =
      SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelLooselyCompatible);
  ASSERT_TRUE(s.IsInvalidArgument());
  std::string err_msg = s.ToString();
  std::string specified = "The specified one is " + kNullptrString;
  std::string persisted = "the persisted one is " + merge_op_name;
  ASSERT_TRUE(err_msg.find(specified) != std::string::npos);
  ASSERT_TRUE(err_msg.find(persisted) != std::string::npos);

  // Test when using a different merge operator
  opts.merge_operator.reset(test::RandomMergeOperator(&rnd));
  s = SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelLooselyCompatible);
  ASSERT_TRUE(s.IsInvalidArgument());
  err_msg = s.ToString();
  specified =
      "The specified one is " + std::string(opts.merge_operator->Name());
  persisted = "the persisted one is " + merge_op_name;
  ASSERT_TRUE(err_msg.find(specified) != std::string::npos);
  ASSERT_TRUE(err_msg.find(persisted) != std::string::npos);
}

TEST_P(OptionsSanityCheckTest, CFOptionsSanityCheck) {
  ColumnFamilyOptions opts;
  Random rnd(301);

  // default ColumnFamilyOptions
  {
    ASSERT_OK(PersistCFOptions(opts));
    ASSERT_OK(
        SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));
  }

  // prefix_extractor
  {
    // Okay to change prefix_extractor form nullptr to non-nullptr
    ASSERT_EQ(opts.prefix_extractor.get(), nullptr);
    opts.prefix_extractor.reset(NewCappedPrefixTransform(10));
    ASSERT_OK(SanityCheckCFOptions(
        opts, ConfigOptions::kSanityLevelLooselyCompatible));
    ASSERT_OK(SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelNone));

    // persist the change
    ASSERT_OK(PersistCFOptions(opts));
    ASSERT_OK(
        SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));

    // use same prefix extractor but with different parameter
    opts.prefix_extractor.reset(NewCappedPrefixTransform(15));
    // expect pass only in
    // ConfigOptions::kSanityLevelLooselyCompatible
    ASSERT_NOK(
        SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));
    ASSERT_OK(SanityCheckCFOptions(
        opts, ConfigOptions::kSanityLevelLooselyCompatible));
    ASSERT_OK(SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelNone));

    // repeat the test with FixedPrefixTransform
    opts.prefix_extractor.reset(NewFixedPrefixTransform(10));
    ASSERT_NOK(
        SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));
    ASSERT_OK(SanityCheckCFOptions(
        opts, ConfigOptions::kSanityLevelLooselyCompatible));
    ASSERT_OK(SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelNone));

    // persist the change of prefix_extractor
    ASSERT_OK(PersistCFOptions(opts));
    ASSERT_OK(
        SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));

    // use same prefix extractor but with different parameter
    opts.prefix_extractor.reset(NewFixedPrefixTransform(15));
    // expect pass only in
    // ConfigOptions::kSanityLevelLooselyCompatible
    SanityCheckCFOptions(opts, false);

    // Change prefix extractor from non-nullptr to nullptr
    opts.prefix_extractor.reset();
    // expect pass as it's safe to change prefix_extractor
    // from non-null to null
    ASSERT_OK(SanityCheckCFOptions(
        opts, ConfigOptions::kSanityLevelLooselyCompatible));
    ASSERT_OK(SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelNone));
  }
  // persist the change
  ASSERT_OK(PersistCFOptions(opts));
  ASSERT_OK(SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));

  // table_factory
  {
    for (int tb = 0; tb <= 2; ++tb) {
      // change the table factory
      opts.table_factory.reset(test::RandomTableFactory(&rnd, tb));
      ASSERT_NOK(SanityCheckCFOptions(
          opts, ConfigOptions::kSanityLevelLooselyCompatible));
      ASSERT_OK(SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelNone));

      // persist the change
      ASSERT_OK(PersistCFOptions(opts));
      ASSERT_OK(
          SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));
    }
  }

  // merge_operator
  {
    // Test when going from nullptr -> merge operator
    opts.merge_operator.reset(test::RandomMergeOperator(&rnd));
    ASSERT_OK(SanityCheckCFOptions(
        opts, ConfigOptions::kSanityLevelLooselyCompatible));
    ASSERT_OK(SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelNone));

    // persist the change
    ASSERT_OK(PersistCFOptions(opts));
    SanityCheckCFOptions(opts, config_options_.ignore_unsupported_options);

    for (int test = 0; test < 5; ++test) {
      // change the merge operator
      opts.merge_operator.reset(test::RandomMergeOperator(&rnd));
      ASSERT_NOK(SanityCheckCFOptions(
          opts, ConfigOptions::kSanityLevelLooselyCompatible));
      ASSERT_OK(SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelNone));

      // persist the change
      ASSERT_OK(PersistCFOptions(opts));
      SanityCheckCFOptions(opts, config_options_.ignore_unsupported_options);
    }

    // Test when going from merge operator -> nullptr
    opts.merge_operator = nullptr;
    ASSERT_NOK(SanityCheckCFOptions(
        opts, ConfigOptions::kSanityLevelLooselyCompatible));
    ASSERT_OK(SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelNone));

    // persist the change
    ASSERT_OK(PersistCFOptions(opts));
    SanityCheckCFOptions(opts, true);
  }

  // compaction_filter
  {
    for (int test = 0; test < 5; ++test) {
      // change the compaction filter
      opts.compaction_filter = test::RandomCompactionFilter(&rnd);
      SanityCheckCFOptions(opts, false);

      // persist the change
      ASSERT_OK(PersistCFOptions(opts));
      SanityCheckCFOptions(opts, config_options_.ignore_unsupported_options);
      delete opts.compaction_filter;
      opts.compaction_filter = nullptr;
    }
  }

  // compaction_filter_factory
  {
    for (int test = 0; test < 5; ++test) {
      // change the compaction filter factory
      opts.compaction_filter_factory.reset(
          test::RandomCompactionFilterFactory(&rnd));
      SanityCheckCFOptions(opts, false);

      // persist the change
      ASSERT_OK(PersistCFOptions(opts));
      SanityCheckCFOptions(opts, config_options_.ignore_unsupported_options);
    }
  }

  // persist_user_defined_timestamps
  {
    // Test change from true to false not allowed in loose and exact mode.
    opts.persist_user_defined_timestamps = false;
    ASSERT_NOK(SanityCheckCFOptions(
        opts, ConfigOptions::kSanityLevelLooselyCompatible));
    ASSERT_NOK(
        SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));

    // persist the change
    ASSERT_OK(PersistCFOptions(opts));
    SanityCheckCFOptions(opts, config_options_.ignore_unsupported_options);

    // Test change from false to true not allowed in loose and exact mode.
    opts.persist_user_defined_timestamps = true;
    ASSERT_NOK(SanityCheckCFOptions(
        opts, ConfigOptions::kSanityLevelLooselyCompatible));
    ASSERT_NOK(
        SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));

    // persist the change
    ASSERT_OK(PersistCFOptions(opts));
  }
}

TEST_P(OptionsSanityCheckTest, DBOptionsSanityCheck) {
  DBOptions opts;
  Random rnd(301);

  // default DBOptions
  {
    ASSERT_OK(PersistDBOptions(opts));
    ASSERT_OK(
        SanityCheckDBOptions(opts, ConfigOptions::kSanityLevelExactMatch));
  }

  // File checksum generator
  {
    class MockFileChecksumGenFactory : public FileChecksumGenFactory {
     public:
      static const char* kClassName() { return "Mock"; }
      const char* Name() const override { return kClassName(); }
      std::unique_ptr<FileChecksumGenerator> CreateFileChecksumGenerator(
          const FileChecksumGenContext& /*context*/) override {
        return nullptr;
      }
    };

    // Okay to change file_checksum_gen_factory form nullptr to non-nullptr
    ASSERT_EQ(opts.file_checksum_gen_factory.get(), nullptr);
    opts.file_checksum_gen_factory.reset(new MockFileChecksumGenFactory());

    // persist the change
    ASSERT_OK(PersistDBOptions(opts));
    SanityCheckDBOptions(opts, config_options_.ignore_unsupported_options);

    // Change file_checksum_gen_factory from non-nullptr to nullptr
    opts.file_checksum_gen_factory.reset();
    // expect pass as it's safe to change file_checksum_gen_factory
    // from non-null to null
    SanityCheckDBOptions(opts, false);
  }
  // persist the change
  ASSERT_OK(PersistDBOptions(opts));
  ASSERT_OK(SanityCheckDBOptions(opts, ConfigOptions::kSanityLevelExactMatch));
}

namespace {
bool IsEscapedString(const std::string& str) {
  for (size_t i = 0; i < str.size(); ++i) {
    if (str[i] == '\\') {
      // since we already handle those two consecutive '\'s in
      // the next if-then branch, any '\' appear at the end
      // of an escaped string in such case is not valid.
      if (i == str.size() - 1) {
        return false;
      }
      if (str[i + 1] == '\\') {
        // if there're two consecutive '\'s, skip the second one.
        i++;
        continue;
      }
      switch (str[i + 1]) {
        case ':':
        case '\\':
        case '#':
          continue;
        default:
          // if true, '\' together with str[i + 1] is not a valid escape.
          if (UnescapeChar(str[i + 1]) == str[i + 1]) {
            return false;
          }
      }
    } else if (isSpecialChar(str[i]) && (i == 0 || str[i - 1] != '\\')) {
      return false;
    }
  }
  return true;
}
}  // namespace

TEST_F(OptionsParserTest, IntegerParsing) {
  ASSERT_EQ(ParseUint64("18446744073709551615"), 18446744073709551615U);
  ASSERT_EQ(ParseUint32("4294967295"), 4294967295U);
  ASSERT_EQ(ParseSizeT("18446744073709551615"), 18446744073709551615U);
  ASSERT_EQ(ParseInt64("9223372036854775807"), 9223372036854775807);
  ASSERT_EQ(ParseInt64("-9223372036854775808"),
            std::numeric_limits<int64_t>::min());
  ASSERT_EQ(ParseInt32("2147483647"), 2147483647);
  ASSERT_EQ(ParseInt32("-2147483648"), std::numeric_limits<int32_t>::min());
  ASSERT_EQ(ParseInt("-32767"), -32767);
  ASSERT_EQ(ParseDouble("-1.234567"), -1.234567);
}

TEST_F(OptionsParserTest, EscapeOptionString) {
  ASSERT_EQ(UnescapeOptionString(
                "This is a test string with \\# \\: and \\\\ escape chars."),
            "This is a test string with # : and \\ escape chars.");

  ASSERT_EQ(
      EscapeOptionString("This is a test string with # : and \\ escape chars."),
      "This is a test string with \\# \\: and \\\\ escape chars.");

  std::string readible_chars =
      "A String like this \"1234567890-=_)(*&^%$#@!ertyuiop[]{POIU"
      "YTREWQasdfghjkl;':LKJHGFDSAzxcvbnm,.?>"
      "<MNBVCXZ\\\" should be okay to \\#\\\\\\:\\#\\#\\#\\ "
      "be serialized and deserialized";

  std::string escaped_string = EscapeOptionString(readible_chars);
  ASSERT_TRUE(IsEscapedString(escaped_string));
  // This two transformations should be canceled and should output
  // the original input.
  ASSERT_EQ(UnescapeOptionString(escaped_string), readible_chars);

  std::string all_chars;
  for (unsigned char c = 0;; ++c) {
    all_chars += c;
    if (c == 255) {
      break;
    }
  }
  escaped_string = EscapeOptionString(all_chars);
  ASSERT_TRUE(IsEscapedString(escaped_string));
  ASSERT_EQ(UnescapeOptionString(escaped_string), all_chars);

  ASSERT_EQ(RocksDBOptionsParser::TrimAndRemoveComment(
                "     A simple statement with a comment.  # like this :)"),
            "A simple statement with a comment.");

  ASSERT_EQ(RocksDBOptionsParser::TrimAndRemoveComment(
                "Escape \\# and # comment together   ."),
            "Escape \\# and");
}

static void TestAndCompareOption(const ConfigOptions& config_options,
                                 const OptionTypeInfo& opt_info,
                                 const std::string& opt_name, void* base_ptr,
                                 void* comp_ptr, bool strip = false) {
  std::string result;
  std::string mismatch;
  ASSERT_OK(opt_info.Serialize(config_options, opt_name, base_ptr, &result));
  if (strip) {
    ASSERT_EQ(result.at(0), '{');
    ASSERT_EQ(result.at(result.size() - 1), '}');
    result = result.substr(1, result.size() - 2);
  }
  ASSERT_OK(opt_info.Parse(config_options, opt_name, result, comp_ptr));
  ASSERT_TRUE(opt_info.AreEqual(config_options, opt_name, base_ptr, comp_ptr,
                                &mismatch));
}

static void TestParseAndCompareOption(const ConfigOptions& config_options,
                                      const OptionTypeInfo& opt_info,
                                      const std::string& opt_name,
                                      const std::string& opt_value,
                                      void* base_ptr, void* comp_ptr,
                                      bool strip = false) {
  ASSERT_OK(opt_info.Parse(config_options, opt_name, opt_value, base_ptr));
  TestAndCompareOption(config_options, opt_info, opt_name, base_ptr, comp_ptr,
                       strip);
}

template <typename T>
void TestOptInfo(const ConfigOptions& config_options, OptionType opt_type,
                 T* base, T* comp) {
  std::string result;
  OptionTypeInfo opt_info(0, opt_type);
  ASSERT_FALSE(opt_info.AreEqual(config_options, "base", base, comp, &result));
  ASSERT_EQ(result, "base");
  ASSERT_NE(*base, *comp);
  TestAndCompareOption(config_options, opt_info, "base", base, comp);
  ASSERT_EQ(*base, *comp);
}

class OptionTypeInfoTest : public testing::Test {};

TEST_F(OptionTypeInfoTest, BasicTypes) {
  ConfigOptions config_options;
  {
    bool a = true;
    bool b = false;
    TestOptInfo(config_options, OptionType::kBoolean, &a, &b);
  }
  {
    int a = 100;
    int b = 200;
    TestOptInfo(config_options, OptionType::kInt, &a, &b);
  }
  {
    int32_t a = 100;
    int32_t b = 200;
    TestOptInfo(config_options, OptionType::kInt32T, &a, &b);
  }
  {
    int64_t a = 100;
    int64_t b = 200;
    TestOptInfo(config_options, OptionType::kInt64T, &a, &b);
  }
  {
    unsigned int a = 100;
    unsigned int b = 200;
    TestOptInfo(config_options, OptionType::kUInt, &a, &b);
  }
  {
    uint32_t a = 100;
    uint32_t b = 200;
    TestOptInfo(config_options, OptionType::kUInt32T, &a, &b);
  }
  {
    uint64_t a = 100;
    uint64_t b = 200;
    TestOptInfo(config_options, OptionType::kUInt64T, &a, &b);
  }
  {
    size_t a = 100;
    size_t b = 200;
    TestOptInfo(config_options, OptionType::kSizeT, &a, &b);
  }
  {
    std::string a = "100";
    std::string b = "200";
    TestOptInfo(config_options, OptionType::kString, &a, &b);
  }
  {
    double a = 1.0;
    double b = 2.0;
    TestOptInfo(config_options, OptionType::kDouble, &a, &b);
  }
}

TEST_F(OptionTypeInfoTest, TestInvalidArgs) {
  ConfigOptions config_options;
  bool b;
  int i;
  int32_t i32;
  int64_t i64;
  unsigned int u;
  int32_t u32;
  int64_t u64;
  size_t sz;
  double d;

  ASSERT_NOK(OptionTypeInfo(0, OptionType::kBoolean)
                 .Parse(config_options, "b", "x", &b));
  ASSERT_NOK(
      OptionTypeInfo(0, OptionType::kInt).Parse(config_options, "b", "x", &i));
  ASSERT_NOK(OptionTypeInfo(0, OptionType::kInt32T)
                 .Parse(config_options, "b", "x", &i32));
  ASSERT_NOK(OptionTypeInfo(0, OptionType::kInt64T)
                 .Parse(config_options, "b", "x", &i64));
  ASSERT_NOK(
      OptionTypeInfo(0, OptionType::kUInt).Parse(config_options, "b", "x", &u));
  ASSERT_NOK(OptionTypeInfo(0, OptionType::kUInt32T)
                 .Parse(config_options, "b", "x", &u32));
  ASSERT_NOK(OptionTypeInfo(0, OptionType::kUInt64T)
                 .Parse(config_options, "b", "x", &u64));
  ASSERT_NOK(OptionTypeInfo(0, OptionType::kSizeT)
                 .Parse(config_options, "b", "x", &sz));
  ASSERT_NOK(OptionTypeInfo(0, OptionType::kDouble)
                 .Parse(config_options, "b", "x", &d));

  // Don't know how to convert Unknowns to anything else
  ASSERT_NOK(OptionTypeInfo(0, OptionType::kUnknown)
                 .Parse(config_options, "b", "x", &d));

  // Verify that if the parse function throws an exception, it is also trapped
  OptionTypeInfo func_info(0, OptionType::kUnknown,
                           OptionVerificationType::kNormal,
                           OptionTypeFlags::kNone,
                           [](const ConfigOptions&, const std::string&,
                              const std::string& value, void* addr) {
                             auto ptr = static_cast<int*>(addr);
                             *ptr = ParseInt(value);
                             return Status::OK();
                           });
  ASSERT_OK(func_info.Parse(config_options, "b", "1", &i));
  ASSERT_NOK(func_info.Parse(config_options, "b", "x", &i));
}

TEST_F(OptionTypeInfoTest, StringMapRejectsMalformedEntries) {
  ConfigOptions config_options;
  OptionTypeInfo string_map = OptionTypeInfo::StringMap(
      0, OptionVerificationType::kNormal, OptionTypeFlags::kNone);
  const std::map<std::string, std::string> expected = {{"existing", "value"}};

  for (const char* malformed : {"{00;}", "{00=zz;}"}) {
    SCOPED_TRACE(malformed);
    std::map<std::string, std::string> parsed = expected;
    ASSERT_TRUE(string_map.Parse(config_options, "map", malformed, &parsed)
                    .IsInvalidArgument());
    ASSERT_EQ(parsed, expected);
  }
}

TEST_F(OptionTypeInfoTest, EncodedStringRejectsMalformedHex) {
  ConfigOptions config_options;
  OptionTypeInfo encoded_string(0, OptionType::kEncodedString);
  const std::string expected = "existing";

  // An odd number of digits, and a digit that is not hex.
  for (const char* malformed : {"616", "61zz"}) {
    SCOPED_TRACE(malformed);
    std::string parsed = expected;
    ASSERT_TRUE(encoded_string.Parse(config_options, "str", malformed, &parsed)
                    .IsInvalidArgument());
    ASSERT_EQ(parsed, expected);
  }

  std::string parsed;
  ASSERT_OK(encoded_string.Parse(config_options, "str", "6162", &parsed));
  ASSERT_EQ(parsed, "ab");
}

TEST_F(OptionTypeInfoTest, TestParseFunc) {
  OptionTypeInfo opt_info(0, OptionType::kUnknown,
                          OptionVerificationType::kNormal,
                          OptionTypeFlags::kNone);
  opt_info.SetParseFunc([](const ConfigOptions& /*opts*/,
                           const std::string& name, const std::string& value,
                           void* addr) {
    auto ptr = static_cast<std::string*>(addr);
    if (name == "Oops") {
      return Status::InvalidArgument(value);
    } else {
      *ptr = value + " " + name;
      return Status::OK();
    }
  });
  ConfigOptions config_options;
  std::string base;
  ASSERT_OK(opt_info.Parse(config_options, "World", "Hello", &base));
  ASSERT_EQ(base, "Hello World");
  ASSERT_NOK(opt_info.Parse(config_options, "Oops", "Hello", &base));
}

TEST_F(OptionTypeInfoTest, TestSerializeFunc) {
  OptionTypeInfo opt_info(0, OptionType::kString,
                          OptionVerificationType::kNormal,
                          OptionTypeFlags::kNone);
  opt_info.SetSerializeFunc([](const ConfigOptions& /*opts*/,
                               const std::string& name, const void* /*addr*/,
                               std::string* value) {
    if (name == "Oops") {
      return Status::InvalidArgument(name);
    } else {
      *value = name;
      return Status::OK();
    }
  });
  ConfigOptions config_options;
  std::string base;
  std::string value;
  ASSERT_OK(opt_info.Serialize(config_options, "Hello", &base, &value));
  ASSERT_EQ(value, "Hello");
  ASSERT_NOK(opt_info.Serialize(config_options, "Oops", &base, &value));
}

TEST_F(OptionTypeInfoTest, TestEqualsFunc) {
  OptionTypeInfo opt_info(0, OptionType::kInt, OptionVerificationType::kNormal,
                          OptionTypeFlags::kNone);
  opt_info.SetEqualsFunc([](const ConfigOptions& /*opts*/,
                            const std::string& name, const void* addr1,
                            const void* addr2, std::string* mismatch) {
    auto i1 = *(static_cast<const int*>(addr1));
    auto i2 = *(static_cast<const int*>(addr2));
    if (name == "LT") {
      return i1 < i2;
    } else if (name == "GT") {
      return i1 > i2;
    } else if (name == "EQ") {
      return i1 == i2;
    } else {
      *mismatch = name + "???";
      return false;
    }
  });

  ConfigOptions config_options;
  int int1 = 100;
  int int2 = 200;
  std::string mismatch;
  ASSERT_TRUE(opt_info.AreEqual(config_options, "LT", &int1, &int2, &mismatch));
  ASSERT_EQ(mismatch, "");
  ASSERT_FALSE(
      opt_info.AreEqual(config_options, "GT", &int1, &int2, &mismatch));
  ASSERT_EQ(mismatch, "GT");
  ASSERT_FALSE(
      opt_info.AreEqual(config_options, "NO", &int1, &int2, &mismatch));
  ASSERT_EQ(mismatch, "NO???");
}

TEST_F(OptionTypeInfoTest, TestPrepareFunc) {
  OptionTypeInfo opt_info(0, OptionType::kInt, OptionVerificationType::kNormal,
                          OptionTypeFlags::kNone);
  opt_info.SetPrepareFunc(
      [](const ConfigOptions& /*opts*/, const std::string& name, void* addr) {
        auto i1 = static_cast<int*>(addr);
        if (name == "x2") {
          *i1 *= 2;
        } else if (name == "/2") {
          *i1 /= 2;
        } else {
          return Status::InvalidArgument("Bad Argument", name);
        }
        return Status::OK();
      });
  ConfigOptions config_options;
  int int1 = 100;
  ASSERT_OK(opt_info.Prepare(config_options, "x2", &int1));
  ASSERT_EQ(int1, 200);
  ASSERT_OK(opt_info.Prepare(config_options, "/2", &int1));
  ASSERT_EQ(int1, 100);
  ASSERT_NOK(opt_info.Prepare(config_options, "??", &int1));
  ASSERT_EQ(int1, 100);
}
TEST_F(OptionTypeInfoTest, TestValidateFunc) {
  OptionTypeInfo opt_info(0, OptionType::kSizeT,
                          OptionVerificationType::kNormal,
                          OptionTypeFlags::kNone);
  opt_info.SetValidateFunc([](const DBOptions& db_opts,
                              const ColumnFamilyOptions& cf_opts,
                              const std::string& name, const void* addr) {
    const auto sz = static_cast<const size_t*>(addr);
    bool is_valid = false;
    if (name == "keep_log_file_num") {
      is_valid = (*sz == db_opts.keep_log_file_num);
    } else if (name == "write_buffer_size") {
      is_valid = (*sz == cf_opts.write_buffer_size);
    }
    if (is_valid) {
      return Status::OK();
    } else {
      return Status::InvalidArgument("Mismatched value", name);
    }
  });
  ConfigOptions config_options;
  DBOptions db_options;
  ColumnFamilyOptions cf_options;

  ASSERT_OK(opt_info.Validate(db_options, cf_options, "keep_log_file_num",
                              &db_options.keep_log_file_num));
  ASSERT_OK(opt_info.Validate(db_options, cf_options, "write_buffer_size",
                              &cf_options.write_buffer_size));
  ASSERT_NOK(opt_info.Validate(db_options, cf_options, "keep_log_file_num",
                               &cf_options.write_buffer_size));
  ASSERT_NOK(opt_info.Validate(db_options, cf_options, "write_buffer_size",
                               &db_options.keep_log_file_num));
}

TEST_F(OptionTypeInfoTest, TestOptionFlags) {
  OptionTypeInfo opt_none(0, OptionType::kString,
                          OptionVerificationType::kNormal,
                          OptionTypeFlags::kDontSerialize);
  OptionTypeInfo opt_never(0, OptionType::kString,
                           OptionVerificationType::kNormal,
                           OptionTypeFlags::kCompareNever);
  OptionTypeInfo opt_alias(0, OptionType::kString,
                           OptionVerificationType::kAlias,
                           OptionTypeFlags::kNone);
  OptionTypeInfo opt_deprecated(0, OptionType::kString,
                                OptionVerificationType::kDeprecated,
                                OptionTypeFlags::kNone);
  ConfigOptions config_options;
  std::string opts_str;
  std::string base = "base";
  std::string comp = "comp";

  // If marked string none, the serialization returns not supported
  ASSERT_NOK(opt_none.Serialize(config_options, "None", &base, &opts_str));
  // If marked never compare, they match even when they do not
  ASSERT_TRUE(opt_never.AreEqual(config_options, "Never", &base, &comp, &base));
  ASSERT_FALSE(opt_none.AreEqual(config_options, "Never", &base, &comp, &base));

  // An alias can change the value via parse, but does nothing on serialize on
  // match
  std::string result;
  ASSERT_OK(opt_alias.Parse(config_options, "Alias", "Alias", &base));
  ASSERT_OK(opt_alias.Serialize(config_options, "Alias", &base, &result));
  ASSERT_TRUE(
      opt_alias.AreEqual(config_options, "Alias", &base, &comp, &result));
  ASSERT_EQ(base, "Alias");
  ASSERT_NE(base, comp);

  // Deprecated options do nothing on any of the commands
  ASSERT_OK(opt_deprecated.Parse(config_options, "Alias", "Deprecated", &base));
  ASSERT_OK(opt_deprecated.Serialize(config_options, "Alias", &base, &result));
  ASSERT_TRUE(
      opt_deprecated.AreEqual(config_options, "Alias", &base, &comp, &result));
  ASSERT_EQ(base, "Alias");
  ASSERT_NE(base, comp);
}

TEST_F(OptionTypeInfoTest, TestCustomEnum) {
  enum TestEnum { kA, kB, kC };
  std::unordered_map<std::string, TestEnum> enum_map = {
      {"A", TestEnum::kA},
      {"B", TestEnum::kB},
      {"C", TestEnum::kC},
  };
  OptionTypeInfo opt_info = OptionTypeInfo::Enum<TestEnum>(0, &enum_map);
  TestEnum e1;
  TestEnum e2;
  ConfigOptions config_options;
  std::string result;
  std::string mismatch;

  e2 = TestEnum::kA;

  ASSERT_OK(opt_info.Parse(config_options, "", "B", &e1));
  ASSERT_OK(opt_info.Serialize(config_options, "", &e1, &result));
  ASSERT_EQ(e1, TestEnum::kB);
  ASSERT_EQ(result, "B");

  ASSERT_FALSE(opt_info.AreEqual(config_options, "Enum", &e1, &e2, &mismatch));
  ASSERT_EQ(mismatch, "Enum");

  TestParseAndCompareOption(config_options, opt_info, "", "C", &e1, &e2);
  ASSERT_EQ(e2, TestEnum::kC);

  ASSERT_NOK(opt_info.Parse(config_options, "", "D", &e1));
  ASSERT_EQ(e1, TestEnum::kC);
}

TEST_F(OptionTypeInfoTest, TestBuiltinEnum) {
  ConfigOptions config_options;
  for (const auto& iter : OptionsHelper::compaction_style_string_map) {
    CompactionStyle e1;
    CompactionStyle e2;
    TestParseAndCompareOption(config_options,
                              OptionTypeInfo(0, OptionType::kCompactionStyle),
                              "CompactionStyle", iter.first, &e1, &e2);
    ASSERT_EQ(e1, iter.second);
  }
  for (const auto& iter : OptionsHelper::compaction_pri_string_map) {
    CompactionPri e1;
    CompactionPri e2;
    TestParseAndCompareOption(config_options,
                              OptionTypeInfo(0, OptionType::kCompactionPri),
                              "CompactionPri", iter.first, &e1, &e2);
    ASSERT_EQ(e1, iter.second);
  }
  for (const auto& iter : OptionsHelper::compression_type_string_map) {
    CompressionType e1;
    CompressionType e2;
    TestParseAndCompareOption(config_options,
                              OptionTypeInfo(0, OptionType::kCompressionType),
                              "CompressionType", iter.first, &e1, &e2);
    ASSERT_EQ(e1, iter.second);
  }
  for (const auto& iter : OptionsHelper::compaction_stop_style_string_map) {
    CompactionStopStyle e1;
    CompactionStopStyle e2;
    TestParseAndCompareOption(
        config_options, OptionTypeInfo(0, OptionType::kCompactionStopStyle),
        "CompactionStopStyle", iter.first, &e1, &e2);
    ASSERT_EQ(e1, iter.second);
  }
  for (const auto& iter : OptionsHelper::checksum_type_string_map) {
    ChecksumType e1;
    ChecksumType e2;
    TestParseAndCompareOption(config_options,
                              OptionTypeInfo(0, OptionType::kChecksumType),
                              "CheckSumType", iter.first, &e1, &e2);
    ASSERT_EQ(e1, iter.second);
  }
  for (const auto& iter : OptionsHelper::encoding_type_string_map) {
    EncodingType e1;
    EncodingType e2;
    TestParseAndCompareOption(config_options,
                              OptionTypeInfo(0, OptionType::kEncodingType),
                              "EncodingType", iter.first, &e1, &e2);
    ASSERT_EQ(e1, iter.second);
  }
}

TEST_F(OptionTypeInfoTest, TestStruct) {
  struct Basic {
    int i = 42;
    std::string s = "Hello";
  };

  struct Extended {
    int j = 11;
    Basic b;
  };

  std::unordered_map<std::string, OptionTypeInfo> basic_type_map = {
      {"i", {offsetof(struct Basic, i), OptionType::kInt}},
      {"s", {offsetof(struct Basic, s), OptionType::kString}},
  };
  OptionTypeInfo basic_info = OptionTypeInfo::Struct(
      "b", &basic_type_map, 0, OptionVerificationType::kNormal,
      OptionTypeFlags::kMutable);

  std::unordered_map<std::string, OptionTypeInfo> extended_type_map = {
      {"j", {offsetof(struct Extended, j), OptionType::kInt}},
      {"b", OptionTypeInfo::Struct(
                "b", &basic_type_map, offsetof(struct Extended, b),
                OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
      {"m", OptionTypeInfo::Struct(
                "m", &basic_type_map, offsetof(struct Extended, b),
                OptionVerificationType::kNormal, OptionTypeFlags::kMutable)},
  };
  OptionTypeInfo extended_info = OptionTypeInfo::Struct(
      "e", &extended_type_map, 0, OptionVerificationType::kNormal,
      OptionTypeFlags::kMutable);
  Extended e1;
  Extended e2;
  ConfigOptions config_options;
  std::string mismatch;
  TestParseAndCompareOption(config_options, basic_info, "b", "{i=33;s=33}",
                            &e1.b, &e2.b);
  ASSERT_EQ(e1.b.i, 33);
  ASSERT_EQ(e1.b.s, "33");

  TestParseAndCompareOption(config_options, basic_info, "b.i", "44", &e1.b,
                            &e2.b);
  ASSERT_EQ(e1.b.i, 44);

  TestParseAndCompareOption(config_options, basic_info, "i", "55", &e1.b,
                            &e2.b);
  ASSERT_EQ(e1.b.i, 55);

  e1.b.i = 0;

  ASSERT_FALSE(
      basic_info.AreEqual(config_options, "b", &e1.b, &e2.b, &mismatch));
  ASSERT_EQ(mismatch, "b.i");
  mismatch.clear();
  ASSERT_FALSE(
      basic_info.AreEqual(config_options, "b.i", &e1.b, &e2.b, &mismatch));
  ASSERT_EQ(mismatch, "b.i");
  mismatch.clear();
  ASSERT_FALSE(
      basic_info.AreEqual(config_options, "i", &e1.b, &e2.b, &mismatch));
  ASSERT_EQ(mismatch, "b.i");
  mismatch.clear();

  e1 = e2;
  ASSERT_NOK(basic_info.Parse(config_options, "b", "{i=33;s=33;j=44}", &e1.b));
  ASSERT_NOK(basic_info.Parse(config_options, "b.j", "44", &e1.b));
  ASSERT_NOK(basic_info.Parse(config_options, "j", "44", &e1.b));

  TestParseAndCompareOption(config_options, extended_info, "e",
                            "b={i=55;s=55}; j=22;", &e1, &e2);
  ASSERT_EQ(e1.b.i, 55);
  ASSERT_EQ(e1.j, 22);
  ASSERT_EQ(e1.b.s, "55");
  TestParseAndCompareOption(config_options, extended_info, "e.b",
                            "{i=66;s=66;}", &e1, &e2);
  ASSERT_EQ(e1.b.i, 66);
  ASSERT_EQ(e1.j, 22);
  ASSERT_EQ(e1.b.s, "66");
  TestParseAndCompareOption(config_options, extended_info, "e.b.i", "77", &e1,
                            &e2);
  ASSERT_EQ(e1.b.i, 77);
  ASSERT_EQ(e1.j, 22);
  ASSERT_EQ(e1.b.s, "66");
}

TEST_F(OptionTypeInfoTest, TestArrayType) {
  OptionTypeInfo array_info = OptionTypeInfo::Array<std::string, 4>(
      0, OptionVerificationType::kNormal, OptionTypeFlags::kNone,
      {0, OptionType::kString});
  std::array<std::string, 4> array1;
  std::array<std::string, 4> array2;
  std::string mismatch;

  ConfigOptions config_options;
  TestParseAndCompareOption(config_options, array_info, "v", "a:b:c:d", &array1,
                            &array2);

  ASSERT_EQ(array1.size(), 4);
  ASSERT_EQ(array1[0], "a");
  ASSERT_EQ(array1[1], "b");
  ASSERT_EQ(array1[2], "c");
  ASSERT_EQ(array1[3], "d");
  array1[3] = "e";
  ASSERT_FALSE(
      array_info.AreEqual(config_options, "v", &array1, &array2, &mismatch));
  ASSERT_EQ(mismatch, "v");

  // Test vectors with inner brackets
  TestParseAndCompareOption(config_options, array_info, "v", "a:{b}:c:d",
                            &array1, &array2);
  ASSERT_EQ(array1.size(), 4);
  ASSERT_EQ(array1[0], "a");
  ASSERT_EQ(array1[1], "b");
  ASSERT_EQ(array1[2], "c");
  ASSERT_EQ(array1[3], "d");

  std::array<std::string, 3> array3;
  std::array<std::string, 3> array4;
  OptionTypeInfo bar_info = OptionTypeInfo::Array<std::string, 3>(
      0, OptionVerificationType::kNormal, OptionTypeFlags::kNone,
      {0, OptionType::kString}, '|');
  TestParseAndCompareOption(config_options, bar_info, "v", "x|y|z", &array3,
                            &array4);

  // Test arrays with inner array
  TestParseAndCompareOption(config_options, bar_info, "v",
                            "a|{b1|b2}|{c1|c2|{d1|d2}}", &array3, &array4,
                            false);
  ASSERT_EQ(array3.size(), 3);
  ASSERT_EQ(array3[0], "a");
  ASSERT_EQ(array3[1], "b1|b2");
  ASSERT_EQ(array3[2], "c1|c2|{d1|d2}");

  TestParseAndCompareOption(config_options, bar_info, "v",
                            "{a1|a2}|{b1|{c1|c2}}|d1", &array3, &array4, true);
  ASSERT_EQ(array3.size(), 3);
  ASSERT_EQ(array3[0], "a1|a2");
  ASSERT_EQ(array3[1], "b1|{c1|c2}");
  ASSERT_EQ(array3[2], "d1");

  // Test invalid input: less element than requested
  auto s = bar_info.Parse(config_options, "opt_name1", "a1|a2", &array3);
  ASSERT_TRUE(s.IsInvalidArgument());

  // Test invalid input: more element than requested
  s = bar_info.Parse(config_options, "opt_name2", "a1|b|c1|d3", &array3);
  ASSERT_TRUE(s.IsInvalidArgument());
}

TEST_F(OptionTypeInfoTest, TestVectorType) {
  OptionTypeInfo vec_info = OptionTypeInfo::Vector<std::string>(
      0, OptionVerificationType::kNormal, OptionTypeFlags::kNone,
      {0, OptionType::kString});
  std::vector<std::string> vec1;
  std::vector<std::string> vec2;
  std::string mismatch;

  ConfigOptions config_options;
  TestParseAndCompareOption(config_options, vec_info, "v", "a:b:c:d", &vec1,
                            &vec2);
  ASSERT_EQ(vec1.size(), 4);
  ASSERT_EQ(vec1[0], "a");
  ASSERT_EQ(vec1[1], "b");
  ASSERT_EQ(vec1[2], "c");
  ASSERT_EQ(vec1[3], "d");
  vec1[3] = "e";
  ASSERT_FALSE(vec_info.AreEqual(config_options, "v", &vec1, &vec2, &mismatch));
  ASSERT_EQ(mismatch, "v");

  // Test vectors with inner brackets
  TestParseAndCompareOption(config_options, vec_info, "v", "a:{b}:c:d", &vec1,
                            &vec2);
  ASSERT_EQ(vec1.size(), 4);
  ASSERT_EQ(vec1[0], "a");
  ASSERT_EQ(vec1[1], "b");
  ASSERT_EQ(vec1[2], "c");
  ASSERT_EQ(vec1[3], "d");

  OptionTypeInfo bar_info = OptionTypeInfo::Vector<std::string>(
      0, OptionVerificationType::kNormal, OptionTypeFlags::kNone,
      {0, OptionType::kString}, '|');
  TestParseAndCompareOption(config_options, vec_info, "v", "x|y|z", &vec1,
                            &vec2);
  // Test vectors with inner vector
  TestParseAndCompareOption(config_options, bar_info, "v",
                            "a|{b1|b2}|{c1|c2|{d1|d2}}", &vec1, &vec2, false);
  ASSERT_EQ(vec1.size(), 3);
  ASSERT_EQ(vec1[0], "a");
  ASSERT_EQ(vec1[1], "b1|b2");
  ASSERT_EQ(vec1[2], "c1|c2|{d1|d2}");

  TestParseAndCompareOption(config_options, bar_info, "v",
                            "{a1|a2}|{b1|{c1|c2}}|d1", &vec1, &vec2, true);
  ASSERT_EQ(vec1.size(), 3);
  ASSERT_EQ(vec1[0], "a1|a2");
  ASSERT_EQ(vec1[1], "b1|{c1|c2}");
  ASSERT_EQ(vec1[2], "d1");

  TestParseAndCompareOption(config_options, bar_info, "v", "{a1}", &vec1, &vec2,
                            false);
  ASSERT_EQ(vec1.size(), 1);
  ASSERT_EQ(vec1[0], "a1");

  TestParseAndCompareOption(config_options, bar_info, "v", "{a1|a2}|{b1|b2}",
                            &vec1, &vec2, true);
  ASSERT_EQ(vec1.size(), 2);
  ASSERT_EQ(vec1[0], "a1|a2");
  ASSERT_EQ(vec1[1], "b1|b2");
}

TEST_F(OptionTypeInfoTest, TestStaticType) {
  struct SimpleOptions {
    size_t size = 0;
    bool verify = true;
  };

  static std::unordered_map<std::string, OptionTypeInfo> type_map = {
      {"size", {offsetof(struct SimpleOptions, size), OptionType::kSizeT}},
      {"verify",
       {offsetof(struct SimpleOptions, verify), OptionType::kBoolean}},
  };

  ConfigOptions config_options;
  SimpleOptions opts;
  SimpleOptions copy;
  opts.size = 12345;
  opts.verify = false;
  std::string str;
  std::string mismatch;

  ASSERT_OK(
      OptionTypeInfo::SerializeType(config_options, type_map, &opts, &str));
  ASSERT_FALSE(OptionTypeInfo::TypesAreEqual(config_options, type_map, &opts,
                                             &copy, &mismatch));
  ASSERT_OK(OptionTypeInfo::ParseType(config_options, str, type_map, &copy));
  ASSERT_TRUE(OptionTypeInfo::TypesAreEqual(config_options, type_map, &opts,
                                            &copy, &mismatch));
}

class ConfigOptionsTest : public testing::Test {};

TEST_F(ConfigOptionsTest, EnvFromConfigOptions) {
  ConfigOptions config_options;
  DBOptions db_opts;
  Options opts;
  Env* mem_env = NewMemEnv(Env::Default());
  config_options.registry->AddLibrary("custom-env", RegisterCustomEnv,
                                      kCustomEnvName);

  config_options.env = mem_env;
  // First test that we can get the env as expected
  ASSERT_OK(GetDBOptionsFromString(config_options, DBOptions(), kCustomEnvProp,
                                   &db_opts));
  ASSERT_OK(
      GetOptionsFromString(config_options, Options(), kCustomEnvProp, &opts));
  ASSERT_NE(config_options.env, db_opts.env);
  ASSERT_EQ(opts.env, db_opts.env);
  Env* custom_env = db_opts.env;

  // Now try a "bad" env" and check that nothing changed
  config_options.ignore_unsupported_options = true;
  ASSERT_OK(
      GetDBOptionsFromString(config_options, db_opts, "env=unknown", &db_opts));
  ASSERT_OK(GetOptionsFromString(config_options, opts, "env=unknown", &opts));
  ASSERT_EQ(config_options.env, mem_env);
  ASSERT_EQ(db_opts.env, custom_env);
  ASSERT_EQ(opts.env, db_opts.env);

  // Now try a "bad" env" ignoring unknown objects
  config_options.ignore_unsupported_options = false;
  ASSERT_NOK(
      GetDBOptionsFromString(config_options, db_opts, "env=unknown", &db_opts));
  ASSERT_EQ(config_options.env, mem_env);
  ASSERT_EQ(db_opts.env, custom_env);
  ASSERT_EQ(opts.env, db_opts.env);

  delete mem_env;
}
TEST_F(ConfigOptionsTest, MergeOperatorFromString) {
  ConfigOptions config_options;
  std::shared_ptr<MergeOperator> merge_op;

  ASSERT_OK(MergeOperator::CreateFromString(config_options, "put", &merge_op));
  ASSERT_NE(merge_op, nullptr);
  ASSERT_TRUE(merge_op->IsInstanceOf("put"));
  ASSERT_STREQ(merge_op->Name(), "PutOperator");

  ASSERT_OK(
      MergeOperator::CreateFromString(config_options, "put_v1", &merge_op));
  ASSERT_NE(merge_op, nullptr);
  ASSERT_TRUE(merge_op->IsInstanceOf("PutOperator"));

  ASSERT_OK(
      MergeOperator::CreateFromString(config_options, "uint64add", &merge_op));
  ASSERT_NE(merge_op, nullptr);
  ASSERT_TRUE(merge_op->IsInstanceOf("uint64add"));
  ASSERT_STREQ(merge_op->Name(), "UInt64AddOperator");

  ASSERT_OK(MergeOperator::CreateFromString(config_options, "max", &merge_op));
  ASSERT_NE(merge_op, nullptr);
  ASSERT_TRUE(merge_op->IsInstanceOf("max"));
  ASSERT_STREQ(merge_op->Name(), "MaxOperator");

  ASSERT_OK(
      MergeOperator::CreateFromString(config_options, "bytesxor", &merge_op));
  ASSERT_NE(merge_op, nullptr);
  ASSERT_TRUE(merge_op->IsInstanceOf("bytesxor"));
  ASSERT_STREQ(merge_op->Name(), BytesXOROperator::kClassName());

  ASSERT_OK(
      MergeOperator::CreateFromString(config_options, "sortlist", &merge_op));
  ASSERT_NE(merge_op, nullptr);
  ASSERT_TRUE(merge_op->IsInstanceOf("sortlist"));
  ASSERT_STREQ(merge_op->Name(), SortList::kClassName());

  ASSERT_OK(MergeOperator::CreateFromString(config_options, "stringappend",
                                            &merge_op));
  ASSERT_NE(merge_op, nullptr);
  ASSERT_TRUE(merge_op->IsInstanceOf("stringappend"));
  ASSERT_STREQ(merge_op->Name(), StringAppendOperator::kClassName());
  auto delimiter = merge_op->GetOptions<std::string>("Delimiter");
  ASSERT_NE(delimiter, nullptr);
  ASSERT_EQ(*delimiter, ",");

  ASSERT_OK(MergeOperator::CreateFromString(config_options, "stringappendtest",
                                            &merge_op));
  ASSERT_NE(merge_op, nullptr);
  ASSERT_TRUE(merge_op->IsInstanceOf("stringappendtest"));
  ASSERT_STREQ(merge_op->Name(), StringAppendTESTOperator::kClassName());
  delimiter = merge_op->GetOptions<std::string>("Delimiter");
  ASSERT_NE(delimiter, nullptr);
  ASSERT_EQ(*delimiter, ",");

  ASSERT_OK(MergeOperator::CreateFromString(
      config_options, "id=stringappend; delimiter=||", &merge_op));
  ASSERT_NE(merge_op, nullptr);
  ASSERT_TRUE(merge_op->IsInstanceOf("stringappend"));
  ASSERT_STREQ(merge_op->Name(), StringAppendOperator::kClassName());
  delimiter = merge_op->GetOptions<std::string>("Delimiter");
  ASSERT_NE(delimiter, nullptr);
  ASSERT_EQ(*delimiter, "||");

  ASSERT_OK(MergeOperator::CreateFromString(
      config_options, "id=stringappendtest; delimiter=&&", &merge_op));
  ASSERT_NE(merge_op, nullptr);
  ASSERT_TRUE(merge_op->IsInstanceOf("stringappendtest"));
  ASSERT_STREQ(merge_op->Name(), StringAppendTESTOperator::kClassName());
  delimiter = merge_op->GetOptions<std::string>("Delimiter");
  ASSERT_NE(delimiter, nullptr);
  ASSERT_EQ(*delimiter, "&&");

  std::shared_ptr<MergeOperator> copy;
  std::string mismatch;
  std::string opts_str = merge_op->ToString(config_options);

  ASSERT_OK(MergeOperator::CreateFromString(config_options, opts_str, &copy));
  ASSERT_TRUE(merge_op->AreEquivalent(config_options, copy.get(), &mismatch));
  ASSERT_NE(copy, nullptr);
  delimiter = copy->GetOptions<std::string>("Delimiter");
  ASSERT_NE(delimiter, nullptr);
  ASSERT_EQ(*delimiter, "&&");
}

TEST_F(ConfigOptionsTest, ConfiguringOptionsDoesNotRevertRateLimiterBandwidth) {
  // Regression test for bug where rate limiter's dynamically set bandwidth
  // could be silently reverted when configuring an options structure with an
  // existing `rate_limiter`.
  Options base_options;
  base_options.rate_limiter.reset(
      NewGenericRateLimiter(1 << 20 /* rate_bytes_per_sec */));
  Options copy_options(base_options);

  base_options.rate_limiter->SetBytesPerSecond(2 << 20);
  ASSERT_EQ(2 << 20, base_options.rate_limiter->GetBytesPerSecond());

  ASSERT_OK(GetOptionsFromString(base_options, "", &copy_options));
  ASSERT_EQ(2 << 20, base_options.rate_limiter->GetBytesPerSecond());
}

INSTANTIATE_TEST_CASE_P(OptionsSanityCheckTest, OptionsSanityCheckTest,
                        ::testing::Bool());

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
#ifdef GFLAGS
  ParseCommandLineFlags(&argc, &argv, true);
#endif  // GFLAGS
  return RUN_ALL_TESTS();
}
