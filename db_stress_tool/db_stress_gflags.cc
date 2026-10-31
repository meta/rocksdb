//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/cache.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/backup_engine.h"
#ifdef GFLAGS
#include "db_stress_tool/db_stress_common.h"
#include "util/cast_util.h"

static bool ValidateUint32Range(const char* flagname, uint64_t value) {
  if (value > std::numeric_limits<uint32_t>::max()) {
    fprintf(stderr, "Invalid value for --%s: %lu, overflow\n", flagname,
            (unsigned long)value);
    return false;
  }
  return true;
}

static bool ValidateStressDiagnosticEntryCount(const char* flagname,
                                               uint64_t value) {
  constexpr uint64_t kMaxStressDiagnosticEntries = 64 * 1024;
  if (value > kMaxStressDiagnosticEntries) {
    fprintf(stderr, "Invalid value for --%s: %lu, maximum is %lu\n", flagname,
            (unsigned long)value, (unsigned long)kMaxStressDiagnosticEntries);
    return false;
  }
  return true;
}

DEFINE_uint64(seed, 2341234,
              "Seed for PRNG. When --nooverwritepercent is "
              "nonzero and --expected_values_dir is nonempty, this value "
              "must be fixed across invocations.");
static const bool FLAGS_seed_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_seed, &ValidateUint32Range);

DEFINE_bool(read_only, false, "True if open DB in read-only mode during tests");

DEFINE_int64(max_key, 1 * KB * KB,
             "Max number of key/values to place in database");

DEFINE_int32(max_key_len, 3, "Maximum length of a key in 8-byte units");

DEFINE_uint64(max_sequential_skip_in_iterations,
              ROCKSDB_NAMESPACE::Options().max_sequential_skip_in_iterations,
              "Iterator will reseek after scanning this number of keys with"
              "the same user key during Next/Prev().");

DEFINE_string(key_len_percent_dist, "",
              "Percentages of keys of various lengths. For example, 1,30,69 "
              "means 1% of keys are 8 bytes, 30% are 16 bytes, and 69% are "
              "24 bytes. If not specified, it will be evenly distributed");

DEFINE_int32(key_window_scale_factor, 10,
             "This value will be multiplied by 100 to come up with a window "
             "size for varying the key length");

DEFINE_int32(column_families, 10, "Number of column families");

DEFINE_double(
    hot_key_alpha, 0,
    "Use Zipfian distribution to generate the key "
    "distribution. If it is not specified, write path will use random "
    "distribution to generate the keys. The parameter is [0, double_max]). "
    "However, the larger alpha is, the more shewed will be. If alpha is "
    "larger than 2, it is likely that only 1 key will be accessed. The "
    "Recommended value is [0.8-1.5]. The distribution is also related to "
    "max_key and total iterations of generating the hot key. ");

DEFINE_string(
    options_file, "",
    "The path to a RocksDB options file.  If specified, then db_stress will "
    "run with the RocksDB options in the default column family of the "
    "specified options file. Note that, when an options file is provided, "
    "db_stress will ignore the flag values for all options that may be passed "
    "via options file.");

DEFINE_int64(
    active_width, 0,
    "Number of keys in active span of the key-range at any given time. The "
    "span begins with its left endpoint at key 0, gradually moves rightwards, "
    "and ends with its right endpoint at max_key. If set to 0, active_width "
    "will be sanitized to be equal to max_key.");

// TODO(noetzli) Add support for single deletes
DEFINE_bool(test_batches_snapshots, false,
            "If set, the test uses MultiGet(), MultiPut() and MultiDelete()"
            " which read/write/delete multiple keys in a batch. In this mode,"
            " we do not verify db content by comparing the content with the "
            "pre-allocated array. Instead, we do partial verification inside"
            " MultiGet() by checking various values in a batch. Benefit of"
            " this mode:\n"
            "\t(a) No need to acquire mutexes during writes (less cache "
            "flushes in multi-core leading to speed up)\n"
            "\t(b) No long validation at the end (more speed up)\n"
            "\t(c) Test snapshot and atomicity of batch writes");

DEFINE_bool(atomic_flush, false,
            "If set, enables atomic flush in the options.\n");

DEFINE_int32(
    manual_wal_flush_one_in, 0,
    "If non-zero, then `FlushWAL(bool sync)`, where `bool sync` is randomly "
    "decided, will be explictly called in db stress once for every N ops "
    "on average. Setting `manual_wal_flush_one_in` to be greater than 0 "
    "implies `Options::manual_wal_flush = true` is set.");

DEFINE_int32(lock_wal_one_in, 1000000,
             "If non-zero, then `LockWAL()` + `UnlockWAL()` will be called in "
             "db_stress once for every N ops on average.");

DEFINE_bool(test_cf_consistency, false,
            "If set, runs the stress test dedicated to verifying writes to "
            "multiple column families are consistent. Setting this implies "
            "`atomic_flush=true` is set true if `disable_wal=false`.\n");

DEFINE_bool(test_multi_ops_txns, false,
            "If set, runs stress test dedicated to verifying multi-ops "
            "transactions on a simple relational table with primary and "
            "secondary index.");

DEFINE_int32(threads, 32, "Number of concurrent threads to run.");

DEFINE_int32(ttl, -1,
             "Opens the db with this ttl value if this is not -1. "
             "Carefully specify a large value such that verifications on "
             "deleted values don't fail");

DEFINE_int32(value_size_mult, 8,
             "Size of value will be this number times rand_int(1,3) bytes");

DEFINE_int32(compaction_readahead_size, 0, "Compaction readahead size");

DEFINE_bool(enable_pipelined_write, false, "Pipeline WAL/memtable writes");

DEFINE_bool(verify_before_write, false, "Verify before write");

DEFINE_string(
    verify_cpu_corruption_dir, "",
    "When non-empty, activates a slow, meticulous verification mode intended "
    "only for use with a CPU fault injector; on the first corruption found it "
    "writes a result file here and fails the run. See "
    "StressTest::MaybeVerifyCpuCorruption for the full behavior and "
    "output-file contract. Empty (default) = off.");

DEFINE_bool(histogram, false, "Print histogram of operation timings");

DEFINE_bool(destroy_db_initially, true,
            "Destroys the database dir before start if this is true");

DEFINE_bool(destroy_db_and_exit, false,
            "Destroys the database dir and exits. Useful for cleanup without "
            "running stress test. Other options are mostly ignored.");

DEFINE_string(delete_dir_and_exit, "",
              "Recursively deletes the specified directory and exits. "
              "Useful for cleaning up TEST_TMPDIR after crash tests.");

DEFINE_bool(verbose, false, "Verbose");

DEFINE_bool(progress_reports, true,
            "If true, db_stress will report number of finished operations");

DEFINE_uint64(
    liveness_check_interval_sec, 0,
    "If non-zero, check periodically whether db_stress operations are making "
    "progress. Requires --liveness_no_progress_timeout_sec to be non-zero.");

DEFINE_uint64(
    liveness_no_progress_timeout_sec, 0,
    "If non-zero with --liveness_check_interval_sec, terminate db_stress when "
    "no operation completes for this many seconds during the operation phase.");

DEFINE_string(stress_diagnostics_dir, "",
              "Directory where db_stress writes optional diagnostic artifacts. "
              "If empty, diagnostics that require file output are disabled.");

DEFINE_bool(stress_diagnostics_breadcrumbs, false,
            "If true, keep a bounded per-thread memory-mapped ring of recent "
            "stress operations in --stress_diagnostics_dir when set.");

DEFINE_uint64(stress_diagnostics_breadcrumb_entries, 256,
              "Number of recent operation breadcrumbs retained per stress "
              "thread when --stress_diagnostics_breadcrumbs is true. Maximum "
              "is 65536.");
// NOLINTNEXTLINE(cert-err58-cpp)
static const bool FLAGS_stress_diagnostics_breadcrumb_entries_dummy
    __attribute__((__unused__)) =
        RegisterFlagValidator(&FLAGS_stress_diagnostics_breadcrumb_entries,
                              &ValidateStressDiagnosticEntryCount);

DEFINE_bool(stress_diagnostics_scan_witness, false,
            "If true, retain a bounded in-memory witness log for PrefixScan "
            "and MultiScan operations and write it to --stress_diagnostics_dir "
            "only when the operation reports a verification failure.");

DEFINE_uint64(stress_diagnostics_scan_witness_entries, 128,
              "Number of recent public iterator events retained per "
              "PrefixScan/MultiScan operation when "
              "--stress_diagnostics_scan_witness is true. Maximum is 65536.");
// NOLINTNEXTLINE(cert-err58-cpp)
static const bool FLAGS_stress_diagnostics_scan_witness_entries_dummy
    __attribute__((__unused__)) =
        RegisterFlagValidator(&FLAGS_stress_diagnostics_scan_witness_entries,
                              &ValidateStressDiagnosticEntryCount);

DEFINE_uint64(db_write_buffer_size,
              ROCKSDB_NAMESPACE::Options().db_write_buffer_size,
              "Number of bytes to buffer in all memtables before compacting");

DEFINE_bool(use_write_buffer_manager, false,
            "Charge WriteBufferManager memory to the block cache");

DEFINE_int32(
    wbm_flush_policy,
    ROCKSDB_NAMESPACE::lossless_cast<int32_t>(
        ROCKSDB_NAMESPACE::WriteBufferFlushPolicy::kFlushOldest),
    "Which memtable the WriteBufferManager flushes to free memory (see `enum "
    "class WriteBufferFlushPolicy` in write_buffer_manager.h): 0 = oldest, 1 = "
    "largest in the same DB, 2 = largest across all DBs sharing the manager");

DEFINE_int32(
    write_buffer_size,
    static_cast<int32_t>(ROCKSDB_NAMESPACE::Options().write_buffer_size),
    "Number of bytes to buffer in memtable before compacting");

DEFINE_int32(max_write_buffer_number,
             ROCKSDB_NAMESPACE::Options().max_write_buffer_number,
             "The number of in-memory memtables. "
             "Each memtable is of size FLAGS_write_buffer_size.");

DEFINE_int32(min_write_buffer_number_to_merge,
             ROCKSDB_NAMESPACE::Options().min_write_buffer_number_to_merge,
             "The minimum number of write buffers that will be merged together "
             "before writing to storage. This is cheap because it is an "
             "in-memory merge. If this feature is not enabled, then all these "
             "write buffers are flushed to L0 as separate files and this "
             "increases read amplification because a get request has to check "
             "in all of these files. Also, an in-memory merge may result in "
             "writing less data to storage if there are duplicate records in"
             " each of these individual write buffers.");

DEFINE_int64(max_write_buffer_size_to_maintain,
             ROCKSDB_NAMESPACE::Options().max_write_buffer_size_to_maintain,
             "The total maximum size of write buffers to maintain in memory "
             "including copies of buffers that have already been flushed. "
             "Unlike max_write_buffer_number, this parameter does not affect "
             "flushing. This controls the minimum amount of write history "
             "that will be available in memory for conflict checking when "
             "Transactions are used. If this value is too low, some "
             "transactions may fail at commit time due to not being able to "
             "determine whether there were any write conflicts. Setting this "
             "value to 0 will cause write buffers to be freed immediately "
             "after they are flushed.  If this value is set to -1, "
             "'max_write_buffer_number' will be used.");

DEFINE_double(memtable_prefix_bloom_size_ratio,
              ROCKSDB_NAMESPACE::Options().memtable_prefix_bloom_size_ratio,
              "creates prefix blooms for memtables, each with size "
              "`write_buffer_size * memtable_prefix_bloom_size_ratio`.");

DEFINE_bool(memtable_whole_key_filtering,
            ROCKSDB_NAMESPACE::Options().memtable_whole_key_filtering,
            "Enable whole key filtering in memtables.");

DEFINE_int32(open_files, ROCKSDB_NAMESPACE::Options().max_open_files,
             "Maximum number of files to keep open at the same time "
             "(use default if == 0)");

DEFINE_bool(open_files_async, ROCKSDB_NAMESPACE::Options().open_files_async,
            "Options.open_files_async");

DEFINE_bool(async_wal_precreate,
            ROCKSDB_NAMESPACE::Options().async_wal_precreate,
            "Options.async_wal_precreate");

DEFINE_uint64(compressed_secondary_cache_size, 0,
              "Number of bytes to use as a cache of compressed data."
              " 0 means use default settings.");

DEFINE_int32(compressed_secondary_cache_numshardbits, -1,
             "Number of shards for the compressed secondary cache is 2 ** "
             "compressed_secondary_cache_numshardbits. "
             "Negative value means default settings. This is applied only "
             "if compressed_secondary_cache_size is greater than 0.");

DEFINE_double(compressed_secondary_cache_ratio, 0.0,
              "Fraction of block cache memory budget to use for compressed "
              "secondary cache");

DEFINE_int32(secondary_cache_update_interval, 30 * 1000 * 1000,
             "Interval between modification of secondary cache parameters, in "
             "microseconds");

DEFINE_int32(compaction_style, ROCKSDB_NAMESPACE::Options().compaction_style,
             "");

DEFINE_int32(compaction_pri, ROCKSDB_NAMESPACE::Options().compaction_pri,
             "Which file from a level should be picked to merge to the next "
             "level in level-based compaction");

DEFINE_int32(num_levels, ROCKSDB_NAMESPACE::Options().num_levels,
             "Number of levels in the DB");

DEFINE_int32(level0_file_num_compaction_trigger,
             ROCKSDB_NAMESPACE::Options().level0_file_num_compaction_trigger,
             "Level0 compaction start trigger");

DEFINE_int32(level0_slowdown_writes_trigger,
             ROCKSDB_NAMESPACE::Options().level0_slowdown_writes_trigger,
             "Number of files in level-0 that will slow down writes");

DEFINE_int32(level0_stop_writes_trigger,
             ROCKSDB_NAMESPACE::Options().level0_stop_writes_trigger,
             "Number of files in level-0 that will trigger put stop.");

DEFINE_int32(block_size,
             static_cast<int32_t>(
                 ROCKSDB_NAMESPACE::BlockBasedTableOptions().block_size),
             "Number of bytes in a block.");

DEFINE_int32(format_version,
             static_cast<int32_t>(
                 ROCKSDB_NAMESPACE::BlockBasedTableOptions().format_version),
             "Format version of SST files.");

DEFINE_bool(separate_key_value_in_data_block,
            ROCKSDB_NAMESPACE::BlockBasedTableOptions()
                .separate_key_value_in_data_block,
            "If true, data blocks store keys and values separately.");

DEFINE_int32(
    index_block_restart_interval,
    ROCKSDB_NAMESPACE::BlockBasedTableOptions().index_block_restart_interval,
    "Number of keys between restart points "
    "for delta encoding of keys in index block.");

DEFINE_bool(disable_auto_compactions,
            ROCKSDB_NAMESPACE::Options().disable_auto_compactions,
            "If true, RocksDB internally will not trigger compactions.");

DEFINE_int32(max_background_compactions,
             ROCKSDB_NAMESPACE::Options().max_background_compactions,
             "The maximum number of concurrent background compactions "
             "that can occur in parallel.");

DEFINE_int32(num_bottom_pri_threads, 0,
             "The number of threads in the bottom-priority thread pool (used "
             "by universal compaction only).");

DEFINE_int32(compaction_thread_pool_adjust_interval, 0,
             "The interval (in milliseconds) to adjust compaction thread pool "
             "size. Don't change it periodically if the value is 0.");

DEFINE_int32(compaction_thread_pool_variations, 2,
             "Range of background thread pool size variations when adjusted "
             "periodically.");

DEFINE_int32(max_background_flushes,
             ROCKSDB_NAMESPACE::Options().max_background_flushes,
             "The maximum number of concurrent background flushes "
             "that can occur in parallel.");

DEFINE_int32(universal_size_ratio, 0,
             "The ratio of file sizes that trigger"
             " compaction in universal style");

DEFINE_int32(universal_min_merge_width, 0,
             "The minimum number of files to "
             "compact in universal style compaction");

DEFINE_int32(universal_max_merge_width, 0,
             "The max number of files to compact"
             " in universal style compaction");

DEFINE_int32(universal_max_size_amplification_percent, 0,
             "The max size amplification for universal style compaction");

DEFINE_int32(universal_max_read_amp, -1,
             "The limit on the number of sorted runs");

DEFINE_int32(clear_column_family_one_in, 1000000,
             "With a chance of 1/N, delete a column family and then recreate "
             "it again. If N == 0, never drop/create column families. "
             "When test_batches_snapshots is true, this flag has no effect");

DEFINE_bool(test_cf_lifecycle, false,
            "Run bounded column-family lifecycle stress. This mode uses two "
            "column families so a failed replacement create can be recovered "
            "without changing expected-state slot ordering.");

DEFINE_int32(
    cf_lifecycle_fault_target, 0,
    "Target the replacement CreateColumnFamily persistence path with an "
    "injected error: 0=none, 1=OPTIONS, 2=MANIFEST. Requires "
    "--test_cf_lifecycle and tracked OPTIONS.");

DEFINE_int32(
    get_live_files_apis_one_in, 1000000,
    "With a chance of 1/N, call GetLiveFiles(), GetLiveFilesMetaData() and "
    "GetLiveFilesChecksumInfo() to verify if it returns "
    "OK or violate any internal assertion. If N == 0, do not call the "
    "interface.");

DEFINE_bool(
    checkpoint_atomic_flush, false,
    "If true, when calling GetLiveFilesStorageInfo (e.g., during checkpoint), "
    "use LiveFilesStorageInfoOptions::atomic_flush=true to force atomic flush "
    "of all column families regardless of the DBOptions::atomic_flush setting. "
    "This exercises the per-request atomic flush override code path.");

DEFINE_int32(
    get_all_column_family_metadata_one_in, 1000000,
    "With a chance of 1/N, call GetAllColumnFamilyMetaData to verify if it "
    "violates any internal assertion. If N == 0, do not call the interface.");

DEFINE_int32(
    get_sorted_wal_files_one_in, 1000000,
    "With a chance of 1/N, call GetSortedWalFiles to verify if it returns "
    "correctly. (Note that this API may legitimately return an error.) If N == "
    "0, do not call the interface.");

DEFINE_int32(
    get_current_wal_file_one_in, 1000000,
    "With a chance of 1/N, call GetCurrentWalFile to verify if it returns "
    "correctly. (Note that this API may legitimately return an error.) If N == "
    "0, do not call the interface.");

DEFINE_int32(set_options_one_in, 0,
             "With a chance of 1/N, change some random options");

DEFINE_int32(set_in_place_one_in, 0,
             "With a chance of 1/N, toggle in place support option");

DEFINE_int64(cache_size, 2LL * KB * KB * KB,
             "Number of bytes to use as a cache of uncompressed data.");

DEFINE_int32(cache_numshardbits, 6,
             "Number of shards for the block cache"
             " is 2 ** cache_numshardbits. Negative means use default settings."
             " This is applied only if FLAGS_cache_size is greater than 0.");

DEFINE_bool(cache_index_and_filter_blocks, false,
            "True if indexes/filters should be cached in block cache.");

DEFINE_bool(charge_compression_dictionary_building_buffer, false,
            "Setting for "
            "CacheEntryRoleOptions::charged of "
            "CacheEntryRole::kCompressionDictionaryBuildingBuffer");

DEFINE_bool(charge_filter_construction, false,
            "Setting for "
            "CacheEntryRoleOptions::charged of "
            "CacheEntryRole::kFilterConstruction");

DEFINE_bool(charge_table_reader, false,
            "Setting for "
            "CacheEntryRoleOptions::charged of "
            "CacheEntryRole::kBlockBasedTableReader");

DEFINE_bool(charge_file_metadata, false,
            "Setting for "
            "CacheEntryRoleOptions::charged of "
            "kFileMetadata");

DEFINE_bool(charge_blob_cache, false,
            "Setting for "
            "CacheEntryRoleOptions::charged of "
            "kBlobCache");

DEFINE_bool(
    decouple_partitioned_filters,
    ROCKSDB_NAMESPACE::BlockBasedTableOptions().decouple_partitioned_filters,
    "Decouple filter partitioning from index partitioning.");

DEFINE_int32(
    top_level_index_pinning,
    static_cast<int32_t>(ROCKSDB_NAMESPACE::PinningTier::kFallback),
    "Type of pinning for top-level indexes into metadata partitions (see "
    "`enum PinningTier` in table.h)");

DEFINE_int32(
    partition_pinning,
    static_cast<int32_t>(ROCKSDB_NAMESPACE::PinningTier::kFallback),
    "Type of pinning for metadata partitions (see `enum PinningTier` in "
    "table.h)");

DEFINE_int32(
    unpartitioned_pinning,
    static_cast<int32_t>(ROCKSDB_NAMESPACE::PinningTier::kFallback),
    "Type of pinning for unpartitioned metadata blocks (see `enum PinningTier` "
    "in table.h)");

DEFINE_string(cache_type, "lru_cache", "Type of block cache.");

DEFINE_uint64(subcompactions, 1,
              "Maximum number of subcompactions to divide L0-L1 compactions "
              "into.");

DEFINE_uint64(periodic_compaction_seconds, 1000,
              "Files older than this value will be picked up for compaction.");
DEFINE_string(daily_offpeak_time_utc, "",
              "If set, process periodic compactions during this period only");

DEFINE_uint64(compaction_ttl, 1000,
              "Files older than TTL will be compacted to the next level.");

DEFINE_bool(fifo_allow_compaction, false,
            "If true, set `Options::compaction_options_fifo.allow_compaction = "
            "true`. It only take effect when FIFO compaction is used.");

DEFINE_uint64(fifo_compaction_max_data_files_size_mb, 0,
              "If non-zero, set "
              "`Options::compaction_options_fifo.max_data_files_size` to this "
              "value (in MB). Only takes effect with FIFO compaction. "
              "When non-zero, overrides `max_table_files_size` for "
              "determining when FIFO drops old SST files.");

DEFINE_uint64(fifo_compaction_max_table_files_size_mb, 0,
              "If non-zero, set "
              "`Options::compaction_options_fifo.max_table_files_size` to this "
              "value (in MB). Only takes effect with FIFO compaction. "
              "When zero, uses the default (1GB). Ignored when "
              "`fifo_compaction_max_data_files_size_mb` is non-zero.");

DEFINE_bool(fifo_compaction_use_kv_ratio_compaction, false,
            "If true, set "
            "`Options::compaction_options_fifo.use_kv_ratio_compaction = "
            "true`. Recommends fifo_allow_compaction and "
            "fifo_compaction_max_data_files_size_mb > 0 (falls back to "
            "cost-based intra-L0 compaction if not set).");

DEFINE_bool(allow_concurrent_memtable_write, false,
            "Allow multi-writers to update mem tables in parallel.");

DEFINE_double(experimental_mempurge_threshold, 0.0,
              "Maximum estimated useful payload that triggers a "
              "mempurge process to collect memtable garbage bytes.");

DEFINE_bool(enable_write_thread_adaptive_yield,
            ROCKSDB_NAMESPACE::Options().enable_write_thread_adaptive_yield,
            "Use a yielding spin loop for brief writer thread waits.");

// Options for StackableDB-based BlobDB
DEFINE_bool(use_blob_db, false, "[Stacked BlobDB] Use BlobDB.");

DEFINE_uint64(blob_db_file_size,
              ROCKSDB_NAMESPACE::blob_db::BlobDBOptions().blob_file_size,
              "[Stacked BlobDB] Target size of each blob file.");

DEFINE_bool(
    blob_db_enable_gc,
    ROCKSDB_NAMESPACE::blob_db::BlobDBOptions().enable_garbage_collection,
    "[Stacked BlobDB] Enable BlobDB garbage collection.");

// Options for integrated BlobDB
DEFINE_bool(allow_setting_blob_options_dynamically, false,
            "[Integrated BlobDB] Allow setting blob options dynamically.");

DEFINE_bool(
    enable_blob_files,
    ROCKSDB_NAMESPACE::AdvancedColumnFamilyOptions().enable_blob_files,
    "[Integrated BlobDB] Enable writing large values to separate blob files.");

DEFINE_bool(
    enable_blob_direct_write,
    ROCKSDB_NAMESPACE::AdvancedColumnFamilyOptions().enable_blob_direct_write,
    "[Integrated BlobDB] Enable direct-write blob file creation on "
    "the write path.");

DEFINE_uint64(blob_direct_write_partitions,
              ROCKSDB_NAMESPACE::AdvancedColumnFamilyOptions()
                  .blob_direct_write_partitions,
              "[Integrated BlobDB] Number of partitions for direct-write blob "
              "files.");

namespace ROCKSDB_NAMESPACE {

void RegisterDbStressBdwFlagValidators() {
  static const bool blob_direct_write_partitions_validator_registered =
      RegisterFlagValidator(&FLAGS_blob_direct_write_partitions,
                            &ValidateUint32Range);
  (void)blob_direct_write_partitions_validator_registered;
}

}  // namespace ROCKSDB_NAMESPACE

DEFINE_uint64(min_blob_size,
              ROCKSDB_NAMESPACE::AdvancedColumnFamilyOptions().min_blob_size,
              "[Integrated BlobDB] The size of the smallest value to be stored "
              "separately in a blob file.");

DEFINE_uint64(blob_file_size,
              ROCKSDB_NAMESPACE::AdvancedColumnFamilyOptions().blob_file_size,
              "[Integrated BlobDB] The size limit for blob files.");

DEFINE_uint64(
    blob_file_writable_file_max_buffer_size,
    ROCKSDB_NAMESPACE::AdvancedColumnFamilyOptions()
        .blob_file_writable_file_max_buffer_size,
    "[Integrated BlobDB] Max WritableFileWriter buffer size for blob files. "
    "0 means inherit writable_file_max_buffer_size.");

DEFINE_string(blob_compression_type, "none",
              "[Integrated BlobDB] The compression algorithm to use for large "
              "values stored in blob files.");
DEFINE_string(compression_manager, "mixed",
              "Ability to change compression manager specified in "
              "simple_mixed_manager.h (mixed -> roundRobin)");
DEFINE_bool(enable_blob_garbage_collection,
            ROCKSDB_NAMESPACE::AdvancedColumnFamilyOptions()
                .enable_blob_garbage_collection,
            "[Integrated BlobDB] Enable blob garbage collection.");

DEFINE_double(blob_garbage_collection_age_cutoff,
              ROCKSDB_NAMESPACE::AdvancedColumnFamilyOptions()
                  .blob_garbage_collection_age_cutoff,
              "[Integrated BlobDB] The cutoff in terms of blob file age for "
              "garbage collection.");

DEFINE_double(blob_garbage_collection_force_threshold,
              ROCKSDB_NAMESPACE::AdvancedColumnFamilyOptions()
                  .blob_garbage_collection_force_threshold,
              "[Integrated BlobDB] The threshold for the ratio of garbage in "
              "the eligible blob files for forcing garbage collection.");

DEFINE_uint64(blob_compaction_readahead_size,
              ROCKSDB_NAMESPACE::AdvancedColumnFamilyOptions()
                  .blob_compaction_readahead_size,
              "[Integrated BlobDB] Compaction readahead for blob files.");

DEFINE_int32(
    blob_file_starting_level,
    ROCKSDB_NAMESPACE::AdvancedColumnFamilyOptions().blob_file_starting_level,
    "[Integrated BlobDB] Enable writing blob files during flushes and "
    "compactions starting from the specified level.");

DEFINE_bool(use_blob_cache, false, "[Integrated BlobDB] Enable blob cache.");

DEFINE_bool(
    use_shared_block_and_blob_cache, true,
    "[Integrated BlobDB] Use a shared backing cache for both block "
    "cache and blob cache. It only takes effect if use_blob_cache is enabled.");

DEFINE_uint64(
    blob_cache_size, 2LL * KB * KB * KB,
    "[Integrated BlobDB] Number of bytes to use as a cache of blobs. It only "
    "takes effect if the block and blob caches are different "
    "(use_shared_block_and_blob_cache = false).");

DEFINE_int32(blob_cache_numshardbits, 6,
             "[Integrated BlobDB] Number of shards for the blob cache is 2 ** "
             "blob_cache_numshardbits. Negative means use default settings. "
             "It only takes effect if blob_cache_size is greater than 0, and "
             "the block and blob caches are different "
             "(use_shared_block_and_blob_cache = false).");

DEFINE_int32(prepopulate_blob_cache, 0,
             "[Integrated BlobDB] Pre-populate hot/warm blobs in blob cache. 0 "
             "to disable and 1 to insert during flush.");

DEFINE_int64(preclude_last_level_data_seconds, 0,
             "Preclude data from the last level. Used with tiered storage "
             "feature to preclude new data from comacting to the last level.");

DEFINE_int64(
    preserve_internal_time_seconds, 0,
    "Preserve internal time information which is attached to each SST.");

DEFINE_uint32(use_timed_put_one_in, 0,
              "If greater than zero, TimedPut is used per every N write ops on "
              "on average.");

DEFINE_string(file_temperature_age_thresholds, "",
              "See CompactionOptionsFIFO::file_temperature_age_thresholds. "
              "empty == unset");

DEFINE_bool(allow_trivial_copy_when_change_temperature, true,
            "Allow kChangeTemperature to do trivial copy");

static const bool FLAGS_subcompactions_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_subcompactions, &ValidateUint32Range);

static bool ValidateInt32Positive(const char* flagname, int32_t value) {
  if (value < 0) {
    fprintf(stderr, "Invalid value for --%s: %d, must be >=0\n", flagname,
            value);
    return false;
  }
  return true;
}
DEFINE_int32(reopen, 10, "Number of times database reopens");
static const bool FLAGS_reopen_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_reopen, &ValidateInt32Positive);

DEFINE_double(bloom_bits, 10,
              "Bloom filter bits per key. "
              "Negative means use default settings.");

DEFINE_int32(
    bloom_before_level, 999,
    "Use Bloom filter on levels below specified and Ribbon beginning on level "
    "specified. Flush is considered level -1. Setting -1 -> always Ribbon. "
    "0 -> Ribbon except Bloom for flush. INT_MAX (typically 2147483647) -> "
    "always Bloom.");

DEFINE_bool(partition_filters, false,
            "use partitioned filters "
            "for block-based table");

DEFINE_bool(
    optimize_filters_for_memory,
    ROCKSDB_NAMESPACE::BlockBasedTableOptions().optimize_filters_for_memory,
    "Minimize memory footprint of filters");

DEFINE_bool(
    detect_filter_construct_corruption,
    ROCKSDB_NAMESPACE::BlockBasedTableOptions()
        .detect_filter_construct_corruption,
    "Detect corruption during new Bloom Filter and Ribbon Filter construction");

DEFINE_string(sqfc_name, "foo",
              "Config name to select from SstQueryFilterConfigsManager.");

DEFINE_uint32(sqfc_version, 0,
              "User-defined filtering version to select from "
              "SstQueryFilterConfigsManager. 0 = disable writing filters");

DEFINE_bool(use_sqfc_for_range_queries, true,
            "Apply SstQueryFilters to range queries");

DEFINE_int32(
    index_type,
    static_cast<int32_t>(
        ROCKSDB_NAMESPACE::BlockBasedTableOptions().index_type),
    "Type of block-based table index (see `enum IndexType` in table.h)");

DEFINE_int32(
    data_block_index_type,
    static_cast<int32_t>(
        ROCKSDB_NAMESPACE::BlockBasedTableOptions().data_block_index_type),
    "Index type for data blocks (see `enum DataBlockIndexType` in table.h)");

DEFINE_int32(optimize_key_common_prefix,
             static_cast<int32_t>(ROCKSDB_NAMESPACE::BlockBasedTableOptions()
                                      .optimize_key_common_prefix),
             "When/whether data blocks store the common user-key prefix once "
             "(see `enum OptimizeKeyCommonPrefix` in table.h): 0=kDisabled, "
             "1=kIfFastSeek, 2=kEnabled. Requires format_version >= 8.");

DEFINE_int32(index_block_search_type,
             static_cast<int32_t>(ROCKSDB_NAMESPACE::BlockBasedTableOptions()
                                      .index_block_search_type),
             "Search algorithm for index blocks (see `enum BlockSearchType` in "
             "table.h)");

DEFINE_double(uniform_cv_threshold,
              ROCKSDB_NAMESPACE::BlockBasedTableOptions().uniform_cv_threshold,
              "CV threshold for marking index blocks as uniform. Set to -1 to "
              "disable. (see `uniform_cv_threshold` in table.h)");

DEFINE_bool(use_trie_index, false,
            "Use trie-based user defined index (UDI) for SST files. "
            "Compatible with all operation types (Put, Delete, Merge, etc.) "
            "and all iteration directions (forward and reverse). "
            "Combined with index_mode to control the UDI role.");

DEFINE_uint32(trie_index_format_version, 1,
              "TrieIndexFactory format_version for new SST files, when "
              "use_trie_index is set. Existing SSTs of every format remain "
              "readable.");

DEFINE_int32(index_mode,
             static_cast<int32_t>(
                 ROCKSDB_NAMESPACE::BlockBasedTableOptions().index_mode),
             "BlockBasedTableOptions::index_mode: 0=kStandardOnly, "
             "1=kStandardDefault, 2=kCustomDefault, 3=kCustomOnly, "
             "4=kStandardRequired. Values 2-4 require use_trie_index.");

DEFINE_int32(read_index, -1,
             "Per-read index selection (ReadOptions::read_index). -1=auto "
             "(kPreferCustom when use_trie_index=1 and the custom index is "
             "secondary, kDefault otherwise), 0=kDefault, 1=kBuiltin, "
             "2=kPreferCustom. A value above 0 requires use_trie_index; "
             "kBuiltin is incompatible with index_mode=3 (kCustomOnly).");

DEFINE_bool(use_full_external_table, false,
            "Use the simple full external table test factory.");

DEFINE_bool(test_backward_scan, true,
            "Test backward iteration (Prev, SeekForPrev) in stress tests.");

DEFINE_string(db, "", "Use the db with the following name.");

DEFINE_string(secondaries_base, "",
              "Use this path as the base path for secondary instances.");

DEFINE_bool(test_secondary, false,
            "If true, start an additional secondary instance which can be used "
            "for verification.");

DEFINE_int32(open_read_only_one_in, 0,
             "If greater than 0, on thread 0 with probability 1/N per "
             "operation, open a read-only DB instance on the primary's live "
             "directory, flush the primary, then close the reader. Stresses "
             "concurrent primary + read-only DB on the same directory to guard "
             "against a read-only DB's close deleting the primary's live SST "
             "files. 0 disables.");

DEFINE_string(
    expected_values_dir, "",
    "Dir where files containing info about the latest/historical values will "
    "be stored. If provided and non-empty, the DB state will be verified "
    "against values from these files after recovery. --max_key and "
    "--column_family must be kept the same across invocations of this program "
    "that use the same --expected_values_dir. Currently historical values are "
    "only tracked when --sync_fault_injection is set. See --seed and "
    "--nooverwritepercent for further requirements.");

DEFINE_bool(verify_checksum, false,
            "Verify checksum for every block read from storage");

DEFINE_bool(mmap_read, ROCKSDB_NAMESPACE::Options().allow_mmap_reads,
            "Allow reads to occur via mmap-ing files");

DEFINE_bool(mmap_write, ROCKSDB_NAMESPACE::Options().allow_mmap_writes,
            "Allow writes to occur via mmap-ing files");

DEFINE_bool(use_direct_reads, ROCKSDB_NAMESPACE::Options().use_direct_reads,
            "Use O_DIRECT for reading data");

DEFINE_bool(use_direct_io_for_compaction_reads,
            ROCKSDB_NAMESPACE::Options().use_direct_io_for_compaction_reads,
            "Use O_DIRECT for compaction-input SST reads only, while keeping "
            "user reads buffered");

DEFINE_bool(use_direct_io_for_flush_and_compaction,
            ROCKSDB_NAMESPACE::Options().use_direct_io_for_flush_and_compaction,
            "Use O_DIRECT for writing data");

DEFINE_bool(mock_direct_io, false,
            "Mock direct IO by not using O_DIRECT for direct IO read");

DEFINE_bool(statistics, false, "Create database statistics");

DEFINE_bool(sync, false, "Sync all writes to disk");

DEFINE_bool(use_fsync, false, "If true, issue fsync instead of fdatasync");

DEFINE_uint64(bytes_per_sync, ROCKSDB_NAMESPACE::Options().bytes_per_sync,
              "If nonzero, sync SST file data incrementally after every "
              "`bytes_per_sync` bytes are written");

DEFINE_uint64(wal_bytes_per_sync,
              ROCKSDB_NAMESPACE::Options().wal_bytes_per_sync,
              "If nonzero, sync WAL file data incrementally after every "
              "`bytes_per_sync` bytes are written");

DEFINE_int32(kill_random_test, 0,
             "If non-zero, kill at various points in source code with "
             "probability 1/this");
static const bool FLAGS_kill_random_test_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_kill_random_test, &ValidateInt32Positive);

DEFINE_string(kill_exclude_prefixes, "",
              "If non-empty, kill points with prefix in the list given will be"
              " skipped. Items are comma-separated.");
extern std::vector<std::string> rocksdb_kill_exclude_prefixes;

DEFINE_bool(disable_wal, false, "If true, do not write WAL for write.");

DEFINE_uint64(recycle_log_file_num,
              ROCKSDB_NAMESPACE::Options().recycle_log_file_num,
              "Number of old WAL files to keep around for later recycling");

DEFINE_int64(target_file_size_base,
             ROCKSDB_NAMESPACE::Options().target_file_size_base,
             "Target level-1 file size for compaction");

DEFINE_int32(target_file_size_multiplier, 1,
             "A multiplier to compute target level-N file size (N >= 2)");

DEFINE_uint64(max_bytes_for_level_base,
              ROCKSDB_NAMESPACE::Options().max_bytes_for_level_base,
              "Max bytes for level-1");

DEFINE_double(max_bytes_for_level_multiplier, 2,
              "A multiplier to compute max bytes for level-N (N >= 2)");

DEFINE_int32(range_deletion_width, 10,
             "The width of the range deletion intervals.");

DEFINE_uint64(rate_limiter_bytes_per_sec, 0, "Set options.rate_limiter value.");

DEFINE_bool(rate_limit_bg_reads, false,
            "Use options.rate_limiter on compaction reads");

DEFINE_bool(rate_limit_user_ops, false,
            "When true use Env::IO_USER priority level to charge internal rate "
            "limiter for reads associated with user operations.");

DEFINE_bool(rate_limit_auto_wal_flush, false,
            "When true use Env::IO_USER priority level to charge internal rate "
            "limiter for automatic WAL flush (`Options::manual_wal_flush` == "
            "false) after the user "
            "write operation.");

DEFINE_uint64(sst_file_manager_bytes_per_sec, 0,
              "Set `Options::sst_file_manager` to delete at this rate. By "
              "default the deletion rate is unbounded.");

DEFINE_uint64(sst_file_manager_bytes_per_truncate, 0,
              "Set `Options::sst_file_manager` to delete in chunks of this "
              "many bytes. By default whole files will be deleted.");

DEFINE_bool(use_txn, false,
            "Use TransactionDB or OptimisticTransactionDB. When "
            "use_optimistic_txn == false (by default), "
            "it's (Pessimistic) TransactionDB");

DEFINE_uint64(txn_write_policy, 0,
              "The transaction write policy. Default is "
              "TxnDBWritePolicy::WRITE_COMMITTED. Note that this should not be "
              "changed across crashes.");

DEFINE_bool(use_per_key_point_lock_mgr, true,
            "Use PointLockManager(false) or PerKeyPointLockManager(true) in "
            "TransactionDB.");

DEFINE_bool(use_optimistic_txn, false, "Use OptimisticTransactionDB.");
DEFINE_uint64(occ_validation_policy, 1,
              "Optimistic Concurrency Control Validation Policy for "
              "OptimisticTransactionDB");
DEFINE_bool(share_occ_lock_buckets, false,
            "Share a pool of locks across DB instances for buckets");
DEFINE_uint32(
    occ_lock_bucket_count, 500,
    "Bucket Count for shared Optimistic Concurrency Control (OCC) locks");

DEFINE_bool(unordered_write, false,
            "Turn on the unordered_write feature. This options is currently "
            "tested only in combination with use_txn=true and "
            "txn_write_policy=TxnDBWritePolicy::WRITE_PREPARED.");

DEFINE_int32(backup_one_in, 0,
             "If non-zero, then CreateNewBackup() will be called once for "
             "every N operations on average.  0 indicates CreateNewBackup() "
             "is disabled.");

DEFINE_uint64(backup_max_size, 100 * 1024 * 1024,
              "If non-zero, skip checking backup/restore when DB size in "
              "bytes exceeds this setting.");

DEFINE_int32(checkpoint_one_in, 0,
             "If non-zero, then CreateCheckpoint() will be called once for "
             "every N operations on average.  0 indicates CreateCheckpoint() "
             "is disabled.");

DEFINE_int32(checkpoint_engine_max_background_operations, 1,
             "Background-thread parallelism of the shared CheckpointEngine "
             "(always opened). Values <= 0 are treated as 1.");

DEFINE_bool(checkpoint_engine_use_link_file_when_available, true,
            "Whether the parallel CheckpointEngine may hard-link data files "
            "instead of copying them. False forces the parallel copy path.");

DEFINE_int32(parallel_checkpoint_one_in, 0,
             "Each checkpoint uses the parallel CheckpointEngine with "
             "probability 1/N, else the legacy serial API (0 = always serial, "
             "1 = always parallel).");

DEFINE_int32(subset_cf_checkpoint_one_in, 0,
             "Each checkpoint covers only a random proper subset of the column "
             "families with probability 1/N (0 = always all column families, "
             "1 = always a subset). Ignored when there is only the default "
             "column family.");

DEFINE_int32(ingest_external_file_one_in, 0,
             "If non-zero, then IngestExternalFile() will be called once for "
             "every N operations on average.  0 indicates IngestExternalFile() "
             "is disabled.");

DEFINE_int32(ingest_external_file_width, 100,
             "The width of the ingested external files.");

DEFINE_int32(ingest_external_file_prepare_commit_one_in, 0,
             "If non-zero, an ingestion that would call IngestExternalFile() "
             "instead uses the two-phase PrepareFileIngestion()/"
             "CommitFileIngestion() API once for every N such ingestions on "
             "average, occasionally dropping the prepared handle without "
             "committing to exercise the rollback path. 0 disables it.");

DEFINE_int32(ingest_external_file_use_file_info_one_in, 0,
             "If non-zero, the ingestexternalfile flow reuses each file's "
             "metadata via IngestExternalFileArg::file_infos (from "
             "SstFileWriter::Finish) once every N ingestions on average, so "
             "ingestion skips re-opening and scanning the files.");

DEFINE_int32(
    ingest_external_file_atomic_replace_one_in, 0,
    "If non-zero, file ingestion atomically replaces a range strictly inside "
    "an existing SST once every N ingestion operations on average. Requires "
    "universal compaction, ingest_external_file_width >= 2, no user-defined "
    "timestamps, and no acquired snapshots.");

DEFINE_bool(
    ingest_external_file_with_embedded_blobs, false,
    "If true, external files for ingestion are written with embedded "
    "blobs via SstFileWriter::OpenWithEmbeddedBlobs(). min_blob_size is "
    "set so that only the largest generated values are embedded. "
    "Requires block-based table format_version >= 7 and "
    "ingest_external_file_one_in > 0.");

DEFINE_int32(compact_files_one_in, 0,
             "If non-zero, then CompactFiles() will be called once for every N "
             "operations on average.  0 indicates CompactFiles() is disabled.");

DEFINE_int32(compact_range_one_in, 0,
             "If non-zero, then CompactRange() will be called once for every N "
             "operations on average.  0 indicates CompactRange() is disabled.");

DEFINE_int32(promote_l0_one_in, 0,
             "If non-zero, then PromoteL0() will be called once for every N "
             "operations on average.  0 indicates PromoteL0() is disabled.");

DEFINE_int32(mark_for_compaction_one_file_in, 0,
             "A `TablePropertiesCollectorFactory` will be registered, which "
             "creates a `TablePropertiesCollector` with `NeedCompact()` "
             "returning true once for every N files on average. 0 or negative "
             "mean `NeedCompact()` always returns false.");

DEFINE_int32(flush_one_in, 0,
             "If non-zero, then Flush() will be called once for every N ops "
             "on average.  0 indicates calls to Flush() are disabled.");

DEFINE_int32(key_may_exist_one_in, 0,
             "If non-zero, then KeyMayExist() will be called "
             "once for every N ops on average.  0 disables.");

DEFINE_int32(reset_stats_one_in, 0,
             "If non-zero, then ResetStats() will be called "
             "once for every N ops on average.  0 disables.");

DEFINE_int32(pause_background_one_in, 0,
             "If non-zero, then PauseBackgroundWork()+Continue will be called "
             "once for every N ops on average.  0 disables.");

DEFINE_int32(disable_file_deletions_one_in, 0,
             "If non-zero, then DisableFileDeletions()+Enable will be called "
             "once for every N ops on average.  0 disables.");

DEFINE_int32(
    disable_manual_compaction_one_in, 0,
    "If non-zero, then DisableManualCompaction()+Enable will be called "
    "once for every N ops on average.  0 disables.");

DEFINE_int32(abort_and_resume_compactions_one_in, 0,
             "If non-zero, then AbortAllCompactions()+Resume will be called "
             "once for every N ops on average. 0 disables.");

DEFINE_int32(abort_and_resume_cf_compactions_one_in, 0,
             "If non-zero, then AbortCompactions(cf)+ResumeCompactions(cf) "
             "will be called on a random column family once for every N ops "
             "on average. 0 disables.");

DEFINE_int32(compact_range_width, 10000,
             "The width of the ranges passed to CompactRange().");

DEFINE_int32(acquire_snapshot_one_in, 0,
             "If non-zero, then acquires a snapshot once every N operations on "
             "average.");

DEFINE_bool(compare_full_db_state_snapshot, false,
            "If set we compare state of entire db (in one of the threads) with"
            "each snapshot.");

DEFINE_uint64(snapshot_hold_ops, 0,
              "If non-zero, then releases snapshots N operations after they're "
              "acquired.");

DEFINE_bool(long_running_snapshots, false,
            "If set, hold on some some snapshots for much longer time.");

// The following four options affect both regular read operations during the
// test and initial/final database verification through VerifyDB.
DEFINE_bool(use_multiget, false,
            "If set, use the batched MultiGet API for reads.");

DEFINE_bool(use_async_db_api, false,
            "If set, use DB::GetAsync and DB::MultiGetAsync for reads.");

DEFINE_bool(use_get_entity, false, "If set, use the GetEntity API for reads.");

DEFINE_bool(use_multi_get_entity, false,
            "If set, use the MultiGetEntity API for reads.");

DEFINE_int32(
    lazy_entity_read_one_in, 0,
    "If greater than 0, then on roughly 1/N of GetEntity / MultiGetEntity "
    "operations, additionally read the same key(s) via the lazy wide-column "
    "API "
    "(GetEntityLazy / MultiGetEntityLazy) under a pinned snapshot and verify "
    "the "
    "lazily-resolved columns (a random subset, possibly none) match the eager "
    "GetEntity result. Only takes effect when open_files == -1 (required by "
    "the "
    "lazy API), user_timestamp_size == 0, and not using transactions.");

DEFINE_int32(test_ingest_standalone_range_deletion_one_in, 0,
             "If non-zero, file ingestion flow will test standalone range "
             "deletion file once every N file ingestion operations.");

DEFINE_bool(allow_unprepared_value,
            ROCKSDB_NAMESPACE::ReadOptions().allow_unprepared_value,
            "Allow lazy loading of values for range scans");

DEFINE_bool(track_and_verify_wals,
            ROCKSDB_NAMESPACE::Options().track_and_verify_wals,
            "See Options::track_and_verify_wals");

DEFINE_int32(
    remote_compaction_worker_threads, 2,
    "Remote Compaction Worker Thread count. If 0, remote compaction is "
    "disabled");

DEFINE_int32(remote_compaction_worker_interval, 10,
             "Remote Compaction Worker Thread dequeue tasks every N "
             "milliseconds. (Default: 10ms)");

DEFINE_bool(remote_compaction_failure_fall_back_to_local, true,
            "If true, remote compaction failures will be ignored and "
            "compactions will fall back to local and retried");

DEFINE_bool(use_separate_remote_compaction_pool, false,
            "Sets DBOptions.use_separate_remote_compaction_pool");

DEFINE_uint32(
    openandcompact_max_secondary_open_retries,
    ROCKSDB_NAMESPACE::OpenAndCompactOptions().max_secondary_open_retries,
    "Maximum number of retries when OpenAndCompact opens the source DB as a "
    "secondary");

DEFINE_int32(allow_resumption_one_in, 0,
             "If non-zero, enable resumable compaction with 1/N probability "
             "for each OpenAndCompact call.Requires "
             "remote_compaction_worker_threads > 0");

DEFINE_uint32(ingest_wbwi_one_in, 0,
              "If set, will call"
              "IngestWriteBatchWithIndex() instead of regular write operations "
              "once every N writes.");

static bool ValidateInt32Percent(const char* flagname, int32_t value) {
  if (value < 0 || value > 100) {
    fprintf(stderr, "Invalid value for --%s: %d, 0<= pct <=100 \n", flagname,
            value);
    return false;
  }
  return true;
}

DEFINE_int32(readpercent, 10,
             "Ratio of reads to total workload (expressed as a percentage)");
static const bool FLAGS_readpercent_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_readpercent, &ValidateInt32Percent);

DEFINE_int32(prefixpercent, 20,
             "Ratio of prefix iterators to total workload (expressed as a"
             " percentage)");
static const bool FLAGS_prefixpercent_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_prefixpercent, &ValidateInt32Percent);

DEFINE_int32(writepercent, 45,
             "Ratio of writes to total workload (expressed as a percentage)");
static const bool FLAGS_writepercent_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_writepercent, &ValidateInt32Percent);

DEFINE_int32(delpercent, 15,
             "Ratio of deletes to total workload (expressed as a percentage)");
static const bool FLAGS_delpercent_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_delpercent, &ValidateInt32Percent);

DEFINE_int32(delrangepercent, 0,
             "Ratio of range deletions to total workload (expressed as a "
             "percentage). Cannot be used with test_batches_snapshots");
static const bool FLAGS_delrangepercent_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_delrangepercent, &ValidateInt32Percent);

DEFINE_int32(nooverwritepercent, 60,
             "Ratio of keys without overwrite to total workload (expressed as "
             "a percentage). When --expected_values_dir is nonempty, must "
             "keep this value constant across invocations.");
static const bool FLAGS_nooverwritepercent_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_nooverwritepercent, &ValidateInt32Percent);

DEFINE_int32(iterpercent, 10,
             "Ratio of iterations to total workload"
             " (expressed as a percentage)");
static const bool FLAGS_iterpercent_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_iterpercent, &ValidateInt32Percent);

DEFINE_uint64(num_iterations, 10,
              "Number of iterations per iterator or MultiScan run");
static const bool FLAGS_num_iterations_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_num_iterations, &ValidateUint32Range);

DEFINE_int32(
    customopspercent, 0,
    "Ratio of custom operations to total workload (expressed as a percentage)");

DEFINE_string(compression_type, "snappy",
              "Algorithm to use to compress the database");

DEFINE_int32(compression_max_dict_bytes, 0,
             "Maximum size of dictionary used to prime the compression "
             "library.");

DEFINE_int32(compression_zstd_max_train_bytes, 0,
             "Maximum size of training data passed to zstd's dictionary "
             "trainer.");

DEFINE_int32(compression_parallel_threads, 1,
             "Number of threads for parallel compression.");

DEFINE_uint64(compression_max_dict_buffer_bytes, 0,
              "Buffering limit for SST file data to sample for dictionary "
              "compression.");

DEFINE_bool(compression_auto_skip, false,
            "Enable AutoSkip compression: stop attempting compression on data "
            "blocks once it is not paying off (reuses max_compressed_bytes_per_"
            "kb as the bar).");

DEFINE_int32(compression_auto_skip_min_sample_every, 0,
             "AutoSkip nominal sampling interval (skipped data blocks between "
             "forced compression samples). 0 selects an internal default.");

DEFINE_bool(
    compression_use_zstd_dict_trainer, true,
    "Use zstd's trainer to generate dictionary. If the options is false, "
    "zstd's finalizeDictionary() API is used to generate dictionary. "
    "ZSTD 1.4.5+ is required. If ZSTD 1.4.5+ is not linked with the binary, "
    "this flag will have the default value true.");

DEFINE_bool(compression_checksum, false,
            "Turn on zstd's checksum feature for detecting corruption.");

DEFINE_string(bottommost_compression_type, "disable",
              "Algorithm to use to compress bottommost level of the database. "
              "\"disable\" means disabling the feature");

DEFINE_string(checksum_type, "kCRC32c", "Algorithm to use to checksum blocks");

DEFINE_string(env_uri, "",
              "URI for env lookup. Mutually exclusive with --fs_uri");

DEFINE_string(fs_uri, "",
              "URI for registry Filesystem lookup. Mutually exclusive"
              " with --env_uri."
              " Creates a default environment with the specified filesystem.");

DEFINE_bool(
    tolerate_non_injected_io_errors_for_remote_dbs, false,
    "Treat non-injected, non-data-loss IO errors as retryable. Intended "
    "only for remote DBs (--env_uri / --fs_uri) where infrastructure "
    "can return transient IO errors; db_crashtest.py forces it off for "
    "local DBs so real local IO errors are not masked.");

DEFINE_uint64(ops_per_thread, 1200000, "Number of operations per thread.");
static const bool FLAGS_ops_per_thread_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_ops_per_thread, &ValidateUint32Range);

DEFINE_uint64(log2_keys_per_lock, 2, "Log2 of number of keys per lock");
static const bool FLAGS_log2_keys_per_lock_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_log2_keys_per_lock, &ValidateUint32Range);

DEFINE_uint64(max_manifest_file_size, 16384,
              "Maximum size of a MANIFEST file (without auto-tuning)");

DEFINE_int32(max_manifest_space_amp_pct, 500,
             "Max manifest space amp percentage for auto-tuning");

DEFINE_bool(verify_manifest_content_on_close, false,
            "If true, verify MANIFEST content (CRC + decode) on DB close");

DEFINE_bool(in_place_update, false, "On true, does inplace update in memtable");

DEFINE_string(memtablerep, "skip_list", "");

inline static bool ValidatePrefixSize(const char* flagname, int32_t value) {
  if (value < -1 || value > 8) {
    fprintf(stderr, "Invalid value for --%s: %d. -1 <= PrefixSize <= 8\n",
            flagname, value);
    return false;
  }
  return true;
}
DEFINE_int32(prefix_size, 7,
             "Control the prefix size for HashSkipListRep. "
             "-1 is disabled.");
static const bool FLAGS_prefix_size_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_prefix_size, &ValidatePrefixSize);

DEFINE_bool(use_merge, false,
            "On true, replaces all writes with a Merge "
            "that behaves like a Put");

DEFINE_uint32(use_put_entity_one_in, 0,
              "If greater than zero, PutEntity will be used once per every N "
              "write ops on average.");

DEFINE_bool(use_attribute_group, false,
            "If set, use the attribute_group API to put/get entities");

DEFINE_bool(use_multi_cf_iterator, false,
            "If set, use the multi_cf_iterator for TestIterate");

DEFINE_bool(use_full_merge_v1, false,
            "On true, use a merge operator that implement the deprecated "
            "version of FullMerge");

DEFINE_int32(sync_wal_one_in, 0,
             "If non-zero, then SyncWAL() will be called once for every N ops "
             "on average. 0 indicates that calls to SyncWAL() are disabled.");

DEFINE_bool(avoid_unnecessary_blocking_io,
            ROCKSDB_NAMESPACE::Options().avoid_unnecessary_blocking_io,
            "If true, some expensive cleaning up operations will be moved from "
            "user reads to high-pri background threads.");

DEFINE_bool(write_dbid_to_manifest,
            ROCKSDB_NAMESPACE::Options().write_dbid_to_manifest,
            "Write DB_ID to manifest");

DEFINE_bool(track_options_file_number_in_manifest,
            ROCKSDB_NAMESPACE::Options().track_options_file_number_in_manifest,
            "Publish the committed OPTIONS file number in MANIFEST");

DEFINE_bool(optimize_manifest_for_recovery,
            ROCKSDB_NAMESPACE::Options().optimize_manifest_for_recovery,
            "Reduce recovery work after a clean shutdown");

DEFINE_bool(write_identity_file,
            ROCKSDB_NAMESPACE::Options().write_identity_file,
            "Write DB_ID to IDENTITY file");

DEFINE_bool(reuse_manifest_on_open,
            ROCKSDB_NAMESPACE::Options().reuse_manifest_on_open,
            "Reopen existing MANIFEST for append after recovery instead of "
            "creating a fresh one");

DEFINE_bool(avoid_flush_during_recovery,
            ROCKSDB_NAMESPACE::Options().avoid_flush_during_recovery,
            "Avoid flush during recovery");

DEFINE_bool(
    enforce_write_buffer_manager_during_recovery,
    ROCKSDB_NAMESPACE::Options().enforce_write_buffer_manager_during_recovery,
    "Enforce write buffer manager memory limit during WAL recovery");

DEFINE_uint64(max_write_batch_group_size_bytes,
              ROCKSDB_NAMESPACE::Options().max_write_batch_group_size_bytes,
              "Max write batch group size");

DEFINE_bool(level_compaction_dynamic_level_bytes,
            ROCKSDB_NAMESPACE::Options().level_compaction_dynamic_level_bytes,
            "Use dynamic level");

DEFINE_int32(verify_checksum_one_in, 0,
             "If non-zero, then DB::VerifyChecksum() will be called to do"
             " checksum verification of all the files in the database once for"
             " every N ops on average. 0 indicates that calls to"
             " VerifyChecksum() are disabled.");

DEFINE_int32(verify_file_checksums_one_in, 0,
             "If non-zero, then DB::VerifyFileChecksums() will be called to do"
             " checksum verification of all the files in the database once for"
             " every N ops on average. 0 indicates that calls to"
             " VerifyFileChecksums() are disabled.");

DEFINE_int32(verify_db_one_in, 0,
             "If non-zero, call VerifyDb() once for every N ops. 0 indicates "
             "that VerifyDb() will not be called in OperateDb(). Note that "
             "enabling this can slow down tests.");

DEFINE_int32(continuous_verification_interval, 1000,
             "While test is running, verify db every N milliseconds. 0 "
             "disables continuous verification.");

DEFINE_int32(approximate_size_one_in, 64,
             "If non-zero, DB::GetApproximateSizes() and "
             "DB::GetApproximateMemTableStats() will be called against "
             "random key ranges.");

DEFINE_int32(read_fault_one_in, 1000,
             "On non-zero, enables fault injection on read");

DEFINE_bool(error_recovery_with_no_fault_injection, false,
            "If true, error recovery will be done without fault injection if "
            "fault injection is enabled");

DEFINE_int32(metadata_read_fault_one_in, 1000,
             "On non-zero, enables fault injection on metadata read (i.e, "
             "directory and file metadata read)");

DEFINE_int32(get_property_one_in, 1000,
             "If non-zero, then DB::GetProperty() will be called to get various"
             " properties for every N ops on average. 0 indicates that"
             " GetProperty() will be not be called.");

DEFINE_int32(get_properties_of_all_tables_one_in, 1000,
             "If non-zero, then DB::GetPropertiesOfAllTables() will be called "
             "for every N ops on average. 0 indicates that"
             " it will be not be called.");

DEFINE_bool(sync_fault_injection, false,
            "If true, FaultInjectionTestFS will be used for write operations, "
            "and unsynced data in DB will lost after crash. In such a case we "
            "track DB changes in a trace file (\"*.trace\") in "
            "--expected_values_dir for verifying there are no holes in the "
            "recovered data.");

DEFINE_bool(best_efforts_recovery, false,
            "If true, use best efforts recovery.");
DEFINE_bool(skip_verifydb, false,
            "If true, skip VerifyDb() calls and Get()/Iterator verifications"
            "against expected state.");

DEFINE_bool(enable_compaction_filter, false,
            "If true, configures a compaction filter that returns a kRemove "
            "decision for deleted keys.");

DEFINE_bool(paranoid_file_checks, true,
            "After writing every SST file, reopen it and read all the keys "
            "and validate checksums");

DEFINE_uint64(batch_protection_bytes_per_key, 0,
              "If nonzero, enables integrity protection in `WriteBatch` at the "
              "specified number of bytes per key. Currently the only supported "
              "nonzero value is eight.");

DEFINE_uint32(
    memtable_protection_bytes_per_key, 0,
    "If nonzero, enables integrity protection in memtable entries at the "
    "specified number of bytes per key. Currently the supported "
    "nonzero values are 1, 2, 4 and 8.");

DEFINE_uint32(block_protection_bytes_per_key, 0,
              "If nonzero, enables integrity protection in blocks at the "
              "specified number of bytes per key. Currently the supported "
              "nonzero values are 1, 2, 4 and 8.");

DEFINE_string(file_checksum_impl, "none",
              "Name of an implementation for file_checksum_gen_factory, or "
              "\"none\" for null.");

DEFINE_int32(write_fault_one_in, 0,
             "On non-zero, enables fault injection on write. Currently only"
             "injects write error when writing to SST files.");

DEFINE_bool(exclude_wal_from_write_fault_injection, false,
            "If true, we won't inject write fault when writing to WAL file");

DEFINE_bool(file_scope_wal_write_faults, false,
            "If true, injected write faults on WAL files have file scope so "
            "the file-scoped WAL recovery protocol is exercised. Other fault "
            "injection and unsupported recovery options must be disabled");

DEFINE_int32(metadata_write_fault_one_in, 1000,
             "On non-zero, enables fault injection on metadata write (i.e, "
             "directory and file metadata write)");

DEFINE_uint64(user_timestamp_size, 0,
              "Number of bytes for a user-defined timestamp. Currently, only "
              "8-byte is supported");

DEFINE_bool(persist_user_defined_timestamps, true,
            "Flag to indicate whether user-defined timestamps will be persisted"
            " during Flush");

DEFINE_int32(open_metadata_read_fault_one_in, 0,
             "On non-zero, enables fault injection on file metadata read (i.e, "
             "directory and file metadata read)"
             "during DB reopen.");

DEFINE_int32(open_metadata_write_fault_one_in, 0,
             "On non-zero, enables fault injection on file metadata write "
             "during DB reopen.");

DEFINE_string(secondary_cache_uri, "",
              "Full URI for creating a customized secondary cache object");
DEFINE_int32(secondary_cache_fault_one_in, 0,
             "On non-zero, enables fault injection in secondary cache inserts"
             " and lookups");
DEFINE_double(tiered_cache_percent_compressed, 0.0,
              "Percentage of total block cache budget to allocate to the "
              "compressed cache");
DEFINE_int32(open_write_fault_one_in, 0,
             "On non-zero, enables fault injection on file writes "
             "during DB reopen.");
DEFINE_int32(open_read_fault_one_in, 0,
             "On non-zero, enables fault injection on file reads "
             "during DB reopen.");
DEFINE_int32(inject_error_severity, 1,
             "The severity of the injected IO Error. 1 is soft error (e.g. "
             "retryable error), 2 is fatal error, and the default is "
             "retryable error.");
DEFINE_int32(prepopulate_block_cache,
             static_cast<int32_t>(ROCKSDB_NAMESPACE::BlockBasedTableOptions::
                                      PrepopulateBlockCache::kDisable),
             "Options related to cache warming (see `enum "
             "PrepopulateBlockCache` in table.h)");

DEFINE_bool(two_write_queues, false,
            "Set to true to enable two write queues. Default: false");

DEFINE_bool(use_only_the_last_commit_time_batch_for_recovery, false,
            "If true, the commit-time write batch will not be immediately "
            "inserted into the memtables. Default: false");

DEFINE_uint64(
    wp_snapshot_cache_bits, 7ull,
    "Number of bits to represent write-prepared transaction db's snapshot "
    "cache. Default: 7 (128 entries)");

DEFINE_uint64(wp_commit_cache_bits, 23ull,
              "Number of bits to represent write-prepared transaction db's "
              "commit cache. Default: 23 (8M entries)");

DEFINE_bool(adaptive_readahead, false,
            "Carry forward internal auto readahead size from one file to next "
            "file at each level during iteration");
DEFINE_bool(
    async_io, false,
    "Does asynchronous prefetching when internal auto readahead is enabled");

DEFINE_string(wal_compression, "none",
              "Algorithm to use for WAL compression. none to disable.");

DEFINE_bool(
    verify_sst_unique_id_in_manifest, false,
    "Enable DB options `verify_sst_unique_id_in_manifest`, if true, during "
    "DB-open try verifying the SST unique id between MANIFEST and SST "
    "properties.");

DEFINE_bool(fast_sst_open, false,
            "If true, retrieve and persist file open metadata in the MANIFEST "
            "for faster SST file re-opening on DB open.");

DEFINE_int32(
    create_timestamped_snapshot_one_in, 0,
    "On non-zero, create timestamped snapshots upon transaction commits.");

DEFINE_bool(allow_data_in_errors,
            ROCKSDB_NAMESPACE::Options().allow_data_in_errors,
            "If true, allow logging data, e.g. key, value in LOG files.");

DEFINE_bool(enable_thread_tracking,
            ROCKSDB_NAMESPACE::Options().enable_thread_tracking,
            "If true, the status of the threads involved in this DB will be "
            "tracked and available via GetThreadList() API.");

DEFINE_int32(verify_iterator_with_expected_state_one_in, 0,
             "If non-zero, when TestIterate() is to be called, there is a "
             "1/verify_iterator_with_expected_state_one_in "
             "chance that the iterator is verified against the expected state "
             "file, instead of comparing keys between two iterators.");

DEFINE_uint64(readahead_size, 0, "Iterator readahead size");
DEFINE_uint64(initial_auto_readahead_size, 0,
              "Initial auto readahead size for prefetching during Iteration");
DEFINE_uint64(max_auto_readahead_size, 0,
              "Max auto readahead size for prefetching during Iteration");
DEFINE_uint64(
    num_file_reads_for_auto_readahead, 0,
    "Num of sequential reads to enable auto prefetching during Iteration");

DEFINE_bool(
    preserve_unverified_changes, false,
    "DB files of the current run will all be preserved in `FLAGS_db`. DB files "
    "from the last run will be preserved in `FLAGS_db/unverified` until the "
    "first verification succeeds. Expected state files from the last run will "
    "be preserved similarly under `FLAGS_expected_values_dir/unverified` when "
    "`--expected_values_dir` is nonempty.");

DEFINE_uint64(mutex_hold_log_threshold_us,
              ROCKSDB_NAMESPACE::Options().mutex_hold_log_threshold_us,
              "Completed global mutex hold threshold in microseconds; 0 "
              "disables hold logs.");

DEFINE_uint64(stats_dump_period_sec,
              ROCKSDB_NAMESPACE::Options().stats_dump_period_sec,
              "Gap between printing stats to log in seconds");

DEFINE_uint64(
    max_compaction_trigger_wakeup_seconds,
    ROCKSDB_NAMESPACE::Options().max_compaction_trigger_wakeup_seconds,
    "Sets DB option max_compaction_trigger_wakeup_seconds.");

DEFINE_int32(
    periodic_compaction_phase_recovery_percent,
    ROCKSDB_NAMESPACE::Options().periodic_compaction_phase_recovery_percent,
    "Sets DB option periodic_compaction_phase_recovery_percent.");

DEFINE_bool(verification_only, false,
            "If true, tests will only execute verification step");
extern "C" bool RocksDbIOUringEnable() { return true; }

DEFINE_uint32(memtable_max_range_deletions, 0,
              "If nonzero, RocksDB will try to flush the current memtable"
              "after the number of range deletions is >= this limit");

DEFINE_uint32(bottommost_file_compaction_delay, 0,
              "Delay kBottommostFiles compaction by this amount of seconds."
              "See more in option comment.");

DEFINE_bool(auto_readahead_size, false,
            "Does auto tuning of readahead_size when enabled during scans.");

DEFINE_bool(allow_fallocate, ROCKSDB_NAMESPACE::Options().allow_fallocate,
            "Options.allow_fallocate");

DEFINE_int32(table_cache_numshardbits,
             ROCKSDB_NAMESPACE::Options().table_cache_numshardbits,
             "Options.table_cache_numshardbits");

DEFINE_uint64(log_readahead_size,
              ROCKSDB_NAMESPACE::Options().log_readahead_size,
              "Options.log_readahead_size");

DEFINE_uint64(bgerror_resume_retry_interval,
              ROCKSDB_NAMESPACE::Options().bgerror_resume_retry_interval,
              "Options.bgerror_resume_retry_interval");

DEFINE_uint64(delete_obsolete_files_period_micros,
              ROCKSDB_NAMESPACE::Options().delete_obsolete_files_period_micros,
              "Options.delete_obsolete_files_period_micros");

DEFINE_uint64(max_log_file_size, ROCKSDB_NAMESPACE::Options().max_log_file_size,
              "Options.max_log_file_sizes");

DEFINE_uint64(log_file_time_to_roll,
              ROCKSDB_NAMESPACE::Options().log_file_time_to_roll,
              "Options.log_file_time_to_roll");

DEFINE_bool(use_adaptive_mutex, ROCKSDB_NAMESPACE::Options().use_adaptive_mutex,
            "Options.use_adaptive_mutex");

DEFINE_bool(advise_random_on_open,
            ROCKSDB_NAMESPACE::Options().advise_random_on_open,
            "Options.advise_random_on_open");

DEFINE_uint64(WAL_ttl_seconds, ROCKSDB_NAMESPACE::Options().WAL_ttl_seconds,
              "Options.WAL_ttl_seconds");

DEFINE_uint64(WAL_size_limit_MB, ROCKSDB_NAMESPACE::Options().WAL_size_limit_MB,
              "Options.WAL_size_limit_MB");

DEFINE_bool(strict_bytes_per_sync,
            ROCKSDB_NAMESPACE::Options().strict_bytes_per_sync,
            "Options.strict_bytes_per_sync");

DEFINE_bool(avoid_flush_during_shutdown,
            ROCKSDB_NAMESPACE::Options().avoid_flush_during_shutdown,
            "Options.avoid_flush_during_shutdown");

DEFINE_bool(fill_cache, ROCKSDB_NAMESPACE::ReadOptions().fill_cache,
            "ReadOptions.fill_cache");

DEFINE_bool(optimize_multiget_for_io,
            ROCKSDB_NAMESPACE::ReadOptions().optimize_multiget_for_io,
            "ReadOptions.optimize_multiget_for_io");

DEFINE_bool(memtable_insert_hint_per_batch,
            ROCKSDB_NAMESPACE::WriteOptions().memtable_insert_hint_per_batch,
            "WriteOptions.memtable_insert_hint_per_batch");

DEFINE_bool(dump_malloc_stats, ROCKSDB_NAMESPACE::Options().dump_malloc_stats,
            "Options.dump_malloc_stats");

DEFINE_uint64(stats_history_buffer_size,
              ROCKSDB_NAMESPACE::Options().stats_history_buffer_size,
              "Options.stats_history_buffer_size");

DEFINE_bool(skip_stats_update_on_db_open,
            ROCKSDB_NAMESPACE::Options().skip_stats_update_on_db_open,
            "Options.skip_stats_update_on_db_open");

DEFINE_bool(optimize_filters_for_hits,
            ROCKSDB_NAMESPACE::Options().optimize_filters_for_hits,
            "Options.optimize_filters_for_hits");

DEFINE_uint64(sample_for_compression,
              ROCKSDB_NAMESPACE::Options().sample_for_compression,
              "Options.sample_for_compression");

DEFINE_bool(report_bg_io_stats, ROCKSDB_NAMESPACE::Options().report_bg_io_stats,
            "Options.report_bg_io_stats");

DEFINE_bool(
    cache_index_and_filter_blocks_with_high_priority,
    ROCKSDB_NAMESPACE::BlockBasedTableOptions()
        .cache_index_and_filter_blocks_with_high_priority,
    "BlockBasedTableOptions.cache_index_and_filter_blocks_with_high_priority");

DEFINE_bool(use_delta_encoding,
            ROCKSDB_NAMESPACE::BlockBasedTableOptions().use_delta_encoding,
            "BlockBasedTableOptions.use_delta_encoding");

DEFINE_bool(verify_compression,
            ROCKSDB_NAMESPACE::BlockBasedTableOptions().verify_compression,
            "BlockBasedTableOptions.verify_compression");

DEFINE_uint32(
    read_amp_bytes_per_bit,
    ROCKSDB_NAMESPACE::BlockBasedTableOptions().read_amp_bytes_per_bit,
    "Options.read_amp_bytes_per_bit");

DEFINE_bool(
    enable_index_compression,
    ROCKSDB_NAMESPACE::BlockBasedTableOptions().enable_index_compression,
    "BlockBasedTableOptions.enable_index_compression");

DEFINE_uint32(index_shortening,
              static_cast<uint32_t>(
                  ROCKSDB_NAMESPACE::BlockBasedTableOptions().index_shortening),
              "BlockBasedTableOptions.index_shortening");

DEFINE_uint32(metadata_charge_policy,
              static_cast<uint32_t>(ROCKSDB_NAMESPACE::ShardedCacheOptions()
                                        .metadata_charge_policy),
              "ShardedCacheOptions.metadata_charge_policy");

DEFINE_bool(use_adaptive_mutex_lru,
            ROCKSDB_NAMESPACE::LRUCacheOptions().use_adaptive_mutex,
            "LRUCacheOptions.use_adaptive_mutex");

DEFINE_uint64(manifest_preallocation_size,
              ROCKSDB_NAMESPACE::Options().manifest_preallocation_size,
              "Options.manifest_preallocation_size");

DEFINE_uint64(max_total_wal_size,
              ROCKSDB_NAMESPACE::Options().max_total_wal_size,
              "Options.max_total_wal_size");

DEFINE_bool(enable_checksum_handoff, false,
            "If true, include all the supported files in "
            "Options.checksum_handoff_file. Otherwise include no files.");

DEFINE_double(high_pri_pool_ratio,
              ROCKSDB_NAMESPACE::LRUCacheOptions().high_pri_pool_ratio,
              "LRUCacheOptions.high_pri_pool_ratio");

DEFINE_double(low_pri_pool_ratio,
              ROCKSDB_NAMESPACE::LRUCacheOptions().low_pri_pool_ratio,
              "LRUCacheOptions.low_pri_pool_ratio");

DEFINE_uint64(soft_pending_compaction_bytes_limit,
              ROCKSDB_NAMESPACE::Options().soft_pending_compaction_bytes_limit,
              "Options.soft_pending_compaction_bytes_limit");

DEFINE_uint64(hard_pending_compaction_bytes_limit,
              ROCKSDB_NAMESPACE::Options().hard_pending_compaction_bytes_limit,
              "Options.hard_pending_compaction_bytes_limit");

DEFINE_bool(enable_sst_partitioner_factory, false,
            "If true, set Options.sst_partitioner_factory to "
            "SstPartitionerFixedPrefixFactory with prefix length equal to 1");

DEFINE_bool(
    enable_do_not_compress_roles, false,
    "If true, set CompressedSecondaryCacheOptions.do_not_compress_roles to "
    "include all cache roles");

DEFINE_bool(block_align,
            ROCKSDB_NAMESPACE::BlockBasedTableOptions().block_align,
            "BlockBasedTableOptions.block_align");

DEFINE_uint64(
    super_block_alignment_size,
    ROCKSDB_NAMESPACE::BlockBasedTableOptions().super_block_alignment_size,
    "BlockBasedTableOptions.super_block_alignment_size");

DEFINE_uint64(
    super_block_alignment_space_overhead_ratio,
    ROCKSDB_NAMESPACE::BlockBasedTableOptions()
        .super_block_alignment_space_overhead_ratio,
    "BlockBasedTableOptions.super_block_alignment_space_overhead_ratio");

DEFINE_uint32(
    lowest_used_cache_tier,
    static_cast<uint32_t>(ROCKSDB_NAMESPACE::Options().lowest_used_cache_tier),
    "Options.lowest_used_cache_tier");

DEFINE_bool(enable_custom_split_merge,
            ROCKSDB_NAMESPACE::CompressedSecondaryCacheOptions()
                .enable_custom_split_merge,
            "CompressedSecondaryCacheOptions.enable_custom_split_merge");

DEFINE_uint32(
    adm_policy,
    static_cast<uint32_t>(ROCKSDB_NAMESPACE::TieredCacheOptions().adm_policy),
    "TieredCacheOptions.adm_policy");

DEFINE_string(last_level_temperature, "kUnknown",
              "Options.last_level_temperature");

DEFINE_string(default_write_temperature, "kUnknown",
              "Options.default_write_temperature");

DEFINE_string(default_temperature, "kUnknown", "Options.default_temperature");

DEFINE_bool(enable_memtable_insert_with_hint_prefix_extractor,
            ROCKSDB_NAMESPACE::Options()
                    .memtable_insert_with_hint_prefix_extractor != nullptr,
            "If true and FLAGS_prefix_size > 0, set "
            "Options.memtable_insert_with_hint_prefix_extractor to "
            "be Options.prefix_extractor");

DEFINE_bool(check_multiget_consistency, true,
            "If true, check consistency of MultiGet result by comparing it "
            "with Get's under a snapshot");

DEFINE_bool(check_multiget_entity_consistency, true,
            "If true, check consistency of MultiGetEntity result by comparing "
            "it GetEntity's under a snapshot");

DEFINE_bool(inplace_update_support,
            ROCKSDB_NAMESPACE::Options().inplace_update_support,
            "Options.inplace_update_support");

DEFINE_uint32(uncache_aggressiveness,
              ROCKSDB_NAMESPACE::ColumnFamilyOptions().uncache_aggressiveness,
              "Aggressiveness of erasing cache entries that are likely "
              "obsolete. 0 = disabled, 1 = minimum, 100 = moderate, 10000 = "
              "normal max");

DEFINE_uint32(verify_output_flags, 0,
              "Sets CF option verify_output_flags as a uint32_t bitmask. "
              "See VerifyOutputFlags enum for bit definitions.");

DEFINE_bool(paranoid_memory_checks,
            ROCKSDB_NAMESPACE::Options().paranoid_memory_checks,
            "Sets CF option paranoid_memory_checks.");

DEFINE_bool(
    memtable_verify_per_key_checksum_on_seek,
    ROCKSDB_NAMESPACE::Options().memtable_verify_per_key_checksum_on_seek,
    "Sets CF option memtable_verify_per_key_checksum_on_seek.");

DEFINE_bool(memtable_batch_lookup_optimization,
            ROCKSDB_NAMESPACE::Options().memtable_batch_lookup_optimization,
            "Sets CF option memtable_batch_lookup_optimization.");

DEFINE_uint32(commit_bypass_memtable_one_in, 0,
              "If greater than zero, transaction option will set "
              "commit_bypass_memtable to per every N transactions on average.");

// Compaction on deletion trigger flags
DEFINE_bool(enable_compaction_on_deletion_trigger, false,
            "Enable CompactOnDeletionCollectorFactory for stress testing "
            "deletion-triggered compaction scenarios.");

DEFINE_uint64(compaction_on_deletion_min_file_size, 32 * 1024,
              "Minimum file size (in bytes) for deletion-triggered compaction. "
              "Files smaller than this will not trigger compaction even if "
              "deletion ratio is exceeded. Default: 32KB");

DEFINE_int32(compaction_on_deletion_trigger_count, 50,
             "Number of deletions that triggers compaction when deletion "
             "ratio is exceeded. Default: 50");

DEFINE_int32(compaction_on_deletion_window_size, 100,
             "Size of the sliding window for tracking deletions. "
             "Default: 100");

DEFINE_double(compaction_on_deletion_ratio, 0.5,
              "Deletion ratio threshold for triggering compaction. "
              "Default: 0.5 (50%)");

DEFINE_double(read_triggered_compaction_threshold,
              ROCKSDB_NAMESPACE::Options().read_triggered_compaction_threshold,
              "Sets CF option read_triggered_compaction_threshold.");

DEFINE_string(listener_uri, "",
              "URI for an additional EventListener to attach (e.g. "
              "auto_tuner://). Empty means none.");

DEFINE_bool(
    auto_refresh_iterator_with_snapshot,
    ROCKSDB_NAMESPACE::ReadOptions().auto_refresh_iterator_with_snapshot,
    "ReadOptions.auto_refresh_iterator_with_snapshot");

DEFINE_uint32(
    memtable_op_scan_flush_trigger,
    ROCKSDB_NAMESPACE::ColumnFamilyOptions().memtable_op_scan_flush_trigger,
    "Sets CF option memtable_op_scan_flush_trigger.");

DEFINE_uint32(
    memtable_avg_op_scan_flush_trigger,
    ROCKSDB_NAMESPACE::ColumnFamilyOptions().memtable_avg_op_scan_flush_trigger,
    "Sets CF option memtable_avg_op_scan_flush_trigger.");

DEFINE_uint32(min_tombstones_for_range_conversion,
              ROCKSDB_NAMESPACE::ColumnFamilyOptions()
                  .min_tombstones_for_range_conversion,
              "Sets CF option min_tombstones_for_range_conversion.");

DEFINE_bool(
    universal_reduce_file_locking,
    ROCKSDB_NAMESPACE::ColumnFamilyOptions()
        .compaction_options_universal.reduce_file_locking,
    "Sets "
    "ColumnFamilyOptions().compaciton_options_universal.reduce_file_locking.");

DEFINE_bool(use_multiscan, false,
            "If set, use the batched MultiScan API for scans.");

DEFINE_bool(multiscan_reverse, false,
            "If set with use_multiscan, scan each MultiScan range in reverse "
            "using SeekForPrev and Prev. This does not require "
            "test_backward_scan.");

DEFINE_bool(multiscan_use_async_io, false,
            "If set, enable async_io for MultiScan operations.");

DEFINE_bool(read_scoped_block_buffer_provider, false,
            "If set, configure ReadOptions::read_scoped_block_buffer_provider "
            "with a stress-test provider for supported scan reads.");

DEFINE_uint64(multiscan_max_prefetch_memory_bytes, 0,
              "If non-zero, sets the max_prefetch_memory_bytes on the "
              "IODispatcher used for MultiScan. This limits the total memory "
              "used for prefetching data blocks across all concurrent "
              "MultiScan ReadSets.");

DEFINE_int32(num_dbs, 1,
             "Number of DB instances to run in parallel. "
             "For num_dbs=1: --db is the DB path directly (default). "
             "For num_dbs>1: --db is a parent directory; db_stress "
             "auto-creates db_0/, db_1/, ... subdirs underneath "
             "(same for --expected_values_dir and secondaries). "
             "Per-DB flags: threads, max_key, ops_per_thread, reopen, "
             "column_families, and all DB options. "
             "Shared across all instances: background env threads "
             "(compaction, flush pool), cache, write_buffer_manager, "
             "compressed_secondary_cache, rate_limiter, and "
             "compaction_thread_pool_adjust_interval.");

#endif  // GFLAGS
