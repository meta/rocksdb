//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/internal_stats.h"

#include <algorithm>
#include <cinttypes>
#include <cstddef>
#include <limits>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cache/cache_entry_roles.h"
#include "cache/cache_entry_stats.h"
#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/write_stall_stats.h"
#include "port/port.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/table.h"
#include "table/block_based/cachable_entry.h"
#include "util/hash_containers.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

const std::map<LevelStatType, LevelStat> InternalStats::compaction_level_stats =
    {
        {LevelStatType::NUM_FILES, LevelStat{"NumFiles", "Files"}},
        {LevelStatType::COMPACTED_FILES,
         LevelStat{"CompactedFiles", "CompactedFiles"}},
        {LevelStatType::SIZE_BYTES, LevelStat{"SizeBytes", "Size"}},
        {LevelStatType::SCORE, LevelStat{"Score", "Score"}},
        {LevelStatType::READ_GB, LevelStat{"ReadGB", "Read(GB)"}},
        {LevelStatType::RN_GB, LevelStat{"RnGB", "Rn(GB)"}},
        {LevelStatType::RNP1_GB, LevelStat{"Rnp1GB", "Rnp1(GB)"}},
        {LevelStatType::WRITE_GB, LevelStat{"WriteGB", "Write(GB)"}},
        {LevelStatType::WRITE_PRE_COMP_GB,
         LevelStat{"WPreCompGB", "WPreComp(GB)"}},
        {LevelStatType::W_NEW_GB, LevelStat{"WnewGB", "Wnew(GB)"}},
        {LevelStatType::MOVED_GB, LevelStat{"MovedGB", "Moved(GB)"}},
        {LevelStatType::WRITE_AMP, LevelStat{"WriteAmp", "W-Amp"}},
        {LevelStatType::READ_MBPS, LevelStat{"ReadMBps", "Rd(MB/s)"}},
        {LevelStatType::WRITE_MBPS, LevelStat{"WriteMBps", "Wr(MB/s)"}},
        {LevelStatType::COMP_SEC, LevelStat{"CompSec", "Comp(sec)"}},
        {LevelStatType::COMP_CPU_SEC,
         LevelStat{"CompMergeCPU", "CompMergeCPU(sec)"}},
        {LevelStatType::COMP_COUNT, LevelStat{"CompCount", "Comp(cnt)"}},
        {LevelStatType::AVG_SEC, LevelStat{"AvgSec", "Avg(sec)"}},
        {LevelStatType::KEY_IN, LevelStat{"KeyIn", "KeyIn"}},
        {LevelStatType::KEY_DROP, LevelStat{"KeyDrop", "KeyDrop"}},
        {LevelStatType::R_BLOB_GB, LevelStat{"RblobGB", "Rblob(GB)"}},
        {LevelStatType::W_BLOB_GB, LevelStat{"WblobGB", "Wblob(GB)"}},
};

const std::map<InternalStats::InternalDBStatsType, DBStatInfo>
    InternalStats::db_stats_type_to_info = {
        {InternalStats::kIntStatsWalFileBytes,
         DBStatInfo{"db.wal_bytes_written"}},
        {InternalStats::kIntStatsWalFileSynced, DBStatInfo{"db.wal_syncs"}},
        {InternalStats::kIntStatsBytesWritten,
         DBStatInfo{"db.user_bytes_written"}},
        {InternalStats::kIntStatsNumKeysWritten,
         DBStatInfo{"db.user_keys_written"}},
        {InternalStats::kIntStatsWriteDoneByOther,
         DBStatInfo{"db.user_writes_by_other"}},
        {InternalStats::kIntStatsWriteDoneBySelf,
         DBStatInfo{"db.user_writes_by_self"}},
        {InternalStats::kIntStatsWriteWithWal,
         DBStatInfo{"db.user_writes_with_wal"}},
        {InternalStats::kIntStatsWriteStallMicros,
         DBStatInfo{"db.user_write_stall_micros"}},
        {InternalStats::kIntStatsWriteBufferManagerLimitStopsCounts,
         DBStatInfo{WriteStallStatsMapKeys::CauseConditionCount(
             WriteStallCause::kWriteBufferManagerLimit,
             WriteStallCondition::kStopped)}},
};

namespace {
const double kMB = 1048576.0;
const double kGB = kMB * 1024;
const double kMicrosInSec = 1000000.0;

void PrintLevelStatsHeader(char* buf, size_t len, const std::string& cf_name,
                           const std::string& group_by) {
  int written_size =
      snprintf(buf, len, "\n** Compaction Stats [%s] **\n", cf_name.c_str());
  written_size = std::min(written_size, static_cast<int>(len));
  auto hdr = [](LevelStatType t) {
    return InternalStats::compaction_level_stats.at(t).header_name.c_str();
  };
  int line_size = snprintf(
      buf + written_size, len - written_size,
      "%s    %s   %s     %s %s  %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s "
      "%s "
      "%s\n",
      // Note that we skip COMPACTED_FILES and merge it with Files column
      group_by.c_str(), hdr(LevelStatType::NUM_FILES),
      hdr(LevelStatType::SIZE_BYTES), hdr(LevelStatType::SCORE),
      hdr(LevelStatType::READ_GB), hdr(LevelStatType::RN_GB),
      hdr(LevelStatType::RNP1_GB), hdr(LevelStatType::WRITE_GB),
      hdr(LevelStatType::WRITE_PRE_COMP_GB), hdr(LevelStatType::W_NEW_GB),
      hdr(LevelStatType::MOVED_GB), hdr(LevelStatType::WRITE_AMP),
      hdr(LevelStatType::READ_MBPS), hdr(LevelStatType::WRITE_MBPS),
      hdr(LevelStatType::COMP_SEC), hdr(LevelStatType::COMP_CPU_SEC),
      hdr(LevelStatType::COMP_COUNT), hdr(LevelStatType::AVG_SEC),
      hdr(LevelStatType::KEY_IN), hdr(LevelStatType::KEY_DROP),
      hdr(LevelStatType::R_BLOB_GB), hdr(LevelStatType::W_BLOB_GB));

  written_size += line_size;
  written_size = std::min(written_size, static_cast<int>(len));
  snprintf(buf + written_size, len - written_size, "%s\n",
           std::string(line_size, '-').c_str());
}

void PrepareLevelStats(std::map<LevelStatType, double>* level_stats,
                       int num_files, int being_compacted,
                       double total_file_size, double score, double w_amp,
                       const InternalStats::CompactionStats& stats) {
  const uint64_t bytes_read = stats.bytes_read_non_output_levels +
                              stats.bytes_read_output_level +
                              stats.bytes_read_blob;
  const uint64_t bytes_written = stats.bytes_written + stats.bytes_written_blob;
  const int64_t bytes_new = stats.bytes_written - stats.bytes_read_output_level;
  const double elapsed = (stats.micros + 1) / kMicrosInSec;

  (*level_stats)[LevelStatType::NUM_FILES] = num_files;
  (*level_stats)[LevelStatType::COMPACTED_FILES] = being_compacted;
  (*level_stats)[LevelStatType::SIZE_BYTES] = total_file_size;
  (*level_stats)[LevelStatType::SCORE] = score;
  (*level_stats)[LevelStatType::READ_GB] = bytes_read / kGB;
  (*level_stats)[LevelStatType::RN_GB] =
      stats.bytes_read_non_output_levels / kGB;
  (*level_stats)[LevelStatType::RNP1_GB] = stats.bytes_read_output_level / kGB;
  (*level_stats)[LevelStatType::WRITE_GB] = stats.bytes_written / kGB;
  (*level_stats)[LevelStatType::WRITE_PRE_COMP_GB] =
      stats.bytes_written_pre_comp / kGB;
  (*level_stats)[LevelStatType::W_NEW_GB] = bytes_new / kGB;
  (*level_stats)[LevelStatType::MOVED_GB] = stats.bytes_moved / kGB;
  (*level_stats)[LevelStatType::WRITE_AMP] = w_amp;
  (*level_stats)[LevelStatType::READ_MBPS] = bytes_read / kMB / elapsed;
  (*level_stats)[LevelStatType::WRITE_MBPS] = bytes_written / kMB / elapsed;
  (*level_stats)[LevelStatType::COMP_SEC] = stats.micros / kMicrosInSec;
  (*level_stats)[LevelStatType::COMP_CPU_SEC] = stats.cpu_micros / kMicrosInSec;
  (*level_stats)[LevelStatType::COMP_COUNT] = stats.count;
  (*level_stats)[LevelStatType::AVG_SEC] =
      stats.count == 0 ? 0 : stats.micros / kMicrosInSec / stats.count;
  (*level_stats)[LevelStatType::KEY_IN] =
      static_cast<double>(stats.num_input_records);
  (*level_stats)[LevelStatType::KEY_DROP] =
      static_cast<double>(stats.num_dropped_records);
  (*level_stats)[LevelStatType::R_BLOB_GB] = stats.bytes_read_blob / kGB;
  (*level_stats)[LevelStatType::W_BLOB_GB] = stats.bytes_written_blob / kGB;
}

void PrintLevelStats(char* buf, size_t len, const std::string& name,
                     const std::map<LevelStatType, double>& stat_value) {
  snprintf(
      buf, len,
      "%4s "      /*  Level */
      "%6d/%-3d " /*  Files */
      "%10s "     /*  Size */
      "%5.1f "    /*  Score */
      "%8.1f "    /*  Read(GB) */
      "%7.1f "    /*  Rn(GB) */
      "%8.1f "    /*  Rnp1(GB) */
      "%9.1f "    /*  Write(GB) */
      "%9.1f "    /*  WPreComp(GB) */
      "%8.1f "    /*  Wnew(GB) */
      "%9.1f "    /*  Moved(GB) */
      "%5.1f "    /*  W-Amp */
      "%8.1f "    /*  Rd(MB/s) */
      "%8.1f "    /*  Wr(MB/s) */
      "%9.2f "    /*  Comp(sec) */
      "%17.2f "   /*  CompMergeCPU(sec) */
      "%9d "      /*  Comp(cnt) */
      "%8.3f "    /*  Avg(sec) */
      "%7s "      /*  KeyIn */
      "%6s "      /*  KeyDrop */
      "%9.1f "    /*  Rblob(GB) */
      "%9.1f\n",  /*  Wblob(GB) */
      name.c_str(), static_cast<int>(stat_value.at(LevelStatType::NUM_FILES)),
      static_cast<int>(stat_value.at(LevelStatType::COMPACTED_FILES)),
      BytesToHumanString(
          static_cast<uint64_t>(stat_value.at(LevelStatType::SIZE_BYTES)))
          .c_str(),
      stat_value.at(LevelStatType::SCORE),
      stat_value.at(LevelStatType::READ_GB),
      stat_value.at(LevelStatType::RN_GB),
      stat_value.at(LevelStatType::RNP1_GB),
      stat_value.at(LevelStatType::WRITE_GB),
      stat_value.at(LevelStatType::WRITE_PRE_COMP_GB),
      stat_value.at(LevelStatType::W_NEW_GB),
      stat_value.at(LevelStatType::MOVED_GB),
      stat_value.at(LevelStatType::WRITE_AMP),
      stat_value.at(LevelStatType::READ_MBPS),
      stat_value.at(LevelStatType::WRITE_MBPS),
      stat_value.at(LevelStatType::COMP_SEC),
      stat_value.at(LevelStatType::COMP_CPU_SEC),
      static_cast<int>(stat_value.at(LevelStatType::COMP_COUNT)),
      stat_value.at(LevelStatType::AVG_SEC),
      NumberToHumanString(
          static_cast<std::int64_t>(stat_value.at(LevelStatType::KEY_IN)))
          .c_str(),
      NumberToHumanString(
          static_cast<std::int64_t>(stat_value.at(LevelStatType::KEY_DROP)))
          .c_str(),
      stat_value.at(LevelStatType::R_BLOB_GB),
      stat_value.at(LevelStatType::W_BLOB_GB));
}

void PrintLevelStats(char* buf, size_t len, const std::string& name,
                     int num_files, int being_compacted, double total_file_size,
                     double score, double w_amp,
                     const InternalStats::CompactionStats& stats) {
  std::map<LevelStatType, double> level_stats;
  PrepareLevelStats(&level_stats, num_files, being_compacted, total_file_size,
                    score, w_amp, stats);
  PrintLevelStats(buf, len, name, level_stats);
}

// Assumes that trailing numbers represent an optional argument. This requires
// property names to not end with numbers.
std::pair<Slice, Slice> GetPropertyNameAndArg(const Slice& property) {
  Slice name = property;
  Slice arg = property;
  size_t sfx_len = 0;
  while (sfx_len < property.size() &&
         isdigit(property[property.size() - sfx_len - 1])) {
    ++sfx_len;
  }
  name.remove_suffix(sfx_len);
  arg.remove_prefix(property.size() - sfx_len);
  return {name, arg};
}
}  // anonymous namespace

static const std::string rocksdb_prefix = "rocksdb.";

static const std::string num_files_at_level_prefix = "num-files-at-level";
static const std::string compression_ratio_at_level_prefix =
    "compression-ratio-at-level";
static const std::string allstats = "stats";
static const std::string sstables = "sstables";
static const std::string cfstats = "cfstats";
static const std::string cfstats_no_file_histogram =
    "cfstats-no-file-histogram";
static const std::string cf_file_histogram = "cf-file-histogram";
static const std::string cf_write_stall_stats = "cf-write-stall-stats";
static const std::string dbstats = "dbstats";
static const std::string db_write_stall_stats = "db-write-stall-stats";
static const std::string levelstats = "levelstats";
static const std::string block_cache_entry_stats = "block-cache-entry-stats";
static const std::string fast_block_cache_entry_stats =
    "fast-block-cache-entry-stats";
static const std::string num_immutable_mem_table = "num-immutable-mem-table";
static const std::string num_immutable_mem_table_flushed =
    "num-immutable-mem-table-flushed";
static const std::string mem_table_flush_pending = "mem-table-flush-pending";
static const std::string compaction_pending = "compaction-pending";
static const std::string background_errors = "background-errors";
static const std::string cur_size_active_mem_table =
    "cur-size-active-mem-table";
static const std::string cur_size_all_mem_tables = "cur-size-all-mem-tables";
static const std::string size_all_mem_tables = "size-all-mem-tables";
static const std::string num_entries_active_mem_table =
    "num-entries-active-mem-table";
static const std::string num_entries_imm_mem_tables =
    "num-entries-imm-mem-tables";
static const std::string num_deletes_active_mem_table =
    "num-deletes-active-mem-table";
static const std::string num_deletes_imm_mem_tables =
    "num-deletes-imm-mem-tables";
static const std::string estimate_num_keys = "estimate-num-keys";
static const std::string estimate_table_readers_mem =
    "estimate-table-readers-mem";
static const std::string is_file_deletions_enabled =
    "is-file-deletions-enabled";
static const std::string num_snapshots = "num-snapshots";
static const std::string oldest_snapshot_time = "oldest-snapshot-time";
static const std::string oldest_snapshot_sequence = "oldest-snapshot-sequence";
static const std::string num_live_versions = "num-live-versions";
static const std::string current_version_number =
    "current-super-version-number";
static const std::string estimate_live_data_size = "estimate-live-data-size";
static const std::string min_log_number_to_keep_str = "min-log-number-to-keep";
static const std::string min_obsolete_sst_number_to_keep_str =
    "min-obsolete-sst-number-to-keep";
static const std::string base_level_str = "base-level";
static const std::string total_sst_files_size = "total-sst-files-size";
static const std::string live_sst_files_size = "live-sst-files-size";
static const std::string obsolete_sst_files_size = "obsolete-sst-files-size";
static const std::string live_sst_files_size_at_temperature =
    "live-sst-files-size-at-temperature";
static const std::string estimate_pending_comp_bytes =
    "estimate-pending-compaction-bytes";
static const std::string aggregated_table_properties =
    "aggregated-table-properties";
static const std::string aggregated_table_properties_at_level =
    aggregated_table_properties + "-at-level";
static const std::string num_running_compactions = "num-running-compactions";
static const std::string num_running_remote_compactions =
    "num-running-remote-compactions";
static const std::string num_remote_compaction_pool_threads =
    "num-remote-compaction-pool-threads";
static const std::string num_running_bottom_compactions =
    "num-running-bottom-compactions";
static const std::string num_running_compaction_sorted_runs =
    "num-running-compaction-sorted-runs";
static const std::string compaction_abort_count = "compaction-abort-count";
static const std::string num_unscheduled_compactions =
    "num-unscheduled-compactions";
static const std::string num_running_flushes = "num-running-flushes";
static const std::string actual_delayed_write_rate =
    "actual-delayed-write-rate";
static const std::string is_write_stopped = "is-write-stopped";
static const std::string estimate_oldest_key_time = "estimate-oldest-key-time";
static const std::string block_cache_capacity = "block-cache-capacity";
static const std::string block_cache_usage = "block-cache-usage";
static const std::string block_cache_pinned_usage = "block-cache-pinned-usage";
static const std::string options_statistics = "options-statistics";
static const std::string num_blob_files = "num-blob-files";
static const std::string blob_stats = "blob-stats";
static const std::string total_blob_file_size = "total-blob-file-size";
static const std::string live_blob_file_size = "live-blob-file-size";
static const std::string live_blob_file_garbage_size =
    "live-blob-file-garbage-size";
static const std::string blob_cache_capacity = "blob-cache-capacity";
static const std::string blob_cache_usage = "blob-cache-usage";
static const std::string blob_cache_pinned_usage = "blob-cache-pinned-usage";

const std::string DB::Properties::kNumFilesAtLevelPrefix =
    rocksdb_prefix + num_files_at_level_prefix;
const std::string DB::Properties::kCompressionRatioAtLevelPrefix =
    rocksdb_prefix + compression_ratio_at_level_prefix;
const std::string DB::Properties::kStats = rocksdb_prefix + allstats;
const std::string DB::Properties::kSSTables = rocksdb_prefix + sstables;
const std::string DB::Properties::kCFStats = rocksdb_prefix + cfstats;
const std::string DB::Properties::kCFStatsNoFileHistogram =
    rocksdb_prefix + cfstats_no_file_histogram;
const std::string DB::Properties::kCFFileHistogram =
    rocksdb_prefix + cf_file_histogram;
const std::string DB::Properties::kCFWriteStallStats =
    rocksdb_prefix + cf_write_stall_stats;
const std::string DB::Properties::kDBWriteStallStats =
    rocksdb_prefix + db_write_stall_stats;
const std::string DB::Properties::kDBStats = rocksdb_prefix + dbstats;
const std::string DB::Properties::kLevelStats = rocksdb_prefix + levelstats;
const std::string DB::Properties::kBlockCacheEntryStats =
    rocksdb_prefix + block_cache_entry_stats;
const std::string DB::Properties::kFastBlockCacheEntryStats =
    rocksdb_prefix + fast_block_cache_entry_stats;
const std::string DB::Properties::kNumImmutableMemTable =
    rocksdb_prefix + num_immutable_mem_table;
const std::string DB::Properties::kNumImmutableMemTableFlushed =
    rocksdb_prefix + num_immutable_mem_table_flushed;
const std::string DB::Properties::kMemTableFlushPending =
    rocksdb_prefix + mem_table_flush_pending;
const std::string DB::Properties::kCompactionPending =
    rocksdb_prefix + compaction_pending;
const std::string DB::Properties::kNumRunningCompactions =
    rocksdb_prefix + num_running_compactions;
const std::string DB::Properties::kNumRunningRemoteCompactions =
    rocksdb_prefix + num_running_remote_compactions;
const std::string DB::Properties::kNumRemoteCompactionPoolThreads =
    rocksdb_prefix + num_remote_compaction_pool_threads;
const std::string DB::Properties::kNumRunningBottomCompactions =
    rocksdb_prefix + num_running_bottom_compactions;
const std::string DB::Properties::kNumRunningCompactionSortedRuns =
    rocksdb_prefix + num_running_compaction_sorted_runs;
const std::string DB::Properties::kCompactionAbortCount =
    rocksdb_prefix + compaction_abort_count;
const std::string DB::Properties::kNumUnscheduledCompactions =
    rocksdb_prefix + num_unscheduled_compactions;
const std::string DB::Properties::kNumRunningFlushes =
    rocksdb_prefix + num_running_flushes;
const std::string DB::Properties::kBackgroundErrors =
    rocksdb_prefix + background_errors;
const std::string DB::Properties::kCurSizeActiveMemTable =
    rocksdb_prefix + cur_size_active_mem_table;
const std::string DB::Properties::kCurSizeAllMemTables =
    rocksdb_prefix + cur_size_all_mem_tables;
const std::string DB::Properties::kSizeAllMemTables =
    rocksdb_prefix + size_all_mem_tables;
const std::string DB::Properties::kNumEntriesActiveMemTable =
    rocksdb_prefix + num_entries_active_mem_table;
const std::string DB::Properties::kNumEntriesImmMemTables =
    rocksdb_prefix + num_entries_imm_mem_tables;
const std::string DB::Properties::kNumDeletesActiveMemTable =
    rocksdb_prefix + num_deletes_active_mem_table;
const std::string DB::Properties::kNumDeletesImmMemTables =
    rocksdb_prefix + num_deletes_imm_mem_tables;
const std::string DB::Properties::kEstimateNumKeys =
    rocksdb_prefix + estimate_num_keys;
const std::string DB::Properties::kEstimateTableReadersMem =
    rocksdb_prefix + estimate_table_readers_mem;
const std::string DB::Properties::kIsFileDeletionsEnabled =
    rocksdb_prefix + is_file_deletions_enabled;
const std::string DB::Properties::kNumSnapshots =
    rocksdb_prefix + num_snapshots;
const std::string DB::Properties::kOldestSnapshotTime =
    rocksdb_prefix + oldest_snapshot_time;
const std::string DB::Properties::kOldestSnapshotSequence =
    rocksdb_prefix + oldest_snapshot_sequence;
const std::string DB::Properties::kNumLiveVersions =
    rocksdb_prefix + num_live_versions;
const std::string DB::Properties::kCurrentSuperVersionNumber =
    rocksdb_prefix + current_version_number;
const std::string DB::Properties::kEstimateLiveDataSize =
    rocksdb_prefix + estimate_live_data_size;
const std::string DB::Properties::kMinLogNumberToKeep =
    rocksdb_prefix + min_log_number_to_keep_str;
const std::string DB::Properties::kMinObsoleteSstNumberToKeep =
    rocksdb_prefix + min_obsolete_sst_number_to_keep_str;
const std::string DB::Properties::kTotalSstFilesSize =
    rocksdb_prefix + total_sst_files_size;
const std::string DB::Properties::kLiveSstFilesSize =
    rocksdb_prefix + live_sst_files_size;
const std::string DB::Properties::kObsoleteSstFilesSize =
    rocksdb_prefix + obsolete_sst_files_size;
const std::string DB::Properties::kBaseLevel = rocksdb_prefix + base_level_str;
const std::string DB::Properties::kEstimatePendingCompactionBytes =
    rocksdb_prefix + estimate_pending_comp_bytes;
const std::string DB::Properties::kAggregatedTableProperties =
    rocksdb_prefix + aggregated_table_properties;
const std::string DB::Properties::kAggregatedTablePropertiesAtLevel =
    rocksdb_prefix + aggregated_table_properties_at_level;
const std::string DB::Properties::kActualDelayedWriteRate =
    rocksdb_prefix + actual_delayed_write_rate;
const std::string DB::Properties::kIsWriteStopped =
    rocksdb_prefix + is_write_stopped;
const std::string DB::Properties::kEstimateOldestKeyTime =
    rocksdb_prefix + estimate_oldest_key_time;
const std::string DB::Properties::kBlockCacheCapacity =
    rocksdb_prefix + block_cache_capacity;
const std::string DB::Properties::kBlockCacheUsage =
    rocksdb_prefix + block_cache_usage;
const std::string DB::Properties::kBlockCachePinnedUsage =
    rocksdb_prefix + block_cache_pinned_usage;
const std::string DB::Properties::kOptionsStatistics =
    rocksdb_prefix + options_statistics;
const std::string DB::Properties::kLiveSstFilesSizeAtTemperature =
    rocksdb_prefix + live_sst_files_size_at_temperature;
const std::string DB::Properties::kNumBlobFiles =
    rocksdb_prefix + num_blob_files;
const std::string DB::Properties::kBlobStats = rocksdb_prefix + blob_stats;
const std::string DB::Properties::kTotalBlobFileSize =
    rocksdb_prefix + total_blob_file_size;
const std::string DB::Properties::kLiveBlobFileSize =
    rocksdb_prefix + live_blob_file_size;
const std::string DB::Properties::kLiveBlobFileGarbageSize =
    rocksdb_prefix + live_blob_file_garbage_size;
const std::string DB::Properties::kBlobCacheCapacity =
    rocksdb_prefix + blob_cache_capacity;
const std::string DB::Properties::kBlobCacheUsage =
    rocksdb_prefix + blob_cache_usage;
const std::string DB::Properties::kBlobCachePinnedUsage =
    rocksdb_prefix + blob_cache_pinned_usage;

const std::string InternalStats::kPeriodicCFStats =
    DB::Properties::kCFStats + ".periodic";
const int InternalStats::kMaxNoChangePeriodSinceDump = 8;

const UnorderedMap<std::string, DBPropertyInfo>
    InternalStats::ppt_name_to_info = {
        {DB::Properties::kNumFilesAtLevelPrefix,
         {false, &InternalStats::HandleNumFilesAtLevel, nullptr, nullptr,
          nullptr}},
        {DB::Properties::kCompressionRatioAtLevelPrefix,
         {false, &InternalStats::HandleCompressionRatioAtLevelPrefix, nullptr,
          nullptr, nullptr}},
        {DB::Properties::kLevelStats,
         {false, &InternalStats::HandleLevelStats, nullptr, nullptr, nullptr}},
        {DB::Properties::kStats,
         {false, &InternalStats::HandleStats, nullptr, nullptr, nullptr}},
        {DB::Properties::kCFStats,
         {false, &InternalStats::HandleCFStats, nullptr,
          &InternalStats::HandleCFMapStats, nullptr}},
        {InternalStats::kPeriodicCFStats,
         {false, &InternalStats::HandleCFStatsPeriodic, nullptr, nullptr,
          nullptr}},
        {DB::Properties::kCFStatsNoFileHistogram,
         {false, &InternalStats::HandleCFStatsNoFileHistogram, nullptr, nullptr,
          nullptr}},
        {DB::Properties::kCFFileHistogram,
         {false, &InternalStats::HandleCFFileHistogram, nullptr, nullptr,
          nullptr}},
        {DB::Properties::kCFWriteStallStats,
         {false, &InternalStats::HandleCFWriteStallStats, nullptr,
          &InternalStats::HandleCFWriteStallStatsMap, nullptr}},
        {DB::Properties::kDBStats,
         {false, &InternalStats::HandleDBStats, nullptr,
          &InternalStats::HandleDBMapStats, nullptr}},
        {DB::Properties::kDBWriteStallStats,
         {false, &InternalStats::HandleDBWriteStallStats, nullptr,
          &InternalStats::HandleDBWriteStallStatsMap, nullptr}},
        {DB::Properties::kBlockCacheEntryStats,
         {true, &InternalStats::HandleBlockCacheEntryStats, nullptr,
          &InternalStats::HandleBlockCacheEntryStatsMap, nullptr}},
        {DB::Properties::kFastBlockCacheEntryStats,
         {true, &InternalStats::HandleFastBlockCacheEntryStats, nullptr,
          &InternalStats::HandleFastBlockCacheEntryStatsMap, nullptr}},
        {DB::Properties::kSSTables,
         {false, &InternalStats::HandleSsTables, nullptr, nullptr, nullptr}},
        {DB::Properties::kAggregatedTableProperties,
         {false, &InternalStats::HandleAggregatedTableProperties, nullptr,
          &InternalStats::HandleAggregatedTablePropertiesMap, nullptr}},
        {DB::Properties::kAggregatedTablePropertiesAtLevel,
         {false, &InternalStats::HandleAggregatedTablePropertiesAtLevel,
          nullptr, &InternalStats::HandleAggregatedTablePropertiesAtLevelMap,
          nullptr}},
        {DB::Properties::kNumImmutableMemTable,
         {false, nullptr, &InternalStats::HandleNumImmutableMemTable, nullptr,
          nullptr}},
        {DB::Properties::kNumImmutableMemTableFlushed,
         {false, nullptr, &InternalStats::HandleNumImmutableMemTableFlushed,
          nullptr, nullptr}},
        {DB::Properties::kMemTableFlushPending,
         {false, nullptr, &InternalStats::HandleMemTableFlushPending, nullptr,
          nullptr}},
        {DB::Properties::kCompactionPending,
         {false, nullptr, &InternalStats::HandleCompactionPending, nullptr,
          nullptr}},
        {DB::Properties::kBackgroundErrors,
         {false, nullptr, &InternalStats::HandleBackgroundErrors, nullptr,
          nullptr}},
        {DB::Properties::kCurSizeActiveMemTable,
         {false, nullptr, &InternalStats::HandleCurSizeActiveMemTable, nullptr,
          nullptr}},
        {DB::Properties::kCurSizeAllMemTables,
         {false, nullptr, &InternalStats::HandleCurSizeAllMemTables, nullptr,
          nullptr}},
        {DB::Properties::kSizeAllMemTables,
         {false, nullptr, &InternalStats::HandleSizeAllMemTables, nullptr,
          nullptr}},
        {DB::Properties::kNumEntriesActiveMemTable,
         {false, nullptr, &InternalStats::HandleNumEntriesActiveMemTable,
          nullptr, nullptr}},
        {DB::Properties::kNumEntriesImmMemTables,
         {false, nullptr, &InternalStats::HandleNumEntriesImmMemTables, nullptr,
          nullptr}},
        {DB::Properties::kNumDeletesActiveMemTable,
         {false, nullptr, &InternalStats::HandleNumDeletesActiveMemTable,
          nullptr, nullptr}},
        {DB::Properties::kNumDeletesImmMemTables,
         {false, nullptr, &InternalStats::HandleNumDeletesImmMemTables, nullptr,
          nullptr}},
        {DB::Properties::kEstimateNumKeys,
         {false, nullptr, &InternalStats::HandleEstimateNumKeys, nullptr,
          nullptr}},
        {DB::Properties::kEstimateTableReadersMem,
         {true, nullptr, &InternalStats::HandleEstimateTableReadersMem, nullptr,
          nullptr}},
        {DB::Properties::kIsFileDeletionsEnabled,
         {false, nullptr, &InternalStats::HandleIsFileDeletionsEnabled, nullptr,
          nullptr}},
        {DB::Properties::kNumSnapshots,
         {false, nullptr, &InternalStats::HandleNumSnapshots, nullptr,
          nullptr}},
        {DB::Properties::kOldestSnapshotTime,
         {false, nullptr, &InternalStats::HandleOldestSnapshotTime, nullptr,
          nullptr}},
        {DB::Properties::kOldestSnapshotSequence,
         {false, nullptr, &InternalStats::HandleOldestSnapshotSequence, nullptr,
          nullptr}},
        {DB::Properties::kNumLiveVersions,
         {false, nullptr, &InternalStats::HandleNumLiveVersions, nullptr,
          nullptr}},
        {DB::Properties::kCurrentSuperVersionNumber,
         {false, nullptr, &InternalStats::HandleCurrentSuperVersionNumber,
          nullptr, nullptr}},
        {DB::Properties::kEstimateLiveDataSize,
         {true, nullptr, &InternalStats::HandleEstimateLiveDataSize, nullptr,
          nullptr}},
        {DB::Properties::kMinLogNumberToKeep,
         {false, nullptr, &InternalStats::HandleMinLogNumberToKeep, nullptr,
          nullptr}},
        {DB::Properties::kMinObsoleteSstNumberToKeep,
         {false, nullptr, &InternalStats::HandleMinObsoleteSstNumberToKeep,
          nullptr, nullptr}},
        {DB::Properties::kBaseLevel,
         {false, nullptr, &InternalStats::HandleBaseLevel, nullptr, nullptr}},
        {DB::Properties::kTotalSstFilesSize,
         {false, nullptr, &InternalStats::HandleTotalSstFilesSize, nullptr,
          nullptr}},
        {DB::Properties::kLiveSstFilesSize,
         {false, nullptr, &InternalStats::HandleLiveSstFilesSize, nullptr,
          nullptr}},
        {DB::Properties::kLiveSstFilesSizeAtTemperature,
         {false, &InternalStats::HandleLiveSstFilesSizeAtTemperature, nullptr,
          nullptr, nullptr}},
        {DB::Properties::kObsoleteSstFilesSize,
         {false, nullptr, &InternalStats::HandleObsoleteSstFilesSize, nullptr,
          nullptr}},
        {DB::Properties::kEstimatePendingCompactionBytes,
         {false, nullptr, &InternalStats::HandleEstimatePendingCompactionBytes,
          nullptr, nullptr}},
        {DB::Properties::kNumRunningFlushes,
         {false, nullptr, &InternalStats::HandleNumRunningFlushes, nullptr,
          nullptr}},
        {DB::Properties::kNumRunningCompactions,
         {false, nullptr, &InternalStats::HandleNumRunningCompactions, nullptr,
          nullptr}},
        {DB::Properties::kNumRunningRemoteCompactions,
         {false, nullptr, &InternalStats::HandleNumRunningRemoteCompactions,
          nullptr, nullptr}},
        {DB::Properties::kNumRemoteCompactionPoolThreads,
         {false, nullptr, &InternalStats::HandleNumRemoteCompactionPoolThreads,
          nullptr, nullptr}},
        {DB::Properties::kNumRunningBottomCompactions,
         {false, nullptr, &InternalStats::HandleNumRunningBottomCompactions,
          nullptr, nullptr}},
        {DB::Properties::kNumRunningCompactionSortedRuns,
         {false, nullptr, &InternalStats::HandleNumRunningCompactionSortedRuns,
          nullptr, nullptr}},
        {DB::Properties::kCompactionAbortCount,
         {false, nullptr, &InternalStats::HandleCompactionAbortCount, nullptr,
          nullptr}},
        {DB::Properties::kNumUnscheduledCompactions,
         {false, nullptr, &InternalStats::HandleNumUnscheduledCompactions,
          nullptr, nullptr}},
        {DB::Properties::kActualDelayedWriteRate,
         {false, nullptr, &InternalStats::HandleActualDelayedWriteRate, nullptr,
          nullptr}},
        {DB::Properties::kIsWriteStopped,
         {false, nullptr, &InternalStats::HandleIsWriteStopped, nullptr,
          nullptr}},
        {DB::Properties::kEstimateOldestKeyTime,
         {false, nullptr, &InternalStats::HandleEstimateOldestKeyTime, nullptr,
          nullptr}},
        {DB::Properties::kBlockCacheCapacity,
         {false, nullptr, &InternalStats::HandleBlockCacheCapacity, nullptr,
          nullptr}},
        {DB::Properties::kBlockCacheUsage,
         {false, nullptr, &InternalStats::HandleBlockCacheUsage, nullptr,
          nullptr}},
        {DB::Properties::kBlockCachePinnedUsage,
         {false, nullptr, &InternalStats::HandleBlockCachePinnedUsage, nullptr,
          nullptr}},
        {DB::Properties::kOptionsStatistics,
         {true, nullptr, nullptr, nullptr,
          &DBImpl::GetPropertyHandleOptionsStatistics}},
        {DB::Properties::kNumBlobFiles,
         {false, nullptr, &InternalStats::HandleNumBlobFiles, nullptr,
          nullptr}},
        {DB::Properties::kBlobStats,
         {false, &InternalStats::HandleBlobStats, nullptr, nullptr, nullptr}},
        {DB::Properties::kTotalBlobFileSize,
         {false, nullptr, &InternalStats::HandleTotalBlobFileSize, nullptr,
          nullptr}},
        {DB::Properties::kLiveBlobFileSize,
         {false, nullptr, &InternalStats::HandleLiveBlobFileSize, nullptr,
          nullptr}},
        {DB::Properties::kLiveBlobFileGarbageSize,
         {false, nullptr, &InternalStats::HandleLiveBlobFileGarbageSize,
          nullptr, nullptr}},
        {DB::Properties::kBlobCacheCapacity,
         {false, nullptr, &InternalStats::HandleBlobCacheCapacity, nullptr,
          nullptr}},
        {DB::Properties::kBlobCacheUsage,
         {false, nullptr, &InternalStats::HandleBlobCacheUsage, nullptr,
          nullptr}},
        {DB::Properties::kBlobCachePinnedUsage,
         {false, nullptr, &InternalStats::HandleBlobCachePinnedUsage, nullptr,
          nullptr}},
};

InternalStats::InternalStats(int num_levels, SystemClock* clock,
                             ColumnFamilyData* cfd)
    : db_stats_{},
      cf_stats_value_{},
      cf_stats_count_{},
      comp_stats_(num_levels),
      comp_stats_by_pri_(Env::Priority::TOTAL),
      file_read_latency_(num_levels),
      has_cf_change_since_dump_(true),
      bg_error_count_(0),
      num_running_compaction_sorted_runs_(0),
      number_levels_(num_levels),
      clock_(clock),
      cfd_(cfd),
      started_at_(clock->NowMicros()) {
  Cache* block_cache = GetBlockCacheForStats();
  if (block_cache) {
    // Extract or create stats collector. Could fail in rare cases.
    Status s = CacheEntryStatsCollector<CacheEntryRoleStats>::GetShared(
        block_cache, clock_, &cache_entry_stats_collector_);
    if (s.ok()) {
      assert(cache_entry_stats_collector_);
    } else {
      assert(!cache_entry_stats_collector_);
    }
  }
}

void InternalStats::TEST_GetCacheEntryRoleStats(CacheEntryRoleStats* stats,
                                                bool foreground) {
  CollectCacheEntryStats(foreground);
  if (cache_entry_stats_collector_) {
    cache_entry_stats_collector_->GetStats(stats);
  }
}

void InternalStats::CollectCacheEntryStats(bool foreground) {
  // This function is safe to call from any thread because
  // cache_entry_stats_collector_ field is const after constructor
  // and ->GetStats does its own synchronization, which also suffices for
  // cache_entry_stats_.

  if (!cache_entry_stats_collector_) {
    return;  // nothing to do (e.g. no block cache)
  }

  // For "background" collections, strictly cap the collection time by
  // expanding effective cache TTL. For foreground, be more aggressive about
  // getting latest data.
  int min_interval_seconds = foreground ? 10 : 180;
  // 1/500 = max of 0.2% of one CPU thread
  int min_interval_factor = foreground ? 10 : 500;
  cache_entry_stats_collector_->CollectStats(min_interval_seconds,
                                             min_interval_factor);
}

std::function<void(const Slice& key, Cache::ObjectPtr value, size_t charge,
                   const Cache::CacheItemHelper* helper)>
InternalStats::CacheEntryRoleStats::GetEntryCallback() {
  return [&](const Slice& /*key*/, Cache::ObjectPtr /*value*/, size_t charge,
             const Cache::CacheItemHelper* helper) -> void {
    size_t role_idx =
        static_cast<size_t>(helper ? helper->role : CacheEntryRole::kMisc);
    entry_counts[role_idx]++;
    total_charges[role_idx] += charge;
  };
}

void InternalStats::CacheEntryRoleStats::BeginCollection(
    Cache* cache, SystemClock*, uint64_t start_time_micros) {
  Clear();
  last_start_time_micros_ = start_time_micros;
  ++collection_count;
  std::ostringstream str;
  str << cache->Name() << "@" << static_cast<void*>(cache) << "#"
      << port::GetProcessID();
  cache_id = str.str();
  cache_capacity = cache->GetCapacity();
  cache_usage = cache->GetUsage();
  table_size = cache->GetTableAddressCount();
  occupancy = cache->GetOccupancyCount();
  hash_seed = cache->GetHashSeed();
}

void InternalStats::CacheEntryRoleStats::EndCollection(
    Cache*, SystemClock*, uint64_t end_time_micros) {
  last_end_time_micros_ = end_time_micros;
}

void InternalStats::CacheEntryRoleStats::SkippedCollection() {
  ++copies_of_last_collection;
}

uint64_t InternalStats::CacheEntryRoleStats::GetLastDurationMicros() const {
  if (last_end_time_micros_ > last_start_time_micros_) {
    return last_end_time_micros_ - last_start_time_micros_;
  } else {
    return 0U;
  }
}

std::string InternalStats::CacheEntryRoleStats::ToString(
    SystemClock* clock) const {
  std::ostringstream str;
  str << "Block cache " << cache_id
      << " capacity: " << BytesToHumanString(cache_capacity)
      << " seed: " << hash_seed << " usage: " << BytesToHumanString(cache_usage)
      << " table_size: " << table_size << " occupancy: " << occupancy
      << " collections: " << collection_count
      << " last_copies: " << copies_of_last_collection
      << " last_secs: " << (GetLastDurationMicros() / 1000000.0)
      << " secs_since: "
      << ((clock->NowMicros() - last_end_time_micros_) / 1000000U) << "\n";
  str << "Block cache entry stats(count,size,portion):";
  for (size_t i = 0; i < kNumCacheEntryRoles; ++i) {
    if (entry_counts[i] > 0) {
      str << " " << kCacheEntryRoleToCamelString[i] << "(" << entry_counts[i]
          << "," << BytesToHumanString(total_charges[i]) << ","
          << (100.0 * total_charges[i] / cache_capacity) << "%)";
    }
  }
  str << "\n";
  return str.str();
}

void InternalStats::CacheEntryRoleStats::ToMap(
    std::map<std::string, std::string>* values, SystemClock* clock) const {
  values->clear();
  auto& v = *values;
  v[BlockCacheEntryStatsMapKeys::CacheId()] = cache_id;
  v[BlockCacheEntryStatsMapKeys::CacheCapacityBytes()] =
      std::to_string(cache_capacity);
  v[BlockCacheEntryStatsMapKeys::LastCollectionDurationSeconds()] =
      std::to_string(GetLastDurationMicros() / 1000000.0);
  v[BlockCacheEntryStatsMapKeys::LastCollectionAgeSeconds()] =
      std::to_string((clock->NowMicros() - last_end_time_micros_) / 1000000U);
  for (size_t i = 0; i < kNumCacheEntryRoles; ++i) {
    auto role = static_cast<CacheEntryRole>(i);
    v[BlockCacheEntryStatsMapKeys::EntryCount(role)] =
        std::to_string(entry_counts[i]);
    v[BlockCacheEntryStatsMapKeys::UsedBytes(role)] =
        std::to_string(total_charges[i]);
    v[BlockCacheEntryStatsMapKeys::UsedPercent(role)] =
        std::to_string(100.0 * total_charges[i] / cache_capacity);
  }
}

bool InternalStats::HandleBlockCacheEntryStatsInternal(std::string* value,
                                                       bool fast) {
  if (!cache_entry_stats_collector_) {
    return false;
  }
  CollectCacheEntryStats(!fast /* foreground */);
  CacheEntryRoleStats stats;
  cache_entry_stats_collector_->GetStats(&stats);
  *value = stats.ToString(clock_);
  return true;
}

bool InternalStats::HandleBlockCacheEntryStatsMapInternal(
    std::map<std::string, std::string>* values, bool fast) {
  if (!cache_entry_stats_collector_) {
    return false;
  }
  CollectCacheEntryStats(!fast /* foreground */);
  CacheEntryRoleStats stats;
  cache_entry_stats_collector_->GetStats(&stats);
  stats.ToMap(values, clock_);
  return true;
}

bool InternalStats::HandleBlockCacheEntryStats(std::string* value,
                                               Slice /*suffix*/) {
  return HandleBlockCacheEntryStatsInternal(value, false /* fast */);
}

bool InternalStats::HandleBlockCacheEntryStatsMap(
    std::map<std::string, std::string>* values, Slice /*suffix*/) {
  return HandleBlockCacheEntryStatsMapInternal(values, false /* fast */);
}

bool InternalStats::HandleFastBlockCacheEntryStats(std::string* value,
                                                   Slice /*suffix*/) {
  return HandleBlockCacheEntryStatsInternal(value, true /* fast */);
}

bool InternalStats::HandleFastBlockCacheEntryStatsMap(
    std::map<std::string, std::string>* values, Slice /*suffix*/) {
  return HandleBlockCacheEntryStatsMapInternal(values, true /* fast */);
}

bool InternalStats::HandleLiveSstFilesSizeAtTemperature(std::string* value,
                                                        Slice suffix) {
  uint64_t temperature;
  bool ok = ConsumeDecimalNumber(&suffix, &temperature) && suffix.empty();
  if (!ok) {
    return false;
  }

  uint64_t size = 0;
  const auto* vstorage = cfd_->current()->storage_info();
  for (int level = 0; level < vstorage->num_levels(); level++) {
    for (const auto& file_meta : vstorage->LevelFiles(level)) {
      if (static_cast<uint8_t>(file_meta->temperature) == temperature) {
        size += file_meta->fd.GetFileSize();
      }
    }
  }

  *value = std::to_string(size);
  return true;
}

bool InternalStats::HandleNumBlobFiles(uint64_t* value, DBImpl* /*db*/,
                                       Version* /*version*/) {
  assert(value);
  assert(cfd_);

  const auto* current = cfd_->current();
  assert(current);

  const auto* vstorage = current->storage_info();
  assert(vstorage);

  const auto& blob_files = vstorage->GetBlobFiles();

  *value = blob_files.size();

  return true;
}

bool InternalStats::HandleBlobStats(std::string* value, Slice /*suffix*/) {
  assert(value);
  assert(cfd_);

  const auto* current = cfd_->current();
  assert(current);

  const auto* vstorage = current->storage_info();
  assert(vstorage);

  const auto blob_st = vstorage->GetBlobStats();

  std::ostringstream oss;

  oss << "Number of blob files: " << vstorage->GetBlobFiles().size()
      << "\nTotal size of blob files: " << blob_st.total_file_size
      << "\nTotal size of garbage in blob files: " << blob_st.total_garbage_size
      << "\nBlob file space amplification: " << blob_st.space_amp << '\n';

  value->append(oss.str());

  return true;
}

bool InternalStats::HandleTotalBlobFileSize(uint64_t* value, DBImpl* /*db*/,
                                            Version* /*version*/) {
  assert(value);
  assert(cfd_);

  *value = cfd_->GetTotalBlobFileSize();

  return true;
}

bool InternalStats::HandleLiveBlobFileSize(uint64_t* value, DBImpl* /*db*/,
                                           Version* /*version*/) {
  assert(value);
  assert(cfd_);

  const auto* current = cfd_->current();
  assert(current);

  const auto* vstorage = current->storage_info();
  assert(vstorage);

  *value = vstorage->GetBlobStats().total_file_size;

  return true;
}

bool InternalStats::HandleLiveBlobFileGarbageSize(uint64_t* value,
                                                  DBImpl* /*db*/,
                                                  Version* /*version*/) {
  assert(value);
  assert(cfd_);

  const auto* current = cfd_->current();
  assert(current);

  const auto* vstorage = current->storage_info();
  assert(vstorage);

  *value = vstorage->GetBlobStats().total_garbage_size;

  return true;
}

Cache* InternalStats::GetBlobCacheForStats() {
  return cfd_->ioptions().blob_cache.get();
}

bool InternalStats::HandleBlobCacheCapacity(uint64_t* value, DBImpl* /*db*/,
                                            Version* /*version*/) {
  Cache* blob_cache = GetBlobCacheForStats();
  if (blob_cache) {
    *value = static_cast<uint64_t>(blob_cache->GetCapacity());
    return true;
  }
  return false;
}

bool InternalStats::HandleBlobCacheUsage(uint64_t* value, DBImpl* /*db*/,
                                         Version* /*version*/) {
  Cache* blob_cache = GetBlobCacheForStats();
  if (blob_cache) {
    *value = static_cast<uint64_t>(blob_cache->GetUsage());
    return true;
  }
  return false;
}

bool InternalStats::HandleBlobCachePinnedUsage(uint64_t* value, DBImpl* /*db*/,
                                               Version* /*version*/) {
  Cache* blob_cache = GetBlobCacheForStats();
  if (blob_cache) {
    *value = static_cast<uint64_t>(blob_cache->GetPinnedUsage());
    return true;
  }
  return false;
}

const DBPropertyInfo* GetPropertyInfo(const Slice& property) {
  std::string ppt_name = GetPropertyNameAndArg(property).first.ToString();
  auto ppt_info_iter = InternalStats::ppt_name_to_info.find(ppt_name);
  if (ppt_info_iter == InternalStats::ppt_name_to_info.end()) {
    return nullptr;
  }
  return &ppt_info_iter->second;
}

bool InternalStats::GetStringProperty(const DBPropertyInfo& property_info,
                                      const Slice& property,
                                      std::string* value) {
  assert(value != nullptr);
  assert(property_info.handle_string != nullptr);
  Slice arg = GetPropertyNameAndArg(property).second;
  return (this->*(property_info.handle_string))(value, arg);
}

bool InternalStats::GetMapProperty(const DBPropertyInfo& property_info,
                                   const Slice& property,
                                   std::map<std::string, std::string>* value) {
  assert(value != nullptr);
  assert(property_info.handle_map != nullptr);
  Slice arg = GetPropertyNameAndArg(property).second;
  return (this->*(property_info.handle_map))(value, arg);
}

bool InternalStats::GetIntProperty(const DBPropertyInfo& property_info,
                                   uint64_t* value, DBImpl* db) {
  assert(value != nullptr);
  assert(property_info.handle_int != nullptr &&
         !property_info.need_out_of_mutex);
  db->mutex_.AssertHeld();
  return (this->*(property_info.handle_int))(value, db, nullptr /* version */);
}

bool InternalStats::GetIntPropertyOutOfMutex(
    const DBPropertyInfo& property_info, Version* version, uint64_t* value) {
  assert(value != nullptr);
  assert(property_info.handle_int != nullptr &&
         property_info.need_out_of_mutex);
  return (this->*(property_info.handle_int))(value, nullptr /* db */, version);
}

bool InternalStats::HandleNumFilesAtLevel(std::string* value, Slice suffix) {
  uint64_t level;
  const auto* vstorage = cfd_->current()->storage_info();
  bool ok = ConsumeDecimalNumber(&suffix, &level) && suffix.empty();
  if (!ok || static_cast<int>(level) >= number_levels_) {
    return false;
  } else {
    char buf[100];
    snprintf(buf, sizeof(buf), "%d",
             vstorage->NumLevelFiles(static_cast<int>(level)));
    *value = buf;
    return true;
  }
}

bool InternalStats::HandleCompressionRatioAtLevelPrefix(std::string* value,
                                                        Slice suffix) {
  uint64_t level;
  const auto* vstorage = cfd_->current()->storage_info();
  bool ok = ConsumeDecimalNumber(&suffix, &level) && suffix.empty();
  if (!ok || level >= static_cast<uint64_t>(number_levels_)) {
    return false;
  }
  *value = std::to_string(
      vstorage->GetEstimatedCompressionRatioAtLevel(static_cast<int>(level)));
  return true;
}

bool InternalStats::HandleLevelStats(std::string* value, Slice /*suffix*/) {
  char buf[1000];
  const auto* vstorage = cfd_->current()->storage_info();
  snprintf(buf, sizeof(buf),
           "Level Files Size(MB)\n"
           "--------------------\n");
  value->append(buf);

  for (int level = 0; level < number_levels_; level++) {
    snprintf(buf, sizeof(buf), "%3d %8d %8.0f\n", level,
             vstorage->NumLevelFiles(level),
             vstorage->NumLevelBytes(level) / kMB);
    value->append(buf);
  }
  return true;
}

bool InternalStats::HandleStats(std::string* value, Slice suffix) {
  if (!HandleCFStats(value, suffix)) {
    return false;
  }
  if (!HandleDBStats(value, suffix)) {
    return false;
  }
  return true;
}

bool InternalStats::HandleCFMapStats(
    std::map<std::string, std::string>* cf_stats, Slice /*suffix*/) {
  DumpCFMapStats(cf_stats);
  return true;
}

bool InternalStats::HandleCFStats(std::string* value, Slice /*suffix*/) {
  DumpCFStats(value);
  return true;
}

bool InternalStats::HandleCFStatsPeriodic(std::string* value,
                                          Slice /*suffix*/) {
  bool has_change = has_cf_change_since_dump_;
  if (!has_change) {
    // If file histogram changes, there is activity in this period too.
    uint64_t new_histogram_num = 0;
    for (int level = 0; level < number_levels_; level++) {
      new_histogram_num += file_read_latency_[level].num();
    }
    new_histogram_num += blob_file_read_latency_.num();
    if (new_histogram_num != last_histogram_num) {
      has_change = true;
      last_histogram_num = new_histogram_num;
    }
  }
  if (has_change) {
    no_cf_change_period_since_dump_ = 0;
    has_cf_change_since_dump_ = false;
  } else if (no_cf_change_period_since_dump_++ > 0) {
    // Not ready to sync
    if (no_cf_change_period_since_dump_ == kMaxNoChangePeriodSinceDump) {
      // Next periodic, we need to dump stats even if there is no change.
      no_cf_change_period_since_dump_ = 0;
    }
    return true;
  }

  DumpCFStatsNoFileHistogram(/*is_periodic=*/true, value);
  DumpCFFileHistogram(value);
  return true;
}

bool InternalStats::HandleCFStatsNoFileHistogram(std::string* value,
                                                 Slice /*suffix*/) {
  DumpCFStatsNoFileHistogram(/*is_periodic=*/false, value);
  return true;
}

bool InternalStats::HandleCFFileHistogram(std::string* value,
                                          Slice /*suffix*/) {
  DumpCFFileHistogram(value);
  return true;
}

bool InternalStats::HandleCFWriteStallStats(std::string* value,
                                            Slice /*suffix*/) {
  DumpCFStatsWriteStall(value);
  return true;
}

bool InternalStats::HandleCFWriteStallStatsMap(
    std::map<std::string, std::string>* value, Slice /*suffix*/) {
  DumpCFMapStatsWriteStall(value);
  return true;
}

bool InternalStats::HandleDBMapStats(
    std::map<std::string, std::string>* db_stats, Slice /*suffix*/) {
  DumpDBMapStats(db_stats);
  return true;
}

bool InternalStats::HandleDBStats(std::string* value, Slice /*suffix*/) {
  DumpDBStats(value);
  return true;
}

bool InternalStats::HandleDBWriteStallStats(std::string* value,
                                            Slice /*suffix*/) {
  DumpDBStatsWriteStall(value);
  return true;
}

bool InternalStats::HandleDBWriteStallStatsMap(
    std::map<std::string, std::string>* value, Slice /*suffix*/) {
  DumpDBMapStatsWriteStall(value);
  return true;
}

bool InternalStats::HandleSsTables(std::string* value, Slice /*suffix*/) {
  auto* current = cfd_->current();
  *value = current->DebugString(true, true);
  return true;
}

bool InternalStats::HandleAggregatedTableProperties(std::string* value,
                                                    Slice /*suffix*/) {
  std::shared_ptr<const TableProperties> tp;
  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;
  auto s = cfd_->current()->GetAggregatedTableProperties(read_options, &tp);
  if (!s.ok()) {
    return false;
  }
  *value = tp->ToString();
  return true;
}

static std::map<std::string, std::string> MapUint64ValuesToString(
    const std::map<std::string, uint64_t>& from) {
  std::map<std::string, std::string> to;
  for (const auto& e : from) {
    to[e.first] = std::to_string(e.second);
  }
  return to;
}

bool InternalStats::HandleAggregatedTablePropertiesMap(
    std::map<std::string, std::string>* values, Slice /*suffix*/) {
  std::shared_ptr<const TableProperties> tp;
  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;
  auto s = cfd_->current()->GetAggregatedTableProperties(read_options, &tp);
  if (!s.ok()) {
    return false;
  }
  *values = MapUint64ValuesToString(tp->GetAggregatablePropertiesAsMap());
  return true;
}

bool InternalStats::HandleAggregatedTablePropertiesAtLevel(std::string* values,
                                                           Slice suffix) {
  uint64_t level;
  bool ok = ConsumeDecimalNumber(&suffix, &level) && suffix.empty();
  if (!ok || static_cast<int>(level) >= number_levels_) {
    return false;
  }
  std::shared_ptr<const TableProperties> tp;
  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;
  auto s = cfd_->current()->GetAggregatedTableProperties(
      read_options, &tp, static_cast<int>(level));
  if (!s.ok()) {
    return false;
  }
  *values = tp->ToString();
  return true;
}

bool InternalStats::HandleAggregatedTablePropertiesAtLevelMap(
    std::map<std::string, std::string>* values, Slice suffix) {
  uint64_t level;
  bool ok = ConsumeDecimalNumber(&suffix, &level) && suffix.empty();
  if (!ok || static_cast<int>(level) >= number_levels_) {
    return false;
  }
  std::shared_ptr<const TableProperties> tp;
  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;
  auto s = cfd_->current()->GetAggregatedTableProperties(
      read_options, &tp, static_cast<int>(level));
  if (!s.ok()) {
    return false;
  }
  *values = MapUint64ValuesToString(tp->GetAggregatablePropertiesAsMap());
  return true;
}

bool InternalStats::HandleNumImmutableMemTable(uint64_t* value, DBImpl* /*db*/,
                                               Version* /*version*/) {
  *value = cfd_->imm()->NumNotFlushed();
  return true;
}

bool InternalStats::HandleNumImmutableMemTableFlushed(uint64_t* value,
                                                      DBImpl* /*db*/,
                                                      Version* /*version*/) {
  *value = cfd_->imm()->NumFlushed();
  return true;
}

bool InternalStats::HandleMemTableFlushPending(uint64_t* value, DBImpl* /*db*/,
                                               Version* /*version*/) {
  *value = (cfd_->imm()->IsFlushPending() ? 1 : 0);
  return true;
}

bool InternalStats::HandleNumRunningFlushes(uint64_t* value, DBImpl* db,
                                            Version* /*version*/) {
  *value = db->num_running_flushes();
  return true;
}

bool InternalStats::HandleCompactionPending(uint64_t* value, DBImpl* /*db*/,
                                            Version* /*version*/) {
  // 1 if the system already determines at least one compaction is needed.
  // 0 otherwise,
  const auto* vstorage = cfd_->current()->storage_info();
  *value = (cfd_->compaction_picker()->NeedsCompaction(vstorage) ? 1 : 0);
  return true;
}

bool InternalStats::HandleNumRunningCompactions(uint64_t* value, DBImpl* db,
                                                Version* /*version*/) {
  *value = db->num_running_compactions_;
  return true;
}

bool InternalStats::HandleNumRunningRemoteCompactions(uint64_t* value,
                                                      DBImpl* db,
                                                      Version* /*version*/) {
  const int num_running =
      db->num_running_remote_compactions_.load(std::memory_order_relaxed);
  assert(num_running >= 0);
  *value = static_cast<uint64_t>(num_running);
  return true;
}

bool InternalStats::HandleNumRemoteCompactionPoolThreads(uint64_t* value,
                                                         DBImpl* /*db*/,
                                                         Version* /*version*/) {
  auto pool = RemoteCompactionThreadPool::GetShared(false);
  *value = pool ? static_cast<uint64_t>(pool->GetBackgroundThreads()) : 0;
  return true;
}

bool InternalStats::HandleNumRunningBottomCompactions(uint64_t* value,
                                                      DBImpl* db,
                                                      Version* /*version*/) {
  *value = static_cast<uint64_t>(db->num_running_bottom_compactions_);
  return true;
}

bool InternalStats::HandleNumRunningCompactionSortedRuns(uint64_t* value,
                                                         DBImpl* db,
                                                         Version* /*version*/) {
  db->mutex()->AssertHeld();
  uint64_t sorted_runs = 0;
  for (auto* cfd : *db->versions_->GetColumnFamilySet()) {
    sorted_runs += cfd->internal_stats()->NumRunningCompactionSortedRuns();
  }
  *value = sorted_runs;
  return true;
}

bool InternalStats::HandleCompactionAbortCount(uint64_t* value, DBImpl* db,
                                               Version* /*version*/) {
  *value = static_cast<uint64_t>(
      db->compaction_aborted_.load(std::memory_order_acquire));
  return true;
}

bool InternalStats::HandleNumUnscheduledCompactions(uint64_t* value, DBImpl* db,
                                                    Version* /*version*/) {
  db->mutex()->AssertHeld();
  assert(db->unscheduled_compactions_ >= 0);
  *value = static_cast<uint64_t>(db->unscheduled_compactions_);
  return true;
}

bool InternalStats::HandleBackgroundErrors(uint64_t* value, DBImpl* /*db*/,
                                           Version* /*version*/) {
  // Accumulated number of  errors in background flushes or compactions.
  *value = GetBackgroundErrorCount();
  return true;
}

bool InternalStats::HandleCurSizeActiveMemTable(uint64_t* value, DBImpl* /*db*/,
                                                Version* /*version*/) {
  // Current size of the active memtable
  // Using ApproximateMemoryUsageFast to avoid the need for synchronization
  *value = cfd_->mem()->ApproximateMemoryUsageFast();
  return true;
}

bool InternalStats::HandleCurSizeAllMemTables(uint64_t* value, DBImpl* /*db*/,
                                              Version* /*version*/) {
  // Current size of the active memtable + immutable memtables
  // Using ApproximateMemoryUsageFast to avoid the need for synchronization
  *value = cfd_->mem()->ApproximateMemoryUsageFast() +
           cfd_->imm()->ApproximateUnflushedMemTablesMemoryUsage();
  return true;
}

bool InternalStats::HandleSizeAllMemTables(uint64_t* value, DBImpl* /*db*/,
                                           Version* /*version*/) {
  // Using ApproximateMemoryUsageFast to avoid the need for synchronization
  *value = cfd_->mem()->ApproximateMemoryUsageFast() +
           cfd_->imm()->ApproximateMemoryUsage();
  return true;
}

bool InternalStats::HandleNumEntriesActiveMemTable(uint64_t* value,
                                                   DBImpl* /*db*/,
                                                   Version* /*version*/) {
  // Current number of entires in the active memtable
  *value = cfd_->mem()->NumEntries();
  return true;
}

bool InternalStats::HandleNumEntriesImmMemTables(uint64_t* value,
                                                 DBImpl* /*db*/,
                                                 Version* /*version*/) {
  // Current number of entries in the immutable memtables
  *value = cfd_->imm()->current()->GetTotalNumEntries();
  return true;
}

bool InternalStats::HandleNumDeletesActiveMemTable(uint64_t* value,
                                                   DBImpl* /*db*/,
                                                   Version* /*version*/) {
  // Current number of entires in the active memtable
  *value = cfd_->mem()->NumDeletion();
  return true;
}

bool InternalStats::HandleNumDeletesImmMemTables(uint64_t* value,
                                                 DBImpl* /*db*/,
                                                 Version* /*version*/) {
  // Current number of entries in the immutable memtables
  *value = cfd_->imm()->current()->GetTotalNumDeletes();
  return true;
}

bool InternalStats::HandleEstimateNumKeys(uint64_t* value, DBImpl* /*db*/,
                                          Version* /*version*/) {
  // Estimate number of entries in the column family:
  // Use estimated entries in tables + total entries in memtables.
  const auto* vstorage = cfd_->current()->storage_info();
  uint64_t estimate_keys = cfd_->mem()->NumEntries() +
                           cfd_->imm()->current()->GetTotalNumEntries() +
                           vstorage->GetEstimatedActiveKeys();
  uint64_t estimate_deletes =
      cfd_->mem()->NumDeletion() + cfd_->imm()->current()->GetTotalNumDeletes();
  *value = estimate_keys > estimate_deletes * 2
               ? estimate_keys - (estimate_deletes * 2)
               : 0;
  return true;
}

bool InternalStats::HandleNumSnapshots(uint64_t* value, DBImpl* db,
                                       Version* /*version*/) {
  *value = db->snapshots().count();
  return true;
}

bool InternalStats::HandleOldestSnapshotTime(uint64_t* value, DBImpl* db,
                                             Version* /*version*/) {
  *value = static_cast<uint64_t>(db->snapshots().GetOldestSnapshotTime());
  return true;
}

bool InternalStats::HandleOldestSnapshotSequence(uint64_t* value, DBImpl* db,
                                                 Version* /*version*/) {
  *value = static_cast<uint64_t>(db->snapshots().GetOldestSnapshotSequence());
  return true;
}

bool InternalStats::HandleNumLiveVersions(uint64_t* value, DBImpl* /*db*/,
                                          Version* /*version*/) {
  *value = cfd_->GetNumLiveVersions();
  return true;
}

bool InternalStats::HandleCurrentSuperVersionNumber(uint64_t* value,
                                                    DBImpl* /*db*/,
                                                    Version* /*version*/) {
  *value = cfd_->GetSuperVersionNumber();
  return true;
}

bool InternalStats::HandleIsFileDeletionsEnabled(uint64_t* value, DBImpl* db,
                                                 Version* /*version*/) {
  *value = db->IsFileDeletionsEnabled() ? 1 : 0;
  return true;
}

bool InternalStats::HandleBaseLevel(uint64_t* value, DBImpl* /*db*/,
                                    Version* /*version*/) {
  const auto* vstorage = cfd_->current()->storage_info();
  *value = vstorage->base_level();
  return true;
}

bool InternalStats::HandleTotalSstFilesSize(uint64_t* value, DBImpl* /*db*/,
                                            Version* /*version*/) {
  *value = cfd_->GetTotalSstFilesSize();
  return true;
}

bool InternalStats::HandleLiveSstFilesSize(uint64_t* value, DBImpl* /*db*/,
                                           Version* /*version*/) {
  *value = cfd_->GetLiveSstFilesSize();
  return true;
}

bool InternalStats::HandleObsoleteSstFilesSize(uint64_t* value, DBImpl* db,
                                               Version* /*version*/) {
  *value = db->GetObsoleteSstFilesSize();
  return true;
}

bool InternalStats::HandleEstimatePendingCompactionBytes(uint64_t* value,
                                                         DBImpl* /*db*/,
                                                         Version* /*version*/) {
  const auto* vstorage = cfd_->current()->storage_info();
  *value = vstorage->estimated_compaction_needed_bytes();
  return true;
}

bool InternalStats::HandleEstimateTableReadersMem(uint64_t* value,
                                                  DBImpl* /*db*/,
                                                  Version* version) {
  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;
  *value = (version == nullptr)
               ? 0
               : version->GetMemoryUsageByTableReaders(read_options);
  return true;
}

bool InternalStats::HandleEstimateLiveDataSize(uint64_t* value, DBImpl* /*db*/,
                                               Version* version) {
  const auto* vstorage = version->storage_info();
  *value = vstorage->EstimateLiveDataSize();
  return true;
}

bool InternalStats::HandleMinLogNumberToKeep(uint64_t* value, DBImpl* db,
                                             Version* /*version*/) {
  *value = db->MinLogNumberToKeep();
  return true;
}

bool InternalStats::HandleMinObsoleteSstNumberToKeep(uint64_t* value,
                                                     DBImpl* db,
                                                     Version* /*version*/) {
  *value = db->MinObsoleteSstNumberToKeep();
  return true;
}

bool InternalStats::HandleActualDelayedWriteRate(uint64_t* value, DBImpl* db,
                                                 Version* /*version*/) {
  const WriteController& wc = db->write_controller();
  if (!wc.NeedsDelay()) {
    *value = 0;
  } else {
    *value = wc.delayed_write_rate();
  }
  return true;
}

bool InternalStats::HandleIsWriteStopped(uint64_t* value, DBImpl* db,
                                         Version* /*version*/) {
  *value = db->write_controller().IsStopped() ? 1 : 0;
  return true;
}

bool InternalStats::HandleEstimateOldestKeyTime(uint64_t* value, DBImpl* /*db*/,
                                                Version* /*version*/) {
  // TODO(yiwu): The property is currently available for fifo compaction
  // with allow_compaction = false. This is because we don't propagate
  // oldest_key_time on compaction.
  if (cfd_->ioptions().compaction_style != kCompactionStyleFIFO ||
      cfd_->GetCurrentMutableCFOptions()
          .compaction_options_fifo.allow_compaction) {
    return false;
  }
  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;
  TablePropertiesCollection collection;
  auto s = cfd_->current()->GetPropertiesOfAllTables(read_options, &collection);
  if (!s.ok()) {
    return false;
  }
  *value = std::numeric_limits<uint64_t>::max();
  for (auto& p : collection) {
    *value = std::min(*value, p.second->oldest_key_time);
    if (*value == 0) {
      break;
    }
  }
  if (*value > 0) {
    *value = std::min({cfd_->mem()->ApproximateOldestKeyTime(),
                       cfd_->imm()->ApproximateOldestKeyTime(), *value});
  }
  return *value > 0 && *value < std::numeric_limits<uint64_t>::max();
}

Cache* InternalStats::GetBlockCacheForStats() {
  // NOTE: called in startup before GetCurrentMutableCFOptions() is ready
  auto* table_factory = cfd_->GetLatestMutableCFOptions().table_factory.get();
  assert(table_factory != nullptr);
  // FIXME: need to a shared_ptr if/when block_cache is going to be mutable
  return table_factory->GetOptions<Cache>(TableFactory::kBlockCacheOpts());
}

bool InternalStats::HandleBlockCacheCapacity(uint64_t* value, DBImpl* /*db*/,
                                             Version* /*version*/) {
  Cache* block_cache = GetBlockCacheForStats();
  if (block_cache) {
    *value = static_cast<uint64_t>(block_cache->GetCapacity());
    return true;
  }
  return false;
}

bool InternalStats::HandleBlockCacheUsage(uint64_t* value, DBImpl* /*db*/,
                                          Version* /*version*/) {
  Cache* block_cache = GetBlockCacheForStats();
  if (block_cache) {
    *value = static_cast<uint64_t>(block_cache->GetUsage());
    return true;
  }
  return false;
}

bool InternalStats::HandleBlockCachePinnedUsage(uint64_t* value, DBImpl* /*db*/,
                                                Version* /*version*/) {
  Cache* block_cache = GetBlockCacheForStats();
  if (block_cache) {
    *value = static_cast<uint64_t>(block_cache->GetPinnedUsage());
    return true;
  }
  return false;
}

void InternalStats::DumpDBMapStats(
    std::map<std::string, std::string>* db_stats) {
  for (int i = 0; i < static_cast<int>(kIntStatsNumMax); ++i) {
    InternalDBStatsType type = static_cast<InternalDBStatsType>(i);
    (*db_stats)[db_stats_type_to_info.at(type).property_name] =
        std::to_string(GetDBStats(type));
  }
  double seconds_up = (clock_->NowMicros() - started_at_) / kMicrosInSec;
  (*db_stats)["db.uptime"] = std::to_string(seconds_up);
}

void InternalStats::DumpDBStats(std::string* value) {
  char buf[1000];
  // DB-level stats, only available from default column family
  double seconds_up = (clock_->NowMicros() - started_at_) / kMicrosInSec;
  double interval_seconds_up = seconds_up - db_stats_snapshot_.seconds_up;
  snprintf(buf, sizeof(buf),
           "\n** DB Stats **\nUptime(secs): %.1f total, %.1f interval\n",
           seconds_up, interval_seconds_up);
  value->append(buf);
  // Cumulative
  uint64_t user_bytes_written =
      GetDBStats(InternalStats::kIntStatsBytesWritten);
  uint64_t num_keys_written =
      GetDBStats(InternalStats::kIntStatsNumKeysWritten);
  uint64_t write_other = GetDBStats(InternalStats::kIntStatsWriteDoneByOther);
  uint64_t write_self = GetDBStats(InternalStats::kIntStatsWriteDoneBySelf);
  uint64_t wal_bytes = GetDBStats(InternalStats::kIntStatsWalFileBytes);
  uint64_t wal_synced = GetDBStats(InternalStats::kIntStatsWalFileSynced);
  uint64_t write_with_wal = GetDBStats(InternalStats::kIntStatsWriteWithWal);
  uint64_t write_stall_micros =
      GetDBStats(InternalStats::kIntStatsWriteStallMicros);

  const int kHumanMicrosLen = 32;
  char human_micros[kHumanMicrosLen];

  // Data
  // writes: total number of write requests.
  // keys: total number of key updates issued by all the write requests
  // commit groups: number of group commits issued to the DB. Each group can
  //                contain one or more writes.
  // so writes/keys is the average number of put in multi-put or put
  // writes/groups is the average group commit size.
  //
  // The format is the same for interval stats.
  snprintf(buf, sizeof(buf),
           "Cumulative writes: %s writes, %s keys, %s commit groups, "
           "%.1f writes per commit group, ingest: %.2f GB, %.2f MB/s\n",
           NumberToHumanString(write_other + write_self).c_str(),
           NumberToHumanString(num_keys_written).c_str(),
           NumberToHumanString(write_self).c_str(),
           (write_other + write_self) /
               std::max(1.0, static_cast<double>(write_self)),
           user_bytes_written / kGB,
           user_bytes_written / kMB / std::max(seconds_up, 0.001));
  value->append(buf);
  // WAL
  snprintf(buf, sizeof(buf),
           "Cumulative WAL: %s writes, %s syncs, "
           "%.2f writes per sync, written: %.2f GB, %.2f MB/s\n",
           NumberToHumanString(write_with_wal).c_str(),
           NumberToHumanString(wal_synced).c_str(),
           write_with_wal / std::max(1.0, static_cast<double>(wal_synced)),
           wal_bytes / kGB, wal_bytes / kMB / std::max(seconds_up, 0.001));
  value->append(buf);
  // Stall
  AppendHumanMicros(write_stall_micros, human_micros, kHumanMicrosLen, true);
  snprintf(buf, sizeof(buf), "Cumulative stall: %s, %.1f percent\n",
           human_micros,
           // 10000 = divide by 1M to get secs, then multiply by 100 for pct
           write_stall_micros / 10000.0 / std::max(seconds_up, 0.001));
  value->append(buf);

  // Interval
  uint64_t interval_write_other = write_other - db_stats_snapshot_.write_other;
  uint64_t interval_write_self = write_self - db_stats_snapshot_.write_self;
  uint64_t interval_num_keys_written =
      num_keys_written - db_stats_snapshot_.num_keys_written;
  snprintf(
      buf, sizeof(buf),
      "Interval writes: %s writes, %s keys, %s commit groups, "
      "%.1f writes per commit group, ingest: %.2f MB, %.2f MB/s\n",
      NumberToHumanString(interval_write_other + interval_write_self).c_str(),
      NumberToHumanString(interval_num_keys_written).c_str(),
      NumberToHumanString(interval_write_self).c_str(),
      static_cast<double>(interval_write_other + interval_write_self) /
          std::max(1.0, static_cast<double>(interval_write_self)),
      (user_bytes_written - db_stats_snapshot_.ingest_bytes) / kMB,
      (user_bytes_written - db_stats_snapshot_.ingest_bytes) / kMB /
          std::max(interval_seconds_up, 0.001)),
      value->append(buf);

  uint64_t interval_write_with_wal =
      write_with_wal - db_stats_snapshot_.write_with_wal;
  uint64_t interval_wal_synced = wal_synced - db_stats_snapshot_.wal_synced;
  uint64_t interval_wal_bytes = wal_bytes - db_stats_snapshot_.wal_bytes;

  snprintf(buf, sizeof(buf),
           "Interval WAL: %s writes, %s syncs, "
           "%.2f writes per sync, written: %.2f GB, %.2f MB/s\n",
           NumberToHumanString(interval_write_with_wal).c_str(),
           NumberToHumanString(interval_wal_synced).c_str(),
           interval_write_with_wal /
               std::max(1.0, static_cast<double>(interval_wal_synced)),
           interval_wal_bytes / kGB,
           interval_wal_bytes / kMB / std::max(interval_seconds_up, 0.001));
  value->append(buf);

  // Stall
  AppendHumanMicros(write_stall_micros - db_stats_snapshot_.write_stall_micros,
                    human_micros, kHumanMicrosLen, true);
  snprintf(buf, sizeof(buf), "Interval stall: %s, %.1f percent\n", human_micros,
           // 10000 = divide by 1M to get secs, then multiply by 100 for pct
           (write_stall_micros - db_stats_snapshot_.write_stall_micros) /
               10000.0 / std::max(interval_seconds_up, 0.001));
  value->append(buf);

  std::string write_stall_stats;
  DumpDBStatsWriteStall(&write_stall_stats);
  value->append(write_stall_stats);

  db_stats_snapshot_.seconds_up = seconds_up;
  db_stats_snapshot_.ingest_bytes = user_bytes_written;
  db_stats_snapshot_.write_other = write_other;
  db_stats_snapshot_.write_self = write_self;
  db_stats_snapshot_.num_keys_written = num_keys_written;
  db_stats_snapshot_.wal_bytes = wal_bytes;
  db_stats_snapshot_.wal_synced = wal_synced;
  db_stats_snapshot_.write_with_wal = write_with_wal;
  db_stats_snapshot_.write_stall_micros = write_stall_micros;
}

void InternalStats::DumpDBMapStatsWriteStall(
    std::map<std::string, std::string>* value) {
  constexpr uint32_t max_db_scope_write_stall_cause =
      static_cast<uint32_t>(WriteStallCause::kDBScopeWriteStallCauseEnumMax);

  for (uint32_t i =
           max_db_scope_write_stall_cause - kNumDBScopeWriteStallCauses;
       i < max_db_scope_write_stall_cause; ++i) {
    for (uint32_t j = 0;
         j < static_cast<uint32_t>(WriteStallCondition::kNormal); ++j) {
      WriteStallCause cause = static_cast<WriteStallCause>(i);
      WriteStallCondition condition = static_cast<WriteStallCondition>(j);
      InternalStats::InternalDBStatsType internal_db_stat =
          InternalDBStat(cause, condition);

      if (internal_db_stat == InternalStats::kIntStatsNumMax) {
        continue;
      }

      std::string name =
          WriteStallStatsMapKeys::CauseConditionCount(cause, condition);
      uint64_t stat =
          db_stats_[static_cast<std::size_t>(internal_db_stat)].load(
              std::memory_order_relaxed);
      (*value)[name] = std::to_string(stat);
    }
  }
}

void InternalStats::DumpDBStatsWriteStall(std::string* value) {
  assert(value);

  std::map<std::string, std::string> write_stall_stats_map;
  DumpDBMapStatsWriteStall(&write_stall_stats_map);

  std::ostringstream str;
  str << "Write Stall (count): ";

  for (auto write_stall_stats_map_iter = write_stall_stats_map.begin();
       write_stall_stats_map_iter != write_stall_stats_map.end();
       write_stall_stats_map_iter++) {
    const auto& name_and_stat = *write_stall_stats_map_iter;
    str << name_and_stat.first << ": " << name_and_stat.second;
    if (std::next(write_stall_stats_map_iter) == write_stall_stats_map.end()) {
      str << "\n";
    } else {
      str << ", ";
    }
  }
  *value = str.str();
}

/**
 * Dump Compaction Level stats to a map of stat name with "compaction." prefix
 * to value in double as string. The level in stat name is represented with
 * a prefix "Lx" where "x" is the level number. A special level "Sum"
 * represents the sum of a stat for all levels.
 * The result also contains IO stall counters which keys start with "io_stalls."
 * and values represent uint64 encoded as strings.
 */
void InternalStats::DumpCFMapStats(
    std::map<std::string, std::string>* cf_stats) {
  const VersionStorageInfo* vstorage = cfd_->current()->storage_info();
  CompactionStats compaction_stats_sum;
  std::map<int, std::map<LevelStatType, double>> levels_stats;
  DumpCFMapStats(vstorage, &levels_stats, &compaction_stats_sum);
  for (auto const& level_ent : levels_stats) {
    auto level_str =
        level_ent.first == -1 ? "Sum" : "L" + std::to_string(level_ent.first);
    for (auto const& stat_ent : level_ent.second) {
      auto stat_type = stat_ent.first;
      auto key_str =
          "compaction." + level_str + "." +
          InternalStats::compaction_level_stats.at(stat_type).property_name;
      (*cf_stats)[key_str] = std::to_string(stat_ent.second);
    }
  }

  DumpCFMapStatsWriteStall(cf_stats);
}

void InternalStats::DumpCFMapStats(
    const VersionStorageInfo* vstorage,
    std::map<int, std::map<LevelStatType, double>>* levels_stats,
    CompactionStats* compaction_stats_sum) {
  assert(vstorage);

  int num_levels_to_check =
      (cfd_->ioptions().compaction_style == kCompactionStyleLevel)
          ? vstorage->num_levels() - 1
          : 1;

  // Compaction scores are sorted based on its value. Restore them to the
  // level order
  std::vector<double> compaction_score(number_levels_, 0);
  for (int i = 0; i < num_levels_to_check; ++i) {
    compaction_score[vstorage->CompactionScoreLevel(i)] =
        vstorage->CompactionScore(i);
  }
  // Count # of files being compacted for each level
  std::vector<int> files_being_compacted(number_levels_, 0);
  for (int level = 0; level < number_levels_; ++level) {
    for (auto* f : vstorage->LevelFiles(level)) {
      if (f->being_compacted) {
        ++files_being_compacted[level];
      }
    }
  }

  int total_files = 0;
  int total_files_being_compacted = 0;
  double total_file_size = 0;
  uint64_t flush_ingest = cf_stats_value_[BYTES_FLUSHED];
  uint64_t add_file_ingest = cf_stats_value_[BYTES_INGESTED_ADD_FILE];
  uint64_t curr_ingest = flush_ingest + add_file_ingest;
  for (int level = 0; level < number_levels_; level++) {
    int files = vstorage->NumLevelFiles(level);
    total_files += files;
    total_files_being_compacted += files_being_compacted[level];
    if (comp_stats_[level].micros > 0 || comp_stats_[level].cpu_micros > 0 ||
        files > 0) {
      compaction_stats_sum->Add(comp_stats_[level]);
      total_file_size += vstorage->NumLevelBytes(level);
      uint64_t input_bytes;
      if (level == 0) {
        input_bytes = curr_ingest;
      } else {
        input_bytes = comp_stats_[level].bytes_read_non_output_levels +
                      comp_stats_[level].bytes_read_blob;
      }
      double w_amp =
          (input_bytes == 0)
              ? 0.0
              : static_cast<double>(comp_stats_[level].bytes_written +
                                    comp_stats_[level].bytes_written_blob) /
                    input_bytes;
      std::map<LevelStatType, double> level_stats;
      PrepareLevelStats(&level_stats, files, files_being_compacted[level],
                        static_cast<double>(vstorage->NumLevelBytes(level)),
                        compaction_score[level], w_amp, comp_stats_[level]);
      (*levels_stats)[level] = level_stats;
    }
  }
  // Cumulative summary
  double w_amp = (0 == curr_ingest)
                     ? 0.0
                     : (compaction_stats_sum->bytes_written +
                        compaction_stats_sum->bytes_written_blob) /
                           static_cast<double>(curr_ingest);
  // Stats summary across levels
  std::map<LevelStatType, double> sum_stats;
  PrepareLevelStats(&sum_stats, total_files, total_files_being_compacted,
                    total_file_size, 0, w_amp, *compaction_stats_sum);
  (*levels_stats)[-1] = sum_stats;  //  -1 is for the Sum level
}

void InternalStats::DumpCFMapStatsByPriority(
    std::map<int, std::map<LevelStatType, double>>* priorities_stats) {
  for (size_t priority = 0; priority < comp_stats_by_pri_.size(); priority++) {
    if (comp_stats_by_pri_[priority].micros > 0) {
      std::map<LevelStatType, double> priority_stats;
      PrepareLevelStats(&priority_stats, 0 /* num_files */,
                        0 /* being_compacted */, 0 /* total_file_size */,
                        0 /* compaction_score */, 0 /* w_amp */,
                        comp_stats_by_pri_[priority]);
      (*priorities_stats)[static_cast<int>(priority)] = priority_stats;
    }
  }
}

void InternalStats::DumpCFMapStatsWriteStall(
    std::map<std::string, std::string>* value) {
  uint64_t total_delays = 0;
  uint64_t total_stops = 0;
  constexpr uint32_t max_cf_scope_write_stall_cause =
      static_cast<uint32_t>(WriteStallCause::kCFScopeWriteStallCauseEnumMax);

  for (uint32_t i =
           max_cf_scope_write_stall_cause - kNumCFScopeWriteStallCauses;
       i < max_cf_scope_write_stall_cause; ++i) {
    for (uint32_t j = 0;
         j < static_cast<uint32_t>(WriteStallCondition::kNormal); ++j) {
      WriteStallCause cause = static_cast<WriteStallCause>(i);
      WriteStallCondition condition = static_cast<WriteStallCondition>(j);
      InternalStats::InternalCFStatsType internal_cf_stat =
          InternalCFStat(cause, condition);

      if (internal_cf_stat == InternalStats::INTERNAL_CF_STATS_ENUM_MAX) {
        continue;
      }

      std::string name =
          WriteStallStatsMapKeys::CauseConditionCount(cause, condition);
      uint64_t stat =
          cf_stats_count_[static_cast<std::size_t>(internal_cf_stat)];
      (*value)[name] = std::to_string(stat);

      if (condition == WriteStallCondition::kDelayed) {
        total_delays += stat;
      } else if (condition == WriteStallCondition::kStopped) {
        total_stops += stat;
      }
    }
  }

  (*value)[WriteStallStatsMapKeys::
               CFL0FileCountLimitDelaysWithOngoingCompaction()] =
      std::to_string(
          cf_stats_count_[L0_FILE_COUNT_LIMIT_DELAYS_WITH_ONGOING_COMPACTION]);
  (*value)[WriteStallStatsMapKeys::
               CFL0FileCountLimitStopsWithOngoingCompaction()] =
      std::to_string(
          cf_stats_count_[L0_FILE_COUNT_LIMIT_STOPS_WITH_ONGOING_COMPACTION]);

  (*value)[WriteStallStatsMapKeys::TotalStops()] = std::to_string(total_stops);
  (*value)[WriteStallStatsMapKeys::TotalDelays()] =
      std::to_string(total_delays);
}

void InternalStats::DumpCFStatsWriteStall(std::string* value,
                                          uint64_t* total_stall_count) {
  assert(value);

  std::map<std::string, std::string> write_stall_stats_map;
  DumpCFMapStatsWriteStall(&write_stall_stats_map);

  std::ostringstream str;
  str << "Write Stall (count): ";

  for (auto write_stall_stats_map_iter = write_stall_stats_map.begin();
       write_stall_stats_map_iter != write_stall_stats_map.end();
       write_stall_stats_map_iter++) {
    const auto& name_and_stat = *write_stall_stats_map_iter;
    str << name_and_stat.first << ": " << name_and_stat.second;
    if (std::next(write_stall_stats_map_iter) == write_stall_stats_map.end()) {
      str << "\n";
    } else {
      str << ", ";
    }
  }

  if (total_stall_count) {
    *total_stall_count =
        ParseUint64(
            write_stall_stats_map[WriteStallStatsMapKeys::TotalStops()]) +
        ParseUint64(
            write_stall_stats_map[WriteStallStatsMapKeys::TotalDelays()]);
    if (*total_stall_count > 0) {
      str << "interval: " << *total_stall_count - cf_stats_snapshot_.stall_count
          << " total count\n";
    }
  }
  *value = str.str();
}

void InternalStats::DumpCFStats(std::string* value) {
  DumpCFStatsNoFileHistogram(/*is_periodic=*/false, value);
  DumpCFFileHistogram(value);
}

void InternalStats::DumpCFStatsNoFileHistogram(bool is_periodic,
                                               std::string* value) {
  char buf[2000];
  // Per-ColumnFamily stats
  PrintLevelStatsHeader(buf, sizeof(buf), cfd_->GetName(), "Level");
  value->append(buf);

  // Print stats for each level
  const VersionStorageInfo* vstorage = cfd_->current()->storage_info();
  std::map<int, std::map<LevelStatType, double>> levels_stats;
  CompactionStats compaction_stats_sum;
  DumpCFMapStats(vstorage, &levels_stats, &compaction_stats_sum);
  for (int l = 0; l < number_levels_; ++l) {
    if (levels_stats.find(l) != levels_stats.end()) {
      PrintLevelStats(buf, sizeof(buf), "L" + std::to_string(l),
                      levels_stats[l]);
      value->append(buf);
    }
  }

  // Print sum of level stats
  PrintLevelStats(buf, sizeof(buf), "Sum", levels_stats[-1]);
  value->append(buf);

  uint64_t flush_ingest = cf_stats_value_[BYTES_FLUSHED];
  uint64_t add_file_ingest = cf_stats_value_[BYTES_INGESTED_ADD_FILE];
  uint64_t ingest_files_addfile = cf_stats_value_[INGESTED_NUM_FILES_TOTAL];
  uint64_t ingest_l0_files_addfile =
      cf_stats_value_[INGESTED_LEVEL0_NUM_FILES_TOTAL];
  uint64_t ingest_keys_addfile = cf_stats_value_[INGESTED_NUM_KEYS_TOTAL];
  // Interval summary
  uint64_t interval_flush_ingest =
      flush_ingest - cf_stats_snapshot_.ingest_bytes_flush;
  uint64_t interval_add_file_inget =
      add_file_ingest - cf_stats_snapshot_.ingest_bytes_addfile;
  uint64_t interval_ingest =
      interval_flush_ingest + interval_add_file_inget + 1;
  CompactionStats interval_stats(compaction_stats_sum);
  interval_stats.Subtract(cf_stats_snapshot_.comp_stats);
  double w_amp =
      (interval_stats.bytes_written + interval_stats.bytes_written_blob) /
      static_cast<double>(interval_ingest);
  PrintLevelStats(buf, sizeof(buf), "Int", 0, 0, 0, 0, w_amp, interval_stats);
  value->append(buf);

  PrintLevelStatsHeader(buf, sizeof(buf), cfd_->GetName(), "Priority");
  value->append(buf);
  std::map<int, std::map<LevelStatType, double>> priorities_stats;
  DumpCFMapStatsByPriority(&priorities_stats);
  for (size_t priority = 0; priority < comp_stats_by_pri_.size(); ++priority) {
    if (priorities_stats.find(static_cast<int>(priority)) !=
        priorities_stats.end()) {
      PrintLevelStats(
          buf, sizeof(buf),
          Env::PriorityToString(static_cast<Env::Priority>(priority)),
          priorities_stats[static_cast<int>(priority)]);
      value->append(buf);
    }
  }

  const auto blob_st = vstorage->GetBlobStats();

  snprintf(buf, sizeof(buf),
           "\nBlob file count: %" ROCKSDB_PRIszt
           ", total size: %.1f GB, garbage size: %.1f GB, space amp: %.1f\n\n",
           vstorage->GetBlobFiles().size(), blob_st.total_file_size / kGB,
           blob_st.total_garbage_size / kGB, blob_st.space_amp);
  value->append(buf);

  uint64_t now_micros = clock_->NowMicros();
  double seconds_up = (now_micros - started_at_) / kMicrosInSec;
  double interval_seconds_up = seconds_up - cf_stats_snapshot_.seconds_up;
  snprintf(buf, sizeof(buf), "Uptime(secs): %.1f total, %.1f interval\n",
           seconds_up, interval_seconds_up);
  value->append(buf);
  snprintf(buf, sizeof(buf), "Flush(GB): cumulative %.3f, interval %.3f\n",
           flush_ingest / kGB, interval_flush_ingest / kGB);
  value->append(buf);
  snprintf(buf, sizeof(buf), "AddFile(GB): cumulative %.3f, interval %.3f\n",
           add_file_ingest / kGB, interval_add_file_inget / kGB);
  value->append(buf);

  uint64_t interval_ingest_files_addfile =
      ingest_files_addfile - cf_stats_snapshot_.ingest_files_addfile;
  snprintf(buf, sizeof(buf),
           "AddFile(Total Files): cumulative %" PRIu64 ", interval %" PRIu64
           "\n",
           ingest_files_addfile, interval_ingest_files_addfile);
  value->append(buf);

  uint64_t interval_ingest_l0_files_addfile =
      ingest_l0_files_addfile - cf_stats_snapshot_.ingest_l0_files_addfile;
  snprintf(buf, sizeof(buf),
           "AddFile(L0 Files): cumulative %" PRIu64 ", interval %" PRIu64 "\n",
           ingest_l0_files_addfile, interval_ingest_l0_files_addfile);
  value->append(buf);

  uint64_t interval_ingest_keys_addfile =
      ingest_keys_addfile - cf_stats_snapshot_.ingest_keys_addfile;
  snprintf(buf, sizeof(buf),
           "AddFile(Keys): cumulative %" PRIu64 ", interval %" PRIu64 "\n",
           ingest_keys_addfile, interval_ingest_keys_addfile);
  value->append(buf);

  // Compact
  uint64_t compact_bytes_read = 0;
  uint64_t compact_bytes_write = 0;
  uint64_t compact_micros = 0;
  for (int level = 0; level < number_levels_; level++) {
    compact_bytes_read += comp_stats_[level].bytes_read_output_level +
                          comp_stats_[level].bytes_read_non_output_levels +
                          comp_stats_[level].bytes_read_blob;
    compact_bytes_write += comp_stats_[level].bytes_written +
                           comp_stats_[level].bytes_written_blob;
    compact_micros += comp_stats_[level].micros;
  }

  snprintf(buf, sizeof(buf),
           "Cumulative compaction: %.2f GB write, %.2f MB/s write, "
           "%.2f GB read, %.2f MB/s read, %.1f seconds\n",
           compact_bytes_write / kGB,
           compact_bytes_write / kMB / std::max(seconds_up, 0.001),
           compact_bytes_read / kGB,
           compact_bytes_read / kMB / std::max(seconds_up, 0.001),
           compact_micros / kMicrosInSec);
  value->append(buf);

  // Compaction interval
  uint64_t interval_compact_bytes_write =
      compact_bytes_write - cf_stats_snapshot_.compact_bytes_write;
  uint64_t interval_compact_bytes_read =
      compact_bytes_read - cf_stats_snapshot_.compact_bytes_read;
  uint64_t interval_compact_micros =
      compact_micros - cf_stats_snapshot_.compact_micros;

  snprintf(
      buf, sizeof(buf),
      "Interval compaction: %.2f GB write, %.2f MB/s write, "
      "%.2f GB read, %.2f MB/s read, %.1f seconds\n",
      interval_compact_bytes_write / kGB,
      interval_compact_bytes_write / kMB / std::max(interval_seconds_up, 0.001),
      interval_compact_bytes_read / kGB,
      interval_compact_bytes_read / kMB / std::max(interval_seconds_up, 0.001),
      interval_compact_micros / kMicrosInSec);
  value->append(buf);

  snprintf(buf, sizeof(buf),
           "Estimated pending compaction bytes: %" PRIu64 "\n",
           vstorage->estimated_compaction_needed_bytes());
  value->append(buf);

  if (is_periodic) {
    cf_stats_snapshot_.compact_bytes_write = compact_bytes_write;
    cf_stats_snapshot_.compact_bytes_read = compact_bytes_read;
    cf_stats_snapshot_.compact_micros = compact_micros;
  }

  std::string write_stall_stats;
  uint64_t total_stall_count;
  DumpCFStatsWriteStall(&write_stall_stats, &total_stall_count);
  value->append(write_stall_stats);

  if (is_periodic) {
    cf_stats_snapshot_.seconds_up = seconds_up;
    cf_stats_snapshot_.ingest_bytes_flush = flush_ingest;
    cf_stats_snapshot_.ingest_bytes_addfile = add_file_ingest;
    cf_stats_snapshot_.ingest_files_addfile = ingest_files_addfile;
    cf_stats_snapshot_.ingest_l0_files_addfile = ingest_l0_files_addfile;
    cf_stats_snapshot_.ingest_keys_addfile = ingest_keys_addfile;
    cf_stats_snapshot_.comp_stats = compaction_stats_sum;
    cf_stats_snapshot_.stall_count = total_stall_count;
  }

  // Do not gather cache entry stats during CFStats because DB
  // mutex is held. Only dump last cached collection (rely on DB
  // periodic stats dump to update)
  if (cache_entry_stats_collector_) {
    CacheEntryRoleStats stats;
    // thread safe
    cache_entry_stats_collector_->GetStats(&stats);

    constexpr uint64_t kDayInMicros = uint64_t{86400} * 1000000U;

    // Skip if stats are extremely old (> 1 day, incl not yet populated)
    if (now_micros - stats.last_end_time_micros_ < kDayInMicros) {
      value->append(stats.ToString(clock_));
    }
  }
}

void InternalStats::DumpCFFileHistogram(std::string* value) {
  assert(value);
  assert(cfd_);

  std::ostringstream oss;
  oss << "\n** File Read Latency Histogram By Level [" << cfd_->GetName()
      << "] **\n";

  for (int level = 0; level < number_levels_; level++) {
    if (!file_read_latency_[level].Empty()) {
      oss << "** Level " << level << " read latency histogram (micros):\n"
          << file_read_latency_[level].ToString() << '\n';
    }
  }

  if (!blob_file_read_latency_.Empty()) {
    oss << "** Blob file read latency histogram (micros):\n"
        << blob_file_read_latency_.ToString() << '\n';
  }

  value->append(oss.str());
}

namespace {

class SumPropertyAggregator : public IntPropertyAggregator {
 public:
  SumPropertyAggregator() : aggregated_value_(0) {}
  ~SumPropertyAggregator() override = default;

  void Add(ColumnFamilyData* cfd, uint64_t value) override {
    (void)cfd;
    aggregated_value_ += value;
  }

  uint64_t Aggregate() const override { return aggregated_value_; }

 private:
  uint64_t aggregated_value_;
};

// A block cache may be shared by multiple column families.
// BlockCachePropertyAggregator ensures that the same cache is only added once.
class BlockCachePropertyAggregator : public IntPropertyAggregator {
 public:
  BlockCachePropertyAggregator() = default;
  ~BlockCachePropertyAggregator() override = default;

  void Add(ColumnFamilyData* cfd, uint64_t value) override {
    auto* table_factory = cfd->GetCurrentMutableCFOptions().table_factory.get();
    assert(table_factory != nullptr);
    Cache* cache =
        table_factory->GetOptions<Cache>(TableFactory::kBlockCacheOpts());
    if (cache != nullptr) {
      block_cache_properties_.emplace(cache, value);
    }
  }

  uint64_t Aggregate() const override {
    uint64_t sum = 0;
    for (const auto& p : block_cache_properties_) {
      sum += p.second;
    }
    return sum;
  }

 private:
  std::unordered_map<Cache*, uint64_t> block_cache_properties_;
};

}  // anonymous namespace

std::unique_ptr<IntPropertyAggregator> CreateIntPropertyAggregator(
    const Slice& property) {
  if (property == DB::Properties::kBlockCacheCapacity ||
      property == DB::Properties::kBlockCacheUsage ||
      property == DB::Properties::kBlockCachePinnedUsage) {
    return std::make_unique<BlockCachePropertyAggregator>();
  } else {
    return std::make_unique<SumPropertyAggregator>();
  }
}

}  // namespace ROCKSDB_NAMESPACE
