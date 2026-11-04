//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_picker_fifo.h"

#include <algorithm>
#include <cinttypes>
#include <string>
#include <vector>

#include "db/column_family.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"
#include "options/options_helper.h"
#include "rocksdb/listener.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
namespace {
uint64_t GetTotalFilesSize(const std::vector<FileMetaData*>& files) {
  uint64_t total_size = 0;
  for (const auto& f : files) {
    total_size += f->fd.file_size;
  }
  return total_size;
}

// Compute effective data size and capacity limit for FIFO compaction.
// When max_data_files_size > 0 (blob-aware mode), the effective size includes
// both SST and blob file sizes, and the limit is max_data_files_size.
// Otherwise, only SST sizes are used with max_table_files_size as the limit.
void GetEffectiveSizeAndLimit(const CompactionOptionsFIFO& fifo_opts,
                              uint64_t total_sst_size, uint64_t total_blob_size,
                              uint64_t* effective_size,
                              uint64_t* effective_max) {
  *effective_size = total_sst_size;
  *effective_max = fifo_opts.max_table_files_size;
  if (fifo_opts.max_data_files_size > 0) {
    *effective_size += total_blob_size;
    *effective_max = fifo_opts.max_data_files_size;
  }
}

// Return the effective capacity limit for FIFO compaction.
// Convenience wrapper when only the limit is needed (e.g., PickTTLCompaction).
uint64_t GetEffectiveMax(const CompactionOptionsFIFO& fifo_opts) {
  return fifo_opts.max_data_files_size > 0 ? fifo_opts.max_data_files_size
                                           : fifo_opts.max_table_files_size;
}
}  // anonymous namespace

bool FIFOCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  const int kLevel0 = 0;
  return vstorage->CompactionScore(kLevel0) >= 1;
}

Compaction* FIFOCompactionPicker::PickTTLCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer) {
  assert(mutable_cf_options.ttl > 0);

  const int kLevel0 = 0;
  const std::vector<FileMetaData*>& level_files = vstorage->LevelFiles(kLevel0);
  uint64_t total_size = GetTotalFilesSize(level_files);

  int64_t _current_time;
  auto status = ioptions_.clock->GetCurrentTime(&_current_time);
  if (!status.ok()) {
    ROCKS_LOG_BUFFER(log_buffer,
                     "[%s] FIFO compaction: Couldn't get current time: %s. "
                     "Not doing compactions based on TTL. ",
                     cf_name.c_str(), status.ToString().c_str());
    return nullptr;
  }
  const uint64_t current_time = static_cast<uint64_t>(_current_time);

  if (!level0_compactions_in_progress_.empty()) {
    ROCKS_LOG_BUFFER(
        log_buffer,
        "[%s] FIFO compaction: Already executing compaction. No need "
        "to run parallel compactions since compactions are very fast",
        cf_name.c_str());
    return nullptr;
  }

  std::vector<CompactionInputFiles> inputs;
  inputs.emplace_back();
  inputs[0].level = 0;

  // avoid underflow
  if (current_time > mutable_cf_options.ttl) {
    for (auto ritr = level_files.rbegin(); ritr != level_files.rend(); ++ritr) {
      FileMetaData* f = *ritr;
      assert(f);
      TableReader* reader = f->fd.pinned_reader.Get();
      if (reader != nullptr && reader->GetTableProperties() != nullptr) {
        uint64_t newest_key_time = f->TryGetNewestKeyTime();
        uint64_t oldest_ancestor_time =
            reader->GetTableProperties()->oldest_ancestor_time;
        uint64_t est_newest_key_time = newest_key_time == kUnknownNewestKeyTime
                                           ? oldest_ancestor_time
                                           : newest_key_time;
        if (est_newest_key_time == kUnknownNewestKeyTime) {
          // E.g. a file written by SstFileWriter. Every key in the file was
          // written no later than the file's contents were built.
          est_newest_key_time = f->TryGetFileCreationTime();
        }
        // CheckExternalFileNotTooOld() (reject_too_old_file) mirrors this
        // estimate
        if (est_newest_key_time == kUnknownNewestKeyTime ||
            est_newest_key_time >= (current_time - mutable_cf_options.ttl)) {
          break;
        }
      }
      total_size -= f->fd.file_size;
      inputs[0].files.push_back(f);
    }
  }

  // Return a nullptr and proceed to size-based FIFO compaction if:
  // 1. there are no files older than ttl OR
  // 2. there are a few files older than ttl, but deleting them will not bring
  //    the total size to be less than the size threshold.
  uint64_t effective_max =
      GetEffectiveMax(mutable_cf_options.compaction_options_fifo);
  // Estimate the effective remaining data after dropping TTL-expired SSTs.
  // Each dropped SST also frees a proportional share of blob data.
  //
  // In multi-level FIFO (migration), we must use total SST across ALL levels
  // as the reference, because total_blob covers all levels. Using only L0
  // SST would inflate the blob estimate.
  uint64_t effective_remaining = total_size;
  if (mutable_cf_options.compaction_options_fifo.max_data_files_size > 0) {
    uint64_t total_blob = vstorage->GetBlobStats().total_file_size;
    // Compute total SST across all levels so the reference scope matches
    // total_blob's scope (all levels).
    uint64_t total_sst_all_levels = GetTotalFilesSize(level_files);
    for (int level = 1; level < vstorage->num_levels(); ++level) {
      total_sst_all_levels += GetTotalFilesSize(vstorage->LevelFiles(level));
    }
    // remaining_sst_all = total_sst_all - dropped_l0_sst
    // total_size is the remaining L0 SST after removing expired files;
    // original L0 SST minus remaining L0 SST = dropped.
    uint64_t original_l0_sst = GetTotalFilesSize(level_files);
    uint64_t dropped_sst = original_l0_sst - total_size;
    uint64_t remaining_sst_all = total_sst_all_levels - dropped_sst;
    // Proportional blob estimate: each SST byte "owns" a proportional
    // share of blob bytes. Both reference sizes must come from the same
    // scope (all levels) to avoid inflated estimates.
    if (total_sst_all_levels > 0 && total_blob > 0) {
      effective_remaining =
          remaining_sst_all +
          static_cast<uint64_t>(static_cast<double>(remaining_sst_all) /
                                total_sst_all_levels * total_blob);
    } else {
      effective_remaining = remaining_sst_all;
    }
  }
  if (inputs[0].files.empty() || effective_remaining > effective_max) {
    return nullptr;
  }

  for (const auto& f : inputs[0].files) {
    assert(f);
    uint64_t newest_key_time = f->TryGetNewestKeyTime();
    uint64_t oldest_ancestor_time = 0;
    TableReader* reader = f->fd.pinned_reader.Get();
    if (reader != nullptr && reader->GetTableProperties() != nullptr) {
      oldest_ancestor_time = reader->GetTableProperties()->oldest_ancestor_time;
    }
    uint64_t est_newest_key_time = newest_key_time == kUnknownNewestKeyTime
                                       ? oldest_ancestor_time
                                       : newest_key_time;
    ROCKS_LOG_BUFFER(log_buffer,
                     "[%s] FIFO compaction: picking file %" PRIu64
                     " with estimated newest key time %" PRIu64 " for deletion",
                     cf_name.c_str(), f->fd.GetNumber(), est_newest_key_time);
  }

  Compaction* c = new Compaction(
      vstorage, ioptions_, mutable_cf_options, mutable_db_options,
      std::move(inputs), 0, 0, 0, 0, kNoCompression,
      mutable_cf_options.compression_opts, Temperature::kUnknown,
      /* max_subcompactions */ 0, {}, /* earliest_snapshot */ std::nullopt,
      /* snapshot_checker */ nullptr, CompactionReason::kFIFOTtl,
      /* trim_ts */ "", vstorage->CompactionScore(0),
      /* l0_files_might_overlap */ true);
  return c;
}

// The size-based compaction picker for FIFO.
//
// When the entire column family size exceeds max_table_files_size, FIFO will
// try to delete the oldest sst file(s) until the resulting column family size
// is smaller than max_table_files_size.
//
// This function also takes care the case where a DB is migrating from level /
// universal compaction to FIFO compaction.  During the migration, the column
// family will also have non-L0 files while FIFO can only create L0 files.
// In this case, this function will first purge the sst files in the bottom-
// most non-empty level first, and the DB will eventually converge to the
// regular FIFO case where there're only L0 files.  Note that during the
// migration case, the purge order will only be an approximation of "FIFO"
// as entries inside lower-level files might sometimes be newer than some
// entries inside upper-level files.
Compaction* FIFOCompactionPicker::PickSizeCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer) {
  const auto& fifo_opts = mutable_cf_options.compaction_options_fifo;

  // compute the total SST size and identify the last non-empty level
  int last_level = 0;
  uint64_t total_size = 0;
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    auto level_size = GetTotalFilesSize(vstorage->LevelFiles(level));
    total_size += level_size;
    if (level_size > 0) {
      last_level = level;
    }
  }
  const std::vector<FileMetaData*>& last_level_files =
      vstorage->LevelFiles(last_level);

  // Compute effective size and limit for comparison.
  uint64_t effective_size;
  uint64_t effective_max;
  GetEffectiveSizeAndLimit(fifo_opts, total_size,
                           vstorage->GetBlobStats().total_file_size,
                           &effective_size, &effective_max);

  if (last_level == 0 && effective_size <= effective_max) {
    return nullptr;
  }

  if (!level0_compactions_in_progress_.empty()) {
    ROCKS_LOG_BUFFER(
        log_buffer,
        "[%s] FIFO compaction: Already executing compaction. No need "
        "to run parallel compactions since compactions are very fast",
        cf_name.c_str());
    return nullptr;
  }

  std::vector<CompactionInputFiles> inputs;
  inputs.emplace_back();
  inputs[0].level = last_level;

  if (last_level == 0) {
    // When using blob-aware sizing, use proportional estimation (same
    // principle as EstimateTotalDataForSST): each SST "owns"
    // effective_size / num_files of total data. This is an approximation
    // -- individual SSTs may reference different amounts of blob data,
    // but uniform distribution is a reasonable estimate for FIFO dropping.
    uint64_t remaining_size = effective_size;
    const uint64_t num_files = last_level_files.size();
    // Proportional estimate of data per file (SST + blob).
    // Use max(1) to prevent stalling when effective_size < num_files.
    const uint64_t data_per_file =
        (fifo_opts.max_data_files_size > 0 && num_files > 0)
            ? std::max(effective_size / num_files, uint64_t{1})
            : 0;

    // In L0, right-most files are the oldest files.
    for (auto ritr = last_level_files.rbegin(); ritr != last_level_files.rend();
         ++ritr) {
      auto f = *ritr;
      if (fifo_opts.max_data_files_size > 0) {
        remaining_size -= std::min(remaining_size, data_per_file);
      } else {
        remaining_size -= std::min(remaining_size, f->fd.file_size);
      }
      inputs[0].files.push_back(f);
      char tmp_fsize[16];
      AppendHumanBytes(f->fd.GetFileSize(), tmp_fsize, sizeof(tmp_fsize));
      ROCKS_LOG_BUFFER(log_buffer,
                       "[%s] FIFO compaction: picking file %" PRIu64
                       " with size %s for deletion",
                       cf_name.c_str(), f->fd.GetNumber(), tmp_fsize);
      if (remaining_size <= effective_max) {
        break;
      }
    }
  } else if (effective_size > effective_max) {
    // If the last level is non-L0, we actually don't know which file is
    // logically the oldest since the file creation time only represents
    // when this file was compacted to this level, which is independent
    // to when the entries in this file were first inserted.
    //
    // As a result, we delete files from the left instead.  This means the sst
    // file with the smallest key will be deleted first.  This design decision
    // better serves a major type of FIFO use cases where smaller keys are
    // associated with older data.
    const uint64_t num_files = last_level_files.size();
    // Proportional estimate of data per file (SST + blob), same as L0 path.
    const uint64_t data_per_file =
        (fifo_opts.max_data_files_size > 0 && num_files > 0)
            ? std::max(effective_size / num_files, uint64_t{1})
            : 0;
    for (const auto& f : last_level_files) {
      if (f->being_compacted) {
        continue;
      }
      if (fifo_opts.max_data_files_size > 0) {
        effective_size -= std::min(effective_size, data_per_file);
      } else {
        effective_size -= std::min(effective_size, f->fd.file_size);
      }
      inputs[0].files.push_back(f);
      char tmp_fsize[16];
      AppendHumanBytes(f->fd.GetFileSize(), tmp_fsize, sizeof(tmp_fsize));
      ROCKS_LOG_BUFFER(log_buffer,
                       "[%s] FIFO compaction: picking file %" PRIu64
                       " with size %s for deletion under total size %" PRIu64
                       " vs max size %" PRIu64,
                       cf_name.c_str(), f->fd.GetNumber(), tmp_fsize,
                       effective_size, effective_max);

      if (effective_size <= effective_max) {
        break;
      }
    }
  } else {
    return nullptr;
  }

  Compaction* c = new Compaction(
      vstorage, ioptions_, mutable_cf_options, mutable_db_options,
      std::move(inputs), last_level,
      /* target_file_size */ 0,
      /* max_compaction_bytes */ 0,
      /* output_path_id */ 0, kNoCompression,
      mutable_cf_options.compression_opts, Temperature::kUnknown,
      /* max_subcompactions */ 0, {}, /* earliest_snapshot */ std::nullopt,
      /* snapshot_checker */ nullptr, CompactionReason::kFIFOMaxSize,
      /* trim_ts */ "", vstorage->CompactionScore(0),
      /* l0_files_might_overlap */ true);
  return c;
}

Compaction* FIFOCompactionPicker::PickTemperatureChangeCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer) const {
  const std::vector<FileTemperatureAge>& ages =
      mutable_cf_options.compaction_options_fifo
          .file_temperature_age_thresholds;
  if (ages.empty()) {
    return nullptr;
  }

  // Does not apply to multi-level FIFO.
  if (vstorage->num_levels() > 1) {
    return nullptr;
  }

  const int kLevel0 = 0;
  const std::vector<FileMetaData*>& level_files = vstorage->LevelFiles(kLevel0);
  if (level_files.empty()) {
    return nullptr;
  }

  int64_t _current_time;
  auto status = ioptions_.clock->GetCurrentTime(&_current_time);
  if (!status.ok()) {
    ROCKS_LOG_BUFFER(
        log_buffer,
        "[%s] FIFO compaction: Couldn't get current time: %s. "
        "Not doing compactions based on file temperature-age threshold. ",
        cf_name.c_str(), status.ToString().c_str());
    return nullptr;
  }
  const uint64_t current_time = static_cast<uint64_t>(_current_time);

  if (!level0_compactions_in_progress_.empty()) {
    ROCKS_LOG_BUFFER(
        log_buffer,
        "[%s] FIFO compaction: Already executing compaction. Parallel "
        "compactions are not supported",
        cf_name.c_str());
    return nullptr;
  }

  std::vector<CompactionInputFiles> inputs;
  inputs.emplace_back();
  inputs[0].level = 0;

  // avoid underflow
  uint64_t min_age = ages[0].age;
  // kLastTemperature means target temperature is to be determined.
  Temperature compaction_target_temp = Temperature::kLastTemperature;
  if (current_time > min_age) {
    uint64_t create_time_threshold = current_time - min_age;
    assert(level_files.size() >= 1);
    for (size_t index = level_files.size(); index >= 1; --index) {
      // Try to add cur_file to compaction inputs.
      FileMetaData* cur_file = level_files[index - 1];
      FileMetaData* prev_file = index < 2 ? nullptr : level_files[index - 2];
      if (cur_file->being_compacted) {
        // Should not happen since we check for
        // `level0_compactions_in_progress_` above. Here we simply just don't
        // schedule anything.
        return nullptr;
      }
      uint64_t est_newest_key_time = cur_file->TryGetNewestKeyTime(prev_file);
      // Newer file could have newest_key_time populated
      if (est_newest_key_time == kUnknownNewestKeyTime) {
        continue;
      }
      if (est_newest_key_time > create_time_threshold) {
        break;
      }
      Temperature cur_target_temp = ages[0].temperature;
      for (size_t i = 1; i < ages.size(); ++i) {
        if (current_time >= ages[i].age &&
            est_newest_key_time <= current_time - ages[i].age) {
          cur_target_temp = ages[i].temperature;
        }
      }
      if (cur_file->temperature == cur_target_temp) {
        continue;
      }

      // cur_file needs to change temperature
      assert(compaction_target_temp == Temperature::kLastTemperature);
      compaction_target_temp = cur_target_temp;
      inputs[0].files.push_back(cur_file);
      ROCKS_LOG_BUFFER(log_buffer,
                       "[%s] FIFO compaction: picking file %" PRIu64
                       " with estimated newest key time %" PRIu64
                       " and temperature %s for temperature %s.",
                       cf_name.c_str(), cur_file->fd.GetNumber(),
                       est_newest_key_time,
                       temperature_to_string[cur_file->temperature].c_str(),
                       temperature_to_string[cur_target_temp].c_str());
      break;
    }
  }

  if (inputs[0].files.empty()) {
    return nullptr;
  }
  assert(compaction_target_temp != Temperature::kLastTemperature);
  // Only compact one file at a time.
  assert(inputs.size() == 1);
  assert(inputs[0].size() == 1);
  Compaction* c = new Compaction(
      vstorage, ioptions_, mutable_cf_options, mutable_db_options,
      std::move(inputs), 0, 0 /* output file size limit */,
      0 /* max compaction bytes, not applicable */, 0 /* output path ID */,
      mutable_cf_options.compression, mutable_cf_options.compression_opts,
      compaction_target_temp,
      /* max_subcompactions */ 0, {}, /* earliest_snapshot */ std::nullopt,
      /* snapshot_checker */ nullptr, CompactionReason::kChangeTemperature,
      /* trim_ts */ "", vstorage->CompactionScore(0),
      /* l0_files_might_overlap */ true);
  return c;
}

Compaction* FIFOCompactionPicker::PickIntraL0Compaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer) {
  const auto& fifo_opts = mutable_cf_options.compaction_options_fifo;

  if (!fifo_opts.allow_compaction) {
    return nullptr;
  }

  const std::vector<FileMetaData*>& level0_files = vstorage->LevelFiles(0);
  if (level0_files.empty()) {
    return nullptr;
  }

  if (fifo_opts.use_kv_ratio_compaction) {
    if (fifo_opts.max_data_files_size == 0) {
      ROCKS_LOG_BUFFER(
          log_buffer,
          "[%s] FIFO kv-ratio compaction: skipping -- "
          "max_data_files_size is 0, cannot compute target file size. ",
          cf_name.c_str());
    } else if (fifo_opts.max_data_files_size < fifo_opts.max_table_files_size) {
      ROCKS_LOG_BUFFER(log_buffer,
                       "[%s] FIFO kv-ratio compaction: skipping -- "
                       "max_data_files_size (%" PRIu64
                       ") < max_table_files_size "
                       "(%" PRIu64 ").",
                       cf_name.c_str(), fifo_opts.max_data_files_size,
                       fifo_opts.max_table_files_size);
    } else {
      return PickRatioBasedIntraL0Compaction(cf_name, mutable_cf_options,
                                             mutable_db_options, vstorage,
                                             log_buffer);
    }
    ROCKS_LOG_BUFFER(
        log_buffer, "[%s] FIFO: falling back to cost-based intra-L0 compaction",
        cf_name.c_str());
  }

  // Old intra-L0 path: merge small files using PickCostBasedIntraL0Compaction.
  // Minimum files to compact follows level0_file_num_compaction_trigger.
  // Try to prevent same files from being compacted multiple times, which
  // could produce large files that may never TTL-expire. Achieve this by
  // disallowing compactions with files larger than memtable (inflate its
  // size by 10% to account for uncompressed L0 files that may have size
  // slightly greater than memtable size limit).

  CompactionInputFiles comp_inputs;
  size_t max_compact_bytes_per_del_file =
      static_cast<size_t>(MultiplyCheckOverflow(
          static_cast<uint64_t>(mutable_cf_options.write_buffer_size), 1.1));
  if (PickCostBasedIntraL0Compaction(
          level0_files,
          mutable_cf_options
              .level0_file_num_compaction_trigger /* min_files_to_compact */,
          max_compact_bytes_per_del_file,
          mutable_cf_options.max_compaction_bytes, &comp_inputs)) {
    Compaction* c = new Compaction(
        vstorage, ioptions_, mutable_cf_options, mutable_db_options,
        {comp_inputs}, 0, 16 * 1024 * 1024 /* output file size limit */,
        0 /* max compaction bytes, not applicable */, 0 /* output path ID */,
        mutable_cf_options.compression, mutable_cf_options.compression_opts,
        Temperature::kUnknown, 0 /* max_subcompactions */, {},
        /* earliest_snapshot */ std::nullopt,
        /* snapshot_checker */ nullptr, CompactionReason::kFIFOReduceNumFiles,
        /* trim_ts */ "", vstorage->CompactionScore(0),
        /* l0_files_might_overlap */ true);
    return c;
  }

  return nullptr;
}

Compaction* FIFOCompactionPicker::PickRatioBasedIntraL0Compaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer) {
  const auto& fifo_opts = mutable_cf_options.compaction_options_fifo;
  assert(fifo_opts.use_kv_ratio_compaction);
  assert(fifo_opts.max_data_files_size > 0);

  // During migration from level/universal compaction to FIFO, non-L0 levels
  // may still contain files. The ratio-based algorithm only operates on L0,
  // so skip it until PickSizeCompaction has drained all non-L0 levels.
  // Once levels collapse to L0-only, this algorithm will kick in.
  for (int level = 1; level < vstorage->num_levels(); ++level) {
    if (!vstorage->LevelFiles(level).empty()) {
      ROCKS_LOG_BUFFER(log_buffer,
                       "[%s] FIFO kv-ratio compaction: skipping -- non-L0 "
                       "level %d still has %" ROCKSDB_PRIszt
                       " files (migration in progress)",
                       cf_name.c_str(), level,
                       vstorage->LevelFiles(level).size());
      return nullptr;
    }
  }

  if (!level0_compactions_in_progress_.empty()) {
    return nullptr;
  }

  const std::vector<FileMetaData*>& level0_files = vstorage->LevelFiles(0);
  if (mutable_cf_options.level0_file_num_compaction_trigger <= 1) {
    // trigger <= 0 is invalid; trigger == 1 means compact after every flush,
    // which doesn't make sense for tiered merging (the tier boundary loop
    // divides by trigger, so trigger == 1 would cause an infinite loop).
    return nullptr;
  }
  const size_t trigger = static_cast<size_t>(
      mutable_cf_options.level0_file_num_compaction_trigger);
  if (level0_files.size() < trigger) {
    return nullptr;
  }

  // Determine the target compacted file size.
  //
  // When max_compaction_bytes > 0 (explicitly set by user), use it directly
  // as the target. This allows users to override the auto-calculated value.
  //
  // When max_compaction_bytes == 0 (default), auto-calculate from the data
  // capacity and observed SST/blob ratio:
  //   target = max_data_files_size * sst_ratio / trigger
  //
  // This is recomputed on every PickCompaction call. The computation is
  // trivial (sum file sizes + arithmetic) and PickCompaction is only called
  // once per flush or compaction completion, so no caching is needed.
  uint64_t target = 0;
  if (mutable_cf_options.max_compaction_bytes > 0) {
    // User explicitly set max_compaction_bytes -- use it as target
    target = mutable_cf_options.max_compaction_bytes;
  } else {
    // Auto-calculate from capacity and observed SST/blob ratio
    uint64_t total_sst = GetTotalFilesSize(level0_files);
    uint64_t total_blob = vstorage->GetBlobStats().total_file_size;
    uint64_t total_data = total_sst + total_blob;

    if (total_data == 0 || total_sst == 0) {
      return nullptr;
    }

    // Compute sst_ratio (inverse of EstimateTotalDataForSST's proportion):
    // when no blob files exist, sst_ratio is 1.0 and the target becomes
    // max_data_files_size / trigger, which is large. The algorithm will
    // naturally not find small enough files to compact.
    double sst_ratio =
        (total_blob > 0) ? static_cast<double>(total_sst) / total_data : 1.0;

    uint64_t total_sst_at_cap =
        static_cast<uint64_t>(fifo_opts.max_data_files_size * sst_ratio);
    target = total_sst_at_cap / trigger;

    ROCKS_LOG_BUFFER(log_buffer,
                     "[%s] FIFO ratio-based compaction: sst_ratio=%.4f, "
                     "target_file_size=%" PRIu64,
                     cf_name.c_str(), sst_ratio, target);
  }
  if (target == 0) {
    return nullptr;
  }

  // Tiered size-based file selection.
  //
  // Tier boundaries form a geometric sequence descending from target:
  //   ..., target/trigger^2, target/trigger, target
  // For each boundary (smallest first), find contiguous L0 files with
  // size < boundary. If their accumulated bytes >= boundary, merge them.
  // The output (~boundary bytes) advances to the next tier. Files that
  // reach target are "graduated" and never compacted again.
  //
  // Trade-off: write amplification vs L0 file count.
  //
  // Write amp: O(log(target/flush) / log(trigger)) per byte, instead of
  //   O(target / (trigger * flush)) from flat merging. Each byte is
  //   rewritten once per tier crossing.
  //
  // L0 file count: trigger + k * (trigger - 1) at steady state, where
  //   k = ceil(log(target/flush) / log(trigger)). This is higher than
  //   the original trigger target because intermediate tier files
  //   accumulate while waiting for the next tier merge. The trade-off
  //   is explicit: more L0 files in exchange for logarithmic (instead
  //   of linear) write amplification.

  // Build tier boundaries from smallest to largest.
  // Stop at 10KB minimum -- SST files of most workloads are larger than
  // this, so lower boundaries would only waste CPU scanning L0 files.
  // Files smaller than the lowest boundary simply merge at that boundary.
  static constexpr uint64_t kMinTierBoundary = 10 * 1024;  // 10KB
  std::vector<uint64_t> boundaries;
  for (uint64_t b = target; b >= kMinTierBoundary; b /= trigger) {
    boundaries.push_back(b);
  }
  if (boundaries.empty()) {
    // target itself is below kMinTierBoundary -- use target as the
    // sole boundary so we can still compact at the target size.
    boundaries.push_back(target);
  }
  std::reverse(boundaries.begin(), boundaries.end());

  // For each tier boundary (smallest first), scan L0 for mergeable batches.
  // L0 files are stored newest-first; oldest is at the end.
  for (const uint64_t boundary : boundaries) {
    for (size_t scan = level0_files.size(); scan > 0;) {
      // Skip files >= boundary (they belong to higher tiers) or in-progress
      if (level0_files[scan - 1]->fd.file_size >= boundary ||
          level0_files[scan - 1]->being_compacted) {
        --scan;
        continue;
      }

      // Found a file < boundary -- collect contiguous batch
      std::vector<FileMetaData*> batch;
      uint64_t accumulated = 0;
      size_t pos = scan;
      while (pos > 0 && level0_files[pos - 1]->fd.file_size < boundary &&
             !level0_files[pos - 1]->being_compacted) {
        // Don't let output exceed 2x boundary (prevent tier-skipping)
        if (accumulated >= boundary &&
            accumulated + level0_files[pos - 1]->fd.file_size > boundary * 2) {
          break;
        }
        batch.push_back(level0_files[pos - 1]);
        accumulated += level0_files[pos - 1]->fd.file_size;
        --pos;
      }

      // Viable: >= 2 files and accumulated >= boundary
      if (batch.size() >= 2 && accumulated >= boundary) {
        CompactionInputFiles comp_inputs;
        comp_inputs.level = 0;
        comp_inputs.files = std::move(batch);

        ROCKS_LOG_BUFFER(
            log_buffer,
            "[%s] FIFO kv-ratio compaction: picking %" ROCKSDB_PRIszt
            " files (%" PRIu64 " bytes) at tier boundary %" PRIu64
            " for intra-L0 compaction, target=%" PRIu64,
            cf_name.c_str(), comp_inputs.files.size(), accumulated, boundary,
            target);

        Compaction* c = new Compaction(
            vstorage, ioptions_, mutable_cf_options, mutable_db_options,
            {comp_inputs}, 0, boundary /* output file size limit */,
            0 /* max compaction bytes, not applicable */,
            0 /* output path ID */, mutable_cf_options.compression,
            mutable_cf_options.compression_opts, Temperature::kUnknown,
            0 /* max_subcompactions */, {},
            /* earliest_snapshot */ std::nullopt,
            /* snapshot_checker */ nullptr,
            CompactionReason::kFIFOReduceNumFiles,
            /* trim_ts */ "", vstorage->CompactionScore(0),
            /* l0_files_might_overlap */ true);
        return c;
      }

      // This batch wasn't enough -- advance past it
      scan = pos;
    }
  }

  return nullptr;
}

// The full_history_ts_low parameter is used to control bottommost file marking
// for compaction when user-defined timestamps (UDT) are enabled.

// TODO leverage full_history_ts_low for FIFO compaction, by trigggerring
// compaction early for data that has already expired to achieve the goal of TTL
// enforced compliance.
Compaction* FIFOCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& /* existing_snapshots */,
    const SnapshotChecker* /* snapshot_checker */, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer, const std::string& /* full_history_ts_low */,
    bool /* require_max_output_level*/) {
  Compaction* c = nullptr;
  if (mutable_cf_options.ttl > 0) {
    c = PickTTLCompaction(cf_name, mutable_cf_options, mutable_db_options,
                          vstorage, log_buffer);
  }
  if (c == nullptr) {
    c = PickSizeCompaction(cf_name, mutable_cf_options, mutable_db_options,
                           vstorage, log_buffer);
  }
  // Intra-L0 compaction merges small files to reduce file count.
  // It runs after size-based dropping: if PickSizeCompaction dropped files,
  // it returned non-null and we skip this. Otherwise, we try to reduce
  // L0 file count by merging small files together.
  if (c == nullptr) {
    c = PickIntraL0Compaction(cf_name, mutable_cf_options, mutable_db_options,
                              vstorage, log_buffer);
  }
  if (c == nullptr) {
    c = PickTemperatureChangeCompaction(
        cf_name, mutable_cf_options, mutable_db_options, vstorage, log_buffer);
  }
  if (c == nullptr) {
    ROCKS_LOG_BUFFER(log_buffer, "[%s] FIFO compaction: no compaction picked",
                     cf_name.c_str());
  }
  RegisterCompaction(c);
  return c;
}

Compaction* FIFOCompactionPicker::PickCompactionForCompactRange(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    int input_level, int output_level,
    const CompactRangeOptions& /*compact_range_options*/,
    const InternalKey* /*begin*/, const InternalKey* /*end*/,
    InternalKey** compaction_end, bool* /*manual_conflict*/,
    uint64_t /*max_file_num_to_ignore*/, const std::string& /*trim_ts*/,
    const std::string& full_history_ts_low) {
#ifdef NDEBUG
  (void)input_level;
  (void)output_level;
#endif
  assert(input_level == 0);
  assert(output_level == 0);
  *compaction_end = nullptr;
  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL, ioptions_.logger);
  Compaction* c = PickCompaction(
      cf_name, mutable_cf_options, mutable_db_options,
      /*existing_snapshots*/ {}, /*snapshot_checker*/ nullptr, vstorage,
      &log_buffer, full_history_ts_low, /* require_max_output_level */ false);
  log_buffer.FlushBufferToLog();
  return c;
}

}  // namespace ROCKSDB_NAMESPACE
