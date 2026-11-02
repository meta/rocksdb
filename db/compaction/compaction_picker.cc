//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_picker.h"

#include <cinttypes>
#include <limits>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "db/column_family.h"
#include "file/filename.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"
#include "monitoring/statistics_impl.h"
#include "test_util/sync_point.h"
#include "util/random.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

#ifndef NDEBUG
static void AssertCleanCut(const InternalKeyComparator* icmp,
                           VersionStorageInfo* vstorage,
                           CompactionInputFiles* inputs, int level,
                           Logger* logger) {
  const std::vector<FileMetaData*>& level_files = vstorage->LevelFiles(level);
  if (inputs->files.empty() || level_files.empty()) {
    return;
  }

  const Comparator* ucmp = icmp->user_comparator();

  // Find first and last input file indices in level
  int first_input_idx = -1;
  int last_input_idx = -1;
  for (size_t i = 0; i < level_files.size(); i++) {
    if (level_files[i] == inputs->files.front()) {
      first_input_idx = static_cast<int>(i);
    }
    if (level_files[i] == inputs->files.back()) {
      last_input_idx = static_cast<int>(i);
    }
  }

  // Check file before first input
  if (first_input_idx > 0) {
    const FileMetaData* prev_file = level_files[first_input_idx - 1];
    const FileMetaData* first_file = inputs->files.front();
    int cmp = sstableKeyCompare(ucmp, prev_file->largest, first_file->smallest);
    if (cmp == 0) {
      ROCKS_LOG_ERROR(logger,
                      "Clean cut violated: L%d unselected file %" PRIu64
                      " adjacent to first selected file %" PRIu64,
                      level, prev_file->fd.GetNumber(),
                      first_file->fd.GetNumber());
      assert(false);
    }
  }

  // Check file after last input
  if (last_input_idx >= 0 &&
      static_cast<size_t>(last_input_idx) < level_files.size() - 1) {
    const FileMetaData* last_file = inputs->files.back();
    const FileMetaData* next_file = level_files[last_input_idx + 1];
    int cmp = sstableKeyCompare(ucmp, last_file->largest, next_file->smallest);
    if (cmp == 0) {
      ROCKS_LOG_ERROR(logger,
                      "Clean cut violated: L%d unselected file %" PRIu64
                      " adjacent to last selected file %" PRIu64,
                      level, next_file->fd.GetNumber(),
                      last_file->fd.GetNumber());
      assert(false);
    }
  }
}
#endif  // NDEBUG

bool PickCostBasedIntraL0Compaction(
    const std::vector<FileMetaData*>& level_files, size_t min_files_to_compact,
    uint64_t max_compact_bytes_per_del_file, uint64_t max_compaction_bytes,
    CompactionInputFiles* comp_inputs) {
  TEST_SYNC_POINT("PickCostBasedIntraL0Compaction");

  size_t start = 0;

  if (level_files.size() == 0 || level_files[start]->being_compacted) {
    return false;
  }

  size_t compact_bytes = static_cast<size_t>(level_files[start]->fd.file_size);
  size_t compact_bytes_per_del_file = std::numeric_limits<size_t>::max();
  // Compaction range will be [start, limit).
  size_t limit;
  // Pull in files until the amount of compaction work per deleted file begins
  // increasing or maximum total compaction size is reached.
  size_t new_compact_bytes_per_del_file = 0;
  for (limit = start + 1; limit < level_files.size(); ++limit) {
    compact_bytes += static_cast<size_t>(level_files[limit]->fd.file_size);
    new_compact_bytes_per_del_file = compact_bytes / (limit - start);
    if (level_files[limit]->being_compacted ||
        new_compact_bytes_per_del_file > compact_bytes_per_del_file ||
        compact_bytes > max_compaction_bytes) {
      break;
    }
    compact_bytes_per_del_file = new_compact_bytes_per_del_file;
  }

  if ((limit - start) >= min_files_to_compact &&
      compact_bytes_per_del_file < max_compact_bytes_per_del_file) {
    assert(comp_inputs != nullptr);
    comp_inputs->level = 0;
    for (size_t i = start; i < limit; ++i) {
      comp_inputs->files.push_back(level_files[i]);
    }
    return true;
  }
  return false;
}

// Determine compression type, based on user options, level of the output
// file and whether compression is disabled.
// If enable_compression is false, then compression is always disabled no
// matter what the values of the other two parameters are.
// Otherwise, the compression type is determined based on options and level.
CompressionType GetCompressionType(const VersionStorageInfo* vstorage,
                                   const MutableCFOptions& mutable_cf_options,
                                   int level, int base_level,
                                   const bool enable_compression) {
  if (!enable_compression) {
    // disable compression
    return kNoCompression;
  }

  // If bottommost_compression is set and we are compacting to the
  // bottommost level then we should use it.
  if (mutable_cf_options.bottommost_compression != kDisableCompressionOption &&
      level >= (vstorage->num_non_empty_levels() - 1)) {
    return mutable_cf_options.bottommost_compression;
  }
  // If the user has specified a different compression level for each level,
  // then pick the compression for that level.
  if (!mutable_cf_options.compression_per_level.empty()) {
    assert(level == 0 || level >= base_level);
    int idx = (level == 0) ? 0 : level - base_level + 1;

    const int n =
        static_cast<int>(mutable_cf_options.compression_per_level.size()) - 1;
    // It is possible for level_ to be -1; in that case, we use level
    // 0's compression.  This occurs mostly in backwards compatibility
    // situations when the builder doesn't know what level the file
    // belongs to.  Likewise, if level is beyond the end of the
    // specified compression levels, use the last value.
    return mutable_cf_options
        .compression_per_level[std::max(0, std::min(idx, n))];
  } else {
    return mutable_cf_options.compression;
  }
}

CompressionOptions GetCompressionOptions(const MutableCFOptions& cf_options,
                                         const VersionStorageInfo* vstorage,
                                         int level,
                                         const bool enable_compression) {
  if (!enable_compression) {
    return cf_options.compression_opts;
  }
  // If bottommost_compression_opts is enabled and we are compacting to the
  // bottommost level then we should use the specified compression options.
  if (level >= (vstorage->num_non_empty_levels() - 1) &&
      cf_options.bottommost_compression_opts.enabled) {
    return cf_options.bottommost_compression_opts;
  }
  return cf_options.compression_opts;
}

CompactionPicker::CompactionPicker(const ImmutableOptions& ioptions,
                                   const InternalKeyComparator* icmp)
    : ioptions_(ioptions), icmp_(icmp) {}

CompactionPicker::~CompactionPicker() = default;

// Delete this compaction from the list of running compactions.
void CompactionPicker::ReleaseCompactionFiles(Compaction* c,
                                              const Status& status) {
  UnregisterCompaction(c);
  if (!status.ok()) {
    c->ResetNextCompactionIndex();
  }
}

void CompactionPicker::GetRange(const CompactionInputFiles& inputs,
                                InternalKey* smallest,
                                InternalKey* largest) const {
  const int level = inputs.level;
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();

  if (level == 0) {
    for (size_t i = 0; i < inputs.size(); i++) {
      FileMetaData* f = inputs[i];
      if (i == 0) {
        *smallest = f->smallest;
        *largest = f->largest;
      } else {
        if (icmp_->Compare(f->smallest, *smallest) < 0) {
          *smallest = f->smallest;
        }
        if (icmp_->Compare(f->largest, *largest) > 0) {
          *largest = f->largest;
        }
      }
    }
  } else {
    *smallest = inputs[0]->smallest;
    *largest = inputs[inputs.size() - 1]->largest;
  }
}

void CompactionPicker::GetRange(const CompactionInputFiles& inputs1,
                                const CompactionInputFiles& inputs2,
                                InternalKey* smallest,
                                InternalKey* largest) const {
  assert(!inputs1.empty() || !inputs2.empty());
  if (inputs1.empty()) {
    GetRange(inputs2, smallest, largest);
  } else if (inputs2.empty()) {
    GetRange(inputs1, smallest, largest);
  } else {
    InternalKey smallest1;
    InternalKey smallest2;
    InternalKey largest1;
    InternalKey largest2;
    GetRange(inputs1, &smallest1, &largest1);
    GetRange(inputs2, &smallest2, &largest2);
    *smallest =
        icmp_->Compare(smallest1, smallest2) < 0 ? smallest1 : smallest2;
    *largest = icmp_->Compare(largest1, largest2) < 0 ? largest2 : largest1;
  }
}

void CompactionPicker::GetRange(const std::vector<CompactionInputFiles>& inputs,
                                InternalKey* smallest, InternalKey* largest,
                                int exclude_level) const {
  InternalKey current_smallest;
  InternalKey current_largest;
  bool initialized = false;
  for (const auto& in : inputs) {
    if (in.empty() || in.level == exclude_level) {
      continue;
    }
    GetRange(in, &current_smallest, &current_largest);
    if (!initialized) {
      *smallest = current_smallest;
      *largest = current_largest;
      initialized = true;
    } else {
      if (icmp_->Compare(current_smallest, *smallest) < 0) {
        *smallest = current_smallest;
      }
      if (icmp_->Compare(current_largest, *largest) > 0) {
        *largest = current_largest;
      }
    }
  }
  assert(initialized);
}

bool CompactionPicker::ExpandInputsToCleanCut(const std::string& /*cf_name*/,
                                              VersionStorageInfo* vstorage,
                                              CompactionInputFiles* inputs,
                                              InternalKey** next_smallest) {
  // This isn't good compaction
  assert(!inputs->empty());

  const int level = inputs->level;
  // GetOverlappingInputs will always do the right thing for level-0.
  // So we don't need to do any expansion if level == 0.
  if (level == 0) {
    return true;
  }

  InternalKey smallest;
  InternalKey largest;

  // Keep expanding inputs until we are sure that there is a "clean cut"
  // boundary between the files in input and the surrounding files.
  // This will ensure that no parts of a key are lost during compaction.
  int hint_index = -1;
  size_t old_size;
  do {
    old_size = inputs->size();
    GetRange(*inputs, &smallest, &largest);
    inputs->clear();
    vstorage->GetOverlappingInputs(level, &smallest, &largest, &inputs->files,
                                   hint_index, &hint_index, true, nullptr,
                                   next_smallest);
  } while (inputs->size() > old_size);

  // we started off with inputs non-empty and the previous loop only grew
  // inputs. thus, inputs should be non-empty here
  assert(!inputs->empty());

#ifndef NDEBUG
  AssertCleanCut(icmp_, vstorage, inputs, level, ioptions_.logger);
#endif  // NDEBUG

  // If, after the expansion, there are files that are already under
  // compaction, then we must drop/cancel this compaction.
  if (AreFilesInCompaction(inputs->files)) {
    return false;
  }
  return true;
}

bool CompactionPicker::RangeOverlapWithCompaction(
    const Slice& smallest_user_key, const Slice& largest_user_key, int level,
    bool range_limit_exclusive) const {
  const Comparator* ucmp = icmp_->user_comparator();
  for (Compaction* c : compactions_in_progress_) {
    const int largest_cmp = ucmp->CompareWithoutTimestamp(
        largest_user_key, c->GetSmallestUserKey());
    if (c->output_level() == level &&
        ucmp->CompareWithoutTimestamp(smallest_user_key,
                                      c->GetLargestUserKey()) <= 0 &&
        (range_limit_exclusive ? largest_cmp > 0 : largest_cmp >= 0)) {
      // Overlap
      return true;
    }
    if (c->SupportsPerKeyPlacement()) {
      if (c->OverlapProximalLevelOutputRange(
              smallest_user_key, largest_user_key, range_limit_exclusive)) {
        return true;
      }
    }
  }
  // Did not overlap with any running compaction in level `level`
  return false;
}

bool CompactionPicker::FilesRangeOverlapWithCompaction(
    const std::vector<CompactionInputFiles>& inputs, int level,
    int proximal_level) const {
  bool is_empty = true;
  for (auto& in : inputs) {
    if (!in.empty()) {
      is_empty = false;
      break;
    }
  }
  if (is_empty) {
    // No files in inputs
    return false;
  }

  // TODO: Intra L0 compactions can have the ranges overlapped, but the input
  //  files cannot be overlapped in the order of L0 files.
  InternalKey smallest;
  InternalKey largest;
  GetRange(inputs, &smallest, &largest, Compaction::kInvalidLevel);
  if (proximal_level != Compaction::kInvalidLevel) {
    if (ioptions_.compaction_style == kCompactionStyleUniversal) {
      if (RangeOverlapWithCompaction(smallest.user_key(), largest.user_key(),
                                     proximal_level,
                                     /*range_limit_exclusive=*/false)) {
        return true;
      }
    } else {
      InternalKey proximal_smallest;
      InternalKey proximal_largest;
      GetRange(inputs, &proximal_smallest, &proximal_largest, level);
      if (RangeOverlapWithCompaction(proximal_smallest.user_key(),
                                     proximal_largest.user_key(),
                                     proximal_level,
                                     /*range_limit_exclusive=*/false)) {
        return true;
      }
    }
  }

  return RangeOverlapWithCompaction(smallest.user_key(), largest.user_key(),
                                    level,
                                    /*range_limit_exclusive=*/false);
}

// Returns true if any one of specified files are being compacted
bool CompactionPicker::AreFilesInCompaction(
    const std::vector<FileMetaData*>& files) {
  for (size_t i = 0; i < files.size(); i++) {
    if (files[i]->being_compacted) {
      return true;
    }
  }
  return false;
}

Compaction* CompactionPicker::PickCompactionForCompactFiles(
    const CompactionOptions& compact_options,
    const std::vector<CompactionInputFiles>& input_files, int output_level,
    VersionStorageInfo* vstorage, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, uint32_t output_path_id,
    std::optional<SequenceNumber> earliest_snapshot,
    const SnapshotChecker* snapshot_checker) {
#ifndef NDEBUG
  assert(input_files.size());
  // This compaction output should not overlap with a running compaction as
  // `SanitizeAndConvertCompactionInputFiles` should've checked earlier and db
  // mutex shouldn't have been released since.
  int start_level = Compaction::kInvalidLevel;
  for (const auto& in : input_files) {
    // input_files should already be sorted by level
    if (!in.empty()) {
      start_level = in.level;
      break;
    }
  }
  assert(output_level == 0 || !FilesRangeOverlapWithCompaction(
                                  input_files, output_level,
                                  Compaction::EvaluateProximalLevel(
                                      vstorage, mutable_cf_options, ioptions_,
                                      start_level, output_level)));
#endif /* !NDEBUG */

  CompressionType compression_type;
  if (compact_options.compression == kDisableCompressionOption) {
    int base_level;
    if (ioptions_.compaction_style == kCompactionStyleLevel) {
      base_level = vstorage->base_level();
    } else {
      base_level = 1;
    }
    compression_type = GetCompressionType(vstorage, mutable_cf_options,
                                          output_level, base_level);
  } else {
    // TODO(ajkr): `CompactionOptions` offers configurable `CompressionType`
    // without configurable `CompressionOptions`, which is inconsistent.
    compression_type = compact_options.compression;
  }

  auto c = new Compaction(
      vstorage, ioptions_, mutable_cf_options, mutable_db_options, input_files,
      output_level, compact_options.output_file_size_limit,
      mutable_cf_options.max_compaction_bytes, output_path_id, compression_type,
      GetCompressionOptions(mutable_cf_options, vstorage, output_level),
      compact_options.output_temperature_override,
      compact_options.max_subcompactions,
      /* grandparents */ {}, earliest_snapshot, snapshot_checker,
      CompactionReason::kManualCompaction);
  RegisterCompaction(c);
  return c;
}

Status CompactionPicker::GetCompactionInputsFromFileNumbers(
    std::vector<CompactionInputFiles>* input_files,
    std::unordered_set<uint64_t>* input_set, const VersionStorageInfo* vstorage,
    const CompactionOptions& /*compact_options*/) const {
  if (input_set->size() == 0U) {
    return Status::InvalidArgument(
        "Compaction must include at least one file.");
  }
  assert(input_files);

  std::vector<CompactionInputFiles> matched_input_files;
  matched_input_files.resize(vstorage->num_levels());
  int first_non_empty_level = -1;
  int last_non_empty_level = -1;
  // TODO(yhchiang): use a lazy-initialized mapping from
  //                 file_number to FileMetaData in Version.
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    for (auto file : vstorage->LevelFiles(level)) {
      auto iter = input_set->find(file->fd.GetNumber());
      if (iter != input_set->end()) {
        matched_input_files[level].files.push_back(file);
        input_set->erase(iter);
        last_non_empty_level = level;
        if (first_non_empty_level == -1) {
          first_non_empty_level = level;
        }
      }
    }
  }

  if (!input_set->empty()) {
    std::string message(
        "Cannot find matched SST files for the following file numbers:");
    for (auto fn : *input_set) {
      message += " ";
      message += std::to_string(fn);
    }
    return Status::InvalidArgument(message);
  }

  for (int level = first_non_empty_level; level <= last_non_empty_level;
       ++level) {
    matched_input_files[level].level = level;
    input_files->emplace_back(std::move(matched_input_files[level]));
  }

  return Status::OK();
}

// Returns true if any one of the parent files are being compacted
bool CompactionPicker::IsRangeInCompaction(VersionStorageInfo* vstorage,
                                           const InternalKey* smallest,
                                           const InternalKey* largest,
                                           int level, int* level_index) {
  std::vector<FileMetaData*> inputs;
  assert(level < NumberLevels());

  vstorage->GetOverlappingInputs(level, smallest, largest, &inputs,
                                 level_index ? *level_index : 0, level_index);
  return AreFilesInCompaction(inputs);
}

// Populates the set of inputs of all other levels that overlap with the
// start level.
// Now we assume all levels except start level and output level are empty.
// Will also attempt to expand "start level" if that doesn't expand
// "output level" or cause "level" to include a file for compaction that has an
// overlapping user-key with another file.
// REQUIRES: input_level and output_level are different
// REQUIRES: inputs->empty() == false
// Returns false if files on parent level are currently in compaction, which
// means that we can't compact them
bool CompactionPicker::SetupOtherInputs(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, CompactionInputFiles* inputs,
    CompactionInputFiles* output_level_inputs, int* parent_index,
    int base_index, bool only_expand_towards_right,
    const FileMetaData* starting_l0_file) {
  assert(!inputs->empty());
  assert(output_level_inputs->empty());
  const int input_level = inputs->level;
  const int output_level = output_level_inputs->level;
  if (input_level == output_level) {
    // no possibility of conflict
    return true;
  }

  // For now, we only support merging two levels, start level and output level.
  // We need to assert other levels are empty.
  for (int l = input_level + 1; l < output_level; l++) {
    assert(vstorage->NumLevelFiles(l) == 0);
  }

  InternalKey smallest;
  InternalKey largest;

  // Get the range one last time.
  GetRange(*inputs, &smallest, &largest);

  // Populate the set of next-level files (inputs_GetOutputLevelInputs()) to
  // include in compaction
  vstorage->GetOverlappingInputs(output_level, &smallest, &largest,
                                 &output_level_inputs->files, *parent_index,
                                 parent_index);
  if (AreFilesInCompaction(output_level_inputs->files)) {
    return false;
  }
  if (!output_level_inputs->empty()) {
    if (!ExpandInputsToCleanCut(cf_name, vstorage, output_level_inputs)) {
      return false;
    }
  }

  // See if we can further grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up. We also choose NOT
  // to expand if this would cause "level" to include some entries for some
  // user key, while excluding other entries for the same user key. This
  // can happen when one user key spans multiple files.
  if (!output_level_inputs->empty()) {
    const uint64_t output_level_inputs_size =
        TotalFileSize(output_level_inputs->files);
    const uint64_t inputs_size = TotalFileSize(inputs->files);
    bool expand_inputs = false;

    CompactionInputFiles expanded_inputs;
    expanded_inputs.level = input_level;
    // Get closed interval of output level
    InternalKey all_start;
    InternalKey all_limit;
    GetRange(*inputs, *output_level_inputs, &all_start, &all_limit);
    bool try_overlapping_inputs = true;
    if (only_expand_towards_right) {
      // Round-robin compaction only allows expansion towards the larger side.
      vstorage->GetOverlappingInputs(input_level, &smallest, &all_limit,
                                     &expanded_inputs.files, base_index,
                                     nullptr, true, starting_l0_file);
    } else {
      vstorage->GetOverlappingInputs(input_level, &all_start, &all_limit,
                                     &expanded_inputs.files, base_index,
                                     nullptr, true, starting_l0_file);
    }
    uint64_t expanded_inputs_size = TotalFileSize(expanded_inputs.files);
    if (!ExpandInputsToCleanCut(cf_name, vstorage, &expanded_inputs)) {
      try_overlapping_inputs = false;
    }
    // It helps to reduce write amp and avoid a further separate compaction
    // to include more input level files without expanding output level files.
    // So we apply a softer limit. We still need a limit to avoid overly large
    // compactions and potential high space amp spikes.
    const uint64_t limit =
        MultiplyCheckOverflow(mutable_cf_options.max_compaction_bytes, 2.0);
    if (try_overlapping_inputs && expanded_inputs.size() > inputs->size() &&
        !AreFilesInCompaction(expanded_inputs.files) &&
        output_level_inputs_size + expanded_inputs_size < limit) {
      InternalKey new_start;
      InternalKey new_limit;
      GetRange(expanded_inputs, &new_start, &new_limit);
      CompactionInputFiles expanded_output_level_inputs;
      expanded_output_level_inputs.level = output_level;
      vstorage->GetOverlappingInputs(output_level, &new_start, &new_limit,
                                     &expanded_output_level_inputs.files,
                                     *parent_index, parent_index);
      assert(!expanded_output_level_inputs.empty());
      if (!AreFilesInCompaction(expanded_output_level_inputs.files) &&
          ExpandInputsToCleanCut(cf_name, vstorage,
                                 &expanded_output_level_inputs) &&
          expanded_output_level_inputs.size() == output_level_inputs->size()) {
        expand_inputs = true;
      }
    }
    if (!expand_inputs) {
      vstorage->GetCleanInputsWithinInterval(input_level, &all_start,
                                             &all_limit, &expanded_inputs.files,
                                             base_index, nullptr);
      expanded_inputs_size = TotalFileSize(expanded_inputs.files);
      if (expanded_inputs.size() > inputs->size() &&
          !AreFilesInCompaction(expanded_inputs.files) &&
          (output_level_inputs_size + expanded_inputs_size) < limit) {
        expand_inputs = true;
      }
    }
    if (expand_inputs) {
      ROCKS_LOG_INFO(ioptions_.logger,
                     "[%s] Expanding@%d %" ROCKSDB_PRIszt "+%" ROCKSDB_PRIszt
                     "(%" PRIu64 "+%" PRIu64 " bytes) to %" ROCKSDB_PRIszt
                     "+%" ROCKSDB_PRIszt " (%" PRIu64 "+%" PRIu64 " bytes)\n",
                     cf_name.c_str(), input_level, inputs->size(),
                     output_level_inputs->size(), inputs_size,
                     output_level_inputs_size, expanded_inputs.size(),
                     output_level_inputs->size(), expanded_inputs_size,
                     output_level_inputs_size);
      inputs->files = expanded_inputs.files;
    }
  } else {
    // Likely to be trivial move. Expand files if they are still trivial moves,
    // but limit to mutable_cf_options.max_compaction_bytes or 8 files so that
    // we don't create too much compaction pressure for the next level.
  }
  return true;
}

void CompactionPicker::GetGrandparents(
    VersionStorageInfo* vstorage, const CompactionInputFiles& inputs,
    const CompactionInputFiles& output_level_inputs,
    std::vector<FileMetaData*>* grandparents) {
  InternalKey start;
  InternalKey limit;
  GetRange(inputs, output_level_inputs, &start, &limit);
  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2 or the first
  // level after that has overlapping files)
  for (int level = output_level_inputs.level + 1; level < NumberLevels();
       level++) {
    vstorage->GetOverlappingInputs(level, &start, &limit, grandparents);
    if (!grandparents->empty()) {
      break;
    }
  }
}

Compaction* CompactionPicker::PickCompactionForCompactRange(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    int input_level, int output_level,
    const CompactRangeOptions& compact_range_options, const InternalKey* begin,
    const InternalKey* end, InternalKey** compaction_end, bool* manual_conflict,
    uint64_t max_file_num_to_ignore, const std::string& trim_ts,
    const std::string& full_history_ts_low) {
  // CompactionPickerFIFO has its own implementation of compact range
  assert(ioptions_.compaction_style != kCompactionStyleFIFO);

  if (input_level == ColumnFamilyData::kCompactAllLevels) {
    assert(ioptions_.compaction_style == kCompactionStyleUniversal);

    // Universal compaction with more than one level always compacts all the
    // files together to the last level.
    assert(vstorage->num_levels() > 1);
    int max_output_level = vstorage->MaxOutputLevel(
        ioptions_.cf_allow_ingest_behind || ioptions_.allow_ingest_behind);
    // DBImpl::CompactRange() set output level to be the last level
    assert(output_level == max_output_level);
    // DBImpl::RunManualCompaction will make full range for universal compaction
    assert(begin == nullptr);
    assert(end == nullptr);
    *compaction_end = nullptr;

    int start_level = 0;
    for (; start_level <= max_output_level &&
           vstorage->NumLevelFiles(start_level) == 0;
         start_level++) {
    }
    if (start_level > max_output_level) {
      return nullptr;
    }

    if ((start_level == 0) && (!level0_compactions_in_progress_.empty())) {
      *manual_conflict = true;
      // Only one level 0 compaction allowed
      return nullptr;
    }

    std::vector<CompactionInputFiles> inputs(max_output_level + 1 -
                                             start_level);
    for (int level = start_level; level <= max_output_level; level++) {
      inputs[level - start_level].level = level;
      auto& files = inputs[level - start_level].files;
      for (FileMetaData* f : vstorage->LevelFiles(level)) {
        files.push_back(f);
      }
      if (AreFilesInCompaction(files)) {
        *manual_conflict = true;
        return nullptr;
      }
    }

    // 2 non-exclusive manual compactions could run at the same time producing
    // overlaping outputs in the same level.
    if (FilesRangeOverlapWithCompaction(
            inputs, output_level,
            Compaction::EvaluateProximalLevel(vstorage, mutable_cf_options,
                                              ioptions_, start_level,
                                              output_level))) {
      // This compaction output could potentially conflict with the output
      // of a currently running compaction, we cannot run it.
      *manual_conflict = true;
      return nullptr;
    }

    Compaction* c = new Compaction(
        vstorage, ioptions_, mutable_cf_options, mutable_db_options,
        std::move(inputs), output_level,
        MaxFileSizeForLevel(mutable_cf_options, output_level,
                            ioptions_.compaction_style),
        /* max_compaction_bytes */ LLONG_MAX,
        compact_range_options.target_path_id,
        GetCompressionType(vstorage, mutable_cf_options, output_level, 1),
        GetCompressionOptions(mutable_cf_options, vstorage, output_level),
        Temperature::kUnknown, compact_range_options.max_subcompactions,
        /* grandparents */ {}, /* earliest_snapshot */ std::nullopt,
        /* snapshot_checker */ nullptr, CompactionReason::kManualCompaction,
        trim_ts, /* score */ -1,
        /* l0_files_might_overlap */ true,
        compact_range_options.blob_garbage_collection_policy,
        compact_range_options.blob_garbage_collection_age_cutoff);

    RegisterCompaction(c);
    vstorage->ComputeCompactionScore(ioptions_, mutable_cf_options,
                                     full_history_ts_low);
    return c;
  }

  CompactionInputFiles inputs;
  inputs.level = input_level;
  bool covering_the_whole_range = true;

  // All files are 'overlapping' in universal style compaction.
  // We have to compact the entire range in one shot.
  if (ioptions_.compaction_style == kCompactionStyleUniversal) {
    begin = nullptr;
    end = nullptr;
  }

  vstorage->GetOverlappingInputs(input_level, begin, end, &inputs.files);
  if (inputs.empty()) {
    return nullptr;
  }

  if ((input_level == 0) && (!level0_compactions_in_progress_.empty())) {
    // Only one level 0 compaction allowed
    TEST_SYNC_POINT("CompactionPicker::CompactRange:Conflict");
    *manual_conflict = true;
    return nullptr;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  if (input_level > 0) {
    const uint64_t limit = mutable_cf_options.max_compaction_bytes;
    int hint_index = -1;
    assert(!inputs.empty());
    // Always include first file for progress.
    uint64_t input_level_total = inputs[0]->fd.GetFileSize();
    InternalKey* smallest = &(inputs[0]->smallest);
    InternalKey* largest = nullptr;
    for (size_t i = 1; i < inputs.size(); ++i) {
      // Consider whether to include inputs[i]
      largest = &inputs[i]->largest;

      uint64_t input_file_size = inputs[i]->fd.GetFileSize();
      uint64_t output_level_total = 0;
      if (output_level < vstorage->num_non_empty_levels()) {
        std::vector<FileMetaData*> files;
        vstorage->GetOverlappingInputsRangeBinarySearch(
            output_level, smallest, largest, &files, hint_index, &hint_index);
        for (const auto& file : files) {
          output_level_total += file->fd.GetFileSize();
        }
      }

      input_level_total += input_file_size;

      if (input_level_total + output_level_total > limit) {
        // To ensure compaction size is <= limit, leave out inputs from
        // index i onwards.
        covering_the_whole_range = false;
        inputs.files.resize(i);
        break;
      }
    }
  }

  assert(compact_range_options.target_path_id <
         static_cast<uint32_t>(ioptions_.cf_paths.size()));

  // for BOTTOM LEVEL compaction only, use max_file_num_to_ignore to filter out
  // files that are created during the current compaction.
  if ((compact_range_options.bottommost_level_compaction ==
           BottommostLevelCompaction::kForceOptimized ||
       compact_range_options.bottommost_level_compaction ==
           BottommostLevelCompaction::kIfHaveCompactionFilter) &&
      max_file_num_to_ignore != std::numeric_limits<uint64_t>::max()) {
    assert(input_level == output_level);
    // inputs_shrunk holds a continuous subset of input files which were all
    // created before the current manual compaction
    std::vector<FileMetaData*> inputs_shrunk;
    size_t skip_input_index = inputs.size();
    for (size_t i = 0; i < inputs.size(); ++i) {
      if (inputs[i]->fd.GetNumber() < max_file_num_to_ignore) {
        inputs_shrunk.push_back(inputs[i]);
      } else if (!inputs_shrunk.empty()) {
        // inputs[i] was created during the current manual compaction and
        // need to be skipped
        skip_input_index = i;
        break;
      }
    }
    if (inputs_shrunk.empty()) {
      return nullptr;
    }
    // set covering_the_whole_range to false if there is any file that need to
    // be compacted in the range of inputs[skip_input_index+1, inputs.size()).
    // This must check the original inputs, so it runs before the swap below.
    for (size_t i = skip_input_index + 1; i < inputs.size(); ++i) {
      if (inputs[i]->fd.GetNumber() < max_file_num_to_ignore) {
        covering_the_whole_range = false;
      }
    }
    if (inputs.size() != inputs_shrunk.size()) {
      inputs.files.swap(inputs_shrunk);
    }
  }

  InternalKey key_storage;
  InternalKey* next_smallest = &key_storage;
  if (ExpandInputsToCleanCut(cf_name, vstorage, &inputs, &next_smallest) ==
      false) {
    // manual compaction is now multi-threaded, so it can
    // happen that ExpandWhileOverlapping fails
    // we handle it higher in RunManualCompaction
    *manual_conflict = true;
    return nullptr;
  }

  if (covering_the_whole_range || !next_smallest) {
    *compaction_end = nullptr;
  } else {
    **compaction_end = *next_smallest;
  }

  CompactionInputFiles output_level_inputs;
  if (output_level == ColumnFamilyData::kCompactToBaseLevel) {
    assert(input_level == 0);
    output_level = vstorage->base_level();
    assert(output_level > 0);
  }
  for (int i = input_level + 1; i < output_level; i++) {
    assert(vstorage->NumLevelFiles(i) == 0);
  }
  output_level_inputs.level = output_level;
  if (input_level != output_level) {
    int parent_index = -1;
    if (!SetupOtherInputs(cf_name, mutable_cf_options, vstorage, &inputs,
                          &output_level_inputs, &parent_index, -1)) {
      // manual compaction is now multi-threaded, so it can
      // happen that SetupOtherInputs fails
      // we handle it higher in RunManualCompaction
      *manual_conflict = true;
      return nullptr;
    }
  }

  std::vector<CompactionInputFiles> compaction_inputs({inputs});
  if (!output_level_inputs.empty()) {
    compaction_inputs.push_back(output_level_inputs);
  }
  for (size_t i = 0; i < compaction_inputs.size(); i++) {
    if (AreFilesInCompaction(compaction_inputs[i].files)) {
      *manual_conflict = true;
      return nullptr;
    }
  }

  // 2 non-exclusive manual compactions could run at the same time producing
  // overlaping outputs in the same level.
  if (FilesRangeOverlapWithCompaction(
          compaction_inputs, output_level,
          Compaction::EvaluateProximalLevel(vstorage, mutable_cf_options,
                                            ioptions_, input_level,
                                            output_level))) {
    // This compaction output could potentially conflict with the output
    // of a currently running compaction, we cannot run it.
    *manual_conflict = true;
    return nullptr;
  }

  std::vector<FileMetaData*> grandparents;
  GetGrandparents(vstorage, inputs, output_level_inputs, &grandparents);
  Compaction* compaction = new Compaction(
      vstorage, ioptions_, mutable_cf_options, mutable_db_options,
      std::move(compaction_inputs), output_level,
      MaxFileSizeForLevel(mutable_cf_options, output_level,
                          ioptions_.compaction_style, vstorage->base_level(),
                          ioptions_.level_compaction_dynamic_level_bytes),
      mutable_cf_options.max_compaction_bytes,
      compact_range_options.target_path_id,
      GetCompressionType(vstorage, mutable_cf_options, output_level,
                         vstorage->base_level()),
      GetCompressionOptions(mutable_cf_options, vstorage, output_level),
      Temperature::kUnknown, compact_range_options.max_subcompactions,
      std::move(grandparents),
      /* earliest_snapshot */ std::nullopt, /* snapshot_checker */ nullptr,
      CompactionReason::kManualCompaction, trim_ts, /* score */ -1,
      /* l0_files_might_overlap */ true,
      compact_range_options.blob_garbage_collection_policy,
      compact_range_options.blob_garbage_collection_age_cutoff);

  TEST_SYNC_POINT_CALLBACK("CompactionPicker::CompactRange:Return", compaction);
  RegisterCompaction(compaction);

  // Creating a compaction influences the compaction score because the score
  // takes running compactions into account (by skipping files that are already
  // being compacted). Since we just changed compaction score, we recalculate it
  // here
  vstorage->ComputeCompactionScore(ioptions_, mutable_cf_options,
                                   full_history_ts_low);

  return compaction;
}

namespace {
// Test whether two files have overlapping key-ranges.
bool HaveOverlappingKeyRanges(const Comparator* c, const SstFileMetaData& a,
                              const SstFileMetaData& b) {
  if (c->CompareWithoutTimestamp(a.smallestkey, b.smallestkey) >= 0) {
    if (c->CompareWithoutTimestamp(a.smallestkey, b.largestkey) <= 0) {
      // b.smallestkey <= a.smallestkey <= b.largestkey
      return true;
    }
  } else if (c->CompareWithoutTimestamp(a.largestkey, b.smallestkey) >= 0) {
    // a.smallestkey < b.smallestkey <= a.largestkey
    return true;
  }
  if (c->CompareWithoutTimestamp(a.largestkey, b.largestkey) <= 0) {
    if (c->CompareWithoutTimestamp(a.largestkey, b.smallestkey) >= 0) {
      // b.smallestkey <= a.largestkey <= b.largestkey
      return true;
    }
  } else if (c->CompareWithoutTimestamp(a.smallestkey, b.largestkey) <= 0) {
    // a.smallestkey <= b.largestkey < a.largestkey
    return true;
  }
  return false;
}
}  // namespace

Status CompactionPicker::SanitizeCompactionInputFilesForAllLevels(
    std::unordered_set<uint64_t>* input_files,
    const ColumnFamilyMetaData& cf_meta, const int output_level) const {
  auto& levels = cf_meta.levels;
  auto comparator = icmp_->user_comparator();

  // TODO(yhchiang): add is_adjustable to CompactionOptions

  // the smallest and largest key of the current compaction input
  std::string smallestkey;
  std::string largestkey;
  // a flag for initializing smallest and largest key
  bool is_first = false;
  const int kNotFound = -1;

  // For each level, it does the following things:
  // 1. Find the first and the last compaction input files
  //    in the current level.
  // 2. Include all files between the first and the last
  //    compaction input files.
  // 3. Update the compaction key-range.
  // 4. For all remaining levels, include files that have
  //    overlapping key-range with the compaction key-range.
  for (int l = 0; l <= output_level; ++l) {
    auto& current_files = levels[l].files;
    int first_included = static_cast<int>(current_files.size());
    int last_included = kNotFound;

    // identify the first and the last compaction input files
    // in the current level.
    for (size_t f = 0; f < current_files.size(); ++f) {
      const uint64_t file_number = TableFileNameToNumber(current_files[f].name);
      if (input_files->find(file_number) == input_files->end()) {
        continue;
      }
      first_included = std::min(first_included, static_cast<int>(f));
      last_included = std::max(last_included, static_cast<int>(f));
      if (is_first == false) {
        smallestkey = current_files[f].smallestkey;
        largestkey = current_files[f].largestkey;
        is_first = true;
      }
    }
    if (last_included == kNotFound) {
      continue;
    }

    if (l != 0) {
      // expand the compaction input of the current level if it
      // has overlapping key-range with other non-compaction input
      // files in the same level.
      while (first_included > 0) {
        if (comparator->CompareWithoutTimestamp(
                current_files[first_included - 1].largestkey,
                current_files[first_included].smallestkey) < 0) {
          break;
        }
        first_included--;
      }

      while (last_included < static_cast<int>(current_files.size()) - 1) {
        if (comparator->CompareWithoutTimestamp(
                current_files[last_included + 1].smallestkey,
                current_files[last_included].largestkey) > 0) {
          break;
        }
        last_included++;
      }
    } else if (output_level > 0) {
      last_included = static_cast<int>(current_files.size() - 1);
    }

    // include all files between the first and the last compaction input files.
    for (int f = first_included; f <= last_included; ++f) {
      if (current_files[f].being_compacted) {
        return Status::Aborted("Necessary compaction input file " +
                               current_files[f].name +
                               " is currently being compacted.");
      }

      input_files->insert(TableFileNameToNumber(current_files[f].name));
    }

    // update smallest and largest key
    if (l == 0) {
      for (int f = first_included; f <= last_included; ++f) {
        if (comparator->CompareWithoutTimestamp(
                smallestkey, current_files[f].smallestkey) > 0) {
          smallestkey = current_files[f].smallestkey;
        }
        if (comparator->CompareWithoutTimestamp(
                largestkey, current_files[f].largestkey) < 0) {
          largestkey = current_files[f].largestkey;
        }
      }
    } else {
      if (comparator->CompareWithoutTimestamp(
              smallestkey, current_files[first_included].smallestkey) > 0) {
        smallestkey = current_files[first_included].smallestkey;
      }
      if (comparator->CompareWithoutTimestamp(
              largestkey, current_files[last_included].largestkey) < 0) {
        largestkey = current_files[last_included].largestkey;
      }
    }

    SstFileMetaData aggregated_file_meta;
    aggregated_file_meta.smallestkey = smallestkey;
    aggregated_file_meta.largestkey = largestkey;

    // For all lower levels, include all overlapping files.
    // We need to add overlapping files from the current level too because even
    // if there no input_files in level l, we would still need to add files
    // which overlap with the range containing the input_files in levels 0 to l
    // Level 0 doesn't need to be handled this way because files are sorted by
    // time and not by key
    for (int m = std::max(l, 1); m <= output_level; ++m) {
      for (auto& next_lv_file : levels[m].files) {
        if (HaveOverlappingKeyRanges(comparator, aggregated_file_meta,
                                     next_lv_file)) {
          if (next_lv_file.being_compacted) {
            return Status::Aborted(
                "File " + next_lv_file.name +
                " that has overlapping key range with one of the compaction "
                " input file is currently being compacted.");
          }
          input_files->insert(TableFileNameToNumber(next_lv_file.name));
        }
      }
    }
  }
  return Status::OK();
}

Status CompactionPicker::SanitizeAndConvertCompactionInputFiles(
    std::unordered_set<uint64_t>* input_files, const int output_level,
    Version* version,
    std::vector<CompactionInputFiles>* converted_input_files) const {
  ColumnFamilyMetaData cf_meta;
  version->GetColumnFamilyMetaData(&cf_meta);

  assert(static_cast<int>(cf_meta.levels.size()) - 1 ==
         cf_meta.levels[cf_meta.levels.size() - 1].level);
  assert(converted_input_files);

  if (output_level >= static_cast<int>(cf_meta.levels.size())) {
    return Status::InvalidArgument(
        "Output level for column family " + cf_meta.name +
        " must between [0, " +
        std::to_string(cf_meta.levels[cf_meta.levels.size() - 1].level) + "].");
  }

  if (output_level > MaxOutputLevel()) {
    return Status::InvalidArgument(
        "Exceed the maximum output level defined by "
        "the current compaction algorithm --- " +
        std::to_string(MaxOutputLevel()));
  }

  if (output_level < 0) {
    return Status::InvalidArgument("Output level cannot be negative.");
  }

  if (input_files->size() == 0) {
    return Status::InvalidArgument(
        "A compaction must contain at least one file.");
  }

  Status s = SanitizeCompactionInputFilesForAllLevels(input_files, cf_meta,
                                                      output_level);
  if (!s.ok()) {
    return s;
  }

  // for all input files, check whether the file number matches
  // any currently-existing files.
  for (auto file_num : *input_files) {
    bool found = false;
    int input_file_level = -1;
    for (const auto& level_meta : cf_meta.levels) {
      for (const auto& file_meta : level_meta.files) {
        if (file_num == TableFileNameToNumber(file_meta.name)) {
          if (file_meta.being_compacted) {
            return Status::Aborted("Specified compaction input file " +
                                   MakeTableFileName("", file_num) +
                                   " is already being compacted.");
          }
          found = true;
          input_file_level = level_meta.level;
          break;
        }
      }
      if (found) {
        break;
      }
    }
    if (!found) {
      return Status::InvalidArgument(
          "Specified compaction input file " + MakeTableFileName("", file_num) +
          " does not exist in column family " + cf_meta.name + ".");
    }
    if (input_file_level > output_level) {
      return Status::InvalidArgument(
          "Cannot compact file to up level, input file: " +
          MakeTableFileName("", file_num) + " level " +
          std::to_string(input_file_level) + " > output level " +
          std::to_string(output_level));
    }
  }

  s = GetCompactionInputsFromFileNumbers(converted_input_files, input_files,
                                         version->storage_info(),
                                         CompactionOptions());
  if (!s.ok()) {
    return s;
  }
  assert(converted_input_files->size() > 0);
  if (output_level != 0 &&
      FilesRangeOverlapWithCompaction(
          *converted_input_files, output_level,
          Compaction::EvaluateProximalLevel(
              version->storage_info(), version->GetMutableCFOptions(),
              ioptions_, (*converted_input_files)[0].level, output_level))) {
    return Status::Aborted(
        "A running compaction is writing to the same output level(s) in an "
        "overlapping key range");
  }
  return Status::OK();
}

void CompactionPicker::RegisterCompaction(Compaction* c) {
  if (c == nullptr) {
    return;
  }
  assert(ioptions_.compaction_style != kCompactionStyleLevel ||
         c->output_level() == 0 ||
         !FilesRangeOverlapWithCompaction(*c->inputs(), c->output_level(),
                                          c->GetProximalLevel()));
  // CompactionReason::kExternalSstIngestion's start level is just a placeholder
  // number without actual meaning as file ingestion technically does not have
  // an input level like other compactions
  if ((c->start_level() == 0 &&
       c->compaction_reason() != CompactionReason::kExternalSstIngestion) ||
      ioptions_.compaction_style == kCompactionStyleUniversal) {
    level0_compactions_in_progress_.insert(c);
  }
  compactions_in_progress_.insert(c);
  TEST_SYNC_POINT_CALLBACK("CompactionPicker::RegisterCompaction:Registered",
                           c);
}

void CompactionPicker::UnregisterCompaction(Compaction* c) {
  if (c == nullptr) {
    return;
  }
  if (c->start_level() == 0 ||
      ioptions_.compaction_style == kCompactionStyleUniversal) {
    level0_compactions_in_progress_.erase(c);
  }
  compactions_in_progress_.erase(c);
}

void CompactionPicker::PickFilesMarkedForCompaction(
    const std::string& cf_name, VersionStorageInfo* vstorage, int* start_level,
    int* output_level, CompactionInputFiles* start_level_inputs,
    std::function<bool(const FileMetaData*)> skip_marked_file) {
  if (vstorage->FilesMarkedForCompaction().empty()) {
    return;
  }

  auto continuation = [&, cf_name](std::pair<int, FileMetaData*> level_file) {
    // If it's being compacted it has nothing to do here.
    // If this assert() fails that means that some function marked some
    // files as being_compacted, but didn't call ComputeCompactionScore()
    assert(!level_file.second->being_compacted);
    if (skip_marked_file(level_file.second)) {
      return false;
    }
    *start_level = level_file.first;
    *output_level =
        (*start_level == 0) ? vstorage->base_level() : *start_level + 1;

    if (*start_level == 0 && !level0_compactions_in_progress()->empty()) {
      return false;
    }

    start_level_inputs->files = {level_file.second};
    start_level_inputs->level = *start_level;
    return ExpandInputsToCleanCut(cf_name, vstorage, start_level_inputs);
  };

  // take a chance on a random file first
  Random64 rnd(/* seed */ reinterpret_cast<uint64_t>(vstorage));
  size_t random_file_index = static_cast<size_t>(rnd.Uniform(
      static_cast<uint64_t>(vstorage->FilesMarkedForCompaction().size())));
  TEST_SYNC_POINT_CALLBACK("CompactionPicker::PickFilesMarkedForCompaction",
                           &random_file_index);

  if (continuation(vstorage->FilesMarkedForCompaction()[random_file_index])) {
    // found the compaction!
    return;
  }

  for (auto& level_file : vstorage->FilesMarkedForCompaction()) {
    if (continuation(level_file)) {
      // found the compaction!
      return;
    }
  }
  start_level_inputs->files.clear();
}

bool CompactionPicker::GetOverlappingL0Files(
    VersionStorageInfo* vstorage, CompactionInputFiles* start_level_inputs,
    int output_level, int* parent_index, const FileMetaData* starting_l0_file) {
  // Two level 0 compaction won't run at the same time, so don't need to worry
  // about files on level 0 being compacted.
  assert(level0_compactions_in_progress()->empty());
  InternalKey smallest;
  InternalKey largest;
  GetRange(*start_level_inputs, &smallest, &largest);
  // Note that the next call will discard the file we placed in
  // c->inputs_[0] earlier and replace it with an overlapping set
  // which will include the picked file.
  start_level_inputs->files.clear();
  vstorage->GetOverlappingInputs(0, &smallest, &largest,
                                 &(start_level_inputs->files),
                                 /*hint_index=*/-1,
                                 /*file_index=*/nullptr,
                                 /*expand_range=*/true,
                                 /*starting_l0_file=*/starting_l0_file);

  // If we include more L0 files in the same compaction run it can
  // cause the 'smallest' and 'largest' key to get extended to a
  // larger range. So, re-invoke GetRange to get the new key range
  GetRange(*start_level_inputs, &smallest, &largest);
  if (IsRangeInCompaction(vstorage, &smallest, &largest, output_level,
                          parent_index)) {
    return false;
  }
  assert(!start_level_inputs->files.empty());

  return true;
}

}  // namespace ROCKSDB_NAMESPACE
