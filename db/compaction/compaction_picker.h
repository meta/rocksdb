//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

#include "db/compaction/compaction.h"
#include "db/snapshot_checker.h"
#include "db/version_set.h"
#include "options/cf_options.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// The file contains an abstract class CompactionPicker, and its two
// sub-classes LevelCompactionPicker and NullCompactionPicker, as
// well as some helper functions used by them.

class LogBuffer;
class Compaction;
class VersionStorageInfo;
struct CompactionInputFiles;

// An abstract class to pick compactions from an existing LSM-tree.
//
// Each compaction style inherits the class and implement the
// interface to form automatic compactions. If NeedCompaction() is true,
// then call PickCompaction() to find what files need to be compacted
// and where to put the output files.
//
// Non-virtual functions CompactRange() and CompactFiles() are used to
// pick files to compact based on users' DB::CompactRange() and
// DB::CompactFiles() requests, respectively. There is little
// compaction style specific logic for them.
class CompactionPicker {
 public:
  CompactionPicker(const ImmutableOptions& ioptions,
                   const InternalKeyComparator* icmp);
  virtual ~CompactionPicker();

  // Pick level and inputs for a new compaction.
  //
  // Returns nullptr if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  // Currently, only universal compaction will query existing snapshots and
  // pass it to aid compaction picking. And it's only passed when user-defined
  // timestamps is not enabled. The other compaction styles do not pass or use
  // `existing_snapshots` or `snapshot_checker`.
  virtual Compaction* PickCompaction(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      const MutableDBOptions& mutable_db_options,
      const std::vector<SequenceNumber>& existing_snapshots,
      const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
      LogBuffer* log_buffer, const std::string& full_history_ts_low,
      bool require_max_output_level = false) = 0;

  // The returned Compaction might not include the whole requested range.
  // In that case, compaction_end will be set to the next key that needs
  // compacting. In case the compaction will compact the whole range,
  // compaction_end will be set to nullptr.
  // Client is responsible for compaction_end storage -- when called,
  // *compaction_end should point to valid InternalKey!
  // REQUIRES: If not compacting all levels (input_level == kCompactAllLevels),
  // then levels between input_level and output_level should be empty.
  virtual Compaction* PickCompactionForCompactRange(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
      int input_level, int output_level,
      const CompactRangeOptions& compact_range_options,
      const InternalKey* begin, const InternalKey* end,
      InternalKey** compaction_end, bool* manual_conflict,
      uint64_t max_file_num_to_ignore, const std::string& trim_ts,
      const std::string& full_history_ts_low);

  // The maximum allowed output level.  Default value is NumberLevels() - 1.
  virtual int MaxOutputLevel() const { return NumberLevels() - 1; }

  virtual bool NeedsCompaction(const VersionStorageInfo* vstorage) const = 0;

  // Sanitize the input set of compaction input files and convert it to
  // `std::vector<CompactionInputFiles>` in the output parameter
  // `converted_input_files`.
  // When the input parameters do not describe a valid
  // compaction, the function will try to fix the input_files by adding
  // necessary files.  If it's not possible to convert an invalid input_files
  // into a valid one by adding more files, the function will return a
  // non-ok status with specific reason.
  //
  Status SanitizeAndConvertCompactionInputFiles(
      std::unordered_set<uint64_t>* input_files, const int output_level,
      Version* version,
      std::vector<CompactionInputFiles>* converted_input_files) const;

  // Free up the files that participated in a compaction
  //
  // Requirement: DB mutex held
  void ReleaseCompactionFiles(Compaction* c, const Status& status);

  // Returns true if any one of the specified files are being compacted
  bool AreFilesInCompaction(const std::vector<FileMetaData*>& files);

  // Takes a list of CompactionInputFiles and returns a (manual) Compaction
  // object.
  //
  // Caller must provide a set of input files that has been passed through
  // `SanitizeAndConvertCompactionInputFiles` earlier. The lock should not be
  // released between that call and this one.
  //
  //  TODO - Remove default values for earliest_snapshot and snapshot_checker
  //  and require all callers to pass them in so that DB::CompactFiles() can
  //  also benefit from Standalone Range Tombstone Optimization
  Compaction* PickCompactionForCompactFiles(
      const CompactionOptions& compact_options,
      const std::vector<CompactionInputFiles>& input_files, int output_level,
      VersionStorageInfo* vstorage, const MutableCFOptions& mutable_cf_options,
      const MutableDBOptions& mutable_db_options, uint32_t output_path_id,
      std::optional<SequenceNumber> earliest_snapshot = std::nullopt,
      const SnapshotChecker* snapshot_checker = nullptr);

  // Converts a set of compaction input file numbers into
  // a list of CompactionInputFiles.
  // TODO(hx235): remove the unused paramter `compact_options`
  Status GetCompactionInputsFromFileNumbers(
      std::vector<CompactionInputFiles>* input_files,
      std::unordered_set<uint64_t>* input_set,
      const VersionStorageInfo* vstorage,
      const CompactionOptions& compact_options) const;

  // Is there currently a compaction involving level 0 taking place
  bool IsLevel0CompactionInProgress() const {
    return !level0_compactions_in_progress_.empty();
  }

  // Is any compaction in progress
  bool IsCompactionInProgress() const {
    return !(level0_compactions_in_progress_.empty() &&
             compactions_in_progress_.empty());
  }

  // Return true if the passed key range overlap with a compaction output
  // that is currently running.
  bool RangeOverlapWithCompaction(const Slice& smallest_user_key,
                                  const Slice& largest_user_key, int level,
                                  bool range_limit_exclusive) const;

  // Stores the minimal range that covers all entries in inputs in
  // *smallest, *largest.
  // REQUIRES: inputs is not empty
  void GetRange(const CompactionInputFiles& inputs, InternalKey* smallest,
                InternalKey* largest) const;

  // Stores the minimal range that covers all entries in inputs1 and inputs2
  // in *smallest, *largest.
  // REQUIRES: inputs is not empty
  void GetRange(const CompactionInputFiles& inputs1,
                const CompactionInputFiles& inputs2, InternalKey* smallest,
                InternalKey* largest) const;

  // Stores the minimal range that covers all entries in inputs
  // in *smallest, *largest.
  // REQUIRES: inputs is not empty (at least on entry have one file)
  void GetRange(const std::vector<CompactionInputFiles>& inputs,
                InternalKey* smallest, InternalKey* largest,
                int exclude_level) const;

  int NumberLevels() const { return ioptions_.num_levels; }

  // Add more files to the inputs on "level" to make sure that
  // no newer version of a key is compacted to "level+1" while leaving an older
  // version in a "level". Otherwise, any Get() will search "level" first,
  // and will likely return an old/stale value for the key, since it always
  // searches in increasing order of level to find the value. This could
  // also scramble the order of merge operands. This function should be
  // called any time a new Compaction is created, and its inputs_[0] are
  // populated.
  //
  // Will return false if it is impossible to apply this compaction.
  bool ExpandInputsToCleanCut(const std::string& cf_name,
                              VersionStorageInfo* vstorage,
                              CompactionInputFiles* inputs,
                              InternalKey** next_smallest = nullptr);

  // Returns true if any one of the parent files are being compacted
  bool IsRangeInCompaction(VersionStorageInfo* vstorage,
                           const InternalKey* smallest,
                           const InternalKey* largest, int level, int* index);

  // Returns true if the key range that `inputs` files cover overlap with the
  // key range of a currently running compaction.
  bool FilesRangeOverlapWithCompaction(
      const std::vector<CompactionInputFiles>& inputs, int level,
      int proximal_level) const;

  // @param starting_l0_file If not null, restricts L0 file selection to only
  //                         include files at or older than starting_l0_file.
  bool SetupOtherInputs(const std::string& cf_name,
                        const MutableCFOptions& mutable_cf_options,
                        VersionStorageInfo* vstorage,
                        CompactionInputFiles* inputs,
                        CompactionInputFiles* output_level_inputs,
                        int* parent_index, int base_index,
                        bool only_expand_towards_right = false,
                        const FileMetaData* starting_l0_file = nullptr);

  void GetGrandparents(VersionStorageInfo* vstorage,
                       const CompactionInputFiles& inputs,
                       const CompactionInputFiles& output_level_inputs,
                       std::vector<FileMetaData*>* grandparents);

  void PickFilesMarkedForCompaction(
      const std::string& cf_name, VersionStorageInfo* vstorage,
      int* start_level, int* output_level,
      CompactionInputFiles* start_level_inputs,
      std::function<bool(const FileMetaData*)> skip_marked_file);

  // @param starting_l0_file If not null, restricts L0 file selection to only
  //                         include files at or older than starting_l0_file.
  bool GetOverlappingL0Files(VersionStorageInfo* vstorage,
                             CompactionInputFiles* start_level_inputs,
                             int output_level, int* parent_index,
                             const FileMetaData* starting_l0_file = nullptr);

  // Register this compaction in the set of running compactions
  void RegisterCompaction(Compaction* c);

  // Remove this compaction from the set of running compactions
  void UnregisterCompaction(Compaction* c);

  std::set<Compaction*>* level0_compactions_in_progress() {
    return &level0_compactions_in_progress_;
  }
  std::unordered_set<Compaction*>* compactions_in_progress() {
    return &compactions_in_progress_;
  }

  const InternalKeyComparator* icmp() const { return icmp_; }

 protected:
  const ImmutableOptions& ioptions_;

  // A helper function to SanitizeAndConvertCompactionInputFiles() that
  // sanitizes "input_files" by adding necessary files.
  virtual Status SanitizeCompactionInputFilesForAllLevels(
      std::unordered_set<uint64_t>* input_files,
      const ColumnFamilyMetaData& cf_meta, const int output_level) const;

  // Keeps track of all compactions that are running on Level0.
  // Protected by DB mutex
  std::set<Compaction*> level0_compactions_in_progress_;

  // Keeps track of all compactions that are running.
  // Protected by DB mutex
  std::unordered_set<Compaction*> compactions_in_progress_;

  const InternalKeyComparator* const icmp_;
};

// A dummy compaction that never triggers any automatic
// compaction.
class NullCompactionPicker : public CompactionPicker {
 public:
  NullCompactionPicker(const ImmutableOptions& ioptions,
                       const InternalKeyComparator* icmp)
      : CompactionPicker(ioptions, icmp) {}
  ~NullCompactionPicker() override = default;

  // Always return "nullptr"
  Compaction* PickCompaction(
      const std::string& /*cf_name*/,
      const MutableCFOptions& /*mutable_cf_options*/,
      const MutableDBOptions& /*mutable_db_options*/,
      const std::vector<SequenceNumber>& /*existing_snapshots*/,
      const SnapshotChecker* /*snapshot_checker*/,
      VersionStorageInfo* /*vstorage*/, LogBuffer* /* log_buffer */,
      const std::string& /*full_history_ts_low*/,
      bool /*require_max_output_level*/) override {
    return nullptr;
  }

  // Always return "nullptr"
  Compaction* PickCompactionForCompactRange(
      const std::string& /*cf_name*/,
      const MutableCFOptions& /*mutable_cf_options*/,
      const MutableDBOptions& /*mutable_db_options*/,
      VersionStorageInfo* /*vstorage*/, int /*input_level*/,
      int /*output_level*/,
      const CompactRangeOptions& /*compact_range_options*/,
      const InternalKey* /*begin*/, const InternalKey* /*end*/,
      InternalKey** /*compaction_end*/, bool* /*manual_conflict*/,
      uint64_t /*max_file_num_to_ignore*/, const std::string& /*trim_ts*/,
      const std::string& /*full_history_ts_low*/) override {
    return nullptr;
  }

  // Always returns false.
  bool NeedsCompaction(const VersionStorageInfo* /*vstorage*/) const override {
    return false;
  }
};

// Attempts to find an intra L0 compaction conforming to the given parameters.
//
// @param level_files                     Metadata for L0 files.
// @param min_files_to_compact            Minimum number of files required to
//                                        do the compaction.
// @param max_compact_bytes_per_del_file  Maximum average size in bytes per
//                                        file that is going to get deleted by
//                                        the compaction.
// @param max_compaction_bytes            Maximum total size in bytes (in terms
//                                        of compensated file size) for files
//                                        to be compacted.
// @param [out] comp_inputs               If a compaction was found, will be
//                                        initialized with corresponding input
//                                        files. Cannot be nullptr.
//
// @return                                true iff compaction was found.
bool PickCostBasedIntraL0Compaction(
    const std::vector<FileMetaData*>& level_files, size_t min_files_to_compact,
    uint64_t max_compact_bytes_per_del_file, uint64_t max_compaction_bytes,
    CompactionInputFiles* comp_inputs);

CompressionType GetCompressionType(const VersionStorageInfo* vstorage,
                                   const MutableCFOptions& mutable_cf_options,
                                   int level, int base_level,
                                   const bool enable_compression = true);

CompressionOptions GetCompressionOptions(
    const MutableCFOptions& mutable_cf_options,
    const VersionStorageInfo* vstorage, int level,
    const bool enable_compression = true);

}  // namespace ROCKSDB_NAMESPACE
