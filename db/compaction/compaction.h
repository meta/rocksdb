//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "db/snapshot_checker.h"
#include "db/version_set.h"
#include "memory/arena.h"
#include "options/cf_options.h"
#include "rocksdb/sst_partitioner.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {
// The file contains class Compaction, as well as some helper functions
// and data structures used by the class.

// Utility for comparing sstable boundary keys. Returns -1 if either a or b is
// null which provides the property that a==null indicates a key that is less
// than any key and b==null indicates a key that is greater than any key. Note
// that the comparison is performed primarily on the user-key portion of the
// key. If the user-keys compare equal, an additional test is made to sort
// range tombstone sentinel keys before other keys with the same user-key. The
// result is that 2 user-keys will compare equal if they differ purely on
// their sequence number and value, but the range tombstone sentinel for that
// user-key will compare not equal. This is necessary because the range
// tombstone sentinel key is set as the largest key for an sstable even though
// that key never appears in the database. We don't want adjacent sstables to
// be considered overlapping if they are separated by the range tombstone
// sentinel.
int sstableKeyCompare(const Comparator* user_cmp, const Slice&, const Slice&);
inline int sstableKeyCompare(const Comparator* user_cmp, const Slice& a,
                             const InternalKey& b) {
  return sstableKeyCompare(user_cmp, a, b.Encode());
}
inline int sstableKeyCompare(const Comparator* user_cmp, const InternalKey& a,
                             const Slice& b) {
  return sstableKeyCompare(user_cmp, a.Encode(), b);
}
inline int sstableKeyCompare(const Comparator* user_cmp, const InternalKey& a,
                             const InternalKey& b) {
  return sstableKeyCompare(user_cmp, a.Encode(), b.Encode());
}
int sstableKeyCompare(const Comparator* user_cmp, const InternalKey* a,
                      const InternalKey& b);
int sstableKeyCompare(const Comparator* user_cmp, const InternalKey& a,
                      const InternalKey* b);

// An AtomicCompactionUnitBoundary represents a range of keys [smallest,
// largest] that exactly spans one ore more neighbouring SSTs on the same
// level. Every pair of  SSTs in this range "overlap" (i.e., the largest
// user key of one file is the smallest user key of the next file). These
// boundaries are propagated down to RangeDelAggregator during compaction
// to provide safe truncation boundaries for range tombstones.
struct AtomicCompactionUnitBoundary {
  const InternalKey* smallest = nullptr;
  const InternalKey* largest = nullptr;
};

// The structure that manages compaction input files associated
// with the same physical level.
struct CompactionInputFiles {
  int level;
  std::vector<FileMetaData*> files;
  std::vector<AtomicCompactionUnitBoundary> atomic_compaction_unit_boundaries;
  inline bool empty() const { return files.empty(); }
  inline size_t size() const { return files.size(); }
  inline void clear() { files.clear(); }
  inline FileMetaData* operator[](size_t i) const { return files[i]; }
};

class Version;
class ColumnFamilyData;
class VersionStorageInfo;
class CompactionFilter;

// A Compaction encapsulates metadata about a compaction.
class Compaction {
 public:
  Compaction(VersionStorageInfo* input_version,
             const ImmutableOptions& immutable_options,
             const MutableCFOptions& mutable_cf_options,
             const MutableDBOptions& mutable_db_options,
             std::vector<CompactionInputFiles> inputs, int output_level,
             uint64_t target_file_size, uint64_t max_compaction_bytes,
             uint32_t output_path_id, CompressionType compression,
             CompressionOptions compression_opts,
             Temperature output_temperature_override,
             uint32_t max_subcompactions,
             std::vector<FileMetaData*> grandparents,
             std::optional<SequenceNumber> earliest_snapshot,
             const SnapshotChecker* snapshot_checker,
             CompactionReason compaction_reason,
             const std::string& trim_ts = "", double score = -1,
             bool l0_files_might_overlap = true,
             BlobGarbageCollectionPolicy blob_garbage_collection_policy =
                 BlobGarbageCollectionPolicy::kUseDefault,
             double blob_garbage_collection_age_cutoff = -1);

  // The type of the proximal level output range
  enum class ProximalOutputRangeType : int {
    kNotSupported,  // it cannot output to the proximal level
    kFullRange,     // any data could be output to the proximal level
    kNonLastRange,  // only the keys within non_last_level compaction inputs can
                    // be outputted to the proximal level
    kDisabled,      // no data can be outputted to the proximal level
  };

  // No copying allowed
  Compaction(const Compaction&) = delete;
  void operator=(const Compaction&) = delete;
  Compaction(Compaction&&) = delete;
  void operator=(Compaction&&) = delete;

  ~Compaction();

  // Returns the level associated to the specified compaction input level.
  // If compaction_input_level is not specified, then input_level is set to 0.
  int level(size_t compaction_input_level = 0) const {
    return inputs_[compaction_input_level].level;
  }

  int start_level() const { return start_level_; }

  // Outputs will go to this level
  int output_level() const { return output_level_; }

  // Returns the number of input levels in this compaction.
  size_t num_input_levels() const { return inputs_.size(); }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  VersionEdit* edit() { return &edit_; }

  // Returns the number of input files associated to the specified
  // compaction input level.
  // The function will return 0 if when "compaction_input_level" < 0
  // or "compaction_input_level" >= "num_input_levels()".
  size_t num_input_files(size_t compaction_input_level) const {
    if (compaction_input_level < inputs_.size()) {
      return inputs_[compaction_input_level].size();
    }
    return 0;
  }

  // Returns input version of the compaction
  Version* input_version() const { return input_version_; }

  // Returns the ColumnFamilyData associated with the compaction.
  ColumnFamilyData* column_family_data() const { return cfd_; }

  // Returns the file meta data of the 'i'th input file at the
  // specified compaction input level.
  // REQUIREMENT: "compaction_input_level" must be >= 0 and
  //              < "input_levels()"
  FileMetaData* input(size_t compaction_input_level, size_t i) const {
    assert(compaction_input_level < inputs_.size());
    return inputs_[compaction_input_level][i];
  }

  const std::vector<AtomicCompactionUnitBoundary>* boundaries(
      size_t compaction_input_level) const {
    assert(compaction_input_level < inputs_.size());
    return &inputs_[compaction_input_level].atomic_compaction_unit_boundaries;
  }

  // Returns the list of file meta data of the specified compaction
  // input level.
  // REQUIREMENT: "compaction_input_level" must be >= 0 and
  //              < "input_levels()"
  const std::vector<FileMetaData*>* inputs(
      size_t compaction_input_level) const {
    assert(compaction_input_level < inputs_.size());
    return &inputs_[compaction_input_level].files;
  }

  const std::vector<CompactionInputFiles>* inputs() { return &inputs_; }

  // Returns the LevelFilesBrief of the specified compaction input level.
  // Note that if the compaction includes standalone range deletion file,
  // this function returns the result after filtering out input files covered
  // by the range deletion file.
  // Use inputs() if you want to get the original input files.
  const LevelFilesBrief* input_levels(size_t compaction_input_level) const {
    return &input_levels_[compaction_input_level];
  }

  // Returns the filtered input files of the specified compaction input level.
  // For now, only non start level is filtered.
  const std::vector<FileMetaData*>& filtered_input_levels(
      size_t compaction_input_level) const {
    const std::vector<FileMetaData*>& filtered_input_level =
        filtered_input_levels_[compaction_input_level];
    assert(compaction_input_level != 0 || filtered_input_level.size() == 0);
    return filtered_input_level;
  }

  // Maximum size of files to build during this compaction.
  uint64_t max_output_file_size() const { return max_output_file_size_; }

  // Target output file size for this compaction
  uint64_t target_output_file_size() const { return target_output_file_size_; }

  // What compression for output
  CompressionType output_compression() const { return output_compression_; }

  // What compression options for output
  const CompressionOptions& output_compression_opts() const {
    return output_compression_opts_;
  }

  // Whether need to write output file to second DB path.
  uint32_t output_path_id() const { return output_path_id_; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  bool IsTrivialMove() const;

  // The split user key in the output level if this compaction is required to
  // split the output files according to the existing cursor in the output
  // level under round-robin compaction policy. Empty indicates no required
  // splitting key
  const InternalKey* GetOutputSplitKey() const { return output_split_key_; }

  // If true, then the compaction can be done by simply deleting input files.
  bool deletion_compaction() const { return deletion_compaction_; }

  // Add all inputs to this compaction as delete operations to *edit.
  void AddInputDeletions(VersionEdit* edit);

  // Returns true if the available information we have guarantees that
  // the input "user_key" does not exist in any level beyond `output_level()`.
  bool KeyNotExistsBeyondOutputLevel(const Slice& user_key,
                                     std::vector<size_t>* level_ptrs) const;

  // Returns true if the user key range [begin_key, end_key) does not exist
  // in any level beyond `output_level()`.
  // Used for checking range tombstones, so we assume begin_key < end_key.
  // begin_key and end_key should include timestamp if enabled.
  bool KeyRangeNotExistsBeyondOutputLevel(
      const Slice& begin_key, const Slice& end_key,
      std::vector<size_t>* level_ptrs) const;

  // Clear all files to indicate that they are not being compacted
  // Delete this compaction from the list of running compactions.
  //
  // Requirement: DB mutex held
  void ReleaseCompactionFiles(const Status& status);

  // Returns the summary of the compaction in "output" with maximum "len"
  // in bytes.  The caller is responsible for the memory management of
  // "output".
  void Summary(char* output, int len);

  // Return the score that was used to pick this compaction run.
  double score() const { return score_; }

  // Is this compaction creating a file in the bottom most level?
  bool bottommost_level() const { return bottommost_level_; }

  // Is the compaction compact to the last level
  bool is_last_level() const {
    return output_level_ == immutable_options_.num_levels - 1;
  }

  // Does this compaction include all sst files?
  bool is_full_compaction() const { return is_full_compaction_; }

  bool use_separate_remote_compaction_pool() const {
    return use_separate_remote_compaction_pool_;
  }
  void SetUseSeparateRemoteCompactionPool(bool enabled) {
    use_separate_remote_compaction_pool_ = enabled;
  }

  // Was this compaction triggered manually by the client?
  bool is_manual_compaction() const { return is_manual_compaction_; }

  std::string trim_ts() const { return trim_ts_; }

  // Used when allow_trivial_move option is set in
  // Universal compaction. If all the input files are
  // non overlapping, then is_trivial_move_ variable
  // will be set true, else false
  void set_is_trivial_move(bool trivial_move) {
    is_trivial_move_ = trivial_move;
  }

  // Used when allow_trivial_move option is set in
  // Universal compaction. Returns true, if the input files
  // are non-overlapping and can be trivially moved.
  bool is_trivial_move() const { return is_trivial_move_; }

  bool is_trivial_copy_compaction() const {
    return immutable_options_.compaction_style == kCompactionStyleFIFO &&
           compaction_reason_ == CompactionReason::kChangeTemperature &&
           mutable_cf_options_.compaction_options_fifo
               .allow_trivial_copy_when_change_temperature;
  }

  // How many total levels are there?
  int number_levels() const { return number_levels_; }

  // Return the ImmutableOptions that should be used throughout the compaction
  // procedure
  const ImmutableOptions& immutable_options() const {
    return immutable_options_;
  }

  // Return the MutableCFOptions that should be used throughout the compaction
  // procedure
  const MutableCFOptions& mutable_cf_options() const {
    return mutable_cf_options_;
  }

  // Returns the size in bytes that the output file should be preallocated to.
  // In level compaction, that is max_file_size_. In universal compaction, that
  // is the sum of all input file sizes.
  uint64_t OutputFilePreallocationSize() const;

  void FinalizeInputInfo(Version* input_version);

  struct InputLevelSummaryBuffer {
    char buffer[128];
  };

  const char* InputLevelSummary(InputLevelSummaryBuffer* scratch) const;

  uint64_t CalculateTotalInputSize() const;

  // In case of compaction error, reset the nextIndex that is used
  // to pick up the next file to be compacted from files_by_size_
  void ResetNextCompactionIndex();

  // Create a CompactionFilter from compaction_filter_factory
  std::unique_ptr<CompactionFilter> CreateCompactionFilter() const;

  // Create a SstPartitioner from sst_partitioner_factory
  std::unique_ptr<SstPartitioner> CreateSstPartitioner() const;

  // Is the input level corresponding to output_level_ empty?
  bool IsOutputLevelEmpty() const;

  // Should this compaction be broken up into smaller ones run in parallel?
  bool ShouldFormSubcompactions() const;

  // Returns true iff at least one input file references a blob file.
  //
  // PRE: input version has been set.
  bool DoesInputReferenceBlobFiles() const;

  // test function to validate the functionality of IsBottommostLevel()
  // function -- determines if compaction with inputs and storage is bottommost
  static bool TEST_IsBottommostLevel(
      int output_level, VersionStorageInfo* vstorage,
      const std::vector<CompactionInputFiles>& inputs);

  // TODO(hx235): eventually we should consider `InitInputTableProperties()`'s
  // status and fail the compaction if needed
  //
  // May open and read table files for table property.
  // Should not be called while holding mutex_.
  const TablePropertiesCollection& GetOrInitInputTableProperties() {
    InitInputTableProperties().PermitUncheckedError();
    return input_table_properties_;
  }

  const TablePropertiesCollection& GetInputTableProperties() const {
    return input_table_properties_;
  }

  // TODO(hx235): consider making this function symmetric to
  // InitInputTableProperties()
  void SetOutputTableProperties(
      const std::string& file_name,
      const std::shared_ptr<const TableProperties>& tp) {
    output_table_properties_[file_name] = tp;
  }

  const TablePropertiesCollection& GetOutputTableProperties() const {
    return output_table_properties_;
  }

  Slice GetSmallestUserKey() const { return smallest_user_key_; }

  Slice GetLargestUserKey() const { return largest_user_key_; }

  ProximalOutputRangeType GetProximalOutputRangeType() const {
    return proximal_output_range_type_;
  }

  // Return true if the compaction supports per_key_placement
  bool SupportsPerKeyPlacement() const;

  // Get per_key_placement proximal output level, which is `last_level - 1`
  // if per_key_placement feature is supported. Otherwise, return -1.
  int GetProximalLevel() const;

  // Return true if the given range is overlap with proximal level output
  // range.
  // Both smallest_key and largest_key include timestamps if user-defined
  // timestamp is enabled.
  bool OverlapProximalLevelOutputRange(const Slice& smallest_key,
                                       const Slice& largest_key,
                                       bool range_limit_exclusive) const;

  // For testing purposes, check that a key is within proximal level
  // output range for per_key_placement feature, which is safe to place the key
  // to the proximal level. Different compaction strategies have different
  // rules. `user_key` includes timestamp if user-defined timestamp is enabled.
  void TEST_AssertWithinProximalLevelOutputRange(
      const Slice& user_key, bool expect_failure = false) const;

  CompactionReason compaction_reason() const { return compaction_reason_; }

  const std::vector<FileMetaData*>& grandparents() const {
    return grandparents_;
  }

  uint64_t max_compaction_bytes() const { return max_compaction_bytes_; }

  // Order of precedence for temperature:
  // 1. Override temp if not kUnknown
  // 2. Temperature of the last level files if applicable
  // 3. Default write temperature
  Temperature GetOutputTemperature(bool is_proximal_level = false) const;

  uint32_t max_subcompactions() const { return max_subcompactions_; }

  bool enable_blob_garbage_collection() const {
    return enable_blob_garbage_collection_;
  }

  double blob_garbage_collection_age_cutoff() const {
    return blob_garbage_collection_age_cutoff_;
  }

  // start and end are sub compact range. Null if no boundary.
  // This is used to calculate the newest_key_time table property after
  // compaction.
  uint64_t MaxInputFileNewestKeyTime(const InternalKey* start,
                                     const InternalKey* end) const;

  // start and end are sub compact range. Null if no boundary.
  // This is used to filter out some input files' ancestor's time range.
  uint64_t MinInputFileOldestAncestorTime(const InternalKey* start,
                                          const InternalKey* end) const;
  // Return the minimum epoch number among
  // input files' associated with this compaction
  uint64_t MinInputFileEpochNumber() const;

  // Called by DBImpl::NotifyOnCompactionCompleted to make sure number of
  // compaction begin and compaction completion callbacks match.
  void SetNotifyOnCompactionCompleted() {
    notify_on_compaction_completion_ = true;
  }

  bool ShouldNotifyOnCompactionCompleted() const {
    return notify_on_compaction_completion_;
  }

  // Called by DBImpl::NotifyOnCompactionPreCommit to ensure the
  // OnCompactionPreCommit listener callback fires at most once per
  // compaction lifecycle, even though the notify helper is invoked at multiple
  // potential ReleaseCompactionFiles() sites for safety.
  void SetNotifyOnCompactionPreCommitCalled() {
    notify_on_compaction_pre_commit_called_ = true;
  }

  bool WasNotifyOnCompactionPreCommitCalled() const {
    return notify_on_compaction_pre_commit_called_;
  }

  static constexpr int kInvalidLevel = -1;

  // Evaluate proximal output level. If the compaction supports
  // per_key_placement feature, it returns the proximal level number.
  // Otherwise, it's set to kInvalidLevel (-1), which means
  // output_to_proximal_level is not supported.
  // Note: even the proximal level output is supported (ProximalLevel !=
  // kInvalidLevel), some key range maybe unsafe to be outputted to the
  // proximal level. The safe key range is populated by
  // `PopulateProximalLevelOutputRange()`.
  // Which could potentially disable all proximal level output.
  static int EvaluateProximalLevel(const VersionStorageInfo* vstorage,
                                   const MutableCFOptions& mutable_cf_options,
                                   const ImmutableOptions& immutable_options,
                                   const int start_level,
                                   const int output_level);

  static bool OutputToNonZeroMaxOutputLevel(int output_level,
                                            int max_output_level) {
    return output_level > 0 && output_level == max_output_level;
  }

  // If some data cannot be safely migrated "up" the LSM tree due to a change
  // in the preclude_last_level_data_seconds setting, this indicates a sequence
  // number for the newest data that must be kept in the last level.
  SequenceNumber GetKeepInLastLevelThroughSeqno() const {
    return keep_in_last_level_through_seqno_;
  }

  // mark (or clear) all files that are being compacted
  void MarkFilesBeingCompacted(bool being_compacted) const;

 private:
  Status InitInputTableProperties();

  // get the smallest and largest key present in files to be compacted
  static void GetBoundaryKeys(VersionStorageInfo* vstorage,
                              const std::vector<CompactionInputFiles>& inputs,
                              Slice* smallest_key, Slice* largest_key,
                              int exclude_level = -1);

  // get the smallest and largest internal key present in files to be compacted
  static void GetBoundaryInternalKeys(
      VersionStorageInfo* vstorage,
      const std::vector<CompactionInputFiles>& inputs,
      InternalKey* smallest_key, InternalKey* largest_key,
      int exclude_level = -1);

  // populate proximal level output range, which will be used to determine if
  // a key is safe to output to the proximal level (details see
  // `Compaction::WithinProximalLevelOutputRange()`.
  void PopulateProximalLevelOutputRange();

  // If oldest snapshot is specified at Compaction construction time, we have
  // an opportunity to optimize inputs for compaction iterator for this case:
  // When a standalone range deletion file on the start level is recognized and
  // can be determined to completely shadow some input files on non-start level.
  // These files will be filtered out and later not feed to compaction iterator.
  void FilterInputsForCompactionIterator();

  // Get the atomic file boundaries for all files in the compaction. Necessary
  // in order to avoid the scenario described in
  // https://github.com/meta/rocksdb/pull/4432#discussion_r221072219 and
  // plumb down appropriate key boundaries to RangeDelAggregator during
  // compaction.
  static std::vector<CompactionInputFiles> PopulateWithAtomicBoundaries(
      VersionStorageInfo* vstorage, std::vector<CompactionInputFiles> inputs);

  // helper function to determine if compaction with inputs and storage is
  // bottommost
  static bool IsBottommostLevel(
      int output_level, VersionStorageInfo* vstorage,
      const std::vector<CompactionInputFiles>& inputs);

  static bool IsFullCompaction(VersionStorageInfo* vstorage,
                               const std::vector<CompactionInputFiles>& inputs);

  VersionStorageInfo* input_vstorage_;

  const int start_level_;   // the lowest level to be compacted
  const int output_level_;  // levels to which output files are stored
  uint64_t target_output_file_size_;
  uint64_t max_output_file_size_;
  uint64_t max_compaction_bytes_;
  uint32_t max_subcompactions_;
  const ImmutableOptions immutable_options_;
  const MutableCFOptions mutable_cf_options_;
  Version* input_version_;
  VersionEdit edit_;
  const int number_levels_;
  ColumnFamilyData* cfd_;
  Arena arena_;  // Arena used to allocate space for file_levels_

  const uint32_t output_path_id_;
  CompressionType output_compression_;
  CompressionOptions output_compression_opts_;
  Temperature output_temperature_override_;
  // If true, then the compaction can be done by simply deleting input files.
  const bool deletion_compaction_;
  // should it split the output file using the compact cursor?
  const InternalKey* output_split_key_;

  // L0 files in LSM-tree might be overlapping. But the compaction picking
  // logic might pick a subset of the files that aren't overlapping. if
  // that is the case, set the value to false. Otherwise, set it true.
  bool l0_files_might_overlap_;

  // Compaction input files organized by level. Constant after construction
  const std::vector<CompactionInputFiles> inputs_;

  // All files from inputs_ that are not filtered and will be fed to compaction
  // iterator, organized more closely in memory.
  autovector<LevelFilesBrief, 2> input_levels_;

  // State used to check for number of overlapping grandparent files
  // (grandparent == "output_level_ + 1")
  std::vector<FileMetaData*> grandparents_;

  // The earliest snapshot and snapshot checker at compaction picking time.
  // These fields are only set for deletion triggered compactions picked in
  // universal compaction. And when user-defined timestamp is not enabled.
  // It will be used to possibly filter out some non start level input files.
  std::optional<SequenceNumber> earliest_snapshot_;
  const SnapshotChecker* snapshot_checker_;

  // Markers for which non start level input files are filtered out if
  // applicable. Only applicable if earliest_snapshot_ is provided and input
  // start level has a standalone range deletion file. Filtered files are
  // tracked in `filtered_input_levels_`.
  std::vector<std::vector<bool>> non_start_level_input_files_filtered_;

  // All files from inputs_ that are filtered.
  std::vector<std::vector<FileMetaData*>> filtered_input_levels_;

  const double score_;  // score that was used to pick this compaction.

  // Is this compaction creating a file in the bottom most level?
  const bool bottommost_level_;
  // Does this compaction include all sst files?
  const bool is_full_compaction_;
  bool use_separate_remote_compaction_pool_ = false;

  // Is this compaction requested by the client?
  const bool is_manual_compaction_;

  // The data with timestamp > trim_ts_ will be removed
  const std::string trim_ts_;

  // True if we can do trivial move in Universal multi level
  // compaction
  bool is_trivial_move_;

  // Does input compression match the output compression?
  bool InputCompressionMatchesOutput() const;

  TablePropertiesCollection input_table_properties_;
  TablePropertiesCollection output_table_properties_;

  // smallest user keys in compaction
  // includes timestamp if user-defined timestamp is enabled.
  Slice smallest_user_key_;

  // largest user keys in compaction
  // includes timestamp if user-defined timestamp is enabled.
  Slice largest_user_key_;

  // Reason for compaction
  CompactionReason compaction_reason_;

  // Notify on compaction completion only if listener was notified on compaction
  // begin.
  bool notify_on_compaction_completion_;

  // Tracks whether OnCompactionPreCommit has already fired for this
  // compaction. The notify helper is called from multiple potential
  // ReleaseCompactionFiles() call sites; this flag guarantees that listeners
  // observe the callback exactly once per compaction.
  bool notify_on_compaction_pre_commit_called_ = false;

  // Enable/disable GC collection for blobs during compaction.
  bool enable_blob_garbage_collection_;

  // Blob garbage collection age cutoff.
  double blob_garbage_collection_age_cutoff_;

  SequenceNumber keep_in_last_level_through_seqno_ = kMaxSequenceNumber;

  // only set when per_key_placement feature is enabled, -1 (kInvalidLevel)
  // means not supported.
  const int proximal_level_;

  // Key range for proximal level output
  // includes timestamp if user-defined timestamp is enabled.
  // proximal_output_range_type_ shows the range type
  InternalKey proximal_level_smallest_;
  InternalKey proximal_level_largest_;
  ProximalOutputRangeType proximal_output_range_type_ =
      ProximalOutputRangeType::kNotSupported;
};

#ifndef NDEBUG
// Helper struct only for tests, which contains the data to decide if a key
// should be output to the proximal level.
// TODO: remove this when the public feature knob is available
struct PerKeyPlacementContext {
  const int level;
  const Slice key;
  const Slice value;
  const SequenceNumber seq_num;

  bool& output_to_proximal_level;

  PerKeyPlacementContext(int _level, Slice _key, Slice _value,
                         SequenceNumber _seq_num,
                         bool& _output_to_proximal_level)
      : level(_level),
        key(_key),
        value(_value),
        seq_num(_seq_num),
        output_to_proximal_level(_output_to_proximal_level) {}
};
#endif /* !NDEBUG */

// Return sum of sizes of all files in `files`.
uint64_t TotalFileSize(const std::vector<FileMetaData*>& files);

}  // namespace ROCKSDB_NAMESPACE
