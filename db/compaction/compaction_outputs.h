//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "db/blob/blob_garbage_meter.h"
#include "db/compaction/compaction.h"
#include "db/compaction/compaction_iterator.h"
#include "db/internal_stats.h"
#include "db/output_validator.h"

namespace ROCKSDB_NAMESPACE {

class CompactionOutputs;
using CompactionFileOpenFunc = std::function<Status(CompactionOutputs&)>;
using CompactionFileCloseFunc =
    std::function<Status(const Status&, const ParsedInternalKey&, const Slice&,
                         const CompactionIterator*, bool, CompactionOutputs&)>;

// Files produced by subcompaction, most of the functions are used by
// compaction_job Open/Close compaction file functions.
class CompactionOutputs {
 public:
  // compaction output file
  struct Output {
    Output(FileMetaData&& _meta, const InternalKeyComparator& _icmp,
           bool _enable_hash, bool _finished, uint64_t precalculated_hash,
           bool _is_proximal_level)
        : meta(std::move(_meta)),
          validator(_icmp, _enable_hash, precalculated_hash),
          finished(_finished),
          is_proximal_level(_is_proximal_level) {}
    FileMetaData meta;
    OutputValidator validator;
    bool finished;
    bool is_proximal_level;
    std::shared_ptr<const TableProperties> table_properties;
  };

  CompactionOutputs() = delete;

  explicit CompactionOutputs(const Compaction* compaction,
                             const bool is_proximal_level);

  bool IsProximalLevel() const { return is_proximal_level_; }

  // Add generated output to the list
  void AddOutput(FileMetaData&& meta, const InternalKeyComparator& icmp,
                 bool enable_hash, bool finished = false,
                 uint64_t precalculated_hash = 0) {
    outputs_.emplace_back(std::move(meta), icmp, enable_hash, finished,
                          precalculated_hash, is_proximal_level_);
  }

  const std::vector<Output>& GetOutputs() const { return outputs_; }
  std::vector<Output>& GetMutableOutputs() { return outputs_; }

  // Set new table builder for the current output
  void NewBuilder(const TableBuilderOptions& tboptions);

  // Assign a new WritableFileWriter to the current output
  void AssignFileWriter(WritableFileWriter* writer) {
    file_writer_.reset(writer);
  }

  // TODO: Move the BlobDB builder into CompactionOutputs
  const std::vector<BlobFileAddition>& GetBlobFileAdditions() const {
    if (is_proximal_level_) {
      assert(blob_file_additions_.empty());
    }
    return blob_file_additions_;
  }

  std::vector<BlobFileAddition>* GetBlobFileAdditionsPtr() {
    assert(!is_proximal_level_);
    return &blob_file_additions_;
  }

  bool HasBlobFileAdditions() const { return !blob_file_additions_.empty(); }

  // Get all file paths (SST and blob) created during compaction.
  const std::vector<std::string>& GetOutputFilePaths() const {
    return output_file_paths_;
  }

  std::vector<std::string>* GetOutputFilePathsPtr() {
    return &output_file_paths_;
  }

  void AddOutputFilePath(const std::string& path) {
    output_file_paths_.push_back(path);
  }

  BlobGarbageMeter* CreateBlobGarbageMeter() {
    assert(!is_proximal_level_);
    blob_garbage_meter_ = std::make_unique<BlobGarbageMeter>();
    return blob_garbage_meter_.get();
  }

  BlobGarbageMeter* GetBlobGarbageMeter() const {
    if (is_proximal_level_) {
      // blobdb doesn't support per_key_placement yet
      assert(blob_garbage_meter_ == nullptr);
      return nullptr;
    }
    return blob_garbage_meter_.get();
  }

  void UpdateBlobStats() {
    assert(!is_proximal_level_);
    stats_.num_output_files_blob =
        static_cast<int>(blob_file_additions_.size());
    for (const auto& blob : blob_file_additions_) {
      stats_.bytes_written_blob += blob.GetTotalBlobBytes();
    }
  }

  // Finish the current output file
  Status Finish(const Status& intput_status,
                const SeqnoToTimeMapping& seqno_to_time_mapping);

  // Update output table properties from already populated TableProperties.
  // Used for remote compaction
  void UpdateTableProperties(const TableProperties& table_properties) {
    current_output().table_properties =
        std::make_shared<TableProperties>(table_properties);
  }
  // Update output table properties from table builder
  void UpdateTableProperties() {
    current_output().table_properties =
        std::make_shared<TableProperties>(GetTableProperties());
  }

  IOStatus WriterSyncClose(const Status& intput_status, SystemClock* clock,
                           Statistics* statistics, bool use_fsync);

  TableProperties GetTableProperties() {
    return builder_->GetTableProperties();
  }

  Slice SmallestUserKey() const {
    if (!outputs_.empty() && outputs_[0].finished) {
      return outputs_[0].meta.smallest.user_key();
    } else {
      return Slice{nullptr, 0};
    }
  }

  Slice LargestUserKey() const {
    if (!outputs_.empty() && outputs_.back().finished) {
      return outputs_.back().meta.largest.user_key();
    } else {
      return Slice{nullptr, 0};
    }
  }

  // In case the last output file is empty, which doesn't need to keep.
  void RemoveLastEmptyOutput() {
    if (!outputs_.empty() && !outputs_.back().meta.fd.file_size) {
      // An error occurred, so ignore the last output.
      outputs_.pop_back();
    }
  }

  // Remove the last output, for example the last output doesn't have data (no
  // entry and no range-dels), but file_size might not be 0, as it has SST
  // metadata.
  void RemoveLastOutput() {
    assert(!outputs_.empty());
    outputs_.pop_back();
  }

  bool HasBuilder() const { return builder_ != nullptr; }

  FileMetaData* GetMetaData() { return &current_output().meta; }

  bool HasOutput() const { return !outputs_.empty(); }

  uint64_t NumEntries() const { return builder_->NumEntries(); }

  uint64_t GetWorkerCPUMicros() const {
    return worker_cpu_micros_ + (builder_ ? builder_->GetWorkerCPUMicros() : 0);
  }

  void ResetBuilder() {
    builder_.reset();
    current_output_file_size_ = 0;
  }

  // Add range deletions from the range_del_agg_ to the current output file.
  // Input parameters, `range_tombstone_lower_bound_` and current output's
  // metadata determine the bounds on range deletions to add. Updates output
  // file metadata boundary if extended by range tombstones.
  //
  // @param comp_start_user_key and comp_end_user_key include timestamp if
  // user-defined timestamp is enabled. Their timestamp should be max timestamp.
  // @param next_table_min_key internal key lower bound for the next compaction
  // output.
  // @param full_history_ts_low used for range tombstone garbage collection.
  Status AddRangeDels(
      CompactionRangeDelAggregator& range_del_agg,
      const Slice* comp_start_user_key, const Slice* comp_end_user_key,
      CompactionIterationStats& range_del_out_stats, bool bottommost_level,
      const InternalKeyComparator& icmp, SequenceNumber earliest_snapshot,
      std::pair<SequenceNumber, SequenceNumber> keep_seqno_range,
      const Slice& next_table_min_key, const std::string& full_history_ts_low);

  void SetNumOutputRecords(uint64_t num_output_records) {
    stats_.num_output_records = num_output_records;
  }

 private:
  friend class SubcompactionState;

  void FillFilesToCutForTtl();

  void SetOutputSlitKey(const std::optional<Slice> start,
                        const std::optional<Slice> end) {
    const InternalKeyComparator* icmp =
        &compaction_->column_family_data()->internal_comparator();

    const InternalKey* output_split_key = compaction_->GetOutputSplitKey();
    // Invalid output_split_key indicates that we do not need to split
    if (output_split_key != nullptr) {
      // We may only split the output when the cursor is in the range. Split
      if ((!end.has_value() ||
           icmp->user_comparator()->Compare(
               ExtractUserKey(output_split_key->Encode()), *end) < 0) &&
          (!start.has_value() ||
           icmp->user_comparator()->Compare(
               ExtractUserKey(output_split_key->Encode()), *start) > 0)) {
        local_output_split_key_ = output_split_key;
      }
    }
  }

  // Returns true iff we should stop building the current output
  // before processing the current key in compaction iterator.
  bool ShouldStopBefore(const CompactionIterator& c_iter);

  // Re-establishes per-file state that depends on the first key accepted by a
  // freshly opened builder. This keeps caller-requested file cuts equivalent to
  // RocksDB's built-in file-boundary decisions for partition tracking.
  void PrepareNewOutputForFirstKey(const CompactionIterator& c_iter);

  void Cleanup() {
    if (builder_ != nullptr) {
      // May happen if we get a shutdown call in the middle of compaction
      builder_->Abandon();
      builder_.reset();
    }
  }

  // Updates states related to file cutting for TTL.
  // Returns a boolean value indicating whether the current
  // compaction output file should be cut before `internal_key`.
  //
  // @param internal_key the current key to be added to output.
  bool UpdateFilesToCutForTTLStates(const Slice& internal_key);

  // update tracked grandparents information like grandparent index, if it's
  // in the gap between 2 grandparent files, accumulated grandparent files size
  // etc.
  // It returns how many boundaries it crosses by including current key.
  size_t UpdateGrandparentBoundaryInfo(const Slice& internal_key);

  // helper function to get the overlapped grandparent files size, it's only
  // used for calculating the first key's overlap.
  uint64_t GetCurrentKeyGrandparentOverlappedBytes(
      const Slice& internal_key) const;

  // Add current key from compaction_iterator to the output file. If needed
  // close and open new compaction output with the functions provided.
  Status AddToOutput(CompactionIterator& c_iter,
                     const CompactionFileOpenFunc& open_file_func,
                     const CompactionFileCloseFunc& close_file_func,
                     const ParsedInternalKey& prev_iter_output_internal_key);

  // Close the current output. `open_file_func` is needed for creating new file
  // for range-dels only output file.
  Status CloseOutput(const Status& curr_status,
                     CompactionRangeDelAggregator* range_del_agg,
                     const CompactionFileOpenFunc& open_file_func,
                     const CompactionFileCloseFunc& close_file_func) {
    Status status = curr_status;
    // Handle subcompaction containing only range deletions. They could
    // be dropped or sent to another output level, so this is only an
    // over-approximate check for whether opening is needed.
    if (status.ok() && !HasBuilder() && !HasOutput() && range_del_agg &&
        !range_del_agg->IsEmpty()) {
      status = open_file_func(*this);
    }

    if (HasBuilder()) {
      const ParsedInternalKey empty_internal_key{};
      const Slice empty_key{};
      Status s = close_file_func(status, empty_internal_key, empty_key,
                                 nullptr /* c_iter */,
                                 /*post_prepare_retry=*/false, *this);
      if (!s.ok() && status.ok()) {
        status = s;
      }
    }

    return status;
  }

  // This subcompaction's output could be empty if compaction was aborted before
  // this subcompaction had a chance to generate any output files. When
  // subcompactions are executed sequentially this is more likely and will be
  // particularly likely for the later subcompactions to be empty. Once they are
  // run in parallel however it should be much rarer.
  // It's caller's responsibility to make sure it's not empty.
  Output& current_output() {
    assert(!outputs_.empty());
    return outputs_.back();
  }

  const Compaction* compaction_;

  // current output builder and writer
  std::unique_ptr<TableBuilder> builder_;
  std::unique_ptr<WritableFileWriter> file_writer_;
  uint64_t current_output_file_size_ = 0;
  SequenceNumber smallest_preferred_seqno_ = kMaxSequenceNumber;

  // Sum of all the GetWorkerCPUMicros() for all the closed builders so far.
  uint64_t worker_cpu_micros_ = 0;

  // all the compaction outputs so far
  std::vector<Output> outputs_;

  // BlobDB info
  std::vector<BlobFileAddition> blob_file_additions_;
  std::unique_ptr<BlobGarbageMeter> blob_garbage_meter_;

  // All file paths (SST and blob) created during compaction.
  // Used for cleanup on abort - ensures orphan files are deleted even if
  // they were removed from outputs_ or blob_file_additions_ (e.g., by
  // RemoveLastEmptyOutput when file_size is 0 because builder was abandoned).
  std::vector<std::string> output_file_paths_;

  // Per level's output stat
  InternalStats::CompactionStats stats_;

  // indicate if this CompactionOutputs obj for proximal_level, should always
  // be false if per_key_placement feature is not enabled.
  const bool is_proximal_level_;

  // partitioner information
  std::unique_ptr<SstPartitioner> partitioner_;
  // Previous user key, only maintained (copied per key) in the fallback mode
  // (see below).
  std::string last_key_for_partitioner_;
  // Fast path for partitioners that support ShouldPartitionByPrefix(): when
  // partitioner_prefix_valid_ is true, all keys in the current output file
  // share partitioner_prefix_, and a partition boundary is requested at the
  // first future key that does not start with that prefix. This is (re)derived
  // once per output file from its first key, avoiding a per-key copy and
  // virtual ShouldPartition() call. When false, we fall back to comparing
  // against last_key_for_partitioner_ via ShouldPartition() on every key.
  std::string partitioner_prefix_;
  bool partitioner_prefix_valid_ = false;

  // A flag determines if this subcompaction has been split by the cursor
  // for RoundRobin compaction
  bool is_split_ = false;

  // We also maintain the output split key for each subcompaction to avoid
  // repetitive comparison in ShouldStopBefore()
  const InternalKey* local_output_split_key_ = nullptr;

  // Some identified files with old oldest ancestor time and the range should be
  // isolated out so that the output file(s) in that range can be merged down
  // for TTL and clear the timestamps for the range.
  std::vector<FileMetaData*> files_to_cut_for_ttl_;
  int cur_files_to_cut_for_ttl_ = -1;
  int next_files_to_cut_for_ttl_ = 0;

  // An index that used to speed up ShouldStopBefore().
  size_t grandparent_index_ = 0;

  // if the output key is being grandparent files gap, so:
  //  key > grandparents[grandparent_index_ - 1].largest &&
  //  key < grandparents[grandparent_index_].smallest
  bool being_grandparent_gap_ = true;

  // The number of bytes overlapping between the current output and
  // grandparent files used in ShouldStopBefore().
  uint64_t grandparent_overlapped_bytes_ = 0;

  // A flag determines whether the key has been seen in ShouldStopBefore()
  bool seen_key_ = false;

  // for the current output file, how many file boundaries has it crossed,
  // basically number of files overlapped * 2
  size_t grandparent_boundary_switched_num_ = 0;

  // The smallest key of the current output file, this is set when current
  // output file's smallest key is a range tombstone start key.
  InternalKey range_tombstone_lower_bound_;

  // Used for calls to compaction->KeyRangeNotExistsBeyondOutputLevel() in
  // CompactionOutputs::AddRangeDels().
  // level_ptrs_[i] holds index of the file that was checked during the last
  // call to compaction->KeyRangeNotExistsBeyondOutputLevel(). This allows
  // future calls to the function to pick up where it left off, since each
  // range tombstone added to output file within each subcompaction is in
  // increasing key range.
  std::vector<size_t> level_ptrs_;
};

// helper struct to concatenate the last level and proximal level outputs
// which could be replaced by std::ranges::join_view() in c++20
struct OutputIterator {
 public:
  explicit OutputIterator(const std::vector<CompactionOutputs::Output>& a,
                          const std::vector<CompactionOutputs::Output>& b)
      : a_(a), b_(b) {
    within_a = !a_.empty();
    idx_ = 0;
  }

  OutputIterator begin() { return *this; }

  OutputIterator end() { return *this; }

  size_t size() { return a_.size() + b_.size(); }

  const CompactionOutputs::Output& operator*() const {
    return within_a ? a_[idx_] : b_[idx_];
  }

  OutputIterator& operator++() {
    idx_++;
    if (within_a && idx_ >= a_.size()) {
      within_a = false;
      idx_ = 0;
    }
    assert(within_a || idx_ <= b_.size());
    return *this;
  }

  bool operator!=(const OutputIterator& /*rhs*/) const {
    return within_a || idx_ < b_.size();
  }

 private:
  const std::vector<CompactionOutputs::Output>& a_;
  const std::vector<CompactionOutputs::Output>& b_;
  bool within_a;
  size_t idx_;
};

}  // namespace ROCKSDB_NAMESPACE
