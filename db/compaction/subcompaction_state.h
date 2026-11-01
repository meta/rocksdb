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

#include <optional>

#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_garbage_meter.h"
#include "db/compaction/compaction.h"
#include "db/compaction/compaction_iterator.h"
#include "db/compaction/compaction_outputs.h"
#include "db/internal_stats.h"
#include "db/output_validator.h"
#include "db/range_del_aggregator.h"

namespace ROCKSDB_NAMESPACE {

// Maintains state and outputs for each sub-compaction
// It contains 2 `CompactionOutputs`:
//  1. one for the normal output files
//  2. another for the proximal level outputs
// a `current` pointer maintains the current output group, when calling
// `AddToOutput()`, it checks the output of the current compaction_iterator key
// and point `current` to the target output group. By default, it just points to
// normal compaction_outputs, if the compaction_iterator key should be placed on
// the proximal level, `current` is changed to point to
// `proximal_level_outputs`.
// The later operations uses `Current()` to get the target group.
//
// +----------+          +-----------------------------+      +---------+
// | *current |--------> | compaction_outputs          |----->| output  |
// +----------+          +-----------------------------+      +---------+
//       |                                                    | output  |
//       |                                                    +---------+
//       |                                                    |  ...    |
//       |
//       |               +-----------------------------+      +---------+
//       +-------------> | proximal_level_outputs      |----->| output  |
//                       +-----------------------------+      +---------+
//                                                            |  ...    |

class SubcompactionState {
 public:
  const Compaction* compaction;

  // The boundaries of the key-range this compaction is interested in. No two
  // sub-compactions may have overlapping key-ranges.
  // 'start' is inclusive, 'end' is exclusive, and nullptr means unbounded
  const std::optional<Slice> start, end;

  // The return status of this sub-compaction
  Status status;

  // The return IO Status of this sub-compaction
  IOStatus io_status;

  // Notify on sub-compaction completion only if listener was notified on
  // sub-compaction begin.
  bool notify_on_subcompaction_completion = false;

  // compaction job stats for this sub-compaction
  CompactionJobStats compaction_job_stats;

  // sub-compaction job id, which is used to identify different sub-compaction
  // within the same compaction job.
  const uint32_t sub_job_id;

  Slice SmallestUserKey() const;

  Slice LargestUserKey() const;

  // Get all outputs from the subcompaction. For per_key_placement compaction,
  // it returns both the last level outputs and proximal level outputs.
  OutputIterator GetOutputs() const;

  // Get mutable access to outputs for post-processing (e.g., retrieving
  // file open metadata).
  std::vector<CompactionOutputs::Output>& GetMutableCompactionOutputs() {
    return compaction_outputs_.GetMutableOutputs();
  }
  std::vector<CompactionOutputs::Output>& GetMutableProximalOutputs() {
    return proximal_level_outputs_.GetMutableOutputs();
  }

  // Assign range dels aggregator. The various tombstones will potentially
  // be filtered to different outputs.
  void AssignRangeDelAggregator(
      std::unique_ptr<CompactionRangeDelAggregator>&& range_del_agg) {
    assert(range_del_agg_ == nullptr);
    assert(range_del_agg);
    range_del_agg_ = std::move(range_del_agg);
  }

  void RemoveLastEmptyOutput() {
    compaction_outputs_.RemoveLastEmptyOutput();
    proximal_level_outputs_.RemoveLastEmptyOutput();
  }

  // Cleanup output builders for abandoning in-progress files.
  void CleanupOutputs() {
    compaction_outputs_.Cleanup();
    if (compaction->SupportsPerKeyPlacement()) {
      proximal_level_outputs_.Cleanup();
    }
  }

  void BuildSubcompactionJobInfo(
      SubcompactionJobInfo& subcompaction_job_info) const {
    const Compaction* c = compaction;
    const ColumnFamilyData* cfd = c->column_family_data();

    subcompaction_job_info.cf_id = cfd->GetID();
    subcompaction_job_info.cf_name = cfd->GetName();
    subcompaction_job_info.status = status;
    subcompaction_job_info.subcompaction_job_id = static_cast<int>(sub_job_id);
    subcompaction_job_info.base_input_level = c->start_level();
    subcompaction_job_info.output_level = c->output_level();
    subcompaction_job_info.compaction_reason = c->compaction_reason();
    subcompaction_job_info.compression = c->output_compression();
    subcompaction_job_info.stats = compaction_job_stats;
    subcompaction_job_info.blob_compression_type =
        c->mutable_cf_options().blob_compression_type;
  }

  SubcompactionState() = delete;
  SubcompactionState(const SubcompactionState&) = delete;
  SubcompactionState& operator=(const SubcompactionState&) = delete;

  SubcompactionState(Compaction* c, const std::optional<Slice> _start,
                     const std::optional<Slice> _end, uint32_t _sub_job_id)
      : compaction(c),
        start(_start),
        end(_end),
        sub_job_id(_sub_job_id),
        compaction_outputs_(c, /*is_proximal_level=*/false),
        proximal_level_outputs_(c, /*is_proximal_level=*/true) {
    assert(compaction != nullptr);
    // Set output split key (used for RoundRobin feature) only for normal
    // compaction_outputs, output to proximal_level feature doesn't support
    // RoundRobin feature (and may never going to be supported, because for
    // RoundRobin, the data time is mostly naturally sorted, no need to have
    // per-key placement with output_to_proximal_level).
    compaction_outputs_.SetOutputSlitKey(start, end);
  }

  SubcompactionState(SubcompactionState&& state) noexcept
      : compaction(state.compaction),
        start(state.start),
        end(state.end),
        status(std::move(state.status)),
        io_status(std::move(state.io_status)),
        notify_on_subcompaction_completion(
            state.notify_on_subcompaction_completion),
        compaction_job_stats(std::move(state.compaction_job_stats)),
        sub_job_id(state.sub_job_id),
        compaction_outputs_(std::move(state.compaction_outputs_)),
        proximal_level_outputs_(std::move(state.proximal_level_outputs_)),
        range_del_agg_(std::move(state.range_del_agg_)),
        blob_garbage_checkpoint_(std::move(state.blob_garbage_checkpoint_)) {
    current_outputs_ = state.current_outputs_ == &state.proximal_level_outputs_
                           ? &proximal_level_outputs_
                           : &compaction_outputs_;
  }
  SubcompactionState& operator=(SubcompactionState&&) = delete;

  ~SubcompactionState() = default;

  // Add all the new files from this compaction to version_edit
  void AddOutputsEdit(VersionEdit* out_edit) const {
    for (const auto& file : proximal_level_outputs_.outputs_) {
      out_edit->AddFile(compaction->GetProximalLevel(), file.meta);
    }
    for (const auto& file : compaction_outputs_.outputs_) {
      out_edit->AddFile(compaction->output_level(), file.meta);
    }
  }

  void Cleanup(Cache* cache, const Status& overall_status);

  void AggregateCompactionOutputStats(
      InternalStats::CompactionStatsFull& internal_stats) const;

  CompactionOutputs& Current() const {
    assert(current_outputs_);
    return *current_outputs_;
  }

  CompactionOutputs* Outputs(bool is_proximal_level) {
    assert(compaction);
    if (is_proximal_level) {
      assert(compaction->SupportsPerKeyPlacement());
      return &proximal_level_outputs_;
    }
    return &compaction_outputs_;
  }

  // Per-level stats for the output
  InternalStats::CompactionStats* OutputStats(bool is_proximal_level) {
    assert(compaction);
    if (is_proximal_level) {
      assert(compaction->SupportsPerKeyPlacement());
      return &proximal_level_outputs_.stats_;
    }
    return &compaction_outputs_.stats_;
  }

  uint64_t GetWorkerCPUMicros() const {
    uint64_t rv = compaction_outputs_.GetWorkerCPUMicros();
    if (compaction->SupportsPerKeyPlacement()) {
      rv += proximal_level_outputs_.GetWorkerCPUMicros();
    }
    return rv;
  }

  CompactionRangeDelAggregator* RangeDelAgg() const {
    return range_del_agg_.get();
  }

  // if the outputs have range delete, range delete is also data
  bool HasRangeDel() const {
    return range_del_agg_ && !range_del_agg_->IsEmpty();
  }

  void SetSubcompactionProgress(
      const SubcompactionProgress& subcompaction_progress) {
    subcompaction_progress_ = subcompaction_progress;
  }

  SubcompactionProgress& GetSubcompactionProgressRef() {
    return subcompaction_progress_;
  }

  // Add compaction_iterator key/value to the `Current` output group.
  Status AddToOutput(CompactionIterator& iter, bool use_proximal_output,
                     const CompactionFileOpenFunc& open_file_func,
                     const CompactionFileCloseFunc& close_file_func,
                     const ParsedInternalKey& prev_iter_output_internal_key);

  Status UpdateBlobGarbageCheckpoint(const BlobGarbageMeter* next_key_inflow);
  const BlobGarbageMeter* GetBlobGarbageCheckpoint() const {
    return blob_garbage_checkpoint_.get();
  }

  // Close all compaction output files, both output_to_proximal_level outputs
  // and normal outputs.
  Status CloseCompactionFiles(const Status& curr_status,
                              const CompactionFileOpenFunc& open_file_func,
                              const CompactionFileCloseFunc& close_file_func) {
    auto per_key = compaction->SupportsPerKeyPlacement();
    // Call FinishCompactionOutputFile() even if status is not ok: it needs to
    // close the output file.
    // CloseOutput() may open new compaction output files.
    Status s = curr_status;
    if (per_key) {
      s = proximal_level_outputs_.CloseOutput(s, range_del_agg_.get(),
                                              open_file_func, close_file_func);
    } else {
      assert(proximal_level_outputs_.HasBuilder() == false);
      assert(proximal_level_outputs_.HasOutput() == false);
    }
    s = compaction_outputs_.CloseOutput(s, range_del_agg_.get(), open_file_func,
                                        close_file_func);
    return s;
  }

 private:
  // State kept for output being generated
  CompactionOutputs compaction_outputs_;
  CompactionOutputs proximal_level_outputs_;
  CompactionOutputs* current_outputs_ = &compaction_outputs_;
  std::unique_ptr<CompactionRangeDelAggregator> range_del_agg_;
  std::unique_ptr<BlobGarbageMeter> blob_garbage_checkpoint_;

  SubcompactionProgress subcompaction_progress_;
};

}  // namespace ROCKSDB_NAMESPACE
