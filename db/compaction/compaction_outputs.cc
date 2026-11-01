//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_outputs.h"

#include <inttypes.h>

#include "db/builder.h"
#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE {

void CompactionOutputs::NewBuilder(const TableBuilderOptions& tboptions) {
  builder_.reset(NewTableBuilder(tboptions, file_writer_.get()));
}

Status CompactionOutputs::Finish(
    const Status& intput_status,
    const SeqnoToTimeMapping& seqno_to_time_mapping) {
  FileMetaData* meta = GetMetaData();
  assert(meta != nullptr);
  Status s = intput_status;
  if (s.ok()) {
    SeqnoToTimeMapping relevant_mapping;
    relevant_mapping.CopyFromSeqnoRange(
        seqno_to_time_mapping,
        std::min(smallest_preferred_seqno_, meta->fd.smallest_seqno),
        meta->fd.largest_seqno);
    relevant_mapping.SetCapacity(kMaxSeqnoTimePairsPerSST);
    builder_->SetSeqnoTimeTableProperties(relevant_mapping,
                                          meta->oldest_ancestor_time);
    s = builder_->Finish();

  } else {
    builder_->Abandon();
  }
  Status io_s = builder_->io_status();
  if (s.ok()) {
    s = io_s;
  } else {
    io_s.PermitUncheckedError();
  }
  const uint64_t current_bytes = builder_->FileSize();
  if (s.ok()) {
    meta->fd.file_size = current_bytes;
    meta->tail_size = builder_->GetTailSize();
    meta->marked_for_compaction = builder_->NeedCompact();
    const TableProperties& tp = builder_->GetTableProperties();
    meta->user_defined_timestamps_persisted =
        static_cast<bool>(tp.user_defined_timestamps_persisted);
    ExtractTimestampFromTableProperties(tp, meta);
  }
  current_output().finished = true;
  stats_.bytes_written += current_bytes;
  stats_.bytes_written_pre_comp += builder_->PreCompressionSize();
  stats_.num_output_files = static_cast<int>(outputs_.size());
  worker_cpu_micros_ += builder_->GetWorkerCPUMicros();

  return s;
}

IOStatus CompactionOutputs::WriterSyncClose(const Status& input_status,
                                            SystemClock* clock,
                                            Statistics* statistics,
                                            bool use_fsync) {
  IOStatus io_s;
  IOOptions opts;
  io_s = WritableFileWriter::PrepareIOOptions(
      WriteOptions(Env::IOActivity::kCompaction), opts);
  if (input_status.ok() && io_s.ok()) {
    StopWatch sw(clock, statistics, COMPACTION_OUTFILE_SYNC_MICROS);
    io_s = use_fsync ? file_writer_->Fsync(opts) : file_writer_->Sync(opts);
  }
  if (input_status.ok() && io_s.ok()) {
    io_s = file_writer_->Close(opts);
  }

  if (input_status.ok() && io_s.ok()) {
    FileMetaData* meta = GetMetaData();
    meta->file_checksum = file_writer_->GetFileChecksum();
    meta->file_checksum_func_name = file_writer_->GetFileChecksumFuncName();
  }

  file_writer_.reset();

  return io_s;
}

bool CompactionOutputs::UpdateFilesToCutForTTLStates(
    const Slice& internal_key) {
  if (!files_to_cut_for_ttl_.empty()) {
    const InternalKeyComparator* icmp =
        &compaction_->column_family_data()->internal_comparator();
    if (cur_files_to_cut_for_ttl_ != -1) {
      // Previous key is inside the range of a file
      if (icmp->Compare(internal_key,
                        files_to_cut_for_ttl_[cur_files_to_cut_for_ttl_]
                            ->largest.Encode()) > 0) {
        next_files_to_cut_for_ttl_ = cur_files_to_cut_for_ttl_ + 1;
        cur_files_to_cut_for_ttl_ = -1;
        return true;
      }
    } else {
      // Look for the key position
      while (next_files_to_cut_for_ttl_ <
             static_cast<int>(files_to_cut_for_ttl_.size())) {
        if (icmp->Compare(internal_key,
                          files_to_cut_for_ttl_[next_files_to_cut_for_ttl_]
                              ->smallest.Encode()) >= 0) {
          if (icmp->Compare(internal_key,
                            files_to_cut_for_ttl_[next_files_to_cut_for_ttl_]
                                ->largest.Encode()) <= 0) {
            // With in the current file
            cur_files_to_cut_for_ttl_ = next_files_to_cut_for_ttl_;
            return true;
          }
          // Beyond the current file
          next_files_to_cut_for_ttl_++;
        } else {
          // Still fall into the gap
          break;
        }
      }
    }
  }
  return false;
}

size_t CompactionOutputs::UpdateGrandparentBoundaryInfo(
    const Slice& internal_key) {
  size_t curr_key_boundary_switched_num = 0;
  const std::vector<FileMetaData*>& grandparents = compaction_->grandparents();

  if (grandparents.empty()) {
    return curr_key_boundary_switched_num;
  }
  const Comparator* ucmp = compaction_->column_family_data()->user_comparator();

  // Move the grandparent_index_ to the file containing the current user_key.
  // If there are multiple files containing the same user_key, make sure the
  // index points to the last file containing the key.
  while (grandparent_index_ < grandparents.size()) {
    if (being_grandparent_gap_) {
      if (sstableKeyCompare(ucmp, internal_key,
                            grandparents[grandparent_index_]->smallest) < 0) {
        break;
      }
      if (seen_key_) {
        curr_key_boundary_switched_num++;
        grandparent_overlapped_bytes_ +=
            grandparents[grandparent_index_]->fd.GetFileSize();
        grandparent_boundary_switched_num_++;
      }
      being_grandparent_gap_ = false;
    } else {
      int cmp_result = sstableKeyCompare(
          ucmp, internal_key, grandparents[grandparent_index_]->largest);
      // If it's same key, make sure grandparent_index_ is pointing to the last
      // one.
      if (cmp_result < 0 ||
          (cmp_result == 0 &&
           (grandparent_index_ == grandparents.size() - 1 ||
            sstableKeyCompare(ucmp, internal_key,
                              grandparents[grandparent_index_ + 1]->smallest) <
                0))) {
        break;
      }
      if (seen_key_) {
        curr_key_boundary_switched_num++;
        grandparent_boundary_switched_num_++;
      }
      being_grandparent_gap_ = true;
      grandparent_index_++;
    }
  }

  // If the first key is in the middle of a grandparent file, adding it to the
  // overlap
  if (!seen_key_ && !being_grandparent_gap_) {
    assert(grandparent_overlapped_bytes_ == 0);
    grandparent_overlapped_bytes_ =
        GetCurrentKeyGrandparentOverlappedBytes(internal_key);
  }

  seen_key_ = true;
  return curr_key_boundary_switched_num;
}

uint64_t CompactionOutputs::GetCurrentKeyGrandparentOverlappedBytes(
    const Slice& internal_key) const {
  // no overlap with any grandparent file
  if (being_grandparent_gap_) {
    return 0;
  }
  uint64_t overlapped_bytes = 0;

  const std::vector<FileMetaData*>& grandparents = compaction_->grandparents();
  const Comparator* ucmp = compaction_->column_family_data()->user_comparator();
  InternalKey ikey;
  ikey.DecodeFrom(internal_key);
#ifndef NDEBUG
  // make sure the grandparent_index_ is pointing to the last files containing
  // the current key.
  int cmp_result =
      sstableKeyCompare(ucmp, ikey, grandparents[grandparent_index_]->largest);
  assert(
      cmp_result < 0 ||
      (cmp_result == 0 &&
       (grandparent_index_ == grandparents.size() - 1 ||
        sstableKeyCompare(
            ucmp, ikey, grandparents[grandparent_index_ + 1]->smallest) < 0)));
  assert(sstableKeyCompare(ucmp, ikey,
                           grandparents[grandparent_index_]->smallest) >= 0);
#endif
  overlapped_bytes += grandparents[grandparent_index_]->fd.GetFileSize();

  // go backwards to find all overlapped files, one key can overlap multiple
  // files. In the following example, if the current output key is `c`, and one
  // compaction file was cut before `c`, current `c` can overlap with 3 files:
  //  [a b]               [c...
  // [b, b] [c, c] [c, c] [c, d]
  for (int64_t i = static_cast<int64_t>(grandparent_index_) - 1;
       i >= 0 && sstableKeyCompare(ucmp, ikey, grandparents[i]->largest) == 0;
       i--) {
    overlapped_bytes += grandparents[i]->fd.GetFileSize();
  }

  return overlapped_bytes;
}

bool CompactionOutputs::ShouldStopBefore(const CompactionIterator& c_iter) {
  assert(c_iter.Valid());
  const Slice& internal_key = c_iter.key();
#ifndef NDEBUG
  bool should_stop = false;
  std::pair<bool*, const Slice> p{&should_stop, internal_key};
  TEST_SYNC_POINT_CALLBACK(
      "CompactionOutputs::ShouldStopBefore::manual_decision", (void*)&p);
  if (should_stop) {
    return true;
  }
#endif  // NDEBUG
  const uint64_t previous_overlapped_bytes = grandparent_overlapped_bytes_;
  const InternalKeyComparator* icmp =
      &compaction_->column_family_data()->internal_comparator();
  size_t num_grandparent_boundaries_crossed = 0;
  bool should_stop_for_ttl = false;
  // Always update grandparent information like overlapped file number, size
  // etc., and TTL states.
  // If compaction_->output_level() == 0, there is no need to update grandparent
  // info, and that `grandparent` should be empty.
  if (compaction_->output_level() > 0) {
    num_grandparent_boundaries_crossed =
        UpdateGrandparentBoundaryInfo(internal_key);
    should_stop_for_ttl = UpdateFilesToCutForTTLStates(internal_key);
  }

  if (!HasBuilder()) {
    return false;
  }

  if (should_stop_for_ttl) {
    return true;
  }

  if (partitioner_) {
    if (partitioner_prefix_valid_) {
      // Fast path: no per-key copy or virtual call. A partition is requested at
      // the first key that no longer shares the established prefix. An empty
      // prefix means no further partitions (every key "starts with" it).
      if (UNLIKELY(!c_iter.user_key().starts_with(partitioner_prefix_))) {
        return true;
      }
    } else {
      // Slow path: per-key callback (and copy elsewhere)
      if (UNLIKELY(partitioner_->ShouldPartition(PartitionerRequest(
                       last_key_for_partitioner_, c_iter.user_key(),
                       current_output_file_size_)) == kRequired)) {
        return true;
      }
    }
  }

  // files output to Level 0 won't be split
  if (compaction_->output_level() == 0) {
    return false;
  }

  // reach the max file size
  uint64_t estimated_file_size = current_output_file_size_;
  if (compaction_->mutable_cf_options().target_file_size_is_upper_bound) {
    estimated_file_size += builder_->EstimatedTailSize();
  }
  if (estimated_file_size >= compaction_->max_output_file_size()) {
    return true;
  }

  // Check if it needs to split for RoundRobin
  // Invalid local_output_split_key indicates that we do not need to split
  if (local_output_split_key_ != nullptr && !is_split_) {
    // Split occurs when the next key is larger than/equal to the cursor
    if (icmp->Compare(internal_key, local_output_split_key_->Encode()) >= 0) {
      is_split_ = true;
      return true;
    }
  }

  // only check if the current key is going to cross the grandparents file
  // boundary (either the file beginning or ending).
  if (num_grandparent_boundaries_crossed > 0) {
    // Cut the file before the current key if the size of the current output
    // file + its overlapped grandparent files is bigger than
    // max_compaction_bytes. Which is to prevent future bigger than
    // max_compaction_bytes compaction from the current output level.
    if (grandparent_overlapped_bytes_ + current_output_file_size_ >
        compaction_->max_compaction_bytes()) {
      return true;
    }

    // Cut the file if including the key is going to add a skippable file on
    // the grandparent level AND its size is reasonably big (1/8 of target file
    // size). For example, if it's compacting the files L0 + L1:
    //  L0:  [1,   21]
    //  L1:    [3,   23]
    //  L2: [2, 4] [11, 15] [22, 24]
    // Without this break, it will output as:
    //  L1: [1,3, 21,23]
    // With this break, it will output as (assuming [11, 15] at L2 is bigger
    // than 1/8 of target size):
    //  L1: [1,3] [21,23]
    // Then for the future compactions, [11,15] won't be included.
    // For random datasets (either evenly distributed or skewed), it rarely
    // triggers this condition, but if the user is adding 2 different datasets
    // without any overlap, it may likely happen.
    // More details, check PR #1963
    const size_t num_skippable_boundaries_crossed =
        being_grandparent_gap_ ? 2 : 3;
    if (compaction_->immutable_options().compaction_style ==
            kCompactionStyleLevel &&
        num_grandparent_boundaries_crossed >=
            num_skippable_boundaries_crossed &&
        grandparent_overlapped_bytes_ - previous_overlapped_bytes >
            compaction_->target_output_file_size() / 8) {
      return true;
    }

    // Pre-cut the output file if it's reaching a certain size AND it's at the
    // boundary of a grandparent file. It can reduce the future compaction size,
    // the cost is having smaller files.
    // The pre-cut size threshold is based on how many grandparent boundaries
    // it has seen before. Basically, if it has seen no boundary at all, then it
    // will pre-cut at 50% target file size. Every boundary it has seen
    // increases the threshold by 5%, max at 90%, which it will always cut.
    // The idea is based on if it has seen more boundaries before, it will more
    // likely to see another boundary (file cutting opportunity) before the
    // target file size. The test shows it can generate larger files than a
    // static threshold like 75% and has a similar write amplification
    // improvement.
    if (compaction_->immutable_options().compaction_style ==
            kCompactionStyleLevel &&
        current_output_file_size_ >=
            ((compaction_->target_output_file_size() + 99) / 100) *
                (50 + std::min(grandparent_boundary_switched_num_ * 5,
                               size_t{40}))) {
      return true;
    }
  }

  return false;
}

void CompactionOutputs::PrepareNewOutputForFirstKey(
    const CompactionIterator& c_iter) {
  // A new output file starts a fresh partition-tracking window. Establish the
  // partitioner fast path from this file's first key: if the partitioner can
  // express its next boundary as "first key not sharing this prefix", cache
  // that prefix; otherwise fall back to per-key ShouldPartition(). This is the
  // only place the prefix needs to be (re)derived, since a partition boundary
  // always forces a file cut, and re-deriving on non-partition cuts just
  // recomputes the same still-valid prefix.
  if (partitioner_) {
    OptSlice prefix = partitioner_->ShouldPartitionByPrefix(c_iter.user_key());
    partitioner_prefix_valid_ = prefix.has_value();
    if (partitioner_prefix_valid_) {
      partitioner_prefix_.assign(prefix->data(), prefix->size());
    }
  }
}

Status CompactionOutputs::AddToOutput(
    CompactionIterator& c_iter, const CompactionFileOpenFunc& open_file_func,
    const CompactionFileCloseFunc& close_file_func,
    const ParsedInternalKey& prev_iter_output_internal_key) {
  Status s;
  bool is_range_del = c_iter.IsDeleteRangeSentinelKey();
  if (is_range_del && compaction_->bottommost_level()) {
    // We don't consider range tombstone for bottommost level since:
    // 1. there is no grandparent and hence no overlap to consider
    // 2. range tombstone may be dropped at bottommost level.
    return s;
  }
  if (ShouldStopBefore(c_iter) && HasBuilder()) {
    const Slice next_table_min_key = c_iter.key();
    s = close_file_func(c_iter.InputStatus(), prev_iter_output_internal_key,
                        next_table_min_key, &c_iter,
                        /*post_prepare_retry=*/false, *this);
    if (!s.ok()) {
      return s;
    }
    // reset grandparent information
    grandparent_boundary_switched_num_ = 0;
    grandparent_overlapped_bytes_ =
        GetCurrentKeyGrandparentOverlappedBytes(next_table_min_key);
    if (UNLIKELY(is_range_del)) {
      // lower bound for this new output file, this is needed as the lower bound
      // does not come from the smallest point key in this case.
      range_tombstone_lower_bound_.DecodeFrom(next_table_min_key);
    } else {
      range_tombstone_lower_bound_.Clear();
    }
  }

  c_iter.PrepareOutputIfNeeded();
  if (!c_iter.status().ok() || !c_iter.Valid()) {
    s = c_iter.status();
    return s;
  }
  const Slice& key = c_iter.key();

  // Open output file if necessary
  if (!HasBuilder()) {
    s = open_file_func(*this);
    if (!s.ok()) {
      return s;
    }
    PrepareNewOutputForFirstKey(c_iter);
  }

  if (UNLIKELY(is_range_del)) {
    // c_iter may emit range deletion keys, so update
    // `last_key_for_partitioner_` before returning. Only needed in the fallback
    // mode; the prefix fast path does not use it.
    if (partitioner_ && !partitioner_prefix_valid_) {
      last_key_for_partitioner_.assign(c_iter.user_key().data_,
                                       c_iter.user_key().size_);
    }
    return s;
  }

  assert(builder_ != nullptr);
  const Slice& value = c_iter.value();
  bool retried_after_new_table = false;
  while (true) {
    s = current_output().validator.CanAdd(key);
    if (!s.ok()) {
      return s;
    }

    TableBuilderAddContext add_context;
    const TableBuilderAddResult add_result =
        builder_->TryAdd(key, value, &add_context);

    if (add_result == TableBuilderAddResult::kError) {
      s = builder_->status();
      if (s.ok()) {
        return Status::Corruption(
            "TableBuilder returned kError without a non-OK status");
      }
      return s;
    }

    if (add_result == TableBuilderAddResult::kAdded) {
      current_output().validator.AddValidated(key, value);
      break;
    }

    assert(add_result == TableBuilderAddResult::kRequiresNewTable);
    s = builder_->status();
    if (!s.ok()) {
      return s;
    }
    const char* const new_table_reason =
        add_context.new_table_reason == TableBuilderNewTableReason::kNone
            ? "Unspecified"
            : TableBuilderNewTableReasonToString(add_context.new_table_reason);
    const Compaction* compaction = compaction_;
    if (compaction == nullptr) {
      return Status::Corruption("Compaction output is missing compaction");
    }
    ColumnFamilyData* cfd = compaction->column_family_data();
    if (cfd == nullptr) {
      return Status::Corruption(
          "Compaction output is missing column family data");
    }
    const bool allow_data_in_errors =
        compaction->immutable_options().allow_data_in_errors;
    const std::string key_for_diagnostics =
        allow_data_in_errors ? key.ToString(/* hex */ true) : "hidden";
    if (compaction->output_level() == 0) {
      const std::string detail =
          "cf: " + cfd->GetName() + ", file_number: " +
          std::to_string(current_output().meta.fd.GetNumber()) +
          ", reason: " + new_table_reason + ", key: " + key_for_diagnostics;
      return Status::Corruption(
          "TableBuilder requested a new table while producing level 0 output",
          detail);
    }
    if (retried_after_new_table || builder_->IsEmpty()) {
      const std::string detail =
          "cf: " + cfd->GetName() + ", file_number: " +
          std::to_string(current_output().meta.fd.GetNumber()) +
          ", reason: " + new_table_reason + ", key: " + key_for_diagnostics;
      return Status::Corruption(
          "TableBuilder requested a new table without accepting any rows",
          detail);
    }

    ROCKS_LOG_INFO(
        compaction->immutable_options().info_log,
        "[%s] Compaction output table builder requested a new table before "
        "key %s for file #%" PRIu64 ". reason: %s",
        cfd->GetName().c_str(), key_for_diagnostics.c_str(),
        current_output().meta.fd.GetNumber(), new_table_reason);

    s = close_file_func(c_iter.InputStatus(), prev_iter_output_internal_key,
                        key, &c_iter, /*post_prepare_retry=*/true, *this);
    if (!s.ok()) {
      return s;
    }

    grandparent_boundary_switched_num_ = 0;
    grandparent_overlapped_bytes_ =
        GetCurrentKeyGrandparentOverlappedBytes(key);
    range_tombstone_lower_bound_.Clear();

    s = open_file_func(*this);
    if (!s.ok()) {
      return s;
    }
    PrepareNewOutputForFirstKey(c_iter);
    retried_after_new_table = true;
  }

  if (partitioner_ && !partitioner_prefix_valid_) {
    last_key_for_partitioner_.assign(c_iter.user_key().data_,
                                     c_iter.user_key().size_);
  }

  stats_.num_output_records++;
  current_output_file_size_ = builder_->EstimatedFileSize();

  if (blob_garbage_meter_) {
    s = blob_garbage_meter_->ProcessOutFlow(key, value);
  }

  if (!s.ok()) {
    return s;
  }

  const ParsedInternalKey& ikey = c_iter.ikey();
  if (ikey.type == kTypeValuePreferredSeqno) {
    SequenceNumber preferred_seqno = ParsePackedValueForSeqno(value);
    smallest_preferred_seqno_ =
        std::min(smallest_preferred_seqno_, preferred_seqno);
  }
  s = current_output().meta.UpdateBoundaries(key, value, ikey.sequence,
                                             ikey.type);

  return s;
}

namespace {
void SetMaxSeqAndTs(InternalKey& internal_key, const Slice& user_key,
                    const size_t ts_sz) {
  if (ts_sz) {
    static constexpr char kTsMax[] = "\xff\xff\xff\xff\xff\xff\xff\xff\xff";
    if (ts_sz <= strlen(kTsMax)) {
      internal_key = InternalKey(user_key, kMaxSequenceNumber,
                                 kTypeRangeDeletion, Slice(kTsMax, ts_sz));
    } else {
      internal_key =
          InternalKey(user_key, kMaxSequenceNumber, kTypeRangeDeletion,
                      std::string(ts_sz, '\xff'));
    }
  } else {
    internal_key.Set(user_key, kMaxSequenceNumber, kTypeRangeDeletion);
  }
}
}  // namespace

Status CompactionOutputs::AddRangeDels(
    CompactionRangeDelAggregator& range_del_agg,
    const Slice* comp_start_user_key, const Slice* comp_end_user_key,
    CompactionIterationStats& range_del_out_stats, bool bottommost_level,
    const InternalKeyComparator& icmp, SequenceNumber earliest_snapshot,
    std::pair<SequenceNumber, SequenceNumber> keep_seqno_range,
    const Slice& next_table_min_key, const std::string& full_history_ts_low) {
  // The following example does not happen since
  // CompactionOutput::ShouldStopBefore() always return false for the first
  // point key. But we should consider removing this dependency. Suppose for the
  // first compaction output file,
  //  - next_table_min_key.user_key == comp_start_user_key
  //  - no point key is in the output file
  //  - there is a range tombstone @seqno to be added that covers
  //  comp_start_user_key
  // Then meta.smallest will be set to comp_start_user_key@seqno
  // and meta.largest will be set to comp_start_user_key@kMaxSequenceNumber
  // which violates the assumption that meta.smallest should be <= meta.largest.
  assert(!range_del_agg.IsEmpty());
  FileMetaData& meta = current_output().meta;
  const Comparator* ucmp = icmp.user_comparator();
  InternalKey lower_bound_buf;
  InternalKey upper_bound_buf;
  Slice lower_bound_guard;
  Slice upper_bound_guard;
  std::string smallest_user_key;
  const Slice* lower_bound;
  const Slice* upper_bound;

  // We first determine the internal key lower_bound and upper_bound for
  // this output file. All and only range tombstones that overlap with
  // [lower_bound, upper_bound] should be added to this file. File
  // boundaries (meta.smallest/largest) should be updated accordingly when
  // extended by range tombstones.
  size_t output_size = outputs_.size();
  if (output_size == 1) {
    // This is the first file in the subcompaction.
    //
    // When outputting a range tombstone that spans a subcompaction boundary,
    // the files on either side of that boundary need to include that
    // boundary's user key. Otherwise, the spanning range tombstone would lose
    // coverage.
    //
    // To achieve this while preventing files from overlapping in internal key
    // (an LSM invariant violation), we allow the earlier file to include the
    // boundary user key up to `kMaxSequenceNumber,kTypeRangeDeletion`. The
    // later file can begin at the boundary user key at the newest key version
    // it contains. At this point that version number is unknown since we have
    // not processed the range tombstones yet, so permit any version. Same story
    // applies to timestamp, and a non-nullptr `comp_start_user_key` should have
    // `kMaxTs` here, which similarly permits any timestamp.
    if (comp_start_user_key) {
      lower_bound_buf.Set(*comp_start_user_key, kMaxSequenceNumber,
                          kTypeRangeDeletion);
      lower_bound_guard = lower_bound_buf.Encode();
      lower_bound = &lower_bound_guard;
    } else {
      lower_bound = nullptr;
    }
  } else {
    // For subsequent output tables, only include range tombstones from min
    // key onwards since the previous file was extended to contain range
    // tombstones falling before min key.
    if (range_tombstone_lower_bound_.size() > 0) {
      assert(meta.smallest.size() == 0 ||
             icmp.Compare(range_tombstone_lower_bound_, meta.smallest) < 0);
      lower_bound_guard = range_tombstone_lower_bound_.Encode();
    } else {
      assert(meta.smallest.size() > 0);
      lower_bound_guard = meta.smallest.Encode();
    }
    lower_bound = &lower_bound_guard;
  }

  const size_t ts_sz = ucmp->timestamp_size();
  if (next_table_min_key.empty()) {
    // Last file of the subcompaction.
    if (comp_end_user_key) {
      upper_bound_buf.Set(*comp_end_user_key, kMaxSequenceNumber,
                          kTypeRangeDeletion);
      upper_bound_guard = upper_bound_buf.Encode();
      upper_bound = &upper_bound_guard;
    } else {
      upper_bound = nullptr;
    }
  } else {
    // There is another file coming whose coverage will begin at
    // `next_table_min_key`. The current file needs to extend range tombstone
    // coverage through its own keys (through `meta.largest`) and through user
    // keys preceding `next_table_min_key`'s user key.
    ParsedInternalKey next_table_min_key_parsed;
    ParseInternalKey(next_table_min_key, &next_table_min_key_parsed,
                     false /* log_err_key */)
        .PermitUncheckedError();
    assert(next_table_min_key_parsed.sequence < kMaxSequenceNumber);
    assert(meta.largest.size() == 0 ||
           icmp.Compare(meta.largest.Encode(), next_table_min_key) < 0);
    assert(!lower_bound || icmp.Compare(*lower_bound, next_table_min_key) <= 0);
    if (meta.largest.size() > 0 &&
        ucmp->EqualWithoutTimestamp(meta.largest.user_key(),
                                    next_table_min_key_parsed.user_key)) {
      // Caution: this assumes meta.largest.Encode() lives longer than
      // upper_bound, which is only true if meta.largest is never updated.
      // This just happens to be the case here since meta.largest serves
      // as the upper_bound.
      upper_bound_guard = meta.largest.Encode();
    } else {
      SetMaxSeqAndTs(upper_bound_buf, next_table_min_key_parsed.user_key,
                     ts_sz);
      upper_bound_guard = upper_bound_buf.Encode();
    }
    upper_bound = &upper_bound_guard;
  }
  if (lower_bound && upper_bound &&
      icmp.Compare(*lower_bound, *upper_bound) > 0) {
    assert(meta.smallest.size() == 0 &&
           ucmp->EqualWithoutTimestamp(ExtractUserKey(*lower_bound),
                                       ExtractUserKey(*upper_bound)));
    // This can only happen when lower_bound have the same user key as
    // next_table_min_key and that there is no point key in the current
    // compaction output file.
    return Status::OK();
  }
  // The end key of the subcompaction must be bigger or equal to the upper
  // bound. If the end of subcompaction is null or the upper bound is null,
  // it means that this file is the last file in the compaction. So there
  // will be no overlapping between this file and others.
  assert(comp_end_user_key == nullptr || upper_bound == nullptr ||
         ucmp->CompareWithoutTimestamp(ExtractUserKey(*upper_bound),
                                       *comp_end_user_key) <= 0);
  auto it = range_del_agg.NewIterator(lower_bound, upper_bound);
  Slice last_tombstone_start_user_key{};
  bool reached_lower_bound = false;
  const ReadOptions read_options(Env::IOActivity::kCompaction);
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    auto tombstone = it->Tombstone();
    auto kv = tombstone.Serialize();
    // Filter out by seqno for per-key placement
    if (tombstone.seq_ < keep_seqno_range.first ||
        tombstone.seq_ >= keep_seqno_range.second) {
      continue;
    }

    InternalKey tombstone_end = tombstone.SerializeEndKey();
    // TODO: the underlying iterator should support clamping the bounds.
    // tombstone_end.Encode is of form user_key@kMaxSeqno
    // if it is equal to lower_bound, there is no need to include
    // such range tombstone.
    if (!reached_lower_bound && lower_bound &&
        icmp.Compare(tombstone_end.Encode(), *lower_bound) <= 0) {
      continue;
    }
    assert(!lower_bound ||
           icmp.Compare(*lower_bound, tombstone_end.Encode()) <= 0);
    reached_lower_bound = true;

    // Garbage collection for range tombstones.
    // If user-defined timestamp is enabled, range tombstones are dropped if
    // they are at bottommost_level, below full_history_ts_low and not visible
    // in any snapshot. trim_ts_ is passed to the constructor for
    // range_del_agg_, and range_del_agg_ internally drops tombstones above
    // trim_ts_.
    bool consider_drop =
        tombstone.seq_ <= earliest_snapshot &&
        (ts_sz == 0 ||
         (!full_history_ts_low.empty() &&
          ucmp->CompareTimestamp(tombstone.ts_, full_history_ts_low) < 0));
    if (consider_drop && bottommost_level) {
      // TODO(andrewkr): tombstones that span multiple output files are
      // counted for each compaction output file, so lots of double
      // counting.
      range_del_out_stats.num_range_del_drop_obsolete++;
      range_del_out_stats.num_record_drop_obsolete++;
      continue;
    }

    assert(lower_bound == nullptr ||
           ucmp->CompareWithoutTimestamp(ExtractUserKey(*lower_bound),
                                         kv.second) < 0);
    InternalKey tombstone_start = kv.first;
    if (lower_bound &&
        ucmp->CompareWithoutTimestamp(tombstone_start.user_key(),
                                      ExtractUserKey(*lower_bound)) < 0) {
      // This just updates the non-timestamp portion of `tombstone_start`'s user
      // key. Ideally there would be a simpler API usage
      ParsedInternalKey tombstone_start_parsed;
      ParseInternalKey(tombstone_start.Encode(), &tombstone_start_parsed,
                       false /* log_err_key */)
          .PermitUncheckedError();
      // timestamp should be from where sequence number is from, which is from
      // tombstone in this case
      std::string ts =
          tombstone_start_parsed.GetTimestamp(ucmp->timestamp_size())
              .ToString();
      tombstone_start_parsed.user_key = ExtractUserKey(*lower_bound);
      tombstone_start.SetFrom(tombstone_start_parsed, ts);
    }
    if (upper_bound != nullptr &&
        icmp.Compare(*upper_bound, tombstone_start.Encode()) < 0) {
      break;
    }
    if (lower_bound &&
        icmp.Compare(tombstone_start.Encode(), *lower_bound) < 0) {
      tombstone_start.DecodeFrom(*lower_bound);
    }
    if (upper_bound && icmp.Compare(*upper_bound, tombstone_end.Encode()) < 0) {
      tombstone_end.DecodeFrom(*upper_bound);
    }
    if (consider_drop && compaction_->KeyRangeNotExistsBeyondOutputLevel(
                             tombstone_start.user_key(),
                             tombstone_end.user_key(), &level_ptrs_)) {
      range_del_out_stats.num_range_del_drop_obsolete++;
      range_del_out_stats.num_record_drop_obsolete++;
      continue;
    }
    // Here we show that *only* range tombstones that overlap with
    // [lower_bound, upper_bound] are added to the current file, and
    // sanity checking invariants that should hold:
    // - [tombstone_start, tombstone_end] overlaps with [lower_bound,
    // upper_bound]
    // - meta.smallest <= meta.largest
    // Corresponding assertions are made, the proof is broken is any of them
    // fails.
    // TODO: show that *all* range tombstones that overlap with
    //  [lower_bound, upper_bound] are added.
    // TODO: some invariant about boundaries are correctly updated.
    //
    // Note that `tombstone_start` is updated in the if condition above, we use
    // tombstone_start to refer to its initial value, i.e.,
    // it->Tombstone().first, and use tombstone_start* to refer to its value
    // after the update.
    //
    // To show [lower_bound, upper_bound] overlaps with [tombstone_start,
    // tombstone_end]:
    // lower_bound <= upper_bound from the if condition right after all
    // bounds are initialized. We assume each tombstone fragment has
    // start_key.user_key < end_key.user_key, so
    // tombstone_start < tombstone_end by
    // FragmentedTombstoneIterator::Tombstone(). So these two ranges are both
    // non-emtpy. The flag `reached_lower_bound` and the if logic before it
    // ensures lower_bound <= tombstone_end. tombstone_start is only updated
    // if it has a smaller user_key than lower_bound user_key, so
    // tombstone_start <= tombstone_start*. The above if condition implies
    // tombstone_start* <= upper_bound. So we have
    // tombstone_start <= upper_bound and lower_bound <= tombstone_end
    // and the two ranges overlap.
    //
    // To show meta.smallest <= meta.largest:
    // From the implementation of UpdateBoundariesForRange(), it suffices to
    // prove that when it is first called in this function, its parameters
    // satisfy `start <= end`, where start = max(tombstone_start*, lower_bound)
    // and end = min(tombstone_end, upper_bound). From the above proof we have
    // lower_bound <= tombstone_end and lower_bound <= upper_bound. We only need
    // to show that tombstone_start* <= min(tombstone_end, upper_bound).
    // Note that tombstone_start*.user_key = max(tombstone_start.user_key,
    // lower_bound.user_key). Assuming tombstone_end always has
    // kMaxSequenceNumber and lower_bound.seqno < kMaxSequenceNumber.
    // Since lower_bound <= tombstone_end and lower_bound.seqno <
    // tombstone_end.seqno (in absolute number order, not internal key order),
    // lower_bound.user_key < tombstone_end.user_key.
    // Since lower_bound.user_key < tombstone_end.user_key and
    // tombstone_start.user_key < tombstone_end.user_key, tombstone_start* <
    // tombstone_end. Since tombstone_start* <= upper_bound from the above proof
    // and tombstone_start* < tombstone_end, tombstone_start* <=
    // min(tombstone_end, upper_bound), so the two ranges overlap.

    // Range tombstone is not supported by output validator yet.
    builder_->Add(kv.first.Encode(), kv.second);
    assert(icmp.Compare(tombstone_start, tombstone_end) <= 0);
    meta.UpdateBoundariesForRange(tombstone_start, tombstone_end,
                                  tombstone.seq_, icmp);
    if (!bottommost_level) {
      bool start_user_key_changed =
          last_tombstone_start_user_key.empty() ||
          ucmp->CompareWithoutTimestamp(last_tombstone_start_user_key,
                                        it->start_key()) < 0;
      last_tombstone_start_user_key = it->start_key();
      if (start_user_key_changed) {
        // If tombstone_start >= tombstone_end, then either no key range is
        // covered, or that they have the same user key. If they have the same
        // user key, then the internal key range should only be within this
        // level, and no keys from older levels is covered.
        if (ucmp->CompareWithoutTimestamp(tombstone_start.user_key(),
                                          tombstone_end.user_key()) < 0) {
          SizeApproximationOptions approx_opts;
          approx_opts.files_size_error_margin = 0.1;
          auto approximate_covered_size =
              compaction_->input_version()->version_set()->ApproximateSize(
                  approx_opts, read_options, compaction_->input_version(),
                  tombstone_start.Encode(), tombstone_end.Encode(),
                  compaction_->output_level() + 1 /* start_level */,
                  -1 /* end_level */, kCompaction);
          meta.compensated_range_deletion_size += approximate_covered_size;
        }
      }
    }
  }
  return Status::OK();
}

void CompactionOutputs::FillFilesToCutForTtl() {
  if (compaction_->immutable_options().compaction_style !=
          kCompactionStyleLevel ||
      compaction_->immutable_options().compaction_pri != kMinOverlappingRatio ||
      compaction_->mutable_cf_options().ttl == 0 ||
      compaction_->num_input_levels() < 2 || compaction_->bottommost_level()) {
    return;
  }

  // We define new file with the oldest ancestor time to be younger than 1/4
  // TTL, and an old one to be older than 1/2 TTL time.
  int64_t temp_current_time;
  auto get_time_status = compaction_->immutable_options().clock->GetCurrentTime(
      &temp_current_time);
  if (!get_time_status.ok()) {
    return;
  }

  auto current_time = static_cast<uint64_t>(temp_current_time);
  if (current_time < compaction_->mutable_cf_options().ttl) {
    return;
  }

  uint64_t old_age_thres =
      current_time - compaction_->mutable_cf_options().ttl / 2;
  const std::vector<FileMetaData*>& olevel =
      *(compaction_->inputs(compaction_->num_input_levels() - 1));
  for (FileMetaData* file : olevel) {
    // Worth filtering out by start and end?
    uint64_t oldest_ancestor_time = file->TryGetOldestAncestorTime();
    // We put old files if they are not too small to prevent a flood
    // of small files.
    if (oldest_ancestor_time < old_age_thres &&
        file->fd.GetFileSize() >
            compaction_->mutable_cf_options().target_file_size_base / 2) {
      files_to_cut_for_ttl_.push_back(file);
    }
  }
}

CompactionOutputs::CompactionOutputs(const Compaction* compaction,
                                     const bool is_proximal_level)
    : compaction_(compaction), is_proximal_level_(is_proximal_level) {
  partitioner_ = compaction->output_level() == 0
                     ? nullptr
                     : compaction->CreateSstPartitioner();

  if (compaction->output_level() != 0) {
    FillFilesToCutForTtl();
  }

  level_ptrs_ = std::vector<size_t>(compaction_->number_levels(), 0);
}

}  // namespace ROCKSDB_NAMESPACE
