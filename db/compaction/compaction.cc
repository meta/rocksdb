//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction.h"

#include <cinttypes>
#include <vector>

#include "db/column_family.h"
#include "db/dbformat.h"
#include "logging/logging.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/sst_partitioner.h"
#include "test_util/sync_point.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

int sstableKeyCompare(const Comparator* uc, const Slice& a, const Slice& b) {
  auto c = uc->CompareWithoutTimestamp(ExtractUserKey(a), ExtractUserKey(b));
  if (c != 0) {
    return c;
  }
  auto a_footer = ExtractInternalKeyFooter(a);
  auto b_footer = ExtractInternalKeyFooter(b);
  if (a_footer == kRangeTombstoneSentinel) {
    if (b_footer != kRangeTombstoneSentinel) {
      return -1;
    }
  } else if (b_footer == kRangeTombstoneSentinel) {
    return 1;
  }
  return 0;
}

int sstableKeyCompare(const Comparator* user_cmp, const InternalKey* a,
                      const InternalKey& b) {
  if (a == nullptr) {
    return -1;
  }
  return sstableKeyCompare(user_cmp, *a, b);
}

int sstableKeyCompare(const Comparator* user_cmp, const InternalKey& a,
                      const InternalKey* b) {
  if (b == nullptr) {
    return -1;
  }
  return sstableKeyCompare(user_cmp, a, *b);
}

uint64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  uint64_t sum = 0;
  for (size_t i = 0; i < files.size() && files[i]; i++) {
    sum += files[i]->fd.GetFileSize();
  }
  return sum;
}

// TODO(hx235): consider making this function part of the construction so we
// don't forget to call it
void Compaction::FinalizeInputInfo(Version* _input_version) {
  input_version_ = _input_version;
  cfd_ = input_version_->cfd();

  cfd_->Ref();
  input_version_->Ref();
  edit_.SetColumnFamily(cfd_->GetID());
}

void Compaction::GetBoundaryKeys(
    VersionStorageInfo* vstorage,
    const std::vector<CompactionInputFiles>& inputs, Slice* smallest_user_key,
    Slice* largest_user_key, int exclude_level) {
  bool initialized = false;
  const Comparator* ucmp = vstorage->InternalComparator()->user_comparator();
  for (size_t i = 0; i < inputs.size(); ++i) {
    if (inputs[i].files.empty() || inputs[i].level == exclude_level) {
      continue;
    }
    if (inputs[i].level == 0) {
      // we need to consider all files on level 0
      for (const auto* f : inputs[i].files) {
        const Slice& start_user_key = f->smallest.user_key();
        if (!initialized ||
            ucmp->Compare(start_user_key, *smallest_user_key) < 0) {
          *smallest_user_key = start_user_key;
        }
        const Slice& end_user_key = f->largest.user_key();
        if (!initialized ||
            ucmp->Compare(end_user_key, *largest_user_key) > 0) {
          *largest_user_key = end_user_key;
        }
        initialized = true;
      }
    } else {
      // we only need to consider the first and last file
      const Slice& start_user_key = inputs[i].files[0]->smallest.user_key();
      if (!initialized ||
          ucmp->Compare(start_user_key, *smallest_user_key) < 0) {
        *smallest_user_key = start_user_key;
      }
      const Slice& end_user_key = inputs[i].files.back()->largest.user_key();
      if (!initialized || ucmp->Compare(end_user_key, *largest_user_key) > 0) {
        *largest_user_key = end_user_key;
      }
      initialized = true;
    }
  }
}

void Compaction::GetBoundaryInternalKeys(
    VersionStorageInfo* vstorage,
    const std::vector<CompactionInputFiles>& inputs, InternalKey* smallest_key,
    InternalKey* largest_key, int exclude_level) {
  bool initialized = false;
  const InternalKeyComparator* icmp = vstorage->InternalComparator();
  for (size_t i = 0; i < inputs.size(); ++i) {
    if (inputs[i].files.empty() || inputs[i].level == exclude_level) {
      continue;
    }
    if (inputs[i].level == 0) {
      // we need to consider all files on level 0
      for (const auto* f : inputs[i].files) {
        if (!initialized || icmp->Compare(f->smallest, *smallest_key) < 0) {
          *smallest_key = f->smallest;
        }
        if (!initialized || icmp->Compare(f->largest, *largest_key) > 0) {
          *largest_key = f->largest;
        }
        initialized = true;
      }
    } else {
      // we only need to consider the first and last file
      if (!initialized ||
          icmp->Compare(inputs[i].files[0]->smallest, *smallest_key) < 0) {
        *smallest_key = inputs[i].files[0]->smallest;
      }
      if (!initialized ||
          icmp->Compare(inputs[i].files.back()->largest, *largest_key) > 0) {
        *largest_key = inputs[i].files.back()->largest;
      }
      initialized = true;
    }
  }
}

std::vector<CompactionInputFiles> Compaction::PopulateWithAtomicBoundaries(
    VersionStorageInfo* vstorage, std::vector<CompactionInputFiles> inputs) {
  const Comparator* ucmp = vstorage->InternalComparator()->user_comparator();
  for (size_t i = 0; i < inputs.size(); i++) {
    if (inputs[i].level == 0 || inputs[i].files.empty()) {
      continue;
    }
    inputs[i].atomic_compaction_unit_boundaries.reserve(inputs[i].files.size());
    AtomicCompactionUnitBoundary cur_boundary;
    size_t first_atomic_idx = 0;
    auto add_unit_boundary = [&](size_t to) {
      if (first_atomic_idx == to) {
        return;
      }
      for (size_t k = first_atomic_idx; k < to; k++) {
        inputs[i].atomic_compaction_unit_boundaries.push_back(cur_boundary);
      }
      first_atomic_idx = to;
    };
    for (size_t j = 0; j < inputs[i].files.size(); j++) {
      const auto* f = inputs[i].files[j];
      if (j == 0) {
        // First file in a level.
        cur_boundary.smallest = &f->smallest;
        cur_boundary.largest = &f->largest;
      } else if (sstableKeyCompare(ucmp, *cur_boundary.largest, f->smallest) ==
                 0) {
        // SSTs overlap but the end key of the previous file was not
        // artificially extended by a range tombstone. Extend the current
        // boundary.
        cur_boundary.largest = &f->largest;
      } else {
        // Atomic compaction unit has ended.
        add_unit_boundary(j);
        cur_boundary.smallest = &f->smallest;
        cur_boundary.largest = &f->largest;
      }
    }
    add_unit_boundary(inputs[i].files.size());
    assert(inputs[i].files.size() ==
           inputs[i].atomic_compaction_unit_boundaries.size());
  }
  return inputs;
}

// helper function to determine if compaction is creating files at the
// bottommost level
bool Compaction::IsBottommostLevel(
    int output_level, VersionStorageInfo* vstorage,
    const std::vector<CompactionInputFiles>& inputs) {
  int output_l0_idx;
  if (output_level == 0) {
    output_l0_idx = 0;
    for (const auto* file : vstorage->LevelFiles(0)) {
      if (inputs[0].files.back() == file) {
        break;
      }
      ++output_l0_idx;
    }
    assert(static_cast<size_t>(output_l0_idx) < vstorage->LevelFiles(0).size());
  } else {
    output_l0_idx = -1;
  }
  Slice smallest_key;
  Slice largest_key;
  GetBoundaryKeys(vstorage, inputs, &smallest_key, &largest_key);
  return !vstorage->RangeMightExistAfterSortedRun(smallest_key, largest_key,
                                                  output_level, output_l0_idx);
}

// test function to validate the functionality of IsBottommostLevel()
// function -- determines if compaction with inputs and storage is bottommost
bool Compaction::TEST_IsBottommostLevel(
    int output_level, VersionStorageInfo* vstorage,
    const std::vector<CompactionInputFiles>& inputs) {
  return IsBottommostLevel(output_level, vstorage, inputs);
}

bool Compaction::IsFullCompaction(
    VersionStorageInfo* vstorage,
    const std::vector<CompactionInputFiles>& inputs) {
  size_t num_files_in_compaction = 0;
  size_t total_num_files = 0;
  for (int l = 0; l < vstorage->num_levels(); l++) {
    total_num_files += vstorage->NumLevelFiles(l);
  }
  for (size_t i = 0; i < inputs.size(); i++) {
    num_files_in_compaction += inputs[i].size();
  }
  return num_files_in_compaction == total_num_files;
}

Status Compaction::InitInputTableProperties() {
  if (!input_table_properties_.empty()) {
    return Status::OK();
  }

  Status s;
  const ReadOptions read_options(Env::IOActivity::kCompaction);
  assert(input_version_);
  for (size_t i = 0; i < num_input_levels(); ++i) {
    for (const FileMetaData* fmd : *(this->inputs(i))) {
      std::shared_ptr<const TableProperties> tp;
      std::string file_name =
          TableFileName(immutable_options_.cf_paths, fmd->fd.GetNumber(),
                        fmd->fd.GetPathId());
      s = input_version_->GetTableProperties(read_options, &tp, fmd,
                                             &file_name);
      if (s.ok()) {
        input_table_properties_[file_name] = tp;
      } else {
        ROCKS_LOG_ERROR(immutable_options_.info_log,
                        "Unable to load table properties for file %" PRIu64
                        " --- %s\n",
                        fmd->fd.GetNumber(), s.ToString().c_str());
        input_table_properties_.clear();
        return s;
      }
    }
  }

  return s;
}

Compaction::Compaction(
    VersionStorageInfo* vstorage, const ImmutableOptions& _immutable_options,
    const MutableCFOptions& _mutable_cf_options,
    const MutableDBOptions& _mutable_db_options,
    std::vector<CompactionInputFiles> _inputs, int _output_level,
    uint64_t _target_file_size, uint64_t _max_compaction_bytes,
    uint32_t _output_path_id, CompressionType _compression,
    CompressionOptions _compression_opts,
    Temperature _output_temperature_override, uint32_t _max_subcompactions,
    std::vector<FileMetaData*> _grandparents,
    std::optional<SequenceNumber> _earliest_snapshot,
    const SnapshotChecker* _snapshot_checker,
    CompactionReason _compaction_reason, const std::string& _trim_ts,
    double _score, bool l0_files_might_overlap,
    BlobGarbageCollectionPolicy _blob_garbage_collection_policy,
    double _blob_garbage_collection_age_cutoff)
    : input_vstorage_(vstorage),
      start_level_(_inputs[0].level),
      output_level_(_output_level),
      target_output_file_size_(_target_file_size),
      max_compaction_bytes_(_max_compaction_bytes),
      max_subcompactions_(_max_subcompactions),
      immutable_options_(_immutable_options),
      mutable_cf_options_(_mutable_cf_options),
      input_version_(nullptr),
      number_levels_(vstorage->num_levels()),
      cfd_(nullptr),
      output_path_id_(_output_path_id),
      output_compression_(_compression),
      output_compression_opts_(_compression_opts),
      output_temperature_override_(_output_temperature_override),
      deletion_compaction_(_compaction_reason == CompactionReason::kFIFOTtl ||
                           _compaction_reason ==
                               CompactionReason::kFIFOMaxSize),
      l0_files_might_overlap_(l0_files_might_overlap),
      inputs_(PopulateWithAtomicBoundaries(vstorage, std::move(_inputs))),
      grandparents_(std::move(_grandparents)),
      earliest_snapshot_(_earliest_snapshot),
      snapshot_checker_(_snapshot_checker),
      score_(_score),
      bottommost_level_(
          // For simplicity, we don't support the concept of "bottommost level"
          // with
          // `CompactionReason::kExternalSstIngestion` and
          // `CompactionReason::kRefitLevel`
          (_compaction_reason == CompactionReason::kExternalSstIngestion ||
           _compaction_reason == CompactionReason::kRefitLevel)
              ? false
              : IsBottommostLevel(output_level_, vstorage, inputs_)),
      is_full_compaction_(IsFullCompaction(vstorage, inputs_)),
      is_manual_compaction_(_compaction_reason ==
                            CompactionReason::kManualCompaction),
      trim_ts_(_trim_ts),
      is_trivial_move_(false),
      compaction_reason_(_compaction_reason),
      notify_on_compaction_completion_(false),
      enable_blob_garbage_collection_(
          _blob_garbage_collection_policy == BlobGarbageCollectionPolicy::kForce
              ? true
              : (_blob_garbage_collection_policy ==
                         BlobGarbageCollectionPolicy::kDisable
                     ? false
                     : mutable_cf_options().enable_blob_garbage_collection)),
      blob_garbage_collection_age_cutoff_(
          _blob_garbage_collection_age_cutoff < 0 ||
                  _blob_garbage_collection_age_cutoff > 1
              ? mutable_cf_options().blob_garbage_collection_age_cutoff
              : _blob_garbage_collection_age_cutoff),
      proximal_level_(
          // For simplicity, we don't support the concept of "proximal level"
          // with `CompactionReason::kExternalSstIngestion` and
          // `CompactionReason::kRefitLevel`
          _compaction_reason == CompactionReason::kExternalSstIngestion ||
                  _compaction_reason == CompactionReason::kRefitLevel
              ? Compaction::kInvalidLevel
              : EvaluateProximalLevel(vstorage, mutable_cf_options_,
                                      immutable_options_, start_level_,
                                      output_level_)) {
  MarkFilesBeingCompacted(true);
  if (max_subcompactions_ == 0) {
    max_subcompactions_ = _mutable_db_options.max_subcompactions;
  }

  // for the non-bottommost levels, it tries to build files match the target
  // file size, but not guaranteed. It could be 2x the size of the target size.
  max_output_file_size_ = bottommost_level_ || grandparents_.empty()
                              ? target_output_file_size_
                              : 2 * target_output_file_size_;

#ifndef NDEBUG
  for (size_t i = 1; i < inputs_.size(); ++i) {
    assert(inputs_[i].level > inputs_[i - 1].level);
  }
#endif

  // setup input_levels_ and filtered_input_levels_
  {
    input_levels_.resize(num_input_levels());
    filtered_input_levels_.resize(num_input_levels());
    if (earliest_snapshot_.has_value()) {
      FilterInputsForCompactionIterator();
    } else {
      for (size_t which = 0; which < num_input_levels(); which++) {
        DoGenerateLevelFilesBrief(&input_levels_[which], inputs_[which].files,
                                  &arena_);
      }
    }
  }

  GetBoundaryKeys(vstorage, inputs_, &smallest_user_key_, &largest_user_key_);

  // Every compaction regardless of any compaction reason may respect the
  // existing compact cursor in the output level to split output files
  output_split_key_ = nullptr;
  if (immutable_options_.compaction_style == kCompactionStyleLevel &&
      immutable_options_.compaction_pri == kRoundRobin) {
    const InternalKey* cursor =
        &input_vstorage_->GetCompactCursors()[output_level_];
    if (cursor->size() != 0) {
      const Slice& cursor_user_key = ExtractUserKey(cursor->Encode());
      auto ucmp = vstorage->InternalComparator()->user_comparator();
      // May split output files according to the cursor if it in the user-key
      // range
      if (ucmp->CompareWithoutTimestamp(cursor_user_key, smallest_user_key_) >
              0 &&
          ucmp->CompareWithoutTimestamp(cursor_user_key, largest_user_key_) <=
              0) {
        output_split_key_ = cursor;
      }
    }
  }

  PopulateProximalLevelOutputRange();
}

void Compaction::PopulateProximalLevelOutputRange() {
  if (!SupportsPerKeyPlacement()) {
    assert(keep_in_last_level_through_seqno_ == kMaxSequenceNumber);
    return;
  }

  // exclude the last level, the range of all input levels is the safe range
  // of keys that can be moved up.
  int exclude_level = number_levels_ - 1;
  proximal_output_range_type_ = ProximalOutputRangeType::kNonLastRange;

  // For universal compaction, the proximal_output_range could be extended if
  // all proximal level files are included in the compaction (which includes
  // the case that the proximal level is empty).
  if (immutable_options_.compaction_style == kCompactionStyleUniversal) {
    exclude_level = kInvalidLevel;
    proximal_output_range_type_ = ProximalOutputRangeType::kFullRange;
    std::set<uint64_t> proximal_inputs;
    for (const auto& input_lvl : inputs_) {
      if (input_lvl.level == proximal_level_) {
        for (const auto& file : input_lvl.files) {
          proximal_inputs.emplace(file->fd.GetNumber());
        }
      }
    }
    auto proximal_files = input_vstorage_->LevelFiles(proximal_level_);
    for (const auto& file : proximal_files) {
      if (proximal_inputs.find(file->fd.GetNumber()) == proximal_inputs.end()) {
        exclude_level = number_levels_ - 1;
        proximal_output_range_type_ = ProximalOutputRangeType::kNonLastRange;
        break;
      }
    }
  }

  // FIXME: should make use of `proximal_output_range_type_`.
  // FIXME: when last level's input range does not overlap with
  //  proximal level, and proximal level input is empty,
  //  this call will not set proximal_level_smallest_ or
  //  proximal_level_largest_. No keys will be compacted up.
  GetBoundaryInternalKeys(input_vstorage_, inputs_, &proximal_level_smallest_,
                          &proximal_level_largest_, exclude_level);

  if (proximal_output_range_type_ != ProximalOutputRangeType::kFullRange) {
    // If not full range in proximal level, must keep everything already
    // in the last level there, because moving it back up might cause
    // overlap/placement issues that are difficult to resolve properly in the
    // presence of range deletes
    SequenceNumber max_last_level_seqno = 0;
    for (const auto& input_lvl : inputs_) {
      if (input_lvl.level == output_level_) {
        for (const auto& file : input_lvl.files) {
          max_last_level_seqno =
              std::max(max_last_level_seqno, file->fd.largest_seqno);
        }
      }
    }

    keep_in_last_level_through_seqno_ = max_last_level_seqno;
  } else {
    keep_in_last_level_through_seqno_ = 0;
  }
}

Compaction::~Compaction() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
  }
  if (cfd_ != nullptr) {
    cfd_->UnrefAndTryDelete();
  }
}

bool Compaction::SupportsPerKeyPlacement() const {
  return proximal_level_ != kInvalidLevel;
}

int Compaction::GetProximalLevel() const { return proximal_level_; }

// smallest_key and largest_key include timestamps if user-defined timestamp is
// enabled.
bool Compaction::OverlapProximalLevelOutputRange(
    const Slice& smallest_key, const Slice& largest_key,
    bool range_limit_exclusive) const {
  if (!SupportsPerKeyPlacement()) {
    return false;
  }

  // See FIXME in Compaction::PopulateProximalLevelOutputRange().
  // We do not compact any key up in this case.
  if (proximal_level_smallest_.size() == 0 ||
      proximal_level_largest_.size() == 0) {
    return false;
  }

  const Comparator* ucmp =
      input_vstorage_->InternalComparator()->user_comparator();

  const int largest_cmp = ucmp->CompareWithoutTimestamp(
      largest_key, proximal_level_smallest_.user_key());
  return ucmp->CompareWithoutTimestamp(
             smallest_key, proximal_level_largest_.user_key()) <= 0 &&
         (range_limit_exclusive ? largest_cmp > 0 : largest_cmp >= 0);
}

// key includes timestamp if user-defined timestamp is enabled.
void Compaction::TEST_AssertWithinProximalLevelOutputRange(
    const Slice& user_key, bool expect_failure) const {
#ifdef NDEBUG
  (void)user_key;
  (void)expect_failure;
#else
  assert(SupportsPerKeyPlacement());

  assert(proximal_level_smallest_.size() > 0);
  assert(proximal_level_largest_.size() > 0);

  auto* cmp = input_vstorage_->user_comparator();

  // op_type of a key can change during compaction, e.g. Merge -> Put.
  if (!(cmp->Compare(user_key, proximal_level_smallest_.user_key()) >= 0)) {
    assert(expect_failure);
  } else if (!(cmp->Compare(user_key, proximal_level_largest_.user_key()) <=
               0)) {
    assert(expect_failure);
  } else {
    assert(!expect_failure);
  }
#endif
}

bool Compaction::InputCompressionMatchesOutput() const {
  int base_level = input_vstorage_->base_level();
  bool matches =
      (GetCompressionType(input_vstorage_, mutable_cf_options_, start_level_,
                          base_level) == output_compression_);
  if (matches) {
    TEST_SYNC_POINT("Compaction::InputCompressionMatchesOutput:Matches");
    return true;
  }
  TEST_SYNC_POINT("Compaction::InputCompressionMatchesOutput:DidntMatch");
  return matches;
}

bool Compaction::IsTrivialMove() const {
  if (skip_missing_blob_files_) {
    return false;
  }

  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  // If start_level_== output_level_, the purpose is to force compaction
  // filter to be applied to that level, and thus cannot be a trivial move.

  // Check if start level have files with overlapping ranges
  if (start_level_ == 0 && input_vstorage_->level0_non_overlapping() == false &&
      l0_files_might_overlap_) {
    // We cannot move files from L0 to L1 if the L0 files in the LSM-tree are
    // overlapping, unless we are sure that files picked in L0 don't overlap.
    return false;
  }

  if (is_manual_compaction_ &&
      (immutable_options_.compaction_filter != nullptr ||
       immutable_options_.compaction_filter_factory != nullptr)) {
    // This is a manual compaction and we have a compaction filter that should
    // be executed, we cannot do a trivial move
    return false;
  }

  if (start_level_ == output_level_) {
    // It doesn't make sense if compaction picker picks files just to trivial
    // move to the same level.
    return false;
  }

  if (compaction_reason_ == CompactionReason::kChangeTemperature) {
    // Changing temperature usually requires rewriting the file.
    return false;
  }

  // Used in universal compaction, where trivial move can be done if the
  // input files are non overlapping
  if ((mutable_cf_options_.compaction_options_universal.allow_trivial_move) &&
      (output_level_ != 0) &&
      (cfd_->ioptions().compaction_style == kCompactionStyleUniversal)) {
    return is_trivial_move_;
  }

  if (!(start_level_ != output_level_ && num_input_levels() == 1 &&
        input(0, 0)->fd.GetPathId() == output_path_id() &&
        InputCompressionMatchesOutput())) {
    return false;
  }

  // assert inputs_.size() == 1

  if (output_level_ + 1 < number_levels_) {
    std::unique_ptr<SstPartitioner> partitioner = CreateSstPartitioner();
    for (const auto& file : inputs_.front().files) {
      std::vector<FileMetaData*> file_grand_parents;
      input_vstorage_->GetOverlappingInputs(output_level_ + 1, &file->smallest,
                                            &file->largest,
                                            &file_grand_parents);
      const auto compaction_size =
          file->fd.GetFileSize() + TotalFileSize(file_grand_parents);
      if (compaction_size > max_compaction_bytes_) {
        return false;
      }

      if (partitioner.get() != nullptr) {
        if (!partitioner->CanDoTrivialMove(file->smallest.user_key(),
                                           file->largest.user_key())) {
          return false;
        }
      }
    }
  }

  // PerKeyPlacement compaction should never be trivial move.
  if (SupportsPerKeyPlacement()) {
    return false;
  }

  return true;
}

void Compaction::AddInputDeletions(VersionEdit* out_edit) {
  for (size_t which = 0; which < num_input_levels(); which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      out_edit->DeleteFile(level(which), inputs_[which][i]->fd.GetNumber());
    }
  }
}

bool Compaction::KeyNotExistsBeyondOutputLevel(
    const Slice& user_key, std::vector<size_t>* level_ptrs) const {
  assert(input_version_ != nullptr);
  assert(level_ptrs != nullptr);
  assert(level_ptrs->size() == static_cast<size_t>(number_levels_));
  if (bottommost_level_) {
    return true;
  } else if (output_level_ != 0 &&
             cfd_->ioptions().compaction_style == kCompactionStyleLevel) {
    // TODO: apply the optimization here to other compaction styles and
    // compaction/flush to L0.
    // Maybe use binary search to find right entry instead of linear search?
    const Comparator* user_cmp = cfd_->user_comparator();
    for (int lvl = output_level_ + 1; lvl < number_levels_; lvl++) {
      const std::vector<FileMetaData*>& files =
          input_vstorage_->LevelFiles(lvl);
      for (; level_ptrs->at(lvl) < files.size(); level_ptrs->at(lvl)++) {
        auto* f = files[level_ptrs->at(lvl)];
        if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
          // We've advanced far enough
          // In the presence of user-defined timestamp, we may need to handle
          // the case in which f->smallest.user_key() (including ts) has the
          // same user key, but the ts part is smaller. If so,
          // Compare(user_key, f->smallest.user_key()) returns -1.
          // That's why we need CompareWithoutTimestamp().
          if (user_cmp->CompareWithoutTimestamp(user_key,
                                                f->smallest.user_key()) >= 0) {
            // Key falls in this file's range, so it may
            // exist beyond output level
            return false;
          }
          break;
        }
      }
    }
    return true;
  }
  return false;
}

bool Compaction::KeyRangeNotExistsBeyondOutputLevel(
    const Slice& begin_key, const Slice& end_key,
    std::vector<size_t>* level_ptrs) const {
  assert(input_version_ != nullptr);
  assert(level_ptrs != nullptr);
  assert(level_ptrs->size() == static_cast<size_t>(number_levels_));
  assert(cfd_->user_comparator()->CompareWithoutTimestamp(begin_key, end_key) <
         0);
  if (bottommost_level_) {
    return true /* does not overlap */;
  } else if (output_level_ != 0 &&
             cfd_->ioptions().compaction_style == kCompactionStyleLevel) {
    const Comparator* user_cmp = cfd_->user_comparator();
    for (int lvl = output_level_ + 1; lvl < number_levels_; lvl++) {
      const std::vector<FileMetaData*>& files =
          input_vstorage_->LevelFiles(lvl);
      for (; level_ptrs->at(lvl) < files.size(); level_ptrs->at(lvl)++) {
        auto* f = files[level_ptrs->at(lvl)];
        // Advance until the first file with begin_key <= f->largest.user_key()
        if (user_cmp->CompareWithoutTimestamp(begin_key,
                                              f->largest.user_key()) > 0) {
          continue;
        }
        // We know that the previous file prev_f, if exists, has
        // prev_f->largest.user_key() < begin_key.
        if (user_cmp->CompareWithoutTimestamp(end_key,
                                              f->smallest.user_key()) <= 0) {
          // not overlapping with this level
          break;
        } else {
          // We have:
          // - begin_key < end_key,
          // - begin_key <= f->largest.user_key(), and
          // - end_key > f->smallest.user_key()
          return false /* overlap */;
        }
      }
    }
    return true /* does not overlap */;
  }
  return false /* overlaps */;
};

// Mark (or clear) each file that is being compacted
void Compaction::MarkFilesBeingCompacted(bool being_compacted) const {
  for (size_t i = 0; i < num_input_levels(); i++) {
    for (size_t j = 0; j < inputs_[i].size(); j++) {
      assert(being_compacted != inputs_[i][j]->being_compacted);
      inputs_[i][j]->being_compacted = being_compacted;
    }
  }
}

// Sample output:
// If compacting 3 L0 files, 2 L3 files and 1 L4 file, and outputting to L5,
// print: "3@0 + 2@3 + 1@4 files to L5"
const char* Compaction::InputLevelSummary(
    InputLevelSummaryBuffer* scratch) const {
  int len = 0;
  bool is_first = true;
  for (auto& input_level : inputs_) {
    if (input_level.empty()) {
      continue;
    }
    if (!is_first) {
      len +=
          snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len, " + ");
      len = std::min(len, static_cast<int>(sizeof(scratch->buffer)));
    } else {
      is_first = false;
    }
    len += snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len,
                    "%" ROCKSDB_PRIszt "@%d", input_level.size(),
                    input_level.level);
    len = std::min(len, static_cast<int>(sizeof(scratch->buffer)));
  }
  snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len,
           " files to L%d", output_level());

  return scratch->buffer;
}

uint64_t Compaction::CalculateTotalInputSize() const {
  uint64_t size = 0;
  for (auto& input_level : inputs_) {
    for (auto f : input_level.files) {
      size += f->fd.GetFileSize();
    }
  }
  return size;
}

void Compaction::ReleaseCompactionFiles(const Status& status) {
  MarkFilesBeingCompacted(false);
  cfd_->compaction_picker()->ReleaseCompactionFiles(this, status);
}

void Compaction::ResetNextCompactionIndex() {
  assert(input_version_ != nullptr);
  input_vstorage_->ResetNextCompactionIndex(start_level_);
}

namespace {
int InputSummary(const std::vector<FileMetaData*>& files,
                 const std::vector<bool>& files_filtered, char* output,
                 int len) {
  assert(files_filtered.empty() || (files.size() == files_filtered.size()));
  *output = '\0';
  int write = 0;
  for (size_t i = 0; i < files.size(); i++) {
    int sz = len - write;
    int ret;
    char sztxt[16];
    AppendHumanBytes(files.at(i)->fd.GetFileSize(), sztxt, 16);
    if (files_filtered.empty()) {
      ret = snprintf(output + write, sz, "%" PRIu64 "(%s) ",
                     files.at(i)->fd.GetNumber(), sztxt);
    } else {
      ret = snprintf(output + write, sz, "%" PRIu64 "(%s filtered:%s) ",
                     files.at(i)->fd.GetNumber(), sztxt,
                     files_filtered.at(i) ? "true" : "false");
    }
    if (ret < 0 || ret >= sz) {
      break;
    }
    write += ret;
  }
  // if files.size() is non-zero, overwrite the last space
  return write - !!files.size();
}
}  // namespace

void Compaction::Summary(char* output, int len) {
  int write =
      snprintf(output, len, "Base version %" PRIu64 " Base level %d, inputs: [",
               input_version_->GetVersionNumber(), start_level_);
  if (write < 0 || write >= len) {
    return;
  }

  for (size_t level_iter = 0; level_iter < num_input_levels(); ++level_iter) {
    if (level_iter > 0) {
      write += snprintf(output + write, len - write, "], [");
      if (write < 0 || write >= len) {
        return;
      }
    }

    assert(non_start_level_input_files_filtered_.empty() ||
           non_start_level_input_files_filtered_.size() == inputs_.size() - 1);
    write += InputSummary(
        inputs_[level_iter].files,
        (level_iter == 0 || non_start_level_input_files_filtered_.empty())
            ? std::vector<bool>{}
            : non_start_level_input_files_filtered_[level_iter - 1],
        output + write, len - write);
    if (write < 0 || write >= len) {
      return;
    }
  }

  snprintf(output + write, len - write, "]");
}

uint64_t Compaction::OutputFilePreallocationSize() const {
  uint64_t preallocation_size = 0;

  for (const auto& level_files : inputs_) {
    for (const auto& file : level_files.files) {
      preallocation_size += file->fd.GetFileSize();
    }
  }

  if (max_output_file_size_ != std::numeric_limits<uint64_t>::max() &&
      (immutable_options_.compaction_style == kCompactionStyleLevel ||
       output_level() > 0)) {
    preallocation_size = std::min(max_output_file_size_, preallocation_size);
  }

  // Over-estimate slightly so we don't end up just barely crossing
  // the threshold
  // No point to preallocate more than 1GB.
  return std::min(uint64_t{1073741824},
                  preallocation_size + (preallocation_size / 10));
}

std::unique_ptr<CompactionFilter> Compaction::CreateCompactionFilter() const {
  if (!cfd_->ioptions().compaction_filter_factory) {
    return nullptr;
  }

  if (!cfd_->ioptions()
           .compaction_filter_factory->ShouldFilterTableFileCreation(
               TableFileCreationReason::kCompaction)) {
    return nullptr;
  }

  CompactionFilter::Context context;
  context.is_full_compaction = is_full_compaction_;
  context.is_manual_compaction = is_manual_compaction_;
  context.input_start_level = start_level_;
  context.column_family_id = cfd_->GetID();
  context.reason = TableFileCreationReason::kCompaction;
  context.input_table_properties = GetInputTableProperties();
  if (context.input_table_properties.empty()) {
    ROCKS_LOG_WARN(
        immutable_options_.info_log,
        "Unable to set `input_table_properties` of `CompactionFilter::Context` "
        "for compaction.");
  }

  return cfd_->ioptions().compaction_filter_factory->CreateCompactionFilter(
      context);
}

std::unique_ptr<SstPartitioner> Compaction::CreateSstPartitioner() const {
  if (!immutable_options_.sst_partitioner_factory) {
    return nullptr;
  }

  SstPartitioner::Context context;
  context.is_full_compaction = is_full_compaction_;
  context.is_manual_compaction = is_manual_compaction_;
  context.output_level = output_level_;
  context.smallest_user_key = smallest_user_key_;
  context.largest_user_key = largest_user_key_;
  return immutable_options_.sst_partitioner_factory->CreatePartitioner(context);
}

bool Compaction::IsOutputLevelEmpty() const {
  return inputs_.back().level != output_level_ || inputs_.back().empty();
}

bool Compaction::ShouldFormSubcompactions() const {
  if (cfd_ == nullptr) {
    return false;
  }

  if (mutable_cf_options_.table_factory->Name() ==
      TableFactory::kPlainTableName()) {
    return false;
  }

  // Round-Robin pri under leveled compaction allows subcompactions by default
  // and the number of subcompactions can be larger than max_subcompactions_
  if (cfd_->ioptions().compaction_pri == kRoundRobin &&
      cfd_->ioptions().compaction_style == kCompactionStyleLevel) {
    return output_level_ > 0;
  }

  if (max_subcompactions_ <= 1) {
    return false;
  }

  if (cfd_->ioptions().compaction_style == kCompactionStyleLevel) {
    return (start_level_ == 0 || is_manual_compaction_) && output_level_ > 0;
  } else if (cfd_->ioptions().compaction_style == kCompactionStyleUniversal) {
    return number_levels_ > 1 && output_level_ > 0;
  } else {
    return false;
  }
}

bool Compaction::DoesInputReferenceBlobFiles() const {
  assert(input_version_);

  const VersionStorageInfo* storage_info = input_version_->storage_info();
  assert(storage_info);

  if (storage_info->GetBlobFiles().empty()) {
    return false;
  }

  for (size_t i = 0; i < inputs_.size(); ++i) {
    for (const FileMetaData* meta : inputs_[i].files) {
      assert(meta);

      if (meta->oldest_blob_file_number != kInvalidBlobFileNumber) {
        return true;
      }
    }
  }

  return false;
}

bool Compaction::RepairsMissingBlobReferences() const {
  if (skip_missing_blob_files_) {
    return true;
  }

  assert(input_version_);
  const VersionStorageInfo* storage_info = input_version_->storage_info();
  assert(storage_info);

  const std::vector<uint64_t>& dropped_blob_files =
      storage_info->GetDroppedBlobFiles();
  if (dropped_blob_files.empty()) {
    return false;
  }

  // An SST file only references blob files at or above its oldest referenced
  // blob file.
  const uint64_t newest_dropped_blob_file = dropped_blob_files.back();
  for (const CompactionInputFiles& level_inputs : inputs_) {
    for (const FileMetaData* meta : level_inputs.files) {
      assert(meta);

      if (meta->oldest_blob_file_number != kInvalidBlobFileNumber &&
          meta->oldest_blob_file_number <= newest_dropped_blob_file) {
        return true;
      }
    }
  }

  return false;
}

uint64_t Compaction::MaxInputFileNewestKeyTime(const InternalKey* start,
                                               const InternalKey* end) const {
  uint64_t newest_key_time = kUnknownNewestKeyTime;
  const InternalKeyComparator& icmp =
      column_family_data()->internal_comparator();
  for (const auto& level_files : inputs_) {
    for (const auto& file : level_files.files) {
      if (start != nullptr && icmp.Compare(file->largest, *start) < 0) {
        continue;
      }
      if (end != nullptr && icmp.Compare(file->smallest, *end) > 0) {
        continue;
      }
      newest_key_time = std::max(newest_key_time, file->TryGetNewestKeyTime());
    }
  }
  return newest_key_time;
}

uint64_t Compaction::MinInputFileOldestAncestorTime(
    const InternalKey* start, const InternalKey* end) const {
  uint64_t min_oldest_ancestor_time = std::numeric_limits<uint64_t>::max();
  const InternalKeyComparator& icmp =
      column_family_data()->internal_comparator();
  for (const auto& level_files : inputs_) {
    for (const auto& file : level_files.files) {
      if (start != nullptr && icmp.Compare(file->largest, *start) < 0) {
        continue;
      }
      if (end != nullptr && icmp.Compare(file->smallest, *end) > 0) {
        continue;
      }
      uint64_t oldest_ancestor_time = file->TryGetOldestAncestorTime();
      if (oldest_ancestor_time != 0) {
        min_oldest_ancestor_time =
            std::min(min_oldest_ancestor_time, oldest_ancestor_time);
      }
    }
  }
  return min_oldest_ancestor_time;
}

uint64_t Compaction::MinInputFileEpochNumber() const {
  uint64_t min_epoch_number = std::numeric_limits<uint64_t>::max();
  for (const auto& inputs_per_level : inputs_) {
    for (const auto& file : inputs_per_level.files) {
      min_epoch_number = std::min(min_epoch_number, file->epoch_number);
    }
  }
  return min_epoch_number;
}

int Compaction::EvaluateProximalLevel(
    const VersionStorageInfo* vstorage,
    const MutableCFOptions& mutable_cf_options,
    const ImmutableOptions& immutable_options, const int start_level,
    const int output_level) {
  // TODO: currently per_key_placement feature only support level and universal
  //  compaction
  if (immutable_options.compaction_style != kCompactionStyleLevel &&
      immutable_options.compaction_style != kCompactionStyleUniversal) {
    return kInvalidLevel;
  }
  if (output_level != immutable_options.num_levels - 1) {
    return kInvalidLevel;
  }

  int proximal_level = output_level - 1;
  assert(proximal_level < immutable_options.num_levels);
  if (proximal_level <= 0) {
    return kInvalidLevel;
  }

  // If the proximal level is not within input level -> output level range
  // check if the proximal output level is empty, if it's empty, it could
  // also be locked for the proximal output.
  // TODO: ideally, it only needs to check if there's a file within the
  //  compaction output key range. For simplicity, it just check if there's any
  //  file on the proximal level.
  if (start_level == immutable_options.num_levels - 1 &&
      (immutable_options.compaction_style != kCompactionStyleUniversal ||
       !vstorage->LevelFiles(proximal_level).empty())) {
    return kInvalidLevel;
  }

  bool supports_per_key_placement =
      mutable_cf_options.preclude_last_level_data_seconds > 0;

  // it could be overridden by unittest
  TEST_SYNC_POINT_CALLBACK("Compaction::SupportsPerKeyPlacement:Enabled",
                           &supports_per_key_placement);
  if (!supports_per_key_placement) {
    return kInvalidLevel;
  }

  return proximal_level;
}

void Compaction::FilterInputsForCompactionIterator() {
  assert(earliest_snapshot_.has_value());
  // cfd_ is not populated at Compaction construction time, get it from
  // VersionStorageInfo instead.
  assert(input_vstorage_);
  const auto* ucmp = input_vstorage_->user_comparator();
  assert(ucmp);
  // Simply comparing file boundaries when user-defined timestamp is defined
  // is not as safe because we need to also compare timestamp to know for
  // sure. Although entries with higher timestamp is also supposed to have
  // higher sequence number for the same user key (without timestamp).
  assert(ucmp->timestamp_size() == 0);
  size_t num_input_levels = inputs_.size();
  // TODO(yuzhangyu): filtering of older L0 file by new L0 file is not
  // supported yet.
  FileMetaData* rangedel_candidate = inputs_[0].level == 0
                                         ? inputs_[0].files.back()
                                         : inputs_[0].files.front();
  assert(rangedel_candidate);
  if (!rangedel_candidate->FileIsStandAloneRangeTombstone() ||
      !DataIsDefinitelyInSnapshot(rangedel_candidate->fd.smallest_seqno,
                                  earliest_snapshot_.value(),
                                  snapshot_checker_)) {
    for (size_t level = 0; level < num_input_levels; level++) {
      DoGenerateLevelFilesBrief(&input_levels_[level], inputs_[level].files,
                                &arena_);
    }
    return;
  }

  Slice rangedel_start_ukey = rangedel_candidate->smallest.user_key();
  Slice rangedel_end_ukey = rangedel_candidate->largest.user_key();
  SequenceNumber rangedel_seqno = rangedel_candidate->fd.smallest_seqno;

  std::vector<std::vector<FileMetaData*>> non_start_level_input_files;
  non_start_level_input_files.reserve(num_input_levels - 1);
  non_start_level_input_files_filtered_.reserve(num_input_levels - 1);
  for (size_t level = 1; level < num_input_levels; level++) {
    non_start_level_input_files.emplace_back();
    non_start_level_input_files_filtered_.emplace_back();
    for (FileMetaData* file : inputs_[level].files) {
      non_start_level_input_files_filtered_.back().push_back(false);
      // When range data and point data has the same sequence number, point
      // data wins. Range deletion end key is exclusive, so check it's bigger
      // than file right boundary user key.
      if (rangedel_seqno > file->fd.largest_seqno &&
          ucmp->CompareWithoutTimestamp(rangedel_start_ukey,
                                        file->smallest.user_key()) <= 0 &&
          ucmp->CompareWithoutTimestamp(rangedel_end_ukey,
                                        file->largest.user_key()) > 0) {
        non_start_level_input_files_filtered_.back().back() = true;
        filtered_input_levels_[level].push_back(file);
      } else {
        non_start_level_input_files.back().push_back(file);
      }
    }
  }

  DoGenerateLevelFilesBrief(&input_levels_[0], inputs_[0].files, &arena_);
  assert(non_start_level_input_files.size() == num_input_levels - 1);
  for (size_t level = 1; level < num_input_levels; level++) {
    DoGenerateLevelFilesBrief(&input_levels_[level],
                              non_start_level_input_files[level - 1], &arena_);
  }
}

Temperature Compaction::GetOutputTemperature(bool is_proximal_level) const {
  if (output_temperature_override_ != Temperature::kUnknown) {
    return output_temperature_override_;
  }

  if (is_last_level() && !is_proximal_level &&
      mutable_cf_options_.last_level_temperature != Temperature::kUnknown) {
    return mutable_cf_options_.last_level_temperature;
  }

  return mutable_cf_options_.default_write_temperature;
}

}  // namespace ROCKSDB_NAMESPACE
