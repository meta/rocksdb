//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_picker_universal.h"

#include <cstdint>
#include <limits>
#include <queue>
#include <string>
#include <utility>

#include "db/column_family.h"
#include "file/filename.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"
#include "monitoring/statistics_impl.h"
#include "test_util/sync_point.h"
#include "util/random.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
namespace {
// A helper class that form universal compactions. The class is used by
// UniversalCompactionPicker::PickCompaction().
// The usage is to create the class, and get the compaction object by calling
// PickCompaction().
class UniversalCompactionBuilder {
 public:
  UniversalCompactionBuilder(
      const ImmutableOptions& ioptions, const InternalKeyComparator* icmp,
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      const MutableDBOptions& mutable_db_options,
      const std::vector<SequenceNumber>& existing_snapshots,
      const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
      UniversalCompactionPicker* picker, LogBuffer* log_buffer,
      bool require_max_output_level, const std::string& full_history_ts_low)
      : ioptions_(ioptions),
        icmp_(icmp),
        cf_name_(cf_name),
        mutable_cf_options_(mutable_cf_options),
        mutable_db_options_(mutable_db_options),
        vstorage_(vstorage),
        picker_(picker),
        log_buffer_(log_buffer),
        require_max_output_level_(require_max_output_level),
        allow_ingest_behind_(ioptions.cf_allow_ingest_behind ||
                             ioptions.allow_ingest_behind),
        full_history_ts_low_(full_history_ts_low) {
    assert(icmp_);
    const auto* ucmp = icmp_->user_comparator();
    assert(ucmp);
    // These parameters are only passed when user-defined timestamp is not
    // enabled.
    if (ucmp->timestamp_size() == 0) {
      earliest_snapshot_ = existing_snapshots.empty()
                               ? kMaxSequenceNumber
                               : existing_snapshots.at(0);
      snapshot_checker_ = snapshot_checker;
    }
  }

  // Form and return the compaction object. The caller owns return object.
  Compaction* PickCompaction();

 private:
  struct SortedRun {
    SortedRun(int _level, FileMetaData* _file, uint64_t _size,
              uint64_t _compensated_file_size, bool _being_compacted,
              bool _level_has_marked_standalone_rangedel)
        : level(_level),
          file(_file),
          size(_size),
          compensated_file_size(_compensated_file_size),
          being_compacted(_being_compacted),
          level_has_marked_standalone_rangedel(
              _level_has_marked_standalone_rangedel) {
      assert(compensated_file_size > 0);
      assert(level != 0 || file != nullptr);
    }

    void Dump(char* out_buf, size_t out_buf_size,
              bool print_path = false) const;

    // sorted_run_count is added into the string to print
    void DumpSizeInfo(char* out_buf, size_t out_buf_size,
                      size_t sorted_run_count) const;

    int level;
    // `file` Will be null for level > 0. For level = 0, the sorted run is
    // for this file.
    FileMetaData* file;
    // For level > 0, `size` and `compensated_file_size` are sum of sizes all
    // files in the level. `being_compacted` should be the same for all files
    // in a non-zero level. Use the value here.
    uint64_t size;
    uint64_t compensated_file_size;
    bool being_compacted;
    // True if this level has any file that is a standalone range deletion file
    // marked for compaction. Best effort is made to make only deletion
    // triggered compaction pick this type of file.
    bool level_has_marked_standalone_rangedel;
  };

  unsigned int GetMaxNumFilesToCompactBasedOnMaxReadAmp(
      const int file_num_compaction_trigger, const unsigned int ratio,
      int* num_sr_not_compacted_output, int* max_num_runs_output) const {
    assert(num_sr_not_compacted_output);
    assert(max_num_runs_output);
    int max_num_runs =
        mutable_cf_options_.compaction_options_universal.max_read_amp;
    if (max_num_runs < 0) {
      // any value < -1 is not valid
      assert(max_num_runs == -1);
      // By default, fall back to `level0_file_num_compaction_trigger`
      max_num_runs = file_num_compaction_trigger;
    } else if (max_num_runs == 0) {
      if (mutable_cf_options_.compaction_options_universal.stop_style ==
          kCompactionStopStyleTotalSize) {
        // 0 means auto-tuning by RocksDB. We estimate max num run based on
        // max_run_size, size_ratio and write buffer size:
        // Assume the size of the lowest level size is equal to
        // write_buffer_size. Each subsequent level is the max size without
        // triggering size_ratio compaction. `max_num_runs` is the minimum
        // number of levels required such that the target size of the
        // largest level is at least `max_run_size_`.
        max_num_runs = 1;
        double cur_level_max_size =
            static_cast<double>(mutable_cf_options_.write_buffer_size);
        double total_run_size = 0;
        while (cur_level_max_size < static_cast<double>(max_run_size_)) {
          // This loop should not take too many iterations since
          // cur_level_max_size at least doubles each iteration.
          total_run_size += cur_level_max_size;
          cur_level_max_size = (100.0 + ratio) / 100.0 * total_run_size;
          ++max_num_runs;
        }
      } else {
        // TODO: implement the auto-tune logic for this stop style
        max_num_runs = file_num_compaction_trigger;
      }
    } else {
      // max_num_runs > 0, it's the limit on the number of sorted run
    }

    // Get the total number of sorted runs that are not being compacted
    int num_sr_not_compacted = 0;
    for (size_t i = 0; i < sorted_runs_.size(); i++) {
      if (sorted_runs_[i].being_compacted == false &&
          !sorted_runs_[i].level_has_marked_standalone_rangedel) {
        num_sr_not_compacted++;
      }
    }

    *num_sr_not_compacted_output = num_sr_not_compacted;
    *max_num_runs_output = max_num_runs;

    if (num_sr_not_compacted > max_num_runs) {
      return num_sr_not_compacted - max_num_runs + 1;
    } else {
      return 0;
    }
  }

  Compaction* MaybePickPeriodicCompaction(Compaction* const prev_picked_c) {
    if (prev_picked_c != nullptr ||
        vstorage_->FilesMarkedForPeriodicCompaction().empty()) {
      return prev_picked_c;
    }
    // Always need to do a full compaction for periodic compaction.
    Compaction* c = PickPeriodicCompaction();
    TEST_SYNC_POINT_CALLBACK("PostPickPeriodicCompaction", c);
    if (c != nullptr) {
      ROCKS_LOG_BUFFER(log_buffer_,
                       "[%s] Universal: picked for periodic compaction\n",
                       cf_name_.c_str());
    }
    return c;
  }

  Compaction* MaybePickSizeAmpCompaction(Compaction* const prev_picked_c,
                                         int file_num_compaction_trigger) {
    if (prev_picked_c != nullptr ||
        sorted_runs_.size() <
            static_cast<size_t>(file_num_compaction_trigger)) {
      return prev_picked_c;
    }
    Compaction* c = PickCompactionToReduceSizeAmp();
    if (c != nullptr) {
      TEST_SYNC_POINT("PickCompactionToReduceSizeAmpReturnNonnullptr");
      ROCKS_LOG_BUFFER(log_buffer_,
                       "[%s] Universal: picked for size amp compaction \n",
                       cf_name_.c_str());
    }
    return c;
  }

  Compaction* MaybePickCompactionToReduceSortedRunsBasedFileRatio(
      Compaction* const prev_picked_c, int file_num_compaction_trigger,
      unsigned int ratio) {
    if (prev_picked_c != nullptr ||
        sorted_runs_.size() <
            static_cast<size_t>(file_num_compaction_trigger)) {
      return prev_picked_c;
    }
    Compaction* c = PickCompactionToReduceSortedRuns(ratio, UINT_MAX);
    if (c != nullptr) {
      TEST_SYNC_POINT("PickCompactionToReduceSortedRunsReturnNonnullptr");
      ROCKS_LOG_BUFFER(log_buffer_,
                       "[%s] Universal: picked for size ratio compaction to "
                       "reduce sorted run\n",
                       cf_name_.c_str());
    }
    return c;
  }

  Compaction* MaybePickCompactionToReduceSortedRuns(
      Compaction* const prev_picked_c, int file_num_compaction_trigger,
      unsigned int ratio) {
    if (prev_picked_c != nullptr ||
        sorted_runs_.size() <
            static_cast<size_t>(file_num_compaction_trigger)) {
      return prev_picked_c;
    }

    int num_sr_not_compacted = 0;
    int max_num_runs = 0;
    const unsigned int max_num_files_to_compact =
        GetMaxNumFilesToCompactBasedOnMaxReadAmp(file_num_compaction_trigger,
                                                 ratio, &num_sr_not_compacted,
                                                 &max_num_runs);
    if (max_num_files_to_compact == 0) {
      ROCKS_LOG_BUFFER(
          log_buffer_,
          "[%s] Universal: skipping compaction to reduce sorted run, num "
          "sorted runs not "
          "being compacted -- %u, max num runs allowed -- %d, max_run_size "
          "-- %" PRIu64 "\n",
          cf_name_.c_str(), num_sr_not_compacted, max_num_runs, max_run_size_);
      return nullptr;
    }

    Compaction* c =
        PickCompactionToReduceSortedRuns(UINT_MAX, max_num_files_to_compact);
    if (c != nullptr) {
      ROCKS_LOG_BUFFER(log_buffer_,
                       "[%s] Universal: picked for sorted run num compaction "
                       "to reduce sorted run, to "
                       "compact file num -- %u, max num runs allowed"
                       "-- %d, max_run_size -- %" PRIu64 "\n",
                       cf_name_.c_str(), max_num_files_to_compact, max_num_runs,
                       max_run_size_);
    }
    return c;
  }

  Compaction* MaybePickDeleteTriggeredCompaction(
      Compaction* const prev_picked_c) {
    if (prev_picked_c != nullptr) {
      return prev_picked_c;
    }
    Compaction* c = PickDeleteTriggeredCompaction();
    if (c != nullptr) {
      TEST_SYNC_POINT("PickDeleteTriggeredCompactionReturnNonnullptr");
      ROCKS_LOG_BUFFER(
          log_buffer_,
          "[%s] Universal: picked for delete triggered compaction\n",
          cf_name_.c_str());
    }
    return c;
  }

  Compaction* MaybePickReadTriggeredCompaction(
      Compaction* const prev_picked_c) {
    if (prev_picked_c != nullptr ||
        vstorage_->ReadTriggeredCompactionFiles().empty()) {
      return prev_picked_c;
    }
    Compaction* c = PickReadTriggeredCompaction();
    if (c != nullptr) {
      ROCKS_LOG_BUFFER(log_buffer_,
                       "[%s] Universal: picked for read triggered compaction\n",
                       cf_name_.c_str());
    }
    return c;
  }

  // Pick Universal compaction to limit read amplification
  Compaction* PickCompactionToReduceSortedRuns(
      unsigned int ratio, unsigned int max_number_of_files_to_compact);

  // Pick Universal compaction to limit space amplification.
  Compaction* PickCompactionToReduceSizeAmp();

  // Try to pick incremental compaction to reduce space amplification.
  // It will return null if it cannot find a fanout within the threshold.
  // Fanout is defined as
  //    total size of files to compact at output level
  //  --------------------------------------------------
  //    total size of files to compact at other levels
  Compaction* PickIncrementalForReduceSizeAmp(double fanout_threshold);

  Compaction* PickDeleteTriggeredCompaction();

  Compaction* PickReadTriggeredCompaction();

  // Given already-selected start_level_inputs, find the first non-empty output
  // level, compute overlapping inputs, and create a Compaction. Can produce
  // intra-L0 compaction if there is only level 0.
  Compaction* BuildCompactionToNextLevel(
      CompactionInputFiles& start_level_inputs, CompactionReason reason);

  // Returns true if this given file (that is marked be compaction) should be
  // skipped from being picked for now. We do this to best use standalone range
  // tombstone files.
  bool ShouldSkipMarkedFile(const FileMetaData* file) const;

  // Form a compaction from the sorted run indicated by start_index to the
  // oldest sorted run.
  // The caller is responsible for making sure that those files are not in
  // compaction.
  Compaction* PickCompactionToOldest(size_t start_index,
                                     CompactionReason compaction_reason);

  Compaction* PickCompactionWithSortedRunRange(
      size_t start_index, size_t end_index, CompactionReason compaction_reason);

  // Try to pick periodic compaction. The caller should only call it
  // if there is at least one file marked for periodic compaction.
  // null will be returned if no such a compaction can be formed
  // because some files are being compacted.
  Compaction* PickPeriodicCompaction();

  bool ShouldSkipLastSortedRunForSizeAmpCompaction() const {
    assert(!sorted_runs_.empty());
    return mutable_cf_options_.preclude_last_level_data_seconds > 0 &&
           ioptions_.num_levels > 2 &&
           sorted_runs_.back().level == ioptions_.num_levels - 1 &&
           sorted_runs_.size() > 1;
  }
  // Used in universal compaction when the allow_trivial_move
  // option is set. Checks whether there are any overlapping files
  // in the input. Returns true if the input files are non
  // overlapping.
  bool IsInputFilesNonOverlapping(Compaction* c);

  uint64_t GetMaxOverlappingBytes() const;

  // To conditionally exclude some of the newest L0 files
  // from a size amp compaction. This is to prevent a large number of L0
  // files from being locked by a size amp compaction, potentially leading to
  // write stop with a few more flushes.
  //
  // Such exclusion is based on `num_l0_input_pre_exclusion`,
  // `level0_stop_writes_trigger`, `max/min_merge_width` and the pre-exclusion
  // compaction score. Noted that it will not make the size amp compaction of
  // interest invalid from running as a size amp compaction as long as its
  // pre-exclusion compaction score satisfies the condition to run.
  //
  // @param `num_l0_input_pre_exclusion` Number of L0 input files prior to
  // exclusion
  // @param `end_index` Index of the last sorted run selected as compaction
  // input. Will not be affected by this exclusion.
  // @param `start_index` Index of the first input sorted run prior to
  // exclusion. Will be modified as output based on the exclusion.
  // @param `candidate_size` Total size of all except for the last input sorted
  // runs prior to exclusion. Will be modified as output based on the exclusion.
  //
  // @return Number of L0 files to exclude. `start_index` and
  // `candidate_size` will be modified accordingly
  std::size_t MightExcludeNewL0sToReduceWriteStop(
      std::size_t num_l0_input_pre_exclusion, std::size_t end_index,
      std::size_t& start_index, uint64_t& candidate_size) const {
    if (num_l0_input_pre_exclusion == 0) {
      return 0;
    }

    assert(start_index <= end_index && sorted_runs_.size() > end_index);
    assert(mutable_cf_options_.level0_stop_writes_trigger > 0);
    const std::size_t level0_stop_writes_trigger = static_cast<std::size_t>(
        mutable_cf_options_.level0_stop_writes_trigger);
    const std::size_t max_merge_width = static_cast<std::size_t>(
        mutable_cf_options_.compaction_options_universal.max_merge_width);
    const std::size_t min_merge_width = static_cast<std::size_t>(
        mutable_cf_options_.compaction_options_universal.min_merge_width);
    const uint64_t max_size_amplification_percent =
        mutable_cf_options_.compaction_options_universal
            .max_size_amplification_percent;
    const uint64_t base_sr_size = sorted_runs_[end_index].size;

    // Leave at least 1 L0 file and 2 input sorted runs after exclusion
    const std::size_t max_num_l0_to_exclude =
        std::min(num_l0_input_pre_exclusion - 1, end_index - start_index - 1);
    // In universal compaction, sorted runs from non L0 levels are counted
    // toward `level0_stop_writes_trigger`. Therefore we need to subtract the
    // total number of sorted runs picked originally for this compaction from
    // `level0_stop_writes_trigger` to calculate
    // `num_extra_l0_before_write_stop`
    const std::size_t num_extra_l0_before_write_stop =
        level0_stop_writes_trigger -
        std::min(level0_stop_writes_trigger, end_index - start_index + 1);
    const std::size_t num_l0_to_exclude_for_max_merge_width =
        std::min(max_merge_width -
                     std::min(max_merge_width, num_extra_l0_before_write_stop),
                 max_num_l0_to_exclude);
    const std::size_t num_l0_to_exclude_for_min_merge_width =
        std::min(min_merge_width -
                     std::min(min_merge_width, num_extra_l0_before_write_stop),
                 max_num_l0_to_exclude);

    std::size_t num_l0_to_exclude = 0;
    uint64_t candidate_size_post_exclusion = candidate_size;

    for (std::size_t possible_num_l0_to_exclude =
             num_l0_to_exclude_for_min_merge_width;
         possible_num_l0_to_exclude <= num_l0_to_exclude_for_max_merge_width;
         ++possible_num_l0_to_exclude) {
      uint64_t current_candidate_size = candidate_size_post_exclusion;
      for (std::size_t j = num_l0_to_exclude; j < possible_num_l0_to_exclude;
           ++j) {
        current_candidate_size -=
            sorted_runs_.at(start_index + j).compensated_file_size;
      }

      // To ensure the compaction score before and after exclusion is similar
      // so this exclusion will not make the size amp compaction of
      // interest invalid from running as a size amp compaction as long as its
      // pre-exclusion compaction score satisfies the condition to run.
      if (current_candidate_size * 100 <
              max_size_amplification_percent * base_sr_size ||
          current_candidate_size < candidate_size * 9 / 10) {
        break;
      }
      num_l0_to_exclude = possible_num_l0_to_exclude;
      candidate_size_post_exclusion = current_candidate_size;
    }

    start_index += num_l0_to_exclude;
    candidate_size = candidate_size_post_exclusion;
    return num_l0_to_exclude;
  }

  bool MeetsOutputLevelRequirements(int output_level) const {
    return !require_max_output_level_ ||
           Compaction::OutputToNonZeroMaxOutputLevel(
               output_level, vstorage_->MaxOutputLevel(allow_ingest_behind_));
  }

  const ImmutableOptions& ioptions_;
  const InternalKeyComparator* icmp_;
  double score_;
  std::vector<SortedRun> sorted_runs_;
  uint64_t max_run_size_;
  const std::string& cf_name_;
  const MutableCFOptions& mutable_cf_options_;
  const MutableDBOptions& mutable_db_options_;
  VersionStorageInfo* vstorage_;
  UniversalCompactionPicker* picker_;
  LogBuffer* log_buffer_;
  // Optional earliest snapshot at time of compaction picking. This is only
  // provided if the column family doesn't enable user-defined timestamps.
  // And this information is only passed to `Compaction` picked by deletion
  // triggered compaction for possible optimizations.
  std::optional<SequenceNumber> earliest_snapshot_;
  const SnapshotChecker* snapshot_checker_ = nullptr;
  // Mapping from file id to its index in the sorted run for the files that are
  // marked for compaction. This is only populated when snapshot info is
  // populated.
  std::map<uint64_t, size_t> file_marked_for_compaction_to_sorted_run_index_;
  bool require_max_output_level_;
  bool allow_ingest_behind_;
  const std::string& full_history_ts_low_;

  std::vector<UniversalCompactionBuilder::SortedRun> CalculateSortedRuns(
      const VersionStorageInfo& vstorage, int last_level,
      uint64_t* max_run_size);

  // Pick a path ID to place a newly generated file, with its estimated file
  // size.
  static uint32_t GetPathId(const ImmutableCFOptions& ioptions,
                            const MutableCFOptions& mutable_cf_options,
                            uint64_t file_size);
};

// Used in universal compaction when trivial move is enabled.
// This structure is used for the construction of min heap
// that contains the file meta data, the level of the file
// and the index of the file in that level

struct InputFileInfo {
  InputFileInfo() : InputFileInfo(nullptr, 0, 0) {}
  InputFileInfo(FileMetaData* file_meta, size_t l, size_t i)
      : f(file_meta), level(l), index(i) {}

  FileMetaData* f;
  size_t level;
  size_t index;
};

// Used in universal compaction when trivial move is enabled.
// This comparator is used for the construction of min heap
// based on the smallest key of the file.
struct SmallestKeyHeapComparator {
  explicit SmallestKeyHeapComparator(const Comparator* ucmp) { ucmp_ = ucmp; }

  bool operator()(InputFileInfo i1, InputFileInfo i2) const {
    return (ucmp_->CompareWithoutTimestamp(i1.f->smallest.user_key(),
                                           i2.f->smallest.user_key()) > 0);
  }

 private:
  const Comparator* ucmp_;
};

using SmallestKeyHeap =
    std::priority_queue<InputFileInfo, std::vector<InputFileInfo>,
                        SmallestKeyHeapComparator>;

// This function creates the heap that is used to find if the files are
// overlapping during universal compaction when the allow_trivial_move
// is set.
SmallestKeyHeap create_level_heap(Compaction* c, const Comparator* ucmp) {
  SmallestKeyHeap smallest_key_priority_q =
      SmallestKeyHeap(SmallestKeyHeapComparator(ucmp));

  for (size_t l = 0; l < c->num_input_levels(); l++) {
    if (c->num_input_files(l) != 0) {
      if (l == 0 && c->start_level() == 0) {
        for (size_t i = 0; i < c->num_input_files(0); i++) {
          smallest_key_priority_q.emplace(c->input(0, i), 0, i);
        }
      } else {
        smallest_key_priority_q.emplace(c->input(l, 0), l, 0);
      }
    }
  }
  return smallest_key_priority_q;
}

#ifndef NDEBUG
// smallest_seqno and largest_seqno are set iff. `files` is not empty.
void GetSmallestLargestSeqno(const std::vector<FileMetaData*>& files,
                             SequenceNumber* smallest_seqno,
                             SequenceNumber* largest_seqno) {
  bool is_first = true;
  for (FileMetaData* f : files) {
    assert(f->fd.smallest_seqno <= f->fd.largest_seqno);
    if (is_first) {
      is_first = false;
      *smallest_seqno = f->fd.smallest_seqno;
      *largest_seqno = f->fd.largest_seqno;
    } else {
      if (f->fd.smallest_seqno < *smallest_seqno) {
        *smallest_seqno = f->fd.smallest_seqno;
      }
      if (f->fd.largest_seqno > *largest_seqno) {
        *largest_seqno = f->fd.largest_seqno;
      }
    }
  }
}
#endif
}  // namespace

// Algorithm that checks to see if there are any overlapping
// files in the input
bool UniversalCompactionBuilder::IsInputFilesNonOverlapping(Compaction* c) {
  auto comparator = icmp_->user_comparator();
  int first_iter = 1;

  InputFileInfo prev;
  InputFileInfo curr;

  SmallestKeyHeap smallest_key_priority_q =
      create_level_heap(c, icmp_->user_comparator());

  while (!smallest_key_priority_q.empty()) {
    curr = smallest_key_priority_q.top();
    smallest_key_priority_q.pop();

    if (first_iter) {
      prev = curr;
      first_iter = 0;
    } else {
      if (comparator->CompareWithoutTimestamp(
              prev.f->largest.user_key(), curr.f->smallest.user_key()) >= 0) {
        // found overlapping files, return false
        return false;
      }
      assert(comparator->CompareWithoutTimestamp(
                 curr.f->largest.user_key(), prev.f->largest.user_key()) > 0);
      prev = curr;
    }

    if (c->level(curr.level) != 0 &&
        curr.index < c->num_input_files(curr.level) - 1) {
      smallest_key_priority_q.emplace(c->input(curr.level, curr.index + 1),
                                      curr.level, curr.index + 1);
    }
  }
  return true;
}

bool UniversalCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  const int kLevel0 = 0;
  if (vstorage->CompactionScore(kLevel0) >= 1) {
    return true;
  }
  if (!vstorage->FilesMarkedForPeriodicCompaction().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForCompaction().empty()) {
    return true;
  }
  if (!vstorage->ReadTriggeredCompactionFiles().empty()) {
    return true;
  }
  return false;
}

// TODO leverage full_history_ts_low in universal compaction picking. It could
// help reduce the same infinite compaction loop issue found in level
// compaction.
Compaction* UniversalCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& existing_snapshots,
    const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer, const std::string& full_history_ts_low,
    bool require_max_output_level) {
  UniversalCompactionBuilder builder(
      ioptions_, icmp_, cf_name, mutable_cf_options, mutable_db_options,
      existing_snapshots, snapshot_checker, vstorage, this, log_buffer,
      require_max_output_level, full_history_ts_low);
  return builder.PickCompaction();
}

void UniversalCompactionBuilder::SortedRun::Dump(char* out_buf,
                                                 size_t out_buf_size,
                                                 bool print_path) const {
  if (level == 0) {
    assert(file != nullptr);
    if (file->fd.GetPathId() == 0 || !print_path) {
      snprintf(out_buf, out_buf_size, "file %" PRIu64, file->fd.GetNumber());
    } else {
      snprintf(out_buf, out_buf_size,
               "file %" PRIu64
               "(path "
               "%" PRIu32 ")",
               file->fd.GetNumber(), file->fd.GetPathId());
    }
  } else {
    snprintf(out_buf, out_buf_size, "level %d", level);
  }
}

void UniversalCompactionBuilder::SortedRun::DumpSizeInfo(
    char* out_buf, size_t out_buf_size, size_t sorted_run_count) const {
  if (level == 0) {
    assert(file != nullptr);
    snprintf(out_buf, out_buf_size,
             "file %" PRIu64 "[%" ROCKSDB_PRIszt
             "] "
             "with size %" PRIu64 " (compensated size %" PRIu64 ")",
             file->fd.GetNumber(), sorted_run_count, file->fd.GetFileSize(),
             file->compensated_file_size);
  } else {
    snprintf(out_buf, out_buf_size,
             "level %d[%" ROCKSDB_PRIszt
             "] "
             "with size %" PRIu64 " (compensated size %" PRIu64 ")",
             level, sorted_run_count, size, compensated_file_size);
  }
}

std::vector<UniversalCompactionBuilder::SortedRun>
UniversalCompactionBuilder::CalculateSortedRuns(
    const VersionStorageInfo& vstorage, int last_level,
    uint64_t* max_run_size) {
  assert(max_run_size);
  *max_run_size = 0;
  std::vector<UniversalCompactionBuilder::SortedRun> ret;
  for (FileMetaData* f : vstorage.LevelFiles(0)) {
    if (earliest_snapshot_.has_value() && f->marked_for_compaction) {
      file_marked_for_compaction_to_sorted_run_index_.emplace(f->fd.GetNumber(),
                                                              ret.size());
    }
    ret.emplace_back(
        0, f, f->fd.GetFileSize(), f->compensated_file_size, f->being_compacted,
        f->marked_for_compaction && f->FileIsStandAloneRangeTombstone());
    *max_run_size = std::max(*max_run_size, f->fd.GetFileSize());
  }
  for (int level = 1; level <= last_level; level++) {
    uint64_t total_compensated_size = 0U;
    uint64_t total_size = 0U;
    bool being_compacted = false;
    bool level_has_marked_standalone_rangedel = false;
    for (FileMetaData* f : vstorage.LevelFiles(level)) {
      total_compensated_size += f->compensated_file_size;
      total_size += f->fd.GetFileSize();
      // Size amp, read amp and periodic compactions always include all files
      // for a non-zero level. However, a delete triggered compaction and
      // a trivial move might pick a subset of files in a sorted run. So
      // always check all files in a sorted run and mark the entire run as
      // being compacted if one or more files are being compacted
      if (f->being_compacted) {
        being_compacted = f->being_compacted;
      }
      level_has_marked_standalone_rangedel =
          level_has_marked_standalone_rangedel ||
          (f->marked_for_compaction && f->FileIsStandAloneRangeTombstone());
      if (earliest_snapshot_.has_value() && f->marked_for_compaction) {
        file_marked_for_compaction_to_sorted_run_index_.emplace(
            f->fd.GetNumber(), ret.size());
      }
    }
    if (total_compensated_size > 0) {
      ret.emplace_back(level, nullptr, total_size, total_compensated_size,
                       being_compacted, level_has_marked_standalone_rangedel);
    }
    *max_run_size = std::max(*max_run_size, total_size);
  }
  return ret;
}

bool UniversalCompactionBuilder::ShouldSkipMarkedFile(
    const FileMetaData* file) const {
  assert(file->marked_for_compaction);
  if (!earliest_snapshot_.has_value()) {
    return false;
  }
  if (!file->FileIsStandAloneRangeTombstone()) {
    return false;
  }
  // Skip until earliest snapshot advances at or above this standalone range
  // tombstone file. `DB::ReleaseSnapshot` will re-examine and schedule
  // compaction for it.
  if (!DataIsDefinitelyInSnapshot(file->fd.largest_seqno,
                                  earliest_snapshot_.value(),
                                  snapshot_checker_)) {
    return true;
  }

  auto iter = file_marked_for_compaction_to_sorted_run_index_.find(
      file->fd.GetNumber());
  assert(iter != file_marked_for_compaction_to_sorted_run_index_.end());
  size_t idx = iter->second;
  const SortedRun* succeeding_sorted_run =
      idx < sorted_runs_.size() - 1 ? &sorted_runs_[idx + 1] : nullptr;
  // Marked standalone range tombstone file is best used if it's in the start
  // input level. Skip to let that compaction happen first.
  if (succeeding_sorted_run &&
      succeeding_sorted_run->level_has_marked_standalone_rangedel) {
    return true;
  }

  return false;
}

// Universal style of compaction. Pick files that are contiguous in
// time-range to compact.
Compaction* UniversalCompactionBuilder::PickCompaction() {
  const int kLevel0 = 0;
  score_ = vstorage_->CompactionScore(kLevel0);
  const int max_output_level = vstorage_->MaxOutputLevel(allow_ingest_behind_);
  const int file_num_compaction_trigger =
      mutable_cf_options_.level0_file_num_compaction_trigger;
  const unsigned int ratio =
      mutable_cf_options_.compaction_options_universal.size_ratio;

  if (max_output_level == 0 &&
      !MeetsOutputLevelRequirements(0 /* output_level */)) {
    return nullptr;
  }

  max_run_size_ = 0;
  sorted_runs_ =
      CalculateSortedRuns(*vstorage_, max_output_level, &max_run_size_);

  if (sorted_runs_.size() == 0 ||
      (vstorage_->FilesMarkedForPeriodicCompaction().empty() &&
       vstorage_->FilesMarkedForCompaction().empty() &&
       vstorage_->ReadTriggeredCompactionFiles().empty() &&
       sorted_runs_.size() < (unsigned int)file_num_compaction_trigger)) {
    ROCKS_LOG_BUFFER(log_buffer_, "[%s] Universal: nothing to do\n",
                     cf_name_.c_str());
    TEST_SYNC_POINT_CALLBACK(
        "UniversalCompactionBuilder::PickCompaction:Return", nullptr);
    return nullptr;
  }

  VersionStorageInfo::LevelSummaryStorage tmp;
  ROCKS_LOG_BUFFER_MAX_SZ(
      log_buffer_, 3072,
      "[%s] Universal: sorted runs: %" ROCKSDB_PRIszt " files: %s\n",
      cf_name_.c_str(), sorted_runs_.size(), vstorage_->LevelSummary(&tmp));

  Compaction* c = nullptr;

  c = MaybePickPeriodicCompaction(c);
  c = MaybePickSizeAmpCompaction(c, file_num_compaction_trigger);
  c = MaybePickCompactionToReduceSortedRunsBasedFileRatio(
      c, file_num_compaction_trigger, ratio);
  c = MaybePickCompactionToReduceSortedRuns(c, file_num_compaction_trigger,
                                            ratio);
  c = MaybePickDeleteTriggeredCompaction(c);
  c = MaybePickReadTriggeredCompaction(c);

  if (c == nullptr) {
    TEST_SYNC_POINT_CALLBACK(
        "UniversalCompactionBuilder::PickCompaction:Return", nullptr);
    return nullptr;
  }
  assert(c->output_level() <= vstorage_->MaxOutputLevel(allow_ingest_behind_));
  assert(MeetsOutputLevelRequirements(c->output_level()));

  if (mutable_cf_options_.compaction_options_universal.allow_trivial_move ==
          true &&
      c->compaction_reason() != CompactionReason::kPeriodicCompaction) {
    c->set_is_trivial_move(IsInputFilesNonOverlapping(c));
  }

// validate that all the chosen files of L0 are non overlapping in time
#ifndef NDEBUG
  bool is_first = true;

  size_t level_index = 0U;
  if (c->start_level() == 0) {
    for (auto f : *c->inputs(0)) {
      assert(f->fd.smallest_seqno <= f->fd.largest_seqno);
      if (is_first) {
        is_first = false;
      }
    }
    level_index = 1U;
  }
  for (; level_index < c->num_input_levels(); level_index++) {
    if (c->num_input_files(level_index) != 0) {
      SequenceNumber smallest_seqno = 0U;
      SequenceNumber largest_seqno = 0U;
      GetSmallestLargestSeqno(*(c->inputs(level_index)), &smallest_seqno,
                              &largest_seqno);
      if (is_first) {
        is_first = false;
      }
    }
  }
#endif
  // update statistics
  size_t num_files = 0;
  for (auto& each_level : *c->inputs()) {
    num_files += each_level.files.size();
  }
  RecordInHistogram(ioptions_.stats, NUM_FILES_IN_SINGLE_COMPACTION, num_files);

  picker_->RegisterCompaction(c);
  vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_,
                                    full_history_ts_low_);

  TEST_SYNC_POINT_CALLBACK("UniversalCompactionBuilder::PickCompaction:Return",
                           c);
  return c;
}

uint32_t UniversalCompactionBuilder::GetPathId(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, uint64_t file_size) {
  // Two conditions need to be satisfied:
  // (1) the target path needs to be able to hold the file's size
  // (2) Total size left in this and previous paths need to be not
  //     smaller than expected future file size before this new file is
  //     compacted, which is estimated based on size_ratio.
  // For example, if now we are compacting files of size (1, 1, 2, 4, 8),
  // we will make sure the target file, probably with size of 16, will be
  // placed in a path so that eventually when new files are generated and
  // compacted to (1, 1, 2, 4, 8, 16), all those files can be stored in or
  // before the path we chose.
  //
  // TODO(sdong): now the case of multiple column families is not
  // considered in this algorithm. So the target size can be violated in
  // that case. We need to improve it.
  uint64_t accumulated_size = 0;
  uint64_t future_size =
      file_size *
      (100 - mutable_cf_options.compaction_options_universal.size_ratio) / 100;
  uint32_t p = 0;
  assert(!ioptions.cf_paths.empty());
  for (; p < ioptions.cf_paths.size() - 1; p++) {
    uint64_t target_size = ioptions.cf_paths[p].target_size;
    if (target_size > file_size &&
        accumulated_size + (target_size - file_size) > future_size) {
      return p;
    }
    accumulated_size += target_size;
  }
  return p;
}

//
// Consider compaction files based on their size differences with
// the next file in time order.
//
Compaction* UniversalCompactionBuilder::PickCompactionToReduceSortedRuns(
    unsigned int ratio, unsigned int max_number_of_files_to_compact) {
  unsigned int min_merge_width =
      mutable_cf_options_.compaction_options_universal.min_merge_width;
  unsigned int max_merge_width =
      mutable_cf_options_.compaction_options_universal.max_merge_width;

  const SortedRun* sr = nullptr;
  bool done = false;
  size_t start_index = 0;
  unsigned int candidate_count = 0;

  unsigned int max_files_to_compact =
      std::min(max_merge_width, max_number_of_files_to_compact);
  min_merge_width = std::max(min_merge_width, 2U);

  // Caller checks the size before executing this function. This invariant is
  // important because otherwise we may have a possible integer underflow when
  // dealing with unsigned types.
  assert(sorted_runs_.size() > 0);

  // Considers a candidate file only if it is smaller than the
  // total size accumulated so far.
  for (size_t loop = 0; loop < sorted_runs_.size(); loop++) {
    candidate_count = 0;

    // Skip files that are already being compacted
    for (sr = nullptr; loop < sorted_runs_.size(); loop++) {
      sr = &sorted_runs_[loop];

      if (!sr->being_compacted && !sr->level_has_marked_standalone_rangedel) {
        candidate_count = 1;
        break;
      }
      char file_num_buf[kFormatFileNumberBufSize];
      sr->Dump(file_num_buf, sizeof(file_num_buf));
      if (sr->being_compacted) {
        ROCKS_LOG_BUFFER(log_buffer_,
                         "[%s] Universal: %s"
                         "[%" ROCKSDB_PRIszt
                         "] being compacted, skipping for compaction to "
                         "reduce sorted runs",
                         cf_name_.c_str(), file_num_buf, loop);
      } else if (sr->level_has_marked_standalone_rangedel) {
        ROCKS_LOG_BUFFER(
            log_buffer_,
            "[%s] Universal: %s"
            "[%" ROCKSDB_PRIszt
            "] has standalone range tombstone files marked for "
            "compaction, skipping for compaction to reduce sorted runs",
            cf_name_.c_str(), file_num_buf, loop);
      }

      sr = nullptr;
    }

    // This file is not being compacted. Consider it as the
    // first candidate to be compacted.
    uint64_t candidate_size = sr != nullptr ? sr->compensated_file_size : 0;
    if (sr != nullptr) {
      char file_num_buf[kFormatFileNumberBufSize];
      sr->Dump(file_num_buf, sizeof(file_num_buf), true);
      ROCKS_LOG_BUFFER(log_buffer_,
                       "[%s] Universal: Possible candidate for compaction to "
                       "reduce sorted runs %s[%" ROCKSDB_PRIszt "].",
                       cf_name_.c_str(), file_num_buf, loop);
    }

    // Check if the succeeding files need compaction.
    for (size_t i = loop + 1;
         candidate_count < max_files_to_compact && i < sorted_runs_.size();
         i++) {
      const SortedRun* succeeding_sr = &sorted_runs_[i];
      if (succeeding_sr->being_compacted ||
          succeeding_sr->level_has_marked_standalone_rangedel) {
        break;
      }
      // Pick files if the total/last candidate file size (increased by the
      // specified ratio) is still larger than the next candidate file.
      // candidate_size is the total size of files picked so far with the
      // default kCompactionStopStyleTotalSize; with
      // kCompactionStopStyleSimilarSize, it's simply the size of the last
      // picked file.
      double sz = candidate_size * (100.0 + ratio) / 100.0;
      if (sz < static_cast<double>(succeeding_sr->size)) {
        break;
      }
      if (mutable_cf_options_.compaction_options_universal.stop_style ==
          kCompactionStopStyleSimilarSize) {
        // Similar-size stopping rule: also check the last picked file isn't
        // far larger than the next candidate file.
        sz = (succeeding_sr->size * (100.0 + ratio)) / 100.0;
        if (sz < static_cast<double>(candidate_size)) {
          // If the small file we've encountered begins a run of similar-size
          // files, we'll pick them up on a future iteration of the outer
          // loop. If it's some lonely straggler, it'll eventually get picked
          // by the last-resort read amp strategy which disregards size ratios.
          break;
        }
        candidate_size = succeeding_sr->compensated_file_size;
      } else {  // default kCompactionStopStyleTotalSize
        candidate_size += succeeding_sr->compensated_file_size;
      }
      candidate_count++;
    }

    // Found a series of consecutive files that need compaction.
    if (candidate_count >= (unsigned int)min_merge_width) {
      start_index = loop;
      done = true;
      break;
    } else {
      for (size_t i = loop;
           i < loop + candidate_count && i < sorted_runs_.size(); i++) {
        const SortedRun* skipping_sr = &sorted_runs_[i];
        char file_num_buf[256];
        skipping_sr->DumpSizeInfo(file_num_buf, sizeof(file_num_buf), loop);
        ROCKS_LOG_BUFFER(log_buffer_, "[%s] Universal: Skipping %s",
                         cf_name_.c_str(), file_num_buf);
      }
    }
  }
  if (!done || candidate_count <= 1) {
    return nullptr;
  }
  size_t first_index_after = start_index + candidate_count;
  // Compression is enabled if files compacted earlier already reached
  // size ratio of compression.
  bool enable_compression = true;
  int ratio_to_compress =
      mutable_cf_options_.compaction_options_universal.compression_size_percent;
  if (ratio_to_compress >= 0) {
    uint64_t total_size = 0;
    for (auto& sorted_run : sorted_runs_) {
      total_size += sorted_run.compensated_file_size;
    }

    uint64_t older_file_size = 0;
    for (size_t i = sorted_runs_.size() - 1; i >= first_index_after; i--) {
      older_file_size += sorted_runs_[i].size;
      if (older_file_size * 100L >= total_size * (long)ratio_to_compress) {
        enable_compression = false;
        break;
      }
    }
  }

  uint64_t estimated_total_size = 0;
  for (unsigned int i = 0; i < first_index_after; i++) {
    estimated_total_size += sorted_runs_[i].size;
  }
  uint32_t path_id =
      GetPathId(ioptions_, mutable_cf_options_, estimated_total_size);
  int start_level = sorted_runs_[start_index].level;
  int output_level;
  // last level is reserved for the files ingested behind
  int max_output_level = vstorage_->MaxOutputLevel(allow_ingest_behind_);
  if (first_index_after == sorted_runs_.size()) {
    output_level = max_output_level;
  } else if (sorted_runs_[first_index_after].level == 0) {
    output_level = 0;
  } else {
    output_level = sorted_runs_[first_index_after].level - 1;
  }

  if (!MeetsOutputLevelRequirements(output_level)) {
    return nullptr;
  }

  std::vector<CompactionInputFiles> inputs(max_output_level + 1);
  for (size_t i = 0; i < inputs.size(); ++i) {
    inputs[i].level = start_level + static_cast<int>(i);
  }
  for (size_t i = start_index; i < first_index_after; i++) {
    auto& picking_sr = sorted_runs_[i];
    if (picking_sr.level == 0) {
      FileMetaData* picking_file = picking_sr.file;
      inputs[0].files.push_back(picking_file);
    } else {
      auto& files = inputs[picking_sr.level - start_level].files;
      for (auto* f : vstorage_->LevelFiles(picking_sr.level)) {
        files.push_back(f);
      }
    }
    char file_num_buf[256];
    picking_sr.DumpSizeInfo(file_num_buf, sizeof(file_num_buf), i);
    ROCKS_LOG_BUFFER(log_buffer_, "[%s] Universal: Picking %s",
                     cf_name_.c_str(), file_num_buf);
  }

  std::vector<FileMetaData*> grandparents;
  // Include grandparents for potential file cutting in incremental
  // mode. It is for aligning file cutting boundaries across levels,
  // so that subsequent compactions can pick files with aligned
  // buffer.
  // Single files are only picked up in incremental mode, so that
  // there is no need for full range.
  if (mutable_cf_options_.compaction_options_universal.incremental &&
      first_index_after < sorted_runs_.size() &&
      sorted_runs_[first_index_after].level > 1) {
    grandparents = vstorage_->LevelFiles(sorted_runs_[first_index_after].level);
  }

  if (output_level != 0 && picker_->FilesRangeOverlapWithCompaction(
                               inputs, output_level,
                               Compaction::EvaluateProximalLevel(
                                   vstorage_, mutable_cf_options_, ioptions_,
                                   start_level, output_level))) {
    return nullptr;
  }
  CompactionReason compaction_reason;
  if (max_number_of_files_to_compact == UINT_MAX) {
    compaction_reason = CompactionReason::kUniversalSizeRatio;
  } else {
    compaction_reason = CompactionReason::kUniversalSortedRunNum;
  }
  return new Compaction(vstorage_, ioptions_, mutable_cf_options_,
                        mutable_db_options_, std::move(inputs), output_level,
                        MaxFileSizeForLevel(mutable_cf_options_, output_level,
                                            kCompactionStyleUniversal),
                        GetMaxOverlappingBytes(), path_id,
                        GetCompressionType(vstorage_, mutable_cf_options_,
                                           output_level, 1, enable_compression),
                        GetCompressionOptions(mutable_cf_options_, vstorage_,
                                              output_level, enable_compression),
                        Temperature::kUnknown,
                        /* max_subcompactions */ 0, grandparents,
                        /* earliest_snapshot */ std::nullopt,
                        /* snapshot_checker */ nullptr, compaction_reason,
                        /* trim_ts */ "", score_,
                        /* l0_files_might_overlap */ true);
}

// Look at overall size amplification. If size amplification
// exceeds the configured value, then do a compaction
// on longest span of candidate files without conflict with other compactions
// ending at the earliest base file (overriding configured values of file-size
// ratios, min_merge_width and max_merge_width).
Compaction* UniversalCompactionBuilder::PickCompactionToReduceSizeAmp() {
  assert(!sorted_runs_.empty());

  const size_t end_index = ShouldSkipLastSortedRunForSizeAmpCompaction()
                               ? sorted_runs_.size() - 2
                               : sorted_runs_.size() - 1;
  if (sorted_runs_[end_index].being_compacted ||
      sorted_runs_[end_index].level_has_marked_standalone_rangedel) {
    return nullptr;
  }
  const uint64_t base_sr_size = sorted_runs_[end_index].size;
  size_t start_index = end_index;
  uint64_t candidate_size = 0;
  size_t num_l0_files = 0;

  // Get longest span (i.e, [start_index, end_index]) of available sorted runs
  while (start_index > 0) {
    const SortedRun* sr = &sorted_runs_[start_index - 1];
    if (sr->being_compacted || sr->level_has_marked_standalone_rangedel) {
      char file_num_buf[kFormatFileNumberBufSize];
      sr->Dump(file_num_buf, sizeof(file_num_buf), true);
      if (sr->being_compacted) {
        ROCKS_LOG_BUFFER(log_buffer_,
                         "[%s] Universal: stopping for size amp compaction at "
                         "sorted run undergoing compaction: "
                         "%s[%" ROCKSDB_PRIszt "]",
                         cf_name_.c_str(), file_num_buf, start_index - 1);
      } else if (sr->level_has_marked_standalone_rangedel) {
        ROCKS_LOG_BUFFER(log_buffer_,
                         "[%s] Universal: stopping for size amp compaction at "
                         "sorted run that has "
                         "standalone range "
                         "tombstone files marked for compaction: "
                         "%s[%" ROCKSDB_PRIszt "]",
                         cf_name_.c_str(), file_num_buf, start_index - 1);
      }
      break;
    }
    candidate_size += sr->compensated_file_size;
    num_l0_files += sr->level == 0 ? 1 : 0;
    --start_index;
  }

  if (start_index == end_index) {
    return nullptr;
  }

  {
    const size_t num_l0_to_exclude = MightExcludeNewL0sToReduceWriteStop(
        num_l0_files, end_index, start_index, candidate_size);
    ROCKS_LOG_BUFFER(
        log_buffer_,
        "[%s] Universal: Excluding for size amp compaction %" ROCKSDB_PRIszt
        " latest L0 files to reduce potential write stop "
        "triggered by `level0_stop_writes_trigger`",
        cf_name_.c_str(), num_l0_to_exclude);
  }

  {
    char file_num_buf[kFormatFileNumberBufSize];
    sorted_runs_[start_index].Dump(file_num_buf, sizeof(file_num_buf), true);
    ROCKS_LOG_BUFFER(
        log_buffer_,
        "[%s] Universal: First candidate %s[%" ROCKSDB_PRIszt "] %s",
        cf_name_.c_str(), file_num_buf, start_index, " to reduce size amp.\n");
  }

  // percentage flexibility while reducing size amplification
  const uint64_t ratio = mutable_cf_options_.compaction_options_universal
                             .max_size_amplification_percent;

  // size amplification = percentage of additional size
  if (candidate_size * 100 < ratio * base_sr_size) {
    ROCKS_LOG_BUFFER(log_buffer_,
                     "[%s] Universal: size amp compction not needed. "
                     "newer-files-total-size %" PRIu64
                     " earliest-file-size %" PRIu64,
                     cf_name_.c_str(), candidate_size, base_sr_size);
    return nullptr;
  } else {
    ROCKS_LOG_BUFFER(log_buffer_,
                     "[%s] Universal: size amp compaction needed. "
                     "newer-files-total-size %" PRIu64
                     " earliest-file-size %" PRIu64,
                     cf_name_.c_str(), candidate_size, base_sr_size);
  }
  // Since incremental compaction can't include more than second last
  // level, it can introduce penalty, compared to full compaction. We
  // hard code the pentalty to be 80%. If we end up with a compaction
  // fanout higher than 80% of full level compactions, we fall back
  // to full level compaction.
  // The 80% threshold is arbitrary and can be adjusted or made
  // configurable in the future.
  // This also prevent the case when compaction falls behind and we
  // need to compact more levels for compactions to catch up.
  if (mutable_cf_options_.compaction_options_universal.incremental) {
    double fanout_threshold = static_cast<double>(base_sr_size) /
                              static_cast<double>(candidate_size) * 1.8;
    Compaction* picked = PickIncrementalForReduceSizeAmp(fanout_threshold);
    if (picked != nullptr) {
      // As the feature is still incremental, picking incremental compaction
      // might fail and we will fall bck to compacting full level.
      return picked;
    }
  }
  return PickCompactionWithSortedRunRange(
      start_index, end_index, CompactionReason::kUniversalSizeAmplification);
}

Compaction* UniversalCompactionBuilder::PickIncrementalForReduceSizeAmp(
    double fanout_threshold) {
  // Try find all potential compactions with total size just over
  // options.max_compaction_size / 2, and take the one with the lowest
  // fanout (defined in declaration of the function).
  // This is done by having a sliding window of the files at the second
  // lowest level, and keep expanding while finding overlapping in the
  // last level. Once total size exceeds the size threshold, calculate
  // the fanout value. And then shrinking from the small side of the
  // window. Keep doing it until the end.
  // Finally, we try to include upper level files if they fall into
  // the range.
  //
  // Note that it is a similar problem as leveled compaction's
  // kMinOverlappingRatio priority, but instead of picking single files
  // we expand to a target compaction size. The reason is that in
  // leveled compaction, actual fanout value tends to high, e.g. 10, so
  // even with single file in down merging level, the extra size
  // compacted in boundary files is at a lower ratio. But here users
  // often have size of second last level size to be 1/4, 1/3 or even
  // 1/2 of the bottommost level, so picking single file in second most
  // level will cause significant waste, which is not desirable.
  //
  // This algorithm has lots of room to improve to pick more efficient
  // compactions.
  assert(sorted_runs_.size() >= 2);
  int second_last_level = sorted_runs_[sorted_runs_.size() - 2].level;
  if (second_last_level == 0) {
    // Can't split Level 0.
    return nullptr;
  }
  int output_level = sorted_runs_.back().level;
  const std::vector<FileMetaData*>& bottom_files =
      vstorage_->LevelFiles(output_level);
  const std::vector<FileMetaData*>& files =
      vstorage_->LevelFiles(second_last_level);
  assert(!bottom_files.empty());
  assert(!files.empty());

  //  std::unordered_map<uint64_t, uint64_t> file_to_order;

  int picked_start_idx = 0;
  int picked_end_idx = 0;
  double picked_fanout = fanout_threshold;

  // Use half target compaction bytes as anchor to stop growing second most
  // level files, and reserve growing space for more overlapping bottom level,
  // clean cut, files from other levels, etc.
  uint64_t comp_thres_size = mutable_cf_options_.max_compaction_bytes / 2;
  int start_idx = 0;
  int bottom_end_idx = 0;
  int bottom_start_idx = 0;
  uint64_t non_bottom_size = 0;
  uint64_t bottom_size = 0;
  bool end_bottom_size_counted = false;
  for (int end_idx = 0; end_idx < static_cast<int>(files.size()); end_idx++) {
    FileMetaData* end_file = files[end_idx];

    // Include bottom most level files smaller than the current second
    // last level file.
    int num_skipped = 0;
    while (bottom_end_idx < static_cast<int>(bottom_files.size()) &&
           icmp_->Compare(bottom_files[bottom_end_idx]->largest,
                          end_file->smallest) < 0) {
      if (!end_bottom_size_counted) {
        bottom_size += bottom_files[bottom_end_idx]->fd.file_size;
      }
      bottom_end_idx++;
      end_bottom_size_counted = false;
      num_skipped++;
    }

    if (num_skipped > 1) {
      // At least a file in the bottom most level falls into the file gap. No
      // reason to include the file. We cut the range and start a new sliding
      // window.
      start_idx = end_idx;
    }

    if (start_idx == end_idx) {
      // new sliding window.
      non_bottom_size = 0;
      bottom_size = 0;
      bottom_start_idx = bottom_end_idx;
      end_bottom_size_counted = false;
    }

    non_bottom_size += end_file->fd.file_size;

    // Include all overlapping files in bottom level.
    while (bottom_end_idx < static_cast<int>(bottom_files.size()) &&
           icmp_->Compare(bottom_files[bottom_end_idx]->smallest,
                          end_file->largest) < 0) {
      if (!end_bottom_size_counted) {
        bottom_size += bottom_files[bottom_end_idx]->fd.file_size;
        end_bottom_size_counted = true;
      }
      if (icmp_->Compare(bottom_files[bottom_end_idx]->largest,
                         end_file->largest) > 0) {
        // next level file cross large boundary of current file.
        break;
      }
      bottom_end_idx++;
      end_bottom_size_counted = false;
    }

    if ((non_bottom_size + bottom_size > comp_thres_size ||
         end_idx == static_cast<int>(files.size()) - 1) &&
        non_bottom_size > 0) {  // Do we alow 0 size file at all?
      // If it is a better compaction, remember it in picked* variables.
      double fanout = static_cast<double>(bottom_size) /
                      static_cast<double>(non_bottom_size);
      if (fanout < picked_fanout) {
        picked_start_idx = start_idx;
        picked_end_idx = end_idx;
        picked_fanout = fanout;
      }
      // Shrink from the start end to under comp_thres_size
      while (non_bottom_size + bottom_size > comp_thres_size &&
             start_idx <= end_idx) {
        non_bottom_size -= files[start_idx]->fd.file_size;
        start_idx++;
        if (start_idx < static_cast<int>(files.size())) {
          while (bottom_start_idx <= bottom_end_idx &&
                 icmp_->Compare(bottom_files[bottom_start_idx]->largest,
                                files[start_idx]->smallest) < 0) {
            bottom_size -= bottom_files[bottom_start_idx]->fd.file_size;
            bottom_start_idx++;
          }
        }
      }
    }
  }

  if (picked_fanout >= fanout_threshold) {
    assert(picked_fanout == fanout_threshold);
    return nullptr;
  }

  std::vector<CompactionInputFiles> inputs;
  CompactionInputFiles bottom_level_inputs;
  CompactionInputFiles second_last_level_inputs;
  second_last_level_inputs.level = second_last_level;
  bottom_level_inputs.level = output_level;
  for (int i = picked_start_idx; i <= picked_end_idx; i++) {
    if (files[i]->being_compacted) {
      return nullptr;
    }
    second_last_level_inputs.files.push_back(files[i]);
  }
  assert(!second_last_level_inputs.empty());
  if (!picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                       &second_last_level_inputs,
                                       /*next_smallest=*/nullptr)) {
    return nullptr;
  }
  // We might be able to avoid this binary search if we save and expand
  // from bottom_start_idx and bottom_end_idx, but for now, we use
  // SetupOtherInputs() for simplicity.
  int parent_index = -1;  // Create and use bottom_start_idx?
  if (!picker_->SetupOtherInputs(cf_name_, mutable_cf_options_, vstorage_,
                                 &second_last_level_inputs,
                                 &bottom_level_inputs, &parent_index,
                                 /*base_index=*/-1)) {
    return nullptr;
  }

  // Try to include files in upper levels if they fall into the range.
  // Since we need to go from lower level up and this is in the reverse
  // order, compared to level order, we first write to an reversed
  // data structure and finally copy them to compaction inputs.
  InternalKey smallest;
  InternalKey largest;
  picker_->GetRange(second_last_level_inputs, &smallest, &largest);
  std::vector<CompactionInputFiles> inputs_reverse;
  for (auto it = ++(++sorted_runs_.rbegin()); it != sorted_runs_.rend(); it++) {
    SortedRun& sr = *it;
    if (sr.level == 0) {
      break;
    }
    std::vector<FileMetaData*> level_inputs;
    vstorage_->GetCleanInputsWithinInterval(sr.level, &smallest, &largest,
                                            &level_inputs);
    if (!level_inputs.empty()) {
      inputs_reverse.push_back({});
      inputs_reverse.back().level = sr.level;
      inputs_reverse.back().files = level_inputs;
      picker_->GetRange(inputs_reverse.back(), &smallest, &largest);
    }
  }
  for (auto it = inputs_reverse.rbegin(); it != inputs_reverse.rend(); it++) {
    inputs.push_back(*it);
  }

  inputs.push_back(second_last_level_inputs);
  inputs.push_back(bottom_level_inputs);

  int start_level = Compaction::kInvalidLevel;
  for (const auto& in : inputs) {
    if (!in.empty()) {
      // inputs should already be sorted by level
      start_level = in.level;
      break;
    }
  }

  // intra L0 compactions outputs could have overlap
  if (output_level != 0 && picker_->FilesRangeOverlapWithCompaction(
                               inputs, output_level,
                               Compaction::EvaluateProximalLevel(
                                   vstorage_, mutable_cf_options_, ioptions_,
                                   start_level, output_level))) {
    return nullptr;
  }

  // TODO support multi paths?
  uint32_t path_id = 0;
  return new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, mutable_db_options_,
      std::move(inputs), output_level,
      MaxFileSizeForLevel(mutable_cf_options_, output_level,
                          kCompactionStyleUniversal),
      GetMaxOverlappingBytes(), path_id,
      GetCompressionType(vstorage_, mutable_cf_options_, output_level, 1,
                         true /* enable_compression */),
      GetCompressionOptions(mutable_cf_options_, vstorage_, output_level,
                            true /* enable_compression */),
      Temperature::kUnknown,
      /* max_subcompactions */ 0, /* grandparents */ {},
      /* earliest_snapshot */ std::nullopt,
      /* snapshot_checker */ nullptr,
      CompactionReason::kUniversalSizeAmplification,
      /* trim_ts */ "", score_,
      /* l0_files_might_overlap */ true);
}

// Pick files marked for compaction. Typically, files are marked by
// CompactOnDeleteCollector due to the presence of tombstones.
Compaction* UniversalCompactionBuilder::PickDeleteTriggeredCompaction() {
  CompactionInputFiles start_level_inputs;

  if (vstorage_->num_levels() == 1) {
    // This is single level universal. Since we're basically trying to reclaim
    // space by processing files marked for compaction due to high tombstone
    // density, let's do the same thing as compaction to reduce size amp which
    // has the same goals.
    int start_index = -1;

    start_level_inputs.level = 0;
    start_level_inputs.files.clear();
    // Find the first file marked for compaction. Ignore the last file
    for (size_t loop = 0; loop + 1 < sorted_runs_.size(); loop++) {
      SortedRun* sr = &sorted_runs_[loop];
      if (sr->being_compacted) {
        continue;
      }
      FileMetaData* f = vstorage_->LevelFiles(0)[loop];
      if (f->marked_for_compaction && !ShouldSkipMarkedFile(f)) {
        start_level_inputs.files.push_back(f);
        start_index =
            static_cast<int>(loop);  // Consider this as the first candidate.
        break;
      }
    }
    if (start_index < 0) {
      // Either no file marked, or they're already being compacted
      return nullptr;
    }

    for (size_t loop = start_index + 1; loop < sorted_runs_.size(); loop++) {
      SortedRun* sr = &sorted_runs_[loop];
      if (sr->being_compacted || sr->level_has_marked_standalone_rangedel) {
        break;
      }

      FileMetaData* f = vstorage_->LevelFiles(0)[loop];
      start_level_inputs.files.push_back(f);
    }
  } else {
    int start_level;
    int output_level;

    // For multi-level universal, the strategy is to make this look more like
    // leveled. We pick one of the files marked for compaction and compact with
    // overlapping files in the adjacent level.
    picker_->PickFilesMarkedForCompaction(cf_name_, vstorage_, &start_level,
                                          &output_level, &start_level_inputs,
                                          [this](const FileMetaData* file) {
                                            return ShouldSkipMarkedFile(file);
                                          });
    if (start_level_inputs.empty()) {
      return nullptr;
    }
  }

  return BuildCompactionToNextLevel(
      start_level_inputs, CompactionReason::kFilesMarkedForCompaction);
}

Compaction* UniversalCompactionBuilder::PickCompactionToOldest(
    size_t start_index, CompactionReason compaction_reason) {
  return PickCompactionWithSortedRunRange(start_index, sorted_runs_.size() - 1,
                                          compaction_reason);
}

Compaction* UniversalCompactionBuilder::PickCompactionWithSortedRunRange(
    size_t start_index, size_t end_index, CompactionReason compaction_reason) {
  assert(start_index < sorted_runs_.size());

  // Estimate total file size
  uint64_t estimated_total_size = 0;
  for (size_t loop = start_index; loop <= end_index; loop++) {
    estimated_total_size += sorted_runs_[loop].size;
  }
  uint32_t path_id =
      GetPathId(ioptions_, mutable_cf_options_, estimated_total_size);
  int start_level = sorted_runs_[start_index].level;
  int max_output_level = vstorage_->MaxOutputLevel(allow_ingest_behind_);
  std::vector<CompactionInputFiles> inputs(max_output_level + 1);
  for (size_t i = 0; i < inputs.size(); ++i) {
    inputs[i].level = start_level + static_cast<int>(i);
  }
  for (size_t loop = start_index; loop <= end_index; loop++) {
    auto& picking_sr = sorted_runs_[loop];
    if (picking_sr.level == 0) {
      FileMetaData* f = picking_sr.file;
      inputs[0].files.push_back(f);
    } else {
      auto& files = inputs[picking_sr.level - start_level].files;
      for (auto* f : vstorage_->LevelFiles(picking_sr.level)) {
        files.push_back(f);
      }
    }
    std::string comp_reason_print_string;
    if (compaction_reason == CompactionReason::kPeriodicCompaction) {
      comp_reason_print_string = "periodic compaction";
    } else if (compaction_reason ==
               CompactionReason::kUniversalSizeAmplification) {
      comp_reason_print_string = "size amp";
    } else {
      assert(false);
      comp_reason_print_string = "unknown: ";
      comp_reason_print_string.append(
          std::to_string(static_cast<int>(compaction_reason)));
    }

    char file_num_buf[256];
    picking_sr.DumpSizeInfo(file_num_buf, sizeof(file_num_buf), loop);
    ROCKS_LOG_BUFFER(log_buffer_, "[%s] Universal: %s picking %s",
                     cf_name_.c_str(), comp_reason_print_string.c_str(),
                     file_num_buf);
  }

  int output_level;
  if (end_index == sorted_runs_.size() - 1) {
    output_level = max_output_level;
  } else {
    // if it's not including all sorted_runs, it can only output to the level
    // above the `end_index + 1` sorted_run.
    output_level = sorted_runs_[end_index + 1].level - 1;
  }

  if (!MeetsOutputLevelRequirements(output_level)) {
    return nullptr;
  }

  // intra L0 compactions outputs could have overlap
  if (output_level != 0 && picker_->FilesRangeOverlapWithCompaction(
                               inputs, output_level,
                               Compaction::EvaluateProximalLevel(
                                   vstorage_, mutable_cf_options_, ioptions_,
                                   start_level, output_level))) {
    return nullptr;
  }

  // We never check size for
  // compaction_options_universal.compression_size_percent,
  // because we always compact all the files, so always compress.
  return new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, mutable_db_options_,
      std::move(inputs), output_level,
      MaxFileSizeForLevel(mutable_cf_options_, output_level,
                          kCompactionStyleUniversal),
      GetMaxOverlappingBytes(), path_id,
      GetCompressionType(vstorage_, mutable_cf_options_, output_level, 1,
                         true /* enable_compression */),
      GetCompressionOptions(mutable_cf_options_, vstorage_, output_level,
                            true /* enable_compression */),
      Temperature::kUnknown,
      /* max_subcompactions */ 0, /* grandparents */ {},
      /* earliest_snapshot */ std::nullopt,
      /* snapshot_checker */ nullptr, compaction_reason,
      /* trim_ts */ "", score_,
      /* l0_files_might_overlap */ true);
}

Compaction* UniversalCompactionBuilder::PickPeriodicCompaction() {
  ROCKS_LOG_BUFFER(log_buffer_, "[%s] Universal: Periodic Compaction",
                   cf_name_.c_str());

  // In universal compaction, sorted runs contain older data are almost always
  // generated earlier too. To simplify the problem, we just try to trigger
  // a full compaction. We start from the oldest sorted run and include
  // all sorted runs, until we hit a sorted already being compacted.
  // Since usually the largest (which is usually the oldest) sorted run is
  // included anyway, doing a full compaction won't increase write
  // amplification much.

  // Get some information from marked files to check whether a file is
  // included in the compaction.

  size_t start_index = sorted_runs_.size();
  while (start_index > 0 && !sorted_runs_[start_index - 1].being_compacted &&
         !sorted_runs_[start_index - 1].level_has_marked_standalone_rangedel) {
    start_index--;
  }
  if (start_index == sorted_runs_.size()) {
    return nullptr;
  }

  // There is a rare corner case where we can't pick up all the files
  // because some files are being compacted and we end up with picking files
  // but none of them need periodic compaction. Unless we simply recompact
  // the last sorted run (either the last level or last L0 file), we would just
  // execute the compaction, in order to simplify  the logic.
  if (start_index == sorted_runs_.size() - 1) {
    bool included_file_marked = false;
    int start_level = sorted_runs_[start_index].level;
    FileMetaData* start_file = sorted_runs_[start_index].file;
    for (const std::pair<int, FileMetaData*>& level_file_pair :
         vstorage_->FilesMarkedForPeriodicCompaction()) {
      if (start_level != 0) {
        // Last sorted run is a level
        if (start_level == level_file_pair.first) {
          included_file_marked = true;
          break;
        }
      } else {
        // Last sorted run is a L0 file.
        if (start_file == level_file_pair.second) {
          included_file_marked = true;
          break;
        }
      }
    }
    if (!included_file_marked) {
      ROCKS_LOG_BUFFER(log_buffer_,
                       "[%s] Universal: Cannot form a compaction covering file "
                       "marked for periodic compaction",
                       cf_name_.c_str());
      return nullptr;
    }
  }

  Compaction* c = PickCompactionToOldest(start_index,
                                         CompactionReason::kPeriodicCompaction);

  TEST_SYNC_POINT_CALLBACK(
      "UniversalCompactionPicker::PickPeriodicCompaction:Return", c);

  return c;
}

Compaction* UniversalCompactionBuilder::PickReadTriggeredCompaction() {
  ROCKS_LOG_BUFFER(log_buffer_, "[%s] Universal: Read Triggered Compaction",
                   cf_name_.c_str());

  if (vstorage_->ReadTriggeredCompactionFiles().empty()) {
    return nullptr;
  }

  CompactionInputFiles start_level_inputs;
  int start_level = -1;

  for (const auto& level_file : vstorage_->ReadTriggeredCompactionFiles()) {
    assert(!level_file.second->being_compacted);
    start_level = level_file.first;

    if (start_level == 0 &&
        !picker_->level0_compactions_in_progress()->empty()) {
      continue;
    }

    start_level_inputs.files = {level_file.second};
    start_level_inputs.level = start_level;
    if (picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                        &start_level_inputs)) {
      break;
    }
    start_level_inputs.files.clear();
  }

  if (start_level_inputs.empty()) {
    return nullptr;
  }

  // For intra L0 compactions, pick up all overlapping files
  if (vstorage_->num_levels() == 1 && start_level_inputs.level == 0) {
    if (!picker_->GetOverlappingL0Files(vstorage_, &start_level_inputs,
                                        /*output_level=*/0, nullptr, nullptr)) {
      return nullptr;
    }
  }

  return BuildCompactionToNextLevel(start_level_inputs,
                                    CompactionReason::kReadTriggered);
}

Compaction* UniversalCompactionBuilder::BuildCompactionToNextLevel(
    CompactionInputFiles& start_level_inputs, CompactionReason reason) {
  int start_level = start_level_inputs.level;
  int max_output_level = vstorage_->MaxOutputLevel(allow_ingest_behind_);

  // Pick the first non-empty level after start_level as output.
  int output_level;
  for (output_level = start_level + 1; output_level <= max_output_level;
       output_level++) {
    if (vstorage_->NumLevelFiles(output_level) != 0) {
      break;
    }
  }

  // If all higher levels are empty, pick the highest level as output level
  // for L0 start. For non-L0 start, a trivial move doesn't help reclaim
  // space, so skip.
  if (output_level > max_output_level) {
    if (start_level == 0) {
      output_level = max_output_level;
    } else {
      return nullptr;
    }
  }

  if (start_level_inputs.size() <= 1 && output_level == 0) {
    // If only the last file in L0 is marked for compaction, ignore it
    return nullptr;
  }

  std::vector<CompactionInputFiles> inputs;
  std::vector<FileMetaData*> grandparents;

  if (output_level != 0) {
    if (!MeetsOutputLevelRequirements(output_level)) {
      return nullptr;
    }

    // For standalone range deletion, we don't want to compact it with newer
    // L0 files that it doesn't cover.
    const FileMetaData* starting_l0_file =
        (start_level == 0 && start_level_inputs.size() == 1 &&
         start_level_inputs.files[0]->FileIsStandAloneRangeTombstone())
            ? start_level_inputs.files[0]
            : nullptr;

    if (start_level == 0) {
      if (!picker_->GetOverlappingL0Files(vstorage_, &start_level_inputs,
                                          output_level, nullptr,
                                          starting_l0_file)) {
        return nullptr;
      }
    }

    CompactionInputFiles output_level_inputs;
    int parent_index = -1;

    output_level_inputs.level = output_level;
    if (!picker_->SetupOtherInputs(
            cf_name_, mutable_cf_options_, vstorage_, &start_level_inputs,
            &output_level_inputs, &parent_index, -1, false, starting_l0_file)) {
      return nullptr;
    }
    inputs.push_back(start_level_inputs);
    if (!output_level_inputs.empty()) {
      inputs.push_back(output_level_inputs);
    }
    if (picker_->FilesRangeOverlapWithCompaction(
            inputs, output_level,
            Compaction::EvaluateProximalLevel(vstorage_, mutable_cf_options_,
                                              ioptions_, start_level,
                                              output_level))) {
      return nullptr;
    }

    picker_->GetGrandparents(vstorage_, start_level_inputs, output_level_inputs,
                             &grandparents);
  } else {
    inputs.push_back(start_level_inputs);
  }

  uint64_t estimated_total_size = 0;
  for (FileMetaData* f : vstorage_->LevelFiles(output_level)) {
    estimated_total_size += f->fd.GetFileSize();
  }
  uint32_t path_id =
      GetPathId(ioptions_, mutable_cf_options_, estimated_total_size);
  return new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, mutable_db_options_,
      std::move(inputs), output_level,
      MaxFileSizeForLevel(mutable_cf_options_, output_level,
                          kCompactionStyleUniversal),
      /* max_grandparent_overlap_bytes */ GetMaxOverlappingBytes(), path_id,
      GetCompressionType(vstorage_, mutable_cf_options_, output_level, 1),
      GetCompressionOptions(mutable_cf_options_, vstorage_, output_level),
      Temperature::kUnknown,
      /* max_subcompactions */ 0, grandparents, earliest_snapshot_,
      snapshot_checker_, reason,
      /* trim_ts */ "", score_,
      /* l0_files_might_overlap */ true);
}

uint64_t UniversalCompactionBuilder::GetMaxOverlappingBytes() const {
  if (!mutable_cf_options_.compaction_options_universal.incremental) {
    return std::numeric_limits<uint64_t>::max();
  } else {
    // Try to align cutting boundary with files at the next level if the
    // file isn't end up with 1/2 of target size, or it would overlap
    // with two full size files at the next level.
    return mutable_cf_options_.target_file_size_base / 2 * 3;
  }
}
}  // namespace ROCKSDB_NAMESPACE
