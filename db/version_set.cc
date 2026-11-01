//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <array>
#include <cinttypes>
#include <cstdio>
#include <list>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/blob/blob_fetcher.h"
#include "db/blob/blob_file_cache.h"
#include "db/blob/blob_file_reader.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_source.h"
#include "db/compaction/compaction.h"
#include "db/compaction/file_pri.h"
#include "db/dbformat.h"
#include "db/internal_stats.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/manifest_ops.h"
#include "db/memtable.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/pinned_iterators_manager.h"
#include "db/table_cache.h"
#include "db/version_builder.h"
#include "db/version_edit.h"
#include "db/version_edit_handler.h"
#include "db/wide/wide_columns_helper.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "file/line_file_reader.h"
#include "file/random_access_file_reader.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "monitoring/file_read_sample.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/persistent_stats_history.h"
#include "options/options_helper.h"
#include "options/options_parser.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/compaction_merging_iterator.h"
#include "table/format.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/merging_iterator.h"
#include "table/meta_blocks.h"
#include "table/multiget_context.h"
#include "table/plain/plain_table_factory.h"
#include "table/table_reader.h"
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/coro_utils.h"
#include "util/fastrange.h"
#include "util/hash.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/user_comparator_wrapper.h"

#if USE_COROUTINES
#include "folly/coro/BlockingWait.h"
#include "folly/coro/Collect.h"
#endif  // USE_COROUTINES

namespace ROCKSDB_NAMESPACE {

void OptionsFileManifestState::ApplyCommit(uint64_t number) {
  committed_options_file_number = number;
  has_committed_options_file_number = true;
}

void OptionsFileManifestState::ApplyColumnFamilyEdit(const VersionEdit& edit) {
  if (edit.IsColumnFamilyAdd()) {
    column_family_names[edit.GetColumnFamily()] = edit.GetColumnFamilyName();
  } else if (edit.IsColumnFamilyDrop()) {
    column_family_names.erase(edit.GetColumnFamily());
  }
}

namespace {

using ScanOptionsMap = std::unordered_map<size_t, MultiScanArgs>;

// Find File in LevelFilesBrief data structure
// Within an index range defined by left and right
int FindFileInRange(const InternalKeyComparator& icmp,
                    const LevelFilesBrief& file_level, const Slice& key,
                    uint32_t left, uint32_t right) {
  auto cmp = [&](const FdWithKeyRange& f, const Slice& k) -> bool {
    return icmp.InternalKeyComparator::Compare(f.largest_key, k) < 0;
  };
  const auto& b = file_level.files;
  return static_cast<int>(std::lower_bound(b + left, b + right, key, cmp) - b);
}

InternalKey MultiScanInternalKey(const Slice& user_key,
                                 const Comparator* user_comparator) {
  const size_t timestamp_size = user_comparator->timestamp_size();
  if (timestamp_size == 0) {
    return InternalKey(user_key, kMaxSequenceNumber, kValueTypeForSeek);
  }

  std::string key_with_ts;
  AppendKeyWithMaxTimestamp(&key_with_ts, user_key, timestamp_size);
  return InternalKey(key_with_ts, kMaxSequenceNumber, kValueTypeForSeek);
}

bool MultiScanRangeOverlapsFile(const UserComparatorWrapper& user_comparator,
                                const ScanOptions& scan_opts,
                                const FdWithKeyRange& file) {
  assert(scan_opts.range.start.has_value());
  assert(scan_opts.range.limit.has_value());

  if (user_comparator.CompareWithoutTimestamp(
          scan_opts.range.limit.value(), /*a_has_ts=*/false,
          ExtractUserKey(file.smallest_key), /*b_has_ts=*/true) <= 0) {
    return false;
  }

  if (user_comparator.CompareWithoutTimestamp(
          scan_opts.range.start.value(), /*a_has_ts=*/false,
          ExtractUserKey(file.largest_key), /*b_has_ts=*/true) > 0) {
    return false;
  }

  return true;
}

void AddMultiScanFileIndex(std::vector<size_t>* file_indexes,
                           std::vector<char>* selected, size_t file_index) {
  if ((*selected)[file_index]) {
    return;
  }
  (*selected)[file_index] = true;
  file_indexes->push_back(file_index);
}

template <typename OnOverlappingFile>
bool NotifyMultiScanOverlappingFile(OnOverlappingFile& on_overlapping_file,
                                    size_t file_index,
                                    const ScanOptions& scan_range) {
  if constexpr (std::is_same_v<std::invoke_result_t<OnOverlappingFile, size_t,
                                                    const ScanOptions&>,
                               bool>) {
    return on_overlapping_file(file_index, scan_range);
  } else {
    on_overlapping_file(file_index, scan_range);
    return true;
  }
}

template <typename OnOverlappingFile>
void ForEachMultiScanOverlappingFile(const InternalKeyComparator& icmp,
                                     const UserComparatorWrapper& ucmp,
                                     const LevelFilesBrief& file_level,
                                     int level, const MultiScanArgs& scan_opts,
                                     OnOverlappingFile on_overlapping_file) {
  assert(scan_opts.HasBoundedScanRanges());
  if (file_level.num_files == 0) {
    return;
  }

  if (level == 0) {
    for (size_t i = 0; i < file_level.num_files; ++i) {
      for (const ScanOptions& scan_range : scan_opts.GetScanRanges()) {
        if (MultiScanRangeOverlapsFile(ucmp, scan_range, file_level.files[i])) {
          if (!NotifyMultiScanOverlappingFile(on_overlapping_file, i,
                                              scan_range)) {
            return;
          }
        }

        if (ucmp.CompareWithoutTimestamp(
                scan_range.range.start.value(), /*a_has_ts=*/false,
                ExtractUserKey(file_level.files[i].largest_key),
                /*b_has_ts=*/true) > 0) {
          break;
        }
      }
    }
    return;
  }

  for (const ScanOptions& scan_range : scan_opts.GetScanRanges()) {
    const InternalKey start_key = MultiScanInternalKey(
        scan_range.range.start.value(), ucmp.user_comparator());
    size_t first_file = FindFile(icmp, file_level, start_key.Encode());
    if (first_file >= file_level.num_files) {
      continue;
    }

    const InternalKey limit_key = MultiScanInternalKey(
        scan_range.range.limit.value(), ucmp.user_comparator());
    size_t last_file = FindFile(icmp, file_level, limit_key.Encode());
    if (last_file >= file_level.num_files) {
      last_file = file_level.num_files - 1;
    }

    for (size_t i = first_file; i <= last_file; ++i) {
      if (MultiScanRangeOverlapsFile(ucmp, scan_range, file_level.files[i])) {
        if (!NotifyMultiScanOverlappingFile(on_overlapping_file, i,
                                            scan_range)) {
          return;
        }
      }
    }
  }
}

std::vector<size_t> GetMultiScanOverlappingFiles(
    const InternalKeyComparator& icmp, const UserComparatorWrapper& ucmp,
    const LevelFilesBrief& file_level, int level,
    const MultiScanArgs& scan_opts,
    size_t max_file_indexes = std::numeric_limits<size_t>::max()) {
  std::vector<size_t> file_indexes;
  if (!scan_opts.HasBoundedScanRanges() || file_level.num_files == 0) {
    return file_indexes;
  }
  file_indexes.reserve(
      std::min(static_cast<size_t>(file_level.num_files), max_file_indexes));

  std::vector<char> selected(file_level.num_files, false);
  ForEachMultiScanOverlappingFile(
      icmp, ucmp, file_level, level, scan_opts,
      [&](size_t file_index, const ScanOptions& /*scan_range*/) {
        if (file_indexes.size() >= max_file_indexes) {
          return false;
        }
        AddMultiScanFileIndex(&file_indexes, &selected, file_index);
        return file_indexes.size() < max_file_indexes;
      });

  return file_indexes;
}

void AddTableIteratorForLevel(
    ColumnFamilyData* cfd, const ReadOptions& read_options,
    const FileOptions& soptions, const MutableCFOptions& mutable_cf_options,
    MergeIteratorBuilder* merge_iter_builder, Arena* arena,
    const LevelFilesBrief& file_level, int level, size_t file_index,
    bool skip_filters, size_t max_file_size_for_l0_meta_pin,
    bool allow_unprepared_value, SequenceNumber read_seq) {
  std::unique_ptr<TruncatedRangeDelIterator> tombstone_iter = nullptr;
  const auto& file = file_level.files[file_index];
  auto table_iter = cfd->table_cache()->NewIterator(
      read_options, soptions, cfd->internal_comparator(), *file.file_metadata,
      /*range_del_agg=*/nullptr, mutable_cf_options, nullptr,
      cfd->internal_stats()->GetFileReadHist(level),
      TableReaderCaller::kUserIterator, arena, skip_filters, level,
      max_file_size_for_l0_meta_pin,
      /*smallest_compaction_key=*/nullptr,
      /*largest_compaction_key=*/nullptr, allow_unprepared_value, &read_seq,
      &tombstone_iter,
      /*maybe_pin_table_handle=*/true);
#ifndef NDEBUG
  TEST_SYNC_POINT_CALLBACK("Version::AddIteratorsForLevel:AddedFile",
                           file.file_metadata);
  std::pair<bool, bool> iterator_type(true /* is_block_based_table_iterator */,
                                      false /* is_level_iterator */);
  TEST_SYNC_POINT_CALLBACK("Version::AddIteratorsForLevel:IteratorType",
                           &iterator_type);
#endif
  if (read_options.ignore_range_deletions) {
    merge_iter_builder->AddIterator(table_iter);
  } else {
    merge_iter_builder->AddPointAndTombstoneIterator(table_iter,
                                                     std::move(tombstone_iter));
  }
}

Status OverlapWithIterator(const Comparator* ucmp,
                           const Slice& smallest_user_key,
                           const Slice& largest_user_key,
                           InternalIterator* iter, bool* overlap) {
  InternalKey range_start(smallest_user_key, kMaxSequenceNumber,
                          kValueTypeForSeek);
  iter->Seek(range_start.Encode());
  if (!iter->status().ok()) {
    return iter->status();
  }

  *overlap = false;
  if (iter->Valid()) {
    ParsedInternalKey seek_result;
    Status s = ParseInternalKey(iter->key(), &seek_result,
                                false /* log_err_key */);  // TODO
    if (!s.ok()) {
      return s;
    }

    if (ucmp->CompareWithoutTimestamp(seek_result.user_key, largest_user_key) <=
        0) {
      *overlap = true;
    }
  }

  return iter->status();
}

}  // namespace

// Class to help choose the next file to search for the particular key.
// Searches and returns files level by level.
// We can search level-by-level since entries never hop across
// levels. Therefore we are guaranteed that if we find data
// in a smaller level, later levels are irrelevant (unless we
// are MergeInProgress).
class FilePicker {
 public:
  FilePicker(const Slice& user_key, const Slice& ikey,
             autovector<LevelFilesBrief>* file_levels, unsigned int num_levels,
             FileIndexer* file_indexer, const Comparator* user_comparator,
             const InternalKeyComparator* internal_comparator)
      : num_levels_(num_levels),
        curr_level_(static_cast<unsigned int>(-1)),
        returned_file_level_(static_cast<unsigned int>(-1)),
        hit_file_level_(static_cast<unsigned int>(-1)),
        search_left_bound_(0),
        search_right_bound_(FileIndexer::kLevelMaxIndex),
        level_files_brief_(file_levels),
        is_hit_file_last_in_level_(false),
        curr_file_level_(nullptr),
        user_key_(user_key),
        ikey_(ikey),
        file_indexer_(file_indexer),
        user_comparator_(user_comparator),
        internal_comparator_(internal_comparator) {
    // Setup member variables to search first level.
    search_ended_ = !PrepareNextLevel();
    if (!search_ended_) {
      // Prefetch Level 0 table data to avoid cache miss if possible.
      for (unsigned int i = 0; i < (*level_files_brief_)[0].num_files; ++i) {
        auto* r = (*level_files_brief_)[0].files[i].fd.pinned_reader.Get();
        if (r) {
          r->Prepare(ikey);
        }
      }
    }
  }

  int GetCurrentLevel() const { return curr_level_; }

  FdWithKeyRange* GetNextFile() {
    while (!search_ended_) {  // Loops over different levels.
      while (curr_index_in_curr_level_ < curr_file_level_->num_files) {
        // Loops over all files in current level.
        FdWithKeyRange* f = &curr_file_level_->files[curr_index_in_curr_level_];
        hit_file_level_ = curr_level_;
        is_hit_file_last_in_level_ =
            curr_index_in_curr_level_ == curr_file_level_->num_files - 1;
        int cmp_largest = -1;

        // Do key range filtering of files or/and fractional cascading if:
        // (1) not all the files are in level 0, or
        // (2) there are more than 3 current level files
        // If there are only 3 or less current level files in the system, we
        // skip the key range filtering. In this case, more likely, the system
        // is highly tuned to minimize number of tables queried by each query,
        // so it is unlikely that key range filtering is more efficient than
        // querying the files.
        if (num_levels_ > 1 || curr_file_level_->num_files > 3) {
          // Check if key is within a file's range. If search left bound and
          // right bound point to the same find, we are sure key falls in
          // range.
          assert(curr_level_ == 0 ||
                 curr_index_in_curr_level_ == start_index_in_curr_level_ ||
                 user_comparator_->CompareWithoutTimestamp(
                     user_key_, ExtractUserKey(f->smallest_key)) <= 0);

          int cmp_smallest = user_comparator_->CompareWithoutTimestamp(
              user_key_, ExtractUserKey(f->smallest_key));
          if (cmp_smallest >= 0) {
            cmp_largest = user_comparator_->CompareWithoutTimestamp(
                user_key_, ExtractUserKey(f->largest_key));
          }

          // Setup file search bound for the next level based on the
          // comparison results
          if (curr_level_ > 0) {
            file_indexer_->GetNextLevelIndex(
                curr_level_, curr_index_in_curr_level_, cmp_smallest,
                cmp_largest, &search_left_bound_, &search_right_bound_);
          }
          // Key falls out of current file's range
          if (cmp_smallest < 0 || cmp_largest > 0) {
            if (curr_level_ == 0) {
              ++curr_index_in_curr_level_;
              continue;
            } else {
              // Search next level.
              break;
            }
          }
        }

        returned_file_level_ = curr_level_;
        if (curr_level_ > 0 && cmp_largest < 0) {
          // No more files to search in this level.
          search_ended_ = !PrepareNextLevel();
        } else {
          ++curr_index_in_curr_level_;
        }
        return f;
      }
      // Start searching next level.
      search_ended_ = !PrepareNextLevel();
    }
    // Search ended.
    return nullptr;
  }

  // getter for current file level
  // for GET_HIT_L0, GET_HIT_L1 & GET_HIT_L2_AND_UP counts
  unsigned int GetHitFileLevel() { return hit_file_level_; }

  // Returns true if the most recent "hit file" (i.e., one returned by
  // GetNextFile()) is at the last index in its level.
  bool IsHitFileLastInLevel() { return is_hit_file_last_in_level_; }

 private:
  unsigned int num_levels_;
  unsigned int curr_level_;
  unsigned int returned_file_level_;
  unsigned int hit_file_level_;
  int32_t search_left_bound_;
  int32_t search_right_bound_;
  autovector<LevelFilesBrief>* level_files_brief_;
  bool search_ended_;
  bool is_hit_file_last_in_level_;
  LevelFilesBrief* curr_file_level_;
  unsigned int curr_index_in_curr_level_;
  unsigned int start_index_in_curr_level_;
  Slice user_key_;
  Slice ikey_;
  FileIndexer* file_indexer_;
  const Comparator* user_comparator_;
  const InternalKeyComparator* internal_comparator_;

  // Setup local variables to search next level.
  // Returns false if there are no more levels to search.
  bool PrepareNextLevel() {
    curr_level_++;
    while (curr_level_ < num_levels_) {
      curr_file_level_ = &(*level_files_brief_)[curr_level_];
      if (curr_file_level_->num_files == 0) {
        // When current level is empty, the search bound generated from upper
        // level must be [0, -1] or [0, FileIndexer::kLevelMaxIndex] if it is
        // also empty.
        assert(search_left_bound_ == 0);
        assert(search_right_bound_ == -1 ||
               search_right_bound_ == FileIndexer::kLevelMaxIndex);
        // Since current level is empty, it will need to search all files in
        // the next level
        search_left_bound_ = 0;
        search_right_bound_ = FileIndexer::kLevelMaxIndex;
        curr_level_++;
        continue;
      }

      // Some files may overlap each other. We find
      // all files that overlap user_key and process them in order from
      // newest to oldest. In the context of merge-operator, this can occur at
      // any level. Otherwise, it only occurs at Level-0 (since Put/Deletes
      // are always compacted into a single entry).
      int32_t start_index;
      if (curr_level_ == 0) {
        // On Level-0, we read through all files to check for overlap.
        start_index = 0;
      } else {
        // On Level-n (n>=1), files are sorted. Binary search to find the
        // earliest file whose largest key >= ikey. Search left bound and
        // right bound are used to narrow the range.
        if (search_left_bound_ <= search_right_bound_) {
          if (search_right_bound_ == FileIndexer::kLevelMaxIndex) {
            search_right_bound_ =
                static_cast<int32_t>(curr_file_level_->num_files) - 1;
          }
          // `search_right_bound_` is an inclusive upper-bound, but since it was
          // determined based on user key, it is still possible the lookup key
          // falls to the right of `search_right_bound_`'s corresponding file.
          // So, pass a limit one higher, which allows us to detect this case.
          start_index =
              FindFileInRange(*internal_comparator_, *curr_file_level_, ikey_,
                              static_cast<uint32_t>(search_left_bound_),
                              static_cast<uint32_t>(search_right_bound_) + 1);
          if (start_index == search_right_bound_ + 1) {
            // `ikey_` comes after `search_right_bound_`. The lookup key does
            // not exist on this level, so let's skip this level and do a full
            // binary search on the next level.
            search_left_bound_ = 0;
            search_right_bound_ = FileIndexer::kLevelMaxIndex;
            curr_level_++;
            continue;
          }
        } else {
          // search_left_bound > search_right_bound, key does not exist in
          // this level. Since no comparison is done in this level, it will
          // need to search all files in the next level.
          search_left_bound_ = 0;
          search_right_bound_ = FileIndexer::kLevelMaxIndex;
          curr_level_++;
          continue;
        }
      }
      start_index_in_curr_level_ = start_index;
      curr_index_in_curr_level_ = start_index;

      return true;
    }
    // curr_level_ = num_levels_. So, no more levels to search.
    return false;
  }
};

class FilePickerMultiGet {
 private:
  struct FilePickerContext;

 public:
  FilePickerMultiGet(MultiGetRange* range,
                     autovector<LevelFilesBrief>* file_levels,
                     unsigned int num_levels, FileIndexer* file_indexer,
                     const Comparator* user_comparator,
                     const InternalKeyComparator* internal_comparator)
      : num_levels_(num_levels),
        curr_level_(static_cast<unsigned int>(-1)),
        returned_file_level_(static_cast<unsigned int>(-1)),
        hit_file_level_(static_cast<unsigned int>(-1)),
        range_(*range, range->begin(), range->end()),
        maybe_repeat_key_(false),
        current_level_range_(*range, range->begin(), range->end()),
        current_file_range_(*range, range->begin(), range->end()),
        batch_iter_(range->begin()),
        batch_iter_prev_(range->begin()),
        upper_key_(range->begin()),
        level_files_brief_(file_levels),
        is_hit_file_last_in_level_(false),
        curr_file_level_(nullptr),
        file_indexer_(file_indexer),
        user_comparator_(user_comparator),
        internal_comparator_(internal_comparator),
        hit_file_(nullptr) {
    for (auto iter = range_.begin(); iter != range_.end(); ++iter) {
      fp_ctx_array_[iter.index()] =
          FilePickerContext(0, FileIndexer::kLevelMaxIndex);
    }

    // Setup member variables to search first level.
    search_ended_ = !PrepareNextLevel();
    if (!search_ended_) {
      // REVISIT
      // Prefetch Level 0 table data to avoid cache miss if possible.
      // As of now, only PlainTableReader and CuckooTableReader do any
      // prefetching. This may not be necessary anymore once we implement
      // batching in those table readers
      for (unsigned int i = 0; i < (*level_files_brief_)[0].num_files; ++i) {
        auto* r = (*level_files_brief_)[0].files[i].fd.pinned_reader.Get();
        if (r) {
          for (auto iter = range_.begin(); iter != range_.end(); ++iter) {
            r->Prepare(iter->ikey);
          }
        }
      }
    }
  }

  FilePickerMultiGet(MultiGetRange* range, const FilePickerMultiGet& other)
      : num_levels_(other.num_levels_),
        curr_level_(other.curr_level_),
        returned_file_level_(other.returned_file_level_),
        hit_file_level_(other.hit_file_level_),
        fp_ctx_array_(other.fp_ctx_array_),
        range_(*range, range->begin(), range->end()),
        maybe_repeat_key_(false),
        current_level_range_(*range, range->begin(), range->end()),
        current_file_range_(*range, range->begin(), range->end()),
        batch_iter_(range->begin()),
        batch_iter_prev_(range->begin()),
        upper_key_(range->begin()),
        level_files_brief_(other.level_files_brief_),
        is_hit_file_last_in_level_(false),
        curr_file_level_(other.curr_file_level_),
        file_indexer_(other.file_indexer_),
        user_comparator_(other.user_comparator_),
        internal_comparator_(other.internal_comparator_),
        hit_file_(nullptr) {
    PrepareNextLevelForSearch();
  }

  int GetCurrentLevel() const { return curr_level_; }

  void PrepareNextLevelForSearch() { search_ended_ = !PrepareNextLevel(); }

  FdWithKeyRange* GetNextFileInLevel() {
    if (batch_iter_ == current_level_range_.end() || search_ended_) {
      hit_file_ = nullptr;
      return nullptr;
    } else {
      if (maybe_repeat_key_) {
        maybe_repeat_key_ = false;
        // Check if we found the final value for the last key in the
        // previous lookup range. If we did, then there's no need to look
        // any further for that key, so advance batch_iter_. Else, keep
        // batch_iter_ positioned on that key so we look it up again in
        // the next file
        // For L0, always advance the key because we will look in the next
        // file regardless for all keys not found yet
        if (current_level_range_.CheckKeyDone(batch_iter_) ||
            curr_level_ == 0) {
          batch_iter_ = upper_key_;
        }
      }
      // batch_iter_prev_ will become the start key for the next file
      // lookup
      batch_iter_prev_ = batch_iter_;
    }

    MultiGetRange next_file_range(current_level_range_, batch_iter_prev_,
                                  current_level_range_.end());
    size_t curr_file_index =
        (batch_iter_ != current_level_range_.end())
            ? fp_ctx_array_[batch_iter_.index()].curr_index_in_curr_level
            : curr_file_level_->num_files;
    FdWithKeyRange* f;
    bool is_last_key_in_file;
    if (!GetNextFileInLevelWithKeys(&next_file_range, &curr_file_index, &f,
                                    &is_last_key_in_file)) {
      hit_file_ = nullptr;
      return nullptr;
    } else {
      if (is_last_key_in_file) {
        // Since cmp_largest is 0, batch_iter_ still points to the last key
        // that falls in this file, instead of the next one. Increment
        // the file index for all keys between batch_iter_ and upper_key_
        auto tmp_iter = batch_iter_;
        while (tmp_iter != upper_key_) {
          ++(fp_ctx_array_[tmp_iter.index()].curr_index_in_curr_level);
          ++tmp_iter;
        }
        maybe_repeat_key_ = true;
      }
      // Set the range for this file
      current_file_range_ =
          MultiGetRange(next_file_range, batch_iter_prev_, upper_key_);
      returned_file_level_ = curr_level_;
      hit_file_level_ = curr_level_;
      is_hit_file_last_in_level_ =
          curr_file_index == curr_file_level_->num_files - 1;
      hit_file_ = f;
      return f;
    }
  }

  // getter for current file level
  // for GET_HIT_L0, GET_HIT_L1 & GET_HIT_L2_AND_UP counts
  unsigned int GetHitFileLevel() { return hit_file_level_; }

  FdWithKeyRange* GetHitFile() { return hit_file_; }

  // Returns true if the most recent "hit file" (i.e., one returned by
  // GetNextFile()) is at the last index in its level.
  bool IsHitFileLastInLevel() { return is_hit_file_last_in_level_; }

  bool KeyMaySpanNextFile() { return maybe_repeat_key_; }

  bool IsSearchEnded() { return search_ended_; }

  const MultiGetRange& CurrentFileRange() { return current_file_range_; }

  bool RemainingOverlapInLevel() {
    return !current_level_range_.Suffix(current_file_range_).empty();
  }

  MultiGetRange& GetRange() { return range_; }

  void ReplaceRange(const MultiGetRange& other) {
    assert(hit_file_ == nullptr);
    range_ = other;
    current_level_range_ = other;
  }

  FilePickerMultiGet(FilePickerMultiGet&& other)
      : num_levels_(other.num_levels_),
        curr_level_(other.curr_level_),
        returned_file_level_(other.returned_file_level_),
        hit_file_level_(other.hit_file_level_),
        fp_ctx_array_(std::move(other.fp_ctx_array_)),
        range_(std::move(other.range_)),
        maybe_repeat_key_(other.maybe_repeat_key_),
        current_level_range_(std::move(other.current_level_range_)),
        current_file_range_(std::move(other.current_file_range_)),
        batch_iter_(other.batch_iter_, &current_level_range_),
        batch_iter_prev_(other.batch_iter_prev_, &current_level_range_),
        upper_key_(other.upper_key_, &current_level_range_),
        level_files_brief_(other.level_files_brief_),
        search_ended_(other.search_ended_),
        is_hit_file_last_in_level_(other.is_hit_file_last_in_level_),
        curr_file_level_(other.curr_file_level_),
        file_indexer_(other.file_indexer_),
        user_comparator_(other.user_comparator_),
        internal_comparator_(other.internal_comparator_),
        hit_file_(other.hit_file_) {}
  FilePickerMultiGet& operator=(FilePickerMultiGet&&) = delete;
  ~FilePickerMultiGet() = default;

 private:
  unsigned int num_levels_;
  unsigned int curr_level_;
  unsigned int returned_file_level_;
  unsigned int hit_file_level_;

  struct FilePickerContext {
    int32_t search_left_bound;
    int32_t search_right_bound;
    unsigned int curr_index_in_curr_level;
    unsigned int start_index_in_curr_level;

    FilePickerContext(int32_t left, int32_t right)
        : search_left_bound(left),
          search_right_bound(right),
          curr_index_in_curr_level(0),
          start_index_in_curr_level(0) {}

    FilePickerContext() = default;
  };
  std::array<FilePickerContext, MultiGetContext::MAX_BATCH_SIZE> fp_ctx_array_;
  MultiGetRange range_;
  bool maybe_repeat_key_;
  MultiGetRange current_level_range_;
  MultiGetRange current_file_range_;
  // Iterator to iterate through the keys in a MultiGet batch, that gets reset
  // at the beginning of each level. Each call to GetNextFile() will position
  // batch_iter_ at or right after the last key that was found in the returned
  // SST file
  MultiGetRange::Iterator batch_iter_;
  // An iterator that records the previous position of batch_iter_, i.e last
  // key found in the previous SST file, in order to serve as the start of
  // the batch key range for the next SST file
  MultiGetRange::Iterator batch_iter_prev_;
  MultiGetRange::Iterator upper_key_;
  autovector<LevelFilesBrief>* level_files_brief_;
  bool search_ended_;
  bool is_hit_file_last_in_level_;
  LevelFilesBrief* curr_file_level_;
  FileIndexer* file_indexer_;
  const Comparator* user_comparator_;
  const InternalKeyComparator* internal_comparator_;
  FdWithKeyRange* hit_file_;

  // Iterates through files in the current level until it finds a file that
  // contains at least one key from the MultiGet batch
  bool GetNextFileInLevelWithKeys(MultiGetRange* next_file_range,
                                  size_t* file_index, FdWithKeyRange** fd,
                                  bool* is_last_key_in_file) {
    size_t curr_file_index = *file_index;
    FdWithKeyRange* f = nullptr;
    bool file_hit = false;
    int cmp_largest = -1;
    int cmp_smallest = -1;
    if (curr_file_index >= curr_file_level_->num_files) {
      // In the unlikely case the next key is a duplicate of the current key,
      // and the current key is the last in the level and the internal key
      // was not found, we need to skip lookup for the remaining keys and
      // reset the search bounds
      if (batch_iter_ != current_level_range_.end()) {
#ifndef NDEBUG
        if (curr_level_ < num_levels_ + 1) {
          if ((*level_files_brief_)[curr_level_].num_files == 0) {
            struct FilePickerContext& fp_ctx =
                fp_ctx_array_[batch_iter_.index()];

            assert(fp_ctx.search_left_bound == 0);
            assert(fp_ctx.search_right_bound == -1 ||
                   fp_ctx.search_right_bound == FileIndexer::kLevelMaxIndex);
          }
        }
#endif  // NDBEUG

        ++batch_iter_;
        for (; batch_iter_ != current_level_range_.end(); ++batch_iter_) {
          struct FilePickerContext& fp_ctx = fp_ctx_array_[batch_iter_.index()];
          fp_ctx.search_left_bound = 0;
          fp_ctx.search_right_bound = FileIndexer::kLevelMaxIndex;
        }
      }
      return false;
    }
    // Loops over keys in the MultiGet batch until it finds a file with
    // atleast one of the keys. Then it keeps moving forward until the
    // last key in the batch that falls in that file
    while (batch_iter_ != current_level_range_.end() &&
           (fp_ctx_array_[batch_iter_.index()].curr_index_in_curr_level ==
                curr_file_index ||
            !file_hit)) {
      struct FilePickerContext& fp_ctx = fp_ctx_array_[batch_iter_.index()];
      f = &curr_file_level_->files[fp_ctx.curr_index_in_curr_level];
      Slice& user_key = batch_iter_->ukey_without_ts;

      // Do key range filtering of files or/and fractional cascading if:
      // (1) not all the files are in level 0, or
      // (2) there are more than 3 current level files
      // If there are only 3 or less current level files in the system, we
      // skip the key range filtering. In this case, more likely, the system
      // is highly tuned to minimize number of tables queried by each query,
      // so it is unlikely that key range filtering is more efficient than
      // querying the files.
      if (num_levels_ > 1 || curr_file_level_->num_files > 3) {
        // Check if key is within a file's range. If search left bound and
        // right bound point to the same find, we are sure key falls in
        // range.
        cmp_smallest = user_comparator_->CompareWithoutTimestamp(
            user_key, false, ExtractUserKey(f->smallest_key), true);

        assert(curr_level_ == 0 ||
               fp_ctx.curr_index_in_curr_level ==
                   fp_ctx.start_index_in_curr_level ||
               cmp_smallest <= 0);

        if (cmp_smallest >= 0) {
          cmp_largest = user_comparator_->CompareWithoutTimestamp(
              user_key, false, ExtractUserKey(f->largest_key), true);
        } else {
          cmp_largest = -1;
        }

        // Setup file search bound for the next level based on the
        // comparison results
        if (curr_level_ > 0) {
          file_indexer_->GetNextLevelIndex(
              curr_level_, fp_ctx.curr_index_in_curr_level, cmp_smallest,
              cmp_largest, &fp_ctx.search_left_bound,
              &fp_ctx.search_right_bound);
        }
        // Key falls out of current file's range
        if (cmp_smallest < 0 || cmp_largest > 0) {
          next_file_range->SkipKey(batch_iter_);
        } else {
          file_hit = true;
        }
      } else {
        file_hit = true;
      }
      if (cmp_largest == 0) {
        // cmp_largest is 0, which means the next key will not be in this
        // file, so stop looking further. However, its possible there are
        // duplicates in the batch, so find the upper bound for the batch
        // in this file (upper_key_) by skipping past the duplicates. We
        // leave batch_iter_ as is since we may have to pick up from there
        // for the next file, if this file has a merge value rather than
        // final value
        upper_key_ = batch_iter_;
        ++upper_key_;
        while (upper_key_ != current_level_range_.end() &&
               user_comparator_->CompareWithoutTimestamp(
                   batch_iter_->ukey_without_ts, false,
                   upper_key_->ukey_without_ts, false) == 0) {
          if (curr_level_ > 0) {
            struct FilePickerContext& ctx = fp_ctx_array_[upper_key_.index()];
            file_indexer_->GetNextLevelIndex(
                curr_level_, ctx.curr_index_in_curr_level, cmp_smallest,
                cmp_largest, &ctx.search_left_bound, &ctx.search_right_bound);
          }
          ++upper_key_;
        }
        break;
      } else {
        if (curr_level_ == 0) {
          // We need to look through all files in level 0
          ++fp_ctx.curr_index_in_curr_level;
        }
        ++batch_iter_;
      }
      if (!file_hit) {
        curr_file_index =
            (batch_iter_ != current_level_range_.end())
                ? fp_ctx_array_[batch_iter_.index()].curr_index_in_curr_level
                : curr_file_level_->num_files;
      }
    }

    *fd = f;
    *file_index = curr_file_index;
    *is_last_key_in_file = cmp_largest == 0;
    if (!*is_last_key_in_file) {
      // If the largest key in the batch overlapping the file is not the
      // largest key in the file, upper_ley_ would not have been updated so
      // update it here
      upper_key_ = batch_iter_;
    }
    return file_hit;
  }

  // Setup local variables to search next level.
  // Returns false if there are no more levels to search.
  bool PrepareNextLevel() {
    if (curr_level_ == 0) {
      MultiGetRange::Iterator mget_iter = current_level_range_.begin();
      if (fp_ctx_array_[mget_iter.index()].curr_index_in_curr_level <
          curr_file_level_->num_files) {
        batch_iter_prev_ = current_level_range_.begin();
        upper_key_ = batch_iter_ = current_level_range_.begin();
        return true;
      }
    }

    curr_level_++;
    // Reset key range to saved value
    while (curr_level_ < num_levels_) {
      bool level_contains_keys = false;
      curr_file_level_ = &(*level_files_brief_)[curr_level_];
      if (curr_file_level_->num_files == 0) {
        // When current level is empty, the search bound generated from upper
        // level must be [0, -1] or [0, FileIndexer::kLevelMaxIndex] if it is
        // also empty.

        for (auto mget_iter = current_level_range_.begin();
             mget_iter != current_level_range_.end(); ++mget_iter) {
          struct FilePickerContext& fp_ctx = fp_ctx_array_[mget_iter.index()];

          assert(fp_ctx.search_left_bound == 0);
          assert(fp_ctx.search_right_bound == -1 ||
                 fp_ctx.search_right_bound == FileIndexer::kLevelMaxIndex);
          // Since current level is empty, it will need to search all files in
          // the next level
          fp_ctx.search_left_bound = 0;
          fp_ctx.search_right_bound = FileIndexer::kLevelMaxIndex;
        }
        // Skip all subsequent empty levels
        do {
          ++curr_level_;
        } while ((curr_level_ < num_levels_) &&
                 (*level_files_brief_)[curr_level_].num_files == 0);
        continue;
      }

      // Some files may overlap each other. We find
      // all files that overlap user_key and process them in order from
      // newest to oldest. In the context of merge-operator, this can occur at
      // any level. Otherwise, it only occurs at Level-0 (since Put/Deletes
      // are always compacted into a single entry).
      int32_t start_index = -1;
      current_level_range_ =
          MultiGetRange(range_, range_.begin(), range_.end());
      for (auto mget_iter = current_level_range_.begin();
           mget_iter != current_level_range_.end(); ++mget_iter) {
        struct FilePickerContext& fp_ctx = fp_ctx_array_[mget_iter.index()];
        if (curr_level_ == 0) {
          // On Level-0, we read through all files to check for overlap.
          start_index = 0;
          level_contains_keys = true;
        } else {
          // On Level-n (n>=1), files are sorted. Binary search to find the
          // earliest file whose largest key >= ikey. Search left bound and
          // right bound are used to narrow the range.
          if (fp_ctx.search_left_bound <= fp_ctx.search_right_bound) {
            if (fp_ctx.search_right_bound == FileIndexer::kLevelMaxIndex) {
              fp_ctx.search_right_bound =
                  static_cast<int32_t>(curr_file_level_->num_files) - 1;
            }
            // `search_right_bound_` is an inclusive upper-bound, but since it
            // was determined based on user key, it is still possible the lookup
            // key falls to the right of `search_right_bound_`'s corresponding
            // file. So, pass a limit one higher, which allows us to detect this
            // case.
            Slice& ikey = mget_iter->ikey;
            start_index = FindFileInRange(
                *internal_comparator_, *curr_file_level_, ikey,
                static_cast<uint32_t>(fp_ctx.search_left_bound),
                static_cast<uint32_t>(fp_ctx.search_right_bound) + 1);
            if (start_index == fp_ctx.search_right_bound + 1) {
              // `ikey_` comes after `search_right_bound_`. The lookup key does
              // not exist on this level, so let's skip this level and do a full
              // binary search on the next level.
              fp_ctx.search_left_bound = 0;
              fp_ctx.search_right_bound = FileIndexer::kLevelMaxIndex;
              current_level_range_.SkipKey(mget_iter);
              continue;
            } else {
              level_contains_keys = true;
            }
          } else {
            // search_left_bound > search_right_bound, key does not exist in
            // this level. Since no comparison is done in this level, it will
            // need to search all files in the next level.
            fp_ctx.search_left_bound = 0;
            fp_ctx.search_right_bound = FileIndexer::kLevelMaxIndex;
            current_level_range_.SkipKey(mget_iter);
            continue;
          }
        }
        assert(start_index >= 0);
        assert(start_index < static_cast<int32_t>(curr_file_level_->num_files));
        fp_ctx.start_index_in_curr_level = start_index;
        fp_ctx.curr_index_in_curr_level = start_index;
      }
      if (level_contains_keys) {
        batch_iter_prev_ = current_level_range_.begin();
        upper_key_ = batch_iter_ = current_level_range_.begin();
        return true;
      }
      curr_level_++;
    }
    // curr_level_ = num_levels_. So, no more levels to search.
    return false;
  }
};

VersionStorageInfo::~VersionStorageInfo() { delete[] files_; }

Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  for (int level = 0; level < storage_info_.num_levels_; level++) {
    for (size_t i = 0; i < storage_info_.files_[level].size(); i++) {
      FileMetaData* f = storage_info_.files_[level][i];
      assert(f->refs > 0);
      f->refs--;
      if (f->refs <= 0) {
        assert(cfd_ != nullptr);
        // When not in the process of closing the DB, we'll have a superversion
        // to get current mutable options from
        auto* sv = cfd_->GetSuperVersion();
        uint32_t path_id = f->fd.GetPathId();
        assert(path_id < cfd_->ioptions().cf_paths.size());
        vset_->obsolete_files_.emplace_back(
            f, cfd_->ioptions().cf_paths[path_id].path,
            sv ? sv->mutable_cf_options.uncache_aggressiveness : 0,
            cfd_->GetFileMetadataCacheReservationManager());
      }
    }
  }
}

int FindFile(const InternalKeyComparator& icmp,
             const LevelFilesBrief& file_level, const Slice& key) {
  return FindFileInRange(icmp, file_level, key, 0,
                         static_cast<uint32_t>(file_level.num_files));
}

void DoGenerateLevelFilesBrief(LevelFilesBrief* file_level,
                               const std::vector<FileMetaData*>& files,
                               Arena* arena) {
  assert(file_level);
  assert(arena);

  size_t num = files.size();
  file_level->num_files = num;
  char* mem = arena->AllocateAligned(num * sizeof(FdWithKeyRange));
  file_level->files = new (mem) FdWithKeyRange[num];

  for (size_t i = 0; i < num; i++) {
    Slice smallest_key = files[i]->smallest.Encode();
    Slice largest_key = files[i]->largest.Encode();

    // Copy key slice to sequential memory
    size_t smallest_size = smallest_key.size();
    size_t largest_size = largest_key.size();
    mem = arena->AllocateAligned(smallest_size + largest_size);
    memcpy(mem, smallest_key.data(), smallest_size);
    memcpy(mem + smallest_size, largest_key.data(), largest_size);

    FdWithKeyRange& f = file_level->files[i];
    f.fd = files[i]->fd;
    f.file_metadata = files[i];
    f.smallest_key = Slice(mem, smallest_size);
    f.largest_key = Slice(mem + smallest_size, largest_size);
  }
}

static bool AfterFile(const Comparator* ucmp, const Slice* user_key,
                      const FdWithKeyRange* f) {
  // nullptr user_key occurs before all keys and is therefore never after *f
  return (user_key != nullptr &&
          ucmp->CompareWithoutTimestamp(*user_key,
                                        ExtractUserKey(f->largest_key)) > 0);
}

static bool BeforeFile(const Comparator* ucmp, const Slice* user_key,
                       const FdWithKeyRange* f) {
  // nullptr user_key occurs after all keys and is therefore never before *f
  return (user_key != nullptr &&
          ucmp->CompareWithoutTimestamp(*user_key,
                                        ExtractUserKey(f->smallest_key)) < 0);
}

bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const LevelFilesBrief& file_level,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();
  if (!disjoint_sorted_files) {
    // Need to check against all files
    for (size_t i = 0; i < file_level.num_files; i++) {
      const FdWithKeyRange* f = &(file_level.files[i]);
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) {
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }

  // Binary search over file list
  uint32_t index = 0;
  if (smallest_user_key != nullptr) {
    // Find the leftmost possible internal key for smallest_user_key
    InternalKey small;
    small.SetMinPossibleForUserKey(*smallest_user_key);
    index = FindFile(icmp, file_level, small.Encode());
  }

  if (index >= file_level.num_files) {
    // beginning of range is after all files, so no overlap.
    return false;
  }

  return !BeforeFile(ucmp, largest_user_key, &file_level.files[index]);
}

namespace {

TableCacheOpenOptions GetCompactionTableCacheOpenOptions(
    bool open_ephemeral_table_reader) {
  TableCacheOpenOptions options;
  options.open_ephemeral_table_reader = open_ephemeral_table_reader;
  options.avoid_shared_metadata_cache = open_ephemeral_table_reader;
  options.skip_filters = open_ephemeral_table_reader;
  return options;
}

class LevelIterator final : public InternalIterator {
 public:
  // NOTE: many of the const& parameters are saved in this object (so
  // must outlive this object)
  LevelIterator(
      TableCache* table_cache, const ReadOptions& read_options,
      const FileOptions& file_options, const InternalKeyComparator& icomparator,
      const LevelFilesBrief* flevel, const MutableCFOptions& mutable_cf_options,
      HistogramImpl* file_read_hist, TableReaderCaller caller,
      bool skip_filters, int level, RangeDelAggregator* range_del_agg,
      const std::vector<AtomicCompactionUnitBoundary>* compaction_boundaries =
          nullptr,
      bool allow_unprepared_value = false,
      std::unique_ptr<TruncatedRangeDelIterator>*** range_tombstone_iter_ptr_ =
          nullptr,
      SequenceNumber read_seq = kMaxSequenceNumber,
      Statistics* db_statistics = nullptr, SystemClock* clock = nullptr,
      bool open_ephemeral_table_reader = false)
      : table_cache_(table_cache),
        read_options_(read_options),
        file_options_(file_options),
        icomparator_(icomparator),
        user_comparator_(icomparator.user_comparator()),
        flevel_(flevel),
        mutable_cf_options_(mutable_cf_options),
        prefix_extractor_(mutable_cf_options.prefix_extractor.get()),
        file_read_hist_(file_read_hist),
        caller_(caller),
        file_index_(flevel_->num_files),
        range_del_agg_(range_del_agg),
        pinned_iters_mgr_(nullptr),
        compaction_boundaries_(compaction_boundaries),
        range_tombstone_iter_(nullptr),
        range_tombstone_iter_required_(range_tombstone_iter_ptr_ != nullptr),
        read_seq_(read_seq != kMaxSequenceNumber ||
                          read_options.snapshot == nullptr
                      ? read_seq
                      : read_options.snapshot->GetSequenceNumber()),
        level_(level),
        skip_filters_(skip_filters),
        allow_unprepared_value_(allow_unprepared_value),
        is_next_read_sequential_(false),
        to_return_sentinel_(false),
        scan_opts_(nullptr),
        db_statistics_(db_statistics),
        clock_(clock),
        table_cache_open_options_(
            GetCompactionTableCacheOpenOptions(open_ephemeral_table_reader)) {
    // Empty level is not supported.
    assert(flevel_ != nullptr && flevel_->num_files > 0);
    if (range_tombstone_iter_ptr_) {
      *range_tombstone_iter_ptr_ = &range_tombstone_iter_;
    }
  }

  // No copying allowed
  LevelIterator(const LevelIterator&) = delete;
  LevelIterator& operator=(const LevelIterator&) = delete;
  LevelIterator(LevelIterator&&) = delete;
  LevelIterator& operator=(LevelIterator&&) = delete;

  ~LevelIterator() override {
    delete file_iter_.Set(nullptr);
    // Clean up any prepared iterators that weren't used
    for (auto& entry : prepared_iters_) {
      delete entry.second;
    }
    prepared_iters_.clear();
    assert(prepared_iters_.size() == 0);
  }

  // Seek to the first file with a key >= target.
  // If range_tombstone_iter_ is not nullptr, then we pretend that file
  // boundaries are fake keys (sentinel keys). These keys are used to keep range
  // tombstones alive even when all point keys in an SST file are exhausted.
  // These sentinel keys will be skipped in merging iterator.
  void Seek(const Slice& target) override;
  void SeekForPrev(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() final;
  bool NextAndGetResult(IterateResult* result) override;
  void Prev() override;

  // In addition to valid and invalid state (!file_iter.Valid() and
  // status.ok()), a third state of the iterator is when !file_iter_.Valid() and
  // to_return_sentinel_. This means we are at the end of a file, and a sentinel
  // key (the file boundary that we pretend as a key) is to be returned next.
  // file_iter_.Valid() and to_return_sentinel_ should not both be true.
  bool Valid() const override {
    assert(!(file_iter_.Valid() && to_return_sentinel_));
    return file_iter_.Valid() || to_return_sentinel_;
  }
  Slice key() const override {
    assert(Valid());
    if (to_return_sentinel_) {
      // Sentinel should be returned after file_iter_ reaches the end of the
      // file
      assert(!file_iter_.Valid());
      return sentinel_;
    }
    return file_iter_.key();
  }

  Slice value() const override {
    assert(Valid());
    assert(!to_return_sentinel_);
    return file_iter_.value();
  }

  uint64_t write_unix_time() const override {
    assert(Valid());
    return file_iter_.write_unix_time();
  }

  Status status() const override {
    return file_iter_.iter() ? file_iter_.status() : Status::OK();
  }

  bool PrepareValue() override { return file_iter_.PrepareValue(); }

  inline bool MayBeOutOfLowerBound() override {
    assert(Valid());
    return may_be_out_of_lower_bound_ && file_iter_.MayBeOutOfLowerBound();
  }

  inline IterBoundCheck UpperBoundCheckResult() override {
    if (Valid()) {
      return file_iter_.UpperBoundCheckResult();
    } else {
      return IterBoundCheck::kUnknown;
    }
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
    if (file_iter_.iter()) {
      file_iter_.SetPinnedItersMgr(pinned_iters_mgr);
    }
  }

  bool IsKeyPinned() const override {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           file_iter_.iter() && file_iter_.IsKeyPinned();
  }

  bool IsValuePinned() const override {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           file_iter_.iter() && file_iter_.IsValuePinned();
  }

  bool IsDeleteRangeSentinelKey() const override { return to_return_sentinel_; }

  void SetRangeDelReadSeqno(SequenceNumber read_seq) override {
    read_seq_ = read_seq;
  }

  inline bool FileHasMultiScanArg(size_t file_index) {
    if (file_to_scan_opts_.get()) {
      auto it = file_to_scan_opts_->find(file_index);
      if (it != file_to_scan_opts_->end()) {
        return !it->second.empty();
      }
    }
    return false;
  }

  MultiScanArgs& GetMultiScanArgForFile(size_t file_index) {
    auto multi_scan_args_it = file_to_scan_opts_->find(file_index);
    if (multi_scan_args_it == file_to_scan_opts_->end()) {
      auto ret = file_to_scan_opts_->emplace(
          file_index, MultiScanArgs(user_comparator_.user_comparator()));
      multi_scan_args_it = ret.first;
      assert(ret.second);
    }
    return multi_scan_args_it->second;
  }

  void Prepare(const MultiScanArgs* so) override {
    // We assume here that scan_opts is sorted such that
    // scan_opts[0].range.start < scan_opts[1].range.start, and non
    // overlapping
    if (so == nullptr) {
      return;
    }

    scan_opts_ = so;

    // Verify comparator is consistent
    assert(so->GetComparator() == user_comparator_.user_comparator());

    file_to_scan_opts_ = std::make_unique<ScanOptionsMap>();
    ForEachMultiScanOverlappingFile(
        icomparator_, user_comparator_, *flevel_, level_, *scan_opts_,
        [&](size_t file_index, const ScanOptions& scan_range) {
          auto const metadata = flevel_->files[file_index].file_metadata;
          if (metadata->FileIsStandAloneRangeTombstone()) {
            // Skip stand alone range deletion files.
            return;
          }
          auto& args = GetMultiScanArgForFile(file_index);
          args.insert(scan_range.range.start.value(),
                      scan_range.range.limit.value(), scan_range.property_bag);
        });

    StopWatch timer(clock_, db_statistics_, MULTISCAN_PREPARE_ITERATORS);

    // Propagate multiscan configs
    for (auto& file_to_arg : *file_to_scan_opts_) {
      file_to_arg.second.CopyConfigFrom(*so);
      assert(OverlapRange(*file_to_arg.second.GetScanRanges().begin(),
                          file_to_arg.first) &&
             OverlapRange(*file_to_arg.second.GetScanRanges().rbegin(),
                          file_to_arg.first));
    }

    if (so->use_async_io &&
        (!range_tombstone_iter_required_ || range_tombstone_iter_ != nullptr)) {
      auto before = file_index_;
      // Pre-create and prepare only relevant file iterators
      for (auto& file_to_arg : *file_to_scan_opts_) {
        size_t file_index = file_to_arg.first;

        file_index_ = file_index;
        // Create iterator for this file
        auto iter = NewFileIterator();
        if (iter != nullptr) {
          // If we have async enabled, lets prepare all our iterators.
          iter->Prepare(&file_to_arg.second);
          // Store the prepared iterator
          prepared_iters_[file_index] = iter;
        }
      }
      file_index_ = before;
    }
  }

 private:
  // Return true if at least one invalid file is seen and skipped.
  bool SkipEmptyFileForward();
  void SkipEmptyFileBackward();
  void SetFileIterator(InternalIterator* iter);
  void InitFileIterator(size_t new_file_index);

  const Slice& file_smallest_key(size_t file_index) {
    assert(file_index < flevel_->num_files);
    return flevel_->files[file_index].smallest_key;
  }

  const Slice& file_largest_key(size_t file_index) {
    assert(file_index < flevel_->num_files);
    return flevel_->files[file_index].largest_key;
  }

  bool KeyReachedUpperBound(const Slice& internal_key) {
    return read_options_.iterate_upper_bound != nullptr &&
           user_comparator_.CompareWithoutTimestamp(
               ExtractUserKey(internal_key), /*a_has_ts=*/true,
               *read_options_.iterate_upper_bound, /*b_has_ts=*/false) >= 0;
  }

  template <bool IsSeek>
  void SampleRead() {
    bool sampled =
        IsSeek ? should_sample_file_read() : should_sample_file_read_next();
    if (!sampled) {
      return;
    }

    if (file_index_ >= flevel_->num_files || !file_iter_.Valid()) {
      return;
    }
    const FileMetaData* meta = flevel_->files[file_index_].file_metadata;
    sample_file_read_inc(meta);
    ValueType type = ExtractValueType(file_iter_.key());
    if (type == kTypeDeletion || type == kTypeSingleDeletion ||
        type == kTypeDeletionWithTimestamp || type == kTypeMerge) {
      sample_collapsible_entry_file_read_inc(meta);
    }
  }

  void ClearRangeTombstoneIter() {
    if (range_tombstone_iter_) {
      range_tombstone_iter_->reset();
    }
  }

  // Move file_iter_ to the file at file_index_.
  // range_tombstone_iter_ is updated with a range tombstone iterator
  // into the new file. Old range tombstone iterator is cleared.
  InternalIterator* NewFileIterator() {
    assert(file_index_ < flevel_->num_files);
    auto file_meta = flevel_->files[file_index_];

    const InternalKey* smallest_compaction_key = nullptr;
    const InternalKey* largest_compaction_key = nullptr;
    if (compaction_boundaries_ != nullptr) {
      smallest_compaction_key = (*compaction_boundaries_)[file_index_].smallest;
      largest_compaction_key = (*compaction_boundaries_)[file_index_].largest;
    }
    CheckMayBeOutOfLowerBound();
    ClearRangeTombstoneIter();
    return table_cache_->NewIterator(
        read_options_, file_options_, icomparator_, *file_meta.file_metadata,
        range_del_agg_, mutable_cf_options_,
        nullptr /* don't need reference to table */, file_read_hist_, caller_,
        /*arena=*/nullptr, skip_filters_, level_,
        /*max_file_size_for_l0_meta_pin=*/0, smallest_compaction_key,
        largest_compaction_key, allow_unprepared_value_, &read_seq_,
        range_tombstone_iter_,
        /*maybe_pin_table_handle=*/
        !table_cache_open_options_.open_ephemeral_table_reader,
        /*file_open_metadata=*/nullptr, table_cache_open_options_);
  }

  // Check if current file being fully within iterate_lower_bound.
  //
  // Note MyRocks may update iterate bounds between seek. To workaround it,
  // we need to check and update may_be_out_of_lower_bound_ accordingly.
  void CheckMayBeOutOfLowerBound() {
    if (read_options_.iterate_lower_bound != nullptr &&
        file_index_ < flevel_->num_files) {
      may_be_out_of_lower_bound_ =
          user_comparator_.CompareWithoutTimestamp(
              ExtractUserKey(file_smallest_key(file_index_)), /*a_has_ts=*/true,
              *read_options_.iterate_lower_bound, /*b_has_ts=*/false) < 0;
    }
  }

#ifndef NDEBUG
  bool OverlapRange(const ScanOptions& opts, size_t file_index);
#endif

  TableCache* table_cache_;
  const ReadOptions& read_options_;
  const FileOptions& file_options_;
  const InternalKeyComparator& icomparator_;
  const UserComparatorWrapper user_comparator_;
  const LevelFilesBrief* flevel_;
  mutable FileDescriptor current_value_;
  const MutableCFOptions& mutable_cf_options_;
  const SliceTransform* prefix_extractor_;

  HistogramImpl* file_read_hist_;
  TableReaderCaller caller_;
  size_t file_index_;
  RangeDelAggregator* range_del_agg_;
  IteratorWrapper file_iter_;  // May be nullptr
  PinnedIteratorsManager* pinned_iters_mgr_;

  // To be propagated to RangeDelAggregator in order to safely truncate range
  // tombstones.
  const std::vector<AtomicCompactionUnitBoundary>* compaction_boundaries_;

  // This is set when this level iterator is used under a merging iterator
  // that processes range tombstones. range_tombstone_iter_ points to where the
  // merging iterator stores the range tombstones iterator for this level. When
  // this level iterator moves to a new SST file, it updates the range
  // tombstones accordingly through this pointer. So the merging iterator always
  // has access to the current SST file's range tombstones.
  //
  // The level iterator treats file boundary as fake keys (sentinel keys) to
  // keep range tombstones alive if needed and make upper level, i.e. merging
  // iterator, aware of file changes (when level iterator moves to a new SST
  // file, there is some bookkeeping work that needs to be done at merging
  // iterator end).
  //
  // *range_tombstone_iter_ points to range tombstones of the current SST file
  std::unique_ptr<TruncatedRangeDelIterator>* range_tombstone_iter_;
  bool range_tombstone_iter_required_;

  // The sentinel key to be returned
  Slice sentinel_;
  SequenceNumber read_seq_;

  int level_;
  bool skip_filters_;
  bool allow_unprepared_value_;
  bool may_be_out_of_lower_bound_ = true;
  bool is_next_read_sequential_;
  // Set in Seek() when a prefix seek reaches end of the current file,
  // and the next file has a different prefix. SkipEmptyFileForward()
  // will not move to next file when this flag is set.
  bool prefix_exhausted_ = false;
  // Whether next/prev key is a sentinel key.
  bool to_return_sentinel_ = false;
  const MultiScanArgs* scan_opts_ = nullptr;

  Statistics* db_statistics_ = nullptr;
  SystemClock* clock_ = nullptr;
  TableCacheOpenOptions table_cache_open_options_;

  // Our stored scan_opts for each prefix
  std::unique_ptr<ScanOptionsMap> file_to_scan_opts_ = nullptr;

  // Map to store pre-created iterators by file index
  std::unordered_map<size_t, InternalIterator*> prepared_iters_;

  // Sets flags for if we should return the sentinel key next.
  // The condition for returning sentinel is reaching the end of current
  // file_iter_: !Valid() && status.().ok().
  void TrySetDeleteRangeSentinel(const Slice& boundary_key);
  void ClearSentinel() { to_return_sentinel_ = false; }
};

void LevelIterator::TrySetDeleteRangeSentinel(const Slice& boundary_key) {
  assert(range_tombstone_iter_);
  if (file_iter_.iter() != nullptr && !file_iter_.Valid() &&
      file_iter_.status().ok()) {
    to_return_sentinel_ = true;
    sentinel_ = boundary_key;
  }
}

void LevelIterator::Seek(const Slice& target) {
  prefix_exhausted_ = false;
  ClearSentinel();
  // Check whether the seek key fall under the same file
  bool need_to_reseek = true;
  if (file_iter_.iter() != nullptr && file_index_ < flevel_->num_files) {
    const FdWithKeyRange& cur_file = flevel_->files[file_index_];
    if (icomparator_.InternalKeyComparator::Compare(
            target, cur_file.largest_key) <= 0 &&
        icomparator_.InternalKeyComparator::Compare(
            target, cur_file.smallest_key) >= 0) {
      need_to_reseek = false;
      assert(static_cast<size_t>(FindFile(icomparator_, *flevel_, target)) ==
             file_index_);
    }
  }
  if (need_to_reseek) {
    TEST_SYNC_POINT("LevelIterator::Seek:BeforeFindFile");
    size_t new_file_index = FindFile(icomparator_, *flevel_, target);
    InitFileIterator(new_file_index);
  }

  if (file_iter_.iter() != nullptr) {
    if (scan_opts_) {
      // At this point, we only know that the seek target is < largest_key
      // in the file. We need to check whether there is actual overlap.
      const FdWithKeyRange& cur_file = flevel_->files[file_index_];
      if (KeyReachedUpperBound(cur_file.smallest_key)) {
        return;
      }
    }
    file_iter_.Seek(target);
    // Status::TryAgain indicates asynchronous request for retrieval of data
    // blocks has been submitted. So it should return at this point and Seek
    // should be called again to retrieve the requested block and execute the
    // remaining code.
    if (file_iter_.status() == Status::TryAgain()) {
      return;
    }
    if (!file_iter_.Valid() && file_iter_.status().ok() &&
        prefix_extractor_ != nullptr && !read_options_.total_order_seek &&
        !read_options_.auto_prefix_mode &&
        file_index_ < flevel_->num_files - 1) {
      size_t ts_sz = user_comparator_.user_comparator()->timestamp_size();
      Slice target_user_key_without_ts =
          ExtractUserKeyAndStripTimestamp(target, ts_sz);
      Slice next_file_first_user_key_without_ts =
          ExtractUserKeyAndStripTimestamp(file_smallest_key(file_index_ + 1),
                                          ts_sz);
      if (prefix_extractor_->InDomain(target_user_key_without_ts) &&
          (!prefix_extractor_->InDomain(next_file_first_user_key_without_ts) ||
           prefix_extractor_->Transform(target_user_key_without_ts)
                   .compare(prefix_extractor_->Transform(
                       next_file_first_user_key_without_ts)) != 0)) {
        // SkipEmptyFileForward() will not advance to next file when this flag
        // is set for reason detailed below.
        //
        // The file we initially positioned to has no keys under the target
        // prefix, and the next file's smallest key has a different prefix than
        // target. When doing prefix iterator seek, when keys for one prefix
        // have been exhausted, it can jump to any key that is larger. Here we
        // are enforcing a stricter contract than that, in order to make it
        // easier for higher layers (merging and DB iterator) to reason the
        // correctness:
        // 1. Within the prefix, the result should be accurate.
        // 2. If keys for the prefix is exhausted, it is either positioned to
        // the next key after the prefix, or make the iterator invalid.
        // A side benefit will be that it invalidates the iterator earlier so
        // that the upper level merging iterator can merge fewer child
        // iterators.
        //
        // The flag is cleared in Seek*() calls. There is no need to clear the
        // flag in Prev() since Prev() will not be called when the flag is set
        // for reasons explained below. If range_tombstone_iter_ is nullptr,
        // then there is no file boundary sentinel key. Since
        // !file_iter_.Valid() from the if condition above, this level iterator
        // is !Valid(), so Prev() will not be called. If range_tombstone_iter_
        // is not nullptr, there are two cases depending on if this level
        // iterator reaches top of the heap in merging iterator (the upper
        // layer).
        //  If so, merging iterator will see the sentinel key, call
        //  NextAndGetResult() and the call to NextAndGetResult() will skip the
        //  sentinel key and makes this level iterator invalid. If not, then it
        //  could be because the upper layer is done before any method of this
        //  level iterator is called or another Seek*() call is invoked. Either
        //  way, Prev() is never called before Seek*().
        // The flag should not be cleared at the beginning of
        // Next/NextAndGetResult() since it is used in SkipEmptyFileForward()
        // called in Next/NextAndGetResult().
        prefix_exhausted_ = true;
      }
    }

    if (range_tombstone_iter_) {
      TrySetDeleteRangeSentinel(file_largest_key(file_index_));
    }
  }
  SkipEmptyFileForward();
  CheckMayBeOutOfLowerBound();
  SampleRead<true>();
}

void LevelIterator::SeekForPrev(const Slice& target) {
  prefix_exhausted_ = false;
  ClearSentinel();
  size_t new_file_index = FindFile(icomparator_, *flevel_, target);
  // Seek beyond this level's smallest key
  if (new_file_index == 0 &&
      icomparator_.Compare(target, file_smallest_key(0)) < 0) {
    SetFileIterator(nullptr);
    ClearRangeTombstoneIter();
    CheckMayBeOutOfLowerBound();
    return;
  }
  if (new_file_index >= flevel_->num_files) {
    new_file_index = flevel_->num_files - 1;
  }

  InitFileIterator(new_file_index);
  if (file_iter_.iter() != nullptr) {
    file_iter_.SeekForPrev(target);
    if (range_tombstone_iter_ &&
        icomparator_.Compare(target, file_smallest_key(file_index_)) >= 0) {
      // In SeekForPrev() case, it is possible that the target is less than
      // file's lower boundary since largest key is used to determine file index
      // (FindFile()). When target is less than file's lower boundary, sentinel
      // key should not be set so that SeekForPrev() does not result in a key
      // larger than target. This is correct in that there is no need to keep
      // the range tombstones in this file alive as they only cover keys
      // starting from the file's lower boundary, which is after `target`.
      TrySetDeleteRangeSentinel(file_smallest_key(file_index_));
    }
    SkipEmptyFileBackward();
  }
  CheckMayBeOutOfLowerBound();
  SampleRead<true>();
}

void LevelIterator::SeekToFirst() {
  prefix_exhausted_ = false;
  ClearSentinel();
  InitFileIterator(0);
  if (file_iter_.iter() != nullptr) {
    file_iter_.SeekToFirst();
    if (range_tombstone_iter_) {
      // We do this in SeekToFirst() and SeekToLast() since
      // we could have an empty file with only range tombstones.
      TrySetDeleteRangeSentinel(file_largest_key(file_index_));
    }
  }
  SkipEmptyFileForward();
  CheckMayBeOutOfLowerBound();
  SampleRead<true>();
}

void LevelIterator::SeekToLast() {
  prefix_exhausted_ = false;
  ClearSentinel();
  InitFileIterator(flevel_->num_files - 1);
  if (file_iter_.iter() != nullptr) {
    file_iter_.SeekToLast();
    if (range_tombstone_iter_) {
      TrySetDeleteRangeSentinel(file_smallest_key(file_index_));
    }
  }
  SkipEmptyFileBackward();
  CheckMayBeOutOfLowerBound();
  SampleRead<true>();
}

void LevelIterator::Next() {
  assert(Valid());
  if (to_return_sentinel_) {
    // file_iter_ is at EOF already when to_return_sentinel_
    ClearSentinel();
  } else {
    file_iter_.Next();
    if (range_tombstone_iter_) {
      TrySetDeleteRangeSentinel(file_largest_key(file_index_));
    }
  }
  SkipEmptyFileForward();
  SampleRead<false>();
}

bool LevelIterator::NextAndGetResult(IterateResult* result) {
  assert(Valid());
  // file_iter_ is at EOF already when to_return_sentinel_
  bool is_valid = !to_return_sentinel_ && file_iter_.NextAndGetResult(result);
  if (!is_valid) {
    if (to_return_sentinel_) {
      ClearSentinel();
    } else if (range_tombstone_iter_) {
      TrySetDeleteRangeSentinel(file_largest_key(file_index_));
    }
    is_next_read_sequential_ = true;
    SkipEmptyFileForward();
    is_next_read_sequential_ = false;
    is_valid = Valid();
    if (is_valid) {
      // This could be set in TrySetDeleteRangeSentinel() or
      // SkipEmptyFileForward() above.
      if (to_return_sentinel_) {
        result->key = sentinel_;
        result->bound_check_result = IterBoundCheck::kUnknown;
        result->value_prepared = true;
      } else {
        result->key = key();
        result->bound_check_result = file_iter_.UpperBoundCheckResult();
        // Ideally, we should return the real file_iter_.value_prepared but the
        // information is not here. It would casue an extra PrepareValue()
        // for the first key of a file.
        result->value_prepared = !allow_unprepared_value_;
      }
    }
  }
  SampleRead<false>();
  return is_valid;
}

void LevelIterator::Prev() {
  assert(Valid());
  if (to_return_sentinel_) {
    ClearSentinel();
  } else {
    file_iter_.Prev();
    if (range_tombstone_iter_) {
      TrySetDeleteRangeSentinel(file_smallest_key(file_index_));
    }
  }
  SkipEmptyFileBackward();
  SampleRead<false>();
}

bool LevelIterator::SkipEmptyFileForward() {
  bool seen_empty_file = false;
  // Pause at sentinel key
  while (!to_return_sentinel_ &&
         (file_iter_.iter() == nullptr ||
          (!file_iter_.Valid() && file_iter_.status().ok() &&
           file_iter_.iter()->UpperBoundCheckResult() !=
               IterBoundCheck::kOutOfBound))) {
    seen_empty_file = true;
    // Move to next file
    if (file_index_ >= flevel_->num_files - 1 ||
        KeyReachedUpperBound(file_smallest_key(file_index_ + 1)) ||
        prefix_exhausted_) {
      SetFileIterator(nullptr);
      ClearRangeTombstoneIter();
      break;
    }
    // may init a new *range_tombstone_iter
    InitFileIterator(file_index_ + 1);
    // We moved to a new SST file
    // Seek range_tombstone_iter_ to reset its !Valid() default state.
    // We do not need to call range_tombstone_iter_.Seek* in
    // LevelIterator::Seek* since when the merging iterator calls
    // LevelIterator::Seek*, it should also call Seek* into the corresponding
    // range tombstone iterator.
    if (file_iter_.iter() != nullptr) {
      // If we are doing prepared scan opts then we should seek to the values
      // specified by the scan opts

      if (scan_opts_ && FileHasMultiScanArg(file_index_)) {
        const ScanOptions& opts =
            GetMultiScanArgForFile(file_index_).GetScanRanges().front();
        assert(opts.range.start.has_value());
        InternalKey target;
        const size_t ts_size =
            user_comparator_.user_comparator()->timestamp_size();
        if (ts_size == 0) {
          target = InternalKey(opts.range.start.value(), kMaxSequenceNumber,
                               kValueTypeForSeek);
        } else {
          std::string seek_key;
          AppendKeyWithMaxTimestamp(&seek_key, opts.range.start.value(),
                                    ts_size);
          target = InternalKey(seek_key, kMaxSequenceNumber, kValueTypeForSeek);
        }
        file_iter_.Seek(target.Encode());
      } else {
        file_iter_.SeekToFirst();
      }
      if (range_tombstone_iter_) {
        if (*range_tombstone_iter_) {
          (*range_tombstone_iter_)->SeekToFirst();
        }
        TrySetDeleteRangeSentinel(file_largest_key(file_index_));
      }
    }
  }
  return seen_empty_file;
}

void LevelIterator::SkipEmptyFileBackward() {
  // Pause at sentinel key
  while (!to_return_sentinel_ &&
         (file_iter_.iter() == nullptr ||
          (!file_iter_.Valid() && file_iter_.status().ok()))) {
    // Move to previous file
    if (file_index_ == 0) {
      // Already the first file
      SetFileIterator(nullptr);
      ClearRangeTombstoneIter();
      return;
    }
    InitFileIterator(file_index_ - 1);
    // We moved to a new SST file
    // Seek range_tombstone_iter_ to reset its !Valid() default state.
    if (file_iter_.iter() != nullptr) {
      file_iter_.SeekToLast();
      if (range_tombstone_iter_) {
        if (*range_tombstone_iter_) {
          (*range_tombstone_iter_)->SeekToLast();
        }
        TrySetDeleteRangeSentinel(file_smallest_key(file_index_));
        if (to_return_sentinel_) {
          break;
        }
      }
    }
  }
}

#ifndef NDEBUG
bool LevelIterator::OverlapRange(const ScanOptions& opts, size_t file_index) {
  return MultiScanRangeOverlapsFile(user_comparator_, opts,
                                    flevel_->files[file_index]);
}
#endif

void LevelIterator::SetFileIterator(InternalIterator* iter) {
  if (pinned_iters_mgr_ && iter) {
    iter->SetPinnedItersMgr(pinned_iters_mgr_);
  }

  InternalIterator* old_iter = file_iter_.Set(iter);
  // Update the read pattern for PrefetchBuffer.
  if (is_next_read_sequential_) {
    file_iter_.UpdateReadaheadState(old_iter);
  }

  if (pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
      old_iter != nullptr && !old_iter->IsSafeToDropWhilePinned()) {
    TEST_SYNC_POINT_CALLBACK("LevelIterator::SetFileIterator:PinIterator",
                             old_iter);
    pinned_iters_mgr_->PinIterator(old_iter);
  } else {
    TEST_SYNC_POINT_CALLBACK("LevelIterator::SetFileIterator:DeleteIterator",
                             old_iter);
    delete old_iter;
  }
}

void LevelIterator::InitFileIterator(size_t new_file_index) {
  if (new_file_index >= flevel_->num_files) {
    file_index_ = new_file_index;
    SetFileIterator(nullptr);
    ClearRangeTombstoneIter();
    return;
  } else {
    // If the file iterator shows incomplete, we try it again if users seek
    // to the same file, as this time we may go to a different data block
    // which is cached in block cache.
    //
    if (file_iter_.iter() != nullptr && !file_iter_.status().IsIncomplete() &&
        new_file_index == file_index_) {
      // file_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      file_index_ = new_file_index;
      if (!prepared_iters_.empty()) {
        auto prepared_it = prepared_iters_.find(file_index_);
        if (prepared_it != prepared_iters_.end()) {
          InternalIterator* iter = prepared_it->second;
          prepared_iters_.erase(prepared_it);
          SetFileIterator(iter);
          return;
        }
      }

      InternalIterator* iter = NewFileIterator();
      if (FileHasMultiScanArg(file_index_)) {
        auto& args = GetMultiScanArgForFile(file_index_);
        assert(OverlapRange(*args.GetScanRanges().begin(), file_index_) &&
               OverlapRange(*args.GetScanRanges().rbegin(), file_index_));
        iter->Prepare(&args);
      }

      SetFileIterator(iter);
    }
  }
}

}  // anonymous namespace

Status Version::GetTableProperties(const ReadOptions& read_options,
                                   std::shared_ptr<const TableProperties>* tp,
                                   const FileMetaData* file_meta,
                                   const std::string* fname) const {
  auto* table_cache = cfd_->table_cache();
  const auto& ioptions = cfd_->ioptions();
  Status s = table_cache->GetTableProperties(
      file_options_, read_options, cfd_->internal_comparator(), *file_meta, tp,
      mutable_cf_options_, true /* no io */);
  if (s.ok()) {
    return s;
  }

  // We only ignore error type `Incomplete` since it's by design that we
  // disallow table when it's not in table cache.
  if (!s.IsIncomplete()) {
    return s;
  }

  // 2. Table is not present in table cache, we'll read the table properties
  // directly from the properties block in the file.
  std::unique_ptr<FSRandomAccessFile> file;
  std::string file_name;
  if (fname != nullptr) {
    file_name = *fname;
  } else {
    file_name = TableFileName(ioptions.cf_paths, file_meta->fd.GetNumber(),
                              file_meta->fd.GetPathId());
  }
  FileOptions fopts = file_options_;
  fopts.file_checksum = file_meta->file_checksum;
  fopts.file_checksum_func_name = file_meta->file_checksum_func_name;
  s = ioptions.fs->NewRandomAccessFile(file_name, fopts, &file, nullptr);
  if (!s.ok()) {
    return s;
  }

  // By setting the magic number to kNullTableMagicNumber, we can bypass
  // the magic number check in the footer.
  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(
          std::move(file), file_name, ioptions.clock /* clock */, io_tracer_,
          ioptions.stats /* stats */,
          Histograms::SST_READ_MICROS /* hist_type */,
          nullptr /* file_read_hist */, nullptr /* rate_limiter */,
          ioptions.listeners));
  std::unique_ptr<TableProperties> props;
  s = ReadTableProperties(
      file_reader.get(), file_meta->fd.GetFileSize(),
      Footer::kNullTableMagicNumber /* table's magic number */, ioptions,
      read_options, &props);
  if (!s.ok()) {
    return s;
  }
  *tp = std::move(props);
  RecordTick(ioptions.stats, NUMBER_DIRECT_LOAD_TABLE_PROPERTIES);
  return s;
}

Status Version::GetPropertiesOfAllTables(
    const ReadOptions& read_options, TablePropertiesCollection* props) const {
  Status s;
  for (int level = 0; level < storage_info_.num_levels_; level++) {
    s = GetPropertiesOfAllTables(read_options, props, level);
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

Status Version::TablesRangeTombstoneSummary(int max_entries_to_print,
                                            std::string* out_str) {
  if (max_entries_to_print <= 0) {
    return Status::OK();
  }
  int num_entries_left = max_entries_to_print;

  std::stringstream ss;

  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;
  for (int level = 0; level < storage_info_.num_levels_; level++) {
    for (const auto& file_meta : storage_info_.files_[level]) {
      auto fname =
          TableFileName(cfd_->ioptions().cf_paths, file_meta->fd.GetNumber(),
                        file_meta->fd.GetPathId());

      ss << "=== file : " << fname << " ===\n";

      TableCache* table_cache = cfd_->table_cache();
      std::unique_ptr<FragmentedRangeTombstoneIterator> tombstone_iter;

      Status s = table_cache->GetRangeTombstoneIterator(
          read_options, cfd_->internal_comparator(), *file_meta,
          mutable_cf_options_, &tombstone_iter);
      if (!s.ok()) {
        return s;
      }
      if (tombstone_iter) {
        tombstone_iter->SeekToFirst();

        // TODO: print timestamp
        while (tombstone_iter->Valid() && num_entries_left > 0) {
          ss << "start: " << tombstone_iter->start_key().ToString(true)
             << " end: " << tombstone_iter->end_key().ToString(true)
             << " seq: " << tombstone_iter->seq() << '\n';
          tombstone_iter->Next();
          num_entries_left--;
        }
        if (num_entries_left <= 0) {
          break;
        }
      }
    }
    if (num_entries_left <= 0) {
      break;
    }
  }
  assert(num_entries_left >= 0);
  if (num_entries_left <= 0) {
    ss << "(results may not be complete)\n";
  }

  *out_str = ss.str();
  return Status::OK();
}

Status Version::GetPropertiesOfAllTables(const ReadOptions& read_options,
                                         TablePropertiesCollection* props,
                                         int level) const {
  for (const auto& file_meta : storage_info_.files_[level]) {
    auto fname =
        TableFileName(cfd_->ioptions().cf_paths, file_meta->fd.GetNumber(),
                      file_meta->fd.GetPathId());
    // 1. If the table is already present in table cache, load table
    // properties from there.
    std::shared_ptr<const TableProperties> table_properties;
    Status s =
        GetTableProperties(read_options, &table_properties, file_meta, &fname);
    if (s.ok()) {
      props->insert({fname, table_properties});
    } else {
      return s;
    }
  }

  return Status::OK();
}

Status Version::GetPropertiesOfTablesInRange(
    const ReadOptions& read_options, const autovector<UserKeyRange>& ranges,
    TablePropertiesCollection* props) const {
  for (int level = 0; level < storage_info_.num_non_empty_levels(); level++) {
    for (const auto& range : ranges) {
      // Convert user_key into a corresponding internal key.
      InternalKey k1(range.start, kMaxSequenceNumber, kValueTypeForSeek);
      InternalKey k2(range.limit, kMaxSequenceNumber, kValueTypeForSeek);
      std::vector<FileMetaData*> files;
      storage_info_.GetOverlappingInputs(level, &k1, &k2, &files, -1, nullptr,
                                         false);
      for (const auto& file_meta : files) {
        auto fname =
            TableFileName(cfd_->ioptions().cf_paths, file_meta->fd.GetNumber(),
                          file_meta->fd.GetPathId());
        if (props->count(fname) == 0) {
          // 1. If the table is already present in table cache, load table
          // properties from there.
          std::shared_ptr<const TableProperties> table_properties;
          Status s = GetTableProperties(read_options, &table_properties,
                                        file_meta, &fname);
          if (s.ok()) {
            props->insert({fname, table_properties});
          } else {
            return s;
          }
        }
      }
    }
  }

  return Status::OK();
}

Status Version::GetPropertiesOfTablesByLevel(
    const ReadOptions& read_options,
    std::vector<std::unique_ptr<TablePropertiesCollection>>* props_by_level)
    const {
  Status s;

  props_by_level->reserve(storage_info_.num_levels_);
  for (int level = 0; level < storage_info_.num_levels_; level++) {
    props_by_level->push_back(std::make_unique<TablePropertiesCollection>());
    s = GetPropertiesOfAllTables(read_options, props_by_level->back().get(),
                                 level);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status Version::GetAggregatedTableProperties(
    const ReadOptions& read_options, std::shared_ptr<const TableProperties>* tp,
    int level) {
  TablePropertiesCollection props;
  Status s;
  if (level < 0) {
    s = GetPropertiesOfAllTables(read_options, &props);
  } else {
    s = GetPropertiesOfAllTables(read_options, &props, level);
  }
  if (!s.ok()) {
    return s;
  }

  auto* new_tp = new TableProperties();
  for (const auto& item : props) {
    new_tp->Add(*item.second);
  }
  tp->reset(new_tp);
  return Status::OK();
}

size_t Version::GetMemoryUsageByTableReaders(const ReadOptions& read_options) {
  size_t total_usage = 0;
  for (auto& file_level : storage_info_.level_files_brief_) {
    for (size_t i = 0; i < file_level.num_files; i++) {
      total_usage += cfd_->table_cache()->GetMemoryUsageByTableReader(
          file_options_, read_options, cfd_->internal_comparator(),
          *file_level.files[i].file_metadata, mutable_cf_options_);
    }
  }
  return total_usage;
}

void Version::GetColumnFamilyMetaData(ColumnFamilyMetaData* cf_meta) {
  assert(cf_meta);
  assert(cfd_);

  cf_meta->name = cfd_->GetName();
  cf_meta->size = 0;
  cf_meta->file_count = 0;
  cf_meta->levels.clear();

  cf_meta->blob_file_size = 0;
  cf_meta->blob_file_count = 0;
  cf_meta->blob_files.clear();

  const auto& ioptions = cfd_->ioptions();
  auto* vstorage = storage_info();

  for (int level = 0; level < cfd_->NumberLevels(); level++) {
    uint64_t level_size = 0;
    cf_meta->file_count += vstorage->LevelFiles(level).size();
    std::vector<SstFileMetaData> files;
    for (const auto& file : vstorage->LevelFiles(level)) {
      uint32_t path_id = file->fd.GetPathId();
      std::string file_path;
      if (path_id < ioptions.cf_paths.size()) {
        file_path = ioptions.cf_paths[path_id].path;
      } else {
        assert(!ioptions.cf_paths.empty());
        file_path = ioptions.cf_paths.back().path;
      }
      const uint64_t file_number = file->fd.GetNumber();
      files.emplace_back(
          MakeTableFileName("", file_number), file_number, file_path,
          file->fd.GetFileSize(), file->fd.smallest_seqno,
          file->fd.largest_seqno, file->smallest.user_key().ToString(),
          file->largest.user_key().ToString(),
          file->stats.num_reads_sampled.load(std::memory_order_relaxed),
          file->being_compacted, file->temperature,
          file->oldest_blob_file_number, file->TryGetOldestAncestorTime(),
          file->TryGetFileCreationTime(), file->epoch_number,
          file->file_checksum, file->file_checksum_func_name);
      files.back().num_entries = file->num_entries;
      files.back().num_deletions = file->num_deletions;
      files.back().smallest = file->smallest.Encode().ToString();
      files.back().largest = file->largest.Encode().ToString();
      level_size += file->fd.GetFileSize();
    }
    cf_meta->levels.emplace_back(level, level_size, std::move(files));
    cf_meta->size += level_size;
  }
  for (const auto& meta : vstorage->GetBlobFiles()) {
    assert(meta);

    cf_meta->blob_files.emplace_back(
        meta->GetBlobFileNumber(), BlobFileName("", meta->GetBlobFileNumber()),
        ioptions.cf_paths.front().path, meta->GetBlobFileSize(),
        meta->GetTotalBlobCount(), meta->GetTotalBlobBytes(),
        meta->GetGarbageBlobCount(), meta->GetGarbageBlobBytes(),
        meta->GetChecksumMethod(), meta->GetChecksumValue());
    ++cf_meta->blob_file_count;
    cf_meta->blob_file_size += meta->GetBlobFileSize();
  }
}

void Version::GetColumnFamilyMetaData(
    const GetColumnFamilyMetaDataOptions& options,
    ColumnFamilyMetaData* cf_meta) {
  assert(cf_meta);
  assert(cfd_);

  cf_meta->name = cfd_->GetName();
  cf_meta->size = 0;
  cf_meta->file_count = 0;
  cf_meta->levels.clear();
  cf_meta->blob_file_size = 0;
  cf_meta->blob_file_count = 0;
  cf_meta->blob_files.clear();

  const auto& ioptions = cfd_->ioptions();
  auto* vstorage = storage_info();

  int first_level = (options.level >= 0) ? options.level : 0;
  int last_level =
      (options.level >= 0) ? options.level + 1 : cfd_->NumberLevels();

  InternalKey ikey_start;
  InternalKey ikey_end;
  const InternalKey* begin = nullptr;
  const InternalKey* end = nullptr;

  if (options.range.start.has_value()) {
    ikey_start = InternalKey(options.range.start.value(), kMaxSequenceNumber,
                             kValueTypeForSeek);
    begin = &ikey_start;
  }

  if (options.range.limit.has_value()) {
    ikey_end = InternalKey(options.range.limit.value(), kMaxSequenceNumber,
                           kValueTypeForSeek);
    end = &ikey_end;
  }

  for (int l = first_level; l < last_level; ++l) {
    uint64_t level_size = 0;
    std::vector<SstFileMetaData> files;
    std::vector<FileMetaData*> overlapping_files;
    vstorage->GetOverlappingInputs(l, begin, end, &overlapping_files);

    for (const auto& file : overlapping_files) {
      uint32_t path_id = file->fd.GetPathId();
      const auto& file_path = (path_id < ioptions.cf_paths.size())
                                  ? ioptions.cf_paths[path_id].path
                                  : ioptions.cf_paths.back().path;
      const uint64_t file_number = file->fd.GetNumber();
      files.emplace_back(
          MakeTableFileName("", file_number), file_number, file_path,
          file->fd.GetFileSize(), file->fd.smallest_seqno,
          file->fd.largest_seqno, file->smallest.user_key().ToString(),
          file->largest.user_key().ToString(),
          file->stats.num_reads_sampled.load(std::memory_order_relaxed),
          file->being_compacted, file->temperature,
          file->oldest_blob_file_number, file->TryGetOldestAncestorTime(),
          file->TryGetFileCreationTime(), file->epoch_number,
          file->file_checksum, file->file_checksum_func_name);
      files.back().num_entries = file->num_entries;
      files.back().num_deletions = file->num_deletions;
      files.back().smallest = file->smallest.Encode().ToString();
      files.back().largest = file->largest.Encode().ToString();
      level_size += file->fd.GetFileSize();
      cf_meta->file_count++;
    }
    if (!files.empty()) {
      cf_meta->levels.emplace_back(l, level_size, std::move(files));
      cf_meta->size += level_size;
    }
  }
}

uint64_t Version::GetSstFilesSize() {
  uint64_t sst_files_size = 0;
  for (int level = 0; level < storage_info_.num_levels_; level++) {
    for (const auto& file_meta : storage_info_.LevelFiles(level)) {
      sst_files_size += file_meta->fd.GetFileSize();
    }
  }
  return sst_files_size;
}

void Version::GetSstFilesBoundaryKeys(Slice* smallest_user_key,
                                      Slice* largest_user_key) {
  smallest_user_key->clear();
  largest_user_key->clear();
  bool initialized = false;
  const Comparator* ucmp = storage_info_.user_comparator_;
  for (int level = 0; level < cfd_->NumberLevels(); level++) {
    if (storage_info_.LevelFiles(level).size() == 0) {
      continue;
    }
    if (level == 0) {
      // we need to consider all files on level 0
      for (const auto& file : storage_info_.LevelFiles(level)) {
        const Slice& start_user_key = file->smallest.user_key();
        if (!initialized ||
            ucmp->Compare(start_user_key, *smallest_user_key) < 0) {
          *smallest_user_key = start_user_key;
        }
        const Slice& end_user_key = file->largest.user_key();
        if (!initialized ||
            ucmp->Compare(end_user_key, *largest_user_key) > 0) {
          *largest_user_key = end_user_key;
        }
        initialized = true;
      }
    } else {
      // we only need to consider the first and last file
      const Slice& start_user_key =
          storage_info_.LevelFiles(level)[0]->smallest.user_key();
      if (!initialized ||
          ucmp->Compare(start_user_key, *smallest_user_key) < 0) {
        *smallest_user_key = start_user_key;
      }
      const Slice& end_user_key =
          storage_info_.LevelFiles(level).back()->largest.user_key();
      if (!initialized || ucmp->Compare(end_user_key, *largest_user_key) > 0) {
        *largest_user_key = end_user_key;
      }
      initialized = true;
    }
  }
}

void Version::GetCreationTimeOfOldestFile(uint64_t* creation_time) {
  uint64_t oldest_time = std::numeric_limits<uint64_t>::max();
  for (int level = 0; level < storage_info_.num_non_empty_levels_; level++) {
    for (FileMetaData* meta : storage_info_.LevelFiles(level)) {
      uint64_t file_creation_time = meta->TryGetFileCreationTime();
      TEST_SYNC_POINT_CALLBACK(
          "Version::GetCreationTimeOfOldestFile::FileCreationTime",
          &file_creation_time);
      if (file_creation_time == kUnknownFileCreationTime) {
        *creation_time = 0;
        return;
      }
      if (file_creation_time < oldest_time) {
        oldest_time = file_creation_time;
      }
    }
  }
  *creation_time = oldest_time;
}

InternalIterator* Version::TEST_GetLevelIterator(
    const ReadOptions& read_options, MergeIteratorBuilder* merge_iter_builder,
    int level, bool allow_unprepared_value) {
  auto* arena = merge_iter_builder->GetArena();
  auto* mem = arena->AllocateAligned(sizeof(LevelIterator));
  std::unique_ptr<TruncatedRangeDelIterator>** tombstone_iter_ptr = nullptr;
  auto level_iter = new (mem) LevelIterator(
      cfd_->table_cache(), read_options, file_options_,
      cfd_->internal_comparator(), &storage_info_.LevelFilesBrief(level),
      mutable_cf_options_, cfd_->internal_stats()->GetFileReadHist(level),
      TableReaderCaller::kUserIterator, IsFilterSkipped(level), level,
      nullptr /* range_del_agg */, nullptr /* compaction_boundaries */,
      allow_unprepared_value,
      read_options.ignore_range_deletions ? nullptr : &tombstone_iter_ptr,
      kMaxSequenceNumber, db_statistics_, clock_);
  if (read_options.ignore_range_deletions) {
    merge_iter_builder->AddIterator(level_iter);
  } else {
    merge_iter_builder->AddPointAndTombstoneIterator(
        level_iter, nullptr /* tombstone_iter */, tombstone_iter_ptr);
  }
  return level_iter;
}

uint64_t VersionStorageInfo::GetEstimatedActiveKeys() const {
  // Estimation will be inaccurate when:
  // (1) there exist merge keys
  // (2) keys are directly overwritten
  // (3) deletion on non-existing keys
  // (4) low number of samples
  if (current_num_samples_ == 0) {
    return 0;
  }

  if (current_num_non_deletions_ <= current_num_deletions_) {
    return 0;
  }

  uint64_t est = current_num_non_deletions_ - current_num_deletions_;

  uint64_t file_count = 0;
  for (int level = 0; level < num_levels_; ++level) {
    file_count += files_[level].size();
  }

  if (current_num_samples_ < file_count) {
    assert(current_num_samples_ != 0);
    assert(est != 0);
    double multiplier = static_cast<double>(file_count) / current_num_samples_;
    double maximum_multiplier =
        static_cast<double>(std::numeric_limits<uint64_t>::max()) / est;
    // If it can overflow, we return the maximum unsigned long.
    if (multiplier >= maximum_multiplier) {
      return std::numeric_limits<uint64_t>::max();
    }
    return static_cast<uint64_t>(est * multiplier);
  } else {
    return est;
  }
}

double VersionStorageInfo::GetEstimatedCompressionRatioAtLevel(
    int level) const {
  assert(level < num_levels_);
  uint64_t sum_file_size_bytes = 0;
  uint64_t sum_data_size_bytes = 0;
  for (auto* file_meta : files_[level]) {
    auto raw_size = file_meta->raw_key_size + file_meta->raw_value_size;
    // Check if the table property is properly initialized. It might not be
    // because in `UpdateAccumulatedStats` we limit the maximum number of
    // properties to read once.
    if (raw_size > 0) {
      sum_file_size_bytes += file_meta->fd.GetFileSize();
      sum_data_size_bytes += raw_size;
    }
  }
  if (sum_file_size_bytes == 0) {
    return -1.0;
  }
  return static_cast<double>(sum_data_size_bytes) / sum_file_size_bytes;
}

void Version::AddIterators(const ReadOptions& read_options,
                           const FileOptions& soptions,
                           MergeIteratorBuilder* merge_iter_builder,
                           bool allow_unprepared_value, SequenceNumber read_seq,
                           const MultiScanArgs* scan_opts) {
  assert(storage_info_.finalized_);

  for (int level = 0; level < storage_info_.num_non_empty_levels(); level++) {
    AddIteratorsForLevel(read_options, soptions, merge_iter_builder, level,
                         allow_unprepared_value, read_seq, scan_opts);
  }
}

void Version::AddIteratorsForLevel(const ReadOptions& read_options,
                                   const FileOptions& soptions,
                                   MergeIteratorBuilder* merge_iter_builder,
                                   int level, bool allow_unprepared_value,
                                   SequenceNumber read_seq,
                                   const MultiScanArgs* scan_opts) {
  assert(storage_info_.finalized_);
  if (level >= storage_info_.num_non_empty_levels()) {
    // This is an empty level
    return;
  } else if (storage_info_.LevelFilesBrief(level).num_files == 0) {
    // No files in this level
    return;
  }

  auto* arena = merge_iter_builder->GetArena();
  const LevelFilesBrief& file_level = storage_info_.LevelFilesBrief(level);
  const UserComparatorWrapper user_comparator(
      cfd_->internal_comparator().user_comparator());

  if (scan_opts != nullptr) {
    const std::vector<size_t> file_indexes = GetMultiScanOverlappingFiles(
        cfd_->internal_comparator(), user_comparator, file_level, level,
        *scan_opts, level == 0 ? std::numeric_limits<size_t>::max() : 2);
    if (file_indexes.empty()) {
      return;
    }

    if (level == 0) {
      const bool should_sample = should_sample_file_read();
      for (size_t file_index : file_indexes) {
        AddTableIteratorForLevel(
            cfd_, read_options, soptions, mutable_cf_options_,
            merge_iter_builder, arena, file_level, level, file_index,
            /*skip_filters=*/false, max_file_size_for_l0_meta_pin_,
            allow_unprepared_value, read_seq);
        if (should_sample) {
          sample_file_read_inc(file_level.files[file_index].file_metadata);
        }
      }
      return;
    }

    if (file_indexes.size() == 1) {
      AddTableIteratorForLevel(cfd_, read_options, soptions,
                               mutable_cf_options_, merge_iter_builder, arena,
                               file_level, level, file_indexes.front(),
                               IsFilterSkipped(level),
                               /*max_file_size_for_l0_meta_pin=*/0,
                               allow_unprepared_value, read_seq);
      return;
    }

#ifndef NDEBUG
    for (size_t file_index = 0; file_index < file_level.num_files;
         ++file_index) {
      TEST_SYNC_POINT_CALLBACK("Version::AddIteratorsForLevel:AddedFile",
                               file_level.files[file_index].file_metadata);
    }
#endif
    auto* mem = arena->AllocateAligned(sizeof(LevelIterator));
    std::unique_ptr<TruncatedRangeDelIterator>** tombstone_iter_ptr = nullptr;
    auto level_iter = new (mem) LevelIterator(
        cfd_->table_cache(), read_options, soptions,
        cfd_->internal_comparator(), &file_level, mutable_cf_options_,
        cfd_->internal_stats()->GetFileReadHist(level),
        TableReaderCaller::kUserIterator, IsFilterSkipped(level), level,
        /*range_del_agg=*/nullptr,
        /*compaction_boundaries=*/nullptr, allow_unprepared_value,
        read_options.ignore_range_deletions ? nullptr : &tombstone_iter_ptr,
        read_seq, db_statistics_, clock_);
#ifndef NDEBUG
    std::pair<bool, bool> iterator_type(
        false /* is_block_based_table_iterator */,
        true /* is_level_iterator */);
    TEST_SYNC_POINT_CALLBACK("Version::AddIteratorsForLevel:IteratorType",
                             &iterator_type);
#endif
    if (read_options.ignore_range_deletions) {
      merge_iter_builder->AddIterator(level_iter);
    } else {
      assert(tombstone_iter_ptr);
      merge_iter_builder->AddPointAndTombstoneIterator(
          level_iter, nullptr /* tombstone_iter */, tombstone_iter_ptr);
    }
    return;
  }

  if (level == 0) {
    // Merge all level zero files together since they may overlap
    const bool should_sample = should_sample_file_read();
    for (size_t i = 0; i < file_level.num_files; i++) {
      AddTableIteratorForLevel(
          cfd_, read_options, soptions, mutable_cf_options_, merge_iter_builder,
          arena, file_level, level, i,
          /*skip_filters=*/false, max_file_size_for_l0_meta_pin_,
          allow_unprepared_value, read_seq);
      if (should_sample) {
        // Count once for every L0 file. This is done per iterator creation
        // rather than Seek(), while files in other levels are sampled on
        // seek/next/prev.
        sample_file_read_inc(file_level.files[i].file_metadata);
      }
    }
  } else if (file_level.num_files > 0) {
    // For levels > 0, we can use a concatenating iterator that sequentially
    // walks through the non-overlapping files in the level, opening them
    // lazily.
    auto* mem = arena->AllocateAligned(sizeof(LevelIterator));
    std::unique_ptr<TruncatedRangeDelIterator>** tombstone_iter_ptr = nullptr;
    auto level_iter = new (mem) LevelIterator(
        cfd_->table_cache(), read_options, soptions,
        cfd_->internal_comparator(), &file_level, mutable_cf_options_,
        cfd_->internal_stats()->GetFileReadHist(level),
        TableReaderCaller::kUserIterator, IsFilterSkipped(level), level,
        /*range_del_agg=*/nullptr,
        /*compaction_boundaries=*/nullptr, allow_unprepared_value,
        read_options.ignore_range_deletions ? nullptr : &tombstone_iter_ptr,
        read_seq, db_statistics_, clock_);
    if (read_options.ignore_range_deletions) {
      merge_iter_builder->AddIterator(level_iter);
    } else {
      merge_iter_builder->AddPointAndTombstoneIterator(
          level_iter, nullptr /* tombstone_iter */, tombstone_iter_ptr);
    }
  }
}

Status Version::OverlapWithLevelIterator(const ReadOptions& read_options,
                                         const FileOptions& file_options,
                                         const Slice& smallest_user_key,
                                         const Slice& largest_user_key,
                                         int level, bool* overlap) {
  assert(storage_info_.finalized_);

  auto icmp = cfd_->internal_comparator();
  auto ucmp = icmp.user_comparator();

  Arena arena;
  Status status;
  ReadRangeDelAggregator range_del_agg(&icmp,
                                       kMaxSequenceNumber /* upper_bound */);

  *overlap = false;

  if (level == 0) {
    for (size_t i = 0; i < storage_info_.LevelFilesBrief(0).num_files; i++) {
      const auto file = &storage_info_.LevelFilesBrief(0).files[i];
      if (AfterFile(ucmp, &smallest_user_key, file) ||
          BeforeFile(ucmp, &largest_user_key, file)) {
        continue;
      }
      ScopedArenaPtr<InternalIterator> iter(cfd_->table_cache()->NewIterator(
          read_options, file_options, cfd_->internal_comparator(),
          *file->file_metadata, &range_del_agg, mutable_cf_options_, nullptr,
          cfd_->internal_stats()->GetFileReadHist(0),
          TableReaderCaller::kUserIterator, &arena,
          /*skip_filters=*/false, /*level=*/0, max_file_size_for_l0_meta_pin_,
          /*smallest_compaction_key=*/nullptr,
          /*largest_compaction_key=*/nullptr,
          /*allow_unprepared_value=*/false));
      status = OverlapWithIterator(ucmp, smallest_user_key, largest_user_key,
                                   iter.get(), overlap);
      if (!status.ok() || *overlap) {
        break;
      }
    }
  } else if (storage_info_.LevelFilesBrief(level).num_files > 0) {
    auto mem = arena.AllocateAligned(sizeof(LevelIterator));
    ScopedArenaPtr<InternalIterator> iter(new (mem) LevelIterator(
        cfd_->table_cache(), read_options, file_options,
        cfd_->internal_comparator(), &storage_info_.LevelFilesBrief(level),
        mutable_cf_options_, cfd_->internal_stats()->GetFileReadHist(level),
        TableReaderCaller::kUserIterator, IsFilterSkipped(level), level,
        &range_del_agg, nullptr, false, nullptr, kMaxSequenceNumber,
        db_statistics_, clock_));
    status = OverlapWithIterator(ucmp, smallest_user_key, largest_user_key,
                                 iter.get(), overlap);
  }

  if (status.ok() && *overlap == false &&
      range_del_agg.IsRangeOverlapped(smallest_user_key, largest_user_key,
                                      /*end_exclusive=*/false)) {
    *overlap = true;
  }
  return status;
}

VersionStorageInfo::VersionStorageInfo(
    const InternalKeyComparator* internal_comparator,
    const Comparator* user_comparator, int levels,
    CompactionStyle compaction_style, VersionStorageInfo* ref_vstorage,
    bool _force_consistency_checks,
    EpochNumberRequirement epoch_number_requirement, SystemClock* clock,
    uint32_t bottommost_file_compaction_delay,
    OffpeakTimeOption offpeak_time_option,
    PeriodicCompactionPhaseParams periodic_compaction_phase_params)
    : internal_comparator_(internal_comparator),
      user_comparator_(user_comparator),
      // cfd is nullptr if Version is dummy
      num_levels_(levels),
      num_non_empty_levels_(0),
      file_indexer_(user_comparator),
      compaction_style_(compaction_style),
      files_(new std::vector<FileMetaData*>[num_levels_]),
      base_level_(num_levels_ == 1 ? -1 : 1),
      lowest_unnecessary_level_(-1),
      level_multiplier_(0.0),
      files_by_compaction_pri_(num_levels_),
      level0_non_overlapping_(false),
      next_file_to_compact_by_size_(num_levels_),
      compaction_score_(num_levels_),
      compaction_level_(num_levels_),
      l0_delay_trigger_count_(0),
      compact_cursor_(num_levels_),
      accumulated_file_size_(0),
      accumulated_raw_key_size_(0),
      accumulated_raw_value_size_(0),
      accumulated_num_non_deletions_(0),
      accumulated_num_deletions_(0),
      current_num_non_deletions_(0),
      current_num_deletions_(0),
      current_num_samples_(0),
      estimated_compaction_needed_bytes_(0),
      clock_(clock),
      bottommost_file_compaction_delay_(bottommost_file_compaction_delay),
      finalized_(false),
      force_consistency_checks_(_force_consistency_checks),
      epoch_number_requirement_(epoch_number_requirement),
      offpeak_time_option_(std::move(offpeak_time_option)),
      periodic_compaction_phase_params_(periodic_compaction_phase_params) {
  if (ref_vstorage != nullptr) {
    accumulated_file_size_ = ref_vstorage->accumulated_file_size_;
    accumulated_raw_key_size_ = ref_vstorage->accumulated_raw_key_size_;
    accumulated_raw_value_size_ = ref_vstorage->accumulated_raw_value_size_;
    accumulated_num_non_deletions_ =
        ref_vstorage->accumulated_num_non_deletions_;
    accumulated_num_deletions_ = ref_vstorage->accumulated_num_deletions_;
    current_num_non_deletions_ = ref_vstorage->current_num_non_deletions_;
    current_num_deletions_ = ref_vstorage->current_num_deletions_;
    current_num_samples_ = ref_vstorage->current_num_samples_;
    oldest_snapshot_seqnum_ = ref_vstorage->oldest_snapshot_seqnum_;
    preserve_time_min_seqno_ = ref_vstorage->preserve_time_min_seqno_;
    compact_cursor_ = ref_vstorage->compact_cursor_;
    compact_cursor_.resize(num_levels_);
  }
}

Version::Version(ColumnFamilyData* column_family_data, VersionSet* vset,
                 const FileOptions& file_opt,
                 const MutableCFOptions& mutable_cf_options,
                 const std::shared_ptr<IOTracer>& io_tracer,
                 uint64_t version_number,
                 EpochNumberRequirement epoch_number_requirement)
    : env_(vset->env_),
      clock_(vset->clock_),
      cfd_(column_family_data),
      info_log_((cfd_ == nullptr) ? nullptr : cfd_->ioptions().logger),
      db_statistics_((cfd_ == nullptr) ? nullptr : cfd_->ioptions().stats),
      table_cache_((cfd_ == nullptr) ? nullptr : cfd_->table_cache()),
      blob_source_(cfd_ ? cfd_->blob_source() : nullptr),
      merge_operator_(
          (cfd_ == nullptr) ? nullptr : cfd_->ioptions().merge_operator.get()),
      storage_info_(
          (cfd_ == nullptr) ? nullptr : &cfd_->internal_comparator(),
          (cfd_ == nullptr) ? nullptr : cfd_->user_comparator(),
          cfd_ == nullptr ? 0 : cfd_->NumberLevels(),
          cfd_ == nullptr ? kCompactionStyleLevel
                          : cfd_->ioptions().compaction_style,
          (cfd_ == nullptr || cfd_->current() == nullptr)
              ? nullptr
              : cfd_->current()->storage_info(),
          cfd_ == nullptr ? false : cfd_->ioptions().force_consistency_checks,
          epoch_number_requirement,
          cfd_ == nullptr ? nullptr : cfd_->ioptions().clock,
          cfd_ == nullptr ? 0
                          : mutable_cf_options.bottommost_file_compaction_delay,
          vset->offpeak_time_option(),
          cfd_ == nullptr
              ? PeriodicCompactionPhaseParams{}
              : vset->GetPeriodicCompactionPhaseParams(cfd_->GetID())),
      vset_(vset),
      next_(this),
      prev_(this),
      refs_(0),
      file_options_(file_opt),
      mutable_cf_options_(mutable_cf_options),
      max_file_size_for_l0_meta_pin_(
          MaxFileSizeForL0MetaPin(mutable_cf_options_)),
      version_number_(version_number),
      io_tracer_(io_tracer) {}

Status Version::GetBlob(const ReadOptions& read_options, const Slice& user_key,
                        const BlobIndex& blob_index,
                        FilePrefetchBuffer* prefetch_buffer,
                        PinnableSlice* value, uint64_t* bytes_read) const {
  assert(value);

  if (blob_index.HasTTL() || blob_index.IsInlined()) {
    return Status::Corruption("Unexpected TTL/inlined blob index");
  }

  const uint64_t blob_file_number = blob_index.file_number();

  auto blob_file_meta = storage_info_.GetBlobFileMetaData(blob_file_number);
  if (!blob_file_meta) {
    // INTEGRITY CHECK -- do not weaken. The blob index must reference a known
    // external blob file. No metadata (including file_number 0, the
    // kInvalidBlobFileNumber / same-file "embedded" sentinel) means a corrupt
    // index, or a same-file reference that should have been resolved by
    // EmbeddedBlobResolvingIterator before reaching the integrated-BlobDB read
    // path. See FileMetaData::UpdateBoundaries for the write-side tripwire.
    return Status::Corruption("Invalid blob file number");
  }

  assert(blob_source_);
  value->Reset();
  const BlobFileOpenInfo blob_file{blob_file_number,
                                   blob_file_meta->GetChecksumValue(),
                                   blob_file_meta->GetChecksumMethod()};
  const Status s = blob_source_->GetBlob(
      read_options, user_key, blob_file, blob_index.offset(),
      blob_file_meta->GetBlobFileSize(), blob_index.size(),
      blob_index.compression(), prefetch_buffer, value, bytes_read);

  return s;
}

Status Version::GetBlobRange(const ReadOptions& read_options,
                             const Slice& user_key, const BlobIndex& blob_index,
                             uint64_t range_offset, size_t range_length,
                             PinnableSlice* value, uint64_t* bytes_read) const {
  assert(value);

  if (blob_index.HasTTL() || blob_index.IsInlined()) {
    return Status::Corruption("Unexpected TTL/inlined blob index");
  }

  // A strict sub-range of a compressed blob cannot be decompressed in
  // isolation; the caller resolves such columns whole and slices instead.
  if (blob_index.compression() != kNoCompression) {
    return Status::Corruption("Cannot range-read a compressed blob");
  }

  const uint64_t blob_file_number = blob_index.file_number();

  auto blob_file_meta = storage_info_.GetBlobFileMetaData(blob_file_number);
  if (!blob_file_meta) {
    // INTEGRITY CHECK -- see Version::GetBlob. No metadata (including
    // file_number 0, the same-file "embedded" sentinel) means a corrupt index
    // or a same-file reference that the lazy caller must not route here.
    return Status::Corruption("Invalid blob file number");
  }

  assert(blob_source_);
  value->Reset();
  const BlobFileOpenInfo blob_file{blob_file_number,
                                   blob_file_meta->GetChecksumValue(),
                                   blob_file_meta->GetChecksumMethod()};
  return blob_source_->GetBlobRange(
      read_options, user_key, blob_file, blob_index.offset(),
      blob_file_meta->GetBlobFileSize(), blob_index.size(),
      blob_index.compression(), range_offset, range_length, value, bytes_read);
}

void Version::MultiGetBlobLazy(const ReadOptions& read_options,
                               autovector<LazyBlobReadRequest>& reqs) const {
  assert(blob_source_);

  // Turn the flat list of separate-file blob references into per-blob-file
  // batches that BlobSource can each resolve with a single coalesced MultiRead.
  // Whole-value and sub-range requests use different BlobSource entry points,
  // so they are grouped independently:
  //   whole_reqs -- batches of whole-value reads (BlobReadRequest)
  //   range_reqs -- batches of sub-range reads (BlobRangeReadRequest)
  // Each batch holds at most MultiGetContext::MAX_BATCH_SIZE requests (see the
  // loop below), so one file may contribute several batches. whole_idx /
  // range_idx map a file number to the index, within whole_reqs / range_reqs,
  // of that file's current (most recently started, still fillable) batch.
  autovector<BlobFileReadRequests> whole_reqs;
  autovector<BlobFileRangeReadRequests> range_reqs;
  std::unordered_map<uint64_t, size_t> whole_idx;
  std::unordered_map<uint64_t, size_t> range_idx;

  for (auto& req : reqs) {
    assert(req.user_key);
    assert(req.blob_index);
    assert(req.result);
    assert(req.status);
    const BlobIndex& blob_index = *req.blob_index;

    if (blob_index.HasTTL() || blob_index.IsInlined()) {
      *req.status = Status::Corruption("Unexpected TTL/inlined blob index");
      continue;
    }

    const uint64_t file_number = blob_index.file_number();
    auto blob_file_meta = storage_info_.GetBlobFileMetaData(file_number);
    if (!blob_file_meta) {
      // INTEGRITY CHECK -- see Version::GetBlob. A same-file/embedded reference
      // (file_number 0) must be resolved via SameFileBlobReader, not here.
      *req.status = Status::Corruption("Invalid blob file number");
      continue;
    }
    const uint64_t file_size = blob_file_meta->GetBlobFileSize();
    const BlobFileOpenInfo blob_file{file_number,
                                     blob_file_meta->GetChecksumValue(),
                                     blob_file_meta->GetChecksumMethod()};

    if (req.range_length == kWholeBlobLength) {
      // Append to this file's current batch, or start a new batch when the file
      // has none yet or its current batch is full. Capping each batch at
      // MAX_BATCH_SIZE is required, not just an optimization:
      // BlobFileReader::MultiGetBlob[Range] asserts a batch is at most
      // MAX_BATCH_SIZE, and BlobSource tracks a batch's cache hits in a 64-bit
      // mask -- while MultiGetEntityLazy does not otherwise bound how many keys
      // can reference a single blob file.
      size_t idx;
      auto it = whole_idx.find(file_number);
      if (it != whole_idx.end() && std::get<2>(whole_reqs[it->second]).size() <
                                       MultiGetContext::MAX_BATCH_SIZE) {
        idx = it->second;  // this file's current batch still has room
      } else {
        idx = whole_reqs.size();  // no batch yet for this file, or it is full
        whole_idx[file_number] = idx;
        whole_reqs.emplace_back(blob_file, file_size,
                                autovector<BlobReadRequest>());
      }
      std::get<2>(whole_reqs[idx])
          .emplace_back(*req.user_key, blob_index.offset(), blob_index.size(),
                        blob_index.compression(), req.result, req.status);
    } else {
      // A strict sub-range of a compressed blob cannot be decompressed in
      // isolation; the caller resolves such columns whole and slices instead.
      if (blob_index.compression() != kNoCompression) {
        *req.status = Status::Corruption("Cannot range-read a compressed blob");
        continue;
      }
      // Same per-file batching and MAX_BATCH_SIZE cap as the whole-value path
      // above.
      size_t idx;
      auto it = range_idx.find(file_number);
      if (it != range_idx.end() && std::get<2>(range_reqs[it->second]).size() <
                                       MultiGetContext::MAX_BATCH_SIZE) {
        idx = it->second;  // this file's current batch still has room
      } else {
        idx = range_reqs.size();  // no batch yet for this file, or it is full
        range_idx[file_number] = idx;
        range_reqs.emplace_back(blob_file, file_size,
                                autovector<BlobRangeReadRequest>());
      }
      std::get<2>(range_reqs[idx])
          .emplace_back(*req.user_key, blob_index.offset(), blob_index.size(),
                        req.range_offset, req.range_length, req.result,
                        req.status);
    }
  }

  // Resolve every batch; BlobSource issues one coalesced MultiRead per batch.
  if (!whole_reqs.empty()) {
    blob_source_->MultiGetBlob(read_options, whole_reqs,
                               /*bytes_read=*/nullptr);
  }
  if (!range_reqs.empty()) {
    blob_source_->MultiGetBlobRange(read_options, range_reqs,
                                    /*bytes_read=*/nullptr);
  }
}

void Version::MultiGetBlob(
    const ReadOptions& read_options, MultiGetRange& range,
    std::unordered_map<uint64_t, BlobReadContexts>& blob_ctxs) {
  assert(!blob_ctxs.empty());

  autovector<BlobFileReadRequests> blob_reqs;

  for (auto& ctx : blob_ctxs) {
    const auto file_number = ctx.first;
    const auto blob_file_meta = storage_info_.GetBlobFileMetaData(file_number);

    autovector<BlobReadRequest> blob_reqs_in_file;
    BlobReadContexts& blobs_in_file = ctx.second;
    for (auto& blob : blobs_in_file) {
      const BlobIndex& blob_index = blob.blob_index;
      KeyContext* const key_context = blob.key_context;
      assert(key_context);
      assert(key_context->get_context);
      assert(key_context->s);

      if (key_context->value) {
        key_context->value->Reset();
      } else {
        assert(key_context->columns);
        key_context->columns->Reset();
      }

      if (!blob_file_meta) {
        // INTEGRITY CHECK -- do not weaken; see Version::GetBlob and
        // FileMetaData::UpdateBoundaries. A same-file/embedded reference must
        // be resolved before reaching the integrated-BlobDB read path.
        *key_context->s = Status::Corruption("Invalid blob file number");
        continue;
      }

      if (blob_index.HasTTL() || blob_index.IsInlined()) {
        *key_context->s =
            Status::Corruption("Unexpected TTL/inlined blob index");
        continue;
      }

      blob_reqs_in_file.emplace_back(
          key_context->get_context->ukey_to_get_blob_value(),
          blob_index.offset(), blob_index.size(), blob_index.compression(),
          &blob.result, key_context->s);
    }
    if (blob_reqs_in_file.size() > 0) {
      const auto file_size = blob_file_meta->GetBlobFileSize();
      const BlobFileOpenInfo blob_file{file_number,
                                       blob_file_meta->GetChecksumValue(),
                                       blob_file_meta->GetChecksumMethod()};
      blob_reqs.emplace_back(blob_file, file_size, blob_reqs_in_file);
    }
  }

  if (blob_reqs.size() > 0) {
    blob_source_->MultiGetBlob(read_options, blob_reqs,
                               /*bytes_read=*/nullptr);
  }

  for (auto& ctx : blob_ctxs) {
    BlobReadContexts& blobs_in_file = ctx.second;
    for (auto& blob : blobs_in_file) {
      KeyContext* const key_context = blob.key_context;
      assert(key_context);
      assert(key_context->get_context);
      assert(key_context->s);

      if (key_context->s->ok()) {
        if (key_context->value) {
          *key_context->value = std::move(blob.result);
          range.AddValueSize(key_context->value->size());
        } else {
          assert(key_context->columns);
          key_context->columns->SetPlainValue(std::move(blob.result));
          range.AddValueSize(key_context->columns->payload_size());
        }
        // MultiGetBlob() has already materialized the blob reference into the
        // user-visible value, so clear this flag before later MultiGet
        // post-processing checks whether further blob resolution is needed.
        key_context->is_blob_index = false;

        if (range.GetValueSize() > read_options.value_size_soft_limit) {
          *key_context->s = Status::Aborted();
        }
      } else if (key_context->s->IsIncomplete()) {
        // read_options.read_tier == kBlockCacheTier
        // Cannot read blob(s): no disk I/O allowed
        auto& get_context = *(key_context->get_context);
        get_context.MarkKeyMayExist();
      }
    }
  }
}

#ifdef USE_COROUTINES
Status Version::ProcessBatch(
    const ReadOptions& read_options, FilePickerMultiGet* batch,
    std::vector<folly::coro::Task<Status>>& mget_tasks,
    std::unordered_map<uint64_t, BlobReadContexts>* blob_ctxs,
    autovector<FilePickerMultiGet, 4>& batches, std::deque<size_t>& waiting,
    std::deque<size_t>& to_process, unsigned int& num_tasks_queued,
    std::unordered_map<int, std::tuple<uint64_t, uint64_t, uint64_t>>&
        mget_stats) {
  FilePickerMultiGet& fp = *batch;
  MultiGetRange range = fp.GetRange();
  // Initialize a new empty range. Any keys that are not in this level will
  // eventually become part of the new range.
  MultiGetRange leftover(range, range.begin(), range.begin());
  FdWithKeyRange* f = nullptr;
  Status s;

  f = fp.GetNextFileInLevel();
  while (!f) {
    fp.PrepareNextLevelForSearch();
    if (!fp.IsSearchEnded()) {
      f = fp.GetNextFileInLevel();
    } else {
      break;
    }
  }
  while (f) {
    MultiGetRange file_range = fp.CurrentFileRange();
    TableCache::TypedHandle* table_handle = nullptr;
    bool skip_filters = IsFilterSkipped(static_cast<int>(fp.GetHitFileLevel()),
                                        fp.IsHitFileLastInLevel());
    bool skip_range_deletions = false;
    if (!skip_filters) {
      Status status = table_cache_->MultiGetFilter(
          read_options, *internal_comparator(), *f->file_metadata,
          mutable_cf_options_,
          cfd_->internal_stats()->GetFileReadHist(fp.GetHitFileLevel()),
          fp.GetHitFileLevel(), &file_range, &table_handle);
      if (status.ok()) {
        skip_filters = true;
        skip_range_deletions = true;
      } else if (!status.IsNotSupported()) {
        s = status;
      }
    }
    if (!s.ok()) {
      break;
    }
    // At this point, file_range contains any keys that are likely in this
    // file. It may have false positives, but that's ok since higher level
    // lookups for the key are dependent on this lookup anyway.
    // Add the complement of file_range to leftover. That's the set of keys
    // definitely not in this level.
    // Subtract the complement of file_range from range, since they will be
    // processed in a separate batch in parallel.
    leftover += ~file_range;
    range -= ~file_range;
    if (!file_range.empty()) {
      int level = fp.GetHitFileLevel();
      auto stat = mget_stats.find(level);
      if (stat == mget_stats.end()) {
        auto entry = mget_stats.insert({level, {0, 0, 0}});
        assert(entry.second);
        stat = entry.first;
      }

      if (waiting.empty() && to_process.empty() &&
          !fp.RemainingOverlapInLevel() && leftover.empty() &&
          mget_tasks.empty()) {
        // All keys are in one SST file, so take the fast path
        s = MultiGetFromSST(read_options, file_range, fp.GetHitFileLevel(),
                            skip_filters, skip_range_deletions, f, *blob_ctxs,
                            table_handle, std::get<0>(stat->second),
                            std::get<1>(stat->second),
                            std::get<2>(stat->second));
      } else {
        mget_tasks.emplace_back(MultiGetFromSSTCoroutine(
            read_options, file_range, fp.GetHitFileLevel(), skip_filters,
            skip_range_deletions, f, *blob_ctxs, table_handle,
            std::get<0>(stat->second), std::get<1>(stat->second),
            std::get<2>(stat->second)));
        ++num_tasks_queued;
      }
    }
    if (fp.KeyMaySpanNextFile() && !file_range.empty()) {
      break;
    }
    f = fp.GetNextFileInLevel();
  }
  // Split the current batch only if some keys are likely in this level and
  // some are not. Only split if we're done with this level, i.e f is null.
  // Otherwise, it means there are more files in this level to look at.
  if (s.ok() && !f && !leftover.empty() && !range.empty()) {
    fp.ReplaceRange(range);
    batches.emplace_back(&leftover, fp);
    to_process.emplace_back(batches.size() - 1);
  }
  // 1. If f is non-null, that means we might not be done with this level.
  //    This can happen if one of the keys is the last key in the file, i.e
  //    fp.KeyMaySpanNextFile() is true.
  // 2. If range is empty, then we're done with this range and no need to
  //    prepare the next level
  // 3. If some tasks were queued for this range, then the next level will be
  //    prepared after executing those tasks
  if (!f && !range.empty() && !num_tasks_queued) {
    fp.PrepareNextLevelForSearch();
  }
  return s;
}

Status Version::MultiGetAsync(
    const ReadOptions& options, MultiGetRange* range,
    std::unordered_map<uint64_t, BlobReadContexts>* blob_ctxs) {
  autovector<FilePickerMultiGet, 4> batches;
  std::deque<size_t> waiting;
  std::deque<size_t> to_process;
  Status s;
  std::vector<folly::coro::Task<Status>> mget_tasks;
  std::unordered_map<int, std::tuple<uint64_t, uint64_t, uint64_t>> mget_stats;

  // Create the initial batch with the input range
  batches.emplace_back(range, &storage_info_.level_files_brief_,
                       storage_info_.num_non_empty_levels_,
                       &storage_info_.file_indexer_, user_comparator(),
                       internal_comparator());
  to_process.emplace_back(0);

  while (!to_process.empty()) {
    // As we process a batch, it may get split into two. So reserve space for
    // an additional batch in the autovector in order to prevent later moves
    // of elements in ProcessBatch().
    batches.reserve(batches.size() + 1);

    size_t idx = to_process.front();
    FilePickerMultiGet* batch = &batches.at(idx);
    unsigned int num_tasks_queued = 0;
    to_process.pop_front();
    if (batch->IsSearchEnded() || batch->GetRange().empty()) {
      // If to_process is empty, i.e no more batches to look at, then we need
      // schedule the enqueued coroutines and wait for them. Otherwise, we
      // skip this batch and move to the next one in to_process.
      if (!to_process.empty()) {
        continue;
      }
    } else {
      // Look through one level. This may split the batch and enqueue it to
      // to_process
      s = ProcessBatch(options, batch, mget_tasks, blob_ctxs, batches, waiting,
                       to_process, num_tasks_queued, mget_stats);
      // If ProcessBatch didn't enqueue any coroutine tasks, it means all
      // keys were filtered out. So put the batch back in to_process to
      // lookup in the next level
      if (!num_tasks_queued && !batch->IsSearchEnded()) {
        // Put this back in the processing queue
        to_process.emplace_back(idx);
      } else if (num_tasks_queued) {
        waiting.emplace_back(idx);
      }
    }
    // If ProcessBatch() returned an error, then schedule the enqueued
    // coroutines and wait for them, then abort the MultiGet.
    if (to_process.empty() || !s.ok()) {
      if (mget_tasks.size() > 0) {
        assert(waiting.size());
        RecordTick(db_statistics_, MULTIGET_COROUTINE_COUNT, mget_tasks.size());
        // Collect all results so far
        std::vector<Status> statuses =
            folly::coro::blockingWait(co_withExecutor(
                &range->context()->executor(),
                folly::coro::collectAllRange(std::move(mget_tasks))));
        mget_tasks.clear();
        if (s.ok()) {
          for (Status stat : statuses) {
            if (!stat.ok()) {
              s = std::move(stat);
              break;
            }
          }
        }

        if (!s.ok()) {
          break;
        }

        for (size_t wait_idx : waiting) {
          FilePickerMultiGet& fp = batches.at(wait_idx);
          // 1. If fp.GetHitFile() is non-null, then there could be more
          // overlap in this level. So skip preparing next level.
          // 2. If fp.GetRange() is empty, then this batch is completed
          // and no need to prepare the next level.
          if (!fp.GetHitFile() && !fp.GetRange().empty()) {
            fp.PrepareNextLevelForSearch();
          }
        }
        to_process.swap(waiting);
      } else {
        assert(!s.ok() || waiting.size() == 0);
      }
    }
    if (!s.ok()) {
      break;
    }
  }

  uint64_t num_levels = 0;
  for (auto& stat : mget_stats) {
    if (stat.first == 0) {
      num_levels += std::get<2>(stat.second);
    } else {
      num_levels++;
    }

    uint64_t num_meta_reads =
        std::get<0>(stat.second) + std::get<1>(stat.second);
    uint64_t num_sst_reads = std::get<2>(stat.second);
    if (num_meta_reads > 0) {
      RecordInHistogram(db_statistics_,
                        NUM_INDEX_AND_FILTER_BLOCKS_READ_PER_LEVEL,
                        num_meta_reads);
    }
    if (num_sst_reads > 0) {
      RecordInHistogram(db_statistics_, NUM_SST_READ_PER_LEVEL, num_sst_reads);
    }
  }
  if (num_levels > 0) {
    RecordInHistogram(db_statistics_, NUM_LEVEL_READ_PER_MULTIGET, num_levels);
  }

  return s;
}
#endif

bool Version::IsFilterSkipped(int level, bool is_file_last_in_level) {
  // Reaching the bottom level implies misses at all upper levels, so we'll
  // skip checking the filters when we predict a hit.
  return cfd_->ioptions().optimize_filters_for_hits &&
         (level > 0 || is_file_last_in_level) &&
         level == storage_info_.num_non_empty_levels() - 1;
}

void VersionStorageInfo::GenerateLevelFilesBrief() {
  level_files_brief_.resize(num_non_empty_levels_);
  for (int level = 0; level < num_non_empty_levels_; level++) {
    DoGenerateLevelFilesBrief(&level_files_brief_[level], files_[level],
                              &arena_);
  }
}

void VersionStorageInfo::PrepareForVersionAppend(
    const ImmutableOptions& immutable_options,
    const MutableCFOptions& mutable_cf_options) {
  ComputeCompensatedSizes();
  UpdateNumNonEmptyLevels();
  CalculateBaseBytes(immutable_options, mutable_cf_options);
  UpdateFilesByCompactionPri(immutable_options, mutable_cf_options);
  GenerateFileIndexer();
  GenerateLevelFilesBrief();
  GenerateLevel0NonOverlapping();
  GenerateBottommostFiles();
  GenerateFileLocationIndex();
}

void Version::PrepareAppend(const ReadOptions& read_options,
                            bool update_stats) {
  TEST_SYNC_POINT_CALLBACK(
      "Version::PrepareAppend:forced_check",
      static_cast<void*>(&storage_info_.force_consistency_checks_));

  if (update_stats) {
    UpdateAccumulatedStats(read_options);
  }

  storage_info_.PrepareForVersionAppend(cfd_->ioptions(), mutable_cf_options_);
}

bool Version::MaybeInitializeFileMetaData(const ReadOptions& read_options,
                                          FileMetaData* file_meta) {
  if (file_meta->init_stats_from_file || file_meta->compensated_file_size > 0) {
    return false;
  }
  std::shared_ptr<const TableProperties> tp;
  Status s = GetTableProperties(read_options, &tp, file_meta);
  file_meta->init_stats_from_file = true;
  if (!s.ok()) {
    ROCKS_LOG_ERROR(vset_->db_options_->info_log,
                    "Unable to load table properties for file %" PRIu64
                    " --- %s\n",
                    file_meta->fd.GetNumber(), s.ToString().c_str());
    return false;
  }
  if (tp.get() == nullptr) {
    return false;
  }
  file_meta->num_entries = tp->num_entries;
  file_meta->num_deletions = tp->num_deletions;
  file_meta->raw_value_size = tp->raw_value_size;
  file_meta->raw_key_size = tp->raw_key_size;
  file_meta->num_range_deletions = tp->num_range_deletions;
  // Ensure new invariants on old files
  file_meta->num_deletions =
      std::max(tp->num_deletions, tp->num_range_deletions);
  file_meta->num_entries = std::max(tp->num_entries, file_meta->num_deletions);
  return true;
}

void VersionStorageInfo::UpdateAccumulatedStats(FileMetaData* file_meta) {
  TEST_SYNC_POINT_CALLBACK("VersionStorageInfo::UpdateAccumulatedStats",
                           nullptr);

  assert(file_meta->init_stats_from_file);
  accumulated_file_size_ += file_meta->fd.GetFileSize();
  accumulated_raw_key_size_ += file_meta->raw_key_size;
  accumulated_raw_value_size_ += file_meta->raw_value_size;
  assert(file_meta->num_entries >= file_meta->num_deletions);
  accumulated_num_non_deletions_ +=
      file_meta->num_entries - file_meta->num_deletions;
  accumulated_num_deletions_ += file_meta->num_deletions;

  current_num_non_deletions_ +=
      file_meta->num_entries - file_meta->num_deletions;
  current_num_deletions_ += file_meta->num_deletions;
  current_num_samples_++;
}

void VersionStorageInfo::RemoveCurrentStats(FileMetaData* file_meta) {
  if (file_meta->init_stats_from_file) {
    current_num_non_deletions_ -=
        file_meta->num_entries - file_meta->num_deletions;
    current_num_deletions_ -= file_meta->num_deletions;
    current_num_samples_--;
  }
}

void Version::UpdateAccumulatedStats(const ReadOptions& read_options) {
  // maximum number of table properties loaded from files.
  const int kMaxInitCount = 20;
  int init_count = 0;
  // here only the first kMaxInitCount files which haven't been
  // initialized from file will be updated with num_deletions.
  // The motivation here is to cap the maximum I/O per Version creation.
  // The reason for choosing files from lower-level instead of higher-level
  // is that such design is able to propagate the initialization from
  // lower-level to higher-level:  When the num_deletions of lower-level
  // files are updated, it will make the lower-level files have accurate
  // compensated_file_size, making lower-level to higher-level compaction
  // will be triggered, which creates higher-level files whose num_deletions
  // will be updated here.
  for (int level = 0;
       level < storage_info_.num_levels_ && init_count < kMaxInitCount;
       ++level) {
    for (auto* file_meta : storage_info_.files_[level]) {
      if (MaybeInitializeFileMetaData(read_options, file_meta)) {
        // each FileMeta will be initialized only once.
        storage_info_.UpdateAccumulatedStats(file_meta);
        // when option "max_open_files" is -1, all the file metadata has
        // already been read, so MaybeInitializeFileMetaData() won't incur
        // any I/O cost. "max_open_files=-1" means that the table cache passed
        // to the VersionSet and then to the ColumnFamilySet has a size of
        // TableCache::kInfiniteCapacity
        if (vset_->GetColumnFamilySet()->get_table_cache()->GetCapacity() ==
            TableCache::kInfiniteCapacity) {
          continue;
        }
        if (++init_count >= kMaxInitCount) {
          break;
        }
      }
    }
  }
  // In case all sampled-files contain only deletion entries, then we
  // load the table-property of a file in higher-level to initialize
  // that value.
  for (int level = storage_info_.num_levels_ - 1;
       storage_info_.accumulated_raw_value_size_ == 0 && level >= 0; --level) {
    for (int i = static_cast<int>(storage_info_.files_[level].size()) - 1;
         storage_info_.accumulated_raw_value_size_ == 0 && i >= 0; --i) {
      if (MaybeInitializeFileMetaData(read_options,
                                      storage_info_.files_[level][i])) {
        storage_info_.UpdateAccumulatedStats(storage_info_.files_[level][i]);
      }
    }
  }
}

void VersionStorageInfo::ComputeCompensatedSizes() {
  static const int kDeletionWeightOnCompaction = 2;
  uint64_t average_value_size = GetAverageValueSize();

  // compute the compensated size
  for (int level = 0; level < num_levels_; level++) {
    for (auto* file_meta : files_[level]) {
      // Here we only compute compensated_file_size for those file_meta
      // which compensated_file_size is uninitialized (== 0). This is true only
      // for files that have been created right now and no other thread has
      // access to them. That's why we can safely mutate compensated_file_size.
      if (file_meta->compensated_file_size == 0) {
        file_meta->compensated_file_size = file_meta->fd.GetFileSize();
        // Here we only boost the size of deletion entries of a file only
        // when the number of deletion entries is greater than the number of
        // non-deletion entries in the file.  The motivation here is that in
        // a stable workload, the number of deletion entries should be roughly
        // equal to the number of non-deletion entries.  If we compensate the
        // size of deletion entries in a stable workload, the deletion
        // compensation logic might introduce unwanted effet which changes the
        // shape of LSM tree.
        if ((file_meta->num_deletions - file_meta->num_range_deletions) * 2 >=
            file_meta->num_entries) {
          file_meta->compensated_file_size +=
              ((file_meta->num_deletions - file_meta->num_range_deletions) * 2 -
               file_meta->num_entries) *
              average_value_size * kDeletionWeightOnCompaction;
        }
        file_meta->compensated_file_size +=
            file_meta->compensated_range_deletion_size;
      }
    }
  }
}

int VersionStorageInfo::MaxInputLevel() const {
  if (compaction_style_ == kCompactionStyleLevel) {
    return num_levels() - 2;
  }
  return 0;
}

int VersionStorageInfo::MaxOutputLevel(bool allow_ingest_behind) const {
  if (allow_ingest_behind) {
    assert(num_levels() > 1);
    return num_levels() - 2;
  }
  return num_levels() - 1;
}

void VersionStorageInfo::EstimateCompactionBytesNeeded(
    const MutableCFOptions& mutable_cf_options) {
  // Only implemented for level-based compaction
  if (compaction_style_ != kCompactionStyleLevel) {
    estimated_compaction_needed_bytes_ = 0;
    return;
  }

  // Start from Level 0, if level 0 qualifies compaction to level 1,
  // we estimate the size of compaction.
  // Then we move on to the next level and see whether it qualifies compaction
  // to the next level. The size of the level is estimated as the actual size
  // on the level plus the input bytes from the previous level if there is any.
  // If it exceeds, take the exceeded bytes as compaction input and add the size
  // of the compaction size to tatal size.
  // We keep doing it to Level 2, 3, etc, until the last level and return the
  // accumulated bytes.

  uint64_t bytes_compact_to_next_level = 0;
  uint64_t level_size = 0;
  for (auto* f : files_[0]) {
    level_size += f->fd.GetFileSize();
  }
  // Level 0
  bool level0_compact_triggered = false;
  if (static_cast<int>(files_[0].size()) >=
          mutable_cf_options.level0_file_num_compaction_trigger ||
      level_size >= mutable_cf_options.max_bytes_for_level_base) {
    level0_compact_triggered = true;
    estimated_compaction_needed_bytes_ = level_size;
    bytes_compact_to_next_level = level_size;
  } else {
    estimated_compaction_needed_bytes_ = 0;
  }

  // Level 1 and up.
  uint64_t bytes_next_level = 0;
  for (int level = base_level(); level <= MaxInputLevel(); level++) {
    level_size = 0;
    if (bytes_next_level > 0) {
#ifndef NDEBUG
      uint64_t level_size2 = 0;
      for (auto* f : files_[level]) {
        level_size2 += f->fd.GetFileSize();
      }
      assert(level_size2 == bytes_next_level);
#endif
      level_size = bytes_next_level;
      bytes_next_level = 0;
    } else {
      for (auto* f : files_[level]) {
        level_size += f->fd.GetFileSize();
      }
    }
    if (level == base_level() && level0_compact_triggered) {
      // Add base level size to compaction if level0 compaction triggered.
      estimated_compaction_needed_bytes_ += level_size;
    }
    // Add size added by previous compaction
    level_size += bytes_compact_to_next_level;
    bytes_compact_to_next_level = 0;
    uint64_t level_target = MaxBytesForLevel(level);
    if (level_size > level_target) {
      bytes_compact_to_next_level = level_size - level_target;
      // Estimate the actual compaction fan-out ratio as size ratio between
      // the two levels.

      assert(bytes_next_level == 0);
      if (level + 1 < num_levels_) {
        for (auto* f : files_[level + 1]) {
          bytes_next_level += f->fd.GetFileSize();
        }
      }
      if (bytes_next_level > 0) {
        assert(level_size > 0);
        estimated_compaction_needed_bytes_ += static_cast<uint64_t>(
            static_cast<double>(bytes_compact_to_next_level) *
            (static_cast<double>(bytes_next_level) /
                 static_cast<double>(level_size) +
             1));
      }
    }
  }
}

namespace {
uint32_t GetExpiredTtlFilesCount(const ImmutableOptions& ioptions,
                                 const MutableCFOptions& mutable_cf_options,
                                 const std::vector<FileMetaData*>& files) {
  uint32_t ttl_expired_files_count = 0;

  int64_t _current_time;
  auto status = ioptions.clock->GetCurrentTime(&_current_time);
  if (status.ok()) {
    const uint64_t current_time = static_cast<uint64_t>(_current_time);
    for (FileMetaData* f : files) {
      if (!f->being_compacted) {
        uint64_t oldest_ancestor_time = f->TryGetOldestAncestorTime();
        if (oldest_ancestor_time != 0 &&
            oldest_ancestor_time < (current_time - mutable_cf_options.ttl)) {
          ttl_expired_files_count++;
        }
      }
    }
  }
  return ttl_expired_files_count;
}

bool ShouldChangeFileTemperature(const ImmutableOptions& ioptions,
                                 const MutableCFOptions& mutable_cf_options,
                                 const std::vector<FileMetaData*>& files) {
  const std::vector<FileTemperatureAge>& ages =
      mutable_cf_options.compaction_options_fifo
          .file_temperature_age_thresholds;
  if (ages.empty()) {
    return false;
  }
  if (files.empty()) {
    return false;
  }
  int64_t _current_time;
  auto status = ioptions.clock->GetCurrentTime(&_current_time);
  const uint64_t current_time = static_cast<uint64_t>(_current_time);
  // This is the same logic used in
  // FIFOCompactionPicker::PickTemperatureChangeCompaction().
  if (status.ok() && current_time >= ages[0].age) {
    uint64_t create_time_threshold = current_time - ages[0].age;
    Temperature target_temp;
    assert(files.size() >= 1);
    for (size_t index = files.size(); index >= 1; --index) {
      FileMetaData* cur_file = files[index - 1];
      FileMetaData* prev_file = index < 2 ? nullptr : files[index - 2];
      if (!cur_file->being_compacted) {
        uint64_t est_newest_key_time = cur_file->TryGetNewestKeyTime(prev_file);
        // Newer file could have newest_key_time populated
        if (est_newest_key_time == kUnknownNewestKeyTime) {
          continue;
        }
        if (est_newest_key_time > create_time_threshold) {
          return false;
        }
        target_temp = ages[0].temperature;
        for (size_t i = 1; i < ages.size(); ++i) {
          if (current_time >= ages[i].age &&
              est_newest_key_time <= current_time - ages[i].age) {
            target_temp = ages[i].temperature;
          }
        }
        if (cur_file->temperature != target_temp) {
          return true;
        }
      }
    }
  }
  return false;
}
}  // anonymous namespace

void VersionStorageInfo::ComputeCompactionScore(
    const ImmutableOptions& immutable_options,
    const MutableCFOptions& mutable_cf_options,
    const std::string& full_history_ts_low) {
  double total_downcompact_bytes = 0.0;
  // Historically, score is defined as actual bytes in a level divided by
  // the level's target size, and 1.0 is the threshold for triggering
  // compaction. Higher score means higher prioritization.
  // Now we keep the compaction triggering condition, but consider more
  // factors for prioritization, while still keeping the 1.0 threshold.
  // In order to provide flexibility for reducing score while still
  // maintaining it to be over 1.0, we scale the original score by 10x
  // if it is larger than 1.0.
  const double kScoreScale = 10.0;
  int max_output_level =
      MaxOutputLevel(immutable_options.cf_allow_ingest_behind ||
                     immutable_options.allow_ingest_behind);
  for (int level = 0; level <= MaxInputLevel(); level++) {
    double score;
    if (level == 0) {
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      //
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      //
      // (2) The files in level-0 are merged on every read and
      // therefore we wish to avoid too many files when the individual
      // file size is small (perhaps because of a small write-buffer
      // setting, or very high compression ratios, or lots of
      // overwrites/deletions).
      int num_sorted_runs = 0;
      uint64_t total_size = 0;
      for (auto* f : files_[level]) {
        total_downcompact_bytes += static_cast<double>(f->fd.GetFileSize());
        if (!f->being_compacted) {
          total_size += f->compensated_file_size;
          num_sorted_runs++;
        }
      }
      if (compaction_style_ == kCompactionStyleUniversal) {
        // For universal compaction, we use level0 score to indicate
        // compaction score for the whole DB. Adding other levels as if
        // they are L0 files.
        for (int i = 1; i <= max_output_level; i++) {
          // It's possible that a subset of the files in a level may be in a
          // compaction, due to delete triggered compaction or trivial move.
          // In that case, the below check may not catch a level being
          // compacted as it only checks the first file. The worst that can
          // happen is a scheduled compaction thread will find nothing to do.
          if (!files_[i].empty() && !files_[i][0]->being_compacted) {
            num_sorted_runs++;
          }
        }
      }

      if (compaction_style_ == kCompactionStyleFIFO) {
        const auto& fifo_opts = mutable_cf_options.compaction_options_fifo;
        uint64_t effective_size = total_size;
        uint64_t effective_max = fifo_opts.max_table_files_size;
        if (fifo_opts.max_data_files_size > 0) {
          // Blob-aware: include blob file sizes in the total
          effective_size += GetBlobStats().total_file_size;
          effective_max = fifo_opts.max_data_files_size;
        }
        if (effective_max == 0) {
          // avoid divide 0
          effective_max = 1;
        }
        score = static_cast<double>(effective_size) / effective_max;
        if (score < 1 && fifo_opts.allow_compaction) {
          score = std::max(
              static_cast<double>(num_sorted_runs) /
                  mutable_cf_options.level0_file_num_compaction_trigger,
              score);
        }
        if (score < 1 && mutable_cf_options.ttl > 0) {
          score =
              std::max(static_cast<double>(GetExpiredTtlFilesCount(
                           immutable_options, mutable_cf_options, files_[0])),
                       score);
        }
        if (score < 1 &&
            ShouldChangeFileTemperature(immutable_options, mutable_cf_options,
                                        files_[0])) {
          // For FIFO, just need a large enough score to trigger compaction.
          const double kScoreForNeedCompaction = 1.1;
          score = kScoreForNeedCompaction;
        }
      } else {
        // For universal compaction, if a user configures `max_read_amp`, then
        // the score may be a false positive signal.
        // `level0_file_num_compaction_trigger` is used as a trigger to check
        // if there is any compaction work to do.
        score = static_cast<double>(num_sorted_runs) /
                mutable_cf_options.level0_file_num_compaction_trigger;
        if (compaction_style_ == kCompactionStyleLevel && num_levels() > 1) {
          // Level-based involves L0->L0 compactions that can lead to oversized
          // L0 files. Take into account size as well to avoid later giant
          // compactions to the base level.
          // If score in L0 is always too high, L0->LBase will always be
          // prioritized over LBase->LBase+1 compaction and LBase will
          // accumulate to too large. But if L0 score isn't high enough, L0 will
          // accumulate and data is not moved to LBase fast enough. The score
          // calculation below takes into account L0 size vs LBase size.
          if (immutable_options.level_compaction_dynamic_level_bytes) {
            if (total_size >= mutable_cf_options.max_bytes_for_level_base) {
              // When calculating estimated_compaction_needed_bytes, we assume
              // L0 is qualified as pending compactions. We will need to make
              // sure that it qualifies for compaction.
              // It might be guaranteed by logic below anyway, but we are
              // explicit here to make sure we don't stop writes with no
              // compaction scheduled.
              score = std::max(score, 1.01);
            }
            if (total_size > level_max_bytes_[base_level_]) {
              // In this case, we compare L0 size with actual LBase size and
              // make sure score is more than 1.0 (10.0 after scaled) if L0 is
              // larger than LBase. Since LBase score = LBase size /
              // (target size + total_downcompact_bytes) where
              // total_downcompact_bytes = total_size > LBase size,
              // LBase score is lower than 10.0. So L0->LBase is prioritized
              // over LBase -> LBase+1.
              uint64_t base_level_size = 0;
              for (auto f : files_[base_level_]) {
                base_level_size += f->compensated_file_size;
              }
              score = std::max(score, static_cast<double>(total_size) /
                                          static_cast<double>(std::max(
                                              base_level_size,
                                              level_max_bytes_[base_level_])));
            }
            if (score > 1.0) {
              score *= kScoreScale;
            }
          } else {
            score = std::max(score,
                             static_cast<double>(total_size) /
                                 mutable_cf_options.max_bytes_for_level_base);
          }
        }
      }
    } else {  // level > 0
      // Compute the ratio of current size to size limit.
      uint64_t level_bytes_no_compacting = 0;
      uint64_t level_total_bytes = 0;
      for (auto f : files_[level]) {
        level_total_bytes += f->fd.GetFileSize();
        if (!f->being_compacted) {
          level_bytes_no_compacting += f->compensated_file_size;
        }
      }
      if (!immutable_options.level_compaction_dynamic_level_bytes) {
        score = static_cast<double>(level_bytes_no_compacting) /
                MaxBytesForLevel(level);
      } else {
        if (level_bytes_no_compacting < MaxBytesForLevel(level)) {
          score = static_cast<double>(level_bytes_no_compacting) /
                  MaxBytesForLevel(level);
        } else {
          // If there are a large mount of data being compacted down to the
          // current level soon, we would de-prioritize compaction from
          // a level where the incoming data would be a large ratio. We do
          // it by dividing level size not by target level size, but
          // the target size and the incoming compaction bytes.
          score = static_cast<double>(level_bytes_no_compacting) /
                  (MaxBytesForLevel(level) + total_downcompact_bytes) *
                  kScoreScale;
        }
        // Drain unnecessary levels, but with lower priority compared to
        // when L0 is eligible. Only non-empty levels can be unnecessary.
        // If there is no unnecessary levels, lowest_unnecessary_level_ = -1.
        if (level_bytes_no_compacting > 0 &&
            level <= lowest_unnecessary_level_) {
          score = std::max(
              score, kScoreScale *
                         (1.001 + 0.001 * (lowest_unnecessary_level_ - level)));
        }
      }
      if (level <= lowest_unnecessary_level_) {
        total_downcompact_bytes += level_total_bytes;
      } else if (level_total_bytes > MaxBytesForLevel(level)) {
        total_downcompact_bytes +=
            static_cast<double>(level_total_bytes - MaxBytesForLevel(level));
      }
    }
    compaction_level_[level] = level;
    compaction_score_[level] = score;
  }

  // sort all the levels based on their score. Higher scores get listed
  // first. Use bubble sort because the number of entries are small.
  for (int i = 0; i < num_levels() - 2; i++) {
    for (int j = i + 1; j < num_levels() - 1; j++) {
      if (compaction_score_[i] < compaction_score_[j]) {
        double score = compaction_score_[i];
        int level = compaction_level_[i];
        compaction_score_[i] = compaction_score_[j];
        compaction_level_[i] = compaction_level_[j];
        compaction_score_[j] = score;
        compaction_level_[j] = level;
      }
    }
  }
  ComputeFilesMarkedForCompaction(max_output_level);
  ComputeBottommostFilesMarkedForCompaction(
      immutable_options.cf_allow_ingest_behind ||
          immutable_options.allow_ingest_behind,
      immutable_options.user_comparator, full_history_ts_low);
  ComputeExpiredTtlFiles(immutable_options, mutable_cf_options.ttl);
  ComputeFilesMarkedForPeriodicCompaction(
      immutable_options, mutable_cf_options.periodic_compaction_seconds,
      max_output_level);
  ComputeFilesMarkedForForcedBlobGC(
      mutable_cf_options.blob_garbage_collection_age_cutoff,
      mutable_cf_options.blob_garbage_collection_force_threshold,
      mutable_cf_options.enable_blob_garbage_collection);
  ComputeFilesMarkedForReadTriggeredCompaction(
      mutable_cf_options.read_triggered_compaction_threshold,
      immutable_options.compaction_style);

  EstimateCompactionBytesNeeded(mutable_cf_options);
}

void VersionStorageInfo::RemoveFilesBeingCompactedFromCompactionCandidates() {
  for (auto* candidates :
       {&files_marked_for_compaction_, &bottommost_files_marked_for_compaction_,
        &expired_ttl_files_, &files_marked_for_periodic_compaction_,
        &files_marked_for_forced_blob_gc_, &read_triggered_compaction_files_}) {
    const size_t remaining =
        std::remove_if(candidates->begin(), candidates->end(),
                       [](const auto& level_file) {
                         return level_file.second->being_compacted;
                       }) -
        candidates->begin();
    candidates->resize(remaining);
  }
  standalone_range_tombstone_files_mark_threshold_ = kMaxSequenceNumber;
  for (const auto& level_file : files_marked_for_compaction_) {
    if (level_file.second->FileIsStandAloneRangeTombstone()) {
      standalone_range_tombstone_files_mark_threshold_ =
          std::min(standalone_range_tombstone_files_mark_threshold_,
                   level_file.second->fd.smallest_seqno);
    }
  }
}

void VersionStorageInfo::ComputeFilesMarkedForCompaction(int last_level) {
  files_marked_for_compaction_.clear();
  int last_qualify_level = 0;
  standalone_range_tombstone_files_mark_threshold_ = kMaxSequenceNumber;

  // Do not include files from the last level with data
  // If table properties collector suggests a file on the last level,
  // we should not move it to a new level.
  for (int level = last_level; level >= 1; level--) {
    if (!files_[level].empty()) {
      last_qualify_level = level - 1;
      break;
    }
  }

  for (int level = 0; level <= last_qualify_level; level++) {
    for (auto* f : files_[level]) {
      if (!f->being_compacted && f->marked_for_compaction) {
        files_marked_for_compaction_.emplace_back(level, f);
        if (f->FileIsStandAloneRangeTombstone()) {
          standalone_range_tombstone_files_mark_threshold_ =
              std::min(standalone_range_tombstone_files_mark_threshold_,
                       f->fd.smallest_seqno);
        }
      }
    }
  }
}

void VersionStorageInfo::ComputeExpiredTtlFiles(
    const ImmutableOptions& ioptions, const uint64_t ttl) {
  expired_ttl_files_.clear();
  if (ttl == 0 || compaction_style_ != CompactionStyle::kCompactionStyleLevel) {
    return;
  }

  int64_t _current_time;
  auto status = ioptions.clock->GetCurrentTime(&_current_time);
  if (!status.ok()) {
    return;
  }
  const uint64_t current_time = static_cast<uint64_t>(_current_time);

  for (int level = 0; level < num_levels() - 1; level++) {
    for (FileMetaData* f : files_[level]) {
      if (!f->being_compacted) {
        uint64_t oldest_ancestor_time = f->TryGetOldestAncestorTime();
        if (oldest_ancestor_time > 0 &&
            oldest_ancestor_time < (current_time - ttl)) {
          expired_ttl_files_.emplace_back(level, f);
        }
      }
    }
  }
}

void VersionStorageInfo::ComputeFilesMarkedForPeriodicCompaction(
    const ImmutableOptions& ioptions,
    const uint64_t periodic_compaction_seconds, int last_level) {
  files_marked_for_periodic_compaction_.clear();
  if (periodic_compaction_seconds == 0) {
    return;
  }

  int64_t temp_current_time;
  auto status = ioptions.clock->GetCurrentTime(&temp_current_time);
  if (!status.ok()) {
    return;
  }
  const uint64_t current_time = static_cast<uint64_t>(temp_current_time);

  // If periodic_compaction_seconds is larger than current time, periodic
  // compaction can't possibly be triggered.
  if (periodic_compaction_seconds > current_time) {
    return;
  }

  // Existing offpeak behavior pulls a file's deadline earlier by the time until
  // the next daily offpeak window (so a whole TTL's worth can be marked at the
  // start of offpeak).
  const OffpeakTimeInfo offpeak_time_info =
      offpeak_time_option_.GetOffpeakTimeInfo(current_time);
  const uint64_t offpeak_pull =
      offpeak_time_info.is_now_offpeak
          ? offpeak_time_info.seconds_till_next_offpeak_start
          : 0;

  // Preferred-phase scheduling (see
  // DBOptions::periodic_compaction_phase_recovery_percent). When
  // recovery_percent == 0 this is disabled and marking matches the classic
  // "file age >= periodic_compaction_seconds" behavior (adjusted for offpeak).
  const PeriodicCompactionPhaseParams& phase_params =
      periodic_compaction_phase_params_;

  for (int level = 0; level <= last_level; level++) {
    for (auto f : files_[level]) {
      if (!f->being_compacted) {
        // Compute a file's modification time in the following order:
        // 1. Use file_creation_time table property if it is > 0.
        // 2. Use oldest_ancestor_time table property if it is > 0.
        // 3. Use file's mtime metadata if the above two table properties are 0.
        // Don't consider the file at all if the modification time cannot be
        // correctly determined based on the above conditions.
        uint64_t file_modification_time = f->TryGetFileCreationTime();
        if (file_modification_time == kUnknownFileCreationTime) {
          file_modification_time = f->TryGetOldestAncestorTime();
        }
        if (file_modification_time == kUnknownOldestAncestorTime) {
          auto file_path = TableFileName(ioptions.cf_paths, f->fd.GetNumber(),
                                         f->fd.GetPathId());
          status = ioptions.env->GetFileModificationTime(
              file_path, &file_modification_time);
          if (!status.ok()) {
            ROCKS_LOG_WARN(ioptions.logger,
                           "Can't get file modification time: %s: %s",
                           file_path.c_str(), status.ToString().c_str());
            continue;
          }
        }
        if (file_modification_time == 0) {
          continue;
        }

        const uint64_t trigger_time = PeriodicCompactionPhaser::TriggerTime(
            file_modification_time, periodic_compaction_seconds, phase_params);
        if (current_time + offpeak_pull > trigger_time) {
          files_marked_for_periodic_compaction_.emplace_back(level, f);
        }
      }
    }
  }
}

void VersionStorageInfo::ComputeFilesMarkedForForcedBlobGC(
    double blob_garbage_collection_age_cutoff,
    double blob_garbage_collection_force_threshold,
    bool enable_blob_garbage_collection) {
  files_marked_for_forced_blob_gc_.clear();
  if (!(enable_blob_garbage_collection &&
        blob_garbage_collection_age_cutoff > 0.0 &&
        blob_garbage_collection_force_threshold < 1.0)) {
    return;
  }

  if (blob_files_.empty()) {
    return;
  }

  // Number of blob files eligible for GC based on age
  const size_t cutoff_count = static_cast<size_t>(
      blob_garbage_collection_age_cutoff * blob_files_.size());
  if (!cutoff_count) {
    return;
  }

  // Compute the sum of total and garbage bytes over the batch of blob files
  // currently eligible for garbage collection based on
  // blob_garbage_collection_age_cutoff, and if the garbage ratio exceeds
  // blob_garbage_collection_force_threshold, schedule compaction for the
  // SST files that reference the oldest batch of blob files. Here is a toy
  // example. Let's assume we have three SSTs 1, 2, and 3, and four blob files
  // 10, 11, 12, and 13, which correspond to the range that is eligible for GC
  // and satisfy the garbage ratio threshold. Also, let's say SSTs 1 and 2 both
  // rely on blob file 10 and potentially some higher-numbered ones, while SST 3
  // relies on blob file 12 and potentially some higher-numbered ones. Then, the
  // SST to oldest blob file mapping is as follows:
  //
  // SST file number               Oldest blob file number
  // 1                             10
  // 2                             10
  // 3                             12
  //
  // This is what the same thing looks like from the blob files' POV. (Note that
  // the linked SSTs simply denote the inverse mapping of the above.)
  //
  // Blob file number              Linked SST set
  // 10                            {1, 2}
  // 11                            {}
  // 12                            {3}
  // 13                            {}
  //
  // Then, the oldest batch of blob files consists of blob files 10 and 11,
  // and we can get rid of them by forcing the compaction of SSTs 1 and 2.
  const auto& oldest_meta = blob_files_.front();
  assert(oldest_meta);

  const auto& linked_ssts = oldest_meta->GetLinkedSsts();
  assert(!linked_ssts.empty());

  size_t count = 1;
  uint64_t sum_total_blob_bytes = oldest_meta->GetTotalBlobBytes();
  uint64_t sum_garbage_blob_bytes = oldest_meta->GetGarbageBlobBytes();

  assert(cutoff_count <= blob_files_.size());

  for (; count < cutoff_count; ++count) {
    const auto& meta = blob_files_[count];
    assert(meta);

    sum_total_blob_bytes += meta->GetTotalBlobBytes();
    sum_garbage_blob_bytes += meta->GetGarbageBlobBytes();
  }

  if (sum_garbage_blob_bytes <
      blob_garbage_collection_force_threshold * sum_total_blob_bytes) {
    return;
  }

  for (uint64_t sst_file_number : linked_ssts) {
    const FileLocation location = GetFileLocation(sst_file_number);
    assert(location.IsValid());

    const int level = location.GetLevel();
    assert(level >= 0);

    const size_t pos = location.GetPosition();

    FileMetaData* const sst_meta = files_[level][pos];
    assert(sst_meta);

    if (sst_meta->being_compacted) {
      continue;
    }

    files_marked_for_forced_blob_gc_.emplace_back(level, sst_meta);
  }
}

void VersionStorageInfo::ComputeFilesMarkedForReadTriggeredCompaction(
    double threshold, CompactionStyle compaction_style) {
  read_triggered_compaction_files_.clear();
  if (threshold <= 0.0 || compaction_style == kCompactionStyleFIFO) {
    return;
  }

  // Skip files at the last non-empty level -- there is no lower level to
  // compact into. Exception: L0 files are allowed even when L0 is the last
  // non-empty level, because in single-level universal or FIFO compaction, L0
  // files can be compacted together (L0 -> L0).
  int last_non_empty = num_non_empty_levels_ - 1;

  for (int level = 0; level < num_levels(); level++) {
    if (level >= last_non_empty && level != 0) {
      continue;
    }
    for (auto* f : files_[level]) {
      if (f->being_compacted) {
        continue;
      }
      uint64_t file_size = f->fd.GetFileSize();
      if (file_size == 0) {
        continue;
      }
      double reads_per_byte =
          static_cast<double>(f->stats.num_collapsible_entry_reads_sampled.load(
              std::memory_order_relaxed)) /
          static_cast<double>(file_size);
      if (reads_per_byte > threshold) {
        read_triggered_compaction_files_.emplace_back(level, f);
      }
    }
  }

  // Snapshot reads_per_byte for each file so the sort comparator sees stable
  // values.
  std::unordered_map<const FileMetaData*, double> reads_per_byte_snapshot;
  reads_per_byte_snapshot.reserve(read_triggered_compaction_files_.size());
  for (const auto& entry : read_triggered_compaction_files_) {
    const auto* f = entry.second;
    uint64_t file_size = f->fd.GetFileSize();
    reads_per_byte_snapshot[f] =
        file_size > 0 ? static_cast<double>(
                            f->stats.num_collapsible_entry_reads_sampled.load(
                                std::memory_order_relaxed)) /
                            static_cast<double>(file_size)
                      : 0.0;
  }

  // Sort by reads_per_byte descending so the hottest files are picked first
  std::sort(read_triggered_compaction_files_.begin(),
            read_triggered_compaction_files_.end(),
            [&reads_per_byte_snapshot](const auto& a, const auto& b) {
              return reads_per_byte_snapshot.at(a.second) >
                     reads_per_byte_snapshot.at(b.second);
            });
}

namespace {

// used to sort files by size
struct Fsize {
  size_t index;
  FileMetaData* file;
};

// Comparator that is used to sort files based on their size
// In normal mode: descending size
bool CompareCompensatedSizeDescending(const Fsize& first, const Fsize& second) {
  return (first.file->compensated_file_size >
          second.file->compensated_file_size);
}
}  // anonymous namespace

void VersionStorageInfo::AddFile(int level, FileMetaData* f) {
  auto& level_files = files_[level];
  level_files.push_back(f);

  f->refs++;
}

void VersionStorageInfo::AddBlobFile(
    std::shared_ptr<BlobFileMetaData> blob_file_meta) {
  assert(blob_file_meta);

  assert(blob_files_.empty() ||
         (blob_files_.back() && blob_files_.back()->GetBlobFileNumber() <
                                    blob_file_meta->GetBlobFileNumber()));

  blob_files_.emplace_back(std::move(blob_file_meta));
}

VersionStorageInfo::BlobFiles::const_iterator
VersionStorageInfo::GetBlobFileMetaDataLB(uint64_t blob_file_number) const {
  return std::lower_bound(
      blob_files_.begin(), blob_files_.end(), blob_file_number,
      [](const std::shared_ptr<BlobFileMetaData>& lhs, uint64_t rhs) {
        assert(lhs);
        return lhs->GetBlobFileNumber() < rhs;
      });
}

void VersionStorageInfo::SetFinalized() {
  finalized_ = true;

#ifndef NDEBUG
  if (compaction_style_ != kCompactionStyleLevel) {
    // Not level based compaction.
    return;
  }
  assert(base_level_ < 0 || num_levels() == 1 ||
         (base_level_ >= 1 && base_level_ < num_levels()));
  // Verify all levels newer than base_level are empty except L0
  for (int level = 1; level < base_level(); level++) {
    assert(NumLevelBytes(level) == 0);
  }
  uint64_t max_bytes_prev_level = 0;
  for (int level = base_level(); level < num_levels() - 1; level++) {
    if (LevelFiles(level).size() == 0) {
      continue;
    }
    assert(MaxBytesForLevel(level) >= max_bytes_prev_level);
    max_bytes_prev_level = MaxBytesForLevel(level);
  }
  for (int level = 0; level < num_levels(); level++) {
    assert(LevelFiles(level).size() == 0 ||
           LevelFiles(level).size() == LevelFilesBrief(level).num_files);
    if (LevelFiles(level).size() > 0) {
      assert(level < num_non_empty_levels());
    }
  }
  assert(compaction_level_.size() > 0);
  assert(compaction_level_.size() == compaction_score_.size());
#endif
}

void VersionStorageInfo::UpdateNumNonEmptyLevels() {
  num_non_empty_levels_ = num_levels_;
  for (int i = num_levels_ - 1; i >= 0; i--) {
    if (files_[i].size() != 0) {
      return;
    } else {
      num_non_empty_levels_ = i;
    }
  }
}

namespace {
// Sort `temp` based on ratio of overlapping size over file size
void SortFileByOverlappingRatio(
    const InternalKeyComparator& icmp, const std::vector<FileMetaData*>& files,
    const std::vector<FileMetaData*>& next_level_files, SystemClock* clock,
    int level, int num_non_empty_levels, uint64_t ttl,
    std::vector<Fsize>* temp) {
  std::unordered_map<uint64_t, uint64_t> file_to_order;
  auto next_level_it = next_level_files.begin();

  int64_t curr_time;
  Status status = clock->GetCurrentTime(&curr_time);
  if (!status.ok()) {
    // If we can't get time, disable TTL.
    ttl = 0;
  }

  FileTtlBooster ttl_booster(static_cast<uint64_t>(curr_time), ttl,
                             num_non_empty_levels, level);

  for (auto& file : files) {
    uint64_t overlapping_bytes = 0;
    // Skip files in next level that is smaller than current file
    while (next_level_it != next_level_files.end() &&
           icmp.Compare((*next_level_it)->largest, file->smallest) < 0) {
      next_level_it++;
    }

    while (next_level_it != next_level_files.end() &&
           icmp.Compare((*next_level_it)->smallest, file->largest) < 0) {
      overlapping_bytes += (*next_level_it)->fd.file_size;

      if (icmp.Compare((*next_level_it)->largest, file->largest) > 0) {
        // next level file cross large boundary of current file.
        break;
      }
      next_level_it++;
    }

    uint64_t ttl_boost_score = (ttl > 0) ? ttl_booster.GetBoostScore(file) : 1;
    assert(ttl_boost_score > 0);
    assert(file->compensated_file_size != 0);
    file_to_order[file->fd.GetNumber()] = overlapping_bytes * 1024U /
                                          file->compensated_file_size /
                                          ttl_boost_score;
  }

  size_t num_to_sort = temp->size() > VersionStorageInfo::kNumberFilesToSort
                           ? VersionStorageInfo::kNumberFilesToSort
                           : temp->size();

  std::partial_sort(
      temp->begin(), temp->begin() + num_to_sort, temp->end(),
      [&](const Fsize& f1, const Fsize& f2) -> bool {
        // If score is the same, pick file with smaller keys.
        // This makes the algorithm more deterministic, and also
        // help the trivial move case to have more files to
        // extend.
        if (f1.file->marked_for_compaction == f2.file->marked_for_compaction) {
          if (file_to_order[f1.file->fd.GetNumber()] ==
              file_to_order[f2.file->fd.GetNumber()]) {
            return icmp.Compare(f1.file->smallest, f2.file->smallest) < 0;
          }
          return file_to_order[f1.file->fd.GetNumber()] <
                 file_to_order[f2.file->fd.GetNumber()];
        } else {
          return f1.file->marked_for_compaction >
                 f2.file->marked_for_compaction;
        }
      });
}

void SortFileByRoundRobin(const InternalKeyComparator& icmp,
                          std::vector<InternalKey>* compact_cursor,
                          bool level0_non_overlapping, int level,
                          std::vector<Fsize>* temp) {
  if (level == 0 && !level0_non_overlapping) {
    // Using kOldestSmallestSeqFirst when level === 0, since the
    // files may overlap (not fully sorted)
    std::sort(temp->begin(), temp->end(),
              [](const Fsize& f1, const Fsize& f2) -> bool {
                return f1.file->fd.smallest_seqno < f2.file->fd.smallest_seqno;
              });
    return;
  }

  bool should_move_files =
      compact_cursor->at(level).size() > 0 && temp->size() > 1;

  // The iterator points to the Fsize with smallest key larger than or equal to
  // the given cursor
  std::vector<Fsize>::iterator current_file_iter;
  if (should_move_files) {
    // Find the file of which the smallest key is larger than or equal to
    // the cursor (the smallest key in the successor file of the last
    // chosen file), skip this if the cursor is invalid or there is only
    // one file in this level
    current_file_iter = std::lower_bound(
        temp->begin(), temp->end(), compact_cursor->at(level),
        [&](const Fsize& f, const InternalKey& cursor) -> bool {
          return icmp.Compare(cursor, f.file->smallest) > 0;
        });

    should_move_files =
        current_file_iter != temp->end() && current_file_iter != temp->begin();
  }
  if (should_move_files) {
    // Construct a local temporary vector
    std::vector<Fsize> local_temp;
    local_temp.reserve(temp->size());
    // Move the selected File into the first position and its successors
    // into the second, third, ..., positions
    for (auto iter = current_file_iter; iter != temp->end(); iter++) {
      local_temp.push_back(*iter);
    }
    // Move the origin predecessors of the selected file in a round-robin
    // manner
    for (auto iter = temp->begin(); iter != current_file_iter; iter++) {
      local_temp.push_back(*iter);
    }
    // Replace all the items in temp
    for (size_t i = 0; i < local_temp.size(); i++) {
      temp->at(i) = local_temp[i];
    }
  }
}
}  // anonymous namespace

void VersionStorageInfo::UpdateFilesByCompactionPri(
    const ImmutableOptions& ioptions, const MutableCFOptions& options) {
  if (compaction_style_ == kCompactionStyleNone ||
      compaction_style_ == kCompactionStyleFIFO ||
      compaction_style_ == kCompactionStyleUniversal) {
    // don't need this
    return;
  }
  // No need to sort the highest level because it is never compacted.
  for (int level = 0; level < num_levels() - 1; level++) {
    const std::vector<FileMetaData*>& files = files_[level];
    auto& files_by_compaction_pri = files_by_compaction_pri_[level];
    assert(files_by_compaction_pri.size() == 0);

    // populate a temp vector for sorting based on size
    std::vector<Fsize> temp(files.size());
    for (size_t i = 0; i < files.size(); i++) {
      temp[i].index = i;
      temp[i].file = files[i];
    }

    // sort the top kNumberFilesToSort based on file size
    size_t num = VersionStorageInfo::kNumberFilesToSort;
    if (num > temp.size()) {
      num = temp.size();
    }
    switch (ioptions.compaction_pri) {
      case kByCompensatedSize:
        std::partial_sort(temp.begin(), temp.begin() + num, temp.end(),
                          CompareCompensatedSizeDescending);
        break;
      case kOldestLargestSeqFirst:
        std::sort(temp.begin(), temp.end(),
                  [](const Fsize& f1, const Fsize& f2) -> bool {
                    return f1.file->fd.largest_seqno <
                           f2.file->fd.largest_seqno;
                  });
        break;
      case kOldestSmallestSeqFirst:
        std::sort(temp.begin(), temp.end(),
                  [](const Fsize& f1, const Fsize& f2) -> bool {
                    return f1.file->fd.smallest_seqno <
                           f2.file->fd.smallest_seqno;
                  });
        break;
      case kMinOverlappingRatio:
        SortFileByOverlappingRatio(*internal_comparator_, files_[level],
                                   files_[level + 1], ioptions.clock, level,
                                   num_non_empty_levels_, options.ttl, &temp);
        break;
      case kRoundRobin:
        SortFileByRoundRobin(*internal_comparator_, &compact_cursor_,
                             level0_non_overlapping_, level, &temp);
        break;
      default:
        assert(false);
    }
    assert(temp.size() == files.size());

    // initialize files_by_compaction_pri_
    for (size_t i = 0; i < temp.size(); i++) {
      files_by_compaction_pri.push_back(static_cast<int>(temp[i].index));
    }
    next_file_to_compact_by_size_[level] = 0;
    assert(files_[level].size() == files_by_compaction_pri_[level].size());
  }
}

void VersionStorageInfo::GenerateLevel0NonOverlapping() {
  assert(!finalized_);
  level0_non_overlapping_ = true;
  if (level_files_brief_.size() == 0) {
    return;
  }

  // A copy of L0 files sorted by smallest key
  std::vector<FdWithKeyRange> level0_sorted_file(
      level_files_brief_[0].files,
      level_files_brief_[0].files + level_files_brief_[0].num_files);
  std::sort(level0_sorted_file.begin(), level0_sorted_file.end(),
            [this](const FdWithKeyRange& f1, const FdWithKeyRange& f2) -> bool {
              return (internal_comparator_->Compare(f1.smallest_key,
                                                    f2.smallest_key) < 0);
            });

  for (size_t i = 1; i < level0_sorted_file.size(); ++i) {
    FdWithKeyRange& f = level0_sorted_file[i];
    FdWithKeyRange& prev = level0_sorted_file[i - 1];
    if (internal_comparator_->Compare(prev.largest_key, f.smallest_key) >= 0) {
      level0_non_overlapping_ = false;
      break;
    }
  }
}

void VersionStorageInfo::GenerateBottommostFiles() {
  assert(!finalized_);
  assert(bottommost_files_.empty());
  for (size_t level = 0; level < level_files_brief_.size(); ++level) {
    for (size_t file_idx = 0; file_idx < level_files_brief_[level].num_files;
         ++file_idx) {
      const FdWithKeyRange& f = level_files_brief_[level].files[file_idx];
      int l0_file_idx;
      if (level == 0) {
        l0_file_idx = static_cast<int>(file_idx);
      } else {
        l0_file_idx = -1;
      }
      Slice smallest_user_key = ExtractUserKey(f.smallest_key);
      Slice largest_user_key = ExtractUserKey(f.largest_key);
      if (!RangeMightExistAfterSortedRun(smallest_user_key, largest_user_key,
                                         static_cast<int>(level),
                                         l0_file_idx)) {
        bottommost_files_.emplace_back(static_cast<int>(level),
                                       f.file_metadata);
      }
    }
  }
}

void VersionStorageInfo::GenerateFileLocationIndex() {
  size_t num_files = 0;

  for (int level = 0; level < num_levels_; ++level) {
    num_files += files_[level].size();
  }

  file_locations_.reserve(num_files);

  for (int level = 0; level < num_levels_; ++level) {
    for (size_t pos = 0; pos < files_[level].size(); ++pos) {
      const FileMetaData* const meta = files_[level][pos];
      assert(meta);

      const uint64_t file_number = meta->fd.GetNumber();

      assert(file_locations_.find(file_number) == file_locations_.end());
      file_locations_.emplace(file_number, FileLocation(level, pos));
    }
  }
}

void VersionStorageInfo::UpdateOldestSnapshot(
    SequenceNumber seqnum, bool allow_ingest_behind, const Comparator* ucmp,
    const std::string& full_history_ts_low) {
  assert(seqnum >= oldest_snapshot_seqnum_);
  oldest_snapshot_seqnum_ = seqnum;
  if (oldest_snapshot_seqnum_ > bottommost_files_mark_threshold_) {
    ComputeBottommostFilesMarkedForCompaction(allow_ingest_behind, ucmp,
                                              full_history_ts_low);
  }
}

void VersionStorageInfo::ComputeBottommostFilesMarkedForCompaction(
    bool allow_ingest_behind, const Comparator* ucmp,
    const std::string& full_history_ts_low) {
  bottommost_files_marked_for_compaction_.clear();
  bottommost_files_mark_threshold_ = kMaxSequenceNumber;
  if (allow_ingest_behind) {
    return;
  }
  // If a file's creation time is larger than creation_time_ub,
  // it is too new to be marked for compaction.
  int64_t creation_time_ub = 0;
  bool needs_delay = bottommost_file_compaction_delay_ > 0;
  if (needs_delay) {
    int64_t current_time = 0;
    clock_->GetCurrentTime(&current_time).PermitUncheckedError();
    // Note that if GetCurrentTime() fails, current_time will be 0.
    // We will treat it as is and treat all files as too new.
    // The subtraction will not underflow since
    // bottommost_file_compaction_delay_ is of type uint32_t.
    creation_time_ub =
        current_time - static_cast<int64_t>(bottommost_file_compaction_delay_);
  }

  // A bottommost file should only be marked for compaction when that compaction
  // could actually zero out its largest sequence number; otherwise the rewrite
  // makes no progress and the file is re-marked forever (infinite compaction
  // loop). The zeroability conditions -- the seqno->time "preserve" window and
  // the UDT history cutoff -- are shared with CompactionIterator::PrepareOutput
  // through BottommostSeqnoCanBeZeroed() so the two decisions cannot drift.
  // Snapshot visibility is checked separately (largest_seqno <
  // oldest_snapshot_seqnum_) since that gate legitimately differs between the
  // two sites.
  const size_t ts_sz = ucmp ? ucmp->timestamp_size() : 0;
  const bool full_history_ts_low_set = !full_history_ts_low.empty();
  // Max seqno a bottommost compaction could zero given the preserve window.
  // CompactionJob computes preserve_seqno_after_ the same way but then further
  // caps it with min(., earliest_snapshot_). We intentionally omit that cap
  // here: the snapshot-visibility gate above (largest_seqno <
  // oldest_snapshot_seqnum_) already guarantees a marked file's largest seqno
  // is below the earliest snapshot, so the cap can never make compaction unable
  // to zero a file we marked. Omitting it can at most leave unmarked a file a
  // compaction could still zero -- a missed optimization, not a loop.
  const SequenceNumber preserve_seqno_after =
      std::max(preserve_time_min_seqno_, SequenceNumber{1}) - 1;

  for (auto& level_and_file : bottommost_files_) {
    if (!level_and_file.second->being_compacted &&
        level_and_file.second->fd.largest_seqno != 0) {
      // largest_seqno might be nonzero due to containing the final key in an
      // earlier compaction, whose seqnum we didn't zero out.
      if (level_and_file.second->fd.largest_seqno < oldest_snapshot_seqnum_) {
        // Compute the file's timestamp collapsibility (only meaningful under
        // UDT with full_history_ts_low set). A file whose max timestamp is
        // below full_history_ts_low can have its timestamp history collapsed
        // and its seqno zeroed. An unknown (empty) max timestamp means the file
        // predates timestamp metadata (e.g. written by an older version); mark
        // it once so a bounded compaction backfills FileMetaData::max_timestamp
        // and reclaims obsolete versions/tombstones. When full_history_ts_low
        // is unset, the guard below (full_history_ts_low_set == false) keeps
        // such a file unmarked, since compaction could not collapse it and the
        // rewrite would loop.
        bool ts_below_full_history_ts_low = false;
        if (ts_sz > 0 && full_history_ts_low_set) {
          const std::string& max_ts = level_and_file.second->max_timestamp;
          ts_below_full_history_ts_low =
              max_ts.empty() ||
              ucmp->CompareTimestamp(Slice(max_ts), full_history_ts_low) < 0;
        }
        if (!BottommostSeqnoCanBeZeroed(
                level_and_file.second->fd.largest_seqno, preserve_seqno_after,
                ts_sz, full_history_ts_low_set, ts_below_full_history_ts_low)) {
          continue;
        }

        if (!needs_delay) {
          bottommost_files_marked_for_compaction_.push_back(level_and_file);
        } else if (creation_time_ub > 0) {
          int64_t creation_time = static_cast<int64_t>(
              level_and_file.second->TryGetFileCreationTime());
          if (creation_time == kUnknownFileCreationTime ||
              creation_time <= creation_time_ub) {
            bottommost_files_marked_for_compaction_.push_back(level_and_file);
          } else {
            // Just ignore this file for both
            // bottommost_files_marked_for_compaction_ and
            // bottommost_files_mark_threshold_. The next time
            // this method is called, it will try this file again. The method
            // is called after a new Version creation (compaction, flush, etc.),
            // after a compaction is picked, and after a snapshot newer than
            // bottommost_files_mark_threshold_ is released.
          }
        } else {
          // creation_time_ub <= 0, all files are too new to be marked for
          // compaction.
        }
      } else {
        bottommost_files_mark_threshold_ =
            std::min(bottommost_files_mark_threshold_,
                     level_and_file.second->fd.largest_seqno);
      }
    }
  }
}

void Version::Ref() { ++refs_; }

bool Version::Unref() {
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
    return true;
  }
  return false;
}

bool VersionStorageInfo::OverlapInLevel(int level,
                                        const Slice* smallest_user_key,
                                        const Slice* largest_user_key) {
  if (level >= num_non_empty_levels_) {
    // empty level, no overlap
    return false;
  }
  return SomeFileOverlapsRange(*internal_comparator_, (level > 0),
                               level_files_brief_[level], smallest_user_key,
                               largest_user_key);
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
// If hint_index is specified, then it points to a file in the
// overlapping range.
// The file_index returns a pointer to any file in an overlapping range.
void VersionStorageInfo::GetOverlappingInputs(
    int level, const InternalKey* begin, const InternalKey* end,
    std::vector<FileMetaData*>* inputs, int hint_index, int* file_index,
    bool expand_range, const FileMetaData* starting_l0_file,
    InternalKey** next_smallest) const {
  if (level >= num_non_empty_levels_) {
    // this level is empty, no overlapping inputs
    return;
  }

  inputs->clear();
  if (file_index) {
    *file_index = -1;
  }
  const Comparator* user_cmp = user_comparator_;
  if (level > 0) {
    GetOverlappingInputsRangeBinarySearch(level, begin, end, inputs, hint_index,
                                          file_index, false, next_smallest);
    return;
  }

  if (next_smallest) {
    // next_smallest key only makes sense for non-level 0, where files are
    // non-overlapping
    *next_smallest = nullptr;
  }

  Slice user_begin;
  Slice user_end;
  if (begin != nullptr) {
    user_begin = begin->user_key();
  }
  if (end != nullptr) {
    user_end = end->user_key();
  }

  // index stores the file index need to check.
  std::list<size_t> index;
  size_t start_index = 0;
  if (starting_l0_file != nullptr) {
    uint64_t starting_file_number = starting_l0_file->fd.GetNumber();
    for (size_t i = 0; i < level_files_brief_[level].num_files; i++) {
      if (level_files_brief_[level].files[i].fd.GetNumber() ==
          starting_file_number) {
        start_index = i;
        break;
      }
    }
    assert(start_index < level_files_brief_[level].num_files);
  }
  for (size_t i = start_index; i < level_files_brief_[level].num_files; i++) {
    index.emplace_back(i);
  }

  while (!index.empty()) {
    bool found_overlapping_file = false;
    auto iter = index.begin();
    while (iter != index.end()) {
      FdWithKeyRange* f = &(level_files_brief_[level].files[*iter]);
      const Slice file_start = ExtractUserKey(f->smallest_key);
      const Slice file_limit = ExtractUserKey(f->largest_key);
      if (begin != nullptr &&
          user_cmp->CompareWithoutTimestamp(file_limit, user_begin) < 0) {
        // "f" is completely before specified range; skip it
        iter++;
      } else if (end != nullptr &&
                 user_cmp->CompareWithoutTimestamp(file_start, user_end) > 0) {
        // "f" is completely after specified range; skip it
        iter++;
      } else {
        // if overlap
        inputs->emplace_back(files_[level][*iter]);
        found_overlapping_file = true;
        // record the first file index.
        if (file_index && *file_index == -1) {
          *file_index = static_cast<int>(*iter);
        }
        // the related file is overlap, erase to avoid checking again.
        iter = index.erase(iter);
        if (expand_range) {
          if (begin != nullptr &&
              user_cmp->CompareWithoutTimestamp(file_start, user_begin) < 0) {
            user_begin = file_start;
          }
          if (end != nullptr &&
              user_cmp->CompareWithoutTimestamp(file_limit, user_end) > 0) {
            user_end = file_limit;
          }
        }
      }
    }
    // if all the files left are not overlap, break
    if (!found_overlapping_file) {
      break;
    }
  }
}

// Store in "*inputs" files in "level" that within range [begin,end]
// Guarantee a "clean cut" boundary between the files in inputs
// and the surrounding files and the maxinum number of files.
// This will ensure that no parts of a key are lost during compaction.
// If hint_index is specified, then it points to a file in the range.
// The file_index returns a pointer to any file in an overlapping range.
void VersionStorageInfo::GetCleanInputsWithinInterval(
    int level, const InternalKey* begin, const InternalKey* end,
    std::vector<FileMetaData*>* inputs, int hint_index, int* file_index) const {
  inputs->clear();
  if (file_index) {
    *file_index = -1;
  }
  if (level >= num_non_empty_levels_ || level == 0 ||
      level_files_brief_[level].num_files == 0) {
    // this level is empty, no inputs within range
    // also don't support clean input interval within L0
    return;
  }

  GetOverlappingInputsRangeBinarySearch(level, begin, end, inputs, hint_index,
                                        file_index, true /* within_interval */);
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
// Employ binary search to find at least one file that overlaps the
// specified range. From that file, iterate backwards and
// forwards to find all overlapping files.
// if within_range is set, then only store the maximum clean inputs
// within range [begin, end]. "clean" means there is a boundary
// between the files in "*inputs" and the surrounding files
void VersionStorageInfo::GetOverlappingInputsRangeBinarySearch(
    int level, const InternalKey* begin, const InternalKey* end,
    std::vector<FileMetaData*>* inputs, int hint_index, int* file_index,
    bool within_interval, InternalKey** next_smallest) const {
  assert(level > 0);

  auto user_cmp = user_comparator_;
  const FdWithKeyRange* files = level_files_brief_[level].files;
  const int num_files = static_cast<int>(level_files_brief_[level].num_files);

  // begin to use binary search to find lower bound
  // and upper bound.
  int start_index = 0;
  int end_index = num_files;

  if (begin != nullptr) {
    // if within_interval is true, with file_key would find
    // not overlapping ranges in std::lower_bound.
    auto cmp = [&user_cmp, &within_interval](const FdWithKeyRange& f,
                                             const InternalKey* k) {
      auto& file_key = within_interval ? f.file_metadata->smallest
                                       : f.file_metadata->largest;
      return sstableKeyCompare(user_cmp, file_key, *k) < 0;
    };

    start_index = static_cast<int>(
        std::lower_bound(files,
                         files + (hint_index == -1 ? num_files : hint_index),
                         begin, cmp) -
        files);

    if (start_index > 0 && within_interval) {
      bool is_overlapping = true;
      while (is_overlapping && start_index < num_files) {
        auto& pre_limit = files[start_index - 1].file_metadata->largest;
        auto& cur_start = files[start_index].file_metadata->smallest;
        is_overlapping = sstableKeyCompare(user_cmp, pre_limit, cur_start) == 0;
        start_index += is_overlapping;
      }
    }
  }

  if (end != nullptr) {
    // if within_interval is true, with file_key would find
    // not overlapping ranges in std::upper_bound.
    auto cmp = [&user_cmp, &within_interval](const InternalKey* k,
                                             const FdWithKeyRange& f) {
      auto& file_key = within_interval ? f.file_metadata->largest
                                       : f.file_metadata->smallest;
      return sstableKeyCompare(user_cmp, *k, file_key) < 0;
    };

    end_index = static_cast<int>(
        std::upper_bound(files + start_index, files + num_files, end, cmp) -
        files);

    if (end_index < num_files && within_interval) {
      bool is_overlapping = true;
      while (is_overlapping && end_index > start_index) {
        auto& next_start = files[end_index].file_metadata->smallest;
        auto& cur_limit = files[end_index - 1].file_metadata->largest;
        is_overlapping =
            sstableKeyCompare(user_cmp, cur_limit, next_start) == 0;
        end_index -= is_overlapping;
      }
    }
  }

  assert(start_index <= end_index);

  // If there were no overlapping files, return immediately.
  if (start_index == end_index) {
    if (next_smallest) {
      *next_smallest = nullptr;
    }
    return;
  }

  assert(start_index < end_index);

  // returns the index where an overlap is found
  if (file_index) {
    *file_index = start_index;
  }

  // insert overlapping files into vector
  for (int i = start_index; i < end_index; i++) {
    inputs->push_back(files_[level][i]);
  }

  if (next_smallest != nullptr) {
    // Provide the next key outside the range covered by inputs
    if (end_index < static_cast<int>(files_[level].size())) {
      **next_smallest = files_[level][end_index]->smallest;
    } else {
      *next_smallest = nullptr;
    }
  }
}

uint64_t VersionStorageInfo::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < num_levels());
  return TotalFileSize(files_[level]);
}

const char* VersionStorageInfo::LevelSummary(
    LevelSummaryStorage* scratch) const {
  int len = 0;
  if (compaction_style_ == kCompactionStyleLevel && num_levels() > 1) {
    assert(base_level_ < static_cast<int>(level_max_bytes_.size()));
    if (level_multiplier_ != 0.0) {
      len = snprintf(
          scratch->buffer, sizeof(scratch->buffer),
          "base level %d level multiplier %.2f max bytes base %" PRIu64 " ",
          base_level_, level_multiplier_, level_max_bytes_[base_level_]);
    }
  }
  len +=
      snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len, "files[");
  for (int i = 0; i < num_levels(); i++) {
    int sz = sizeof(scratch->buffer) - len;
    int ret = snprintf(scratch->buffer + len, sz, "%d ", int(files_[i].size()));
    if (ret < 0 || ret >= sz) {
      break;
    }
    len += ret;
  }
  if (len > 0) {
    // overwrite the last space
    --len;
  }
  len +=
      snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len,
               "] max score %.2f, estimated pending compaction bytes %" PRIu64,
               compaction_score_[0], estimated_compaction_needed_bytes_);

  if (!files_marked_for_compaction_.empty()) {
    snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len,
             " (%" ROCKSDB_PRIszt " files need compaction)",
             files_marked_for_compaction_.size());
  }

  return scratch->buffer;
}

const char* VersionStorageInfo::LevelFileSummary(FileSummaryStorage* scratch,
                                                 int level) const {
  int len = snprintf(scratch->buffer, sizeof(scratch->buffer), "files_size[");
  for (const auto& f : files_[level]) {
    int sz = sizeof(scratch->buffer) - len;
    char sztxt[16];
    AppendHumanBytes(f->fd.GetFileSize(), sztxt, sizeof(sztxt));
    int ret = snprintf(scratch->buffer + len, sz,
                       "#%" PRIu64 "(seq=%" PRIu64 ",sz=%s,%d) ",
                       f->fd.GetNumber(), f->fd.smallest_seqno, sztxt,
                       static_cast<int>(f->being_compacted));
    if (ret < 0 || ret >= sz) {
      break;
    }
    len += ret;
  }
  // overwrite the last space (only if files_[level].size() is non-zero)
  if (files_[level].size() && len > 0) {
    --len;
  }
  snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len, "]");
  return scratch->buffer;
}

bool VersionStorageInfo::HasMissingEpochNumber() const {
  for (int level = 0; level < num_levels_; ++level) {
    for (const FileMetaData* f : files_[level]) {
      if (f->epoch_number == kUnknownEpochNumber) {
        return true;
      }
    }
  }
  return false;
}

uint64_t VersionStorageInfo::GetMaxEpochNumberOfFiles() const {
  uint64_t max_epoch_number = kUnknownEpochNumber;
  for (int level = 0; level < num_levels_; ++level) {
    for (const FileMetaData* f : files_[level]) {
      max_epoch_number = std::max(max_epoch_number, f->epoch_number);
    }
  }
  return max_epoch_number;
}

void VersionStorageInfo::RecoverEpochNumbers(ColumnFamilyData* cfd,
                                             bool restart_epoch, bool force) {
  if (restart_epoch) {
    cfd->ResetNextEpochNumber();

    bool reserve_epoch_num_for_file_ingested_behind = cfd->AllowIngestBehind();
    if (reserve_epoch_num_for_file_ingested_behind) {
      uint64_t reserved_epoch_number = cfd->NewEpochNumber();
      assert(reserved_epoch_number ==
             kReservedEpochNumberForFileIngestedBehind);
      ROCKS_LOG_INFO(cfd->ioptions().info_log.get(),
                     "[%s]CF has reserved epoch number %" PRIu64
                     " for files ingested "
                     "behind since `Options::allow_ingest_behind` or "
                     "`Options::cf_allow_ingest_behind` is true",
                     cfd->GetName().c_str(), reserved_epoch_number);
    }
  }

  bool missing_epoch_number = HasMissingEpochNumber();
  if (missing_epoch_number || force) {
    for (int level = num_levels_ - 1; level >= 1; --level) {
      auto& files_at_level = files_[level];
      if (files_at_level.empty()) {
        continue;
      }
      uint64_t next_epoch_number = cfd->NewEpochNumber();
      for (FileMetaData* f : files_at_level) {
        f->epoch_number = next_epoch_number;
      }
    }
    for (auto file_meta_iter = files_[0].rbegin();
         file_meta_iter != files_[0].rend(); file_meta_iter++) {
      FileMetaData* f = *file_meta_iter;
      f->epoch_number = cfd->NewEpochNumber();
    }
    if (missing_epoch_number) {
      assert(epoch_number_requirement_ ==
             EpochNumberRequirement::kMightMissing);
      ROCKS_LOG_WARN(cfd->ioptions().info_log.get(),
                     "[%s]CF's epoch numbers are inferred based on seqno",
                     cfd->GetName().c_str());
      epoch_number_requirement_ = EpochNumberRequirement::kMustPresent;
    }
  } else {
    assert(epoch_number_requirement_ == EpochNumberRequirement::kMustPresent);
    cfd->SetNextEpochNumber(
        std::max(GetMaxEpochNumberOfFiles() + 1, cfd->GetNextEpochNumber()));
  }
}

uint64_t VersionStorageInfo::MaxNextLevelOverlappingBytes() {
  uint64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < num_levels() - 1; level++) {
    for (const auto& f : files_[level]) {
      GetOverlappingInputs(level + 1, &f->smallest, &f->largest, &overlaps);
      const uint64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

uint64_t VersionStorageInfo::MaxBytesForLevel(int level) const {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.
  assert(level >= 0);
  assert(level < static_cast<int>(level_max_bytes_.size()));
  return level_max_bytes_[level];
}

void VersionStorageInfo::CalculateBaseBytes(const ImmutableOptions& ioptions,
                                            const MutableCFOptions& options) {
  // Special logic to set number of sorted runs.
  // It is to match the previous behavior when all files are in L0.
  int num_l0_count = static_cast<int>(files_[0].size());
  if (compaction_style_ == kCompactionStyleUniversal) {
    // For universal compaction, we use level0 score to indicate
    // compaction score for the whole DB. Adding other levels as if
    // they are L0 files.
    for (int i = 1; i < num_levels(); i++) {
      if (!files_[i].empty()) {
        num_l0_count++;
      }
    }
  }
  set_l0_delay_trigger_count(num_l0_count);

  level_max_bytes_.resize(ioptions.num_levels);
  if (!ioptions.level_compaction_dynamic_level_bytes) {
    base_level_ = (ioptions.compaction_style == kCompactionStyleLevel) ? 1 : -1;

    // Calculate for static bytes base case
    for (int i = 0; i < ioptions.num_levels; ++i) {
      if (i == 0 && ioptions.compaction_style == kCompactionStyleUniversal) {
        level_max_bytes_[i] = options.max_bytes_for_level_base;
      } else if (i > 1) {
        level_max_bytes_[i] = MultiplyCheckOverflow(
            MultiplyCheckOverflow(level_max_bytes_[i - 1],
                                  options.max_bytes_for_level_multiplier),
            options.MaxBytesMultiplerAdditional(i - 1));
      } else {
        level_max_bytes_[i] = options.max_bytes_for_level_base;
      }
    }
  } else {
    assert(ioptions.compaction_style == kCompactionStyleLevel);
    uint64_t max_level_size = 0;

    int first_non_empty_level = -1;
    // Find size of non-L0 level of most data.
    // Cannot use the size of the last level because it can be empty or less
    // than previous levels after compaction.
    for (int i = 1; i < num_levels_; i++) {
      uint64_t total_size = 0;
      for (const auto& f : files_[i]) {
        total_size += f->fd.GetFileSize();
      }
      if (total_size > 0 && first_non_empty_level == -1) {
        first_non_empty_level = i;
      }
      if (total_size > max_level_size) {
        max_level_size = total_size;
      }
    }

    // Prefill every level's max bytes to disallow compaction from there.
    for (int i = 0; i < num_levels_; i++) {
      level_max_bytes_[i] = std::numeric_limits<uint64_t>::max();
    }

    lowest_unnecessary_level_ = -1;
    if (max_level_size == 0) {
      // No data for L1 and up. L0 compacts to last level directly.
      // No compaction from L1+ needs to be scheduled.
      base_level_ = num_levels_ - 1;
    } else {
      assert(first_non_empty_level >= 1);
      uint64_t base_bytes_max = options.max_bytes_for_level_base;
      uint64_t base_bytes_min = static_cast<uint64_t>(
          base_bytes_max / options.max_bytes_for_level_multiplier);

      // Try whether we can make last level's target size to be max_level_size
      uint64_t cur_level_size = max_level_size;
      for (int i = num_levels_ - 2; i >= first_non_empty_level; i--) {
        // Round up after dividing
        cur_level_size = static_cast<uint64_t>(
            cur_level_size / options.max_bytes_for_level_multiplier);
        if (lowest_unnecessary_level_ == -1 &&
            cur_level_size <= base_bytes_min &&
            (options.preclude_last_level_data_seconds == 0 ||
             i < num_levels_ - 2)) {
          // When per_key_placement is enabled, the proximal level is
          // necessary.
          lowest_unnecessary_level_ = i;
        }
      }

      // Calculate base level and its size.
      uint64_t base_level_size;
      if (cur_level_size <= base_bytes_min) {
        // If per_key_placement is not enabled,
        // either there is only one non-empty level after level 0,
        // which can less than base_bytes_min AND necessary,
        // or there is some unnecessary level.
        assert(first_non_empty_level == num_levels_ - 1 ||
               options.preclude_last_level_data_seconds > 0 ||
               lowest_unnecessary_level_ != -1);
        // Case 1. If we make target size of last level to be max_level_size,
        // target size of the first non-empty level would be smaller than
        // base_bytes_min. We set it be base_bytes_min.
        base_level_size = base_bytes_min + 1U;
        base_level_ = first_non_empty_level;
        if (base_level_ < num_levels_ - 1) {
          ROCKS_LOG_INFO(
              ioptions.logger,
              "More existing levels in DB than needed: all non-zero "
              "levels <= level %d are unnecessary.  "
              "max_bytes_for_level_multiplier may not be guaranteed.",
              lowest_unnecessary_level_);
        }
      } else {
        assert(lowest_unnecessary_level_ == -1);
        // Find base level (where L0 data is compacted to).
        base_level_ = first_non_empty_level;
        while (base_level_ > 1 && cur_level_size > base_bytes_max) {
          --base_level_;
          cur_level_size = static_cast<uint64_t>(
              cur_level_size / options.max_bytes_for_level_multiplier);
        }
        if (cur_level_size > base_bytes_max) {
          // Even L1 will be too large
          assert(base_level_ == 1);
          base_level_size = base_bytes_max;
        } else {
          base_level_size = std::max(static_cast<uint64_t>(1), cur_level_size);
        }
      }

      level_multiplier_ = options.max_bytes_for_level_multiplier;
      assert(base_level_size > 0);

      uint64_t level_size = base_level_size;
      for (int i = base_level_; i < num_levels_; i++) {
        if (i > base_level_) {
          level_size = MultiplyCheckOverflow(level_size, level_multiplier_);
        }
        // Don't set any level below base_bytes_max. Otherwise, the LSM can
        // assume an hourglass shape where L1+ sizes are smaller than L0. This
        // causes compaction scoring, which depends on level sizes, to favor L1+
        // at the expense of L0, which may fill up and stall.
        level_max_bytes_[i] = std::max(level_size, base_bytes_max);
      }
    }
  }
}

uint64_t VersionStorageInfo::EstimateLiveDataSize() const {
  // Estimate the live data size by adding up the size of a maximal set of
  // sst files with no range overlap in same or higher level. The less
  // compacted, the more optimistic (smaller) this estimate is. Also,
  // for multiple sorted runs within a level, file order will matter.
  uint64_t size = 0;

  auto ikey_lt = [this](InternalKey* x, InternalKey* y) {
    return internal_comparator_->Compare(*x, *y) < 0;
  };
  // (Ordered) map of largest keys in files being included in size estimate
  std::map<InternalKey*, FileMetaData*, decltype(ikey_lt)> ranges(ikey_lt);

  for (int l = num_levels_ - 1; l >= 0; l--) {
    bool found_end = false;
    for (auto file : files_[l]) {
      // Find the first file already included with largest key is larger than
      // the smallest key of `file`. If that file does not overlap with the
      // current file, none of the files in the map does. If there is
      // no potential overlap, we can safely insert the rest of this level
      // (if the level is not 0) into the map without checking again because
      // the elements in the level are sorted and non-overlapping.
      auto lb = (found_end && l != 0) ? ranges.end()
                                      : ranges.lower_bound(&file->smallest);
      found_end = (lb == ranges.end());
      if (found_end || internal_comparator_->Compare(
                           file->largest, (*lb).second->smallest) < 0) {
        ranges.emplace_hint(lb, &file->largest, file);
        size += file->fd.file_size;
      }
    }
  }

  // For BlobDB, the result also includes the exact value of live bytes in the
  // blob files of the version.
  for (const auto& meta : blob_files_) {
    assert(meta);

    size += meta->GetTotalBlobBytes();
    size -= meta->GetGarbageBlobBytes();
  }

  return size;
}

bool VersionStorageInfo::RangeMightExistAfterSortedRun(
    const Slice& smallest_user_key, const Slice& largest_user_key,
    int last_level, int last_l0_idx) {
  assert((last_l0_idx != -1) == (last_level == 0));
  // TODO(ajkr): this preserves earlier behavior where we considered an L0 file
  // bottommost only if it's the oldest L0 file and there are no files on older
  // levels. It'd be better to consider it bottommost if there's no overlap in
  // older levels/files.
  if (last_level == 0 &&
      last_l0_idx != static_cast<int>(LevelFiles(0).size() - 1)) {
    return true;
  }

  // Checks whether there are files living beyond the `last_level`. If lower
  // levels have files, it checks for overlap between [`smallest_key`,
  // `largest_key`] and those files. Bottomlevel optimizations can be made if
  // there are no files in lower levels or if there is no overlap with the files
  // in the lower levels.
  for (int level = last_level + 1; level < num_levels(); level++) {
    // The range is not in the bottommost level if there are files in lower
    // levels when the `last_level` is 0 or if there are files in lower levels
    // which overlap with [`smallest_key`, `largest_key`].
    if (files_[level].size() > 0 &&
        (last_level == 0 ||
         OverlapInLevel(level, &smallest_user_key, &largest_user_key))) {
      return true;
    }
  }
  return false;
}

Env::WriteLifeTimeHint VersionStorageInfo::CalculateSSTWriteHint(
    int level, CompactionStyleSet compaction_style_set) const {
  if (!compaction_style_set.Contains(compaction_style_)) {
    return Env::WLTH_NOT_SET;
  }

  switch (compaction_style_) {
    case kCompactionStyleLevel:
      if (level == 0) {
        return Env::WLTH_MEDIUM;
      }

      // L1: medium, L2: long, ...
      if (level - base_level_ >= 2) {
        return Env::WLTH_EXTREME;
      } else if (level < base_level_) {
        // There is no restriction which prevents level passed in to be smaller
        // than base_level.
        return Env::WLTH_MEDIUM;
      }
      return static_cast<Env::WriteLifeTimeHint>(
          level - base_level_ + static_cast<int>(Env::WLTH_MEDIUM));
    case kCompactionStyleUniversal:
      if (level == 0) {
        return Env::WLTH_SHORT;
      }
      if (level == 1) {
        return Env::WLTH_MEDIUM;
      }
      return Env::WLTH_LONG;
    default:
      return Env::WLTH_NOT_SET;
  }
}

void Version::AddLiveFiles(std::vector<uint64_t>* live_table_files,
                           std::vector<uint64_t>* live_blob_files) const {
  assert(live_table_files);
  assert(live_blob_files);

  for (int level = 0; level < storage_info_.num_levels(); ++level) {
    const auto& level_files = storage_info_.LevelFiles(level);
    for (const auto& meta : level_files) {
      assert(meta);

      live_table_files->emplace_back(meta->fd.GetNumber());
    }
  }

  const auto& blob_files = storage_info_.GetBlobFiles();
  for (const auto& meta : blob_files) {
    assert(meta);

    live_blob_files->emplace_back(meta->GetBlobFileNumber());
  }
}

void Version::RemoveLiveFiles(
    std::vector<ObsoleteFileInfo>& sst_delete_candidates,
    std::vector<ObsoleteBlobFileInfo>& blob_delete_candidates) const {
  for (ObsoleteFileInfo& fi : sst_delete_candidates) {
    if (!fi.only_delete_metadata &&
        storage_info()->GetFileLocation(fi.metadata->fd.GetNumber()) !=
            VersionStorageInfo::FileLocation::Invalid()) {
      fi.only_delete_metadata = true;
    }
  }

  blob_delete_candidates.erase(
      std::remove_if(
          blob_delete_candidates.begin(), blob_delete_candidates.end(),
          [this](ObsoleteBlobFileInfo& x) {
            return storage_info()->GetBlobFileMetaData(x.GetBlobFileNumber());
          }),
      blob_delete_candidates.end());
}

std::string Version::DebugString(bool hex, bool print_stats) const {
  std::string r;
  for (int level = 0; level < storage_info_.num_levels_; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123[1 .. 124]['a' .. 'd']
    //   20:43[124 .. 128]['e' .. 'g']
    //
    // if print_stats=true:
    //   17:123[1 .. 124]['a' .. 'd'](4096)
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" --- version# ");
    AppendNumberTo(&r, version_number_);
    if (storage_info_.compact_cursor_[level].Valid()) {
      r.append(" --- compact_cursor: ");
      r.append(storage_info_.compact_cursor_[level].DebugString(hex));
    }
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = storage_info_.files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->fd.GetNumber());
      r.push_back(':');
      AppendNumberTo(&r, files[i]->fd.GetFileSize());
      r.append("[");
      AppendNumberTo(&r, files[i]->fd.smallest_seqno);
      r.append(" .. ");
      AppendNumberTo(&r, files[i]->fd.largest_seqno);
      r.append("]");
      r.append("[");
      r.append(files[i]->smallest.DebugString(hex));
      r.append(" .. ");
      r.append(files[i]->largest.DebugString(hex));
      r.append("]");
      if (files[i]->oldest_blob_file_number != kInvalidBlobFileNumber) {
        r.append(" blob_file:");
        AppendNumberTo(&r, files[i]->oldest_blob_file_number);
      }
      if (print_stats) {
        r.append("(");
        r.append(std::to_string(
            files[i]->stats.num_reads_sampled.load(std::memory_order_relaxed)));
        r.append(")");
      }
      r.append("\n");
    }
  }

  const auto& blob_files = storage_info_.GetBlobFiles();
  if (!blob_files.empty()) {
    r.append("--- blob files --- version# ");
    AppendNumberTo(&r, version_number_);
    r.append(" ---\n");
    for (const auto& blob_file_meta : blob_files) {
      assert(blob_file_meta);

      r.append(blob_file_meta->DebugString());
      r.push_back('\n');
    }
  }

  return r;
}

// this is used to batch writes to the manifest file
struct VersionSet::ManifestWriter {
  Status status;
  bool done;
  InstrumentedCondVar cv;
  ColumnFamilyData* cfd;
  const autovector<VersionEdit*>& edit_list;
  const std::function<void(const Status&)> manifest_write_callback;
  int max_file_opening_threads;

  explicit ManifestWriter(
      InstrumentedMutex* mu, ColumnFamilyData* _cfd,
      const autovector<VersionEdit*>& e,
      const std::function<void(const Status&)>& manifest_wcb,
      int _max_file_opening_threads = 1)
      : done(false),
        cv(mu),
        cfd(_cfd),
        edit_list(e),
        manifest_write_callback(manifest_wcb),
        max_file_opening_threads(_max_file_opening_threads) {}
  // No copying allowed
  ManifestWriter(const ManifestWriter&) = delete;
  ManifestWriter& operator=(const ManifestWriter&) = delete;
  ManifestWriter(ManifestWriter&&) = delete;
  ManifestWriter& operator=(ManifestWriter&&) = delete;
  ~ManifestWriter() { status.PermitUncheckedError(); }

  bool IsAllVersionIndependentEdits() const {
    for (const auto& e : edit_list) {
      if (!e->IsWalManipulation() && !e->IsOptionsFileManipulation()) {
        return false;
      }
    }
    return true;
  }
};

Status AtomicGroupReadBuffer::AddEdit(VersionEdit* edit) {
  assert(edit);
  if (edit->IsInAtomicGroup()) {
    TEST_SYNC_POINT("AtomicGroupReadBuffer::AddEdit:AtomicGroup");
    if (replay_buffer_.empty()) {
      replay_buffer_.resize(edit->GetRemainingEntries() + 1);
      TEST_SYNC_POINT_CALLBACK(
          "AtomicGroupReadBuffer::AddEdit:FirstInAtomicGroup", edit);
    }
    read_edits_in_atomic_group_++;
    if (read_edits_in_atomic_group_ + edit->GetRemainingEntries() !=
        static_cast<uint32_t>(replay_buffer_.size())) {
      TEST_SYNC_POINT_CALLBACK(
          "AtomicGroupReadBuffer::AddEdit:IncorrectAtomicGroupSize", edit);
      return Status::Corruption("corrupted atomic group");
    }
    replay_buffer_[read_edits_in_atomic_group_ - 1] = *edit;
    if (read_edits_in_atomic_group_ == replay_buffer_.size()) {
      TEST_SYNC_POINT_CALLBACK(
          "AtomicGroupReadBuffer::AddEdit:LastInAtomicGroup", edit);
      return Status::OK();
    }
    return Status::OK();
  }

  // A normal edit.
  if (!replay_buffer().empty()) {
    TEST_SYNC_POINT_CALLBACK(
        "AtomicGroupReadBuffer::AddEdit:AtomicGroupMixedWithNormalEdits", edit);
    return Status::Corruption("corrupted atomic group");
  }
  return Status::OK();
}

bool AtomicGroupReadBuffer::IsFull() const {
  return read_edits_in_atomic_group_ == replay_buffer_.size();
}

bool AtomicGroupReadBuffer::IsEmpty() const { return replay_buffer_.empty(); }

void AtomicGroupReadBuffer::Clear() {
  read_edits_in_atomic_group_ = 0;
  replay_buffer_.clear();
}

VersionSet::VersionSet(
    const std::string& dbname, const ImmutableDBOptions* _db_options,
    const MutableDBOptions& mutable_db_options,
    const FileOptions& storage_options, Cache* table_cache,
    WriteBufferManager* write_buffer_manager, WriteController* write_controller,
    BlockCacheTracer* const block_cache_tracer,
    const std::shared_ptr<IOTracer>& io_tracer, const std::string& db_id,
    const std::string& db_session_id, const std::string& daily_offpeak_time_utc,
    ErrorHandler* error_handler, bool unchanging)
    : column_family_set_(new ColumnFamilySet(
          dbname, _db_options, storage_options, table_cache,
          write_buffer_manager, write_controller, block_cache_tracer, io_tracer,
          db_id, db_session_id, mutable_db_options.fast_sst_open)),
      table_cache_(table_cache),
      env_(_db_options->env),
      fs_(_db_options->fs, io_tracer),
      clock_(_db_options->clock),
      dbname_(dbname),
      db_id_(db_id),
      db_options_(_db_options),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      options_file_number_(0),
      options_file_size_(0),
      has_committed_options_file_number_(false),
      pending_manifest_file_number_(0),
      last_sequence_(0),
      last_allocated_sequence_(0),
      last_published_sequence_(0),
      prev_log_number_(0),
      current_version_number_(0),
      manifest_file_size_(0),
      manifest_recovery_last_valid_record_end_(0),
      manifest_last_valid_record_end_(0),
      manifest_last_valid_record_end_file_number_(0),
      force_new_manifest_on_open_(false),
      last_compacted_manifest_file_size_(0),
      file_options_(storage_options),
      block_cache_tracer_(block_cache_tracer),
      io_tracer_(io_tracer),
      db_session_id_(db_session_id),
      offpeak_time_option_(OffpeakTimeOption(daily_offpeak_time_utc)),
      error_handler_(error_handler),
      unchanging_(unchanging),
      closed_(false) {
  UpdatedMutableDbOptions(mutable_db_options, /*mu=*/nullptr);
}

Status VersionSet::Close(FSDirectory* db_dir, InstrumentedMutex* mu) {
  Status s;
  if (closed_ || unchanging_ || !manifest_file_number_ || !descriptor_log_) {
    return s;
  }

  std::string manifest_file_name =
      DescriptorFileName(dbname_, manifest_file_number_);
  uint64_t size = 0;
  IOStatus io_s = descriptor_log_->Close(WriteOptions());
  descriptor_log_.reset();
  TEST_SYNC_POINT("VersionSet::Close:AfterClose");
  if (io_s.ok()) {
    io_s = fs_->GetFileSize(manifest_file_name, IOOptions(), &size, nullptr);
  }
  if (!io_s.ok() || size != manifest_file_size_) {
    if (io_s.ok()) {
      // This means the size is not as expected. So we treat it as a
      // corruption and set io_s appropriately
      io_s = IOStatus::Corruption();
    }
    ColumnFamilyData* cfd = GetColumnFamilySet()->GetDefault();
    IOErrorInfo io_error_info(io_s, FileOperationType::kVerify,
                              manifest_file_name, /*length=*/size,
                              /*offset=*/0);

    for (auto& listener : cfd->ioptions().listeners) {
      listener->OnIOError(io_error_info);
    }
    io_s.PermitUncheckedError();
    io_error_info.io_status.PermitUncheckedError();
    ROCKS_LOG_ERROR(db_options_->info_log,
                    "MANIFEST verification on Close, "
                    "filename %s, expected size %" PRIu64
                    " failed with status %s and "
                    "actual size %" PRIu64 "\n",
                    manifest_file_name.c_str(), manifest_file_size_,
                    io_s.ToString().c_str(), size);
    VersionEdit edit;
    assert(cfd);
    s = LogAndApply(cfd, ReadOptions(), WriteOptions(), &edit, mu, db_dir);
  }

  // Content validation: read back the manifest and verify CRC + decode.
  // Loop up to 2 checks with 1 rewrite attempt in between, so we also verify
  // the rewritten manifest is healthy.
  if (s.ok() && verify_manifest_content_on_close_) {
    TEST_SYNC_POINT("VersionSet::Close:BeforeContentValidation");
    constexpr int kMaxContentChecks = 2;
    for (int content_check = 0; s.ok() && content_check < kMaxContentChecks;
         ++content_check) {
      // Re-read the manifest file name in case it was rotated by a rewrite
      std::string content_manifest_name =
          DescriptorFileName(dbname_, manifest_file_number_);
      std::unique_ptr<FSSequentialFile> manifest_file;
      IOStatus content_io_s = fs_->NewSequentialFile(
          content_manifest_name, fs_->OptimizeForManifestRead(file_options_),
          &manifest_file, nullptr);
      if (!content_io_s.ok()) {
        // Surface I/O errors to the caller -- users who call DB::Close() and
        // check the status should know about filesystem problems.
        s = content_io_s;
        ROCKS_LOG_ERROR(db_options_->info_log,
                        "MANIFEST content verification on Close: "
                        "could not open %s for reading: %s\n",
                        content_manifest_name.c_str(),
                        content_io_s.ToString().c_str());
        break;
      }
      std::unique_ptr<SequentialFileReader> manifest_file_reader(
          new SequentialFileReader(std::move(manifest_file),
                                   content_manifest_name,
                                   db_options_->log_readahead_size, io_tracer_,
                                   db_options_->listeners));
      LogReporter reporter;
      Status log_read_status;
      reporter.status = &log_read_status;
      log::Reader reader(nullptr, std::move(manifest_file_reader), &reporter,
                         /*checksum=*/true, /*log_num=*/0);
      Slice record;
      std::string scratch;
      bool content_corrupt = false;
      while (reader.ReadRecord(&record, &scratch,
                               WALRecoveryMode::kAbsoluteConsistency)) {
        VersionEdit edit;
        Status decode_s = edit.DecodeFrom(record);
        if (!decode_s.ok()) {
          content_corrupt = true;
          break;
        }
      }
      if (!content_corrupt && !log_read_status.ok()) {
        content_corrupt = true;
      }
      if (!content_corrupt) {
        // Manifest is healthy, no need to check again
        break;
      }
      RecordTick(db_options_->statistics.get(),
                 MANIFEST_VALIDATION_FAILURE_COUNT);
      IOStatus corrupt_io_s =
          IOStatus::Corruption("MANIFEST content validation failed");
      IOErrorInfo io_error_info(corrupt_io_s, FileOperationType::kVerify,
                                content_manifest_name, /*length=*/0,
                                /*offset=*/0);
      for (auto& listener : db_options_->listeners) {
        listener->OnIOError(io_error_info);
      }
      corrupt_io_s.PermitUncheckedError();
      io_error_info.io_status.PermitUncheckedError();
      if (content_check == 0) {
        // First check failed -- rewrite and verify again
        ROCKS_LOG_ERROR(db_options_->info_log,
                        "MANIFEST content verification on Close failed, "
                        "filename %s, rewriting manifest\n",
                        content_manifest_name.c_str());
        ColumnFamilyData* cfd = GetColumnFamilySet()->GetDefault();
        VersionEdit recovery_edit;
        assert(cfd);
        s = LogAndApply(cfd, ReadOptions(), WriteOptions(), &recovery_edit, mu,
                        db_dir);
      } else {
        // Rewritten manifest is also corrupt -- likely a recurring filesystem
        // issue. Surface it so DB::Close() callers can detect the problem.
        ROCKS_LOG_ERROR(db_options_->info_log,
                        "MANIFEST content verification on Close failed again "
                        "after rewrite, filename %s\n",
                        content_manifest_name.c_str());
        s = Status::Corruption(
            "MANIFEST content verification failed after rewrite: " +
            content_manifest_name);
      }
    }
  }

  closed_ = true;
  return s;
}

VersionSet::~VersionSet() {
  // Must clean up column families to make all files "obsolete"
  column_family_set_.reset();

  for (auto& file : obsolete_files_) {
    // NOTE: DB is shutting down, so file is probably not obsolete, just
    // no longer referenced by Versions in memory.
    // For more context, see comment on "table_cache_->EraseUnRefEntries()"
    // in DBImpl::CloseHelper().
    // Using uncache_aggressiveness=0 overrides any previous marking to
    // attempt to uncache the file's blocks (which after cleaning up
    // column families could cause use-after-free)
    TableCache::ReleaseObsolete(
        table_cache_, file.metadata->fd.GetNumber(),
        file.metadata->fd.pinned_reader.GetCacheHandle(),
        /*uncache_aggressiveness=*/0);
    file.DeleteMetadata();
  }
  obsolete_files_.clear();
  io_status_.PermitUncheckedError();
}

void VersionSet::ApplyCommittedOptionsFileNumber(uint64_t number) {
  options_file_number_ = number;
  has_committed_options_file_number_ = true;
}

void VersionSet::ApplyLegacyOptionsFileNumber(uint64_t number) {
  options_file_number_ = number;
  has_committed_options_file_number_ = false;
}

void VersionSet::Reset() {
  if (column_family_set_) {
    WriteBufferManager* wbm = column_family_set_->write_buffer_manager();
    WriteController* wc = column_family_set_->write_controller();
    FlushInitiator* flush_initiator = column_family_set_->flush_initiator();
    const bool fast_sst_open = column_family_set_->GetFastSstOpen();

    // Destruction removes column families one at a time. Detach the accounting
    // target first so each removal does not rescan all remaining column
    // families while falling back to another MANIFEST.
    column_family_set_->SetFlushInitiator(nullptr);

    // Clear TableCache to prevent use-after-free: Reset() deletes old
    // ColumnFamilySet but reuses table_cache_, which may contain
    // BlockBasedTable entries with dangling references to deleted CFD's
    // ioptions.
    if (table_cache_) {
      table_cache_->EraseUnRefEntries();
    }

    // db_id becomes the source of truth after DBImpl::Recover():
    // https://github.com/meta/rocksdb/blob/v7.3.1/db/db_impl/db_impl_open.cc#L527
    // Note: we may not be able to recover db_id from MANIFEST if
    // options.write_dbid_to_manifest is false (default).
    column_family_set_.reset(
        new ColumnFamilySet(dbname_, db_options_, file_options_, table_cache_,
                            wbm, wc, block_cache_tracer_, io_tracer_, db_id_,
                            db_session_id_, fast_sst_open));
    column_family_set_->SetFlushInitiator(flush_initiator);
  }
  db_id_.clear();
  next_file_number_.store(2);
  min_log_number_to_keep_.store(0);
  manifest_file_number_ = 0;
  options_file_number_ = 0;
  options_file_size_ = 0;
  has_committed_options_file_number_ = false;
  pending_manifest_file_number_ = 0;
  last_sequence_.store(0);
  last_allocated_sequence_.store(0);
  last_published_sequence_.store(0);
  prev_log_number_ = 0;
  descriptor_log_.reset();
  current_version_number_ = 0;
  manifest_writers_.clear();
  manifest_file_size_ = 0;
  manifest_recovery_last_valid_record_end_ = 0;
  manifest_last_valid_record_end_ = 0;
  manifest_last_valid_record_end_file_number_ = 0;
  force_new_manifest_on_open_ = false;
  last_compacted_manifest_file_size_ = 0;
  TuneMaxManifestFileSize();
  obsolete_files_.clear();
  obsolete_manifests_.clear();
  wals_.Reset();
}

void VersionSet::UpdatedMutableDbOptions(
    const MutableDBOptions& updated_options, InstrumentedMutex* mu) {
  // Must be holding mutex if not called during initialization
  if (mu) {
    mu->AssertHeld();
  } else {
    // manifest_file_size_ must be 0 if called from the constructor
    assert(manifest_file_size_ == 0);
  }
  file_options_.writable_file_max_buffer_size =
      updated_options.writable_file_max_buffer_size;
  min_max_manifest_file_size_ = updated_options.max_manifest_file_size;
  max_manifest_space_amp_pct_ = static_cast<unsigned>(
      std::max(updated_options.max_manifest_space_amp_pct, 0));
  manifest_preallocation_size_ = updated_options.manifest_preallocation_size;
  verify_manifest_content_on_close_ =
      updated_options.verify_manifest_content_on_close;

  if (periodic_compaction_phaser_.SetConfig(
          updated_options.periodic_compaction_phase_recovery_percent)) {
    // (Re)anchor phasing so that switching it on (at open or via SetDBOptions)
    // spreads the initial periodic-compaction catch-up burst over time rather
    // than triggering it all at once.
    int64_t now = 0;
    if (clock_ != nullptr && clock_->GetCurrentTime(&now).ok()) {
      periodic_compaction_phaser_.Reanchor(static_cast<uint64_t>(now));
    }
    if (mu != nullptr) {
      // Live SetDBOptions change (not construction): push the refreshed phase
      // params into each column family's current Version so the change takes
      // effect on the next periodic re-evaluation (which recomputes scores on
      // the current Version) rather than only when the next Version is built.
      // Safe because periodic_compaction_phase_params_ is read only under the
      // DB mutex, which is held here.
      for (auto* cfd : *column_family_set_) {
        if (cfd->IsDropped()) {
          continue;
        }
        Version* v = cfd->current();
        if (v != nullptr) {
          v->storage_info_.periodic_compaction_phase_params_ =
              GetPeriodicCompactionPhaseParams(cfd->GetID());
        }
      }
    }
  }

  TuneMaxManifestFileSize();
}

PeriodicCompactionPhaseParams VersionSet::GetPeriodicCompactionPhaseParams(
    uint32_t cf_id) const {
  return periodic_compaction_phaser_.ParamsForCf(cf_id);
}

void VersionSet::ReanchorCompactionPhase() {
  // Caller holds the DB mutex. Move the phasing anchor to now and refresh each
  // CF's current Version's cached params, mirroring the refresh done for
  // seed/recovery changes in UpdatedMutableDbOptions(). Used when a CF's
  // periodic_compaction_seconds changes, so a turn-down's newly past-due cohort
  // is spread rather than fired at once. Re-anchoring is DB-level and benign to
  // CFs whose interval did not change (their not-past-due files keep phasing).
  int64_t now = 0;
  if (clock_ != nullptr && clock_->GetCurrentTime(&now).ok()) {
    periodic_compaction_phaser_.Reanchor(static_cast<uint64_t>(now));
  }
  for (auto* cfd : *column_family_set_) {
    if (cfd->IsDropped()) {
      continue;
    }
    Version* v = cfd->current();
    if (v != nullptr) {
      v->storage_info_.periodic_compaction_phase_params_ =
          GetPeriodicCompactionPhaseParams(cfd->GetID());
    }
  }
}

void VersionSet::TuneMaxManifestFileSize() {
  tuned_max_manifest_file_size_ =
      std::max(min_max_manifest_file_size_,
               last_compacted_manifest_file_size_ *
                   (100U + max_manifest_space_amp_pct_) / 100U);
}

void VersionSet::AppendVersion(ColumnFamilyData* column_family_data,
                               Version* v) {
  // compute new compaction score
  v->storage_info()->ComputeCompactionScore(
      column_family_data->ioptions(),
      column_family_data->GetLatestMutableCFOptions(),
      column_family_data->GetFullHistoryTsLow());

  // Mark v finalized
  v->storage_info_.SetFinalized();

  // Make "v" current
  assert(v->refs_ == 0);
  Version* current = column_family_data->current();
  assert(v != current);
  if (current != nullptr) {
    assert(current->refs_ > 0);
    current->Unref();
  }
  column_family_data->SetCurrent(v);
  v->Ref();

  // Append to linked list
  v->prev_ = column_family_data->dummy_versions()->prev_;
  v->next_ = column_family_data->dummy_versions();
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

Status VersionSet::ProcessManifestWrites(
    std::deque<ManifestWriter>& writers, InstrumentedMutex* mu,
    FSDirectory* dir_contains_current_file, bool new_descriptor_log,
    const ColumnFamilyOptions* new_cf_options,
    bool new_cf_options_are_sanitized, const ReadOptions& read_options,
    const WriteOptions& write_options) {
  mu->AssertHeld();
  assert(!writers.empty());
  ManifestWriter& first_writer = writers.front();
  ManifestWriter* last_writer = &first_writer;

  // Takes this batch, from the queue head through `last_writer`, off
  // manifest_writers_: records `s` on each writer, runs its callback, and wakes
  // writers other threads wait on, then the next batch's leader. Every return
  // path must do this, because the writers in `writers` live on the caller's
  // stack and a queued pointer to one would outlive them.
  auto finish_batch = [&](const Status& s) {
    while (true) {
      ManifestWriter* ready = manifest_writers_.front();
      manifest_writers_.pop_front();
      bool need_signal = true;
      for (const auto& w : writers) {
        if (&w == ready) {
          need_signal = false;
          break;
        }
      }
      ready->status = s;
      ready->done = true;
      if (ready->manifest_write_callback) {
        (ready->manifest_write_callback)(s);
      }
      if (need_signal) {
        ready->cv.Signal();
      }
      if (ready == last_writer) {
        break;
      }
    }
    if (!manifest_writers_.empty()) {
      manifest_writers_.front()->cv.Signal();
    }
  };

  // Supports opening table readers with multiple threads, mostly useful for
  // batched external sst file ingestion.
  int batch_max_file_opening_threads = 1;

  assert(!manifest_writers_.empty());
  assert(manifest_writers_.front() == &first_writer);

  autovector<VersionEdit*> batch_edits;
  // This vector keeps track of the corresponding user-defined timestamp size
  // for `batch_edits` side by side, which is only needed for encoding a
  // `VersionEdit` that adds new SST files.
  // Note that anytime `batch_edits` has new element added or get existing
  // element removed, `batch_edits_ts_sz` should be updated too.
  autovector<std::optional<size_t>> batch_edits_ts_sz;
  autovector<Version*> versions;
  std::vector<std::unique_ptr<BaseReferencedVersionBuilder>> builder_guards;
  autovector<const autovector<uint64_t>*> files_to_quarantine_if_commit_fail;
  autovector<uint64_t> limbo_descriptor_log_file_number;

  // Tracking `max_last_sequence` is needed to ensure we write
  // `VersionEdit::last_sequence_`s in non-decreasing order according to the
  // recovery code's requirement. It also allows us to defer updating
  // `descriptor_last_sequence_` until the apply phase, after the log phase
  // succeeds.
  SequenceNumber max_last_sequence = descriptor_last_sequence_;

  bool skip_manifest_write =
      first_writer.edit_list.front()->IsNoManifestWriteDummy();

  if (first_writer.edit_list.front()->IsColumnFamilyManipulation()) {
    // No group commits for column family add or drop
    LogAndApplyCFHelper(first_writer.edit_list.front(), &max_last_sequence);
    batch_edits.push_back(first_writer.edit_list.front());
    batch_edits_ts_sz.push_back(std::nullopt);
  } else {
    auto it = manifest_writers_.cbegin();
    size_t group_start = std::numeric_limits<size_t>::max();
    Status batch_apply_status;
    for (;;) {
      assert(!(*it)->edit_list.front()->IsColumnFamilyManipulation());
      last_writer = *it;
      batch_max_file_opening_threads =
          std::max(batch_max_file_opening_threads,
                   last_writer->max_file_opening_threads);
      assert(last_writer != nullptr);
      assert(last_writer->cfd != nullptr);
      if (!batch_apply_status.ok()) {
        // An earlier edit failed, so apply no more edits, but keep grouping
        // until the loop conditions below end the batch as usual. LogAndApply()
        // returns once a caller's first writer is done, so none of its writers
        // may be left queued.
      } else if (last_writer->cfd->IsDropped()) {
        // If we detect a dropped CF at this point, and the corresponding
        // version edits belong to an atomic group, then we need to find out
        // the preceding version edits in the same atomic group, and update
        // their `remaining_entries_` member variable because we are NOT going
        // to write the version edits' of dropped CF to the MANIFEST. If we
        // don't update, then Recover can report corrupted atomic group
        // because the `remaining_entries_` do not match.
        if (!batch_edits.empty()) {
          if (batch_edits.back()->IsInAtomicGroup() &&
              batch_edits.back()->GetRemainingEntries() > 0) {
            assert(group_start < batch_edits.size());
            const auto& edit_list = last_writer->edit_list;
            size_t k = 0;
            while (k < edit_list.size()) {
              if (!edit_list[k]->IsInAtomicGroup()) {
                break;  // inner while-`k` loop
              } else if (edit_list[k]->GetRemainingEntries() == 0) {
                ++k;
                break;  // inner while-`k` loop
              }
              ++k;
            }
            for (auto i = group_start; i < batch_edits.size(); ++i) {
              assert(static_cast<uint32_t>(k) <=
                     batch_edits.back()->GetRemainingEntries());
              batch_edits[i]->SetRemainingEntries(
                  batch_edits[i]->GetRemainingEntries() -
                  static_cast<uint32_t>(k));
            }
          }
        }
      } else {
        // We do a linear search on versions because versions is small.
        // TODO(yanqin) maybe consider unordered_map
        Version* version = nullptr;
        VersionBuilder* builder = nullptr;
        for (int i = 0; i != static_cast<int>(versions.size()); ++i) {
          uint32_t cf_id = last_writer->cfd->GetID();
          if (versions[i]->cfd()->GetID() == cf_id) {
            version = versions[i];
            assert(!builder_guards.empty() &&
                   builder_guards.size() == versions.size());
            builder = builder_guards[i]->version_builder();
            TEST_SYNC_POINT_CALLBACK(
                "VersionSet::ProcessManifestWrites:SameColumnFamily", &cf_id);
            break;  // inner for-`i` loop
          }
        }
        if (version == nullptr) {
          // WAL and OPTIONS protocol manipulations do not change LSM Versions.
          if (!last_writer->IsAllVersionIndependentEdits()) {
            version = new Version(
                last_writer->cfd, this, file_options_,
                last_writer->cfd ? last_writer->cfd->GetLatestMutableCFOptions()
                                 : MutableCFOptions(*new_cf_options),
                io_tracer_, current_version_number_++);
            versions.push_back(version);
            builder_guards.emplace_back(new BaseReferencedVersionBuilder(
                last_writer->cfd, last_writer->cfd->current()));
            builder = builder_guards.back()->version_builder();
          }
          assert(last_writer->IsAllVersionIndependentEdits() || builder);
          assert(last_writer->IsAllVersionIndependentEdits() || version);
          TEST_SYNC_POINT_CALLBACK(
              "VersionSet::ProcessManifestWrites:NewVersion", version);
        }
        const Comparator* ucmp = last_writer->cfd->user_comparator();
        assert(ucmp);
        std::optional<size_t> edit_ts_sz = ucmp->timestamp_size();
        for (const auto& e : last_writer->edit_list) {
          if (e->IsInAtomicGroup()) {
            if (batch_edits.empty() || !batch_edits.back()->IsInAtomicGroup() ||
                (batch_edits.back()->IsInAtomicGroup() &&
                 batch_edits.back()->GetRemainingEntries() == 0)) {
              group_start = batch_edits.size();
            }
          } else if (group_start != std::numeric_limits<size_t>::max()) {
            group_start = std::numeric_limits<size_t>::max();
          }
          batch_apply_status = LogAndApplyHelper(last_writer->cfd, builder, e,
                                                 &max_last_sequence, mu);
          if (!batch_apply_status.ok()) {
            break;  // inner for-`e` loop
          }
          batch_edits.push_back(e);
          batch_edits_ts_sz.push_back(edit_ts_sz);
        }
      }
      // Loop increment/conditions
      ++it;
      if (it == manifest_writers_.cend()) {
        break;
      }
      if (skip_manifest_write) {
        // no grouping when skipping manifest write
        break;
      }
      const auto* next = (*it)->edit_list.front();
      if (next->IsColumnFamilyManipulation() ||
          next->IsNoManifestWriteDummy()) {
        // no group commits for column family add or drop
        // nor for dummy skipping manifest write
        break;
      }
    }
    if (!batch_apply_status.ok()) {
      for (auto v : versions) {
        delete v;
      }
      finish_batch(batch_apply_status);
      return batch_apply_status;
    }
    for (int i = 0; i < static_cast<int>(versions.size()); ++i) {
      assert(!builder_guards.empty() &&
             builder_guards.size() == versions.size());
      auto* builder = builder_guards[i]->version_builder();
      Status s = builder->SaveTo(versions[i]->storage_info());
      if (!s.ok()) {
        // free up the allocated memory
        for (auto v : versions) {
          delete v;
        }
        finish_batch(s);
        return s;
      }
    }
  }

#ifndef NDEBUG
  // Verify that version edits of atomic groups have correct
  // remaining_entries_.
  size_t k = 0;
  while (k < batch_edits.size()) {
    while (k < batch_edits.size() && !batch_edits[k]->IsInAtomicGroup()) {
      ++k;
    }
    if (k == batch_edits.size()) {
      break;
    }
    size_t i = k;
    while (i < batch_edits.size()) {
      if (!batch_edits[i]->IsInAtomicGroup()) {
        break;
      }
      assert(i - k + batch_edits[i]->GetRemainingEntries() ==
             batch_edits[k]->GetRemainingEntries());
      if (batch_edits[i]->GetRemainingEntries() == 0) {
        ++i;
        break;
      }
      ++i;
    }
    assert(batch_edits[i - 1]->IsInAtomicGroup());
    assert(0 == batch_edits[i - 1]->GetRemainingEntries());
    std::vector<VersionEdit*> tmp;
    for (size_t j = k; j != i; ++j) {
      tmp.emplace_back(batch_edits[j]);
    }
    TEST_SYNC_POINT_CALLBACK(
        "VersionSet::ProcessManifestWrites:CheckOneAtomicGroup", &tmp);
    k = i;
  }
  if (skip_manifest_write) {
    // no grouping when skipping manifest write
    assert(last_writer == &first_writer);
  }
#endif  // NDEBUG

  uint64_t prev_manifest_file_size = manifest_file_size_;
  assert(pending_manifest_file_number_ == 0);
  bool has_foreground_operation = false;
  for (const VersionEdit* e : batch_edits) {
    if (e->IsForegroundOperation()) {
      has_foreground_operation = true;
      break;
    }
  }
  // For MANIFEST write batches with any foreground operation (external file
  // ingestion/import, DeleteFilesInRange(s), and column family manipulations
  // like CreateColumnFamily and DropColumnFamily), relax the size limit by 25%
  // to reduce the likelihood of a user operation blocking on MANIFEST rotation.
  // Background-only batches (flush/compaction) still rotate at the normal
  // threshold.
  // TODO/future: for workloads like atomic-replace ingestion-only, with zero
  // or few flushes and compactions, it might be nice to trigger background
  // manifest rotation if we are beyond the soft limit. But the vast majority
  // of workloads should have plenty of background manifest ops to avoid
  // foreground rotation.
  uint64_t enforced_limit = tuned_max_manifest_file_size_;
  if (has_foreground_operation) {
    uint64_t new_limit = enforced_limit + enforced_limit / 4;
    // don't keep in case of overflow
    enforced_limit = std::max(enforced_limit, new_limit);
  }
  if (!skip_manifest_write &&
      (!descriptor_log_ || prev_manifest_file_size >= enforced_limit)) {
    TEST_SYNC_POINT("VersionSet::ProcessManifestWrites:BeforeNewManifest");
    new_descriptor_log = true;
  } else {
    pending_manifest_file_number_ = manifest_file_number_;
  }

  // Local cached copy of state variable(s). WriteCurrentStateToManifest()
  // reads its content after releasing db mutex to avoid race with
  // SwitchMemtable().
  std::unordered_map<uint32_t, MutableCFState> curr_state;
  VersionEdit wal_additions;
  OptionsFileManifestState options_manifest_state;
  if (new_descriptor_log) {
    pending_manifest_file_number_ = NewFileNumber();
    batch_edits.back()->SetNextFile(next_file_number_.load());

    // if we are writing out new snapshot make sure to persist max column
    // family.
    if (column_family_set_->GetMaxColumnFamily() > 0) {
      first_writer.edit_list.front()->SetMaxColumnFamily(
          column_family_set_->GetMaxColumnFamily());
    }
    for (const auto* cfd : *column_family_set_) {
      assert(curr_state.find(cfd->GetID()) == curr_state.end());
      curr_state.emplace(
          cfd->GetID(),
          MutableCFState(cfd->GetLogNumber(), cfd->GetFullHistoryTsLow()));
    }

    for (const auto& wal : wals_.GetWals()) {
      wal_additions.AddWal(wal.first, wal.second);
    }
    options_manifest_state = options_file_manifest_state();
  }

  uint64_t new_manifest_file_size = 0;
  Status s;
  IOStatus io_s;
  IOStatus manifest_io_status;
  manifest_io_status.PermitUncheckedError();
  std::unique_ptr<log::Writer> new_desc_log_ptr;
  // Save before releasing mu
  uint64_t manifest_preallocation_size = manifest_preallocation_size_;
  if (skip_manifest_write) {
    if (s.ok()) {
      constexpr bool update_stats = true;
      for (int i = 0; i < static_cast<int>(versions.size()); ++i) {
        // NOTE: normally called with DB mutex released, but we don't
        // want to release the DB mutex in this mode of LogAndApply
        versions[i]->PrepareAppend(read_options, update_stats);
      }
    }
  } else {
    FileOptions opt_file_opts = GetFileOptionsForManifestWrite();
    mu->Unlock();
    TEST_SYNC_POINT("VersionSet::LogAndApply:WriteManifestStart");
    TEST_SYNC_POINT_CALLBACK("VersionSet::LogAndApply:WriteManifest", nullptr);
    if (!first_writer.edit_list.front()->IsColumnFamilyManipulation()) {
      for (int i = 0; i < static_cast<int>(versions.size()); ++i) {
        assert(!builder_guards.empty() &&
               builder_guards.size() == versions.size());
        ColumnFamilyData* cfd = versions[i]->cfd_;
        s = builder_guards[i]->version_builder()->LoadTableHandlers(
            cfd->internal_stats(), batch_max_file_opening_threads,
            true /* prefetch_index_and_filter_in_cache */,
            false /* is_initial_load */, versions[i]->GetMutableCFOptions(),
            MaxFileSizeForL0MetaPin(versions[i]->GetMutableCFOptions()),
            read_options);
        if (!s.ok()) {
          if (db_options_->paranoid_checks) {
            break;
          }
          s = Status::OK();
        }
      }
    }

    log::Writer* raw_desc_log_ptr = descriptor_log_.get();
    if (s.ok() && new_descriptor_log) {
      // This is fine because everything inside of this block is serialized --
      // only one thread can be here at the same time
      // create new manifest file
      std::string descriptor_fname =
          DescriptorFileName(dbname_, pending_manifest_file_number_);
      std::unique_ptr<FSWritableFile> descriptor_file;
      io_s = NewWritableFile(fs_.get(), descriptor_fname, &descriptor_file,
                             opt_file_opts);
      if (io_s.ok()) {
        new_desc_log_ptr =
            CreateManifestWriter(std::move(descriptor_file), descriptor_fname,
                                 opt_file_opts, manifest_preallocation_size);
        raw_desc_log_ptr = new_desc_log_ptr.get();
        s = WriteCurrentStateToManifest(write_options, curr_state,
                                        wal_additions, options_manifest_state,
                                        raw_desc_log_ptr, io_s);
        assert(s == io_s);
      }
      if (!io_s.ok()) {
        manifest_io_status = io_s;
        s = io_s;
      }
    }

    if (s.ok()) {
      if (!first_writer.edit_list.front()->IsColumnFamilyManipulation()) {
        constexpr bool update_stats = true;

        for (int i = 0; i < static_cast<int>(versions.size()); ++i) {
          versions[i]->PrepareAppend(read_options, update_stats);
        }
      }

      // Write new records to MANIFEST log
#ifndef NDEBUG
      size_t idx = 0;
#endif
      assert(batch_edits.size() == batch_edits_ts_sz.size());
      for (size_t bidx = 0; bidx < batch_edits.size(); bidx++) {
        auto& e = batch_edits[bidx];
        files_to_quarantine_if_commit_fail.push_back(
            e->GetFilesToQuarantineIfCommitFail());
        std::string record;
        if (!e->EncodeTo(&record, batch_edits_ts_sz[bidx])) {
          s = Status::Corruption("Unable to encode VersionEdit:" +
                                 e->DebugString(true));
          break;
        }
        TEST_KILL_RANDOM_WITH_WEIGHT("VersionSet::LogAndApply:BeforeAddRecord",
                                     REDUCE_ODDS2);
#ifndef NDEBUG
        if (batch_edits.size() > 1 && batch_edits.size() - 1 == idx) {
          TEST_SYNC_POINT_CALLBACK(
              "VersionSet::ProcessManifestWrites:BeforeWriteLastVersionEdit:"
              "0",
              nullptr);
          TEST_SYNC_POINT(
              "VersionSet::ProcessManifestWrites:BeforeWriteLastVersionEdit:"
              "1");
        }
        ++idx;
#endif /* !NDEBUG */
        TEST_SYNC_POINT("VersionSet::ProcessManifestWrites:AddRecord");
        io_s = raw_desc_log_ptr->AddRecord(write_options, record);
        if (!io_s.ok()) {
          s = io_s;
          manifest_io_status = io_s;
          break;
        }
      }

      if (s.ok()) {
        io_s =
            SyncManifest(db_options_, write_options, raw_desc_log_ptr->file());
        manifest_io_status = io_s;
        TEST_SYNC_POINT_CALLBACK(
            "VersionSet::ProcessManifestWrites:AfterSyncManifest", &io_s);
      }
      if (!io_s.ok()) {
        s = io_s;
        ROCKS_LOG_ERROR(db_options_->info_log, "MANIFEST write %s\n",
                        s.ToString().c_str());
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    if (s.ok()) {
      assert(manifest_io_status.ok());
    }
    if (s.ok() && new_descriptor_log) {
      io_s = SetCurrentFile(
          write_options, fs_.get(), dbname_, pending_manifest_file_number_,
          file_options_.temperature, dir_contains_current_file);
      TEST_SYNC_POINT_CALLBACK(
          "VersionSet::ProcessManifestWrites:AfterSetCurrentFile", &io_s);
      if (!io_s.ok()) {
        s = io_s;
        // Quarantine old manifest file in case new manifest file's CURRENT
        // file wasn't created successfully and the old manifest is needed.
        limbo_descriptor_log_file_number.push_back(manifest_file_number_);
        files_to_quarantine_if_commit_fail.push_back(
            &limbo_descriptor_log_file_number);
      }
    }

    if (s.ok()) {
      // find offset in manifest file where this version is stored.
      new_manifest_file_size = raw_desc_log_ptr->file()->GetFileSize();
      if (new_descriptor_log) {
        ROCKS_LOG_INFO(db_options_->info_log,
                       "Created manifest %" PRIu64
                       ", compacted+appended from %" PRIu64 " to %" PRIu64
                       ", db_session_id=%s\n",
                       pending_manifest_file_number_, prev_manifest_file_size,
                       new_manifest_file_size, db_session_id_.c_str());
      }
    }

    if (first_writer.edit_list.front()->IsColumnFamilyDrop()) {
      TEST_SYNC_POINT("VersionSet::LogAndApply::ColumnFamilyDrop:0");
      TEST_SYNC_POINT("VersionSet::LogAndApply::ColumnFamilyDrop:1");
      TEST_SYNC_POINT("VersionSet::LogAndApply::ColumnFamilyDrop:2");
    }

    LogFlush(db_options_->info_log);
    TEST_SYNC_POINT("VersionSet::LogAndApply:WriteManifestDone");
    mu->Lock();
  }

  if (s.ok()) {
    // Apply WAL edits, DB mutex must be held.
    for (auto& e : batch_edits) {
      if (e->IsWalAddition()) {
        s = wals_.AddWals(e->GetWalAdditions());
      } else if (e->IsWalDeletion()) {
        s = wals_.DeleteWalsBefore(e->GetWalDeletion().GetLogNumber());
      }
      if (!s.ok()) {
        break;
      }
    }
  }

  if (!io_s.ok()) {
    if (io_status_.ok()) {
      io_status_ = io_s;
      if (error_handler_) {
        error_handler_->AddFilesToQuarantine(
            files_to_quarantine_if_commit_fail);
      }
    }
  } else if (!io_status_.ok()) {
    io_status_ = io_s;
    if (error_handler_) {
      error_handler_->ClearFilesToQuarantine();
    }
  }

  // Append the old manifest file to the obsolete_manifest_ list to be deleted
  // by PurgeObsoleteFiles later.
  if (s.ok() && new_descriptor_log) {
    descriptor_log_ = std::move(new_desc_log_ptr);
    obsolete_manifests_.emplace_back(
        DescriptorFileName("", manifest_file_number_));
    last_compacted_manifest_file_size_ = new_manifest_file_size;
    TuneMaxManifestFileSize();
  }

  // Install the new versions
  if (s.ok()) {
    if (first_writer.edit_list.front()->IsColumnFamilyAdd()) {
      assert(batch_edits.size() == 1);
      assert(new_cf_options != nullptr);
      assert(max_last_sequence == descriptor_last_sequence_);
      CreateColumnFamily(*new_cf_options, read_options,
                         first_writer.edit_list.front(),
                         /*read_only=*/false, new_cf_options_are_sanitized);
    } else if (first_writer.edit_list.front()->IsColumnFamilyDrop()) {
      assert(batch_edits.size() == 1);
      assert(max_last_sequence == descriptor_last_sequence_);
      first_writer.cfd->SetDropped();
      first_writer.cfd->UnrefAndTryDelete();
    } else {
      // Each version in versions corresponds to a column family.
      // For each column family, update its log number indicating that logs
      // with number smaller than this should be ignored.
      uint64_t last_min_log_number_to_keep = 0;
      for (const auto& e : batch_edits) {
        ColumnFamilyData* cfd = nullptr;
        if (!e->IsColumnFamilyManipulation()) {
          cfd = column_family_set_->GetColumnFamily(e->GetColumnFamily());
          // e would not have been added to batch_edits if its corresponding
          // column family is dropped.
          assert(cfd);
        }
        if (cfd) {
          if (e->HasLogNumber() && e->GetLogNumber() > cfd->GetLogNumber()) {
            cfd->SetLogNumber(e->GetLogNumber());
          }
          if (e->HasFullHistoryTsLow()) {
            cfd->SetFullHistoryTsLow(e->GetFullHistoryTsLow());
          }
        }
        if (e->HasMinLogNumberToKeep()) {
          last_min_log_number_to_keep =
              std::max(last_min_log_number_to_keep, e->GetMinLogNumberToKeep());
        }
      }

      if (last_min_log_number_to_keep != 0) {
        MarkMinLogNumberToKeep(last_min_log_number_to_keep);
      }

      for (int i = 0; i < static_cast<int>(versions.size()); ++i) {
        ColumnFamilyData* cfd = versions[i]->cfd_;
        AppendVersion(cfd, versions[i]);
      }
    }
    if (!skip_manifest_write) {
      for (const auto* edit : batch_edits) {
        assert(edit != nullptr);
        if (edit->HasCommittedOptionsFileNumber()) {
          ApplyCommittedOptionsFileNumber(
              edit->GetCommittedOptionsFileNumber());
        }
      }
      assert(max_last_sequence >= descriptor_last_sequence_);
      descriptor_last_sequence_ = max_last_sequence;
      manifest_file_number_ = pending_manifest_file_number_;
      manifest_file_size_ = new_manifest_file_size;
      manifest_last_valid_record_end_ = new_manifest_file_size;
      manifest_last_valid_record_end_file_number_ = manifest_file_number_;
      prev_log_number_ = first_writer.edit_list.front()->GetPrevLogNumber();
    }
  } else {
    std::string version_edits;
    for (auto& e : batch_edits) {
      version_edits += ("\n" + e->DebugString(true));
    }
    ROCKS_LOG_ERROR(db_options_->info_log,
                    "Error in committing version edit to MANIFEST: %s",
                    version_edits.c_str());
    for (auto v : versions) {
      delete v;
    }
    // If manifest append failed for whatever reason, the file could be
    // corrupted. So we need to force the next version update to start a
    // new manifest file.
    descriptor_log_.reset();
    new_desc_log_ptr.reset();
    // If manifest operations failed, then we know the CURRENT file still
    // points to the original MANIFEST. Therefore, we can safely delete the
    // new MANIFEST.
    // If manifest operations succeeded, and we are here, then it is possible
    // that renaming tmp file to CURRENT failed.
    //
    // On local POSIX-compliant FS, the CURRENT must point to the original
    // MANIFEST. We can delete the new MANIFEST for simplicity, but we can
    // also keep it. Future recovery will ignore this MANIFEST. It's also ok
    // for the process not to crash and continue using the db. Any future
    // LogAndApply() call will switch to a new MANIFEST and update CURRENT,
    // still ignoring this one.
    //
    // On non-local FS, it is
    // possible that the rename operation succeeded on the server (remote)
    // side, but the client somehow returns a non-ok status to RocksDB. Note
    // that this does not violate atomicity. Should we delete the new MANIFEST
    // successfully, a subsequent recovery attempt will likely see the CURRENT
    // pointing to the new MANIFEST, thus fail. We will not be able to open
    // the DB again. Therefore, if manifest operations succeed, we should keep
    // the the new MANIFEST. If the process proceeds, any future LogAndApply()
    // call will switch to a new MANIFEST and update CURRENT. If user tries to
    // re-open the DB,
    // a) CURRENT points to the new MANIFEST, and the new MANIFEST is present.
    // b) CURRENT points to the original MANIFEST, and the original MANIFEST
    //    also exists.
    if (!manifest_io_status.ok() && new_descriptor_log) {
      ROCKS_LOG_INFO(db_options_->info_log,
                     "Deleting manifest %" PRIu64 " current manifest %" PRIu64
                     "\n",
                     pending_manifest_file_number_, manifest_file_number_);
      Status manifest_del_status = env_->DeleteFile(
          DescriptorFileName(dbname_, pending_manifest_file_number_));
      if (!manifest_del_status.ok()) {
        ROCKS_LOG_WARN(db_options_->info_log,
                       "Failed to delete manifest %" PRIu64 ": %s",
                       pending_manifest_file_number_,
                       manifest_del_status.ToString().c_str());
      }
    }
  }

  pending_manifest_file_number_ = 0;

#ifndef NDEBUG
  // This is here kind of awkwardly because there's no other consistency
  // checks on `VersionSet`'s updates for the new `Version`s. We might want
  // to move it to a dedicated function, or remove it if we gain enough
  // confidence in `descriptor_last_sequence_`.
  if (s.ok()) {
    for (const auto* v : versions) {
      const auto* vstorage = v->storage_info();
      for (int level = 0; level < vstorage->num_levels(); ++level) {
        for (const auto& file : vstorage->LevelFiles(level)) {
          assert(file->fd.largest_seqno <= descriptor_last_sequence_);
        }
      }
    }
  }
#endif  // NDEBUG

  finish_batch(s);
  return s;
}

void VersionSet::WakeUpWaitingManifestWriters() {
  // wake up all the waiting writers
  // Notify new head of manifest write queue.
  if (!manifest_writers_.empty()) {
    manifest_writers_.front()->cv.Signal();
  }
}

// 'datas' is grammatically incorrect. We still use this notation to indicate
// that this variable represents a collection of column_family_data.
Status VersionSet::LogAndApply(
    const autovector<ColumnFamilyData*>& column_family_datas,
    const ReadOptions& read_options, const WriteOptions& write_options,
    const autovector<autovector<VersionEdit*>>& edit_lists,
    InstrumentedMutex* mu, FSDirectory* dir_contains_current_file,
    bool new_descriptor_log, const ColumnFamilyOptions* new_cf_options,
    const std::vector<std::function<void(const Status&)>>& manifest_wcbs,
    const std::function<Status()>& pre_cb, int max_file_opening_threads) {
  return LogAndApplyImpl(column_family_datas, read_options, write_options,
                         edit_lists, mu, dir_contains_current_file,
                         new_descriptor_log, new_cf_options,
                         /*new_cf_options_are_sanitized=*/false, manifest_wcbs,
                         pre_cb, max_file_opening_threads);
}

Status VersionSet::LogAndApplyColumnFamilyAddWithSanitizedOptions(
    const ReadOptions& read_options, const WriteOptions& write_options,
    VersionEdit* edit, InstrumentedMutex* mu,
    FSDirectory* dir_contains_current_file,
    const ColumnFamilyOptions& sanitized_options) {
  assert(edit != nullptr);
  assert(edit->IsColumnFamilyAdd());
  autovector<ColumnFamilyData*> cfds{nullptr};
  autovector<VersionEdit*> edit_list{edit};
  autovector<autovector<VersionEdit*>> edit_lists{edit_list};
  return LogAndApplyImpl(cfds, read_options, write_options, edit_lists, mu,
                         dir_contains_current_file,
                         /*new_descriptor_log=*/false, &sanitized_options,
                         /*new_cf_options_are_sanitized=*/true,
                         /*manifest_wcbs=*/{}, /*pre_cb=*/{},
                         /*max_file_opening_threads=*/1);
}

Status VersionSet::LogAndApplyImpl(
    const autovector<ColumnFamilyData*>& column_family_datas,
    const ReadOptions& read_options, const WriteOptions& write_options,
    const autovector<autovector<VersionEdit*>>& edit_lists,
    InstrumentedMutex* mu, FSDirectory* dir_contains_current_file,
    bool new_descriptor_log, const ColumnFamilyOptions* new_cf_options,
    bool new_cf_options_are_sanitized,
    const std::vector<std::function<void(const Status&)>>& manifest_wcbs,
    const std::function<Status()>& pre_cb, int max_file_opening_threads) {
  mu->AssertHeld();
  int num_edits = 0;
  for (const auto& elist : edit_lists) {
    num_edits += static_cast<int>(elist.size());
  }
  if (num_edits == 0) {
    return Status::OK();
  } else if (num_edits > 1) {
#ifndef NDEBUG
    for (const auto& edit_list : edit_lists) {
      for (const auto& edit : edit_list) {
        assert(!edit->IsColumnFamilyManipulation());
        assert(!edit->IsNoManifestWriteDummy());
      }
    }
#endif /* ! NDEBUG */
  }

  int num_cfds = static_cast<int>(column_family_datas.size());
  if (num_cfds == 1 && column_family_datas[0] == nullptr) {
    assert(edit_lists.size() == 1 && edit_lists[0].size() == 1);
    assert(edit_lists[0][0]->IsColumnFamilyAdd());
    assert(new_cf_options != nullptr);
  }
  std::deque<ManifestWriter> writers;
  if (num_cfds > 0) {
    assert(static_cast<size_t>(num_cfds) == edit_lists.size());
  }
  for (int i = 0; i < num_cfds; ++i) {
    const auto wcb =
        manifest_wcbs.empty() ? [](const Status&) {} : manifest_wcbs[i];
    writers.emplace_back(mu, column_family_datas[i], edit_lists[i], wcb,
                         max_file_opening_threads);
    manifest_writers_.push_back(&writers[i]);
  }
  assert(!writers.empty());
  ManifestWriter& first_writer = writers.front();
  TEST_SYNC_POINT_CALLBACK("VersionSet::LogAndApply:BeforeWriterWaiting",
                           nullptr);
  while (!first_writer.done && &first_writer != manifest_writers_.front()) {
    first_writer.cv.Wait();
  }
  if (first_writer.done) {
    // All non-CF-manipulation operations can be grouped together and
    // committed to MANIFEST. They should all have finished. The status code
    // is stored in the first manifest writer.
#ifndef NDEBUG
    for (const auto& writer : writers) {
      assert(writer.done);
    }
    TEST_SYNC_POINT_CALLBACK("VersionSet::LogAndApply:WakeUpAndDone", mu);
#endif /* !NDEBUG */
    // FIXME: One MANIFEST write failure can cause all writes to SetBGError,
    // should only SetBGError once.
    return first_writer.status;
  }
  TEST_SYNC_POINT_CALLBACK("VersionSet::LogAndApply:WakeUpAndNotDone", mu);

  int num_undropped_cfds = 0;
  for (auto cfd : column_family_datas) {
    // if cfd == nullptr, it is a column family add.
    if (cfd == nullptr || !cfd->IsDropped()) {
      ++num_undropped_cfds;
    }
  }
  Status s;
  if (0 == num_undropped_cfds) {
    s = Status::ColumnFamilyDropped();
  }
  // Call pre_cb once we know we have work to do and are scheduled as the
  // exclusive manifest writer (and new Version appender)
  if (s.ok() && pre_cb) {
    s = pre_cb();
  }
  if (!s.ok()) {
    // Revert manifest_writers_
    for (int i = 0; i != num_cfds; ++i) {
      manifest_writers_.pop_front();
    }
    // Notify new head of manifest write queue.
    if (!manifest_writers_.empty()) {
      manifest_writers_.front()->cv.Signal();
    }
    return s;
  } else {
    return ProcessManifestWrites(writers, mu, dir_contains_current_file,
                                 new_descriptor_log, new_cf_options,
                                 new_cf_options_are_sanitized, read_options,
                                 write_options);
  }
}

void VersionSet::LogAndApplyCFHelper(VersionEdit* edit,
                                     SequenceNumber* max_last_sequence) {
  assert(max_last_sequence != nullptr);
  assert(edit->IsColumnFamilyManipulation());
  edit->SetNextFile(next_file_number_.load());
  assert(!edit->HasLastSequence());
  edit->SetLastSequence(*max_last_sequence);
  if (edit->IsColumnFamilyDrop()) {
    // if we drop column family, we have to make sure to save max column
    // family, so that we don't reuse existing ID
    edit->SetMaxColumnFamily(column_family_set_->GetMaxColumnFamily());
  }
}

Status VersionSet::LogAndApplyHelper(ColumnFamilyData* cfd,
                                     VersionBuilder* builder, VersionEdit* edit,
                                     SequenceNumber* max_last_sequence,
                                     InstrumentedMutex* mu) {
#ifdef NDEBUG
  (void)cfd;
#endif
  mu->AssertHeld();
  assert(!edit->IsColumnFamilyManipulation());
  assert(max_last_sequence != nullptr);

  if (edit->HasLogNumber()) {
    assert(edit->GetLogNumber() >= cfd->GetLogNumber());
    assert(edit->GetLogNumber() < next_file_number_.load());
  }

  if (!edit->HasPrevLogNumber()) {
    edit->SetPrevLogNumber(prev_log_number_);
  }
  edit->SetNextFile(next_file_number_.load());
  if (edit->HasLastSequence() && edit->GetLastSequence() > *max_last_sequence) {
    *max_last_sequence = edit->GetLastSequence();
  } else {
    edit->SetLastSequence(*max_last_sequence);
  }

  // WAL and OPTIONS protocol edits update DB-wide metadata without changing an
  // LSM Version, so they do not need a VersionBuilder.
  assert(builder || edit->IsWalManipulation() ||
         edit->IsOptionsFileManipulation());
  return builder ? builder->Apply(edit) : Status::OK();
}

FileOptions VersionSet::GetFileOptionsForManifestWrite() const {
  FileOptions opt_file_opts = fs_->OptimizeForManifestWrite(file_options_);
  // DB-configured temperature wins over the FS's default (when not kUnknown).
  if (file_options_.temperature != Temperature::kUnknown) {
    opt_file_opts.temperature = file_options_.temperature;
  }
  return opt_file_opts;
}

std::unique_ptr<log::Writer> VersionSet::CreateManifestWriter(
    std::unique_ptr<FSWritableFile> file, const std::string& fname,
    const FileOptions& opts, uint64_t preallocation_size,
    uint64_t initial_file_size) const {
  file->SetPreallocationBlockSize(preallocation_size);
  FileTypeSet tmp_set = db_options_->checksum_handoff_file_types;
  auto file_writer = std::make_unique<WritableFileWriter>(
      std::move(file), fname, opts, clock_, io_tracer_,
      /*stats=*/nullptr, Histograms::HISTOGRAM_ENUM_MAX, db_options_->listeners,
      /*file_checksum_gen_factory=*/nullptr,
      tmp_set.Contains(FileType::kDescriptorFile),
      tmp_set.Contains(FileType::kDescriptorFile), initial_file_size);
  const size_t block_offset = initial_file_size % log::kBlockSize;
  return std::make_unique<log::Writer>(
      std::move(file_writer), /*log_number=*/0, /*recycle_log_files=*/false,
      /*manual_flush=*/false, kNoCompression,
      /*track_and_verify_wals=*/false, block_offset);
}

Status VersionSet::GetManifestAppendBoundary(uint64_t* manifest_size) const {
  assert(manifest_size != nullptr);
  if (manifest_last_valid_record_end_file_number_ != manifest_file_number_) {
    return Status::TryAgain(
        "Current MANIFEST changed while determining its append boundary");
  }
  if (manifest_last_valid_record_end_ == 0 ||
      manifest_last_valid_record_end_ > manifest_file_size_) {
    return Status::Corruption("Invalid MANIFEST append boundary");
  }
  *manifest_size = manifest_last_valid_record_end_;
  return Status::OK();
}

Status VersionSet::ReopenManifestForAppend(const std::string& manifest_path) {
  assert(db_options_->reuse_manifest_on_open);
  assert(manifest_recovery_last_valid_record_end_ > 0);
  assert(manifest_last_valid_record_end_file_number_ == manifest_file_number_);

  // Disabled under best_efforts_recovery: that mode rebuilds the
  // MANIFEST + CURRENT from scratch via the side-effect of
  // LogAndApplyForRecovery emitting an edit. Reusing the (possibly
  // stale) prior MANIFEST contradicts the salvage contract.
  if (db_options_->best_efforts_recovery) {
    return Status::OK();
  }

  auto force_new_manifest_on_open = [&]() {
    force_new_manifest_on_open_ = true;
  };

  FileOptions opt_file_opts = GetFileOptionsForManifestWrite();

  // Bail if the on-disk size diverges from what Recover's Reader
  // consumed -- likely a torn tail from a prior crash; mid-block append
  // would mis-frame the record stream.
  uint64_t physical_size = 0;
  IOStatus stat_s = fs_->GetFileSize(manifest_path, IOOptions(), &physical_size,
                                     /*dbg=*/nullptr);
  if (!stat_s.ok() ||
      physical_size != manifest_recovery_last_valid_record_end_) {
    ROCKS_LOG_WARN(db_options_->info_log,
                   "reuse_manifest_on_open: physical size %" PRIu64
                   " != last valid record end %" PRIu64
                   " (tail corruption?); creating fresh MANIFEST during open",
                   physical_size, manifest_recovery_last_valid_record_end_);
    force_new_manifest_on_open();
    return Status::OK();
  }

  // Direct-writes path is hard to reason about for an already-populated
  // file (alignment + tail-pad). POSIX's OptimizeForManifestWrite forces
  // it off, but custom FileSystems may not.
  if (opt_file_opts.use_direct_writes) {
    ROCKS_LOG_WARN(db_options_->info_log,
                   "reuse_manifest_on_open: direct writes enabled; "
                   "creating fresh MANIFEST during open");
    force_new_manifest_on_open();
    return Status::OK();
  }

  std::unique_ptr<FSWritableFile> manifest_file;
  IOStatus io_s = fs_->ReopenWritableFile(manifest_path, opt_file_opts,
                                          &manifest_file, /*dbg=*/nullptr);
  if (!io_s.ok()) {
    ROCKS_LOG_WARN(db_options_->info_log,
                   "Failed to reopen MANIFEST for append: %s",
                   io_s.ToString().c_str());
    force_new_manifest_on_open();
    return Status::OK();
  }

  const uint64_t reopened_size =
      manifest_file->GetFileSize(opt_file_opts.io_options, /*dbg=*/nullptr);
  if (reopened_size != manifest_recovery_last_valid_record_end_) {
    ROCKS_LOG_WARN(db_options_->info_log,
                   "reuse_manifest_on_open: reopened handle size %" PRIu64
                   " != last valid record end %" PRIu64
                   "; creating fresh MANIFEST during open",
                   reopened_size, manifest_recovery_last_valid_record_end_);
    force_new_manifest_on_open();
    return Status::OK();
  }

  descriptor_log_ = CreateManifestWriter(
      std::move(manifest_file), manifest_path, opt_file_opts,
      manifest_preallocation_size_, reopened_size);

  ROCKS_LOG_INFO(
      db_options_->info_log,
      "Reusing existing MANIFEST file: %s (valid data size: %" PRIu64 ")",
      manifest_path.c_str(), manifest_recovery_last_valid_record_end_);
  TEST_SYNC_POINT("VersionSet::ReopenManifestForAppend:Reopened");
  return Status::OK();
}

Status VersionSet::AppendColumnFamilyDropsToManifest(
    const std::string& manifest_path, uint64_t manifest_size,
    const std::vector<uint32_t>& cf_ids, const WriteOptions& write_options,
    uint64_t manifest_preallocation_size,
    uint64_t committed_options_file_number) {
  if (cf_ids.empty() && committed_options_file_number == 0) {
    return Status::OK();
  }
  if (committed_options_file_number == std::numeric_limits<uint64_t>::max()) {
    return Status::InvalidArgument("OPTIONS file number is too large");
  }
  const uint64_t next_options_file_number =
      committed_options_file_number == 0 ? 0
                                         : committed_options_file_number + 1;

  // Rewrite the checkpoint MANIFEST via a bounded-memory buffered copy into a
  // .tmp file (append drops, sync, close), then atomically rename over the
  // original. Avoids depending on FileSystem::ReopenWritableFile (optional,
  // unsupported on some backends) and avoids reading the whole MANIFEST into
  // memory (which can reach ~1GB depending on configuration).
  const std::string tmp_path = manifest_path + ".tmp";

  // Best-effort cleanup of any stale .tmp left by a prior aborted run.
  fs_->DeleteFile(tmp_path, IOOptions(), /*dbg=*/nullptr)
      .PermitUncheckedError();

  std::unique_ptr<FSSequentialFile> src_file;
  {
    IOStatus io_s = fs_->NewSequentialFile(manifest_path, FileOptions(),
                                           &src_file, /*dbg=*/nullptr);
    if (!io_s.ok()) {
      return io_s;
    }
  }

  FileOptions opt_file_opts = GetFileOptionsForManifestWrite();
  // Buffered writes only: direct writes would need alignment / tail-pad
  // reasoning that isn't worth the complexity for a checkpoint MANIFEST.
  opt_file_opts.use_direct_writes = false;

  std::unique_ptr<FSWritableFile> dst_file;
  {
    IOStatus io_s = fs_->NewWritableFile(tmp_path, opt_file_opts, &dst_file,
                                         /*dbg=*/nullptr);
    if (!io_s.ok()) {
      return io_s;
    }
  }

  // Copy exactly manifest_size bytes through a fixed-size buffer.
  constexpr size_t kBufSize = 1 * 1024 * 1024;
  std::string buffer(kBufSize, '\0');
  uint64_t remaining = manifest_size;
  while (remaining > 0) {
    Slice result;
    const size_t to_read =
        static_cast<size_t>(std::min<uint64_t>(kBufSize, remaining));
    IOStatus read_s = src_file->Read(to_read, opt_file_opts.io_options, &result,
                                     buffer.data(), /*dbg=*/nullptr);
    if (!read_s.ok()) {
      return read_s;
    }
    if (result.empty()) {
      return Status::Corruption(
          "Unexpected EOF reading source checkpoint MANIFEST");
    }
    IOStatus write_s = dst_file->Append(result, opt_file_opts.io_options,
                                        /*dbg=*/nullptr);
    if (!write_s.ok()) {
      return write_s;
    }
    remaining -= result.size();
  }
  src_file.reset();

  // Seeds initial_block_offset = manifest_size % kBlockSize so drop-record
  // framing continues from where the copied contents left off, matching the
  // position-based log reader.
  std::unique_ptr<log::Writer> writer =
      CreateManifestWriter(std::move(dst_file), tmp_path, opt_file_opts,
                           manifest_preallocation_size, manifest_size);

  // Always attempt to Close the writer on any exit path, folding any Close
  // error into the primary status. Guards against dropping the underlying
  // FSWritableFile with buffered data / no filesystem close hook.
  Status s;
  auto close_writer = [&]() {
    if (writer == nullptr) {
      return;
    }
    Status close_s = writer->Close(write_options);
    if (s.ok() && !close_s.ok()) {
      s = close_s;
    }
    writer.reset();
  };

  for (size_t i = 0; i < cf_ids.size(); ++i) {
    assert(writer != nullptr);
    VersionEdit edit;
    edit.SetColumnFamily(cf_ids[i]);
    edit.DropColumnFamily();
    if (i + 1 == cf_ids.size()) {
      if (committed_options_file_number != 0) {
        edit.SetCommittedOptionsFileNumber(committed_options_file_number);
        edit.SetNextFile(next_options_file_number);
      }
    }
    std::string record;
    if (!edit.EncodeTo(&record)) {
      s = Status::Corruption(
          "Unable to encode column family drop VersionEdit for checkpoint");
      close_writer();
      return s;
    }
    IOStatus add_s = writer->AddRecord(write_options, record);
    if (!add_s.ok()) {
      s = add_s;
      close_writer();
      return s;
    }
  }

  if (cf_ids.empty()) {
    assert(writer != nullptr);
    VersionEdit edit;
    if (committed_options_file_number != 0) {
      edit.SetCommittedOptionsFileNumber(committed_options_file_number);
      edit.SetNextFile(next_options_file_number);
    }
    std::string record;
    if (!edit.EncodeTo(&record)) {
      s = Status::Corruption("Unable to encode committed OPTIONS VersionEdit");
      close_writer();
      return s;
    }
    IOStatus add_s = writer->AddRecord(write_options, record);
    if (!add_s.ok()) {
      s = add_s;
      close_writer();
      return s;
    }
  }

  IOStatus sync_s = SyncManifest(db_options_, write_options, writer->file());
  if (!sync_s.ok()) {
    s = sync_s;
    close_writer();
    return s;
  }
  close_writer();
  if (!s.ok()) {
    return s;
  }

  // Replace the original with the rewritten .tmp.
  IOStatus rename_s =
      fs_->RenameFile(tmp_path, manifest_path, IOOptions(), /*dbg=*/nullptr);
  if (!rename_s.ok()) {
    return rename_s;
  }
  const size_t slash = manifest_path.find_last_of("/\\");
  const std::string manifest_dir =
      slash == std::string::npos ? "." : manifest_path.substr(0, slash);
  std::unique_ptr<FSDirectory> dir;
  IOStatus dir_s = fs_->NewDirectory(manifest_dir, IOOptions(), &dir, nullptr);
  if (!dir_s.ok()) {
    return dir_s;
  }
  assert(dir != nullptr);
  dir_s = dir->FsyncWithDirOptions(IOOptions(), nullptr,
                                   DirFsyncOptions(manifest_path));
  if (!dir_s.ok()) {
    return dir_s;
  }
  return Status::OK();
}

Status VersionSet::Recover(
    const std::vector<ColumnFamilyDescriptor>& column_families, bool read_only,
    std::string* db_id, bool no_error_if_files_missing, bool is_retry,
    Status* log_status) {
  const ReadOptions read_options(Env::IOActivity::kDBOpen);
  // Read "CURRENT" file, which contains a pointer to the current manifest
  // file
  std::string manifest_path;
  Status s = GetCurrentManifestPath(dbname_, fs_.get(), is_retry,
                                    &manifest_path, &manifest_file_number_);
  if (!s.ok()) {
    return s;
  }

  ROCKS_LOG_INFO(db_options_->info_log, "Recovering from manifest file: %s\n",
                 manifest_path.c_str());

  std::unique_ptr<SequentialFileReader> manifest_file_reader;
  {
    std::unique_ptr<FSSequentialFile> manifest_file;
    s = fs_->NewSequentialFile(manifest_path,
                               fs_->OptimizeForManifestRead(file_options_),
                               &manifest_file, nullptr);
    if (!s.ok()) {
      return s;
    }
    manifest_file_reader.reset(new SequentialFileReader(
        std::move(manifest_file), manifest_path,
        db_options_->log_readahead_size, io_tracer_, db_options_->listeners,
        /*rate_limiter=*/nullptr, is_retry));
  }
  TEST_SYNC_POINT("VersionSet::Recover:StartManifestRead");

  uint64_t current_manifest_file_size = 0;
  uint64_t log_number = 0;
  {
    force_new_manifest_on_open_ = false;
    VersionSet::LogReporter reporter;
    Status log_read_status;
    reporter.status = &log_read_status;
    log::Reader reader(nullptr, std::move(manifest_file_reader), &reporter,
                       true /* checksum */, 0 /* log_number */);
    VersionEditHandler handler(
        read_only, column_families, const_cast<VersionSet*>(this),
        /*track_found_and_missing_files=*/false, no_error_if_files_missing,
        io_tracer_, read_options, /*allow_incomplete_valid_version=*/false,
        EpochNumberRequirement::kMightMissing,
        /*skip_load_table_files=*/db_options_->open_files_async);
    handler.Iterate(reader, &log_read_status);
    s = handler.status();
    if (s.ok()) {
      log_number = handler.GetVersionEditParams().GetLogNumber();
      current_manifest_file_size = reader.GetReadOffset();
      assert(current_manifest_file_size != 0);
      handler.GetDbId(db_id);
    }
    if (s.ok()) {
      RecoverEpochNumbers();
    }
    if (log_status) {
      *log_status = log_read_status;
    }
  }

  if (s.ok()) {
    manifest_file_size_ = current_manifest_file_size;
    ROCKS_LOG_INFO(
        db_options_->info_log,
        "Recovered from manifest file:%s succeeded,"
        "manifest_file_number is %" PRIu64 ", next_file_number is %" PRIu64
        ", last_sequence is %" PRIu64 ", log_number is %" PRIu64
        ",prev_log_number is %" PRIu64 ",max_column_family is %" PRIu32
        ",min_log_number_to_keep is %" PRIu64 "\n",
        manifest_path.c_str(), manifest_file_number_, next_file_number_.load(),
        last_sequence_.load(), log_number, prev_log_number_,
        column_family_set_->GetMaxColumnFamily(), min_log_number_to_keep());

    for (auto cfd : *column_family_set_) {
      if (cfd->IsDropped()) {
        continue;
      }
      ROCKS_LOG_INFO(db_options_->info_log,
                     "Column family [%s] (ID %" PRIu32
                     "), log number is %" PRIu64 "\n",
                     cfd->GetName().c_str(), cfd->GetID(), cfd->GetLogNumber());
    }
  }

  if (s.ok() && !read_only && db_options_->reuse_manifest_on_open &&
      manifest_recovery_last_valid_record_end_ > 0) {
    s = ReopenManifestForAppend(manifest_path);
  }

  return s;
}

namespace {
class ManifestPicker {
 public:
  explicit ManifestPicker(const std::string& dbname,
                          const std::vector<std::string>& files_in_dbname);
  // REQUIRES Valid() == true
  std::string GetNextManifest(uint64_t* file_number, std::string* file_name);
  bool Valid() const { return manifest_file_iter_ != manifest_files_.end(); }

 private:
  const std::string& dbname_;
  // MANIFEST file names(s)
  std::vector<std::string> manifest_files_;
  std::vector<std::string>::const_iterator manifest_file_iter_;
};

ManifestPicker::ManifestPicker(const std::string& dbname,
                               const std::vector<std::string>& files_in_dbname)
    : dbname_(dbname) {
  // populate manifest files
  assert(!files_in_dbname.empty());
  for (const auto& fname : files_in_dbname) {
    uint64_t file_num = 0;
    FileType file_type;
    bool parse_ok = ParseFileName(fname, &file_num, &file_type);
    if (parse_ok && file_type == kDescriptorFile) {
      manifest_files_.push_back(fname);
    }
  }
  // seek to first manifest
  std::sort(manifest_files_.begin(), manifest_files_.end(),
            [](const std::string& lhs, const std::string& rhs) {
              uint64_t num1 = 0;
              uint64_t num2 = 0;
              FileType type1;
              FileType type2;
              bool parse_ok1 = ParseFileName(lhs, &num1, &type1);
              bool parse_ok2 = ParseFileName(rhs, &num2, &type2);
#ifndef NDEBUG
              assert(parse_ok1);
              assert(parse_ok2);
#else
              (void)parse_ok1;
              (void)parse_ok2;
#endif
              return num1 > num2;
            });
  manifest_file_iter_ = manifest_files_.begin();
}

std::string ManifestPicker::GetNextManifest(uint64_t* number,
                                            std::string* file_name) {
  assert(Valid());
  std::string ret;
  if (manifest_file_iter_ != manifest_files_.end()) {
    ret.assign(dbname_);
    if (ret.back() != kFilePathSeparator) {
      ret.push_back(kFilePathSeparator);
    }
    ret.append(*manifest_file_iter_);
    if (number) {
      FileType type;
      bool parse = ParseFileName(*manifest_file_iter_, number, &type);
      assert(type == kDescriptorFile);
#ifndef NDEBUG
      assert(parse);
#else
      (void)parse;
#endif
    }
    if (file_name) {
      *file_name = *manifest_file_iter_;
    }
    ++manifest_file_iter_;
  }
  return ret;
}
}  // anonymous namespace

Status VersionSet::TryRecover(
    const std::vector<ColumnFamilyDescriptor>& column_families, bool read_only,
    const std::vector<std::string>& files_in_dbname, std::string* db_id,
    bool* has_missing_table_file) {
  ManifestPicker manifest_picker(dbname_, files_in_dbname);
  if (!manifest_picker.Valid()) {
    return Status::Corruption("Cannot locate MANIFEST file in " + dbname_);
  }
  Status s;
  std::string manifest_path =
      manifest_picker.GetNextManifest(&manifest_file_number_, nullptr);
  while (!manifest_path.empty()) {
    s = TryRecoverFromOneManifest(manifest_path, column_families, read_only,
                                  db_id, has_missing_table_file);
    if (s.ok() || !manifest_picker.Valid()) {
      break;
    }
    Reset();
    manifest_path =
        manifest_picker.GetNextManifest(&manifest_file_number_, nullptr);
  }
  return s;
}

Status VersionSet::TryRecoverFromOneManifest(
    const std::string& manifest_path,
    const std::vector<ColumnFamilyDescriptor>& column_families, bool read_only,
    std::string* db_id, bool* has_missing_table_file) {
  const ReadOptions read_options(Env::IOActivity::kDBOpen);
  ROCKS_LOG_INFO(db_options_->info_log, "Trying to recover from manifest: %s\n",
                 manifest_path.c_str());
  std::unique_ptr<SequentialFileReader> manifest_file_reader;
  Status s;
  {
    std::unique_ptr<FSSequentialFile> manifest_file;
    s = fs_->NewSequentialFile(manifest_path,
                               fs_->OptimizeForManifestRead(file_options_),
                               &manifest_file, nullptr);
    if (!s.ok()) {
      return s;
    }
    manifest_file_reader.reset(new SequentialFileReader(
        std::move(manifest_file), manifest_path,
        db_options_->log_readahead_size, io_tracer_, db_options_->listeners));
  }

  assert(s.ok());
  VersionSet::LogReporter reporter;
  reporter.status = &s;
  log::Reader reader(nullptr, std::move(manifest_file_reader), &reporter,
                     /*checksum=*/true, /*log_num=*/0);
  VersionEditHandlerPointInTime handler_pit(
      read_only, column_families, const_cast<VersionSet*>(this), io_tracer_,
      read_options, /*allow_incomplete_valid_version=*/true,
      /*trust_manifest_recovery=*/false,
      /*defer_sst_file_opening=*/false, EpochNumberRequirement::kMightMissing);

  handler_pit.Iterate(reader, &s);

  handler_pit.GetDbId(db_id);

  assert(nullptr != has_missing_table_file);
  *has_missing_table_file = handler_pit.HasMissingFiles();

  s = handler_pit.status();
  if (s.ok()) {
    RecoverEpochNumbers();
  }
  return s;
}

void VersionSet::RecoverEpochNumbers() {
  for (auto cfd : *column_family_set_) {
    if (cfd->IsDropped()) {
      continue;
    }
    assert(cfd->initialized());
    cfd->RecoverEpochNumbers();
  }
}

Status VersionSet::ListColumnFamilies(std::vector<std::string>* column_families,
                                      const std::string& dbname,
                                      FileSystem* fs) {
  // Read "CURRENT" file, which contains a pointer to the current manifest
  // file
  std::string manifest_path;
  uint64_t manifest_file_number;
  Status s = GetCurrentManifestPath(dbname, fs, /*is_retry=*/false,
                                    &manifest_path, &manifest_file_number);
  if (!s.ok()) {
    return s;
  }
  return ListColumnFamiliesFromManifest(manifest_path, fs, column_families);
}

Status VersionSet::ListColumnFamiliesFromManifest(
    const std::string& manifest_path, FileSystem* fs,
    std::vector<std::string>* column_families) {
  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;
  std::unique_ptr<SequentialFileReader> file_reader;
  Status s;
  {
    std::unique_ptr<FSSequentialFile> file;
    // these are just for performance reasons, not correctness,
    // so we're fine using the defaults
    s = fs->NewSequentialFile(manifest_path, FileOptions(), &file, nullptr);
    if (!s.ok()) {
      return s;
    }
    file_reader = std::make_unique<SequentialFileReader>(
        std::move(file), manifest_path, /*io_tracer=*/nullptr);
  }

  VersionSet::LogReporter reporter;
  reporter.status = &s;
  log::Reader reader(nullptr, std::move(file_reader), &reporter,
                     true /* checksum */, 0 /* log_number */);

  ListColumnFamiliesHandler handler(read_options);
  handler.Iterate(reader, &s);

  assert(column_families);
  column_families->clear();
  if (handler.status().ok()) {
    for (const auto& iter : handler.GetColumnFamilyNames()) {
      column_families->push_back(iter.second);
    }
  }

  return handler.status();
}

Status VersionSet::GetOptionsFileManifestState(
    const std::string& dbname, FileSystem* fs,
    OptionsFileManifestState* manifest_state) {
  return GetOptionsFileManifestState(
      dbname, fs, manifest_state, /*next_file_number=*/nullptr,
      /*last_valid_manifest_record_end=*/nullptr);
}

Status VersionSet::GetOptionsFileManifestState(
    const std::string& dbname, FileSystem* fs,
    OptionsFileManifestState* manifest_state, uint64_t* next_file_number,
    uint64_t* last_valid_manifest_record_end) {
  assert(fs != nullptr);
  assert(manifest_state != nullptr);
  *manifest_state = OptionsFileManifestState();
  if (next_file_number != nullptr) {
    *next_file_number = 0;
  }
  if (last_valid_manifest_record_end != nullptr) {
    *last_valid_manifest_record_end = 0;
  }

  constexpr int kMaxAttempts = 2;
  for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
    std::string manifest_path;
    uint64_t manifest_file_number = 0;
    Status s = GetCurrentManifestPath(dbname, fs, /*is_retry=*/attempt > 0,
                                      &manifest_path, &manifest_file_number);
    if (s.IsNotFound()) {
      if (attempt + 1 == kMaxAttempts) {
        return Status::OK();
      }
      continue;
    }
    if (!s.ok()) {
      return s;
    }

    std::unique_ptr<FSSequentialFile> file;
    s = fs->NewSequentialFile(manifest_path, FileOptions(), &file, nullptr);
    if (s.IsNotFound() || s.IsPathNotFound()) {
      if (attempt + 1 == kMaxAttempts) {
        return s;
      }
      continue;
    }
    if (!s.ok()) {
      return s;
    }
    auto file_reader = std::make_unique<SequentialFileReader>(
        std::move(file), manifest_path, /*io_tracer=*/nullptr);
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(nullptr, std::move(file_reader), &reporter,
                       /*checksum=*/true, /*log_number=*/0);

    OptionsFileManifestState found_manifest_state;
    // The default column family is implicit in MANIFEST snapshots and is not
    // written as a kColumnFamilyAdd record.
    found_manifest_state.column_family_names.emplace(0,
                                                     kDefaultColumnFamilyName);
    uint64_t found_next_file_number = 0;
    uint64_t found_last_valid_record_end = 0;
    auto apply_edit = [&](const VersionEdit& edit) {
      found_manifest_state.ApplyColumnFamilyEdit(edit);
      if (edit.HasCommittedOptionsFileNumber()) {
        found_manifest_state.ApplyCommit(edit.GetCommittedOptionsFileNumber());
      }
      if (edit.HasNextFile()) {
        found_next_file_number = edit.GetNextFile();
      }
    };

    AtomicGroupReadBuffer atomic_group;
    Slice record;
    std::string scratch;
    while (s.ok() && reader.ReadRecord(&record, &scratch)) {
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (!s.ok()) {
        break;
      }
      s = atomic_group.AddEdit(&edit);
      if (!s.ok()) {
        break;
      }
      if (edit.IsInAtomicGroup()) {
        if (!atomic_group.IsFull()) {
          continue;
        }
        found_last_valid_record_end = reader.LastRecordEnd();
        for (const auto& grouped_edit : atomic_group.replay_buffer()) {
          apply_edit(grouped_edit);
        }
        atomic_group.Clear();
      } else {
        found_last_valid_record_end = reader.LastRecordEnd();
        apply_edit(edit);
      }
    }
    if (!s.ok()) {
      return s;
    }

    *manifest_state = found_manifest_state;
    if (next_file_number != nullptr) {
      *next_file_number = found_next_file_number;
    }
    if (last_valid_manifest_record_end != nullptr) {
      *last_valid_manifest_record_end = found_last_valid_record_end;
    }
    return Status::OK();
  }

  return Status::OK();
}

Status VersionSet::ResolveOptionsFileNumber(
    const std::string& dbname, FileSystem* fs,
    const OptionsFileManifestState& manifest_state,
    const std::vector<std::string>& filenames,
    bool inspect_options_file_tracking, OptionsFileSelection* selection) {
  assert(fs != nullptr);
  assert(selection != nullptr);
  *selection = OptionsFileSelection();

  std::map<uint64_t, std::string, std::greater<uint64_t>> options_files;
  for (const std::string& filename : filenames) {
    uint64_t number = 0;
    FileType type;
    if (ParseFileName(filename, &number, &type) && type == kOptionsFile) {
      options_files.emplace(number, filename);
    }
  }
  if (options_files.empty()) {
    return Status::OK();
  }

  const auto highest = options_files.begin();
  if (!manifest_state.has_committed_options_file_number &&
      !inspect_options_file_tracking) {
    selection->file_number = highest->first;
    return Status::OK();
  }

  auto is_tracked = [&](const std::string& filename, bool* tracked) -> Status {
    assert(tracked != nullptr);
    *tracked = false;
    const std::string path = dbname + "/" + filename;
    TEST_SYNC_POINT(
        "VersionSet::ResolveOptionsFileNumber:InspectTrackingMarker");
    std::unique_ptr<FSSequentialFile> file;
    Status s = fs->NewSequentialFile(path, FileOptions(), &file, nullptr);
    if (!s.ok()) {
      return s;
    }
    LineFileReader reader(std::move(file), path, /*io_tracer=*/nullptr,
                          std::vector<std::shared_ptr<EventListener>>{},
                          /*rate_limiter=*/nullptr);
    bool in_db_options = false;
    std::string line;
    while (reader.ReadLine(&line, Env::IO_TOTAL)) {
      line = RocksDBOptionsParser::TrimAndRemoveComment(line);
      if (line.empty()) {
        continue;
      }
      if (line.front() == '[' && line.back() == ']') {
        if (in_db_options) {
          return Status::OK();
        }
        in_db_options = line == "[DBOptions]";
        continue;
      }
      if (!in_db_options) {
        continue;
      }
      const size_t separator = line.find('=');
      if (separator == std::string::npos) {
        continue;
      }
      const std::string name = RocksDBOptionsParser::TrimAndRemoveComment(
          line.substr(0, separator), /*trim_only=*/true);
      if (name != "track_options_file_number_in_manifest") {
        continue;
      }
      const std::string value = RocksDBOptionsParser::TrimAndRemoveComment(
          line.substr(separator + 1), /*trim_only=*/true);
      if (value == "true" || value == "1") {
        *tracked = true;
        return Status::OK();
      }
      if (value == "false" || value == "0") {
        return Status::OK();
      }
      return Status::InvalidArgument(
          "Invalid track_options_file_number_in_manifest value in " + path);
    }
    return reader.GetStatus();
  };

  if (!manifest_state.has_committed_options_file_number) {
    selection->file_number = highest->first;
    bool highest_is_tracked = false;
    Status marker_status = is_tracked(highest->second, &highest_is_tracked);
    // Marker inspection only selects between legacy and MANIFEST semantics.
    // The caller that consumes the file will report any actual read error.
    selection->used_fallback = !marker_status.ok() || highest_is_tracked;
    return Status::OK();
  }

  std::vector<uint64_t> rejected_tracked_file_numbers;
  for (auto it = options_files.begin(); it != options_files.end(); ++it) {
    if (it->first == manifest_state.committed_options_file_number) {
      selection->file_number = it->first;
      selection->selected_by_manifest = true;
      selection->rejected_tracked_file_numbers =
          std::move(rejected_tracked_file_numbers);
      return Status::OK();
    }
    if (it->first < manifest_state.committed_options_file_number) {
      break;
    }
    bool tracked = false;
    Status marker_status = is_tracked(it->second, &tracked);
    if (!marker_status.ok()) {
      // An unreadable higher-numbered file cannot safely override a committed
      // snapshot, but it also must not make DB::Open fail. Loading the selected
      // file remains responsible for reporting content errors to utility APIs.
      // Keep the unreadable candidate because it might be a legacy snapshot.
      continue;
    }
    if (!tracked) {
      selection->file_number = it->first;
      selection->rejected_tracked_file_numbers =
          std::move(rejected_tracked_file_numbers);
      return Status::OK();
    }
    rejected_tracked_file_numbers.push_back(it->first);
  }

  selection->file_number = highest->first;
  selection->used_fallback = true;
  return Status::OK();
}

Status VersionSet::ReduceNumberOfLevels(const std::string& dbname,
                                        const Options* options,
                                        const FileOptions& file_options,
                                        int new_levels) {
  if (new_levels <= 1) {
    return Status::InvalidArgument(
        "Number of levels needs to be bigger than 1");
  }

  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;
  const WriteOptions write_options;

  ImmutableDBOptions imm_db_options(*options);
  MutableDBOptions mutable_db_options(*options);
  ColumnFamilyOptions cf_options(*options);
  std::shared_ptr<Cache> tc(NewLRUCache(options->max_open_files - 10,
                                        options->table_cache_numshardbits));
  WriteController wc(options->delayed_write_rate);
  WriteBufferManager wb(options->db_write_buffer_size);
  VersionSet versions(dbname, &imm_db_options, mutable_db_options, file_options,
                      tc.get(), &wb, &wc, nullptr /*BlockCacheTracer*/,
                      nullptr /*IOTracer*/,
                      /*db_id*/ "",
                      /*db_session_id*/ "", options->daily_offpeak_time_utc,
                      /*error_handler_*/ nullptr, /*unchanging=*/false);
  Status status;

  std::vector<ColumnFamilyDescriptor> dummy;
  ColumnFamilyDescriptor dummy_descriptor(kDefaultColumnFamilyName,
                                          ColumnFamilyOptions(*options));
  dummy.push_back(dummy_descriptor);
  status = versions.Recover(dummy);
  if (!status.ok()) {
    return status;
  }

  Version* current_version =
      versions.GetColumnFamilySet()->GetDefault()->current();
  auto* vstorage = current_version->storage_info();
  int current_levels = vstorage->num_levels();

  if (current_levels <= new_levels) {
    return Status::OK();
  }

  // Make sure there are file only on one level from
  // (new_levels-1) to (current_levels-1)
  int first_nonempty_level = -1;
  int first_nonempty_level_filenum = 0;
  for (int i = new_levels - 1; i < current_levels; i++) {
    int file_num = vstorage->NumLevelFiles(i);
    if (file_num != 0) {
      if (first_nonempty_level < 0) {
        first_nonempty_level = i;
        first_nonempty_level_filenum = file_num;
      } else {
        char msg[255];
        snprintf(msg, sizeof(msg),
                 "Found at least two levels containing files: "
                 "[%d:%d],[%d:%d].\n",
                 first_nonempty_level, first_nonempty_level_filenum, i,
                 file_num);
        return Status::InvalidArgument(msg);
      }
    }
  }

  // we need to allocate an array with the old number of levels size to
  // avoid SIGSEGV in WriteCurrentStatetoManifest()
  // however, all levels bigger or equal to new_levels will be empty
  std::vector<FileMetaData*>* new_files_list =
      new std::vector<FileMetaData*>[current_levels];
  for (int i = 0; i < new_levels - 1; i++) {
    new_files_list[i] = vstorage->LevelFiles(i);
  }

  if (first_nonempty_level > 0) {
    auto& new_last_level = new_files_list[new_levels - 1];

    new_last_level = vstorage->LevelFiles(first_nonempty_level);

    for (size_t i = 0; i < new_last_level.size(); ++i) {
      const FileMetaData* const meta = new_last_level[i];
      assert(meta);

      const uint64_t file_number = meta->fd.GetNumber();

      vstorage->file_locations_[file_number] =
          VersionStorageInfo::FileLocation(new_levels - 1, i);
    }
  }

  delete[] vstorage->files_;
  vstorage->files_ = new_files_list;
  vstorage->num_levels_ = new_levels;
  vstorage->ResizeCompactCursors(new_levels);

  VersionEdit ve;
  InstrumentedMutex dummy_mutex;
  InstrumentedMutexLock l(&dummy_mutex);
  return versions.LogAndApply(versions.GetColumnFamilySet()->GetDefault(),
                              read_options, write_options, &ve, &dummy_mutex,
                              nullptr, true);
}

// Get the checksum information including the checksum and checksum function
// name of all SST and blob files in VersionSet. Store the information in
// FileChecksumList which contains a map from file number to its checksum
// info. If DB is not running, make sure call VersionSet::Recover() to load
// the file metadata from Manifest to VersionSet before calling this function.
Status VersionSet::GetLiveFilesChecksumInfo(FileChecksumList* checksum_list) {
  // Clean the previously stored checksum information if any.
  Status s;
  if (checksum_list == nullptr) {
    s = Status::InvalidArgument("checksum_list is nullptr");
    return s;
  }
  checksum_list->reset();

  for (auto cfd : *column_family_set_) {
    assert(cfd);

    if (cfd->IsDropped() || !cfd->initialized()) {
      continue;
    }

    const auto* current = cfd->current();
    assert(current);

    const auto* vstorage = current->storage_info();
    assert(vstorage);

    /* SST files */
    for (int level = 0; level < cfd->NumberLevels(); level++) {
      const auto& level_files = vstorage->LevelFiles(level);

      for (const auto& file : level_files) {
        assert(file);

        s = checksum_list->InsertOneFileChecksum(file->fd.GetNumber(),
                                                 file->file_checksum,
                                                 file->file_checksum_func_name);
        if (!s.ok()) {
          return s;
        }
      }
    }

    /* Blob files */
    const auto& blob_files = vstorage->GetBlobFiles();
    for (const auto& meta : blob_files) {
      assert(meta);

      std::string checksum_value = meta->GetChecksumValue();
      std::string checksum_method = meta->GetChecksumMethod();
      assert(checksum_value.empty() == checksum_method.empty());
      if (meta->GetChecksumMethod().empty()) {
        checksum_value = kUnknownFileChecksum;
        checksum_method = kUnknownFileChecksumFuncName;
      }

      s = checksum_list->InsertOneFileChecksum(meta->GetBlobFileNumber(),
                                               checksum_value, checksum_method);
      if (!s.ok()) {
        return s;
      }
    }
  }

  return s;
}

Status VersionSet::DumpManifest(
    Options& options, std::string& dscname, bool verbose, bool hex, bool json,
    const std::vector<ColumnFamilyDescriptor>& cf_descs) {
  assert(options.env);
  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;

  std::vector<std::string> column_families;
  Status s = ListColumnFamiliesFromManifest(
      dscname, options.env->GetFileSystem().get(), &column_families);
  if (!s.ok()) {
    return s;
  }

  // Open the specified manifest file.
  std::unique_ptr<SequentialFileReader> file_reader;
  {
    std::unique_ptr<FSSequentialFile> file;
    const std::shared_ptr<FileSystem>& fs = options.env->GetFileSystem();
    s = fs->NewSequentialFile(
        dscname, fs->OptimizeForManifestRead(file_options_), &file, nullptr);
    if (!s.ok()) {
      return s;
    }
    file_reader = std::make_unique<SequentialFileReader>(
        std::move(file), dscname, db_options_->log_readahead_size, io_tracer_);
  }

  std::map<std::string, const ColumnFamilyDescriptor*> cf_name_to_desc;
  for (const auto& cf_desc : cf_descs) {
    cf_name_to_desc[cf_desc.name] = &cf_desc;
  }
  std::vector<ColumnFamilyDescriptor> final_cf_descs;
  for (const auto& cf : column_families) {
    const auto iter = cf_name_to_desc.find(cf);
    if (iter != cf_name_to_desc.cend()) {
      final_cf_descs.push_back(*iter->second);
    } else {
      final_cf_descs.emplace_back(cf, options);
    }
  }

  DumpManifestHandler handler(final_cf_descs, this, io_tracer_, read_options,
                              verbose, hex, json);
  {
    VersionSet::LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(nullptr, std::move(file_reader), &reporter,
                       true /* checksum */, 0 /* log_number */);
    handler.Iterate(reader, &s);
  }

  return handler.status();
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  // only called during recovery and repair which are single threaded, so this
  // works because there can't be concurrent calls
  if (next_file_number_.load(std::memory_order_relaxed) <= number) {
    next_file_number_.store(number + 1, std::memory_order_relaxed);
  }
}
// Called only either from ::LogAndApply which is protected by mutex or during
// recovery which is single-threaded.
void VersionSet::MarkMinLogNumberToKeep(uint64_t number) {
  if (min_log_number_to_keep_.load(std::memory_order_relaxed) < number) {
    min_log_number_to_keep_.store(number, std::memory_order_relaxed);
  }
}

Status VersionSet::WriteCurrentStateToManifest(
    const WriteOptions& write_options,
    const std::unordered_map<uint32_t, MutableCFState>& curr_state,
    const VersionEdit& wal_additions,
    const OptionsFileManifestState& options_file_manifest_state,
    log::Writer* log, IOStatus& io_s) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // WARNING: This method doesn't hold a mutex!!

  // This is done without DB mutex lock held, but only within single-threaded
  // LogAndApply. Column family manipulations can only happen within
  // LogAndApply (the same single thread), so we're safe to iterate.

  assert(io_s.ok());
  if (db_options_->write_dbid_to_manifest) {
    VersionEdit edit_for_db_id;
    assert(!db_id_.empty());
    edit_for_db_id.SetDBId(db_id_);
    std::string db_id_record;
    if (!edit_for_db_id.EncodeTo(&db_id_record)) {
      return Status::Corruption("Unable to Encode VersionEdit:" +
                                edit_for_db_id.DebugString(true));
    }
    io_s = log->AddRecord(write_options, db_id_record);
    if (!io_s.ok()) {
      return io_s;
    }
  }

  // Save WALs.
  if (!wal_additions.GetWalAdditions().empty()) {
    TEST_SYNC_POINT_CALLBACK("VersionSet::WriteCurrentStateToManifest:SaveWal",
                             const_cast<VersionEdit*>(&wal_additions));
    std::string record;
    if (!wal_additions.EncodeTo(&record)) {
      return Status::Corruption("Unable to Encode VersionEdit: " +
                                wal_additions.DebugString(true));
    }
    io_s = log->AddRecord(write_options, record);
    if (!io_s.ok()) {
      return io_s;
    }
  }

  // New manifest should rollover the WAL deletion record from previous
  // manifest. Otherwise, when an addition record of a deleted WAL gets added
  // to this new manifest later (which can happens in e.g, SyncWAL()), this
  // new manifest creates an illusion that such WAL hasn't been deleted.
  VersionEdit wal_deletions;
  wal_deletions.DeleteWalsBefore(min_log_number_to_keep());
  std::string wal_deletions_record;
  if (!wal_deletions.EncodeTo(&wal_deletions_record)) {
    return Status::Corruption("Unable to Encode VersionEdit: " +
                              wal_deletions.DebugString(true));
  }
  io_s = log->AddRecord(write_options, wal_deletions_record);
  if (!io_s.ok()) {
    return io_s;
  }

  for (auto cfd : *column_family_set_) {
    assert(cfd);

    if (cfd->IsDropped()) {
      continue;
    }
    assert(cfd->initialized());
    {
      // Store column family info
      VersionEdit edit;
      if (cfd->GetID() != 0) {
        // default column family is always there,
        // no need to explicitly write it
        edit.AddColumnFamily(cfd->GetName());
        edit.SetColumnFamily(cfd->GetID());
      }
      edit.SetComparatorName(
          cfd->internal_comparator().user_comparator()->Name());
      edit.SetPersistUserDefinedTimestamps(
          cfd->ioptions().persist_user_defined_timestamps);
      std::string record;
      if (!edit.EncodeTo(&record)) {
        return Status::Corruption("Unable to Encode VersionEdit:" +
                                  edit.DebugString(true));
      }
      io_s = log->AddRecord(write_options, record);
      if (!io_s.ok()) {
        return io_s;
      }
    }

    {
      // Save files
      VersionEdit edit;
      edit.SetColumnFamily(cfd->GetID());

      const auto* current = cfd->current();
      assert(current);

      const auto* vstorage = current->storage_info();
      assert(vstorage);

      for (int level = 0; level < cfd->NumberLevels(); level++) {
        const auto& level_files = vstorage->LevelFiles(level);

        for (const auto& f : level_files) {
          assert(f);
          // Preserve file_open_metadata so live SSTs still benefit from
          // fast_sst_open after a MANIFEST rotation. Without this, the
          // rotated MANIFEST snapshot drops kFileOpenMetadata for unchanged
          // files and their fast-open advantage is lost until they are
          // rewritten.
          edit.AddFile(level, f->fd.GetNumber(), f->fd.GetPathId(),
                       f->fd.GetFileSize(), f->smallest, f->largest,
                       f->fd.smallest_seqno, f->fd.largest_seqno,
                       f->marked_for_compaction, f->temperature,
                       f->oldest_blob_file_number, f->oldest_ancestor_time,
                       f->file_creation_time, f->epoch_number, f->file_checksum,
                       f->file_checksum_func_name, f->unique_id,
                       f->compensated_range_deletion_size, f->tail_size,
                       f->user_defined_timestamps_persisted, f->min_timestamp,
                       f->max_timestamp, f->file_open_metadata);
        }
      }

      edit.SetCompactCursors(vstorage->GetCompactCursors());

      const auto& blob_files = vstorage->GetBlobFiles();
      for (const auto& meta : blob_files) {
        assert(meta);

        const uint64_t blob_file_number = meta->GetBlobFileNumber();

        edit.AddBlobFile(blob_file_number, meta->GetTotalBlobCount(),
                         meta->GetTotalBlobBytes(), meta->GetChecksumMethod(),
                         meta->GetChecksumValue());
        if (meta->GetGarbageBlobCount() > 0) {
          edit.AddBlobFileGarbage(blob_file_number, meta->GetGarbageBlobCount(),
                                  meta->GetGarbageBlobBytes());
        }
      }

      const auto iter = curr_state.find(cfd->GetID());
      assert(iter != curr_state.end());
      uint64_t log_number = iter->second.log_number;
      edit.SetLogNumber(log_number);

      if (cfd->GetID() == 0) {
        // min_log_number_to_keep is for the whole db, not for specific column
        // family. So it does not need to be set for every column family, just
        // need to be set once. Since default CF can never be dropped, we set
        // the min_log to the default CF here.
        uint64_t min_log = min_log_number_to_keep();
        if (min_log != 0) {
          edit.SetMinLogNumberToKeep(min_log);
        }
      }

      const std::string& full_history_ts_low = iter->second.full_history_ts_low;
      if (!full_history_ts_low.empty()) {
        edit.SetFullHistoryTsLow(full_history_ts_low);
      }

      edit.SetLastSequence(descriptor_last_sequence_);

      const Comparator* ucmp = cfd->user_comparator();
      assert(ucmp);
      std::string record;
      if (!edit.EncodeTo(&record, ucmp->timestamp_size())) {
        return Status::Corruption("Unable to Encode VersionEdit:" +
                                  edit.DebugString(true));
      }
      io_s = log->AddRecord(write_options, record);
      if (!io_s.ok()) {
        return io_s;
      }
    }
  }

  if (options_file_manifest_state.has_committed_options_file_number) {
    assert(log != nullptr);
    VersionEdit edit;
    edit.SetCommittedOptionsFileNumber(
        options_file_manifest_state.committed_options_file_number);
    std::string record;
    if (!edit.EncodeTo(&record)) {
      return Status::Corruption("Unable to Encode VersionEdit:" +
                                edit.DebugString(true));
    }
    io_s = log->AddRecord(write_options, record);
    if (!io_s.ok()) {
      return io_s;
    }
  }

  // Record the approximate compacted manifest size so it's available at
  // recovery time for TuneMaxManifestFileSize(). This record must come last
  // in WriteCurrentStateToManifest for an accurate size estimate.
  {
    // Include a rough estimate of this record's own size (~15 bytes for the
    // VersionEdit payload + log record header).
    constexpr uint64_t kEstimatedRecordOverhead = 15;
    VersionEdit edit;
    edit.SetLastCompactedManifestFileSize(log->file()->GetFileSize() +
                                          kEstimatedRecordOverhead);
    std::string record;
    if (!edit.EncodeTo(&record)) {
      return Status::Corruption("Unable to Encode VersionEdit:" +
                                edit.DebugString(true));
    }
    io_s = log->AddRecord(write_options, record);
    if (!io_s.ok()) {
      return io_s;
    }
  }

  return Status::OK();
}

// TODO(aekmekji): in CompactionJob::GenSubcompactionBoundaries(), this
// function is called repeatedly with consecutive pairs of slices. For example
// if the slice list is [a, b, c, d] this function is called with arguments
// (a,b) then (b,c) then (c,d). Knowing this, an optimization is possible
// where we avoid doing binary search for the keys b and c twice and instead
// somehow maintain state of where they first appear in the files.
uint64_t VersionSet::ApproximateSize(const SizeApproximationOptions& options,
                                     const ReadOptions& read_options,
                                     Version* v, const Slice& start,
                                     const Slice& end, int start_level,
                                     int end_level, TableReaderCaller caller) {
  const auto& icmp = v->cfd_->internal_comparator();

  // pre-condition
  assert(icmp.Compare(start, end) <= 0);

  uint64_t total_full_size = 0;
  const auto* vstorage = v->storage_info();
  const int num_non_empty_levels = vstorage->num_non_empty_levels();
  end_level = (end_level == -1) ? num_non_empty_levels
                                : std::min(end_level, num_non_empty_levels);
  if (end_level <= start_level) {
    return 0;
  }

  // Outline of the optimization that uses options.files_size_error_margin.
  // When approximating the files total size that is used to store a keys
  // range, we first sum up the sizes of the files that fully fall into the
  // range. Then we sum up the sizes of all the files that may intersect with
  // the range (this includes all files in L0 as well). Then, if
  // total_intersecting_size is smaller than total_full_size *
  // options.files_size_error_margin - we can infer that the intersecting
  // files have a sufficiently negligible contribution to the total size, and
  // we can approximate the storage required for the keys in range as just
  // half of the intersecting_files_size. E.g., if the value of
  // files_size_error_margin is 0.1, then the error of the approximation is
  // limited to only ~10% of the total size of files that fully fall into the
  // keys range. In such case, this helps to avoid a costly process of binary
  // searching the intersecting files that is required only for a more precise
  // calculation of the total size.

  autovector<FdWithKeyRange*, 32> first_files;
  autovector<FdWithKeyRange*, 16> last_files;

  // scan all the levels
  for (int level = start_level; level < end_level; ++level) {
    const LevelFilesBrief& files_brief = vstorage->LevelFilesBrief(level);
    if (files_brief.num_files == 0) {
      // empty level, skip exploration
      continue;
    }

    if (level == 0) {
      // level 0 files are not in sorted order, we need to iterate through
      // the list to compute the total bytes that require scanning,
      // so handle the case explicitly (similarly to first_files case)
      for (size_t i = 0; i < files_brief.num_files; i++) {
        first_files.push_back(&files_brief.files[i]);
      }
      continue;
    }

    assert(level > 0);
    assert(files_brief.num_files > 0);

    // identify the file position for start key
    const int idx_start =
        FindFileInRange(icmp, files_brief, start, 0,
                        static_cast<uint32_t>(files_brief.num_files - 1));
    assert(static_cast<size_t>(idx_start) < files_brief.num_files);

    // identify the file position for end key
    int idx_end = idx_start;
    if (icmp.Compare(files_brief.files[idx_end].largest_key, end) < 0) {
      idx_end =
          FindFileInRange(icmp, files_brief, end, idx_start,
                          static_cast<uint32_t>(files_brief.num_files - 1));
    }
    assert(idx_end >= idx_start &&
           static_cast<size_t>(idx_end) < files_brief.num_files);

    // scan all files from the starting index to the ending index
    // (inferred from the sorted order)

    // first scan all the intermediate full files (excluding first and last)
    for (int i = idx_start + 1; i < idx_end; ++i) {
      uint64_t file_size = files_brief.files[i].fd.GetFileSize();
      // The entire file falls into the range, so we can just take its size.
      assert(file_size == ApproximateSize(read_options, v, files_brief.files[i],
                                          start, end, caller));
      total_full_size += file_size;
    }

    // save the first and the last files (which may be the same file), so we
    // can scan them later.
    first_files.push_back(&files_brief.files[idx_start]);
    if (idx_start != idx_end) {
      // we need to estimate size for both files, only if they are different
      last_files.push_back(&files_brief.files[idx_end]);
    }
  }

  // The sum of all file sizes that intersect the [start, end] keys range.
  uint64_t total_intersecting_size = 0;
  for (const auto* file_ptr : first_files) {
    total_intersecting_size += file_ptr->fd.GetFileSize();
  }
  for (const auto* file_ptr : last_files) {
    total_intersecting_size += file_ptr->fd.GetFileSize();
  }

  // Now scan all the first & last files at each level, and estimate their
  // size. If the total_intersecting_size is less than X% of the
  // total_full_size - we want to approximate the result in order to avoid the
  // costly binary search inside ApproximateSize. We use half of file size as
  // an approximation below.

  const double margin = options.files_size_error_margin;
  if (margin > 0 && total_intersecting_size <
                        static_cast<uint64_t>(total_full_size * margin)) {
    total_full_size += total_intersecting_size / 2;
  } else {
    // Estimate for all the first files (might also be last files), at each
    // level
    for (const auto file_ptr : first_files) {
      total_full_size +=
          ApproximateSize(read_options, v, *file_ptr, start, end, caller);
    }

    // Estimate for all the last files, at each level
    for (const auto file_ptr : last_files) {
      // We could use ApproximateSize here, but calling ApproximateOffsetOf
      // directly is just more efficient.
      total_full_size +=
          ApproximateOffsetOf(read_options, v, *file_ptr, end, caller);
    }
  }

  return total_full_size;
}

uint64_t VersionSet::ApproximateOffsetOf(const ReadOptions& read_options,
                                         Version* v, const FdWithKeyRange& f,
                                         const Slice& key,
                                         TableReaderCaller caller) {
  // pre-condition
  assert(v);
  const auto& icmp = v->cfd_->internal_comparator();

  uint64_t result = 0;
  if (icmp.Compare(f.largest_key, key) <= 0) {
    // Entire file is before "key", so just add the file size
    result = f.fd.GetFileSize();
  } else if (icmp.Compare(f.smallest_key, key) > 0) {
    // Entire file is after "key", so ignore
    result = 0;
  } else {
    // "key" falls in the range for this table.  Add the
    // approximate offset of "key" within the table.
    TableCache* table_cache = v->cfd_->table_cache();
    const MutableCFOptions& cf_opts = v->GetMutableCFOptions();
    if (table_cache != nullptr) {
      result = table_cache->ApproximateOffsetOf(
          read_options, key, *f.file_metadata, caller, icmp, cf_opts);
    }
  }
  return result;
}

uint64_t VersionSet::ApproximateSize(const ReadOptions& read_options,
                                     Version* v, const FdWithKeyRange& f,
                                     const Slice& start, const Slice& end,
                                     TableReaderCaller caller) {
  // pre-condition
  assert(v);
  const auto& icmp = v->cfd_->internal_comparator();
  assert(icmp.Compare(start, end) <= 0);

  if (icmp.Compare(f.largest_key, start) <= 0 ||
      icmp.Compare(f.smallest_key, end) > 0) {
    // Entire file is before or after the start/end keys range
    return 0;
  }

  if (icmp.Compare(f.smallest_key, start) >= 0) {
    // Start of the range is before the file start - approximate by end offset
    return ApproximateOffsetOf(read_options, v, f, end, caller);
  }

  if (icmp.Compare(f.largest_key, end) < 0) {
    // End of the range is after the file end - approximate by subtracting
    // start offset from the file size
    uint64_t start_offset =
        ApproximateOffsetOf(read_options, v, f, start, caller);
    assert(f.fd.GetFileSize() >= start_offset);
    return f.fd.GetFileSize() - start_offset;
  }

  // The interval falls entirely in the range for this file.
  TableCache* table_cache = v->cfd_->table_cache();
  if (table_cache == nullptr) {
    return 0;
  }
  const MutableCFOptions& cf_opts = v->GetMutableCFOptions();
  return table_cache->ApproximateSize(read_options, start, end,
                                      *f.file_metadata, caller, icmp, cf_opts);
}

void VersionSet::RemoveLiveFiles(
    std::vector<ObsoleteFileInfo>& sst_delete_candidates,
    std::vector<ObsoleteBlobFileInfo>& blob_delete_candidates) const {
  assert(column_family_set_);
  for (auto cfd : *column_family_set_) {
    assert(cfd);
    if (!cfd->initialized()) {
      continue;
    }

    auto* current = cfd->current();
    bool found_current = false;

    Version* const dummy_versions = cfd->dummy_versions();
    assert(dummy_versions);

    for (Version* v = dummy_versions->next_; v != dummy_versions;
         v = v->next_) {
      v->RemoveLiveFiles(sst_delete_candidates, blob_delete_candidates);
      if (v == current) {
        found_current = true;
      }
    }

    if (!found_current && current != nullptr) {
      // Should never happen unless it is a bug.
      assert(false);
      current->RemoveLiveFiles(sst_delete_candidates, blob_delete_candidates);
    }
  }
}

void VersionSet::AddLiveFiles(std::vector<uint64_t>* live_table_files,
                              std::vector<uint64_t>* live_blob_files) const {
  assert(live_table_files);
  assert(live_blob_files);

  // pre-calculate space requirement
  size_t total_table_files = 0;
  size_t total_blob_files = 0;

  assert(column_family_set_);
  for (auto cfd : *column_family_set_) {
    assert(cfd);

    if (!cfd->initialized()) {
      continue;
    }

    Version* const dummy_versions = cfd->dummy_versions();
    assert(dummy_versions);

    for (Version* v = dummy_versions->next_; v != dummy_versions;
         v = v->next_) {
      assert(v);

      const auto* vstorage = v->storage_info();
      assert(vstorage);

      for (int level = 0; level < vstorage->num_levels(); ++level) {
        total_table_files += vstorage->LevelFiles(level).size();
      }

      total_blob_files += vstorage->GetBlobFiles().size();
    }
  }

  // just one time extension to the right size
  live_table_files->reserve(live_table_files->size() + total_table_files);
  live_blob_files->reserve(live_blob_files->size() + total_blob_files);

  assert(column_family_set_);
  for (auto cfd : *column_family_set_) {
    assert(cfd);
    if (!cfd->initialized()) {
      continue;
    }

    auto* current = cfd->current();
    bool found_current = false;

    Version* const dummy_versions = cfd->dummy_versions();
    assert(dummy_versions);

    for (Version* v = dummy_versions->next_; v != dummy_versions;
         v = v->next_) {
      v->AddLiveFiles(live_table_files, live_blob_files);
      if (v == current) {
        found_current = true;
      }
    }

    if (!found_current && current != nullptr) {
      // Should never happen unless it is a bug.
      assert(false);
      current->AddLiveFiles(live_table_files, live_blob_files);
    }
  }
}

InternalIterator* VersionSet::MakeInputIterator(
    const ReadOptions& read_options, const Compaction* c,
    RangeDelAggregator* range_del_agg,
    const FileOptions& file_options_compactions,
    const std::optional<const Slice>& start,
    const std::optional<const Slice>& end, bool open_ephemeral_table_reader) {
  auto cfd = c->column_family_data();
  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  const size_t space = (c->level() == 0 ? c->input_levels(0)->num_files +
                                              c->num_input_levels() - 1
                                        : c->num_input_levels());
  InternalIterator** list = new InternalIterator*[space];
  // First item in the pair is a pointer to range tombstones.
  // Second item is a pointer to a member of a LevelIterator,
  // that will be initialized to where CompactionMergingIterator stores
  // pointer to its range tombstones. This is used by LevelIterator
  // to update pointer to range tombstones as it traverse different SST files.
  std::vector<std::pair<std::unique_ptr<TruncatedRangeDelIterator>,
                        std::unique_ptr<TruncatedRangeDelIterator>**>>
      range_tombstones;
  size_t num = 0;
  [[maybe_unused]] size_t num_input_files = 0;
  const TableCacheOpenOptions table_cache_open_options =
      GetCompactionTableCacheOpenOptions(open_ephemeral_table_reader);
  for (size_t which = 0; which < c->num_input_levels(); which++) {
    const LevelFilesBrief* flevel = c->input_levels(which);
    num_input_files += flevel->num_files;
    if (flevel->num_files != 0) {
      if (c->level(which) == 0) {
        for (size_t i = 0; i < flevel->num_files; i++) {
          const FileMetaData& fmd = *flevel->files[i].file_metadata;
          if (start.has_value() &&
              cfd->user_comparator()->CompareWithoutTimestamp(
                  *start, fmd.largest.user_key()) > 0) {
            continue;
          }
          // We should be able to filter out the case where the end key
          // equals to the end boundary, since the end key is exclusive.
          // We try to be extra safe here.
          if (end.has_value() &&
              cfd->user_comparator()->CompareWithoutTimestamp(
                  *end, fmd.smallest.user_key()) < 0) {
            continue;
          }
          std::unique_ptr<TruncatedRangeDelIterator> range_tombstone_iter =
              nullptr;
          list[num++] = cfd->table_cache()->NewIterator(
              read_options, file_options_compactions,
              cfd->internal_comparator(), fmd, range_del_agg,
              c->mutable_cf_options(),
              /*table_reader_ptr=*/nullptr,
              /*file_read_hist=*/nullptr, TableReaderCaller::kCompaction,
              /*arena=*/nullptr,
              /*skip_filters=*/false,
              /*level=*/static_cast<int>(c->level(which)),
              MaxFileSizeForL0MetaPin(c->mutable_cf_options()),
              /*smallest_compaction_key=*/nullptr,
              /*largest_compaction_key=*/nullptr,
              /*allow_unprepared_value=*/false,
              /*range_del_read_seqno=*/nullptr,
              /*range_del_iter=*/&range_tombstone_iter,
              /*maybe_pin_table_handle=*/false,
              /*file_open_metadata=*/nullptr, table_cache_open_options);
          range_tombstones.emplace_back(std::move(range_tombstone_iter),
                                        nullptr);
        }
      } else {
        // Create concatenating iterator for the files from this level
        std::unique_ptr<TruncatedRangeDelIterator>** tombstone_iter_ptr =
            nullptr;
        list[num++] = new LevelIterator(
            cfd->table_cache(), read_options, file_options_compactions,
            cfd->internal_comparator(), flevel, c->mutable_cf_options(),
            /*no per level latency histogram=*/nullptr,
            TableReaderCaller::kCompaction, /*skip_filters=*/false,
            /*level=*/static_cast<int>(c->level(which)), range_del_agg,
            c->boundaries(which), false, &tombstone_iter_ptr,
            kMaxSequenceNumber, db_options_->statistics.get(), clock_,
            open_ephemeral_table_reader);
        range_tombstones.emplace_back(nullptr, tombstone_iter_ptr);
      }
    }
  }
  TEST_SYNC_POINT_CALLBACK(
      "VersionSet::MakeInputIterator:NewCompactionMergingIterator",
      &num_input_files);
  assert(num <= space);
  InternalIterator* result = NewCompactionMergingIterator(
      &c->column_family_data()->internal_comparator(), list,
      static_cast<int>(num), range_tombstones, /*arena=*/nullptr,
      c->column_family_data()->internal_stats());
  delete[] list;
  return result;
}

Status VersionSet::GetMetadataForFile(uint64_t number, int* filelevel,
                                      FileMetaData** meta,
                                      ColumnFamilyData** cfd) {
  for (auto cfd_iter : *column_family_set_) {
    if (!cfd_iter->initialized()) {
      continue;
    }
    Version* version = cfd_iter->current();
    const auto* vstorage = version->storage_info();
    for (int level = 0; level < vstorage->num_levels(); level++) {
      for (const auto& file : vstorage->LevelFiles(level)) {
        if (file->fd.GetNumber() == number) {
          *meta = file;
          *filelevel = level;
          *cfd = cfd_iter;
          return Status::OK();
        }
      }
    }
  }
  return Status::NotFound("File not present in any level");
}

void VersionSet::GetLiveFilesMetaData(std::vector<LiveFileMetaData>* metadata) {
  if (!metadata) {
    return;
  }
  assert(metadata);
  size_t count = 0;
  for (auto cfd : *column_family_set_) {
    if (cfd->IsDropped() || !cfd->initialized()) {
      continue;
    }
    for (int level = 0; level < cfd->NumberLevels(); level++) {
      count += cfd->current()->storage_info()->LevelFiles(level).size();
    }
  }
  metadata->reserve(count);
  for (auto cfd : *column_family_set_) {
    if (cfd->IsDropped() || !cfd->initialized()) {
      continue;
    }
    for (int level = 0; level < cfd->NumberLevels(); level++) {
      for (const auto& file :
           cfd->current()->storage_info()->LevelFiles(level)) {
        LiveFileMetaData filemetadata;
        filemetadata.column_family_name = cfd->GetName();
        uint32_t path_id = file->fd.GetPathId();
        if (path_id < cfd->ioptions().cf_paths.size()) {
          filemetadata.db_path = cfd->ioptions().cf_paths[path_id].path;
        } else {
          assert(!cfd->ioptions().cf_paths.empty());
          filemetadata.db_path = cfd->ioptions().cf_paths.back().path;
        }
        filemetadata.directory = filemetadata.db_path;
        const uint64_t file_number = file->fd.GetNumber();
        filemetadata.name = MakeTableFileName("", file_number);
        filemetadata.relative_filename = filemetadata.name.substr(1);
        filemetadata.file_number = file_number;
        filemetadata.level = level;
        filemetadata.size = file->fd.GetFileSize();
        filemetadata.smallestkey = file->smallest.user_key().ToString();
        filemetadata.largestkey = file->largest.user_key().ToString();
        filemetadata.smallest_seqno = file->fd.smallest_seqno;
        filemetadata.largest_seqno = file->fd.largest_seqno;
        filemetadata.num_reads_sampled =
            file->stats.num_reads_sampled.load(std::memory_order_relaxed);
        filemetadata.being_compacted = file->being_compacted;
        filemetadata.num_entries = file->num_entries;
        filemetadata.num_deletions = file->num_deletions;
        filemetadata.oldest_blob_file_number = file->oldest_blob_file_number;
        filemetadata.file_checksum = file->file_checksum;
        filemetadata.file_checksum_func_name = file->file_checksum_func_name;
        filemetadata.temperature = file->temperature;
        filemetadata.oldest_ancestor_time = file->TryGetOldestAncestorTime();
        filemetadata.file_creation_time = file->TryGetFileCreationTime();
        filemetadata.epoch_number = file->epoch_number;
        metadata->push_back(filemetadata);
      }
    }
  }
}

void VersionSet::GetObsoleteFiles(std::vector<ObsoleteFileInfo>* files,
                                  std::vector<ObsoleteBlobFileInfo>* blob_files,
                                  std::vector<std::string>* manifest_filenames,
                                  uint64_t min_pending_output) {
  assert(files);
  assert(blob_files);
  assert(manifest_filenames);
  assert(files->empty());
  assert(blob_files->empty());
  assert(manifest_filenames->empty());

  std::vector<ObsoleteFileInfo> pending_files;
  for (auto& f : obsolete_files_) {
    if (f.metadata->fd.GetNumber() < min_pending_output) {
      files->emplace_back(std::move(f));
    } else {
      pending_files.emplace_back(std::move(f));
    }
  }
  obsolete_files_.swap(pending_files);

  std::vector<ObsoleteBlobFileInfo> pending_blob_files;
  for (auto& blob_file : obsolete_blob_files_) {
    if (blob_file.GetBlobFileNumber() < min_pending_output) {
      blob_files->emplace_back(std::move(blob_file));
    } else {
      pending_blob_files.emplace_back(std::move(blob_file));
    }
  }
  obsolete_blob_files_.swap(pending_blob_files);

  obsolete_manifests_.swap(*manifest_filenames);
}

uint64_t VersionSet::GetObsoleteSstFilesSize() const {
  uint64_t ret = 0;
  for (auto& f : obsolete_files_) {
    if (f.metadata != nullptr) {
      ret += f.metadata->fd.GetFileSize();
    }
  }
  return ret;
}

ColumnFamilyData* VersionSet::CreateColumnFamily(
    const ColumnFamilyOptions& cf_options, const ReadOptions& read_options,
    const VersionEdit* edit, bool read_only, bool options_are_sanitized) {
  assert(edit->IsColumnFamilyAdd());
  // Unchanging LSM tree implies no writes to the CF
  assert(!unchanging_ || read_only);

  MutableCFOptions dummy_cf_options;
  Version* dummy_versions =
      new Version(nullptr, this, file_options_, dummy_cf_options, io_tracer_);
  // Ref() dummy version once so that later we can call Unref() to delete it
  // by avoiding calling "delete" explicitly (~Version is private)
  dummy_versions->Ref();
  auto new_cfd = column_family_set_->CreateColumnFamily(
      edit->GetColumnFamilyName(), edit->GetColumnFamily(), dummy_versions,
      cf_options, read_only, options_are_sanitized);

  Version* v = new Version(new_cfd, this, file_options_,
                           new_cfd->GetLatestMutableCFOptions(), io_tracer_,
                           current_version_number_++);

  constexpr bool update_stats = false;

  v->PrepareAppend(read_options, update_stats);

  AppendVersion(new_cfd, v);
  // GetLatestMutableCFOptions() is safe here without mutex since the
  // cfd is not available to client
  new_cfd->CreateNewMemtable(LastSequence());
  new_cfd->SetLogNumber(edit->GetLogNumber());
  return new_cfd;
}

uint64_t VersionSet::GetNumLiveVersions(Version* dummy_versions) {
  uint64_t count = 0;
  for (Version* v = dummy_versions->next_; v != dummy_versions; v = v->next_) {
    count++;
  }
  return count;
}

uint64_t VersionSet::GetTotalSstFilesSize(Version* dummy_versions) {
  std::unordered_set<uint64_t> unique_files;
  uint64_t total_files_size = 0;
  for (Version* v = dummy_versions->next_; v != dummy_versions; v = v->next_) {
    VersionStorageInfo* storage_info = v->storage_info();
    for (int level = 0; level < storage_info->num_levels_; level++) {
      for (const auto& file_meta : storage_info->LevelFiles(level)) {
        if (unique_files.find(file_meta->fd.packed_number_and_path_id) ==
            unique_files.end()) {
          unique_files.insert(file_meta->fd.packed_number_and_path_id);
          total_files_size += file_meta->fd.GetFileSize();
        }
      }
    }
  }
  return total_files_size;
}

uint64_t VersionSet::GetTotalBlobFileSize(Version* dummy_versions) {
  std::unordered_set<uint64_t> unique_blob_files;

  uint64_t all_versions_blob_file_size = 0;

  for (auto* v = dummy_versions->next_; v != dummy_versions; v = v->next_) {
    // iterate all the versions
    const auto* vstorage = v->storage_info();
    assert(vstorage);

    const auto& blob_files = vstorage->GetBlobFiles();

    for (const auto& meta : blob_files) {
      assert(meta);

      const uint64_t blob_file_number = meta->GetBlobFileNumber();

      if (unique_blob_files.find(blob_file_number) == unique_blob_files.end()) {
        // find Blob file that has not been counted
        unique_blob_files.insert(blob_file_number);
        all_versions_blob_file_size += meta->GetBlobFileSize();
      }
    }
  }

  return all_versions_blob_file_size;
}

Status VersionSet::VerifyFileMetadata(const ReadOptions& read_options,
                                      ColumnFamilyData* cfd,
                                      const std::string& fpath, int level,
                                      const FileMetaData& meta) {
  uint64_t fsize = 0;
  Status status = fs_->GetFileSize(fpath, IOOptions(), &fsize, nullptr);
  if (status.ok()) {
    if (fsize != meta.fd.GetFileSize()) {
      status = Status::Corruption("File size mismatch: " + fpath);
    }
  }
  if (status.ok() && db_options_->verify_sst_unique_id_in_manifest) {
    assert(cfd);
    TableCache* table_cache = cfd->table_cache();
    assert(table_cache);

    const auto& cf_opts = cfd->GetLatestMutableCFOptions();
    size_t max_sz_for_l0_meta_pin = MaxFileSizeForL0MetaPin(cf_opts);

    const FileOptions& file_opts = file_options();

    Version* version = cfd->current();
    assert(version);
    VersionStorageInfo& storage_info = version->storage_info_;
    const InternalKeyComparator* icmp = storage_info.InternalComparator();
    assert(icmp);

    InternalStats* internal_stats = cfd->internal_stats();

    TableCache::TypedHandle* handle = nullptr;
    TableReader* table_reader = nullptr;
    FileMetaData meta_copy = meta;
    status = table_cache->FindTable(
        read_options, file_opts, *icmp, meta_copy, &handle, cf_opts,
        &table_reader, /*no_io=*/false, internal_stats->GetFileReadHist(level),
        false, level,
        /*prefetch_index_and_filter_in_cache*/ false, max_sz_for_l0_meta_pin,
        meta_copy.temperature);
    if (handle) {
      table_cache->get_cache().Release(handle);
    }
  }
  return status;
}

ReactiveVersionSet::ReactiveVersionSet(
    const std::string& dbname, const ImmutableDBOptions* imm_db_options,
    const MutableDBOptions& mutable_db_options,
    const FileOptions& _file_options, Cache* table_cache,
    WriteBufferManager* write_buffer_manager, WriteController* write_controller,
    const std::shared_ptr<IOTracer>& io_tracer, const std::string& db_id,
    const std::string& db_session_id, bool defer_sst_file_opening)
    : VersionSet(dbname, imm_db_options, mutable_db_options, _file_options,
                 table_cache, write_buffer_manager, write_controller,
                 /*block_cache_tracer=*/nullptr, io_tracer, db_id,
                 db_session_id, /*daily_offpeak_time_utc*/ "",
                 /*error_handler=*/nullptr, /*unchanging=*/false),
      defer_sst_file_opening_(defer_sst_file_opening) {}

ReactiveVersionSet::~ReactiveVersionSet() = default;

Status ReactiveVersionSet::Recover(
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::unique_ptr<log::FragmentBufferedReader>* manifest_reader,
    std::unique_ptr<log::Reader::Reporter>* manifest_reporter,
    std::unique_ptr<Status>* manifest_reader_status) {
  assert(manifest_reader != nullptr);
  assert(manifest_reporter != nullptr);
  assert(manifest_reader_status != nullptr);

  manifest_reader_status->reset(new Status());
  manifest_reporter->reset(new LogReporter());
  static_cast_with_check<LogReporter>(manifest_reporter->get())->status =
      manifest_reader_status->get();
  Status s = MaybeSwitchManifest(manifest_reporter->get(), manifest_reader);
  if (!s.ok()) {
    return s;
  }
  log::Reader* reader = manifest_reader->get();
  assert(reader);

  manifest_tailer_.reset(new ManifestTailer(
      column_families, const_cast<ReactiveVersionSet*>(this), io_tracer_,
      read_options_, trust_manifest_recovery_, defer_sst_file_opening_,
      EpochNumberRequirement::kMightMissing));

  manifest_tailer_->Iterate(*reader, manifest_reader_status->get());

  s = manifest_tailer_->status();
  if (s.ok()) {
    RecoverEpochNumbers();
  }
  return s;
}

Status ReactiveVersionSet::ReadAndApply(
    InstrumentedMutex* mu,
    std::unique_ptr<log::FragmentBufferedReader>* manifest_reader,
    Status* manifest_read_status,
    std::unordered_set<ColumnFamilyData*>* cfds_changed,
    std::vector<std::string>* files_to_delete) {
  assert(manifest_reader != nullptr);
  assert(cfds_changed != nullptr);
  mu->AssertHeld();

  Status s;
  log::Reader* reader = manifest_reader->get();
  assert(reader);
  s = MaybeSwitchManifest(reader->GetReporter(), manifest_reader);
  if (!s.ok()) {
    return s;
  }
  manifest_tailer_->Iterate(*(manifest_reader->get()), manifest_read_status);
  s = manifest_tailer_->status();
  if (s.ok()) {
    *cfds_changed = std::move(manifest_tailer_->GetUpdatedColumnFamilies());
  }
  if (files_to_delete) {
    *files_to_delete = manifest_tailer_->GetAndClearIntermediateFiles();
  }

  return s;
}

uint64_t ReactiveVersionSet::GetInstalledVersionLogNumber(
    uint32_t cf_id) const {
  assert(manifest_tailer_ != nullptr);
  return manifest_tailer_->GetInstalledVersionLogNumber(cf_id);
}

Status ReactiveVersionSet::MaybeSwitchManifest(
    log::Reader::Reporter* reporter,
    std::unique_ptr<log::FragmentBufferedReader>* manifest_reader) {
  assert(manifest_reader != nullptr);
  Status s;
  std::string manifest_path;
  s = GetCurrentManifestPath(dbname_, fs_.get(), /*is_retry=*/false,
                             &manifest_path, &manifest_file_number_);
  if (!s.ok()) {
    return s;
  }
  std::unique_ptr<FSSequentialFile> manifest_file;
  if (manifest_reader->get() != nullptr &&
      manifest_reader->get()->file()->file_name() == manifest_path) {
    // CURRENT points to the same MANIFEST as before, no need to switch
    // MANIFEST.
    return s;
  }
  assert(nullptr == manifest_reader->get() ||
         manifest_reader->get()->file()->file_name() != manifest_path);
  s = fs_->FileExists(manifest_path, IOOptions(), nullptr);
  if (s.IsNotFound()) {
    return Status::TryAgain(
        "The primary may have switched to a new MANIFEST and deleted the old "
        "one.");
  } else if (!s.ok()) {
    return s;
  }
  TEST_SYNC_POINT(
      "ReactiveVersionSet::MaybeSwitchManifest:"
      "AfterGetCurrentManifestPath:0");
  TEST_SYNC_POINT(
      "ReactiveVersionSet::MaybeSwitchManifest:"
      "AfterGetCurrentManifestPath:1");
  // The primary can also delete the MANIFEST while the secondary is reading
  // it. This is OK on POSIX. For other file systems, maybe create a hard link
  // to MANIFEST. The hard link should be cleaned up later by the secondary.
  s = fs_->NewSequentialFile(manifest_path,
                             fs_->OptimizeForManifestRead(file_options_),
                             &manifest_file, nullptr);
  std::unique_ptr<SequentialFileReader> manifest_file_reader;
  if (s.ok()) {
    manifest_file_reader.reset(new SequentialFileReader(
        std::move(manifest_file), manifest_path,
        db_options_->log_readahead_size, io_tracer_, db_options_->listeners));
    manifest_reader->reset(new log::FragmentBufferedReader(
        nullptr, std::move(manifest_file_reader), reporter, true /* checksum */,
        0 /* log_number */));
    ROCKS_LOG_INFO(db_options_->info_log, "Switched to new manifest: %s\n",
                   manifest_path.c_str());
    if (manifest_tailer_) {
      manifest_tailer_->PrepareToReadNewManifest();
    }
  } else if (s.IsPathNotFound()) {
    // This can happen if the primary switches to a new MANIFEST after the
    // secondary reads the CURRENT file but before the secondary actually
    // tries to open the MANIFEST.
    s = Status::TryAgain(
        "The primary may have switched to a new MANIFEST and deleted the old "
        "one.");
  }
  return s;
}

#ifndef NDEBUG
uint64_t ReactiveVersionSet::TEST_read_edits_in_atomic_group() const {
  assert(manifest_tailer_);
  return manifest_tailer_->GetReadBuffer().TEST_read_edits_in_atomic_group();
}
#endif  // !NDEBUG

std::vector<VersionEdit>& ReactiveVersionSet::replay_buffer() {
  assert(manifest_tailer_);
  return manifest_tailer_->GetReadBuffer().replay_buffer();
}

}  // namespace ROCKSDB_NAMESPACE

// Generate the regular and coroutine versions of the functions defined in
// version_set_sync_and_async.h by including it twice. Included here, after the
// namespace and all definitions in this file, so the generated bodies see the
// file-local types and helpers they use. The macros expand differently based on
// whether WITH_COROUTINES or WITHOUT_COROUTINES is defined.
// clang-format off
#define WITHOUT_COROUTINES
#include "db/version_set_sync_and_async.h"
#undef WITHOUT_COROUTINES
#define WITH_COROUTINES
#include "db/version_set_sync_and_async.h"
#undef WITH_COROUTINES
// clang-format on
