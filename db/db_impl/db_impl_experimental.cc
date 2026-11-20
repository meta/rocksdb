//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cinttypes>
#include <vector>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/job_context.h"
#include "db/version_set.h"
#include "logging/logging.h"
#include "rocksdb/status.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {

Status DBImpl::SuggestCompactRange(ColumnFamilyHandle* column_family,
                                   const Slice* begin, const Slice* end) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();
  InternalKey start_key;
  InternalKey end_key;
  if (begin != nullptr) {
    start_key.SetMinPossibleForUserKey(*begin);
  }
  if (end != nullptr) {
    end_key.SetMaxPossibleForUserKey(*end);
  }
  {
    InstrumentedMutexLock l(&mutex_);
    auto vstorage = cfd->current()->storage_info();
    for (int level = 0; level < vstorage->num_non_empty_levels() - 1; ++level) {
      std::vector<FileMetaData*> inputs;
      vstorage->GetOverlappingInputs(
          level, begin == nullptr ? nullptr : &start_key,
          end == nullptr ? nullptr : &end_key, &inputs);
      for (auto f : inputs) {
        f->marked_for_compaction = true;
      }
    }
    // Since we have some more files to compact, we should also recompute
    // compaction score
    vstorage->ComputeCompactionScore(cfd->ioptions(),
                                     cfd->GetLatestMutableCFOptions(),
                                     cfd->GetFullHistoryTsLow());
    EnqueuePendingCompaction(cfd);
    MaybeScheduleFlushOrCompaction();
  }
  return Status::OK();
}

Status DBImpl::PromoteL0(ColumnFamilyHandle* column_family, int target_level) {
  assert(column_family);

  if (target_level < 1) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "PromoteL0 FAILED. Invalid target level %d\n", target_level);
    return Status::InvalidArgument("Invalid target level");
  }
  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;
  const WriteOptions write_options;

  Status status;
  VersionEdit edit;
  JobContext job_context(next_job_id_.fetch_add(1), true);
  {
    InstrumentedMutexLock l(&mutex_);
    auto* cfd = static_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();
    const auto* vstorage = cfd->current()->storage_info();

    if (target_level >= vstorage->num_levels()) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "PromoteL0 FAILED. Target level %d does not exist\n",
                     target_level);
      job_context.Clean();
      status = Status::InvalidArgument("Target level does not exist");
      return status;
    }

    // Sort L0 files by range.
    const InternalKeyComparator* icmp = &cfd->internal_comparator();
    auto l0_files = vstorage->LevelFiles(0);
    std::sort(l0_files.begin(), l0_files.end(),
              [icmp](FileMetaData* f1, FileMetaData* f2) {
                return icmp->Compare(f1->largest, f2->largest) < 0;
              });

    // Check that no L0 file is being compacted and that they have
    // non-overlapping ranges.
    for (size_t i = 0; i < l0_files.size(); ++i) {
      auto f = l0_files[i];
      if (f->being_compacted) {
        ROCKS_LOG_INFO(immutable_db_options_.info_log,
                       "PromoteL0 FAILED. File %" PRIu64 " being compacted\n",
                       f->fd.GetNumber());
        job_context.Clean();
        status =
            Status::InvalidArgument("PromoteL0 called during L0 compaction");
        return status;
      }

      if (i == 0) {
        continue;
      }
      auto prev_f = l0_files[i - 1];
      if (icmp->Compare(prev_f->largest, f->smallest) >= 0) {
        ROCKS_LOG_INFO(immutable_db_options_.info_log,
                       "PromoteL0 FAILED. Files %" PRIu64 " and %" PRIu64
                       " have overlapping ranges\n",
                       prev_f->fd.GetNumber(), f->fd.GetNumber());
        job_context.Clean();
        status = Status::InvalidArgument("L0 has overlapping files");
        return status;
      }
    }

    // Check that all levels up to target_level are empty.
    for (int level = 1; level <= target_level; ++level) {
      if (vstorage->NumLevelFiles(level) > 0) {
        ROCKS_LOG_INFO(immutable_db_options_.info_log,
                       "PromoteL0 FAILED. Level %d not empty\n", level);
        job_context.Clean();
        status = Status::InvalidArgument(
            "All levels up to target_level "
            "must be empty");
        return status;
      }
    }

    edit.SetColumnFamily(cfd->GetID());
    for (const auto& f : l0_files) {
      edit.DeleteFile(0, f->fd.GetNumber());
      edit.AddExistingFile(target_level, *f, kProvenanceMovedBetweenLevels);
    }

    status = versions_->LogAndApply(cfd, read_options, write_options, &edit,
                                    &mutex_, directories_.GetDbDir());
    if (status.ok()) {
      InstallSuperVersionAndScheduleWork(
          cfd, job_context.superversion_contexts.data(),
          /*defer_snapshot_refresh=*/false);
    }
  }  // lock released here
  LogFlush(immutable_db_options_.info_log);
  job_context.Clean();

  return status;
}

}  // namespace ROCKSDB_NAMESPACE
