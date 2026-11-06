//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <deque>
#include <limits>
#include <list>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "db/blob/blob_file_completion_callback.h"
#include "db/column_family.h"
#include "db/flush_scheduler.h"
#include "db/internal_stats.h"
#include "db/job_context.h"
#include "db/log_writer.h"
#include "db/logs_with_prep_tracker.h"
#include "db/memtable_list.h"
#include "db/seqno_to_time_mapping.h"
#include "db/snapshot_impl.h"
#include "db/version_edit.h"
#include "db/write_controller.h"
#include "db/write_thread.h"
#include "logging/event_logger.h"
#include "monitoring/instrumented_mutex.h"
#include "options/db_options.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/listener.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/wal_iterator.h"
#include "util/autovector.h"
#include "util/stop_watch.h"
#include "util/thread_local.h"

namespace ROCKSDB_NAMESPACE {

class DBImpl;
class MemTable;
class SnapshotChecker;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;
class Arena;

class FlushJob {
 public:
  // TODO(icanadi) make effort to reduce number of parameters here
  // IMPORTANT: mutable_cf_options needs to be alive while FlushJob is alive
  FlushJob(const std::string& dbname, ColumnFamilyData* cfd,
           const ImmutableDBOptions& db_options,
           const MutableCFOptions& mutable_cf_options, uint64_t max_memtable_id,
           const FileOptions& file_options, VersionSet* versions,
           InstrumentedMutex* db_mutex, std::atomic<bool>* shutting_down,
           JobContext* job_context, FlushReason flush_reason,
           LogBuffer* log_buffer, FSDirectory* db_directory,
           FSDirectory* output_file_directory,
           CompressionType output_compression, Statistics* stats,
           EventLogger* event_logger, bool measure_io_stats,
           const bool sync_output_directory, const bool write_manifest,
           Env::Priority thread_pri, const std::shared_ptr<IOTracer>& io_tracer,
           std::shared_ptr<const SeqnoToTimeMapping> seqno_to_time_mapping,
           const std::string& db_id = "", const std::string& db_session_id = "",
           std::string full_history_ts_low = "",
           BlobFileCompletionCallback* blob_callback = nullptr,
           bool fast_sst_open = false);

  ~FlushJob();

  // Require db_mutex held.
  // Once PickMemTable() is called, either Run() or Cancel() has to be called.
  void PickMemTable();
  // @param skip_since_bg_error If not nullptr and if atomic_flush=false,
  // then it is set to true if flush installation is skipped and memtable
  // is rolled back due to existing background error.
  Status Run(LogsWithPrepTracker* prep_tracker = nullptr,
             FileMetaData* file_meta = nullptr,
             bool* switched_to_mempurge = nullptr,
             bool* skipped_since_bg_error = nullptr,
             ErrorHandler* error_handler = nullptr);
  void Cancel();
  const autovector<ReadOnlyMemTable*>& GetMemTables() const { return mems_; }

  // Returns the log number recorded in the flush VersionEdit after
  // PickMemTable() initializes `edit_`.
  uint64_t GetLogNumber() const {
    assert(edit_ != nullptr);
    return edit_->GetLogNumber();
  }

  // Stashes write-path blob files so WriteLevel0Table() can add them to the
  // same VersionEdit as the flushed SST.
  void AddExternalBlobFileAdditions(std::vector<BlobFileAddition>&& additions) {
    external_blob_file_additions_ = std::move(additions);
  }

  // Stashes write-path initial-garbage updates so they are committed with the
  // same VersionEdit as the matching blob-file additions and flushed SST.
  void AddExternalBlobFileGarbages(std::vector<BlobFileGarbage>&& garbages) {
    external_blob_file_garbages_ = std::move(garbages);
  }

  // Transfers back any prepared blob file additions that were not consumed by
  // the flush.
  std::vector<BlobFileAddition> TakeExternalBlobFileAdditions() {
    return std::move(external_blob_file_additions_);
  }

  // Transfers back any prepared blob-file garbage updates that were not
  // consumed by the flush.
  std::vector<BlobFileGarbage> TakeExternalBlobFileGarbages() {
    return std::move(external_blob_file_garbages_);
  }

  std::list<std::unique_ptr<FlushJobInfo>>* GetCommittedFlushJobsInfo() {
    return &committed_flush_jobs_info_;
  }

 private:
  friend class FlushJobTest_GetRateLimiterPriorityForWrite_Test;

  void ReportStartedFlush();
  static void ReportFlushInputSize(const autovector<ReadOnlyMemTable*>& mems);
  void RecordFlushIOStats();
  Status WriteLevel0Table();

  // Memtable Garbage Collection algorithm: a MemPurge takes the list
  // of immutable memtables and filters out (or "purge") the outdated bytes
  // out of it. The output (the filtered bytes, or "useful payload") is
  // then transferred into a new memtable. If this memtable is filled, then
  // the mempurge is aborted and rerouted to a regular flush process. Else,
  // depending on the heuristics, placed onto the immutable memtable list.
  // The addition to the imm list will not trigger a flush operation. The
  // flush of the imm list will instead be triggered once the mutable memtable
  // is added to the imm list.
  // This process is typically intended for workloads with heavy overwrites
  // when we want to avoid SSD writes (and reads) as much as possible.
  // "MemPurge" is an experimental feature still at a very early stage
  // of development. At the moment it is only compatible with the Get, Put,
  // Delete operations as well as Iterators and CompactionFilters.
  // For this early version, "MemPurge" is called by setting the
  // options.experimental_mempurge_threshold value as >0.0. When this is
  // the case, ALL automatic flush operations (kWRiteBufferManagerFull) will
  // first go through the MemPurge process. Therefore, we strongly
  // recommend all users not to set this flag as true given that the MemPurge
  // process has not matured yet.
  Status MemPurge();
  bool MemPurgeDecider(double threshold);
  // The rate limiter priority (io_priority) is determined dynamically here.
  Env::IOPriority GetRateLimiterPriority();
  std::unique_ptr<FlushJobInfo> GetFlushJobInfo() const;

  // Require db_mutex held.
  // Called only when UDT feature is enabled and
  // `persist_user_defined_timestamps` flag is false. Because we will refrain
  // from flushing as long as there are still UDTs in a memtable that hasn't
  // expired w.r.t `full_history_ts_low`. However, flush is continued if there
  // is risk of entering write stall mode. In that case, we need
  // to track the effective cutoff timestamp below which all the udts are
  // removed because of flush, and use it to increase `full_history_ts_low` if
  // the effective cutoff timestamp is newer. See
  // `MaybeIncreaseFullHistoryTsLowToAboveCutoffUDT` for details.
  void GetEffectiveCutoffUDTForPickedMemTables();

  // If this column family enables tiering feature, it will find the current
  // `preclude_last_level_min_seqno_`, and the smaller one between this and
  // the `earliset_snapshot_` will later be announced to user property
  // collectors. It indicates to tiering use cases which data are old enough to
  // be placed on the last level.
  void GetPrecludeLastLevelMinSeqno();

  Status MaybeIncreaseFullHistoryTsLowToAboveCutoffUDT();

  const std::string& dbname_;
  const std::string db_id_;
  const std::string db_session_id_;
  ColumnFamilyData* cfd_;
  const ImmutableDBOptions& db_options_;
  const MutableCFOptions& mutable_cf_options_;
  // A variable storing the largest memtable id to flush in this
  // flush job. RocksDB uses this variable to select the memtables to flush in
  // this job. All memtables in this column family with an ID smaller than or
  // equal to max_memtable_id_ will be selected for flush.
  uint64_t max_memtable_id_;
  FileOptions file_options_;
  VersionSet* versions_;
  InstrumentedMutex* db_mutex_;
  std::atomic<bool>* shutting_down_;
  SequenceNumber earliest_snapshot_;
  JobContext* job_context_;
  FlushReason flush_reason_;
  LogBuffer* log_buffer_;
  FSDirectory* db_directory_;
  FSDirectory* output_file_directory_;
  CompressionType output_compression_;
  Statistics* stats_;
  EventLogger* event_logger_;
  TableProperties table_properties_;
  bool measure_io_stats_;
  // True if this flush job should call fsync on the output directory. False
  // otherwise.
  // Usually sync_output_directory_ is true. A flush job needs to call sync on
  // the output directory before committing to the MANIFEST.
  // However, an individual flush job does not have to call sync on the output
  // directory if it is part of an atomic flush. After all flush jobs in the
  // atomic flush succeed, call sync once on each distinct output directory.
  const bool sync_output_directory_;
  // True if this flush job should write to MANIFEST after successfully
  // flushing memtables. False otherwise.
  // Usually write_manifest_ is true. A flush job commits to the MANIFEST after
  // flushing the memtables.
  // However, an individual flush job cannot rashly write to the MANIFEST
  // immediately after it finishes the flush if it is part of an atomic flush.
  // In this case, only after all flush jobs succeed in flush can RocksDB
  // commit to the MANIFEST.
  const bool write_manifest_;
  // The current flush job can commit flush result of a concurrent flush job.
  // We collect FlushJobInfo of all jobs committed by current job and fire
  // OnFlushCompleted for them.
  std::list<std::unique_ptr<FlushJobInfo>> committed_flush_jobs_info_;

  // Variables below are set by PickMemTable():
  FileMetaData meta_;
  // Memtables to be flushed by this job.
  // Ordered by increasing memtable id, i.e., oldest memtable first.
  autovector<ReadOnlyMemTable*> mems_;
  VersionEdit* edit_;
  Version* base_;
  bool pick_memtable_called;
  Env::Priority thread_pri_;

  const std::shared_ptr<IOTracer> io_tracer_;
  SystemClock* clock_;

  const std::string full_history_ts_low_;
  BlobFileCompletionCallback* blob_callback_;
  bool fast_sst_open_;
  // Write-path blob files that should be committed with this flush.
  std::vector<BlobFileAddition> external_blob_file_additions_;
  // Initial garbage for write-path blob files that were partially abandoned
  // before their owning flush committed.
  std::vector<BlobFileGarbage> external_blob_file_garbages_;

  // Shared copy of DB's seqno to time mapping stored in SuperVersion. The
  // ownership is shared with this FlushJob when it's created.
  // FlushJob accesses and ref counts immutable MemTables directly via
  // `MemTableListVersion` instead of ref `SuperVersion`, so we need to give
  // the flush job shared ownership of the mapping.
  // Note this is only installed when seqno to time recording feature is
  // enables, so it could be nullptr.
  std::shared_ptr<const SeqnoToTimeMapping> seqno_to_time_mapping_;

  // Keeps track of the newest user-defined timestamp for this flush job if
  // `persist_user_defined_timestamps` flag is false.
  std::string cutoff_udt_;

  // The current minimum seqno that compaction jobs will preclude the data from
  // the last level. Data with seqnos larger than this or larger than
  // `earliest_snapshot_` will be output to the proximal level had it gone
  // through a compaction to the last level.
  SequenceNumber preclude_last_level_min_seqno_ = kMaxSequenceNumber;
};

}  // namespace ROCKSDB_NAMESPACE
