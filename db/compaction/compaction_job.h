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
#include <functional>
#include <limits>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/blob/blob_counting_iterator.h"
#include "db/blob/blob_file_completion_callback.h"
#include "db/column_family.h"
#include "db/compaction/compaction_iterator.h"
#include "db/compaction/compaction_outputs.h"
#include "db/flush_scheduler.h"
#include "db/internal_stats.h"
#include "db/job_context.h"
#include "db/log_writer.h"
#include "db/memtable_list.h"
#include "db/range_del_aggregator.h"
#include "db/seqno_to_time_mapping.h"
#include "db/version_edit.h"
#include "db/write_controller.h"
#include "db/write_thread.h"
#include "logging/event_logger.h"
#include "options/cf_options.h"
#include "options/db_options.h"
#include "port/port.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/compaction_job_stats.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/wal_iterator.h"
#include "util/autovector.h"
#include "util/stop_watch.h"
#include "util/thread_local.h"

namespace ROCKSDB_NAMESPACE {

class Arena;
class CompactionState;
class ErrorHandler;
class MemTable;
class SnapshotChecker;
class SystemClock;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

class SubcompactionState;

// CompactionJob is responsible for executing the compaction. Each (manual or
// automated) compaction corresponds to a CompactionJob object, and usually
// goes through the stages of `Prepare()`->`Run()`->`Install()`. CompactionJob
// will divide the compaction into subcompactions and execute them in parallel
// if needed.
//
// CompactionJob has 2 main stats:
// 1. CompactionJobStats job_stats_
//    CompactionJobStats is a public data structure which is part of Compaction
//    event listener that rocksdb share the job stats with the user.
//    Internally it's an aggregation of all the compaction_job_stats from each
//    `SubcompactionState`:
//                                           +------------------------+
//                                           | SubcompactionState     |
//                                           |                        |
//                                +--------->|   compaction_job_stats |
//                                |          |                        |
//                                |          +------------------------+
// +------------------------+     |
// | CompactionJob          |     |          +------------------------+
// |                        |     |          | SubcompactionState     |
// |   job_stats            +-----+          |                        |
// |                        |     +--------->|   compaction_job_stats |
// |                        |     |          |                        |
// +------------------------+     |          +------------------------+
//                                |
//                                |          +------------------------+
//                                |          | SubcompactionState     |
//                                |          |                        |
//                                +--------->+   compaction_job_stats |
//                                |          |                        |
//                                |          +------------------------+
//                                |
//                                |          +------------------------+
//                                |          |       ...              |
//                                +--------->+                        |
//                                           +------------------------+
//
// 2. CompactionStatsFull internal_stats_
//    `CompactionStatsFull` is an internal stats about the compaction, which
//    is eventually sent to `ColumnFamilyData::internal_stats_` and used for
//    logging and public metrics.
//    Internally, it's an aggregation of stats_ from each `SubcompactionState`.
//    It has 2 parts, ordinary output level stats and the proximal level output
//    stats.
//                                                +---------------------------+
//                                                | SubcompactionState        |
//                                                |                           |
//                                                | +----------------------+  |
//                                                | | CompactionOutputs    |  |
//                                                | | (normal output)      |  |
//                                            +---->|   stats_             |  |
//                                            |   | +----------------------+  |
//                                            |   |                           |
//                                            |   | +----------------------+  |
// +--------------------------------+         |   | | CompactionOutputs    |  |
// | CompactionJob                  |         |   | | (proximal_level)     |  |
// |                                |    +--------->|   stats_             |  |
// |   internal_stats_              |    |    |   | +----------------------+  |
// |    +-------------------------+ |    |    |   |                           |
// |    |output_level_stats       |------|----+   +---------------------------+
// |    +-------------------------+ |    |    |
// |                                |    |    |
// |    +-------------------------+ |    |    |   +---------------------------+
// |    |proximal_level_stats     |------+    |   | SubcompactionState        |
// |    +-------------------------+ |    |    |   |                           |
// |                                |    |    |   | +----------------------+  |
// |                                |    |    |   | | CompactionOutputs    |  |
// +--------------------------------+    |    |   | | (normal output)      |  |
//                                       |    +---->|   stats_             |  |
//                                       |        | +----------------------+  |
//                                       |        |                           |
//                                       |        | +----------------------+  |
//                                       |        | | CompactionOutputs    |  |
//                                       |        | | (proximal_level)     |  |
//                                       +--------->|   stats_             |  |
//                                                | +----------------------+  |
//                                                |                           |
//                                                +---------------------------+

class CompactionJob {
 public:
  // Constant false aborted flag, used for compaction service jobs
  static const std::atomic<int> kCompactionAbortedFalse;

  CompactionJob(int job_id, Compaction* compaction,
                const ImmutableDBOptions& db_options,
                const MutableDBOptions& mutable_db_options,
                const FileOptions& file_options, VersionSet* versions,
                const std::atomic<bool>* shutting_down, LogBuffer* log_buffer,
                FSDirectory* db_directory, FSDirectory* output_directory,
                FSDirectory* blob_output_directory, Statistics* stats,
                InstrumentedMutex* db_mutex, ErrorHandler* db_error_handler,
                JobContext* job_context, std::shared_ptr<Cache> table_cache,
                EventLogger* event_logger, bool paranoid_file_checks,
                bool measure_io_stats, const std::string& dbname,
                CompactionJobStats* compaction_job_stats,
                Env::Priority thread_pri,
                const std::shared_ptr<IOTracer>& io_tracer,
                const std::atomic<bool>& manual_compaction_canceled,
                const std::atomic<int>& compaction_aborted,
                const std::string& db_id = "",
                const std::string& db_session_id = "",
                std::string full_history_ts_low = "", std::string trim_ts = "",
                BlobFileCompletionCallback* blob_callback = nullptr,
                int* bg_compaction_scheduled = nullptr,
                int* bg_bottom_compaction_scheduled = nullptr,
                std::atomic<int>* num_running_remote_compactions = nullptr);

  virtual ~CompactionJob();

  // no copy/move
  CompactionJob(CompactionJob&& job) = delete;
  CompactionJob(const CompactionJob& job) = delete;
  CompactionJob& operator=(const CompactionJob& job) = delete;
  CompactionJob& operator=(CompactionJob&& job) = delete;

  // REQUIRED: mutex held
  // Prepare for the compaction by setting up boundaries for each subcompaction
  // and organizing seqno <-> time info. `known_single_subcompact` is non-null
  // if we already have a known single subcompaction, with optional key bounds
  // (currently for executing a remote compaction).
  //
  // @param compaction_progress Previously saved compaction progress
  //   to resume from. If empty, compaction starts fresh from the
  //   beginning.
  //
  // @param compaction_progress_writer Writer for persisting
  //   subcompaction progress periodically during compaction
  //   execution. If nullptr, progress tracking is disabled and compaction
  //   cannot be resumed later.
  void Prepare(
      std::optional<std::pair<std::optional<Slice>, std::optional<Slice>>>
          known_single_subcompact,
      const CompactionProgress& compaction_progress = CompactionProgress{},
      log::Writer* compaction_progress_writer = nullptr);

  // REQUIRED mutex not held
  // Launch threads for each subcompaction and wait for them to finish. After
  // that, verify table is usable and finally do bookkeeping to unify
  // subcompaction results
  Status Run();

  // REQUIRED: mutex held
  // Add compaction input/output to the current version
  // Releases compaction file through Compaction::ReleaseCompactionFiles().
  // Sets *compaction_released to true if compaction is released.
  Status Install(bool* compaction_released);

  // Return the IO status
  IOStatus io_status() const { return io_status_; }

 protected:
  void UpdateCompactionJobOutputStatsFromInternalStats(
      const Status& status,
      const InternalStats::CompactionStatsFull& internal_stats) const;

  void LogCompaction();
  virtual void RecordCompactionIOStats();
  void CleanupCompaction();

  // Iterate through input and compact the kv-pairs.
  void ProcessKeyValueCompaction(SubcompactionState* sub_compact);

  CompactionState* compact_;
  InternalStats::CompactionStatsFull internal_stats_;
  const ImmutableDBOptions& db_options_;
  const MutableDBOptions mutable_db_options_copy_;
  LogBuffer* log_buffer_;
  FSDirectory* output_directory_;
  Statistics* stats_;
  // Is this compaction creating a file in the bottom most level?
  bool bottommost_level_;

  Env::WriteLifeTimeHint write_hint_;

  IOStatus io_status_;

  CompactionJobStats* job_stats_;

  // Lower bound for the minimal sequence number whose write time is
  // preserved. See Prepare().
  SequenceNumber preserve_time_min_seqno_floor_ = 0;

 private:
  friend class CompactionJobTestBase;

  // Collect the following stats from input files and table properties
  // - num_input_files_in_non_output_levels
  // - num_input_files_in_output_level
  // - bytes_read_non_output_levels
  // - bytes_read_output_level
  // - num_input_records
  // - bytes_read_blob
  // - num_dropped_records
  // and set them in internal_stats_.output_level_stats
  //
  // @param num_input_range_del if non-null, will be set to the number of range
  // deletion entries in this compaction input.
  //
  // If any input file has potentially unreliable num_entries count (old SST
  // files - details in implementation),
  // job_stats_->has_accurate_num_input_records is set to false.
  //
  // Returns true iff internal_stats_.output_level_stats.num_input_records and
  // num_input_range_del are calculated successfully.
  //
  // This should be called only once for compactions (not per subcompaction)
  bool UpdateInternalStatsFromInputFiles(
      uint64_t* num_input_range_del = nullptr);

  void UpdateCompactionJobInputStatsFromInternalStats(
      const InternalStats::CompactionStatsFull& internal_stats,
      uint64_t num_input_range_del) const;

  Status VerifyInputRecordCount(uint64_t num_input_range_del) const;
  Status VerifyOutputRecordCount() const;

  // Generates a histogram representing potential divisions of key ranges from
  // the input. It adds the starting and/or ending keys of certain input files
  // to the working set and then finds the approximate size of data in between
  // each consecutive pair of slices. Then it divides these ranges into
  // consecutive groups such that each group has a similar size.
  void GenSubcompactionBoundaries();

  void MaybeAssignCompactionProgressAndWriter(
      const CompactionProgress& compaction_progress,
      log::Writer* compaction_progress_writer);

  // Get the number of planned subcompactions based on max_subcompactions and
  // extra reserved resources
  uint64_t GetSubcompactionsLimit();

  // Additional reserved threads are reserved and the number is stored in
  // extra_num_subcompaction_threads_reserved__. For now, this happens only if
  // the compaction priority is round-robin and max_subcompactions is not
  // sufficient (extra resources may be needed)
  void AcquireSubcompactionResources(int num_extra_required_subcompactions);

  // Additional threads may be reserved during IncreaseSubcompactionResources()
  // if num_actual_subcompactions is less than num_planned_subcompactions.
  // Additional threads will be released and the bg_compaction_scheduled_ or
  // bg_bottom_compaction_scheduled_ will be updated if they are used.
  // DB Mutex lock is required.
  void ShrinkSubcompactionResources(uint64_t num_extra_resources);

  // Release all reserved threads and update the compaction limits.
  void ReleaseSubcompactionResources();

  void InitializeCompactionRun();
  void RunSubcompactions();
  void RecordRemoteCompactionInputBlobFileCount();
  void UpdateTimingStats(uint64_t start_micros);
  void RemoveEmptyOutputs();
  void CleanupAbortedSubcompactions();
  bool HasNewBlobFiles() const;
  Status CollectSubcompactionErrors();
  Status SyncOutputDirectories();
  Status VerifyOutputFiles();
  void SetOutputTableProperties();
  // Aggregates subcompaction output stats to internal stat, and aggregates
  // subcompaction's compaction job stats to the whole entire surrounding
  // compaction job stats.
  void AggregateSubcompactionOutputAndJobStats();
  Status VerifyCompactionRecordCounts(bool stats_built_from_input_table_prop,
                                      uint64_t num_input_range_del);
  void FinalizeCompactionRun(const Status& status,
                             bool stats_built_from_input_table_prop,
                             uint64_t num_input_range_del);

  CompactionServiceJobStatus ProcessKeyValueCompactionWithCompactionService(
      SubcompactionState* sub_compact,
      CompactionFilter::MergeOrder merge_order);

  struct CompactionIOStatsSnapshot {
    PerfLevel prev_perf_level = PerfLevel::kEnableTime;
    uint64_t prev_write_nanos = 0;
    uint64_t prev_fsync_nanos = 0;
    uint64_t prev_range_sync_nanos = 0;
    uint64_t prev_prepare_write_nanos = 0;
    uint64_t prev_cpu_write_nanos = 0;
    uint64_t prev_cpu_read_nanos = 0;
  };

  struct SubcompactionKeyBoundaries {
    const std::optional<const Slice> start;
    const std::optional<const Slice> end;

    // Boundaries without timestamps for read options
    std::optional<Slice> start_without_ts;
    std::optional<Slice> end_without_ts;

    // Timestamp management
    static constexpr char kMaxTs[] =
        "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff";
    std::string max_ts;
    Slice ts_slice;

    // Internal key boundaries
    IterKey start_ikey;
    IterKey end_ikey;
    Slice start_internal_key;
    Slice end_internal_key;

    // User key boundaries
    Slice start_user_key;
    Slice end_user_key;

    SubcompactionKeyBoundaries(std::optional<const Slice> start_boundary,
                               std::optional<const Slice> end_boundary)
        : start(start_boundary), end(end_boundary) {}
  };

  struct SubcompactionInternalIterators {
    std::unique_ptr<InternalIterator> raw_input;
    std::unique_ptr<InternalIterator> clip;
    std::unique_ptr<BlobCountingIterator> blob_counter;
    std::unique_ptr<InternalIterator> trim_history_iter;
  };

  bool ShouldUseLocalCompaction(SubcompactionState* sub_compact);
  CompactionIOStatsSnapshot InitializeIOStats();
  Status SetupAndValidateCompactionFilter(
      SubcompactionState* sub_compact,
      const CompactionFilter* configured_compaction_filter,
      const CompactionFilter*& compaction_filter,
      std::unique_ptr<CompactionFilter>& compaction_filter_from_factory,
      CompactionFilter::MergeOrder& merge_order);
  void InitializeReadOptionsAndBoundaries(
      size_t ts_sz, ReadOptions& read_options,
      SubcompactionKeyBoundaries& boundaries);
  InternalIterator* CreateInputIterator(
      SubcompactionState* sub_compact, ColumnFamilyData* cfd,
      SubcompactionInternalIterators& iterators,
      SubcompactionKeyBoundaries& boundaries, ReadOptions& read_options);
  void CreateBlobFileBuilder(
      SubcompactionState* sub_compact, ColumnFamilyData* cfd,
      std::unique_ptr<BlobFileBuilder>& blob_file_builder,
      const WriteOptions& write_options);
  std::unique_ptr<CompactionIterator> CreateCompactionIterator(
      SubcompactionState* sub_compact, ColumnFamilyData* cfd,
      InternalIterator* input_iter, const CompactionFilter* compaction_filter,
      MergeHelper& merge, std::unique_ptr<BlobFileBuilder>& blob_file_builder,
      const WriteOptions& write_options);
  std::pair<CompactionFileOpenFunc, CompactionFileCloseFunc> CreateFileHandlers(
      SubcompactionState* sub_compact, SubcompactionKeyBoundaries& boundaries,
      BlobFileBuilder* blob_file_builder,
      const BlobCountingIterator* blob_counter);
  Status ProcessKeyValue(SubcompactionState* sub_compact, ColumnFamilyData* cfd,
                         CompactionIterator* c_iter,
                         const CompactionFileOpenFunc& open_file_func,
                         const CompactionFileCloseFunc& close_file_func,
                         bool defer_blob_output_preparation,
                         uint64_t& prev_cpu_micros);
  void UpdateSubcompactionJobStatsIncrementally(
      CompactionIterator* c_iter, CompactionJobStats* compaction_job_stats,
      uint64_t cur_cpu_micros, uint64_t& prev_cpu_micros);
  void FinalizeSubcompactionJobStats(SubcompactionState* sub_compact,
                                     CompactionIterator* c_iter,
                                     uint64_t start_cpu_micros,
                                     uint64_t prev_cpu_micros,
                                     const CompactionIOStatsSnapshot& io_stats);
  Status FinalizeProcessKeyValueStatus(ColumnFamilyData* cfd,
                                       InternalIterator* input_iter,
                                       CompactionIterator* c_iter,
                                       Status status);
  Status CleanupCompactionFiles(SubcompactionState* sub_compact, Status status,
                                const CompactionFileOpenFunc& open_file_func,
                                const CompactionFileCloseFunc& close_file_func);
  Status FinalizeBlobFiles(SubcompactionState* sub_compact,
                           BlobFileBuilder* blob_file_builder, Status status);
  void FinalizeSubcompaction(SubcompactionState* sub_compact, Status status,
                             const CompactionFileOpenFunc& open_file_func,
                             const CompactionFileCloseFunc& close_file_func,
                             BlobFileBuilder* blob_file_builder,
                             CompactionIterator* c_iter,
                             InternalIterator* input_iter,
                             uint64_t start_cpu_micros,
                             uint64_t prev_cpu_micros,
                             const CompactionIOStatsSnapshot& io_stats);

  // update the thread status for starting a compaction.
  void ReportStartedCompaction(Compaction* compaction);

  Status FinishCompactionOutputFile(
      const Status& input_status,
      const ParsedInternalKey& prev_iter_output_internal_key,
      const Slice& next_table_min_key, const Slice* comp_start_user_key,
      const Slice* comp_end_user_key, const CompactionIterator* c_iter,
      bool post_prepare_retry, SubcompactionState* sub_compact,
      BlobFileBuilder* blob_file_builder,
      const BlobCountingIterator* blob_counter, CompactionOutputs& outputs);
  Status InstallCompactionResults(bool* compaction_released);
  Status OpenCompactionOutputFile(SubcompactionState* sub_compact,
                                  CompactionOutputs& outputs);

  void RecordDroppedKeys(const CompactionIterationStats& c_iter_stats,
                         CompactionJobStats* compaction_job_stats = nullptr);

  void NotifyOnSubcompactionBegin(SubcompactionState* sub_compact);

  void NotifyOnSubcompactionCompleted(SubcompactionState* sub_compact);

  uint32_t job_id_;
  // Limits unsafe resume-boundary diagnostics to one per compaction job.
  mutable std::atomic<bool> logged_unsafe_resume_boundary_{false};

  // DBImpl state
  const std::string& dbname_;
  const std::string db_id_;
  const std::string db_session_id_;
  const FileOptions file_options_;

  Env* env_;
  std::shared_ptr<IOTracer> io_tracer_;
  FileSystemPtr fs_;
  // env_option optimized for compaction table reads
  FileOptions file_options_for_read_;
  // file_options_for_read_ plus compaction-input-only overrides.
  FileOptions file_options_for_compaction_input_read_;
  VersionSet* versions_;
  const std::atomic<bool>* shutting_down_;
  const std::atomic<bool>& manual_compaction_canceled_;
  const std::atomic<int>& compaction_aborted_;
  FSDirectory* db_directory_;
  FSDirectory* blob_output_directory_;
  InstrumentedMutex* db_mutex_;
  ErrorHandler* db_error_handler_;

  SequenceNumber earliest_snapshot_;
  JobContext* job_context_;

  std::shared_ptr<Cache> table_cache_;

  EventLogger* event_logger_;

  bool paranoid_file_checks_;
  bool measure_io_stats_;
  // Stores the Slices that designate the boundaries for each subcompaction
  std::vector<std::string> boundaries_;
  Env::Priority thread_pri_;
  std::string full_history_ts_low_;
  std::string trim_ts_;
  BlobFileCompletionCallback* blob_callback_;

  uint64_t GetCompactionId(SubcompactionState* sub_compact) const;
  // Stores the number of reserved threads in shared env_ for the number of
  // extra subcompaction in kRoundRobin compaction priority
  int extra_num_subcompaction_threads_reserved_;

  // Stores the pointer to bg_compaction_scheduled_,
  // bg_bottom_compaction_scheduled_ in DBImpl. Mutex is required when accessing
  // or updating it.
  int* bg_compaction_scheduled_;
  int* bg_bottom_compaction_scheduled_;

  // Stores the pointer to DBImpl::num_running_remote_compactions_, backing the
  // public rocksdb.num-running-remote-compactions property. Null when the job
  // has no owning DBImpl to report to (compaction service workers, unit tests).
  // Updated without the DB mutex held.
  std::atomic<int>* num_running_remote_compactions_;

  // Stores the sequence number to time mapping gathered from all input files
  // it also collects the smallest_seqno -> oldest_ancestor_time from the SST.
  SeqnoToTimeMapping seqno_to_time_mapping_;

  // Max seqno that can be zeroed out in last level, including for preserving
  // write times.
  SequenceNumber preserve_seqno_after_ = kMaxSequenceNumber;

  // Minimal sequence number to preclude the data from the last level. If the
  // key has bigger (newer) sequence number than this, it will be precluded from
  // the last level (output to proximal level).
  SequenceNumber proximal_after_seqno_ = kMaxSequenceNumber;

  // Options File Number used for Remote Compaction
  // Setting this requires DBMutex.
  uint64_t options_file_number_ = 0;

  // MANIFEST position (file number and size, in bytes) captured in Prepare()
  // (mutex held) to send to the remote worker as a floor. Zero when
  // DBOptions::remote_compaction_manifest_floor is off (or this is not a remote
  // compaction), meaning no floor is sent. Setting these requires DBMutex.
  uint64_t min_manifest_file_number_ = 0;
  uint64_t min_manifest_file_size_ = 0;

  // Remote subcompactions run concurrently and validate their blob garbage
  // before moving output into the primary. This aggregate prevents each
  // subcompaction from independently claiming up to the same blob-file total.
  port::Mutex remote_blob_garbage_mutex_;
  std::unordered_map<uint64_t, BlobGarbageMeter::BlobStats>
      remote_blob_garbage_;

  // Minimum blob-file dependency among subcompactions successfully dispatched
  // to a remote worker. Recorded once for the whole job after they finish.
  std::atomic<uint64_t> remote_compaction_min_blob_file_number_{
      std::numeric_limits<uint64_t>::max()};

  // Writer for persisting compaction progress during compaction
  log::Writer* compaction_progress_writer_ = nullptr;

  // Get table file name in where it's outputting to, which should also be in
  // `output_directory_`.
  virtual std::string GetTableFileName(uint64_t file_number);
  virtual const std::string& GetBlobOutputPath() const;
  virtual uint64_t NewBlobFileNumber();
  virtual uint64_t GetNextBlobFileNumber() const;
  virtual Status RestoreNextBlobFileNumber(
      const SubcompactionProgress& subcompaction_progress);
  virtual std::optional<uint64_t> GetBlobGarbageCollectionCutoffFileNumber()
      const {
    return std::nullopt;
  }

  // True if this job builds output files as a remote (offloaded)
  // CompactionService worker. Overridden by CompactionServiceCompactionJob.
  virtual bool IsRemoteCompaction() const { return false; }
  virtual Status ValidateCompactionFilterMergeOrder(
      CompactionFilter::MergeOrder /*merge_order*/) {
    return Status::OK();
  }
  // The rate limiter priority (io_priority) is determined dynamically here.
  // The Compaction Read and Write priorities are the same for different
  // scenarios, such as write stalled.
  Env::IOPriority GetRateLimiterPriority();

  Status MaybeResumeSubcompactionProgressOnInputIterator(
      SubcompactionState* sub_compact, InternalIterator* input_iter);

  Status ReadOutputFilesTableProperties(
      const autovector<FileMetaData>& temporary_output_file_allocation,
      const ReadOptions& read_options,
      std::vector<std::shared_ptr<const TableProperties>>&
          output_files_table_properties,
      bool is_proximal_level = false);

  Status ReadTablePropertiesDirectly(
      const ImmutableOptions& ioptions, const MutableCFOptions& moptions,
      const FileMetaData* file_meta, const ReadOptions& read_options,
      std::shared_ptr<const TableProperties>* tp);

  void RestoreCompactionOutputs(
      const ColumnFamilyData* cfd,
      const std::vector<std::shared_ptr<const TableProperties>>&
          output_files_table_properties,
      SubcompactionProgressPerLevel& subcompaction_progress_per_level,
      CompactionOutputs* outputs_to_restore);

  bool ShouldUpdateSubcompactionProgress(
      const SubcompactionState* sub_compact, const CompactionIterator* c_iter,
      const ParsedInternalKey& prev_iter_output_internal_key,
      const Slice& next_table_min_internal_key, const FileMetaData* meta,
      const BlobFileBuilder* blob_file_builder,
      const BlobCountingIterator* blob_counter, bool post_prepare_retry) const;

  void UpdateSubcompactionProgress(const CompactionIterator* c_iter,
                                   const Slice next_table_min_key,
                                   SubcompactionState* sub_compact);

  Status RestoreBlobCompactionProgress(
      SubcompactionState* sub_compact,
      const SubcompactionProgress& subcompaction_progress);

  void UpdateBlobCompactionProgress(
      SubcompactionState* sub_compact,
      SubcompactionProgress& subcompaction_progress);

  Status PersistSubcompactionProgress(SubcompactionState* sub_compact);

  void UpdateSubcompactionProgressPerLevel(
      SubcompactionState* sub_compact, bool is_proximal_level,
      SubcompactionProgress& subcompaction_progress);
};

// Version of the blob-aware remote compaction result produced by this binary.
// Zero means the worker did not advertise support.
constexpr uint64_t kCompactionServiceBlobVersion = 1;
constexpr uint64_t kCompactionServiceBlobGCCutoffVersion = 1;
constexpr uint64_t kCompactionServiceMergeBeforeFilterVersion = 1;

// CompactionServiceInput is used the pass compaction information between two
// db instances. It contains the information needed to do a compaction. It
// doesn't contain the LSM tree information, which is passed though MANIFEST
// file.
struct CompactionServiceInput {
  std::string cf_name;

  // Requests merge-before-filter. Old workers ignore this field and return no
  // applied-order acknowledgment, so the primary falls back to local compaction
  // before importing their output.
  bool merge_before_filter = false;

  // Snapshot sequence numbers in strictly increasing order.
  std::vector<SequenceNumber> snapshots;

  // SST files for compaction, it should already be expended to include all the
  // files needed for this compaction, for both input level files and output
  // level files.
  std::vector<std::string> input_files;

  // Blob files potentially referenced by input_files. CompactionService
  // implementations can use this list to make the files available to the
  // remote worker.
  std::vector<std::string> input_blob_files;

  // When present, the remote worker uses this primary-computed cutoff instead
  // of deriving one from its reconstructed Version. An absent field disables
  // blob GC on the worker for compatibility with older primaries.
  bool has_blob_gc_cutoff_file_number = false;
  uint64_t blob_gc_cutoff_file_number = 0;

  // The primary reserves this half-open file-number range before scheduling
  // the remote compaction. A worker that creates blob files must allocate them
  // consecutively from this range.
  uint64_t blob_file_number_start = kInvalidBlobFileNumber;
  uint64_t blob_file_number_limit = kInvalidBlobFileNumber;
  int output_level = 0;

  // db_id is used to generate unique id of sst on the remote compactor
  std::string db_id;

  // information for subcompaction
  bool has_begin = false;
  std::string begin;
  bool has_end = false;
  std::string end;

  uint64_t options_file_number = 0;

  // MANIFEST position (file number and size, in bytes) the primary scheduled
  // this compaction from, used as a floor by the remote worker: it refuses to
  // reconstruct the compaction against an older MANIFEST view (an older file
  // number, or the same file number read to a smaller size) and falls back to
  // local compaction. A non-zero min_manifest_file_number also signals the
  // worker to use "trust the MANIFEST" recovery. Zero means the primary did not
  // provide a floor (kill switch off, or an older primary); the worker then
  // uses the original recovery with no floor check.
  // See DBOptions::remote_compaction_manifest_floor.
  uint64_t min_manifest_file_number = 0;
  uint64_t min_manifest_file_size = 0;

  // The primary's values. The worker opens the DB as a secondary, which
  // sanitizes them to 0 as a read-only DB. They are sent here rather than
  // re-read from the OPTIONS file so that they always arrive together with
  // preserve_time_min_seqno_floor: a preserve window without that floor would
  // bring back the kBottommostFiles compaction loop.
  uint64_t preserve_internal_time_seconds = 0;
  uint64_t preclude_last_level_data_seconds = 0;
  // The primary's lower bound for the minimal sequence number whose write time
  // is preserved: the version-carried preserve boundary for a kBottommostFiles
  // compaction, 0 otherwise.
  SequenceNumber preserve_time_min_seqno_floor = 0;
  // The primary's per-key placement decisions. The worker declines the job if
  // its own Compaction differs. proximal_level is Compaction::kInvalidLevel
  // when per-key placement is off, and 0 only from an older primary.
  int proximal_level = 0;
  int proximal_output_range_type = 0;
  SequenceNumber keep_in_last_level_through_seqno = 0;

  // serialization interface to read and write the object
  static Status Read(const std::string& data_str, CompactionServiceInput* obj);
  Status Write(std::string* output);

#ifndef NDEBUG
  bool TEST_Equals(CompactionServiceInput* other);
  bool TEST_Equals(CompactionServiceInput* other, std::string* mismatch);
#endif  // NDEBUG
};

// CompactionServiceOutputFile is the metadata for the output SST file
struct CompactionServiceOutputFile {
  std::string file_name;
  uint64_t file_size{};
  SequenceNumber smallest_seqno{};
  SequenceNumber largest_seqno{};
  std::string smallest_internal_key;
  std::string largest_internal_key;
  uint64_t oldest_blob_file_number = kInvalidBlobFileNumber;
  uint64_t oldest_ancestor_time = kUnknownOldestAncestorTime;
  uint64_t file_creation_time = kUnknownFileCreationTime;
  uint64_t epoch_number = kUnknownEpochNumber;
  std::string file_checksum = kUnknownFileChecksum;
  std::string file_checksum_func_name = kUnknownFileChecksumFuncName;
  uint64_t paranoid_hash{};
  bool marked_for_compaction;
  UniqueId64x2 unique_id{};
  TableProperties table_properties;
  bool is_proximal_level_output;
  Temperature file_temperature = Temperature::kUnknown;

  CompactionServiceOutputFile() = default;
  CompactionServiceOutputFile(
      const std::string& name, uint64_t size, SequenceNumber smallest,
      SequenceNumber largest, std::string _smallest_internal_key,
      std::string _largest_internal_key, uint64_t _oldest_blob_file_number,
      uint64_t _oldest_ancestor_time, uint64_t _file_creation_time,
      uint64_t _epoch_number, const std::string& _file_checksum,
      const std::string& _file_checksum_func_name, uint64_t _paranoid_hash,
      bool _marked_for_compaction, UniqueId64x2 _unique_id,
      const TableProperties& _table_properties, bool _is_proximal_level_output,
      Temperature _file_temperature)
      : file_name(name),
        file_size(size),
        smallest_seqno(smallest),
        largest_seqno(largest),
        smallest_internal_key(std::move(_smallest_internal_key)),
        largest_internal_key(std::move(_largest_internal_key)),
        oldest_blob_file_number(_oldest_blob_file_number),
        oldest_ancestor_time(_oldest_ancestor_time),
        file_creation_time(_file_creation_time),
        epoch_number(_epoch_number),
        file_checksum(_file_checksum),
        file_checksum_func_name(_file_checksum_func_name),
        paranoid_hash(_paranoid_hash),
        marked_for_compaction(_marked_for_compaction),
        unique_id(std::move(_unique_id)),
        table_properties(_table_properties),
        is_proximal_level_output(_is_proximal_level_output),
        file_temperature(_file_temperature) {}
};

// CompactionServiceResult contains the compaction result from a different db
// instance, with these information, the primary db instance with write
// permission is able to install the result to the DB.
struct CompactionServiceResult {
  Status status;
  // Nonzero only after validating the actual worker filter/factory order.
  uint64_t merge_before_filter_version = 0;
  // The worker declined an order mismatch before processing input records.
  bool merge_order_mismatch = false;
  uint64_t num_purge_normalized = 0;
  std::vector<CompactionServiceOutputFile> output_files;

  // Blob metadata generated by the remote compaction. Blob file additions
  // identify files in output_path that must be installed before the output
  // SSTs. Garbage records apply to blob files already known to the primary.
  std::vector<BlobFileAddition> blob_file_additions;
  std::vector<BlobFileGarbage> blob_file_garbages;
  uint64_t blob_compaction_version = 0;
  uint64_t blob_gc_cutoff_version = 0;
  int output_level = 0;

  // location of the output files
  std::string output_path;

  uint64_t bytes_read = 0;
  uint64_t bytes_written = 0;

  // Job-level Compaction Stats.
  //
  // NOTE: Job level stats cannot be rebuilt from scratch by simply aggregating
  // per-level stats due to some fields populated directly during compaction
  // (e.g. RecordDroppedKeys()). This is why we need both job-level stats and
  // per-level in the serialized result. If rebuilding job-level stats from
  // per-level stats become possible in the future, consider deprecating this
  // field.
  CompactionJobStats stats;

  // Per-level Compaction Stats for both output_level_stats and
  // proximal_level_stats
  InternalStats::CompactionStatsFull internal_stats;

  // serialization interface to read and write the object
  static Status Read(const std::string& data_str, CompactionServiceResult* obj);
  Status Write(std::string* output);

#ifndef NDEBUG
  bool TEST_Equals(CompactionServiceResult* other);
  bool TEST_Equals(CompactionServiceResult* other, std::string* mismatch);
#endif  // NDEBUG
};

// CompactionServiceCompactionJob is an read-only compaction job, it takes
// input information from `compaction_service_input` and put result information
// in `compaction_service_result`, the SST files are generated to `output_path`.
class CompactionServiceCompactionJob : private CompactionJob {
 public:
  CompactionServiceCompactionJob(
      int job_id, Compaction* compaction, const ImmutableDBOptions& db_options,
      const MutableDBOptions& mutable_db_options,
      const FileOptions& file_options, VersionSet* versions,
      const std::atomic<bool>* shutting_down, LogBuffer* log_buffer,
      FSDirectory* output_directory, Statistics* stats,
      InstrumentedMutex* db_mutex, ErrorHandler* db_error_handler,
      JobContext* job_context, std::shared_ptr<Cache> table_cache,
      EventLogger* event_logger, const std::string& dbname,
      const std::shared_ptr<IOTracer>& io_tracer,
      const std::atomic<bool>& manual_compaction_canceled,
      const std::string& db_id, const std::string& db_session_id,
      std::string output_path,
      const CompactionServiceInput& compaction_service_input,
      CompactionServiceResult* compaction_service_result);

  // REQUIRED: mutex held
  // Like CompactionJob::Prepare()
  void Prepare(
      const CompactionProgress& compaction_progress = CompactionProgress{},
      log::Writer* compaction_progress_writer = nullptr);

  // Run the compaction in current thread and return the result
  Status Run();

  void CleanupCompaction();

  IOStatus io_status() const { return CompactionJob::io_status(); }

 protected:
  void RecordCompactionIOStats() override;

 private:
  // Get table file name in output_path
  std::string GetTableFileName(uint64_t file_number) override;
  const std::string& GetBlobOutputPath() const override;
  uint64_t NewBlobFileNumber() override;
  uint64_t GetNextBlobFileNumber() const override;
  Status RestoreNextBlobFileNumber(
      const SubcompactionProgress& subcompaction_progress) override;
  std::optional<uint64_t> GetBlobGarbageCollectionCutoffFileNumber()
      const override;
  bool IsRemoteCompaction() const override { return true; }
  Status ValidateCompactionFilterMergeOrder(
      CompactionFilter::MergeOrder merge_order) override;
  // Specific the compaction output path, otherwise it uses default DB path
  const std::string output_path_;

  // Compaction job input
  const CompactionServiceInput& compaction_input_;

  uint64_t next_blob_file_number_;

  // Compaction job result
  CompactionServiceResult* compaction_result_;
};

}  // namespace ROCKSDB_NAMESPACE
